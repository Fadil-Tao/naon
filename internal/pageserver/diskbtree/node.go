package diskbtree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/Fadil-Tao/naon/internal/pageserver/blockio"
)

const (
	headerSize = 4
	valueSize  = 8
)

var ErrCorrupt = errors.New("diskbtree: corrupt node")

func nodeSize(keyLen, prefixLen, n int) int {
	return headerSize + prefixLen + n*(keyLen-prefixLen+valueSize)
}

func commonPrefixLen(a, b []byte) int {
	n := min(len(a), len(b))
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}

func encodeNode(level uint8, keyLen int, keys [][]byte, values []uint64) []byte {
	block := make([]byte, blockio.BlockSize)
	prefixLen := 0
	if len(keys) > 0 {
		prefixLen = commonPrefixLen(keys[0], keys[len(keys)-1])
	}
	block[0] = level
	binary.BigEndian.PutUint16(block[1:3], uint16(len(keys)))
	block[3] = uint8(prefixLen)
	pos := headerSize
	if len(keys) > 0 {
		pos += copy(block[pos:], keys[0][:prefixLen])
	}
	for i, k := range keys {
		pos += copy(block[pos:], k[prefixLen:])
		binary.BigEndian.PutUint64(block[pos:], values[i])
		pos += valueSize
	}
	return block
}

type node struct {
	level     uint8
	n         int
	keyLen    int
	prefix    []byte
	entries   []byte 
	suffixLen int
}

func decodeNode(block []byte, keyLen int) (node, error) {
	if len(block) != blockio.BlockSize {
		return node{}, fmt.Errorf("%w: block is %d bytes", ErrCorrupt, len(block))
	}
	nd := node{
		level:  block[0],
		n:      int(binary.BigEndian.Uint16(block[1:3])),
		keyLen: keyLen,
	}
	prefixLen := int(block[3])
	if prefixLen > keyLen {
		return node{}, fmt.Errorf("%w: prefix %d longer than key %d", ErrCorrupt, prefixLen, keyLen)
	}
	if nodeSize(keyLen, prefixLen, nd.n) > len(block) {
		return node{}, fmt.Errorf("%w: %d entries do not fit in a block", ErrCorrupt, nd.n)
	}
	nd.suffixLen = keyLen - prefixLen
	nd.prefix = block[headerSize : headerSize+prefixLen]
	start := headerSize + prefixLen
	nd.entries = block[start : start+nd.n*(nd.suffixLen+valueSize)]
	return nd, nil
}

func (nd node) entrySize() int { return nd.suffixLen + valueSize }

func (nd node) suffix(i int) []byte {
	off := i * nd.entrySize()
	return nd.entries[off : off+nd.suffixLen]
}

func (nd node) value(i int) uint64 {
	off := i*nd.entrySize() + nd.suffixLen
	return binary.BigEndian.Uint64(nd.entries[off : off+valueSize])
}

// key appends the full key of entry i to dst.
func (nd node) key(dst []byte, i int) []byte {
	dst = append(dst, nd.prefix...)
	return append(dst, nd.suffix(i)...)
}

func (nd node) compare(i int, search []byte) int {
	p := len(nd.prefix)
	if c := bytes.Compare(nd.prefix, search[:p]); c != 0 {
		return c
	}
	return bytes.Compare(nd.suffix(i), search[p:])
}

func (nd node) lastAtOrBefore(search []byte) int {
	lo, hi := 0, nd.n // find the first entry > search, answer is the one before
	for lo < hi {
		mid := (lo + hi) / 2
		if nd.compare(mid, search) <= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo - 1
}

func (nd node) firstAtOrAfter(search []byte) int {
	lo, hi := 0, nd.n
	for lo < hi {
		mid := (lo + hi) / 2
		if nd.compare(mid, search) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}