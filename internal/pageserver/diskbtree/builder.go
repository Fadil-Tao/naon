// Package diskbtree is the index stored inside every layer file: a B-tree of
// fixed-length keys to 8-byte values (blob offsets), stored as 8 KiB blocks.
package diskbtree

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/Fadil-Tao/naon/internal/pageserver/blockio"
)

var (
	ErrKeyOrder = errors.New("diskbtree: keys must be appended in strictly increasing order")
	ErrKeyLen = errors.New("diskbtree: wrong key length")
	ErrFinished = errors.New("diskbtree: builder already finished")
)

const MaxKeyLen = 255

type level struct {
	keys   [][]byte
	values []uint64
}

func (l *level) reset() { l.keys, l.values = l.keys[:0], l.values[:0] }


type Builder struct {
	w       *blockio.Writer
	keyLen  int
	levels  []*level // levels[0] = leaves
	lastKey []byte
	count   int
	done    bool
}

// NewBuilder starts a tree. w must be at a block boundary (call PadToBlock).
func NewBuilder(w *blockio.Writer, keyLen int) (*Builder, error) {
	if keyLen <= 0 || keyLen > MaxKeyLen {
		return nil, fmt.Errorf("%w: %d", ErrKeyLen, keyLen)
	}
	if nodeSize(keyLen, 0, 2) > blockio.BlockSize {
		return nil, fmt.Errorf("%w: %d is too long to fit two entries per node", ErrKeyLen, keyLen)
	}
	if w.Offset()%blockio.BlockSize != 0 {
		return nil, blockio.ErrMisaligned
	}
	return &Builder{w: w, keyLen: keyLen, levels: []*level{{}}}, nil
}

func (b *Builder) Append(key []byte, value uint64) error {
	if b.done {
		return ErrFinished
	}
	if len(key) != b.keyLen {
		return fmt.Errorf("%w: got %d, want %d", ErrKeyLen, len(key), b.keyLen)
	}
	if b.lastKey != nil && bytes.Compare(key, b.lastKey) <= 0 {
		return fmt.Errorf("%w: %x after %x", ErrKeyOrder, key, b.lastKey)
	}
	b.lastKey = append(b.lastKey[:0], key...)
	b.count++
	return b.add(0, bytes.Clone(key), value)
}

func (b *Builder) add(lvl int, key []byte, value uint64) error {
	if lvl == len(b.levels) {
		b.levels = append(b.levels, &level{})
	}
	l := b.levels[lvl]
	if len(l.keys) > 0 && !b.fits(l, key) {
		if err := b.writeNode(lvl); err != nil {
			return err
		}
	}
	l.keys = append(l.keys, key)
	l.values = append(l.values, value)
	return nil
}

func (b *Builder) fits(l *level, key []byte) bool {
	prefix := commonPrefixLen(l.keys[0], key)
	return nodeSize(b.keyLen, prefix, len(l.keys)+1) <= blockio.BlockSize
}

func (b *Builder) writeNode(lvl int) error {
	l := b.levels[lvl]
	blk, err := b.w.WriteBlock(encodeNode(uint8(lvl), b.keyLen, l.keys, l.values))
	if err != nil {
		return err
	}
	first := l.keys[0]
	l.reset()
	return b.add(lvl+1, first, uint64(blk))
}

func (b *Builder) Finish() (root uint32, err error) {
	if b.done {
		return 0, ErrFinished
	}
	b.done = true
	for lvl := 0; ; lvl++ {
		l := b.levels[lvl]
		isTop := lvl == len(b.levels)-1
		if isTop && len(l.keys) <= 1 && lvl > 0 {
			return uint32(l.values[0]), nil
		}
		if isTop {
			return b.w.WriteBlock(encodeNode(uint8(lvl), b.keyLen, l.keys, l.values))
		}
		if len(l.keys) > 0 {
			if err := b.writeNode(lvl); err != nil {
				return 0, err
			}
		}
	}
}

func (b *Builder) Count() int { return b.count }