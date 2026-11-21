package diskbtree

import (
	"fmt"

	"github.com/Fadil-Tao/naon/internal/pageserver/blockio"
)

type Direction int

const (
	Forward Direction = iota
	Backward
)

const maxDepth = 32

type Reader struct {
	blocks *blockio.Reader
	root   uint32
	keyLen int
}

// NewReader opens the tree whose root is at block root.
func NewReader(blocks *blockio.Reader, root uint32, keyLen int) *Reader {
	return &Reader{blocks: blocks, root: root, keyLen: keyLen}
}

func (r *Reader) readNode(blk uint32) (node, error) {
	buf, err := r.blocks.Block(blk)
	if err != nil {
		return node{}, err
	}
	nd, err := decodeNode(buf, r.keyLen)
	if err != nil {
		return node{}, fmt.Errorf("block %d: %w", blk, err)
	}
	return nd, nil
}

// Get returns the value stored for key exactly.
func (r *Reader) Get(key []byte) (value uint64, found bool, err error) {
	if len(key) != r.keyLen {
		return 0, false, fmt.Errorf("%w: got %d, want %d", ErrKeyLen, len(key), r.keyLen)
	}
	blk := r.root
	for depth := 0; ; depth++ {
		if depth > maxDepth {
			return 0, false, fmt.Errorf("%w: tree deeper than %d", ErrCorrupt, maxDepth)
		}
		nd, err := r.readNode(blk)
		if err != nil {
			return 0, false, err
		}
		i := nd.lastAtOrBefore(key)
		if i < 0 {
			return 0, false, nil // key is smaller than everything here
		}
		if nd.level == 0 {
			if nd.compare(i, key) == 0 {
				return nd.value(i), true, nil
			}
			return 0, false, nil
		}
		// Internal node: entry i's child is the only one that can hold key.
		blk = uint32(nd.value(i))
	}
}

// Visit calls fn for each key starting at search, in the given direction,
// until fn returns false or the keys run out.
//
//	Forward:  every key >= search, smallest first
//	Backward: every key <= search, largest first
//
// The key passed to fn is only valid during the call; copy it to keep it.
//
// Delta layers use Backward: searching from (key, lsn) walks back through
// older versions of the same key, which is exactly what a Get needs.
func (r *Reader) Visit(search []byte, dir Direction, fn func(key []byte, value uint64) bool) error {
	if len(search) != r.keyLen {
		return fmt.Errorf("%w: got %d, want %d", ErrKeyLen, len(search), r.keyLen)
	}
	keyBuf := make([]byte, 0, r.keyLen)
	_, err := r.visit(r.root, search, dir, fn, keyBuf, 0)
	return err
}

// visit walks the subtree at blk. search is nil once we are past the search
// key, meaning "visit everything in this subtree". It returns false when fn
// asked to stop.
func (r *Reader) visit(blk uint32, search []byte, dir Direction, fn func([]byte, uint64) bool, keyBuf []byte, depth int) (bool, error) {
	if depth > maxDepth {
		return false, fmt.Errorf("%w: tree deeper than %d", ErrCorrupt, maxDepth)
	}
	nd, err := r.readNode(blk)
	if err != nil {
		return false, err
	}
	if nd.n == 0 {
		return true, nil
	}

	if dir == Forward {
		start := 0
		if search != nil {
			if nd.level == 0 {
				start = nd.firstAtOrAfter(search)
			} else {
				// The child that could contain search is the last one whose
				// first key <= search (or the first child if none).
				start = max(nd.lastAtOrBefore(search), 0)
			}
		}
		for i := start; i < nd.n; i++ {
			if nd.level == 0 {
				if !fn(nd.key(keyBuf[:0], i), nd.value(i)) {
					return false, nil
				}
				continue
			}
			s := search
			if i > start {
				s = nil // later children are entirely > search
			}
			cont, err := r.visit(uint32(nd.value(i)), s, dir, fn, keyBuf, depth+1)
			if err != nil || !cont {
				return cont, err
			}
		}
		return true, nil
	}

	start := nd.n - 1
	if search != nil {
		start = nd.lastAtOrBefore(search) // -1 means nothing here is <= search
	}
	for i := start; i >= 0; i-- {
		if nd.level == 0 {
			if !fn(nd.key(keyBuf[:0], i), nd.value(i)) {
				return false, nil
			}
			continue
		}
		s := search
		if i < start {
			s = nil // earlier children are entirely < search
		}
		cont, err := r.visit(uint32(nd.value(i)), s, dir, fn, keyBuf, depth+1)
		if err != nil || !cont {
			return cont, err
		}
	}
	return true, nil
}