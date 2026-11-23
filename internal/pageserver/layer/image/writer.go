package image

// Package image writes and reads image layer files: a snapshot of every key
// in a key range at one LSN.
//
// Index key: Key (18 bytes). Index value: blob offset of the page.

import (
	"errors"
	"fmt"

	"github.com/Fadil-Tao/naon/internal/pageserver/diskbtree"
	"github.com/Fadil-Tao/naon/internal/pageserver/ids"
	"github.com/Fadil-Tao/naon/internal/pageserver/key"
	"github.com/Fadil-Tao/naon/internal/pageserver/layer"
	"github.com/Fadil-Tao/naon/internal/pageserver/lsn"
)

var (
	// ErrOrder means Put was called with a key that is not increasing.
	ErrOrder = errors.New("image: keys must be added in increasing order")
	// ErrOutOfRange means a key is outside the layer's key range.
	ErrOutOfRange = errors.New("image: key outside the layer")
)

type indexEntry struct {
	key [key.KeySize]byte
	pos uint64
}

// Writer builds one image layer file.
type Writer struct {
	fw       *layer.FileWriter
	tenant   ids.TenantID
	timeline ids.TimelineID
	keyRange key.KeyRange
	at       lsn.Lsn

	index   []indexEntry
	lastKey key.Key
	hasLast bool
}

// NewWriter starts an image layer in dir for keyRange at LSN at.
func NewWriter(dir string, tenant ids.TenantID, timeline ids.TimelineID, keyRange key.KeyRange, at lsn.Lsn) (*Writer, error) {
	if keyRange.IsEmpty() {
		return nil, fmt.Errorf("image: empty key range %s", keyRange)
	}
	if at == lsn.Max {
		return nil, fmt.Errorf("image: LSN %s too large", at)
	}
	fw, err := layer.CreateFileWriter(dir)
	if err != nil {
		return nil, err
	}
	return &Writer{fw: fw, tenant: tenant, timeline: timeline, keyRange: keyRange, at: at}, nil
}

// Put adds the page image of one key. Keys must be strictly increasing.
// Keys that are never Put do not exist at this LSN.
func (w *Writer) Put(k key.Key, page []byte) error {
	if !w.keyRange.Contains(k) {
		return fmt.Errorf("%w: %s not in %s", ErrOutOfRange, k, w.keyRange)
	}
	if w.hasLast && !w.lastKey.Less(k) {
		return fmt.Errorf("%w: %s after %s", ErrOrder, k, w.lastKey)
	}
	off, err := w.fw.WriteBlob(page)
	if err != nil {
		return err
	}
	w.index = append(w.index, indexEntry{key: k.Encode(), pos: off})
	w.lastKey, w.hasLast = k, true
	return nil
}

// NumKeys is how many keys were added.
func (w *Writer) NumKeys() int { return len(w.index) }

// Size is the bytes written so far.
func (w *Writer) Size() uint64 { return w.fw.Size() }

// Finish completes the file and returns its description and path.
func (w *Writer) Finish(generation uint32) (layer.Desc, string, error) {
	desc := layer.Desc{
		KeyRange:   w.keyRange,
		LsnRange:   lsn.Range{Start: w.at, End: w.at + 1},
		Generation: generation,
	}
	sum := layer.Summary{
		Magic:         layer.ImageMagic,
		FormatVersion: layer.FormatVersion,
		Tenant:        w.tenant,
		Timeline:      w.timeline,
		KeyRange:      desc.KeyRange,
		LsnRange:      desc.LsnRange,
	}
	fill := func(b *diskbtree.Builder) error {
		for i := range w.index {
			if err := b.Append(w.index[i].key[:], w.index[i].pos); err != nil {
				return err
			}
		}
		return nil
	}
	path, err := w.fw.Finish(key.KeySize, fill, sum, desc.FileName())
	if err != nil {
		return layer.Desc{}, "", err
	}
	return desc, path, nil
}

func (w *Writer) Abort() { w.fw.Abort() }