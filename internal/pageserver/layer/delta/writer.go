package delta

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/Fadil-Tao/naon/internal/pageserver/diskbtree"
	"github.com/Fadil-Tao/naon/internal/pageserver/ids"
	"github.com/Fadil-Tao/naon/internal/pageserver/key"
	"github.com/Fadil-Tao/naon/internal/pageserver/layer"
	"github.com/Fadil-Tao/naon/internal/pageserver/lsn"
	"github.com/Fadil-Tao/naon/internal/pageserver/value"
)

// this package writes and read delta layer files the change to kaye rangeover an lsn range

// Index key: Key (18 bytes) + LSN (8 bytes, big-endian) = 26 bytes. Because
// the LSN comes last, all versions of one key sit next to each other, oldest
// first, and a Backward visit from (key, lsn) walks back through them.
//
// Index value: a BlobRef (offset + will_init bit).

const IndexKeyLen = key.KeySize + 8

var (
	// ErrOrder means Put was called out of (key, lsn) order.
	ErrOrder = errors.New("delta: entries must be added in increasing (key, lsn) order")
	// ErrOutOfRange means a key or LSN is outside the layer's ranges.
	ErrOutOfRange = errors.New("delta: key or lsn outside the layer")
)

func indexKey(k key.Key, l lsn.Lsn) [IndexKeyLen]byte {
	var b [IndexKeyLen]byte
	k.PutBytes(b[:key.KeySize])
	binary.BigEndian.PutUint64(b[key.KeySize:], uint64(l))
	return b
}

// splitIndexKey is the reverse of indexKey.
func splitIndexKey(b []byte) (key.Key, lsn.Lsn) {
	k, _ := key.DecodeKey(b[:key.KeySize])
	return k, lsn.Lsn(binary.BigEndian.Uint64(b[key.KeySize:]))
}

type indexEntry struct {
	key [IndexKeyLen]byte
	ref layer.BlobRef
}

type Writer struct {
	fw       *layer.FileWriter
	tenant   ids.TenantID
	timeline ids.TimelineID
	keyStart key.Key
	lsnRange lsn.Range

	index   []indexEntry
	lastKey key.Key
	lastLsn lsn.Lsn
	hasLast bool
}

// NewWriter starts a delta layer in dir for keys >= keyStart and LSNs in
// lsnRange. The end key is given at Finish.
func NewWriter(dir string, tenant ids.TenantID, timeline ids.TimelineID, keyStart key.Key, lsnRange lsn.Range) (*Writer, error) {
	if lsnRange.IsEmpty() {
		return nil, fmt.Errorf("delta: empty LSN range %v", lsnRange)
	}
	fw, err := layer.CreateFileWriter(dir)
	if err != nil {
		return nil, err
	}
	return &Writer{fw: fw, tenant: tenant, timeline: timeline, keyStart: keyStart, lsnRange: lsnRange}, nil
}

// Put adds one version of a key. Calls must be in increasing (key, lsn) order.
func (w *Writer) Put(k key.Key, l lsn.Lsn, v value.Value) error {
	if k.Less(w.keyStart) || !w.lsnRange.Contains(l) {
		return fmt.Errorf("%w: %s @ %s, layer starts at key %s, lsn %v", ErrOutOfRange, k, l, w.keyStart, w.lsnRange)
	}
	if w.hasLast {
		c := k.Compare(w.lastKey)
		if c < 0 || (c == 0 && l <= w.lastLsn) {
			return fmt.Errorf("%w: %s @ %s after %s @ %s", ErrOrder, k, l, w.lastKey, w.lastLsn)
		}
	}
	off, err := w.fw.WriteBlob(v.Encode())
	if err != nil {
		return err
	}
	w.index = append(w.index, indexEntry{key: indexKey(k, l), ref: layer.NewBlobRef(off, v.WillInit())})
	w.lastKey, w.lastLsn, w.hasLast = k, l, true
	return nil
}

// NumEntries is how many versions were added.
func (w *Writer) NumEntries() int { return len(w.index) }

// Size is the bytes written so far (values only; the index comes at Finish).
func (w *Writer) Size() uint64 { return w.fw.Size() }

// Finish completes the file with key range [keyStart, keyEnd) and returns its
// description and path. keyEnd must be greater than every key added.
func (w *Writer) Finish(keyEnd key.Key, generation uint32) (layer.Desc, string, error) {
	if !w.keyStart.Less(keyEnd) || (w.hasLast && !w.lastKey.Less(keyEnd)) {
		w.fw.Abort()
		return layer.Desc{}, "", fmt.Errorf("%w: end key %s must be after start %s and last key %s", ErrOutOfRange, keyEnd, w.keyStart, w.lastKey)
	}
	desc := layer.Desc{
		KeyRange:   key.KeyRange{Start: w.keyStart, End: keyEnd},
		LsnRange:   w.lsnRange,
		IsDelta:    true,
		Generation: generation,
	}
	sum := layer.Summary{
		Magic:         layer.DeltaMagic,
		FormatVersion: layer.FormatVersion,
		Tenant:        w.tenant,
		Timeline:      w.timeline,
		KeyRange:      desc.KeyRange,
		LsnRange:      desc.LsnRange,
	}
	fill := func(b *diskbtree.Builder) error {
		for i := range w.index {
			if err := b.Append(w.index[i].key[:], uint64(w.index[i].ref)); err != nil {
				return err
			}
		}
		return nil
	}
	path, err := w.fw.Finish(IndexKeyLen, fill, sum, desc.FileName())
	if err != nil {
		return layer.Desc{}, "", err
	}
	return desc, path, nil
}

// Abort throws the unfinished file away. Safe to defer.
func (w *Writer) Abort() { w.fw.Abort() }

