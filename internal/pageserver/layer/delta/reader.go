package delta

import (
	"fmt"

	"github.com/Fadil-Tao/naon/internal/pageserver/diskbtree"
	"github.com/Fadil-Tao/naon/internal/pageserver/key"
	"github.com/Fadil-Tao/naon/internal/pageserver/layer"
	"github.com/Fadil-Tao/naon/internal/pageserver/lsn"
	"github.com/Fadil-Tao/naon/internal/pageserver/value"
)

type Reader struct {
	file *layer.File
	tree *diskbtree.Reader
}

var _ layer.Reader = (*Reader)(nil)

func Open(path string) (*Reader, error) {
	f, err := layer.OpenFile(path, layer.DeltaMagic)
	if err != nil {
		return nil, err
	}
	tree := diskbtree.NewReader(f.Blocks, f.Summary.IndexRootBlk, IndexKeyLen)
	return &Reader{file: f, tree: tree}, nil
}

func (r *Reader) Desc() layer.Desc { return r.file.Desc }

func (r *Reader) Summary() layer.Summary { return r.file.Summary }

func (r *Reader) Close() error { return r.file.Close() }

func (r *Reader) GetValueReconstructData(k key.Key, lsnRange lsn.Range, st *value.ReconstructState) error {
	d := r.file.Desc
	if !d.KeyRange.Contains(k) || !d.LsnRange.Overlaps(lsnRange) {
		return nil // nothing for this key in this part of history
	}
	// Newest LSN we may return: below the range end and inside this layer.
	top := lsn.Min(lsnRange.End, d.LsnRange.End) - 1
	search := indexKey(k, top)

	var readErr error
	err := r.tree.Visit(search[:], diskbtree.Backward, func(ik []byte, v uint64) bool {
		gotKey, gotLsn := splitIndexKey(ik)
		if gotKey != k || gotLsn < lsnRange.Start {
			return false // past all versions of k, or below the floor
		}
		blob, err := r.file.Blobs.ReadBlob(layer.BlobRef(v).Pos())
		if err != nil {
			readErr = err
			return false
		}
		val, err := value.DecodeValue(blob)
		if err != nil {
			readErr = fmt.Errorf("delta %s: %s @ %s: %w", d, k, gotLsn, err)
			return false
		}
		if err := st.Add(gotLsn, val); err != nil {
			readErr = err
			return false
		}
		return !st.IsComplete()
	})
	if err != nil {
		return err
	}
	return readErr
}

// Iter calls fn for every entry in (key, lsn) order until fn returns false.
// Compaction and tests use it.
func (r *Reader) Iter(fn func(k key.Key, l lsn.Lsn, v value.Value) bool) error {
	var start [IndexKeyLen]byte // all zeros: before every key
	var readErr error
	err := r.tree.Visit(start[:], diskbtree.Forward, func(ik []byte, ref uint64) bool {
		k, l := splitIndexKey(ik)
		blob, err := r.file.Blobs.ReadBlob(layer.BlobRef(ref).Pos())
		if err != nil {
			readErr = err
			return false
		}
		v, err := value.DecodeValue(blob)
		if err != nil {
			readErr = err
			return false
		}
		return fn(k, l, v)
	})
	if err != nil {
		return err
	}
	return readErr
}