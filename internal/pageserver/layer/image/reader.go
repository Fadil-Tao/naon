package image

import (
	"github.com/Fadil-Tao/naon/internal/pageserver/diskbtree"
	"github.com/Fadil-Tao/naon/internal/pageserver/key"
	"github.com/Fadil-Tao/naon/internal/pageserver/layer"
	"github.com/Fadil-Tao/naon/internal/pageserver/lsn"
	"github.com/Fadil-Tao/naon/internal/pageserver/value"
)

// Reader reads one image layer file. Safe for concurrent use.
type Reader struct {
	file *layer.File
	tree *diskbtree.Reader
}

var _ layer.Reader = (*Reader)(nil)

// Open opens an image layer file and checks its summary against its name.
func Open(path string) (*Reader, error) {
	f, err := layer.OpenFile(path, layer.ImageMagic)
	if err != nil {
		return nil, err
	}
	tree := diskbtree.NewReader(f.Blocks, f.Summary.IndexRootBlk, key.KeySize)
	return &Reader{file: f, tree: tree}, nil
}

// Desc describes the layer.
func (r *Reader) Desc() layer.Desc { return r.file.Desc }

// Summary is the file's block 0.
func (r *Reader) Summary() layer.Summary { return r.file.Summary }

// Close closes the file.
func (r *Reader) Close() error { return r.file.Close() }

// GetValueReconstructData completes st for key k: with the page image if the
// key is in the file, or as "absent" if the key is in range but not in the
// file. Keys outside the range, or an image whose LSN is outside lsnRange,
// leave st untouched.
func (r *Reader) GetValueReconstructData(k key.Key, lsnRange lsn.Range, st *value.ReconstructState) error {
	d := r.file.Desc
	if !d.KeyRange.Contains(k) || !lsnRange.Contains(d.ImageLsn()) {
		return nil
	}
	enc := k.Encode()
	pos, found, err := r.tree.Get(enc[:])
	if err != nil {
		return err
	}
	if !found {
		return st.MarkAbsent()
	}
	page, err := r.file.Blobs.ReadBlob(pos)
	if err != nil {
		return err
	}
	return st.Add(d.ImageLsn(), value.Image(page))
}

// Iter calls fn for every key and page in key order until fn returns false.
func (r *Reader) Iter(fn func(k key.Key, page []byte) bool) error {
	var start [key.KeySize]byte
	var readErr error
	err := r.tree.Visit(start[:], diskbtree.Forward, func(ik []byte, pos uint64) bool {
		k, _ := key.DecodeKey(ik)
		page, err := r.file.Blobs.ReadBlob(pos)
		if err != nil {
			readErr = err
			return false
		}
		return fn(k, page)
	})
	if err != nil {
		return err
	}
	return readErr
}