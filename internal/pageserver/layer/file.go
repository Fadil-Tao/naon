package layer

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"

	"github.com/Fadil-Tao/naon/internal/pageserver/blobio"
	"github.com/Fadil-Tao/naon/internal/pageserver/blockio"
	"github.com/Fadil-Tao/naon/internal/pageserver/diskbtree"
	"github.com/Fadil-Tao/naon/internal/pageserver/iox"
)

// Every layer file has the same three parts:
//
//	block 0          summary (written last)
//	blocks 1..       values, as blobs (blobio)
//	blocks N..       B-tree index (diskbtree), starting on a block boundary
//
// FileWriter and OpenFile handle those parts so the delta and image packages
// only deal with what is different,what goes into the index.

// FileWriter writes the common parts of a layer file.
type FileWriter struct {
	dir    string
	f      *iox.AtomicFile
	blocks *blockio.Writer
	blobs  *blobio.Writer
	done   bool
}

// CreateFileWriter starts a layer file in dir under a temporary name.
// The real name is chosen at Finish, once the whole Desc is known.
func CreateFileWriter(dir string) (*FileWriter, error) {
	var rnd [8]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		return nil, err
	}
	f, err := iox.Create(filepath.Join(dir, "layer-"+hex.EncodeToString(rnd[:])))
	if err != nil {
		return nil, err
	}
	blocks := blockio.NewWriter(f, blockio.BlockSize) // block 0 = summary, later
	return &FileWriter{dir: dir, f: f, blocks: blocks, blobs: blobio.NewWriter(blocks)}, nil
}

// WriteBlob stores a value and returns its offset.
func (w *FileWriter) WriteBlob(data []byte) (uint64, error) { return w.blobs.WriteBlob(data) }

// Size is the number of bytes written so far. Used later to cut layers at a
// target size.
func (w *FileWriter) Size() uint64 { return w.blocks.Offset() }

// Finish writes the index and the summary, makes the file durable, and gives
// it its real name. fillIndex must Append every index entry in key order.
func (w *FileWriter) Finish(keyLen int, fillIndex func(*diskbtree.Builder) error, sum Summary, name string) (string, error) {
	if w.done {
		return "", fmt.Errorf("layer: writer already finished")
	}
	w.done = true
	fail := func(err error) (string, error) { w.f.Abort(); return "", err }

	startBlk, err := w.blocks.PadToBlock()
	if err != nil {
		return fail(err)
	}
	tree, err := diskbtree.NewBuilder(w.blocks, keyLen)
	if err != nil {
		return fail(err)
	}
	if err := fillIndex(tree); err != nil {
		return fail(err)
	}
	root, err := tree.Finish()
	if err != nil {
		return fail(err)
	}
	sum.IndexStartBlk, sum.IndexRootBlk = startBlk, root
	if err := w.blocks.WriteBlockAt(0, sum.Encode()); err != nil {
		return fail(err)
	}
	if err := w.blocks.Flush(); err != nil {
		return fail(err)
	}
	path := filepath.Join(w.dir, name)
	if err := w.f.CommitAs(path); err != nil {
		return "", err
	}
	return path, nil
}

func (w *FileWriter) Abort() {
	w.done = true
	w.f.Abort()
}

// File is an open layer file: the parts both readers need.
type File struct {
	f       *os.File
	Desc    Desc
	Summary Summary
	Blocks  *blockio.Reader
	Blobs   *blobio.Reader
}

// OpenFile opens a layer file, reads its summary and checks that the summary
// matches the file name.
func OpenFile(path string, magic uint16) (*File, error) {
	desc, err := ParseFileName(filepath.Base(path))
	if err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	blocks := blockio.NewReader(f)
	block0, err := blocks.Block(0)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	sum, err := DecodeSummary(block0, magic)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if err := sum.CheckMatches(desc); err != nil {
		f.Close()
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return &File{
		f:       f,
		Desc:    desc,
		Summary: sum,
		Blocks:  blocks,
		Blobs:   blobio.NewReader(f, st.Size()),
	}, nil
}

// Close closes the file.
func (lf *File) Close() error { return lf.f.Close() }