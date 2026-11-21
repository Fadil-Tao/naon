package blockio

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/Fadil-Tao/naon/internal/pageserver/iox"
)

// fill returns a block where every byte is b, so blocks are easy to tell apart.
func fill(b byte) []byte { return bytes.Repeat([]byte{b}, BlockSize) }

func TestWriteAndReadBlocks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "f")
	f, err := iox.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Abort()

	w := NewWriter(f, BlockSize) // leave block 0 for the "summary"
	for i, b := range []byte{'a', 'b', 'c'} {
		blk, err := w.WriteBlock(fill(b))
		if err != nil || blk != uint32(i+1) {
			t.Fatalf("WriteBlock #%d = %d, %v", i, blk, err)
		}
	}
	if err := w.WriteBlockAt(0, fill('S')); err != nil { // summary written last
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := f.Commit(); err != nil {
		t.Fatal(err)
	}

	rf, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer rf.Close()
	r := NewReader(rf)
	for blk, want := range []byte{'S', 'a', 'b', 'c'} {
		got, err := r.Block(uint32(blk))
		if err != nil || !bytes.Equal(got, fill(want)) {
			t.Fatalf("block %d: err %v", blk, err)
		}
	}
	if _, err := r.Block(4); !errors.Is(err, ErrShortRead) {
		t.Fatalf("reading past end: %v", err)
	}
}

func TestPaddingAndAlignment(t *testing.T) {
	path := filepath.Join(t.TempDir(), "f")
	f, _ := iox.Create(path)
	defer f.Abort()
	w := NewWriter(f, BlockSize)

	w.Write([]byte("hello")) // offset now mid-block
	if _, err := w.WriteBlock(fill('x')); !errors.Is(err, ErrMisaligned) {
		t.Fatalf("want ErrMisaligned, got %v", err)
	}
	if _, err := w.WriteBlock([]byte("short")); !errors.Is(err, ErrBlockSize) {
		t.Fatalf("want ErrBlockSize, got %v", err)
	}
	blk, err := w.PadToBlock()
	if err != nil || blk != 2 || w.Offset() != 2*BlockSize {
		t.Fatalf("PadToBlock = %d, offset %d, %v", blk, w.Offset(), err)
	}
	// Already aligned: padding adds nothing.
	if blk, _ := w.PadToBlock(); blk != 2 {
		t.Fatalf("second PadToBlock = %d", blk)
	}
}

// More than flushThreshold of data, so the buffer flushes in the middle.
func TestLargeWritesFlush(t *testing.T) {
	path := filepath.Join(t.TempDir(), "f")
	f, _ := iox.Create(path)
	defer f.Abort()
	w := NewWriter(f, 0)
	var want []byte
	for i := 0; i < 50; i++ {
		chunk := bytes.Repeat([]byte{byte(i)}, 3001)
		want = append(want, chunk...)
		if _, err := w.Write(chunk); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := f.Commit(); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(path)
	if !bytes.Equal(got, want) || w.Offset() != uint64(len(want)) {
		t.Fatalf("file has %d bytes, want %d", len(got), len(want))
	}
}