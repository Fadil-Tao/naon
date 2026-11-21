package blobio

import (
	"bytes"
	"errors"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/Fadil-Tao/naon/internal/pageserver/blockio"
	"github.com/Fadil-Tao/naon/internal/pageserver/iox"
)

// writeBlobs writes blobs to a new file (starting after block 0, like a layer
// file) and returns the path and each blob's offset.
func writeBlobs(t *testing.T, blobs [][]byte) (string, []uint64) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "blobs")
	f, err := iox.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Abort()
	bw := blockio.NewWriter(f, blockio.BlockSize)
	w := NewWriter(bw)
	offs := make([]uint64, len(blobs))
	for i, b := range blobs {
		if offs[i], err = w.WriteBlob(b); err != nil {
			t.Fatal(err)
		}
	}
	if err := bw.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := f.Commit(); err != nil {
		t.Fatal(err)
	}
	return path, offs
}

func openReader(t *testing.T, path string) *Reader {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	st, _ := f.Stat()
	return NewReader(f, st.Size())
}

func TestHeaderFormat(t *testing.T) {
	cases := []struct {
		n    int
		want []byte
	}{
		{0, []byte{0x00}},
		{127, []byte{0x7F}},
		{128, []byte{0x80, 0x00, 0x00, 0x80}},
		{8192, []byte{0x80, 0x00, 0x20, 0x00}},
		{MaxBlobLen, []byte{0x8F, 0xFF, 0xFF, 0xFF}},
	}
	for _, c := range cases {
		if got := appendHeader(nil, c.n); !bytes.Equal(got, c.want) || HeaderLen(c.n) != len(c.want) {
			t.Fatalf("header(%d) = % X, want % X", c.n, got, c.want)
		}
	}
}

// Sizes around every boundary: header switch (127/128) and block size (8192).
func TestRoundTripBoundaries(t *testing.T) {
	sizes := []int{0, 1, 126, 127, 128, 129, 8190, 8191, 8192, 8193, 20000, 100000}
	var blobs [][]byte
	for i, n := range sizes {
		blobs = append(blobs, bytes.Repeat([]byte{byte(i + 1)}, n))
	}
	path, offs := writeBlobs(t, blobs)
	r := openReader(t, path)
	if offs[0] != blockio.BlockSize {
		t.Fatalf("first blob at %d, want %d (block 0 reserved)", offs[0], blockio.BlockSize)
	}
	for i, off := range offs {
		got, err := r.ReadBlob(off)
		if err != nil || !bytes.Equal(got, blobs[i]) {
			t.Fatalf("blob %d (len %d): err %v, got len %d", i, len(blobs[i]), err, len(got))
		}
	}
}

func TestRoundTripRandom(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	blobs := make([][]byte, 10000)
	for i := range blobs {
		n := rng.Intn(300)
		if rng.Intn(20) == 0 {
			n = rng.Intn(20000) // some large ones that cross blocks
		}
		blobs[i] = make([]byte, n)
		rng.Read(blobs[i])
	}
	path, offs := writeBlobs(t, blobs)
	r := openReader(t, path)

	// Read in random order, reusing one buffer.
	var buf []byte
	for _, i := range rng.Perm(len(blobs)) {
		var err error
		buf, err = r.ReadBlobInto(buf[:0], offs[i])
		if err != nil || !bytes.Equal(buf, blobs[i]) {
			t.Fatalf("blob %d mismatch: %v", i, err)
		}
	}
}

func TestTooLarge(t *testing.T) {
	path := filepath.Join(t.TempDir(), "f")
	f, _ := iox.Create(path)
	defer f.Abort()
	w := NewWriter(blockio.NewWriter(f, 0))
	if _, err := w.WriteBlob(make([]byte, MaxBlobLen+1)); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("want ErrTooLarge, got %v", err)
	}
}

func TestCorruptInput(t *testing.T) {
	cases := map[string][]byte{
		"length past EOF":  {0x05, 'a', 'b'},              // says 5 bytes, has 2
		"truncated header": {0x80, 0x00},                  // long header cut short
		"huge length":      {0x8F, 0xFF, 0xFF, 0xFF, 'x'}, // 256 MiB claimed
		"zstd":             {0x90, 0x00, 0x00, 0x01, 'x'},
		"unknown header":   {0xA0, 0x00, 0x00, 0x01, 'x'},
		"offset past EOF":  nil,
	}
	for name, data := range cases {
		r := NewReader(bytes.NewReader(data), int64(len(data)))
		_, err := r.ReadBlob(0)
		want := ErrCorrupt
		if name == "zstd" {
			want = ErrUnsupportedCompression
		}
		if !errors.Is(err, want) {
			t.Fatalf("%s: want %v, got %v", name, want, err)
		}
	}
}

// Random bytes must never panic or allocate absurd amounts.
func FuzzReadBlob(f *testing.F) {
	f.Add([]byte{0x03, 'a', 'b', 'c'}, uint16(0))
	f.Add([]byte{0x80, 0x00, 0x00, 0x80}, uint16(0))
	f.Fuzz(func(t *testing.T, data []byte, off uint16) {
		r := NewReader(bytes.NewReader(data), int64(len(data)))
		b, err := r.ReadBlob(uint64(off))
		if err == nil && uint64(off)+uint64(len(b)) > uint64(len(data)) {
			t.Fatal("returned more bytes than the file has")
		}
	})
}