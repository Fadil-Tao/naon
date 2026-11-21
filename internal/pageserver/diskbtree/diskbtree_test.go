package diskbtree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"testing"

	"github.com/Fadil-Tao/naon/internal/pageserver/blockio"
	"github.com/Fadil-Tao/naon/internal/pageserver/iox"
	"github.com/Fadil-Tao/naon/internal/pageserver/key"
	"github.com/Fadil-Tao/naon/internal/pageserver/lsn"
)

type entry struct {
	key   []byte
	value uint64
}

// buildTree writes entries (already sorted) into a new file the way a layer
// file will: block 0 reserved, tree blocks after it. It returns a Reader and
// the number of blocks used by the tree.
func buildTree(t *testing.T, keyLen int, entries []entry) (*Reader, int) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "tree")
	f, err := iox.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Abort()
	w := blockio.NewWriter(f, blockio.BlockSize)
	b, err := NewBuilder(w, keyLen)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if err := b.Append(e.key, e.value); err != nil {
			t.Fatal(err)
		}
	}
	root, err := b.Finish()
	if err != nil {
		t.Fatal(err)
	}
	blocks := int(w.Offset()/blockio.BlockSize) - 1
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
	t.Cleanup(func() { rf.Close() })
	return NewReader(blockio.NewReader(rf), root, keyLen), blocks
}

// randomEntries returns n distinct sorted keys of length keyLen.
func randomEntries(rng *rand.Rand, n, keyLen int) []entry {
	seen := map[string]bool{}
	var es []entry
	for len(es) < n {
		k := make([]byte, keyLen)
		rng.Read(k)
		if seen[string(k)] {
			continue
		}
		seen[string(k)] = true
		es = append(es, entry{key: k})
	}
	sort.Slice(es, func(i, j int) bool { return bytes.Compare(es[i].key, es[j].key) < 0 })
	for i := range es {
		es[i].value = uint64(i) * 7
	}
	return es
}

func collect(t *testing.T, r *Reader, search []byte, dir Direction) []entry {
	t.Helper()
	var out []entry
	err := r.Visit(search, dir, func(k []byte, v uint64) bool {
		out = append(out, entry{bytes.Clone(k), v})
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestEmptyTree(t *testing.T) {
	r, _ := buildTree(t, 4, nil)
	if _, found, err := r.Get([]byte{1, 2, 3, 4}); found || err != nil {
		t.Fatalf("empty tree Get: %v %v", found, err)
	}
	if got := collect(t, r, []byte{0, 0, 0, 0}, Forward); len(got) != 0 {
		t.Fatal("empty tree visited something")
	}
}

func TestSingleKey(t *testing.T) {
	r, _ := buildTree(t, 4, []entry{{[]byte{0, 0, 0, 5}, 42}})
	if v, found, _ := r.Get([]byte{0, 0, 0, 5}); !found || v != 42 {
		t.Fatal("single key not found")
	}
	for _, missing := range [][]byte{{0, 0, 0, 4}, {0, 0, 0, 6}} {
		if _, found, _ := r.Get(missing); found {
			t.Fatalf("found missing key %v", missing)
		}
	}
}

// Many keys -> a multi-level tree. Every key must be found, and keys that
// were never added must not be.
func TestGetManyKeys(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	es := randomEntries(rng, 200_000, key.KeySize)
	r, blocks := buildTree(t, key.KeySize, es)
	if blocks < 100 {
		t.Fatalf("expected a multi-level tree, got %d blocks", blocks)
	}
	for _, e := range es {
		v, found, err := r.Get(e.key)
		if err != nil || !found || v != e.value {
			t.Fatalf("Get(%x) = %d %v %v", e.key, v, found, err)
		}
	}
	for i := 0; i < 10_000; i++ {
		k := make([]byte, key.KeySize)
		rng.Read(k)
		i := sort.Search(len(es), func(i int) bool { return bytes.Compare(es[i].key, k) >= 0 })
		inTree := i < len(es) && bytes.Equal(es[i].key, k)
		if _, found, _ := r.Get(k); found != inTree {
			t.Fatalf("Get(%x) found=%v, want %v", k, found, inTree)
		}
	}
}

// Visit from many search points, both directions, compared against the
// sorted slice. Includes searches before the first and after the last key.
func TestVisitMatchesSortedSlice(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	es := randomEntries(rng, 20_000, 8)
	r, _ := buildTree(t, 8, es)

	searches := [][]byte{
		bytes.Repeat([]byte{0x00}, 8), // before everything
		bytes.Repeat([]byte{0xFF}, 8), // after everything
	}
	for i := 0; i < 200; i++ {
		s := make([]byte, 8)
		rng.Read(s)
		searches = append(searches, s, slices.Clone(es[rng.Intn(len(es))].key)) // exact keys too
	}
	for _, s := range searches {
		i := sort.Search(len(es), func(i int) bool { return bytes.Compare(es[i].key, s) >= 0 })

		fwd := collect(t, r, s, Forward)
		if wantN := len(es) - i; len(fwd) != wantN {
			t.Fatalf("Forward from %x: got %d keys, want %d", s, len(fwd), wantN)
		}
		for j, e := range fwd {
			if !bytes.Equal(e.key, es[i+j].key) || e.value != es[i+j].value {
				t.Fatalf("Forward from %x: wrong entry at %d", s, j)
			}
		}

		// Backward includes s itself if present.
		last := i - 1
		if i < len(es) && bytes.Equal(es[i].key, s) {
			last = i
		}
		bwd := collect(t, r, s, Backward)
		if len(bwd) != last+1 {
			t.Fatalf("Backward from %x: got %d keys, want %d", s, len(bwd), last+1)
		}
		for j, e := range bwd {
			if !bytes.Equal(e.key, es[last-j].key) {
				t.Fatalf("Backward from %x: wrong entry at %d", s, j)
			}
		}
	}
}

func TestVisitStopsEarly(t *testing.T) {
	es := randomEntries(rand.New(rand.NewSource(3)), 5000, 8)
	r, _ := buildTree(t, 8, es)
	n := 0
	r.Visit(es[0].key, Forward, func([]byte, uint64) bool { n++; return n < 10 })
	if n != 10 {
		t.Fatalf("visited %d, want 10", n)
	}
}

// The delta-layer use case: index key = Key (18 bytes) + LSN (8 bytes).
// Visiting Backward from (k, lsn) yields the versions of k at or below lsn,
// newest first, then moves on to smaller keys.
func TestDeltaStyleKeys(t *testing.T) {
	deltaKey := func(k key.Key, l lsn.Lsn) []byte {
		b := make([]byte, key.KeySize+8)
		k.PutBytes(b)
		binary.BigEndian.PutUint64(b[key.KeySize:], uint64(l))
		return b
	}
	k5 := key.Key{Field4: 16400, Field6: 5}
	k6 := key.Key{Field4: 16400, Field6: 6}
	es := []entry{
		{deltaKey(k5, 100), 1},
		{deltaKey(k5, 200), 2},
		{deltaKey(k5, 300), 3},
		{deltaKey(k6, 150), 4},
	}
	r, _ := buildTree(t, key.KeySize+8, es)

	var lsns []lsn.Lsn
	r.Visit(deltaKey(k5, 250), Backward, func(b []byte, _ uint64) bool {
		got, _ := key.DecodeKey(b[:key.KeySize])
		if got != k5 {
			return false // reached a different key: stop
		}
		lsns = append(lsns, lsn.Lsn(binary.BigEndian.Uint64(b[key.KeySize:])))
		return true
	})
	if !slices.Equal(lsns, []lsn.Lsn{200, 100}) {
		t.Fatalf("versions of k5 at or below 250 = %v, want [200 100]", lsns)
	}
}

// Keys that share long prefixes should pack more entries per node.
func TestPrefixCompression(t *testing.T) {
	var es []entry
	for i := 0; i < 20_000; i++ {
		k := key.Key{Field2: 1663, Field3: 16384, Field4: 16400, Field6: uint32(i)}
		b := k.Encode()
		es = append(es, entry{b[:], uint64(i)})
	}
	_, blocks := buildTree(t, key.KeySize, es)
	// Without compression: (8192-4)/(18+8) = 314 entries per leaf -> 64 leaves.
	// With ~15 shared bytes per leaf: (8192-4-15)/(3+8) ≈ 743 -> ~27 leaves.
	if blocks > 35 {
		t.Fatalf("prefix compression not effective: %d blocks", blocks)
	}
}

func TestBuilderRejectsBadInput(t *testing.T) {
	f, _ := iox.Create(filepath.Join(t.TempDir(), "f"))
	defer f.Abort()
	w := blockio.NewWriter(f, blockio.BlockSize)
	b, _ := NewBuilder(w, 4)
	if err := b.Append([]byte{1, 2, 3}, 0); !errors.Is(err, ErrKeyLen) {
		t.Fatalf("short key: %v", err)
	}
	b.Append([]byte{0, 0, 0, 5}, 0)
	for _, k := range [][]byte{{0, 0, 0, 5}, {0, 0, 0, 4}} {
		if err := b.Append(k, 0); !errors.Is(err, ErrKeyOrder) {
			t.Fatalf("out of order %v: %v", k, err)
		}
	}
	b.Finish()
	if err := b.Append([]byte{0, 0, 0, 9}, 0); !errors.Is(err, ErrFinished) {
		t.Fatalf("append after finish: %v", err)
	}

	w.Write([]byte("x")) // misaligned
	if _, err := NewBuilder(w, 4); !errors.Is(err, blockio.ErrMisaligned) {
		t.Fatalf("misaligned builder: %v", err)
	}
	if _, err := NewBuilder(w, 0); !errors.Is(err, ErrKeyLen) {
		t.Fatalf("zero key length: %v", err)
	}
}

// A corrupt root block must give an error, not a panic.
func TestCorruptNode(t *testing.T) {
	garbage := bytes.Repeat([]byte{0xFF}, blockio.BlockSize)
	r := NewReader(blockio.NewReader(bytes.NewReader(garbage)), 0, key.KeySize)
	if _, _, err := r.Get(make([]byte, key.KeySize)); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("want ErrCorrupt, got %v", err)
	}
	// A node pointing to itself must not loop forever.
	loop := encodeNode(1, 4, [][]byte{{0, 0, 0, 0}}, []uint64{0})
	r = NewReader(blockio.NewReader(bytes.NewReader(loop)), 0, 4)
	if _, _, err := r.Get([]byte{0, 0, 0, 1}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("self-loop: want ErrCorrupt, got %v", err)
	}
}

func FuzzDecodeNode(f *testing.F) {
	f.Add(encodeNode(0, 4, [][]byte{{1, 2, 3, 4}}, []uint64{9}))
	f.Fuzz(func(t *testing.T, data []byte) {
		block := make([]byte, blockio.BlockSize)
		copy(block, data)
		r := NewReader(blockio.NewReader(bytes.NewReader(block)), 0, 4)
		r.Get([]byte{1, 2, 3, 4}) // must not panic
		r.Visit([]byte{0, 0, 0, 0}, Forward, func([]byte, uint64) bool { return true })
	})
}