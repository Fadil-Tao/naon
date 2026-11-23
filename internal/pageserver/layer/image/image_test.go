package image

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/Fadil-Tao/naon/internal/pageserver/ids"
	"github.com/Fadil-Tao/naon/internal/pageserver/key"
	"github.com/Fadil-Tao/naon/internal/pageserver/layer"
	"github.com/Fadil-Tao/naon/internal/pageserver/lsn"
	"github.com/Fadil-Tao/naon/internal/pageserver/value"
)

func page(n uint32) key.Key { return key.Key{Field4: 16400, Field6: n} }

func writeImage(t *testing.T, dir string, at lsn.Lsn, keys []uint32) (layer.Desc, string) {
	t.Helper()
	w, err := NewWriter(dir, ids.NewTenantID(), ids.NewTimelineID(), key.KeyRange{Start: page(0), End: page(1000)}, at)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Abort()
	for _, n := range keys {
		if err := w.Put(page(n), []byte(fmt.Sprintf("page%d", n))); err != nil {
			t.Fatal(err)
		}
	}
	desc, path, err := w.Finish(0)
	if err != nil {
		t.Fatal(err)
	}
	return desc, path
}

func TestImageRead(t *testing.T) {
	var keys []uint32
	for n := uint32(0); n < 1000; n += 2 { // even pages exist, odd pages do not
		keys = append(keys, n)
	}
	desc, path := writeImage(t, t.TempDir(), 500, keys)
	if desc.IsDelta || desc.ImageLsn() != 500 || desc.LsnRange != (lsn.Range{Start: 500, End: 501}) {
		t.Fatalf("bad desc %+v", desc)
	}
	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	for n := uint32(0); n < 1000; n++ {
		var st value.ReconstructState
		if err := r.GetValueReconstructData(page(n), lsn.Range{Start: 0, End: 501}, &st); err != nil {
			t.Fatal(err)
		}
		if !st.IsComplete() {
			t.Fatalf("page %d: an image layer always completes keys in its range", n)
		}
		if n%2 == 1 {
			if !st.IsAbsent() {
				t.Fatalf("page %d should be absent", n)
			}
			continue
		}
		in := st.Finish()
		if !in.HasImage || in.ImageLsn != 500 || !bytes.Equal(in.Image, []byte(fmt.Sprintf("page%d", n))) {
			t.Fatalf("page %d: wrong image %q", n, in.Image)
		}
	}
}

func TestImageIgnoresOutOfScope(t *testing.T) {
	_, path := writeImage(t, t.TempDir(), 500, []uint32{1, 2, 3})
	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	cases := []struct {
		k     key.Key
		limit lsn.Lsn
	}{
		{page(1), 500},            // image is AT 500, reading below 500 must not see it
		{page(1), 100},            // far below
		{page(1000), 501},         // key range end is exclusive
		{key.Key{Field4: 1}, 501}, // another table entirely
	}
	for _, c := range cases {
		var st value.ReconstructState
		if err := r.GetValueReconstructData(c.k, lsn.Range{Start: 0, End: c.limit}, &st); err != nil || !st.IsEmpty() || st.IsComplete() {
			t.Fatalf("%s limit %d: state should be untouched", c.k, c.limit)
		}
	}
}

func TestImageIter(t *testing.T) {
	keys := []uint32{3, 7, 8, 500, 999}
	_, path := writeImage(t, t.TempDir(), 10, keys)
	r, _ := Open(path)
	defer r.Close()
	var got []uint32
	r.Iter(func(k key.Key, p []byte) bool {
		got = append(got, k.Field6)
		if string(p) != fmt.Sprintf("page%d", k.Field6) {
			t.Fatalf("wrong page for %d", k.Field6)
		}
		return true
	})
	if fmt.Sprint(got) != fmt.Sprint(keys) {
		t.Fatalf("Iter = %v, want %v", got, keys)
	}
}

func TestImageWriterRejectsBadInput(t *testing.T) {
	w, _ := NewWriter(t.TempDir(), ids.NewTenantID(), ids.NewTimelineID(), key.KeyRange{Start: page(10), End: page(20)}, 5)
	defer w.Abort()
	if err := w.Put(page(9), nil); !errors.Is(err, ErrOutOfRange) {
		t.Fatalf("key before range: %v", err)
	}
	if err := w.Put(page(20), nil); !errors.Is(err, ErrOutOfRange) {
		t.Fatalf("key at range end: %v", err)
	}
	w.Put(page(15), nil)
	if err := w.Put(page(15), nil); !errors.Is(err, ErrOrder) {
		t.Fatalf("duplicate key: %v", err)
	}
	if _, err := NewWriter(t.TempDir(), ids.TenantID{}, ids.TimelineID{}, key.KeyRange{Start: page(5), End: page(5)}, 1); err == nil {
		t.Fatal("empty key range accepted")
	}
}