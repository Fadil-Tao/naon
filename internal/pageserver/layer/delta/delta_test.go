package delta

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/Fadil-Tao/naon/internal/pageserver/ids"
	"github.com/Fadil-Tao/naon/internal/pageserver/key"
	"github.com/Fadil-Tao/naon/internal/pageserver/layer"
	"github.com/Fadil-Tao/naon/internal/pageserver/lsn"
	"github.com/Fadil-Tao/naon/internal/pageserver/value"
)

var (
	tenant   = ids.NewTenantID()
	timeline = ids.NewTimelineID()
)

func page(n uint32) key.Key { return key.Key{Field2: 1663, Field3: 16384, Field4: 16400, Field6: n} }

type version struct {
	lsn lsn.Lsn
	val value.Value
}

// expectedPage computes the answer the slow, obvious way: take the versions of
// one key with LSN < limit, walk newest to oldest until a will_init value,
// then apply them oldest first. ok=false means "no base found in this layer".
func expectedPage(versions []version, limit lsn.Lsn) (page string, ok bool, found bool) {
	var stack []version
	for i := len(versions) - 1; i >= 0; i-- {
		v := versions[i]
		if v.lsn >= limit {
			continue
		}
		stack = append(stack, v)
		if v.val.WillInit() {
			ok = true
			break
		}
	}
	if len(stack) == 0 {
		return "", false, false
	}
	var p []byte
	for i := len(stack) - 1; i >= 0; i-- {
		v := stack[i].val
		if v.IsImage() {
			p = append([]byte(nil), v.ImageBytes()...)
			continue
		}
		tr := v.WalRecord().(value.TestRecord)
		if tr.Init {
			p = p[:0]
		}
		p = append(p, tr.Append...)
	}
	return string(p), ok, true
}

// writeRandomDelta writes nKeys keys with random histories in [100, 1000).
func writeRandomDelta(t *testing.T, dir string, rng *rand.Rand, nKeys int) (layer.Desc, string, map[key.Key][]version) {
	t.Helper()
	w, err := NewWriter(dir, tenant, timeline, page(0), lsn.Range{Start: 100, End: 1000})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Abort()
	model := map[key.Key][]version{}
	for i := 0; i < nKeys; i++ {
		k := page(uint32(i * 3)) // gaps between keys
		l := lsn.Lsn(100)
		for j := 0; j < 1+rng.Intn(8); j++ {
			l += lsn.Lsn(1 + rng.Intn(100))
			if l >= 1000 {
				break
			}
			var v value.Value
			switch rng.Intn(5) {
			case 0:
				v = value.Image([]byte(fmt.Sprintf("img%d@%d|", i, l)))
			case 1:
				v = value.Record(value.TestInit(fmt.Sprintf("init%d@%d|", i, l)))
			default:
				v = value.Record(value.TestAppend(fmt.Sprintf("+%d|", l)))
			}
			if err := w.Put(k, l, v); err != nil {
				t.Fatal(err)
			}
			model[k] = append(model[k], version{l, v})
		}
	}
	desc, path, err := w.Finish(page(uint32(nKeys*3+1)), 1)
	if err != nil {
		t.Fatal(err)
	}
	return desc, path, model
}

// Compare every (key, limit) against the model.
func TestReadMatchesModel(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	dir := t.TempDir()
	desc, path, model := writeRandomDelta(t, dir, rng, 2000)
	if filepath.Base(path) != desc.FileName() {
		t.Fatalf("file is %s, want %s", filepath.Base(path), desc.FileName())
	}

	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if r.Desc() != desc || r.Summary().Tenant != tenant || r.Summary().Timeline != timeline {
		t.Fatal("desc or summary mismatch after reopen")
	}

	for k, versions := range model {
		for _, limit := range []lsn.Lsn{50, 100, 101, 150, 300, 500, 999, 1000, 5000} {
			var st value.ReconstructState
			if err := r.GetValueReconstructData(k, lsn.Range{Start: 0, End: limit}, &st); err != nil {
				t.Fatal(err)
			}
			want, complete, found := expectedPage(versions, limit)
			if st.IsEmpty() == found || st.IsComplete() != complete {
				t.Fatalf("%s limit %d: found=%v complete=%v, want %v %v", k, limit, !st.IsEmpty(), st.IsComplete(), found, complete)
			}
			if complete {
				got, err := value.ApplyTest(st.Finish())
				if err != nil || string(got) != want {
					t.Fatalf("%s limit %d: page %q, want %q (%v)", k, limit, got, want, err)
				}
			}
		}
	}

	// A key between two stored keys, and keys outside the range.
	for _, k := range []key.Key{page(1), page(1 << 30), {Field1: 0x10}} {
		var st value.ReconstructState
		if err := r.GetValueReconstructData(k, lsn.Range{Start: 0, End: 1000}, &st); err != nil || !st.IsEmpty() {
			t.Fatalf("%s: expected nothing, got empty=%v err=%v", k, st.IsEmpty(), err)
		}
	}
}

func TestIterInOrder(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	_, path, model := writeRandomDelta(t, t.TempDir(), rng, 300)
	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	n, total := 0, 0
	for _, v := range model {
		total += len(v)
	}
	var lastK key.Key
	var lastL lsn.Lsn
	err = r.Iter(func(k key.Key, l lsn.Lsn, v value.Value) bool {
		if n > 0 && (k.Less(lastK) || (k == lastK && l <= lastL)) {
			t.Fatalf("Iter out of order at %s @ %s", k, l)
		}
		want := model[k]
		idx := 0
		for idx < len(want) && want[idx].lsn != l {
			idx++
		}
		if idx == len(want) {
			t.Fatalf("Iter returned %s @ %s which was never written", k, l)
		}
		lastK, lastL = k, l
		n++
		return true
	})
	if err != nil || n != total {
		t.Fatalf("Iter visited %d of %d: %v", n, total, err)
	}
}

func TestWriterRejectsBadInput(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWriter(dir, tenant, timeline, page(10), lsn.Range{Start: 100, End: 200})
	defer w.Abort()
	img := value.Image([]byte("x"))
	cases := []struct {
		k   key.Key
		l   lsn.Lsn
		err error
	}{
		{page(5), 150, ErrOutOfRange},  // key before start
		{page(10), 99, ErrOutOfRange},  // lsn before range
		{page(10), 200, ErrOutOfRange}, // lsn at end (exclusive)
	}
	for _, c := range cases {
		if err := w.Put(c.k, c.l, img); !errors.Is(err, c.err) {
			t.Fatalf("Put(%s, %d) = %v, want %v", c.k, c.l, err, c.err)
		}
	}
	if err := w.Put(page(20), 150, img); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		k key.Key
		l lsn.Lsn
	}{{page(20), 150}, {page(20), 140}, {page(15), 160}} {
		if err := w.Put(c.k, c.l, img); !errors.Is(err, ErrOrder) {
			t.Fatalf("Put(%s, %d) = %v, want ErrOrder", c.k, c.l, err)
		}
	}
	if _, _, err := w.Finish(page(20), 0); !errors.Is(err, ErrOutOfRange) {
		t.Fatalf("Finish with end key <= last key: %v", err)
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 0 {
		t.Fatalf("failed Finish left %d files behind", len(entries))
	}
}

func TestAbortLeavesNothing(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWriter(dir, tenant, timeline, page(0), lsn.Range{Start: 1, End: 2})
	w.Put(page(1), 1, value.Image([]byte("x")))
	w.Abort()
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Fatalf("Abort left %d files", len(entries))
	}
}

func TestEmptyDelta(t *testing.T) {
	w, _ := NewWriter(t.TempDir(), tenant, timeline, key.MinKey, lsn.Range{Start: 1, End: 2})
	_, path, err := w.Finish(key.MaxKey, 0)
	if err != nil {
		t.Fatal(err)
	}
	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if !r.Desc().IsL0() {
		t.Fatal("full-range delta should be L0")
	}
	var st value.ReconstructState
	if err := r.GetValueReconstructData(page(1), lsn.Range{Start: 0, End: 2}, &st); err != nil || !st.IsEmpty() {
		t.Fatal("empty delta returned data")
	}
}

// A file whose name does not match its summary must be rejected.
func TestOpenRejectsRenamedFile(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	dir := t.TempDir()
	desc, path, _ := writeRandomDelta(t, dir, rng, 10)
	wrong := desc
	wrong.LsnRange.End++
	renamed := filepath.Join(dir, wrong.FileName())
	if err := os.Rename(path, renamed); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(renamed); !errors.Is(err, layer.ErrBadSummary) {
		t.Fatalf("renamed file opened: %v", err)
	}
}

// The floor stops the walk: versions below it belong to an older layer.
func TestFloor(t *testing.T) {
	w, _ := NewWriter(t.TempDir(), tenant, timeline, page(0), lsn.Range{Start: 100, End: 400})
	w.Put(page(1), 150, value.Record(value.TestInit("a")))
	w.Put(page(1), 250, value.Record(value.TestAppend("b")))
	w.Put(page(1), 350, value.Record(value.TestAppend("c")))
	_, path, err := w.Finish(page(2), 0)
	if err != nil {
		t.Fatal(err)
	}
	r, _ := Open(path)
	defer r.Close()

	var st value.ReconstructState
	if err := r.GetValueReconstructData(page(1), lsn.Range{Start: 200, End: 400}, &st); err != nil {
		t.Fatal(err)
	}
	in := st.Finish()
	if st.IsComplete() || len(in.Records) != 2 || in.Records[0].Lsn != 250 || in.Records[1].Lsn != 350 {
		t.Fatalf("floor 200: got %+v complete=%v", in.Records, st.IsComplete())
	}
	// A range entirely outside the layer touches nothing.
	var st2 value.ReconstructState
	r.GetValueReconstructData(page(1), lsn.Range{Start: 400, End: 500}, &st2)
	if !st2.IsEmpty() {
		t.Fatal("range above the layer returned data")
	}
}
