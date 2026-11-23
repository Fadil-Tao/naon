package layer_test

// These tests read through a stack of layers the way the read path will:
// newest layer first, then older ones, until the state is complete.

import (
	"testing"

	"github.com/Fadil-Tao/naon/internal/pageserver/ids"
	"github.com/Fadil-Tao/naon/internal/pageserver/key"
	"github.com/Fadil-Tao/naon/internal/pageserver/layer"
	"github.com/Fadil-Tao/naon/internal/pageserver/layer/delta"
	"github.com/Fadil-Tao/naon/internal/pageserver/layer/image"
	"github.com/Fadil-Tao/naon/internal/pageserver/lsn"
	"github.com/Fadil-Tao/naon/internal/pageserver/value"
)

func pg(n uint32) key.Key { return key.Key{Field4: 16400, Field6: n} }

// Image at LSN 100 holds pages 1 and 2 ("a", "b"). Page 3 does not exist.
// A delta over [100, 300) appends to page 1 at 150 and 250, creates page 3
// at 200 (will_init), and appends to it at 260.
func buildStack(t *testing.T) (img, dl layer.Reader) {
	t.Helper()
	dir := t.TempDir()
	tn, tl := ids.NewTenantID(), ids.NewTimelineID()

	iw, _ := image.NewWriter(dir, tn, tl, key.KeyRange{Start: pg(0), End: pg(100)}, 100)
	iw.Put(pg(1), []byte("a"))
	iw.Put(pg(2), []byte("b"))
	_, ipath, err := iw.Finish(0)
	if err != nil {
		t.Fatal(err)
	}

	dw, _ := delta.NewWriter(dir, tn, tl, pg(0), lsn.Range{Start: 101, End: 300})
	dw.Put(pg(1), 150, value.Record(value.TestAppend("+150")))
	dw.Put(pg(1), 250, value.Record(value.TestAppend("+250")))
	dw.Put(pg(3), 200, value.Record(value.TestInit("new")))
	dw.Put(pg(3), 260, value.Record(value.TestAppend("+260")))
	_, dpath, err := dw.Finish(pg(100), 0)
	if err != nil {
		t.Fatal(err)
	}

	ir, err := image.Open(ipath)
	if err != nil {
		t.Fatal(err)
	}
	dr, err := delta.Open(dpath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ir.Close(); dr.Close() })
	return ir, dr
}

// read walks the layers newest first, like the read path will in A8.
func read(t *testing.T, layers []layer.Reader, k key.Key, at lsn.Lsn) (string, bool) {
	t.Helper()
	var st value.ReconstructState
	for _, l := range layers {
		if err := l.GetValueReconstructData(k, lsn.Range{Start: 0, End: at + 1}, &st); err != nil {
			t.Fatal(err)
		}
		if st.IsComplete() {
			break
		}
	}
	if st.IsAbsent() || st.IsEmpty() {
		return "", false
	}
	if !st.IsComplete() {
		t.Fatalf("%s @ %d: records found but no base", k, at)
	}
	p, err := value.ApplyTest(st.Finish())
	if err != nil {
		t.Fatal(err)
	}
	return string(p), true
}

func TestReadThroughStack(t *testing.T) {
	img, dl := buildStack(t)
	stack := []layer.Reader{dl, img} // newest first

	cases := []struct {
		k      key.Key
		at     lsn.Lsn
		want   string
		exists bool
	}{
		{pg(1), 100, "a", true},         // image only
		{pg(1), 149, "a", true},         // before the first delta record
		{pg(1), 150, "a+150", true},     // one record on top of the image
		{pg(1), 299, "a+150+250", true}, // both records
		{pg(2), 299, "b", true},         // untouched by the delta
		{pg(3), 199, "", false},         // absent in the image, not yet created
		{pg(3), 200, "new", true},       // will_init: image layer never consulted
		{pg(3), 299, "new+260", true},
		{pg(4), 299, "", false}, // never existed
	}
	for _, c := range cases {
		got, ok := read(t, stack, c.k, c.at)
		if ok != c.exists || got != c.want {
			t.Fatalf("%s @ %d = %q (exists=%v), want %q (exists=%v)", c.k, c.at, got, ok, c.want, c.exists)
		}
	}
}