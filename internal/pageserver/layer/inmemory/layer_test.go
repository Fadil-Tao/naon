package inmemory

import (
	"errors"
	"math/rand"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Fadil-Tao/naon/internal/pageserver/ids"
	"github.com/Fadil-Tao/naon/internal/pageserver/key"
	"github.com/Fadil-Tao/naon/internal/pageserver/layer"
	"github.com/Fadil-Tao/naon/internal/pageserver/layer/delta"
	"github.com/Fadil-Tao/naon/internal/pageserver/lsn"
	"github.com/Fadil-Tao/naon/internal/pageserver/value"
)

var (
	tenant   = ids.NewTenantID()
	timeline = ids.NewTimelineID()
)

func pg(n uint32) key.Key { return key.Key{Field2: 1663, Field3: 16384, Field4: 16400, Field6: n} }

func newLayer(t *testing.T, start lsn.Lsn) (*Layer, string) {
	t.Helper()
	dir := t.TempDir()
	l, err := New(dir, tenant, timeline, start)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	return l, dir
}

func put(k key.Key, l lsn.Lsn, v value.Value) Put { return Put{Key: k, Lsn: l, Value: v} }
func app(s string) value.Value                    { return value.Record(value.TestAppend(s)) }
func init_(s string) value.Value                  { return value.Record(value.TestInit(s)) }

func read(t *testing.T, r layer.Reader, k key.Key, rng lsn.Range) (lsns []lsn.Lsn, complete bool, st *value.ReconstructState) {
	t.Helper()
	st = &value.ReconstructState{}
	if err := r.GetValueReconstructData(k, rng, st); err != nil {
		t.Fatal(err)
	}
	in := st.Finish()
	if in.HasImage {
		lsns = append(lsns, in.ImageLsn)
	}
	for _, rec := range in.Records {
		lsns = append(lsns, rec.Lsn)
	}
	return lsns, st.IsComplete(), st
}

func TestPutThenRead(t *testing.T) {
	l, _ := newLayer(t, 100)
	if err := l.PutBatch([]Put{
		put(pg(1), 110, init_("a")),
		put(pg(1), 120, app("b")),
		put(pg(2), 120, init_("z")),
		put(pg(1), 130, app("c")),
	}); err != nil {
		t.Fatal(err)
	}

	lsns, complete, st := read(t, l, pg(1), lsn.Range{Start: 100, End: 200})
	if !slices.Equal(lsns, []lsn.Lsn{110, 120, 130}) || !complete {
		t.Fatalf("got %v complete=%v", lsns, complete)
	}
	if p, err := value.ApplyTest(st.Finish()); err != nil || string(p) != "abc" {
		t.Fatalf("page = %q, %v", p, err)
	}

	lsns, _, _ = read(t, l, pg(1), lsn.Range{Start: 100, End: 130})
	if !slices.Equal(lsns, []lsn.Lsn{110, 120}) {
		t.Fatalf("end 130: %v", lsns)
	}

	lsns, complete, _ = read(t, l, pg(1), lsn.Range{Start: 120, End: 200})
	if !slices.Equal(lsns, []lsn.Lsn{120, 130}) || complete {
		t.Fatalf("floor 120: %v complete=%v", lsns, complete)
	}

	if _, _, st := read(t, l, pg(9), lsn.Range{Start: 0, End: 1000}); !st.IsEmpty() {
		t.Fatal("unknown key returned data")
	}
	if _, _, st := read(t, l, pg(1), lsn.Range{Start: 131, End: 1000}); !st.IsEmpty() {
		t.Fatal("range above the data returned data")
	}
}

func TestReadStopsAtImage(t *testing.T) {
	l, _ := newLayer(t, 0)
	l.PutBatch([]Put{
		put(pg(1), 10, init_("old")),
		put(pg(1), 20, value.Image([]byte("img"))),
		put(pg(1), 30, app("+")),
	})
	lsns, complete, st := read(t, l, pg(1), lsn.Range{Start: 0, End: 100})
	if !slices.Equal(lsns, []lsn.Lsn{20, 30}) || !complete {
		t.Fatalf("got %v complete=%v", lsns, complete)
	}
	if p, _ := value.ApplyTest(st.Finish()); string(p) != "img+" {
		t.Fatalf("page = %q", p)
	}
}

func TestPutValidation(t *testing.T) {
	l, _ := newLayer(t, 100)
	if err := l.PutBatch([]Put{put(pg(1), 150, init_("a"))}); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		puts []Put
		want error
	}{
		{"below layer start", []Put{put(pg(2), 99, init_("a"))}, ErrLsnRange},
		{"same lsn again for the same key", []Put{put(pg(1), 150, app("b"))}, ErrOrder},
		{"older lsn for the same key", []Put{put(pg(1), 140, app("b"))}, ErrOrder},
		{"duplicate inside the batch", []Put{put(pg(3), 160, init_("a")), put(pg(3), 160, app("b"))}, ErrOrder},
		{"decreasing inside the batch", []Put{put(pg(3), 170, init_("a")), put(pg(3), 165, app("b"))}, ErrOrder},
		{"MaxKey", []Put{put(key.MaxKey, 200, init_("a"))}, ErrKey},
	}
	for _, c := range cases {
		if err := l.PutBatch(c.puts); !errors.Is(err, c.want) {
			t.Fatalf("%s: err = %v, want %v", c.name, err, c.want)
		}
	}

	if l.NumEntries() != 1 {
		t.Fatalf("entries = %d, want 1", l.NumEntries())
	}
	if _, _, st := read(t, l, pg(3), lsn.Range{Start: 0, End: 1000}); !st.IsEmpty() {
		t.Fatal("a failed batch was partly published")
	}

	if err := l.PutBatch([]Put{put(pg(4), 150, init_("a")), put(pg(5), 150, init_("b"))}); err != nil {
		t.Fatal(err)
	}

	if err := l.PutBatch(nil); err != nil {
		t.Fatal(err)
	}
}

func TestFreeze(t *testing.T) {
	l, _ := newLayer(t, 100)
	if d := l.Desc(); d.LsnRange != (lsn.Range{Start: 100, End: lsn.Max}) || d.KeyRange != key.FullRange() || !d.IsDelta {
		t.Fatalf("open desc = %+v", d)
	}
	l.PutBatch([]Put{put(pg(1), 150, init_("a"))})

	for _, end := range []lsn.Lsn{0, 100, 150, lsn.Max} {
		if err := l.Freeze(end); !errors.Is(err, ErrLsnRange) {
			t.Fatalf("Freeze(%s) = %v, want ErrLsnRange", end, err)
		}
	}
	if err := l.Freeze(151); err != nil {
		t.Fatal(err)
	}
	if d := l.Desc(); d.LsnRange != (lsn.Range{Start: 100, End: 151}) {
		t.Fatalf("frozen desc = %+v", d)
	}
	if err := l.Freeze(200); !errors.Is(err, ErrFrozen) {
		t.Fatalf("second freeze = %v", err)
	}
	if err := l.PutBatch([]Put{put(pg(1), 160, app("b"))}); !errors.Is(err, ErrFrozen) {
		t.Fatalf("put after freeze = %v", err)
	}

	if lsns, _, _ := read(t, l, pg(1), lsn.Range{Start: 0, End: 1000}); !slices.Equal(lsns, []lsn.Lsn{150}) {
		t.Fatalf("read after freeze: %v", lsns)
	}
}

func TestFreezeEmptyLayer(t *testing.T) {
	l, dir := newLayer(t, 5)
	if err := l.Freeze(6); err != nil {
		t.Fatal(err)
	}
	desc, path, err := l.WriteToL0(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !desc.IsL0() {
		t.Fatalf("desc = %s", desc)
	}
	r, err := delta.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
}

func TestWriteToL0RequiresFreeze(t *testing.T) {
	l, dir := newLayer(t, 0)
	l.PutBatch([]Put{put(pg(1), 1, init_("a"))})
	if _, _, err := l.WriteToL0(dir); !errors.Is(err, ErrNotFrozen) {
		t.Fatalf("err = %v, want ErrNotFrozen", err)
	}
}

type version struct {
	lsn lsn.Lsn
	val value.Value
}

func modelRead(vs []version, rng lsn.Range) (lsns []lsn.Lsn, complete bool) {
	var newestFirst []lsn.Lsn
	for i := len(vs) - 1; i >= 0; i-- {
		v := vs[i]
		if v.lsn >= rng.End {
			continue
		}
		if v.lsn < rng.Start {
			break
		}
		newestFirst = append(newestFirst, v.lsn)
		if v.val.WillInit() {
			complete = true
			break
		}
	}
	slices.Reverse(newestFirst)
	return newestFirst, complete
}

func randomValue(r *rand.Rand, first bool, l lsn.Lsn) value.Value {
	switch {
	case first || r.Intn(10) == 0:
		return init_("i" + l.String())
	case r.Intn(15) == 0:
		return value.Image([]byte("img" + l.String()))
	default:
		return app("," + l.String())
	}
}

func TestLayerMatchesModelAndDeltaFile(t *testing.T) {
	const nKeys = 8
	for round := 0; round < 60; round++ {
		r := rand.New(rand.NewSource(int64(round)))
		start := lsn.Lsn(r.Intn(50))
		l, dir := newLayer(t, start)

		model := map[key.Key][]version{}
		cur := start
		var batch []Put
		flush := func() {
			if err := l.PutBatch(batch); err != nil {
				t.Fatal(err)
			}
			batch = nil
		}
		for i, n := 0, 200+r.Intn(300); i < n; i++ {
			cur += lsn.Lsn(1 + r.Intn(3))

			for _, ki := range r.Perm(nKeys)[:1+r.Intn(3)] {
				k := pg(uint32(ki))
				v := randomValue(r, len(model[k]) == 0, cur)
				model[k] = append(model[k], version{cur, v})
				batch = append(batch, put(k, cur, v))
			}
			if r.Intn(4) == 0 {
				flush()
			}
		}
		flush()

		end := cur + 1
		if err := l.Freeze(end); err != nil {
			t.Fatal(err)
		}
		desc, path, err := l.WriteToL0(dir)
		if err != nil {
			t.Fatal(err)
		}
		if !desc.IsL0() || desc.LsnRange != (lsn.Range{Start: start, End: end}) {
			t.Fatalf("round %d: desc = %s", round, desc)
		}
		file, err := delta.Open(path)
		if err != nil {
			t.Fatal(err)
		}

		type kv struct {
			k key.Key
			l lsn.Lsn
		}
		var fromFile, fromModel []kv
		file.Iter(func(k key.Key, l lsn.Lsn, _ value.Value) bool { fromFile = append(fromFile, kv{k, l}); return true })
		keys := make([]key.Key, 0, len(model))
		for k := range model {
			keys = append(keys, k)
		}
		slices.SortFunc(keys, key.Key.Compare)
		for _, k := range keys {
			for _, v := range model[k] {
				fromModel = append(fromModel, kv{k, v.lsn})
			}
		}
		if !slices.Equal(fromFile, fromModel) {
			t.Fatalf("round %d: file entries differ from the model", round)
		}

		for q := 0; q < 200; q++ {
			k := pg(uint32(r.Intn(nKeys + 2)))
			a := lsn.Lsn(r.Intn(int(end) + 5))
			b := a + lsn.Lsn(r.Intn(int(end)+5))
			rng := lsn.Range{Start: a, End: b}
			wantL, wantC := modelRead(model[k], rng)

			gotL, gotC, st := read(t, l, k, rng)
			if !slices.Equal(gotL, wantL) || gotC != wantC {
				t.Fatalf("round %d: memory layer %s %v = %v/%v, model %v/%v", round, k, rng, gotL, gotC, wantL, wantC)
			}
			fileL, fileC, fst := read(t, file, k, rng)
			if !slices.Equal(fileL, wantL) || fileC != wantC {
				t.Fatalf("round %d: delta file %s %v = %v/%v, model %v/%v", round, k, rng, fileL, fileC, wantL, wantC)
			}
			if st.IsComplete() {
				p1, e1 := value.ApplyTest(st.Finish())
				p2, e2 := value.ApplyTest(fst.Finish())
				if e1 != nil || e2 != nil || string(p1) != string(p2) {
					t.Fatalf("round %d: pages differ: %q vs %q (%v, %v)", round, p1, p2, e1, e2)
				}
			}
		}
		file.Close()
	}
}

func TestReadAfterRemove(t *testing.T) {
	l, _ := newLayer(t, 0)
	l.PutBatch([]Put{put(pg(1), 1, init_("a"))})
	l.Freeze(2)
	if err := l.Remove(); err != nil {
		t.Fatal(err)
	}
	if lsns, complete, _ := read(t, l, pg(1), lsn.Range{Start: 0, End: 10}); !slices.Equal(lsns, []lsn.Lsn{1}) || !complete {
		t.Fatalf("read after Remove: %v %v", lsns, complete)
	}
}

func TestConcurrentReadersAndWriter(t *testing.T) {
	l, _ := newLayer(t, 0)
	const n = 3000
	var published atomic.Uint64
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				seen := published.Load()
				if seen == 0 {
					continue
				}
				var st value.ReconstructState
				if err := l.GetValueReconstructData(pg(1), lsn.Range{Start: 0, End: lsn.Max}, &st); err != nil {
					t.Errorf("read: %v", err)
					return
				}
				recs := st.Finish().Records
				if uint64(len(recs)) < seen || uint64(recs[len(recs)-1].Lsn) < seen {
					t.Errorf("published %d but reader saw %d records, newest %s", seen, len(recs), recs[len(recs)-1].Lsn)
					return
				}
			}
		}()
	}
	for i := uint64(1); i <= n; i++ {
		if err := l.PutBatch([]Put{put(pg(1), lsn.Lsn(i), app("x"))}); err != nil {
			t.Fatal(err)
		}
		published.Store(i)
	}
	close(stop)
	wg.Wait()
}
