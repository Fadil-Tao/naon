package layermap

import (
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/Fadil-Tao/naon/internal/pageserver/key"
	"github.com/Fadil-Tao/naon/internal/pageserver/layer"
	"github.com/Fadil-Tao/naon/internal/pageserver/lsn"
	"github.com/Fadil-Tao/naon/internal/pageserver/value"
)

type rec struct {
	lsn lsn.Lsn
	v   value.Value
}

type fake struct {
	desc layer.Desc
	recs map[key.Key][]rec
	imgs map[key.Key][]byte
}

var _ layer.Reader = (*fake)(nil)

func (f *fake) Desc() layer.Desc { return f.desc }
func (f *fake) Close() error     { return nil }

func (f *fake) GetValueReconstructData(k key.Key, r lsn.Range, st *value.ReconstructState) error {
	d := f.desc
	if !d.KeyRange.Contains(k) {
		return nil
	}
	if !d.IsDelta {
		l := d.ImageLsn()
		if !r.Contains(l) {
			return nil
		}
		if b, ok := f.imgs[k]; ok {
			return st.Add(l, value.Image(b))
		}
		return st.MarkAbsent()
	}
	rs := f.recs[k]
	for i := len(rs) - 1; i >= 0; i-- {
		if rs[i].lsn >= r.End {
			continue
		}
		if rs[i].lsn < r.Start {
			break
		}
		if err := st.Add(rs[i].lsn, rs[i].v); err != nil {
			return err
		}
		if st.IsComplete() {
			break
		}
	}
	return nil
}

func fk(n uint32) key.Key { return key.Key{Field4: 16400, Field6: n} }

func rng(a, b uint32) key.KeyRange { return key.KeyRange{Start: fk(a), End: fk(b)} }

func newEntry(d layer.Desc) Entry {
	return Entry{Desc: d, InMemory: false, Layer: &fake{desc: d}}
}

func delta(s, e lsn.Lsn) Entry {
	return newEntry(layer.Desc{KeyRange: key.FullRange(), LsnRange: lsn.Range{Start: s, End: e}, IsDelta: true})
}

func deltaR(a, b uint32, s, e lsn.Lsn) Entry {
	return newEntry(layer.Desc{KeyRange: rng(a, b), LsnRange: lsn.Range{Start: s, End: e}, IsDelta: true})
}

func img(l lsn.Lsn) Entry {
	return newEntry(layer.Desc{KeyRange: key.FullRange(), LsnRange: lsn.Range{Start: l, End: l + 1}})
}

func mem(s, e lsn.Lsn) Entry {
	d := layer.Desc{KeyRange: key.FullRange(), LsnRange: lsn.Range{Start: s, End: e}, IsDelta: true}
	return Entry{Desc: d, InMemory: true, Layer: &fake{desc: d}}
}

func build(t *testing.T, frozen []Entry, open *Entry, hist ...Entry) *Snapshot {
	t.Helper()
	m := New()
	err := m.Update(func(u *Updater) error {
		for _, f := range frozen {
			e := f
			e.Desc.LsnRange.End = lsn.Max
			if err := u.SetOpen(e); err != nil {
				return err
			}
			if err := u.FreezeOpen(f.Desc.LsnRange.End); err != nil {
				return err
			}
		}
		if open != nil {
			if err := u.SetOpen(*open); err != nil {
				return err
			}
		}
		for _, h := range hist {
			if err := u.InsertHistoric(h); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return m.Snapshot()
}

func TestSearchRules(t *testing.T) {
	type want struct {
		layer layer.Desc
		floor lsn.Lsn
	}
	w := func(e Entry, floor lsn.Lsn) *want { return &want{e.Desc, floor} }
	openM := func(s lsn.Lsn) *Entry { e := mem(s, lsn.Max); return &e }

	f1, f2 := mem(50, 100), mem(100, 150)
	dA, dB := delta(10, 20), delta(20, 30)
	open20 := openM(20)

	cases := []struct {
		name   string
		frozen []Entry
		open   *Entry
		hist   []Entry
		k      key.Key
		end    lsn.Lsn
		want   *want
	}{
		{name: "empty map", end: 100},

		{name: "open: start must be below end (not found at start)", open: openM(100), end: 100},
		{name: "open: found just above start", open: openM(100), end: 101, want: w(*openM(100), 100)},
		{name: "open: far above start", open: openM(100), end: 500, want: w(*openM(100), 100)},

		{name: "frozen: below all", frozen: []Entry{f1, f2}, open: openM(150), end: 50},
		{name: "frozen: oldest", frozen: []Entry{f1, f2}, open: openM(150), end: 100, want: w(f1, 50)},
		{name: "frozen: newest below open", frozen: []Entry{f1, f2}, open: openM(150), end: 150, want: w(f2, 100)},
		{name: "frozen: inside the newer one", frozen: []Entry{f1, f2}, open: openM(150), end: 120, want: w(f2, 100)},
		{name: "open wins when it starts below end", frozen: []Entry{f1, f2}, open: openM(150), end: 151, want: w(*openM(150), 150)},

		{name: "delta: picks the newest below end", hist: []Entry{dA, dB}, end: 25, want: w(dB, 20)},
		{name: "delta: older one when end is at the boundary", hist: []Entry{dA, dB}, end: 20, want: w(dA, 10)},
		{name: "delta: nothing below its start", hist: []Entry{dA, dB}, end: 10},
		{name: "delta spanning end is usable", hist: []Entry{delta(10, 30)}, end: 20, want: w(delta(10, 30), 10)},

		{name: "delta: other key range ignored", hist: []Entry{deltaR(0, 3, 10, 20)}, k: fk(5), end: 30},
		{name: "delta: key in range", hist: []Entry{deltaR(0, 3, 10, 20)}, k: fk(1), end: 30, want: w(deltaR(0, 3, 10, 20), 10)},
		{name: "delta: key range end is exclusive", hist: []Entry{deltaR(0, 5, 10, 20)}, k: fk(5), end: 30},

		{name: "image: alone", hist: []Entry{img(5)}, end: 100, want: w(img(5), 5)},
		{name: "image: LSN must be below end", hist: []Entry{img(5)}, end: 5},
		{name: "image: just above its LSN", hist: []Entry{img(5)}, end: 6, want: w(img(5), 5)},

		{name: "image newer than delta", hist: []Entry{dA, img(25)}, end: 30, want: w(img(25), 25)},
		{name: "image ties delta end: image wins", hist: []Entry{delta(10, 21), img(20)}, end: 30, want: w(img(20), 20)},
		{name: "delta newer than image", hist: []Entry{img(5), dA}, end: 30, want: w(dA, 10)},
		{name: "floor is image+1 when the delta starts below the image", hist: []Entry{img(5), delta(3, 20)}, end: 30, want: w(delta(3, 20), 6)},
		{name: "image exact match", hist: []Entry{img(5), delta(3, 20)}, end: 6, want: w(img(5), 5)},

		{name: "memory beats older layers", hist: []Entry{dA}, open: open20, end: 30, want: w(*open20, 20)},
		{name: "memory floor lifted above an older image", hist: []Entry{img(25)}, open: open20, end: 30, want: w(*open20, 26)},
		{name: "image reaching end ties memory: image wins", hist: []Entry{img(29)}, open: open20, end: 30, want: w(img(29), 29)},

		{name: "three-way: memory first", hist: []Entry{img(5), delta(3, 20)}, open: open20, end: 30, want: w(*open20, 20)},
		{name: "three-way: then delta with lifted floor", hist: []Entry{img(5), delta(3, 20)}, open: open20, end: 20, want: w(delta(3, 20), 6)},
		{name: "three-way: then image", hist: []Entry{img(5), delta(3, 20)}, open: open20, end: 6, want: w(img(5), 5)},

		{name: "overlapping deltas, same end: smaller start wins", hist: []Entry{delta(20, 30), delta(10, 30)}, end: 40, want: w(delta(10, 30), 10)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			k := c.k
			if k == (key.Key{}) {
				k = fk(5)
			}
			snap := build(t, c.frozen, c.open, c.hist...)
			got, ok := snap.Search(k, c.end)
			if c.want == nil {
				if ok {
					t.Fatalf("found %s floor %s, want nothing", got.Entry.Desc, got.LsnFloor)
				}
				return
			}
			if !ok {
				t.Fatalf("found nothing, want %s floor %s", c.want.layer, c.want.floor)
			}
			if got.Entry.Desc != c.want.layer || got.LsnFloor != c.want.floor {
				t.Fatalf("got %s floor %s, want %s floor %s", got.Entry.Desc, got.LsnFloor, c.want.layer, c.want.floor)
			}
			if got.LsnFloor >= c.end {
				t.Fatalf("floor %s not below end %s: the read loop would not progress", got.LsnFloor, c.end)
			}
		})
	}
}

func TestFreezeFlushSequence(t *testing.T) {
	m := New()
	open1 := mem(0, lsn.Max)
	if err := m.Update(func(u *Updater) error { return u.SetOpen(open1) }); err != nil {
		t.Fatal(err)
	}
	before := m.Snapshot()

	err := m.Update(func(u *Updater) error {
		if err := u.FreezeOpen(100); err != nil {
			return err
		}
		return u.SetOpen(mem(100, lsn.Max))
	})
	if err != nil {
		t.Fatal(err)
	}
	snap := m.Snapshot()
	if o, ok := snap.Open(); !ok || o.Desc.LsnRange.Start != 100 {
		t.Fatalf("open after freeze = %v %v", o.Desc, ok)
	}
	fr := snap.Frozen()
	if len(fr) != 1 || fr[0].Desc.LsnRange != (lsn.Range{Start: 0, End: 100}) {
		t.Fatalf("frozen = %v", fr)
	}

	l0 := delta(0, 100)
	err = m.Update(func(u *Updater) error {
		if err := u.InsertHistoric(l0); err != nil {
			return err
		}
		return u.RemoveFrozen(fr[0].Desc)
	})
	if err != nil {
		t.Fatal(err)
	}
	after := m.Snapshot()
	if len(after.Frozen()) != 0 || len(after.Historic()) != 1 || !after.Historic()[0].Desc.IsL0() {
		t.Fatalf("after flush: frozen=%v historic=%v", after.Frozen(), after.Historic())
	}

	if _, ok := before.Open(); !ok || len(before.Frozen()) != 0 || len(before.Historic()) != 0 {
		t.Fatal("an old snapshot was modified")
	}
	if len(snap.Frozen()) != 1 || len(snap.Historic()) != 0 {
		t.Fatal("the pre-flush snapshot was modified")
	}

	if r, ok := after.Search(fk(1), 51); !ok || r.Entry.Desc != l0.Desc || r.LsnFloor != 0 {
		t.Fatalf("search after flush = %v %v", r, ok)
	}
}

func TestFailedUpdateChangesNothing(t *testing.T) {
	m := New()
	if err := m.Update(func(u *Updater) error { return u.InsertHistoric(delta(1, 2)) }); err != nil {
		t.Fatal(err)
	}
	before := m.Snapshot()

	boom := errors.New("boom")
	err := m.Update(func(u *Updater) error {
		if err := u.InsertHistoric(delta(2, 3)); err != nil {
			return err
		}
		if err := u.RemoveHistoric(delta(9, 9).Desc); err != nil {
			return err
		}
		return nil
	})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
	if err := m.Update(func(u *Updater) error { u.InsertHistoric(delta(5, 6)); return boom }); err != boom {
		t.Fatalf("err = %v, want boom", err)
	}
	if m.Snapshot() != before || len(m.Snapshot().Historic()) != 1 {
		t.Fatal("failed update changed the map")
	}
}

func TestPanicInUpdateChangesNothing(t *testing.T) {
	m := New()
	before := m.Snapshot()
	func() {
		defer func() { recover() }()
		m.Update(func(u *Updater) error {
			u.InsertHistoric(delta(1, 2))
			panic("bug")
		})
	}()
	if m.Snapshot() != before {
		t.Fatal("panicking update changed the map")
	}

	if err := m.Update(func(u *Updater) error { return u.InsertHistoric(delta(1, 2)) }); err != nil {
		t.Fatal(err)
	}
}

func TestUpdaterErrors(t *testing.T) {
	m := New()
	try := func(fn func(u *Updater) error) error { return m.Update(fn) }

	if err := try(func(u *Updater) error { return u.FreezeOpen(10) }); !errors.Is(err, ErrNoOpen) {
		t.Fatalf("freeze with no open: %v", err)
	}
	if err := try(func(u *Updater) error { return u.SetOpen(mem(0, 50)) }); !errors.Is(err, ErrInvalid) {
		t.Fatalf("bounded open layer: %v", err)
	}
	if err := try(func(u *Updater) error { return u.SetOpen(delta(0, lsn.Max)) }); !errors.Is(err, ErrInvalid) {
		t.Fatalf("on-disk entry as open layer: %v", err)
	}
	if err := try(func(u *Updater) error { return u.SetOpen(mem(10, lsn.Max)) }); err != nil {
		t.Fatal(err)
	}
	if err := try(func(u *Updater) error { return u.SetOpen(mem(20, lsn.Max)) }); !errors.Is(err, ErrOpenExists) {
		t.Fatalf("second open: %v", err)
	}
	if err := try(func(u *Updater) error { return u.FreezeOpen(10) }); !errors.Is(err, ErrInvalid) {
		t.Fatalf("freeze at start: %v", err)
	}
	if err := try(func(u *Updater) error { return u.FreezeOpen(lsn.Max) }); !errors.Is(err, ErrInvalid) {
		t.Fatalf("freeze at Max: %v", err)
	}
	if err := try(func(u *Updater) error { return u.FreezeOpen(20) }); err != nil {
		t.Fatal(err)
	}
	if err := try(func(u *Updater) error { return u.SetOpen(mem(15, lsn.Max)) }); !errors.Is(err, ErrInvalid) {
		t.Fatalf("open starting before frozen end: %v", err)
	}
	if err := try(func(u *Updater) error { return u.SetOpen(mem(20, lsn.Max)) }); err != nil {
		t.Fatal(err)
	}
	if err := try(func(u *Updater) error { return u.FreezeOpen(30) }); err != nil {
		t.Fatal(err)
	}

	newer := layer.Desc{KeyRange: key.FullRange(), LsnRange: lsn.Range{Start: 20, End: 30}, IsDelta: true}
	if err := try(func(u *Updater) error { return u.RemoveFrozen(newer) }); !errors.Is(err, ErrNotOldest) {
		t.Fatalf("remove newest frozen: %v", err)
	}
	older := layer.Desc{KeyRange: key.FullRange(), LsnRange: lsn.Range{Start: 10, End: 20}, IsDelta: true}
	if err := try(func(u *Updater) error { return u.RemoveFrozen(older) }); err != nil {
		t.Fatal(err)
	}

	d := delta(1, 2)
	if err := try(func(u *Updater) error { return u.InsertHistoric(d) }); err != nil {
		t.Fatal(err)
	}
	if err := try(func(u *Updater) error { return u.InsertHistoric(d) }); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("duplicate: %v", err)
	}
	if err := try(func(u *Updater) error { return u.InsertHistoric(mem(1, 2)) }); !errors.Is(err, ErrInvalid) {
		t.Fatalf("in-memory entry as historic: %v", err)
	}
	if err := try(func(u *Updater) error { return u.InsertHistoric(Entry{Desc: d.Desc}) }); !errors.Is(err, ErrInvalid) {
		t.Fatalf("entry without a layer: %v", err)
	}
	if err := try(func(u *Updater) error { return u.InsertHistoric(delta(5, 5)) }); !errors.Is(err, ErrInvalid) {
		t.Fatalf("empty LSN range: %v", err)
	}
	if err := try(func(u *Updater) error { return u.RemoveHistoric(d.Desc) }); err != nil {
		t.Fatal(err)
	}
	if err := try(func(u *Updater) error { return u.RemoveHistoric(d.Desc) }); !errors.Is(err, ErrNotFound) {
		t.Fatalf("remove twice: %v", err)
	}
}

func TestHistoricSortedByLsnStart(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	m := New()
	var all []lsn.Lsn
	for _, s := range r.Perm(50) {
		l := lsn.Lsn(s + 1)
		all = append(all, l)
		if err := m.Update(func(u *Updater) error { return u.InsertHistoric(delta(l, l+5)) }); err != nil {
			t.Fatal(err)
		}
	}
	slices.Sort(all)
	var got []lsn.Lsn
	for _, e := range m.Snapshot().Historic() {
		got = append(got, e.Desc.LsnRange.Start)
	}
	if !slices.Equal(got, all) {
		t.Fatalf("historic order = %v", got)
	}
}

func TestConcurrentReadersAndWriter(t *testing.T) {
	m := New()
	stop := make(chan struct{})
	var wg sync.WaitGroup
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
				snap := m.Snapshot()
				if res, ok := snap.Search(fk(1), 1000); ok {
					found := false
					for _, e := range snap.Historic() {
						if e.Desc == res.Entry.Desc {
							found = true
						}
					}
					if !found {
						t.Errorf("search returned %s which is not in its own snapshot", res.Entry.Desc)
						return
					}
				}
			}
		}()
	}
	for i := lsn.Lsn(1); i <= 300; i++ {
		i := i
		if err := m.Update(func(u *Updater) error { return u.InsertHistoric(delta(i, i+1)) }); err != nil {
			t.Fatal(err)
		}
		if i > 3 {
			if err := m.Update(func(u *Updater) error { return u.RemoveHistoric(delta(i-3, i-2).Desc) }); err != nil {
				t.Fatal(err)
			}
		}
	}
	close(stop)
	wg.Wait()
}

type op struct {
	k    uint32
	lsn  lsn.Lsn
	init bool
	text string
}

func genHistory(r *rand.Rand, nKeys int, n lsn.Lsn) []op {
	seen := map[uint32]bool{}
	var ops []op
	for l := lsn.Lsn(1); l <= n; l++ {
		if r.Intn(5) == 0 {
			continue
		}
		k := uint32(r.Intn(nKeys))
		init := !seen[k] || r.Intn(8) == 0
		seen[k] = true
		ops = append(ops, op{k, l, init, fmt.Sprintf("%d,", l)})
	}
	return ops
}

func (o op) value() value.Value {
	if o.init {
		return value.Record(value.TestInit(o.text))
	}
	return value.Record(value.TestAppend(o.text))
}

func expect(ops []op, k uint32, at lsn.Lsn) (string, bool) {
	var s string
	exists := false
	for _, o := range ops {
		if o.k != k || o.lsn > at {
			continue
		}
		if o.init {
			s = o.text
		} else {
			s += o.text
		}
		exists = true
	}
	return s, exists
}

func randomLayout(t *testing.T, r *rand.Rand, ops []op, nKeys int, n lsn.Lsn) *LayerMap {
	t.Helper()

	edgeSet := map[lsn.Lsn]bool{1: true, n + 1: true}
	for i, nb := 0, r.Intn(5); i < nb; i++ {
		edgeSet[lsn.Lsn(2+r.Intn(int(n)-1))] = true
	}
	var edges []lsn.Lsn
	for e := range edgeSet {
		edges = append(edges, e)
	}
	slices.Sort(edges)
	nBands := len(edges) - 1
	nMem := min(r.Intn(3), nBands)

	collect := func(kr key.KeyRange, s, e lsn.Lsn) map[key.Key][]rec {
		out := map[key.Key][]rec{}
		for _, o := range ops {
			if o.lsn >= s && o.lsn < e && kr.Contains(fk(o.k)) {
				out[fk(o.k)] = append(out[fk(o.k)], rec{o.lsn, o.value()})
			}
		}
		return out
	}

	m := New()
	err := m.Update(func(u *Updater) error {
		for b := 0; b < nBands-nMem; b++ {
			s, e := edges[b], edges[b+1]
			var ranges []key.KeyRange
			if r.Intn(3) == 0 {
				ranges = []key.KeyRange{key.FullRange()}
			} else {
				cuts := map[uint32]bool{0: true, uint32(nKeys): true}
				for i := 0; i < r.Intn(4); i++ {
					cuts[uint32(1+r.Intn(nKeys-1))] = true
				}
				var cs []uint32
				for c := range cuts {
					cs = append(cs, c)
				}
				slices.Sort(cs)
				for i := 0; i+1 < len(cs); i++ {
					ranges = append(ranges, rng(cs[i], cs[i+1]))
				}
			}
			for _, kr := range ranges {
				d := layer.Desc{KeyRange: kr, LsnRange: lsn.Range{Start: s, End: e}, IsDelta: true}
				f := &fake{desc: d, recs: collect(kr, s, e)}
				if err := u.InsertHistoric(Entry{Desc: d, Layer: f}); err != nil {
					return err
				}
			}
		}

		for i, ni := 0, r.Intn(4); i < ni; i++ {
			l := lsn.Lsn(1 + r.Intn(int(n)))
			a := uint32(r.Intn(nKeys))
			b := a + 1 + uint32(r.Intn(nKeys-int(a)))
			d := layer.Desc{KeyRange: rng(a, b), LsnRange: lsn.Range{Start: l, End: l + 1}}
			f := &fake{desc: d, imgs: map[key.Key][]byte{}}
			for k := a; k < b; k++ {
				if s, ok := expect(ops, k, l); ok {
					f.imgs[fk(k)] = []byte(s)
				}
			}
			if err := u.InsertHistoric(Entry{Desc: d, Layer: f}); errors.Is(err, ErrDuplicate) {
				continue
			} else if err != nil {
				return err
			}
		}

		for b := nBands - nMem; b < nBands; b++ {
			s, e := edges[b], edges[b+1]
			d := layer.Desc{KeyRange: key.FullRange(), LsnRange: lsn.Range{Start: s, End: lsn.Max}, IsDelta: true}
			f := &fake{desc: d, recs: collect(key.FullRange(), s, e)}
			if err := u.SetOpen(Entry{Desc: d, InMemory: true, Layer: f}); err != nil {
				return err
			}
			if b < nBands-1 {
				if err := u.FreezeOpen(e); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func readLoop(snap *Snapshot, k key.Key, at lsn.Lsn) (string, bool, error) {
	var st value.ReconstructState
	end := at + 1
	for steps := 0; ; steps++ {
		if steps > 1000 {
			return "", false, errors.New("read loop does not terminate")
		}
		res, ok := snap.Search(k, end)
		if !ok {
			break
		}
		if res.LsnFloor >= end {
			return "", false, fmt.Errorf("floor %s >= end %s from %s", res.LsnFloor, end, res.Entry.Desc)
		}
		err := res.Entry.Layer.GetValueReconstructData(k, lsn.Range{Start: res.LsnFloor, End: end}, &st)
		if err != nil {
			return "", false, err
		}
		if st.IsComplete() {
			break
		}
		end = res.LsnFloor
	}
	if st.IsEmpty() || st.IsAbsent() {
		return "", false, nil
	}
	if !st.IsComplete() {
		return "", false, errors.New("records found but no base")
	}
	page, err := value.ApplyTest(st.Finish())
	return string(page), true, err
}

func dump(snap *Snapshot) string {
	var b strings.Builder
	if o, ok := snap.Open(); ok {
		fmt.Fprintf(&b, "  open    %s\n", o.Desc.LsnRange)
	}
	for _, f := range snap.Frozen() {
		fmt.Fprintf(&b, "  frozen  %s\n", f.Desc.LsnRange)
	}
	hs := snap.Historic()
	sort.SliceStable(hs, func(i, j int) bool { return compareEntries(hs[i], hs[j]) < 0 })
	for _, h := range hs {
		kind := "image"
		if h.Desc.IsDelta {
			kind = "delta"
		}
		fmt.Fprintf(&b, "  %s  keys %d..%d lsn %s\n", kind, h.Desc.KeyRange.Start.Field6, h.Desc.KeyRange.End.Field6, h.Desc.LsnRange)
	}
	return b.String()
}

func TestReadLoopMatchesHistory(t *testing.T) {
	const nKeys, n = 6, 40
	for round := 0; round < 300; round++ {
		r := rand.New(rand.NewSource(int64(round)))
		ops := genHistory(r, nKeys, n)
		lm := randomLayout(t, r, ops, nKeys, n)
		snap := lm.Snapshot()
		for k := uint32(0); k < nKeys; k++ {
			for at := lsn.Lsn(0); at <= n+3; at++ {
				got, ok, err := readLoop(snap, fk(k), at)
				want, wantOK := expect(ops, k, at)
				if err != nil || ok != wantOK || got != want {
					t.Fatalf("round %d: key %d @ %d = %q (exists=%v, err=%v), want %q (exists=%v)\nlayers:\n%s",
						round, k, at, got, ok, err, want, wantOK, dump(snap))
				}
			}
		}
	}
}
