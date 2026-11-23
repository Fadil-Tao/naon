package timeline

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Fadil-Tao/naon/internal/pageserver/ids"
	"github.com/Fadil-Tao/naon/internal/pageserver/key"
	"github.com/Fadil-Tao/naon/internal/pageserver/layer/inmemory"
	"github.com/Fadil-Tao/naon/internal/pageserver/lsn"
	"github.com/Fadil-Tao/naon/internal/pageserver/value"
)

func pg(n uint32) key.Key { return key.Key{Field4: 16400, Field6: n} }

func testTimeline(t *testing.T, start lsn.Lsn) *Timeline {
	t.Helper()
	tl, err := Create(t.TempDir(), ids.NewTenantID(), ids.NewTimelineID(), RootMetadata(start, 17), DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tl.Stop)
	return tl
}

func put(k key.Key, l lsn.Lsn, v value.Value) inmemory.Put {
	return inmemory.Put{Key: k, Lsn: l, Value: v}
}
func app(s string) value.Value   { return value.Record(value.TestAppend(s)) }
func init_(s string) value.Value { return value.Record(value.TestInit(s)) }

func readAll(t testing.TB, tl *Timeline, k key.Key, at lsn.Lsn) []lsn.Lsn {
	t.Helper()
	snap := tl.Layers().Snapshot()
	var st value.ReconstructState
	end := at + 1
	for {
		res, ok := snap.Search(k, end)
		if !ok {
			break
		}
		if err := res.Entry.Layer.GetValueReconstructData(k, lsn.Range{Start: res.LsnFloor, End: end}, &st); err != nil {
			t.Fatal(err)
		}
		if st.IsComplete() {
			break
		}
		end = res.LsnFloor
	}
	var out []lsn.Lsn
	for _, r := range st.Finish().Records {
		out = append(out, r.Lsn)
	}
	return out
}

func TestPutIsReadableImmediately(t *testing.T) {
	tl := testTimeline(t, 0)
	if err := tl.PutBatch([]inmemory.Put{put(pg(1), 10, init_("a"))}, 10); err != nil {
		t.Fatal(err)
	}
	if got := readAll(t, tl, pg(1), 10); !slices.Equal(got, []lsn.Lsn{10}) {
		t.Fatalf("read = %v", got)
	}
	if tl.LastRecordLsn() != 10 {
		t.Fatalf("last_record_lsn = %s", tl.LastRecordLsn())
	}
	if err := tl.PutBatch([]inmemory.Put{put(pg(1), 20, app("b")), put(pg(2), 20, init_("z"))}, 25); err != nil {
		t.Fatal(err)
	}
	if got := readAll(t, tl, pg(1), 25); !slices.Equal(got, []lsn.Lsn{10, 20}) {
		t.Fatalf("read = %v", got)
	}
	if got := readAll(t, tl, pg(1), 19); !slices.Equal(got, []lsn.Lsn{10}) {
		t.Fatalf("time travel read = %v", got)
	}
	if tl.LastRecordLsn() != 25 || tl.PrevRecordLsn() != 10 {
		t.Fatalf("last=%s prev=%s", tl.LastRecordLsn(), tl.PrevRecordLsn())
	}

	snap := tl.Layers().Snapshot()
	o, ok := snap.Open()
	if !ok || o.Desc.LsnRange.Start != 1 || len(snap.Frozen()) != 0 || len(snap.Historic()) != 0 {
		t.Fatalf("layers: open=%v frozen=%d historic=%d", o.Desc, len(snap.Frozen()), len(snap.Historic()))
	}
}

func TestOpenLayerStartsAtTimelineStart(t *testing.T) {
	tl := testTimeline(t, 0x1000)
	if _, ok := tl.Layers().Snapshot().Open(); ok {
		t.Fatal("open layer before any write")
	}
	if err := tl.PutBatch([]inmemory.Put{put(pg(1), 0x1010, init_("a"))}, 0x1010); err != nil {
		t.Fatal(err)
	}
	o, _ := tl.Layers().Snapshot().Open()
	if o.Desc.LsnRange.Start != 0x1001 {
		t.Fatalf("open layer starts at %s", o.Desc.LsnRange.Start)
	}
}

func TestRejectedWritesChangeNothing(t *testing.T) {
	tl := testTimeline(t, 0)
	if err := tl.PutBatch([]inmemory.Put{put(pg(1), 100, init_("a"))}, 100); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		puts []inmemory.Put
		end  lsn.Lsn
		want error
	}{
		{"put at last_record_lsn", []inmemory.Put{put(pg(2), 100, init_("b"))}, 110, ErrLsnBackwards},
		{"put below last_record_lsn", []inmemory.Put{put(pg(2), 50, init_("b"))}, 110, ErrLsnBackwards},
		{"valid put then a backwards one", []inmemory.Put{put(pg(2), 120, init_("b")), put(pg(3), 90, init_("c"))}, 130, ErrLsnBackwards},
		{"end below last_record_lsn", nil, 99, ErrLsnBackwards},
		{"end below a put", []inmemory.Put{put(pg(2), 120, init_("b"))}, 119, nil},
		{"same key twice at one LSN", []inmemory.Put{put(pg(2), 120, init_("b")), put(pg(2), 120, app("c"))}, 130, inmemory.ErrOrder},
		{"same key going down", []inmemory.Put{put(pg(2), 125, init_("b")), put(pg(2), 120, app("c"))}, 130, inmemory.ErrOrder},
	}
	for _, c := range cases {
		err := tl.PutBatch(c.puts, c.end)
		if err == nil || (c.want != nil && !errors.Is(err, c.want)) {
			t.Fatalf("%s: err = %v, want %v", c.name, err, c.want)
		}
		if tl.LastRecordLsn() != 100 || tl.PrevRecordLsn() != 0 {
			t.Fatalf("%s: last=%s prev=%s", c.name, tl.LastRecordLsn(), tl.PrevRecordLsn())
		}
		for _, k := range []key.Key{pg(2), pg(3)} {
			if got := readAll(t, tl, k, 1000); len(got) != 0 {
				t.Fatalf("%s: %s has data %v", c.name, k, got)
			}
		}
	}

	if err := tl.PutBatch([]inmemory.Put{put(pg(2), 120, init_("b"))}, 120); err != nil {
		t.Fatal(err)
	}
}

func TestSameLsnForDifferentKeys(t *testing.T) {
	tl := testTimeline(t, 0)
	if err := tl.PutBatch([]inmemory.Put{put(pg(1), 10, init_("a")), put(pg(2), 10, init_("b")), put(pg(3), 10, init_("c"))}, 10); err != nil {
		t.Fatal(err)
	}
	for _, k := range []key.Key{pg(1), pg(2), pg(3)} {
		if got := readAll(t, tl, k, 10); !slices.Equal(got, []lsn.Lsn{10}) {
			t.Fatalf("%s: %v", k, got)
		}
	}
}

func TestEmptyBatchAdvancesLsn(t *testing.T) {
	tl := testTimeline(t, 5)
	if err := tl.PutBatch(nil, 5); err != nil {
		t.Fatal(err)
	}
	if err := tl.PutBatch(nil, 50); err != nil {
		t.Fatal(err)
	}
	if tl.LastRecordLsn() != 50 || tl.PrevRecordLsn() != 5 {
		t.Fatalf("last=%s prev=%s", tl.LastRecordLsn(), tl.PrevRecordLsn())
	}
	if _, ok := tl.Layers().Snapshot().Open(); ok {
		t.Fatal("empty batch created a layer")
	}
}

func TestWaitLsnWakesAfterDataIsVisible(t *testing.T) {
	tl := testTimeline(t, 4900)
	if err := tl.PutBatch([]inmemory.Put{put(pg(1), 4990, init_("a"))}, 4990); err != nil {
		t.Fatal(err)
	}

	type result struct {
		err  error
		lsns []lsn.Lsn
	}
	done := make(chan result, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := tl.WaitLsn(ctx, 5000)
		done <- result{err, readAll(t, tl, pg(1), 5000)}
	}()

	select {
	case <-done:
		t.Fatal("woke before the LSN was reached")
	case <-time.After(30 * time.Millisecond):
	}
	if err := tl.PutBatch([]inmemory.Put{put(pg(1), 4995, app("b"))}, 4995); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
		t.Fatal("woke at 4995")
	case <-time.After(30 * time.Millisecond):
	}
	if err := tl.PutBatch([]inmemory.Put{put(pg(1), 5000, app("c"))}, 5000); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-done:
		if r.err != nil || !slices.Equal(r.lsns, []lsn.Lsn{4990, 4995, 5000}) {
			t.Fatalf("woken reader saw %v, %v", r.lsns, r.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter did not wake")
	}
}

func TestWaitLsnTimeout(t *testing.T) {
	tl := testTimeline(t, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := tl.WaitLsn(ctx, 100); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
}

func TestStopTimeline(t *testing.T) {
	tl := testTimeline(t, 0)
	errc := make(chan error, 1)
	go func() { errc <- tl.WaitLsn(context.Background(), 100) }()
	time.Sleep(20 * time.Millisecond)
	tl.Stop()
	select {
	case err := <-errc:
		if !errors.Is(err, ErrStopped) {
			t.Fatalf("waiter: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter not woken by Stop")
	}
	if err := tl.PutBatch([]inmemory.Put{put(pg(1), 1, init_("a"))}, 1); !errors.Is(err, ErrStopped) {
		t.Fatalf("write after stop = %v", err)
	}
	tl.Stop()
}

func TestConcurrentWriterReadersWaiters(t *testing.T) {
	tl := testTimeline(t, 0)
	const n = 1500
	var wg sync.WaitGroup
	var failed atomic.Bool

	for r := 0; r < 6; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			for target := lsn.Lsn(r + 1); target <= n && !failed.Load(); target += 7 {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				err := tl.WaitLsn(ctx, target)
				cancel()
				if err != nil {
					failed.Store(true)
					t.Errorf("wait %s: %v", target, err)
					return
				}
				got := readAll(t, tl, pg(1), target)
				if len(got) == 0 || got[len(got)-1] < target {
					failed.Store(true)
					t.Errorf("woken for %s but newest version is %v", target, got)
					return
				}
			}
		}(r)
	}
	for l := lsn.Lsn(1); l <= n; l++ {
		v := app("x")
		if l == 1 {
			v = init_("a")
		}
		if err := tl.PutBatch([]inmemory.Put{put(pg(1), l, v)}, l); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
}
