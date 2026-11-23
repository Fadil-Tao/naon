package tenant

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Fadil-Tao/naon/internal/pageserver/ids"
	"github.com/Fadil-Tao/naon/internal/pageserver/key"
	"github.com/Fadil-Tao/naon/internal/pageserver/layer/inmemory"
	"github.com/Fadil-Tao/naon/internal/pageserver/lsn"
	"github.com/Fadil-Tao/naon/internal/pageserver/timeline"
	"github.com/Fadil-Tao/naon/internal/pageserver/value"
)

func pg(n uint32) key.Key { return key.Key{Field4: 16400, Field6: n} }

type opts struct {
	timeline.Config
	timeline.Plugins
}

func testCfg() opts {
	c := timeline.DefaultConfig()
	c.FlushRetryDelay = time.Millisecond
	return opts{Config: c, Plugins: timeline.Plugins{Redo: timeline.TestRedo}}
}

func ctx5(t testing.TB) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

type env struct {
	dir    string
	tenant ids.TenantID
}

func newEnv(t *testing.T) env {
	return env{dir: filepath.Join(t.TempDir(), "tenant"), tenant: ids.NewTenantID()}
}

func (e env) open(t testing.TB, cfg opts) *Tenant {
	t.Helper()
	tn, err := Open(e.dir, e.tenant, cfg.Config, cfg.Plugins)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tn.Stop)
	return tn
}

func newTenant(t *testing.T) (*Tenant, env) {
	e := newEnv(t)
	return e.open(t, testCfg()), e
}

func initP(k uint32, l lsn.Lsn, text string) inmemory.Put {
	return inmemory.Put{Key: pg(k), Lsn: l, Value: value.Record(value.TestInit(text))}
}

func appP(k uint32, l lsn.Lsn, text string) inmemory.Put {
	return inmemory.Put{Key: pg(k), Lsn: l, Value: value.Record(value.TestAppend(text))}
}

func write(t testing.TB, tl *timeline.Timeline, l lsn.Lsn, puts ...inmemory.Put) {
	t.Helper()
	if err := tl.PutBatch(puts, l); err != nil {
		t.Fatalf("PutBatch @ %s: %v", l, err)
	}
}

func get(t testing.TB, tl *timeline.Timeline, k uint32, at lsn.Lsn) (string, error) {
	t.Helper()
	p, err := tl.Get(ctx5(t), pg(k), at)
	return string(p), err
}

func mustGet(t testing.TB, tl *timeline.Timeline, k uint32, at lsn.Lsn, want string) {
	t.Helper()
	if got, err := get(t, tl, k, at); err != nil || got != want {
		t.Fatalf("key %d @ %s = %q, %v; want %q", k, at, got, err, want)
	}
}

func mustMissing(t testing.TB, tl *timeline.Timeline, k uint32, at lsn.Lsn) {
	t.Helper()
	if got, err := get(t, tl, k, at); !errors.Is(err, timeline.ErrNotFound) {
		t.Fatalf("key %d @ %s = %q, %v; want ErrNotFound", k, at, got, err)
	}
}

func root(t testing.TB, tn *Tenant) *timeline.Timeline {
	t.Helper()
	tl, err := tn.CreateRoot(ids.NewTimelineID(), 0, 17)
	if err != nil {
		t.Fatal(err)
	}
	return tl
}

func branch(t testing.TB, tn *Tenant, parent *timeline.Timeline, at lsn.Lsn) *timeline.Timeline {
	t.Helper()
	tl, err := tn.Branch(ctx5(t), ids.NewTimelineID(), parent.ID(), at)
	if err != nil {
		t.Fatal(err)
	}
	return tl
}

func TestBranchSeesParentUpToBranchPoint(t *testing.T) {
	tn, _ := newTenant(t)
	main := root(t, tn)
	write(t, main, 10, initP(1, 10, "a"))
	write(t, main, 20, appP(1, 20, "b"))
	write(t, main, 30, appP(1, 30, "c"))

	child := branch(t, tn, main, 20)
	if child.LastRecordLsn() != 20 || child.AncestorLsn() != 20 || child.Ancestor() != main {
		t.Fatalf("child: last=%s anc=%s", child.LastRecordLsn(), child.AncestorLsn())
	}
	mustGet(t, child, 1, 20, "ab")
	if _, err := get(t, child, 1, 19); !errors.Is(err, timeline.ErrBelowGCCutoff) {
		t.Fatalf("below the branch point: %v", err)
	}

	write(t, child, 25, appP(1, 25, "X"))
	mustGet(t, child, 1, 25, "abX")

	write(t, main, 40, appP(1, 40, "P"))
	write(t, child, 40, appP(1, 40, "Z"))
	mustGet(t, child, 1, 40, "abXZ")
	mustGet(t, main, 1, 40, "abcP")
	mustGet(t, main, 1, 30, "abc")
	mustGet(t, child, 1, 30, "abX")
}

func TestKeysCreatedAfterBranchStayOnTheirSide(t *testing.T) {
	tn, _ := newTenant(t)
	main := root(t, tn)
	write(t, main, 10, initP(1, 10, "a"))
	child := branch(t, tn, main, 10)

	write(t, main, 20, initP(2, 20, "parent-only"))
	write(t, child, 20, initP(3, 20, "child-only"))

	mustMissing(t, child, 2, 20)
	mustGet(t, child, 3, 20, "child-only")
	mustMissing(t, main, 3, 20)
	mustGet(t, main, 2, 20, "parent-only")
	mustGet(t, child, 1, 20, "a")
}

func TestBranchOfBranch(t *testing.T) {
	tn, _ := newTenant(t)
	main := root(t, tn)
	write(t, main, 10, initP(1, 10, "a"))
	write(t, main, 20, appP(1, 20, "b"))

	a := branch(t, tn, main, 20)
	write(t, a, 25, appP(1, 25, "A"))
	write(t, a, 35, appP(1, 35, "a2"))

	b := branch(t, tn, a, 25)
	write(t, b, 30, appP(1, 30, "B"))
	write(t, main, 30, appP(1, 30, "M"))

	mustGet(t, b, 1, 25, "abA")
	mustGet(t, b, 1, 30, "abAB")
	mustGet(t, a, 1, 35, "abAa2")
	mustGet(t, main, 1, 30, "abM")
	if _, err := get(t, b, 1, 24); !errors.Is(err, timeline.ErrBelowGCCutoff) {
		t.Fatalf("below b's branch point: %v", err)
	}
}

func TestBranchReadsThroughImagesAndFlushedLayers(t *testing.T) {
	tn, _ := newTenant(t)
	main := root(t, tn)
	write(t, main, 10, inmemory.Put{Key: pg(1), Lsn: 10, Value: value.Image([]byte("IMG"))})
	write(t, main, 20, appP(1, 20, "+"))
	if err := main.FlushAndWait(ctx5(t)); err != nil {
		t.Fatal(err)
	}
	child := branch(t, tn, main, 20)
	write(t, child, 30, appP(1, 30, "!"))
	if err := child.FlushAndWait(ctx5(t)); err != nil {
		t.Fatal(err)
	}
	mustGet(t, child, 1, 20, "IMG+")
	mustGet(t, child, 1, 30, "IMG+!")
}

func TestBranchRules(t *testing.T) {
	tn, _ := newTenant(t)
	rootID := ids.NewTimelineID()
	main, err := tn.CreateRoot(rootID, 100, 17)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tn.CreateRoot(ids.NewTimelineID(), 0, 17); !errors.Is(err, ErrRootExists) {
		t.Fatalf("second root: %v", err)
	}
	if _, err := tn.CreateRoot(rootID, 0, 17); !errors.Is(err, ErrExists) {
		t.Fatalf("same id: %v", err)
	}
	write(t, main, 110, initP(1, 110, "a"))

	if _, err := tn.Branch(ctx5(t), ids.NewTimelineID(), ids.NewTimelineID(), 110); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown parent: %v", err)
	}
	if _, err := tn.Branch(ctx5(t), ids.NewTimelineID(), rootID, 50); !errors.Is(err, ErrBranchPoint) {
		t.Fatalf("below the start of history: %v", err)
	}
	c1 := branch(t, tn, main, 110)
	if _, err := tn.Branch(ctx5(t), c1.ID(), rootID, 110); !errors.Is(err, ErrExists) {
		t.Fatalf("existing id: %v", err)
	}
	if _, err := tn.Branch(ctx5(t), ids.NewTimelineID(), c1.ID(), 105); !errors.Is(err, ErrBranchPoint) {
		t.Fatalf("below the child's own branch point: %v", err)
	}
	if _, err := tn.Branch(ctx5(t), ids.NewTimelineID(), c1.ID(), 110); err != nil {
		t.Fatalf("at the child's branch point: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err := tn.Branch(ctx, ids.NewTimelineID(), rootID, 500); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("future branch point: %v", err)
	}
	if got := len(tn.TimelineIDs()); got != 3 {
		t.Fatalf("%d timelines, want 3", got)
	}
}

func TestBranchWaitsForTheParent(t *testing.T) {
	tn, _ := newTenant(t)
	main := root(t, tn)
	write(t, main, 10, initP(1, 10, "a"))
	type res struct {
		tl  *timeline.Timeline
		err error
	}
	done := make(chan res, 1)
	go func() {
		tl, err := tn.Branch(ctx5(t), ids.NewTimelineID(), main.ID(), 20)
		done <- res{tl, err}
	}()
	select {
	case <-done:
		t.Fatal("branched before the parent reached the LSN")
	case <-time.After(30 * time.Millisecond):
	}
	write(t, main, 20, appP(1, 20, "b"))
	r := <-done
	if r.err != nil {
		t.Fatal(r.err)
	}
	mustGet(t, r.tl, 1, 20, "ab")
}

func TestBranchFlushesTheParent(t *testing.T) {
	tn, _ := newTenant(t)
	main := root(t, tn)
	write(t, main, 10, initP(1, 10, "a"))
	write(t, main, 20, appP(1, 20, "b"))
	if main.DiskConsistentLsn() != 0 {
		t.Fatal("setup: parent already flushed")
	}
	branch(t, tn, main, 15)
	if main.DiskConsistentLsn() < 15 {
		t.Fatalf("parent disk_consistent_lsn = %s after branching at 15", main.DiskConsistentLsn())
	}
}

func TestRetainLsns(t *testing.T) {
	tn, e := newTenant(t)
	main := root(t, tn)
	write(t, main, 10, initP(1, 10, "a"))
	write(t, main, 20, appP(1, 20, "b"))
	branch(t, tn, main, 10)
	branch(t, tn, main, 10)
	branch(t, tn, main, 20)
	if got := main.RetainLsns(); !slices.Equal(got, []lsn.Lsn{10, 20}) {
		t.Fatalf("retain = %v", got)
	}
	if _, err := tn.Branch(ctx5(t), ids.NewTimelineID(), ids.NewTimelineID(), 10); err == nil {
		t.Fatal("branch from unknown parent succeeded")
	}
	if got := main.RetainLsns(); !slices.Equal(got, []lsn.Lsn{10, 20}) {
		t.Fatalf("retain after a failed branch = %v", got)
	}
	main.RemoveRetainLsn(10)
	if got := main.RetainLsns(); !slices.Equal(got, []lsn.Lsn{10, 20}) {
		t.Fatalf("one of two pins removed: %v", got)
	}
	main.RemoveRetainLsn(10)
	main.RemoveRetainLsn(10)
	if got := main.RetainLsns(); !slices.Equal(got, []lsn.Lsn{20}) {
		t.Fatalf("after removing all pins at 10: %v", got)
	}
	main.AddRetainLsn(10)

	if err := tn.Shutdown(ctx5(t)); err != nil {
		t.Fatal(err)
	}
	tn2 := e.open(t, testCfg())
	m2, _ := tn2.Timeline(main.ID())
	if got := m2.RetainLsns(); !slices.Equal(got, []lsn.Lsn{10, 20}) {
		t.Fatalf("retain after restart = %v", got)
	}
}

func TestRestartLoadsParentsFirst(t *testing.T) {
	tn, e := newTenant(t)
	main := root(t, tn)
	write(t, main, 10, initP(1, 10, "a"))
	a := branch(t, tn, main, 10)
	write(t, a, 20, appP(1, 20, "A"))
	b := branch(t, tn, a, 20)
	write(t, b, 30, appP(1, 30, "B"))
	c := branch(t, tn, main, 10)
	write(t, c, 15, appP(1, 15, "C"))
	if err := tn.Shutdown(ctx5(t)); err != nil {
		t.Fatal(err)
	}

	tn2 := e.open(t, testCfg())
	if len(tn2.TimelineIDs()) != 4 {
		t.Fatalf("%d timelines after restart", len(tn2.TimelineIDs()))
	}
	pos := map[ids.TimelineID]int{}
	for i, id := range tn2.TimelineIDs() {
		pos[id] = i
	}
	if !(pos[main.ID()] < pos[a.ID()] && pos[a.ID()] < pos[b.ID()] && pos[main.ID()] < pos[c.ID()]) {
		t.Fatalf("load order %v", pos)
	}
	get2 := func(id ids.TimelineID) *timeline.Timeline {
		tl, ok := tn2.Timeline(id)
		if !ok {
			t.Fatalf("timeline %s missing", id)
		}
		return tl
	}
	mustGet(t, get2(b.ID()), 1, 30, "aAB")
	mustGet(t, get2(a.ID()), 1, 20, "aA")
	mustGet(t, get2(c.ID()), 1, 15, "aC")
	mustGet(t, get2(main.ID()), 1, 10, "a")
	if get2(b.ID()).Ancestor() != get2(a.ID()) {
		t.Fatal("ancestor pointer not restored")
	}
}

func TestCrashRestartKeepsBranchesConsistent(t *testing.T) {
	tn, e := newTenant(t)
	main := root(t, tn)
	write(t, main, 10, initP(1, 10, "a"))
	write(t, main, 20, appP(1, 20, "b"))
	child := branch(t, tn, main, 20)
	write(t, main, 30, appP(1, 30, "c"))
	write(t, child, 30, appP(1, 30, "X"))
	tn.Stop()

	tn2 := e.open(t, testCfg())
	m2, _ := tn2.Timeline(main.ID())
	c2, _ := tn2.Timeline(child.ID())
	if m2.LastRecordLsn() != 20 || c2.LastRecordLsn() != 20 {
		t.Fatalf("recovered at %s / %s, want 20 / 20", m2.LastRecordLsn(), c2.LastRecordLsn())
	}
	mustGet(t, c2, 1, 20, "ab")
	write(t, m2, 30, appP(1, 30, "c"))
	write(t, c2, 30, appP(1, 30, "X"))
	mustGet(t, m2, 1, 30, "abc")
	mustGet(t, c2, 1, 30, "abX")
}

func TestOpenCleansAndRejects(t *testing.T) {
	tn, e := newTenant(t)
	main := root(t, tn)
	write(t, main, 10, initP(1, 10, "a"))
	branch(t, tn, main, 10)
	tn.Stop()
	tlDir := filepath.Join(e.dir, "timelines")

	leftover := filepath.Join(tlDir, ids.NewTimelineID().String()+".uninit")
	if err := os.MkdirAll(leftover, 0o755); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(leftover, "junk"), []byte("x"), 0o644)
	os.MkdirAll(filepath.Join(tlDir, "not-a-timeline"), 0o755)
	os.WriteFile(filepath.Join(tlDir, "README"), []byte("x"), 0o644)
	tn2, err := Open(e.dir, e.tenant, testCfg().Config, testCfg().Plugins)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(leftover); !os.IsNotExist(err) {
		t.Fatalf("uninit dir not removed: %v", err)
	}
	if len(tn2.TimelineIDs()) != 2 {
		t.Fatalf("%d timelines", len(tn2.TimelineIDs()))
	}
	tn2.Stop()

	if err := os.RemoveAll(filepath.Join(tlDir, main.ID().String())); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(e.dir, e.tenant, testCfg().Config, testCfg().Plugins); !errors.Is(err, ErrMissingParent) {
		t.Fatalf("missing parent: %v", err)
	}
}

func TestStoppedTenant(t *testing.T) {
	tn, _ := newTenant(t)
	main := root(t, tn)
	write(t, main, 10, initP(1, 10, "a"))
	tn.Stop()
	if _, err := tn.CreateRoot(ids.NewTimelineID(), 0, 17); !errors.Is(err, ErrStopped) {
		t.Fatalf("CreateRoot: %v", err)
	}
	if _, err := tn.Branch(ctx5(t), ids.NewTimelineID(), main.ID(), 10); !errors.Is(err, ErrStopped) && !errors.Is(err, timeline.ErrStopped) {
		t.Fatalf("Branch: %v", err)
	}
	if err := tn.Shutdown(ctx5(t)); err != nil {
		t.Fatalf("Shutdown after Stop: %v", err)
	}
}

func TestShutdownFlushesEveryTimeline(t *testing.T) {
	tn, e := newTenant(t)
	main := root(t, tn)
	write(t, main, 10, initP(1, 10, "a"))
	child := branch(t, tn, main, 10)
	write(t, child, 20, appP(1, 20, "c"))
	write(t, main, 30, appP(1, 30, "m"))
	if err := tn.Shutdown(ctx5(t)); err != nil {
		t.Fatal(err)
	}
	tn2 := e.open(t, testCfg())
	m2, _ := tn2.Timeline(main.ID())
	c2, _ := tn2.Timeline(child.ID())
	mustGet(t, m2, 1, 30, "am")
	mustGet(t, c2, 1, 20, "ac")
}

func TestConcurrentBranchingWhileParentIsWritten(t *testing.T) {
	cfg := testCfg()
	cfg.CheckpointDistance = 12
	e := newEnv(t)
	tn := e.open(t, cfg)
	main := root(t, tn)

	const n = 600
	page := func(upTo lsn.Lsn) string {
		var b strings.Builder
		for l := lsn.Lsn(1); l <= upTo; l++ {
			fmt.Fprintf(&b, "%d,", l)
		}
		return b.String()
	}
	var failed atomic.Bool
	fail := func(format string, args ...any) {
		if failed.CompareAndSwap(false, true) {
			t.Errorf(format, args...)
		}
	}
	var wg sync.WaitGroup
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for at := lsn.Lsn(5 + w*3); at <= n && !failed.Load(); at += 40 {
				child, err := tn.Branch(ctx5(t), ids.NewTimelineID(), main.ID(), at)
				if err != nil {
					fail("branch @ %s: %v", at, err)
					return
				}
				got, err := child.Get(ctx5(t), pg(1), at)
				if err != nil || string(got) != page(at) {
					fail("child @ %s: %v, page ends %q", at, err, string(got))
					return
				}
				next := at + 1
				if err := child.PutBatch([]inmemory.Put{appP(1, next, "child")}, next); err != nil {
					fail("child write: %v", err)
					return
				}
				got, err = child.Get(ctx5(t), pg(1), next)
				if err != nil || string(got) != page(at)+"child" {
					fail("child after write @ %s: %v, page ends %q", next, err, string(got))
					return
				}
			}
		}(w)
	}
	for l := lsn.Lsn(1); l <= n; l++ {
		p := appP(1, l, fmt.Sprintf("%d,", l))
		if l == 1 {
			p = initP(1, l, "1,")
		}
		if err := main.PutBatch([]inmemory.Put{p}, l); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
	got, err := main.Get(ctx5(t), pg(1), n)
	if err != nil || string(got) != page(n) {
		t.Fatalf("parent damaged by its branches: %v", err)
	}
}

func TestChildWithoutLayersIgnoresParentWritesAfterBranchPoint(t *testing.T) {
	tn, _ := newTenant(t)
	main := root(t, tn)
	write(t, main, 10, initP(1, 10, "a"))
	child := branch(t, tn, main, 10)
	write(t, main, 20, appP(1, 20, "b"))

	if err := child.PutBatch(nil, 30); err != nil {
		t.Fatal(err)
	}
	if _, ok := child.Layers().Snapshot().Open(); ok {
		t.Fatal("setup: the child has an open layer")
	}
	mustGet(t, child, 1, 30, "a")
	mustGet(t, main, 1, 20, "ab")
}

func TestChildImageHoldsAncestorPages(t *testing.T) {
	cfg := testCfg()
	cfg.KeySpace = func(context.Context, *timeline.Timeline, lsn.Lsn) (key.KeySpace, error) {
		return key.FromRanges(key.KeyRange{Start: pg(0), End: pg(4)}), nil
	}
	e := newEnv(t)
	tn := e.open(t, cfg)
	main := root(t, tn)
	write(t, main, 10, initP(1, 10, "a"), initP(2, 10, "parent-only"))
	child := branch(t, tn, main, 10)
	for l := lsn.Lsn(20); l <= 40; l += 10 {
		write(t, child, l, appP(1, l, fmt.Sprintf("+%d", l)))
		if err := child.FlushAndWait(ctx5(t)); err != nil {
			t.Fatal(err)
		}
	}
	n, err := child.CreateImageLayers(ctx5(t))
	if err != nil || n != 1 {
		t.Fatalf("child images: %d, %v", n, err)
	}
	res, ok := child.Layers().Snapshot().Search(pg(2), 41)
	if !ok || res.Entry.Desc.IsDelta || res.Entry.Desc.ImageLsn() != 40 {
		t.Fatalf("child read of a parent-only page goes to %v", res.Entry.Desc)
	}
	mustGet(t, child, 2, 40, "parent-only")
	mustGet(t, child, 1, 40, "a+20+30+40")
	mustGet(t, child, 1, 25, "a+20")
	mustMissing(t, child, 3, 40)
	if n, _ := main.CreateImageLayers(ctx5(t)); n != 0 {
		t.Fatalf("parent got %d images from the child's deltas", n)
	}
}
