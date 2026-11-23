package timeline

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
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
	"github.com/Fadil-Tao/naon/internal/pageserver/layer"
	"github.com/Fadil-Tao/naon/internal/pageserver/layer/inmemory"
	"github.com/Fadil-Tao/naon/internal/pageserver/lsn"
	"github.com/Fadil-Tao/naon/internal/pageserver/value"
)

type env struct {
	dir    string
	tenant ids.TenantID
	id     ids.TimelineID
}

func newEnv(t *testing.T) env {
	return env{dir: filepath.Join(t.TempDir(), "tl"), tenant: ids.NewTenantID(), id: ids.NewTimelineID()}
}

type opts struct {
	Config
	Plugins
}

func testCfg() opts {
	c := DefaultConfig()
	c.FlushRetryDelay = time.Millisecond
	return opts{Config: c}
}

func (e env) create(t *testing.T, cfg opts) *Timeline {
	t.Helper()
	tl, err := Create(e.dir, e.tenant, e.id, RootMetadata(0, 17), cfg.Config, cfg.Plugins)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tl.Stop)
	return tl
}

func (e env) load(t *testing.T, cfg opts) (*Timeline, error) {
	t.Helper()
	tl, err := Load(e.dir, e.tenant, e.id, nil, cfg.Config, cfg.Plugins)
	if err == nil {
		t.Cleanup(tl.Stop)
	}
	return tl, err
}

func ctx5(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func flush(t *testing.T, tl *Timeline) {
	t.Helper()
	if err := tl.FlushAndWait(ctx5(t)); err != nil {
		t.Fatalf("FlushAndWait: %v (background: %v)", err, tl.BackgroundErr())
	}
}

type op struct {
	k    uint32
	lsn  lsn.Lsn
	init bool
	text string
	img  bool
}

func (o op) put() inmemory.Put {
	switch {
	case o.img:
		return put(pg(o.k), o.lsn, value.Image([]byte(o.text)))
	case o.init:
		return put(pg(o.k), o.lsn, init_(o.text))
	}
	return put(pg(o.k), o.lsn, app(o.text))
}

func genOps(r *rand.Rand, seen map[uint32]bool, after lsn.Lsn, n, nKeys int) []op {
	var ops []op
	cur := after
	for i := 0; i < n; i++ {
		cur += lsn.Lsn(1 + r.Intn(3))
		for _, k := range r.Perm(nKeys)[:1+r.Intn(2)] {
			init := !seen[uint32(k)] || r.Intn(8) == 0
			seen[uint32(k)] = true
			ops = append(ops, op{k: uint32(k), lsn: cur, init: init, text: fmt.Sprintf("%d,", cur)})
		}
	}
	return ops
}

func ingest(t *testing.T, tl *Timeline, ops []op) {
	t.Helper()
	for i := 0; i < len(ops); {
		j := i
		var batch []inmemory.Put
		for j < len(ops) && ops[j].lsn == ops[i].lsn {
			batch = append(batch, ops[j].put())
			j++
		}
		if err := tl.PutBatch(batch, ops[i].lsn); err != nil {
			t.Fatalf("PutBatch @ %s: %v", ops[i].lsn, err)
		}
		i = j
	}
}

func lastLsn(ops []op) lsn.Lsn { return ops[len(ops)-1].lsn }

func expect(ops []op, k uint32, at lsn.Lsn) (string, bool) {
	var s string
	ok := false
	for _, o := range ops {
		if o.k != k || o.lsn > at {
			continue
		}
		if o.init || o.img {
			s = o.text
		} else {
			s += o.text
		}
		ok = true
	}
	return s, ok
}

func readPage(tl *Timeline, k key.Key, at lsn.Lsn) (string, bool, error) {
	snap := tl.Layers().Snapshot()
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
		rng := lsn.Range{Start: res.LsnFloor, End: end}
		if err := res.Entry.Layer.GetValueReconstructData(k, rng, &st); err != nil {
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
		return "", false, errors.New("records without a base")
	}
	p, err := value.ApplyTest(st.Finish())
	return string(p), true, err
}

func checkAgainstModel(t *testing.T, tl *Timeline, ops []op, nKeys int) {
	t.Helper()
	for k := uint32(0); k < uint32(nKeys); k++ {
		for at := lsn.Lsn(0); at <= lastLsn(ops)+2; at++ {
			got, ok, err := readPage(tl, pg(k), at)
			want, wantOK := expect(ops, k, at)
			if err != nil || ok != wantOK || got != want {
				t.Fatalf("key %d @ %s = %q (exists=%v, err=%v), want %q (exists=%v)", k, at, got, ok, err, want, wantOK)
			}
		}
	}
}

func layerDescs(tl *Timeline) (frozen, historic []layer.Desc) {
	snap := tl.Layers().Snapshot()
	for _, e := range snap.Frozen() {
		frozen = append(frozen, e.Desc)
	}
	for _, e := range snap.Historic() {
		historic = append(historic, e.Desc)
	}
	return
}

func filesWithSuffix(t *testing.T, dir, suffix string) []string {
	t.Helper()
	es, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range es {
		if strings.HasSuffix(e.Name(), suffix) {
			out = append(out, e.Name())
		}
	}
	return out
}

func layerFileCount(t *testing.T, dir string) int {
	t.Helper()
	es, _ := os.ReadDir(dir)
	n := 0
	for _, e := range es {
		if _, err := layer.ParseFileName(e.Name()); err == nil {
			n++
		}
	}
	return n
}

func TestFreezeAndFlush(t *testing.T) {
	const nKeys = 5
	r := rand.New(rand.NewSource(1))
	ops := genOps(r, map[uint32]bool{}, 0, 40, nKeys)
	tl := newEnv(t).create(t, testCfg())

	ingest(t, tl, ops)
	checkAgainstModel(t, tl, ops, nKeys)
	flush(t, tl)

	snap := tl.Layers().Snapshot()
	if _, ok := snap.Open(); ok || len(snap.Frozen()) != 0 || len(snap.Historic()) != 1 {
		t.Fatalf("after flush: open=%v frozen=%d historic=%d", ok, len(snap.Frozen()), len(snap.Historic()))
	}
	d := snap.Historic()[0].Desc
	end := lastLsn(ops) + 1
	if !d.IsL0() || d.LsnRange != (lsn.Range{Start: 1, End: end}) {
		t.Fatalf("L0 = %s, want all keys, LSN [1, %s)", d, end)
	}
	if tl.DiskConsistentLsn() != lastLsn(ops) {
		t.Fatalf("disk_consistent_lsn = %s, want %s (end - 1)", tl.DiskConsistentLsn(), lastLsn(ops))
	}
	m, err := LoadMetadata(tl.metadataPath())
	if err != nil || m != tl.Metadata() || m.DiskConsistentLsn != lastLsn(ops) {
		t.Fatalf("metadata file = %+v (%v), in memory %+v", m, err, tl.Metadata())
	}
	if m.PrevRecordLsn == 0 || m.PrevRecordLsn != tl.PrevRecordLsn() {
		t.Fatalf("prev_record_lsn in metadata = %s, timeline has %s", m.PrevRecordLsn, tl.PrevRecordLsn())
	}

	if n := len(filesWithSuffix(t, tl.dir, "")); n != 2 {
		t.Fatalf("directory has %d files, want layer + metadata", n)
	}
	checkAgainstModel(t, tl, ops, nKeys)
}

func TestFreezeWithoutDataDoesNothing(t *testing.T) {
	tl := newEnv(t).create(t, testCfg())
	if ok, err := tl.Freeze(); ok || err != nil {
		t.Fatalf("Freeze on empty timeline = %v, %v", ok, err)
	}

	if err := tl.PutBatch(nil, 50); err != nil {
		t.Fatal(err)
	}
	if ok, err := tl.Freeze(); ok || err != nil {
		t.Fatalf("Freeze after an empty batch = %v, %v", ok, err)
	}
	if err := tl.FlushAndWait(ctx5(t)); err != nil {
		t.Fatal(err)
	}
	if tl.DiskConsistentLsn() != 0 {
		t.Fatalf("disk_consistent_lsn = %s", tl.DiskConsistentLsn())
	}
}

func TestReadsAndWritesDuringFlush(t *testing.T) {
	const nKeys = 5
	r := rand.New(rand.NewSource(2))
	seen := map[uint32]bool{}
	ops1 := genOps(r, seen, 0, 30, nKeys)
	ops2 := genOps(r, seen, lastLsn(ops1), 30, nKeys)

	hit, release := make(chan struct{}, 1), make(chan struct{})
	var once sync.Once
	cfg := testCfg()
	cfg.failpoint = func(name string) error {
		if name == fpAfterL0Written {
			select {
			case hit <- struct{}{}:
				<-release
			default:
			}
		}
		return nil
	}
	tl := newEnv(t).create(t, cfg)
	t.Cleanup(func() { once.Do(func() { close(release) }) })

	ingest(t, tl, ops1)
	if ok, err := tl.Freeze(); !ok || err != nil {
		t.Fatalf("Freeze = %v, %v", ok, err)
	}
	<-hit
	frozen, historic := layerDescs(tl)
	if len(frozen) != 1 || len(historic) != 0 {
		t.Fatalf("mid-flush: frozen=%d historic=%d", len(frozen), len(historic))
	}
	checkAgainstModel(t, tl, ops1, nKeys)

	ingest(t, tl, ops2)
	o, ok := tl.Layers().Snapshot().Open()
	if !ok || o.Desc.LsnRange.Start != lastLsn(ops1)+1 {
		t.Fatalf("new open layer = %v %v", o.Desc, ok)
	}
	all := append(slices.Clone(ops1), ops2...)
	checkAgainstModel(t, tl, all, nKeys)

	once.Do(func() { close(release) })
	flush(t, tl)
	frozen, historic = layerDescs(tl)
	if len(frozen) != 0 || len(historic) != 2 {
		t.Fatalf("after flush: frozen=%d historic=%d", len(frozen), len(historic))
	}
	if historic[0].LsnRange.End != historic[1].LsnRange.Start {
		t.Fatalf("L0 files are not contiguous: %s, %s", historic[0], historic[1])
	}
	checkAgainstModel(t, tl, all, nKeys)
}

func TestFlushAndWaitEdges(t *testing.T) {
	tl := newEnv(t).create(t, testCfg())
	tl.Stop()
	if err := tl.FlushAndWait(ctx5(t)); !errors.Is(err, ErrStopped) {
		t.Fatalf("on stopped timeline: %v", err)
	}

	hit, release := make(chan struct{}, 1), make(chan struct{})
	var once sync.Once
	cfg := testCfg()
	cfg.failpoint = func(name string) error {
		if name == fpFlushStart {
			select {
			case hit <- struct{}{}:
				<-release
			default:
			}
		}
		return nil
	}
	tl = newEnv(t).create(t, cfg)
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	ingest(t, tl, []op{{k: 0, lsn: 5, init: true, text: "a"}})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := tl.FlushAndWait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked flush: %v", err)
	}
	once.Do(func() { close(release) })
	flush(t, tl)
}

func TestFreezeByDistance(t *testing.T) {
	cfg := testCfg()
	cfg.CheckpointDistance = 100
	tl := newEnv(t).create(t, cfg)

	ingest(t, tl, []op{{k: 0, lsn: 10, init: true, text: "a"}, {k: 0, lsn: 50, text: "b"}})
	if _, ok := tl.Layers().Snapshot().Open(); !ok || tl.DiskConsistentLsn() != 0 {
		t.Fatal("froze before the distance was reached")
	}
	ingest(t, tl, []op{{k: 0, lsn: 101, text: "c"}})
	if err := tl.flushed.Wait(ctx5(t), 101); err != nil {
		t.Fatalf("no automatic flush: %v (background %v)", err, tl.BackgroundErr())
	}
	if _, ok := tl.Layers().Snapshot().Open(); ok {
		t.Fatal("open layer still there after the freeze")
	}

	ingest(t, tl, []op{{k: 0, lsn: 120, text: "d"}})
	if o, _ := tl.Layers().Snapshot().Open(); o.Desc.LsnRange.Start != 102 {
		t.Fatalf("next open layer starts at %s, want 102", o.Desc.LsnRange.Start)
	}
}

func TestFreezeBySize(t *testing.T) {
	cfg := testCfg()
	cfg.MaxOpenLayerBytes = 200
	tl := newEnv(t).create(t, cfg)
	for i := lsn.Lsn(1); i <= 200; i++ {
		before := uint64(0)
		if o, ok := tl.Layers().Snapshot().Open(); ok {
			before = o.Layer.(*inmemory.Layer).Size()
		}
		ingest(t, tl, []op{{k: 0, lsn: i, init: i == 1, text: "xxxxxxxx"}})
		if _, ok := tl.Layers().Snapshot().Open(); !ok {
			if before >= 200 {
				t.Fatalf("layer was already %d bytes before this write", before)
			}
			if err := tl.flushed.Wait(ctx5(t), i); err != nil {
				t.Fatal(err)
			}
			return
		}
	}
	t.Fatal("never froze")
}

func TestFreezeByTimeout(t *testing.T) {
	cfg := testCfg()
	cfg.CheckpointTimeout = time.Hour
	tl := newEnv(t).create(t, cfg)
	now := time.Now()

	if ok, err := tl.FreezeIfStale(now.Add(2 * time.Hour)); ok || err != nil {
		t.Fatalf("froze an empty timeline: %v %v", ok, err)
	}
	ingest(t, tl, []op{{k: 0, lsn: 5, init: true, text: "a"}})
	if ok, _ := tl.FreezeIfStale(now.Add(time.Minute)); ok {
		t.Fatal("froze before the timeout")
	}
	if ok, err := tl.FreezeIfStale(now.Add(2 * time.Hour)); !ok || err != nil {
		t.Fatalf("did not freeze after the timeout: %v %v", ok, err)
	}
	if err := tl.flushed.Wait(ctx5(t), 5); err != nil {
		t.Fatal(err)
	}
}

func TestRestartGivesIdenticalReads(t *testing.T) {
	const nKeys = 6
	for round := 0; round < 5; round++ {
		r := rand.New(rand.NewSource(int64(100 + round)))
		e := newEnv(t)
		tl := e.create(t, testCfg())

		seen := map[uint32]bool{}
		var all []op
		for chunk := 0; chunk < 4; chunk++ {
			ops := genOps(r, seen, lastOrZero(all), 20+r.Intn(30), nKeys)
			ingest(t, tl, ops)
			all = append(all, ops...)
			flush(t, tl)
		}
		checkAgainstModel(t, tl, all, nKeys)
		wantDcl := lastLsn(all)
		wantPrev := tl.PrevRecordLsn()
		_, before := layerDescs(tl)
		tl.Stop()

		tl2, err := e.load(t, testCfg())
		if err != nil {
			t.Fatal(err)
		}
		if tl2.LastRecordLsn() != wantDcl || tl2.DiskConsistentLsn() != wantDcl {
			t.Fatalf("after load: last_record=%s disk_consistent=%s, want %s", tl2.LastRecordLsn(), tl2.DiskConsistentLsn(), wantDcl)
		}
		if tl2.PrevRecordLsn() != wantPrev {
			t.Fatalf("prev_record_lsn after load = %s, want %s", tl2.PrevRecordLsn(), wantPrev)
		}
		if _, after := layerDescs(tl2); !slices.Equal(before, after) {
			t.Fatalf("layers changed over restart:\n before %v\n after  %v", before, after)
		}
		checkAgainstModel(t, tl2, all, nKeys)

		if err := tl2.PutBatch([]inmemory.Put{put(pg(0), wantDcl, app("x"))}, wantDcl); !errors.Is(err, ErrLsnBackwards) {
			t.Fatalf("write at disk_consistent_lsn: %v", err)
		}
		more := genOps(r, seen, wantDcl, 10, nKeys)
		ingest(t, tl2, more)
		all = append(all, more...)
		flush(t, tl2)
		checkAgainstModel(t, tl2, all, nKeys)
		tl2.Stop()

		tl3, err := e.load(t, testCfg())
		if err != nil {
			t.Fatal(err)
		}
		checkAgainstModel(t, tl3, all, nKeys)
	}
}

func lastOrZero(ops []op) lsn.Lsn {
	if len(ops) == 0 {
		return 0
	}
	return lastLsn(ops)
}

func TestUnflushedDataIsLostOnStop(t *testing.T) {
	const nKeys = 4
	r := rand.New(rand.NewSource(7))
	seen := map[uint32]bool{}
	ops1 := genOps(r, seen, 0, 20, nKeys)
	ops2 := genOps(r, seen, lastLsn(ops1), 20, nKeys)
	e := newEnv(t)
	tl := e.create(t, testCfg())
	ingest(t, tl, ops1)
	flush(t, tl)
	ingest(t, tl, ops2)
	tl.Stop()

	tl2, err := e.load(t, testCfg())
	if err != nil {
		t.Fatal(err)
	}
	if tl2.LastRecordLsn() != lastLsn(ops1) {
		t.Fatalf("last_record_lsn = %s, want %s", tl2.LastRecordLsn(), lastLsn(ops1))
	}
	checkAgainstModel(t, tl2, ops1, nKeys)

	ingest(t, tl2, ops2)
	checkAgainstModel(t, tl2, append(slices.Clone(ops1), ops2...), nKeys)
}

func TestShutdownFlushes(t *testing.T) {
	const nKeys = 4
	ops := genOps(rand.New(rand.NewSource(3)), map[uint32]bool{}, 0, 25, nKeys)
	e := newEnv(t)
	tl := e.create(t, testCfg())
	ingest(t, tl, ops)
	if err := tl.Shutdown(ctx5(t)); err != nil {
		t.Fatal(err)
	}
	if err := tl.PutBatch(nil, lastLsn(ops)+1); !errors.Is(err, ErrStopped) {
		t.Fatalf("write after shutdown: %v", err)
	}
	tl2, err := e.load(t, testCfg())
	if err != nil {
		t.Fatal(err)
	}
	checkAgainstModel(t, tl2, ops, nKeys)
	if err := tl.Shutdown(ctx5(t)); err != nil {
		t.Fatal(err)
	}
}

func crashAt(point string, n int) func(string) error {
	var hits atomic.Int32
	return func(name string) error {
		if name == point && int(hits.Add(1)) == n {
			return errSimulatedCrash
		}
		return nil
	}
}

func TestCrashDuringSecondFlush(t *testing.T) {
	const nKeys = 5
	cases := []struct {
		point      string
		wantOldL0s int
		survives   bool
	}{
		{fpFlushStart, 0, false},
		{fpAfterL0Written, 1, false},
		{fpAfterLayerSwap, 1, false},
		{fpAfterMetadataSaved, 0, true},
	}
	for _, c := range cases {
		t.Run(c.point, func(t *testing.T) {
			r := rand.New(rand.NewSource(11))
			seen := map[uint32]bool{}
			ops1 := genOps(r, seen, 0, 25, nKeys)
			ops2 := genOps(r, seen, lastLsn(ops1), 25, nKeys)
			all := append(slices.Clone(ops1), ops2...)

			cfg := testCfg()
			cfg.failpoint = crashAt(c.point, 2)
			e := newEnv(t)
			tl := e.create(t, cfg)
			ingest(t, tl, ops1)
			flush(t, tl)
			ingest(t, tl, ops2)
			if ok, err := tl.Freeze(); !ok || err != nil {
				t.Fatal(ok, err)
			}
			select {
			case <-tl.done:
			case <-time.After(5 * time.Second):
				t.Fatal("flush did not reach the crash point")
			}
			tl.Stop()

			tl2, err := e.load(t, testCfg())
			if err != nil {
				t.Fatal(err)
			}
			if got := len(filesWithSuffix(t, e.dir, oldSuffix)); got != c.wantOldL0s {
				t.Fatalf("%d files set aside, want %d", got, c.wantOldL0s)
			}
			survived := ops1
			if c.survives {
				survived = all
			}
			if tl2.DiskConsistentLsn() != lastLsn(survived) || tl2.LastRecordLsn() != lastLsn(survived) {
				t.Fatalf("recovered at %s/%s, want %s", tl2.DiskConsistentLsn(), tl2.LastRecordLsn(), lastLsn(survived))
			}
			checkAgainstModel(t, tl2, survived, nKeys)

			if !c.survives {
				ingest(t, tl2, ops2)
				flush(t, tl2)
			}
			checkAgainstModel(t, tl2, all, nKeys)
			if _, hist := layerDescs(tl2); len(hist) != 2 {
				t.Fatalf("%d layers after replay, want 2", len(hist))
			}
		})
	}
}

func TestCrashDuringFirstFlushRecoversEmpty(t *testing.T) {
	ops := genOps(rand.New(rand.NewSource(5)), map[uint32]bool{}, 0, 10, 3)
	cfg := testCfg()
	cfg.failpoint = crashAt(fpAfterL0Written, 1)
	e := newEnv(t)
	tl := e.create(t, cfg)
	ingest(t, tl, ops)
	tl.Freeze()
	<-tl.done
	tl.Stop()

	tl2, err := e.load(t, testCfg())
	if err != nil {
		t.Fatal(err)
	}
	if tl2.DiskConsistentLsn() != 0 || tl2.LastRecordLsn() != 0 {
		t.Fatalf("recovered at %s/%s, want 0", tl2.DiskConsistentLsn(), tl2.LastRecordLsn())
	}
	if _, hist := layerDescs(tl2); len(hist) != 0 {
		t.Fatalf("loaded %d layers", len(hist))
	}
	ingest(t, tl2, ops)
	flush(t, tl2)
	checkAgainstModel(t, tl2, ops, 3)
}

func TestLoadCleansTempAndScratchFiles(t *testing.T) {
	e := newEnv(t)
	tl := e.create(t, testCfg())
	ingest(t, tl, []op{{k: 0, lsn: 5, init: true, text: "a"}})
	flush(t, tl)
	tl.Stop()

	for _, name := range []string{"ephemeral-123", "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000000000009-000000000000000A.___temp"} {
		if err := os.WriteFile(filepath.Join(e.dir, name), []byte("junk"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := e.load(t, testCfg()); err != nil {
		t.Fatal(err)
	}
	if n := len(filesWithSuffix(t, e.dir, "")); n != 2 {
		t.Fatalf("%d files left, want 2", n)
	}
}

func TestLoadBoundary(t *testing.T) {
	const nKeys = 3
	r := rand.New(rand.NewSource(9))
	seen := map[uint32]bool{}
	ops1 := genOps(r, seen, 0, 10, nKeys)
	ops2 := genOps(r, seen, lastLsn(ops1), 10, nKeys)
	e := newEnv(t)
	tl := e.create(t, testCfg())
	ingest(t, tl, ops1)
	flush(t, tl)
	ingest(t, tl, ops2)
	flush(t, tl)
	meta := tl.Metadata()
	_, hist := layerDescs(tl)
	if len(hist) != 2 {
		t.Fatalf("%d layers", len(hist))
	}
	tl.Stop()

	setDcl := func(dcl lsn.Lsn) {
		m := meta
		m.DiskConsistentLsn = dcl
		if err := SaveMetadata(filepath.Join(e.dir, MetadataFileName), m); err != nil {
			t.Fatal(err)
		}
	}
	end2 := hist[1].LsnRange.End

	setDcl(end2 - 2)
	tl2, err := e.load(t, testCfg())
	if err != nil {
		t.Fatal(err)
	}
	if _, h := layerDescs(tl2); len(h) != 1 || h[0] != hist[0] {
		t.Fatalf("kept %v, want only %s", h, hist[0])
	}
	tl2.Stop()
	if got := filesWithSuffix(t, e.dir, oldSuffix); len(got) != 1 || got[0] != hist[1].FileName()+oldSuffix {
		t.Fatalf("set aside %v", got)
	}

	if err := os.Rename(filepath.Join(e.dir, hist[1].FileName()+oldSuffix), filepath.Join(e.dir, hist[1].FileName())); err != nil {
		t.Fatal(err)
	}
	setDcl(end2 - 1)
	tl3, err := e.load(t, testCfg())
	if err != nil {
		t.Fatal(err)
	}
	if _, h := layerDescs(tl3); len(h) != 2 {
		t.Fatalf("kept %d layers, want both", len(h))
	}
	checkAgainstModel(t, tl3, append(slices.Clone(ops1), ops2...), nKeys)
}

func TestLoadErrors(t *testing.T) {
	t.Run("no metadata", func(t *testing.T) {
		_, err := Load(t.TempDir(), ids.NewTenantID(), ids.NewTimelineID(), nil, testCfg().Config, testCfg().Plugins)
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("corrupt metadata", func(t *testing.T) {
		e := newEnv(t)
		e.create(t, testCfg()).Stop()
		os.WriteFile(filepath.Join(e.dir, MetadataFileName), []byte("junk"), 0o644)
		if _, err := Load(e.dir, e.tenant, e.id, nil, testCfg().Config, testCfg().Plugins); !errors.Is(err, ErrBadMetadata) {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("other timeline's layer", func(t *testing.T) {
		e := newEnv(t)
		tl := e.create(t, testCfg())
		ingest(t, tl, []op{{k: 0, lsn: 5, init: true, text: "a"}})
		flush(t, tl)
		tl.Stop()
		_, err := Load(e.dir, e.tenant, ids.NewTimelineID(), nil, testCfg().Config, testCfg().Plugins)
		if err == nil || !strings.Contains(err.Error(), "belongs to") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("corrupt layer", func(t *testing.T) {
		e := newEnv(t)
		tl := e.create(t, testCfg())
		ingest(t, tl, []op{{k: 0, lsn: 5, init: true, text: "a"}})
		flush(t, tl)
		_, hist := layerDescs(tl)
		tl.Stop()
		path := filepath.Join(e.dir, hist[0].FileName())
		b, _ := os.ReadFile(path)
		b[10] ^= 0xFF
		os.WriteFile(path, b, 0o644)
		if _, err := Load(e.dir, e.tenant, e.id, nil, testCfg().Config, testCfg().Plugins); !errors.Is(err, layer.ErrBadSummary) {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("non-layer files are ignored", func(t *testing.T) {
		e := newEnv(t)
		tl := e.create(t, testCfg())
		ingest(t, tl, []op{{k: 0, lsn: 5, init: true, text: "a"}})
		flush(t, tl)
		tl.Stop()
		os.WriteFile(filepath.Join(e.dir, "README"), []byte("hi"), 0o644)
		os.Mkdir(filepath.Join(e.dir, "subdir"), 0o755)
		tl2, err := e.load(t, testCfg())
		if err != nil {
			t.Fatal(err)
		}
		if _, h := layerDescs(tl2); len(h) != 1 {
			t.Fatalf("%d layers", len(h))
		}
	})
}

func TestCreateErrors(t *testing.T) {
	e := newEnv(t)
	e.create(t, testCfg())
	if _, err := Create(e.dir, e.tenant, e.id, RootMetadata(0, 17), testCfg().Config, testCfg().Plugins); !errors.Is(err, os.ErrExist) {
		t.Fatalf("second create: %v", err)
	}
	bad := RootMetadata(0, 17)
	bad.LatestGCCutoffLsn = 5
	if _, err := Create(filepath.Join(t.TempDir(), "x"), e.tenant, e.id, bad, testCfg().Config, testCfg().Plugins); !errors.Is(err, ErrInvalidMetadata) {
		t.Fatalf("invalid metadata: %v", err)
	}
}

func TestFlushRetriesAfterErrors(t *testing.T) {
	for point, wantHits := range map[string]int32{fpFlushStart: 4, fpAfterL0Written: 4, fpAfterLayerSwap: 1, fpAfterMetadataSaved: 1} {
		t.Run(point, func(t *testing.T) {
			var hits atomic.Int32
			cfg := testCfg()
			cfg.failpoint = func(name string) error {
				if name == point && hits.Add(1) <= 3 {
					return errors.New("boom")
				}
				return nil
			}
			ops := genOps(rand.New(rand.NewSource(4)), map[uint32]bool{}, 0, 15, 3)
			e := newEnv(t)
			tl := e.create(t, cfg)
			ingest(t, tl, ops)
			flush(t, tl)
			deadline := time.Now().Add(2 * time.Second)
			for hits.Load() < wantHits && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if hits.Load() != wantHits {
				t.Fatalf("failpoint hit %d times, want %d", hits.Load(), wantHits)
			}
			deadline = time.Now().Add(2 * time.Second)
			for tl.BackgroundErr() == nil {
				if time.Now().After(deadline) {
					t.Fatal("errors were not reported")
				}
				time.Sleep(time.Millisecond)
			}
			if tl.DiskConsistentLsn() != lastLsn(ops) {
				t.Fatalf("disk_consistent_lsn = %s", tl.DiskConsistentLsn())
			}
			checkAgainstModel(t, tl, ops, 3)
			if _, hist := layerDescs(tl); len(hist) != 1 {
				t.Fatalf("%d layers", len(hist))
			}
			tl.Stop()
			tl2, err := e.load(t, testCfg())
			if err != nil {
				t.Fatal(err)
			}
			checkAgainstModel(t, tl2, ops, 3)
		})
	}
}

func TestConcurrentWritesReadsAndFlushes(t *testing.T) {
	cfg := testCfg()
	cfg.CheckpointDistance = 16
	e := newEnv(t)
	tl := e.create(t, cfg)

	const n = 1500
	var wg sync.WaitGroup
	var failed atomic.Bool
	fail := func(format string, args ...any) {
		if failed.CompareAndSwap(false, true) {
			t.Errorf(format, args...)
		}
	}
	expectedPage := func(upTo lsn.Lsn) string {
		var b strings.Builder
		for l := lsn.Lsn(1); l <= upTo; l++ {
			fmt.Fprintf(&b, "%d,", l)
		}
		return b.String()
	}
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			for target := lsn.Lsn(r + 1); target <= n && !failed.Load(); target += 5 {
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				err := tl.WaitLsn(ctx, target)
				cancel()
				if err != nil {
					fail("wait %s: %v", target, err)
					return
				}
				got, ok, err := readPage(tl, pg(1), target)
				if err != nil || !ok || got != expectedPage(target) {
					fail("read @ %s: ok=%v err=%v page ends %q", target, ok, err, tail(got, 30))
					return
				}

				if got, ok, err := readPage(tl, pg(1), 3); err != nil || !ok || got != "1,2,3," {
					if target >= 3 {
						fail("old read: %q ok=%v err=%v", got, ok, err)
						return
					}
				}
			}
		}(r)
	}
	for l := lsn.Lsn(1); l <= n; l++ {
		o := op{k: 1, lsn: l, init: l == 1, text: fmt.Sprintf("%d,", l)}
		if err := tl.PutBatch([]inmemory.Put{o.put()}, l); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
	flush(t, tl)
	if _, hist := layerDescs(tl); len(hist) < 20 {
		t.Fatalf("only %d flushes happened", len(hist))
	}
	tl.Stop()

	tl2, err := e.load(t, testCfg())
	if err != nil {
		t.Fatal(err)
	}
	if got, ok, err := readPage(tl2, pg(1), n); err != nil || !ok || got != expectedPage(n) {
		t.Fatalf("after restart: ok=%v err=%v", ok, err)
	}
}

func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return "..." + s[len(s)-n:]
}
