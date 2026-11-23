package timeline

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Fadil-Tao/naon/internal/pageserver/key"
	"github.com/Fadil-Tao/naon/internal/pageserver/layer/image"
	"github.com/Fadil-Tao/naon/internal/pageserver/layer/inmemory"
	"github.com/Fadil-Tao/naon/internal/pageserver/layermap"
	"github.com/Fadil-Tao/naon/internal/pageserver/lsn"
	"github.com/Fadil-Tao/naon/internal/pageserver/value"
)

func rng(a, b uint32) key.KeyRange { return key.KeyRange{Start: pg(a), End: pg(b)} }

func readCfg() opts {
	c := testCfg()
	c.Redo = TestRedo
	return c
}

func get(t *testing.T, tl *Timeline, k uint32, at lsn.Lsn) (string, error) {
	t.Helper()
	p, err := tl.Get(ctx5(t), pg(k), at)
	return string(p), err
}

func TestGetPagesAtDifferentLsns(t *testing.T) {
	tl := newEnv(t).create(t, readCfg())
	ingest(t, tl, []op{
		{k: 1, lsn: 10, init: true, text: "a"},
		{k: 1, lsn: 20, text: "b"},
		{k: 2, lsn: 20, img: true, text: "IMG"},
		{k: 1, lsn: 30, text: "c"},
		{k: 2, lsn: 40, text: "+"},
		{k: 1, lsn: 50, init: true, text: "reset"},
	})
	for _, c := range []struct {
		k    uint32
		at   lsn.Lsn
		want string
		err  error
	}{
		{1, 9, "", ErrNotFound},
		{1, 10, "a", nil},
		{1, 19, "a", nil},
		{1, 20, "ab", nil},
		{1, 49, "abc", nil},
		{1, 50, "reset", nil},
		{1, 50, "reset", nil},
		{2, 19, "", ErrNotFound},
		{2, 20, "IMG", nil},
		{2, 39, "IMG", nil},
		{2, 50, "IMG+", nil},
		{3, 50, "", ErrNotFound},
	} {
		got, err := get(t, tl, c.k, c.at)
		if !errors.Is(err, c.err) || got != c.want {
			t.Fatalf("key %d @ %d = %q, %v; want %q, %v", c.k, c.at, got, err, c.want, c.err)
		}
	}
}

func TestGetAcrossFlush(t *testing.T) {
	tl := newEnv(t).create(t, readCfg())
	ingest(t, tl, []op{{k: 1, lsn: 10, init: true, text: "a"}, {k: 1, lsn: 20, text: "b"}})
	flush(t, tl)
	ingest(t, tl, []op{{k: 1, lsn: 30, text: "c"}})
	for at, want := range map[lsn.Lsn]string{10: "a", 20: "ab", 29: "ab", 30: "abc"} {
		if got, err := get(t, tl, 1, at); err != nil || got != want {
			t.Fatalf("@ %d = %q, %v; want %q", at, got, err, want)
		}
	}
}

func TestGetWaitsForLsn(t *testing.T) {
	tl := newEnv(t).create(t, readCfg())
	ingest(t, tl, []op{{k: 1, lsn: 4990, init: true, text: "a"}})

	type result struct {
		page string
		err  error
	}
	done := make(chan result, 1)
	go func() {
		p, err := tl.Get(ctx5(t), pg(1), 5000)
		done <- result{string(p), err}
	}()
	select {
	case r := <-done:
		t.Fatalf("returned before the LSN arrived: %+v", r)
	case <-time.After(30 * time.Millisecond):
	}
	ingest(t, tl, []op{{k: 1, lsn: 5000, text: "b"}})
	select {
	case r := <-done:
		if r.err != nil || r.page != "ab" {
			t.Fatalf("%+v", r)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("did not wake")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := tl.Get(ctx, pg(1), 9000); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
}

func TestGetBelowGCCutoff(t *testing.T) {
	meta := RootMetadata(0, 17)
	meta.DiskConsistentLsn, meta.LatestGCCutoffLsn = 100, 60
	e := newEnv(t)
	tl, err := Create(e.dir, e.tenant, e.id, meta, readCfg().Config, readCfg().Plugins)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tl.Stop)
	ingest(t, tl, []op{{k: 1, lsn: 110, init: true, text: "a"}})

	if tl.LatestGCCutoffLsn() != 60 {
		t.Fatalf("cutoff = %s", tl.LatestGCCutoffLsn())
	}
	if _, err := get(t, tl, 1, 59); !errors.Is(err, ErrBelowGCCutoff) {
		t.Fatalf("@ 59: %v", err)
	}
	if _, err := get(t, tl, 1, 60); !errors.Is(err, ErrNotFound) {
		t.Fatalf("@ 60: %v", err)
	}
	if got, err := get(t, tl, 1, 110); err != nil || got != "a" {
		t.Fatalf("@ 110: %q, %v", got, err)
	}

	flush(t, tl)
	tl.Stop()
	tl2, err := e.load(t, readCfg())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := get(t, tl2, 1, 59); !errors.Is(err, ErrBelowGCCutoff) {
		t.Fatalf("after restart @ 59: %v", err)
	}

	if _, err := tl2.GetVectored(ctx5(t), key.FromRanges(rng(0, 2)), 10); !errors.Is(err, ErrBelowGCCutoff) {
		t.Fatalf("vectored: %v", err)
	}
}

func TestGetWithoutRedo(t *testing.T) {
	tl := newEnv(t).create(t, testCfg())
	ingest(t, tl, []op{
		{k: 1, lsn: 10, img: true, text: "IMG"},
		{k: 2, lsn: 10, init: true, text: "a"},
		{k: 2, lsn: 20, text: "b"},
	})
	if got, err := get(t, tl, 1, 10); err != nil || got != "IMG" {
		t.Fatalf("%q, %v", got, err)
	}
	if _, err := get(t, tl, 2, 20); !errors.Is(err, ErrNoRedo) {
		t.Fatalf("err = %v", err)
	}
}

func TestGetCorruptLayers(t *testing.T) {
	tl := newEnv(t).create(t, readCfg())

	ingest(t, tl, []op{{k: 1, lsn: 10, text: "x"}, {k: 1, lsn: 20, text: "y"}})
	if _, err := get(t, tl, 1, 20); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("err = %v", err)
	}
}

func TestRedoInput(t *testing.T) {
	var got struct {
		sync.Mutex
		k    key.Key
		at   lsn.Lsn
		in   value.ReconstructInput
		call int
	}
	cfg := testCfg()
	cfg.Redo = RedoFunc(func(k key.Key, at lsn.Lsn, in value.ReconstructInput) ([]byte, error) {
		got.Lock()
		defer got.Unlock()
		got.k, got.at, got.in = k, at, in
		got.call++
		return []byte("redo-result"), nil
	})
	tl := newEnv(t).create(t, cfg)
	ingest(t, tl, []op{
		{k: 1, lsn: 10, img: true, text: "base"},
		{k: 1, lsn: 20, text: "+1"},
		{k: 1, lsn: 30, text: "+2"},
		{k: 1, lsn: 40, text: "+3"},
	})
	flush(t, tl)
	p, err := tl.Get(ctx5(t), pg(1), 35)
	if err != nil || string(p) != "redo-result" {
		t.Fatalf("%q, %v", p, err)
	}
	got.Lock()
	defer got.Unlock()
	if got.call != 1 || got.k != pg(1) || got.at != 35 {
		t.Fatalf("redo called %d times with %s @ %s", got.call, got.k, got.at)
	}
	in := got.in
	if !in.HasImage || string(in.Image) != "base" || in.ImageLsn != 10 || len(in.Records) != 2 ||
		in.Records[0].Lsn != 20 || in.Records[1].Lsn != 30 {
		t.Fatalf("redo input = %+v", in)
	}
}

func TestRedoError(t *testing.T) {
	boom := errors.New("boom")
	cfg := testCfg()
	cfg.Redo = RedoFunc(func(key.Key, lsn.Lsn, value.ReconstructInput) ([]byte, error) { return nil, boom })
	tl := newEnv(t).create(t, cfg)
	ingest(t, tl, []op{{k: 1, lsn: 10, init: true, text: "a"}, {k: 1, lsn: 20, text: "b"}})
	if _, err := get(t, tl, 1, 20); !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
}

func TestGetReturnsACopy(t *testing.T) {
	tl := newEnv(t).create(t, readCfg())
	ingest(t, tl, []op{{k: 1, lsn: 10, img: true, text: "IMG"}})
	p, _ := tl.Get(ctx5(t), pg(1), 10)
	p[0] = 'X'
	if got, _ := get(t, tl, 1, 10); got != "IMG" {
		t.Fatalf("stored page changed: %q", got)
	}
}

func TestGetAfterStop(t *testing.T) {
	tl := newEnv(t).create(t, readCfg())
	ingest(t, tl, []op{{k: 1, lsn: 10, init: true, text: "a"}})
	tl.Stop()
	if _, err := get(t, tl, 1, 10); !errors.Is(err, ErrStopped) {
		t.Fatalf("err = %v", err)
	}
}

func TestGetVectored(t *testing.T) {
	tl := newEnv(t).create(t, readCfg())
	ingest(t, tl, []op{
		{k: 1, lsn: 10, init: true, text: "a"},
		{k: 3, lsn: 10, init: true, text: "c"},
		{k: 7, lsn: 15, img: true, text: "seven"},
		{k: 3, lsn: 20, text: "!"},
	})
	ks := key.FromRanges(rng(0, 5), rng(6, 9))
	res, err := tl.GetVectored(ctx5(t), ks, 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 8 {
		t.Fatalf("%d results, want 8", len(res))
	}
	want := map[uint32]string{1: "a", 3: "c!", 7: "seven"}
	prev := key.Key{}
	for i, r := range res {
		if i > 0 && !prev.Less(r.Key) {
			t.Fatalf("results not in key order at %d", i)
		}
		prev = r.Key
		w, exists := want[r.Key.Field6]
		switch {
		case exists && (r.Err != nil || string(r.Page) != w):
			t.Fatalf("%s = %q, %v; want %q", r.Key, r.Page, r.Err, w)
		case !exists && !errors.Is(r.Err, ErrNotFound):
			t.Fatalf("%s = %q, %v; want ErrNotFound", r.Key, r.Page, r.Err)
		}
	}

	for _, r := range res {
		if r.Key.Field6 == 5 || r.Key.Field6 == 9 {
			t.Fatalf("unexpected key %s", r.Key)
		}
	}
}

func TestGetVectoredLimits(t *testing.T) {
	tl := newEnv(t).create(t, readCfg())
	if _, err := tl.GetVectored(ctx5(t), key.FromRanges(key.FullRange()), 0); !errors.Is(err, ErrTooManyKeys) {
		t.Fatalf("full range: %v", err)
	}
	if _, err := tl.GetVectored(ctx5(t), key.FromRanges(rng(0, MaxVectoredKeys+1)), 0); !errors.Is(err, ErrTooManyKeys) {
		t.Fatalf("just over the limit: %v", err)
	}
	if res, err := tl.GetVectored(ctx5(t), key.KeySpace{}, 0); err != nil || len(res) != 0 {
		t.Fatalf("empty keyspace: %v, %v", res, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := tl.GetVectored(ctx, key.FromRanges(rng(0, 2)), 100); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiting: %v", err)
	}
}

func addImageLayer(t *testing.T, tl *Timeline, e env, ops []op, a, b uint32, l lsn.Lsn) {
	t.Helper()
	w, err := image.NewWriter(e.dir, e.tenant, e.id, rng(a, b), l)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Abort()
	for k := a; k < b; k++ {
		if page, ok := expect(ops, k, l); ok {
			if err := w.Put(pg(k), []byte(page)); err != nil {
				t.Fatal(err)
			}
		}
	}
	desc, path, err := w.Finish(0)
	if err != nil {
		t.Fatal(err)
	}
	r, err := image.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	err = tl.layers.Update(func(u *layermap.Updater) error {
		return u.InsertHistoric(layermap.Entry{Desc: desc, Layer: r})
	})
	if err != nil {
		r.Close()
		if !errors.Is(err, layermap.ErrDuplicate) {
			t.Fatal(err)
		}
	}
}

func TestGetUsesImageLayers(t *testing.T) {
	const nKeys = 4
	e := newEnv(t)
	tl := e.create(t, readCfg())
	ops := []op{
		{k: 0, lsn: 10, init: true, text: "a"}, {k: 1, lsn: 15, init: true, text: "x"},
		{k: 0, lsn: 20, text: "b"}, {k: 0, lsn: 30, text: "c"}, {k: 1, lsn: 40, text: "y"},
	}
	ingest(t, tl, ops)
	flush(t, tl)
	addImageLayer(t, tl, e, ops, 0, nKeys, 25)
	checkAgainstModelWithGet(t, tl, ops, nKeys)
	tl.Stop()
	tl2, err := e.load(t, readCfg())
	if err != nil {
		t.Fatal(err)
	}
	if _, hist := layerDescs(tl2); len(hist) != 2 {
		t.Fatalf("%d layers after load, want L0 + image", len(hist))
	}
	checkAgainstModelWithGet(t, tl2, ops, nKeys)
}

func checkAgainstModelWithGet(t testing.TB, tl *Timeline, ops []op, nKeys int) {
	t.Helper()
	for k := uint32(0); k < uint32(nKeys); k++ {
		for at := lsn.Lsn(0); at <= lastLsn(ops); at++ {
			if at > tl.LastRecordLsn() {
				break
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			page, err := tl.Get(ctx, pg(k), at)
			cancel()
			want, ok := expect(ops, k, at)
			switch {
			case ok && (err != nil || string(page) != want):
				t.Fatalf("key %d @ %s = %q, %v; want %q", k, at, page, err, want)
			case !ok && !errors.Is(err, ErrNotFound):
				t.Fatalf("key %d @ %s = %q, %v; want ErrNotFound", k, at, page, err)
			}
		}
	}
}

func TestGetMatchesModelProperty(t *testing.T) {
	rounds := 40
	if testing.Short() {
		rounds = 5
	}
	if s := os.Getenv("NAON_ROUNDS"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil {
			t.Fatal(err)
		}
		rounds = n
	}
	for round := 0; round < rounds; round++ {
		runPropertyRound(t, round)
	}
}

func runPropertyRound(t *testing.T, round int) {
	const nKeys = 5
	r := rand.New(rand.NewSource(int64(round)))
	e := newEnv(t)
	cfg := readCfg()
	if r.Intn(3) == 0 {
		cfg.CheckpointDistance = uint64(10 + r.Intn(40))
	}
	tl := e.create(t, cfg)
	var log []string
	fail := func(format string, args ...any) {
		t.Helper()
		t.Fatalf("round %d: %s\nsteps:\n  %s", round, fmt.Sprintf(format, args...), strings.Join(log, "\n  "))
	}

	seen := map[uint32]bool{}
	var all []op
	cur := lsn.Lsn(0)

	sample := func(n int) {
		for i := 0; i < n; i++ {
			last := tl.LastRecordLsn()
			k := uint32(r.Intn(nKeys + 1))
			at := lsn.Lsn(r.Intn(int(last) + 1))
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			page, err := tl.Get(ctx, pg(k), at)
			cancel()
			want, ok := expect(all, k, at)
			if ok && (err != nil || string(page) != want) {
				fail("key %d @ %s = %q, %v; want %q", k, at, page, err, want)
			}
			if !ok && !errors.Is(err, ErrNotFound) {
				fail("key %d @ %s = %q, %v; want ErrNotFound", k, at, page, err)
			}
		}
	}
	ingestLogged := func(ops []op) {
		for i := 0; i < len(ops); {
			j := i
			for j < len(ops) && ops[j].lsn == ops[i].lsn {
				j++
			}
			ingest(t, tl, ops[i:j])
			i = j
		}
	}
	restart := func(flushFirst bool) {
		if flushFirst {
			if err := tl.FlushAndWait(ctx5(t)); err != nil {
				fail("flush before restart: %v", err)
			}
		}
		tl.Stop()
		var err error
		if tl, err = e.load(t, cfg); err != nil {
			fail("load: %v", err)
		}

		var replay []op
		for _, o := range all {
			if o.lsn > tl.LastRecordLsn() {
				replay = append(replay, o)
			}
		}
		if len(replay) > 0 {
			ingestLogged(replay)
		}
	}

	for step := 0; step < 25; step++ {
		switch x := r.Intn(100); {
		case x < 50:
			ops := genOps(r, seen, cur, 1+r.Intn(6), nKeys)
			for i := range ops {
				if ops[i].init && r.Intn(3) == 0 {
					ops[i].img = true
				}
			}
			all = append(all, ops...)
			cur = lastLsn(ops)
			log = append(log, fmt.Sprintf("write %d ops up to %s", len(ops), cur))
			ingestLogged(ops)
		case x < 65:
			log = append(log, "flush")
			if err := tl.FlushAndWait(ctx5(t)); err != nil {
				fail("flush: %v (background %v)", err, tl.BackgroundErr())
			}
		case x < 70:
			log = append(log, "clean restart")
			restart(true)
		case x < 80:
			log = append(log, "crash restart (unflushed data lost, WAL replayed)")
			restart(false)
		case x < 90:
			dcl := tl.DiskConsistentLsn()
			if dcl == 0 {
				continue
			}
			a := uint32(r.Intn(nKeys))
			b := a + 1 + uint32(r.Intn(nKeys-int(a)))
			l := lsn.Lsn(1 + r.Intn(int(dcl)))
			log = append(log, fmt.Sprintf("image layer keys [%d,%d) @ %s", a, b, l))
			addImageLayer(t, tl, e, all, a, b, l)
		default:
			log = append(log, "sample")
		}
		sample(8)
	}
	if len(all) > 0 {
		checkAgainstModelWithGet(t, tl, all, nKeys+1)
	}
	tl.Stop()
}

func TestGetConcurrentWithWritesAndFlushes(t *testing.T) {
	cfg := readCfg()
	cfg.CheckpointDistance = 12
	e := newEnv(t)
	tl := e.create(t, cfg)

	const n = 1200
	expectedPage := func(upTo lsn.Lsn) string {
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
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			for target := lsn.Lsn(r + 1); target <= n && !failed.Load(); target += 6 {
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				page, err := tl.Get(ctx, pg(1), target)
				cancel()
				if err != nil || string(page) != expectedPage(target) {
					fail("Get @ %s: %v, page ends %q", target, err, tail(string(page), 24))
					return
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
}
