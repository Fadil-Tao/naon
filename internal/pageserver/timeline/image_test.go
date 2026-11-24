package timeline

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Fadil-Tao/naon/internal/pageserver/key"
	"github.com/Fadil-Tao/naon/internal/pageserver/layer"
	"github.com/Fadil-Tao/naon/internal/pageserver/lsn"
	"github.com/Fadil-Tao/naon/internal/pageserver/value"
)

func fixedKeySpace(n uint32) KeySpaceFunc {
	return func(context.Context, *Timeline, lsn.Lsn) (key.KeySpace, error) {
		return key.FromRanges(rng(0, n)), nil
	}
}

func imageCfg(n uint32) opts {
	c := readCfg()
	c.KeySpace = fixedKeySpace(n)
	c.noBackground = true
	return c
}

func images(tl *Timeline) []layer.Desc {
	var out []layer.Desc
	for _, e := range tl.Layers().Snapshot().Historic() {
		if !e.Desc.IsDelta {
			out = append(out, e.Desc)
		}
	}
	return out
}

func createImages(t *testing.T, tl *Timeline) int {
	t.Helper()
	n, err := tl.CreateImageLayers(ctx5(t))
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestImageCreationThreshold(t *testing.T) {
	tl := newEnv(t).create(t, imageCfg(4))
	if n := createImages(t, tl); n != 0 {
		t.Fatalf("empty timeline: %d images", n)
	}
	r := rand.New(rand.NewSource(1))
	seen := map[uint32]bool{}
	var all []op
	writeAndFlush := func() {
		ops := genOps(r, seen, lastOrZero(all), 5, 4)
		ingest(t, tl, ops)
		all = append(all, ops...)
		flush(t, tl)
	}

	writeAndFlush()
	writeAndFlush()
	if n := createImages(t, tl); n != 0 {
		t.Fatalf("2 deltas: %d images", n)
	}
	writeAndFlush()
	if n := createImages(t, tl); n != 1 {
		t.Fatalf("3 deltas: %d images, want 1", n)
	}
	img := images(tl)
	if len(img) != 1 || img[0].KeyRange != rng(0, 4) || img[0].ImageLsn() != tl.DiskConsistentLsn() {
		t.Fatalf("images = %v, want one over keys 0-4 at %s", img, tl.DiskConsistentLsn())
	}
	if n := createImages(t, tl); n != 0 {
		t.Fatalf("nothing new: %d images", n)
	}
	writeAndFlush()
	writeAndFlush()
	if n := createImages(t, tl); n != 0 {
		t.Fatalf("2 deltas above the image: %d images", n)
	}
	writeAndFlush()
	if n := createImages(t, tl); n != 1 {
		t.Fatalf("3 deltas above the image: %d, want 1", n)
	}
	if img := images(tl); len(img) != 2 || img[1].ImageLsn() != tl.DiskConsistentLsn() {
		t.Fatalf("images = %v", img)
	}
	checkAgainstModelWithGet(t, tl, all, 5)
}

func TestImageOnlyFromFlushedData(t *testing.T) {
	cfg := imageCfg(3)
	cfg.ImageCreationThreshold = 1
	tl := newEnv(t).create(t, cfg)
	ops := []op{{k: 0, lsn: 10, init: true, text: "a"}}
	ingest(t, tl, ops)
	flush(t, tl)
	more := []op{{k: 0, lsn: 20, text: "b"}, {k: 1, lsn: 20, init: true, text: "new"}}
	ingest(t, tl, more)
	if n := createImages(t, tl); n != 1 {
		t.Fatalf("%d images", n)
	}
	img := images(tl)[0]
	if img.ImageLsn() != 10 {
		t.Fatalf("image at %s, want the disk_consistent_lsn 10", img.ImageLsn())
	}
	checkAgainstModelWithGet(t, tl, append(ops, more...), 3)
}

func TestImageShortensRedo(t *testing.T) {
	var calls, lastRecords atomic.Int64
	cfg := imageCfg(1)
	cfg.Redo = RedoFunc(func(k key.Key, at lsn.Lsn, in value.ReconstructInput) ([]byte, error) {
		calls.Add(1)
		lastRecords.Store(int64(len(in.Records)))
		return value.ApplyTest(in)
	})
	tl := newEnv(t).create(t, cfg)
	var all []op
	for l := lsn.Lsn(1); l <= 30; l++ {
		o := op{k: 0, lsn: l, init: l == 1, text: fmt.Sprintf("%d,", l)}
		all = append(all, o)
		ingest(t, tl, []op{o})
		if l%3 == 0 {
			flush(t, tl)
		}
	}
	want, _ := expect(all, 0, 30)
	if got, err := get(t, tl, 0, 30); err != nil || got != want {
		t.Fatalf("before: %q, %v", got, err)
	}
	if lastRecords.Load() != 30 {
		t.Fatalf("before the image, redo replayed %d records, want 30", lastRecords.Load())
	}

	if n := createImages(t, tl); n != 1 {
		t.Fatalf("%d images", n)
	}
	before := calls.Load()
	if got, err := get(t, tl, 0, 30); err != nil || got != want {
		t.Fatalf("after: %q, %v", got, err)
	}
	if calls.Load() != before {
		t.Fatal("reading at the image LSN still ran redo")
	}
	ingest(t, tl, []op{{k: 0, lsn: 31, text: "+"}})
	if got, err := get(t, tl, 0, 31); err != nil || got != want+"+" {
		t.Fatalf("on top of the image: %q, %v", got, err)
	}
	if lastRecords.Load() != 1 {
		t.Fatalf("on top of the image, redo replayed %d records, want 1", lastRecords.Load())
	}
}

func TestImagePartitions(t *testing.T) {
	cfg := imageCfg(5)
	cfg.ImageTargetSize = 2 * key.PageSize
	cfg.ImageCreationThreshold = 1
	tl := newEnv(t).create(t, cfg)
	ops := []op{{k: 0, lsn: 10, init: true, text: "a"}, {k: 3, lsn: 10, init: true, text: "d"}}
	ingest(t, tl, ops)
	flush(t, tl)
	if n := createImages(t, tl); n != 3 {
		t.Fatalf("%d images, want 3", n)
	}
	img := images(tl)
	want := []key.KeyRange{rng(0, 2), rng(2, 4), rng(4, 5)}
	if len(img) != 3 {
		t.Fatalf("images = %v", img)
	}
	for i := range want {
		if img[i].KeyRange != want[i] {
			t.Fatalf("image %d covers %s, want %s", i, img[i].KeyRange, want[i])
		}
	}
	checkAgainstModelWithGet(t, tl, ops, 6)
}

func TestImageKeySpaceWithHoles(t *testing.T) {
	cfg := readCfg()
	cfg.ImageCreationThreshold = 1
	cfg.KeySpace = func(context.Context, *Timeline, lsn.Lsn) (key.KeySpace, error) {
		return key.FromRanges(rng(0, 2), rng(3, 5)), nil
	}
	cfg.noBackground = true
	tl := newEnv(t).create(t, cfg)
	ops := []op{{k: 0, lsn: 10, init: true, text: "a"}, {k: 4, lsn: 10, init: true, text: "e"}}
	ingest(t, tl, ops)
	flush(t, tl)
	if n := createImages(t, tl); n != 1 {
		t.Fatalf("%d images", n)
	}
	if img := images(tl); len(img) != 1 || img[0].KeyRange != rng(0, 5) {
		t.Fatalf("images = %v", img)
	}
	checkAgainstModelWithGet(t, tl, ops, 6)
}

func TestImageErrors(t *testing.T) {
	tl := newEnv(t).create(t, readCfg())
	if _, err := tl.CreateImageLayers(ctx5(t)); !errors.Is(err, ErrNoKeySpace) {
		t.Fatalf("no keyspace: %v", err)
	}
	boom := errors.New("boom")
	cfg := readCfg()
	cfg.KeySpace = func(context.Context, *Timeline, lsn.Lsn) (key.KeySpace, error) { return key.KeySpace{}, boom }
	tl = newEnv(t).create(t, cfg)
	if _, err := tl.CreateImageLayers(ctx5(t)); !errors.Is(err, boom) {
		t.Fatalf("keyspace error: %v", err)
	}
	cfg = imageCfg(2)
	cfg.ImageCreationThreshold = 1
	cfg.Redo = nil
	tl = newEnv(t).create(t, cfg)
	ingest(t, tl, []op{{k: 0, lsn: 1, init: true, text: "a"}, {k: 0, lsn: 2, text: "b"}})
	flush(t, tl)
	if _, err := tl.CreateImageLayers(ctx5(t)); !errors.Is(err, ErrNoRedo) {
		t.Fatalf("no redo: %v", err)
	}
	if len(images(tl)) != 0 {
		t.Fatal("a failed image creation left a layer")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cfg = imageCfg(2)
	tl = newEnv(t).create(t, cfg)
	if _, err := tl.CreateImageLayers(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled: %v", err)
	}
}

func TestImagesMatchModelProperty(t *testing.T) {
	const nKeys = 5
	for round := 0; round < 25; round++ {
		r := rand.New(rand.NewSource(int64(round)))
		cfg := imageCfg(nKeys)
		cfg.ImageCreationThreshold = 1 + r.Intn(3)
		cfg.ImageTargetSize = uint64(1+r.Intn(nKeys)) * key.PageSize
		e := newEnv(t)
		tl := e.create(t, cfg)
		seen := map[uint32]bool{}
		var all []op
		for step := 0; step < 12; step++ {
			ops := genOps(r, seen, lastOrZero(all), 1+r.Intn(6), nKeys)
			for i := range ops {
				if ops[i].init && r.Intn(3) == 0 {
					ops[i].img = true
				}
			}
			ingest(t, tl, ops)
			all = append(all, ops...)
			if r.Intn(2) == 0 {
				flush(t, tl)
			}
			if r.Intn(2) == 0 {
				createImages(t, tl)
			}
			if r.Intn(6) == 0 {
				flush(t, tl)
				tl.Stop()
				var err error
				if tl, err = e.load(t, cfg); err != nil {
					t.Fatal(err)
				}
			}
		}
		checkAgainstModelWithGet(t, tl, all, nKeys+1)
		tl.Stop()
	}
}

func TestImageCreationWhileWriting(t *testing.T) {
	cfg := imageCfg(3)
	cfg.CheckpointDistance = 10
	cfg.ImageCreationThreshold = 2
	tl := newEnv(t).create(t, cfg)
	const n = 600
	var wg sync.WaitGroup
	stop := make(chan struct{})
	var failed atomic.Bool
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := tl.CreateImageLayers(context.Background()); err != nil {
				failed.Store(true)
				t.Errorf("image creation: %v", err)
				return
			}
		}
	}()
	var all []op
	for l := lsn.Lsn(1); l <= n && !failed.Load(); l++ {
		o := op{k: uint32(l % 3), lsn: l, init: l <= 3, text: fmt.Sprintf("%d,", l)}
		all = append(all, o)
		ingest(t, tl, []op{o})
		if l%50 == 0 {
			flush(t, tl)
		}
	}
	close(stop)
	wg.Wait()
	flush(t, tl)
	if len(images(tl)) == 0 {
		t.Fatal("no image was created")
	}
	checkAgainstModelWithGet(t, tl, all, 3)
}
