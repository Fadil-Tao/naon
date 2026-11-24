package timeline

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Fadil-Tao/naon/internal/pageserver/key"
	"github.com/Fadil-Tao/naon/internal/pageserver/lsn"
)

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestBackgroundFreezesIdleData(t *testing.T) {
	cfg := readCfg()
	cfg.BackgroundPeriod = 5 * time.Millisecond
	cfg.CheckpointTimeout = 50 * time.Millisecond
	tl := newEnv(t).create(t, cfg)
	ingest(t, tl, []op{{k: 0, lsn: 10, init: true, text: "a"}})
	if err := tl.flushed.Wait(ctx5(t), 10); err != nil {
		t.Fatalf("idle data was never flushed: %v", err)
	}
	if _, ok := tl.Layers().Snapshot().Open(); ok {
		t.Fatal("open layer still there after the timeout freeze")
	}
}

func TestBackgroundPeriodZeroMeansNoTimeoutFreeze(t *testing.T) {
	cfg := readCfg()
	cfg.BackgroundPeriod = 0
	cfg.CheckpointTimeout = time.Millisecond
	tl := newEnv(t).create(t, cfg)
	ingest(t, tl, []op{{k: 0, lsn: 10, init: true, text: "a"}})
	time.Sleep(50 * time.Millisecond)
	if tl.DiskConsistentLsn() != 0 {
		t.Fatalf("flushed to %s with the timer off", tl.DiskConsistentLsn())
	}
}

func writeFlush(t *testing.T, tl *Timeline, l lsn.Lsn) {
	t.Helper()
	ingest(t, tl, []op{{k: 0, lsn: l, init: true, text: "x"}})
	flush(t, tl)
}

func TestBackgroundCreatesImagesAfterFlushes(t *testing.T) {
	cfg := readCfg()
	cfg.BackgroundPeriod = 0
	cfg.KeySpace = fixedKeySpace(3)
	tl := newEnv(t).create(t, cfg)
	writeFlush(t, tl, 10)
	writeFlush(t, tl, 20)
	time.Sleep(30 * time.Millisecond)
	if n := len(images(tl)); n != 0 {
		t.Fatalf("%d images after 2 flushes", n)
	}
	writeFlush(t, tl, 30)
	eventually(t, "an image after the 3rd flush", func() bool { return len(images(tl)) == 1 })
	if img := images(tl)[0]; img.ImageLsn() != 30 {
		t.Fatalf("image at %s, want 30", img.ImageLsn())
	}
	if err := tl.BackgroundErr(); err != nil {
		t.Fatal(err)
	}
}

func TestBackgroundWithoutKeySpace(t *testing.T) {
	cfg := readCfg()
	cfg.BackgroundPeriod = 0
	tl := newEnv(t).create(t, cfg)
	for l := lsn.Lsn(10); l <= 40; l += 10 {
		writeFlush(t, tl, l)
	}
	time.Sleep(30 * time.Millisecond)
	if len(images(tl)) != 0 || tl.BackgroundErr() != nil {
		t.Fatalf("images=%d err=%v", len(images(tl)), tl.BackgroundErr())
	}
}

func TestBackgroundReportsErrorsAndKeepsGoing(t *testing.T) {
	var failing atomic.Bool
	failing.Store(true)
	boom := errors.New("boom")
	cfg := readCfg()
	cfg.BackgroundPeriod = 0
	cfg.KeySpace = func(ctx context.Context, tl *Timeline, at lsn.Lsn) (key.KeySpace, error) {
		if failing.Load() {
			return key.KeySpace{}, boom
		}
		return fixedKeySpace(3)(ctx, tl, at)
	}
	tl := newEnv(t).create(t, cfg)
	for l := lsn.Lsn(10); l <= 30; l += 10 {
		writeFlush(t, tl, l)
	}
	eventually(t, "the error to be reported", func() bool { return errors.Is(tl.BackgroundErr(), boom) })
	if len(images(tl)) != 0 {
		t.Fatal("image created while the keyspace fails")
	}
	failing.Store(false)
	writeFlush(t, tl, 40)
	eventually(t, "an image once the keyspace works", func() bool { return len(images(tl)) == 1 })
}

func TestBackgroundStopDuringImageBuild(t *testing.T) {
	base := runtime.NumGoroutine()
	started := make(chan struct{}, 1)
	cfg := readCfg()
	cfg.BackgroundPeriod = time.Millisecond
	cfg.ImageCreationThreshold = 1
	cfg.KeySpace = func(ctx context.Context, _ *Timeline, _ lsn.Lsn) (key.KeySpace, error) {
		select {
		case started <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return key.KeySpace{}, ctx.Err()
	}
	tl, err := Create(newEnv(t).dir, [16]byte{1}, [16]byte{2}, RootMetadata(0, 17), cfg.Config, cfg.Plugins)
	if err != nil {
		t.Fatal(err)
	}
	writeFlush(t, tl, 10)
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("image build never started")
	}
	stopped := make(chan struct{})
	go func() { tl.Stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop hung while an image build was running")
	}
	if err := tl.BackgroundErr(); err != nil {
		t.Fatalf("cancellation reported as an error: %v", err)
	}
	eventually(t, "goroutines to exit", func() bool { return runtime.NumGoroutine() <= base })
}
