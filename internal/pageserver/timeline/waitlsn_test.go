package timeline

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/Fadil-Tao/naon/internal/pageserver/lsn"
)

func waitAsync(w *LsnWaiter, ctx context.Context, target lsn.Lsn) <-chan error {
	ch := make(chan error, 1)
	go func() { ch <- w.Wait(ctx, target) }()
	return ch
}

func waitForWaiters(t *testing.T, w *LsnWaiter, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for w.numWaiters() != n {
		if time.Now().After(deadline) {
			t.Fatalf("waiters = %d, want %d", w.numWaiters(), n)
		}
		time.Sleep(time.Millisecond)
	}
}

func expectBlocked(t *testing.T, ch <-chan error) {
	t.Helper()
	select {
	case err := <-ch:
		t.Fatalf("wait returned early: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
}

func expectDone(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("wait did not return")
		return nil
	}
}

func TestWaitAlreadyReached(t *testing.T) {
	w := NewLsnWaiter(100)
	for _, target := range []lsn.Lsn{0, 50, 100} {
		if err := w.Wait(context.Background(), target); err != nil {
			t.Fatalf("Wait(%s) = %v", target, err)
		}
	}
	if w.numWaiters() != 0 {
		t.Fatal("a satisfied wait left a waiter behind")
	}
}

func TestWaitWakesExactlyAtTarget(t *testing.T) {
	w := NewLsnWaiter(4990)
	ch := waitAsync(w, context.Background(), 5000)
	waitForWaiters(t, w, 1)

	w.Advance(4999)
	expectBlocked(t, ch)
	w.Advance(5000)
	if err := expectDone(t, ch); err != nil {
		t.Fatal(err)
	}
	if w.numWaiters() != 0 {
		t.Fatal("waiter not removed")
	}
}

func TestWaitJumpPastTarget(t *testing.T) {
	w := NewLsnWaiter(0)
	ch := waitAsync(w, context.Background(), 10)
	waitForWaiters(t, w, 1)
	w.Advance(1_000_000)
	if err := expectDone(t, ch); err != nil {
		t.Fatal(err)
	}
}

func TestWaitersWakeInTargetOrder(t *testing.T) {
	w := NewLsnWaiter(0)
	targets := []lsn.Lsn{30, 10, 20, 40}
	chans := make([]<-chan error, len(targets))
	for i, tg := range targets {
		chans[i] = waitAsync(w, context.Background(), tg)
	}
	waitForWaiters(t, w, len(targets))

	w.Advance(20)
	for _, i := range []int{1, 2} {
		if err := expectDone(t, chans[i]); err != nil {
			t.Fatal(err)
		}
	}
	expectBlocked(t, chans[0])
	expectBlocked(t, chans[3])
	if w.numWaiters() != 2 {
		t.Fatalf("waiters = %d, want 2", w.numWaiters())
	}
	w.Advance(40)
	for _, i := range []int{0, 3} {
		if err := expectDone(t, chans[i]); err != nil {
			t.Fatal(err)
		}
	}
}

func TestAdvanceBackwardsIgnored(t *testing.T) {
	w := NewLsnWaiter(100)
	w.Advance(50)
	if w.Current() != 100 {
		t.Fatalf("after Advance(50): current = %s, want 100", w.Current())
	}
	w.Advance(100)
	if w.Current() != 100 {
		t.Fatalf("current = %s", w.Current())
	}
	w.Advance(101)
	if w.Current() != 101 {
		t.Fatalf("current = %s", w.Current())
	}
}

func TestWaitContextCancel(t *testing.T) {
	w := NewLsnWaiter(0)
	ctx, cancel := context.WithCancel(context.Background())
	ch := waitAsync(w, ctx, 10)
	waitForWaiters(t, w, 1)
	cancel()
	if err := expectDone(t, ch); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want Canceled", err)
	}
	if w.numWaiters() != 0 {
		t.Fatal("cancelled waiter was not removed")
	}

	w.Advance(10)
}

func TestWaitTimeout(t *testing.T) {
	w := NewLsnWaiter(0)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := w.Wait(ctx, 10); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
	if w.numWaiters() != 0 {
		t.Fatal("timed-out waiter was not removed")
	}
}

func TestWaitAlreadyCancelledContext(t *testing.T) {
	w := NewLsnWaiter(0)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := w.Wait(ctx, 10); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}

	if err := w.Wait(ctx, 0); err != nil {
		t.Fatalf("reached target with cancelled ctx: %v", err)
	}
}

func TestStop(t *testing.T) {
	w := NewLsnWaiter(100)
	a := waitAsync(w, context.Background(), 200)
	b := waitAsync(w, context.Background(), 300)
	waitForWaiters(t, w, 2)
	w.Stop()
	for _, ch := range []<-chan error{a, b} {
		if err := expectDone(t, ch); !errors.Is(err, ErrStopped) {
			t.Fatalf("err = %v, want ErrStopped", err)
		}
	}
	if err := w.Wait(context.Background(), 200); !errors.Is(err, ErrStopped) {
		t.Fatalf("wait after stop = %v", err)
	}

	if err := w.Wait(context.Background(), 100); err != nil {
		t.Fatalf("wait for reached LSN after stop = %v", err)
	}
}

func TestWaitStress(t *testing.T) {
	const waiters, last = 300, 1000
	w := NewLsnWaiter(0)
	var wg sync.WaitGroup
	r := rand.New(rand.NewSource(1))
	for i := 0; i < waiters; i++ {
		target := lsn.Lsn(1 + r.Intn(last))
		cancelEarly := r.Intn(3) == 0
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if cancelEarly {
				go func() { time.Sleep(time.Duration(rand.Intn(3)) * time.Millisecond); cancel() }()
			}
			err := w.Wait(ctx, target)
			switch {
			case err == nil:
				if w.Current() < target {
					t.Errorf("woken for %s but current is %s", target, w.Current())
				}
			case errors.Is(err, context.Canceled) && cancelEarly:
			default:
				t.Errorf("wait(%s) = %v", target, err)
			}
		}()
	}
	for l := lsn.Lsn(1); l <= last; l++ {
		w.Advance(l)
		if l%100 == 0 {
			time.Sleep(time.Millisecond)
		}
	}
	wg.Wait()
	if w.numWaiters() != 0 {
		t.Fatalf("%d waiters left", w.numWaiters())
	}
}
