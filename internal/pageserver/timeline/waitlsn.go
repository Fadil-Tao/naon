package timeline

import (
	"container/heap"
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/Fadil-Tao/naon/internal/pageserver/lsn"
)

var ErrStopped = errors.New("timeline: stopped")

type LsnWaiter struct {
	cur atomic.Uint64

	mu      sync.Mutex
	waiters waiterHeap
	stopped bool
}

func NewLsnWaiter(initial lsn.Lsn) *LsnWaiter {
	w := &LsnWaiter{}
	w.cur.Store(uint64(initial))
	return w
}

func (w *LsnWaiter) Current() lsn.Lsn { return lsn.Lsn(w.cur.Load()) }

func (w *LsnWaiter) Advance(l lsn.Lsn) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if l <= w.Current() {
		return
	}
	w.cur.Store(uint64(l))
	for len(w.waiters) > 0 && w.waiters[0].target <= l {
		close(heap.Pop(&w.waiters).(*waiter).ch)
	}
}

func (w *LsnWaiter) Wait(ctx context.Context, target lsn.Lsn) error {
	if w.Current() >= target {
		return nil
	}
	w.mu.Lock()
	if w.stopped {
		w.mu.Unlock()
		return ErrStopped
	}
	if w.Current() >= target {
		w.mu.Unlock()
		return nil
	}
	wt := &waiter{target: target, ch: make(chan struct{})}
	heap.Push(&w.waiters, wt)
	w.mu.Unlock()

	select {
	case <-wt.ch:
		return wt.err
	case <-ctx.Done():
		w.mu.Lock()
		defer w.mu.Unlock()
		if wt.idx < 0 {
			return wt.err
		}
		heap.Remove(&w.waiters, wt.idx)
		return ctx.Err()
	}
}

func (w *LsnWaiter) Stop() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.stopped = true
	for len(w.waiters) > 0 {
		wt := heap.Pop(&w.waiters).(*waiter)
		wt.err = ErrStopped
		close(wt.ch)
	}
}

func (w *LsnWaiter) numWaiters() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.waiters)
}

type waiter struct {
	target lsn.Lsn
	ch     chan struct{}
	err    error
	idx    int
}

type waiterHeap []*waiter

func (h waiterHeap) Len() int           { return len(h) }
func (h waiterHeap) Less(i, j int) bool { return h[i].target < h[j].target }
func (h waiterHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].idx, h[j].idx = i, j
}
func (h *waiterHeap) Push(x any) {
	wt := x.(*waiter)
	wt.idx = len(*h)
	*h = append(*h, wt)
}
func (h *waiterHeap) Pop() any {
	old := *h
	n := len(old)
	wt := old[n-1]
	old[n-1] = nil
	wt.idx = -1
	*h = old[:n-1]
	return wt
}
