package timeline

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Fadil-Tao/naon/internal/pageserver/ids"
	"github.com/Fadil-Tao/naon/internal/pageserver/layer"
	"github.com/Fadil-Tao/naon/internal/pageserver/layermap"
	"github.com/Fadil-Tao/naon/internal/pageserver/lsn"
)

type Config struct {
	CheckpointDistance uint64
	CheckpointTimeout  time.Duration

	MaxOpenLayerBytes uint64

	FlushRetryDelay time.Duration

	Redo Redo

	failpoint func(name string) error
}

func DefaultConfig() Config {
	return Config{
		CheckpointDistance: 256 << 20,
		CheckpointTimeout:  10 * time.Minute,
		MaxOpenLayerBytes:  512 << 20,
		FlushRetryDelay:    time.Second,
	}
}

type Timeline struct {
	tenant ids.TenantID
	id     ids.TimelineID
	dir    string
	cfg    Config

	ancestor    *Timeline
	ancestorLsn lsn.Lsn

	retainMu sync.Mutex
	retain   map[lsn.Lsn]int

	layers     *layermap.LayerMap
	lastRecord *LsnWaiter
	prevRecord atomic.Uint64
	flushed    *LsnWaiter
	gcCutoff   atomic.Uint64

	writeMu        sync.Mutex
	lastFreezeAt   lsn.Lsn
	lastFreezeTime time.Time
	stopped        bool

	metaMu    sync.Mutex
	meta      Metadata
	metaDirty bool

	pendingMu sync.Mutex
	prevAt    map[layer.Desc]lsn.Lsn

	flushCh  chan struct{}
	stopCh   chan struct{}
	done     chan struct{}
	stopOnce sync.Once

	bgErr atomic.Pointer[error]
}

func newTimeline(dir string, tenant ids.TenantID, id ids.TimelineID, meta Metadata, ancestor *Timeline, cfg Config) *Timeline {
	dcl := meta.DiskConsistentLsn
	t := &Timeline{
		tenant: tenant, id: id, dir: dir, cfg: cfg,
		ancestor:       ancestor,
		ancestorLsn:    meta.AncestorLsn,
		retain:         make(map[lsn.Lsn]int),
		layers:         layermap.New(),
		lastRecord:     NewLsnWaiter(dcl),
		flushed:        NewLsnWaiter(dcl),
		lastFreezeAt:   dcl + 1,
		lastFreezeTime: time.Now(),
		meta:           meta,
		prevAt:         make(map[layer.Desc]lsn.Lsn),
		flushCh:        make(chan struct{}, 1),
		stopCh:         make(chan struct{}),
		done:           make(chan struct{}),
	}
	t.prevRecord.Store(uint64(meta.PrevRecordLsn))
	t.gcCutoff.Store(uint64(meta.LatestGCCutoffLsn))
	go t.flushLoop()
	return t
}

func (t *Timeline) metadataPath() string { return filepath.Join(t.dir, MetadataFileName) }

func (t *Timeline) TenantID() ids.TenantID     { return t.tenant }
func (t *Timeline) ID() ids.TimelineID         { return t.id }
func (t *Timeline) Layers() *layermap.LayerMap { return t.layers }

func (t *Timeline) Ancestor() *Timeline { return t.ancestor }

func (t *Timeline) AncestorLsn() lsn.Lsn { return t.ancestorLsn }

func (t *Timeline) AddRetainLsn(l lsn.Lsn) {
	t.retainMu.Lock()
	t.retain[l]++
	t.retainMu.Unlock()
}

func (t *Timeline) RemoveRetainLsn(l lsn.Lsn) {
	t.retainMu.Lock()
	defer t.retainMu.Unlock()
	if t.retain[l] <= 1 {
		delete(t.retain, l)
		return
	}
	t.retain[l]--
}

func (t *Timeline) RetainLsns() []lsn.Lsn {
	t.retainMu.Lock()
	defer t.retainMu.Unlock()
	out := make([]lsn.Lsn, 0, len(t.retain))
	for l := range t.retain {
		out = append(out, l)
	}
	slices.Sort(out)
	return out
}

func (t *Timeline) LastRecordLsn() lsn.Lsn { return t.lastRecord.Current() }

func (t *Timeline) PrevRecordLsn() lsn.Lsn { return lsn.Lsn(t.prevRecord.Load()) }

func (t *Timeline) DiskConsistentLsn() lsn.Lsn { return t.flushed.Current() }

func (t *Timeline) Metadata() Metadata {
	t.metaMu.Lock()
	defer t.metaMu.Unlock()
	return t.meta
}

func (t *Timeline) BackgroundErr() error {
	if p := t.bgErr.Load(); p != nil {
		return *p
	}
	return nil
}

func (t *Timeline) setBackgroundErr(err error) { t.bgErr.Store(&err) }

func (t *Timeline) WaitLsn(ctx context.Context, target lsn.Lsn) error {
	return t.lastRecord.Wait(ctx, target)
}

func (t *Timeline) Stop() {
	t.stopOnce.Do(func() {
		t.writeMu.Lock()
		t.stopped = true
		t.writeMu.Unlock()
		t.lastRecord.Stop()
		t.flushed.Stop()
		close(t.stopCh)
		<-t.done
		t.closeLayers()
	})
}

func (t *Timeline) Shutdown(ctx context.Context) error {
	err := t.FlushAndWait(ctx)
	if errors.Is(err, ErrStopped) {
		err = nil
	}
	t.Stop()
	return err
}

func (t *Timeline) closeLayers() {
	snap := t.layers.Snapshot()
	if o, ok := snap.Open(); ok {
		o.Layer.Close()
	}
	for _, e := range snap.Frozen() {
		e.Layer.Close()
	}
	for _, e := range snap.Historic() {
		e.Layer.Close()
	}
}
