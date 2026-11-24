package timeline

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Fadil-Tao/naon/internal/pageserver/layer"
	"github.com/Fadil-Tao/naon/internal/pageserver/layer/delta"
	"github.com/Fadil-Tao/naon/internal/pageserver/layer/inmemory"
	"github.com/Fadil-Tao/naon/internal/pageserver/layermap"
)

const (
	fpFlushStart         = "flush:start"
	fpAfterL0Written     = "flush:after-l0-written"
	fpAfterLayerSwap     = "flush:after-layermap-swap"
	fpAfterMetadataSaved = "flush:after-metadata"
)

var errSimulatedCrash = errors.New("timeline: simulated crash")

func (t *Timeline) fp(name string) error {
	if t.cfg.failpoint == nil {
		return nil
	}
	return t.cfg.failpoint(name)
}

func (t *Timeline) Freeze() (bool, error) {
	t.writeMu.Lock()
	defer t.writeMu.Unlock()
	if t.stopped {
		return false, ErrStopped
	}
	return t.freezeLocked()
}

func (t *Timeline) FreezeIfStale(now time.Time) (bool, error) {
	t.writeMu.Lock()
	defer t.writeMu.Unlock()
	if t.stopped || t.cfg.CheckpointTimeout <= 0 || now.Sub(t.lastFreezeTime) < t.cfg.CheckpointTimeout {
		return false, nil
	}
	return t.freezeLocked()
}

func (t *Timeline) maybeFreezeLocked() error {
	e, ok := t.layers.Snapshot().Open()
	if !ok {
		return nil
	}
	last := t.lastRecord.Current()
	byDistance := t.cfg.CheckpointDistance > 0 && last > t.lastFreezeAt &&
		uint64(last-t.lastFreezeAt) >= t.cfg.CheckpointDistance
	bySize := t.cfg.MaxOpenLayerBytes > 0 && e.Layer.(*inmemory.Layer).Size() >= t.cfg.MaxOpenLayerBytes
	if !byDistance && !bySize {
		return nil
	}
	_, err := t.freezeLocked()
	return err
}

func (t *Timeline) freezeLocked() (bool, error) {
	e, ok := t.layers.Snapshot().Open()
	if !ok {
		return false, nil
	}
	l := e.Layer.(*inmemory.Layer)
	end := t.lastRecord.Current() + 1

	frozenDesc := e.Desc
	frozenDesc.LsnRange.End = end

	t.pendingMu.Lock()
	t.prevAt[frozenDesc] = t.PrevRecordLsn()
	t.pendingMu.Unlock()

	if err := l.Freeze(end); err != nil {
		t.dropPending(frozenDesc)
		return false, fmt.Errorf("timeline: freeze: %w", err)
	}
	err := t.layers.Update(func(u *layermap.Updater) error { return u.FreezeOpen(end) })
	if err != nil {
		t.dropPending(frozenDesc)
		return false, fmt.Errorf("timeline: freeze: %w", err)
	}
	t.lastFreezeAt = end
	t.lastFreezeTime = time.Now()
	select {
	case t.flushCh <- struct{}{}:
	default:
	}
	return true, nil
}

func (t *Timeline) dropPending(d layer.Desc) {
	t.pendingMu.Lock()
	delete(t.prevAt, d)
	t.pendingMu.Unlock()
}

func (t *Timeline) FlushAndWait(ctx context.Context) error {
	t.writeMu.Lock()
	if t.stopped {
		t.writeMu.Unlock()
		return ErrStopped
	}
	_, err := t.freezeLocked()
	target := t.lastFreezeAt - 1
	t.writeMu.Unlock()
	if err != nil {
		return err
	}
	return t.flushed.Wait(ctx, target)
}

func (t *Timeline) flushLoop() {
	defer close(t.done)
	for {
		select {
		case <-t.stopCh:
			return
		case <-t.flushCh:
		}
		for {
			idle, err := t.flushNext()
			if errors.Is(err, errSimulatedCrash) {
				return
			}
			if err != nil {
				t.setBackgroundErr(err)
				select {
				case <-t.stopCh:
					return
				case <-time.After(t.cfg.FlushRetryDelay):
				}
				continue
			}
			if idle {
				break
			}
		}
	}
}

func (t *Timeline) flushNext() (idle bool, err error) {
	if err := t.persistMetadata(); err != nil {
		return false, err
	}
	frozen := t.layers.Snapshot().Frozen()
	if len(frozen) == 0 {
		return true, nil
	}
	e := frozen[0]
	mem := e.Layer.(*inmemory.Layer)

	if err := t.fp(fpFlushStart); err != nil {
		return false, err
	}

	desc, path, err := mem.WriteToL0(t.dir)
	if err != nil {
		return false, fmt.Errorf("timeline: flush %s: %w", e.Desc, err)
	}
	if err := t.fp(fpAfterL0Written); err != nil {
		return false, err
	}

	reader, err := delta.Open(path)
	if err != nil {
		return false, fmt.Errorf("timeline: open flushed layer: %w", err)
	}
	err = t.layers.Update(func(u *layermap.Updater) error {
		if err := u.InsertHistoric(layermap.Entry{Desc: desc, Layer: reader}); err != nil {
			return err
		}
		return u.RemoveFrozen(e.Desc)
	})
	if err != nil {
		reader.Close()
		return false, fmt.Errorf("timeline: swap flushed layer: %w", err)
	}

	mem.Remove()

	t.pendingMu.Lock()
	prev := t.prevAt[e.Desc]
	delete(t.prevAt, e.Desc)
	t.pendingMu.Unlock()
	t.metaMu.Lock()
	t.meta.DiskConsistentLsn = desc.LsnRange.End - 1
	t.meta.PrevRecordLsn = prev
	t.metaDirty = true
	t.metaMu.Unlock()
	if err := t.fp(fpAfterLayerSwap); err != nil {
		return false, err
	}
	if err := t.persistMetadata(); err != nil {
		return false, err
	}
	if err := t.fp(fpAfterMetadataSaved); err != nil {
		return false, err
	}
	return false, nil
}

func (t *Timeline) persistMetadata() error {
	t.metaMu.Lock()
	defer t.metaMu.Unlock()
	if !t.metaDirty {
		return nil
	}
	if err := SaveMetadata(t.metadataPath(), t.meta); err != nil {
		return fmt.Errorf("timeline: save metadata: %w", err)
	}
	t.metaDirty = false
	t.flushed.Advance(t.meta.DiskConsistentLsn)
	select {
	case t.flushedCh <- struct{}{}:
	default:
	}
	return nil
}
