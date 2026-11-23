package timeline

import (
	"errors"
	"fmt"

	"github.com/Fadil-Tao/naon/internal/pageserver/layer/inmemory"
	"github.com/Fadil-Tao/naon/internal/pageserver/layermap"
	"github.com/Fadil-Tao/naon/internal/pageserver/lsn"
)

var ErrLsnBackwards = errors.New("timeline: LSN not above last_record_lsn")

func (t *Timeline) PutBatch(puts []inmemory.Put, endLsn lsn.Lsn) error {
	t.writeMu.Lock()
	defer t.writeMu.Unlock()
	if t.stopped {
		return ErrStopped
	}

	last := t.lastRecord.Current()
	if endLsn < last {
		return fmt.Errorf("%w: end %s < last_record_lsn %s", ErrLsnBackwards, endLsn, last)
	}
	for _, p := range puts {
		if p.Lsn <= last {
			return fmt.Errorf("%w: %s @ %s, last_record_lsn %s", ErrLsnBackwards, p.Key, p.Lsn, last)
		}
		if p.Lsn > endLsn {
			return fmt.Errorf("timeline: put %s @ %s is above the batch end %s", p.Key, p.Lsn, endLsn)
		}
	}

	if len(puts) > 0 {
		open, err := t.openLayer()
		if err != nil {
			return err
		}
		if err := open.PutBatch(puts); err != nil {
			return err
		}
	}
	t.prevRecord.Store(uint64(last))
	t.lastRecord.Advance(endLsn)

	if err := t.maybeFreezeLocked(); err != nil {
		t.setBackgroundErr(err)
	}
	return nil
}

func (t *Timeline) openLayer() (*inmemory.Layer, error) {
	if e, ok := t.layers.Snapshot().Open(); ok {
		return e.Layer.(*inmemory.Layer), nil
	}
	l, err := inmemory.New(t.dir, t.tenant, t.id, t.lastFreezeAt)
	if err != nil {
		return nil, err
	}
	err = t.layers.Update(func(u *layermap.Updater) error {
		return u.SetOpen(layermap.Entry{Desc: l.Desc(), InMemory: true, Layer: l})
	})
	if err != nil {
		l.Close()
		return nil, err
	}
	return l, nil
}
