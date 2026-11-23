package timeline

import (
	"context"
	"errors"
	"fmt"

	"github.com/Fadil-Tao/naon/internal/pageserver/key"
	"github.com/Fadil-Tao/naon/internal/pageserver/lsn"
	"github.com/Fadil-Tao/naon/internal/pageserver/value"
)

var (
	ErrNotFound      = errors.New("timeline: key not found at this LSN")
	ErrBelowGCCutoff = errors.New("timeline: LSN is below the GC cutoff")
	ErrNoRedo        = errors.New("timeline: no Redo configured")

	ErrCorrupt = errors.New("timeline: layers hold WAL records without a base image")

	ErrTooManyKeys = errors.New("timeline: too many keys in one vectored read")
)

const MaxVectoredKeys = 1 << 16

func (t *Timeline) LatestGCCutoffLsn() lsn.Lsn { return lsn.Lsn(t.gcCutoff.Load()) }

func (t *Timeline) Get(ctx context.Context, k key.Key, at lsn.Lsn) ([]byte, error) {
	select {
	case <-t.stopCh:
		return nil, ErrStopped
	default:
	}
	if c := t.LatestGCCutoffLsn(); at < c {
		return nil, fmt.Errorf("%w: %s < %s", ErrBelowGCCutoff, at, c)
	}
	if err := t.WaitLsn(ctx, at); err != nil {
		return nil, err
	}
	return t.get(k, at)
}

func (t *Timeline) get(k key.Key, at lsn.Lsn) ([]byte, error) {
	var st value.ReconstructState
	end := at + 1
	for cur := t; cur != nil && !st.IsComplete(); cur = cur.ancestor {
		if err := cur.readLayers(k, at, &end, &st); err != nil {
			return nil, err
		}
		if cur.ancestor != nil {
			end = lsn.Min(end, cur.ancestorLsn+1)
		}
	}

	if st.IsEmpty() || st.IsAbsent() {
		return nil, fmt.Errorf("%w: %s @ %s", ErrNotFound, k, at)
	}
	if !st.IsComplete() {
		return nil, fmt.Errorf("%w: %s @ %s", ErrCorrupt, k, at)
	}
	in := st.Finish()
	if len(in.Records) == 0 {
		return in.Image, nil
	}
	if t.plugins.Redo == nil {
		return nil, ErrNoRedo
	}
	page, err := t.plugins.Redo.Reconstruct(k, at, in)
	if err != nil {
		return nil, fmt.Errorf("timeline: redo %s @ %s: %w", k, at, err)
	}
	return page, nil
}

type PageResult struct {
	Key  key.Key
	Page []byte
	Err  error
}

func (t *Timeline) GetVectored(ctx context.Context, ks key.KeySpace, at lsn.Lsn) ([]PageResult, error) {
	if n := ks.TotalKeys(); !n.IsInt64() || n.Int64() > MaxVectoredKeys {
		return nil, fmt.Errorf("%w: %s keys, limit %d", ErrTooManyKeys, n, MaxVectoredKeys)
	}
	select {
	case <-t.stopCh:
		return nil, ErrStopped
	default:
	}
	if c := t.LatestGCCutoffLsn(); at < c {
		return nil, fmt.Errorf("%w: %s < %s", ErrBelowGCCutoff, at, c)
	}
	if err := t.WaitLsn(ctx, at); err != nil {
		return nil, err
	}
	var out []PageResult
	for _, r := range ks.Ranges() {
		for k := r.Start; k.Less(r.End); {
			page, err := t.get(k, at)
			out = append(out, PageResult{Key: k, Page: page, Err: err})
			next, nerr := k.Next()
			if nerr != nil {
				break
			}
			k = next
		}
	}
	return out, nil
}

func (t *Timeline) readLayers(k key.Key, at lsn.Lsn, end *lsn.Lsn, st *value.ReconstructState) error {
	snap := t.layers.Snapshot()
	for {
		res, ok := snap.Search(k, *end)
		if !ok {
			return nil
		}
		if res.LsnFloor >= *end {
			return fmt.Errorf("timeline: layer map returned floor %s at end %s for %s", res.LsnFloor, *end, k)
		}
		rng := lsn.Range{Start: res.LsnFloor, End: *end}
		if err := res.Entry.Layer.GetValueReconstructData(k, rng, st); err != nil {
			return fmt.Errorf("timeline: read %s @ %s from %s: %w", k, at, res.Entry.Desc, err)
		}
		if st.IsComplete() {
			return nil
		}
		*end = res.LsnFloor
	}
}
