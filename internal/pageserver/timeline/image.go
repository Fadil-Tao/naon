package timeline

import (
	"context"
	"errors"
	"fmt"

	"github.com/Fadil-Tao/naon/internal/pageserver/key"
	"github.com/Fadil-Tao/naon/internal/pageserver/layer/image"
	"github.com/Fadil-Tao/naon/internal/pageserver/layermap"
	"github.com/Fadil-Tao/naon/internal/pageserver/lsn"
)

type KeySpaceFunc func(ctx context.Context, t *Timeline, at lsn.Lsn) (key.KeySpace, error)

var ErrNoKeySpace = errors.New("timeline: no KeySpace configured")

func (t *Timeline) CreateImageLayers(ctx context.Context) (int, error) {
	if t.plugins.KeySpace == nil {
		return 0, ErrNoKeySpace
	}
	t.imageMu.Lock()
	defer t.imageMu.Unlock()

	at := t.DiskConsistentLsn()
	ks, err := t.plugins.KeySpace(ctx, t, at)
	if err != nil {
		return 0, fmt.Errorf("timeline: keyspace @ %s: %w", at, err)
	}
	created := 0
	for _, part := range ks.Partition(t.cfg.ImageTargetSize) {
		if err := ctx.Err(); err != nil {
			return created, err
		}
		start, _ := part.Start()
		end, _ := part.End()
		r := key.KeyRange{Start: start, End: end}
		if !t.needsImage(r, at) {
			continue
		}
		if err := t.createImageLayer(part, r, at); err != nil {
			return created, err
		}
		created++
	}
	return created, nil
}

func covers(outer, inner key.KeyRange) bool {
	return outer.Start.Compare(inner.Start) <= 0 && inner.End.Compare(outer.End) <= 0
}

func (t *Timeline) needsImage(r key.KeyRange, at lsn.Lsn) bool {
	hist := t.layers.Snapshot().Historic()
	var newest lsn.Lsn
	hasImage := false
	for _, e := range hist {
		d := e.Desc
		if !d.IsDelta && d.ImageLsn() <= at && covers(d.KeyRange, r) && (!hasImage || d.ImageLsn() > newest) {
			newest, hasImage = d.ImageLsn(), true
		}
	}
	deltas := 0
	for _, e := range hist {
		d := e.Desc
		if d.IsDelta && d.KeyRange.Overlaps(r) && d.LsnRange.Start <= at && (!hasImage || d.LsnRange.End-1 > newest) {
			deltas++
		}
	}
	return deltas >= t.cfg.ImageCreationThreshold
}

func (t *Timeline) createImageLayer(ks key.KeySpace, r key.KeyRange, at lsn.Lsn) error {
	w, err := image.NewWriter(t.dir, t.tenant, t.id, r, at)
	if err != nil {
		return err
	}
	defer w.Abort()
	for _, kr := range ks.Ranges() {
		for k := kr.Start; k.Less(kr.End); {
			page, err := t.get(k, at)
			switch {
			case errors.Is(err, ErrNotFound):
			case err != nil:
				return fmt.Errorf("timeline: image %s @ %s: %w", k, at, err)
			default:
				if err := w.Put(k, page); err != nil {
					return err
				}
			}
			next, err := k.Next()
			if err != nil {
				break
			}
			k = next
		}
	}
	desc, path, err := w.Finish(0)
	if err != nil {
		return err
	}
	reader, err := image.Open(path)
	if err != nil {
		return err
	}
	err = t.layers.Update(func(u *layermap.Updater) error {
		return u.InsertHistoric(layermap.Entry{Desc: desc, Layer: reader})
	})
	if err != nil {
		reader.Close()
		return err
	}
	t.requestUpload()
	return nil
}
