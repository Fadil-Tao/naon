package timeline

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/Fadil-Tao/naon/internal/pageserver/ids"
	"github.com/Fadil-Tao/naon/internal/pageserver/iox"
	"github.com/Fadil-Tao/naon/internal/pageserver/layer"
	"github.com/Fadil-Tao/naon/internal/pageserver/layer/delta"
	"github.com/Fadil-Tao/naon/internal/pageserver/layer/image"
	"github.com/Fadil-Tao/naon/internal/pageserver/layer/inmemory"
	"github.com/Fadil-Tao/naon/internal/pageserver/layermap"
	"github.com/Fadil-Tao/naon/internal/pageserver/lsn"
)

const oldSuffix = ".old"

var (
	ErrAncestorMismatch = errors.New("timeline: ancestor does not match the metadata")
	ErrNeedsAncestor    = errors.New("timeline: metadata has an ancestor; open it with Load")
)

func RootMetadata(initdbLsn lsn.Lsn, pgVersion uint32) Metadata {
	return Metadata{DiskConsistentLsn: initdbLsn, InitdbLsn: initdbLsn, PgVersion: pgVersion}
}

func BranchMetadata(parent Metadata, parentID ids.TimelineID, at lsn.Lsn) Metadata {
	return Metadata{
		DiskConsistentLsn: at,
		HasAncestor:       true,
		AncestorTimeline:  parentID,
		AncestorLsn:       at,
		LatestGCCutoffLsn: at,
		InitdbLsn:         parent.InitdbLsn,
		PgVersion:         parent.PgVersion,
	}
}

func Create(dir string, tenant ids.TenantID, id ids.TimelineID, meta Metadata, cfg Config, plugins Plugins) (*Timeline, error) {
	if err := meta.Validate(); err != nil {
		return nil, err
	}
	if meta.HasAncestor {
		return nil, ErrNeedsAncestor
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("timeline: create dir: %w", err)
	}
	path := filepath.Join(dir, MetadataFileName)
	if _, err := os.Stat(path); err == nil {
		return nil, fmt.Errorf("timeline: %s: %w", path, fs.ErrExist)
	}
	if err := SaveMetadata(path, meta); err != nil {
		return nil, err
	}
	return newTimeline(dir, tenant, id, meta, nil, cfg, plugins), nil
}

func Load(dir string, tenant ids.TenantID, id ids.TimelineID, ancestor *Timeline, cfg Config, plugins Plugins) (*Timeline, error) {
	if _, err := iox.CleanupTemp(dir); err != nil {
		return nil, err
	}
	if _, err := inmemory.RemoveLeftovers(dir); err != nil {
		return nil, err
	}
	meta, err := LoadMetadata(filepath.Join(dir, MetadataFileName))
	if err != nil {
		return nil, err
	}
	switch {
	case meta.HasAncestor && ancestor == nil:
		return nil, fmt.Errorf("%w: %s has ancestor %s, none given", ErrAncestorMismatch, id, meta.AncestorTimeline)
	case !meta.HasAncestor && ancestor != nil:
		return nil, fmt.Errorf("%w: %s has no ancestor, %s given", ErrAncestorMismatch, id, ancestor.ID())
	case ancestor != nil && ancestor.ID() != meta.AncestorTimeline:
		return nil, fmt.Errorf("%w: wants %s, given %s", ErrAncestorMismatch, meta.AncestorTimeline, ancestor.ID())
	}
	dcl := meta.DiskConsistentLsn

	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("timeline: list %s: %w", dir, err)
	}
	var entries []layermap.Entry
	closeAll := func() {
		for _, e := range entries {
			e.Layer.Close()
		}
	}
	renamed := false
	for _, f := range files {
		if !f.Type().IsRegular() {
			continue
		}
		desc, err := layer.ParseFileName(f.Name())
		if err != nil {
			continue
		}
		path := filepath.Join(dir, f.Name())
		if desc.LsnRange.End-1 > dcl {
			if err := os.Rename(path, path+oldSuffix); err != nil {
				closeAll()
				return nil, fmt.Errorf("timeline: discard %s: %w", f.Name(), err)
			}
			renamed = true
			continue
		}
		r, sum, err := openLayerFile(path, desc)
		if err != nil {
			closeAll()
			return nil, err
		}
		entries = append(entries, layermap.Entry{Desc: desc, Layer: r})
		if sum.Tenant != tenant || sum.Timeline != id {
			closeAll()
			return nil, fmt.Errorf("timeline: %s belongs to tenant %s timeline %s, not %s/%s",
				f.Name(), sum.Tenant, sum.Timeline, tenant, id)
		}
	}
	if renamed {
		if err := iox.SyncDir(dir); err != nil {
			closeAll()
			return nil, err
		}
	}

	t := newTimeline(dir, tenant, id, meta, ancestor, cfg, plugins)
	err = t.layers.Update(func(u *layermap.Updater) error {
		for _, e := range entries {
			if err := u.InsertHistoric(e); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Stop()
		closeAll()
		return nil, fmt.Errorf("timeline: load layers: %w", err)
	}
	return t, nil
}

func openLayerFile(path string, d layer.Desc) (layer.Reader, layer.Summary, error) {
	if d.IsDelta {
		r, err := delta.Open(path)
		if err != nil {
			return nil, layer.Summary{}, err
		}
		return r, r.Summary(), nil
	}
	r, err := image.Open(path)
	if err != nil {
		return nil, layer.Summary{}, err
	}
	return r, r.Summary(), nil
}
