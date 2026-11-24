package tenant

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/Fadil-Tao/naon/internal/pageserver/ids"
	"github.com/Fadil-Tao/naon/internal/pageserver/iox"
	"github.com/Fadil-Tao/naon/internal/pageserver/lsn"
	"github.com/Fadil-Tao/naon/internal/pageserver/remote"
	"github.com/Fadil-Tao/naon/internal/pageserver/timeline"
)

const (
	timelinesDir = "timelines"
	uninitSuffix = ".uninit"
)

var (
	ErrExists        = errors.New("tenant: timeline already exists")
	ErrNotFound      = errors.New("tenant: timeline not found")
	ErrRootExists    = errors.New("tenant: root timeline already exists")
	ErrBranchPoint   = errors.New("tenant: bad branch point")
	ErrStopped       = errors.New("tenant: stopped")
	ErrMissingParent = errors.New("tenant: timeline's ancestor is missing")
	ErrAncestorCycle = errors.New("tenant: ancestors form a cycle")
)

type Tenant struct {
	id      ids.TenantID
	dir     string
	cfg     timeline.Config
	plugins timeline.Plugins

	mu        sync.RWMutex
	timelines map[ids.TimelineID]*timeline.Timeline
	order     []ids.TimelineID
	stopped   bool
}

func Open(ctx context.Context, dir string, id ids.TenantID, cfg timeline.Config, plugins timeline.Plugins) (*Tenant, error) {
	tlDir := filepath.Join(dir, timelinesDir)
	if err := os.MkdirAll(tlDir, 0o755); err != nil {
		return nil, fmt.Errorf("tenant: create %s: %w", tlDir, err)
	}
	t := &Tenant{id: id, dir: dir, cfg: cfg, plugins: plugins, timelines: make(map[ids.TimelineID]*timeline.Timeline)}
	if plugins.Remote != nil {
		if err := t.restoreMissing(ctx); err != nil {
			return nil, err
		}
	}
	if err := t.loadAll(); err != nil {
		t.Stop()
		return nil, err
	}
	return t, nil
}

func (t *Tenant) ID() ids.TenantID { return t.id }

func (t *Tenant) timelinePath(id ids.TimelineID) string {
	return filepath.Join(t.dir, timelinesDir, id.String())
}

func (t *Tenant) loadAll() error {
	tlDir := filepath.Join(t.dir, timelinesDir)
	entries, err := os.ReadDir(tlDir)
	if err != nil {
		return fmt.Errorf("tenant: list %s: %w", tlDir, err)
	}
	metas := make(map[ids.TimelineID]timeline.Metadata)
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		if strings.HasSuffix(e.Name(), uninitSuffix) {
			if err := os.RemoveAll(filepath.Join(tlDir, e.Name())); err != nil {
				return err
			}
			continue
		}
		id, err := ids.ParseTimelineID(e.Name())
		if err != nil {
			continue
		}
		m, err := timeline.LoadMetadata(filepath.Join(tlDir, e.Name(), timeline.MetadataFileName))
		if err != nil {
			return fmt.Errorf("tenant: timeline %s: %w", id, err)
		}
		metas[id] = m
	}

	for len(metas) > 0 {
		progressed := false
		for id, m := range metas {
			var parent *timeline.Timeline
			if m.HasAncestor {
				p, ok := t.timelines[m.AncestorTimeline]
				if !ok {
					continue
				}
				parent = p
			}
			tl, err := timeline.Load(t.timelinePath(id), t.id, id, parent, t.cfg, t.plugins)
			if err != nil {
				return fmt.Errorf("tenant: load timeline %s: %w", id, err)
			}
			if parent != nil {
				parent.AddRetainLsn(m.AncestorLsn)
			}
			t.timelines[id] = tl
			t.order = append(t.order, id)
			delete(metas, id)
			progressed = true
		}
		if !progressed {
			for id, m := range metas {
				if _, known := metas[m.AncestorTimeline]; known {
					return fmt.Errorf("%w: %s", ErrAncestorCycle, id)
				}
				return fmt.Errorf("%w: %s wants %s", ErrMissingParent, id, m.AncestorTimeline)
			}
		}
	}
	return nil
}

func (t *Tenant) Timeline(id ids.TimelineID) (*timeline.Timeline, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	tl, ok := t.timelines[id]
	return tl, ok
}

func (t *Tenant) TimelineIDs() []ids.TimelineID {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return append([]ids.TimelineID(nil), t.order...)
}

func (t *Tenant) CreateRoot(id ids.TimelineID, startLsn lsn.Lsn, pgVersion uint32) (*timeline.Timeline, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stopped {
		return nil, ErrStopped
	}
	if _, ok := t.timelines[id]; ok {
		return nil, fmt.Errorf("%w: %s", ErrExists, id)
	}
	for _, tl := range t.timelines {
		if tl.Ancestor() == nil {
			return nil, fmt.Errorf("%w: %s", ErrRootExists, tl.ID())
		}
	}
	return t.install(id, timeline.RootMetadata(startLsn, pgVersion), nil)
}

func (t *Tenant) Branch(ctx context.Context, id, parentID ids.TimelineID, at lsn.Lsn) (*timeline.Timeline, error) {
	t.mu.RLock()
	parent, ok := t.timelines[parentID]
	t.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: parent %s", ErrNotFound, parentID)
	}
	if err := parent.WaitLsn(ctx, at); err != nil {
		return nil, fmt.Errorf("tenant: wait for branch point %s on %s: %w", at, parentID, err)
	}
	if err := parent.FlushAndWait(ctx); err != nil {
		return nil, fmt.Errorf("tenant: flush %s before branching: %w", parentID, err)
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stopped {
		return nil, ErrStopped
	}
	if _, ok := t.timelines[id]; ok {
		return nil, fmt.Errorf("%w: %s", ErrExists, id)
	}
	pm := parent.Metadata()
	if floor := lsn.MaxOf(parent.LatestGCCutoffLsn(), pm.InitdbLsn); at < floor {
		return nil, fmt.Errorf("%w: %s is below %s on %s", ErrBranchPoint, at, floor, parentID)
	}

	parent.AddRetainLsn(at)
	tl, err := t.install(id, timeline.BranchMetadata(pm, parentID, at), parent)
	if err != nil {
		parent.RemoveRetainLsn(at)
		return nil, err
	}
	return tl, nil
}

func (t *Tenant) install(id ids.TimelineID, meta timeline.Metadata, parent *timeline.Timeline) (*timeline.Timeline, error) {
	final := t.timelinePath(id)
	tmp := final + uninitSuffix
	if err := os.RemoveAll(tmp); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(tmp, 0o755); err != nil {
		return nil, fmt.Errorf("tenant: create %s: %w", tmp, err)
	}
	if err := timeline.SaveMetadata(filepath.Join(tmp, timeline.MetadataFileName), meta); err != nil {
		os.RemoveAll(tmp)
		return nil, err
	}
	if err := os.Rename(tmp, final); err != nil {
		os.RemoveAll(tmp)
		return nil, fmt.Errorf("tenant: publish timeline %s: %w", id, err)
	}
	if err := iox.SyncDir(filepath.Dir(final)); err != nil {
		return nil, err
	}
	tl, err := timeline.Load(final, t.id, id, parent, t.cfg, t.plugins)
	if err != nil {
		return nil, err
	}
	t.timelines[id] = tl
	t.order = append(t.order, id)
	return tl, nil
}

func (t *Tenant) Stop() {
	t.mu.Lock()
	t.stopped = true
	order := append([]ids.TimelineID(nil), t.order...)
	t.mu.Unlock()
	for i := len(order) - 1; i >= 0; i-- {
		t.timelines[order[i]].Stop()
	}
}

func (t *Tenant) Shutdown(ctx context.Context) error {
	t.mu.Lock()
	t.stopped = true
	order := append([]ids.TimelineID(nil), t.order...)
	t.mu.Unlock()
	var first error
	for i := len(order) - 1; i >= 0; i-- {
		if err := t.timelines[order[i]].Shutdown(ctx); err != nil && first == nil {
			first = fmt.Errorf("tenant: shutdown %s: %w", order[i], err)
		}
	}
	return first
}

func (t *Tenant) restoreMissing(ctx context.Context) error {
	names, err := t.plugins.Remote.List(ctx, remote.TimelinesPath(t.id))
	if err != nil {
		return fmt.Errorf("tenant: list remote timelines: %w", err)
	}
	for _, name := range names {
		id, err := ids.ParseTimelineID(name)
		if err != nil {
			continue
		}
		final := t.timelinePath(id)
		if _, err := os.Stat(filepath.Join(final, timeline.MetadataFileName)); err == nil {
			continue
		}
		tmp := final + uninitSuffix
		if err := os.RemoveAll(tmp); err != nil {
			return err
		}
		if _, err := timeline.Restore(ctx, t.plugins.Remote, tmp, t.id, id); err != nil {
			return fmt.Errorf("tenant: restore %s: %w", id, err)
		}
		if err := os.RemoveAll(final); err != nil {
			return err
		}
		if err := os.Rename(tmp, final); err != nil {
			return err
		}
	}
	return iox.SyncDir(filepath.Join(t.dir, timelinesDir))
}
