package layermap

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/Fadil-Tao/naon/internal/pageserver/key"
	"github.com/Fadil-Tao/naon/internal/pageserver/layer"
	"github.com/Fadil-Tao/naon/internal/pageserver/lsn"
)

var (
	ErrInvalid    = errors.New("layermap: invalid entry")
	ErrOpenExists = errors.New("layermap: an open layer already exists")
	ErrNoOpen     = errors.New("layermap: no open layer")
	ErrNotFound   = errors.New("layermap: layer not found")
	ErrDuplicate  = errors.New("layermap: layer already present")
	ErrNotOldest  = errors.New("layermap: not the oldest frozen layer")
)

type Entry struct {
	Desc     layer.Desc
	InMemory bool
	Layer    layer.Reader
}

type Snapshot struct {
	open     *Entry
	frozen   []Entry
	historic []Entry
}

func (s *Snapshot) Open() (Entry, bool) {
	if s.open == nil {
		return Entry{}, false
	}
	return *s.open, true
}

func (s *Snapshot) Frozen() []Entry { return slices.Clone(s.frozen) }

func (s *Snapshot) Historic() []Entry { return slices.Clone(s.historic) }

type SearchResult struct {
	Entry    Entry
	LsnFloor lsn.Lsn
}

func (s *Snapshot) Search(k key.Key, end lsn.Lsn) (SearchResult, bool) {
	mem := s.newestMemory(end)

	var delta, image *Entry
	for i := range s.historic {
		e := &s.historic[i]
		if e.Desc.LsnRange.Start >= end {
			break
		}
		if !e.Desc.KeyRange.Contains(k) {
			continue
		}

		if e.Desc.IsDelta {
			if delta == nil || e.Desc.LsnRange.End > delta.Desc.LsnRange.End {
				delta = e
			}
		} else if image == nil || e.Desc.LsnRange.End > image.Desc.LsnRange.End {
			image = e
		}
	}

	reach := func(e *Entry) lsn.Lsn { return lsn.Min(e.Desc.LsnRange.End, end) }

	best := image
	for _, c := range []*Entry{delta, mem} {
		if c != nil && (best == nil || reach(c) > reach(best)) {
			best = c
		}
	}
	if best == nil {
		return SearchResult{}, false
	}

	if best == image {
		return SearchResult{Entry: *best, LsnFloor: best.Desc.ImageLsn()}, true
	}
	floor := best.Desc.LsnRange.Start
	if image != nil {
		floor = lsn.MaxOf(floor, image.Desc.ImageLsn()+1)
	}
	return SearchResult{Entry: *best, LsnFloor: floor}, true
}

func (s *Snapshot) newestMemory(end lsn.Lsn) *Entry {
	if s.open != nil && s.open.Desc.LsnRange.Start < end {
		return s.open
	}
	for i := len(s.frozen) - 1; i >= 0; i-- {
		if s.frozen[i].Desc.LsnRange.Start < end {
			return &s.frozen[i]
		}
	}
	return nil
}

type LayerMap struct {
	mu  sync.Mutex
	cur atomic.Pointer[Snapshot]
}

func New() *LayerMap {
	m := &LayerMap{}
	m.cur.Store(&Snapshot{})
	return m
}

func (m *LayerMap) Snapshot() *Snapshot { return m.cur.Load() }

func (m *LayerMap) Update(fn func(*Updater) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	cur := m.cur.Load()
	u := &Updater{s: Snapshot{
		open:     cur.open,
		frozen:   slices.Clone(cur.frozen),
		historic: slices.Clone(cur.historic),
	}}
	if err := fn(u); err != nil {
		return err
	}
	next := u.s
	m.cur.Store(&next)
	return nil
}

type Updater struct{ s Snapshot }

func (u *Updater) SetOpen(e Entry) error {
	switch {
	case u.s.open != nil:
		return ErrOpenExists
	case !e.InMemory || e.Layer == nil:
		return fmt.Errorf("%w: open layer must be in-memory with a layer", ErrInvalid)
	case e.Desc.KeyRange != key.FullRange():
		return fmt.Errorf("%w: open layer must cover all keys", ErrInvalid)
	case e.Desc.LsnRange.End != lsn.Max:
		return fmt.Errorf("%w: open layer must end at Max", ErrInvalid)
	}
	if n := len(u.s.frozen); n > 0 && e.Desc.LsnRange.Start < u.s.frozen[n-1].Desc.LsnRange.End {
		return fmt.Errorf("%w: open layer starts at %s, before the newest frozen layer ends (%s)",
			ErrInvalid, e.Desc.LsnRange.Start, u.s.frozen[n-1].Desc.LsnRange.End)
	}
	u.s.open = &e
	return nil
}

func (u *Updater) FreezeOpen(end lsn.Lsn) error {
	if u.s.open == nil {
		return ErrNoOpen
	}
	e := *u.s.open
	if end <= e.Desc.LsnRange.Start || end == lsn.Max {
		return fmt.Errorf("%w: cannot freeze [%s, ...) at %s", ErrInvalid, e.Desc.LsnRange.Start, end)
	}
	e.Desc.LsnRange.End = end
	u.s.frozen = append(u.s.frozen, e)
	u.s.open = nil
	return nil
}

func (u *Updater) RemoveFrozen(d layer.Desc) error {
	if len(u.s.frozen) == 0 {
		return fmt.Errorf("%w: no frozen layers", ErrNotFound)
	}
	if u.s.frozen[0].Desc != d {
		return fmt.Errorf("%w: %s", ErrNotOldest, d)
	}
	u.s.frozen = u.s.frozen[1:]
	return nil
}

func (u *Updater) InsertHistoric(e Entry) error {
	switch {
	case e.InMemory || e.Layer == nil:
		return fmt.Errorf("%w: historic layer must be on disk with a layer", ErrInvalid)
	case e.Desc.KeyRange.IsEmpty() || e.Desc.LsnRange.IsEmpty():
		return fmt.Errorf("%w: empty range in %s", ErrInvalid, e.Desc)
	}
	i, found := slices.BinarySearchFunc(u.s.historic, e, compareEntries)
	if found {
		return fmt.Errorf("%w: %s", ErrDuplicate, e.Desc)
	}
	u.s.historic = slices.Insert(u.s.historic, i, e)
	return nil
}

func (u *Updater) RemoveHistoric(d layer.Desc) error {
	i, found := slices.BinarySearchFunc(u.s.historic, Entry{Desc: d}, compareEntries)
	if !found {
		return fmt.Errorf("%w: %s", ErrNotFound, d)
	}
	u.s.historic = slices.Delete(u.s.historic, i, i+1)
	return nil
}

func compareEntries(a, b Entry) int {
	x, y := a.Desc, b.Desc
	return cmp.Or(
		cmp.Compare(x.LsnRange.Start, y.LsnRange.Start),
		cmp.Compare(x.LsnRange.End, y.LsnRange.End),
		x.KeyRange.Start.Compare(y.KeyRange.Start),
		x.KeyRange.End.Compare(y.KeyRange.End),
		cmp.Compare(x.Generation, y.Generation),
		boolCompare(x.IsDelta, y.IsDelta),
	)
}

func boolCompare(a, b bool) int {
	switch {
	case a == b:
		return 0
	case !a:
		return -1
	}
	return 1
}
