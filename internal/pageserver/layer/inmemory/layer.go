package inmemory

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"sync"

	"github.com/Fadil-Tao/naon/internal/pageserver/ids"
	"github.com/Fadil-Tao/naon/internal/pageserver/key"
	"github.com/Fadil-Tao/naon/internal/pageserver/layer"
	"github.com/Fadil-Tao/naon/internal/pageserver/layer/delta"
	"github.com/Fadil-Tao/naon/internal/pageserver/lsn"
	"github.com/Fadil-Tao/naon/internal/pageserver/value"
)

var (
	ErrFrozen    = errors.New("inmemory: layer is frozen")
	ErrNotFrozen = errors.New("inmemory: layer is not frozen")
	ErrLsnRange  = errors.New("inmemory: LSN outside the layer")
	ErrOrder     = errors.New("inmemory: LSNs of one key must strictly increase")
	ErrKey       = errors.New("inmemory: key not storable")
)

type Put struct {
	Key   key.Key
	Lsn   lsn.Lsn
	Value value.Value
}

type entry struct {
	lsn      lsn.Lsn
	off      uint64
	n        uint32
	willInit bool
}

type Layer struct {
	tenant   ids.TenantID
	timeline ids.TimelineID
	file     *Ephemeral
	start    lsn.Lsn

	wmu sync.Mutex

	mu     sync.RWMutex
	end    lsn.Lsn
	frozen bool
	index  map[key.Key][]entry
	count  int
	maxLsn lsn.Lsn
}

var _ layer.Reader = (*Layer)(nil)

func New(dir string, tenant ids.TenantID, timeline ids.TimelineID, start lsn.Lsn) (*Layer, error) {
	f, err := NewEphemeral(dir)
	if err != nil {
		return nil, err
	}
	return &Layer{
		tenant: tenant, timeline: timeline, file: f,
		start: start, end: lsn.Max,
		index: make(map[key.Key][]entry),
	}, nil
}

func (l *Layer) Desc() layer.Desc {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return layer.Desc{
		KeyRange: key.FullRange(),
		LsnRange: lsn.Range{Start: l.start, End: l.end},
		IsDelta:  true,
	}
}

func (l *Layer) PutBatch(puts []Put) error {
	l.wmu.Lock()
	defer l.wmu.Unlock()

	if l.frozen {
		return ErrFrozen
	}
	lastInBatch := make(map[key.Key]lsn.Lsn, len(puts))
	for _, p := range puts {
		if !p.Key.Less(key.MaxKey) {
			return fmt.Errorf("%w: %s", ErrKey, p.Key)
		}
		if p.Lsn < l.start || p.Lsn >= l.end {
			return fmt.Errorf("%w: %s, layer is [%s, %s)", ErrLsnRange, p.Lsn, l.start, l.end)
		}
		last, seen := lastInBatch[p.Key]
		if !seen {
			if es := l.index[p.Key]; len(es) > 0 {
				last, seen = es[len(es)-1].lsn, true
			}
		}
		if seen && p.Lsn <= last {
			return fmt.Errorf("%w: %s @ %s after %s", ErrOrder, p.Key, p.Lsn, last)
		}
		lastInBatch[p.Key] = p.Lsn
	}

	var buf []byte
	ends := make([]int, len(puts))
	for i, p := range puts {
		buf = append(buf, p.Value.Encode()...)
		ends[i] = len(buf)
	}
	base, err := l.file.Append(buf)
	if err != nil {
		return err
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	prev := 0
	for i, p := range puts {
		l.index[p.Key] = append(l.index[p.Key], entry{
			lsn: p.Lsn, off: base + uint64(prev), n: uint32(ends[i] - prev), willInit: p.Value.WillInit(),
		})
		prev = ends[i]
		l.maxLsn = lsn.MaxOf(l.maxLsn, p.Lsn)
	}
	l.count += len(puts)
	return nil
}

func (l *Layer) GetValueReconstructData(k key.Key, r lsn.Range, st *value.ReconstructState) error {
	l.mu.RLock()
	es := l.index[k]
	l.mu.RUnlock()

	i := sort.Search(len(es), func(i int) bool { return es[i].lsn >= r.End })
	for i--; i >= 0; i-- {
		e := es[i]
		if e.lsn < r.Start {
			break
		}
		v, err := l.read(e)
		if err != nil {
			return fmt.Errorf("inmemory: %s @ %s: %w", k, e.lsn, err)
		}
		if err := st.Add(e.lsn, v); err != nil {
			return err
		}
		if st.IsComplete() {
			break
		}
	}
	return nil
}

func (l *Layer) read(e entry) (value.Value, error) {
	buf := make([]byte, e.n)
	if err := l.file.ReadAt(buf, e.off); err != nil {
		return value.Value{}, err
	}
	return value.DecodeValue(buf)
}

func (l *Layer) Freeze(end lsn.Lsn) error {
	l.wmu.Lock()
	defer l.wmu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	switch {
	case l.frozen:
		return ErrFrozen
	case end <= l.start || end == lsn.Max:
		return fmt.Errorf("%w: cannot freeze [%s, ...) at %s", ErrLsnRange, l.start, end)
	case l.count > 0 && end <= l.maxLsn:
		return fmt.Errorf("%w: freeze at %s, but %s is stored", ErrLsnRange, end, l.maxLsn)
	}
	l.end = end
	l.frozen = true
	return nil
}

func (l *Layer) WriteToL0(dir string) (layer.Desc, string, error) {
	l.mu.RLock()
	if !l.frozen {
		l.mu.RUnlock()
		return layer.Desc{}, "", ErrNotFrozen
	}
	keys := make([]key.Key, 0, len(l.index))
	for k := range l.index {
		keys = append(keys, k)
	}
	rng := lsn.Range{Start: l.start, End: l.end}
	l.mu.RUnlock()

	slices.SortFunc(keys, key.Key.Compare)
	w, err := delta.NewWriter(dir, l.tenant, l.timeline, key.MinKey, rng)
	if err != nil {
		return layer.Desc{}, "", err
	}
	defer w.Abort()
	for _, k := range keys {
		for _, e := range l.index[k] {
			v, err := l.read(e)
			if err != nil {
				return layer.Desc{}, "", fmt.Errorf("inmemory: flush %s @ %s: %w", k, e.lsn, err)
			}
			if err := w.Put(k, e.lsn, v); err != nil {
				return layer.Desc{}, "", err
			}
		}
	}
	return w.Finish(key.MaxKey, 0)
}

func (l *Layer) NumEntries() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.count
}

func (l *Layer) Size() uint64 { return l.file.Size() }

func (l *Layer) LastLsn() lsn.Lsn {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.maxLsn
}

func (l *Layer) Remove() error { return l.file.Remove() }

func (l *Layer) Close() error { return l.file.Close() }
