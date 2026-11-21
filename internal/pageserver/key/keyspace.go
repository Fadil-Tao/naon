package key

import (
	"math/big"
	"slices"
	"sort"
	"strings"
)

// PageSize is the size of a Postgres page. Used to turn byte targets into key counts.
const PageSize = 8192

// KeySpace is a set of keys stored as ranges.
//
// Invariant (restored by normalize after every operation):
//   - ranges are sorted by Start
//   - no range is empty
//   - no two ranges overlap or touch (touching ranges are merged)
//
// KeySpace is immutable: every operation returns a new KeySpace.
type KeySpace struct {
	ranges []KeyRange
}

// FromRanges builds a KeySpace from any list of ranges.
func FromRanges(rs ...KeyRange) KeySpace {
	return KeySpace{ranges: normalize(slices.Clone(rs))}
}

// normalize sorts, drops empty ranges and merges overlapping or touching ones.
// It may reuse rs's backing array.
func normalize(rs []KeyRange) []KeyRange {
	rs = slices.DeleteFunc(rs, KeyRange.IsEmpty)
	if len(rs) == 0 {
		return nil
	}
	slices.SortFunc(rs, func(a, b KeyRange) int { return a.Start.Compare(b.Start) })
	out := rs[:1]
	for _, r := range rs[1:] {
		last := &out[len(out)-1]
		if r.Start.Compare(last.End) <= 0 { // overlaps or touches
			last.End = Max(last.End, r.End)
		} else {
			out = append(out, r)
		}
	}
	return out
}

// Ranges returns a copy of the ranges.
func (s KeySpace) Ranges() []KeyRange { return slices.Clone(s.ranges) }

// NumRanges returns how many ranges the KeySpace has.
func (s KeySpace) NumRanges() int { return len(s.ranges) }

// IsEmpty reports whether the set has no keys.
func (s KeySpace) IsEmpty() bool { return len(s.ranges) == 0 }

// Start returns the first key. ok is false for an empty set.
func (s KeySpace) Start() (k Key, ok bool) {
	if s.IsEmpty() {
		return Key{}, false
	}
	return s.ranges[0].Start, true
}

// End returns the end (exclusive) of the last range. ok is false for an empty set.
func (s KeySpace) End() (k Key, ok bool) {
	if s.IsEmpty() {
		return Key{}, false
	}
	return s.ranges[len(s.ranges)-1].End, true
}

// AddRange returns s with r added.
func (s KeySpace) AddRange(r KeyRange) KeySpace {
	rs := make([]KeyRange, 0, len(s.ranges)+1)
	rs = append(rs, s.ranges...)
	rs = append(rs, r)
	return KeySpace{ranges: normalize(rs)}
}

// Union returns the keys in s or o.
func (s KeySpace) Union(o KeySpace) KeySpace {
	rs := make([]KeyRange, 0, len(s.ranges)+len(o.ranges))
	rs = append(rs, s.ranges...)
	rs = append(rs, o.ranges...)
	return KeySpace{ranges: normalize(rs)}
}

// Subtract returns the keys in s that are not in o.
//
// This is the core of the read path: remaining = requested - found.
func (s KeySpace) Subtract(o KeySpace) KeySpace {
	var out []KeyRange
	j := 0
	for _, a := range s.ranges {
		start := a.Start
		// Skip ranges of o that end before a starts. o is sorted, and so is s,
		// so j never needs to move backwards.
		for j < len(o.ranges) && o.ranges[j].End.Compare(start) <= 0 {
			j++
		}
		for k := j; k < len(o.ranges) && o.ranges[k].Start.Compare(a.End) < 0; k++ {
			b := o.ranges[k]
			if b.Start.Compare(start) > 0 {
				out = append(out, KeyRange{Start: start, End: b.Start})
			}
			if b.End.Compare(start) > 0 {
				start = b.End
			}
			if start.Compare(a.End) >= 0 {
				break
			}
		}
		if start.Compare(a.End) < 0 {
			out = append(out, KeyRange{Start: start, End: a.End})
		}
	}
	return KeySpace{ranges: normalize(out)}
}

// Intersect returns the keys in both s and o.
func (s KeySpace) Intersect(o KeySpace) KeySpace {
	var out []KeyRange
	i, j := 0, 0
	for i < len(s.ranges) && j < len(o.ranges) {
		a, b := s.ranges[i], o.ranges[j]
		if in := a.Intersect(b); !in.IsEmpty() {
			out = append(out, in)
		}
		// Advance whichever range ends first.
		if a.End.Compare(b.End) < 0 {
			i++
		} else {
			j++
		}
	}
	return KeySpace{ranges: normalize(out)}
}

// IntersectRange returns the keys of s that fall inside r.
func (s KeySpace) IntersectRange(r KeyRange) KeySpace {
	return s.Intersect(FromRanges(r))
}

// firstEndingAfter returns the index of the first range whose End > k.
func (s KeySpace) firstEndingAfter(k Key) int {
	return sort.Search(len(s.ranges), func(i int) bool {
		return s.ranges[i].End.Compare(k) > 0
	})
}

// Contains reports whether k is in the set.
func (s KeySpace) Contains(k Key) bool {
	i := s.firstEndingAfter(k)
	return i < len(s.ranges) && s.ranges[i].Start.Compare(k) <= 0
}

// Overlaps reports whether any key of r is in the set.
func (s KeySpace) Overlaps(r KeyRange) bool {
	if r.IsEmpty() {
		return false
	}
	i := s.firstEndingAfter(r.Start)
	return i < len(s.ranges) && s.ranges[i].Start.Compare(r.End) < 0
}

// Equal reports whether both sets hold exactly the same keys.
func (s KeySpace) Equal(o KeySpace) bool { return slices.Equal(s.ranges, o.ranges) }

// TotalKeys returns the number of keys in the set.
func (s KeySpace) TotalKeys() *big.Int {
	total := new(big.Int)
	for _, r := range s.ranges {
		total.Add(total, r.Len())
	}
	return total
}

// Partition splits the set into chunks of at most targetBytes/PageSize keys
// each (at least 1). Large ranges are cut. Together the chunks cover exactly
// the original set, with no overlap, in key order.
//
// Compaction uses this to decide image layer boundaries.
func (s KeySpace) Partition(targetBytes uint64) []KeySpace {
	chunkKeys := targetBytes / PageSize
	if chunkKeys == 0 {
		chunkKeys = 1
	}
	chunk := new(big.Int).SetUint64(chunkKeys)

	var parts []KeySpace
	var cur []KeyRange
	capLeft := new(big.Int).Set(chunk)

	flush := func() {
		if len(cur) > 0 {
			parts = append(parts, KeySpace{ranges: normalize(cur)})
			cur = nil
		}
		capLeft.Set(chunk)
	}

	for _, r := range s.ranges {
		start := r.Start
		for start.Compare(r.End) < 0 {
			size := Distance(start, r.End)
			if size.Cmp(capLeft) <= 0 {
				cur = append(cur, KeyRange{Start: start, End: r.End})
				capLeft.Sub(capLeft, size)
				if capLeft.Sign() == 0 {
					flush()
				}
				break
			}
			// Range is larger than the space left: cut it.
			cut, err := start.AddBig(capLeft)
			if err != nil {
				// Cannot happen: cut < r.End <= MaxKey.
				panic("keyspace: partition cut overflow")
			}
			cur = append(cur, KeyRange{Start: start, End: cut})
			flush()
			start = cut
		}
	}
	flush()
	return parts
}

// String renders the set as "[r1, r2, ...]".
func (s KeySpace) String() string {
	parts := make([]string, len(s.ranges))
	for i, r := range s.ranges {
		parts[i] = r.String()
	}
	return "[" + strings.Join(parts, ", ") + "]"
}