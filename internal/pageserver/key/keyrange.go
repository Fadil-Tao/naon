package key

import "math/big"

type KeyRange struct {
	Start Key
	End   Key
}

func FullRange() KeyRange { return KeyRange{Start: MinKey, End: MaxKey} }

func Singleton(k Key) (KeyRange, error) {
	next, err := k.Next()
	if err != nil {
		return KeyRange{}, err
	}
	return KeyRange{Start: k, End: next}, nil
}

func (r KeyRange) IsEmpty() bool { return r.Start.Compare(r.End) >= 0 }

func (r KeyRange) Contains(k Key) bool {
	return r.Start.Compare(k) <= 0 && k.Compare(r.End) < 0
}

func (r KeyRange) Overlaps(o KeyRange) bool {
	return r.Start.Compare(o.End) < 0 && o.Start.Compare(r.End) < 0
}

func (r KeyRange) Intersect(o KeyRange) KeyRange {
	return KeyRange{Start: Max(r.Start, o.Start), End: Min(r.End, o.End)}
}

func (r KeyRange) Len() *big.Int { return Distance(r.Start, r.End) }

func (r KeyRange) String() string { return r.Start.String() + "-" + r.End.String() }