// Package lsn defines the Log Sequence Number: a position in the WAL and the
// time axis of the repository. Formatting matches Postgres and Neon
// (libs/utils/src/lsn.rs).
package lsn

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

type Lsn uint64

const (
	Invalid Lsn = 0
	Max Lsn = math.MaxUint64
)

var ErrParse = errors.New("lsn: parse error")

func (l Lsn) IsValid() bool { return l != Invalid }

func (l Lsn) String() string {
	return fmt.Sprintf("%X/%X", uint64(l)>>32, uint64(l)&0xFFFFFFFF)
}

func Parse(s string) (Lsn, error) {
	parts := strings.Split(strings.TrimSpace(s), "/")
	if len(parts) != 2 {
		return Invalid, fmt.Errorf("%w: %q", ErrParse, s)
	}
	hi, err := strconv.ParseUint(parts[0], 16, 32)
	if err != nil {
		return Invalid, fmt.Errorf("%w: %q", ErrParse, s)
	}
	lo, err := strconv.ParseUint(parts[1], 16, 32)
	if err != nil {
		return Invalid, fmt.Errorf("%w: %q", ErrParse, s)
	}
	return Lsn(hi<<32 | lo), nil
}

func MustParse(s string) Lsn {
	l, err := Parse(s)
	if err != nil {
		panic(err)
	}
	return l
}

func (l Lsn) FileName() string { return fmt.Sprintf("%016X", uint64(l)) }

func ParseFileName(s string) (Lsn, error) {
	if len(s) != 16 {
		return Invalid, fmt.Errorf("%w: file-name lsn %q must be 16 hex digits", ErrParse, s)
	}
	v, err := strconv.ParseUint(s, 16, 64)
	if err != nil {
		return Invalid, fmt.Errorf("%w: %q", ErrParse, s)
	}
	return Lsn(v), nil
}

func (l Lsn) Align() Lsn { return (l + 7) &^ 7 }

func (l Lsn) IsAligned() bool { return l == l.Align() }

func (l Lsn) CheckedSub(o Lsn) (Lsn, bool) {
	if o > l {
		return Invalid, false
	}
	return l - o, true
}

func (l Lsn) Distance(o Lsn) int64 { return int64(l) - int64(o) }

func Min(a, b Lsn) Lsn {
	if a < b {
		return a
	}
	return b
}

func MaxOf(a, b Lsn) Lsn {
	if a > b {
		return a
	}
	return b
}

type Range struct {
	Start Lsn
	End   Lsn
}

func (r Range) IsEmpty() bool { return r.Start >= r.End }

// Contains reports whether Start <= l < End.
func (r Range) Contains(l Lsn) bool { return r.Start <= l && l < r.End }

// Overlaps reports whether the ranges share at least one LSN.
func (r Range) Overlaps(o Range) bool { return r.Start < o.End && o.Start < r.End }

// String returns "start-end" in file-name form.
func (r Range) String() string { return r.Start.FileName() + "-" + r.End.FileName() }