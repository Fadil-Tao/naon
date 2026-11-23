package layer

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/Fadil-Tao/naon/internal/pageserver/key"
	"github.com/Fadil-Tao/naon/internal/pageserver/lsn"
)

// follows neon's file naming

// desc describe one layer: key range, lsn rang, delta or image, generation.
// its file name encode all of it
// pageserver would list the dir and rebuild its view from the file names alone.
// so file name is the description


var ErrBadName = errors.New("layer: not a layer file name")

type Desc struct {
	KeyRange key.KeyRange
	LsnRange lsn.Range // lsnrange is [start, end), image layer at lsn L has [L, L+1)
	IsDelta bool
	Generation uint32
}

func (d Desc) ImageLsn() lsn.Lsn {return d.LsnRange.Start}

func (d Desc) IsL0() bool { return d.IsDelta && d.KeyRange == key.FullRange()}

///	delta: <key start>-<key end>__<lsn start>-<lsn end>[-v1-<generation>]
//	image: <key start>-<key end>__<lsn>[-v1-<generation>]
func (d Desc) FileName() string {
	var b strings.Builder
	b.WriteString(d.KeyRange.Start.String())
	b.WriteByte('-')
	b.WriteString(d.KeyRange.End.String())
	b.WriteString("__")
	b.WriteString(d.LsnRange.Start.FileName())
	if d.IsDelta {
		b.WriteByte('-')
		b.WriteString(d.LsnRange.End.FileName())
	}
	if d.Generation != 0 {
		fmt.Fprintf(&b, "-v1-%08x", d.Generation)
	}
	return b.String()
}

func (d Desc) String() string { return d.FileName() }

func ParseFileName(name string) (Desc, error) {
	bad := func(why string) (Desc, error) {
		return Desc{}, fmt.Errorf("%w: %q: %s", ErrBadName, name, why)
	}
	keyPart, lsnPart, ok := strings.Cut(name, "__")
	if !ok {
		return bad("no __")
	}

	ks, ke, ok := strings.Cut(keyPart, "-")
	if !ok {
		return bad("no key range")
	}
	start, err := key.ParseKeyHex(ks)
	if err != nil {
		return bad(err.Error())
	}
	end, err := key.ParseKeyHex(ke)
	if err != nil {
		return bad(err.Error())
	}
	d := Desc{KeyRange: key.KeyRange{Start: start, End: end}}

	parts := strings.Split(lsnPart, "-")
	l0, err := lsn.ParseFileName(parts[0])
	if err != nil {
		return bad(err.Error())
	}
	rest := parts[1:]
	if len(rest) > 0 && len(rest[0]) == 16 { // a second LSN: this is a delta
		l1, err := lsn.ParseFileName(rest[0])
		if err != nil {
			return bad(err.Error())
		}
		d.IsDelta = true
		d.LsnRange = lsn.Range{Start: l0, End: l1}
		rest = rest[1:]
	} else {
		if l0 == lsn.Max {
			return bad("image LSN overflows")
		}
		d.LsnRange = lsn.Range{Start: l0, End: l0 + 1}
	}

	if len(rest) > 0 && rest[0] == "v1" {
		rest = rest[1:]
		if len(rest) == 0 {
			return bad("v1 without generation")
		}
	}
	switch len(rest) {
	case 0:
	case 1:
		g, err := strconv.ParseUint(rest[0], 16, 32)
		if err != nil || len(rest[0]) != 8 {
			return bad("bad generation")
		}
		d.Generation = uint32(g)
	default:
		return bad("unexpected trailing parts")
	}

	if d.KeyRange.IsEmpty() {
		return bad("empty key range")
	}
	if d.LsnRange.IsEmpty() {
		return bad("empty LSN range")
	}
	return d, nil
}