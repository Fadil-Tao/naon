package walrecord

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math/rand"
	"testing"

	"github.com/Fadil-Tao/naon/internal/pageserver/lsn"
)

func randBytes(r *rand.Rand, n int) []byte {
	b := make([]byte, n)
	r.Read(b)
	return b
}

func randRecord(r *rand.Rand) Record {
	switch r.Intn(3) {
	case 0:
		return PageImage{Page: r.Uint32(), Data: randBytes(r, r.Intn(200))}
	case 1:
		p := PagePatch{Page: r.Uint32()}
		for i := r.Intn(5); i > 0; i-- {
			p.Ranges = append(p.Ranges, Range{Off: r.Uint32(), Data: randBytes(r, r.Intn(20))})
		}
		return p
	default:
		return Commit{TxnID: r.Uint64(), Timestamp: r.Int63() - r.Int63()}
	}
}

func equal(a, b Record) bool {
	switch x := a.(type) {
	case PageImage:
		y, ok := b.(PageImage)
		return ok && x.Page == y.Page && bytes.Equal(x.Data, y.Data)
	case PagePatch:
		y, ok := b.(PagePatch)
		if !ok || x.Page != y.Page || len(x.Ranges) != len(y.Ranges) {
			return false
		}
		for i := range x.Ranges {
			if x.Ranges[i].Off != y.Ranges[i].Off || !bytes.Equal(x.Ranges[i].Data, y.Ranges[i].Data) {
				return false
			}
		}
		return true
	case Commit:
		y, ok := b.(Commit)
		return ok && x == y
	}
	return false
}

func encodeAll(t testing.TB, recs []Record) []byte {
	t.Helper()
	var stream []byte
	for _, rec := range recs {
		var err error
		if stream, err = Append(stream, rec); err != nil {
			t.Fatal(err)
		}
	}
	return stream
}

func decodeAll(t testing.TB, d *Decoder) ([]Decoded, error) {
	t.Helper()
	var out []Decoded
	for {
		rec, ok, err := d.Next()
		if err != nil {
			return out, err
		}
		if !ok {
			return out, nil
		}
		out = append(out, rec)
	}
}

func TestRoundTripInRandomChunks(t *testing.T) {
	for round := 0; round < 300; round++ {
		r := rand.New(rand.NewSource(int64(round)))
		recs := make([]Record, 1+r.Intn(20))
		for i := range recs {
			recs[i] = randRecord(r)
		}
		stream := encodeAll(t, recs)
		start := lsn.Lsn(r.Intn(1<<20)) * Align
		d, err := NewDecoder(start)
		if err != nil {
			t.Fatal(err)
		}

		var got []Decoded
		maxChunk := 1 + r.Intn(64)
		for pos := 0; pos < len(stream); {
			n := min(1+r.Intn(maxChunk), len(stream)-pos)
			d.Feed(stream[pos : pos+n])
			pos += n
			more, err := decodeAll(t, d)
			if err != nil {
				t.Fatalf("round %d: %v", round, err)
			}
			got = append(got, more...)
		}

		if len(got) != len(recs) {
			t.Fatalf("round %d: decoded %d records, want %d", round, len(got), len(recs))
		}
		at := start
		for i, g := range got {
			if g.Start != at || !g.Start.IsAligned() || !g.End.IsAligned() || g.End <= g.Start {
				t.Fatalf("round %d: record %d spans %s-%s, expected start %s", round, i, g.Start, g.End, at)
			}
			if !equal(g.Rec, recs[i]) {
				t.Fatalf("round %d: record %d = %#v, want %#v", round, i, g.Rec, recs[i])
			}
			at = g.End
		}
		if at != start+lsn.Lsn(len(stream)) || d.Lsn() != at || d.Pending() != 0 {
			t.Fatalf("round %d: ended at %s, decoder at %s, pending %d, stream %d bytes", round, at, d.Lsn(), d.Pending(), len(stream))
		}
	}
}

func TestEveryFlippedByteIsNoticed(t *testing.T) {
	recs := []Record{
		PageImage{Page: 7, Data: []byte("hello")},
		PagePatch{Page: 7, Ranges: []Range{{Off: 1, Data: []byte("E")}, {Off: 3, Data: []byte("LO")}}},
		Commit{TxnID: 42, Timestamp: 1234},
	}
	stream := encodeAll(t, recs)
	for i := range stream {
		for _, flip := range []byte{0x01, 0x80, 0xFF} {
			bad := append([]byte(nil), stream...)
			bad[i] ^= flip
			d, _ := NewDecoder(0)
			d.Feed(bad)
			got, err := decodeAll(t, d)
			for j, g := range got {
				if !equal(g.Rec, recs[j]) {
					t.Fatalf("byte %d ^ %#x: record %d silently changed to %#v", i, flip, j, g.Rec)
				}
			}
			if err == nil && len(got) == len(recs) {
				t.Fatalf("byte %d ^ %#x went unnoticed", i, flip)
			}
			if err != nil && !errors.Is(err, ErrCorrupt) {
				t.Fatalf("byte %d ^ %#x: %v", i, flip, err)
			}
		}
	}
}

func TestTornTailIsNeverDecoded(t *testing.T) {
	recs := []Record{
		PageImage{Page: 1, Data: []byte("abcdefghij")},
		Commit{TxnID: 1},
		PagePatch{Page: 1, Ranges: []Range{{Off: 2, Data: []byte("XYZ")}}},
		Commit{TxnID: 2},
	}
	var ends []int
	var stream []byte
	for _, rec := range recs {
		stream, _ = Append(stream, rec)
		ends = append(ends, len(stream))
	}
	for cut := 0; cut <= len(stream); cut++ {
		d, _ := NewDecoder(0)
		d.Feed(stream[:cut])
		got, err := decodeAll(t, d)
		if err != nil {
			t.Fatalf("cut %d: %v", cut, err)
		}
		want := 0
		for want < len(ends) && ends[want] <= cut {
			want++
		}
		if len(got) != want {
			t.Fatalf("cut %d: decoded %d records, want %d", cut, len(got), want)
		}
		lastEnd := 0
		if want > 0 {
			lastEnd = ends[want-1]
		}
		if d.Pending() != cut-lastEnd || d.Lsn() != lsn.Lsn(lastEnd) {
			t.Fatalf("cut %d: pending %d at %s, want %d at %d", cut, d.Pending(), d.Lsn(), cut-lastEnd, lastEnd)
		}
	}
}

func header(total uint32, kind Kind) []byte {
	h := make([]byte, HeaderSize)
	binary.BigEndian.PutUint32(h, total)
	h[4] = byte(kind)
	return h
}

func withCRC(rec []byte, total int) []byte {
	crc := crc32.Update(0, castagnoli, rec[:8])
	crc = crc32.Update(crc, castagnoli, rec[HeaderSize:total])
	binary.BigEndian.PutUint32(rec[8:], crc)
	return rec
}

func TestHugeLengthIsRejectedAtOnce(t *testing.T) {
	for _, total := range []uint32{MaxRecordSize + 1, 1 << 30, 0xFFFFFFFF} {
		d, _ := NewDecoder(0)
		d.Feed(header(total, KindCommit))
		if _, _, err := d.Next(); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("total %d: %v", total, err)
		}
	}
}

func TestBadHeaders(t *testing.T) {
	short := header(HeaderSize-1, KindCommit)
	reserved, _ := Append(nil, Commit{TxnID: 1})
	reserved[6] = 1
	reserved = withCRC(reserved, HeaderSize+16)
	for name, b := range map[string][]byte{"too short": short, "reserved byte": reserved} {
		d, _ := NewDecoder(0)
		d.Feed(b)
		if _, _, err := d.Next(); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("%s: %v", name, err)
		}
	}
}

func TestBadBodiesWithValidChecksum(t *testing.T) {
	mk := func(kind Kind, body []byte) []byte {
		total := HeaderSize + len(body)
		rec := append(header(uint32(total), kind), body...)
		rec = append(rec, make([]byte, padding(total))...)
		return withCRC(rec, total)
	}
	u32 := func(vs ...uint32) []byte {
		var b []byte
		for _, v := range vs {
			b = binary.BigEndian.AppendUint32(b, v)
		}
		return b
	}
	cases := map[string][]byte{
		"unknown kind":           mk(9, nil),
		"image without page":     mk(KindPageImage, []byte{1, 2}),
		"patch range too long":   mk(KindPagePatch, append(u32(1, 1, 0, 100), 1, 2, 3)),
		"patch count too large":  mk(KindPagePatch, u32(1, 0xFFFFFFFF)),
		"patch trailing bytes":   mk(KindPagePatch, append(u32(1, 0), 9)),
		"commit too short":       mk(KindCommit, make([]byte, 15)),
		"commit trailing bytes":  mk(KindCommit, make([]byte, 17)),
		"patch missing a length": mk(KindPagePatch, u32(1, 1, 5)),
	}
	for name, rec := range cases {
		d, _ := NewDecoder(0)
		d.Feed(rec)
		if _, _, err := d.Next(); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("%s: %v", name, err)
		}
	}
	d, _ := NewDecoder(0)
	d.Feed(mk(KindPageImage, u32(3)))
	if got, ok, err := d.Next(); err != nil || !ok || !equal(got.Rec, PageImage{Page: 3}) {
		t.Fatalf("empty image: %#v %v %v", got, ok, err)
	}
}

func TestErrorRepeats(t *testing.T) {
	bad, _ := Append(nil, Commit{TxnID: 1})
	bad[HeaderSize] ^= 1
	good, _ := Append(nil, Commit{TxnID: 2})
	d, _ := NewDecoder(0)
	d.Feed(bad)
	if _, _, err := d.Next(); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
	d.Feed(good)
	if _, ok, err := d.Next(); ok || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("decoder recovered: %v %v", ok, err)
	}
}

func TestUnalignedStart(t *testing.T) {
	for _, l := range []lsn.Lsn{1, 4, 7, 9} {
		if _, err := NewDecoder(l); !errors.Is(err, ErrUnaligned) {
			t.Fatalf("start %s: %v", l, err)
		}
	}
}

func TestTooLarge(t *testing.T) {
	dst := []byte("keep")
	out, err := Append(dst, PageImage{Page: 1, Data: make([]byte, MaxRecordSize)})
	if !errors.Is(err, ErrTooLarge) || string(out) != "keep" {
		t.Fatalf("err = %v, dst = %q", err, out)
	}
}

func applyRanges(page []byte, rs []Range) []byte {
	out := append([]byte(nil), page...)
	for _, rg := range rs {
		copy(out[rg.Off:], rg.Data)
	}
	return out
}

func TestDiff(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	for round := 0; round < 2000; round++ {
		old := randBytes(r, 1+r.Intn(256))
		cur := append([]byte(nil), old...)
		for i := r.Intn(10); i > 0; i-- {
			cur[r.Intn(len(cur))] = byte(r.Intn(256))
		}
		gap := r.Intn(16)
		rs := Diff(old, cur, gap)
		if got := applyRanges(old, rs); !bytes.Equal(got, cur) {
			t.Fatalf("round %d: apply(diff) != new", round)
		}
		end := -1
		for _, rg := range rs {
			off := int(rg.Off)
			if len(rg.Data) == 0 || off < end || off+len(rg.Data) > len(cur) {
				t.Fatalf("round %d: bad range %d+%d after %d", round, off, len(rg.Data), end)
			}
			if old[off] == cur[off] || old[off+len(rg.Data)-1] == cur[off+len(rg.Data)-1] {
				t.Fatalf("round %d: range %d+%d starts or ends on an unchanged byte", round, off, len(rg.Data))
			}
			if end >= 0 && off-end <= gap {
				t.Fatalf("round %d: ranges %d apart should have been merged (gap %d)", round, off-end, gap)
			}
			run, maxRun := 0, 0
			for i := off; i < off+len(rg.Data); i++ {
				if old[i] == cur[i] {
					run++
					maxRun = max(maxRun, run)
				} else {
					run = 0
				}
			}
			if maxRun > gap {
				t.Fatalf("round %d: range %d+%d holds %d unchanged bytes in a row (gap %d)", round, off, len(rg.Data), maxRun, gap)
			}
			end = off + len(rg.Data)
		}
	}
	if rs := Diff([]byte("same"), []byte("same"), 4); len(rs) != 0 {
		t.Fatalf("equal pages: %v", rs)
	}
	if rs := Diff([]byte("ab"), []byte("abc"), 4); len(rs) != 1 || rs[0].Off != 0 || string(rs[0].Data) != "abc" {
		t.Fatalf("different lengths: %v", rs)
	}
	if rs := Diff([]byte("a-b-c"), []byte("A-B-C"), 100); len(rs) != 1 || string(rs[0].Data) != "A-B-C" {
		t.Fatalf("large gap merges everything: %v", rs)
	}
}

func FuzzDecoder(f *testing.F) {
	seed, _ := Append(nil, PageImage{Page: 1, Data: []byte("x")})
	seed, _ = Append(seed, PagePatch{Page: 1, Ranges: []Range{{Off: 0, Data: []byte("y")}}})
	seed, _ = Append(seed, Commit{TxnID: 1})
	f.Add(seed, uint8(3))
	f.Add([]byte{}, uint8(1))
	f.Fuzz(func(t *testing.T, data []byte, chunk uint8) {
		d, _ := NewDecoder(0)
		step := int(chunk) + 1
		var got []Decoded
		for pos := 0; pos < len(data); pos += step {
			d.Feed(data[pos:min(pos+step, len(data))])
			more, err := decodeAll(t, d)
			got = append(got, more...)
			if err != nil {
				break
			}
		}
		var again []byte
		for _, g := range got {
			var err error
			if again, err = Append(again, g.Rec); err != nil {
				t.Fatal(err)
			}
		}
		if !bytes.Equal(again, data[:len(again)]) {
			t.Fatal("decoded records do not re-encode to the bytes they came from")
		}
	})
}
