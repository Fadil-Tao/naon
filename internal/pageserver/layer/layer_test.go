package layer

import (
	"errors"
	"testing"

	"github.com/Fadil-Tao/naon/internal/pageserver/blockio"
	"github.com/Fadil-Tao/naon/internal/pageserver/ids"
	"github.com/Fadil-Tao/naon/internal/pageserver/key"
	"github.com/Fadil-Tao/naon/internal/pageserver/lsn"
)

func TestFileNameRoundTrip(t *testing.T) {
	descs := []Desc{
		{KeyRange: key.FullRange(), LsnRange: lsn.Range{Start: 0x384, End: 0x1388}, IsDelta: true},
		{KeyRange: key.FullRange(), LsnRange: lsn.Range{Start: 0x384, End: 0x1388}, IsDelta: true, Generation: 7},
		{KeyRange: key.KeyRange{Start: key.Key{Field4: 1}, End: key.Key{Field4: 2}}, LsnRange: lsn.Range{Start: 0x50, End: 0x51}},
		{KeyRange: key.KeyRange{Start: key.Key{Field4: 1}, End: key.Key{Field4: 2}}, LsnRange: lsn.Range{Start: 0x50, End: 0x51}, Generation: 0xABCDEF01},
	}
	for _, d := range descs {
		got, err := ParseFileName(d.FileName())
		if err != nil || got != d {
			t.Fatalf("round trip %s -> %+v, %v", d.FileName(), got, err)
		}
	}
}

// File names copied from Neon's layer_name.rs tests.
func TestParseNeonNames(t *testing.T) {
	const keys = "000000000000000000000000000000000000-000000067F00000001000004DF0000000006"
	cases := []struct {
		name  string
		delta bool
		gen   uint32
	}{
		{keys + "__00000000014FED58-v1-00000001", false, 1},
		{keys + "__00000000014FED58-00000001", false, 1},
		{keys + "__00000000014FED58", false, 0},
		{keys + "__00000000014FED58-000000000154C481-v1-00000001", true, 1},
	}
	for _, c := range cases {
		d, err := ParseFileName(c.name)
		if err != nil || d.IsDelta != c.delta || d.Generation != c.gen {
			t.Fatalf("%s: %+v %v", c.name, d, err)
		}
		if d.LsnRange.Start != 0x14FED58 {
			t.Fatalf("%s: wrong lsn %v", c.name, d.LsnRange)
		}
	}
}

func TestParseBadNames(t *testing.T) {
	const keys = "000000000000000000000000000000000000-000000067F00000001000004DF0000000006"
	for _, name := range []string{
		"metadata",
		"layer-abc.___temp",
		keys,                             // no __
		keys + "__XYZ",                   // bad lsn
		keys + "__00000000014FED58-v1",   // v1 without generation
		keys + "__00000000014FED58-v1-1", // generation not 8 digits
		keys + "__0000000000000010-0000000000000010",                                                  // empty delta range
		"000000067F00000001000004DF0000000006-000000000000000000000000000000000000__0000000000000001", // empty key range
	} {
		if _, err := ParseFileName(name); !errors.Is(err, ErrBadName) {
			t.Fatalf("ParseFileName(%q) = %v, want ErrBadName", name, err)
		}
	}
}

func TestIsL0(t *testing.T) {
	l0 := Desc{KeyRange: key.FullRange(), LsnRange: lsn.Range{Start: 1, End: 2}, IsDelta: true}
	if !l0.IsL0() {
		t.Fatal("full-range delta is L0")
	}
	l1 := l0
	l1.KeyRange.End = key.Key{Field1: 1}
	if l1.IsL0() {
		t.Fatal("partial-range delta is not L0")
	}
	img := l0
	img.IsDelta = false
	if img.IsL0() {
		t.Fatal("image is not L0")
	}
}

func TestSummaryRoundTrip(t *testing.T) {
	s := Summary{
		Magic:         DeltaMagic,
		FormatVersion: FormatVersion,
		Tenant:        ids.NewTenantID(),
		Timeline:      ids.NewTimelineID(),
		KeyRange:      key.KeyRange{Start: key.Key{Field4: 1}, End: key.Key{Field4: 9}},
		LsnRange:      lsn.Range{Start: 10, End: 20},
		IndexStartBlk: 5,
		IndexRootBlk:  9,
	}
	b := s.Encode()
	if len(b) != blockio.BlockSize {
		t.Fatalf("summary block is %d bytes", len(b))
	}
	got, err := DecodeSummary(b, DeltaMagic)
	if err != nil || got != s {
		t.Fatalf("round trip: %+v %v", got, err)
	}
	if _, err := DecodeSummary(b, ImageMagic); !errors.Is(err, ErrBadSummary) {
		t.Fatalf("wrong magic accepted: %v", err)
	}
	b[40] ^= 0xFF // damage a byte
	if _, err := DecodeSummary(b, DeltaMagic); !errors.Is(err, ErrBadSummary) {
		t.Fatalf("damaged summary accepted: %v", err)
	}
}

func TestBlobRef(t *testing.T) {
	r := NewBlobRef(12345, true)
	if r.Pos() != 12345 || !r.WillInit() {
		t.Fatal("BlobRef with will_init")
	}
	r = NewBlobRef(8192, false)
	if r.Pos() != 8192 || r.WillInit() {
		t.Fatal("BlobRef without will_init")
	}
}