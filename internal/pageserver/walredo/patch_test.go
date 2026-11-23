package walredo

import (
	"bytes"
	"errors"
	"math/rand"
	"testing"

	"github.com/Fadil-Tao/naon/internal/pageserver/key"
	"github.com/Fadil-Tao/naon/internal/pageserver/lsn"
	"github.com/Fadil-Tao/naon/internal/pageserver/value"
)

var k = key.Key{Field6: 1}

func rec(l lsn.Lsn, p value.PagePatch) value.LsnRecord { return value.LsnRecord{Lsn: l, Rec: p} }

func TestPatchMatchesByteSliceModel(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	for round := 0; round < 2000; round++ {
		size := 1 + r.Intn(128)
		base := make([]byte, size)
		r.Read(base)
		model := append([]byte(nil), base...)
		in := value.ReconstructInput{HasImage: true, ImageLsn: 10, Image: append([]byte(nil), base...)}
		for l := lsn.Lsn(11); l < lsn.Lsn(11+r.Intn(8)); l++ {
			var p value.PagePatch
			for i := r.Intn(4); i > 0; i-- {
				off := r.Intn(size)
				data := make([]byte, r.Intn(size-off+1))
				r.Read(data)
				p.Ranges = append(p.Ranges, value.PatchRange{Off: uint32(off), Data: data})
				copy(model[off:], data)
			}
			in.Records = append(in.Records, rec(l, p))
		}
		got, err := Patch{}.Reconstruct(k, 100, in)
		if err != nil || !bytes.Equal(got, model) {
			t.Fatalf("round %d: %v", round, err)
		}
		if !bytes.Equal(in.Image, base) {
			t.Fatalf("round %d: the base image was modified", round)
		}
	}
}

func TestImageOnly(t *testing.T) {
	in := value.ReconstructInput{HasImage: true, Image: []byte("page")}
	got, err := Patch{}.Reconstruct(k, 1, in)
	if err != nil || string(got) != "page" {
		t.Fatalf("%q, %v", got, err)
	}
	got[0] = 'X'
	if string(in.Image) != "page" {
		t.Fatal("result aliases the image")
	}
}

func TestErrors(t *testing.T) {
	patch := func(off uint32, data string) value.PagePatch {
		return value.PagePatch{Ranges: []value.PatchRange{{Off: off, Data: []byte(data)}}}
	}
	base := value.ReconstructInput{HasImage: true, Image: []byte("0123456789")}
	cases := []struct {
		name string
		in   value.ReconstructInput
		want error
	}{
		{"no base", value.ReconstructInput{Records: []value.LsnRecord{rec(2, patch(0, "x"))}}, ErrNoBase},
		{"past the end", withRecs(base, rec(2, patch(8, "abc"))), ErrOutOfPage},
		{"offset overflow", withRecs(base, rec(2, patch(0xFFFFFFFF, "ab"))), ErrOutOfPage},
		{"other record kind", withRecs(base, value.LsnRecord{Lsn: 2, Rec: value.TestAppend("x")}), ErrUnsupported},
	}
	for _, c := range cases {
		if _, err := (Patch{}).Reconstruct(k, 5, c.in); !errors.Is(err, c.want) {
			t.Fatalf("%s: %v", c.name, err)
		}
	}
	got, err := Patch{}.Reconstruct(k, 5, withRecs(base, rec(2, patch(7, "abc"))))
	if err != nil || string(got) != "0123456abc" {
		t.Fatalf("patch ending exactly at the end: %q, %v", got, err)
	}
}

func withRecs(in value.ReconstructInput, rs ...value.LsnRecord) value.ReconstructInput {
	in.Records = rs
	return in
}
