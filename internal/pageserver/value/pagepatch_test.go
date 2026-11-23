package value

import (
	"bytes"
	"errors"
	"testing"
)

func TestPagePatchRoundTrip(t *testing.T) {
	for _, p := range []PagePatch{
		{},
		{Ranges: []PatchRange{{Off: 0, Data: []byte("abc")}}},
		{Ranges: []PatchRange{{Off: 10, Data: []byte{1, 2}}, {Off: 4000, Data: []byte{}}, {Off: 4095, Data: []byte{9}}}},
	} {
		enc := Record(p).Encode()
		if wi, err := PeekWillInit(enc); err != nil || wi {
			t.Fatalf("PeekWillInit = %v, %v", wi, err)
		}
		v, err := DecodeValue(enc)
		if err != nil {
			t.Fatal(err)
		}
		got, ok := v.WalRecord().(PagePatch)
		if !ok || v.WillInit() || got.Kind() != KindPagePatch || len(got.Ranges) != len(p.Ranges) {
			t.Fatalf("decoded %#v", v.WalRecord())
		}
		for i := range p.Ranges {
			if got.Ranges[i].Off != p.Ranges[i].Off || !bytes.Equal(got.Ranges[i].Data, p.Ranges[i].Data) {
				t.Fatalf("range %d = %+v, want %+v", i, got.Ranges[i], p.Ranges[i])
			}
		}
	}
	if KindPagePatch.String() != "PagePatch" {
		t.Fatal(KindPagePatch.String())
	}
}

func TestPagePatchCorrupt(t *testing.T) {
	good := Record(PagePatch{Ranges: []PatchRange{{Off: 1, Data: []byte("xy")}}}).Encode()
	for n := 0; n < len(good); n++ {
		if _, err := DecodeValue(good[:n]); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("truncated to %d: %v", n, err)
		}
	}
	if _, err := DecodeValue(append(append([]byte(nil), good...), 0)); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("trailing byte: %v", err)
	}
	huge := append([]byte(nil), good[:3]...)
	huge = append(huge, 0xFF, 0xFF, 0xFF, 0xFF)
	if _, err := DecodeValue(huge); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("huge range count: %v", err)
	}
	willInit := append([]byte(nil), good...)
	willInit[1] = 1
	if _, err := DecodeValue(willInit); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("will_init patch: %v", err)
	}
}
