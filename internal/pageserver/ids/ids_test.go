package ids

import (
	"errors"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	tn := NewTenantID()
	got, err := ParseTenantID(tn.String())
	if err != nil || got != tn || len(tn.String()) != 32 {
		t.Fatalf("tenant round trip: %v %v", got, err)
	}
	tl := NewTimelineID()
	got2, err := ParseTimelineID(tl.String())
	if err != nil || got2 != tl {
		t.Fatalf("timeline round trip: %v %v", got2, err)
	}
	if NewTenantID() == NewTenantID() {
		t.Fatal("random IDs collided")
	}
}

func TestParseErrors(t *testing.T) {
	for _, s := range []string{"", "abc", "zz" + "000000000000000000000000000000"} {
		if _, err := ParseTenantID(s); !errors.Is(err, ErrParse) {
			t.Fatalf("ParseTenantID(%q) = %v", s, err)
		}
	}
}