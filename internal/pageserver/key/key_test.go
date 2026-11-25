package key_test

import (
	"errors"
	"testing"

	"github.com/Fadil-Tao/naon/internal/pageserver/key"
)

func TestAddCarryIntoField1(t *testing.T) {
	k := key.Key{Field1: 0, Field2: 0xFFFFFFFF, Field3: 0xFFFFFFFF, Field4: 0xFFFFFFFF, Field5: 0xFF, Field6: 0xFFFFFFFF}
	got, err := k.Next()
	if err != nil || got != (key.Key{Field1: 1}) {
		t.Fatalf("got %v, %v; want Key{Field1: 1}", got, err)
	}
}

func TestAddOverflowAtMaxKey(t *testing.T) {
	if _, err := key.MaxKey.Next(); !errors.Is(err, key.ErrKeyOverflow) {
		t.Fatalf("MaxKey.Next() err = %v, want ErrKeyOverflow", err)
	}
}