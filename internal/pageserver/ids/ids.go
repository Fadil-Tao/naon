package ids

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
)


var ErrParse = errors.New("ids: parse error")

type TenantID [16]byte

type TimelineID [16]byte

func NewTenantID() TenantID { return TenantID(random16()) }

func NewTimelineID() TimelineID { return TimelineID(random16())}

func (id TenantID) String() string {
	return hex.EncodeToString(id[:])
}

func (id TimelineID) String() string {
	return hex.EncodeToString(id[:])
} 

func ParseTenantID(s string) (TenantID, error) {
	b, err := parse16(s)
	return TenantID(b), err
}

func ParseTimelineID(s string) (TimelineID, error) {
	b, err := parse16(s)
	return TimelineID(b), err
}


func parse16(s string) ([16]byte, error) {
	var out [16]byte
	if len(s) != 32 {
		return out, fmt.Errorf("%w: %q must be 32 hex chars", ErrParse, s)
	}

	if _, err := hex.Decode(out[:], []byte(s)); err != nil {
		return out, fmt.Errorf("%w: %q: %v", ErrParse, s, err)
	}

	return out, nil
}

func random16() [16]byte {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil{
		panic("ids: crypto/rand failed " + err.Error())
	}

	return b
}