// Package value defines what the repository stores for each (key, LSN):
// either a full page image or a WAL record. Mirrors Neon's Value
// (libs/wal_decoder/src/models/value.rs).
package value

import (
	"errors"
	"fmt"
)

// Encoded layout:
//
//	Image:  [0] tag=0 | page bytes...
//	Record: [0] tag=1 | [1] will_init (0/1) | [2] kind | body...
//
// will_init sits at a fixed offset so readers can check the stop rule
// with PeekWillInit, without decoding the whole value.
const (
	tagImage  byte = 0
	tagRecord byte = 1
)

// Value is one stored version of a key.
type Value struct {
	image []byte
	rec   WalRecord // nil for images
}

// Image returns a Value holding a full page image.
func Image(page []byte) Value { return Value{image: page} }

// Record returns a Value holding a WAL record.
func Record(r WalRecord) Value {
	if r == nil {
		panic("value: nil WalRecord")
	}
	return Value{rec: r}
}

// IsImage reports whether v is a full page image.
func (v Value) IsImage() bool { return v.rec == nil }

// ImageBytes returns the page bytes of an image value (nil for records).
func (v Value) ImageBytes() []byte { return v.image }

// WalRecord returns the record of a record value (nil for images).
func (v Value) WalRecord() WalRecord { return v.rec }

// WillInit is the read path's stop rule: an image always stops the search,
// a record only if its will_init flag is set.
func (v Value) WillInit() bool {
	if v.IsImage() {
		return true
	}
	return v.rec.WillInit()
}

// Encode serializes v.
func (v Value) Encode() []byte {
	if v.IsImage() {
		out := make([]byte, 0, 1+len(v.image))
		out = append(out, tagImage)
		return append(out, v.image...)
	}
	e := &encoder{buf: make([]byte, 0, 64)}
	e.u8(tagRecord)
	e.bool(v.rec.WillInit())
	e.u8(uint8(v.rec.Kind()))
	v.rec.encodeBody(e)
	return e.buf
}

// DecodeValue parses bytes produced by Encode. It never panics on bad input.
func DecodeValue(b []byte) (Value, error) {
	if len(b) == 0 {
		return Value{}, fmt.Errorf("%w: empty value", ErrCorrupt)
	}
	switch b[0] {
	case tagImage:
		return Image(append([]byte(nil), b[1:]...)), nil
	case tagRecord:
		d := &decoder{buf: b[1:]}
		willInit := d.bool()
		kind := RecordKind(d.u8())
		if d.err != nil {
			return Value{}, d.err
		}
		rec, err := decodeRecord(kind, willInit, d)
		if err != nil {
			return Value{}, err
		}
		return Record(rec), nil
	default:
		return Value{}, fmt.Errorf("%w: unknown tag %d", ErrCorrupt, b[0])
	}
}

// PeekWillInit reads the stop rule from encoded bytes without decoding.
func PeekWillInit(b []byte) (bool, error) {
	if len(b) == 0 {
		return false, fmt.Errorf("%w: empty value", ErrCorrupt)
	}
	switch b[0] {
	case tagImage:
		return true, nil
	case tagRecord:
		if len(b) < 2 {
			return false, fmt.Errorf("%w: truncated record header", ErrCorrupt)
		}
		switch b[1] {
		case 0:
			return false, nil
		case 1:
			return true, nil
		}
		return false, fmt.Errorf("%w: invalid will_init byte", ErrCorrupt)
	default:
		return false, fmt.Errorf("%w: unknown tag %d", ErrCorrupt, b[0])
	}
}

// ErrNotTestRecord is returned by ApplyTest for non-test records.
var ErrNotTestRecord = errors.New("value: ApplyTest got a non-test record")