package value

import (
	"encoding/binary"
	"errors"
	"fmt"
)

var ErrCorrupt = errors.New("value: corrupt encoding")

type encoder struct{ buf []byte }

func (e *encoder) u8(v uint8)   { e.buf = append(e.buf, v) }
func (e *encoder) u32(v uint32) { e.buf = binary.BigEndian.AppendUint32(e.buf, v) }
func (e *encoder) u64(v uint64) { e.buf = binary.BigEndian.AppendUint64(e.buf, v) }
func (e *encoder) bool(v bool) {
	if v {
		e.u8(1)
	} else {
		e.u8(0)
	}
}
func (e *encoder) bytes(b []byte) {
	e.u32(uint32(len(b)))
	e.buf = append(e.buf, b...)
}
func (e *encoder) optU32(v OptU32) {
	e.bool(v.Valid)
	e.u32(v.V)
}
func (e *encoder) u32s(vs []uint32) {
	e.u32(uint32(len(vs)))
	for _, v := range vs {
		e.u32(v)
	}
}

type decoder struct {
	buf []byte
	err error
}

func (d *decoder) need(n int) bool {
	if d.err != nil {
		return false
	}
	if len(d.buf) < n {
		d.err = fmt.Errorf("%w: need %d bytes, have %d", ErrCorrupt, n, len(d.buf))
		return false
	}
	return true
}

func (d *decoder) u8() uint8 {
	if !d.need(1) {
		return 0
	}
	v := d.buf[0]
	d.buf = d.buf[1:]
	return v
}

func (d *decoder) u32() uint32 {
	if !d.need(4) {
		return 0
	}
	v := binary.BigEndian.Uint32(d.buf)
	d.buf = d.buf[4:]
	return v
}

func (d *decoder) u64() uint64 {
	if !d.need(8) {
		return 0
	}
	v := binary.BigEndian.Uint64(d.buf)
	d.buf = d.buf[8:]
	return v
}

func (d *decoder) bool() bool {
	switch d.u8() {
	case 0:
		return false
	case 1:
		return true
	default:
		if d.err == nil {
			d.err = fmt.Errorf("%w: invalid bool", ErrCorrupt)
		}
		return false
	}
}

func (d *decoder) bytes() []byte {
	n := d.u32()
	if !d.need(int(n)) {
		return nil
	}
	// Copy so the result does not alias the caller's buffer (which may be a
	// reused read buffer or page-cache block).
	v := append([]byte(nil), d.buf[:n]...)
	d.buf = d.buf[n:]
	return v
}

func (d *decoder) optU32() OptU32 {
	valid := d.bool()
	v := d.u32()
	return OptU32{Valid: valid, V: v}
}

func (d *decoder) u32s() []uint32 {
	n := d.u32()
	// Each element is 4 bytes; reject absurd lengths before allocating.
	if !d.need(int(n) * 4) {
		return nil
	}
	vs := make([]uint32, n)
	for i := range vs {
		vs[i] = d.u32()
	}
	return vs
}

// finish returns an error if decoding failed or bytes are left over.
func (d *decoder) finish() error {
	if d.err != nil {
		return d.err
	}
	if len(d.buf) != 0 {
		return fmt.Errorf("%w: %d trailing bytes", ErrCorrupt, len(d.buf))
	}
	return nil
}