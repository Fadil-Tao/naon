package walrecord

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"

	"github.com/Fadil-Tao/naon/internal/pageserver/lsn"
)

const (
	HeaderSize    = 12
	Align         = 8
	MaxRecordSize = 1 << 24
)

type Kind uint8

const (
	KindPageImage Kind = 1
	KindPagePatch Kind = 2
	KindCommit    Kind = 3
)

var (
	ErrCorrupt   = errors.New("walrecord: corrupt record")
	ErrUnaligned = errors.New("walrecord: LSN is not 8-byte aligned")
	ErrTooLarge  = errors.New("walrecord: record too large")
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

type Record interface {
	Kind() Kind
	appendBody(b []byte) []byte
}

type PageImage struct {
	Page uint32
	Data []byte
}

type Range struct {
	Off  uint32
	Data []byte
}

type PagePatch struct {
	Page   uint32
	Ranges []Range
}

type Commit struct {
	TxnID     uint64
	Timestamp int64
}

func (PageImage) Kind() Kind { return KindPageImage }
func (PagePatch) Kind() Kind { return KindPagePatch }
func (Commit) Kind() Kind    { return KindCommit }

func (r PageImage) appendBody(b []byte) []byte {
	b = binary.BigEndian.AppendUint32(b, r.Page)
	return append(b, r.Data...)
}

func (r PagePatch) appendBody(b []byte) []byte {
	b = binary.BigEndian.AppendUint32(b, r.Page)
	b = binary.BigEndian.AppendUint32(b, uint32(len(r.Ranges)))
	for _, rg := range r.Ranges {
		b = binary.BigEndian.AppendUint32(b, rg.Off)
		b = binary.BigEndian.AppendUint32(b, uint32(len(rg.Data)))
		b = append(b, rg.Data...)
	}
	return b
}

func (r Commit) appendBody(b []byte) []byte {
	b = binary.BigEndian.AppendUint64(b, r.TxnID)
	return binary.BigEndian.AppendUint64(b, uint64(r.Timestamp))
}

func padding(n int) int { return (Align - n%Align) % Align }

func Append(dst []byte, r Record) ([]byte, error) {
	start := len(dst)
	dst = append(dst, make([]byte, HeaderSize)...)
	dst = r.appendBody(dst)
	total := len(dst) - start
	if total > MaxRecordSize {
		return dst[:start], fmt.Errorf("%w: %d bytes", ErrTooLarge, total)
	}
	h := dst[start : start+HeaderSize]
	binary.BigEndian.PutUint32(h[0:], uint32(total))
	h[4] = byte(r.Kind())
	crc := crc32.Update(0, castagnoli, h[:8])
	crc = crc32.Update(crc, castagnoli, dst[start+HeaderSize:])
	binary.BigEndian.PutUint32(h[8:], crc)
	return append(dst, make([]byte, padding(total))...), nil
}

type Decoded struct {
	Start lsn.Lsn
	End   lsn.Lsn
	Rec   Record
}

type Decoder struct {
	buf []byte
	lsn lsn.Lsn
}

func NewDecoder(start lsn.Lsn) (*Decoder, error) {
	if !start.IsAligned() {
		return nil, fmt.Errorf("%w: %s", ErrUnaligned, start)
	}
	return &Decoder{lsn: start}, nil
}

func (d *Decoder) Feed(b []byte) { d.buf = append(d.buf, b...) }
func (d *Decoder) Lsn() lsn.Lsn  { return d.lsn }
func (d *Decoder) Pending() int  { return len(d.buf) }

func (d *Decoder) Next() (Decoded, bool, error) {
	if len(d.buf) < HeaderSize {
		return Decoded{}, false, nil
	}
	h := d.buf[:HeaderSize]
	total := int(binary.BigEndian.Uint32(h[0:]))
	if total < HeaderSize || total > MaxRecordSize || h[5]|h[6]|h[7] != 0 {
		return d.fail("bad header")
	}
	padded := total + padding(total)
	if len(d.buf) < padded {
		return Decoded{}, false, nil
	}
	body := d.buf[HeaderSize:total]
	crc := crc32.Update(0, castagnoli, h[:8])
	crc = crc32.Update(crc, castagnoli, body)
	if crc != binary.BigEndian.Uint32(h[8:]) {
		return d.fail("checksum mismatch")
	}
	for _, p := range d.buf[total:padded] {
		if p != 0 {
			return d.fail("nonzero padding")
		}
	}
	rec, err := decodeBody(Kind(h[4]), body)
	if err != nil {
		return d.fail(err.Error())
	}
	out := Decoded{Start: d.lsn, End: d.lsn + lsn.Lsn(padded), Rec: rec}
	d.lsn = out.End
	d.buf = d.buf[padded:]
	return out, true, nil
}

func (d *Decoder) fail(why string) (Decoded, bool, error) {
	return Decoded{}, false, fmt.Errorf("%w at %s: %s", ErrCorrupt, d.lsn, why)
}

type reader struct {
	b   []byte
	bad bool
}

func (r *reader) u32() uint32 {
	if len(r.b) < 4 {
		r.bad = true
		return 0
	}
	v := binary.BigEndian.Uint32(r.b)
	r.b = r.b[4:]
	return v
}

func (r *reader) u64() uint64 {
	if len(r.b) < 8 {
		r.bad = true
		return 0
	}
	v := binary.BigEndian.Uint64(r.b)
	r.b = r.b[8:]
	return v
}

func (r *reader) take(n uint32) []byte {
	if uint64(len(r.b)) < uint64(n) {
		r.bad = true
		return nil
	}
	v := append([]byte(nil), r.b[:n]...)
	r.b = r.b[n:]
	return v
}

func decodeBody(k Kind, body []byte) (Record, error) {
	r := &reader{b: body}
	var rec Record
	switch k {
	case KindPageImage:
		page := r.u32()
		rec = PageImage{Page: page, Data: r.take(uint32(len(r.b)))}
	case KindPagePatch:
		page := r.u32()
		n := r.u32()
		if uint64(n)*8 > uint64(len(r.b)) {
			return nil, errors.New("range count too large")
		}
		ranges := make([]Range, n)
		for i := range ranges {
			off := r.u32()
			ln := r.u32()
			ranges[i] = Range{Off: off, Data: r.take(ln)}
		}
		rec = PagePatch{Page: page, Ranges: ranges}
	case KindCommit:
		txn := r.u64()
		rec = Commit{TxnID: txn, Timestamp: int64(r.u64())}
	default:
		return nil, fmt.Errorf("unknown kind %d", k)
	}
	if r.bad {
		return nil, errors.New("body too short")
	}
	if len(r.b) != 0 {
		return nil, fmt.Errorf("%d trailing bytes", len(r.b))
	}
	return rec, nil
}

func Diff(old, cur []byte, mergeGap int) []Range {
	if len(old) != len(cur) {
		return []Range{{Off: 0, Data: append([]byte(nil), cur...)}}
	}
	var out []Range
	for i := 0; i < len(cur); {
		if old[i] == cur[i] {
			i++
			continue
		}
		start, end := i, i+1
		for j := end; j < len(cur); j++ {
			if old[j] != cur[j] {
				end = j + 1
			} else if j-end >= mergeGap {
				break
			}
		}
		out = append(out, Range{Off: uint32(start), Data: append([]byte(nil), cur[start:end]...)})
		i = end
	}
	return out
}
