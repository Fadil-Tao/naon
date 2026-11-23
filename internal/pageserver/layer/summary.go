package layer

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"

	"github.com/Fadil-Tao/naon/internal/pageserver/blockio"
	"github.com/Fadil-Tao/naon/internal/pageserver/ids"
	"github.com/Fadil-Tao/naon/internal/pageserver/key"
	"github.com/Fadil-Tao/naon/internal/pageserver/lsn"
)

// neon has magic number too, 0x5A60/0x5A61.
const (
	ImageMagic uint16 = 0x4760
	DeltaMagic uint16 = 0x4761
	FormatVersion uint16 = 1
)

var ErrBadSummary = errors.New("layer: bad summary block")

// summary is a block 0 of every layer file. it is written last
// (once the index location is known), but read first

type Summary struct {
	Magic uint16
	FormatVersion uint16
	Tenant ids.TenantID
	Timeline ids.TimelineID
	KeyRange key.KeyRange
	LsnRange lsn.Range
	IndexStartBlk uint32 // 1st block of the btree
	IndexRootBlk uint32 
}

const summarySize = 96 

func (s Summary) Encode() []byte {
	b := make([]byte, blockio.BlockSize)

	binary.BigEndian.PutUint16(b[0:], s.Magic)
	binary.BigEndian.PutUint16(b[2:], s.FormatVersion)
	copy(b[4:20], s.Tenant[:])
	copy(b[20:36], s.Timeline[:])
	s.KeyRange.Start.PutBytes(b[36:54])
	s.KeyRange.End.PutBytes(b[54:72])
	binary.BigEndian.PutUint64(b[72:], uint64(s.LsnRange.Start))
	binary.BigEndian.PutUint64(b[80:], uint64(s.LsnRange.End))
	binary.BigEndian.PutUint32(b[88:], s.IndexStartBlk)
	binary.BigEndian.PutUint32(b[92:], s.IndexRootBlk)
	binary.BigEndian.PutUint32(b[summarySize:], crc32.ChecksumIEEE(b[:summarySize]))
	return b
}


func DecodeSummary(b []byte, wantMagic uint16) (Summary, error) {
	if len(b) < summarySize+4 {
		return Summary{}, fmt.Errorf("%w: too short", ErrBadSummary)
	}
	if got, want := binary.BigEndian.Uint32(b[summarySize:]), crc32.ChecksumIEEE(b[:summarySize]); got != want {
		return Summary{}, fmt.Errorf("%w: checksum mismatch", ErrBadSummary)
	}
	var s Summary
	s.Magic = binary.BigEndian.Uint16(b[0:])
	s.FormatVersion = binary.BigEndian.Uint16(b[2:])
	if s.Magic != wantMagic {
		return Summary{}, fmt.Errorf("%w: magic %#x, want %#x", ErrBadSummary, s.Magic, wantMagic)
	}
	if s.FormatVersion != FormatVersion {
		return Summary{}, fmt.Errorf("%w: format version %d, want %d", ErrBadSummary, s.FormatVersion, FormatVersion)
	}
	copy(s.Tenant[:], b[4:20])
	copy(s.Timeline[:], b[20:36])
	s.KeyRange.Start, _ = key.DecodeKey(b[36:54])
	s.KeyRange.End, _ = key.DecodeKey(b[54:72])
	s.LsnRange.Start = lsn.Lsn(binary.BigEndian.Uint64(b[72:]))
	s.LsnRange.End = lsn.Lsn(binary.BigEndian.Uint64(b[80:]))
	s.IndexStartBlk = binary.BigEndian.Uint32(b[88:])
	s.IndexRootBlk = binary.BigEndian.Uint32(b[92:])
	return s, nil
}

func (s Summary) CheckMatches(d Desc) error {
	if s.KeyRange != d.KeyRange || s.LsnRange != d.LsnRange {
		return fmt.Errorf("%w: summary says %s %s, file name says %s %s",
			ErrBadSummary, s.KeyRange, s.LsnRange, d.KeyRange, d.LsnRange,
		)
	}
	if (s.Magic == DeltaMagic) != d.IsDelta {
		return fmt.Errorf("%w: file kind does not match its name",ErrBadName)
	}
		return nil

}


type BlobRef uint64

func NewBlobRef(pos uint64, willInit bool) BlobRef {
	r := BlobRef(pos << 1)
	if willInit {
		r |= 1
	}
	return r
}

// Pos is the blob's offset in the file.
func (r BlobRef) Pos() uint64 { return uint64(r) >> 1 }

// WillInit reports whether the value is an image or a will_init record.
func (r BlobRef) WillInit() bool { return r&1 != 0 }