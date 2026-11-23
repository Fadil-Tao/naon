package timeline

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"

	"github.com/Fadil-Tao/naon/internal/pageserver/ids"
	"github.com/Fadil-Tao/naon/internal/pageserver/iox"
	"github.com/Fadil-Tao/naon/internal/pageserver/lsn"
)

const MetadataFileName = "metadata"

const metadataFormatVersion uint16 = 1

var (
	ErrBadMetadata     = errors.New("timeline: bad metadata file")
	ErrInvalidMetadata = errors.New("timeline: invalid metadata")
)

type Metadata struct {
	DiskConsistentLsn lsn.Lsn
	PrevRecordLsn     lsn.Lsn

	HasAncestor      bool
	AncestorTimeline ids.TimelineID
	AncestorLsn      lsn.Lsn

	LatestGCCutoffLsn lsn.Lsn

	InitdbLsn lsn.Lsn
	PgVersion uint32
}

const (
	metadataBodySize = 63
	metadataSize     = metadataBodySize + 4
)

func (m Metadata) Validate() error {
	if !m.HasAncestor && (m.AncestorTimeline != ids.TimelineID{} || m.AncestorLsn != 0) {
		return fmt.Errorf("%w: ancestor fields set without an ancestor", ErrInvalidMetadata)
	}
	if m.DiskConsistentLsn < m.InitdbLsn {
		return fmt.Errorf("%w: disk_consistent_lsn %s below initdb_lsn %s",
			ErrInvalidMetadata, m.DiskConsistentLsn, m.InitdbLsn)
	}
	if m.LatestGCCutoffLsn > m.DiskConsistentLsn {
		return fmt.Errorf("%w: gc cutoff %s above disk_consistent_lsn %s",
			ErrInvalidMetadata, m.LatestGCCutoffLsn, m.DiskConsistentLsn)
	}
	return nil
}

func (m Metadata) Encode() []byte {
	b := make([]byte, metadataSize)
	binary.BigEndian.PutUint16(b[0:], metadataFormatVersion)
	binary.BigEndian.PutUint64(b[2:], uint64(m.DiskConsistentLsn))
	binary.BigEndian.PutUint64(b[10:], uint64(m.PrevRecordLsn))
	if m.HasAncestor {
		b[18] = 1
	}
	copy(b[19:35], m.AncestorTimeline[:])
	binary.BigEndian.PutUint64(b[35:], uint64(m.AncestorLsn))
	binary.BigEndian.PutUint64(b[43:], uint64(m.LatestGCCutoffLsn))
	binary.BigEndian.PutUint64(b[51:], uint64(m.InitdbLsn))
	binary.BigEndian.PutUint32(b[59:], m.PgVersion)
	binary.BigEndian.PutUint32(b[metadataBodySize:], crc32.ChecksumIEEE(b[:metadataBodySize]))
	return b
}

func DecodeMetadata(b []byte) (Metadata, error) {
	if len(b) != metadataSize {
		return Metadata{}, fmt.Errorf("%w: %d bytes, want %d", ErrBadMetadata, len(b), metadataSize)
	}
	if got, want := binary.BigEndian.Uint32(b[metadataBodySize:]), crc32.ChecksumIEEE(b[:metadataBodySize]); got != want {
		return Metadata{}, fmt.Errorf("%w: checksum mismatch", ErrBadMetadata)
	}
	if v := binary.BigEndian.Uint16(b[0:]); v != metadataFormatVersion {
		return Metadata{}, fmt.Errorf("%w: format version %d, want %d", ErrBadMetadata, v, metadataFormatVersion)
	}
	var m Metadata
	m.DiskConsistentLsn = lsn.Lsn(binary.BigEndian.Uint64(b[2:]))
	m.PrevRecordLsn = lsn.Lsn(binary.BigEndian.Uint64(b[10:]))
	switch b[18] {
	case 0:
	case 1:
		m.HasAncestor = true
	default:
		return Metadata{}, fmt.Errorf("%w: has_ancestor byte is %d", ErrBadMetadata, b[18])
	}
	copy(m.AncestorTimeline[:], b[19:35])
	m.AncestorLsn = lsn.Lsn(binary.BigEndian.Uint64(b[35:]))
	m.LatestGCCutoffLsn = lsn.Lsn(binary.BigEndian.Uint64(b[43:]))
	m.InitdbLsn = lsn.Lsn(binary.BigEndian.Uint64(b[51:]))
	m.PgVersion = binary.BigEndian.Uint32(b[59:])
	if err := m.Validate(); err != nil {
		return Metadata{}, err
	}
	return m, nil
}

func SaveMetadata(path string, m Metadata) error {
	if err := m.Validate(); err != nil {
		return err
	}
	return iox.WriteFile(path, m.Encode())
}

func LoadMetadata(path string) (Metadata, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Metadata{}, fmt.Errorf("timeline: read metadata: %w", err)
	}
	m, err := DecodeMetadata(b)
	if err != nil {
		return Metadata{}, fmt.Errorf("%s: %w", path, err)
	}
	return m, nil
}
