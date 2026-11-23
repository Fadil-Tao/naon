package timeline

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/Fadil-Tao/naon/internal/pageserver/ids"
	"github.com/Fadil-Tao/naon/internal/pageserver/lsn"
)

func rootMeta() Metadata {
	return Metadata{DiskConsistentLsn: 0x5000, PrevRecordLsn: 0x4FF0, LatestGCCutoffLsn: 0x1000, InitdbLsn: 0x100, PgVersion: 17}
}

func branchMeta() Metadata {
	m := rootMeta()
	m.HasAncestor = true
	m.AncestorTimeline = ids.NewTimelineID()
	m.AncestorLsn = 0x3000
	return m
}

func TestMetadataRoundTrip(t *testing.T) {
	for name, m := range map[string]Metadata{"root": rootMeta(), "branch": branchMeta(), "zero": {}} {
		got, err := DecodeMetadata(m.Encode())
		if err != nil || got != m {
			t.Fatalf("%s: got %+v, %v; want %+v", name, got, err, m)
		}
	}
}

func TestMetadataRandomRoundTrip(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	for i := 0; i < 1000; i++ {
		m := Metadata{PgVersion: r.Uint32()}
		m.InitdbLsn = lsn.Lsn(r.Intn(1000))
		m.DiskConsistentLsn = m.InitdbLsn + lsn.Lsn(r.Intn(1000))
		m.LatestGCCutoffLsn = lsn.Lsn(r.Intn(int(m.DiskConsistentLsn) + 1))
		m.PrevRecordLsn = lsn.Lsn(r.Uint64())
		if r.Intn(2) == 0 {
			m.HasAncestor = true
			m.AncestorTimeline = ids.NewTimelineID()
			m.AncestorLsn = lsn.Lsn(r.Uint64())
		}
		got, err := DecodeMetadata(m.Encode())
		if err != nil || got != m {
			t.Fatalf("got %+v, %v; want %+v", got, err, m)
		}
	}
}

func TestMetadataDetectsDamage(t *testing.T) {
	good := branchMeta().Encode()
	for i := range good {
		for _, flip := range []byte{0x01, 0x80, 0xFF} {
			b := append([]byte(nil), good...)
			b[i] ^= flip
			if _, err := DecodeMetadata(b); err == nil {
				t.Fatalf("flipping %#x in byte %d went unnoticed", flip, i)
			}
		}
	}
}

func TestMetadataWrongSize(t *testing.T) {
	good := rootMeta().Encode()
	for _, b := range [][]byte{nil, {}, good[:1], good[:len(good)-1], append(append([]byte(nil), good...), 0)} {
		if _, err := DecodeMetadata(b); !errors.Is(err, ErrBadMetadata) {
			t.Fatalf("len %d: err = %v, want ErrBadMetadata", len(b), err)
		}
	}
}

func TestMetadataFormatVersion(t *testing.T) {
	b := rootMeta().Encode()
	b[1] = 2
	fixCRC(b)
	if _, err := DecodeMetadata(b); !errors.Is(err, ErrBadMetadata) {
		t.Fatalf("err = %v, want ErrBadMetadata", err)
	}
}

func TestMetadataBadAncestorByte(t *testing.T) {
	b := rootMeta().Encode()
	b[18] = 2
	fixCRC(b)
	if _, err := DecodeMetadata(b); !errors.Is(err, ErrBadMetadata) {
		t.Fatalf("err = %v, want ErrBadMetadata", err)
	}
}

func TestMetadataValidate(t *testing.T) {
	bad := map[string]func(m *Metadata){
		"ancestor timeline without ancestor": func(m *Metadata) { m.AncestorTimeline = ids.NewTimelineID() },
		"ancestor lsn without ancestor":      func(m *Metadata) { m.AncestorLsn = 5 },
		"disk consistent below initdb":       func(m *Metadata) { m.DiskConsistentLsn = m.InitdbLsn - 1 },
		"gc cutoff above disk consistent":    func(m *Metadata) { m.LatestGCCutoffLsn = m.DiskConsistentLsn + 1 },
	}
	for name, mutate := range bad {
		m := rootMeta()
		mutate(&m)
		if err := m.Validate(); !errors.Is(err, ErrInvalidMetadata) {
			t.Fatalf("%s: err = %v", name, err)
		}

		if _, err := DecodeMetadata(m.Encode()); !errors.Is(err, ErrInvalidMetadata) {
			t.Fatalf("%s: decode err = %v", name, err)
		}
	}
	for name, m := range map[string]Metadata{
		"root":                         rootMeta(),
		"branch":                       branchMeta(),
		"disk consistent == initdb":    {DiskConsistentLsn: 7, InitdbLsn: 7},
		"gc cutoff == disk consistent": {DiskConsistentLsn: 7, LatestGCCutoffLsn: 7},
	} {
		if err := m.Validate(); err != nil {
			t.Fatalf("%s should be valid: %v", name, err)
		}
	}
}

func TestSaveLoadMetadata(t *testing.T) {
	path := filepath.Join(t.TempDir(), MetadataFileName)
	m := branchMeta()
	if err := SaveMetadata(path, m); err != nil {
		t.Fatal(err)
	}
	got, err := LoadMetadata(path)
	if err != nil || got != m {
		t.Fatalf("got %+v, %v; want %+v", got, err, m)
	}

	m.DiskConsistentLsn += 0x100
	if err := SaveMetadata(path, m); err != nil {
		t.Fatal(err)
	}
	if got, err := LoadMetadata(path); err != nil || got != m {
		t.Fatalf("after overwrite: got %+v, %v", got, err)
	}

	if es, _ := os.ReadDir(filepath.Dir(path)); len(es) != 1 {
		t.Fatalf("dir has %d files, want 1", len(es))
	}
}

func TestSaveInvalidMetadataWritesNothing(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, MetadataFileName)
	m := rootMeta()
	m.LatestGCCutoffLsn = m.DiskConsistentLsn + 1
	if err := SaveMetadata(path, m); !errors.Is(err, ErrInvalidMetadata) {
		t.Fatalf("err = %v", err)
	}
	if es, _ := os.ReadDir(dir); len(es) != 0 {
		t.Fatalf("invalid save left %d files", len(es))
	}
}

func TestLoadMissingAndCorrupt(t *testing.T) {
	dir := t.TempDir()
	if _, err := LoadMetadata(filepath.Join(dir, "nope")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing: err = %v, want os.ErrNotExist", err)
	}
	path := filepath.Join(dir, MetadataFileName)
	if err := os.WriteFile(path, []byte("garbage"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadMetadata(path); !errors.Is(err, ErrBadMetadata) {
		t.Fatalf("corrupt: err = %v, want ErrBadMetadata", err)
	}
}

func FuzzDecodeMetadata(f *testing.F) {
	f.Add(rootMeta().Encode())
	f.Add(branchMeta().Encode())
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		m, err := DecodeMetadata(b)
		if err != nil {
			return
		}
		if m.Validate() != nil {
			t.Fatalf("decoded invalid metadata %+v", m)
		}
		if got, err := DecodeMetadata(m.Encode()); err != nil || got != m {
			t.Fatalf("re-encode mismatch: %+v vs %+v (%v)", got, m, err)
		}
	})
}

func fixCRC(b []byte) {
	crc := crc32.ChecksumIEEE(b[:metadataBodySize])
	binary.BigEndian.PutUint32(b[metadataBodySize:], crc)
}
