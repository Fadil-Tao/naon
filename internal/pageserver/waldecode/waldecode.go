package waldecode

import (
	"fmt"

	"github.com/Fadil-Tao/naon/internal/pageserver/key"
	"github.com/Fadil-Tao/naon/internal/pageserver/layer/inmemory"
	"github.com/Fadil-Tao/naon/internal/pageserver/lsn"
	"github.com/Fadil-Tao/naon/internal/pageserver/value"
	"github.com/Fadil-Tao/naon/internal/pageserver/walrecord"
)

type Batch struct {
	Puts   []inmemory.Put
	EndLsn lsn.Lsn
	Commit bool
}

type Decoder interface {
	Feed(b []byte)
	Next() (Batch, bool, error)
	Lsn() lsn.Lsn
}

func PageKey(page uint32) key.Key { return key.Key{Field6: page} }

func PageOf(k key.Key) (uint32, bool) {
	if k.Field1 != 0 || k.Field2 != 0 || k.Field3 != 0 || k.Field4 != 0 || k.Field5 != 0 {
		return 0, false
	}
	return k.Field6, true
}

type SQLite struct {
	d *walrecord.Decoder
}

var _ Decoder = (*SQLite)(nil)

func NewSQLite(start lsn.Lsn) (*SQLite, error) {
	d, err := walrecord.NewDecoder(start)
	if err != nil {
		return nil, err
	}
	return &SQLite{d: d}, nil
}

func (s *SQLite) Feed(b []byte) { s.d.Feed(b) }
func (s *SQLite) Lsn() lsn.Lsn  { return s.d.Lsn() }

func (s *SQLite) Next() (Batch, bool, error) {
	rec, ok, err := s.d.Next()
	if err != nil || !ok {
		return Batch{}, ok, err
	}
	b := Batch{EndLsn: rec.End}
	switch r := rec.Rec.(type) {
	case walrecord.PageImage:
		b.Puts = []inmemory.Put{{Key: PageKey(r.Page), Lsn: rec.End, Value: value.Image(r.Data)}}
	case walrecord.PagePatch:
		ranges := make([]value.PatchRange, len(r.Ranges))
		for i, rg := range r.Ranges {
			ranges[i] = value.PatchRange{Off: rg.Off, Data: rg.Data}
		}
		b.Puts = []inmemory.Put{{Key: PageKey(r.Page), Lsn: rec.End, Value: value.Record(value.PagePatch{Ranges: ranges})}}
	case walrecord.Commit:
		b.Commit = true
	default:
		return Batch{}, false, fmt.Errorf("waldecode: unexpected record %T", r)
	}
	return b, true, nil
}
