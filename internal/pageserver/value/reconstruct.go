package value

import (
	"errors"
	"fmt"
	"slices"

	"github.com/Fadil-Tao/naon/internal/pageserver/lsn"
)

type Situation uint8

const (
	Continue Situation = iota
	Complete
)

func (s Situation) String() string {
	if s == Complete {
		return "Complete"
	}
	return "Continue"
}

var (
	ErrAlreadyComplete = errors.New("reconstruct: value added after state was complete")
	ErrLsnOrder = errors.New("reconstruct: LSNs must strictly decrease")
	ErrMissingBase = errors.New("reconstruct: records have no base image")
)

type LsnRecord struct {
	Lsn lsn.Lsn
	Rec WalRecord
}

type ReconstructState struct {
	hasImage  bool
	imageLsn  lsn.Lsn
	image     []byte
	records   []LsnRecord 
	situation Situation
	lastLsn   lsn.Lsn
	started   bool
	absent    bool
}

func (s *ReconstructState) Add(l lsn.Lsn, v Value) error {
	if s.situation == Complete {
		return ErrAlreadyComplete
	}
	if s.started && l >= s.lastLsn {
		return fmt.Errorf("%w: got %s after %s", ErrLsnOrder, l, s.lastLsn)
	}
	s.started = true
	s.lastLsn = l

	if v.IsImage() {
		s.hasImage = true
		s.imageLsn = l
		s.image = v.ImageBytes()
		s.situation = Complete
		return nil
	}
	s.records = append(s.records, LsnRecord{Lsn: l, Rec: v.WalRecord()})
	if v.WillInit() {
		s.situation = Complete
	}
	return nil
}

func (s *ReconstructState) MarkAbsent() error {
	if s.situation == Complete {
		return ErrAlreadyComplete
	}
	if len(s.records) > 0 {
		return fmt.Errorf("%w: %d records found above it", ErrMissingBase, len(s.records))
	}
	s.absent = true
	s.situation = Complete
	return nil
}

func (s *ReconstructState) IsAbsent() bool { return s.absent }

func (s *ReconstructState) Situation() Situation { return s.situation }

func (s *ReconstructState) IsComplete() bool { return s.situation == Complete }

func (s *ReconstructState) IsEmpty() bool { return !s.started }

type ReconstructInput struct {
	HasImage bool
	ImageLsn lsn.Lsn
	Image    []byte
	Records  []LsnRecord // oldest first
}

// Finish returns the collected data in redo order (oldest record first).
func (s *ReconstructState) Finish() ReconstructInput {
	recs := slices.Clone(s.records)
	slices.Reverse(recs)
	return ReconstructInput{
		HasImage: s.hasImage,
		ImageLsn: s.imageLsn,
		Image:    s.image,
		Records:  recs,
	}
}

func (s *ReconstructState) NumDeltas() int {
	if s.hasImage || len(s.records) == 0 {
		return len(s.records)
	}
	return len(s.records) - 1
}

func ApplyTest(in ReconstructInput) ([]byte, error) {
	var page []byte
	if in.HasImage {
		page = slices.Clone(in.Image)
	}
	for _, r := range in.Records {
		tr, ok := r.Rec.(TestRecord)
		if !ok {
			return nil, fmt.Errorf("%w: %s at %s", ErrNotTestRecord, r.Rec.Kind(), r.Lsn)
		}
		if tr.Clear || tr.Init {
			page = page[:0]
		}
		page = append(page, tr.Append...)
	}
	return page, nil
}