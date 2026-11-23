package walredo

import (
	"errors"
	"fmt"

	"github.com/Fadil-Tao/naon/internal/pageserver/key"
	"github.com/Fadil-Tao/naon/internal/pageserver/lsn"
	"github.com/Fadil-Tao/naon/internal/pageserver/value"
)

var (
	ErrNoBase      = errors.New("walredo: patch records without a base image")
	ErrOutOfPage   = errors.New("walredo: patch range outside the page")
	ErrUnsupported = errors.New("walredo: record kind not supported")
)

type Patch struct{}

func (Patch) Reconstruct(k key.Key, at lsn.Lsn, in value.ReconstructInput) ([]byte, error) {
	if !in.HasImage {
		return nil, fmt.Errorf("%w: %s @ %s", ErrNoBase, k, at)
	}
	page := append([]byte(nil), in.Image...)
	for _, r := range in.Records {
		p, ok := r.Rec.(value.PagePatch)
		if !ok {
			return nil, fmt.Errorf("%w: %s at %s", ErrUnsupported, r.Rec.Kind(), r.Lsn)
		}
		if err := Apply(page, p); err != nil {
			return nil, fmt.Errorf("%s @ %s: %w", k, r.Lsn, err)
		}
	}
	return page, nil
}

func Apply(page []byte, p value.PagePatch) error {
	for _, rg := range p.Ranges {
		end := uint64(rg.Off) + uint64(len(rg.Data))
		if end > uint64(len(page)) {
			return fmt.Errorf("%w: [%d, %d) of %d", ErrOutOfPage, rg.Off, end, len(page))
		}
		copy(page[rg.Off:], rg.Data)
	}
	return nil
}
