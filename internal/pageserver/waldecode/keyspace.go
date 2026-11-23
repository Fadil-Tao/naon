package waldecode

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/Fadil-Tao/naon/internal/pageserver/key"
	"github.com/Fadil-Tao/naon/internal/pageserver/lsn"
	"github.com/Fadil-Tao/naon/internal/pageserver/timeline"
)

const headerPageCountOff = 28

var _ timeline.KeySpaceFunc = SQLiteKeySpace

func SQLiteKeySpace(ctx context.Context, t *timeline.Timeline, at lsn.Lsn) (key.KeySpace, error) {
	page1, err := t.Get(ctx, PageKey(1), at)
	if errors.Is(err, timeline.ErrNotFound) {
		return key.KeySpace{}, nil
	}
	if err != nil {
		return key.KeySpace{}, err
	}
	if len(page1) < headerPageCountOff+4 {
		return key.KeySpace{}, fmt.Errorf("waldecode: page 1 is only %d bytes", len(page1))
	}
	n := binary.BigEndian.Uint32(page1[headerPageCountOff:])
	if n == 0 || n == ^uint32(0) {
		return key.KeySpace{}, nil
	}
	return key.FromRanges(key.KeyRange{Start: PageKey(1), End: PageKey(n + 1)}), nil
}
