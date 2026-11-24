package waldecode

import (
	"context"
	"encoding/binary"
	"math/rand"
	"path/filepath"
	"testing"
	"time"

	"github.com/Fadil-Tao/naon/internal/pageserver/ids"
	"github.com/Fadil-Tao/naon/internal/pageserver/key"
	"github.com/Fadil-Tao/naon/internal/pageserver/layer/inmemory"
	"github.com/Fadil-Tao/naon/internal/pageserver/lsn"
	"github.com/Fadil-Tao/naon/internal/pageserver/timeline"
	"github.com/Fadil-Tao/naon/internal/pageserver/value"
)

func newTL(t *testing.T, c timeline.Config, p timeline.Plugins) *timeline.Timeline {
	t.Helper()
	tl, err := timeline.Create(filepath.Join(t.TempDir(), "tl"), ids.NewTenantID(), ids.NewTimelineID(), timeline.RootMetadata(0, 0), c, p)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tl.Stop)
	return tl
}

func page1(n uint32, size int) []byte {
	p := make([]byte, size)
	copy(p, "SQLite format 3\x00")
	binary.BigEndian.PutUint32(p[28:], n)
	return p
}

func TestSQLiteKeySpace(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tl := newTL(t, cfg(), plugins)
	if ks, err := SQLiteKeySpace(ctx, tl, 0); err != nil || !ks.IsEmpty() {
		t.Fatalf("empty database: %v %v", ks, err)
	}
	put := func(l lsn.Lsn, b []byte) {
		if err := tl.PutBatch([]inmemory.Put{{Key: PageKey(1), Lsn: l, Value: value.Image(b)}}, l); err != nil {
			t.Fatal(err)
		}
	}
	put(8, page1(5, 64))
	put(16, page1(0, 64))
	put(24, page1(^uint32(0), 64))
	put(32, []byte("short"))

	ks, err := SQLiteKeySpace(ctx, tl, 8)
	want := key.FromRanges(key.KeyRange{Start: PageKey(1), End: PageKey(6)})
	if err != nil || !ks.Equal(want) {
		t.Fatalf("5 pages: %v, %v", ks, err)
	}
	for _, at := range []lsn.Lsn{16, 24} {
		if ks, err := SQLiteKeySpace(ctx, tl, at); err != nil || !ks.IsEmpty() {
			t.Fatalf("@ %s: %v, %v", at, ks, err)
		}
	}
	if _, err := SQLiteKeySpace(ctx, tl, 32); err == nil {
		t.Fatal("short page 1 accepted")
	}
}

func TestImageCreationFromSQLiteWAL(t *testing.T) {
	const nPages = 6
	for round := 0; round < 10; round++ {
		r := rand.New(rand.NewSource(int64(100 + round)))
		stream, points := genWAL(t, r, nPages, 40)
		c := cfg()
		c.ImageCreationThreshold = 1 + r.Intn(3)
		c.ImageTargetSize = uint64(1+r.Intn(nPages)) * key.PageSize
		p := plugins
		p.KeySpace = SQLiteKeySpace
		dir := filepath.Join(t.TempDir(), "tl")
		tenant, id := ids.NewTenantID(), ids.NewTimelineID()
		tl, err := timeline.Create(dir, tenant, id, timeline.RootMetadata(0, 0), c, p)
		if err != nil {
			t.Fatal(err)
		}
		from := lsn.Lsn(0)
		for i := 4; i < len(points); i += 4 {
			ingest(t, r, tl, stream, from, points[i].lsn)
			from = points[i].lsn
			if err := tl.FlushAndWait(context.Background()); err != nil {
				t.Fatal(err)
			}
			if _, err := tl.CreateImageLayers(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
		made := 0
		for _, e := range tl.Layers().Snapshot().Historic() {
			if !e.Desc.IsDelta {
				made++
			}
		}
		ingest(t, r, tl, stream, from, lsn.Lsn(len(stream)))
		if made == 0 {
			t.Fatalf("round %d: no image layers were created", round)
		}
		checkPoints(t, tl, points, nPages)
		tl.Stop()
		if tl, err = timeline.Load(dir, tenant, id, nil, c, p); err != nil {
			t.Fatal(err)
		}
		ingest(t, r, tl, stream, tl.LastRecordLsn(), lsn.Lsn(len(stream)))
		checkPoints(t, tl, points, nPages)
		tl.Stop()
	}
}
