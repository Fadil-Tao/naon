package waldecode

import (
	"bytes"
	"context"
	"errors"
	"math/rand"
	"path/filepath"
	"testing"
	"time"

	"github.com/Fadil-Tao/naon/internal/pageserver/ids"
	"github.com/Fadil-Tao/naon/internal/pageserver/key"
	"github.com/Fadil-Tao/naon/internal/pageserver/lsn"
	"github.com/Fadil-Tao/naon/internal/pageserver/timeline"
	"github.com/Fadil-Tao/naon/internal/pageserver/value"
	"github.com/Fadil-Tao/naon/internal/pageserver/walrecord"
	"github.com/Fadil-Tao/naon/internal/pageserver/walredo"
)

func TestPageKey(t *testing.T) {
	for _, n := range []uint32{0, 1, 2, 4096, 0xFFFFFFFF} {
		got, ok := PageOf(PageKey(n))
		if !ok || got != n {
			t.Fatalf("page %d -> %v %v", n, got, ok)
		}
	}
	for _, k := range []key.Key{{Field1: 1}, {Field2: 1663}, {Field4: 16400, Field6: 3}, {Field5: 1}, key.MaxKey} {
		if _, ok := PageOf(k); ok {
			t.Fatalf("%s mapped to a SQLite page", k)
		}
	}
	if !PageKey(1).Less(PageKey(2)) {
		t.Fatal("page order is not key order")
	}
}

func TestBatches(t *testing.T) {
	var stream []byte
	for _, r := range []walrecord.Record{
		walrecord.PageImage{Page: 3, Data: []byte("page3")},
		walrecord.PagePatch{Page: 3, Ranges: []walrecord.Range{{Off: 1, Data: []byte("A")}}},
		walrecord.Commit{TxnID: 9, Timestamp: 1},
	} {
		stream, _ = walrecord.Append(stream, r)
	}
	d, err := NewSQLite(64)
	if err != nil {
		t.Fatal(err)
	}
	d.Feed(stream)

	img, ok, err := d.Next()
	if err != nil || !ok || img.Commit || len(img.Puts) != 1 {
		t.Fatalf("image batch: %+v %v %v", img, ok, err)
	}
	p := img.Puts[0]
	if p.Key != PageKey(3) || p.Lsn != img.EndLsn || img.EndLsn <= 64 || !p.Value.IsImage() || string(p.Value.ImageBytes()) != "page3" {
		t.Fatalf("image put: %+v", p)
	}

	patch, ok, err := d.Next()
	if err != nil || !ok || len(patch.Puts) != 1 || patch.EndLsn <= img.EndLsn {
		t.Fatalf("patch batch: %+v %v %v", patch, ok, err)
	}
	pp, isPatch := patch.Puts[0].Value.WalRecord().(value.PagePatch)
	if !isPatch || patch.Puts[0].Lsn != patch.EndLsn || len(pp.Ranges) != 1 || pp.Ranges[0].Off != 1 || string(pp.Ranges[0].Data) != "A" {
		t.Fatalf("patch put: %+v", patch.Puts[0])
	}

	commit, ok, err := d.Next()
	if err != nil || !ok || !commit.Commit || len(commit.Puts) != 0 || commit.EndLsn <= patch.EndLsn {
		t.Fatalf("commit batch: %+v %v %v", commit, ok, err)
	}
	if d.Lsn() != commit.EndLsn || commit.EndLsn != 64+lsn.Lsn(len(stream)) {
		t.Fatalf("decoder at %s, commit ends at %s", d.Lsn(), commit.EndLsn)
	}
	if _, ok, err := d.Next(); ok || err != nil {
		t.Fatalf("after the stream: %v %v", ok, err)
	}
	if _, err := NewSQLite(5); !errors.Is(err, walrecord.ErrUnaligned) {
		t.Fatalf("unaligned start: %v", err)
	}
}

func TestCorruptionStopsTheDecoder(t *testing.T) {
	stream, _ := walrecord.Append(nil, walrecord.Commit{TxnID: 1})
	stream[len(stream)-9] ^= 1
	d, _ := NewSQLite(0)
	d.Feed(stream)
	if _, _, err := d.Next(); !errors.Is(err, walrecord.ErrCorrupt) {
		t.Fatalf("err = %v", err)
	}
}

const pageSize = 64

type commitPoint struct {
	lsn   lsn.Lsn
	pages map[uint32][]byte
}

func genWAL(t *testing.T, r *rand.Rand, nPages, nTxns int) ([]byte, []commitPoint) {
	t.Helper()
	pages := map[uint32][]byte{}
	var stream []byte
	var points []commitPoint
	add := func(rec walrecord.Record) {
		var err error
		if stream, err = walrecord.Append(stream, rec); err != nil {
			t.Fatal(err)
		}
	}
	for txn := 0; txn < nTxns; txn++ {
		for _, i := range r.Perm(nPages)[:1+r.Intn(3)] {
			pg := uint32(i + 1)
			old, exists := pages[pg]
			cur := make([]byte, pageSize)
			if exists {
				copy(cur, old)
				for n := 1 + r.Intn(6); n > 0; n-- {
					cur[r.Intn(pageSize)] = byte(r.Intn(256))
				}
			} else {
				r.Read(cur)
			}
			if !exists || r.Intn(10) == 0 {
				add(walrecord.PageImage{Page: pg, Data: cur})
			} else {
				add(walrecord.PagePatch{Page: pg, Ranges: walrecord.Diff(old, cur, 4)})
			}
			pages[pg] = cur
		}
		add(walrecord.Commit{TxnID: uint64(txn), Timestamp: int64(txn)})
		snap := map[uint32][]byte{}
		for pg, b := range pages {
			snap[pg] = append([]byte(nil), b...)
		}
		points = append(points, commitPoint{lsn: lsn.Lsn(len(stream)), pages: snap})
	}
	return stream, points
}

func cfg() timeline.Config {
	c := timeline.DefaultConfig()
	c.FlushRetryDelay = time.Millisecond
	c.Redo = walredo.Patch{}
	return c
}

func ingest(t *testing.T, r *rand.Rand, tl *timeline.Timeline, stream []byte, from, to lsn.Lsn) {
	t.Helper()
	d, err := NewSQLite(from)
	if err != nil {
		t.Fatal(err)
	}
	for pos := from; pos < to; {
		n := min(lsn.Lsn(1+r.Intn(200)), to-pos)
		d.Feed(stream[pos : pos+n])
		pos += n
		for {
			b, ok, err := d.Next()
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				break
			}
			if err := tl.PutBatch(b.Puts, b.EndLsn); err != nil {
				t.Fatalf("PutBatch @ %s: %v", b.EndLsn, err)
			}
		}
	}
	if d.Lsn() != to {
		t.Fatalf("decoder stopped at %s, want %s", d.Lsn(), to)
	}
}

func checkPoints(t *testing.T, tl *timeline.Timeline, points []commitPoint, nPages int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, p := range points {
		if p.lsn > tl.LastRecordLsn() {
			break
		}
		for pg := uint32(1); pg <= uint32(nPages); pg++ {
			got, err := tl.Get(ctx, PageKey(pg), p.lsn)
			want, exists := p.pages[pg]
			switch {
			case exists && (err != nil || !bytes.Equal(got, want)):
				t.Fatalf("page %d @ %s: %v\n got %x\nwant %x", pg, p.lsn, err, got, want)
			case !exists && !errors.Is(err, timeline.ErrNotFound):
				t.Fatalf("page %d @ %s should not exist yet: %x, %v", pg, p.lsn, got, err)
			}
		}
	}
}

func TestWALToPagesEndToEnd(t *testing.T) {
	const nPages = 6
	for round := 0; round < 20; round++ {
		r := rand.New(rand.NewSource(int64(round)))
		stream, points := genWAL(t, r, nPages, 30+r.Intn(30))
		dir := filepath.Join(t.TempDir(), "tl")
		tenant, id := ids.NewTenantID(), ids.NewTimelineID()
		tl, err := timeline.Create(dir, tenant, id, timeline.RootMetadata(0, 0), cfg())
		if err != nil {
			t.Fatal(err)
		}

		mid := points[len(points)/2].lsn
		ingest(t, r, tl, stream, 0, mid)
		if err := tl.FlushAndWait(context.Background()); err != nil {
			t.Fatal(err)
		}
		end := lsn.Lsn(len(stream))
		ingest(t, r, tl, stream, mid, end)
		if tl.LastRecordLsn() != end {
			t.Fatalf("round %d: last_record_lsn %s, want %s", round, tl.LastRecordLsn(), end)
		}
		checkPoints(t, tl, points, nPages)

		tl.Stop()
		tl, err = timeline.Load(dir, tenant, id, nil, cfg())
		if err != nil {
			t.Fatal(err)
		}
		if tl.LastRecordLsn() != mid {
			t.Fatalf("round %d: recovered at %s, want the flushed point %s", round, tl.LastRecordLsn(), mid)
		}
		ingest(t, r, tl, stream, tl.LastRecordLsn(), end)
		checkPoints(t, tl, points, nPages)
		tl.Stop()
	}
}
