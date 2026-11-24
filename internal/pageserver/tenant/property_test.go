package tenant

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Fadil-Tao/naon/internal/pageserver/ids"
	"github.com/Fadil-Tao/naon/internal/pageserver/layer/inmemory"
	"github.com/Fadil-Tao/naon/internal/pageserver/lsn"
	"github.com/Fadil-Tao/naon/internal/pageserver/timeline"
	"github.com/Fadil-Tao/naon/internal/pageserver/value"
)

type op struct {
	k    uint32
	lsn  lsn.Lsn
	init bool
	img  bool
	text string
}

func (o op) put() inmemory.Put {
	switch {
	case o.img:
		return inmemory.Put{Key: pg(o.k), Lsn: o.lsn, Value: value.Image([]byte(o.text))}
	case o.init:
		return initP(o.k, o.lsn, o.text)
	}
	return appP(o.k, o.lsn, o.text)
}

func replay(ops []op, k uint32) (string, bool) {
	var s string
	ok := false
	for _, o := range ops {
		if o.k != k {
			continue
		}
		if o.init || o.img {
			s = o.text
		} else {
			s += o.text
		}
		ok = true
	}
	return s, ok
}

type sim struct {
	id     ids.TimelineID
	anc    int
	ancLsn lsn.Lsn
	ops    []op
	cur    lsn.Lsn
}

func history(sims []*sim, i int, at lsn.Lsn) []op {
	s := sims[i]
	var out []op
	if s.anc >= 0 {
		out = history(sims, s.anc, lsn.Min(at, s.ancLsn))
	}
	for _, o := range s.ops {
		if o.lsn <= at {
			out = append(out, o)
		}
	}
	return out
}

func genOps(r *rand.Rand, exists map[uint32]bool, after lsn.Lsn, n, nKeys int) []op {
	var ops []op
	cur := after
	for i := 0; i < n; i++ {
		cur += lsn.Lsn(1 + r.Intn(3))
		for _, k := range r.Perm(nKeys)[:1+r.Intn(2)] {
			o := op{k: uint32(k), lsn: cur, text: fmt.Sprintf("%d,", cur)}
			switch {
			case !exists[uint32(k)] || r.Intn(8) == 0:
				o.init = true
				o.img = r.Intn(3) == 0
			}
			exists[uint32(k)] = true
			ops = append(ops, o)
		}
	}
	return ops
}

func ingest(t testing.TB, tl *timeline.Timeline, ops []op) {
	t.Helper()
	for i := 0; i < len(ops); {
		j := i
		var batch []inmemory.Put
		for j < len(ops) && ops[j].lsn == ops[i].lsn {
			batch = append(batch, ops[j].put())
			j++
		}
		write(t, tl, ops[i].lsn, batch...)
		i = j
	}
}

func TestBranchesMatchModelProperty(t *testing.T) {
	rounds := 30
	if testing.Short() {
		rounds = 5
	}
	if s := os.Getenv("NAON_ROUNDS"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil {
			t.Fatal(err)
		}
		rounds = n
	}
	for round := 0; round < rounds; round++ {
		runBranchRound(t, round)
	}
}

func runBranchRound(t *testing.T, round int) {
	const nKeys = 4
	r := rand.New(rand.NewSource(int64(round)))
	e := newEnv(t)
	cfg := testCfg()
	if r.Intn(3) == 0 {
		cfg.CheckpointDistance = uint64(8 + r.Intn(30))
	}
	tn, err := Open(context.Background(), e.dir, e.tenant, cfg.Config, cfg.Plugins)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { tn.Stop() }()

	var log []string
	fail := func(format string, args ...any) {
		t.Helper()
		t.Fatalf("round %d: %s\nsteps:\n  %s", round, fmt.Sprintf(format, args...), strings.Join(log, "\n  "))
	}

	rootID := ids.NewTimelineID()
	if _, err := tn.CreateRoot(rootID, 0, 17); err != nil {
		fail("root: %v", err)
	}
	sims := []*sim{{id: rootID, anc: -1}}
	tl := func(i int) *timeline.Timeline {
		x, ok := tn.Timeline(sims[i].id)
		if !ok {
			fail("timeline %d (%s) missing", i, sims[i].id)
		}
		return x
	}

	reopen := func(flushFirst bool) {
		if flushFirst {
			if err := tn.Shutdown(ctx5(t)); err != nil {
				fail("shutdown: %v", err)
			}
		} else {
			tn.Stop()
		}
		var err error
		if tn, err = Open(context.Background(), e.dir, e.tenant, cfg.Config, cfg.Plugins); err != nil {
			fail("open: %v", err)
		}
		for i, s := range sims {
			x := tl(i)
			var lost []op
			for _, o := range s.ops {
				if o.lsn > x.LastRecordLsn() {
					lost = append(lost, o)
				}
			}
			ingest(t, x, lost)
		}
	}

	check := func(i int, k uint32, at lsn.Lsn) {
		x := tl(i)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		page, err := x.Get(ctx, pg(k), at)
		want, ok := replay(history(sims, i, at), k)
		switch {
		case ok && (err != nil || string(page) != want):
			fail("timeline %d key %d @ %s = %q, %v; want %q", i, k, at, page, err, want)
		case !ok && !errors.Is(err, timeline.ErrNotFound):
			fail("timeline %d key %d @ %s = %q, %v; want ErrNotFound", i, k, at, page, err)
		}
	}
	sample := func(n int) {
		for q := 0; q < n; q++ {
			i := r.Intn(len(sims))
			x := tl(i)
			lo, hi := x.LatestGCCutoffLsn(), x.LastRecordLsn()
			if lo > 0 && r.Intn(6) == 0 {
				at := lsn.Lsn(r.Intn(int(lo)))
				if _, err := x.Get(ctx5(t), pg(0), at); !errors.Is(err, timeline.ErrBelowGCCutoff) {
					fail("timeline %d @ %s below cutoff %s: %v", i, at, lo, err)
				}
				continue
			}
			check(i, uint32(r.Intn(nKeys+1)), lo+lsn.Lsn(r.Intn(int(hi-lo)+1)))
		}
	}

	for step := 0; step < 22; step++ {
		switch x := r.Intn(100); {
		case x < 45:
			i := r.Intn(len(sims))
			exists := map[uint32]bool{}
			for k := uint32(0); k < nKeys; k++ {
				_, exists[k] = replay(history(sims, i, lsn.Max), k)
			}
			s := sims[i]
			ops := genOps(r, exists, s.cur, 1+r.Intn(5), nKeys)
			s.ops = append(s.ops, ops...)
			s.cur = ops[len(ops)-1].lsn
			log = append(log, fmt.Sprintf("write timeline %d up to %s", i, s.cur))
			ingest(t, tl(i), ops)
		case x < 55:
			i := r.Intn(len(sims))
			log = append(log, fmt.Sprintf("flush timeline %d", i))
			if err := tl(i).FlushAndWait(ctx5(t)); err != nil {
				fail("flush: %v", err)
			}
		case x < 78 && len(sims) < 5:
			i := r.Intn(len(sims))
			p := tl(i)
			lo, hi := p.LatestGCCutoffLsn(), p.LastRecordLsn()
			at := lo + lsn.Lsn(r.Intn(int(hi-lo)+1))
			log = append(log, fmt.Sprintf("branch timeline %d at %s", i, at))
			id := ids.NewTimelineID()
			if _, err := tn.Branch(ctx5(t), id, sims[i].id, at); err != nil {
				fail("branch: %v", err)
			}
			sims = append(sims, &sim{id: id, anc: i, ancLsn: at, cur: at})
		case x < 85:
			log = append(log, "clean restart")
			reopen(true)
		case x < 92:
			log = append(log, "crash restart")
			reopen(false)
		default:
			log = append(log, "sample")
		}
		sample(8)
	}

	for i := range sims {
		x := tl(i)
		for k := uint32(0); k <= nKeys; k++ {
			for at := x.LatestGCCutoffLsn(); at <= x.LastRecordLsn(); at++ {
				check(i, k, at)
			}
		}
	}
}
