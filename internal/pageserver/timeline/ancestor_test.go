package timeline

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/Fadil-Tao/naon/internal/pageserver/ids"
	"github.com/Fadil-Tao/naon/internal/pageserver/lsn"
)

func TestBranchMetadata(t *testing.T) {
	parentID := ids.NewTimelineID()
	parent := RootMetadata(5, 17)
	parent.LatestGCCutoffLsn = 3
	m := BranchMetadata(parent, parentID, 40)
	if err := m.Validate(); err != nil {
		t.Fatal(err)
	}
	if !m.HasAncestor || m.AncestorTimeline != parentID || m.AncestorLsn != 40 ||
		m.DiskConsistentLsn != 40 || m.LatestGCCutoffLsn != 40 || m.InitdbLsn != 5 || m.PgVersion != 17 {
		t.Fatalf("branch metadata = %+v", m)
	}
}

func TestLoadChecksTheAncestor(t *testing.T) {
	pe := newEnv(t)
	parent := pe.create(t, readCfg())
	other := newEnv(t).create(t, readCfg())

	childDir := filepath.Join(t.TempDir(), "child")
	if err := os.MkdirAll(childDir, 0o755); err != nil {
		t.Fatal(err)
	}
	childID := ids.NewTimelineID()
	meta := BranchMetadata(parent.Metadata(), parent.ID(), 0)
	if err := SaveMetadata(filepath.Join(childDir, MetadataFileName), meta); err != nil {
		t.Fatal(err)
	}

	if _, err := Load(childDir, pe.tenant, childID, nil, readCfg()); !errors.Is(err, ErrAncestorMismatch) {
		t.Fatalf("no ancestor given: %v", err)
	}
	if _, err := Load(childDir, pe.tenant, childID, other, readCfg()); !errors.Is(err, ErrAncestorMismatch) {
		t.Fatalf("wrong ancestor given: %v", err)
	}
	child, err := Load(childDir, pe.tenant, childID, parent, readCfg())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(child.Stop)
	if child.Ancestor() != parent || child.AncestorLsn() != 0 {
		t.Fatalf("ancestor = %v @ %s", child.Ancestor(), child.AncestorLsn())
	}

	if _, err := Load(pe.dir, pe.tenant, pe.id, parent, readCfg()); !errors.Is(err, ErrAncestorMismatch) {
		t.Fatalf("ancestor given to a root: %v", err)
	}
	if _, err := Create(filepath.Join(t.TempDir(), "x"), pe.tenant, childID, meta, readCfg()); !errors.Is(err, ErrNeedsAncestor) {
		t.Fatalf("Create with an ancestor in the metadata: %v", err)
	}
}

func TestRetainLsnRefcount(t *testing.T) {
	tl := newEnv(t).create(t, testCfg())
	if got := tl.RetainLsns(); len(got) != 0 {
		t.Fatalf("fresh timeline retains %v", got)
	}
	for _, l := range []lsn.Lsn{30, 10, 10, 20} {
		tl.AddRetainLsn(l)
	}
	if got := tl.RetainLsns(); !slices.Equal(got, []lsn.Lsn{10, 20, 30}) {
		t.Fatalf("retain = %v", got)
	}
	tl.RemoveRetainLsn(10)
	if got := tl.RetainLsns(); !slices.Equal(got, []lsn.Lsn{10, 20, 30}) {
		t.Fatalf("after removing one of two pins: %v", got)
	}
	tl.RemoveRetainLsn(10)
	tl.RemoveRetainLsn(99)
	if got := tl.RetainLsns(); !slices.Equal(got, []lsn.Lsn{20, 30}) {
		t.Fatalf("retain = %v", got)
	}
}
