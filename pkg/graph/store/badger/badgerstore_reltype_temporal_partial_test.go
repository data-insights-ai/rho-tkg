package badger

import (
	"errors"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 10: the tiered store writes a cross-shard relationship's entity
// through PutRelEntityAndOut (its incoming leg lives on the end node's shard)
// and removes it through DeleteRelEntityAndOut. The relationship row lives
// entirely on this shard, so its rel-type temporal envelope must be
// maintained exactly as PutRelationship / deleteRelByInfo maintain it — else a
// tiered shard keeps every cross-shard row it could prune (lost recall) and a
// deleted row stays covered by a stale envelope.
func TestRelTypeTemporalIndex_PartialDoorsMaintainEnvelope(t *testing.T) {
	bs := newTestBadgerStoreInMemory(t)
	const relType, label = uint16(6), uint16(1)
	if err := bs.PutNode(types.NewNode(types.NodeID(1), label, nil)); err != nil {
		t.Fatal(err)
	}
	if err := bs.CreateRelTemporalIndex(relType); err != nil {
		t.Fatalf("CreateRelTemporalIndex: %v", err)
	}
	put := func(id types.RelID, from, to types.Instant) {
		t.Helper()
		r := types.NewRelationship(id, relType, types.NodeID(1), types.NodeID(99)) // end node lives on another shard
		r.SetTemporal(&types.TemporalMetadata{ValidFrom: from, ValidTo: to})
		if err := bs.PutRelEntityAndOut(r); err != nil {
			t.Fatalf("PutRelEntityAndOut(%d): %v", id, err)
		}
	}
	put(101, 1000, 2000)
	put(102, 3000, 0)
	ids := []types.RelID{101, 102}

	kept, ok := bs.PruneRelTypeTemporalCandidates(relType, ids, QueryOpts{ValidAt: 2500})
	if !ok || len(kept) != 0 {
		t.Errorf("at 2500 kept %v (ok=%v), want none: both rows are covered and cannot overlap", kept, ok)
	}
	kept, _ = bs.PruneRelTypeTemporalCandidates(relType, ids, QueryOpts{ValidAt: 1500})
	if !slices.Equal(kept, []types.RelID{101}) {
		t.Errorf("at 1500 kept %v, want [101]", kept)
	}
	kept, _ = bs.PruneRelTypeTemporalCandidates(relType, ids, QueryOpts{ValidStart: 1900, ValidEnd: 3100})
	if !slices.Equal(kept, ids) {
		t.Errorf("during 1900-3100 kept %v, want %v", kept, ids)
	}

	// Delete: the row is gone from this shard, so no envelope may vouch for it.
	if _, err := bs.DeleteRelEntityAndOut(101); err != nil {
		t.Fatalf("DeleteRelEntityAndOut: %v", err)
	}
	bs.idxMu.RLock()
	_, _, covered := bs.relTypeTemporalIndexes[relType].EnvelopeOf(101)
	bs.idxMu.RUnlock()
	if covered {
		t.Error("deleted cross-shard row still covered by the envelope")
	}
	kept, _ = bs.PruneRelTypeTemporalCandidates(relType, ids, QueryOpts{ValidAt: 2500})
	if !slices.Equal(kept, []types.RelID{101}) {
		t.Errorf("after delete at 2500 kept %v, want [101] (uncovered ids are always kept)", kept)
	}
}

// PutRelVersion joins a LIVE relationship's past version into its rel-type
// temporal envelope (an imported past version stays findable), and leaves a
// relationship without a current row uncovered (never pruned) instead of
// vouching for one version while its other rows stay outside.
func TestRelTypeTemporalIndex_PutRelVersionExtendsLiveOnly(t *testing.T) {
	bs := newTestBadgerStoreInMemory(t)
	const relType, label = uint16(6), uint16(1)
	for _, id := range []types.NodeID{1, 2} {
		if err := bs.PutNode(types.NewNode(id, label, nil)); err != nil {
			t.Fatal(err)
		}
	}
	if err := bs.CreateRelTemporalIndex(relType); err != nil {
		t.Fatal(err)
	}
	live := types.NewRelationship(101, relType, 1, 2)
	live.SetTemporal(&types.TemporalMetadata{ValidFrom: 3000})
	live.SetVersion(2)
	if err := bs.PutRelationship(live); err != nil {
		t.Fatal(err)
	}
	past := types.NewRelationship(101, relType, 1, 2)
	past.SetTemporal(&types.TemporalMetadata{ValidFrom: 1000, ValidTo: 2000})
	past.SetVersion(1)
	if err := bs.PutRelVersion(101, 1, past); err != nil {
		t.Fatal(err)
	}
	orphan := types.NewRelationship(102, relType, 1, 2) // history only, no current row
	orphan.SetTemporal(&types.TemporalMetadata{ValidFrom: 8000, ValidTo: 9000})
	orphan.SetVersion(3)
	if err := bs.PutRelVersion(102, 3, orphan); err != nil {
		t.Fatal(err)
	}
	ids := []types.RelID{101, 102}
	kept, ok := bs.PruneRelTypeTemporalCandidates(relType, ids, QueryOpts{ValidAt: 1500})
	if !ok || !slices.Equal(kept, ids) {
		t.Errorf("at 1500 kept %v (ok=%v), want both: 101's past version and the uncovered 102", kept, ok)
	}
	kept, _ = bs.PruneRelTypeTemporalCandidates(relType, ids, QueryOpts{ValidAt: 500})
	if !slices.Equal(kept, []types.RelID{102}) {
		t.Errorf("at 500 kept %v, want [102] (101's envelope starts at 1000, 102 is not covered)", kept)
	}
	bs.idxMu.RLock()
	_, _, covered := bs.relTypeTemporalIndexes[relType].EnvelopeOf(102)
	bs.idxMu.RUnlock()
	if covered {
		t.Error("a relationship without a current row got an envelope from one history version")
	}
}

// Config.DropRelTemporalIndexesAtOpen: the persisted rel-type temporal index
// definitions are discarded at open without a build (the tiered cold-shard
// open), a read-only open with it only skips the build, and a later plain
// open finds no definition to rebuild.
func TestDropRelTemporalIndexesAtOpen_DiscardsWithoutBuilding(t *testing.T) {
	dir := t.TempDir()
	bs, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNode(types.NewNode(types.NodeID(1), 1, nil)); err != nil {
		t.Fatal(err)
	}
	for _, typ := range []uint16{6, 7} {
		if err := bs.CreateRelTemporalIndex(typ); err != nil {
			t.Fatal(err)
		}
	}
	if got := bs.RelTemporalIndexBuildsForTest(); got != 2 {
		t.Fatalf("builds after two creates = %d, want 2", got)
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}

	ro, err := New(Config{Dir: dir, ReadOnly: true, DropRelTemporalIndexesAtOpen: true})
	if err != nil {
		t.Fatalf("read-only open: %v", err)
	}
	if got, _ := ro.RelTemporalIndexTypes(); len(got) != 0 || ro.RelTemporalIndexBuildsForTest() != 0 {
		t.Errorf("read-only discard open lists %v after %d builds, want none and 0", got, ro.RelTemporalIndexBuildsForTest())
	}
	if err := ro.Close(); err != nil {
		t.Fatal(err)
	}

	plain, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := plain.RelTemporalIndexTypes(); !slices.Equal(got, []uint16{6, 7}) || plain.RelTemporalIndexBuildsForTest() != 2 {
		t.Errorf("plain open after a read-only discard lists %v after %d builds, want [6 7] and 2 (read-only keeps the definitions)", got, plain.RelTemporalIndexBuildsForTest())
	}
	if err := plain.Close(); err != nil {
		t.Fatal(err)
	}

	cold, err := New(Config{Dir: dir, DropRelTemporalIndexesAtOpen: true})
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := cold.RelTemporalIndexTypes(); len(got) != 0 || cold.RelTemporalIndexBuildsForTest() != 0 {
		t.Errorf("discard open lists %v after %d builds, want none and 0", got, cold.RelTemporalIndexBuildsForTest())
	}
	if err := cold.Close(); err != nil {
		t.Fatal(err)
	}
	again, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = again.Close() })
	if got, _ := again.RelTemporalIndexTypes(); len(got) != 0 || again.RelTemporalIndexBuildsForTest() != 0 {
		t.Errorf("plain open after a discard lists %v after %d builds, want none and 0", got, again.RelTemporalIndexBuildsForTest())
	}
}

// CompositePropertyIndexDefs (direct test, rule 1): every definition across
// labels, copies, dropped definitions and labels gone, closed store refused.
func TestCompositePropertyIndexDefs_ListsEveryLabel(t *testing.T) {
	bs := newTestBadgerStoreInMemory(t)
	if defs, err := bs.CompositePropertyIndexDefs(); err != nil || len(defs) != 0 {
		t.Fatalf("empty store defs = %v, %v; want empty", defs, err)
	}
	for _, def := range []struct {
		label uint16
		keys  []string
	}{{3, []string{"a", "b"}}, {3, []string{"b", "a"}}, {5, []string{"x", "y", "z"}}} {
		if err := bs.CreateCompositePropertyIndex(def.label, def.keys); err != nil {
			t.Fatalf("create %d %v: %v", def.label, def.keys, err)
		}
	}
	defs, err := bs.CompositePropertyIndexDefs()
	if err != nil {
		t.Fatal(err)
	}
	if len(defs) != 2 || len(defs[3]) != 2 || !slices.Equal(defs[3][0], []string{"a", "b"}) ||
		!slices.Equal(defs[3][1], []string{"b", "a"}) || len(defs[5]) != 1 || !slices.Equal(defs[5][0], []string{"x", "y", "z"}) {
		t.Fatalf("defs = %v", defs)
	}
	defs[5][0][0] = "mutated"
	if again, _ := bs.CompositePropertyIndexDefs(); again[5][0][0] != "x" {
		t.Fatal("returned keys alias the store's definitions")
	}
	if err := bs.DropCompositePropertyIndex(5, []string{"x", "y", "z"}); err != nil {
		t.Fatal(err)
	}
	if defs, _ := bs.CompositePropertyIndexDefs(); len(defs) != 1 || len(defs[5]) != 0 {
		t.Fatalf("after drop defs = %v, want only label 3", defs)
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := bs.CompositePropertyIndexDefs(); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("closed store: err = %v, want ErrStoreClosed", err)
	}
}
