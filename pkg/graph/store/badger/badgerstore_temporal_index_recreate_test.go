package badger

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// A temporal envelope prunes: once it covers an ID it must cover every row the
// ID has. These tests cover the ways an ID with history but no current row (a
// deleted entity) became covered by one new row only.

// seedDeletedRel22 writes relationship 7010 of type 9 with history row
// [100,200) and current row [5000,6000), then deletes it: history on disk, no
// current row.
func seedDeletedRel22(t *testing.T, bs *Store) types.RelID {
	t.Helper()
	z := types.RelID(snowflake.ID(7010))
	zr := types.NewRelationship(z, 9, 1, 2)
	zr.SetTemporal(&types.TemporalMetadata{ValidFrom: 100, ValidTo: 200})
	if err := bs.PutRelationship(zr); err != nil {
		t.Fatal(err)
	}
	race22Update(t, bs, zr)
	if err := bs.DeleteRelationship(z); err != nil {
		t.Fatal(err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	if hist, err := bs.GetRelHistory(z); err != nil || len(hist) == 0 {
		t.Fatalf("history of deleted %d: %v, %d rows", z, err, len(hist))
	}
	return z
}

// Backlog 22 final review: a relationship deleted before the create (history
// on disk, no current row) gets a PutRelVersion after Phase 3a took the
// written IDs and before Phase 3b installs. The write folds that one version;
// the install must fold the ID's other rows, or its envelope vouches for
// [9000,9100) alone and a prune at 150 drops it.
func TestCreateRelTemporalIndex_PutRelVersionOnDeletedRelAfter3aCollect(t *testing.T) {
	bs, rels := seedRace22(t)
	z := seedDeletedRel22(t, bs)
	bs.relCache = &relGetSeq{EntityCache: bs.relCache, fire: func(k int) {
		if k == 2 {
			race22Update(t, bs, rels[0]) // 7000 written: re-read in Phase 3a
		}
	}}
	seen := 0
	relTemporalBuildFoldHook = func(id snowflake.ID, v int64) {
		if id != 7000 || v != -1 {
			return
		}
		if seen++; seen == 2 { // 7000's current row folded a second time: the Phase 3a re-read
			nz := types.NewRelationship(z, 9, 1, 2)
			nz.SetTemporal(&types.TemporalMetadata{ValidFrom: 9000, ValidTo: 9100})
			nz.SetVersion(7)
			if err := bs.PutRelVersion(z, 7, nz); err != nil {
				t.Error(err)
			}
		}
	}
	defer func() { relTemporalBuildFoldHook = nil }()
	if err := bs.CreateRelTemporalIndex(9); err != nil {
		t.Fatal(err)
	}
	if seen < 2 {
		t.Fatalf("7000's current row folded %d times, the Phase 3a window was not hit", seen)
	}
	race22MustKeep(t, bs, z, 150, 5500, 9050)
}

// Published index: re-creating a deleted relationship whose history is on disk
// covers that history too, not just the new row.
func TestRelTemporalIndex_RecreateDeletedRelAfterPublish(t *testing.T) {
	bs, _ := seedRace22(t)
	z := seedDeletedRel22(t, bs)
	if err := bs.CreateRelTemporalIndex(9); err != nil {
		t.Fatal(err)
	}
	nz := types.NewRelationship(z, 9, 1, 2)
	nz.SetTemporal(&types.TemporalMetadata{ValidFrom: 9000, ValidTo: 9100})
	if err := bs.PutRelationship(nz); err != nil {
		t.Fatal(err)
	}
	race22MustKeep(t, bs, z, 150, 5500, 9050)
}

// The batch create door, same rule; a brand-new relationship in the same batch
// keeps the envelope of its own row.
func TestRelTemporalIndex_RecreateDeletedRelAfterPublishBatch(t *testing.T) {
	bs, _ := seedRace22(t)
	z := seedDeletedRel22(t, bs)
	if err := bs.CreateRelTemporalIndex(9); err != nil {
		t.Fatal(err)
	}
	nz := types.NewRelationship(z, 9, 1, 2)
	nz.SetTemporal(&types.TemporalMetadata{ValidFrom: 9000, ValidTo: 9100})
	fresh := types.NewRelationship(types.RelID(snowflake.ID(7020)), 9, 1, 2)
	fresh.SetTemporal(&types.TemporalMetadata{ValidFrom: 9000, ValidTo: 9100})
	if err := bs.PutRelationshipsBatch([]*types.Relationship{fresh, nz}); err != nil {
		t.Fatal(err)
	}
	race22MustKeep(t, bs, z, 150, 5500, 9050)
	if kept, ok := bs.PruneRelTypeTemporalCandidates(9, []types.RelID{fresh.ID()}, QueryOpts{ValidAt: 150}); !ok || len(kept) != 0 {
		t.Errorf("fresh relationship kept at 150 (%v, ok=%v): its envelope is [9000,9100)", kept, ok)
	}
}

// seedDeletedNode writes node 8010 with label 7, history row [100,200) and
// current row [5000,6000), then deletes it.
func seedDeletedNode(t *testing.T, bs *Store) types.NodeID {
	t.Helper()
	nid := types.NodeID(snowflake.ID(8010))
	n := types.NewNode(nid, 7, nil)
	n.SetTemporal(&types.TemporalMetadata{ValidFrom: 100, ValidTo: 200})
	if err := bs.PutNode(n); err != nil {
		t.Fatal(err)
	}
	next := types.NewNode(nid, 7, nil)
	next.SetTemporal(&types.TemporalMetadata{ValidFrom: 5000, ValidTo: 6000})
	next.SetVersion(1)
	if err := bs.ReplaceNodeWithHistory(next, 0, n); err != nil {
		t.Fatal(err)
	}
	if err := bs.DeleteNode(nid); err != nil {
		t.Fatal(err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	if hist, err := bs.GetNodeHistory(nid); err != nil || len(hist) == 0 {
		t.Fatalf("history of deleted node %d: %v, %d rows", nid, err, len(hist))
	}
	return nid
}

func nodeMustKeep(t *testing.T, bs *Store, id types.NodeID, at ...types.Instant) {
	t.Helper()
	for _, v := range at {
		kept, ok := bs.PruneTemporalCandidates(7, []types.NodeID{id}, QueryOpts{ValidAt: v})
		if !ok || len(kept) != 1 {
			t.Errorf("prune at %d kept %v (ok=%v), want [%d]: it holds a row valid there", v, kept, ok, id)
		}
	}
}

// Node mirror of TestRelTemporalIndex_RecreateDeletedRelAfterPublish.
func TestTemporalIndex_RecreateDeletedNodeAfterPublish(t *testing.T) {
	bs := newFlushParkStore(t, nil)
	nid := seedDeletedNode(t, bs)
	if err := bs.CreateTemporalIndex(7); err != nil {
		t.Fatal(err)
	}
	nn := types.NewNode(nid, 7, nil)
	nn.SetTemporal(&types.TemporalMetadata{ValidFrom: 9000, ValidTo: 9100})
	if err := bs.PutNode(nn); err != nil {
		t.Fatal(err)
	}
	nodeMustKeep(t, bs, nid, 150, 5500, 9050)
}

// The node batch create door, same rule.
func TestTemporalIndex_RecreateDeletedNodeAfterPublishBatch(t *testing.T) {
	bs := newFlushParkStore(t, nil)
	nid := seedDeletedNode(t, bs)
	if err := bs.CreateTemporalIndex(7); err != nil {
		t.Fatal(err)
	}
	nn := types.NewNode(nid, 7, nil)
	nn.SetTemporal(&types.TemporalMetadata{ValidFrom: 9000, ValidTo: 9100})
	fresh := types.NewNode(types.NodeID(snowflake.ID(8020)), 7, nil)
	fresh.SetTemporal(&types.TemporalMetadata{ValidFrom: 9000, ValidTo: 9100})
	if err := bs.PutNodesBatch([]*types.Node{fresh, nn}); err != nil {
		t.Fatal(err)
	}
	nodeMustKeep(t, bs, nid, 150, 5500, 9050)
	if kept, ok := bs.PruneTemporalCandidates(7, []types.NodeID{fresh.ID()}, QueryOpts{ValidAt: 150}); !ok || len(kept) != 0 {
		t.Errorf("fresh node kept at 150 (%v, ok=%v): its envelope is [9000,9100)", kept, ok)
	}
}
