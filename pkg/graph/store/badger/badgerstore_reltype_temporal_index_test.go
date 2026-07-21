package badger

import (
	"errors"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func relEnvelopeCovers(t *testing.T, bs *Store, relType uint16, id snowflake.ID, from, to types.Instant) {
	t.Helper()
	bs.idxMu.RLock()
	ti := bs.relTypeTemporalIndexes[relType]
	bs.idxMu.RUnlock()
	if ti == nil {
		t.Fatalf("no rel-type temporal index for type %d", relType)
	}
	got := ti.QueryOverlap(from, to)
	for _, g := range got {
		if g == id {
			return
		}
	}
	t.Fatalf("rel-type envelope for type %d does not cover window [%d,%d) for id %d (got %v)", relType, from, to, id, got)
}

func putRelTypeTemporalTestRel(t *testing.T, bs *Store, relType uint16, rid types.RelID, startID, endID types.NodeID, from, to types.Instant) *types.Relationship {
	t.Helper()
	r := types.NewRelationship(rid, relType, startID, endID)
	r.SetTemporal(&types.TemporalMetadata{ValidFrom: from, ValidTo: to})
	if err := bs.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}
	return r
}

// TestCreateRelTemporalIndex_ExistsAndNotFound is the direct test (rule 1) for
// CreateRelTemporalIndex/DropRelTemporalIndex's sentinel-error behavior — the
// rel-side mirror of the node CreateTemporalIndex/DropTemporalIndex contract.
func TestCreateRelTemporalIndex_ExistsAndNotFound(t *testing.T) {
	bs, err := New(Config{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { bs.Close() })
	const relType = uint16(1)

	if err := bs.CreateRelTemporalIndex(relType); err != nil {
		t.Fatalf("CreateRelTemporalIndex: %v", err)
	}
	if err := bs.CreateRelTemporalIndex(relType); !errors.Is(err, ErrTemporalIndexExists) {
		t.Fatalf("double-create = %v, want ErrTemporalIndexExists", err)
	}
	if err := bs.DropRelTemporalIndex(relType); err != nil {
		t.Fatalf("DropRelTemporalIndex: %v", err)
	}
	if err := bs.DropRelTemporalIndex(relType); !errors.Is(err, ErrTemporalIndexNotFound) {
		t.Fatalf("double-drop = %v, want ErrTemporalIndexNotFound", err)
	}
}

// TestRelTypeTemporalEnvelope_ForwardMaintenance is the rel-side mirror of
// TestTemporalEnvelope_ForwardMaintenance: proves the write-path maintenance
// wiring (maintainRelTypeTemporalIndexesAdd/Remove at every rel-mutation door)
// keeps the envelope covering a past version's interval after
// ReplaceRelWithHistory moves the current version off it — a two-phase test
// (rule 15): create at t0 in [10,20), mutate to [30,40), assert the envelope
// still answers the t0 window.
func TestRelTypeTemporalEnvelope_ForwardMaintenance(t *testing.T) {
	bs, err := New(Config{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { bs.Close() })
	const relType = uint16(5)
	const label = uint16(1)

	start := types.NewNode(types.NodeID(1), label, nil)
	end := types.NewNode(types.NodeID(2), label, nil)
	if err := bs.PutNode(start); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNode(end); err != nil {
		t.Fatal(err)
	}

	r := putRelTypeTemporalTestRel(t, bs, relType, types.RelID(100), types.NodeID(1), types.NodeID(2), 10, 20)
	if err := bs.CreateRelTemporalIndex(relType); err != nil {
		t.Fatal(err)
	}

	updated := types.NewRelationship(types.RelID(100), relType, types.NodeID(1), types.NodeID(2))
	updated.SetTemporal(&types.TemporalMetadata{ValidFrom: 30, ValidTo: 40})
	updated.SetVersion(1)
	if err := bs.ReplaceRelWithHistory(updated, 0, r); err != nil {
		t.Fatal(err)
	}

	relEnvelopeCovers(t, bs, relType, snowflake.ID(100), 10, 20) // past version
	relEnvelopeCovers(t, bs, relType, snowflake.ID(100), 30, 40) // current version
}

// TestRelTypeTemporalIndex_DoesNotSurviveRestart pins the deliberate BACKLOG 21c
// scope decision (see relTypeTemporalIndexes field doc comment): unlike the
// node-side temporal index, the rel-type mirror is NOT persisted across reopen.
// This is safe (PruneRelTypeTemporalCandidates is a sound-superset optimization
// — an absent index just costs pruning recall, never correctness), and this
// test makes the limitation explicit rather than silent.
func TestRelTypeTemporalIndex_DoesNotSurviveRestart(t *testing.T) {
	dir := t.TempDir()
	bs, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	const relType = uint16(7)
	const label = uint16(1)

	start := types.NewNode(types.NodeID(1), label, nil)
	end := types.NewNode(types.NodeID(2), label, nil)
	if err := bs.PutNode(start); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNode(end); err != nil {
		t.Fatal(err)
	}
	putRelTypeTemporalTestRel(t, bs, relType, types.RelID(200), types.NodeID(1), types.NodeID(2), 10, 20)
	if err := bs.CreateRelTemporalIndex(relType); err != nil {
		t.Fatal(err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}

	bs2, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { bs2.Close() })

	bs2.idxMu.RLock()
	_, exists := bs2.relTypeTemporalIndexes[relType]
	bs2.idxMu.RUnlock()
	if exists {
		t.Fatal("rel-type temporal index survived restart — scope decision changed, update the doc comment and this test together")
	}
	// The data itself must still be intact and queryable (RelationshipsByType
	// falls back to a full scan without the index — correctness unaffected).
	rels, err := bs2.RelationshipsByType(relType, QueryOpts{})
	if err != nil {
		t.Fatalf("RelationshipsByType after reopen: %v", err)
	}
	if len(rels) != 1 || rels[0].ID() != types.RelID(200) {
		t.Fatalf("RelationshipsByType after reopen = %v, want the single rel", rels)
	}
}
