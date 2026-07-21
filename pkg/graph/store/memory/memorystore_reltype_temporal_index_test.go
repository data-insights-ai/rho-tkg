package memory

import (
	"errors"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func relTypeEnvelopeCovers(t *testing.T, ms *Store, relType uint16, id snowflake.ID, from, to types.Instant) {
	t.Helper()
	ms.mu.RLock()
	ti := ms.relTypeTemporalIndexes[relType]
	ms.mu.RUnlock()
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

// TestCreateRelTemporalIndex_ExistsAndNotFound is the direct test (rule 1) for
// CreateRelTemporalIndex/DropRelTemporalIndex's sentinel-error behavior on the
// memory backend (badger mirror lives in badgerstore_reltype_temporal_index_test.go).
func TestCreateRelTemporalIndex_ExistsAndNotFound(t *testing.T) {
	ms := New()
	t.Cleanup(func() { _ = ms.Close() })
	const relType = uint16(1)

	if err := ms.CreateRelTemporalIndex(relType); err != nil {
		t.Fatalf("CreateRelTemporalIndex: %v", err)
	}
	if err := ms.CreateRelTemporalIndex(relType); !errors.Is(err, ErrTemporalIndexExists) {
		t.Fatalf("double-create = %v, want ErrTemporalIndexExists", err)
	}
	if err := ms.DropRelTemporalIndex(relType); err != nil {
		t.Fatalf("DropRelTemporalIndex: %v", err)
	}
	if err := ms.DropRelTemporalIndex(relType); !errors.Is(err, ErrTemporalIndexNotFound) {
		t.Fatalf("double-drop = %v, want ErrTemporalIndexNotFound", err)
	}
}

// TestRelTypeTemporalEnvelope_ForwardMaintenance is the memory-backend mirror of
// the badger forward-maintenance test: a two-phase test (rule 15) proving the
// write-path wiring at every rel-mutation call site keeps the envelope covering
// a past version's interval after ReplaceRelWithHistory moves the current
// version off it.
func TestRelTypeTemporalEnvelope_ForwardMaintenance(t *testing.T) {
	ms := New()
	t.Cleanup(func() { _ = ms.Close() })
	const relType = uint16(5)
	const label = uint16(1)

	start := types.NewNode(types.NodeID(1), label, nil)
	end := types.NewNode(types.NodeID(2), label, nil)
	if err := ms.PutNode(start); err != nil {
		t.Fatal(err)
	}
	if err := ms.PutNode(end); err != nil {
		t.Fatal(err)
	}

	r := types.NewRelationship(types.RelID(100), relType, types.NodeID(1), types.NodeID(2))
	r.SetTemporal(&types.TemporalMetadata{ValidFrom: 10, ValidTo: 20})
	if err := ms.PutRelationship(r); err != nil {
		t.Fatal(err)
	}
	if err := ms.CreateRelTemporalIndex(relType); err != nil {
		t.Fatal(err)
	}

	updated := types.NewRelationship(types.RelID(100), relType, types.NodeID(1), types.NodeID(2))
	updated.SetTemporal(&types.TemporalMetadata{ValidFrom: 30, ValidTo: 40})
	updated.SetVersion(1)
	if err := ms.ReplaceRelWithHistory(updated, 0, r); err != nil {
		t.Fatal(err)
	}

	relTypeEnvelopeCovers(t, ms, relType, snowflake.ID(100), 10, 20) // past version
	relTypeEnvelopeCovers(t, ms, relType, snowflake.ID(100), 30, 40) // current version
}

// TestRelationshipsByType_TemporalIndexFastPath proves the memory-store
// RelationshipsByType fast path (BACKLOG 21c, the rel-side mirror of
// NodesByLabel's temporal-index fast path) is authoritative: once a rel-type
// temporal index exists, ValidAt/interval queries must be answered ONLY from
// entries the index covers, and a rel outside the queried window must be
// excluded.
func TestRelationshipsByType_TemporalIndexFastPath(t *testing.T) {
	ms := New()
	t.Cleanup(func() { _ = ms.Close() })
	const relType = uint16(9)
	const label = uint16(1)

	start := types.NewNode(types.NodeID(1), label, nil)
	end := types.NewNode(types.NodeID(2), label, nil)
	if err := ms.PutNode(start); err != nil {
		t.Fatal(err)
	}
	if err := ms.PutNode(end); err != nil {
		t.Fatal(err)
	}

	inWindow := types.NewRelationship(types.RelID(300), relType, types.NodeID(1), types.NodeID(2))
	inWindow.SetTemporal(&types.TemporalMetadata{ValidFrom: 10, ValidTo: 20})
	if err := ms.PutRelationship(inWindow); err != nil {
		t.Fatal(err)
	}
	outOfWindow := types.NewRelationship(types.RelID(301), relType, types.NodeID(1), types.NodeID(2))
	outOfWindow.SetTemporal(&types.TemporalMetadata{ValidFrom: 500, ValidTo: 600})
	if err := ms.PutRelationship(outOfWindow); err != nil {
		t.Fatal(err)
	}

	if err := ms.CreateRelTemporalIndex(relType); err != nil {
		t.Fatal(err)
	}

	got, err := ms.RelationshipsByType(relType, QueryOpts{ValidAt: 15})
	if err != nil {
		t.Fatalf("RelationshipsByType: %v", err)
	}
	if len(got) != 1 || got[0].ID() != types.RelID(300) {
		t.Fatalf("RelationshipsByType(ValidAt=15) = %v, want only rel 300", got)
	}

	// A point outside both windows must return empty via the index fast path.
	empty, err := ms.RelationshipsByType(relType, QueryOpts{ValidAt: 1000})
	if err != nil {
		t.Fatalf("RelationshipsByType: %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf("RelationshipsByType(ValidAt=1000) = %v, want empty", empty)
	}
}
