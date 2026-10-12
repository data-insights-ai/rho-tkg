package memory

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestMemoryRelTypeEpochSurvivesOtherTypes (round 5 request 9, sigma-tkgd
// C4s): RelMutationEpochForType of a type stays put across writes to another
// type, so RelTypeDegreeStats of the first stays cached (a full pass over the
// type after any relationship write, about 400 ms of 1,048,576), and moves on
// every write to the type itself through every door that holds the
// relationship (put, replace, replace with history, history version, batch
// put); the deletes and Clear move every type (coarse, as on badger).
// Faulty implementations caught: the store-wide epoch (red: A moves on a B
// write), a per-type bump that misses a door of the type's own writes.
func TestMemoryRelTypeEpochSurvivesOtherTypes(t *testing.T) {
	ms := New()
	defer ms.Close()
	const typA, typB uint16 = 3, 4
	for _, id := range []int64{1, 2} {
		n := types.NewNode(types.NodeID(id), 1, nil)
		if err := ms.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	rel := func(id int64, typ uint16) *types.Relationship {
		r := types.NewRelationship(types.RelID(id), typ, 1, 2)
		r.SetTemporal(&types.TemporalMetadata{TxFrom: types.Instant(id)})
		return r
	}
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(ms.PutRelationship(rel(101, typA)))
	must(ms.PutRelationship(rel(201, typB)))

	stays := func(what string, write func()) {
		t.Helper()
		a := ms.RelMutationEpochForType(typA)
		write()
		if got := ms.RelMutationEpochForType(typA); got != a {
			t.Errorf("%s (type B) moved type A's epoch %d -> %d", what, a, got)
		}
	}
	moves := func(what string, tok uint16, write func()) {
		t.Helper()
		before := ms.RelMutationEpochForType(tok)
		write()
		if got := ms.RelMutationEpochForType(tok); got == before {
			t.Errorf("%s did not move type %d's epoch", what, tok)
		}
	}

	stays("put", func() { must(ms.PutRelationship(rel(202, typB))) })
	stays("replace", func() { must(ms.ReplaceRelationship(rel(201, typB))) })
	stays("replace with history", func() { must(ms.ReplaceRelWithHistory(rel(201, typB), 0, rel(201, typB))) })
	stays("history version", func() {
		v := rel(202, typB)
		v.SetVersion(0)
		must(ms.PutRelVersion(202, 0, v))
	})
	stays("batch put", func() { must(ms.PutRelationshipsBatch([]*types.Relationship{rel(203, typB), rel(204, typB)})) })

	moves("put", typA, func() { must(ms.PutRelationship(rel(102, typA))) })
	moves("replace", typA, func() { must(ms.ReplaceRelationship(rel(101, typA))) })
	moves("replace with history", typA, func() { must(ms.ReplaceRelWithHistory(rel(101, typA), 0, rel(101, typA))) })
	moves("batch put", typA, func() { must(ms.PutRelationshipsBatch([]*types.Relationship{rel(103, typA)})) })
	moves("history version", typA, func() {
		v := rel(103, typA)
		v.SetVersion(0)
		must(ms.PutRelVersion(103, 0, v))
	})
	moves("delete with history of a B relationship (coarse)", typA, func() {
		tomb := rel(202, typB)
		must(ms.DeleteRelWithHistory(202, 0, tomb))
	})
	moves("delete of a B relationship (coarse)", typA, func() { must(ms.DeleteRelationship(204)) })
	moves("batch delete (coarse)", typA, func() { must(ms.DeleteRelationshipsBatch([]types.RelID{203})) })
	moves("Clear (coarse)", typA, func() { must(ms.Clear()) })
}
