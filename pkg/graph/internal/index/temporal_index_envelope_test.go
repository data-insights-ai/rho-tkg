package index

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// B4 Stage 1 — the per-node valid-time ENVELOPE. Extend maintains a sound superset
// across all versions, where Add keeps only the current version. These tests pin
// the sound-superset property the core resolver will rely on for predicate-anywhere
// temporal queries (rule 16).

func containsID(ids []snowflake.ID, want snowflake.ID) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}

// Extend inserts when absent and unions (grows the envelope) when present.
func TestTemporalIndexExtend_InsertThenUnion(t *testing.T) {
	t.Parallel()
	ti := NewTemporalIndex()
	const id = snowflake.ID(7)

	// Insert: current version valid [30,40).
	ti.Extend(id, 30, 40)
	if got := ti.QueryOverlap(30, 40); !containsID(got, id) {
		t.Fatalf("after insert, QueryOverlap[30,40) missing id: %v", got)
	}
	// A probe over an EARLIER window must NOT match yet — only [30,40) is known.
	if got := ti.QueryOverlap(10, 20); containsID(got, id) {
		t.Fatalf("before union, QueryOverlap[10,20) should not contain id: %v", got)
	}

	// Union in a PAST version's interval [10,20): the envelope becomes [10,40).
	ti.Extend(id, 10, 20)
	if got := ti.QueryOverlap(12, 15); !containsID(got, id) {
		t.Fatalf("SOUND-SUPERSET FAIL: past-version window [12,15) missing id after union: %v", got)
	}
	if got := ti.QueryOverlap(32, 35); !containsID(got, id) {
		t.Fatalf("current-version window [32,35) missing id after union: %v", got)
	}
	// Exactly one entry — union widens in place, never duplicates.
	if n := ti.Len(); n != 1 {
		t.Fatalf("Len after union = %d, want 1 (envelope is one entry per id)", n)
	}
}

// The exact regression the memory-store probe exposed: Add-style current-version
// semantics MISSES a past-version overlap; Extend's envelope KEEPS it.
func TestTemporalIndexExtend_PastVersionOverlapRetained(t *testing.T) {
	t.Parallel()
	ti := NewTemporalIndex()
	const id = snowflake.ID(100)
	ti.Extend(id, 10, 20) // v0 valid [10,20)
	ti.Extend(id, 30, 40) // v1 valid [30,40) — current

	// Envelope [10,40): a probe where the PAST version held must still match.
	if got := ti.QueryOverlap(10, 20); !containsID(got, id) {
		t.Fatalf("past-version window [10,20) missing after current moved to [30,40): %v", got)
	}
	if got := ti.QueryAt(15); !containsID(got, id) {
		t.Fatalf("QueryAt(15) (inside past version) missing id: %v", got)
	}
}

// Open-ended (To == 0 == +infinity) absorbs any bounded end.
func TestTemporalIndexExtend_OpenEndedTo(t *testing.T) {
	t.Parallel()
	ti := NewTemporalIndex()
	const id = snowflake.ID(5)
	ti.Extend(id, 10, 20) // bounded
	ti.Extend(id, 30, 0)  // open-ended current version

	if got := ti.QueryAt(1_000_000); !containsID(got, id) {
		t.Fatalf("open-ended envelope should match a far-future probe: %v", got)
	}
	// And still covers the earlier bounded interval.
	if got := ti.QueryAt(15); !containsID(got, id) {
		t.Fatalf("open-ended union must retain the earlier bounded interval: %v", got)
	}
}

// The store-level maintenance helper grows the envelope over a node's labels.
func TestExtendNodeInTemporalIndexes(t *testing.T) {
	t.Parallel()
	const label = uint16(2)
	idxs := map[uint16]*TemporalIndex{label: NewTemporalIndex()}

	n := types.NewNode(types.NodeID(snowflake.ID(42)), label, nil)
	n.SetTemporal(&types.TemporalMetadata{ValidFrom: 100, ValidTo: 200})
	ExtendNodeInTemporalIndexes(idxs, n, snowflake.ID(42))

	// Simulate an update: the node's current version moves to [300,400).
	n2 := types.NewNode(types.NodeID(snowflake.ID(42)), label, nil)
	n2.SetTemporal(&types.TemporalMetadata{ValidFrom: 300, ValidTo: 400})
	ExtendNodeInTemporalIndexes(idxs, n2, snowflake.ID(42))

	ti := idxs[label]
	if got := ti.QueryOverlap(100, 200); !containsID(got, snowflake.ID(42)) {
		t.Fatalf("envelope lost the original [100,200) window after update: %v", got)
	}
	if got := ti.QueryOverlap(300, 400); !containsID(got, snowflake.ID(42)) {
		t.Fatalf("envelope missing the current [300,400) window: %v", got)
	}
	if n := ti.Len(); n != 1 {
		t.Fatalf("one node → one envelope entry, got Len=%d", n)
	}
}

// A nil index is a safe no-op (mirrors Add).
func TestTemporalIndexExtend_NilSafe(t *testing.T) {
	t.Parallel()
	var ti *TemporalIndex
	ti.Extend(snowflake.ID(1), 1, 2) // must not panic
	ExtendNodeInTemporalIndexes(nil, nil, snowflake.ID(1))
}
