package sharded

import (
	"errors"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// BACKLOG 20n: Clear() truncated every shard (each shard.Clear() already
// resets that shard's OWN vectorIndexes map to empty) but never reset the
// STORE-LEVEL vectorDefs cache — a separate field kept so cross-shard
// SearchNearestNodes/SearchNearestFiltered can re-rank without re-deriving
// dims/metric. Investigated whether this staleness was observably harmful
// and confirmed it already failed safe: SearchNearestNodes still passes the
// (stale) vectorDefFor check, but every shard then uniformly answers
// ErrVectorIndexNotFound and coalesceUniform surfaces that as a single clean
// error rather than wrong/empty data; a later CreateVectorIndex for the same
// key just overwrites the stale entry. Clear() now resets vectorDefs
// directly instead of relying on that downstream error-uniformity — this
// test proves the map is actually empty afterward (a white-box check,
// package-internal since there is no public introspection door for it) and
// that the search-side behavior (ErrVectorIndexNotFound) is unchanged.
func TestClear_ResetsVectorDefs(t *testing.T) {
	st := newMemStore(t, 0, 2)

	const label, propKey = uint16(5), "embedding"
	if err := st.CreateVectorIndex(label, propKey, 2, storecontract.DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}
	st.vectorDefMu.RLock()
	before := len(st.vectorDefs)
	st.vectorDefMu.RUnlock()
	if before != 1 {
		t.Fatalf("vectorDefs before Clear = %d entries, want 1", before)
	}

	if err := st.Clear(); err != nil {
		t.Fatalf("Clear: %v", err)
	}

	st.vectorDefMu.RLock()
	after := len(st.vectorDefs)
	st.vectorDefMu.RUnlock()
	if after != 0 {
		t.Fatalf("vectorDefs after Clear = %d entries, want 0 — BACKLOG 20n regression", after)
	}

	// Search-side behavior is unchanged either way (already failed safe) —
	// pin it so a future change to the reset doesn't silently break this.
	if _, err := st.SearchNearestNodes(label, propKey, []float32{0, 0}, 1, QueryOpts{}); !errors.Is(err, storecontract.ErrVectorIndexNotFound) {
		t.Fatalf("SearchNearestNodes after Clear = %v, want ErrVectorIndexNotFound", err)
	}

	// Recreating the same key after Clear must work cleanly (fresh per-shard
	// state, no leftover confusion from the wiped def).
	if err := st.CreateVectorIndex(label, propKey, 2, storecontract.DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex after Clear: %v", err)
	}
	st.vectorDefMu.RLock()
	recreated := len(st.vectorDefs)
	st.vectorDefMu.RUnlock()
	if recreated != 1 {
		t.Fatalf("vectorDefs after recreate = %d entries, want 1", recreated)
	}
}
