package tiered

import (
	"testing"

	registrypkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/registry"
)

// --- F3: Store.Clear must wipe store-level indexes ---

// TestTieredStoreClear_ClearsVectorIndexes verifies that the Store-level
// vector index map is reset on Clear so CreateVectorIndex does not return
// ErrVectorIndexExists on a freshly cleared store.
func TestTieredStoreClear_ClearsVectorIndexes(t *testing.T) {
	t.Parallel()
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, _ := reg.GetOrCreate("Case")

	if err := ts.CreateVectorIndex(caseTok, "v", 3, DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}
	if err := ts.Clear(); err != nil {
		t.Fatal(err)
	}
	if err := ts.CreateVectorIndex(caseTok, "v", 3, DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex after Clear: %v", err)
	}
}

// TestTieredStoreClear_ClearsTempIdxLabels verifies that the tracked temporal
// index labels list is wiped on Clear, so a subsequent shard rotation does
// not re-install temporal indexes for stale labels.
func TestTieredStoreClear_ClearsTempIdxLabels(t *testing.T) {
	t.Parallel()
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, _ := reg.GetOrCreate("Case")

	if err := ts.CreateTemporalIndex(caseTok); err != nil {
		t.Fatalf("CreateTemporalIndex: %v", err)
	}

	beforeLen := len(ts.TempIdxLabelsForTest())
	if beforeLen == 0 {
		t.Fatalf("tempIdxLabels not populated after CreateTemporalIndex")
	}

	if err := ts.Clear(); err != nil {
		t.Fatal(err)
	}

	afterLen := len(ts.TempIdxLabelsForTest())
	if afterLen != 0 {
		t.Fatalf("tempIdxLabels after Clear = %d, want 0 (rotation would re-install stale labels)", afterLen)
	}
}
