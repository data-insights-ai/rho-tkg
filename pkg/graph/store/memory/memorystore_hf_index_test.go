package memory

import (
	"errors"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func TestMemoryStoreHighFrequencyIndex_BackfillsExistingNodes(t *testing.T) {
	t.Parallel()
	ms := New()

	n := types.NewNode(types.NodeID(snowflake.ID(100)), 1, nil)
	n.SetTemporal(&types.TemporalMetadata{ValidFrom: 3_600_000})
	if err := ms.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := ms.CreateHighFrequencyIndex(1, time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex: %v", err)
	}

	got := ms.HFIndexPointQueryForTest(1, 3_600_000)
	if !containsHFNodeID(got, n.ID()) {
		t.Fatalf("HFI point query = %v, want node %d", got, n.ID())
	}
}

func TestMemoryStoreHighFrequencyIndex_MaintainsNodeWrites(t *testing.T) {
	t.Parallel()
	ms := New()

	if err := ms.CreateHighFrequencyIndex(1, time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex: %v", err)
	}

	n := types.NewNode(types.NodeID(snowflake.ID(100)), 1, nil)
	n.SetTemporal(&types.TemporalMetadata{ValidFrom: 3_600_000})
	if err := ms.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if got := ms.HFIndexPointQueryForTest(1, 3_600_000); !containsHFNodeID(got, n.ID()) {
		t.Fatalf("HFI point query after PutNode = %v, want node %d", got, n.ID())
	}

	updated := n.DeepCopy()
	updated.SetTemporal(&types.TemporalMetadata{ValidFrom: 7_200_000})
	if err := ms.ReplaceNode(updated); err != nil {
		t.Fatalf("ReplaceNode: %v", err)
	}
	if got := ms.HFIndexPointQueryForTest(1, 3_600_000); containsHFNodeID(got, n.ID()) {
		t.Fatalf("HFI old bucket after ReplaceNode = %v, want node removed", got)
	}
	if got := ms.HFIndexPointQueryForTest(1, 7_200_000); !containsHFNodeID(got, n.ID()) {
		t.Fatalf("HFI new bucket after ReplaceNode = %v, want node %d", got, n.ID())
	}

	if err := ms.DeleteNode(n.ID()); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}
	if got := ms.HFIndexPointQueryForTest(1, 7_200_000); containsHFNodeID(got, n.ID()) {
		t.Fatalf("HFI point query after DeleteNode = %v, want node removed", got)
	}
}

func TestMemoryStoreHighFrequencyIndex_NodesByLabelUsesSafeTemporalCandidates(t *testing.T) {
	t.Parallel()
	ms := New()

	oldOpen := types.NewNode(types.NodeID(snowflake.ID(100)), 1, nil)
	oldOpen.SetTemporal(&types.TemporalMetadata{ValidFrom: 3_600_000})
	later := types.NewNode(types.NodeID(snowflake.ID(101)), 1, nil)
	later.SetTemporal(&types.TemporalMetadata{ValidFrom: 7_200_000})
	if err := ms.PutNode(oldOpen); err != nil {
		t.Fatalf("PutNode oldOpen: %v", err)
	}
	if err := ms.PutNode(later); err != nil {
		t.Fatalf("PutNode later: %v", err)
	}
	if err := ms.CreateHighFrequencyIndex(1, time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex: %v", err)
	}

	nodes, err := ms.NodesByLabel(1, QueryOpts{ValidAt: 7_200_000})
	if err != nil {
		t.Fatalf("NodesByLabel ValidAt through HFI: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("NodesByLabel ValidAt through HFI returned %d nodes, want old open-ended and later", len(nodes))
	}
}

func TestMemoryStoreHighFrequencyIndexRejectsTemporalIndexSameLabel(t *testing.T) {
	t.Parallel()
	ms := New()

	if err := ms.CreateHighFrequencyIndex(1, time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex: %v", err)
	}
	if err := ms.CreateTemporalIndex(1); !errors.Is(err, ErrTemporalIndexExists) {
		t.Fatalf("CreateTemporalIndex after HFI = %v, want ErrTemporalIndexExists", err)
	}
}

func containsHFNodeID(ids []types.NodeID, want types.NodeID) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}
