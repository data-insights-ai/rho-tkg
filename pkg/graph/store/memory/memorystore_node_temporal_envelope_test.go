package memory

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 56, memory mirror of the badger envelope tests: the node temporal
// envelope prunes, so once it covers an ID it must cover every row of that ID
// carrying the label. Each test prunes at an instant only an old row covers;
// the node must be kept.

func envNode(id types.NodeID, version uint32, from, to types.Instant, primary uint16, extra ...uint16) *types.Node {
	n := types.NewNode(id, primary, extra)
	n.SetTemporal(&types.TemporalMetadata{ValidFrom: from, ValidTo: to})
	n.SetVersion(version)
	return n
}

func envMustKeep(t *testing.T, ms *Store, id types.NodeID, at ...types.Instant) {
	t.Helper()
	for _, v := range at {
		kept, ok := ms.PruneTemporalCandidates(7, []types.NodeID{id}, QueryOpts{ValidAt: v})
		if len(kept) != 1 {
			t.Errorf("prune at %d kept %v (ok=%v), want [%d]: it holds a row valid there", v, kept, ok, id)
		}
	}
}

// seedNodeWithHistory writes id with label 7: history row [100,200), current
// row [5000,6000) at version 1.
func seedNodeWithHistory(t *testing.T, ms *Store, id types.NodeID) *types.Node {
	t.Helper()
	v0 := envNode(id, 0, 100, 200, 7)
	if err := ms.PutNode(v0); err != nil {
		t.Fatal(err)
	}
	v1 := envNode(id, 1, 5000, 6000, 7)
	if err := ms.ReplaceNodeWithHistory(v1, 0, v0); err != nil {
		t.Fatal(err)
	}
	return v1
}

// (a) PutNodeVersion on a covered node.
func TestMemoryNodeTemporalEnvelope_PutNodeVersionCovered(t *testing.T) {
	t.Parallel()
	ms := New()
	nid := types.NodeID(snowflake.ID(8030))
	if err := ms.PutNode(envNode(nid, 5, 5000, 6000, 7)); err != nil {
		t.Fatal(err)
	}
	if err := ms.CreateTemporalIndex(7); err != nil {
		t.Fatal(err)
	}
	if err := ms.PutNodeVersion(nid, 1, envNode(nid, 1, 100, 200, 7)); err != nil {
		t.Fatal(err)
	}
	envMustKeep(t, ms, nid, 150, 5500)
}

// (b) The create's window: a node updated while the create scans (Phase 2)
// keeps its earlier rows in the published envelope.
func TestMemoryNodeTemporalEnvelope_UpdateDuringCreate(t *testing.T) {
	ms := New()
	nid := types.NodeID(snowflake.ID(8031))
	v1 := seedNodeWithHistory(t, ms, nid)
	fired := false
	phase2ScanHook = func() {
		if fired {
			return
		}
		fired = true
		if err := ms.ReplaceNodeWithHistory(envNode(nid, 2, 7000, 8000, 7), 1, v1); err != nil {
			t.Error(err)
		}
	}
	t.Cleanup(func() { phase2ScanHook = nil })
	if err := ms.CreateTemporalIndex(7); err != nil {
		t.Fatal(err)
	}
	if !fired {
		t.Fatal("hook did not fire")
	}
	envMustKeep(t, ms, nid, 150, 5500, 7500)
}

// (c) A live node that carried label 7 in [100,200), lost it, and gets it back
// after the index exists.
func TestMemoryNodeTemporalEnvelope_LabelReAddOnLiveNode(t *testing.T) {
	t.Parallel()
	ms := New()
	nid := types.NodeID(snowflake.ID(8032))
	v0 := envNode(nid, 0, 100, 200, 8, 7)
	if err := ms.PutNode(v0); err != nil {
		t.Fatal(err)
	}
	v1 := envNode(nid, 1, 5000, 6000, 8)
	if err := ms.RemoveNodeLabelTokenWithHistory(nid, 7, v1, 0, v0); err != nil {
		t.Fatal(err)
	}
	if err := ms.CreateTemporalIndex(7); err != nil {
		t.Fatal(err)
	}
	if err := ms.AddNodeLabelTokenWithHistory(nid, 7, envNode(nid, 2, 7000, 8000, 8, 7), 1, v1); err != nil {
		t.Fatal(err)
	}
	envMustKeep(t, ms, nid, 150, 7500)
}

// (d) Re-create of a deleted node whose history is kept.
func TestMemoryNodeTemporalEnvelope_RecreateDeletedNode(t *testing.T) {
	t.Parallel()
	ms := New()
	nid := types.NodeID(snowflake.ID(8033))
	seedNodeWithHistory(t, ms, nid)
	if err := ms.DeleteNode(nid); err != nil {
		t.Fatal(err)
	}
	if err := ms.CreateTemporalIndex(7); err != nil {
		t.Fatal(err)
	}
	if err := ms.PutNode(envNode(nid, 0, 9000, 9100, 7)); err != nil {
		t.Fatal(err)
	}
	envMustKeep(t, ms, nid, 150, 5500, 9050)
}
