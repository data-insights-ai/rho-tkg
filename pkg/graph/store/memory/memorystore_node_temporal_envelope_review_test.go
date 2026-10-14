package memory

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Review of backlog 56 (commit 217769f1).

// A PutNodeVersion while the create scans, after the scan read the node: the
// Building index takes the row (the create refolds the written ID).
func TestMemoryNodeTemporalEnvelope_PutNodeVersionDuringCreate(t *testing.T) {
	ms := New()
	nid := types.NodeID(snowflake.ID(8034))
	if err := ms.PutNode(envNode(nid, 5, 5000, 6000, 7)); err != nil {
		t.Fatal(err)
	}
	fired := false
	phase2ScanHook = func() {
		if fired {
			return
		}
		fired = true
		if err := ms.PutNodeVersion(nid, 1, envNode(nid, 1, 300, 400, 7)); err != nil {
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
	envMustKeep(t, ms, nid, 350, 5500)
}

// The plain add-label door (rollback path, no history row) folds history too.
func TestMemoryNodeTemporalEnvelope_PlainAddLabelOnLiveNode(t *testing.T) {
	ms := New()
	nid := types.NodeID(snowflake.ID(8035))
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
	if err := ms.AddNodeLabelToken(nid, 7, envNode(nid, 1, 5000, 6000, 8, 7)); err != nil {
		t.Fatal(err)
	}
	envMustKeep(t, ms, nid, 150, 5500)
}
