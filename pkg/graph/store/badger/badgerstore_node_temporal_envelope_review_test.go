package badger

import (
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Review of backlog 56 (commit 217769f1).

// seedTwoLabelNodeWithHistory writes id with labels {8,7}: history row
// [100,200), current row [5000,6000) at version 1.
func seedTwoLabelNodeWithHistory(t *testing.T, bs *Store, id types.NodeID) *types.Node {
	t.Helper()
	v0 := envNode(id, 0, 100, 200, 8, 7)
	if err := bs.PutNode(v0); err != nil {
		t.Fatal(err)
	}
	v1 := envNode(id, 1, 5000, 6000, 8, 7)
	if err := bs.ReplaceNodeWithHistory(v1, 0, v0); err != nil {
		t.Fatal(err)
	}
	return v1
}

// A member that leaves the label (remove-label or delete) between the
// backfill and the history fold was covered by its current row; the fold must
// still read its history.
func TestNodeTemporalEnvelope_MemberLeavesBeforeFold(t *testing.T) {
	for _, tc := range []struct {
		name  string
		leave func(t *testing.T, bs *Store, id types.NodeID, cur *types.Node)
	}{
		{"remove-label", func(t *testing.T, bs *Store, id types.NodeID, cur *types.Node) {
			if err := bs.RemoveNodeLabelTokenWithHistory(id, 7, envNode(id, 2, 7000, 8000, 8), 1, cur); err != nil {
				t.Error(err)
			}
		}},
		{"delete", func(t *testing.T, bs *Store, id types.NodeID, _ *types.Node) {
			if err := bs.DeleteNode(id); err != nil {
				t.Error(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bs := newFlushParkStore(t, nil)
			nid := types.NodeID(snowflake.ID(8040))
			cur := seedTwoLabelNodeWithHistory(t, bs, nid)
			temporalCreateBeforeFoldHook = func() { tc.leave(t, bs, nid, cur) }
			defer func() { temporalCreateBeforeFoldHook = nil }()
			if err := bs.CreateTemporalIndex(7); err != nil {
				t.Fatal(err)
			}
			nodeMustKeep(t, bs, nid, 150, 5500)
		})
	}
}

// A deleted node re-created during the build (after the member snapshot)
// gets its history folded at the publish.
func TestNodeTemporalEnvelope_RecreateDuringBuild(t *testing.T) {
	bs := newFlushParkStore(t, nil)
	nid := seedDeletedNode(t, bs)
	temporalCreateBeforeFoldHook = func() {
		if err := bs.PutNode(envNode(nid, 0, 9000, 9100, 7)); err != nil {
			t.Error(err)
		}
	}
	defer func() { temporalCreateBeforeFoldHook = nil }()
	if err := bs.CreateTemporalIndex(7); err != nil {
		t.Fatal(err)
	}
	nodeMustKeep(t, bs, nid, 150, 5500, 9050)
}

// The plain add-label door (rollback path, no history row) folds history too.
func TestNodeTemporalEnvelope_PlainAddLabelOnLiveNode(t *testing.T) {
	t.Parallel()
	bs := newFlushParkStore(t, nil)
	nid := types.NodeID(snowflake.ID(8041))
	v0 := envNode(nid, 0, 100, 200, 8, 7)
	if err := bs.PutNode(v0); err != nil {
		t.Fatal(err)
	}
	v1 := envNode(nid, 1, 5000, 6000, 8)
	if err := bs.RemoveNodeLabelTokenWithHistory(nid, 7, v1, 0, v0); err != nil {
		t.Fatal(err)
	}
	if err := bs.CreateTemporalIndex(7); err != nil {
		t.Fatal(err)
	}
	if err := bs.AddNodeLabelToken(nid, 7, envNode(nid, 1, 5000, 6000, 8, 7)); err != nil {
		t.Fatal(err)
	}
	nodeMustKeep(t, bs, nid, 150, 5500)
}

// Under TemporalIndexOnDisk, a create that dies before it publishes leaves
// 0x0B rows with no definition; a later create of the same label plus a crash
// reopen read both rows, and a temporal ByLabel read returned the node twice.
func TestNodeTemporalEnvelope_OnDiskOrphanRowsAfterFailedCreate(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{Dir: dir, TemporalIndexOnDisk: true, FlushInterval: time.Hour}
	nid := types.NodeID(snowflake.ID(8042))
	bs, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNode(envNode(nid, 0, 5000, 6000, 7)); err != nil {
		t.Fatal(err)
	}
	temporalCreateBeforeFoldHook = func() {
		if err := bs.Flush(); err != nil {
			t.Error(err)
		}
		panic("crash before publish")
	}
	func() {
		defer func() { _ = recover() }()
		_ = bs.CreateTemporalIndex(7)
	}()
	temporalCreateBeforeFoldHook = nil
	bs.openImageSuppressed.Store(true) // a crash writes no open image
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}

	if bs, err = New(cfg); err != nil {
		t.Fatal(err)
	}
	if err := bs.ReplaceNode(envNode(nid, 0, 5500, 0, 7)); err != nil {
		t.Fatal(err)
	}
	if err := bs.CreateTemporalIndex(7); err != nil {
		t.Fatal(err)
	}
	bs.openImageSuppressed.Store(true)
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}

	if bs, err = New(cfg); err != nil {
		t.Fatal(err)
	}
	defer bs.Close()
	got, err := bs.NodesByLabel(7, QueryOpts{ValidAt: 5800})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		ids := make([]types.NodeID, len(got))
		for i, n := range got {
			ids[i] = n.ID()
		}
		t.Errorf("NodesByLabel(7, ValidAt 5800) = %v, want [%d] once", ids, nid)
	}
}
