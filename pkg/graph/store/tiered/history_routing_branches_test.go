package tiered

import (
	"errors"
	"reflect"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// History-aware tiered routing has four shapes:
//   (A) live on refShard, !isArchive — merge ref + (optional) archive
//   (B) live on event shard, !isArchive — local history only, no fanout
//   (C) isArchive — merge archive + reference
//   (D) not live anywhere, history-only — fanout across all history shards
//
// The pre-existing history_cursor_test.go covers shape A (with archive).
// This file adds B, C, D plus the "A without archive" sub-case so every
// branch in GetNodeVersion / GetNodeHistory / NodeHistoryVersionsFrom /
// TruncateNodeHistory (and rel parallels) is exercised at least once.

// --- Helpers ---

// branchTestEnv bundles the per-test store and the two reusable generators.
// One generator each for nodes/rels: calling tieredNodeGen multiple times in
// the same microsecond can hand out identical snowflake IDs because each
// fresh generator starts at step=0.
type branchTestEnv struct {
	ts      *Store
	nodeGen *snowflake.Node
	relGen  *snowflake.Node
	caseTok uint16
	signTok uint16
}

func newBranchTestEnv(t *testing.T) *branchTestEnv {
	t.Helper()
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, err := reg.GetOrCreate("Case")
	if err != nil {
		t.Fatalf("GetOrCreate Case: %v", err)
	}
	_, _ = reg.GetOrCreate("User")
	signalTok, err := reg.GetOrCreate("Signal")
	if err != nil {
		t.Fatalf("GetOrCreate Signal: %v", err)
	}
	return &branchTestEnv{
		ts:      ts,
		nodeGen: tieredNodeGen(t),
		relGen:  tieredRelGen(t),
		caseTok: caseTok,
		signTok: signalTok,
	}
}

func (e *branchTestEnv) newEventNode(t *testing.T) *types.Node {
	t.Helper()
	n := types.NewNode(types.NodeID(e.nodeGen.Generate()), e.signTok, nil)
	if err := e.ts.PutNode(n); err != nil {
		t.Fatalf("PutNode signal: %v", err)
	}
	return n
}

func (e *branchTestEnv) newRefNode(t *testing.T) *types.Node {
	t.Helper()
	n := types.NewNode(types.NodeID(e.nodeGen.Generate()), e.caseTok, nil)
	if err := e.ts.PutNode(n); err != nil {
		t.Fatalf("PutNode case: %v", err)
	}
	return n
}

// putRelBetween creates and stores a relationship between start and end. The
// shard ownership of the rel follows the routing rules baked into PutRelationship.
func (e *branchTestEnv) putRelBetween(t *testing.T, start, end *types.Node) *types.Relationship {
	t.Helper()
	r := types.NewRelationship(types.RelID(e.relGen.Generate()), 1, start.ID(), end.ID())
	if err := e.ts.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}
	return r
}

// writeHistoryV0V1 writes two history rows (v0, v1) for the node via direct
// PutNodeVersion calls. Doesn't touch the live row.
func (e *branchTestEnv) writeNodeHistoryV0V1(t *testing.T, n *types.Node) {
	t.Helper()
	if err := e.ts.PutNodeVersion(n.ID(), 0, n); err != nil {
		t.Fatalf("PutNodeVersion v0: %v", err)
	}
	v1 := n.DeepCopy()
	v1.SetVersion(1)
	if err := e.ts.PutNodeVersion(n.ID(), 1, v1); err != nil {
		t.Fatalf("PutNodeVersion v1: %v", err)
	}
}

func (e *branchTestEnv) writeRelHistoryV0V1(t *testing.T, r *types.Relationship) {
	t.Helper()
	if err := e.ts.PutRelVersion(r.ID(), 0, r); err != nil {
		t.Fatalf("PutRelVersion v0: %v", err)
	}
	v1 := r.DeepCopy()
	v1.SetVersion(1)
	if err := e.ts.PutRelVersion(r.ID(), 1, v1); err != nil {
		t.Fatalf("PutRelVersion v1: %v", err)
	}
}

// --- (B) Event-shard live: no fanout, local history only ---

func TestTiered_NodeHistoryVersionsFrom_EventShardLive_NoFanout(t *testing.T) {
	e := newBranchTestEnv(t)
	n := e.newEventNode(t)
	e.writeNodeHistoryV0V1(t, n)

	history, err := e.ts.NodeHistoryVersionsFrom(n.ID(), 0, 5)
	if err != nil {
		t.Fatalf("NodeHistoryVersionsFrom: %v", err)
	}
	if versions := tieredNodeHistoryVersions(history); !reflect.DeepEqual(versions, []uint32{0, 1}) {
		t.Fatalf("event-shard live versions = %v, want [0 1]", versions)
	}
}

func TestTiered_GetNodeVersion_EventShardLive_NotFound(t *testing.T) {
	e := newBranchTestEnv(t)
	n := e.newEventNode(t)
	if err := e.ts.PutNodeVersion(n.ID(), 0, n); err != nil {
		t.Fatalf("PutNodeVersion: %v", err)
	}
	if _, err := e.ts.GetNodeVersion(n.ID(), 0); err != nil {
		t.Fatalf("GetNodeVersion v0: %v", err)
	}
	if _, err := e.ts.GetNodeVersion(n.ID(), 99); !errors.Is(err, ErrVersionNotFound) {
		t.Fatalf("GetNodeVersion v99 = %v, want ErrVersionNotFound", err)
	}
}

func TestTiered_GetNodeHistory_EventShardLive_NoFanout(t *testing.T) {
	e := newBranchTestEnv(t)
	n := e.newEventNode(t)
	e.writeNodeHistoryV0V1(t, n)
	history, err := e.ts.GetNodeHistory(n.ID())
	if err != nil {
		t.Fatalf("GetNodeHistory: %v", err)
	}
	if versions := tieredNodeHistoryVersions(history); !reflect.DeepEqual(versions, []uint32{0, 1}) {
		t.Fatalf("event-shard versions = %v, want [0 1]", versions)
	}
}

func TestTiered_TruncateNodeHistory_EventShardLive_DelegatesToShard(t *testing.T) {
	e := newBranchTestEnv(t)
	n := e.newEventNode(t)
	e.writeNodeHistoryV0V1(t, n)
	if err := e.ts.TruncateNodeHistory(n.ID(), 1); err != nil {
		t.Fatalf("TruncateNodeHistory: %v", err)
	}
	remaining, err := e.ts.GetNodeHistory(n.ID())
	if err != nil {
		t.Fatalf("GetNodeHistory: %v", err)
	}
	if versions := tieredNodeHistoryVersions(remaining); !reflect.DeepEqual(versions, []uint32{1}) {
		t.Fatalf("after truncate keep=1 versions = %v, want [1]", versions)
	}
}

// --- (D) Deleted from current — history-only across all shards ---

func TestTiered_NodeHistoryVersionsFrom_DeletedEvent_FanoutBranch(t *testing.T) {
	e := newBranchTestEnv(t)
	n := e.newEventNode(t)
	e.writeNodeHistoryV0V1(t, n)
	if err := e.ts.DeleteNode(n.ID()); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}
	history, err := e.ts.NodeHistoryVersionsFrom(n.ID(), 0, 5)
	if err != nil {
		t.Fatalf("NodeHistoryVersionsFrom after delete: %v", err)
	}
	if versions := tieredNodeHistoryVersions(history); !reflect.DeepEqual(versions, []uint32{0, 1}) {
		t.Fatalf("history-only versions after delete = %v, want [0 1]", versions)
	}
}

func TestTiered_GetNodeHistory_DeletedEvent_FanoutBranch(t *testing.T) {
	e := newBranchTestEnv(t)
	n := e.newEventNode(t)
	if err := e.ts.PutNodeVersion(n.ID(), 0, n); err != nil {
		t.Fatalf("PutNodeVersion: %v", err)
	}
	if err := e.ts.DeleteNode(n.ID()); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}
	history, err := e.ts.GetNodeHistory(n.ID())
	if err != nil {
		t.Fatalf("GetNodeHistory after delete: %v", err)
	}
	if len(history) != 1 || history[0].Version() != 0 {
		t.Fatalf("history-only after delete = %v, want exactly v0", tieredNodeHistoryVersions(history))
	}
}

func TestTiered_GetNodeVersion_DeletedEvent_HistoryFanout(t *testing.T) {
	e := newBranchTestEnv(t)
	n := e.newEventNode(t)
	if err := e.ts.PutNodeVersion(n.ID(), 0, n); err != nil {
		t.Fatalf("PutNodeVersion: %v", err)
	}
	if err := e.ts.DeleteNode(n.ID()); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}
	got, err := e.ts.GetNodeVersion(n.ID(), 0)
	if err != nil {
		t.Fatalf("GetNodeVersion v0 after delete: %v", err)
	}
	if got == nil || got.ID() != n.ID() || got.Version() != 0 {
		t.Fatalf("got = %+v, want v0 of %d", got, n.ID())
	}
	if _, err := e.ts.GetNodeVersion(n.ID(), 99); !errors.Is(err, ErrVersionNotFound) {
		t.Fatalf("missing v99 after delete = %v, want ErrVersionNotFound", err)
	}
}

func TestTiered_TruncateNodeHistory_DeletedEvent_FanoutBranch(t *testing.T) {
	e := newBranchTestEnv(t)
	n := e.newEventNode(t)
	e.writeNodeHistoryV0V1(t, n)
	if err := e.ts.DeleteNode(n.ID()); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}
	if err := e.ts.TruncateNodeHistory(n.ID(), 1); err != nil {
		t.Fatalf("TruncateNodeHistory keep=1 after delete: %v", err)
	}
	history, err := e.ts.GetNodeHistory(n.ID())
	if err != nil {
		t.Fatalf("GetNodeHistory: %v", err)
	}
	if versions := tieredNodeHistoryVersions(history); !reflect.DeepEqual(versions, []uint32{1}) {
		t.Fatalf("after delete+truncate keep=1 versions = %v, want [1]", versions)
	}
}

// --- (A without archive) — refShard live, no archive shard exists yet ---

func TestTiered_NodeHistoryVersionsFrom_RefLive_NoArchive(t *testing.T) {
	e := newBranchTestEnv(t)
	n := e.newRefNode(t)
	e.writeNodeHistoryV0V1(t, n)
	if e.ts.HasArchiveShardForTest() {
		t.Fatalf("precondition: archive shard should not exist yet")
	}
	history, err := e.ts.NodeHistoryVersionsFrom(n.ID(), 0, 5)
	if err != nil {
		t.Fatalf("NodeHistoryVersionsFrom no-archive: %v", err)
	}
	if versions := tieredNodeHistoryVersions(history); !reflect.DeepEqual(versions, []uint32{0, 1}) {
		t.Fatalf("no-archive history = %v, want [0 1]", versions)
	}
}

func TestTiered_GetNodeHistory_RefLive_NoArchive(t *testing.T) {
	e := newBranchTestEnv(t)
	n := e.newRefNode(t)
	if err := e.ts.PutNodeVersion(n.ID(), 0, n); err != nil {
		t.Fatalf("PutNodeVersion: %v", err)
	}
	history, err := e.ts.GetNodeHistory(n.ID())
	if err != nil {
		t.Fatalf("GetNodeHistory: %v", err)
	}
	if len(history) != 1 || history[0].Version() != 0 {
		t.Fatalf("no-archive history = %v, want exactly v0", tieredNodeHistoryVersions(history))
	}
}

func TestTiered_TruncateNodeHistory_RefLive_NoArchive(t *testing.T) {
	e := newBranchTestEnv(t)
	n := e.newRefNode(t)
	e.writeNodeHistoryV0V1(t, n)
	if err := e.ts.TruncateNodeHistory(n.ID(), 1); err != nil {
		t.Fatalf("TruncateNodeHistory: %v", err)
	}
	history, err := e.ts.GetNodeHistory(n.ID())
	if err != nil {
		t.Fatalf("GetNodeHistory: %v", err)
	}
	if versions := tieredNodeHistoryVersions(history); !reflect.DeepEqual(versions, []uint32{1}) {
		t.Fatalf("after truncate keep=1 versions = %v, want [1]", versions)
	}
}

// --- (C) Archive-shard routed — live row on refArchive ---

func TestTiered_NodeHistoryVersionsFrom_Archive_MergesRefHistory(t *testing.T) {
	e := newBranchTestEnv(t)
	n := e.newRefNode(t)
	if err := e.ts.PutNodeVersion(n.ID(), 0, n); err != nil {
		t.Fatalf("PutNodeVersion v0: %v", err)
	}
	if err := e.ts.ArchiveNode(n.ID()); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}
	v1 := n.DeepCopy()
	v1.SetVersion(1)
	if err := e.ts.PutNodeVersion(n.ID(), 1, v1); err != nil {
		t.Fatalf("PutNodeVersion v1 after archive: %v", err)
	}

	history, err := e.ts.NodeHistoryVersionsFrom(n.ID(), 0, 5)
	if err != nil {
		t.Fatalf("NodeHistoryVersionsFrom after archive: %v", err)
	}
	if versions := tieredNodeHistoryVersions(history); !reflect.DeepEqual(versions, []uint32{0, 1}) {
		t.Fatalf("archived versions = %v, want [0 1] (ref v0 + archive v1)", versions)
	}
}

func TestTiered_GetNodeHistory_Archive_MergesRefHistory(t *testing.T) {
	e := newBranchTestEnv(t)
	n := e.newRefNode(t)
	if err := e.ts.PutNodeVersion(n.ID(), 0, n); err != nil {
		t.Fatalf("PutNodeVersion v0: %v", err)
	}
	if err := e.ts.ArchiveNode(n.ID()); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}
	v1 := n.DeepCopy()
	v1.SetVersion(1)
	if err := e.ts.PutNodeVersion(n.ID(), 1, v1); err != nil {
		t.Fatalf("PutNodeVersion v1: %v", err)
	}
	history, err := e.ts.GetNodeHistory(n.ID())
	if err != nil {
		t.Fatalf("GetNodeHistory: %v", err)
	}
	if versions := tieredNodeHistoryVersions(history); !reflect.DeepEqual(versions, []uint32{0, 1}) {
		t.Fatalf("archived history = %v, want [0 1]", versions)
	}
}

func TestTiered_GetNodeVersion_Archive_FindsRefHistory(t *testing.T) {
	e := newBranchTestEnv(t)
	n := e.newRefNode(t)
	if err := e.ts.PutNodeVersion(n.ID(), 0, n); err != nil {
		t.Fatalf("PutNodeVersion v0: %v", err)
	}
	if err := e.ts.ArchiveNode(n.ID()); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}
	// v0 lives on refShard but the live row is now on refArchive.
	// GetNodeVersion must fall through to the history-shard scan to find v0.
	got, err := e.ts.GetNodeVersion(n.ID(), 0)
	if err != nil {
		t.Fatalf("GetNodeVersion v0 after archive: %v", err)
	}
	if got == nil || got.ID() != n.ID() || got.Version() != 0 {
		t.Fatalf("got = %+v, want v0 of %d", got, n.ID())
	}
}

func TestTiered_TruncateNodeHistory_Archive_MergesRefHistory(t *testing.T) {
	e := newBranchTestEnv(t)
	n := e.newRefNode(t)
	if err := e.ts.PutNodeVersion(n.ID(), 0, n); err != nil {
		t.Fatalf("PutNodeVersion v0: %v", err)
	}
	if err := e.ts.ArchiveNode(n.ID()); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}
	v1 := n.DeepCopy()
	v1.SetVersion(1)
	if err := e.ts.PutNodeVersion(n.ID(), 1, v1); err != nil {
		t.Fatalf("PutNodeVersion v1: %v", err)
	}
	if err := e.ts.TruncateNodeHistory(n.ID(), 1); err != nil {
		t.Fatalf("TruncateNodeHistory keep=1 archived: %v", err)
	}
	history, err := e.ts.GetNodeHistory(n.ID())
	if err != nil {
		t.Fatalf("GetNodeHistory: %v", err)
	}
	if versions := tieredNodeHistoryVersions(history); !reflect.DeepEqual(versions, []uint32{1}) {
		t.Fatalf("after archive+truncate keep=1 versions = %v, want [1]", versions)
	}
}

// --- Relationship parity for all four shapes ---

func TestTiered_RelHistoryVersionsFrom_EventShardLive_NoFanout(t *testing.T) {
	e := newBranchTestEnv(t)
	start := e.newEventNode(t)
	end := e.newEventNode(t)
	r := e.putRelBetween(t, start, end)
	e.writeRelHistoryV0V1(t, r)

	history, err := e.ts.RelHistoryVersionsFrom(r.ID(), 0, 5)
	if err != nil {
		t.Fatalf("RelHistoryVersionsFrom: %v", err)
	}
	if versions := tieredRelHistoryVersions(history); !reflect.DeepEqual(versions, []uint32{0, 1}) {
		t.Fatalf("event-shard rel versions = %v, want [0 1]", versions)
	}
}

func TestTiered_GetRelVersion_EventShardLive_NotFound(t *testing.T) {
	e := newBranchTestEnv(t)
	start := e.newEventNode(t)
	end := e.newEventNode(t)
	r := e.putRelBetween(t, start, end)
	if err := e.ts.PutRelVersion(r.ID(), 0, r); err != nil {
		t.Fatalf("PutRelVersion: %v", err)
	}
	if _, err := e.ts.GetRelVersion(r.ID(), 0); err != nil {
		t.Fatalf("GetRelVersion v0: %v", err)
	}
	if _, err := e.ts.GetRelVersion(r.ID(), 99); !errors.Is(err, ErrVersionNotFound) {
		t.Fatalf("GetRelVersion v99 = %v, want ErrVersionNotFound", err)
	}
}

func TestTiered_GetRelHistory_EventShardLive_NoFanout(t *testing.T) {
	e := newBranchTestEnv(t)
	start := e.newEventNode(t)
	end := e.newEventNode(t)
	r := e.putRelBetween(t, start, end)
	e.writeRelHistoryV0V1(t, r)
	history, err := e.ts.GetRelHistory(r.ID())
	if err != nil {
		t.Fatalf("GetRelHistory: %v", err)
	}
	if versions := tieredRelHistoryVersions(history); !reflect.DeepEqual(versions, []uint32{0, 1}) {
		t.Fatalf("event-shard versions = %v, want [0 1]", versions)
	}
}

func TestTiered_TruncateRelHistory_EventShardLive_DelegatesToShard(t *testing.T) {
	e := newBranchTestEnv(t)
	start := e.newEventNode(t)
	end := e.newEventNode(t)
	r := e.putRelBetween(t, start, end)
	e.writeRelHistoryV0V1(t, r)
	if err := e.ts.TruncateRelHistory(r.ID(), 1); err != nil {
		t.Fatalf("TruncateRelHistory: %v", err)
	}
	history, err := e.ts.GetRelHistory(r.ID())
	if err != nil {
		t.Fatalf("GetRelHistory: %v", err)
	}
	if versions := tieredRelHistoryVersions(history); !reflect.DeepEqual(versions, []uint32{1}) {
		t.Fatalf("after truncate keep=1 versions = %v, want [1]", versions)
	}
}

func TestTiered_RelHistoryVersionsFrom_DeletedEvent_FanoutBranch(t *testing.T) {
	e := newBranchTestEnv(t)
	start := e.newEventNode(t)
	end := e.newEventNode(t)
	r := e.putRelBetween(t, start, end)
	e.writeRelHistoryV0V1(t, r)
	if err := e.ts.DeleteRelationship(r.ID()); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}
	history, err := e.ts.RelHistoryVersionsFrom(r.ID(), 0, 5)
	if err != nil {
		t.Fatalf("RelHistoryVersionsFrom after delete: %v", err)
	}
	if versions := tieredRelHistoryVersions(history); !reflect.DeepEqual(versions, []uint32{0, 1}) {
		t.Fatalf("history-only versions after delete = %v, want [0 1]", versions)
	}
}

func TestTiered_GetRelHistory_DeletedEvent_FanoutBranch(t *testing.T) {
	e := newBranchTestEnv(t)
	start := e.newEventNode(t)
	end := e.newEventNode(t)
	r := e.putRelBetween(t, start, end)
	if err := e.ts.PutRelVersion(r.ID(), 0, r); err != nil {
		t.Fatalf("PutRelVersion: %v", err)
	}
	if err := e.ts.DeleteRelationship(r.ID()); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}
	history, err := e.ts.GetRelHistory(r.ID())
	if err != nil {
		t.Fatalf("GetRelHistory after delete: %v", err)
	}
	if len(history) != 1 || history[0].Version() != 0 {
		t.Fatalf("history-only after delete = %v, want exactly v0", tieredRelHistoryVersions(history))
	}
}

func TestTiered_GetRelVersion_DeletedEvent_HistoryFanout(t *testing.T) {
	e := newBranchTestEnv(t)
	start := e.newEventNode(t)
	end := e.newEventNode(t)
	r := e.putRelBetween(t, start, end)
	if err := e.ts.PutRelVersion(r.ID(), 0, r); err != nil {
		t.Fatalf("PutRelVersion: %v", err)
	}
	if err := e.ts.DeleteRelationship(r.ID()); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}
	got, err := e.ts.GetRelVersion(r.ID(), 0)
	if err != nil {
		t.Fatalf("GetRelVersion v0 after delete: %v", err)
	}
	if got == nil || got.ID() != r.ID() || got.Version() != 0 {
		t.Fatalf("got = %+v, want v0 of %d", got, r.ID())
	}
}

func TestTiered_TruncateRelHistory_DeletedEvent_FanoutBranch(t *testing.T) {
	e := newBranchTestEnv(t)
	start := e.newEventNode(t)
	end := e.newEventNode(t)
	r := e.putRelBetween(t, start, end)
	e.writeRelHistoryV0V1(t, r)
	if err := e.ts.DeleteRelationship(r.ID()); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}
	if err := e.ts.TruncateRelHistory(r.ID(), 1); err != nil {
		t.Fatalf("TruncateRelHistory keep=1 after delete: %v", err)
	}
	history, err := e.ts.GetRelHistory(r.ID())
	if err != nil {
		t.Fatalf("GetRelHistory: %v", err)
	}
	if versions := tieredRelHistoryVersions(history); !reflect.DeepEqual(versions, []uint32{1}) {
		t.Fatalf("after delete+truncate keep=1 versions = %v, want [1]", versions)
	}
}

func TestTiered_RelHistoryVersionsFrom_RefLive_NoArchive(t *testing.T) {
	e := newBranchTestEnv(t)
	start := e.newRefNode(t)
	end := e.newRefNode(t)
	r := e.putRelBetween(t, start, end)
	e.writeRelHistoryV0V1(t, r)
	if e.ts.HasArchiveShardForTest() {
		t.Fatalf("precondition: archive shard should not exist yet")
	}
	history, err := e.ts.RelHistoryVersionsFrom(r.ID(), 0, 5)
	if err != nil {
		t.Fatalf("RelHistoryVersionsFrom no-archive: %v", err)
	}
	if versions := tieredRelHistoryVersions(history); !reflect.DeepEqual(versions, []uint32{0, 1}) {
		t.Fatalf("no-archive history = %v, want [0 1]", versions)
	}
}

func TestTiered_GetRelHistory_RefLive_NoArchive(t *testing.T) {
	e := newBranchTestEnv(t)
	start := e.newRefNode(t)
	end := e.newRefNode(t)
	r := e.putRelBetween(t, start, end)
	if err := e.ts.PutRelVersion(r.ID(), 0, r); err != nil {
		t.Fatalf("PutRelVersion: %v", err)
	}
	history, err := e.ts.GetRelHistory(r.ID())
	if err != nil {
		t.Fatalf("GetRelHistory no-archive: %v", err)
	}
	if len(history) != 1 || history[0].Version() != 0 {
		t.Fatalf("no-archive history = %v, want exactly v0", tieredRelHistoryVersions(history))
	}
}

func TestTiered_TruncateRelHistory_RefLive_NoArchive(t *testing.T) {
	e := newBranchTestEnv(t)
	start := e.newRefNode(t)
	end := e.newRefNode(t)
	r := e.putRelBetween(t, start, end)
	e.writeRelHistoryV0V1(t, r)
	if err := e.ts.TruncateRelHistory(r.ID(), 1); err != nil {
		t.Fatalf("TruncateRelHistory: %v", err)
	}
	history, err := e.ts.GetRelHistory(r.ID())
	if err != nil {
		t.Fatalf("GetRelHistory: %v", err)
	}
	if versions := tieredRelHistoryVersions(history); !reflect.DeepEqual(versions, []uint32{1}) {
		t.Fatalf("after truncate keep=1 versions = %v, want [1]", versions)
	}
}
