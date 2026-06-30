package graph_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// A read-only replica that bootstraps from a snapshot and then tails the
// primary's change-log converges to a BYTE-EXACT copy: same integrity hash,
// version, labels, and full version history on every entity. The integrity hash
// is a SHA-256 over canonical content + prev-hash, so hash equality proves the
// replica reproduced the primary's rows verbatim (no re-stamped TxFrom / version
// / temporal metadata) — the Phase-1 non-negotiable.
func TestReplicaConvergence_ByteExactViaTail(t *testing.T) {
	ctx := context.Background()

	primary, err := graph.New(graph.Config{SnowflakeNodeID: 1, BadgerInMemory: true, ChangeLog: true, SyncWrites: true})
	if err != nil {
		t.Fatalf("primary New: %v", err)
	}
	defer primary.Close()

	// Initial data — registers label tokens A, B and rel-type KNOWS.
	a := mustAdd(t, primary, []string{"A"}, map[string]any{"n": "a"})
	b := mustAdd(t, primary, []string{"A", "B"}, map[string]any{"n": "b"})
	c := mustAdd(t, primary, []string{"A"}, map[string]any{"n": "c"})
	r1, err := primary.Rels().AddByID(ctx, "KNOWS", a.ID(), b.ID(), map[string]any{"w": int64(1)})
	if err != nil {
		t.Fatalf("seed rel: %v", err)
	}

	// Snapshot + handoff LSN.
	var snap bytes.Buffer
	if err := primary.IO().Export(&snap); err != nil {
		t.Fatalf("Export: %v", err)
	}
	lsn0, err := primary.Replication().LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	}

	// Bootstrap a read-only replica from the snapshot and record the handoff LSN.
	replica, err := graph.New(graph.Config{SnowflakeNodeID: 2, BadgerInMemory: true, ReadOnlyReplica: true})
	if err != nil {
		t.Fatalf("replica New: %v", err)
	}
	defer replica.Close()
	if err := replica.IO().Import(&snap, tkgio.ImportOptions{}); err != nil {
		t.Fatalf("replica Import: %v", err)
	}
	if err := replica.Replication().SetAppliedLSN(lsn0); err != nil {
		t.Fatalf("SetAppliedLSN: %v", err)
	}
	assertConverged(t, "after bootstrap", primary, replica)

	// Drive every record kind on the primary, using only already-known tokens
	// (A, B, KNOWS) so no post-snapshot token sync is needed.
	if _, err := primary.Nodes().Update(ctx, a.ID(), map[string]any{"n": "a2"}); err != nil {
		t.Fatalf("Update a (with-history): %v", err)
	}
	if _, err := primary.Nodes().UpdateInPlace(ctx, b.ID(), map[string]any{"n": "b2"}); err != nil {
		t.Fatalf("UpdateInPlace b: %v", err)
	}
	if err := primary.Nodes().AddLabel(ctx, a.ID(), "B"); err != nil {
		t.Fatalf("AddLabel a B: %v", err)
	}
	if err := primary.Nodes().RemoveLabel(ctx, b.ID(), "B"); err != nil {
		t.Fatalf("RemoveLabel b B: %v", err)
	}
	r2, err := primary.Rels().AddByID(ctx, "KNOWS", b.ID(), c.ID(), nil)
	if err != nil {
		t.Fatalf("Add rel b->c: %v", err)
	}
	if _, err := primary.Rels().Update(ctx, r1.ID(), map[string]any{"w": int64(2)}); err != nil {
		t.Fatalf("Update rel r1 (with-history): %v", err)
	}
	// Delete a connected node — cascades the b->c rel, exercising the
	// with-history cascade delete (node tombstone + rel tombstones).
	if err := primary.Nodes().Delete(ctx, c.ID()); err != nil {
		t.Fatalf("Delete c (cascade): %v", err)
	}

	// Tail: pull every record past the handoff LSN and apply it.
	from, err := replica.Replication().AppliedLSN()
	if err != nil {
		t.Fatalf("AppliedLSN: %v", err)
	}
	if from != lsn0 {
		t.Fatalf("resume LSN = %d, want snapshot LSN %d", from, lsn0)
	}
	var recs []store.ChangeRecord
	if err := primary.Replication().ForEachChange(from, func(rec store.ChangeRecord) bool {
		recs = append(recs, rec)
		return true
	}); err != nil {
		t.Fatalf("ForEachChange: %v", err)
	}
	if len(recs) == 0 {
		t.Fatal("no change records past the snapshot LSN — nothing to tail")
	}
	applied, err := replica.Replication().ApplyChanges(recs)
	if err != nil {
		t.Fatalf("ApplyChanges: %v", err)
	}
	if want := recs[len(recs)-1].LSN; applied != want {
		t.Fatalf("applied LSN = %d, want %d", applied, want)
	}

	assertConverged(t, "after tail", primary, replica)
	_ = r2

	// Deleted-entity history must replicate too: c is gone from live state on
	// both, but its tombstone version chain must match byte-for-byte.
	pHist := mustNodeHistory(t, primary, c.ID())
	rHist := mustNodeHistory(t, replica, c.ID())
	if len(pHist) != len(rHist) {
		t.Fatalf("deleted node c history depth: primary %d, replica %d", len(pHist), len(rHist))
	}
	for i := range pHist {
		if ph, rh := nodeHash(pHist[i]), nodeHash(rHist[i]); ph != rh {
			t.Fatalf("deleted node c history[%d] hash: primary %q, replica %q", i, ph, rh)
		}
	}

	// The watermark advanced to the last applied LSN and persists for restart.
	if got, _ := replica.Replication().AppliedLSN(); got != applied {
		t.Fatalf("persisted AppliedLSN = %d, want %d", got, applied)
	}
}

// Re-applying the same records must be a no-op (idempotent at-least-once
// redelivery after a crash between a door commit and the watermark advance).
func TestReplicaConvergence_IdempotentReapply(t *testing.T) {
	ctx := context.Background()
	primary, err := graph.New(graph.Config{SnowflakeNodeID: 1, BadgerInMemory: true, ChangeLog: true, SyncWrites: true})
	if err != nil {
		t.Fatalf("primary New: %v", err)
	}
	defer primary.Close()
	a := mustAdd(t, primary, []string{"A"}, map[string]any{"n": "a"})
	if _, err := primary.Nodes().Update(ctx, a.ID(), map[string]any{"n": "a2"}); err != nil {
		t.Fatalf("Update: %v", err)
	}

	replica, err := graph.New(graph.Config{SnowflakeNodeID: 2, BadgerInMemory: true, ReadOnlyReplica: true})
	if err != nil {
		t.Fatalf("replica New: %v", err)
	}
	defer replica.Close()
	var snap bytes.Buffer
	if err := primary.IO().Export(&snap); err != nil {
		t.Fatalf("Export: %v", err)
	}
	if err := replica.IO().Import(&snap, tkgio.ImportOptions{}); err != nil {
		t.Fatalf("Import: %v", err)
	}

	var recs []store.ChangeRecord
	if err := primary.Replication().ForEachChange(0, func(rec store.ChangeRecord) bool {
		recs = append(recs, rec)
		return true
	}); err != nil {
		t.Fatalf("ForEachChange: %v", err)
	}

	// Apply the full feed twice; the second pass must not double-write history.
	if _, err := replica.Replication().ApplyChanges(recs); err != nil {
		t.Fatalf("ApplyChanges pass 1: %v", err)
	}
	histLen1 := len(mustNodeHistory(t, replica, a.ID()))
	if _, err := replica.Replication().ApplyChanges(recs); err != nil {
		t.Fatalf("ApplyChanges pass 2 (idempotent): %v", err)
	}
	histLen2 := len(mustNodeHistory(t, replica, a.ID()))
	if histLen1 != histLen2 {
		t.Fatalf("re-apply changed history depth: %d -> %d (not idempotent)", histLen1, histLen2)
	}
	assertConverged(t, "after double-apply", primary, replica)
}

// ApplyChanges enforces strictly-ascending LSN order: a record below the running
// maximum must fail the batch (not be silently swallowed by the watermark skip,
// which would leave a permanent gap in coverage). The successful prefix before
// the out-of-order record is still flushed and watermarked.
func TestReplicaApplyChanges_RejectsMisorderedBatch(t *testing.T) {
	primary, err := graph.New(graph.Config{SnowflakeNodeID: 1, BadgerInMemory: true, ChangeLog: true, SyncWrites: true})
	if err != nil {
		t.Fatalf("primary New: %v", err)
	}
	defer primary.Close()
	// Seed a node so label "A"'s token is in the bootstrap snapshot registry; the
	// post-bootstrap records then reuse the known token (no refetch needed).
	mustAdd(t, primary, []string{"A"}, map[string]any{"n": "seed"})

	replica, err := graph.New(graph.Config{SnowflakeNodeID: 2, BadgerInMemory: true, ReadOnlyReplica: true})
	if err != nil {
		t.Fatalf("replica New: %v", err)
	}
	defer replica.Close()
	var snap bytes.Buffer
	if err := primary.IO().Export(&snap); err != nil {
		t.Fatalf("Export: %v", err)
	}
	if err := replica.IO().Import(&snap, tkgio.ImportOptions{}); err != nil {
		t.Fatalf("Import: %v", err)
	}
	lsn0, err := primary.Replication().LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	}
	if err := replica.Replication().SetAppliedLSN(lsn0); err != nil {
		t.Fatalf("SetAppliedLSN: %v", err)
	}

	// Three INDEPENDENT NodePuts past the snapshot, so they can be reordered
	// without a cross-record dependency error.
	mustAdd(t, primary, []string{"A"}, map[string]any{"n": "x"})
	mustAdd(t, primary, []string{"A"}, map[string]any{"n": "y"})
	mustAdd(t, primary, []string{"A"}, map[string]any{"n": "z"})

	var recs []store.ChangeRecord
	if err := primary.Replication().ForEachChange(lsn0, func(rec store.ChangeRecord) bool {
		recs = append(recs, rec)
		return true
	}); err != nil {
		t.Fatalf("ForEachChange: %v", err)
	}
	if len(recs) < 3 {
		t.Fatalf("got %d records past snapshot, want >= 3", len(recs))
	}

	// Misordered batch: r1 arrives AFTER r2 (below the running maximum).
	mis := []store.ChangeRecord{recs[0], recs[2], recs[1]}
	applied, err := replica.Replication().ApplyChanges(mis)
	if !errors.Is(err, store.ErrChangesNotAscending) {
		t.Fatalf("ApplyChanges err = %v, want wrapping ErrChangesNotAscending", err)
	}
	// The strictly-ascending prefix (r0, r2) was applied, flushed and watermarked.
	if applied != recs[2].LSN {
		t.Fatalf("applied LSN = %d, want %d (successful prefix watermarked)", applied, recs[2].LSN)
	}
	if got, _ := replica.Replication().AppliedLSN(); got != recs[2].LSN {
		t.Fatalf("persisted watermark = %d, want %d", got, recs[2].LSN)
	}
}

// Apply reproduces a primary row's temporal metadata (TxFrom / ValidFrom)
// VERBATIM — it must never re-stamp from the replica's own clock. The
// integrity-hash convergence oracle is BLIND to these fields (TxFrom is not
// hashed), so a re-stamping regression would pass every hash-based test in this
// file. This reads the applied replica row's Temporal() directly. The explicit
// world-time (ValidFrom) is a fixed PAST value no wall-clock stamp could
// coincidentally reproduce.
func TestReplicaApply_ReproducesTemporalVerbatim(t *testing.T) {
	ctx := context.Background()
	primary, err := graph.New(graph.Config{SnowflakeNodeID: 1, BadgerInMemory: true, ChangeLog: true, SyncWrites: true})
	if err != nil {
		t.Fatalf("primary New: %v", err)
	}
	defer primary.Close()
	mustAdd(t, primary, []string{"A"}, map[string]any{"n": "seed"}) // seed token A

	replica, err := graph.New(graph.Config{SnowflakeNodeID: 2, BadgerInMemory: true, ReadOnlyReplica: true})
	if err != nil {
		t.Fatalf("replica New: %v", err)
	}
	defer replica.Close()
	var snap bytes.Buffer
	if err := primary.IO().Export(&snap); err != nil {
		t.Fatalf("Export: %v", err)
	}
	if err := replica.IO().Import(&snap, tkgio.ImportOptions{}); err != nil {
		t.Fatalf("Import: %v", err)
	}
	lsn0, _ := primary.Replication().LastCommittedLSN()
	if err := replica.Replication().SetAppliedLSN(lsn0); err != nil {
		t.Fatalf("SetAppliedLSN: %v", err)
	}

	// Create the node under test AFTER bootstrap (so it travels via the apply
	// path, not the snapshot import), with an explicit PAST world-time.
	vf := types.Instant(1_700_000_000_000)
	a := mustAdd(t, primary, []string{"A"}, map[string]any{"n": "a", "tkg_valid_from": vf})

	pn, err := primary.Nodes().Get(ctx, a.ID())
	if err != nil {
		t.Fatalf("primary Get: %v", err)
	}
	pt := pn.Temporal()
	if pt.ValidFrom != vf {
		t.Fatalf("setup: primary ValidFrom = %d, want %d", pt.ValidFrom, vf)
	}
	if pt.TxFrom == 0 {
		t.Fatal("setup: primary TxFrom unset")
	}

	var recs []store.ChangeRecord
	if err := primary.Replication().ForEachChange(lsn0, func(rec store.ChangeRecord) bool {
		recs = append(recs, rec)
		return true
	}); err != nil {
		t.Fatalf("ForEachChange: %v", err)
	}
	if _, err := replica.Replication().ApplyChanges(recs); err != nil {
		t.Fatalf("ApplyChanges: %v", err)
	}

	rn, err := replica.Nodes().Get(ctx, a.ID())
	if err != nil {
		t.Fatalf("replica Get: %v", err)
	}
	rt := rn.Temporal()
	if rt.ValidFrom != pt.ValidFrom {
		t.Fatalf("replica ValidFrom = %d, want %d (apply must reproduce world-time verbatim)", rt.ValidFrom, pt.ValidFrom)
	}
	if rt.TxFrom != pt.TxFrom {
		t.Fatalf("replica TxFrom = %d, want %d (apply must reproduce TxFrom verbatim, NOT re-stamp from the replica clock)", rt.TxFrom, pt.TxFrom)
	}
}

// At-least-once redelivery of a WithHistory label-add NodePut must no-op via the
// nodeWireMatches guard, which runs BEFORE labelTokenDiff would re-route it to the
// label-add door (whose ValidateNodeLabelAddition rejects an already-present
// token). The existing idempotent-reapply test is watermark-gated (the second
// pass is skipped at LSN<=watermark, never reaching nodeWireMatches); this rolls
// the watermark back to simulate a crash that lost the watermark advance, so the
// redelivered record actually re-enters the apply path.
func TestReplicaApply_LabelAddRedeliveryNoOps(t *testing.T) {
	ctx := context.Background()
	primary, err := graph.New(graph.Config{SnowflakeNodeID: 1, BadgerInMemory: true, ChangeLog: true, SyncWrites: true})
	if err != nil {
		t.Fatalf("primary New: %v", err)
	}
	defer primary.Close()
	// Seed BOTH tokens A and B into the bootstrap snapshot so the post-bootstrap
	// label-add record reuses known tokens (no refetch needed).
	mustAdd(t, primary, []string{"A", "B"}, map[string]any{"n": "seed"})
	a := mustAdd(t, primary, []string{"A"}, map[string]any{"n": "a"})

	replica, err := graph.New(graph.Config{SnowflakeNodeID: 2, BadgerInMemory: true, ReadOnlyReplica: true})
	if err != nil {
		t.Fatalf("replica New: %v", err)
	}
	defer replica.Close()
	var snap bytes.Buffer
	if err := primary.IO().Export(&snap); err != nil {
		t.Fatalf("Export: %v", err)
	}
	if err := replica.IO().Import(&snap, tkgio.ImportOptions{}); err != nil {
		t.Fatalf("Import: %v", err)
	}
	lsn0, _ := primary.Replication().LastCommittedLSN()
	if err := replica.Replication().SetAppliedLSN(lsn0); err != nil {
		t.Fatalf("SetAppliedLSN: %v", err)
	}

	// AddLabel emits a WithHistory single-add ChangeNodePut for a (label set {A,B}).
	if err := primary.Nodes().AddLabel(ctx, a.ID(), "B"); err != nil {
		t.Fatalf("AddLabel: %v", err)
	}

	var recs []store.ChangeRecord
	if err := primary.Replication().ForEachChange(lsn0, func(rec store.ChangeRecord) bool {
		recs = append(recs, rec)
		return true
	}); err != nil {
		t.Fatalf("ForEachChange: %v", err)
	}
	if len(recs) == 0 {
		t.Fatal("no records emitted by AddLabel")
	}

	if _, err := replica.Replication().ApplyChanges(recs); err != nil {
		t.Fatalf("ApplyChanges pass 1: %v", err)
	}
	hist1 := mustNodeHistory(t, replica, a.ID())

	// Roll the watermark back so the redelivered records re-enter the apply path
	// (LSN > watermark) instead of being skipped by the watermark guard.
	if err := replica.Replication().SetAppliedLSN(lsn0); err != nil {
		t.Fatalf("SetAppliedLSN rollback: %v", err)
	}
	if _, err := replica.Replication().ApplyChanges(recs); err != nil {
		t.Fatalf("redelivered label-add ApplyChanges must no-op, got: %v "+
			"(nodeWireMatches must short-circuit before the label-add door rejects the already-present token)", err)
	}
	hist2 := mustNodeHistory(t, replica, a.ID())
	if len(hist1) != len(hist2) {
		t.Fatalf("redelivery changed history depth %d -> %d (not idempotent)", len(hist1), len(hist2))
	}
	assertConverged(t, "after label-add redelivery", primary, replica)
}

// A disk-backed replica WITHOUT SyncWrites (async write buffer) must persist its
// applied data AND watermark across a restart, and resume tailing from the
// watermark — the durability-ordering path (flush-before-watermark) that keeps
// the watermark from ever running ahead of the data.
func TestReplicaConvergence_DurableRestartResume(t *testing.T) {
	ctx := context.Background()
	primary, err := graph.New(graph.Config{SnowflakeNodeID: 1, BadgerInMemory: true, ChangeLog: true, SyncWrites: true})
	if err != nil {
		t.Fatalf("primary New: %v", err)
	}
	defer primary.Close()
	a := mustAdd(t, primary, []string{"A"}, map[string]any{"n": "a"})
	if _, err := primary.Nodes().Update(ctx, a.ID(), map[string]any{"n": "a2"}); err != nil {
		t.Fatalf("Update: %v", err)
	}

	dir := t.TempDir()
	// Replica: disk-backed, async buffer (no SyncWrites) — the case where the
	// watermark MetaSet must not outrun the data. NOTE: this test exercises a
	// GRACEFUL restart (replica.Close() flushes both data and watermark), so it
	// pins persistence + resume but does NOT by itself distinguish flush-before-
	// watermark ordering from watermark-before-flush — Close() would flush both
	// either way. A true crash injection is not expressible at the graph façade
	// (Badger's dir lock forbids reopening without Close, and there is no abandon
	// hook). The flush-before-watermark ordering is guaranteed structurally by
	// ApplyChange/ApplyChanges (flushStoreLocked precedes setAppliedLSNLocked).
	replica, err := graph.New(graph.Config{SnowflakeNodeID: 2, BadgerDir: dir, ReadOnlyReplica: true})
	if err != nil {
		t.Fatalf("replica New: %v", err)
	}
	var snap bytes.Buffer
	if err := primary.IO().Export(&snap); err != nil {
		t.Fatalf("Export: %v", err)
	}
	if err := replica.IO().Import(&snap, tkgio.ImportOptions{}); err != nil {
		t.Fatalf("Import: %v", err)
	}
	var recs []store.ChangeRecord
	if err := primary.Replication().ForEachChange(0, func(rec store.ChangeRecord) bool {
		recs = append(recs, rec)
		return true
	}); err != nil {
		t.Fatalf("ForEachChange: %v", err)
	}
	applied, err := replica.Replication().ApplyChanges(recs)
	if err != nil {
		t.Fatalf("ApplyChanges: %v", err)
	}
	assertConverged(t, "before restart", primary, replica)
	if err := replica.Close(); err != nil {
		t.Fatalf("replica Close: %v", err)
	}

	// Reopen the same data dir — the applied data and the watermark must both
	// have survived, and the watermark must equal the last applied LSN.
	replica2, err := graph.New(graph.Config{SnowflakeNodeID: 2, BadgerDir: dir, ReadOnlyReplica: true})
	if err != nil {
		t.Fatalf("replica2 New: %v", err)
	}
	defer replica2.Close()
	resume, err := replica2.Replication().AppliedLSN()
	if err != nil {
		t.Fatalf("AppliedLSN after restart: %v", err)
	}
	if resume != applied {
		t.Fatalf("resume watermark = %d, want %d (watermark must persist across restart)", resume, applied)
	}
	assertConverged(t, "after restart", primary, replica2)
}

// A record at or below the applied watermark (stale / out-of-order redelivery)
// must be a no-op — it must not regress the replica's current row or watermark.
func TestReplicaApply_SkipsStaleLSN(t *testing.T) {
	ctx := context.Background()
	primary, err := graph.New(graph.Config{SnowflakeNodeID: 1, BadgerInMemory: true, ChangeLog: true, SyncWrites: true})
	if err != nil {
		t.Fatalf("primary New: %v", err)
	}
	defer primary.Close()
	a := mustAdd(t, primary, []string{"A"}, map[string]any{"n": "v1"})
	if _, err := primary.Nodes().Update(ctx, a.ID(), map[string]any{"n": "v2"}); err != nil {
		t.Fatalf("Update: %v", err)
	}

	replica, err := graph.New(graph.Config{SnowflakeNodeID: 2, BadgerInMemory: true, ReadOnlyReplica: true})
	if err != nil {
		t.Fatalf("replica New: %v", err)
	}
	defer replica.Close()
	var snap bytes.Buffer
	if err := primary.IO().Export(&snap); err != nil {
		t.Fatalf("Export: %v", err)
	}
	if err := replica.IO().Import(&snap, tkgio.ImportOptions{}); err != nil {
		t.Fatalf("Import: %v", err)
	}
	var recs []store.ChangeRecord
	if err := primary.Replication().ForEachChange(0, func(rec store.ChangeRecord) bool {
		recs = append(recs, rec)
		return true
	}); err != nil {
		t.Fatalf("ForEachChange: %v", err)
	}
	if len(recs) < 2 {
		t.Fatalf("expected >= 2 records (create + update), got %d", len(recs))
	}
	applied, err := replica.Replication().ApplyChanges(recs)
	if err != nil {
		t.Fatalf("ApplyChanges: %v", err)
	}

	// Re-feed the FIRST (lowest-LSN, now stale) record — the node's v1 state.
	// It must be skipped: ApplyChange returns nil, the watermark stays put, and
	// the replica's current row stays at v2 (not regressed to v1).
	stale := recs[0]
	if stale.LSN > applied {
		t.Fatalf("test setup: first record LSN %d not <= watermark %d", stale.LSN, applied)
	}
	if err := replica.Replication().ApplyChange(stale); err != nil {
		t.Fatalf("stale ApplyChange should be a no-op, got: %v", err)
	}
	if got, _ := replica.Replication().AppliedLSN(); got != applied {
		t.Fatalf("watermark regressed to %d after stale apply, want %d", got, applied)
	}
	n, err := replica.Nodes().Get(ctx, a.ID())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if v, _ := n.Properties().Get("n"); v != "v2" {
		t.Fatalf("stale apply regressed current row: n=%v, want v2", v)
	}
}

// Covers the apply handlers the byte-exact tail test does not reach: a standalone
// relationship delete (applyRelDeleteLocked) and version-interval timeline edits
// that emit history-version records (applyNodeHistoryVersionLocked /
// applyRelHistoryVersionLocked). Full convergence — including version-history
// parity for every live and deleted entity — is the oracle.
func TestReplicaConvergence_RelDeleteAndVersionInterval(t *testing.T) {
	ctx := context.Background()
	primary, err := graph.New(graph.Config{SnowflakeNodeID: 1, BadgerInMemory: true, ChangeLog: true, SyncWrites: true})
	if err != nil {
		t.Fatalf("primary New: %v", err)
	}
	defer primary.Close()

	a := mustAdd(t, primary, []string{"A"}, map[string]any{"n": "a"})
	b := mustAdd(t, primary, []string{"A"}, map[string]any{"n": "b", "tkg_valid_from": types.Instant(1000)})
	c := mustAdd(t, primary, []string{"A"}, map[string]any{"n": "c"})
	r1, err := primary.Rels().AddByID(ctx, "KNOWS", a.ID(), b.ID(), map[string]any{"tkg_valid_from": types.Instant(1000), "w": int64(1)})
	if err != nil {
		t.Fatalf("seed rel r1: %v", err)
	}
	r2, err := primary.Rels().AddByID(ctx, "KNOWS", a.ID(), c.ID(), nil)
	if err != nil {
		t.Fatalf("seed rel r2: %v", err)
	}

	var snap bytes.Buffer
	if err := primary.IO().Export(&snap); err != nil {
		t.Fatalf("Export: %v", err)
	}
	lsn0, err := primary.Replication().LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	}
	replica, err := graph.New(graph.Config{SnowflakeNodeID: 2, BadgerInMemory: true, ReadOnlyReplica: true})
	if err != nil {
		t.Fatalf("replica New: %v", err)
	}
	defer replica.Close()
	if err := replica.IO().Import(&snap, tkgio.ImportOptions{}); err != nil {
		t.Fatalf("replica Import: %v", err)
	}
	if err := replica.Replication().SetAppliedLSN(lsn0); err != nil {
		t.Fatalf("SetAppliedLSN: %v", err)
	}
	assertConverged(t, "after bootstrap", primary, replica)

	// 1. Standalone relationship delete (saves a tombstone version) →
	//    ChangeRelDelete{WithHistory} → applyRelDeleteLocked.
	if err := primary.Rels().Delete(ctx, r2.ID()); err != nil {
		t.Fatalf("Delete r2: %v", err)
	}
	// 2. Node version-interval edit tiles the timeline → ChangeNodeHistoryVersion
	//    → applyNodeHistoryVersionLocked.
	if _, err := primary.Temporal().SetNodeVersionInterval(ctx, b.ID(), types.Instant(2000), types.Instant(0), map[string]any{"n": "b-tiled"}); err != nil {
		t.Fatalf("SetNodeVersionInterval: %v", err)
	}
	// 3. Rel version-interval edit → ChangeRelHistoryVersion →
	//    applyRelHistoryVersionLocked.
	if _, err := primary.Temporal().SetRelVersionInterval(ctx, r1.ID(), types.Instant(2000), types.Instant(0), map[string]any{"w": int64(2)}); err != nil {
		t.Fatalf("SetRelVersionInterval: %v", err)
	}

	from, err := replica.Replication().AppliedLSN()
	if err != nil {
		t.Fatalf("AppliedLSN: %v", err)
	}
	var recs []store.ChangeRecord
	if err := primary.Replication().ForEachChange(from, func(rec store.ChangeRecord) bool {
		recs = append(recs, rec)
		return true
	}); err != nil {
		t.Fatalf("ForEachChange: %v", err)
	}
	if len(recs) == 0 {
		t.Fatal("no change records past the snapshot LSN — nothing to tail")
	}
	if _, err := replica.Replication().ApplyChanges(recs); err != nil {
		t.Fatalf("ApplyChanges: %v", err)
	}

	assertConverged(t, "after rel-delete + version-interval tail", primary, replica)

	// The deleted rel's tombstone history must replicate byte-for-byte too.
	pHist := mustRelHistory(t, primary, r2.ID())
	rHist := mustRelHistory(t, replica, r2.ID())
	if len(pHist) != len(rHist) {
		t.Fatalf("deleted rel r2 history depth: primary %d, replica %d", len(pHist), len(rHist))
	}
}

// --- helpers ---

func mustAdd(t *testing.T, g *graph.Graph, labels []string, props map[string]any) *types.Node {
	t.Helper()
	n, err := g.Nodes().Add(context.Background(), labels, props)
	if err != nil {
		t.Fatalf("Add %v: %v", labels, err)
	}
	return n
}

func mustNodeHistory(t *testing.T, g *graph.Graph, id types.NodeID) []*types.Node {
	t.Helper()
	h, err := g.Nodes().History(id)
	if err != nil {
		t.Fatalf("Nodes().History(%v): %v", id, err)
	}
	return h
}

func mustRelHistory(t *testing.T, g *graph.Graph, id types.RelID) []*types.Relationship {
	t.Helper()
	h, err := g.Rels().History(id)
	if err != nil {
		t.Fatalf("Rels().History(%v): %v", id, err)
	}
	return h
}

// assertConverged checks the replica reproduces the primary exactly: equal live
// node/rel sets (by integrity hash + version + label tokens) and equal version
// history (including for explicitly-listed deleted IDs).
func assertConverged(t *testing.T, phase string, primary, replica *graph.Graph) {
	t.Helper()

	pn, err := primary.Nodes().All(graph.QueryOpts{})
	if err != nil {
		t.Fatalf("[%s] primary Nodes().All: %v", phase, err)
	}
	rn, err := replica.Nodes().All(graph.QueryOpts{})
	if err != nil {
		t.Fatalf("[%s] replica Nodes().All: %v", phase, err)
	}
	assertNodeSetEqual(t, phase, pn, rn)

	pr, err := primary.Rels().All(graph.QueryOpts{})
	if err != nil {
		t.Fatalf("[%s] primary Rels().All: %v", phase, err)
	}
	rr, err := replica.Rels().All(graph.QueryOpts{})
	if err != nil {
		t.Fatalf("[%s] replica Rels().All: %v", phase, err)
	}
	assertRelSetEqual(t, phase, pr, rr)

	// Full version-history parity for every live entity. This is the check that
	// catches a mis-applied WithHistory bit: an in-place update that wrongly
	// regenerated a history row (or a with-history update that wrongly didn't)
	// would diverge the history depth here even when the current row matches.
	for _, p := range pn {
		ph, err := primary.Nodes().History(p.InternalID())
		if err != nil {
			t.Fatalf("[%s] primary node %v History: %v", phase, p.InternalID(), err)
		}
		rh, err := replica.Nodes().History(p.InternalID())
		if err != nil {
			t.Fatalf("[%s] replica node %v History: %v", phase, p.InternalID(), err)
		}
		assertHistoryEqual(t, phase, "node", int64(p.InternalID().SnowflakeID()), nodeHashes(ph), nodeHashes(rh))
	}
	for _, p := range pr {
		ph, err := primary.Rels().History(p.InternalID())
		if err != nil {
			t.Fatalf("[%s] primary rel %v History: %v", phase, p.InternalID(), err)
		}
		rh, err := replica.Rels().History(p.InternalID())
		if err != nil {
			t.Fatalf("[%s] replica rel %v History: %v", phase, p.InternalID(), err)
		}
		assertHistoryEqual(t, phase, "rel", int64(p.InternalID().SnowflakeID()), relHashes(ph), relHashes(rh))
	}
}

func assertHistoryEqual(t *testing.T, phase, kind string, id int64, p, r []string) {
	t.Helper()
	if len(p) != len(r) {
		t.Fatalf("[%s] %s %d history depth: primary %d, replica %d", phase, kind, id, len(p), len(r))
	}
	for i := range p {
		if p[i] != r[i] {
			t.Fatalf("[%s] %s %d history[%d] hash: primary %q, replica %q", phase, kind, id, i, p[i], r[i])
		}
	}
}

func nodeHashes(ns []*types.Node) []string {
	out := make([]string, len(ns))
	for i, n := range ns {
		out[i] = nodeHash(n)
	}
	return out
}

func relHashes(rs []*types.Relationship) []string {
	out := make([]string, len(rs))
	for i, r := range rs {
		out[i] = relHash(r)
	}
	return out
}

func assertNodeSetEqual(t *testing.T, phase string, pn, rn []*types.Node) {
	t.Helper()
	if len(pn) != len(rn) {
		t.Fatalf("[%s] live node count: primary %d, replica %d", phase, len(pn), len(rn))
	}
	rmap := make(map[types.NodeID]*types.Node, len(rn))
	for _, n := range rn {
		rmap[n.InternalID()] = n
	}
	for _, p := range pn {
		r, ok := rmap[p.InternalID()]
		if !ok {
			t.Fatalf("[%s] node %v present on primary, missing on replica", phase, p.InternalID())
		}
		if ph, rh := nodeHash(p), nodeHash(r); ph != rh {
			t.Fatalf("[%s] node %v hash: primary %q, replica %q", phase, p.InternalID(), ph, rh)
		}
		if p.Version() != r.Version() {
			t.Fatalf("[%s] node %v version: primary %d, replica %d", phase, p.InternalID(), p.Version(), r.Version())
		}
		if !sameLabelTokens(p, r) {
			t.Fatalf("[%s] node %v label tokens diverged", phase, p.InternalID())
		}
	}
}

func assertRelSetEqual(t *testing.T, phase string, pr, rr []*types.Relationship) {
	t.Helper()
	if len(pr) != len(rr) {
		t.Fatalf("[%s] live rel count: primary %d, replica %d", phase, len(pr), len(rr))
	}
	rmap := make(map[types.RelID]*types.Relationship, len(rr))
	for _, r := range rr {
		rmap[r.InternalID()] = r
	}
	for _, p := range pr {
		r, ok := rmap[p.InternalID()]
		if !ok {
			t.Fatalf("[%s] rel %v present on primary, missing on replica", phase, p.InternalID())
		}
		if ph, rh := relHash(p), relHash(r); ph != rh {
			t.Fatalf("[%s] rel %v hash: primary %q, replica %q", phase, p.InternalID(), ph, rh)
		}
		if p.Version() != r.Version() {
			t.Fatalf("[%s] rel %v version: primary %d, replica %d", phase, p.InternalID(), p.Version(), r.Version())
		}
	}
}

func nodeHash(n *types.Node) string {
	if ig := n.Integrity(); ig != nil {
		return ig.Hash
	}
	return ""
}

func relHash(r *types.Relationship) string {
	if ig := r.Integrity(); ig != nil {
		return ig.Hash
	}
	return ""
}

func sameLabelTokens(a, b *types.Node) bool {
	if a.LabelTokenCount() != b.LabelTokenCount() {
		return false
	}
	set := make(map[uint16]struct{}, a.LabelTokenCount())
	for i := 0; i < a.LabelTokenCount(); i++ {
		set[a.LabelTokenRawAt(i)] = struct{}{}
	}
	for i := 0; i < b.LabelTokenCount(); i++ {
		if _, ok := set[b.LabelTokenRawAt(i)]; !ok {
			return false
		}
	}
	return true
}
