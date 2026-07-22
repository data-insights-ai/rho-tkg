package badger

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestNodesAsOf_BulkScan_NoDropAcrossFlushMidScan is the deterministic
// regression test for the BACKLOG 18k undercounting bug: NodesAsOf/RelsAsOf's
// single-shared-transaction bulk scan (introduced by the transaction-batching
// perf change) opened its badger snapshot ONCE up front, but the FIRST
// implementation re-read the live pending/flushing overlay independently PER
// CANDIDATE ENTITY over the scan's long real-time duration. A background
// flush() that commits a parked history version to badger AND clears it from
// `flushing` strictly BETWEEN the shared transaction's snapshot instant and a
// LATER candidate's overlay read made that version invisible to both the
// (older) transaction snapshot and the (now-drained) overlay — silently
// dropping it from the result. See historyOverlaySnapshot's doc comment
// (badgerstore_txtime.go) for the fix: the overlay is now captured ONCE,
// before the shared transaction opens, and threaded through every candidate's
// resolution instead of re-read live.
//
// Reproduces the race deterministically with bulkAsOfScanTestHook (fires once
// per candidate, by index) + parkPendingIntoFlushing/commitFlushingToBadger
// (the same commit-window primitives badgerstore_flushing_commit_window_test.go
// already established for the single-entity history readers) — no goroutines,
// no timing luck, same failure every run without the fix.
func TestNodesAsOf_BulkScan_NoDropAcrossFlushMidScan(t *testing.T) {
	bs := newFlushParkStore(t, nil)

	const n = 5
	pin := types.Instant(150)
	ids := make([]types.NodeID, n)
	for i := 0; i < n; i++ {
		nid := types.NodeID(100 + int64(i))
		ids[i] = nid
		// v0: TxFrom=100 (<=pin, the version NodesAsOf(pin) must select).
		v0 := types.NewNode(nid, 1, nil)
		v0.SetVersion(0)
		v0.SetTemporal(&types.TemporalMetadata{TxFrom: 100})
		if err := bs.PutNode(v0); err != nil {
			t.Fatalf("PutNode v0(%d): %v", nid, err)
		}
		// v1: TxFrom=200 (>pin) — current row does NOT match the pin, forcing
		// the scan onto the history path where v0 must be found.
		v1 := types.NewNode(nid, 1, nil)
		v1.SetVersion(1)
		v1.SetTemporal(&types.TemporalMetadata{TxFrom: 200})
		if err := bs.ReplaceNodeWithHistory(v1, 0, v0); err != nil {
			t.Fatalf("ReplaceNodeWithHistory v1(%d): %v", nid, err)
		}
	}

	// v0's history rows (the ones NodesAsOf(pin) must find) are now sitting in
	// `pending`, unflushed. Park them into `flushing` — the state a background
	// flush() reaches right before its WriteBatch commit.
	parkPendingIntoFlushing(t, bs)
	if flushingLen(bs) == 0 {
		t.Fatal("expected non-empty flushing after park")
	}

	// Fire the commit exactly once, after candidate index 1 has already been
	// scanned (so candidates 0-1 see the live-parked overlay before the
	// commit, and candidates 2-4 are scanned strictly AFTER `flushing` is
	// cleared and the rows are durable in badger — but durable AFTER the
	// shared transaction's snapshot was taken, so invisible to it). This is
	// exactly the window the fix must close.
	committed := false
	bs.bulkAsOfScanTestHook = func(idx int) {
		if idx == 2 && !committed {
			committed = true
			commitFlushingToBadger(t, bs)
		}
	}
	defer func() { bs.bulkAsOfScanTestHook = nil }()

	got, err := bs.NodesAsOf(pin)
	if err != nil {
		t.Fatalf("NodesAsOf: %v", err)
	}
	if !committed {
		t.Fatal("test hook never fired at index 2 — scan order assumption invalid")
	}
	if len(got) != n {
		byID := make(map[types.NodeID]bool, len(got))
		for _, node := range got {
			byID[node.ID()] = true
		}
		var missing []types.NodeID
		for _, id := range ids {
			if !byID[id] {
				missing = append(missing, id)
			}
		}
		t.Fatalf("NodesAsOf(%d) returned %d nodes, want %d — missing %v (BACKLOG 18k regression: flush-mid-scan dropped a candidate)", pin, len(got), n, missing)
	}
	for _, node := range got {
		if node.Version() != 0 {
			t.Fatalf("node %d: version = %d, want 0 (the pinned v0 belief)", node.ID(), node.Version())
		}
	}
}

// TestRelsAsOf_BulkScan_NoDropAcrossFlushMidScan mirrors the node-side test
// for relationships (structural parity, rule 2).
func TestRelsAsOf_BulkScan_NoDropAcrossFlushMidScan(t *testing.T) {
	bs := newFlushParkStore(t, nil)

	const n = 5
	pin := types.Instant(150)
	startID := types.NodeID(1)
	endID := types.NodeID(2)
	start := types.NewNode(startID, 1, nil)
	start.SetTemporal(&types.TemporalMetadata{TxFrom: 10})
	if err := bs.PutNode(start); err != nil {
		t.Fatalf("PutNode start: %v", err)
	}
	end := types.NewNode(endID, 1, nil)
	end.SetTemporal(&types.TemporalMetadata{TxFrom: 10})
	if err := bs.PutNode(end); err != nil {
		t.Fatalf("PutNode end: %v", err)
	}

	ids := make([]types.RelID, n)
	for i := 0; i < n; i++ {
		rid := types.RelID(200 + int64(i))
		ids[i] = rid
		v0 := types.NewRelationship(rid, 1, startID, endID)
		v0.SetVersion(0)
		v0.SetTemporal(&types.TemporalMetadata{TxFrom: 100})
		if err := bs.PutRelationship(v0); err != nil {
			t.Fatalf("PutRelationship v0(%d): %v", rid, err)
		}
		v1 := types.NewRelationship(rid, 1, startID, endID)
		v1.SetVersion(1)
		v1.SetTemporal(&types.TemporalMetadata{TxFrom: 200})
		if err := bs.ReplaceRelWithHistory(v1, 0, v0); err != nil {
			t.Fatalf("ReplaceRelWithHistory v1(%d): %v", rid, err)
		}
	}

	parkPendingIntoFlushing(t, bs)
	if flushingLen(bs) == 0 {
		t.Fatal("expected non-empty flushing after park")
	}

	committed := false
	bs.bulkAsOfScanTestHook = func(idx int) {
		if idx == 2 && !committed {
			committed = true
			commitFlushingToBadger(t, bs)
		}
	}
	defer func() { bs.bulkAsOfScanTestHook = nil }()

	got, err := bs.RelsAsOf(pin)
	if err != nil {
		t.Fatalf("RelsAsOf: %v", err)
	}
	if !committed {
		t.Fatal("test hook never fired at index 2 — scan order assumption invalid")
	}
	if len(got) != n {
		byID := make(map[types.RelID]bool, len(got))
		for _, r := range got {
			byID[r.ID()] = true
		}
		var missing []types.RelID
		for _, id := range ids {
			if !byID[id] {
				missing = append(missing, id)
			}
		}
		t.Fatalf("RelsAsOf(%d) returned %d rels, want %d — missing %v (BACKLOG 18k regression: flush-mid-scan dropped a candidate)", pin, len(got), n, missing)
	}
	for _, r := range got {
		if r.Version() != 0 {
			t.Fatalf("rel %d: version = %d, want 0 (the pinned v0 belief)", r.ID(), r.Version())
		}
	}
}
