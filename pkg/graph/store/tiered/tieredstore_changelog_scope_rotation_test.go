package tiered

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BACKLOG 19h: forEachScopeShard re-derives its shard set on every call rather
// than snapshotting it at BeginLogScope, so a shard created by rotation WHILE a
// change-log scope is open needs an explicit hand-off (rotateHotShardLocked) or
// its rotation-triggering write would self-commit eagerly outside the scope —
// invisible to the scope's own DiscardLogScope on a later rollback, leaking a
// change-log record. These tests pin the fixed behavior with white-box control
// over the low-level scope API, bypassing the full graph/core Tx layer.

// TestScopeRotation_ShardBeforeFirstDivert_Included covers the clean baseline
// (case A): a shard that already exists before the tx's first SetLogDivert(true)
// is included atomically in the eventual commit, with a contiguous LSN minted at
// CommitLogScope — unaffected by this fix (no rotation involved).
func TestScopeRotation_ShardBeforeFirstDivert_Included(t *testing.T) {
	ts, caseTok, _, _ := newChangeLogTieredStore(t)
	nodeGen := tieredNodeGen(t)

	if err := ts.BeginLogScope(); err != nil {
		t.Fatalf("BeginLogScope: %v", err)
	}
	ts.SetLogDivert(true)
	n1 := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil)
	if err := ts.PutNode(n1); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	ts.SetLogDivert(false)

	lsn, err := ts.CommitLogScope()
	if err != nil {
		t.Fatalf("CommitLogScope: %v", err)
	}
	if lsn != 1 {
		t.Fatalf("CommitLogScope LSN = %d, want 1", lsn)
	}
	feed, err := ts.ChangeFeed(0, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(feed) != 1 || feed[0].LSN != 1 {
		t.Fatalf("feed = %+v, want exactly 1 record at LSN 1", feed)
	}
}

// TestScopeRotation_ShardAfterFirstDivert_NowIncluded proves the fix: a shard
// created by rotation WHILE a scope is open (divert already on) is brought into
// the scope immediately by rotateHotShardLocked's hand-off, so even the very
// first write that lands on it — the analog of the "rotation-triggering write" —
// is buffered into the scope rather than self-committing eagerly outside it.
// Before the fix, LastCommittedLSN would already read 1 after the first PutNode
// (the eager self-commit); after the fix it must read 0 until CommitLogScope.
func TestScopeRotation_ShardAfterFirstDivert_NowIncluded(t *testing.T) {
	ts, _, _, signalTok := newChangeLogTieredStore(t)
	nodeGen := tieredNodeGen(t)

	if err := ts.BeginLogScope(); err != nil {
		t.Fatalf("BeginLogScope: %v", err)
	}
	ts.SetLogDivert(true)

	// Rotate the hot shard WHILE the scope is open and divert is on — exactly the
	// state a mid-tx checkRotation-triggered rotation would run under.
	if err := ts.ForceRotate(); err != nil {
		t.Fatalf("ForceRotate: %v", err)
	}

	// This write lands on the newly-rotated-in hot shard — the analog of the
	// rotation-triggering write.
	n1 := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	if err := ts.PutNode(n1); err != nil {
		t.Fatalf("PutNode 1: %v", err)
	}
	ts.SetLogDivert(false)
	ts.SetLogDivert(true)
	n2 := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	if err := ts.PutNode(n2); err != nil {
		t.Fatalf("PutNode 2: %v", err)
	}
	ts.SetLogDivert(false)

	// Before CommitLogScope, nothing should be durably committed yet — proving
	// BOTH records (including the rotation-triggering one) were buffered into
	// the scope, not eagerly self-committed with their own LSN.
	preCommitLSN, err := ts.LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN pre-commit: %v", err)
	}
	if preCommitLSN != 0 {
		t.Fatalf("LastCommittedLSN before CommitLogScope = %d, want 0 (BACKLOG 19h regression: "+
			"the rotation-triggering write self-committed eagerly outside the scope)", preCommitLSN)
	}

	lsn, err := ts.CommitLogScope()
	if err != nil {
		t.Fatalf("CommitLogScope: %v", err)
	}
	if lsn != 2 {
		t.Fatalf("CommitLogScope LSN = %d, want 2 (both records minted contiguously at commit)", lsn)
	}
	feed, err := ts.ChangeFeed(0, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(feed) != 2 {
		t.Fatalf("feed length = %d, want 2: %+v", len(feed), feed)
	}
	if feed[0].LSN != 1 || feed[1].LSN != 2 {
		t.Fatalf("feed LSNs = [%d,%d], want [1,2] (contiguous, minted at commit)", feed[0].LSN, feed[1].LSN)
	}
}

// TestScopeRotation_Rollback_DoesNotLeak proves the rollback-leak closure: a
// mid-scope rotation followed by DiscardLogScope leaves the feed completely
// empty — no record from the rotation-triggering write leaks out with its own
// LSN, and no LSN is burned. Before the fix, the eagerly self-committed record on
// the rotated-in shard was outside the scope's buffer entirely, so
// DiscardLogScope could never reach it.
func TestScopeRotation_Rollback_DoesNotLeak(t *testing.T) {
	ts, _, _, signalTok := newChangeLogTieredStore(t)
	nodeGen := tieredNodeGen(t)

	if err := ts.BeginLogScope(); err != nil {
		t.Fatalf("BeginLogScope: %v", err)
	}
	ts.SetLogDivert(true)
	if err := ts.ForceRotate(); err != nil {
		t.Fatalf("ForceRotate: %v", err)
	}
	n1 := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	if err := ts.PutNode(n1); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	ts.SetLogDivert(false)

	if err := ts.DiscardLogScope(); err != nil {
		t.Fatalf("DiscardLogScope: %v", err)
	}

	last, err := ts.LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	}
	if last != 0 {
		t.Fatalf("LastCommittedLSN after rollback = %d, want 0 (BACKLOG 19h regression: "+
			"the rotation-triggering write leaked a record/LSN outside the discarded scope)", last)
	}
	feed, err := ts.ChangeFeed(0, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(feed) != 0 {
		t.Fatalf("feed after rollback = %+v, want empty", feed)
	}

	// The store must still be fully usable afterward (the scope-state fields were
	// correctly reset by DiscardLogScope) — a subsequent normal write outside any
	// scope must commit eagerly with a fresh LSN.
	n2 := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	if err := ts.PutNode(n2); err != nil {
		t.Fatalf("PutNode after discard: %v", err)
	}
	last2, err := ts.LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN after post-discard write: %v", err)
	}
	if last2 != 1 {
		t.Fatalf("LastCommittedLSN after post-discard write = %d, want 1", last2)
	}
}
