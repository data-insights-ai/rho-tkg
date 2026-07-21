package badger

import (
	"errors"
	"sync"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// This file exercises store.ScopedDeleteCapability (BACKLOG 11f Batch C —
// foundation only; see the interface's doc comment in
// pkg/graph/store/changefeed.go). It mirrors
// badgerstore_history_scoped_test.go's battery for the two Batch C doors,
// DeleteNodeWithHistoryScoped / DeleteRelWithHistoryScoped.

// Compile-time proof the badger store satisfies the BACKLOG 11f Batch C
// capability.
var _ storecontract.ScopedDeleteCapability = (*Store)(nil)

// ─── token == 0 is exactly the unscoped door ────────────────────────────────

func TestScopedDelete_ZeroTokenMatchesUnscopedDoor(t *testing.T) {
	t.Parallel()
	bs := newChangeLogStore(t, false)

	putTestNode(t, bs, 1, 10, nil)
	current, _ := bs.GetNode(types.NodeID(1))
	tombstone := current.DeepCopy()
	prevVersion := current.Version()

	if err := bs.DeleteNodeWithHistoryScoped(types.NodeID(1), prevVersion, tombstone, nil, 0); err != nil {
		t.Fatalf("DeleteNodeWithHistoryScoped(token=0): %v", err)
	}

	recs := drainFeed(t, bs)
	// [create, delete-with-history] — both eager, since token==0.
	if got := tagSeq(recs); len(got) != 2 || got[0] != storecontract.ChangeNodePut || got[1] != storecontract.ChangeNodeDelete {
		t.Fatalf("tags = %v, want [NodePut NodeDelete]", got)
	}
	assertLSNContiguous(t, recs)

	if _, err := bs.GetNode(types.NodeID(1)); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("GetNode after delete error = %v, want ErrNodeNotFound", err)
	}
	hist, err := bs.GetNodeVersion(types.NodeID(1), prevVersion)
	if err != nil {
		t.Fatalf("GetNodeVersion: %v", err)
	}
	if hist == nil {
		t.Fatal("expected a tombstone history row")
	}
}

// ─── An open (uncommitted) scope is invisible to the feed ──────────────────

func TestScopedDelete_UncommittedScopeInvisibleToFeed(t *testing.T) {
	t.Parallel()
	bs := newChangeLogStore(t, false)

	putTestNode(t, bs, 1, 10, nil)
	current, _ := bs.GetNode(types.NodeID(1))
	tombstone := current.DeepCopy()
	prevVersion := current.Version()

	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	before, err := bs.LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	}

	token, err := bs.BeginScopedLog()
	if err != nil {
		t.Fatalf("BeginScopedLog: %v", err)
	}
	if token == 0 {
		t.Fatal("BeginScopedLog token = 0, want nonzero (log enabled)")
	}

	if err := bs.DeleteNodeWithHistoryScoped(types.NodeID(1), prevVersion, tombstone, nil, token); err != nil {
		t.Fatalf("DeleteNodeWithHistoryScoped: %v", err)
	}

	// Load-bearing: the record must NOT be visible before commit, even though
	// the entity delete + tombstone history row have already landed.
	recs := afterFeed(t, bs, before)
	if len(recs) != 0 {
		t.Fatalf("feed has %d records before CommitScopedLog, want 0", len(recs))
	}
	if _, err := bs.GetNode(types.NodeID(1)); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("entity delete must land even though the log record is scoped: GetNode error = %v, want ErrNodeNotFound", err)
	}

	maxLSN, err := bs.CommitScopedLog(token)
	if err != nil {
		t.Fatalf("CommitScopedLog: %v", err)
	}
	if maxLSN == 0 {
		t.Fatal("CommitScopedLog maxLSN = 0, want nonzero")
	}

	recs = afterFeed(t, bs, before)
	if len(recs) != 1 || recs[0].Tag != storecontract.ChangeNodeDelete {
		t.Fatalf("feed after commit = %v, want [NodeDelete]", tagSeq(recs))
	}
}

// ─── DiscardScopedLog drops the buffer and burns no LSN — a delete record ───
// specifically must leave zero trace in the feed after a discard.

func TestScopedDelete_DiscardEmitsNothingAndBurnsNoLSN(t *testing.T) {
	t.Parallel()
	bs := newChangeLogStore(t, false)

	putTestNode(t, bs, 1, 10, nil) // eager create, LSN 1
	current, _ := bs.GetNode(types.NodeID(1))
	tombstone := current.DeepCopy()
	prevVersion := current.Version()

	token, err := bs.BeginScopedLog()
	if err != nil {
		t.Fatalf("BeginScopedLog: %v", err)
	}
	if err := bs.DeleteNodeWithHistoryScoped(types.NodeID(1), prevVersion, tombstone, nil, token); err != nil {
		t.Fatalf("DeleteNodeWithHistoryScoped: %v", err)
	}
	if err := bs.DiscardScopedLog(token); err != nil {
		t.Fatalf("DiscardScopedLog: %v", err)
	}

	// A second eager write after the discard — its LSN must be contiguous with
	// the first eager record (2), proving the discarded scope burned no LSN.
	putTestNode(t, bs, 2, 10, nil)

	recs := drainFeed(t, bs)
	if len(recs) != 2 {
		t.Fatalf("feed has %d records, want 2 (discarded delete record must never appear)", len(recs))
	}
	assertLSNContiguous(t, recs)
	for _, r := range recs {
		if r.Tag == storecontract.ChangeNodeDelete {
			t.Fatalf("discarded scope's delete record appeared in the feed: %+v", r)
		}
	}

	// The entity delete itself is NOT rolled back by a log-scope discard —
	// only the change-log record is scoped, not the mutation.
	if _, err := bs.GetNode(types.NodeID(1)); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("entity delete must survive discard: GetNode error = %v, want ErrNodeNotFound", err)
	}
}

// ─── Unknown / retired token fails closed ───────────────────────────────────

func TestScopedDelete_UnknownTokenFailsClosed(t *testing.T) {
	t.Parallel()
	bs := newChangeLogStore(t, false)

	putTestNode(t, bs, 1, 10, nil)
	current, _ := bs.GetNode(types.NodeID(1))
	tombstone := current.DeepCopy()
	prevVersion := current.Version()

	if err := bs.DeleteNodeWithHistoryScoped(types.NodeID(1), prevVersion, tombstone, nil, 999); !errors.Is(err, storecontract.ErrInvalidStoreMutation) {
		t.Fatalf("DeleteNodeWithHistoryScoped(unknown token) error = %v, want ErrInvalidStoreMutation", err)
	}
}

// ─── Two concurrent scopes never cross-contaminate ──────────────────────────

func TestScopedDelete_ConcurrentScopesDoNotCrossContaminate(t *testing.T) {
	t.Parallel()
	bs := newChangeLogStore(t, false)

	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)

	n1, _ := bs.GetNode(types.NodeID(1))
	tomb1 := n1.DeepCopy()
	v1 := n1.Version()

	n2, _ := bs.GetNode(types.NodeID(2))
	tomb2 := n2.DeepCopy()
	v2 := n2.Version()

	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	before, _ := bs.LastCommittedLSN()

	tokenA, err := bs.BeginScopedLog()
	if err != nil {
		t.Fatalf("BeginScopedLog A: %v", err)
	}
	tokenB, err := bs.BeginScopedLog()
	if err != nil {
		t.Fatalf("BeginScopedLog B: %v", err)
	}
	if tokenA == tokenB {
		t.Fatalf("BeginScopedLog returned the same token twice: %d", tokenA)
	}

	if err := bs.DeleteNodeWithHistoryScoped(types.NodeID(1), v1, tomb1, nil, tokenA); err != nil {
		t.Fatalf("DeleteNodeWithHistoryScoped A: %v", err)
	}
	if err := bs.DeleteNodeWithHistoryScoped(types.NodeID(2), v2, tomb2, nil, tokenB); err != nil {
		t.Fatalf("DeleteNodeWithHistoryScoped B: %v", err)
	}

	// Commit B first, discard A — proves routing is keyed by the explicit
	// token argument, not by call order or a shared "active scope" flag.
	if _, err := bs.CommitScopedLog(tokenB); err != nil {
		t.Fatalf("CommitScopedLog B: %v", err)
	}
	if err := bs.DiscardScopedLog(tokenA); err != nil {
		t.Fatalf("DiscardScopedLog A: %v", err)
	}

	recs := afterFeed(t, bs, before)
	if len(recs) != 1 || recs[0].Tag != storecontract.ChangeNodeDelete {
		t.Fatalf("feed = %v, want [NodeDelete] (only B's, A discarded)", tagSeq(recs))
	}

	// Node 1's delete is scoped-then-discarded, so the ENTITY MUTATION still
	// landed (only the log record was discarded) but node 2's should be too —
	// both deletes physically happened regardless of log scoping.
	if _, err := bs.GetNode(types.NodeID(1)); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("GetNode(1) error = %v, want ErrNodeNotFound", err)
	}
	if _, err := bs.GetNode(types.NodeID(2)); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("GetNode(2) error = %v, want ErrNodeNotFound", err)
	}
}

// ─── DeleteRelWithHistoryScoped mirrors DeleteNodeWithHistoryScoped (rule 2 parity) ───

func TestScopedDelete_DeleteRelWithHistoryScoped(t *testing.T) {
	t.Parallel()
	bs := newChangeLogStore(t, false)

	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)
	rel := putTestRel(t, bs, 100, 5, 1, 2)

	cur, _ := bs.GetRelationship(types.RelID(100))
	tombstone := cur.DeepCopy()
	prevVersion := cur.Version()
	_ = rel

	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	before, _ := bs.LastCommittedLSN()

	token, err := bs.BeginScopedLog()
	if err != nil {
		t.Fatalf("BeginScopedLog: %v", err)
	}
	if err := bs.DeleteRelWithHistoryScoped(types.RelID(100), prevVersion, tombstone, token); err != nil {
		t.Fatalf("DeleteRelWithHistoryScoped: %v", err)
	}

	recs := afterFeed(t, bs, before)
	if len(recs) != 0 {
		t.Fatalf("feed has %d records before CommitScopedLog, want 0", len(recs))
	}
	if _, err := bs.GetRelationship(types.RelID(100)); !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("GetRelationship error = %v, want ErrRelNotFound", err)
	}

	if _, err := bs.CommitScopedLog(token); err != nil {
		t.Fatalf("CommitScopedLog: %v", err)
	}
	recs = afterFeed(t, bs, before)
	if len(recs) != 1 || recs[0].Tag != storecontract.ChangeRelDelete {
		t.Fatalf("feed = %v, want [RelDelete]", tagSeq(recs))
	}

	hist, err := bs.GetRelVersion(types.RelID(100), prevVersion)
	if err != nil {
		t.Fatalf("GetRelVersion: %v", err)
	}
	if hist == nil {
		t.Fatal("expected a tombstone history row")
	}
}

// ─── Disabled log: the scoped door is a documented no-op fallback ───────────

func TestScopedDelete_DisabledByDefault(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t) // ChangeLog not set

	putTestNode(t, bs, 1, 10, nil)
	current, _ := bs.GetNode(types.NodeID(1))
	tombstone := current.DeepCopy()
	prevVersion := current.Version()

	token, err := bs.BeginScopedLog()
	if err != nil {
		t.Fatalf("BeginScopedLog: %v", err)
	}
	if token != 0 {
		t.Fatalf("BeginScopedLog token = %d, want 0 (log disabled)", token)
	}

	if err := bs.DeleteNodeWithHistoryScoped(types.NodeID(1), prevVersion, tombstone, nil, token); err != nil {
		t.Fatalf("DeleteNodeWithHistoryScoped: %v", err)
	}
	if _, err := bs.GetNode(types.NodeID(1)); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("entity delete must still land with a disabled log: error = %v, want ErrNodeNotFound", err)
	}
}

// ─── The relTombstones-cover-every-connected-relationship invariant holds via
// the scoped route (2+ connected rels, scoped, all correctly tombstoned) ────

func TestScopedDelete_NodeWithMultipleRelsAllTombstoned(t *testing.T) {
	t.Parallel()
	bs := newChangeLogStore(t, false)

	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)
	putTestNode(t, bs, 3, 10, nil)
	relA := putTestRel(t, bs, 100, 5, 1, 2)
	relB := putTestRel(t, bs, 101, 5, 3, 1)

	node, _ := bs.GetNode(types.NodeID(1))
	nodeTombstone := node.DeepCopy()
	prevVersion := node.Version()

	relATombstone := relA.DeepCopy()
	relBTombstone := relB.DeepCopy()

	token, err := bs.BeginScopedLog()
	if err != nil {
		t.Fatalf("BeginScopedLog: %v", err)
	}
	if err := bs.DeleteNodeWithHistoryScoped(types.NodeID(1), prevVersion, nodeTombstone, []RelTombstone{
		{ID: relA.ID(), PrevVersion: relA.Version(), Tombstone: relATombstone},
		{ID: relB.ID(), PrevVersion: relB.Version(), Tombstone: relBTombstone},
	}, token); err != nil {
		t.Fatalf("DeleteNodeWithHistoryScoped: %v", err)
	}
	if _, err := bs.CommitScopedLog(token); err != nil {
		t.Fatalf("CommitScopedLog: %v", err)
	}

	if _, err := bs.GetNode(types.NodeID(1)); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("GetNode error = %v, want ErrNodeNotFound", err)
	}
	if _, err := bs.GetRelationship(types.RelID(100)); !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("GetRelationship(100) error = %v, want ErrRelNotFound", err)
	}
	if _, err := bs.GetRelationship(types.RelID(101)); !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("GetRelationship(101) error = %v, want ErrRelNotFound", err)
	}
	if _, err := bs.GetRelVersion(types.RelID(100), relA.Version()); err != nil {
		t.Fatalf("GetRelVersion(100): %v", err)
	}
	if _, err := bs.GetRelVersion(types.RelID(101), relB.Version()); err != nil {
		t.Fatalf("GetRelVersion(101): %v", err)
	}

	recs := drainFeed(t, bs)
	deletes := 0
	var deleteBody storepkg.NodeDeleteBody
	for _, r := range recs {
		if r.Tag == storecontract.ChangeNodeDelete {
			deletes++
			body, err := storepkg.DecodeNodeDelete(r.Payload)
			if err != nil {
				t.Fatalf("DecodeNodeDelete: %v", err)
			}
			deleteBody = body
		}
	}
	if deletes != 1 {
		t.Fatalf("feed has %d NodeDelete records, want exactly 1", deletes)
	}
	if len(deleteBody.RelTombstones) != 2 {
		t.Fatalf("delete record carries %d rel tombstones, want 2", len(deleteBody.RelTombstones))
	}
}

// ─── Concurrent goroutines each with their own scope: no data race ─────────

func TestScopedDelete_ConcurrentGoroutinesNoRace(t *testing.T) {
	bs := newChangeLogStore(t, false)
	const n = 8
	// Seed n distinct nodes up front (sequential — only the delete-with-
	// history + scope machinery below is exercised concurrently).
	for i := 0; i < n; i++ {
		putTestNode(t, bs, int64(1000+i), 10, nil)
	}

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			id := types.NodeID(snowflake.ID(1000 + i))
			current, err := bs.GetNode(id)
			if err != nil {
				t.Errorf("GetNode[%d]: %v", i, err)
				return
			}
			tombstone := current.DeepCopy()
			prevVersion := current.Version()

			token, err := bs.BeginScopedLog()
			if err != nil {
				t.Errorf("BeginScopedLog[%d]: %v", i, err)
				return
			}
			if err := bs.DeleteNodeWithHistoryScoped(id, prevVersion, tombstone, nil, token); err != nil {
				t.Errorf("DeleteNodeWithHistoryScoped[%d]: %v", i, err)
				return
			}
			if _, err := bs.CommitScopedLog(token); err != nil {
				t.Errorf("CommitScopedLog[%d]: %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	recs := drainFeed(t, bs)
	// n eager creates + n scoped-then-committed deletes.
	if len(recs) != 2*n {
		t.Fatalf("feed has %d records, want %d", len(recs), 2*n)
	}
	assertLSNContiguous(t, recs)
}
