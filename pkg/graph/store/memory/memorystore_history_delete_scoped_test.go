package memory

import (
	"errors"
	"sync"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// This file exercises store.ScopedDeleteCapability (BACKLOG 11f Batch C —
// foundation only; see the interface's doc comment in
// pkg/graph/store/changefeed.go). It mirrors
// memorystore_history_scoped_test.go's battery for the two Batch C doors,
// DeleteNodeWithHistoryScoped / DeleteRelWithHistoryScoped.

// Compile-time proof the memory store satisfies the BACKLOG 11f Batch C
// capability.
var _ storecontract.ScopedDeleteCapability = (*Store)(nil)

// ─── token == 0 is exactly the unscoped door ────────────────────────────────

func TestMemoryScopedDelete_ZeroTokenMatchesUnscopedDoor(t *testing.T) {
	ms := New(WithChangeLog())

	if err := ms.PutNode(memNode(1, 10)); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	current, err := ms.GetNode(types.NodeID(1))
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	tombstone := current.DeepCopy()
	prevVersion := current.Version()

	if err := ms.DeleteNodeWithHistoryScoped(types.NodeID(1), prevVersion, tombstone, nil, 0); err != nil {
		t.Fatalf("DeleteNodeWithHistoryScoped(token=0): %v", err)
	}

	recs, err := ms.ChangeFeed(0, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if got := memTags(recs); len(got) != 2 || got[0] != storecontract.ChangeNodePut || got[1] != storecontract.ChangeNodeDelete {
		t.Fatalf("tags = %v, want [NodePut NodeDelete]", got)
	}
	for i, r := range recs {
		if r.LSN != uint64(i+1) {
			t.Fatalf("record[%d].LSN = %d, want %d", i, r.LSN, i+1)
		}
	}

	if _, err := ms.GetNode(types.NodeID(1)); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("GetNode after delete error = %v, want ErrNodeNotFound", err)
	}
	hist, err := ms.GetNodeVersion(types.NodeID(1), prevVersion)
	if err != nil {
		t.Fatalf("GetNodeVersion: %v", err)
	}
	if hist == nil {
		t.Fatal("expected a tombstone history row")
	}
}

// ─── An open (uncommitted) scope is invisible to the feed ──────────────────

func TestMemoryScopedDelete_UncommittedScopeInvisibleToFeed(t *testing.T) {
	ms := New(WithChangeLog())

	if err := ms.PutNode(memNode(1, 10)); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	current, err := ms.GetNode(types.NodeID(1))
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	tombstone := current.DeepCopy()
	prevVersion := current.Version()

	before, err := ms.LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	}

	token, err := ms.BeginScopedLog()
	if err != nil {
		t.Fatalf("BeginScopedLog: %v", err)
	}
	if token == 0 {
		t.Fatal("BeginScopedLog token = 0, want nonzero (log enabled)")
	}

	if err := ms.DeleteNodeWithHistoryScoped(types.NodeID(1), prevVersion, tombstone, nil, token); err != nil {
		t.Fatalf("DeleteNodeWithHistoryScoped: %v", err)
	}

	// Load-bearing: not visible before commit, even though the entity delete
	// + tombstone history row have already landed.
	recs, err := ms.ChangeFeed(before, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("feed has %d records before CommitScopedLog, want 0", len(recs))
	}
	if _, err := ms.GetNode(types.NodeID(1)); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("entity delete must land even though the log record is scoped: error = %v, want ErrNodeNotFound", err)
	}

	maxLSN, err := ms.CommitScopedLog(token)
	if err != nil {
		t.Fatalf("CommitScopedLog: %v", err)
	}
	if maxLSN == 0 {
		t.Fatal("CommitScopedLog maxLSN = 0, want nonzero")
	}

	recs, err = ms.ChangeFeed(before, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 1 || recs[0].Tag != storecontract.ChangeNodeDelete {
		t.Fatalf("feed after commit = %#v, want one ChangeNodeDelete record", recs)
	}
}

// ─── DiscardScopedLog drops the buffer and burns no LSN — a delete record ───
// specifically must leave zero trace in the feed after a discard.

func TestMemoryScopedDelete_DiscardEmitsNothingAndBurnsNoLSN(t *testing.T) {
	ms := New(WithChangeLog())

	if err := ms.PutNode(memNode(1, 10)); err != nil { // eager create, LSN 1
		t.Fatalf("PutNode: %v", err)
	}
	current, err := ms.GetNode(types.NodeID(1))
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	tombstone := current.DeepCopy()
	prevVersion := current.Version()

	token, err := ms.BeginScopedLog()
	if err != nil {
		t.Fatalf("BeginScopedLog: %v", err)
	}
	if err := ms.DeleteNodeWithHistoryScoped(types.NodeID(1), prevVersion, tombstone, nil, token); err != nil {
		t.Fatalf("DeleteNodeWithHistoryScoped: %v", err)
	}
	if err := ms.DiscardScopedLog(token); err != nil {
		t.Fatalf("DiscardScopedLog: %v", err)
	}

	// A second eager write after the discard — its LSN must be contiguous
	// with the first eager record (2), proving the discarded scope burned no
	// LSN.
	if err := ms.PutNode(memNode(2, 10)); err != nil {
		t.Fatalf("PutNode n2: %v", err)
	}

	recs, err := ms.ChangeFeed(0, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 2 {
		t.Fatalf("feed has %d records, want 2 (discarded delete record must never appear)", len(recs))
	}
	if recs[0].LSN != 1 || recs[1].LSN != 2 {
		t.Fatalf("LSNs = %d,%d, want 1,2 (contiguous — discard burned no sequence number)", recs[0].LSN, recs[1].LSN)
	}
	for _, r := range recs {
		if r.Tag == storecontract.ChangeNodeDelete {
			t.Fatalf("discarded scope's delete record appeared in the feed: %+v", r)
		}
	}

	// The entity delete itself is NOT rolled back by a log-scope discard —
	// only the change-log record is scoped, not the mutation.
	if _, err := ms.GetNode(types.NodeID(1)); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("entity delete must survive discard: error = %v, want ErrNodeNotFound", err)
	}
}

// ─── Unknown / retired token fails closed ───────────────────────────────────

func TestMemoryScopedDelete_UnknownTokenFailsClosed(t *testing.T) {
	ms := New(WithChangeLog())

	if err := ms.PutNode(memNode(1, 10)); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	current, err := ms.GetNode(types.NodeID(1))
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	tombstone := current.DeepCopy()
	prevVersion := current.Version()

	if err := ms.DeleteNodeWithHistoryScoped(types.NodeID(1), prevVersion, tombstone, nil, 999); !errors.Is(err, storecontract.ErrInvalidStoreMutation) {
		t.Fatalf("DeleteNodeWithHistoryScoped(unknown token) error = %v, want ErrInvalidStoreMutation", err)
	}
}

// ─── Two concurrent scopes never cross-contaminate ──────────────────────────

func TestMemoryScopedDelete_ConcurrentScopesDoNotCrossContaminate(t *testing.T) {
	ms := New(WithChangeLog())

	if err := ms.PutNode(memNode(1, 10)); err != nil {
		t.Fatalf("PutNode n1: %v", err)
	}
	if err := ms.PutNode(memNode(2, 10)); err != nil {
		t.Fatalf("PutNode n2: %v", err)
	}

	n1, _ := ms.GetNode(types.NodeID(1))
	tomb1 := n1.DeepCopy()
	v1 := n1.Version()

	n2, _ := ms.GetNode(types.NodeID(2))
	tomb2 := n2.DeepCopy()
	v2 := n2.Version()

	before, err := ms.LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	}

	tokenA, err := ms.BeginScopedLog()
	if err != nil {
		t.Fatalf("BeginScopedLog A: %v", err)
	}
	tokenB, err := ms.BeginScopedLog()
	if err != nil {
		t.Fatalf("BeginScopedLog B: %v", err)
	}
	if tokenA == tokenB {
		t.Fatalf("BeginScopedLog returned the same token twice: %d", tokenA)
	}

	if err := ms.DeleteNodeWithHistoryScoped(types.NodeID(1), v1, tomb1, nil, tokenA); err != nil {
		t.Fatalf("DeleteNodeWithHistoryScoped A: %v", err)
	}
	if err := ms.DeleteNodeWithHistoryScoped(types.NodeID(2), v2, tomb2, nil, tokenB); err != nil {
		t.Fatalf("DeleteNodeWithHistoryScoped B: %v", err)
	}

	// Commit B first, discard A — proves routing is keyed by the explicit
	// token argument, not by call order or a shared "active scope" flag.
	if _, err := ms.CommitScopedLog(tokenB); err != nil {
		t.Fatalf("CommitScopedLog B: %v", err)
	}
	if err := ms.DiscardScopedLog(tokenA); err != nil {
		t.Fatalf("DiscardScopedLog A: %v", err)
	}

	recs, err := ms.ChangeFeed(before, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 1 || recs[0].Tag != storecontract.ChangeNodeDelete {
		t.Fatalf("feed = %v, want [NodeDelete] (only B's, A discarded)", memTags(recs))
	}

	if _, err := ms.GetNode(types.NodeID(1)); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("GetNode(1) error = %v, want ErrNodeNotFound", err)
	}
	if _, err := ms.GetNode(types.NodeID(2)); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("GetNode(2) error = %v, want ErrNodeNotFound", err)
	}
}

// ─── DeleteRelWithHistoryScoped mirrors DeleteNodeWithHistoryScoped (rule 2 parity) ───

func TestMemoryScopedDelete_DeleteRelWithHistoryScoped(t *testing.T) {
	ms := New(WithChangeLog())

	if err := ms.PutNode(memNode(1, 10)); err != nil {
		t.Fatalf("PutNode n1: %v", err)
	}
	if err := ms.PutNode(memNode(2, 10)); err != nil {
		t.Fatalf("PutNode n2: %v", err)
	}
	if err := ms.PutRelationship(memRel(100, 5, 1, 2)); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}

	cur, err := ms.GetRelationship(types.RelID(100))
	if err != nil {
		t.Fatalf("GetRelationship: %v", err)
	}
	tombstone := cur.DeepCopy()
	prevVersion := cur.Version()

	before, err := ms.LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	}

	token, err := ms.BeginScopedLog()
	if err != nil {
		t.Fatalf("BeginScopedLog: %v", err)
	}
	if err := ms.DeleteRelWithHistoryScoped(types.RelID(100), prevVersion, tombstone, token); err != nil {
		t.Fatalf("DeleteRelWithHistoryScoped: %v", err)
	}

	recs, err := ms.ChangeFeed(before, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("feed has %d records before CommitScopedLog, want 0", len(recs))
	}
	if _, err := ms.GetRelationship(types.RelID(100)); !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("GetRelationship error = %v, want ErrRelNotFound", err)
	}

	if _, err := ms.CommitScopedLog(token); err != nil {
		t.Fatalf("CommitScopedLog: %v", err)
	}
	recs, err = ms.ChangeFeed(before, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 1 || recs[0].Tag != storecontract.ChangeRelDelete {
		t.Fatalf("feed = %v, want [RelDelete]", memTags(recs))
	}

	hist, err := ms.GetRelVersion(types.RelID(100), prevVersion)
	if err != nil {
		t.Fatalf("GetRelVersion: %v", err)
	}
	if hist == nil {
		t.Fatal("expected a tombstone history row")
	}
}

// ─── Disabled log: the scoped door is a documented no-op fallback ───────────

func TestMemoryScopedDelete_DisabledByDefault(t *testing.T) {
	ms := New() // no WithChangeLog

	if err := ms.PutNode(memNode(1, 10)); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	current, err := ms.GetNode(types.NodeID(1))
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	tombstone := current.DeepCopy()
	prevVersion := current.Version()

	token, err := ms.BeginScopedLog()
	if err != nil {
		t.Fatalf("BeginScopedLog: %v", err)
	}
	if token != 0 {
		t.Fatalf("BeginScopedLog token = %d, want 0 (log disabled)", token)
	}

	if err := ms.DeleteNodeWithHistoryScoped(types.NodeID(1), prevVersion, tombstone, nil, token); err != nil {
		t.Fatalf("DeleteNodeWithHistoryScoped: %v", err)
	}
	if _, err := ms.GetNode(types.NodeID(1)); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("entity delete must still land with a disabled log: error = %v, want ErrNodeNotFound", err)
	}
}

// ─── The relTombstones-cover-every-connected-relationship invariant holds via
// the scoped route (2+ connected rels, scoped, all correctly tombstoned) ────

func TestMemoryScopedDelete_NodeWithMultipleRelsAllTombstoned(t *testing.T) {
	ms := New(WithChangeLog())

	if err := ms.PutNode(memNode(1, 10)); err != nil {
		t.Fatalf("PutNode n1: %v", err)
	}
	if err := ms.PutNode(memNode(2, 10)); err != nil {
		t.Fatalf("PutNode n2: %v", err)
	}
	if err := ms.PutNode(memNode(3, 10)); err != nil {
		t.Fatalf("PutNode n3: %v", err)
	}
	relA := memRel(100, 5, 1, 2)
	if err := ms.PutRelationship(relA); err != nil {
		t.Fatalf("PutRelationship A: %v", err)
	}
	relB := memRel(101, 5, 3, 1)
	if err := ms.PutRelationship(relB); err != nil {
		t.Fatalf("PutRelationship B: %v", err)
	}

	node, err := ms.GetNode(types.NodeID(1))
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	nodeTombstone := node.DeepCopy()
	prevVersion := node.Version()

	gotA, err := ms.GetRelationship(types.RelID(100))
	if err != nil {
		t.Fatalf("GetRelationship A: %v", err)
	}
	gotB, err := ms.GetRelationship(types.RelID(101))
	if err != nil {
		t.Fatalf("GetRelationship B: %v", err)
	}
	relATombstone := gotA.DeepCopy()
	relBTombstone := gotB.DeepCopy()

	token, err := ms.BeginScopedLog()
	if err != nil {
		t.Fatalf("BeginScopedLog: %v", err)
	}
	if err := ms.DeleteNodeWithHistoryScoped(types.NodeID(1), prevVersion, nodeTombstone, []RelTombstone{
		{ID: gotA.ID(), PrevVersion: gotA.Version(), Tombstone: relATombstone},
		{ID: gotB.ID(), PrevVersion: gotB.Version(), Tombstone: relBTombstone},
	}, token); err != nil {
		t.Fatalf("DeleteNodeWithHistoryScoped: %v", err)
	}
	if _, err := ms.CommitScopedLog(token); err != nil {
		t.Fatalf("CommitScopedLog: %v", err)
	}

	if _, err := ms.GetNode(types.NodeID(1)); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("GetNode error = %v, want ErrNodeNotFound", err)
	}
	if _, err := ms.GetRelationship(types.RelID(100)); !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("GetRelationship(100) error = %v, want ErrRelNotFound", err)
	}
	if _, err := ms.GetRelationship(types.RelID(101)); !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("GetRelationship(101) error = %v, want ErrRelNotFound", err)
	}
	if _, err := ms.GetRelVersion(types.RelID(100), gotA.Version()); err != nil {
		t.Fatalf("GetRelVersion(100): %v", err)
	}
	if _, err := ms.GetRelVersion(types.RelID(101), gotB.Version()); err != nil {
		t.Fatalf("GetRelVersion(101): %v", err)
	}
}

// ─── Concurrent goroutines each with their own scope: no data race ─────────

func TestMemoryScopedDelete_ConcurrentGoroutinesNoRace(t *testing.T) {
	ms := New(WithChangeLog())
	const n = 8
	// Seed n distinct nodes up front (sequential — only the delete-with-
	// history + scope machinery below is exercised concurrently).
	for i := 0; i < n; i++ {
		if err := ms.PutNode(memNode(int64(1000+i), 10)); err != nil {
			t.Fatalf("PutNode[%d]: %v", i, err)
		}
	}

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			id := types.NodeID(1000 + i)
			current, err := ms.GetNode(id)
			if err != nil {
				t.Errorf("GetNode[%d]: %v", i, err)
				return
			}
			tombstone := current.DeepCopy()
			prevVersion := current.Version()

			token, err := ms.BeginScopedLog()
			if err != nil {
				t.Errorf("BeginScopedLog[%d]: %v", i, err)
				return
			}
			if err := ms.DeleteNodeWithHistoryScoped(id, prevVersion, tombstone, nil, token); err != nil {
				t.Errorf("DeleteNodeWithHistoryScoped[%d]: %v", i, err)
				return
			}
			if _, err := ms.CommitScopedLog(token); err != nil {
				t.Errorf("CommitScopedLog[%d]: %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	recs, err := ms.ChangeFeed(0, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	// n eager creates + n scoped-then-committed deletes.
	if len(recs) != 2*n {
		t.Fatalf("feed has %d records, want %d", len(recs), 2*n)
	}
}
