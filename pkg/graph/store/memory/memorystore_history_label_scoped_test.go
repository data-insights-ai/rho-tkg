package memory

import (
	"errors"
	"sync"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// This file exercises store.ScopedLabelCapability (BACKLOG 11f Batch D —
// foundation only; see the interface's doc comment in
// pkg/graph/store/changefeed.go). It mirrors
// memorystore_history_delete_scoped_test.go's battery for the two Batch D
// label doors, AddNodeLabelTokenWithHistoryScoped /
// RemoveNodeLabelTokenWithHistoryScoped.

// Compile-time proof the memory store satisfies the BACKLOG 11f Batch D
// capability.
var _ storecontract.ScopedLabelCapability = (*Store)(nil)

func labelTestNode(id int64, primary uint16, extra ...uint16) *types.Node {
	return types.NewNode(types.NodeID(snowflake.ID(id)), primary, extra)
}

// ─── token == 0 is exactly the unscoped door (add) ──────────────────────────

func TestMemoryScopedLabel_AddZeroTokenMatchesUnscopedDoor(t *testing.T) {
	ms := New(WithChangeLog())

	n := labelTestNode(1, 10)
	if err := ms.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	prev := n.DeepCopy()
	updated := n.DeepCopy()
	updated.AddLabelTokenRaw(20)
	updated.SetVersion(prev.Version() + 1)

	if err := ms.AddNodeLabelTokenWithHistoryScoped(n.ID(), 20, updated, prev.Version(), prev, 0); err != nil {
		t.Fatalf("AddNodeLabelTokenWithHistoryScoped(token=0): %v", err)
	}

	recs, err := ms.ChangeFeed(0, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if got := memTags(recs); len(got) != 2 || got[0] != storecontract.ChangeNodePut || got[1] != storecontract.ChangeNodePut {
		t.Fatalf("tags = %v, want [NodePut NodePut]", got)
	}

	got, err := ms.GetNode(n.ID())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if !got.HasLabelTokenRaw(20) {
		t.Fatal("label 20 not present after AddNodeLabelTokenWithHistoryScoped(token=0)")
	}
	hist, err := ms.GetNodeVersion(n.ID(), prev.Version())
	if err != nil {
		t.Fatalf("GetNodeVersion: %v", err)
	}
	if hist == nil || hist.HasLabelTokenRaw(20) {
		t.Fatal("expected a pre-mutation history row without the new label")
	}
}

// ─── token == 0 is exactly the unscoped door (remove) ───────────────────────

func TestMemoryScopedLabel_RemoveZeroTokenMatchesUnscopedDoor(t *testing.T) {
	ms := New(WithChangeLog())

	n := labelTestNode(1, 10, 20)
	if err := ms.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	prev := n.DeepCopy()
	updated := n.DeepCopy()
	updated.RemoveLabelTokenRaw(20)
	updated.SetVersion(prev.Version() + 1)

	if err := ms.RemoveNodeLabelTokenWithHistoryScoped(n.ID(), 20, updated, prev.Version(), prev, 0); err != nil {
		t.Fatalf("RemoveNodeLabelTokenWithHistoryScoped(token=0): %v", err)
	}

	got, err := ms.GetNode(n.ID())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got.HasLabelTokenRaw(20) {
		t.Fatal("label 20 still present after RemoveNodeLabelTokenWithHistoryScoped(token=0)")
	}
	hist, err := ms.GetNodeVersion(n.ID(), prev.Version())
	if err != nil {
		t.Fatalf("GetNodeVersion: %v", err)
	}
	if hist == nil || !hist.HasLabelTokenRaw(20) {
		t.Fatal("expected a pre-mutation history row that still has the removed label")
	}
}

// ─── An open (uncommitted) scope is invisible to the feed ──────────────────

func TestMemoryScopedLabel_UncommittedScopeInvisibleToFeed(t *testing.T) {
	ms := New(WithChangeLog())

	n := labelTestNode(1, 10)
	if err := ms.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	prev := n.DeepCopy()
	updated := n.DeepCopy()
	updated.AddLabelTokenRaw(20)
	updated.SetVersion(prev.Version() + 1)

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

	if err := ms.AddNodeLabelTokenWithHistoryScoped(n.ID(), 20, updated, prev.Version(), prev, token); err != nil {
		t.Fatalf("AddNodeLabelTokenWithHistoryScoped: %v", err)
	}

	// Load-bearing: not visible before commit, even though the label
	// mutation + history row have already landed.
	recs, err := ms.ChangeFeed(before, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("feed has %d records before CommitScopedLog, want 0", len(recs))
	}
	got, err := ms.GetNode(n.ID())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if !got.HasLabelTokenRaw(20) {
		t.Fatal("label mutation must land even though the log record is scoped")
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
	if len(recs) != 1 || recs[0].Tag != storecontract.ChangeNodePut {
		t.Fatalf("feed after commit = %#v, want one ChangeNodePut record", recs)
	}
}

// ─── DiscardScopedLog drops the buffer and burns no LSN ─────────────────────

func TestMemoryScopedLabel_DiscardEmitsNothingAndBurnsNoLSN(t *testing.T) {
	ms := New(WithChangeLog())

	n1 := labelTestNode(1, 10) // eager create, LSN 1
	if err := ms.PutNode(n1); err != nil {
		t.Fatalf("PutNode n1: %v", err)
	}
	prev := n1.DeepCopy()
	updated := n1.DeepCopy()
	updated.AddLabelTokenRaw(20)
	updated.SetVersion(prev.Version() + 1)

	token, err := ms.BeginScopedLog()
	if err != nil {
		t.Fatalf("BeginScopedLog: %v", err)
	}
	if err := ms.AddNodeLabelTokenWithHistoryScoped(n1.ID(), 20, updated, prev.Version(), prev, token); err != nil {
		t.Fatalf("AddNodeLabelTokenWithHistoryScoped: %v", err)
	}
	if err := ms.DiscardScopedLog(token); err != nil {
		t.Fatalf("DiscardScopedLog: %v", err)
	}

	// A second eager write after the discard — its LSN must be contiguous
	// with the first eager record (2), proving the discarded scope burned no
	// LSN.
	if err := ms.PutNode(labelTestNode(2, 10)); err != nil {
		t.Fatalf("PutNode n2: %v", err)
	}

	recs, err := ms.ChangeFeed(0, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 2 {
		t.Fatalf("feed has %d records, want 2 (discarded label-add record must never appear)", len(recs))
	}
	if recs[0].LSN != 1 || recs[1].LSN != 2 {
		t.Fatalf("LSNs = %d,%d, want 1,2 (contiguous — discard burned no sequence number)", recs[0].LSN, recs[1].LSN)
	}

	// The label mutation itself is NOT rolled back by a log-scope discard —
	// only the change-log record is scoped, not the mutation.
	got, err := ms.GetNode(n1.ID())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if !got.HasLabelTokenRaw(20) {
		t.Fatal("label mutation must survive discard")
	}
}

// ─── Unknown / retired token fails closed ───────────────────────────────────

func TestMemoryScopedLabel_UnknownTokenFailsClosed(t *testing.T) {
	ms := New(WithChangeLog())

	n := labelTestNode(1, 10)
	if err := ms.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	prev := n.DeepCopy()
	updated := n.DeepCopy()
	updated.AddLabelTokenRaw(20)
	updated.SetVersion(prev.Version() + 1)

	if err := ms.AddNodeLabelTokenWithHistoryScoped(n.ID(), 20, updated, prev.Version(), prev, 999); !errors.Is(err, storecontract.ErrInvalidStoreMutation) {
		t.Fatalf("AddNodeLabelTokenWithHistoryScoped(unknown token) error = %v, want ErrInvalidStoreMutation", err)
	}
}

// ─── Two concurrent scopes never cross-contaminate ──────────────────────────

func TestMemoryScopedLabel_ConcurrentScopesDoNotCrossContaminate(t *testing.T) {
	ms := New(WithChangeLog())

	n1 := labelTestNode(1, 10)
	if err := ms.PutNode(n1); err != nil {
		t.Fatalf("PutNode n1: %v", err)
	}
	n2 := labelTestNode(2, 10)
	if err := ms.PutNode(n2); err != nil {
		t.Fatalf("PutNode n2: %v", err)
	}

	prev1 := n1.DeepCopy()
	updated1 := n1.DeepCopy()
	updated1.AddLabelTokenRaw(20)
	updated1.SetVersion(prev1.Version() + 1)

	prev2 := n2.DeepCopy()
	updated2 := n2.DeepCopy()
	updated2.AddLabelTokenRaw(20)
	updated2.SetVersion(prev2.Version() + 1)

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

	if err := ms.AddNodeLabelTokenWithHistoryScoped(n1.ID(), 20, updated1, prev1.Version(), prev1, tokenA); err != nil {
		t.Fatalf("AddNodeLabelTokenWithHistoryScoped A: %v", err)
	}
	if err := ms.AddNodeLabelTokenWithHistoryScoped(n2.ID(), 20, updated2, prev2.Version(), prev2, tokenB); err != nil {
		t.Fatalf("AddNodeLabelTokenWithHistoryScoped B: %v", err)
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
	if len(recs) != 1 || recs[0].Tag != storecontract.ChangeNodePut {
		t.Fatalf("feed = %v, want [NodePut] (only B's, A discarded)", memTags(recs))
	}

	g1, err := ms.GetNode(n1.ID())
	if err != nil {
		t.Fatalf("GetNode(1): %v", err)
	}
	if !g1.HasLabelTokenRaw(20) {
		t.Fatal("node 1's label mutation must have landed even though its record was discarded")
	}
	g2, err := ms.GetNode(n2.ID())
	if err != nil {
		t.Fatalf("GetNode(2): %v", err)
	}
	if !g2.HasLabelTokenRaw(20) {
		t.Fatal("node 2's label mutation must have landed")
	}
}

// ─── RemoveNodeLabelTokenWithHistoryScoped mirrors the add door (rule 2 parity) ───

func TestMemoryScopedLabel_RemoveNodeLabelTokenWithHistoryScoped(t *testing.T) {
	ms := New(WithChangeLog())

	n := labelTestNode(1, 10, 20)
	if err := ms.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	prev := n.DeepCopy()
	updated := n.DeepCopy()
	updated.RemoveLabelTokenRaw(20)
	updated.SetVersion(prev.Version() + 1)

	before, err := ms.LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	}

	token, err := ms.BeginScopedLog()
	if err != nil {
		t.Fatalf("BeginScopedLog: %v", err)
	}
	if err := ms.RemoveNodeLabelTokenWithHistoryScoped(n.ID(), 20, updated, prev.Version(), prev, token); err != nil {
		t.Fatalf("RemoveNodeLabelTokenWithHistoryScoped: %v", err)
	}

	recs, err := ms.ChangeFeed(before, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("feed has %d records before CommitScopedLog, want 0", len(recs))
	}

	if _, err := ms.CommitScopedLog(token); err != nil {
		t.Fatalf("CommitScopedLog: %v", err)
	}
	recs, err = ms.ChangeFeed(before, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 1 || recs[0].Tag != storecontract.ChangeNodePut {
		t.Fatalf("feed = %v, want [NodePut]", memTags(recs))
	}

	got, err := ms.GetNode(n.ID())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got.HasLabelTokenRaw(20) {
		t.Fatal("label 20 still present after RemoveNodeLabelTokenWithHistoryScoped")
	}
	hist, err := ms.GetNodeVersion(n.ID(), prev.Version())
	if err != nil {
		t.Fatalf("GetNodeVersion: %v", err)
	}
	if hist == nil || !hist.HasLabelTokenRaw(20) {
		t.Fatal("expected a pre-mutation history row that still has the removed label")
	}
}

// ─── Disabled log: the scoped door is a documented no-op fallback ───────────

func TestMemoryScopedLabel_DisabledByDefault(t *testing.T) {
	ms := New() // no WithChangeLog

	n := labelTestNode(1, 10)
	if err := ms.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	prev := n.DeepCopy()
	updated := n.DeepCopy()
	updated.AddLabelTokenRaw(20)
	updated.SetVersion(prev.Version() + 1)

	token, err := ms.BeginScopedLog()
	if err != nil {
		t.Fatalf("BeginScopedLog: %v", err)
	}
	if token != 0 {
		t.Fatalf("BeginScopedLog token = %d, want 0 (log disabled)", token)
	}

	if err := ms.AddNodeLabelTokenWithHistoryScoped(n.ID(), 20, updated, prev.Version(), prev, token); err != nil {
		t.Fatalf("AddNodeLabelTokenWithHistoryScoped: %v", err)
	}
	got, err := ms.GetNode(n.ID())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if !got.HasLabelTokenRaw(20) {
		t.Fatal("label mutation must still land with a disabled log")
	}
}

// ─── Legacy single-scope mechanism unaffected ───────────────────────────────

func TestMemoryScopedLabel_LegacyMechanismUnaffected(t *testing.T) {
	ms := New(WithChangeLog())

	n := labelTestNode(1, 10)
	if err := ms.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	prev := n.DeepCopy()
	updated := n.DeepCopy()
	updated.AddLabelTokenRaw(20)
	updated.SetVersion(prev.Version() + 1)

	before, err := ms.LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	}

	// The unscoped door (token 0) still respects the legacy single-scope
	// TxChangeLogScope mechanism exactly as before Batch D. BeginLogScope
	// alone only allocates the buffer — SetLogDivert(true) is what the core
	// calls (under its exclusive write lock) to actually start diverting
	// records, so both are needed to reproduce the legacy tx path here.
	if err := ms.BeginLogScope(); err != nil {
		t.Fatalf("BeginLogScope: %v", err)
	}
	ms.SetLogDivert(true)
	if err := ms.AddNodeLabelTokenWithHistory(n.ID(), 20, updated, prev.Version(), prev); err != nil {
		t.Fatalf("AddNodeLabelTokenWithHistory: %v", err)
	}
	recs, err := ms.ChangeFeed(before, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("feed has %d records while legacy scope is open, want 0", len(recs))
	}
	if _, err := ms.CommitLogScope(); err != nil {
		t.Fatalf("CommitLogScope: %v", err)
	}
	recs, err = ms.ChangeFeed(before, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 1 || recs[0].Tag != storecontract.ChangeNodePut {
		t.Fatalf("feed after legacy commit = %v, want [NodePut]", memTags(recs))
	}
}

// ─── Concurrent goroutines each with their own scope: no data race ─────────

func TestMemoryScopedLabel_ConcurrentGoroutinesNoRace(t *testing.T) {
	ms := New(WithChangeLog())
	const n = 8
	nodes := make([]*types.Node, n)
	for i := 0; i < n; i++ {
		nodes[i] = labelTestNode(int64(2000+i), 10)
		if err := ms.PutNode(nodes[i]); err != nil {
			t.Fatalf("PutNode[%d]: %v", i, err)
		}
	}

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			id := nodes[i].ID()
			current, err := ms.GetNode(id)
			if err != nil {
				t.Errorf("GetNode[%d]: %v", i, err)
				return
			}
			prev := current.DeepCopy()
			updated := current.DeepCopy()
			updated.AddLabelTokenRaw(20)
			updated.SetVersion(prev.Version() + 1)

			token, err := ms.BeginScopedLog()
			if err != nil {
				t.Errorf("BeginScopedLog[%d]: %v", i, err)
				return
			}
			if err := ms.AddNodeLabelTokenWithHistoryScoped(id, 20, updated, prev.Version(), prev, token); err != nil {
				t.Errorf("AddNodeLabelTokenWithHistoryScoped[%d]: %v", i, err)
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
	// n eager creates + n scoped-then-committed label adds.
	if len(recs) != 2*n {
		t.Fatalf("feed has %d records, want %d", len(recs), 2*n)
	}
}
