package badger

import (
	"errors"
	"sync"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// This file exercises store.ScopedLabelCapability (BACKLOG 11f Batch D —
// foundation only; see the interface's doc comment in
// pkg/graph/store/changefeed.go). It mirrors
// badgerstore_history_delete_scoped_test.go's battery for the two Batch D
// label doors, AddNodeLabelTokenWithHistoryScoped /
// RemoveNodeLabelTokenWithHistoryScoped.

// Compile-time proof the badger store satisfies the BACKLOG 11f Batch D
// capability.
var _ storecontract.ScopedLabelCapability = (*Store)(nil)

// ─── token == 0 is exactly the unscoped door (add) ──────────────────────────

func TestScopedLabel_AddZeroTokenMatchesUnscopedDoor(t *testing.T) {
	t.Parallel()
	bs := newChangeLogStore(t, false)

	n := putTestNode(t, bs, 1, 10, nil)
	prev := n.DeepCopy()
	updated := n.DeepCopy()
	updated.AddLabelTokenRaw(20)
	updated.SetVersion(prev.Version() + 1)

	if err := bs.AddNodeLabelTokenWithHistoryScoped(n.ID(), 20, updated, prev.Version(), prev, 0); err != nil {
		t.Fatalf("AddNodeLabelTokenWithHistoryScoped(token=0): %v", err)
	}

	recs := drainFeed(t, bs)
	if got := tagSeq(recs); len(got) != 2 || got[0] != storecontract.ChangeNodePut || got[1] != storecontract.ChangeNodePut {
		t.Fatalf("tags = %v, want [NodePut NodePut]", got)
	}
	assertLSNContiguous(t, recs)

	got, err := bs.GetNode(n.ID())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if !got.HasLabelTokenRaw(20) {
		t.Fatal("label 20 not present after AddNodeLabelTokenWithHistoryScoped(token=0)")
	}
	hist, err := bs.GetNodeVersion(n.ID(), prev.Version())
	if err != nil {
		t.Fatalf("GetNodeVersion: %v", err)
	}
	if hist == nil || hist.HasLabelTokenRaw(20) {
		t.Fatal("expected a pre-mutation history row without the new label")
	}
}

// ─── token == 0 is exactly the unscoped door (remove) ───────────────────────

func TestScopedLabel_RemoveZeroTokenMatchesUnscopedDoor(t *testing.T) {
	t.Parallel()
	bs := newChangeLogStore(t, false)

	n := putTestNode(t, bs, 1, 10, []uint16{20})
	prev := n.DeepCopy()
	updated := n.DeepCopy()
	updated.RemoveLabelTokenRaw(20)
	updated.SetVersion(prev.Version() + 1)

	if err := bs.RemoveNodeLabelTokenWithHistoryScoped(n.ID(), 20, updated, prev.Version(), prev, 0); err != nil {
		t.Fatalf("RemoveNodeLabelTokenWithHistoryScoped(token=0): %v", err)
	}

	got, err := bs.GetNode(n.ID())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got.HasLabelTokenRaw(20) {
		t.Fatal("label 20 still present after RemoveNodeLabelTokenWithHistoryScoped(token=0)")
	}
	hist, err := bs.GetNodeVersion(n.ID(), prev.Version())
	if err != nil {
		t.Fatalf("GetNodeVersion: %v", err)
	}
	if hist == nil || !hist.HasLabelTokenRaw(20) {
		t.Fatal("expected a pre-mutation history row that still has the removed label")
	}
}

// ─── An open (uncommitted) scope is invisible to the feed ──────────────────

func TestScopedLabel_UncommittedScopeInvisibleToFeed(t *testing.T) {
	t.Parallel()
	bs := newChangeLogStore(t, false)

	n := putTestNode(t, bs, 1, 10, nil)
	prev := n.DeepCopy()
	updated := n.DeepCopy()
	updated.AddLabelTokenRaw(20)
	updated.SetVersion(prev.Version() + 1)

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

	if err := bs.AddNodeLabelTokenWithHistoryScoped(n.ID(), 20, updated, prev.Version(), prev, token); err != nil {
		t.Fatalf("AddNodeLabelTokenWithHistoryScoped: %v", err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	recs, err := bs.ChangeFeed(before, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("feed has %d records before CommitScopedLog, want 0", len(recs))
	}
	got, err := bs.GetNode(n.ID())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if !got.HasLabelTokenRaw(20) {
		t.Fatal("label mutation must land even though the log record is scoped")
	}

	maxLSN, err := bs.CommitScopedLog(token)
	if err != nil {
		t.Fatalf("CommitScopedLog: %v", err)
	}
	if maxLSN == 0 {
		t.Fatal("CommitScopedLog maxLSN = 0, want nonzero")
	}

	recs, err = bs.ChangeFeed(before, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 1 || recs[0].Tag != storecontract.ChangeNodePut {
		t.Fatalf("feed after commit = %#v, want one ChangeNodePut record", recs)
	}
}

// ─── DiscardScopedLog drops the buffer and burns no LSN ─────────────────────

func TestScopedLabel_DiscardEmitsNothingAndBurnsNoLSN(t *testing.T) {
	t.Parallel()
	bs := newChangeLogStore(t, false)

	n1 := putTestNode(t, bs, 1, 10, nil)
	prev := n1.DeepCopy()
	updated := n1.DeepCopy()
	updated.AddLabelTokenRaw(20)
	updated.SetVersion(prev.Version() + 1)

	token, err := bs.BeginScopedLog()
	if err != nil {
		t.Fatalf("BeginScopedLog: %v", err)
	}
	if err := bs.AddNodeLabelTokenWithHistoryScoped(n1.ID(), 20, updated, prev.Version(), prev, token); err != nil {
		t.Fatalf("AddNodeLabelTokenWithHistoryScoped: %v", err)
	}
	if err := bs.DiscardScopedLog(token); err != nil {
		t.Fatalf("DiscardScopedLog: %v", err)
	}

	putTestNode(t, bs, 2, 10, nil)

	recs := drainFeed(t, bs)
	if len(recs) != 2 {
		t.Fatalf("feed has %d records, want 2 (discarded label-add record must never appear)", len(recs))
	}
	assertLSNContiguous(t, recs)
	for _, r := range recs {
		if r.Tag != storecontract.ChangeNodePut {
			continue
		}
	}

	got, err := bs.GetNode(n1.ID())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if !got.HasLabelTokenRaw(20) {
		t.Fatal("label mutation must survive discard")
	}
}

// ─── Unknown / retired token fails closed ───────────────────────────────────

func TestScopedLabel_UnknownTokenFailsClosed(t *testing.T) {
	t.Parallel()
	bs := newChangeLogStore(t, false)

	n := putTestNode(t, bs, 1, 10, nil)
	prev := n.DeepCopy()
	updated := n.DeepCopy()
	updated.AddLabelTokenRaw(20)
	updated.SetVersion(prev.Version() + 1)

	if err := bs.AddNodeLabelTokenWithHistoryScoped(n.ID(), 20, updated, prev.Version(), prev, 999); !errors.Is(err, storecontract.ErrInvalidStoreMutation) {
		t.Fatalf("AddNodeLabelTokenWithHistoryScoped(unknown token) error = %v, want ErrInvalidStoreMutation", err)
	}
}

// ─── Two concurrent scopes never cross-contaminate ──────────────────────────

func TestScopedLabel_ConcurrentScopesDoNotCrossContaminate(t *testing.T) {
	t.Parallel()
	bs := newChangeLogStore(t, false)

	n1 := putTestNode(t, bs, 1, 10, nil)
	n2 := putTestNode(t, bs, 2, 10, nil)

	prev1 := n1.DeepCopy()
	updated1 := n1.DeepCopy()
	updated1.AddLabelTokenRaw(20)
	updated1.SetVersion(prev1.Version() + 1)

	prev2 := n2.DeepCopy()
	updated2 := n2.DeepCopy()
	updated2.AddLabelTokenRaw(20)
	updated2.SetVersion(prev2.Version() + 1)

	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	before, err := bs.LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	}

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

	if err := bs.AddNodeLabelTokenWithHistoryScoped(n1.ID(), 20, updated1, prev1.Version(), prev1, tokenA); err != nil {
		t.Fatalf("AddNodeLabelTokenWithHistoryScoped A: %v", err)
	}
	if err := bs.AddNodeLabelTokenWithHistoryScoped(n2.ID(), 20, updated2, prev2.Version(), prev2, tokenB); err != nil {
		t.Fatalf("AddNodeLabelTokenWithHistoryScoped B: %v", err)
	}

	if _, err := bs.CommitScopedLog(tokenB); err != nil {
		t.Fatalf("CommitScopedLog B: %v", err)
	}
	if err := bs.DiscardScopedLog(tokenA); err != nil {
		t.Fatalf("DiscardScopedLog A: %v", err)
	}

	recs, err := bs.ChangeFeed(before, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 1 || recs[0].Tag != storecontract.ChangeNodePut {
		t.Fatalf("feed = %v, want [NodePut] (only B's, A discarded)", tagSeq(recs))
	}

	g1, err := bs.GetNode(n1.ID())
	if err != nil {
		t.Fatalf("GetNode(1): %v", err)
	}
	if !g1.HasLabelTokenRaw(20) {
		t.Fatal("node 1's label mutation must have landed even though its record was discarded")
	}
	g2, err := bs.GetNode(n2.ID())
	if err != nil {
		t.Fatalf("GetNode(2): %v", err)
	}
	if !g2.HasLabelTokenRaw(20) {
		t.Fatal("node 2's label mutation must have landed")
	}
}

// ─── RemoveNodeLabelTokenWithHistoryScoped mirrors the add door (rule 2 parity) ───

func TestScopedLabel_RemoveNodeLabelTokenWithHistoryScoped(t *testing.T) {
	t.Parallel()
	bs := newChangeLogStore(t, false)

	n := putTestNode(t, bs, 1, 10, []uint16{20})
	prev := n.DeepCopy()
	updated := n.DeepCopy()
	updated.RemoveLabelTokenRaw(20)
	updated.SetVersion(prev.Version() + 1)

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
	if err := bs.RemoveNodeLabelTokenWithHistoryScoped(n.ID(), 20, updated, prev.Version(), prev, token); err != nil {
		t.Fatalf("RemoveNodeLabelTokenWithHistoryScoped: %v", err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	recs, err := bs.ChangeFeed(before, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("feed has %d records before CommitScopedLog, want 0", len(recs))
	}

	if _, err := bs.CommitScopedLog(token); err != nil {
		t.Fatalf("CommitScopedLog: %v", err)
	}
	recs, err = bs.ChangeFeed(before, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 1 || recs[0].Tag != storecontract.ChangeNodePut {
		t.Fatalf("feed = %v, want [NodePut]", tagSeq(recs))
	}

	got, err := bs.GetNode(n.ID())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got.HasLabelTokenRaw(20) {
		t.Fatal("label 20 still present after RemoveNodeLabelTokenWithHistoryScoped")
	}
	hist, err := bs.GetNodeVersion(n.ID(), prev.Version())
	if err != nil {
		t.Fatalf("GetNodeVersion: %v", err)
	}
	if hist == nil || !hist.HasLabelTokenRaw(20) {
		t.Fatal("expected a pre-mutation history row that still has the removed label")
	}
}

// ─── Disabled log: the scoped door is a documented no-op fallback ───────────

func TestScopedLabel_DisabledByDefault(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t) // ChangeLog not set

	n := putTestNode(t, bs, 1, 10, nil)
	prev := n.DeepCopy()
	updated := n.DeepCopy()
	updated.AddLabelTokenRaw(20)
	updated.SetVersion(prev.Version() + 1)

	token, err := bs.BeginScopedLog()
	if err != nil {
		t.Fatalf("BeginScopedLog: %v", err)
	}
	if token != 0 {
		t.Fatalf("BeginScopedLog token = %d, want 0 (log disabled)", token)
	}

	if err := bs.AddNodeLabelTokenWithHistoryScoped(n.ID(), 20, updated, prev.Version(), prev, token); err != nil {
		t.Fatalf("AddNodeLabelTokenWithHistoryScoped: %v", err)
	}
	got, err := bs.GetNode(n.ID())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if !got.HasLabelTokenRaw(20) {
		t.Fatal("label mutation must still land with a disabled log")
	}
}

// ─── Concurrent goroutines each with their own scope: no data race ─────────

func TestScopedLabel_ConcurrentGoroutinesNoRace(t *testing.T) {
	t.Parallel()
	bs := newChangeLogStore(t, false)

	const n = 8
	nodes := make([]*types.Node, n)
	for i := 0; i < n; i++ {
		nodes[i] = putTestNode(t, bs, int64(3000+i), 10, nil)
	}

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			id := nodes[i].ID()
			current, err := bs.GetNode(id)
			if err != nil {
				t.Errorf("GetNode[%d]: %v", i, err)
				return
			}
			prev := current.DeepCopy()
			updated := current.DeepCopy()
			updated.AddLabelTokenRaw(20)
			updated.SetVersion(prev.Version() + 1)

			token, err := bs.BeginScopedLog()
			if err != nil {
				t.Errorf("BeginScopedLog[%d]: %v", i, err)
				return
			}
			if err := bs.AddNodeLabelTokenWithHistoryScoped(id, 20, updated, prev.Version(), prev, token); err != nil {
				t.Errorf("AddNodeLabelTokenWithHistoryScoped[%d]: %v", i, err)
				return
			}
			if _, err := bs.CommitScopedLog(token); err != nil {
				t.Errorf("CommitScopedLog[%d]: %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	recs := drainFeed(t, bs)
	// n eager creates + n scoped-then-committed label adds.
	if len(recs) != 2*n {
		t.Fatalf("feed has %d records, want %d", len(recs), 2*n)
	}
}
