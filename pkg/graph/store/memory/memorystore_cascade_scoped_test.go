package memory

import (
	"errors"
	"sync"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// This file exercises store.ScopedCascadeCapability (BACKLOG 11f Batch E —
// foundation only; see the interface's doc comment in
// pkg/graph/store/changefeed.go). It mirrors
// memorystore_history_delete_scoped_test.go's battery for the four Batch E
// doors, PutNodeVersionScoped / ReplaceNodeScoped / PutRelVersionScoped /
// ReplaceRelationshipScoped.

// Compile-time proof the memory store satisfies the BACKLOG 11f Batch E
// capability.
var _ storecontract.ScopedCascadeCapability = (*Store)(nil)

// ─── PutNodeVersionScoped: token == 0 is exactly the unscoped door ─────────

func TestMemoryScopedCascade_PutNodeVersionZeroTokenMatchesUnscopedDoor(t *testing.T) {
	ms := New(WithChangeLog())

	n := memNode(1, 10)
	if err := ms.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	hist := n.DeepCopy()

	if err := ms.PutNodeVersionScoped(n.ID(), n.Version(), hist, 0); err != nil {
		t.Fatalf("PutNodeVersionScoped(token=0): %v", err)
	}

	recs, err := ms.ChangeFeed(0, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if got := memTags(recs); len(got) != 2 || got[0] != storecontract.ChangeNodePut || got[1] != storecontract.ChangeNodeHistoryVersion {
		t.Fatalf("tags = %v, want [NodePut NodeHistoryVersion]", got)
	}

	got, err := ms.GetNodeVersion(n.ID(), n.Version())
	if err != nil {
		t.Fatalf("GetNodeVersion: %v", err)
	}
	if got == nil {
		t.Fatal("expected the history row to exist")
	}
}

// ─── ReplaceNodeScoped: token == 0 is exactly the unscoped door ────────────

func TestMemoryScopedCascade_ReplaceNodeZeroTokenMatchesUnscopedDoor(t *testing.T) {
	ms := New(WithChangeLog())

	n := memNode(1, 10)
	if err := ms.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	updated := n.DeepCopy()
	updated.SetVersion(n.Version() + 1)

	if err := ms.ReplaceNodeScoped(updated, 0); err != nil {
		t.Fatalf("ReplaceNodeScoped(token=0): %v", err)
	}

	got, err := ms.GetNode(n.ID())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got.Version() != updated.Version() {
		t.Fatalf("GetNode version = %d, want %d", got.Version(), updated.Version())
	}
}

// ─── An open (uncommitted) scope is invisible to the feed (PutNodeVersion) ──

func TestMemoryScopedCascade_PutNodeVersionUncommittedScopeInvisibleToFeed(t *testing.T) {
	ms := New(WithChangeLog())

	n := memNode(1, 10)
	if err := ms.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	hist := n.DeepCopy()

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

	if err := ms.PutNodeVersionScoped(n.ID(), n.Version(), hist, token); err != nil {
		t.Fatalf("PutNodeVersionScoped: %v", err)
	}

	recs, err := ms.ChangeFeed(before, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("feed has %d records before CommitScopedLog, want 0", len(recs))
	}
	if got, err := ms.GetNodeVersion(n.ID(), n.Version()); err != nil || got == nil {
		t.Fatalf("history row must land even though the log record is scoped: %v, %v", got, err)
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
	if len(recs) != 1 || recs[0].Tag != storecontract.ChangeNodeHistoryVersion {
		t.Fatalf("feed after commit = %#v, want one ChangeNodeHistoryVersion record", recs)
	}
}

// ─── DiscardScopedLog drops the buffer and burns no LSN (ReplaceNode) ───────

func TestMemoryScopedCascade_ReplaceNodeDiscardEmitsNothingAndBurnsNoLSN(t *testing.T) {
	ms := New(WithChangeLog())

	n1 := memNode(1, 10) // eager create, LSN 1
	if err := ms.PutNode(n1); err != nil {
		t.Fatalf("PutNode n1: %v", err)
	}
	updated := n1.DeepCopy()
	updated.SetVersion(n1.Version() + 1)

	token, err := ms.BeginScopedLog()
	if err != nil {
		t.Fatalf("BeginScopedLog: %v", err)
	}
	if err := ms.ReplaceNodeScoped(updated, token); err != nil {
		t.Fatalf("ReplaceNodeScoped: %v", err)
	}
	if err := ms.DiscardScopedLog(token); err != nil {
		t.Fatalf("DiscardScopedLog: %v", err)
	}

	if err := ms.PutNode(memNode(2, 10)); err != nil {
		t.Fatalf("PutNode n2: %v", err)
	}

	recs, err := ms.ChangeFeed(0, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 2 {
		t.Fatalf("feed has %d records, want 2 (discarded replace record must never appear)", len(recs))
	}
	if recs[0].LSN != 1 || recs[1].LSN != 2 {
		t.Fatalf("LSNs = %d,%d, want 1,2 (contiguous — discard burned no sequence number)", recs[0].LSN, recs[1].LSN)
	}

	got, err := ms.GetNode(n1.ID())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got.Version() != updated.Version() {
		t.Fatal("replace must survive discard — only the log record is scoped")
	}
}

// ─── Unknown / retired token fails closed (both call shapes) ───────────────

func TestMemoryScopedCascade_UnknownTokenFailsClosed(t *testing.T) {
	ms := New(WithChangeLog())

	n := memNode(1, 10)
	if err := ms.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	hist := n.DeepCopy()
	if err := ms.PutNodeVersionScoped(n.ID(), n.Version(), hist, 999); !errors.Is(err, storecontract.ErrInvalidStoreMutation) {
		t.Fatalf("PutNodeVersionScoped(unknown token) error = %v, want ErrInvalidStoreMutation", err)
	}

	updated := n.DeepCopy()
	updated.SetVersion(n.Version() + 1)
	if err := ms.ReplaceNodeScoped(updated, 999); !errors.Is(err, storecontract.ErrInvalidStoreMutation) {
		t.Fatalf("ReplaceNodeScoped(unknown token) error = %v, want ErrInvalidStoreMutation", err)
	}
}

// ─── Two concurrent scopes never cross-contaminate ──────────────────────────

func TestMemoryScopedCascade_ConcurrentScopesDoNotCrossContaminate(t *testing.T) {
	ms := New(WithChangeLog())

	n1 := memNode(1, 10)
	if err := ms.PutNode(n1); err != nil {
		t.Fatalf("PutNode n1: %v", err)
	}
	n2 := memNode(2, 10)
	if err := ms.PutNode(n2); err != nil {
		t.Fatalf("PutNode n2: %v", err)
	}

	hist1 := n1.DeepCopy()
	hist2 := n2.DeepCopy()

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

	if err := ms.PutNodeVersionScoped(n1.ID(), n1.Version(), hist1, tokenA); err != nil {
		t.Fatalf("PutNodeVersionScoped A: %v", err)
	}
	if err := ms.PutNodeVersionScoped(n2.ID(), n2.Version(), hist2, tokenB); err != nil {
		t.Fatalf("PutNodeVersionScoped B: %v", err)
	}

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
	if len(recs) != 1 || recs[0].Tag != storecontract.ChangeNodeHistoryVersion {
		t.Fatalf("feed = %v, want [NodeHistoryVersion] (only B's, A discarded)", memTags(recs))
	}
}

// ─── PutRelVersionScoped / ReplaceRelationshipScoped mirror the node doors ──

func TestMemoryScopedCascade_RelDoors(t *testing.T) {
	ms := New(WithChangeLog())

	if err := ms.PutNode(memNode(1, 10)); err != nil {
		t.Fatalf("PutNode n1: %v", err)
	}
	if err := ms.PutNode(memNode(2, 10)); err != nil {
		t.Fatalf("PutNode n2: %v", err)
	}
	r := memRel(100, 5, 1, 2)
	if err := ms.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}
	hist := r.DeepCopy()

	before, err := ms.LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	}

	token, err := ms.BeginScopedLog()
	if err != nil {
		t.Fatalf("BeginScopedLog: %v", err)
	}
	if err := ms.PutRelVersionScoped(r.ID(), r.Version(), hist, token); err != nil {
		t.Fatalf("PutRelVersionScoped: %v", err)
	}
	updated := r.DeepCopy()
	updated.SetVersion(r.Version() + 1)
	if err := ms.ReplaceRelationshipScoped(updated, token); err != nil {
		t.Fatalf("ReplaceRelationshipScoped: %v", err)
	}

	recs, err := ms.ChangeFeed(before, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("feed has %d records before commit, want 0", len(recs))
	}

	if _, err := ms.CommitScopedLog(token); err != nil {
		t.Fatalf("CommitScopedLog: %v", err)
	}
	recs, err = ms.ChangeFeed(before, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if got := memTags(recs); len(got) != 2 || got[0] != storecontract.ChangeRelHistoryVersion || got[1] != storecontract.ChangeRelPut {
		t.Fatalf("tags = %v, want [RelHistoryVersion RelPut]", got)
	}

	got, err := ms.GetRelationship(r.ID())
	if err != nil {
		t.Fatalf("GetRelationship: %v", err)
	}
	if got.Version() != updated.Version() {
		t.Fatalf("GetRelationship version = %d, want %d", got.Version(), updated.Version())
	}
}

// ─── Disabled log: the scoped doors are documented no-op fallbacks ─────────

func TestMemoryScopedCascade_DisabledByDefault(t *testing.T) {
	ms := New() // no WithChangeLog

	n := memNode(1, 10)
	if err := ms.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	hist := n.DeepCopy()

	token, err := ms.BeginScopedLog()
	if err != nil {
		t.Fatalf("BeginScopedLog: %v", err)
	}
	if token != 0 {
		t.Fatalf("BeginScopedLog token = %d, want 0 (log disabled)", token)
	}

	if err := ms.PutNodeVersionScoped(n.ID(), n.Version(), hist, token); err != nil {
		t.Fatalf("PutNodeVersionScoped: %v", err)
	}
	if got, err := ms.GetNodeVersion(n.ID(), n.Version()); err != nil || got == nil {
		t.Fatalf("history row must still land with a disabled log: %v, %v", got, err)
	}
}

// ─── Concurrent goroutines each with their own scope: no data race ─────────

func TestMemoryScopedCascade_ConcurrentGoroutinesNoRace(t *testing.T) {
	ms := New(WithChangeLog())
	const n = 8
	nodes := make([]*types.Node, n)
	for i := 0; i < n; i++ {
		nodes[i] = memNode(int64(5000+i), 10)
		if err := ms.PutNode(nodes[i]); err != nil {
			t.Fatalf("PutNode[%d]: %v", i, err)
		}
	}

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			hist := nodes[i].DeepCopy()

			token, err := ms.BeginScopedLog()
			if err != nil {
				t.Errorf("BeginScopedLog[%d]: %v", i, err)
				return
			}
			if err := ms.PutNodeVersionScoped(nodes[i].ID(), nodes[i].Version(), hist, token); err != nil {
				t.Errorf("PutNodeVersionScoped[%d]: %v", i, err)
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
	// n eager creates + n scoped-then-committed history-version puts.
	if len(recs) != 2*n {
		t.Fatalf("feed has %d records, want %d", len(recs), 2*n)
	}
}
