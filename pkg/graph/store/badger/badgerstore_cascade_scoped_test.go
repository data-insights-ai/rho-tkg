package badger

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
// badgerstore_history_delete_scoped_test.go's battery for the four Batch E
// doors, PutNodeVersionScoped / ReplaceNodeScoped / PutRelVersionScoped /
// ReplaceRelationshipScoped.

// Compile-time proof the badger store satisfies the BACKLOG 11f Batch E
// capability.
var _ storecontract.ScopedCascadeCapability = (*Store)(nil)

// ─── PutNodeVersionScoped: token == 0 is exactly the unscoped door ─────────

func TestScopedCascade_PutNodeVersionZeroTokenMatchesUnscopedDoor(t *testing.T) {
	t.Parallel()
	bs := newChangeLogStore(t, false)

	n := putTestNode(t, bs, 1, 10, nil)
	hist := n.DeepCopy()

	if err := bs.PutNodeVersionScoped(n.ID(), n.Version(), hist, 0); err != nil {
		t.Fatalf("PutNodeVersionScoped(token=0): %v", err)
	}

	recs := drainFeed(t, bs)
	if got := tagSeq(recs); len(got) != 2 || got[0] != storecontract.ChangeNodePut || got[1] != storecontract.ChangeNodeHistoryVersion {
		t.Fatalf("tags = %v, want [NodePut NodeHistoryVersion]", got)
	}
	assertLSNContiguous(t, recs)

	got, err := bs.GetNodeVersion(n.ID(), n.Version())
	if err != nil {
		t.Fatalf("GetNodeVersion: %v", err)
	}
	if got == nil {
		t.Fatal("expected the history row to exist")
	}
}

// ─── ReplaceNodeScoped: token == 0 is exactly the unscoped door ────────────

func TestScopedCascade_ReplaceNodeZeroTokenMatchesUnscopedDoor(t *testing.T) {
	t.Parallel()
	bs := newChangeLogStore(t, false)

	n := putTestNode(t, bs, 1, 10, nil)
	updated := n.DeepCopy()
	updated.SetVersion(n.Version() + 1)

	if err := bs.ReplaceNodeScoped(updated, 0); err != nil {
		t.Fatalf("ReplaceNodeScoped(token=0): %v", err)
	}

	got, err := bs.GetNode(n.ID())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got.Version() != updated.Version() {
		t.Fatalf("GetNode version = %d, want %d", got.Version(), updated.Version())
	}
}

// ─── An open (uncommitted) scope is invisible to the feed (PutNodeVersion) ──

func TestScopedCascade_PutNodeVersionUncommittedScopeInvisibleToFeed(t *testing.T) {
	t.Parallel()
	bs := newChangeLogStore(t, false)

	n := putTestNode(t, bs, 1, 10, nil)
	hist := n.DeepCopy()

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

	if err := bs.PutNodeVersionScoped(n.ID(), n.Version(), hist, token); err != nil {
		t.Fatalf("PutNodeVersionScoped: %v", err)
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
	if got, err := bs.GetNodeVersion(n.ID(), n.Version()); err != nil || got == nil {
		t.Fatalf("history row must land even though the log record is scoped: %v, %v", got, err)
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
	if len(recs) != 1 || recs[0].Tag != storecontract.ChangeNodeHistoryVersion {
		t.Fatalf("feed after commit = %#v, want one ChangeNodeHistoryVersion record", recs)
	}
}

// ─── DiscardScopedLog drops the buffer and burns no LSN (ReplaceNode) ───────

func TestScopedCascade_ReplaceNodeDiscardEmitsNothingAndBurnsNoLSN(t *testing.T) {
	t.Parallel()
	bs := newChangeLogStore(t, false)

	n1 := putTestNode(t, bs, 1, 10, nil) // eager create, LSN 1
	updated := n1.DeepCopy()
	updated.SetVersion(n1.Version() + 1)

	token, err := bs.BeginScopedLog()
	if err != nil {
		t.Fatalf("BeginScopedLog: %v", err)
	}
	if err := bs.ReplaceNodeScoped(updated, token); err != nil {
		t.Fatalf("ReplaceNodeScoped: %v", err)
	}
	if err := bs.DiscardScopedLog(token); err != nil {
		t.Fatalf("DiscardScopedLog: %v", err)
	}

	putTestNode(t, bs, 2, 10, nil)

	recs := drainFeed(t, bs)
	if len(recs) != 2 {
		t.Fatalf("feed has %d records, want 2 (discarded replace record must never appear)", len(recs))
	}
	assertLSNContiguous(t, recs)

	got, err := bs.GetNode(n1.ID())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got.Version() != updated.Version() {
		t.Fatal("replace must survive discard — only the log record is scoped")
	}
}

// ─── Unknown / retired token fails closed (both call shapes) ───────────────

func TestScopedCascade_UnknownTokenFailsClosed(t *testing.T) {
	t.Parallel()
	bs := newChangeLogStore(t, false)

	n := putTestNode(t, bs, 1, 10, nil)
	hist := n.DeepCopy()
	if err := bs.PutNodeVersionScoped(n.ID(), n.Version(), hist, 999); !errors.Is(err, storecontract.ErrInvalidStoreMutation) {
		t.Fatalf("PutNodeVersionScoped(unknown token) error = %v, want ErrInvalidStoreMutation", err)
	}

	updated := n.DeepCopy()
	updated.SetVersion(n.Version() + 1)
	if err := bs.ReplaceNodeScoped(updated, 999); !errors.Is(err, storecontract.ErrInvalidStoreMutation) {
		t.Fatalf("ReplaceNodeScoped(unknown token) error = %v, want ErrInvalidStoreMutation", err)
	}
}

// ─── Two concurrent scopes never cross-contaminate ──────────────────────────

func TestScopedCascade_ConcurrentScopesDoNotCrossContaminate(t *testing.T) {
	t.Parallel()
	bs := newChangeLogStore(t, false)

	n1 := putTestNode(t, bs, 1, 10, nil)
	n2 := putTestNode(t, bs, 2, 10, nil)

	hist1 := n1.DeepCopy()
	hist2 := n2.DeepCopy()

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

	if err := bs.PutNodeVersionScoped(n1.ID(), n1.Version(), hist1, tokenA); err != nil {
		t.Fatalf("PutNodeVersionScoped A: %v", err)
	}
	if err := bs.PutNodeVersionScoped(n2.ID(), n2.Version(), hist2, tokenB); err != nil {
		t.Fatalf("PutNodeVersionScoped B: %v", err)
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
	if len(recs) != 1 || recs[0].Tag != storecontract.ChangeNodeHistoryVersion {
		t.Fatalf("feed = %v, want [NodeHistoryVersion] (only B's, A discarded)", tagSeq(recs))
	}
}

// ─── PutRelVersionScoped / ReplaceRelationshipScoped mirror the node doors ──

func TestScopedCascade_RelDoors(t *testing.T) {
	t.Parallel()
	bs := newChangeLogStore(t, false)

	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)
	r := putTestRel(t, bs, 100, 5, 1, 2)
	hist := r.DeepCopy()

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
	if err := bs.PutRelVersionScoped(r.ID(), r.Version(), hist, token); err != nil {
		t.Fatalf("PutRelVersionScoped: %v", err)
	}
	updated := r.DeepCopy()
	updated.SetVersion(r.Version() + 1)
	if err := bs.ReplaceRelationshipScoped(updated, token); err != nil {
		t.Fatalf("ReplaceRelationshipScoped: %v", err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	recs, err := bs.ChangeFeed(before, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("feed has %d records before commit, want 0", len(recs))
	}

	if _, err := bs.CommitScopedLog(token); err != nil {
		t.Fatalf("CommitScopedLog: %v", err)
	}
	recs, err = bs.ChangeFeed(before, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if got := tagSeq(recs); len(got) != 2 || got[0] != storecontract.ChangeRelHistoryVersion || got[1] != storecontract.ChangeRelPut {
		t.Fatalf("tags = %v, want [RelHistoryVersion RelPut]", got)
	}

	got, err := bs.GetRelationship(r.ID())
	if err != nil {
		t.Fatalf("GetRelationship: %v", err)
	}
	if got.Version() != updated.Version() {
		t.Fatalf("GetRelationship version = %d, want %d", got.Version(), updated.Version())
	}
}

// ─── Disabled log: the scoped doors are documented no-op fallbacks ─────────

func TestScopedCascade_DisabledByDefault(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t) // ChangeLog not set

	n := putTestNode(t, bs, 1, 10, nil)
	hist := n.DeepCopy()

	token, err := bs.BeginScopedLog()
	if err != nil {
		t.Fatalf("BeginScopedLog: %v", err)
	}
	if token != 0 {
		t.Fatalf("BeginScopedLog token = %d, want 0 (log disabled)", token)
	}

	if err := bs.PutNodeVersionScoped(n.ID(), n.Version(), hist, token); err != nil {
		t.Fatalf("PutNodeVersionScoped: %v", err)
	}
	if got, err := bs.GetNodeVersion(n.ID(), n.Version()); err != nil || got == nil {
		t.Fatalf("history row must still land with a disabled log: %v, %v", got, err)
	}
}

// ─── Concurrent goroutines each with their own scope: no data race ─────────

func TestScopedCascade_ConcurrentGoroutinesNoRace(t *testing.T) {
	t.Parallel()
	bs := newChangeLogStore(t, false)

	const n = 8
	nodes := make([]*types.Node, n)
	for i := 0; i < n; i++ {
		nodes[i] = putTestNode(t, bs, int64(4000+i), 10, nil)
	}

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			hist := nodes[i].DeepCopy()

			token, err := bs.BeginScopedLog()
			if err != nil {
				t.Errorf("BeginScopedLog[%d]: %v", i, err)
				return
			}
			if err := bs.PutNodeVersionScoped(nodes[i].ID(), nodes[i].Version(), hist, token); err != nil {
				t.Errorf("PutNodeVersionScoped[%d]: %v", i, err)
				return
			}
			if _, err := bs.CommitScopedLog(token); err != nil {
				t.Errorf("CommitScopedLog[%d]: %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	recs := drainFeed(t, bs)
	// n eager creates + n scoped-then-committed history-version puts.
	if len(recs) != 2*n {
		t.Fatalf("feed has %d records, want %d", len(recs), 2*n)
	}
}
