package memory

import (
	"errors"
	"sync"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// This file exercises store.ScopedReplaceCapability (BACKLOG 11f Batch B —
// foundation only; see the interface's doc comment in
// pkg/graph/store/changefeed.go). It mirrors
// memorystore_changelog_scoped_test.go's battery for the two Batch B doors,
// ReplaceNodeWithHistoryScoped / ReplaceRelWithHistoryScoped.

// Compile-time proof the memory store satisfies the BACKLOG 11f Batch B
// capability.
var _ storecontract.ScopedReplaceCapability = (*Store)(nil)

// ─── token == 0 is exactly the unscoped door ────────────────────────────────

func TestMemoryScopedReplace_ZeroTokenMatchesUnscopedDoor(t *testing.T) {
	ms := New(WithChangeLog())

	if err := ms.PutNode(memNode(1, 10)); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	current, err := ms.GetNode(types.NodeID(1))
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	prevState := current.DeepCopy()
	prevVersion := current.Version()
	_ = current.SetProperty("name", "Bob")
	current.SetVersion(prevVersion + 1)

	if err := ms.ReplaceNodeWithHistoryScoped(current, prevVersion, prevState, 0); err != nil {
		t.Fatalf("ReplaceNodeWithHistoryScoped(token=0): %v", err)
	}

	recs, err := ms.ChangeFeed(0, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if got := memTags(recs); len(got) != 2 || got[0] != storecontract.ChangeNodePut || got[1] != storecontract.ChangeNodePut {
		t.Fatalf("tags = %v, want [NodePut NodePut]", got)
	}
	for i, r := range recs {
		if r.LSN != uint64(i+1) {
			t.Fatalf("record[%d].LSN = %d, want %d", i, r.LSN, i+1)
		}
	}

	got, err := ms.GetNode(types.NodeID(1))
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got.PropertiesMap()["name"] != "Bob" {
		t.Fatalf("got name=%v, want Bob", got.PropertiesMap()["name"])
	}
	hist, err := ms.GetNodeVersion(types.NodeID(1), prevVersion)
	if err != nil {
		t.Fatalf("GetNodeVersion: %v", err)
	}
	if _, ok := hist.PropertiesMap()["name"]; ok {
		t.Fatalf("history version must not carry the new property: got %v", hist.PropertiesMap())
	}
}

// ─── An open (uncommitted) scope is invisible to the feed ──────────────────

func TestMemoryScopedReplace_UncommittedScopeInvisibleToFeed(t *testing.T) {
	ms := New(WithChangeLog())

	if err := ms.PutNode(memNode(1, 10)); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	current, err := ms.GetNode(types.NodeID(1))
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	prevState := current.DeepCopy()
	prevVersion := current.Version()
	_ = current.SetProperty("name", "Bob")
	current.SetVersion(prevVersion + 1)

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

	if err := ms.ReplaceNodeWithHistoryScoped(current, prevVersion, prevState, token); err != nil {
		t.Fatalf("ReplaceNodeWithHistoryScoped: %v", err)
	}

	// Load-bearing: not visible before commit, even though the entity write
	// (current row + history row) has already landed.
	recs, err := ms.ChangeFeed(before, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("feed has %d records before CommitScopedLog, want 0", len(recs))
	}
	got, err := ms.GetNode(types.NodeID(1))
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got.PropertiesMap()["name"] != "Bob" {
		t.Fatalf("entity write must land even though the log record is scoped: got name=%v, want Bob", got.PropertiesMap()["name"])
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

func TestMemoryScopedReplace_DiscardEmitsNothingAndBurnsNoLSN(t *testing.T) {
	ms := New(WithChangeLog())

	if err := ms.PutNode(memNode(1, 10)); err != nil { // eager create, LSN 1
		t.Fatalf("PutNode: %v", err)
	}
	current, err := ms.GetNode(types.NodeID(1))
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	prevState := current.DeepCopy()
	prevVersion := current.Version()
	_ = current.SetProperty("name", "Bob")
	current.SetVersion(prevVersion + 1)

	token, err := ms.BeginScopedLog()
	if err != nil {
		t.Fatalf("BeginScopedLog: %v", err)
	}
	if err := ms.ReplaceNodeWithHistoryScoped(current, prevVersion, prevState, token); err != nil {
		t.Fatalf("ReplaceNodeWithHistoryScoped: %v", err)
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
		t.Fatalf("feed has %d records, want 2 (discarded scope's record must never appear)", len(recs))
	}
	if recs[0].LSN != 1 || recs[1].LSN != 2 {
		t.Fatalf("LSNs = %d,%d, want 1,2 (contiguous — discard burned no sequence number)", recs[0].LSN, recs[1].LSN)
	}

	// The entity write itself is NOT rolled back by a log-scope discard —
	// only the change-log record is scoped, not the mutation.
	got, err := ms.GetNode(types.NodeID(1))
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got.PropertiesMap()["name"] != "Bob" {
		t.Fatalf("entity write must survive discard: got name=%v, want Bob", got.PropertiesMap()["name"])
	}
}

// ─── Unknown / retired token fails closed ───────────────────────────────────

func TestMemoryScopedReplace_UnknownTokenFailsClosed(t *testing.T) {
	ms := New(WithChangeLog())

	if err := ms.PutNode(memNode(1, 10)); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	current, err := ms.GetNode(types.NodeID(1))
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	prevState := current.DeepCopy()
	prevVersion := current.Version()
	_ = current.SetProperty("name", "Bob")
	current.SetVersion(prevVersion + 1)

	if err := ms.ReplaceNodeWithHistoryScoped(current, prevVersion, prevState, 999); !errors.Is(err, storecontract.ErrInvalidStoreMutation) {
		t.Fatalf("ReplaceNodeWithHistoryScoped(unknown token) error = %v, want ErrInvalidStoreMutation", err)
	}
}

// ─── Two concurrent scopes never cross-contaminate ──────────────────────────

func TestMemoryScopedReplace_ConcurrentScopesDoNotCrossContaminate(t *testing.T) {
	ms := New(WithChangeLog())

	if err := ms.PutNode(memNode(1, 10)); err != nil {
		t.Fatalf("PutNode n1: %v", err)
	}
	if err := ms.PutNode(memNode(2, 10)); err != nil {
		t.Fatalf("PutNode n2: %v", err)
	}

	n1, _ := ms.GetNode(types.NodeID(1))
	prev1 := n1.DeepCopy()
	v1 := n1.Version()
	_ = n1.SetProperty("who", "A")
	n1.SetVersion(v1 + 1)

	n2, _ := ms.GetNode(types.NodeID(2))
	prev2 := n2.DeepCopy()
	v2 := n2.Version()
	_ = n2.SetProperty("who", "B")
	n2.SetVersion(v2 + 1)

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

	if err := ms.ReplaceNodeWithHistoryScoped(n1, v1, prev1, tokenA); err != nil {
		t.Fatalf("ReplaceNodeWithHistoryScoped A: %v", err)
	}
	if err := ms.ReplaceNodeWithHistoryScoped(n2, v2, prev2, tokenB); err != nil {
		t.Fatalf("ReplaceNodeWithHistoryScoped B: %v", err)
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
}

// ─── ReplaceRelWithHistoryScoped mirrors ReplaceNodeWithHistoryScoped (rule 2 parity) ───

func TestMemoryScopedReplace_ReplaceRelWithHistoryScoped(t *testing.T) {
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
	prev := cur.DeepCopy()
	prevVersion := cur.Version()
	_ = cur.SetProperty("w", int64(9))
	cur.SetVersion(prevVersion + 1)

	before, err := ms.LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	}

	token, err := ms.BeginScopedLog()
	if err != nil {
		t.Fatalf("BeginScopedLog: %v", err)
	}
	if err := ms.ReplaceRelWithHistoryScoped(cur, prevVersion, prev, token); err != nil {
		t.Fatalf("ReplaceRelWithHistoryScoped: %v", err)
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
	if len(recs) != 1 || recs[0].Tag != storecontract.ChangeRelPut {
		t.Fatalf("feed = %v, want [RelPut]", memTags(recs))
	}

	// Verify the entity + history rows themselves landed correctly (update-
	// path-specific: prevVersion/prevState correctly persisted via the
	// scoped route too).
	got, err := ms.GetRelationship(types.RelID(100))
	if err != nil {
		t.Fatalf("GetRelationship: %v", err)
	}
	if got.PropertiesMap()["w"] != int64(9) {
		t.Fatalf("got w=%v, want 9", got.PropertiesMap()["w"])
	}
	hist, err := ms.GetRelVersion(types.RelID(100), prevVersion)
	if err != nil {
		t.Fatalf("GetRelVersion: %v", err)
	}
	if _, ok := hist.PropertiesMap()["w"]; ok {
		t.Fatalf("history version must not carry the new property: got %v", hist.PropertiesMap())
	}
}

// ─── Disabled log: the scoped door is a documented no-op fallback ───────────

func TestMemoryScopedReplace_DisabledByDefault(t *testing.T) {
	ms := New() // no WithChangeLog

	if err := ms.PutNode(memNode(1, 10)); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	current, err := ms.GetNode(types.NodeID(1))
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	prevState := current.DeepCopy()
	prevVersion := current.Version()
	_ = current.SetProperty("name", "Bob")
	current.SetVersion(prevVersion + 1)

	token, err := ms.BeginScopedLog()
	if err != nil {
		t.Fatalf("BeginScopedLog: %v", err)
	}
	if token != 0 {
		t.Fatalf("BeginScopedLog token = %d, want 0 (log disabled)", token)
	}

	if err := ms.ReplaceNodeWithHistoryScoped(current, prevVersion, prevState, token); err != nil {
		t.Fatalf("ReplaceNodeWithHistoryScoped: %v", err)
	}
	got, err := ms.GetNode(types.NodeID(1))
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got.PropertiesMap()["name"] != "Bob" {
		t.Fatalf("entity write must still land with a disabled log: got name=%v, want Bob", got.PropertiesMap()["name"])
	}
}

// ─── Concurrent goroutines each with their own scope: no data race ─────────

func TestMemoryScopedReplace_ConcurrentGoroutinesNoRace(t *testing.T) {
	ms := New(WithChangeLog())
	const n = 8
	// Seed n distinct nodes up front (sequential — only the replace-with-
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
			prevState := current.DeepCopy()
			prevVersion := current.Version()
			if err := current.SetProperty("touched", true); err != nil {
				t.Errorf("SetProperty[%d]: %v", i, err)
				return
			}
			current.SetVersion(prevVersion + 1)

			token, err := ms.BeginScopedLog()
			if err != nil {
				t.Errorf("BeginScopedLog[%d]: %v", i, err)
				return
			}
			if err := ms.ReplaceNodeWithHistoryScoped(current, prevVersion, prevState, token); err != nil {
				t.Errorf("ReplaceNodeWithHistoryScoped[%d]: %v", i, err)
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
	// n eager creates + n scoped-then-committed replaces.
	if len(recs) != 2*n {
		t.Fatalf("feed has %d records, want %d", len(recs), 2*n)
	}
}
