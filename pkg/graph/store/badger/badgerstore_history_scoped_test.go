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

// This file exercises store.ScopedReplaceCapability (BACKLOG 11f Batch B —
// foundation only; see the interface's doc comment in
// pkg/graph/store/changefeed.go). It mirrors
// badgerstore_changelog_scoped_test.go's battery for the two Batch B doors,
// ReplaceNodeWithHistoryScoped / ReplaceRelWithHistoryScoped.

// Compile-time proof the badger store satisfies the BACKLOG 11f Batch B
// capability.
var _ storecontract.ScopedReplaceCapability = (*Store)(nil)

// ─── token == 0 is exactly the unscoped door ────────────────────────────────

func TestScopedReplace_ZeroTokenMatchesUnscopedDoor(t *testing.T) {
	t.Parallel()
	bs := newChangeLogStore(t, false)

	putTestNode(t, bs, 1, 10, nil)
	current, _ := bs.GetNode(types.NodeID(1))
	prevState := current.DeepCopy()
	prevVersion := current.Version()
	_ = current.SetProperty("name", "Bob")
	current.SetVersion(prevVersion + 1)

	if err := bs.ReplaceNodeWithHistoryScoped(current, prevVersion, prevState, 0); err != nil {
		t.Fatalf("ReplaceNodeWithHistoryScoped(token=0): %v", err)
	}

	recs := drainFeed(t, bs)
	// [create v0, replace-with-history v1] — both eager, since token==0.
	if got := tagSeq(recs); len(got) != 2 || got[0] != storecontract.ChangeNodePut || got[1] != storecontract.ChangeNodePut {
		t.Fatalf("tags = %v, want [NodePut NodePut]", got)
	}
	assertLSNContiguous(t, recs)

	got, err := bs.GetNode(types.NodeID(1))
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got.PropertiesMap()["name"] != "Bob" {
		t.Fatalf("got name=%v, want Bob", got.PropertiesMap()["name"])
	}
	hist, err := bs.GetNodeVersion(types.NodeID(1), prevVersion)
	if err != nil {
		t.Fatalf("GetNodeVersion: %v", err)
	}
	if _, ok := hist.PropertiesMap()["name"]; ok {
		t.Fatalf("history should not carry name=Bob's key pre-set; got %v", hist.PropertiesMap())
	}
}

// ─── An open (uncommitted) scope is invisible to the feed ──────────────────

func TestScopedReplace_UncommittedScopeInvisibleToFeed(t *testing.T) {
	t.Parallel()
	bs := newChangeLogStore(t, false)

	putTestNode(t, bs, 1, 10, nil)
	current, _ := bs.GetNode(types.NodeID(1))
	prevState := current.DeepCopy()
	prevVersion := current.Version()
	_ = current.SetProperty("name", "Bob")
	current.SetVersion(prevVersion + 1)

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

	if err := bs.ReplaceNodeWithHistoryScoped(current, prevVersion, prevState, token); err != nil {
		t.Fatalf("ReplaceNodeWithHistoryScoped: %v", err)
	}

	// Load-bearing: the record must NOT be visible before commit, even though
	// the entity write (current row + history row) has already landed.
	recs := afterFeed(t, bs, before)
	if len(recs) != 0 {
		t.Fatalf("feed has %d records before CommitScopedLog, want 0", len(recs))
	}
	got, err := bs.GetNode(types.NodeID(1))
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got.PropertiesMap()["name"] != "Bob" {
		t.Fatalf("entity write must land even though the log record is scoped: got name=%v, want Bob", got.PropertiesMap()["name"])
	}

	maxLSN, err := bs.CommitScopedLog(token)
	if err != nil {
		t.Fatalf("CommitScopedLog: %v", err)
	}
	if maxLSN == 0 {
		t.Fatal("CommitScopedLog maxLSN = 0, want nonzero")
	}

	recs = afterFeed(t, bs, before)
	if len(recs) != 1 || recs[0].Tag != storecontract.ChangeNodePut {
		t.Fatalf("feed after commit = %v, want [NodePut]", tagSeq(recs))
	}
	v1, err := storepkg.DecodeNodePut(recs[0].Payload)
	if err != nil {
		t.Fatalf("DecodeNodePut: %v", err)
	}
	if v1.Wire.Version != int(prevVersion)+1 || !v1.WithHistory {
		t.Fatalf("record = {v:%d wh:%v}, want {%d true}", v1.Wire.Version, v1.WithHistory, prevVersion+1)
	}
}

// ─── DiscardScopedLog drops the buffer and burns no LSN ─────────────────────

func TestScopedReplace_DiscardEmitsNothingAndBurnsNoLSN(t *testing.T) {
	t.Parallel()
	bs := newChangeLogStore(t, false)

	putTestNode(t, bs, 1, 10, nil) // eager create, LSN 1
	current, _ := bs.GetNode(types.NodeID(1))
	prevState := current.DeepCopy()
	prevVersion := current.Version()
	_ = current.SetProperty("name", "Bob")
	current.SetVersion(prevVersion + 1)

	token, err := bs.BeginScopedLog()
	if err != nil {
		t.Fatalf("BeginScopedLog: %v", err)
	}
	if err := bs.ReplaceNodeWithHistoryScoped(current, prevVersion, prevState, token); err != nil {
		t.Fatalf("ReplaceNodeWithHistoryScoped: %v", err)
	}
	if err := bs.DiscardScopedLog(token); err != nil {
		t.Fatalf("DiscardScopedLog: %v", err)
	}

	// A second eager write after the discard — its LSN must be contiguous with
	// the first eager record (2), proving the discarded scope burned no LSN.
	putTestNode(t, bs, 2, 10, nil)

	recs := drainFeed(t, bs)
	if len(recs) != 2 {
		t.Fatalf("feed has %d records, want 2 (discarded scope's record must never appear)", len(recs))
	}
	assertLSNContiguous(t, recs)

	// The entity write itself is NOT rolled back by a log-scope discard — only
	// the change-log record is scoped, not the mutation.
	got, err := bs.GetNode(types.NodeID(1))
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got.PropertiesMap()["name"] != "Bob" {
		t.Fatalf("entity write must survive discard: got name=%v, want Bob", got.PropertiesMap()["name"])
	}
}

// ─── Unknown / retired token fails closed ───────────────────────────────────

func TestScopedReplace_UnknownTokenFailsClosed(t *testing.T) {
	t.Parallel()
	bs := newChangeLogStore(t, false)

	putTestNode(t, bs, 1, 10, nil)
	current, _ := bs.GetNode(types.NodeID(1))
	prevState := current.DeepCopy()
	prevVersion := current.Version()
	_ = current.SetProperty("name", "Bob")
	current.SetVersion(prevVersion + 1)

	if err := bs.ReplaceNodeWithHistoryScoped(current, prevVersion, prevState, 999); !errors.Is(err, storecontract.ErrInvalidStoreMutation) {
		t.Fatalf("ReplaceNodeWithHistoryScoped(unknown token) error = %v, want ErrInvalidStoreMutation", err)
	}
}

// ─── Two concurrent scopes never cross-contaminate ──────────────────────────

func TestScopedReplace_ConcurrentScopesDoNotCrossContaminate(t *testing.T) {
	t.Parallel()
	bs := newChangeLogStore(t, false)

	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)

	n1, _ := bs.GetNode(types.NodeID(1))
	prev1 := n1.DeepCopy()
	v1 := n1.Version()
	_ = n1.SetProperty("who", "A")
	n1.SetVersion(v1 + 1)

	n2, _ := bs.GetNode(types.NodeID(2))
	prev2 := n2.DeepCopy()
	v2 := n2.Version()
	_ = n2.SetProperty("who", "B")
	n2.SetVersion(v2 + 1)

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

	if err := bs.ReplaceNodeWithHistoryScoped(n1, v1, prev1, tokenA); err != nil {
		t.Fatalf("ReplaceNodeWithHistoryScoped A: %v", err)
	}
	if err := bs.ReplaceNodeWithHistoryScoped(n2, v2, prev2, tokenB); err != nil {
		t.Fatalf("ReplaceNodeWithHistoryScoped B: %v", err)
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
	if len(recs) != 1 || recs[0].Tag != storecontract.ChangeNodePut {
		t.Fatalf("feed = %v, want [NodePut] (only B's, A discarded)", tagSeq(recs))
	}
	rw, err := storepkg.DecodeNodePut(recs[0].Payload)
	if err != nil {
		t.Fatalf("DecodeNodePut: %v", err)
	}
	if rw.Wire.ID != 2 {
		t.Fatalf("record node ID = %d, want 2 (B's node)", rw.Wire.ID)
	}
}

// ─── ReplaceRelWithHistoryScoped mirrors ReplaceNodeWithHistoryScoped (rule 2 parity) ───

func TestScopedReplace_ReplaceRelWithHistoryScoped(t *testing.T) {
	t.Parallel()
	bs := newChangeLogStore(t, false)

	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)
	putTestRel(t, bs, 100, 5, 1, 2)

	cur, _ := bs.GetRelationship(types.RelID(100))
	prev := cur.DeepCopy()
	prevVersion := cur.Version()
	_ = cur.SetProperty("w", int64(9))
	cur.SetVersion(prevVersion + 1)

	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	before, _ := bs.LastCommittedLSN()

	token, err := bs.BeginScopedLog()
	if err != nil {
		t.Fatalf("BeginScopedLog: %v", err)
	}
	if err := bs.ReplaceRelWithHistoryScoped(cur, prevVersion, prev, token); err != nil {
		t.Fatalf("ReplaceRelWithHistoryScoped: %v", err)
	}

	recs := afterFeed(t, bs, before)
	if len(recs) != 0 {
		t.Fatalf("feed has %d records before CommitScopedLog, want 0", len(recs))
	}

	if _, err := bs.CommitScopedLog(token); err != nil {
		t.Fatalf("CommitScopedLog: %v", err)
	}
	recs = afterFeed(t, bs, before)
	if len(recs) != 1 || recs[0].Tag != storecontract.ChangeRelPut {
		t.Fatalf("feed = %v, want [RelPut]", tagSeq(recs))
	}
	rw, err := storepkg.DecodeRelPut(recs[0].Payload)
	if err != nil {
		t.Fatalf("DecodeRelPut: %v", err)
	}
	if rw.Wire.Version != int(prevVersion)+1 || !rw.WithHistory {
		t.Fatalf("record = {v:%d wh:%v}, want {%d true}", rw.Wire.Version, rw.WithHistory, prevVersion+1)
	}

	// Verify the entity + history rows themselves landed correctly (update-
	// path-specific: prevVersion/prevState correctly persisted via the scoped
	// route too).
	got, err := bs.GetRelationship(types.RelID(100))
	if err != nil {
		t.Fatalf("GetRelationship: %v", err)
	}
	if got.PropertiesMap()["w"] != int64(9) {
		t.Fatalf("got w=%v, want 9", got.PropertiesMap()["w"])
	}
	hist, err := bs.GetRelVersion(types.RelID(100), prevVersion)
	if err != nil {
		t.Fatalf("GetRelVersion: %v", err)
	}
	if _, ok := hist.PropertiesMap()["w"]; ok {
		t.Fatalf("history version must not carry the new property: got %v", hist.PropertiesMap())
	}
}

// ─── Disabled log: the scoped door is a documented no-op fallback ───────────

func TestScopedReplace_DisabledByDefault(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t) // ChangeLog not set

	putTestNode(t, bs, 1, 10, nil)
	current, _ := bs.GetNode(types.NodeID(1))
	prevState := current.DeepCopy()
	prevVersion := current.Version()
	_ = current.SetProperty("name", "Bob")
	current.SetVersion(prevVersion + 1)

	token, err := bs.BeginScopedLog()
	if err != nil {
		t.Fatalf("BeginScopedLog: %v", err)
	}
	if token != 0 {
		t.Fatalf("BeginScopedLog token = %d, want 0 (log disabled)", token)
	}

	if err := bs.ReplaceNodeWithHistoryScoped(current, prevVersion, prevState, token); err != nil {
		t.Fatalf("ReplaceNodeWithHistoryScoped: %v", err)
	}
	got, err := bs.GetNode(types.NodeID(1))
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got.PropertiesMap()["name"] != "Bob" {
		t.Fatalf("entity write must still land with a disabled log: got name=%v, want Bob", got.PropertiesMap()["name"])
	}
}

// ─── Concurrent goroutines each with their own scope: no data race ─────────

func TestScopedReplace_ConcurrentGoroutinesNoRace(t *testing.T) {
	bs := newChangeLogStore(t, false)
	const n = 8
	// Seed n distinct nodes up front (sequential — only the replace-with-
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
			prevState := current.DeepCopy()
			prevVersion := current.Version()
			if err := current.SetProperty("touched", true); err != nil {
				t.Errorf("SetProperty[%d]: %v", i, err)
				return
			}
			current.SetVersion(prevVersion + 1)

			token, err := bs.BeginScopedLog()
			if err != nil {
				t.Errorf("BeginScopedLog[%d]: %v", i, err)
				return
			}
			if err := bs.ReplaceNodeWithHistoryScoped(current, prevVersion, prevState, token); err != nil {
				t.Errorf("ReplaceNodeWithHistoryScoped[%d]: %v", i, err)
				return
			}
			if _, err := bs.CommitScopedLog(token); err != nil {
				t.Errorf("CommitScopedLog[%d]: %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	recs := drainFeed(t, bs)
	// n eager creates + n scoped-then-committed replaces.
	if len(recs) != 2*n {
		t.Fatalf("feed has %d records, want %d", len(recs), 2*n)
	}
	assertLSNContiguous(t, recs)
}
