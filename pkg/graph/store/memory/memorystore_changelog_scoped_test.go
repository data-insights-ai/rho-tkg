package memory

import (
	"errors"
	"sync"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/generatedcreate"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Compile-time proof the memory store satisfies the BACKLOG 11f Batch A
// capabilities (store.ScopedTxChangeLog, store.ScopedPutCapability, and the
// generatedcreate scoped endpoint-hash capability).
var (
	_ storecontract.ScopedTxChangeLog                          = (*Store)(nil)
	_ storecontract.ScopedPutCapability                        = (*Store)(nil)
	_ generatedcreate.RelationshipEndpointHashScopedCapability = (*Store)(nil)
)

func memRel(id int64, relType uint16, startID, endID int64) *types.Relationship {
	return types.NewRelationship(types.RelID(snowflake.ID(id)), relType, types.NodeID(snowflake.ID(startID)), types.NodeID(snowflake.ID(endID)))
}

// ─── Disabled log: every scoped method is a documented no-op ───────────────

func TestMemoryScopedChangeLog_DisabledByDefault(t *testing.T) {
	ms := New() // no WithChangeLog

	token, err := ms.BeginScopedLog()
	if err != nil {
		t.Fatalf("BeginScopedLog: %v", err)
	}
	if token != 0 {
		t.Fatalf("BeginScopedLog token = %d, want 0 (log disabled)", token)
	}
	if err := ms.PutNodeScoped(memNode(1, 10), token); err != nil {
		t.Fatalf("PutNodeScoped: %v", err)
	}
	maxLSN, err := ms.CommitScopedLog(token)
	if err != nil {
		t.Fatalf("CommitScopedLog: %v", err)
	}
	if maxLSN != 0 {
		t.Fatalf("CommitScopedLog maxLSN = %d, want 0 (log disabled)", maxLSN)
	}
}

// ─── token == 0 is exactly the unscoped door ────────────────────────────────

func TestMemoryScopedChangeLog_ZeroTokenMatchesUnscopedDoor(t *testing.T) {
	ms := New(WithChangeLog())

	if err := ms.PutNodeScoped(memNode(1, 10), 0); err != nil {
		t.Fatalf("PutNodeScoped(token=0): %v", err)
	}
	if err := ms.PutNodeScoped(memNode(2, 10), 0); err != nil {
		t.Fatalf("PutNodeScoped(token=0) n2: %v", err)
	}
	if err := ms.PutRelationshipScoped(memRel(100, 5, 1, 2), 0); err != nil {
		t.Fatalf("PutRelationshipScoped(token=0): %v", err)
	}

	recs, err := ms.ChangeFeed(0, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 3 {
		t.Fatalf("feed has %d records, want 3", len(recs))
	}
	wantTags := []storecontract.ChangeTag{storecontract.ChangeNodePut, storecontract.ChangeNodePut, storecontract.ChangeRelPut}
	got := memTags(recs)
	for i := range wantTags {
		if got[i] != wantTags[i] {
			t.Fatalf("record[%d].Tag = %v, want %v", i, got[i], wantTags[i])
		}
	}
	for i, r := range recs {
		if r.LSN != uint64(i+1) {
			t.Fatalf("record[%d].LSN = %d, want %d", i, r.LSN, i+1)
		}
	}
}

// ─── An open (uncommitted) scope is invisible to the feed ──────────────────

func TestMemoryScopedChangeLog_UncommittedScopeInvisibleToFeed(t *testing.T) {
	ms := New(WithChangeLog())

	token, err := ms.BeginScopedLog()
	if err != nil {
		t.Fatalf("BeginScopedLog: %v", err)
	}
	if token == 0 {
		t.Fatal("BeginScopedLog token = 0, want nonzero (log enabled)")
	}
	if err := ms.PutNodeScoped(memNode(1, 10), token); err != nil {
		t.Fatalf("PutNodeScoped: %v", err)
	}

	// Load-bearing: not visible before commit.
	recs, err := ms.ChangeFeed(0, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("feed has %d records before CommitScopedLog, want 0", len(recs))
	}
	if lsn, _ := ms.LastCommittedLSN(); lsn != 0 {
		t.Fatalf("LastCommittedLSN = %d before commit, want 0", lsn)
	}

	maxLSN, err := ms.CommitScopedLog(token)
	if err != nil {
		t.Fatalf("CommitScopedLog: %v", err)
	}
	if maxLSN != 1 {
		t.Fatalf("CommitScopedLog maxLSN = %d, want 1", maxLSN)
	}
	recs, err = ms.ChangeFeed(0, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 1 || recs[0].Tag != storecontract.ChangeNodePut || recs[0].LSN != 1 {
		t.Fatalf("feed after commit = %#v, want one ChangeNodePut record at LSN 1", recs)
	}
}

// ─── DiscardScopedLog drops the buffer and burns no LSN ─────────────────────

func TestMemoryScopedChangeLog_DiscardEmitsNothingAndBurnsNoLSN(t *testing.T) {
	ms := New(WithChangeLog())

	if err := ms.PutNode(memNode(1, 10)); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	token, err := ms.BeginScopedLog()
	if err != nil {
		t.Fatalf("BeginScopedLog: %v", err)
	}
	if err := ms.PutNodeScoped(memNode(2, 10), token); err != nil {
		t.Fatalf("PutNodeScoped: %v", err)
	}
	if err := ms.DiscardScopedLog(token); err != nil {
		t.Fatalf("DiscardScopedLog: %v", err)
	}

	if err := ms.PutNode(memNode(3, 10)); err != nil {
		t.Fatalf("PutNode n3: %v", err)
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
}

// ─── Unknown / retired token fails closed ───────────────────────────────────

func TestMemoryScopedChangeLog_UnknownTokenFailsClosed(t *testing.T) {
	ms := New(WithChangeLog())

	if _, err := ms.CommitScopedLog(999); !errors.Is(err, storecontract.ErrInvalidStoreMutation) {
		t.Fatalf("CommitScopedLog(unknown) error = %v, want ErrInvalidStoreMutation", err)
	}

	token, err := ms.BeginScopedLog()
	if err != nil {
		t.Fatalf("BeginScopedLog: %v", err)
	}
	if _, err := ms.CommitScopedLog(token); err != nil {
		t.Fatalf("CommitScopedLog(first): %v", err)
	}
	if _, err := ms.CommitScopedLog(token); !errors.Is(err, storecontract.ErrInvalidStoreMutation) {
		t.Fatalf("CommitScopedLog(retired token) error = %v, want ErrInvalidStoreMutation", err)
	}
}

// ─── Two concurrent scopes never cross-contaminate ──────────────────────────

func TestMemoryScopedChangeLog_ConcurrentScopesDoNotCrossContaminate(t *testing.T) {
	ms := New(WithChangeLog())

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

	if err := ms.PutNodeScoped(memNode(1, 10), tokenA); err != nil {
		t.Fatalf("PutNodeScoped A: %v", err)
	}
	if err := ms.PutNodeScoped(memNode(2, 10), tokenB); err != nil {
		t.Fatalf("PutNodeScoped B: %v", err)
	}

	if _, err := ms.CommitScopedLog(tokenB); err != nil {
		t.Fatalf("CommitScopedLog B: %v", err)
	}
	if err := ms.DiscardScopedLog(tokenA); err != nil {
		t.Fatalf("DiscardScopedLog A: %v", err)
	}

	recs, err := ms.ChangeFeed(0, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("feed has %d records, want 1 (only B's, A discarded)", len(recs))
	}
	if recs[0].Tag != storecontract.ChangeNodePut {
		t.Fatalf("record.Tag = %v, want ChangeNodePut", recs[0].Tag)
	}
}

// ─── Concurrent goroutines each with their own scope: no data race ─────────

func TestMemoryScopedChangeLog_ConcurrentGoroutinesNoRace(t *testing.T) {
	ms := New(WithChangeLog())
	const n = 8
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			token, err := ms.BeginScopedLog()
			if err != nil {
				t.Errorf("BeginScopedLog[%d]: %v", i, err)
				return
			}
			if err := ms.PutNodeScoped(memNode(int64(1000+i), 10), token); err != nil {
				t.Errorf("PutNodeScoped[%d]: %v", i, err)
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
	if len(recs) != n {
		t.Fatalf("feed has %d records, want %d", len(recs), n)
	}
}

// ─── PutRelationshipScoped with a nonzero token (badger parity) ────────────

func TestMemoryScopedChangeLog_PutRelationshipScoped(t *testing.T) {
	ms := New(WithChangeLog())
	if err := ms.PutNode(memNode(1, 10)); err != nil {
		t.Fatalf("PutNode n1: %v", err)
	}
	if err := ms.PutNode(memNode(2, 10)); err != nil {
		t.Fatalf("PutNode n2: %v", err)
	}
	before, err := ms.LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	}

	token, err := ms.BeginScopedLog()
	if err != nil {
		t.Fatalf("BeginScopedLog: %v", err)
	}
	if err := ms.PutRelationshipScoped(memRel(100, 5, 1, 2), token); err != nil {
		t.Fatalf("PutRelationshipScoped: %v", err)
	}

	recs, err := ms.ChangeFeed(before, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("record visible before CommitScopedLog (%d records)", len(recs))
	}

	if _, err := ms.CommitScopedLog(token); err != nil {
		t.Fatalf("CommitScopedLog: %v", err)
	}
	recs, err = ms.ChangeFeed(before, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 1 || recs[0].Tag != storecontract.ChangeRelPut {
		t.Fatalf("feed after commit = %#v, want one ChangeRelPut record", recs)
	}
}

// ─── PutRelationshipGeneratedIDWithEndpointHashesScoped ─────────────────────

func TestMemoryScopedChangeLog_PutRelationshipGeneratedIDWithEndpointHashesScoped(t *testing.T) {
	ms := New(WithChangeLog())
	if err := ms.PutNode(memNode(1, 10)); err != nil {
		t.Fatalf("PutNode n1: %v", err)
	}
	if err := ms.PutNode(memNode(2, 10)); err != nil {
		t.Fatalf("PutNode n2: %v", err)
	}

	token, err := ms.BeginScopedLog()
	if err != nil {
		t.Fatalf("BeginScopedLog: %v", err)
	}
	r := memRel(100, 5, 1, 2)
	// Bare test nodes carry no Integrity(), so nodeIntegrityHash legitimately
	// returns "" for both — the invariant under test is routing (does the
	// record land in the scope, not the eager log), not hash content.
	if _, _, err := ms.PutRelationshipGeneratedIDWithEndpointHashesScoped(r, token, generatedcreate.FreshGraphID()); err != nil {
		t.Fatalf("PutRelationshipGeneratedIDWithEndpointHashesScoped: %v", err)
	}

	// Not yet visible.
	recs, err := ms.ChangeFeed(0, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	for _, rec := range recs {
		if rec.Tag == storecontract.ChangeRelPut {
			t.Fatal("RelPut record visible before CommitScopedLog")
		}
	}

	if _, err := ms.CommitScopedLog(token); err != nil {
		t.Fatalf("CommitScopedLog: %v", err)
	}
	recs, err = ms.ChangeFeed(0, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	found := false
	for _, rec := range recs {
		if rec.Tag == storecontract.ChangeRelPut {
			found = true
		}
	}
	if !found {
		t.Fatal("RelPut record missing after CommitScopedLog")
	}
}

// token == 0 for the endpoint-hash door matches its unscoped sibling exactly.
func TestMemoryScopedChangeLog_PutRelationshipGeneratedIDWithEndpointHashesScopedZeroToken(t *testing.T) {
	ms := New(WithChangeLog())
	if err := ms.PutNode(memNode(1, 10)); err != nil {
		t.Fatalf("PutNode n1: %v", err)
	}
	if err := ms.PutNode(memNode(2, 10)); err != nil {
		t.Fatalf("PutNode n2: %v", err)
	}
	r := memRel(100, 5, 1, 2)
	if _, _, err := ms.PutRelationshipGeneratedIDWithEndpointHashesScoped(r, 0, generatedcreate.FreshGraphID()); err != nil {
		t.Fatalf("PutRelationshipGeneratedIDWithEndpointHashesScoped(token=0): %v", err)
	}
	recs, err := ms.ChangeFeed(0, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	found := false
	for _, rec := range recs {
		if rec.Tag == storecontract.ChangeRelPut {
			found = true
		}
	}
	if !found {
		t.Fatal("token=0 record should be eagerly visible, matching the unscoped door")
	}
}

// ─── Legacy single-scope mechanism is unaffected (regression check) ────────

func TestMemoryScopedChangeLog_LegacyMechanismUnaffected(t *testing.T) {
	ms := New(WithChangeLog())

	if err := ms.BeginLogScope(); err != nil {
		t.Fatalf("BeginLogScope: %v", err)
	}
	ms.SetLogDivert(true)
	if err := ms.PutNode(memNode(1, 10)); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	ms.SetLogDivert(false)

	recs, err := ms.ChangeFeed(0, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("feed has %d records before CommitLogScope, want 0", len(recs))
	}

	maxLSN, err := ms.CommitLogScope()
	if err != nil {
		t.Fatalf("CommitLogScope: %v", err)
	}
	if maxLSN != 1 {
		t.Fatalf("CommitLogScope maxLSN = %d, want 1", maxLSN)
	}
	recs, err = ms.ChangeFeed(0, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("feed has %d records after CommitLogScope, want 1", len(recs))
	}
}
