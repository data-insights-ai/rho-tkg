package badger

import (
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BACKLOG 18c: the TieredStore cross-shard split-write helpers
// (PutRelEntityAndOut / PutRelIncoming / DeleteRelEntityAndOut /
// DeleteRelIncoming) mutated state and appendOps'd write ops but never called
// logChangeRaw — a cross-shard relationship create/delete was completely
// invisible to a change-log-enabled store's feed, a silent replica/CDC
// divergence. The fix: PutRelEntityAndOut / DeleteRelEntityAndOut (the
// entity-bearing legs, which carry everything a replica needs to reconstruct
// the whole relationship) now co-commit ChangeRelPut / ChangeRelDelete
// records exactly like the single-shard PutRelationship / DeleteRelationship
// doors. PutRelIncoming / DeleteRelIncoming (the in/-only legs, no entity
// data) deliberately stay record-free — the entity leg's record already
// fully describes the relationship.
//
// ChangeFeed(0, 0) always reads from the beginning (it is not a consuming
// cursor), so — mirroring badgerstore_changelog_test.go's own convention —
// every test here does exactly one drainFeed at the end and asserts the
// FULL accumulated tag sequence, rather than draining incrementally.

func newTestBadgerStoreInMemoryWithChangeLog(t *testing.T) *Store {
	t.Helper()
	bs, err := New(Config{
		InMemory:      true,
		ChangeLog:     true,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatalf("New(ChangeLog): %v", err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	return bs
}

func TestPutRelEntityAndOut_EmitsChangeRelPut(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStoreInMemoryWithChangeLog(t)

	gen := newTestGen(t, 0)
	n1 := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
	n2 := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
	if err := bs.PutNode(n1); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNode(n2); err != nil {
		t.Fatal(err)
	}

	relGen := newTestGen(t, 1)
	relID := relGen.Generate()
	r := types.NewRelationship(types.RelID(relID), 1, n1.ID(), n2.ID())
	if err := r.SetProperty("weight", int64(7)); err != nil {
		t.Fatalf("SetProperty: %v", err)
	}
	if err := bs.PutRelEntityAndOut(r); err != nil {
		t.Fatalf("PutRelEntityAndOut: %v", err)
	}

	recs := drainFeed(t, bs)
	got := tagSeq(recs)
	if len(got) != 3 || got[0] != storecontract.ChangeNodePut || got[1] != storecontract.ChangeNodePut || got[2] != storecontract.ChangeRelPut {
		t.Fatalf("tags = %v, want [NodePut NodePut RelPut] — BACKLOG 18c regression (cross-shard rel create invisible to the feed)", got)
	}
	body, err := storepkg.DecodeRelPut(recs[2].Payload)
	if err != nil {
		t.Fatalf("DecodeRelPut: %v", err)
	}
	if body.WithHistory {
		t.Fatal("WithHistory = true, want false (PutRelEntityAndOut is a create door)")
	}
	if body.Wire.ID != int64(relID) {
		t.Fatalf("record rel ID = %d, want %d", body.Wire.ID, relID)
	}
}

func TestPutRelIncoming_EmitsNoChangeRecord(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStoreInMemoryWithChangeLog(t)

	gen := newTestGen(t, 0)
	n1 := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
	n2 := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
	if err := bs.PutNode(n1); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNode(n2); err != nil {
		t.Fatal(err)
	}

	relGen := newTestGen(t, 1)
	relID := relGen.Generate()
	if err := bs.PutRelIncoming(n2.ID().SnowflakeID(), n1.ID().SnowflakeID(), 1, relID); err != nil {
		t.Fatalf("PutRelIncoming: %v", err)
	}

	// Only the 2 node puts should be visible — PutRelIncoming contributes nothing.
	recs := drainFeed(t, bs)
	got := tagSeq(recs)
	if len(got) != 2 || got[0] != storecontract.ChangeNodePut || got[1] != storecontract.ChangeNodePut {
		t.Fatalf("tags = %v, want [NodePut NodePut] (no entity data — the companion PutRelEntityAndOut's record covers it)", got)
	}
}

func TestDeleteRelEntityAndOut_EmitsChangeRelDelete(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStoreInMemoryWithChangeLog(t)

	gen := newTestGen(t, 0)
	n1 := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
	n2 := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
	if err := bs.PutNode(n1); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNode(n2); err != nil {
		t.Fatal(err)
	}

	relGen := newTestGen(t, 1)
	relID := relGen.Generate()
	r := types.NewRelationship(types.RelID(relID), 1, n1.ID(), n2.ID())
	if err := bs.PutRelEntityAndOut(r); err != nil {
		t.Fatalf("PutRelEntityAndOut: %v", err)
	}
	if _, err := bs.DeleteRelEntityAndOut(relID); err != nil {
		t.Fatalf("DeleteRelEntityAndOut: %v", err)
	}

	recs := drainFeed(t, bs)
	got := tagSeq(recs)
	if len(got) != 4 || got[0] != storecontract.ChangeNodePut || got[1] != storecontract.ChangeNodePut ||
		got[2] != storecontract.ChangeRelPut || got[3] != storecontract.ChangeRelDelete {
		t.Fatalf("tags = %v, want [NodePut NodePut RelPut RelDelete] — BACKLOG 18c regression (cross-shard rel delete invisible to the feed)", got)
	}
	body, err := storepkg.DecodeRelDelete(recs[3].Payload)
	if err != nil {
		t.Fatalf("DecodeRelDelete: %v", err)
	}
	if body.ID != int64(relID) {
		t.Fatalf("record rel ID = %d, want %d", body.ID, relID)
	}
	if body.WithHistory {
		t.Fatal("WithHistory = true, want false (hard delete, no tombstone)")
	}
}

func TestDeleteRelIncoming_EmitsNoChangeRecord(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStoreInMemoryWithChangeLog(t)

	gen := newTestGen(t, 0)
	n1 := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
	n2 := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
	if err := bs.PutNode(n1); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNode(n2); err != nil {
		t.Fatal(err)
	}

	relGen := newTestGen(t, 1)
	relID := relGen.Generate()
	if err := bs.PutRelIncoming(n2.ID().SnowflakeID(), n1.ID().SnowflakeID(), 1, relID); err != nil {
		t.Fatalf("PutRelIncoming: %v", err)
	}

	info := RelDeleteInfo{ID: relID, RelType: 1, StartID: n1.ID().SnowflakeID(), EndID: n2.ID().SnowflakeID()}
	if err := bs.DeleteRelIncoming(info); err != nil {
		t.Fatalf("DeleteRelIncoming: %v", err)
	}

	// Only the 2 node puts should be visible — neither PutRelIncoming nor
	// DeleteRelIncoming contributes anything.
	recs := drainFeed(t, bs)
	got := tagSeq(recs)
	if len(got) != 2 || got[0] != storecontract.ChangeNodePut || got[1] != storecontract.ChangeNodePut {
		t.Fatalf("tags = %v, want [NodePut NodePut]", got)
	}
}
