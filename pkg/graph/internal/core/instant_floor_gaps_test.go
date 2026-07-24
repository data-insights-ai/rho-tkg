package core

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ── Two doors the commit-clock floor (lesson 71) must also cover, both of which
// only exist because the change-log vocabulary and the Reset policy grew:
//
//	(1) Admin.Reset()/ChangeClear wipe the whole MetaKV keyspace, which would take
//	    the durable floor watermark with it (instantFloorMeta is reapPolicyPreserve
//	    — see metakv_reap_policy.go), and
//	(2) ChangeForeignIncoming, the cross-machine incoming half-edge stub, whose
//	    body IS a ChangeRelPut body carrying a FOREIGN partition's TxFrom verbatim.

// TestInstantFloor_PreservedAcrossReset: a Reset erases the graph's DATA, but the
// commit clock is not data — it is this node's transaction-time position. If the
// wipe took the floor watermark with it, transaction time could regress across the
// Reset: a burst leaves the floor above the wall clock, the wipe drops the
// watermark, and a later reopen (which reseeds only from that watermark) hands back
// a c.now() BELOW stamps the change log already emitted before the Clear.
//
// Deliberately on badger, not memory: only badger's Clear (DropAll) actually wipes
// the MetaKV keyspace — the memory store's Clear leaves its meta map intact, so it
// cannot exhibit the defect and would make this test vacuously green.
func TestInstantFloor_PreservedAcrossReset(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()

	g, err := New(Config{BadgerDir: dir, AllowReset: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	// Freeze the wall so the per-Core monotonic floor (next = max(wall, last+1))
	// inflates each successive TxFrom above it — the >1 write/ms burst shape.
	frozen := time.Now().Add(time.Second)
	g.SetClockForTest(t, func() time.Time { return frozen })

	const burst = 300
	var maxTx types.Instant
	for i := 0; i < burst; i++ {
		n, err := g.Nodes.Add(ctx, []string{"T"}, map[string]any{"i": i})
		if err != nil {
			t.Fatalf("add %d: %v", i, err)
		}
		maxTx = maxInstant(maxTx, txFromStamp(t, n.Temporal()))
	}
	if maxTx <= types.Instant(frozen.UnixMilli()) {
		t.Fatalf("test precondition broken: burst did not inflate the floor (maxTx=%d, wall=%d)",
			maxTx, frozen.UnixMilli())
	}

	if err := g.Admin.Reset(); err != nil {
		t.Fatalf("Reset: %v", err)
	}

	mk, ok := g.store.(storepkg.MetaKVCapability)
	if !ok {
		t.Fatal("test precondition broken: badger backend must expose MetaKV")
	}
	v, err := mk.MetaGet(instantFloorMeta)
	if err != nil {
		t.Fatalf("MetaGet(instantFloorMeta): %v", err)
	}
	if len(v) != 8 {
		t.Fatalf("Reset wiped the commit-clock floor watermark (got %d bytes, want 8) — "+
			"instantFloorMeta is classified reapPolicyPreserve: the commit clock is this "+
			"node's transaction-time position, not graph data, so a data Reset must not let "+
			"TX time regress across it (lesson 71)", len(v))
	}
	if got := types.Instant(binary.BigEndian.Uint64(v)); got < maxTx {
		t.Fatalf("restored floor watermark %d < pre-Reset max TxFrom %d — a reopen would "+
			"reseed below already-committed stamps", got, maxTx)
	}

	// Live consequence: a post-Reset write must still be stamped strictly above
	// every pre-Reset commit.
	n, err := g.Nodes.Add(ctx, []string{"T"}, map[string]any{"post": true})
	if err != nil {
		t.Fatalf("post-Reset add: %v", err)
	}
	if tx := txFromStamp(t, n.Temporal()); tx <= maxTx {
		t.Fatalf("post-Reset write stamped %d <= pre-Reset max %d — TX time went backwards across Reset", tx, maxTx)
	}
}

// TestRecordCommitStamp_CoversForeignIncomingStub: ChangeForeignIncoming (ADR-0010
// Model A) is a relationship whose ID belongs to a FOREIGN slot, stored on the END
// node's shard. Its body is byte-identical to a ChangeRelPut body, so it carries the
// ORIGINATING partition's TxFrom verbatim — exactly the foreign-stamp ingress the
// floor exists to cover. If recordCommitStamp does not recognise the tag it returns
// 0, the apply-path floor advance is skipped, and NowTx() silently under-covers
// every applied stub row.
//
// The record is produced by the real encoder and then RE-TAGGED, which is also what
// proves the premise ("same body as ChangeRelPut") rather than assuming it.
func TestRecordCommitStamp_CoversForeignIncomingStub(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	g, err := New(Config{Store: memory.New(memory.WithChangeLog()), SnowflakeNodeID: 1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	a, err := g.Nodes.Add(ctx, []string{"T"}, nil)
	if err != nil {
		t.Fatalf("add a: %v", err)
	}
	b, err := g.Nodes.Add(ctx, []string{"T"}, nil)
	if err != nil {
		t.Fatalf("add b: %v", err)
	}
	r, err := g.Rels.Add(ctx, "R", a, b, nil)
	if err != nil {
		t.Fatalf("add rel: %v", err)
	}
	rTx := txFromStamp(t, r.Temporal())

	var relPut storepkg.ChangeRecord
	found := false
	if err := g.Repl.ForEachChange(0, func(rec storepkg.ChangeRecord) bool {
		if rec.Tag == storepkg.ChangeRelPut {
			relPut, found = rec, true
			return false
		}
		return true
	}); err != nil {
		t.Fatalf("ForEachChange: %v", err)
	}
	if !found {
		t.Fatal("no ChangeRelPut record in the feed — nothing to re-tag")
	}

	// Control: the ordinary rel-put path reports the rel's own TxFrom.
	if got := recordCommitStamp(relPut); got != rTx {
		t.Fatalf("control: recordCommitStamp(ChangeRelPut) = %d, want the rel's TxFrom %d", got, rTx)
	}

	stub := relPut
	stub.Tag = storepkg.ChangeForeignIncoming
	if got := recordCommitStamp(stub); got != rTx {
		t.Fatalf("recordCommitStamp(ChangeForeignIncoming) = %d, want %d — the cross-machine "+
			"incoming half-edge stub carries a foreign partition's TxFrom verbatim, so it must "+
			"advance the commit-clock floor exactly like ChangeRelPut; returning 0 leaves NowTx() "+
			"under-covering applied stub rows (lesson 71)", got, rTx)
	}

	// Negative pin: the switch is TAG-driven and deliberate, not payload-sniffing.
	// ChangeForeignIncomingDelete's real body is identifiers only (rel ID + END-node
	// ID for routing) and mints no stamp, so it must stay 0 even when handed a
	// stamp-bearing payload.
	del := relPut
	del.Tag = storepkg.ChangeForeignIncomingDelete
	if got := recordCommitStamp(del); got != 0 {
		t.Fatalf("recordCommitStamp(ChangeForeignIncomingDelete) = %d, want 0 — that record "+
			"carries routing identifiers only, no system-minted stamp", got)
	}
}
