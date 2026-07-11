package core

import (
	"context"
	"errors"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// openDiskTieredGraphAt opens a Graph backed by a disk tiered.Store rooted at
// dir. Unlike newDiskTieredGraph it does NOT register a t.Cleanup close, so a
// test can Close() and reopen the same dir to exercise durability.
func openDiskTieredGraphAt(t *testing.T, dir string) *Core {
	t.Helper()
	ts, err := tiered.New(tiered.Config{
		DataDir:       dir,
		RefLabels:     []string{"Case", "User"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatalf("tiered.New: %v", err)
	}
	g, err := New(Config{SnowflakeNodeID: 0, Store: ts})
	if err != nil {
		_ = ts.Close()
		t.Fatalf("New: %v", err)
	}
	return g
}

// buildLabeledNodeChain mirrors buildNodeChain but takes the primary label, so a
// test can steer an entity onto a specific tiered shard (a reference label lands
// on the reference shard; an event label lands on the hot event shard).
func buildLabeledNodeChain(t *testing.T, g *Core, label string, updates int) (types.NodeID, []types.Instant) {
	t.Helper()
	ctx := context.Background()
	n, err := g.Nodes.Add(ctx, []string{label}, map[string]any{"state": "v0"})
	if err != nil {
		t.Fatalf("add %s: %v", label, err)
	}
	id := n.ID()
	for i := 1; i <= updates; i++ {
		if _, err := g.Nodes.Update(ctx, id, map[string]any{"state": "v" + itoa(i)}); err != nil {
			t.Fatalf("update %s %d: %v", label, i, err)
		}
	}
	return id, nodeChainTxFroms(t, g, id)
}

// TestCompaction_Tiered_NodeEndToEnd is the tiered mirror of
// TestCompactHistoryNodes_EndToEnd: report, physical trim, stub-aware verify,
// two-phase reads above the watermark, and ErrHistoryCompacted at both the point
// and scan doors below it — all against a reference-shard entity.
func TestCompaction_Tiered_NodeEndToEnd(t *testing.T) {
	t.Parallel()
	g, _ := newDiskTieredGraph(t)
	ctx := context.Background()

	id, tx := buildLabeledNodeChain(t, g, "Case", 4) // v0..v4; history v0..v3
	highPin, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatalf("NowTx: %v", err)
	}

	rep, err := g.Admin.CompactHistoryNodes(ctx, RetentionPolicy{KeepVersions: 2})
	if err != nil {
		t.Fatalf("CompactHistoryNodes: %v", err)
	}
	if rep.EntitiesCompacted != 1 || rep.VersionsTrimmed != 2 {
		t.Fatalf("report = %+v, want 1 entity / 2 versions", rep)
	}
	if rep.Watermark != tx[2] {
		t.Fatalf("watermark = %d, want %d (v2 TxFrom)", rep.Watermark, tx[2])
	}

	hist, err := g.Nodes.History(id)
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if len(hist) != 2 {
		t.Fatalf("history len = %d, want 2", len(hist))
	}
	if ok, err := g.Hash.VerifyNodeChain(id); err != nil || !ok {
		t.Fatalf("VerifyNodeChain = (%v,%v), want (true,nil)", ok, err)
	}

	// Two-phase: reads at/above the watermark stay exact after the trim.
	for pin, want := range map[types.Instant]string{tx[2]: "v2", tx[3]: "v3", highPin: "v4"} {
		got, err := g.Temporal.NodeAsOf(id, pin)
		if err != nil {
			t.Fatalf("NodeAsOf(%d): %v", pin, err)
		}
		if s := nodeState(t, got); s != want {
			t.Fatalf("NodeAsOf(%d) state=%q, want %q", pin, s, want)
		}
	}
	// Point door below the boundary: ErrHistoryCompacted, never silent-empty.
	for _, pin := range []types.Instant{tx[0], tx[1]} {
		if _, err := g.Temporal.NodeAsOf(id, pin); !errors.Is(err, ErrHistoryCompacted) {
			t.Fatalf("NodeAsOf(%d) err=%v, want ErrHistoryCompacted", pin, err)
		}
	}
	// Scan door below the watermark fails the whole scan; at/above works.
	if _, err := g.Nodes.ByLabel("Case", storepkg.QueryOpts{TxPin: tx[1]}); !errors.Is(err, ErrHistoryCompacted) {
		t.Fatalf("ByLabel{TxPin<wm} err=%v, want ErrHistoryCompacted", err)
	}
	got, err := g.Nodes.ByLabel("Case", storepkg.QueryOpts{TxPin: highPin})
	if err != nil || len(got) != 1 || nodeState(t, got[0]) != "v4" {
		t.Fatalf("ByLabel{TxPin=high} = (%v,%v), want 1 node v4", got, err)
	}
}

// TestCompaction_Tiered_RelEndToEnd is the relationship mirror (Testing Rule 2).
// The "LINK" rel and its "N" endpoints are event-class, so this exercises the
// event-shard trim path with the stub routed to the reference shard.
func TestCompaction_Tiered_RelEndToEnd(t *testing.T) {
	t.Parallel()
	g, _ := newDiskTieredGraph(t)
	ctx := context.Background()

	id, tx := buildRelChain(t, g, 4)
	highPin, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatalf("NowTx: %v", err)
	}

	rep, err := g.Admin.CompactHistoryRels(ctx, RetentionPolicy{KeepVersions: 2})
	if err != nil {
		t.Fatalf("CompactHistoryRels: %v", err)
	}
	if rep.EntitiesCompacted != 1 || rep.VersionsTrimmed != 2 || rep.Watermark != tx[2] {
		t.Fatalf("report = %+v, want 1/2 watermark %d", rep, tx[2])
	}
	if hist, _ := g.Rels.History(id); len(hist) != 2 {
		t.Fatalf("history len = %d, want 2", len(hist))
	}
	if ok, err := g.Hash.VerifyRelChain(id); err != nil || !ok {
		t.Fatalf("VerifyRelChain = (%v,%v), want (true,nil)", ok, err)
	}
	if got, err := g.Temporal.RelAsOf(id, highPin); err != nil || relState(t, got) != "v4" {
		t.Fatalf("RelAsOf(high) = (%v,%v), want v4", got, err)
	}
	for _, pin := range []types.Instant{tx[0], tx[1]} {
		if _, err := g.Temporal.RelAsOf(id, pin); !errors.Is(err, ErrHistoryCompacted) {
			t.Fatalf("RelAsOf(%d) err=%v, want ErrHistoryCompacted", pin, err)
		}
	}
	if _, err := g.Temporal.RelsAsOf(tx[1]); !errors.Is(err, ErrHistoryCompacted) {
		t.Fatalf("RelsAsOf(%d) err=%v, want ErrHistoryCompacted", tx[1], err)
	}
}

// TestCompaction_Tiered_CrossShard compacts TWO node chains that live on TWO
// different shards (a reference node on the reference shard + an event node on the
// hot event shard) in ONE call, and asserts both are trimmed, exactly ONE global
// watermark is recorded (= the higher of the two boundaries), and after reopen
// every per-entity stub is readable (it landed on the reference shard, not the
// event node's owning shard) so both below-boundary point reads fail closed.
func TestCompaction_Tiered_CrossShard(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()

	var refID, evtID types.NodeID
	var refTx, evtTx []types.Instant
	var highPin types.Instant
	func() {
		g := openDiskTieredGraphAt(t, dir)
		defer g.Close()

		refID, refTx = buildLabeledNodeChain(t, g, "Case", 4)   // reference shard
		evtID, evtTx = buildLabeledNodeChain(t, g, "Signal", 4) // hot event shard
		var err error
		highPin, err = g.Temporal.NowTx()
		if err != nil {
			t.Fatalf("NowTx: %v", err)
		}

		rep, err := g.Admin.CompactHistoryNodes(ctx, RetentionPolicy{KeepVersions: 2})
		if err != nil {
			t.Fatalf("CompactHistoryNodes: %v", err)
		}
		if rep.EntitiesCompacted != 2 || rep.VersionsTrimmed != 4 {
			t.Fatalf("report = %+v, want 2 entities / 4 versions", rep)
		}
		// The event node was created after the reference node, so its boundary is
		// the higher one — that is the single global watermark.
		if rep.Watermark != evtTx[2] {
			t.Fatalf("watermark = %d, want %d (event v2 TxFrom, the max boundary)", rep.Watermark, evtTx[2])
		}
		if h, _ := g.Nodes.History(refID); len(h) != 2 {
			t.Fatalf("reference history len = %d, want 2", len(h))
		}
		if h, _ := g.Nodes.History(evtID); len(h) != 2 {
			t.Fatalf("event history len = %d, want 2", len(h))
		}
	}()

	// Reopen: the watermark reloads from the reference shard, and both stubs are
	// readable there (the event node's stub did NOT scatter to its event shard).
	g2 := openDiskTieredGraphAt(t, dir)
	defer g2.Close()

	if got := types.Instant(g2.compactedThroughTx.Load()); got != evtTx[2] {
		t.Fatalf("reloaded watermark = %d, want %d", got, evtTx[2])
	}
	// Both compacted entities: newest belief resolves, below-boundary point reads
	// fail closed via their reference-shard stubs.
	if got, err := g2.Temporal.NodeAsOf(refID, highPin); err != nil || nodeState(t, got) != "v4" {
		t.Fatalf("post-reopen ref NodeAsOf(high) = (%v,%v), want v4", got, err)
	}
	if got, err := g2.Temporal.NodeAsOf(evtID, highPin); err != nil || nodeState(t, got) != "v4" {
		t.Fatalf("post-reopen evt NodeAsOf(high) = (%v,%v), want v4", got, err)
	}
	if _, err := g2.Temporal.NodeAsOf(refID, refTx[1]); !errors.Is(err, ErrHistoryCompacted) {
		t.Fatalf("post-reopen ref NodeAsOf(%d) err=%v, want ErrHistoryCompacted", refTx[1], err)
	}
	if _, err := g2.Temporal.NodeAsOf(evtID, evtTx[1]); !errors.Is(err, ErrHistoryCompacted) {
		t.Fatalf("post-reopen evt NodeAsOf(%d) err=%v, want ErrHistoryCompacted", evtTx[1], err)
	}
	// Verify chains survive on both shards after reopen.
	if ok, err := g2.Hash.VerifyNodeChain(refID); err != nil || !ok {
		t.Fatalf("post-reopen ref verify = (%v,%v)", ok, err)
	}
	if ok, err := g2.Hash.VerifyNodeChain(evtID); err != nil || !ok {
		t.Fatalf("post-reopen evt verify = (%v,%v)", ok, err)
	}
}

// TestCompaction_Tiered_WatermarkOnRefShardRegression compacts ONLY an event
// node — nothing on the reference shard is compacted. If the global watermark
// were bundled into the event node's owning-shard batch (the pre-fix scatter),
// the store-level MetaGet (which reads the reference shard) would miss it, the
// watermark would stay 0, and the scan door would NOT fail closed below the
// boundary. The scan-door assertion is therefore the regression guard that the
// watermark lands globally on the reference shard.
func TestCompaction_Tiered_WatermarkOnRefShardRegression(t *testing.T) {
	t.Parallel()
	g, _ := newDiskTieredGraph(t)
	ctx := context.Background()

	id, tx := buildLabeledNodeChain(t, g, "Signal", 4) // event shard only

	rep, err := g.Admin.CompactHistoryNodes(ctx, RetentionPolicy{KeepVersions: 2})
	if err != nil {
		t.Fatalf("CompactHistoryNodes: %v", err)
	}
	if rep.Watermark != tx[2] {
		t.Fatalf("watermark = %d, want %d", rep.Watermark, tx[2])
	}

	// Watermark is readable through the store-level MetaGet (reference shard).
	raw, err := g.store.(storepkg.MetaKVCapability).MetaGet(compactedThroughTxMeta)
	if err != nil {
		t.Fatalf("MetaGet(watermark): %v", err)
	}
	if len(raw) != 8 {
		t.Fatalf("watermark on reference shard = %d bytes, want 8 (scattered to the event shard instead?)", len(raw))
	}

	// Scan-door gate fires below the watermark — only possible if the watermark
	// reached the reference shard.
	if _, err := g.Nodes.ByLabel("Signal", storepkg.QueryOpts{TxAt: tx[0]}); !errors.Is(err, ErrHistoryCompacted) {
		t.Fatalf("ByLabel{TxAt<wm} err=%v, want ErrHistoryCompacted", err)
	}
	// The per-entity stub is likewise readable on the reference shard.
	if _, err := g.Temporal.NodeAsOf(id, tx[1]); !errors.Is(err, ErrHistoryCompacted) {
		t.Fatalf("NodeAsOf(%d) err=%v, want ErrHistoryCompacted", tx[1], err)
	}
}

// TestCompaction_Tiered_ColdShardEntity compacts an event node whose history has
// aged onto a COLD shard (rule 14: rotate + demoteToCold, never a sub-second
// ShardWindow). The trim must check the owning cold shard out WRITABLE (brief
// risk 2).
func TestCompaction_Tiered_ColdShardEntity(t *testing.T) {
	t.Parallel()
	g, ts := newTestTieredGraph(t)
	ctx := context.Background()

	id, tx := buildLabeledNodeChain(t, g, "Signal", 4)

	ts.MuForTest().RLock()
	originName := ts.HotShardForTest().Name()
	ts.MuForTest().RUnlock()

	time.Sleep(2 * time.Millisecond)
	if err := ts.RotateHotShard(); err != nil {
		t.Fatalf("RotateHotShard: %v", err)
	}
	demoteToCold(ts, originName)

	rep, err := g.Admin.CompactHistoryNodes(ctx, RetentionPolicy{KeepVersions: 2})
	if err != nil {
		t.Fatalf("CompactHistoryNodes over cold shard: %v", err)
	}
	if rep.EntitiesCompacted != 1 || rep.VersionsTrimmed != 2 || rep.Watermark != tx[2] {
		t.Fatalf("report = %+v, want 1/2 watermark %d", rep, tx[2])
	}
	if hist, _ := g.Nodes.History(id); len(hist) != 2 {
		t.Fatalf("cold-shard history len = %d, want 2 (trim did not reach the cold shard)", len(hist))
	}
	if ok, err := g.Hash.VerifyNodeChain(id); err != nil || !ok {
		t.Fatalf("cold-shard VerifyNodeChain = (%v,%v)", ok, err)
	}
	if _, err := g.Temporal.NodeAsOf(id, tx[1]); !errors.Is(err, ErrHistoryCompacted) {
		t.Fatalf("cold-shard NodeAsOf(%d) err=%v, want ErrHistoryCompacted", tx[1], err)
	}
}

// TestCompaction_Tiered_ProtectedTagRefusal: a registered as-of tag pinning
// knowledge the policy would trim refuses the whole run (no writes), on tiered.
func TestCompaction_Tiered_ProtectedTagRefusal(t *testing.T) {
	t.Parallel()
	g, _ := newDiskTieredGraph(t)
	ctx := context.Background()

	id, tx := buildLabeledNodeChain(t, g, "Case", 4)
	if err := g.Temporal.TagAsOf("audit", tx[1]); err != nil {
		t.Fatalf("TagAsOf: %v", err)
	}
	if _, err := g.Admin.CompactHistoryNodes(ctx, RetentionPolicy{KeepVersions: 2}); !errors.Is(err, ErrCompactionProtectedTag) {
		t.Fatalf("CompactHistoryNodes err=%v, want ErrCompactionProtectedTag", err)
	}
	// No trim happened, no watermark advanced, tag still addressable.
	if hist, _ := g.Nodes.History(id); len(hist) != 4 {
		t.Fatalf("history len = %d, want 4 (no trim)", len(hist))
	}
	if types.Instant(g.compactedThroughTx.Load()) != 0 {
		t.Fatalf("watermark advanced despite protected-tag refusal")
	}
	if got, err := g.Temporal.NodeAsOf(id, tx[1]); err != nil || nodeState(t, got) != "v1" {
		t.Fatalf("NodeAsOf(tag) = (%v,%v), want v1", got, err)
	}
	// Untag → the same policy proceeds.
	if err := g.Temporal.RemoveAsOfTag("audit"); err != nil {
		t.Fatalf("RemoveAsOfTag: %v", err)
	}
	if _, err := g.Admin.CompactHistoryNodes(ctx, RetentionPolicy{KeepVersions: 2}); err != nil {
		t.Fatalf("CompactHistoryNodes after untag: %v", err)
	}
}

// TestCompaction_Tiered_TamperedStubFailsVerify: the stub lands on the reference
// shard (store-level MetaKV), so tampering it through the store-level MetaKV
// makes Verify*Chain fail closed — identical to the badger path.
func TestCompaction_Tiered_TamperedStubFailsVerify(t *testing.T) {
	t.Parallel()
	g, _ := newDiskTieredGraph(t)
	ctx := context.Background()

	id, _ := buildLabeledNodeChain(t, g, "Case", 4)
	if _, err := g.Admin.CompactHistoryNodes(ctx, RetentionPolicy{KeepVersions: 2}); err != nil {
		t.Fatalf("compact: %v", err)
	}
	if ok, err := g.Hash.VerifyNodeChain(id); err != nil || !ok {
		t.Fatalf("pre-tamper verify = (%v,%v)", ok, err)
	}

	mk := g.store.(storepkg.MetaKVCapability)
	key := compactStubNodeKey(id)

	// Valid-but-wrong boundary: decode succeeds, virtual-predecessor link breaks.
	wrong := compactionStub{
		EntityID:              int64(id.SnowflakeID()),
		TrimmedThroughVersion: 1,
		LastTrimmedHash:       "not-the-real-boundary",
		LastTrimmedTxTo:       1,
		CompactedAtTx:         1,
	}.sealed()
	wb, _ := wrong.encode()
	if err := mk.MetaSet(key, wb); err != nil {
		t.Fatalf("MetaSet: %v", err)
	}
	if ok, err := g.Hash.VerifyNodeChain(id); ok || err != nil {
		t.Fatalf("mismatched-boundary verify = (%v,%v), want (false,nil)", ok, err)
	}

	// Bit-flip: self-hash invalid → fails CLOSED with ErrCorruptWire.
	cur, _ := mk.MetaGet(key)
	cur[len(cur)-1] ^= 0xFF
	if err := mk.MetaSet(key, cur); err != nil {
		t.Fatalf("MetaSet: %v", err)
	}
	if ok, err := g.Hash.VerifyNodeChain(id); ok || !errors.Is(err, storepkg.ErrCorruptWire) {
		t.Fatalf("bit-flipped stub verify = (%v,%v), want (false, ErrCorruptWire)", ok, err)
	}
}
