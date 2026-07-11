package core

import (
	"context"
	"errors"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// compactionBackends is the memory + badger matrix. Badger differs from memory
// in history storage and the native as-of reverse scan, and the compaction batch
// commits through a real WriteBatch there.
func compactionBackends() map[string]Config {
	return map[string]Config{
		"memory": {},
		"badger": {BadgerInMemory: true},
	}
}

// buildNodeChain adds a node labeled "T" with state "v0" and applies `updates`
// updates (state "v1".."vN"), returning the node id and the ascending TxFrom of
// every version v0..vN (history ‖ current). Asserts TxFroms strictly increase so
// the transaction-time boundaries are well separated.
func buildNodeChain(t *testing.T, g *Core, updates int) (types.NodeID, []types.Instant) {
	t.Helper()
	ctx := context.Background()
	n, err := g.Nodes.Add(ctx, []string{"T"}, map[string]any{"state": "v0"})
	if err != nil {
		t.Fatalf("add: %v", err)
	}
	id := n.ID()
	for i := 1; i <= updates; i++ {
		if _, err := g.Nodes.Update(ctx, id, map[string]any{"state": "v" + itoa(i)}); err != nil {
			t.Fatalf("update %d: %v", i, err)
		}
	}
	return id, nodeChainTxFroms(t, g, id)
}

func nodeChainTxFroms(t *testing.T, g *Core, id types.NodeID) []types.Instant {
	t.Helper()
	hist, err := g.Nodes.History(id)
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	cur, err := g.Nodes.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	out := make([]types.Instant, 0, len(hist)+1)
	for _, v := range hist {
		out = append(out, v.Temporal().TxFrom)
	}
	out = append(out, cur.Temporal().TxFrom)
	for i := 1; i < len(out); i++ {
		if out[i] <= out[i-1] {
			t.Fatalf("TxFroms not strictly increasing: %v", out)
		}
	}
	return out
}

func itoa(i int) string {
	return string(rune('0' + i))
}

func nodeState(t *testing.T, n *types.Node) string {
	t.Helper()
	v, ok := n.PropertiesMap()["state"].(string)
	if !ok {
		t.Fatalf("node %v has no string state", n.ID())
	}
	return v
}

// TestCompactHistoryNodes_EndToEnd covers the node path: report, trimmed
// history, stub-aware verify, two-phase exact reads above the watermark, and the
// ErrHistoryCompacted signal at both point and scan doors below it.
func TestCompactHistoryNodes_EndToEnd(t *testing.T) {
	t.Parallel()
	for name, cfg := range compactionBackends() {
		t.Run(name, func(t *testing.T) {
			g, err := New(cfg)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			defer g.Close()
			ctx := context.Background()

			id, tx := buildNodeChain(t, g, 4) // v0..v4; history v0..v3
			// A second, never-updated node: compaction is PER ENTITY, so its point
			// reads must not be poisoned by another entity's stub / the watermark.
			other, err := g.Nodes.Add(ctx, []string{"T"}, map[string]any{"state": "solo"})
			if err != nil {
				t.Fatalf("add other: %v", err)
			}
			highPin, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatalf("NowTx: %v", err)
			}

			// Keep newest 2 history versions (v2,v3) + current v4; trim v0,v1.
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

			// Per-entity precision: the uncompacted node has no stub, so a point
			// read below the watermark resolves normally (ErrNoVersionAsOf when the
			// node did not yet exist) — NEVER ErrHistoryCompacted.
			if _, err := g.Temporal.NodeAsOf(other.ID(), tx[1]); errors.Is(err, ErrHistoryCompacted) {
				t.Fatalf("uncompacted node NodeAsOf(low) spuriously returned ErrHistoryCompacted")
			}

			// History physically trimmed to 2 versions.
			hist, err := g.Nodes.History(id)
			if err != nil {
				t.Fatalf("history: %v", err)
			}
			if len(hist) != 2 {
				t.Fatalf("history len = %d, want 2", len(hist))
			}

			// Stub-aware verify still passes (virtual predecessor linkage).
			if ok, err := g.Hash.VerifyNodeChain(id); err != nil || !ok {
				t.Fatalf("VerifyNodeChain = (%v,%v), want (true,nil)", ok, err)
			}

			// Two-phase: reads AT/ABOVE the watermark stay exact after the trim.
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
			// Bitemporal point door (NodeAtTx) mirrors the signal.
			validNow := types.Instant(time.Now().UnixMilli()) + 100000
			if _, err := g.Temporal.NodeAtTx(id, validNow, tx[1]); !errors.Is(err, ErrHistoryCompacted) {
				t.Fatalf("NodeAtTx txAt=%d err=%v, want ErrHistoryCompacted", tx[1], err)
			}
			if got, err := g.Temporal.NodeAtTx(id, validNow, tx[3]); err != nil || nodeState(t, got) != "v3" {
				t.Fatalf("NodeAtTx txAt=%d = (%v,%v), want v3", tx[3], got, err)
			}

			// Scan doors: below watermark errors the whole scan; at/above works.
			if _, err := g.Temporal.NodesAsOf(tx[1]); !errors.Is(err, ErrHistoryCompacted) {
				t.Fatalf("NodesAsOf(%d) err=%v, want ErrHistoryCompacted", tx[1], err)
			}
			if got, err := g.Temporal.NodesAsOf(highPin); err != nil || len(got) != 2 {
				t.Fatalf("NodesAsOf(high) = (%v,%v), want 2 nodes", got, err)
			}
			if _, err := g.Nodes.ByLabel("T", storepkg.QueryOpts{TxPin: tx[1]}); !errors.Is(err, ErrHistoryCompacted) {
				t.Fatalf("ByLabel{TxPin<wm} err=%v, want ErrHistoryCompacted", err)
			}
			if _, err := g.Nodes.ByLabel("T", storepkg.QueryOpts{TxAt: tx[0]}); !errors.Is(err, ErrHistoryCompacted) {
				t.Fatalf("ByLabel{TxAt<wm} err=%v, want ErrHistoryCompacted", err)
			}
			// At/above the watermark the scan succeeds; the compacted node (lowest
			// ID, added first) still resolves to its newest belief v4.
			got, err := g.Nodes.ByLabel("T", storepkg.QueryOpts{TxPin: highPin})
			if err != nil || len(got) != 2 || nodeState(t, got[0]) != "v4" {
				t.Fatalf("ByLabel{TxPin=high} = (%v,%v), want 2 nodes with got[0]=v4", got, err)
			}
		})
	}
}

// buildRelChain adds two nodes + a "LINK" rel with state "v0", applies `updates`
// updates, and returns the rel id and the ascending TxFrom of every version.
func buildRelChain(t *testing.T, g *Core, updates int) (types.RelID, []types.Instant) {
	t.Helper()
	ctx := context.Background()
	a, err := g.Nodes.Add(ctx, []string{"N"}, nil)
	if err != nil {
		t.Fatalf("add a: %v", err)
	}
	b, err := g.Nodes.Add(ctx, []string{"N"}, nil)
	if err != nil {
		t.Fatalf("add b: %v", err)
	}
	r, err := g.Rels.Add(ctx, "LINK", a, b, map[string]any{"state": "v0"})
	if err != nil {
		t.Fatalf("add rel: %v", err)
	}
	id := r.ID()
	for i := 1; i <= updates; i++ {
		if _, err := g.Rels.Update(ctx, id, map[string]any{"state": "v" + itoa(i)}); err != nil {
			t.Fatalf("rel update %d: %v", i, err)
		}
	}
	hist, err := g.Rels.History(id)
	if err != nil {
		t.Fatalf("rel history: %v", err)
	}
	cur, err := g.Rels.Get(ctx, id)
	if err != nil {
		t.Fatalf("rel get: %v", err)
	}
	out := make([]types.Instant, 0, len(hist)+1)
	for _, v := range hist {
		out = append(out, v.Temporal().TxFrom)
	}
	out = append(out, cur.Temporal().TxFrom)
	for i := 1; i < len(out); i++ {
		if out[i] <= out[i-1] {
			t.Fatalf("rel TxFroms not strictly increasing: %v", out)
		}
	}
	return id, out
}

func relState(t *testing.T, r *types.Relationship) string {
	t.Helper()
	v, ok := r.PropertiesMap()["state"].(string)
	if !ok {
		t.Fatalf("rel %v has no string state", r.ID())
	}
	return v
}

// TestCompactHistoryRels_EndToEnd is the relationship mirror (Testing Rule 2).
func TestCompactHistoryRels_EndToEnd(t *testing.T) {
	t.Parallel()
	for name, cfg := range compactionBackends() {
		t.Run(name, func(t *testing.T) {
			g, err := New(cfg)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			defer g.Close()
			ctx := context.Background()

			id, tx := buildRelChain(t, g, 4)
			// A second, never-updated rel: compaction is PER ENTITY.
			na, _ := g.Nodes.Add(ctx, []string{"N"}, nil)
			nb, _ := g.Nodes.Add(ctx, []string{"N"}, nil)
			solo, err := g.Rels.Add(ctx, "LINK", na, nb, map[string]any{"state": "solo"})
			if err != nil {
				t.Fatalf("add solo rel: %v", err)
			}
			highPin, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatalf("NowTx: %v", err)
			}

			rep, err := g.Admin.CompactHistoryRels(ctx, RetentionPolicy{KeepVersions: 2})
			if err != nil {
				t.Fatalf("CompactHistoryRels: %v", err)
			}
			if rep.EntitiesCompacted != 1 || rep.VersionsTrimmed != 2 || rep.Watermark != tx[2] {
				t.Fatalf("report = %+v, want 1/2/%d", rep, tx[2])
			}

			hist, err := g.Rels.History(id)
			if err != nil || len(hist) != 2 {
				t.Fatalf("rel history len = %d (%v), want 2", len(hist), err)
			}
			if ok, err := g.Hash.VerifyRelChain(id); err != nil || !ok {
				t.Fatalf("VerifyRelChain = (%v,%v), want (true,nil)", ok, err)
			}

			for pin, want := range map[types.Instant]string{tx[2]: "v2", tx[3]: "v3", highPin: "v4"} {
				got, err := g.Temporal.RelAsOf(id, pin)
				if err != nil || relState(t, got) != want {
					t.Fatalf("RelAsOf(%d) = (%v,%v), want %q", pin, got, err, want)
				}
			}
			for _, pin := range []types.Instant{tx[0], tx[1]} {
				if _, err := g.Temporal.RelAsOf(id, pin); !errors.Is(err, ErrHistoryCompacted) {
					t.Fatalf("RelAsOf(%d) err=%v, want ErrHistoryCompacted", pin, err)
				}
			}
			// Per-entity precision: the uncompacted rel never returns ErrHistoryCompacted.
			if _, err := g.Temporal.RelAsOf(solo.ID(), tx[1]); errors.Is(err, ErrHistoryCompacted) {
				t.Fatalf("uncompacted rel RelAsOf(low) spuriously returned ErrHistoryCompacted")
			}
			validNow := types.Instant(time.Now().UnixMilli()) + 100000
			if _, err := g.Temporal.RelAtTx(id, validNow, tx[1]); !errors.Is(err, ErrHistoryCompacted) {
				t.Fatalf("RelAtTx txAt=%d err=%v, want ErrHistoryCompacted", tx[1], err)
			}
			if _, err := g.Temporal.RelsAsOf(tx[1]); !errors.Is(err, ErrHistoryCompacted) {
				t.Fatalf("RelsAsOf(%d) err=%v, want ErrHistoryCompacted", tx[1], err)
			}
			if got, err := g.Temporal.RelsAsOf(highPin); err != nil || len(got) != 2 {
				t.Fatalf("RelsAsOf(high) = (%v,%v), want 2 rels", got, err)
			}
			if _, err := g.Rels.ByType("LINK", storepkg.QueryOpts{TxPin: tx[1]}); !errors.Is(err, ErrHistoryCompacted) {
				t.Fatalf("ByType{TxPin<wm} err=%v, want ErrHistoryCompacted", err)
			}
		})
	}
}

// TestCompaction_ProtectedTagRefusal: a registered as-of tag pinning knowledge
// the policy would trim blocks compaction with ErrCompactionProtectedTag, and
// NOTHING is trimmed.
func TestCompaction_ProtectedTagRefusal(t *testing.T) {
	t.Parallel()
	for name, cfg := range compactionBackends() {
		t.Run(name, func(t *testing.T) {
			g, err := New(cfg)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			defer g.Close()
			ctx := context.Background()

			id, tx := buildNodeChain(t, g, 4)
			// Tag pins v1's knowledge — inside the range KeepVersions:2 would trim
			// (boundary = v2 TxFrom; tx[1] < boundary).
			if err := g.Temporal.TagAsOf("audit", tx[1]); err != nil {
				t.Fatalf("TagAsOf: %v", err)
			}
			if _, err := g.Admin.CompactHistoryNodes(ctx, RetentionPolicy{KeepVersions: 2}); !errors.Is(err, ErrCompactionProtectedTag) {
				t.Fatalf("CompactHistoryNodes err=%v, want ErrCompactionProtectedTag", err)
			}
			// No trim happened: full history intact, no watermark, tag still resolvable.
			if hist, _ := g.Nodes.History(id); len(hist) != 4 {
				t.Fatalf("history len = %d, want 4 (no trim)", len(hist))
			}
			if got, err := g.Temporal.NodeAsOf(id, tx[1]); err != nil || nodeState(t, got) != "v1" {
				t.Fatalf("NodeAsOf(tag) = (%v,%v), want v1 (still addressable)", got, err)
			}
			// Removing the tag lets the same policy proceed.
			if err := g.Temporal.RemoveAsOfTag("audit"); err != nil {
				t.Fatalf("RemoveAsOfTag: %v", err)
			}
			if _, err := g.Admin.CompactHistoryNodes(ctx, RetentionPolicy{KeepVersions: 2}); err != nil {
				t.Fatalf("CompactHistoryNodes after untag: %v", err)
			}
		})
	}
}

// TestCompaction_ChangeLogRefusal: compaction refuses while a change-log is
// enabled so no replica can silently diverge from a compacted primary.
func TestCompaction_ChangeLogRefusal(t *testing.T) {
	t.Parallel()
	g, err := New(Config{Store: memory.New(memory.WithChangeLog())})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	ctx := context.Background()
	buildNodeChain(t, g, 3)
	if _, err := g.Admin.CompactHistoryNodes(ctx, RetentionPolicy{KeepVersions: 1}); !errors.Is(err, ErrCompactionChangeLogEnabled) {
		t.Fatalf("node compaction err=%v, want ErrCompactionChangeLogEnabled", err)
	}
	if _, err := g.Admin.CompactHistoryRels(ctx, RetentionPolicy{KeepVersions: 1}); !errors.Is(err, ErrCompactionChangeLogEnabled) {
		t.Fatalf("rel compaction err=%v, want ErrCompactionChangeLogEnabled", err)
	}
}

// TestCompaction_CancelledContext: a cancelled context is honored before work.
func TestCompaction_CancelledContext(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	buildNodeChain(t, g, 3)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := g.Admin.CompactHistoryNodes(ctx, RetentionPolicy{KeepVersions: 1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("node compaction err=%v, want context.Canceled", err)
	}
	if _, err := g.Admin.CompactHistoryRels(ctx, RetentionPolicy{KeepVersions: 1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("rel compaction err=%v, want context.Canceled", err)
	}
}

// TestCompaction_NoMetaKVDeclines: a backend without MetaKV cannot persist the
// stub/watermark, so compaction declines with ErrCapabilityNotSupported.
func TestCompaction_NoMetaKVDeclines(t *testing.T) {
	t.Parallel()
	g, err := New(Config{Store: &noMetaStore{MandatoryStore: memory.New()}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	ctx := context.Background()
	if _, err := g.Admin.CompactHistoryNodes(ctx, RetentionPolicy{KeepVersions: 1}); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Fatalf("node compaction err=%v, want ErrCapabilityNotSupported", err)
	}
	if _, err := g.Admin.CompactHistoryRels(ctx, RetentionPolicy{KeepVersions: 1}); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Fatalf("rel compaction err=%v, want ErrCapabilityNotSupported", err)
	}
}

// TestCompaction_InvalidPolicy: an empty policy is refused before any work.
func TestCompaction_InvalidPolicy(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	ctx := context.Background()
	buildNodeChain(t, g, 2)
	if _, err := g.Admin.CompactHistoryNodes(ctx, RetentionPolicy{}); !errors.Is(err, ErrInvalidRetentionPolicy) {
		t.Fatalf("empty policy err=%v, want ErrInvalidRetentionPolicy", err)
	}
	if _, err := g.Admin.CompactHistoryRels(ctx, RetentionPolicy{KeepVersions: -3}); !errors.Is(err, ErrInvalidRetentionPolicy) {
		t.Fatalf("negative policy err=%v, want ErrInvalidRetentionPolicy", err)
	}
}

// TestCompaction_TamperedStubFailsVerify: corrupting the persisted stub makes
// Verify*Chain fail closed (the stub is the boundary trust anchor).
func TestCompaction_TamperedStubFailsVerify(t *testing.T) {
	t.Parallel()
	for name, cfg := range compactionBackends() {
		t.Run(name, func(t *testing.T) {
			g, err := New(cfg)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			defer g.Close()
			ctx := context.Background()
			id, _ := buildNodeChain(t, g, 4)
			if _, err := g.Admin.CompactHistoryNodes(ctx, RetentionPolicy{KeepVersions: 2}); err != nil {
				t.Fatalf("compact: %v", err)
			}
			if ok, err := g.Hash.VerifyNodeChain(id); err != nil || !ok {
				t.Fatalf("pre-tamper verify = (%v,%v), want (true,nil)", ok, err)
			}

			mk := g.store.(storepkg.MetaKVCapability)
			key := compactStubNodeKey(id)

			// 1) Overwrite the stub with a valid-but-wrong LastTrimmedHash (properly
			// self-hashed, so decode succeeds) — the virtual-predecessor link breaks.
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

			// 2) A raw bit-flip (self-hash no longer valid) fails CLOSED with an error.
			cur, _ := mk.MetaGet(key)
			cur[len(cur)-1] ^= 0xFF
			if err := mk.MetaSet(key, cur); err != nil {
				t.Fatalf("MetaSet: %v", err)
			}
			if ok, err := g.Hash.VerifyNodeChain(id); ok || !errors.Is(err, storepkg.ErrCorruptWire) {
				t.Fatalf("bit-flipped stub verify = (%v,%v), want (false, ErrCorruptWire)", ok, err)
			}
		})
	}
}

// Tiered compaction (Section 3.2 parity) lives in compaction_tiered_test.go.

// TestCompaction_ReopenDurability: after Close/reopen of a badger dir, the
// watermark, stub, trimmed history, verify verdict, and ErrHistoryCompacted
// signal all survive (rule 15 durability + two-phase).
func TestCompaction_ReopenDurability(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()

	var id types.NodeID
	var tx []types.Instant
	var highPin types.Instant
	func() {
		g, err := New(Config{BadgerDir: dir})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		defer g.Close()
		id, tx = buildNodeChain(t, g, 4)
		highPin, _ = g.Temporal.NowTx()
		if _, err := g.Admin.CompactHistoryNodes(ctx, RetentionPolicy{KeepVersions: 2}); err != nil {
			t.Fatalf("compact: %v", err)
		}
	}()

	g2, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer g2.Close()

	// Watermark reloaded into the atomic.
	if got := types.Instant(g2.compactedThroughTx.Load()); got != tx[2] {
		t.Fatalf("reloaded watermark = %d, want %d", got, tx[2])
	}
	if hist, _ := g2.Nodes.History(id); len(hist) != 2 {
		t.Fatalf("post-reopen history len = %d, want 2", len(hist))
	}
	if ok, err := g2.Hash.VerifyNodeChain(id); err != nil || !ok {
		t.Fatalf("post-reopen verify = (%v,%v), want (true,nil)", ok, err)
	}
	if got, err := g2.Temporal.NodeAsOf(id, highPin); err != nil || nodeState(t, got) != "v4" {
		t.Fatalf("post-reopen NodeAsOf(high) = (%v,%v), want v4", got, err)
	}
	if _, err := g2.Temporal.NodeAsOf(id, tx[1]); !errors.Is(err, ErrHistoryCompacted) {
		t.Fatalf("post-reopen NodeAsOf(%d) err=%v, want ErrHistoryCompacted", tx[1], err)
	}
}
