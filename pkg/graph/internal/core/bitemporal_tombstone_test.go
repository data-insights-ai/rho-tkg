package core

import (
	"context"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Lesson 43 extended to DeletedAt: a hard Delete is a TRANSACTION-TIME
// tombstone, not retroactive history rewriting. A delete recorded after txAt
// must not affect what a pinned read at txAt returns — the entity existed in
// the belief state as of txAt. The named as-of door (NodeAsOf/NodesAsOf)
// already reconstructed this via normalizeTemporalVisibleAtTxTime; the
// generic QueryOpts.TxAt scan door dropped the entity because the tombstone's
// delete-stamped ValidTo failed valid-time coverage (rule 17: two doors, same
// shape). Found from the consumer side: sigma-tkgd's Tyla AS OF pinning
// probed 2026-07-03 that NodesAsOf resurrects while ByLabel{TxAt} does not.
//
// Timing discipline (two clock hazards, both observed as flakes here):
//   - Pins are derived from the entities' OWN TxFrom stamps, not wall-clock
//     sleeps — Core.now() carries a monotonic ≥1ms floor, so a burst of
//     mutations can outrun the wall clock and a slept wall-clock pin can land
//     BEFORE the last create's logical stamp. A pin at max(TxFrom of pre-pin
//     writes) is visible for all of them (TxFrom <= txAt), and every later
//     mutation is stamped strictly greater by the same floor.
//   - TxAt-only scans probe valid time at WALL now (resolveOpenEndInstant),
//     so while the monotonic-floor stamps are still ahead of the wall clock
//     the just-written stamps sit "in the future" and coverage flips as the
//     wall clock catches up. Tests wait until the wall clock passes every
//     stamp they minted (waitWallPast over the history rows) before asserting.

// txFromStamp returns the entity's TxFrom, failing the test if unstamped.
func txFromStamp(t *testing.T, tm *types.TemporalMetadata) types.Instant {
	t.Helper()
	if tm == nil || tm.TxFrom == 0 {
		t.Fatal("entity has no TxFrom stamp")
	}
	return tm.TxFrom
}

// maxInstant returns the maximum of the given instants.
func maxInstant(vals ...types.Instant) types.Instant {
	var m types.Instant
	for _, v := range vals {
		if v > m {
			m = v
		}
	}
	return m
}

// maxStamp returns the largest instant recorded on a version's temporal
// metadata (any of the four stamps a mutation can mint).
func maxStamp(tm *types.TemporalMetadata) types.Instant {
	if tm == nil {
		return 0
	}
	return maxInstant(tm.TxFrom, tm.TxTo, tm.DeletedAt, tm.UpdatedAt, tm.ValidTo)
}

// waitWallPast blocks until the wall clock is strictly past target, so a
// TxAt-only scan's implicit "valid at now" probe post-dates every minted stamp.
func waitWallPast(target types.Instant) {
	for types.Instant(time.Now().UnixMilli()) <= target {
		time.Sleep(time.Millisecond)
	}
}

// waitWallPastNodeHistory waits past every stamp on the node's history rows.
func waitWallPastNodeHistory(t *testing.T, g *Core, id types.NodeID) {
	t.Helper()
	hist, err := g.Nodes.History(id)
	if err != nil {
		t.Fatalf("History(%v): %v", id, err)
	}
	var target types.Instant
	for _, v := range hist {
		target = maxInstant(target, maxStamp(v.Temporal()))
	}
	waitWallPast(target)
}

// waitWallPastRelHistory waits past every stamp on the rel's history rows.
func waitWallPastRelHistory(t *testing.T, g *Core, id types.RelID) {
	t.Helper()
	hist, err := g.Rels.History(id)
	if err != nil {
		t.Fatalf("History(%v): %v", id, err)
	}
	var target types.Instant
	for _, v := range hist {
		target = maxInstant(target, maxStamp(v.Temporal()))
	}
	waitWallPast(target)
}

// farFuturePin returns a pin safely after every stamp minted in this test:
// mutation stamps stay within ~ms of the wall clock, so now+10s post-dates
// all of them without any sleep.
func farFuturePin() types.Instant {
	return types.Instant(time.Now().UnixMilli()) + 10_000
}

// Node deleted after the pin: visible through BOTH doors at the pre-delete
// pin, absent from both at a post-delete pin. Runs on both backends (rule 3):
// memory and badger differ in history retrieval and the native as-of
// capability, and the chain rows the fix must not mutate are frozen store rows.
func TestDeletedNodeVisibleAtPriorTxAt(t *testing.T) {
	t.Parallel()
	for name, cfg := range map[string]Config{
		"memory": {},
		"badger": {BadgerInMemory: true},
	} {
		t.Run(name, func(t *testing.T) {
			testDeletedNodeVisibleAtPriorTxAt(t, cfg)
		})
	}
}

func testDeletedNodeVisibleAtPriorTxAt(t *testing.T, cfg Config) {
	g, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	ctx := context.Background()

	n, err := g.Nodes.Add(ctx, []string{"T"}, map[string]any{"state": "alive"})
	if err != nil {
		t.Fatalf("add: %v", err)
	}
	pin := txFromStamp(t, n.Temporal())
	if err := g.Nodes.Delete(ctx, n.ID()); err != nil {
		t.Fatalf("delete: %v", err)
	}
	after := farFuturePin()
	waitWallPastNodeHistory(t, g, n.ID())

	// Generic door: label scan pinned before the delete must contain the node.
	rows, err := g.Nodes.ByLabel("T", storepkg.QueryOpts{TxAt: pin})
	if err != nil {
		t.Fatalf("ByLabel(TxAt=pin): %v", err)
	}
	if len(rows) != 1 || rows[0].ID() != n.ID() {
		t.Fatalf("ByLabel(TxAt=pin) = %d rows, want the deleted node (delete is a tx-time tombstone)", len(rows))
	}
	if v, _ := rows[0].GetProperty("state"); v != "alive" {
		t.Fatalf("pinned row state = %v, want alive", v)
	}
	// The returned row reflects the belief AS OF the pin: no delete stamps.
	if tm := rows[0].Temporal(); tm != nil && (tm.DeletedAt != 0 || tm.ValidTo != 0) {
		t.Fatalf("pinned row carries post-pin delete stamps: DeletedAt=%d ValidTo=%d, want 0/0", tm.DeletedAt, tm.ValidTo)
	}

	// Named door agrees (the two doors must not diverge).
	asof, err := g.Temporal.NodesAsOf(pin)
	if err != nil {
		t.Fatalf("NodesAsOf(pin): %v", err)
	}
	if len(asof) != 1 {
		t.Fatalf("NodesAsOf(pin) = %d rows, want 1", len(asof))
	}

	// Point door: bitemporal point query at (far-future VT, pre-delete pin) —
	// as known at the pin, the node was open-ended.
	at, err := g.Temporal.NodeAtTx(n.ID(), after, pin)
	if err != nil {
		t.Fatalf("NodeAtTx(future, pin): %v", err)
	}
	if v, _ := at.GetProperty("state"); v != "alive" {
		t.Fatalf("NodeAtTx(future, pin) state = %v, want alive", v)
	}

	// Post-delete pin: the delete IS recorded by then — must NOT contain it.
	rows, err = g.Nodes.ByLabel("T", storepkg.QueryOpts{TxAt: after})
	if err != nil {
		t.Fatalf("ByLabel(TxAt=after): %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("ByLabel(TxAt=after delete) = %d rows, want 0", len(rows))
	}
	if _, err := g.Temporal.NodeAtTx(n.ID(), after, after); err == nil {
		t.Fatal("NodeAtTx(future, after delete) returned a version, want none")
	}
}

// Two-phase with supersession: v0 → update v1 → pin → delete. The pinned scan
// must return the v1 content (the belief current at the pin), never v0 and
// never nothing.
func TestDeletedNodeAtPriorTxAt_AfterSupersession(t *testing.T) {
	t.Parallel()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	ctx := context.Background()

	n, err := g.Nodes.Add(ctx, []string{"T"}, map[string]any{"state": "v0"})
	if err != nil {
		t.Fatalf("add: %v", err)
	}
	updated, err := g.Nodes.Update(ctx, n.ID(), map[string]any{"state": "v1"})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	pin := txFromStamp(t, updated.Temporal())
	if err := g.Nodes.Delete(ctx, n.ID()); err != nil {
		t.Fatalf("delete: %v", err)
	}
	waitWallPastNodeHistory(t, g, n.ID())

	rows, err := g.Nodes.ByLabel("T", storepkg.QueryOpts{TxAt: pin})
	if err != nil {
		t.Fatalf("ByLabel(TxAt=pin): %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("ByLabel(TxAt=pin) = %d rows, want 1", len(rows))
	}
	if v, _ := rows[0].GetProperty("state"); v != "v1" {
		t.Fatalf("pinned row state = %v, want v1 (the belief current at pin)", v)
	}
}

// Relationship mirror (testing rule 2), including the node-cascade tombstone
// path: a rel deleted directly AND a rel tombstoned by DeleteNode's cascade
// must both stay visible at a pre-delete pin and vanish at a post-delete pin.
func TestDeletedRelVisibleAtPriorTxAt(t *testing.T) {
	t.Parallel()
	for name, cfg := range map[string]Config{
		"memory": {},
		"badger": {BadgerInMemory: true},
	} {
		t.Run(name, func(t *testing.T) {
			testDeletedRelVisibleAtPriorTxAt(t, cfg)
		})
	}
}

func testDeletedRelVisibleAtPriorTxAt(t *testing.T, cfg Config) {
	g, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	ctx := context.Background()

	a, err := g.Nodes.Add(ctx, []string{"T"}, nil)
	if err != nil {
		t.Fatalf("add a: %v", err)
	}
	b, err := g.Nodes.Add(ctx, []string{"T"}, nil)
	if err != nil {
		t.Fatalf("add b: %v", err)
	}
	c, err := g.Nodes.Add(ctx, []string{"T"}, nil)
	if err != nil {
		t.Fatalf("add c: %v", err)
	}
	direct, err := g.Rels.Add(ctx, "R", a, b, map[string]any{"kind": "direct"})
	if err != nil {
		t.Fatalf("add direct rel: %v", err)
	}
	cascade, err := g.Rels.Add(ctx, "R", b, c, map[string]any{"kind": "cascade"})
	if err != nil {
		t.Fatalf("add cascade rel: %v", err)
	}
	pin := maxInstant(
		txFromStamp(t, direct.Temporal()),
		txFromStamp(t, cascade.Temporal()),
	)

	if err := g.Rels.Delete(ctx, direct.ID()); err != nil {
		t.Fatalf("delete rel: %v", err)
	}
	// DeleteNode(c) cascades and tombstones the b->c rel.
	if err := g.Nodes.Delete(ctx, c.ID()); err != nil {
		t.Fatalf("delete node c: %v", err)
	}
	after := farFuturePin()
	waitWallPastRelHistory(t, g, direct.ID())
	waitWallPastRelHistory(t, g, cascade.ID())

	rows, err := g.Rels.ByType("R", storepkg.QueryOpts{TxAt: pin})
	if err != nil {
		t.Fatalf("ByType(TxAt=pin): %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("ByType(TxAt=pin) = %d rows, want both deleted rels (tx-time tombstone)", len(rows))
	}
	kinds := map[string]bool{}
	for _, r := range rows {
		if tm := r.Temporal(); tm != nil && (tm.DeletedAt != 0 || tm.ValidTo != 0) {
			t.Fatalf("pinned rel carries post-pin delete stamps: DeletedAt=%d ValidTo=%d", tm.DeletedAt, tm.ValidTo)
		}
		if v, ok := r.GetProperty("kind"); ok {
			kinds[v.(string)] = true
		}
	}
	if !kinds["direct"] || !kinds["cascade"] {
		t.Fatalf("pinned rels = %v, want both direct and cascade", kinds)
	}

	// Named door agrees.
	asof, err := g.Temporal.RelsAsOf(pin)
	if err != nil {
		t.Fatalf("RelsAsOf(pin): %v", err)
	}
	if len(asof) != 2 {
		t.Fatalf("RelsAsOf(pin) = %d rows, want 2", len(asof))
	}

	// Post-delete pin: both gone.
	rows, err = g.Rels.ByType("R", storepkg.QueryOpts{TxAt: after})
	if err != nil {
		t.Fatalf("ByType(TxAt=after): %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("ByType(TxAt=after) = %d rows, want 0", len(rows))
	}
}

// Bitemporal combination door: ValidAt + TxAt together. A node deleted after
// the pin must answer a (pre-delete VT, pre-delete pin) query through the
// generic scan with both filters set.
func TestDeletedNodeValidAtPlusTxAt(t *testing.T) {
	t.Parallel()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	ctx := context.Background()

	n, err := g.Nodes.Add(ctx, []string{"T"}, map[string]any{
		"tkg_valid_from": types.Instant(1000), "state": "alive",
	})
	if err != nil {
		t.Fatalf("add: %v", err)
	}
	pin := txFromStamp(t, n.Temporal())
	if err := g.Nodes.Delete(ctx, n.ID()); err != nil {
		t.Fatalf("delete: %v", err)
	}

	rows, err := g.Nodes.ByLabel("T", storepkg.QueryOpts{ValidAt: 1500, TxAt: pin})
	if err != nil {
		t.Fatalf("ByLabel(ValidAt=1500, TxAt=pin): %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("ByLabel(ValidAt+TxAt) = %d rows, want 1 (delete recorded after pin)", len(rows))
	}
}
