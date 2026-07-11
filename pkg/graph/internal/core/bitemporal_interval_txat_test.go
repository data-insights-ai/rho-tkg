package core

import (
	"context"
	"math"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// QueryOpts.ValidStart/ValidEnd (interval) combined with QueryOpts.TxAt
// regression coverage. v4.11.1 fixed the delete-tombstone bug at the shared
// filter{Node,Rel}ChainByTxAt seam (lesson 60), but no test in the repo
// combined an interval filter with TxAt — only ValidAt+TxAt
// (TestDeletedNodeValidAtPlusTxAt) and TxAt-only were covered. This file
// closes that gap: TEST 1/2 re-run the delete-tombstone scenario through
// interval queries (ByLabel, All, ByType); TEST 3 is a non-delete two-version
// interval+TxAt belief-state check.
//
// Reuses txFromStamp / farFuturePin / assertNodeSet / assertRelSet from
// bitemporal_tombstone_test.go and findings_regression_test.go (same
// package) — see that file's header for the two test-clock hazards. Our
// interval queries always supply an explicit ValidEnd (never 0), so they
// never take the "valid at wall now" resolveOpenEndInstant(0) branch that
// motivates waitWallPast* in the TxAt-only tests; no extra waits are needed
// here.

// nodeTombstoneValidTo returns the ValidTo instant stamped on the sole
// tombstoned history row a hard Delete leaves behind for a node that was
// never updated before deletion. Reading it back — instead of hardcoding the
// delete's own instant — keeps the test independent of exactly how many
// c.now() ticks the create/delete path happens to consume.
func nodeTombstoneValidTo(t *testing.T, g *Core, id types.NodeID) types.Instant {
	t.Helper()
	hist, err := g.Nodes.History(id)
	if err != nil {
		t.Fatalf("History(%v): %v", id, err)
	}
	if len(hist) != 1 {
		t.Fatalf("History(%v) = %d rows, want 1 tombstone row", id, len(hist))
	}
	tm := hist[0].Temporal()
	if tm == nil || tm.ValidTo == 0 {
		t.Fatalf("tombstone row has no ValidTo stamp: %+v", tm)
	}
	return tm.ValidTo
}

// relTombstoneValidTo is the relationship counterpart of nodeTombstoneValidTo.
func relTombstoneValidTo(t *testing.T, g *Core, id types.RelID) types.Instant {
	t.Helper()
	hist, err := g.Rels.History(id)
	if err != nil {
		t.Fatalf("History(%v): %v", id, err)
	}
	if len(hist) != 1 {
		t.Fatalf("History(%v) = %d rows, want 1 tombstone row", id, len(hist))
	}
	tm := hist[0].Temporal()
	if tm == nil || tm.ValidTo == 0 {
		t.Fatalf("tombstone row has no ValidTo stamp: %+v", tm)
	}
	return tm.ValidTo
}

// TestDeletedNodeIntervalPlusTxAt is TEST 1: an interval query
// (ValidStart/ValidEnd) combined with TxAt through the generic Nodes.ByLabel
// door. A node deleted after the pin closes its own [ValidFrom, D) window,
// where D is the tombstone's ValidTo. A window starting exactly AT D must:
//   - MATCH at a pin taken BEFORE the delete — the tombstone is not yet
//     recorded, so filterNodeChainByTxAt normalizes the row back to open
//     [5, inf), the belief AS OF that pin (rule 17 / lesson 60);
//   - NOT match at a pin taken AFTER the delete, or unpinned — the
//     tombstone's real ValidTo=D is visible, and [D, MaxInt64) starts
//     exactly where the now-closed [5, D) interval ends (exclusive) — no
//     overlap.
//
// Runs on both backends (rule 3): memory and badger differ in history
// retrieval and native as-of capability, and the chain rows the fix must not
// mutate are frozen store rows.
func TestDeletedNodeIntervalPlusTxAt(t *testing.T) {
	t.Parallel()
	for name, cfg := range map[string]Config{
		"memory": {},
		"badger": {BadgerInMemory: true},
	} {
		t.Run(name, func(t *testing.T) {
			testDeletedNodeIntervalPlusTxAt(t, cfg)
		})
	}
}

func testDeletedNodeIntervalPlusTxAt(t *testing.T, cfg Config) {
	g, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	ctx := context.Background()

	// Adversarial control (rule 16): same label, a short CLOSED window that
	// can never overlap [D, MaxInt64) for any tombstone instant D — proves
	// the query discriminates by interval, not "any node with label T".
	if _, err := g.Nodes.Add(ctx, []string{"T"}, map[string]any{
		"tkg_valid_from": types.Instant(1), "tkg_valid_to": types.Instant(2), "state": "control",
	}); err != nil {
		t.Fatalf("add control: %v", err)
	}

	n, err := g.Nodes.Add(ctx, []string{"T"}, map[string]any{
		"tkg_valid_from": types.Instant(5), "state": "alive",
	})
	if err != nil {
		t.Fatalf("add: %v", err)
	}
	pinBefore := txFromStamp(t, n.Temporal())

	if err := g.Nodes.Delete(ctx, n.ID()); err != nil {
		t.Fatalf("delete: %v", err)
	}
	d := nodeTombstoneValidTo(t, g, n.ID())
	after := farFuturePin()

	opts := storepkg.QueryOpts{ValidStart: d, ValidEnd: math.MaxInt64}

	pinnedBefore := opts
	pinnedBefore.TxAt = pinBefore
	rows, err := g.Nodes.ByLabel("T", pinnedBefore)
	if err != nil {
		t.Fatalf("ByLabel(interval, TxAt=pinBefore): %v", err)
	}
	assertNodeSet(t, "pinBefore", rows, []types.NodeID{n.ID()})

	pinnedAfter := opts
	pinnedAfter.TxAt = after
	rows, err = g.Nodes.ByLabel("T", pinnedAfter)
	if err != nil {
		t.Fatalf("ByLabel(interval, TxAt=after): %v", err)
	}
	assertNodeSet(t, "after", rows, nil)

	rows, err = g.Nodes.ByLabel("T", opts)
	if err != nil {
		t.Fatalf("ByLabel(interval, unpinned): %v", err)
	}
	assertNodeSet(t, "unpinned", rows, nil)
}

// TestDeletedNodeIntervalPlusTxAt_All is TEST 2 (node mirror): the same
// tombstone-interval scenario as TestDeletedNodeIntervalPlusTxAt, through the
// generic Nodes.All door instead of ByLabel. All has no label predicate
// (pred == nil in allNodesLocked), exercising the "any known node"
// resolution path through the same findNodeVersionForOpts /
// filterNodeChainByTxAt seam.
func TestDeletedNodeIntervalPlusTxAt_All(t *testing.T) {
	t.Parallel()
	for name, cfg := range map[string]Config{
		"memory": {},
		"badger": {BadgerInMemory: true},
	} {
		t.Run(name, func(t *testing.T) {
			testDeletedNodeIntervalPlusTxAt_All(t, cfg)
		})
	}
}

func testDeletedNodeIntervalPlusTxAt_All(t *testing.T, cfg Config) {
	g, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	ctx := context.Background()

	if _, err := g.Nodes.Add(ctx, []string{"T"}, map[string]any{
		"tkg_valid_from": types.Instant(1), "tkg_valid_to": types.Instant(2), "state": "control",
	}); err != nil {
		t.Fatalf("add control: %v", err)
	}

	n, err := g.Nodes.Add(ctx, []string{"T"}, map[string]any{
		"tkg_valid_from": types.Instant(5), "state": "alive",
	})
	if err != nil {
		t.Fatalf("add: %v", err)
	}
	pinBefore := txFromStamp(t, n.Temporal())

	if err := g.Nodes.Delete(ctx, n.ID()); err != nil {
		t.Fatalf("delete: %v", err)
	}
	d := nodeTombstoneValidTo(t, g, n.ID())
	after := farFuturePin()

	opts := storepkg.QueryOpts{ValidStart: d, ValidEnd: math.MaxInt64}

	pinnedBefore := opts
	pinnedBefore.TxAt = pinBefore
	rows, err := g.Nodes.All(pinnedBefore)
	if err != nil {
		t.Fatalf("All(interval, TxAt=pinBefore): %v", err)
	}
	assertNodeSet(t, "pinBefore", rows, []types.NodeID{n.ID()})

	pinnedAfter := opts
	pinnedAfter.TxAt = after
	rows, err = g.Nodes.All(pinnedAfter)
	if err != nil {
		t.Fatalf("All(interval, TxAt=after): %v", err)
	}
	assertNodeSet(t, "after", rows, nil)

	rows, err = g.Nodes.All(opts)
	if err != nil {
		t.Fatalf("All(interval, unpinned): %v", err)
	}
	assertNodeSet(t, "unpinned", rows, nil)
}

// TestDeletedRelIntervalPlusTxAt_ByType is TEST 2 (relationship
// mirror): the same tombstone-interval scenario as
// TestDeletedNodeIntervalPlusTxAt, through the generic Rels.ByType door
// (testing rule 2 — Node and Relationship are structural mirrors).
func TestDeletedRelIntervalPlusTxAt_ByType(t *testing.T) {
	t.Parallel()
	for name, cfg := range map[string]Config{
		"memory": {},
		"badger": {BadgerInMemory: true},
	} {
		t.Run(name, func(t *testing.T) {
			testDeletedRelIntervalPlusTxAt_ByType(t, cfg)
		})
	}
}

func testDeletedRelIntervalPlusTxAt_ByType(t *testing.T, cfg Config) {
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

	// Adversarial control (rule 16): same type, reverse direction, a short
	// CLOSED window that can never overlap [D, MaxInt64).
	if _, err := g.Rels.Add(ctx, "R", b, a, map[string]any{
		"tkg_valid_from": types.Instant(1), "tkg_valid_to": types.Instant(2), "state": "control",
	}); err != nil {
		t.Fatalf("add control rel: %v", err)
	}

	rel, err := g.Rels.Add(ctx, "R", a, b, map[string]any{
		"tkg_valid_from": types.Instant(5), "state": "alive",
	})
	if err != nil {
		t.Fatalf("add rel: %v", err)
	}
	pinBefore := txFromStamp(t, rel.Temporal())

	if err := g.Rels.Delete(ctx, rel.ID()); err != nil {
		t.Fatalf("delete rel: %v", err)
	}
	d := relTombstoneValidTo(t, g, rel.ID())
	after := farFuturePin()

	opts := storepkg.QueryOpts{ValidStart: d, ValidEnd: math.MaxInt64}

	pinnedBefore := opts
	pinnedBefore.TxAt = pinBefore
	rows, err := g.Rels.ByType("R", pinnedBefore)
	if err != nil {
		t.Fatalf("ByType(interval, TxAt=pinBefore): %v", err)
	}
	assertRelSet(t, "pinBefore", rows, []types.RelID{rel.ID()})

	pinnedAfter := opts
	pinnedAfter.TxAt = after
	rows, err = g.Rels.ByType("R", pinnedAfter)
	if err != nil {
		t.Fatalf("ByType(interval, TxAt=after): %v", err)
	}
	assertRelSet(t, "after", rows, nil)

	rows, err = g.Rels.ByType("R", opts)
	if err != nil {
		t.Fatalf("ByType(interval, unpinned): %v", err)
	}
	assertRelSet(t, "unpinned", rows, nil)
}

// TestIntervalPlusTxAt_TwoVersionBelief is TEST 3 (non-delete): a
// two-phase interval+TxAt belief-state check. v1 is created with an EXPLICIT
// closed valid-time window [10,20) (both tkg_valid_from and tkg_valid_to set
// at creation — a closed current version cannot be mutated via Update,
// rejectClosedNodeMutation, so a plain Update cannot append v2). A later
// SetNodeVersionInterval cascade appends v2 = [20, open) as a new, later
// version of the SAME entity (append-only: v1's own stored row is untouched
// by the cascade). The pin is taken BETWEEN the two writes, so at that pin
// only v1 is TX-visible.
func TestIntervalPlusTxAt_TwoVersionBelief(t *testing.T) {
	t.Parallel()
	for name, cfg := range map[string]Config{
		"memory": {},
		"badger": {BadgerInMemory: true},
	} {
		t.Run(name, func(t *testing.T) {
			testIntervalPlusTxAt_TwoVersionBelief(t, cfg)
		})
	}
}

func testIntervalPlusTxAt_TwoVersionBelief(t *testing.T, cfg Config) {
	g, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	ctx := context.Background()

	// Adversarial control (rule 16): a closed window entirely outside the
	// test's [10,30) time domain — must never appear in either result.
	if _, err := g.Nodes.Add(ctx, []string{"T"}, map[string]any{
		"tkg_valid_from": types.Instant(1), "tkg_valid_to": types.Instant(2), "state": "control",
	}); err != nil {
		t.Fatalf("add control: %v", err)
	}

	n, err := g.Nodes.Add(ctx, []string{"T"}, map[string]any{
		"tkg_valid_from": types.Instant(10), "tkg_valid_to": types.Instant(20), "state": "v1",
	})
	if err != nil {
		t.Fatalf("add v1: %v", err)
	}
	pin := txFromStamp(t, n.Temporal())

	if _, err := g.Temporal.SetNodeVersionInterval(ctx, n.ID(), 20, 0, map[string]any{"state": "v2"}); err != nil {
		t.Fatalf("SetNodeVersionInterval: %v", err)
	}

	// [12,15) falls entirely inside v1's own explicit [10,20) window — matches
	// at the pin, which TX-sees only v1 (v2's cascade write postdates it).
	rows, err := g.Nodes.ByLabel("T", storepkg.QueryOpts{ValidStart: 12, ValidEnd: 15, TxAt: pin})
	if err != nil {
		t.Fatalf("ByLabel([12,15), TxAt=pin): %v", err)
	}
	assertNodeSet(t, "[12,15)@pin", rows, []types.NodeID{n.ID()})

	// [25,30): derived by hand. At the pin, only v1 is TX-visible (v2 was
	// appended by a LATER cascade write, so filterNodeChainByTxAt excludes
	// it). v1's OWN stored row already carries an EXPLICIT ValidTo=20 — set
	// at creation, not inferred from v2's existence — so nodeVersionBounds'
	// unconditional "tm.ValidTo != 0 => vEnd = tm.ValidTo" override closes v1
	// at 20 regardless of whether v2 is in the TX-filtered chain. [25,30)
	// starts strictly after 20, so vEnd(20) > start(25) is false: no
	// overlap. The query must return empty — NOT the open-ended match a
	// "v1 tiles to v2's ValidFrom" assumption would (wrongly) predict for a
	// pin where v2 isn't even visible yet.
	rows, err = g.Nodes.ByLabel("T", storepkg.QueryOpts{ValidStart: 25, ValidEnd: 30, TxAt: pin})
	if err != nil {
		t.Fatalf("ByLabel([25,30), TxAt=pin): %v", err)
	}
	assertNodeSet(t, "[25,30)@pin", rows, nil)
}

// TestIntervalPlusTxAt_TwoVersionBelief_Rel is the relationship mirror of
// TestIntervalPlusTxAt_TwoVersionBelief (testing rule 2).
func TestIntervalPlusTxAt_TwoVersionBelief_Rel(t *testing.T) {
	t.Parallel()
	for name, cfg := range map[string]Config{
		"memory": {},
		"badger": {BadgerInMemory: true},
	} {
		t.Run(name, func(t *testing.T) {
			testIntervalPlusTxAt_TwoVersionBelief_Rel(t, cfg)
		})
	}
}

func testIntervalPlusTxAt_TwoVersionBelief_Rel(t *testing.T, cfg Config) {
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

	// Adversarial control (rule 16): reverse direction, a closed window
	// entirely outside the test's [10,30) time domain.
	if _, err := g.Rels.Add(ctx, "R", b, a, map[string]any{
		"tkg_valid_from": types.Instant(1), "tkg_valid_to": types.Instant(2), "state": "control",
	}); err != nil {
		t.Fatalf("add control rel: %v", err)
	}

	rel, err := g.Rels.Add(ctx, "R", a, b, map[string]any{
		"tkg_valid_from": types.Instant(10), "tkg_valid_to": types.Instant(20), "state": "v1",
	})
	if err != nil {
		t.Fatalf("add v1 rel: %v", err)
	}
	pin := txFromStamp(t, rel.Temporal())

	if _, err := g.Temporal.SetRelVersionInterval(ctx, rel.ID(), 20, 0, map[string]any{"state": "v2"}); err != nil {
		t.Fatalf("SetRelVersionInterval: %v", err)
	}

	rows, err := g.Rels.ByType("R", storepkg.QueryOpts{ValidStart: 12, ValidEnd: 15, TxAt: pin})
	if err != nil {
		t.Fatalf("ByType([12,15), TxAt=pin): %v", err)
	}
	assertRelSet(t, "[12,15)@pin", rows, []types.RelID{rel.ID()})

	// See the node mirror's derivation comment: v1's own row carries an
	// explicit ValidTo=20, so [25,30) does not overlap regardless of v2's
	// TX-visibility at this pin.
	rows, err = g.Rels.ByType("R", storepkg.QueryOpts{ValidStart: 25, ValidEnd: 30, TxAt: pin})
	if err != nil {
		t.Fatalf("ByType([25,30), TxAt=pin): %v", err)
	}
	assertRelSet(t, "[25,30)@pin", rows, nil)
}
