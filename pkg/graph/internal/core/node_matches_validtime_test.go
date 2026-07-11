package core

import (
	"context"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TempOps.NodeMatchesValidTime is the NODE mirror of TempOps.RelMatchesValidTime
// (rule 17 — see TestRelMatchesValidTime_UnsetValidFromUsesSnowflakeFallback in
// foreach_adjacent_endpoint_at_test.go). Both delegate to the SAME canonical
// storeutil.MatchesTemporalFilter predicate built on storeutil.EntityValidFrom
// (explicit ValidFrom, else snowflake fallback) — this battery exercises the
// node door directly so a divergence between the two doors cannot hide behind
// "only the rel path was tested".

// TestNodeMatchesValidTime_NilNode asserts the documented safe default: a nil
// node never matches, with or without an active filter.
func TestNodeMatchesValidTime_NilNode(t *testing.T) {
	t.Parallel()
	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	if g.Temporal.NodeMatchesValidTime(nil, storepkg.QueryOpts{}) {
		t.Fatal("nil node with no filter must NOT match")
	}
	if g.Temporal.NodeMatchesValidTime(nil, storepkg.QueryOpts{ValidAt: types.Instant(1)}) {
		t.Fatal("nil node with a filter must NOT match")
	}
}

// TestNodeMatchesValidTime_ExplicitStamps mirrors the boundary case a
// query-engine post-filter relies on: an explicit tkg_valid_from/tkg_valid_to
// window is a HALF-OPEN interval [from, to) — from is inclusive, to is
// exclusive — per storeutil.MatchesPointInTime.
func TestNodeMatchesValidTime_ExplicitStamps(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	n, err := g.Nodes.Add(ctx, []string{"N"}, map[string]any{
		"tkg_valid_from": types.Instant(1000),
		"tkg_valid_to":   types.Instant(2000),
	})
	if err != nil {
		t.Fatalf("add node: %v", err)
	}

	for _, tc := range []struct {
		name string
		t    types.Instant
		want bool
	}{
		{"before from", 999, false},
		{"at from (inclusive)", 1000, true},
		{"mid-interval", 1500, true},
		{"just before to", 1999, true},
		{"at to (exclusive)", 2000, false},
		{"after to", 2001, false},
	} {
		got := g.Temporal.NodeMatchesValidTime(n, storepkg.QueryOpts{ValidAt: tc.t})
		if got != tc.want {
			t.Errorf("%s: NodeMatchesValidTime(t=%d) = %v, want %v", tc.name, tc.t, got, tc.want)
		}
	}

	// No temporal filter always matches, regardless of the entity's window.
	if !g.Temporal.NodeMatchesValidTime(n, storepkg.QueryOpts{}) {
		t.Fatal("no temporal filter must match")
	}
}

// TestNodeMatchesValidTime_UnsetValidFromUsesSnowflakeFallback is the direct
// node mirror of TestRelMatchesValidTime_UnsetValidFromUsesSnowflakeFallback:
// a node created WITHOUT an explicit tkg_valid_from must use the EFFECTIVE
// valid-from (snowflake creation time), never the raw shadow value (which
// would read as "valid since epoch").
func TestNodeMatchesValidTime_UnsetValidFromUsesSnowflakeFallback(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	// No tkg_valid_from in props — ValidFrom stays 0, so the effective
	// valid-from is the node's snowflake creation time (≈ now, well after 1970).
	n, err := g.Nodes.Add(ctx, []string{"N"}, nil)
	if err != nil {
		t.Fatalf("add node: %v", err)
	}

	if g.Temporal.NodeMatchesValidTime(n, storepkg.QueryOpts{ValidAt: types.Instant(1)}) {
		t.Fatal("node with unset valid_from must NOT be valid at t=1 (raw-epoch semantics leaked — should use snowflake fallback)")
	}
	if !g.Temporal.NodeMatchesValidTime(n, storepkg.QueryOpts{ValidAt: types.Instant(1 << 62)}) {
		t.Fatal("open-ended node must be valid at a far-future time")
	}
	if !g.Temporal.NodeMatchesValidTime(n, storepkg.QueryOpts{}) {
		t.Fatal("no temporal filter must match")
	}
}

// TestNodeMatchesValidTime_DeletedNode: a hard Delete stamps DeletedAt/ValidTo
// on the FINAL history version (append-only tombstone — see Version History
// design rule). NodeMatchesValidTime, given that tombstone row, must still
// report "valid" for instants inside its former window and "not valid" at or
// after the deletion instant — the predicate does not special-case deletion,
// it just reads ValidTo like any other close.
func TestNodeMatchesValidTime_DeletedNode(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	n, err := g.Nodes.Add(ctx, []string{"N"}, map[string]any{
		"tkg_valid_from": types.Instant(1000),
	})
	if err != nil {
		t.Fatalf("add node: %v", err)
	}
	id := n.ID()

	if err := g.Nodes.Delete(ctx, id); err != nil {
		t.Fatalf("delete node: %v", err)
	}

	history, err := g.Nodes.History(id)
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if len(history) == 0 {
		t.Fatal("deleted node must retain history (append-only)")
	}
	tombstone := history[len(history)-1]
	tm := tombstone.Temporal()
	if tm == nil || tm.DeletedAt == 0 || tm.ValidTo == 0 {
		t.Fatalf("tombstone must carry DeletedAt/ValidTo, got %+v", tm)
	}
	deletedAt := tm.ValidTo

	if !g.Temporal.NodeMatchesValidTime(tombstone, storepkg.QueryOpts{ValidAt: deletedAt - 1}) {
		t.Fatal("deleted node must still match just BEFORE its deletion instant")
	}
	if g.Temporal.NodeMatchesValidTime(tombstone, storepkg.QueryOpts{ValidAt: deletedAt}) {
		t.Fatal("deleted node must NOT match AT its deletion instant (to is exclusive)")
	}
	if g.Temporal.NodeMatchesValidTime(tombstone, storepkg.QueryOpts{ValidAt: deletedAt + 1000}) {
		t.Fatal("deleted node must NOT match well after its deletion instant")
	}
	// The window still held before deletion, inside [1000, deletedAt).
	if !g.Temporal.NodeMatchesValidTime(tombstone, storepkg.QueryOpts{ValidAt: types.Instant(1000)}) {
		t.Fatal("deleted node must match at its original valid_from")
	}
}

// TestNodeMatchesValidTime_FrozenNode: a plural/scan read (ByLabel) returns a
// shared FROZEN pointer (Defensive Copying design rule). NodeMatchesValidTime
// must accept a frozen row exactly like a mutable one — it only reads.
func TestNodeMatchesValidTime_FrozenNode(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	_, err = g.Nodes.Add(ctx, []string{"Frozen"}, map[string]any{
		"tkg_valid_from": types.Instant(1000),
		"tkg_valid_to":   types.Instant(2000),
	})
	if err != nil {
		t.Fatalf("add node: %v", err)
	}

	rows, err := g.Nodes.ByLabel("Frozen", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("ByLabel: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("ByLabel returned %d rows, want 1", len(rows))
	}
	frozen := rows[0]
	if !frozen.IsFrozen() {
		t.Fatal("ByLabel must return a frozen row")
	}

	if !g.Temporal.NodeMatchesValidTime(frozen, storepkg.QueryOpts{ValidAt: types.Instant(1500)}) {
		t.Fatal("frozen node inside its window must match")
	}
	if g.Temporal.NodeMatchesValidTime(frozen, storepkg.QueryOpts{ValidAt: types.Instant(2000)}) {
		t.Fatal("frozen node at its exclusive to-boundary must NOT match")
	}
	// Reading a frozen row via the predicate must not itself mutate/panic.
	if !frozen.IsFrozen() {
		t.Fatal("frozen row must remain frozen after being read by NodeMatchesValidTime")
	}
}

// TestNodeMatchesValidTime_ParityWithRelMatchesValidTime: for a node and a
// relationship stamped with IDENTICAL explicit valid-time windows, the two
// facades must agree at every probed instant — the required parity assertion
// (they share the same underlying storeutil.MatchesTemporalFilter call).
func TestNodeMatchesValidTime_ParityWithRelMatchesValidTime(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	stamps := map[string]any{
		"tkg_valid_from": types.Instant(5000),
		"tkg_valid_to":   types.Instant(6000),
	}

	n, err := g.Nodes.Add(ctx, []string{"P"}, stamps)
	if err != nil {
		t.Fatalf("add node: %v", err)
	}

	a, err := g.Nodes.Add(ctx, []string{"Endpoint"}, nil)
	if err != nil {
		t.Fatalf("add endpoint a: %v", err)
	}
	b, err := g.Nodes.Add(ctx, []string{"Endpoint"}, nil)
	if err != nil {
		t.Fatalf("add endpoint b: %v", err)
	}
	r, err := g.Rels.Add(ctx, "LINK", a, b, stamps)
	if err != nil {
		t.Fatalf("add rel: %v", err)
	}

	for _, at := range []types.Instant{4999, 5000, 5500, 5999, 6000, 6001} {
		nodeMatch := g.Temporal.NodeMatchesValidTime(n, storepkg.QueryOpts{ValidAt: at})
		relMatch := g.Temporal.RelMatchesValidTime(r, storepkg.QueryOpts{ValidAt: at})
		if nodeMatch != relMatch {
			t.Errorf("t=%d: NodeMatchesValidTime=%v, RelMatchesValidTime=%v — must agree for identical stamps", at, nodeMatch, relMatch)
		}
	}

	// No filter: both must match unconditionally.
	if g.Temporal.NodeMatchesValidTime(n, storepkg.QueryOpts{}) != g.Temporal.RelMatchesValidTime(r, storepkg.QueryOpts{}) {
		t.Fatal("no-filter parity: node/rel facades must agree")
	}
}

// TestNodeMatchesValidTime_ExplicitCreatedAtDoesNotShiftEffectiveValidFrom
// pins the canonical-vs-shadow divergence a downstream engine hit when it
// mirrored the effective-valid-from rule via the tkg_created_at shadow: for a
// node with an EXPLICIT tkg_created_at and an UNSET tkg_valid_from, the shadow
// resolver prefers the stored CreatedAt, but the canonical
// storeutil.EntityValidFrom rule (which this door delegates to) IGNORES
// CreatedAt and falls back to the snowflake ID stamp. A consumer replacing its
// shadow-based mirror with this door must see the snowflake rule.
func TestNodeMatchesValidTime_ExplicitCreatedAtDoesNotShiftEffectiveValidFrom(t *testing.T) {
	t.Parallel()
	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	// Explicit created_at far in the past; valid_from left UNSET so the
	// effective valid-from must come from the snowflake ID stamp (now-ish).
	const explicitCreatedAt = types.Instant(1_000_000) // 1970-01-12, unambiguous past
	n, err := g.Nodes.Add(context.Background(), []string{"CreatedAtDivergence"}, map[string]any{
		"tkg_created_at": explicitCreatedAt,
	})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}

	// Sanity: the shadow door reports the explicit created_at (the rule the
	// consumer used to mirror — and the one this door must NOT follow).
	if got, ok := g.Resolve.NodeProperty(n, "tkg_created_at"); !ok || got != explicitCreatedAt {
		t.Fatalf("shadow tkg_created_at = %v, %v; want %v, true", got, ok, explicitCreatedAt)
	}

	// A probe AT the explicit created_at must NOT match: the canonical
	// effective valid-from is the snowflake stamp, which post-dates it.
	if g.Temporal.NodeMatchesValidTime(n, storepkg.QueryOpts{ValidAt: explicitCreatedAt}) {
		t.Fatal("probe at explicit tkg_created_at matched — the door is following the shadow CreatedAt rule instead of canonical EntityValidFrom (snowflake fallback)")
	}
	// A probe at/after the snowflake stamp must match (node alive now).
	sfStamp := storeutil.EntityValidFrom(n.ID().SnowflakeID(), n.Temporal())
	if !g.Temporal.NodeMatchesValidTime(n, storepkg.QueryOpts{ValidAt: sfStamp}) {
		t.Fatal("probe at the snowflake-derived effective valid-from must match")
	}
}
