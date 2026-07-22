package core

import (
	"context"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestNodeAt_BeliefWatermark_DeclinesWhenHistoryOutranksCurrent is a direct,
// whitebox test of the invariant nodeCurrentAnswersAt relies on (BACKLOG
// 10c), constructed WITHOUT depending on finding a specific cascade call
// sequence that happens to leave a bounded history row "underneath" an
// untouched current row.
//
// Natural cascade sequences turn out to be hard to adversarially construct
// here: cascadeNodeVersionInterval's own resumption-boundary logic
// (nodeResumptionEnd) scans EVERY row in the pre-correction chain for the
// next boundary after newVT, which — for a single untouched, still-open
// current row — always yields either "no boundary" (open resumption, which
// then wins the own-open comparison and REPLACES current directly) or "the
// boundary IS current's own vStart" (which makes the correction's coverage
// and current's coverage disjoint by construction, so there is no actual
// overlap for a current-row-alone query to get wrong). That tiling
// discipline is exactly what the 10b fix is FOR.
//
// This test instead directly injects a history row via the store's raw
// PutNodeVersion door — bypassing cascadeNodeVersionInterval's own
// self-consistent tiling entirely — with a TxFrom higher than current's own,
// covering the SAME validAt current would otherwise answer for. This is a
// strictly MORE general proof than any one cascade sequence: it demonstrates
// the fast path's actual documented contract holds ("no history row,
// HOWEVER IT GOT THERE, can outrank current if the watermark says so") for
// any bytes-on-disk state, not merely ones cascadeNodeVersionInterval itself
// would ever produce (e.g., a future write path, a replica-apply edge case,
// or a bug elsewhere that this fast path must not compound into a second,
// deeper bug).
func TestNodeAt_BeliefWatermark_DeclinesWhenHistoryOutranksCurrent(t *testing.T) {
	t.Parallel()
	for name, cfg := range map[string]Config{
		"memory": {},
		"badger": {BadgerInMemory: true},
	} {
		t.Run(name, func(t *testing.T) {
			testNodeAtBeliefWatermarkDeclinesWhenHistoryOutranksCurrent(t, cfg)
		})
	}
}

func testNodeAtBeliefWatermarkDeclinesWhenHistoryOutranksCurrent(t *testing.T, cfg Config) {
	g, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	ctx := context.Background()

	n, err := g.Nodes.Add(ctx, []string{"L"}, map[string]any{
		"tkg_valid_from": types.Instant(1000),
		"state":          "A",
	})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	current, err := g.getCurrentNode(n.ID())
	if err != nil {
		t.Fatalf("getCurrentNode: %v", err)
	}
	txT0 := current.Temporal().TxFrom

	// Sanity: before any injection, the fast path correctly engages (current
	// is the only-ever-recorded belief).
	if !g.nodeCurrentAnswersAt(current, 2000, 0) {
		t.Fatalf("nodeCurrentAnswersAt = false before injection, want true")
	}

	// Directly inject a history row via the raw store door — a version whose
	// OWN interval [1500, +inf) OVERLAPS current's own coverage
	// ([1000, +inf)) at validAt=2000, with a STRICTLY NEWER TxFrom than
	// current. cascadeNodeVersionInterval would never itself produce exactly
	// this layout (its tiling logic prevents the overlap, as the doc comment
	// above explains) — this simulates "some other write path recorded a
	// newer belief in history without going through the current slot,"
	// exactly the class of event store.NodeBeliefWatermarkCapability exists
	// to detect regardless of its source.
	injected := current.DeepCopy()
	injected.SetVersion(current.Version() + 1)
	if err := injected.SetProperty("state", "INJECTED"); err != nil {
		t.Fatalf("SetProperty: %v", err)
	}
	injectedTx := txT0 + 1_000_000
	injected.Temporal().ValidFrom = 1500
	injected.Temporal().ValidTo = 0 // open, own bounds — deliberately overlaps current
	injected.Temporal().TxFrom = injectedTx
	injected.Temporal().UpdatedAt = injectedTx
	if err := g.store.PutNodeVersion(n.ID(), injected.Version(), injected); err != nil {
		t.Fatalf("PutNodeVersion (raw injection): %v", err)
	}

	// The watermark sidecar must now reflect the injected row's higher
	// TxFrom, and the fast path must DECLINE — current's own TxFrom (txT0)
	// no longer equals the entity's max-ever-recorded belief.
	watermark, ok := g.nodeBeliefWatermark.NodeBeliefWatermark(n.ID())
	if !ok {
		t.Fatalf("NodeBeliefWatermark: not found after injection")
	}
	if watermark != injectedTx {
		t.Fatalf("watermark = %d, want %d (the injected row's TxFrom)", watermark, injectedTx)
	}
	if g.nodeCurrentAnswersAt(current, 2000, 0) {
		t.Fatalf("nodeCurrentAnswersAt = true after injecting a higher-belief history row, want false (BACKLOG 10c: the fast path must decline whenever ANY history row could outrank current, regardless of how it got there)")
	}

	// And the full-chain resolver (which the declined fast path now falls
	// through to) must find the injected row, not current's stale state.
	got, err := g.Temporal.NodeAt(n.ID(), 2000)
	if err != nil {
		t.Fatalf("NodeAt(2000): %v", err)
	}
	if v, _ := got.GetProperty("state"); v != "INJECTED" {
		t.Fatalf("NodeAt(2000) state = %v, want INJECTED (the higher-belief history row must win)", v)
	}
}
