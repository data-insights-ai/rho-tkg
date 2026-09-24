package core

import (
	"context"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BACKLOG 10e/10i: temporal_cascade.go's inserted-row PrevHash used to be
// documented as linking to "whichever row it directly supersedes on the VT
// axis," but the implementation linked to the "template" row (the most
// recent non-eclipsed version) — the row the inserted row's content was
// copied from. Query correctness is unaffected either way (verifyChainLinkage
// only requires PrevHash to match SOME hash present anywhere in the entity's
// chain — see temporal_cascade.go's file header).
//
// Since the patch-over-then-valid-state fix, a correction row's content is
// copied from its BASE — the pre-correction belief-winner over its piece of
// the interval — and PrevHash follows the content: it is the base row's hash.
// Only a gap piece (no version valid there) still uses the template as base,
// and so links to the template's hash. This test pins both rules for a
// mid-history insertion and a gap insertion, and that the chain verifies.
func TestCascade_MidHistoryInsertion_PrevHashLinksToBase(t *testing.T) {
	g := newTxTimeGraph(t)
	clk := useTestClock(t, g)

	// Two-tile timeline: [1000, 3000) A, [3000, ∞) C (current).
	n, err := g.Nodes.Add(context.Background(), []string{"L"}, map[string]any{
		"tkg_valid_from": types.Instant(1000),
		"state":          "A",
	})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	clk.Advance(time.Millisecond)
	if _, err := g.Nodes.Update(context.Background(), n.ID(), map[string]any{
		"tkg_valid_from": types.Instant(3000),
		"state":          "C",
	}); err != nil {
		t.Fatalf("update to C: %v", err)
	}

	// Capture A's hash (the then-valid base of the [1500,2500) correction)
	// and C's hash (the template — most recent non-eclipsed — used for a gap
	// piece) exactly as they stand right before the cascade.
	preHistory, err := g.Nodes.History(n.ID())
	if err != nil {
		t.Fatalf("History before cascade: %v", err)
	}
	var baseHash string
	for _, h := range preHistory {
		if v, ok := h.GetProperty("state"); ok && v == "A" && h.Integrity() != nil {
			baseHash = h.Integrity().Hash
		}
	}
	if baseHash == "" {
		t.Fatal("no hashed state=A history row before cascade")
	}
	beforeCascade, err := g.Nodes.Get(context.Background(), n.ID())
	if err != nil {
		t.Fatalf("Get before cascade: %v", err)
	}
	ig := beforeCascade.Integrity()
	if ig == nil || ig.Hash == "" {
		t.Fatalf("node has no integrity hash before cascade: %+v", ig)
	}
	templateHash := ig.Hash

	clk.Advance(time.Millisecond)
	if _, err := g.Temporal.SetNodeVersionInterval(context.Background(), n.ID(), 1500, 2500, map[string]any{
		"state": "B",
	}); err != nil {
		t.Fatalf("cascade insert B: %v", err)
	}

	// Find the inserted "B" row in history and assert its PrevHash equals
	// its base's (A's) hash — the row it corrects.
	history, err := g.Nodes.History(n.ID())
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	var bRow *types.Node
	for _, h := range history {
		if v, ok := h.GetProperty("state"); ok && v == "B" {
			bRow = h
			break
		}
	}
	if bRow == nil {
		t.Fatalf("no history row with state=B found in %d entries", len(history))
	}
	bIG := bRow.Integrity()
	if bIG == nil {
		t.Fatal("B row has no integrity block")
	}
	if bIG.PrevHash != baseHash {
		t.Fatalf("B row PrevHash = %q, want base (A's) hash %q", bIG.PrevHash, baseHash)
	}

	// Gap piece: [100, 500) lies before the entity's first valid-from, so no
	// version is valid there and the template (C) is the base.
	clk.Advance(time.Millisecond)
	gap, err := g.Temporal.SetNodeVersionInterval(context.Background(), n.ID(), 100, 500, map[string]any{
		"state": "G",
	})
	if err != nil {
		t.Fatalf("cascade insert G (gap): %v", err)
	}
	if gIG := gap.Integrity(); gIG == nil || gIG.PrevHash != templateHash {
		t.Fatalf("gap row integrity = %+v, want PrevHash = template (C's) hash %q", gap.Integrity(), templateHash)
	}

	// The chain must still verify — PrevHash pointing at the template (not a
	// true VT-axis predecessor) is a documented-safe choice per
	// verifyChainLinkage's "matches SOME hash in the chain" contract.
	valid, err := g.Hash.VerifyNodeChain(n.ID())
	if err != nil {
		t.Fatalf("VerifyNodeChain: %v", err)
	}
	if !valid {
		t.Fatal("VerifyNodeChain = false after cascade, want true")
	}
}
