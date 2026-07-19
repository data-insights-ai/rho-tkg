package core

import (
	"context"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BACKLOG 10e/10i: temporal_cascade.go's inserted-row PrevHash used to be
// documented as linking to "whichever row it directly supersedes on the VT
// axis," but the implementation actually always links to the "template" row
// (the most-recent-non-eclipsed version chosen for label/property
// carry-over) — a DIFFERENT selection rule from true VT-axis lineage. Query
// correctness is unaffected (verifyChainLinkage only requires PrevHash to
// match SOME hash present anywhere in the entity's chain, not the
// VT-axis-adjacent one specifically — see temporal_cascade.go's file header
// for the full analysis), so this was a documentation-accuracy fix, not a
// behavior change: the doc comment was corrected to describe the actual
// "template" rule and explicitly warn against "fixing" it toward true
// VT-axis lineage without re-running the bitemporal oracle fuzz harness
// (BACKLOG 10b already proved that class of change is a correctness
// minefield in this exact file).
//
// This test pins the actual (correct, verification-passing) behavior for a
// mid-history insertion — the same scenario TestCascade_MidHistoryInsertion
// exercises for resolver correctness, extended to also inspect the inserted
// row's PrevHash — closing the "zero test coverage" gap 10e/10i flagged.
func TestCascade_MidHistoryInsertion_PrevHashLinksToTemplate(t *testing.T) {
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

	// Capture C's hash exactly as it stands right before the cascade — this
	// is the row the cascade will pick as "template" (most-recent-non-eclipsed;
	// the cascade interval [1500,2500) doesn't touch C's [3000,∞) interval).
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
	// the captured template (C's) hash — the documented (post-fix) rule.
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
	if bIG.PrevHash != templateHash {
		t.Fatalf("B row PrevHash = %q, want template (C's pre-cascade) hash %q — BACKLOG 10e regression", bIG.PrevHash, templateHash)
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
