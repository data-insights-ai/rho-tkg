package core

import (
	"context"
	"errors"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ADR-0008 R1 — retention watermark + ErrRetentionExpired fail-closed read
// plumbing (no purge yet; the guard lands before the thing it guards). The
// two-door invariant (ADR §2.2): a POINT door checks the queried entity's label
// watermark; a SCAN door fails the whole scan when the pin precedes the graph
// max watermark. A pin at/above the watermark, or no pin, is never rejected.

func nodeLabelToken(t *testing.T, g *Core, label string) uint16 {
	t.Helper()
	tok, err := g.getOrCreateLabelPersisted(label)
	if err != nil {
		t.Fatalf("resolve label %q: %v", label, err)
	}
	return tok
}

func TestRetention_TwoDoorWatermark(t *testing.T) {
	g := newTxTimeGraph(t)
	ctx := context.Background()

	// Two entities: nL labeled L (will get a watermark), nM labeled M (won't).
	nL, err := g.Nodes.Add(ctx, []string{"L"}, map[string]any{"x": int64(1)})
	if err != nil {
		t.Fatalf("add nL: %v", err)
	}
	nM, err := g.Nodes.Add(ctx, []string{"M"}, map[string]any{"x": int64(2)})
	if err != nil {
		t.Fatalf("add nM: %v", err)
	}

	// No watermark yet: every temporal read answers normally (fast gate).
	if _, err := g.Temporal.NodeAtTx(nL.ID(), 10_000, 5_000); err != nil && !errors.Is(err, storepkg.ErrNoVersionValidAt) {
		t.Fatalf("pre-watermark NodeAtTx: %v", err)
	}

	// Purge boundary for L at watermark W = 5000 (transaction-time pin axis).
	const W = types.Instant(5000)
	lTok := nodeLabelToken(t, g, "L")
	if err := g.advanceRetentionWatermark(lTok, W); err != nil {
		t.Fatalf("advanceRetentionWatermark: %v", err)
	}

	// --- POINT door (per-label) ---
	// A pin BELOW L's watermark fails closed for an L entity.
	if _, err := g.Temporal.NodeAtTx(nL.ID(), 10_000, 4_999); !errors.Is(err, ErrRetentionExpired) {
		t.Fatalf("NodeAtTx(nL, txAt<W) = %v, want ErrRetentionExpired", err)
	}
	// A pin AT/ABOVE the watermark is fine (fast gate).
	if _, err := g.Temporal.NodeAtTx(nL.ID(), 10_000, 5_000); err != nil && !errors.Is(err, storepkg.ErrNoVersionValidAt) {
		t.Fatalf("NodeAtTx(nL, txAt==W) = %v, want no retention error", err)
	}
	// PER-LABEL precision: nM has no watermark, so a below-W pin does NOT reject
	// it at the point door (only its own label's watermark matters).
	if _, err := g.Temporal.NodeAtTx(nM.ID(), 10_000, 4_999); errors.Is(err, ErrRetentionExpired) {
		t.Fatalf("NodeAtTx(nM, txAt<W) wrongly rejected — nM's label M has no watermark")
	}
	// No pin (txAt==0) is never rejected.
	if _, err := g.Temporal.NodeAtTx(nL.ID(), 10_000, 0); err != nil && !errors.Is(err, storepkg.ErrNoVersionValidAt) {
		t.Fatalf("NodeAtTx(nL, txAt==0) = %v, want no retention error", err)
	}

	// --- SCAN door (whole-scan, graph max) ---
	// A scan pinned below the graph max fails closed (some purged entity missing).
	if _, err := g.Temporal.NodesAsOf(4_999); !errors.Is(err, ErrRetentionExpired) {
		t.Fatalf("NodesAsOf(pin<W) = %v, want ErrRetentionExpired", err)
	}
	// A scan at/above the max answers.
	if _, err := g.Temporal.NodesAsOf(5_000); err != nil {
		t.Fatalf("NodesAsOf(pin==W) = %v, want no retention error", err)
	}
	// The generic QueryOpts scan door is guarded via validateTemporalQueryOptsScan.
	if _, err := g.Nodes.All(storepkg.QueryOpts{TxAt: 4_999}); !errors.Is(err, ErrRetentionExpired) {
		t.Fatalf("All{TxAt<W} = %v, want ErrRetentionExpired", err)
	}
	if _, err := g.Nodes.ByLabel("M", storepkg.QueryOpts{ValidAt: 4_999}); !errors.Is(err, ErrRetentionExpired) {
		t.Fatalf("ByLabel(M){ValidAt<W} = %v, want ErrRetentionExpired (whole-scan graph max)", err)
	}
	// No pin → generic scan answers.
	if _, err := g.Nodes.All(storepkg.QueryOpts{}); err != nil {
		t.Fatalf("All{} unpinned = %v, want no error", err)
	}
}

// The watermark advances max-monotonically and never lowers the graph max.
func TestRetention_WatermarkMonotonic(t *testing.T) {
	g := newTxTimeGraph(t)
	lTok := nodeLabelToken(t, g, "L")
	if err := g.advanceRetentionWatermark(lTok, 8000); err != nil {
		t.Fatalf("advance 8000: %v", err)
	}
	// A lower advance does not lower the max.
	if err := g.advanceRetentionWatermark(lTok, 3000); err != nil {
		t.Fatalf("advance 3000: %v", err)
	}
	if got := g.retentionMaxWatermark.Load(); got != 8000 {
		t.Fatalf("max = %d, want 8000 (monotonic)", got)
	}
	if wm, _ := g.retentionWatermarkForLabel(lTok); wm != 8000 {
		t.Fatalf("label watermark = %d, want 8000 (monotonic)", wm)
	}
	// A read below 8000 still fails; between 3000 and 8000 too (max unchanged).
	if _, err := g.Temporal.NodesAsOf(7999); !errors.Is(err, ErrRetentionExpired) {
		t.Fatalf("NodesAsOf(7999) = %v, want ErrRetentionExpired", err)
	}
}

// The graph retention watermark is durable: it rehydrates from MetaKV at open so
// a reopened store still fails closed below the boundary (loadRetentionWatermark).
func TestRetention_PersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	g, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	lTok := nodeLabelToken(t, g, "L")
	if err := g.advanceRetentionWatermark(lTok, 6000); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	g2, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer g2.Close()
	if got := g2.retentionMaxWatermark.Load(); got != 6000 {
		t.Fatalf("rehydrated max = %d, want 6000", got)
	}
	if _, err := g2.Temporal.NodesAsOf(5000); !errors.Is(err, ErrRetentionExpired) {
		t.Fatalf("post-reopen NodesAsOf(5000) = %v, want ErrRetentionExpired", err)
	}
	// The per-label watermark also survived (point door still guards).
	if wm, _ := g2.retentionWatermarkForLabel(lTok); wm != 6000 {
		t.Fatalf("post-reopen label watermark = %d, want 6000", wm)
	}
}

// Reset clears the graph retention watermark so a reset graph does not inherit a
// stale watermark that spuriously fails temporal reads.
func TestRetention_ResetClears(t *testing.T) {
	g := newTxTimeGraph(t)
	lTok := nodeLabelToken(t, g, "L")
	if err := g.advanceRetentionWatermark(lTok, 5000); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if _, err := g.Temporal.NodesAsOf(4000); !errors.Is(err, ErrRetentionExpired) {
		t.Fatalf("pre-reset NodesAsOf(4000) = %v, want ErrRetentionExpired", err)
	}
	if err := g.Admin.Reset(); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	if got := g.retentionMaxWatermark.Load(); got != 0 {
		t.Fatalf("post-reset max = %d, want 0", got)
	}
	if _, err := g.Temporal.NodesAsOf(4000); err != nil {
		t.Fatalf("post-reset NodesAsOf(4000) = %v, want no error", err)
	}
}
