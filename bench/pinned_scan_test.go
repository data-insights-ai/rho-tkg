package bench

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// M1 — PINNED-SCAN SCALING (tasks/measurements-2026-07-11.md).
//
// Hypothesis under test: a TxPin/AsOf ByLabel scan costs O(everything that
// ever carried ANY history), because candidate collection
// (forEachNodeCandidateIDByDepth, temporal.go) folds in every node-history ID
// in the graph — not just history for the queried label — and then resolves
// a full version chain per candidate (findNodeVersionForOpts ->
// nodeAsOfLocked), while a plain ByLabel with no temporal filter answers
// straight from the store's label index in O(current matches). See
// queries.go's nodesByLabelLocked: hasTemporalFilter(opts) is the fork point.
//
// BadgerInMemory only — the mechanism under test lives entirely above the
// Store interface (core-layer candidate fold + chain resolution), so it is
// backend-agnostic; Badger is used because it is the disk-shaped backend
// actually deployed downstream, and running the memory backend too would
// double the (already sizable, up to 100k x 5-version) fixture-build cost
// for no additional signal.

// pinnedScanEntityCounts is the WP's {10k, 100k} entity-count axis.
var pinnedScanEntityCounts = []int{10_000, 100_000}

// churnProfile is the WP's version-churn axis: V1 (no history at all — the
// baseline showing candidate-fold cost with ZERO history to fold), V5 (every
// entity superseded 4 times — every entity contributes a history row,
// regardless of label), and V5D (V5 plus a hard-delete tombstone on 20% of
// each label group, exercising the delete-tombstone belief-state path).
type churnProfile struct {
	name           string
	versions       int     // total versions per entity (1 = create only, no Update).
	deleteFraction float64 // fraction of each label group hard-deleted at the end. 0 = none.
}

var pinnedScanChurnProfiles = []churnProfile{
	{name: "V1", versions: 1, deleteFraction: 0},
	{name: "V5", versions: 5, deleteFraction: 0},
	{name: "V5D", versions: 5, deleteFraction: 0.20},
}

// labelSelectivity is the WP's two label-selectivity fixtures: broad (every
// entity carries L — the plain ByLabel door's best case) and selective (only
// 1% of entities carry L, the rest carry M — the planner-relevant case where
// a healthy engine should cost O(1% of entityCount), and the O(everything)
// hypothesis predicts it instead costs O(entityCount)).
type labelSelectivity struct {
	name      string
	fractionL float64
}

var pinnedScanSelectivities = []labelSelectivity{
	{name: "broad", fractionL: 1.0},
	{name: "selective", fractionL: 0.01},
}

const (
	pinnedScanLabelL = "PinL"
	pinnedScanLabelM = "PinM"
)

// pinnedScanFixture bundles the built graph, the transaction-time pin
// (captured strictly after every fixture write), and the expected match
// count shared by all four measurements run against it.
type pinnedScanFixture struct {
	g *graph.Graph
	// pin is g.Temporal().NowTx() captured AFTER every write below (including
	// the hard-delete pass), so the belief state at pin is identical to the
	// live current state for every entity — no supersession in this fixture
	// changes an entity's label, and a hard-deleted entity is excluded from
	// BOTH the current label index and the as-of belief state at a pin taken
	// after its delete (asof_select.go's retraction rule). That invariant is
	// what makes "expected" a single shared number across all four doors: it
	// isolates COST from RESULT-SET SIZE, so the ns/op comparison is
	// apples-to-apples rather than confounded by differing result sizes.
	pin types.Instant
	// expected is the live/as-of label-L match count: countL - (L entities
	// hard-deleted).
	expected int
	// isL is the full label-L id set (deleted or not) — used to filter
	// g.Temporal().NodesAsOf's un-label-scoped result down to "carries L".
	isL map[types.NodeID]struct{}
}

// deleteEveryKth hard-deletes every k-th id (k = round(1/fraction), id 0
// first) via g.Nodes().Delete — a deterministic, reproducible selection
// (not random) so re-running the fixture build always hard-deletes the same
// entities. Returns the number of ids deleted.
func deleteEveryKth(tb testing.TB, g *graph.Graph, ctx context.Context, ids []types.NodeID, fraction float64) int {
	tb.Helper()
	if fraction <= 0 {
		return 0
	}
	k := int(math.Round(1 / fraction))
	if k < 1 {
		k = 1
	}
	deleted := 0
	for i := 0; i < len(ids); i += k {
		if err := g.Nodes().Delete(ctx, ids[i]); err != nil {
			tb.Fatalf("delete node %d: %v", ids[i], err)
		}
		deleted++
	}
	return deleted
}

// buildPinnedScanFixture creates entityCount nodes split between labels L
// and M per sel.fractionL, advances every entity through churn.versions
// versions via standalone g.Nodes().Update (Update is not batchable — see
// bench/temporal_test.go's buildValidTimeChain/buildTxTimeChain precedent),
// optionally hard-deletes churn.deleteFraction of EACH label group, then
// captures the transaction-time pin after every write completes.
func buildPinnedScanFixture(tb testing.TB, bc backendCase, entityCount int, churn churnProfile, sel labelSelectivity) *pinnedScanFixture {
	tb.Helper()
	ctx := benchCtx()
	g := newBenchGraph(tb, bc)

	countL := int(math.Round(float64(entityCount) * sel.fractionL))
	if countL > entityCount {
		countL = entityCount
	}
	countM := entityCount - countL

	idsL := make([]types.NodeID, 0, countL)
	for i := 0; i < countL; i++ {
		n, err := g.Nodes().Add(ctx, []string{pinnedScanLabelL}, map[string]any{"seq": i, "v": 0})
		if err != nil {
			tb.Fatalf("add L node %d: %v", i, err)
		}
		idsL = append(idsL, n.ID())
	}
	idsM := make([]types.NodeID, 0, countM)
	for i := 0; i < countM; i++ {
		n, err := g.Nodes().Add(ctx, []string{pinnedScanLabelM}, map[string]any{"seq": i, "v": 0})
		if err != nil {
			tb.Fatalf("add M node %d: %v", i, err)
		}
		idsM = append(idsM, n.ID())
	}

	if churn.versions > 1 {
		for v := 1; v < churn.versions; v++ {
			for _, id := range idsL {
				if _, err := g.Nodes().Update(ctx, id, map[string]any{"v": v}); err != nil {
					tb.Fatalf("update L node %d to v%d: %v", id, v, err)
				}
			}
			for _, id := range idsM {
				if _, err := g.Nodes().Update(ctx, id, map[string]any{"v": v}); err != nil {
					tb.Fatalf("update M node %d to v%d: %v", id, v, err)
				}
			}
		}
	}

	deletedL := deleteEveryKth(tb, g, ctx, idsL, churn.deleteFraction)
	deleteEveryKth(tb, g, ctx, idsM, churn.deleteFraction) // M-group deletes exercise the same fold cost; count unused.

	pin, err := g.Temporal().NowTx()
	if err != nil {
		tb.Fatalf("NowTx: %v", err)
	}

	isL := make(map[types.NodeID]struct{}, len(idsL))
	for _, id := range idsL {
		isL[id] = struct{}{}
	}

	return &pinnedScanFixture{g: g, pin: pin, expected: countL - deletedL, isL: isL}
}

// BenchmarkPinnedScanScaling is M1: for every (entityCount, churn,
// selectivity) fixture, measures all four doors named in the WP:
//
//	a_plain_bylabel      — g.Nodes().ByLabel(L, QueryOpts{})
//	b_bylabel_txpin      — g.Nodes().ByLabel(L, QueryOpts{TxPin: pin})
//	c_nodesasof_filtered — g.Temporal().NodesAsOf(pin), filtered to L (the door currently in use downstream)
//	d_bylabel_txat       — g.Nodes().ByLabel(L, QueryOpts{TxAt: pin})
//
// See tasks/measurements-2026-07-11.md for the ratio tables and verdict.
func BenchmarkPinnedScanScaling(b *testing.B) {
	bc := backendCase{name: "badger", snowflakeNode: 3, badger: true}
	for _, entityCount := range pinnedScanEntityCounts {
		for _, churn := range pinnedScanChurnProfiles {
			for _, sel := range pinnedScanSelectivities {
				name := fmt.Sprintf("%d/%s/%s", entityCount, churn.name, sel.name)
				b.Run(name, func(b *testing.B) {
					fx := buildPinnedScanFixture(b, bc, entityCount, churn, sel)

					b.Run("a_plain_bylabel", func(b *testing.B) {
						b.ReportAllocs()
						for b.Loop() {
							nodes, err := fx.g.Nodes().ByLabel(pinnedScanLabelL, storepkg.QueryOpts{})
							if err != nil {
								b.Fatalf("ByLabel: %v", err)
							}
							if len(nodes) != fx.expected {
								b.Fatalf("got %d nodes, want %d", len(nodes), fx.expected)
							}
						}
					})

					b.Run("b_bylabel_txpin", func(b *testing.B) {
						b.ReportAllocs()
						for b.Loop() {
							nodes, err := fx.g.Nodes().ByLabel(pinnedScanLabelL, storepkg.QueryOpts{TxPin: fx.pin})
							if err != nil {
								b.Fatalf("ByLabel TxPin: %v", err)
							}
							if len(nodes) != fx.expected {
								b.Fatalf("got %d nodes, want %d", len(nodes), fx.expected)
							}
						}
					})

					b.Run("c_nodesasof_filtered", func(b *testing.B) {
						b.ReportAllocs()
						for b.Loop() {
							nodes, err := fx.g.Temporal().NodesAsOf(fx.pin)
							if err != nil {
								b.Fatalf("NodesAsOf: %v", err)
							}
							matched := 0
							for _, node := range nodes {
								if _, ok := fx.isL[node.ID()]; ok {
									matched++
								}
							}
							if matched != fx.expected {
								b.Fatalf("got %d matched, want %d", matched, fx.expected)
							}
						}
					})

					// d_bylabel_txat deliberately does NOT assert len(nodes) ==
					// fx.expected. QueryOpts.TxAt alone (no ValidAt) resolves each
					// candidate via nodeAtLockedTx(id, nowInstant()-1, txAt) —
					// nowInstant() is the REAL wall clock (time.Now, opts.go's
					// documented "implicit valid-at-wall-now filter"), while every
					// TxFrom/UpdatedAt/DeletedAt stamp in this fixture comes from
					// c.now()'s MONOTONIC-FLOOR clock, which force-advances by
					// >=1ms per call whenever two mutations land in the same real
					// millisecond (context.go). A tight loop issuing thousands of
					// mutations in microseconds each — exactly this fixture's
					// build shape — makes the monotonic floor race tens of
					// seconds ahead of the real wall clock, so at query time
					// nowInstant() is far BEHIND most of this fixture's own
					// TxFrom/UpdatedAt timestamps. The practical effect (see
					// tasks/measurements-2026-07-11.md): every entity's superseded
					// GENESIS version (whose vStart falls back to the real-clock
					// snowflake-ID timestamp, not a monotonic-floor stamp) ends up
					// "covering" the real-wall-clock probe regardless of later
					// updates OR hard deletes, so this door returns the FULL
					// entity count for V5/V5D fixtures alike — a live illustration
					// of the exact footgun QueryOpts.TxAt's doc comment warns
					// about, not a benchmark bug. ns/op and allocs are still a
					// valid, comparable cost measurement; only the result-set
					// size is untrustworthy under this tight-loop artifact.
					b.Run("d_bylabel_txat", func(b *testing.B) {
						b.ReportAllocs()
						for b.Loop() {
							nodes, err := fx.g.Nodes().ByLabel(pinnedScanLabelL, storepkg.QueryOpts{TxAt: fx.pin})
							if err != nil {
								b.Fatalf("ByLabel TxAt: %v", err)
							}
							if len(nodes) == 0 || len(nodes) > len(fx.isL) {
								b.Fatalf("got %d nodes, want a value in (0, %d]", len(nodes), len(fx.isL))
							}
						}
					})
				})
			}
		}
	}
}
