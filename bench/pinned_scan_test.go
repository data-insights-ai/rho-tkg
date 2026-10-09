package bench

import (
	"context"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
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
//
// K1 RESULT (transaction-time label-membership sidecar; badger, -benchtime 3x,
// the fixture pre-warms the lazy sidecar so ns/op is steady-state query cost):
//
//	100k/V5D/selective  b_bylabel_txpin  2069 ms / 18.2M allocs  ->  9.6 ms / 140k  (~216x)
//	100k/V5 /selective  b_bylabel_txpin  1010 ms /  5.9M allocs  ->  0.40 ms /  8k  (~2500x)
//	100k/V1 /selective  b_bylabel_txpin    27 ms                 ->  1.2 ms
//
// Selectivity now scales with MATCHES, not entity count. The fixed-N
// selective/broad ratio for b_bylabel_txpin collapsed from ~0.62-1.0 to
// selectivity-proportional: 100k/V5D 9.6/2421 = 0.004; 100k/V5 0.40/1197 =
// 0.0003; 100k/V1 1.2/541 = 0.002.
//
// Doors a_plain_bylabel (no temporal filter) and the ingest benches are
// unchanged: the sidecar is lazy (nil until the first pinned scan), so a graph
// that never pins pays a nil-guard no-op on the write path.
//
// c_nodesasof_filtered is UNCHANGED (~1.7 s at 100k/V5D): NodesAsOf(pin) is
// label-INDEPENDENT and its result set is every node live at the pin (~80k of
// 100k here), so its cost is Omega(result size) — a literal >=10x on this door
// is below the physical floor of materializing 80k rows (~0.3 s even for the
// plain label index). The intended >=10x win for the label-scoped as-of pattern
// is delivered by b_bylabel_txpin, which is the correct door for "L nodes as of
// pin" and is ~178x faster than the door-c NodesAsOf(pin)+filter pattern it
// replaces downstream.

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

	// K1 warm-up: trigger the lazy transaction-time membership sidecar build ONCE
	// here, OUTSIDE the timed loops, so the measured ns/op reflects steady-state
	// query cost — the sidecar is a lazily-built index and its one-time build is
	// amortized across queries (standard for index microbenchmarks; a graph that
	// never runs a pinned scan pays nothing). On the pre-K1 baseline this is a
	// harmless extra pinned scan during fixture build.
	if _, err := g.Nodes().ByLabel(pinnedScanLabelL, storepkg.QueryOpts{TxPin: pin}); err != nil {
		tb.Fatalf("warm-up ByLabel TxPin: %v", err)
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

// --- Backlog 8: pinned relationship property lookups ---
//
// BenchmarkPinnedRelPropertyLookup measures Rels().ByTypeAndProperty with a
// transaction-time pin (sigma-tkgd's seat-scoped lookup) against the same
// lookup without a pin, over N relationships spread across 1 or 5 types, for a
// value with 200 current matches ("seat") and a broad one carried by 5 % of
// the type ("grp"). Profiles:
//
//	nochurn       — every rel at its first version
//	sigma         — 20 % of the rels revised (seat moves to the next value) and
//	                5 % deleted, across all types (sigma's 1 M profile shape)
//	unrelated-x1  — N/10 revisions on the OTHER types only (5 types)
//	unrelated-x10 — N revisions on the other types only (10x the churn)
//
// The pinned sub-benchmark reports the lazy sidecar build of the first pinned
// lookup (build-ms); the timed loop is the steady state. Bytes per posting are
// measured on the structure itself (BenchmarkPropertyTxMembersBytesPerPosting in
// pkg/graph/internal/index): a heap delta around a badger lookup is noise.
// Fixture writes go through g.Batch() in groups.
//
// Default (the bench-gate canary, RHO_TKG_PINNED_REL_SIZES unset): 20 000 rels,
// memory and badger, every profile, matches200/{pinned,current} = 24 rows. The
// gate (bench-gate.awk) checks all of them on allocs/op (+10 %) and only the 8
// rows of 1type/sigma and 5types/unrelated-x10 on time: on a loaded runner the
// time of identical code swings tens of percent (sharded and broad most), while
// allocs/op does not move and the regression that matters — the lookup falling
// back to the history fold — multiplies it. Setting RHO_TKG_PINNED_REL_SIZES
// (comma list; the measured report used 100000,1000000) runs the full matrix:
// sharded too, and the broad lookup.

type pinnedRelProfile struct {
	name          string
	types         int
	reviseFrac    float64
	deleteFrac    float64
	unrelatedMult int // revisions on the non-target types, in units of N/10
}

var pinnedRelProfiles = []pinnedRelProfile{
	{name: "1type/nochurn", types: 1},
	{name: "1type/sigma", types: 1, reviseFrac: 0.20, deleteFrac: 0.05},
	{name: "5types/nochurn", types: 5},
	{name: "5types/sigma", types: 5, reviseFrac: 0.20, deleteFrac: 0.05},
	{name: "5types/unrelated-x1", types: 5, unrelatedMult: 1},
	{name: "5types/unrelated-x10", types: 5, unrelatedMult: 10},
}

func pinnedRelSizes(tb testing.TB) []int {
	raw := os.Getenv("RHO_TKG_PINNED_REL_SIZES")
	if raw == "" {
		return []int{20_000}
	}
	var out []int
	for _, f := range strings.Split(raw, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(f))
		if err != nil || n < 1000 {
			tb.Fatalf("RHO_TKG_PINNED_REL_SIZES: bad size %q", f)
		}
		out = append(out, n)
	}
	return out
}

type pinnedRelBackend struct {
	name string
	open func(tb testing.TB) *graph.Graph
}

func pinnedRelBackends() []pinnedRelBackend {
	open := func(tb testing.TB, cfg graph.Config) *graph.Graph {
		g, err := graph.New(cfg)
		if err != nil {
			tb.Fatalf("graph.New: %v", err)
		}
		tb.Cleanup(func() { _ = g.Close() })
		return g
	}
	return []pinnedRelBackend{
		{name: "memory", open: func(tb testing.TB) *graph.Graph { return open(tb, graph.Config{SnowflakeNodeID: 4}) }},
		{name: "badger", open: func(tb testing.TB) *graph.Graph {
			return open(tb, graph.Config{SnowflakeNodeID: 5, BadgerInMemory: true})
		}},
		{name: "sharded", open: func(tb testing.TB) *graph.Graph {
			st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
			if err != nil {
				tb.Fatalf("sharded.New: %v", err)
			}
			return open(tb, graph.Config{Store: st})
		}},
	}
}

type pinnedRelFixture struct {
	g        *graph.Graph
	pin      types.Instant
	target   string // the type the lookups ask for
	seatWant int    // current matches of seat=0
	grpWant  int    // current matches of grp=0
	buildMs  float64
}

func buildPinnedRelFixture(tb testing.TB, be pinnedRelBackend, n int, prof pinnedRelProfile) *pinnedRelFixture {
	tb.Helper()
	ctx := benchCtx()
	g := be.open(tb)
	a, err := g.Nodes().Add(ctx, []string{"P"}, nil)
	if err != nil {
		tb.Fatal(err)
	}
	c, err := g.Nodes().Add(ctx, []string{"P"}, nil)
	if err != nil {
		tb.Fatal(err)
	}
	perType := n / prof.types
	values := perType / 200
	if values < 1 {
		values = 1
	}
	typeName := func(k int) string { return fmt.Sprintf("T%d", k) }
	for k := 0; k < prof.types; k++ {
		if err := g.Index().CreateRelProperty(typeName(k), "seat"); err != nil {
			tb.Fatal(err)
		}
	}
	if err := g.Index().CreateRelProperty(typeName(0), "grp"); err != nil {
		tb.Fatal(err)
	}
	const group = 10_000
	ids := make([][]types.RelID, prof.types)
	seat := make(map[types.RelID]int, n)
	for k := 0; k < prof.types; k++ {
		for start := 0; start < perType; start += group {
			end := min(start+group, perType)
			var rels []*types.Relationship
			if _, err := g.Batch().Run(func(bb *graph.BatchBuilder) error {
				for i := start; i < end; i++ {
					r, err := bb.AddRelationship(typeName(k), a, c, map[string]any{"seat": int64(i % values), "grp": int64(i % 20)})
					if err != nil {
						return err
					}
					rels = append(rels, r)
				}
				return nil
			}); err != nil {
				tb.Fatalf("batch add: %v", err)
			}
			for i, r := range rels {
				ids[k] = append(ids[k], r.ID())
				seat[r.ID()] = (start + i) % values
			}
		}
	}
	update := func(batch []types.RelID) {
		if _, err := g.Batch().Run(func(bb *graph.BatchBuilder) error {
			for _, id := range batch {
				seat[id] = (seat[id] + 1) % values
				if err := bb.UpdateRelationship(id, map[string]any{"seat": int64(seat[id])}); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			tb.Fatalf("batch update: %v", err)
		}
	}
	inGroups := func(all []types.RelID, fn func([]types.RelID)) {
		for start := 0; start < len(all); start += group {
			fn(all[start:min(start+group, len(all))])
		}
	}
	// sigma: every 5th rel of every type revised, every 20th deleted.
	if prof.reviseFrac > 0 {
		for k := range ids {
			var pick []types.RelID
			for i, id := range ids[k] {
				if i%5 == 1 {
					pick = append(pick, id)
				}
			}
			inGroups(pick, update)
		}
	}
	if prof.deleteFrac > 0 {
		for k := range ids {
			var pick []types.RelID
			for i, id := range ids[k] {
				if i%20 == 3 {
					pick = append(pick, id)
				}
			}
			inGroups(pick, func(batch []types.RelID) {
				if _, err := g.Batch().Run(func(bb *graph.BatchBuilder) error {
					for _, id := range batch {
						if err := bb.DeleteRelationship(id); err != nil {
							return err
						}
					}
					return nil
				}); err != nil {
					tb.Fatalf("batch delete: %v", err)
				}
			})
		}
	}
	// Unrelated churn: revisions on the other types only, round-robin.
	if revs := prof.unrelatedMult * n / 10; revs > 0 && prof.types > 1 {
		var others []types.RelID
		for k := 1; k < prof.types; k++ {
			others = append(others, ids[k]...)
		}
		var pick []types.RelID
		for i := 0; i < revs; i++ {
			pick = append(pick, others[i%len(others)])
		}
		inGroups(pick, update)
	}
	pin, err := g.Temporal().NowTx()
	if err != nil {
		tb.Fatal(err)
	}
	fx := &pinnedRelFixture{g: g, pin: pin, target: typeName(0)}
	cur, err := g.Rels().ByTypeAndProperty(fx.target, "seat", int64(0), storepkg.QueryOpts{})
	if err != nil {
		tb.Fatal(err)
	}
	fx.seatWant = len(cur)
	cur, err = g.Rels().ByTypeAndProperty(fx.target, "grp", int64(0), storepkg.QueryOpts{})
	if err != nil {
		tb.Fatal(err)
	}
	fx.grpWant = len(cur)

	// The first pinned lookup per key builds its sidecar lazily: time it.
	start := time.Now()
	rels, err := g.Rels().ByTypeAndProperty(fx.target, "seat", int64(0), storepkg.QueryOpts{TxPin: pin})
	fx.buildMs = float64(time.Since(start).Microseconds()) / 1000
	if err != nil || len(rels) != fx.seatWant {
		tb.Fatalf("warm-up pinned lookup: %d rels (want %d), %v", len(rels), fx.seatWant, err)
	}
	if _, err := g.Rels().ByTypeAndProperty(fx.target, "grp", int64(0), storepkg.QueryOpts{TxPin: pin}); err != nil {
		tb.Fatal(err)
	}
	return fx
}

// pinnedRelFullMatrix reports whether the measurement matrix (sharded and the
// broad lookup) runs: only when sizes are set explicitly.
func pinnedRelFullMatrix() bool { return os.Getenv("RHO_TKG_PINNED_REL_SIZES") != "" }

// pinnedRelRows returns the sub-benchmark names one run produces (without the
// top-level name and the -GOMAXPROCS suffix), in run order.
func pinnedRelRows(sizes []int, full bool) []string {
	var out []string
	for _, n := range sizes {
		for _, be := range pinnedRelBackends() {
			if be.name == "sharded" && !full {
				continue
			}
			for _, prof := range pinnedRelProfiles {
				base := fmt.Sprintf("%s/%d/%s", be.name, n, prof.name)
				out = append(out, base+"/matches200/pinned", base+"/matches200/current")
				if full {
					out = append(out, base+"/broad/pinned")
				}
			}
		}
	}
	return out
}

// BenchmarkPinnedRelPropertyLookup — see the block comment above.
func BenchmarkPinnedRelPropertyLookup(b *testing.B) {
	full := pinnedRelFullMatrix()
	for _, n := range pinnedRelSizes(b) {
		for _, be := range pinnedRelBackends() {
			if be.name == "sharded" && !full {
				continue // measurement matrix only (RHO_TKG_PINNED_REL_SIZES)
			}
			for _, prof := range pinnedRelProfiles {
				b.Run(fmt.Sprintf("%s/%d/%s", be.name, n, prof.name), func(b *testing.B) {
					fx := buildPinnedRelFixture(b, be, n, prof)
					lookup := func(b *testing.B, key string, want int, opts storepkg.QueryOpts) {
						b.ReportAllocs()
						for b.Loop() {
							rels, err := fx.g.Rels().ByTypeAndProperty(fx.target, key, int64(0), opts)
							if err != nil || len(rels) != want {
								b.Fatalf("%s lookup: %d rels (want %d), %v", key, len(rels), want, err)
							}
						}
					}
					b.Run("matches200/pinned", func(b *testing.B) {
						lookup(b, "seat", fx.seatWant, storepkg.QueryOpts{TxPin: fx.pin})
						b.ReportMetric(fx.buildMs, "build-ms")
					})
					b.Run("matches200/current", func(b *testing.B) {
						lookup(b, "seat", fx.seatWant, storepkg.QueryOpts{})
					})
					if full {
						b.Run("broad/pinned", func(b *testing.B) {
							lookup(b, "grp", fx.grpWant, storepkg.QueryOpts{TxPin: fx.pin})
						})
					}
				})
			}
		}
	}
}
