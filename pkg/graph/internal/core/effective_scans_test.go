package core

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Broader effective scans (backlog 35): ForEachNodeEffective /
// ForEachRelEffective, NodesEffectiveByIDs / RelsEffectiveByIDs,
// ForEachRelEffectiveAtNodes and ForEachNodeEffectiveByLabels, checked against
// the per-entity timeline (itself checked pointwise against NodeAtTx /
// RelAtTx by TestEffectiveTimeline_PointwiseOracle).

// esDoor is one call of a scan door and the per-entity oracle of its answer:
// the entities of ids (nil: every tracked entity of the kind), each with the
// segments of its own timeline whose row keep accepts.
type esDoor struct {
	name    string
	rel     bool
	ids     []int64 // the universe in answer order; nil: every tracked entity
	ordered bool    // the entities stream in the order of ids
	keepN   func(*types.Node) bool
	keepR   func(*types.Relationship) bool
	seeds   map[types.NodeID]bool // ForEachRelEffectiveAtNodes: the seeds
	run     func(pin types.Instant, fn func(id int64, s etSeg) bool) error
}

// esStream runs a door and returns entity → segments plus the entity order;
// it fails when one entity's segments are not contiguous (an entity listed
// twice).
func esStream(t *testing.T, d esDoor, pin types.Instant) (map[int64][]etSeg, []int64, error) {
	t.Helper()
	got := map[int64][]etSeg{}
	var order []int64
	err := d.run(pin, func(id int64, s etSeg) bool {
		if n := len(order); n == 0 || order[n-1] != id {
			if _, seen := got[id]; seen {
				t.Fatalf("%s pin %d: entity %d listed twice (segments not contiguous)", d.name, pin, id)
			}
			order = append(order, id)
		}
		got[id] = append(got[id], s)
		return true
	})
	return got, order, err
}

// esWant is the oracle: each universe entity's own timeline at pin, filtered.
func esWant(t *testing.T, g *Core, d esDoor, universe []int64, pin types.Instant) (map[int64][]etSeg, []int64) {
	t.Helper()
	ids := d.ids
	if ids == nil {
		ids = universe
	}
	want := map[int64][]etSeg{}
	var order []int64
	for _, id := range ids {
		if _, dup := want[id]; dup || slices.Contains(order, id) {
			continue
		}
		var segs []etSeg
		if d.rel {
			ss, err := g.Temporal.RelEffectiveTimeline(types.RelID(id), pin)
			if err != nil && !errors.Is(err, storepkg.ErrRelNotFound) {
				t.Fatalf("%s: RelEffectiveTimeline(%d, %d): %v", d.name, id, pin, err)
			}
			for _, s := range ss {
				if d.keepR(s.Rel) {
					segs = append(segs, etRelSegs([]temporal.RelSegment{s})[0])
				}
			}
		} else {
			ss, err := g.Temporal.NodeEffectiveTimeline(types.NodeID(id), pin)
			if err != nil && !errors.Is(err, storepkg.ErrNodeNotFound) {
				t.Fatalf("%s: NodeEffectiveTimeline(%d, %d): %v", d.name, id, pin, err)
			}
			for _, s := range ss {
				if d.keepN(s.Node) {
					segs = append(segs, etNodeSegs([]temporal.NodeSegment{s})[0])
				}
			}
		}
		order = append(order, id)
		if len(segs) > 0 {
			want[id] = segs
		}
	}
	kept := order[:0]
	for _, id := range order {
		if _, ok := want[id]; ok {
			kept = append(kept, id)
		}
	}
	return want, kept
}

func esRender(m map[int64][]etSeg) string {
	ids := make([]int64, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	var b strings.Builder
	for _, id := range ids {
		fmt.Fprintf(&b, "\n  %d:%s", id, etSegsString(m[id]))
	}
	return b.String()
}

// esCoverage counts the shapes a backend's runs exercised; each must occur.
type esCoverage map[string]int

// esDoors builds the doors under test for one graph: the all-entity scans,
// the by-IDs batches (every other entity, the last one first, a duplicate and
// an unknown ID), the seeded adjacency form in both directions with and
// without a type filter (every other node seeded plus a duplicate and an
// unknown seed), and the several-labels scan (two labels, a duplicate and an
// unknown one).
func esDoors(g *Core, nodes, rels []int64) []esDoor {
	te := g.Temporal
	nodeRun := func(call func(types.Instant, func(temporal.NodeSegment) bool) error) func(types.Instant, func(int64, etSeg) bool) error {
		return func(pin types.Instant, fn func(int64, etSeg) bool) error {
			return call(pin, func(s temporal.NodeSegment) bool {
				return fn(int64(s.Node.ID()), etNodeSegs([]temporal.NodeSegment{s})[0])
			})
		}
	}
	relRun := func(call func(types.Instant, func(temporal.RelSegment) bool) error) func(types.Instant, func(int64, etSeg) bool) error {
		return func(pin types.Instant, fn func(int64, etSeg) bool) error {
			return call(pin, func(s temporal.RelSegment) bool {
				return fn(int64(s.Rel.ID()), etRelSegs([]temporal.RelSegment{s})[0])
			})
		}
	}
	all := func(*types.Node) bool { return true }
	allR := func(*types.Relationship) bool { return true }

	// Batches: the last entity first, then every other one, then the first
	// again; an unknown ID of the kind (a relationship ID is never a node
	// ID and vice versa).
	batch := func(ids []int64, unknown int64) []int64 {
		out := []int64{ids[len(ids)-1]}
		for i := 0; i < len(ids); i += 2 {
			out = append(out, ids[i])
		}
		return append(out, ids[0], unknown)
	}
	nodeBatch, relBatch := batch(nodes, rels[0]), batch(rels, nodes[0])
	nodeBatchIDs := make([]types.NodeID, len(nodeBatch))
	for i, id := range nodeBatch {
		nodeBatchIDs[i] = types.NodeID(id)
	}
	relBatchIDs := make([]types.RelID, len(relBatch))
	for i, id := range relBatch {
		relBatchIDs[i] = types.RelID(id)
	}

	doors := []esDoor{
		{name: "ForEachNodeEffective", keepN: all, run: nodeRun(te.ForEachNodeEffective)},
		{name: "ForEachRelEffective", rel: true, keepR: allR, run: relRun(te.ForEachRelEffective)},
		{name: "NodesEffectiveByIDs", ids: nodeBatch, ordered: true, keepN: all,
			run: nodeRun(func(p types.Instant, fn func(temporal.NodeSegment) bool) error {
				return te.NodesEffectiveByIDs(nodeBatchIDs, p, fn)
			})},
		{name: "RelsEffectiveByIDs", rel: true, ids: relBatch, ordered: true, keepR: allR,
			run: relRun(func(p types.Instant, fn func(temporal.RelSegment) bool) error {
				return te.RelsEffectiveByIDs(relBatchIDs, p, fn)
			})},
	}

	seeds := map[types.NodeID]bool{}
	var seedIDs []types.NodeID
	for i := 0; i < len(nodes); i += 2 {
		seeds[types.NodeID(nodes[i])] = true
		seedIDs = append(seedIDs, types.NodeID(nodes[i]))
	}
	seedIDs = append(seedIDs, seedIDs[0], types.NodeID(rels[0]))
	for _, dir := range []temporal.Direction{temporal.Outgoing, temporal.Incoming} {
		for _, relTypes := range [][]string{nil, {oracleRelTypes[0], oracleRelTypes[0], "NoSuchType"}} {
			var toks []uint16
			for _, name := range relTypes {
				if tok, ok := g.lookupRelTypeQueryToken(name); ok {
					toks = append(toks, tok)
				}
			}
			keep := func(r *types.Relationship) bool {
				end := r.StartNodeID()
				if dir == temporal.Incoming {
					end = r.EndNodeID()
				}
				if !seeds[end] {
					return false
				}
				if relTypes == nil {
					return true
				}
				return slices.ContainsFunc(toks, r.HasTypeTokenRaw)
			}
			doors = append(doors, esDoor{
				name: fmt.Sprintf("ForEachRelEffectiveAtNodes(dir=%d,types=%v)", dir, relTypes), rel: true, keepR: keep, seeds: seeds,
				run: relRun(func(p types.Instant, fn func(temporal.RelSegment) bool) error {
					return te.ForEachRelEffectiveAtNodes(seedIDs, dir, relTypes, p, fn)
				}),
			})
		}
	}

	labels := []string{oracleNodeLabels[0], oracleNodeLabels[1], oracleNodeLabels[0], "NoSuchLabel"}
	var ltoks []uint16
	for _, l := range labels {
		if tok, ok := g.labels.Lookup(l); ok {
			ltoks = append(ltoks, tok)
		}
	}
	doors = append(doors, esDoor{
		name:  "ForEachNodeEffectiveByLabels",
		keepN: func(n *types.Node) bool { return slices.ContainsFunc(ltoks, n.HasLabelTokenRaw) },
		run: nodeRun(func(p types.Instant, fn func(temporal.NodeSegment) bool) error {
			return te.ForEachNodeEffectiveByLabels(labels, p, fn)
		}),
	})
	return doors
}

// TestEffectiveScans_MatchPerEntityTimeline — on the bitemporal oracle's
// chains (memory, badger, sharded, tiered; then a GraphTx rollback, a
// re-import of a deleted ID with other endpoints and a history compaction), at
// sampled pins: every new door streams exactly the per-entity timelines its
// predicate selects — the same entities (live, closed, deleted before or after
// the pin; none created after it), the same segments, each entity once and
// contiguous, the by-IDs batches in input order with duplicates once and
// unknown IDs skipped; a rel with both endpoints seeded once; a rel listed
// through an earlier life's endpoint. fn returning false stops after one
// segment. A pin the graph refuses (compaction watermark) is refused by the
// door with the same sentinel.
//
// Catches a door that drops entities deleted before the pin, lists one
// created after it, reads current rows instead of pinned ones, lists an
// entity twice (current ∪ history, duplicate IDs or seeds, several matching
// labels), errors on an unknown ID, folds only deleted relationships into the
// adjacency candidates (misses a re-imported ID's earlier life), streams a
// candidate's whole timeline instead of its seeded / typed / labelled
// segments, ignores the direction, or ignores fn's stop.
func TestEffectiveScans_MatchPerEntityTimeline(t *testing.T) {
	t.Parallel()
	x0 := etX0()
	nSeeds := 3
	if isRaceEnabled() {
		nSeeds = 1
	}
	for _, be := range txbBackends() {
		t.Run(be.name, func(t *testing.T) {
			cov := esCoverage{}
			for i := 0; i < nSeeds; i++ {
				seed := uint64(0x35_35) + uint64(i)
				_, _, _, pins, o := txbOracleRun(t, be, seed, 40, x0, nil, nil)
				esExtraRels(t, o)
				esCheck(t, o, append(etSample(pins, 4), o.x), cov, "after the oracle sequence")
				etTail(o)
				esCheck(t, o, append(etSample(pins, 3), o.x), cov, "after rollback, re-import, compaction")
			}
			for _, shape := range []string{"live", "closed", "deleted-before-pin", "deleted-after-pin", "created-after-pin",
				"rel both endpoints seeded", "rel listed through an earlier life", "duplicate in batch", "unknown in batch", "stop"} {
				if cov[shape] == 0 {
					t.Errorf("shape %q never exercised (coverage %v)", shape, cov)
				}
			}
		})
	}
}

// esExtraRels adds the relationship shapes the oracle's few relationships
// rarely draw, on its clock: one between two seeded nodes (esDoors seeds every
// other node) and one whose ID is deleted and re-imported with other
// endpoints — its first life starts at a seed, its second does not.
func esExtraRels(t *testing.T, o *txbOracle) {
	t.Helper()
	ctx := context.Background()
	g := o.g
	var seeded, unseeded []*types.Node
	for i, id := range o.allNodeIDs() {
		n, err := g.Nodes.Get(ctx, id)
		if err != nil || i == 0 {
			// Not anchor 0 (tiered's reference shard): a re-import that moves
			// a relationship to another tiered shard loses the earlier life's
			// history (finding-tiered-reimport-loses-history.txt), which no
			// door can then show.
			continue
		}
		if i%2 == 0 {
			seeded = append(seeded, n)
		} else {
			unseeded = append(unseeded, n)
		}
	}
	for len(seeded) < 2 || len(unseeded) < 1 {
		o.tick()
		n, err := g.Nodes.Add(ctx, []string{oracleNodeLabels[0]}, map[string]any{"tkg_valid_from": o.nextVF()})
		if err != nil {
			t.Fatalf("add node: %v", err)
		}
		o.nodeIDs = append(o.nodeIDs, n.ID())
		o.nodeAlive[n.ID()] = true
		if (len(o.allNodeIDs())-1)%2 == 0 {
			seeded = append(seeded, n)
		} else {
			unseeded = append(unseeded, n)
		}
	}
	add := func(typ string, s, e *types.Node) types.RelID {
		o.tick()
		r, err := g.Rels.AddByID(ctx, typ, s.ID(), e.ID(), map[string]any{"tkg_valid_from": o.nextVF()})
		if err != nil {
			t.Fatalf("AddByID: %v", err)
		}
		o.relIDs = append(o.relIDs, r.ID())
		o.relType[r.ID()] = typ
		return r.ID()
	}
	add(oracleRelTypes[0], seeded[0], seeded[1])
	moved := add(oracleRelTypes[1], seeded[0], unseeded[0])
	o.tick()
	if err := g.Rels.Delete(ctx, moved); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	o.tick()
	if _, err := g.Rels.Import(ctx, moved, oracleRelTypes[1], unseeded[0], seeded[1], map[string]any{"tkg_valid_from": o.nextVF(), "life": int64(2)}); err != nil {
		t.Fatalf("Rels.Import: %v", err)
	}
	o.tick()
}

func esCheck(t *testing.T, o *txbOracle, pins []types.Instant, cov esCoverage, phase string) {
	t.Helper()
	g := o.g
	var nodes, rels []int64
	for _, id := range o.allNodeIDs() {
		nodes = append(nodes, int64(id))
	}
	for _, id := range o.relIDs {
		rels = append(rels, int64(id))
	}
	cov["duplicate in batch"]++
	cov["unknown in batch"]++
	doors := esDoors(g, nodes, rels)
	for _, pin := range pins {
		if pin > o.x {
			continue
		}
		esClassify(t, g, nodes, rels, pin, o.x, cov)
		for _, d := range doors {
			universe := nodes
			if d.rel {
				universe = rels
			}
			got, gotOrder, err := esStream(t, d, pin)
			if errors.Is(err, ErrHistoryCompacted) || errors.Is(err, ErrRetentionExpired) {
				// Scans refuse on the graph watermark, by-IDs batches on a
				// member's own chain (as its timeline door).
				if !esRefused(g, d, pin, err) {
					t.Fatalf("[%s] %s pin %d: %v, but no gate refuses the pin", phase, d.name, pin, err)
				}
				continue
			}
			if err != nil {
				t.Fatalf("[%s] %s pin %d: %v", phase, d.name, pin, err)
			}
			want, wantOrder := esWant(t, g, d, universe, pin)
			if a, b := esRender(got), esRender(want); a != b {
				t.Fatalf("[%s] %s pin %d:\n got  %s\n want %s", phase, d.name, pin, a, b)
			}
			if d.ordered && !slices.Equal(gotOrder, wantOrder) {
				t.Fatalf("[%s] %s pin %d: order %v, want %v (input order, duplicates once)", phase, d.name, pin, gotOrder, wantOrder)
			}
			if strings.HasPrefix(d.name, "ForEachRelEffectiveAtNodes(dir=1,types=[]") {
				esRelShapes(t, g, d, got, cov)
			}
			if len(want) > 0 {
				calls := 0
				if err := d.run(pin, func(int64, etSeg) bool { calls++; return false }); err != nil || calls != 1 {
					t.Fatalf("[%s] %s pin %d stop: err %v, %d calls, want 1", phase, d.name, pin, err, calls)
				}
				cov["stop"]++
			}
		}
	}
}

// esRefused: the door's refusal at pin is the graph watermark's (a scan) or
// one batch member's own timeline door's (by IDs).
func esRefused(g *Core, d esDoor, pin types.Instant, err error) bool {
	if !d.ordered {
		serr := g.validateEffectiveScanPin(pin)
		return serr != nil && etSameRefusal(err, serr)
	}
	for _, id := range d.ids {
		var perr error
		if d.rel {
			_, perr = g.Temporal.RelEffectiveTimeline(types.RelID(id), pin)
		} else {
			_, perr = g.Temporal.NodeEffectiveTimeline(types.NodeID(id), pin)
		}
		if etSameRefusal(err, perr) {
			return true
		}
	}
	return false
}

// esRelShapes counts, for the untyped outgoing door, listed rels with both
// endpoints seeded and rels listed only through an earlier life (the current
// row's start is not a seed).
func esRelShapes(t *testing.T, g *Core, d esDoor, got map[int64][]etSeg, cov esCoverage) {
	t.Helper()
	for id := range got {
		r, err := g.Rels.Get(context.Background(), types.RelID(id))
		if err != nil {
			continue
		}
		switch {
		case !d.seeds[r.StartNodeID()]:
			cov["rel listed through an earlier life"]++
		case d.seeds[r.EndNodeID()]:
			cov["rel both endpoints seeded"]++
		}
	}
}

// esClassify counts the lifecycle shapes of the tracked entities at pin.
func esClassify(t *testing.T, g *Core, nodes, rels []int64, pin, final types.Instant, cov esCoverage) {
	t.Helper()
	ctx := context.Background()
	shape := func(segsAtPin, segsFinal int, asOfOK, currentOK bool, lastOpen bool) string {
		switch {
		case segsAtPin == 0 && segsFinal > 0:
			return "created-after-pin"
		case segsAtPin == 0:
			return ""
		case !asOfOK:
			return "deleted-before-pin"
		case !currentOK:
			return "deleted-after-pin"
		case lastOpen:
			return "live"
		default:
			return "closed"
		}
	}
	for _, id := range nodes {
		at, _ := g.Temporal.NodeEffectiveTimeline(types.NodeID(id), pin)
		fin, _ := g.Temporal.NodeEffectiveTimeline(types.NodeID(id), final)
		_, aerr := g.Temporal.NodeAsOf(types.NodeID(id), pin)
		_, cerr := g.Nodes.Get(ctx, types.NodeID(id))
		open := len(at) > 0 && at[len(at)-1].ValidTo == 0
		if s := shape(len(at), len(fin), aerr == nil, cerr == nil, open); s != "" {
			cov[s]++
		}
	}
	for _, id := range rels {
		at, _ := g.Temporal.RelEffectiveTimeline(types.RelID(id), pin)
		fin, _ := g.Temporal.RelEffectiveTimeline(types.RelID(id), final)
		_, aerr := g.Temporal.RelAsOf(types.RelID(id), pin)
		_, cerr := g.Rels.Get(ctx, types.RelID(id))
		open := len(at) > 0 && at[len(at)-1].ValidTo == 0
		if s := shape(len(at), len(fin), aerr == nil, cerr == nil, open); s != "" {
			cov[s]++
		}
	}
}

// TestEffectiveScans_Errors — the new doors' refusals (memory): nil fn →
// ErrNilCallback, pin 0 → ErrInvalidTimeRange, a pin above the commit clock →
// ErrTxPinTooNew, an invalid ID in a batch or a seed list →
// ErrInvalidStoreMutation before fn runs, an invalid direction →
// ErrInvalidDirection, an empty label / type name → an error; a pin below the
// compaction watermark → ErrHistoryCompacted; a pin below the retention
// watermark of a label no node carries → ErrRetentionExpired from every scan
// (the graph watermark) while NodesEffectiveByIDs answers (per-entity gates, as
// the timeline door); a closed graph → ErrGraphClosed. Catches a door that
// skips a validation, gates a scan with the per-entity check only, or calls fn
// before refusing.
func TestEffectiveScans_Errors(t *testing.T) {
	t.Parallel()
	g := txbBackends()[0].open(t, false)
	useTestClock(t, g)
	ctx := context.Background()
	te := g.Temporal
	a, err := g.Nodes.Add(ctx, []string{"A"}, map[string]any{"tkg_valid_from": types.Instant(1000)})
	if err != nil {
		t.Fatalf("add: %v", err)
	}
	b, err := g.Nodes.Add(ctx, []string{"A"}, map[string]any{"tkg_valid_from": types.Instant(1000)})
	if err != nil {
		t.Fatalf("add: %v", err)
	}
	r, err := g.Rels.AddByID(ctx, "R", a.ID(), b.ID(), map[string]any{"tkg_valid_from": types.Instant(1000)})
	if err != nil {
		t.Fatalf("add rel: %v", err)
	}
	// Q: an uncompacted node and relationship; only the graph watermark
	// refuses a scan below it.
	q, err := g.Nodes.Add(ctx, []string{"Q"}, map[string]any{"tkg_valid_from": types.Instant(1000)})
	if err != nil {
		t.Fatalf("add Q: %v", err)
	}
	p0, _ := te.NowTx()
	for i := 0; i < 2; i++ {
		if _, err := g.Nodes.Update(ctx, a.ID(), map[string]any{"x": int64(i)}); err != nil {
			t.Fatalf("update: %v", err)
		}
		if _, err := g.Rels.Update(ctx, r.ID(), map[string]any{"x": int64(i)}); err != nil {
			t.Fatalf("update rel: %v", err)
		}
	}
	if _, err := g.Admin.CompactHistoryNodes(ctx, RetentionPolicy{KeepVersions: 1}); err != nil {
		t.Fatalf("compact: %v", err)
	}
	if _, err := g.Admin.CompactHistoryRels(ctx, RetentionPolicy{KeepVersions: 1}); err != nil {
		t.Fatalf("compact rels: %v", err)
	}
	pin, _ := te.NowTx()
	// A retention watermark at pin on a label no node carries: only the graph
	// watermark refuses pin-1.
	if err := g.advanceRetentionWatermark(nodeLabelToken(t, g, "Gone"), pin); err != nil {
		t.Fatalf("advanceRetentionWatermark: %v", err)
	}
	peek, _ := te.PeekTx()
	future := peek + 3_600_000

	calls := 0
	fnN := func(temporal.NodeSegment) bool { calls++; return true }
	fnR := func(temporal.RelSegment) bool { calls++; return true }
	type door struct {
		name string
		run  func(p types.Instant, nilFn bool) error
		// belowRetention: the answer at pin-1 (graph watermark for the scans
		// and the relationship gate, per-entity label gate for nodes by ID).
		belowRetention error
	}
	nf := func(nilFn bool) func(temporal.NodeSegment) bool {
		if nilFn {
			return nil
		}
		return fnN
	}
	rf := func(nilFn bool) func(temporal.RelSegment) bool {
		if nilFn {
			return nil
		}
		return fnR
	}
	doors := []door{
		{"ForEachNodeEffective", func(p types.Instant, n bool) error { return te.ForEachNodeEffective(p, nf(n)) }, ErrRetentionExpired},
		{"ForEachRelEffective", func(p types.Instant, n bool) error { return te.ForEachRelEffective(p, rf(n)) }, ErrRetentionExpired},
		{"NodesEffectiveByIDs", func(p types.Instant, n bool) error {
			return te.NodesEffectiveByIDs([]types.NodeID{a.ID()}, p, nf(n))
		}, nil},
		{"RelsEffectiveByIDs", func(p types.Instant, n bool) error {
			return te.RelsEffectiveByIDs([]types.RelID{r.ID()}, p, rf(n))
		}, ErrRetentionExpired},
		{"ForEachRelEffectiveAtNodes", func(p types.Instant, n bool) error {
			return te.ForEachRelEffectiveAtNodes([]types.NodeID{q.ID()}, temporal.Outgoing, nil, p, rf(n))
		}, ErrRetentionExpired},
		{"ForEachNodeEffectiveByLabels", func(p types.Instant, n bool) error {
			return te.ForEachNodeEffectiveByLabels([]string{"Q"}, p, nf(n))
		}, ErrRetentionExpired},
	}
	for _, d := range doors {
		for _, c := range []struct {
			name  string
			pin   types.Instant
			nilFn bool
			want  error
		}{
			{"nil fn", pin, true, grapherr.ErrNilCallback},
			{"pin 0", 0, false, ErrInvalidTimeRange},
			{"pin -1", -1, false, ErrInvalidTimeRange},
			{"future pin", future, false, ErrTxPinTooNew},
			{"below compaction", p0, false, ErrHistoryCompacted},
		} {
			calls = 0
			if err := d.run(c.pin, c.nilFn); !errors.Is(err, c.want) || calls != 0 {
				t.Fatalf("%s %s: err %v, %d fn calls; want %v and none", d.name, c.name, err, calls, c.want)
			}
		}
		calls = 0
		if err := d.run(pin-1, false); (d.belowRetention == nil && err != nil) || (d.belowRetention != nil && (!errors.Is(err, d.belowRetention) || calls != 0)) {
			t.Fatalf("%s below the retention watermark: err %v, %d fn calls; want %v", d.name, err, calls, d.belowRetention)
		}
		if err := d.run(pin, false); err != nil {
			t.Fatalf("%s at a valid pin: %v", d.name, err)
		}
	}
	// The by-IDs doors gate per entity, as the timeline door: Q's whole chain
	// answers below the graph watermark there, the scans refuse it.
	if err := te.NodesEffectiveByIDs([]types.NodeID{q.ID()}, p0, fnN); err != nil {
		t.Fatalf("NodesEffectiveByIDs of the uncompacted Q below the watermark: %v", err)
	}
	for _, c := range []struct {
		name string
		run  func() error
		want error
	}{
		{"node batch with id 0", func() error { return te.NodesEffectiveByIDs([]types.NodeID{a.ID(), 0}, pin, fnN) }, storepkg.ErrInvalidStoreMutation},
		{"rel batch with id -1", func() error { return te.RelsEffectiveByIDs([]types.RelID{r.ID(), -1}, pin, fnR) }, storepkg.ErrInvalidStoreMutation},
		{"seed 0", func() error {
			return te.ForEachRelEffectiveAtNodes([]types.NodeID{a.ID(), 0}, temporal.Outgoing, nil, pin, fnR)
		}, storepkg.ErrInvalidStoreMutation},
		{"direction 0", func() error { return te.ForEachRelEffectiveAtNodes([]types.NodeID{a.ID()}, 0, nil, pin, fnR) }, ErrInvalidDirection},
		{"direction 3", func() error { return te.ForEachRelEffectiveAtNodes([]types.NodeID{a.ID()}, 3, nil, pin, fnR) }, ErrInvalidDirection},
		{"empty type", func() error {
			return te.ForEachRelEffectiveAtNodes([]types.NodeID{a.ID()}, temporal.Incoming, []string{""}, pin, fnR)
		}, nil},
		{"empty label", func() error { return te.ForEachNodeEffectiveByLabels([]string{"A", ""}, pin, fnN) }, nil},
	} {
		calls = 0
		err := c.run()
		if (c.want == nil && err == nil) || (c.want != nil && !errors.Is(err, c.want)) || calls != 0 {
			t.Fatalf("%s: err %v, %d fn calls; want %v and none", c.name, err, calls, c.want)
		}
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for _, d := range doors {
		if err := d.run(pin, false); !errors.Is(err, ErrGraphClosed) {
			t.Fatalf("%s closed: %v, want ErrGraphClosed", d.name, err)
		}
	}
}
