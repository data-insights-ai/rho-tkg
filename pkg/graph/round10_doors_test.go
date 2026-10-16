package graph_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Round 10 doors (sigma-tkgd, 2026-10-16) against brute-force oracles over
// random histories: windowed effective reads and their typed form, the
// validity hull, the count timeline of the current rows, value classes at a
// pin. The generator reaches every op kind (r10Ops, checked after every run
// at every size) and every edge: untimed creates, valid starts at
// types.UnboundedValidFrom, negative and positive, rows closed before the
// epoch, int and float of one value in every width, NaN, values above 2^53,
// lists and maps, deletes, retractions, label removal, re-imports, excises
// and valid-time rewrites below earlier pins. The -short tier runs fewer
// seeds and steps of the same cases (AGENTS.md race budget).

// r10Values are the property values the histories write.
var r10Values = []any{
	int64(3), 3.0, float32(3), int8(3), uint64(3), int64(1<<53 + 1), float64(1 << 53), math.NaN(), "s", true,
	[]string{"a"}, map[string]any{"k": int64(1)}, math.Inf(-1), int64(-7), -7.0, uint64(math.MaxUint64),
}

// r10Ops are the write kinds the generator must reach on every run.
var r10Ops = []string{
	"create-untimed", "create-unbounded", "create-negative", "create-positive", "create-closed-negative",
	"update-timed", "update-untimed", "close", "delete", "retract", "relabel", "reimport", "excise", "excise-negative",
	"set-interval", "rel-update", "rel-close", "rel-delete", "rel-retract", "rel-excise", "rel-set-interval",
}

// r10World is one generated history.
type r10World struct {
	label string
	hub   types.NodeID
	nodes []types.NodeID
	rels  []types.RelID // R: hub -> node; S: node -> hub, alternating
	pins  []types.Instant
	ops   map[string]int
}

// r10Witnesses is how many of the first entities the random ops leave alone
// (r10History).
const r10Witnesses = 6

// r10Size is the history size: smaller in -short, same op kinds.
func r10Size() (entities, steps int, seeds []uint64) {
	if testing.Short() {
		return 14, 144, []uint64{1, 2}
	}
	return 40, 400, []uint64{1, 2, 3, 4, 5, 6}
}

// r10History writes a random history on g. Ops that the state refuses
// (an update of a closed version, an excise where nothing is valid) are
// skipped; every kind of r10Ops must succeed at least once (checked by the
// caller through r10CheckCoverage).
func r10History(t *testing.T, b storeBackend, g *graphpkg.Graph, seed uint64) *r10World {
	t.Helper()
	ctx := context.Background()
	rng := rand.New(rand.NewPCG(seed, 10))
	entities, steps, _ := r10Size()
	w := &r10World{label: "Ref", ops: map[string]int{}}
	pick := func() any { return r10Values[rng.IntN(len(r10Values))] }
	mark := func() {
		p, err := g.Temporal().NowTx()
		if err != nil {
			t.Fatal(err)
		}
		w.pins = append(w.pins, p)
	}
	did := func(op string, err error) {
		if err == nil {
			w.ops[op]++
		}
	}
	hub, err := g.Nodes().Add(ctx, []string{"Hub"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	w.hub = hub.ID()
	lastFrom, lastRelFrom := map[int]types.Instant{}, map[int]types.Instant{}
	for i := range entities {
		props := map[string]any{"v": pick()}
		var from types.Instant
		op := "create-untimed"
		switch i % 5 {
		case 1:
			from, op = types.UnboundedValidFrom, "create-unbounded"
		case 2:
			from, op = types.Instant(-5000+97*i), "create-negative"
		case 3:
			from, op = types.Instant(1000+100*i), "create-positive"
		case 4:
			from, op = types.Instant(-9000+31*i), "create-closed-negative"
			props["tkg_valid_to"] = types.Instant(-100 - i)
		}
		if from != 0 {
			props["tkg_valid_from"] = from
		}
		n, err := g.Nodes().Add(ctx, []string{w.label}, props)
		if err != nil {
			t.Fatal(err)
		}
		did(op, nil)
		w.nodes = append(w.nodes, n.ID())
		lastFrom[i] = from
		rp := map[string]any{"w": pick(), "n": int64(rng.IntN(100) - 50), "name": fmt.Sprintf("r%d", i)}
		if from != 0 && i%5 != 4 {
			rp["tkg_valid_from"] = from
			lastRelFrom[i] = from
		}
		var r *types.Relationship
		if i%2 == 0 {
			r, err = g.Rels().Add(ctx, "R", hub, n, rp)
		} else {
			r, err = g.Rels().Add(ctx, "S", n, hub, rp)
		}
		if err != nil {
			t.Fatal(err)
		}
		w.rels = append(w.rels, r.ID())
		mark()
	}
	// The witnesses keep their first rows as the head rows: one of each
	// create kind, and an untimed create closed in valid time (its head row
	// is version 1 starting at the genesis TxFrom, the untimed rule's
	// genesis case).
	closeAt, err := g.Temporal().NowTx()
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Nodes().CloseVersion(ctx, w.nodes[r10Witnesses-1], closeAt+1000); err != nil {
		t.Fatal(err)
	}
	if err := g.Rels().CloseVersion(ctx, w.rels[r10Witnesses-1], closeAt+1000); err != nil {
		t.Fatal(err)
	}
	mark()
	dead, deadRel := map[int]bool{}, map[int]bool{}
	firstErr := map[string]error{}
	// Every step tries the next kind in turn on a random entity it applies
	// to, so each kind gets steps/len(cycle) attempts at every size.
	cycle := []string{"update-timed", "update-untimed", "close", "relabel", "excise", "excise-negative", "set-interval",
		"rel-update", "rel-close", "rel-excise", "rel-set-interval", "update-timed", "rel-delete", "delete", "reimport",
		"rel-retract", "retract", "update-timed"}
	for step := range steps {
		op := cycle[step%len(cycle)]
		alive := func(rel bool) (int, bool) {
			for range 3 * len(w.nodes) {
				i := r10Witnesses + rng.IntN(len(w.nodes)-r10Witnesses)
				if (op == "reimport") == dead[i] && (!rel || !deadRel[i]) {
					return i, true
				}
			}
			return 0, false
		}
		i, ok := alive(strings.HasPrefix(op, "rel-"))
		if !ok || (b.tiered && (op == "relabel" || op == "reimport")) {
			mark()
			continue
		}
		id, rid := w.nodes[i], w.rels[i]
		// next is a valid-from above the entity's (relationship's) latest
		// start: an untimed start is its TxFrom on an UntimedFromTxFrom
		// store, so the clock bounds it from below there.
		next := func(m map[int]types.Instant) types.Instant {
			base, recorded := m[i]
			switch {
			case !recorded || base == 0: // no timed start: the row starts at its TxFrom at most
				now, err := g.Temporal().NowTx()
				if err != nil {
					t.Fatal(err)
				}
				base = now
			case base == types.UnboundedValidFrom:
				base = -3000
			}
			m[i] = base + 1 + types.Instant(rng.IntN(400))
			return m[i]
		}
		var err error
		switch op {
		case "update-timed":
			_, err = g.Nodes().Update(ctx, id, map[string]any{"v": pick(), "tkg_valid_from": next(lastFrom)})
		case "update-untimed":
			_, err = g.Nodes().Update(ctx, id, map[string]any{"v": pick()})
		case "close":
			err = g.Nodes().CloseVersion(ctx, id, next(lastFrom)+5)
		case "relabel":
			_ = g.Nodes().AddLabel(ctx, id, "Keep") // a second label, so Ref can be removed (an existing one is fine)
			if err = g.Nodes().RemoveLabel(ctx, id, w.label); err == nil {
				mark()
				err = g.Nodes().AddLabel(ctx, id, w.label)
			}
		case "excise":
			a := types.Instant(rng.IntN(6000) - 2000)
			err = g.Temporal().ExciseNodeValidity(ctx, id, a, a+1+types.Instant(rng.IntN(300)))
		case "excise-negative":
			a := types.Instant(-9000 + rng.IntN(8000))
			err = g.Temporal().ExciseNodeValidity(ctx, id, a, a+1+types.Instant(rng.IntN(500)))
		case "set-interval":
			a := types.Instant(rng.IntN(4000) - 6000)
			_, err = g.Temporal().SetNodeVersionInterval(ctx, id, a, a+1+types.Instant(rng.IntN(300)), map[string]any{"v": pick()})
		case "rel-update":
			_, err = g.Rels().Update(ctx, rid, map[string]any{"w": pick(), "n": int64(step), "tkg_valid_from": next(lastRelFrom)})
		case "rel-close":
			err = g.Rels().CloseVersion(ctx, rid, next(lastRelFrom)+3)
		case "rel-excise":
			a := types.Instant(rng.IntN(8000) - 6000)
			err = g.Temporal().ExciseRelValidity(ctx, rid, a, a+1+types.Instant(rng.IntN(500)))
		case "rel-set-interval":
			a := types.Instant(rng.IntN(4000) - 6000)
			_, err = g.Temporal().SetRelVersionInterval(ctx, rid, a, a+1+types.Instant(rng.IntN(300)), map[string]any{"n": int64(-step)})
		case "rel-delete":
			if err = g.Rels().Delete(ctx, rid); err == nil {
				deadRel[i] = true
			}
		case "rel-retract":
			if err = g.Rels().Retract(ctx, rid); err == nil {
				deadRel[i] = true
			}
		case "delete":
			if err = g.Nodes().Delete(ctx, id); err == nil {
				dead[i], deadRel[i] = true, true // a node delete cascades to its relationships
			}
		case "retract":
			if err = g.Nodes().Retract(ctx, id); err == nil {
				dead[i], deadRel[i] = true, true
			}
		case "reimport":
			if _, err = g.Nodes().Import(ctx, id, []string{w.label}, map[string]any{"v": pick(), "tkg_valid_from": next(lastFrom)}); err == nil {
				dead[i] = false
			}
		}
		did(op, err)
		if err != nil && firstErr[op] == nil {
			firstErr[op] = err
		}
		mark()
	}
	for _, op := range r10Ops {
		if w.ops[op] == 0 && firstErr[op] != nil {
			t.Logf("%s: %s never succeeded, first refusal: %v", b.name, op, firstErr[op])
		}
	}
	return w
}

// r10CheckCoverage fails the test when a kind of r10Ops never succeeded
// (the tiered store has no relabel or re-import: the generator skips them).
func r10CheckCoverage(t *testing.T, b storeBackend, ops map[string]int) {
	t.Helper()
	for _, op := range r10Ops {
		if b.tiered && (op == "relabel" || op == "reimport") {
			continue
		}
		if ops[op] == 0 {
			t.Errorf("%s: the generator never reached %q (ops %v)", b.name, op, ops)
		}
	}
}

// r10ProbePins are pins spread over the history plus the last one.
func r10ProbePins(w *r10World) []types.Instant {
	p := w.pins
	return []types.Instant{p[0], p[len(p)/4], p[len(p)/2], p[3*len(p)/4], p[len(p)-1]}
}

// r10Edges are the instants windows and probes are drawn from.
var r10Edges = []types.Instant{
	types.UnboundedValidFrom, types.UnboundedValidFrom + 1, -9000, -5000, -4000, -101, -100, -1, 0, 1, 50, 999, 1000,
	1500, 3000, 6000, 20000, math.MaxInt64 - 1, math.MaxInt64,
}

// r10Windows draws windows: random disjoint ascending intervals over the
// edges and random instants, plus the fixed shapes (empty, any time,
// touching intervals, a window at 0).
func r10Windows(rng *rand.Rand, n int) []temporal.Window {
	out := []temporal.Window{
		nil,
		{{From: types.UnboundedValidFrom, To: math.MaxInt64}},
		{{From: -100, To: 0}, {From: 0, To: 1}, {From: 1, To: 1000}},
		{{From: 0, To: 1}},
		{{From: types.UnboundedValidFrom, To: types.UnboundedValidFrom + 1}},
	}
	for range n {
		k := 1 + rng.IntN(3)
		pts := map[types.Instant]bool{}
		for len(pts) < 2*k {
			if rng.IntN(3) == 0 {
				pts[types.Instant(rng.IntN(30000)-10000)] = true
			} else {
				pts[r10Edges[rng.IntN(len(r10Edges))]] = true
			}
		}
		var s []types.Instant
		for p := range pts {
			s = append(s, p)
		}
		slices.Sort(s)
		var w temporal.Window
		for j := 0; j+1 < len(s); j += 2 {
			w = append(w, temporal.Interval{From: s[j], To: s[j+1]})
		}
		out = append(out, w)
	}
	return out
}

// r10Meets is the oracle's overlap: [from, to) (to 0 open) meets an
// interval of w.
func r10Meets(w temporal.Window, from, to types.Instant) bool {
	for _, iv := range w {
		if from < iv.To && (to == 0 || to > iv.From) {
			return true
		}
	}
	return false
}

func nodeSegKey(s temporal.NodeSegment) string {
	return fmt.Sprintf("%d/%d/%d/%d", s.Node.ID(), s.Node.Version(), s.ValidFrom, s.ValidTo)
}

func relSegKey(s temporal.RelSegment) string {
	return fmt.Sprintf("%d/%d/%d/%d", s.Rel.ID(), s.Rel.Version(), s.ValidFrom, s.ValidTo)
}

// r10Backends are the four backends with backfill allowed, under both
// untimed modes.
func r10Backends() []storeBackend {
	out := allStoreBackendsWith(func(c *graphpkg.Config) { c.AllowTxBackfill = true })
	for _, b := range allStoreBackendsWith(func(c *graphpkg.Config) { c.AllowTxBackfill = true; txFromCfg(c) }) {
		b.name += "-txfrom"
		out = append(out, b)
	}
	return out
}

// TestRound10WindowDoorsMatchOracle (request 2b and 1): on every backend,
// both untimed modes, at pins across random histories, for random windows
// (empty, any time, touching intervals, intervals at 0, at the far ends):
//   - ForEachNodeEffectiveByLabelsWindow / ForEachRelEffectiveByTypeWindow /
//     ForEachRelEffectiveAtNodesWindow stream exactly the unwindowed door's
//     segments that meet the window (multiset of id/version/span), no other;
//   - at every probe instant inside the window (0 included, with v4.59.0's
//     explicit presence) the entities with a streamed segment containing it
//     are ByLabel's / ByType's at {ValidAt, ValidAtSet, TxAt: pin, TxAtSet};
//   - ScanRelEffectiveColumnsAtNodes hands out the adjacency door's segments
//     as typed rows: IDs, endpoints, spans, int and string columns, no
//     ordinals.
func TestRound10WindowDoorsMatchOracle(t *testing.T) {
	_, _, seeds := r10Size()
	for _, b := range r10Backends() {
		t.Run(b.name, func(t *testing.T) {
			ops := map[string]int{}
			defer func() { r10CheckCoverage(t, b, ops) }()
			for _, seed := range seeds {
				g := b.open(t)
				w := r10History(t, b, g, seed)
				for op, n := range w.ops {
					ops[op] += n
				}
				rng := rand.New(rand.NewPCG(seed, 77))
				for _, pin := range r10ProbePins(w) {
					all := map[string]temporal.NodeSegment{}
					if err := g.Temporal().ForEachNodeEffectiveByLabels([]string{w.label}, pin, func(s temporal.NodeSegment) bool {
						all[nodeSegKey(s)] = s
						return true
					}); err != nil {
						t.Fatal(err)
					}
					allR := map[string]temporal.RelSegment{}
					if err := g.Temporal().ForEachRelEffectiveByType("R", pin, func(s temporal.RelSegment) bool {
						allR[relSegKey(s)] = s
						return true
					}); err != nil {
						t.Fatal(err)
					}
					for _, win := range r10Windows(rng, 6) {
						r10CheckNodeWindow(t, g, w, pin, win, all)
						r10CheckRelWindow(t, g, w, pin, win, allR)
						r10CheckAdjacencyWindow(t, g, w, pin, win)
					}
				}
			}
		})
	}
}

func r10CheckNodeWindow(t *testing.T, g *graphpkg.Graph, w *r10World, pin types.Instant, win temporal.Window, all map[string]temporal.NodeSegment) {
	t.Helper()
	got := map[string]int{}
	if err := g.Temporal().ForEachNodeEffectiveByLabelsWindow([]string{w.label}, pin, win, func(s temporal.NodeSegment) bool {
		got[nodeSegKey(s)]++
		return true
	}); err != nil {
		t.Fatalf("pin %d window %v: %v", pin, win, err)
	}
	for k, s := range all {
		want := 0
		if r10Meets(win, s.ValidFrom, s.ValidTo) {
			want = 1
		}
		if got[k] != want {
			t.Fatalf("pin %d window %v: segment %s streamed %d times, want %d", pin, win, k, got[k], want)
		}
	}
	for k := range got {
		if _, ok := all[k]; !ok {
			t.Fatalf("pin %d window %v: segment %s not in the unwindowed scan", pin, win, k)
		}
	}
	for _, iv := range win {
		for _, at := range []types.Instant{iv.From, iv.From + 1, iv.To - 1} {
			if at < iv.From || at >= iv.To {
				continue
			}
			want := map[types.NodeID]bool{}
			rows, err := g.Nodes().ByLabel(w.label, storepkg.QueryOpts{ValidAt: at, ValidAtSet: true, TxAt: pin, TxAtSet: true})
			if err != nil {
				t.Fatal(err)
			}
			for _, n := range rows {
				want[n.ID()] = true
			}
			have := map[types.NodeID]bool{}
			for k := range got {
				s := all[k]
				if s.ValidFrom <= at && (s.ValidTo == 0 || at < s.ValidTo) {
					have[s.Node.ID()] = true
				}
			}
			if fmt.Sprint(mapKeysSorted(have)) != fmt.Sprint(mapKeysSorted(want)) {
				t.Fatalf("pin %d window %v at %d: streamed %v, ByLabel %v", pin, win, at, mapKeysSorted(have), mapKeysSorted(want))
			}
		}
	}
}

func r10CheckRelWindow(t *testing.T, g *graphpkg.Graph, w *r10World, pin types.Instant, win temporal.Window, all map[string]temporal.RelSegment) {
	t.Helper()
	got := map[string]int{}
	if err := g.Temporal().ForEachRelEffectiveByTypeWindow("R", pin, win, func(s temporal.RelSegment) bool {
		got[relSegKey(s)]++
		return true
	}); err != nil {
		t.Fatalf("pin %d window %v: %v", pin, win, err)
	}
	for k, s := range all {
		want := 0
		if r10Meets(win, s.ValidFrom, s.ValidTo) {
			want = 1
		}
		if got[k] != want {
			t.Fatalf("rels pin %d window %v: segment %s streamed %d times, want %d", pin, win, k, got[k], want)
		}
	}
	for k := range got {
		if _, ok := all[k]; !ok {
			t.Fatalf("rels pin %d window %v: segment %s not in the unwindowed scan", pin, win, k)
		}
	}
	for _, iv := range win {
		for _, at := range []types.Instant{iv.From, iv.To - 1} {
			if at < iv.From || at >= iv.To {
				continue
			}
			rows, err := g.Rels().ByType("R", storepkg.QueryOpts{ValidAt: at, ValidAtSet: true, TxAt: pin, TxAtSet: true})
			if err != nil {
				t.Fatal(err)
			}
			want := map[types.RelID]bool{}
			for _, r := range rows {
				want[r.ID()] = true
			}
			have := map[types.RelID]bool{}
			for k := range got {
				s := all[k]
				if s.ValidFrom <= at && (s.ValidTo == 0 || at < s.ValidTo) {
					have[s.Rel.ID()] = true
				}
			}
			if fmt.Sprint(mapKeysSorted(have)) != fmt.Sprint(mapKeysSorted(want)) {
				t.Fatalf("rels pin %d window %v at %d: streamed %v, ByType %v", pin, win, at, mapKeysSorted(have), mapKeysSorted(want))
			}
		}
	}
}

func r10CheckAdjacencyWindow(t *testing.T, g *graphpkg.Graph, w *r10World, pin types.Instant, win temporal.Window) {
	t.Helper()
	for _, c := range []struct {
		dir   temporal.Direction
		types []string
	}{{temporal.Outgoing, []string{"R"}}, {temporal.Incoming, []string{"S"}}, {temporal.Incoming, nil}, {temporal.Outgoing, []string{"R", "S"}}} {
		seeds := []types.NodeID{w.hub, w.hub}
		var all, got []temporal.RelSegment
		if err := g.Temporal().ForEachRelEffectiveAtNodes(seeds, c.dir, c.types, pin, func(s temporal.RelSegment) bool {
			if r10Meets(win, s.ValidFrom, s.ValidTo) {
				all = append(all, s)
			}
			return true
		}); err != nil {
			t.Fatal(err)
		}
		if err := g.Temporal().ForEachRelEffectiveAtNodesWindow(seeds, c.dir, c.types, pin, win, func(s temporal.RelSegment) bool {
			got = append(got, s)
			return true
		}); err != nil {
			t.Fatalf("adjacency %v %v pin %d window %v: %v", c.dir, c.types, pin, win, err)
		}
		keys := func(segs []temporal.RelSegment) []string {
			out := make([]string, len(segs))
			for i, s := range segs {
				out[i] = relSegKey(s)
			}
			return out
		}
		if !slices.Equal(keys(got), keys(all)) {
			t.Fatalf("adjacency %v %v pin %d window %v: %v, want %v", c.dir, c.types, pin, win, keys(got), keys(all))
		}
		// The typed form: the same segments as typed rows.
		row := 0
		err := g.Temporal().ScanRelEffectiveColumnsAtNodes(seeds, c.dir, c.types, pin, win, []string{"n", "name"}, func(bt *storepkg.RelColumnBatch) bool {
			wantType := ""
			if len(c.types) == 1 {
				wantType = c.types[0]
			}
			if bt.RelType != wantType {
				t.Fatalf("batch type %q, want %q", bt.RelType, wantType)
			}
			for k := range bt.IDs {
				s := got[row]
				row++
				if bt.IDs[k] != s.Rel.ID() || bt.StartIDs[k] != s.Rel.StartNodeID() || bt.EndIDs[k] != s.Rel.EndNodeID() ||
					bt.ValidFrom[k] != int64(s.ValidFrom) || bt.ValidTo[k] != int64(s.ValidTo) ||
					bt.Ordinals[k] != 0 || bt.StartOrdinals[k] != 0 || bt.EndOrdinals[k] != 0 {
					t.Fatalf("typed row %d: id %d %d->%d [%d, %d) ords %d/%d/%d, segment %s", row-1, bt.IDs[k], bt.StartIDs[k], bt.EndIDs[k],
						bt.ValidFrom[k], bt.ValidTo[k], bt.Ordinals[k], bt.StartOrdinals[k], bt.EndOrdinals[k], relSegKey(s))
				}
				n, hasN := s.Rel.GetProperty("n")
				if bt.Null[0][k] == hasN || (hasN && bt.Ints[0][k] != n.(int64)) {
					t.Fatalf("typed row %d: n null %v value %v, row %v", row-1, bt.Null[0][k], bt.Ints[0], n)
				}
				name, hasName := s.Rel.GetProperty("name")
				if bt.Null[1][k] == hasName || (hasName && bt.Strs[1][k] != name.(string)) {
					t.Fatalf("typed row %d: name null %v, row %v", row-1, bt.Null[1][k], name)
				}
			}
			return true
		})
		if err != nil || row != len(got) {
			t.Fatalf("typed adjacency %v %v pin %d window %v: %d rows of %d, err %v", c.dir, c.types, pin, win, row, len(got), err)
		}
	}
}

// TestRound10WindowDoorsRefuse: a window out of order, overlapping or with
// an empty interval is ErrInvalidTimeRange on every windowed door, a bad
// pin the effective scans' error, a nil callback ErrNilCallback; the empty
// window streams nothing but still checks the gates.
func TestRound10WindowDoorsRefuse(t *testing.T) {
	g, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	ctx := context.Background()
	hub, _ := g.Nodes().Add(ctx, []string{"Ref"}, map[string]any{"tkg_valid_from": types.Instant(-10)})
	other, _ := g.Nodes().Add(ctx, []string{"Ref"}, nil)
	if _, err := g.Rels().Add(ctx, "R", hub, other, map[string]any{"w": 1.5}); err != nil {
		t.Fatal(err)
	}
	third, _ := g.Nodes().Add(ctx, []string{"Ref"}, nil)
	if _, err := g.Rels().Add(ctx, "R", hub, third, map[string]any{"w": int64(2)}); err != nil {
		t.Fatal(err)
	}
	pin, _ := g.Temporal().NowTx()
	seeds := []types.NodeID{hub.ID()}
	noN := func(temporal.NodeSegment) bool { t.Fatal("streamed"); return false }
	noR := func(temporal.RelSegment) bool { t.Fatal("streamed"); return false }
	doors := func(win temporal.Window, p types.Instant) []error {
		return []error{
			g.Temporal().ForEachNodeEffectiveByLabelsWindow([]string{"Ref"}, p, win, noN),
			g.Temporal().ForEachRelEffectiveByTypeWindow("R", p, win, noR),
			g.Temporal().ForEachRelEffectiveAtNodesWindow(seeds, temporal.Outgoing, nil, p, win, noR),
			g.Temporal().ScanRelEffectiveColumnsAtNodes(seeds, temporal.Outgoing, nil, p, win, nil, func(*storepkg.RelColumnBatch) bool { t.Fatal("batch"); return false }),
		}
	}
	for _, bad := range []temporal.Window{{{From: 5, To: 5}}, {{From: 0, To: 10}, {From: 5, To: 20}}, {{From: 10, To: 20}, {From: -5, To: 0}}} {
		for i, err := range doors(bad, pin) {
			if !errors.Is(err, graphpkg.ErrInvalidTimeRange) {
				t.Fatalf("door %d window %v: %v, want ErrInvalidTimeRange", i, bad, err)
			}
		}
	}
	for i, err := range doors(nil, pin) {
		if err != nil {
			t.Fatalf("door %d empty window: %v", i, err)
		}
	}
	for _, p := range []types.Instant{0, -1} {
		for i, err := range doors(nil, p) {
			if !errors.Is(err, graphpkg.ErrInvalidTimeRange) {
				t.Fatalf("door %d pin %d: %v, want ErrInvalidTimeRange", i, p, err)
			}
		}
	}
	any := temporal.Window{{From: types.UnboundedValidFrom, To: math.MaxInt64}}
	if err := g.Temporal().ForEachNodeEffectiveByLabelsWindow([]string{"Ref"}, pin, any, nil); !errors.Is(err, grapherr.ErrNilCallback) {
		t.Fatalf("nil callback: %v", err)
	}
	if err := g.Temporal().ScanRelEffectiveColumnsAtNodes(seeds, temporal.Outgoing, nil, pin, any, nil, nil); !errors.Is(err, grapherr.ErrNilCallback) {
		t.Fatalf("nil callback: %v", err)
	}
	// A column of integers and floats refuses, as every column scan does.
	err = g.Temporal().ScanRelEffectiveColumnsAtNodes(seeds, temporal.Outgoing, []string{"R"}, pin, any, []string{"w"}, func(*storepkg.RelColumnBatch) bool { return true })
	if !errors.Is(err, graphpkg.ErrMixedNumericColumn) {
		t.Fatalf("mixed column: %v, want ErrMixedNumericColumn", err)
	}
	// A store created under the default is UntimedUnbounded: the untimed
	// nodes hold from -inf, the node valid from -10 from there on.
	for _, c := range []struct {
		win  temporal.Window
		want []types.NodeID
	}{
		{temporal.Window{{From: -11, To: -10}}, []types.NodeID{other.ID(), third.ID()}},
		{temporal.Window{{From: -10, To: -9}}, []types.NodeID{hub.ID(), other.ID(), third.ID()}},
		{temporal.Window{{From: types.UnboundedValidFrom, To: types.UnboundedValidFrom + 1}}, []types.NodeID{other.ID(), third.ID()}},
	} {
		var seen []types.NodeID
		if err := g.Temporal().ForEachNodeEffectiveByLabelsWindow([]string{"Ref"}, pin, c.win, func(s temporal.NodeSegment) bool {
			seen = append(seen, s.Node.ID())
			return true
		}); err != nil {
			t.Fatal(err)
		}
		slices.Sort(seen)
		want := slices.Sorted(slices.Values(c.want))
		if !slices.Equal(seen, want) {
			t.Fatalf("window %v: %v, want %v", c.win, seen, want)
		}
	}
}

// r10Spans are oracle spans with the round-9 untimed rule.
type r10Span struct{ from, to types.Instant }

func r10CountAt(spans []r10Span, at types.Instant) int64 {
	var n int64
	for _, s := range spans {
		if s.from <= at && (s.to == 0 || at < s.to) {
			n++
		}
	}
	return n
}

// r10Hull is the oracle hull of spans.
func r10Hull(spans []r10Span) storepkg.ValidHull {
	var live []r10Span
	for _, s := range spans {
		if s.to == 0 || s.to > s.from {
			live = append(live, s)
		}
	}
	if len(live) == 0 {
		return storepkg.ValidHull{Empty: true}
	}
	h := storepkg.ValidHull{From: live[0].from}
	for _, s := range live {
		h.From = min(h.From, s.from)
		if s.to == 0 {
			h.OpenEnd = true
		} else if !h.OpenEnd {
			h.To = max(h.To, s.to)
		}
	}
	if h.OpenEnd {
		h.To = 0
	}
	return h
}

// r10CurrentSpans is the oracle of the current rows: each row ForEachByLabel
// (ForEachByType) hands out over [valid-from or its TxFrom, valid-to), from
// -inf under untimed where the row starts at an untimed create (round9Untimed
// on the row as the first segment, the genesis read from History).
func r10CurrentSpans(t *testing.T, g *graphpkg.Graph, rel bool, name string, untimed bool) []r10Span {
	t.Helper()
	var spans []r10Span
	add := func(vf, vt, tx types.Instant, version uint32, genesis func() (types.Instant, types.Instant, bool)) {
		from := vf
		if from == 0 {
			from = tx
		}
		if untimed && round9Untimed(from, vf, tx, version, genesis) {
			from = math.MinInt64
		}
		spans = append(spans, r10Span{from, vt})
	}
	var err error
	if rel {
		err = g.Rels().ForEachByType(name, storepkg.QueryOpts{}, func(r *types.Relationship) bool {
			vf, vt, _ := r.ValidRange()
			tx, _, _ := r.TxStamps()
			add(vf, vt, tx, r.Version(), func() (types.Instant, types.Instant, bool) {
				h, err := g.Rels().History(r.ID())
				if err != nil {
					return 0, 0, false
				}
				for _, v := range h {
					if v.Version() == 0 && v.Temporal() != nil {
						return v.Temporal().ValidFrom, v.Temporal().TxFrom, true
					}
				}
				return 0, 0, false
			})
			return true
		})
	} else {
		err = g.Nodes().ForEachByLabel(name, storepkg.QueryOpts{}, func(n *types.Node) bool {
			vf, vt, _ := n.ValidRange()
			tx, _, _ := n.TxStamps()
			add(vf, vt, tx, n.Version(), func() (types.Instant, types.Instant, bool) {
				h, err := g.Nodes().History(n.ID())
				if err != nil {
					return 0, 0, false
				}
				for _, v := range h {
					if v.Version() == 0 && v.Temporal() != nil {
						return v.Temporal().ValidFrom, v.Temporal().TxFrom, true
					}
				}
				return 0, 0, false
			})
			return true
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	return spans
}

// r10PinnedSpans is the oracle of the state at pin: every segment of the
// effective scan, an entity's first from -inf under untimed (round9Untimed).
func r10PinnedSpans(t *testing.T, g *graphpkg.Graph, rel bool, name string, pin types.Instant, untimed bool) []r10Span {
	t.Helper()
	var spans []r10Span
	var last int64
	add := func(id int64, segFrom, segTo, vf, tx types.Instant, version uint32, genesis func() (types.Instant, types.Instant, bool)) {
		from := segFrom
		if id != last {
			last = id
			if untimed && round9Untimed(segFrom, vf, tx, version, genesis) {
				from = math.MinInt64
			}
		}
		spans = append(spans, r10Span{from, segTo})
	}
	var err error
	if rel {
		err = g.Temporal().ForEachRelEffectiveByType(name, pin, func(s temporal.RelSegment) bool {
			vf, _, _ := s.Rel.ValidRange()
			tx, _, _ := s.Rel.TxStamps()
			add(int64(s.Rel.ID()), s.ValidFrom, s.ValidTo, vf, tx, s.Rel.Version(), func() (types.Instant, types.Instant, bool) {
				h, err := g.Rels().History(s.Rel.ID())
				if err != nil {
					return 0, 0, false
				}
				for _, v := range h {
					if v.Version() == 0 && v.Temporal() != nil {
						return v.Temporal().ValidFrom, v.Temporal().TxFrom, true
					}
				}
				return 0, 0, false
			})
			return true
		})
	} else {
		err = g.Temporal().ForEachNodeEffectiveByLabel(name, pin, func(s temporal.NodeSegment) bool {
			vf, _, _ := s.Node.ValidRange()
			tx, _, _ := s.Node.TxStamps()
			add(int64(s.Node.ID()), s.ValidFrom, s.ValidTo, vf, tx, s.Node.Version(), func() (types.Instant, types.Instant, bool) {
				h, err := g.Nodes().History(s.Node.ID())
				if err != nil {
					return 0, 0, false
				}
				for _, v := range h {
					if v.Version() == 0 && v.Temporal() != nil {
						return v.Temporal().ValidFrom, v.Temporal().TxFrom, true
					}
				}
				return 0, 0, false
			})
			return true
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	return spans
}

// r10CheckTimeline compares segs with the oracle spans at every boundary
// instant (and its neighbours) and checks the segment shape.
func r10CheckTimeline(t *testing.T, what string, segs []storepkg.CountSegment, spans []r10Span) {
	t.Helper()
	checkSegments(t, segs)
	at := map[types.Instant]bool{}
	for _, e := range r10Edges {
		at[e] = true
	}
	for _, s := range spans {
		for _, x := range []types.Instant{s.from, s.from + 1, s.to, s.to - 1} {
			if x != 0 {
				at[x] = true
			}
		}
	}
	for _, s := range segs {
		for _, x := range []types.Instant{s.From, s.From - 1, s.To, s.To - 1} {
			at[x] = true
		}
	}
	for x := range at {
		if got, want := timelineAt(segs, x), r10CountAt(spans, x); got != want {
			t.Fatalf("%s at %d: timeline %d, oracle %d (segments %v)", what, x, got, want, segs)
		}
	}
}

// TestRound10CurrentRowsTimelineAndHull (requests 4 and 2a): on every
// backend and untimed mode, over random histories:
//   - CountTimelineByLabel / ByType with CurrentRows equal, at every
//     boundary instant, the count of the current rows ForEachByLabel /
//     ForEachByType hands out over their own intervals (untimed from -inf
//     under the option), and differ from the pinned timeline where members
//     have valid-time history (the reason for the door);
//   - ValidHullByLabel / ByType equal the oracle hull of those spans
//     (current rows) and of the effective scan's segments (at pins), with
//     explicit Empty and OpenEnd;
//   - CurrentRows with a pin is ErrInvalidTimeRange.
func TestRound10CurrentRowsTimelineAndHull(t *testing.T) {
	_, _, seeds := r10Size()
	for _, b := range r10Backends() {
		t.Run(b.name, func(t *testing.T) {
			ops := map[string]int{}
			defer func() { r10CheckCoverage(t, b, ops) }()
			for _, seed := range seeds {
				g := b.open(t)
				w := r10History(t, b, g, seed)
				for op, n := range w.ops {
					ops[op] += n
				}
				differs := false
				for _, untimed := range []bool{false, true} {
					opts := storepkg.CountTimelineOpts{UntimedFromMinusInf: untimed, CurrentRows: true}
					for _, c := range []struct {
						rel  bool
						name string
					}{{false, w.label}, {true, "R"}, {true, "S"}} {
						var segs []storepkg.CountSegment
						var hull storepkg.ValidHull
						var exact, hexact bool
						var err, herr error
						if c.rel {
							segs, exact, err = g.Rels().CountTimelineByType(c.name, 0, opts)
							hull, hexact, herr = g.Rels().ValidHullByType(c.name, 0, opts)
						} else {
							segs, exact, err = g.Nodes().CountTimelineByLabel(c.name, 0, opts)
							hull, hexact, herr = g.Nodes().ValidHullByLabel(c.name, 0, opts)
						}
						if err != nil || !exact || herr != nil || !hexact {
							t.Fatalf("%s current rows: exact %v %v, err %v %v", c.name, exact, hexact, err, herr)
						}
						spans := r10CurrentSpans(t, g, c.rel, c.name, untimed)
						r10CheckTimeline(t, c.name+" current rows", segs, spans)
						if want := r10Hull(spans); hull != want {
							t.Fatalf("%s current rows hull %+v, oracle %+v", c.name, hull, want)
						}
						pinned := r10PinnedSpans(t, g, c.rel, c.name, w.pins[len(w.pins)-1], untimed)
						if len(pinned) != len(spans) {
							differs = true
						}
						for _, pin := range r10ProbePins(w) {
							popts := storepkg.CountTimelineOpts{UntimedFromMinusInf: untimed}
							if c.rel {
								hull, hexact, herr = g.Rels().ValidHullByType(c.name, pin, popts)
							} else {
								hull, hexact, herr = g.Nodes().ValidHullByLabel(c.name, pin, popts)
							}
							if herr != nil || !hexact {
								t.Fatalf("%s hull at %d: exact %v err %v", c.name, pin, hexact, herr)
							}
							if want := r10Hull(r10PinnedSpans(t, g, c.rel, c.name, pin, untimed)); hull != want {
								t.Fatalf("%s hull at %d: %+v, oracle %+v", c.name, pin, hull, want)
							}
						}
					}
				}
				if !differs {
					t.Errorf("seed %d: no member with valid-time history (the current rows equal the pinned state)", seed)
				}
				if _, _, err := g.Nodes().CountTimelineByLabel(w.label, w.pins[0], storepkg.CountTimelineOpts{CurrentRows: true}); !errors.Is(err, graphpkg.ErrInvalidTimeRange) {
					t.Fatalf("CurrentRows at a pin: %v", err)
				}
				if _, _, err := g.Rels().ValidHullByType("R", -3, storepkg.CountTimelineOpts{CurrentRows: true}); !errors.Is(err, graphpkg.ErrInvalidTimeRange) {
					t.Fatalf("CurrentRows hull at a pin: %v", err)
				}
				if h, ok, err := g.Nodes().ValidHullByLabel("NoSuchLabel", 0, storepkg.CountTimelineOpts{}); err != nil || !ok || !h.Empty {
					t.Fatalf("unknown label hull: %+v %v %v", h, ok, err)
				}
			}
		})
	}
}

// TestRound10HullTwoPhase (2a, two-phase): the hull at a pin keeps the
// pin's answer after later writes; the current hull and the current-rows
// timeline follow them; a valid-time rewrite stamped at or below the pin
// (AllowTxBackfill) moves the pin's hull.
func TestRound10HullTwoPhase(t *testing.T) {
	for _, b := range r10Backends()[:4] {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			ctx := context.Background()
			a, err := g.Nodes().Add(ctx, []string{"P"}, map[string]any{"tkg_valid_from": types.Instant(-500), "tkg_valid_to": types.Instant(-200)})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := g.Nodes().Add(ctx, []string{"P"}, map[string]any{"tkg_valid_from": types.Instant(100), "tkg_valid_to": types.Instant(400)}); err != nil {
				t.Fatal(err)
			}
			pin, _ := g.Temporal().NowTx()
			want := storepkg.ValidHull{From: -500, To: 400}
			check := func(stage string, p types.Instant, opts storepkg.CountTimelineOpts, want storepkg.ValidHull) {
				t.Helper()
				h, ok, err := g.Nodes().ValidHullByLabel("P", p, opts)
				if err != nil || !ok || h != want {
					t.Fatalf("%s: hull %+v %v %v, want %+v", stage, h, ok, err, want)
				}
			}
			check("pin", pin, storepkg.CountTimelineOpts{}, want)
			check("current rows", 0, storepkg.CountTimelineOpts{CurrentRows: true}, want)
			c, err := g.Nodes().Add(ctx, []string{"P"}, map[string]any{"tkg_valid_from": types.UnboundedValidFrom})
			if err != nil {
				t.Fatal(err)
			}
			check("pin after a later create", pin, storepkg.CountTimelineOpts{}, want)
			check("current rows after it", 0, storepkg.CountTimelineOpts{CurrentRows: true}, storepkg.ValidHull{From: types.UnboundedValidFrom, OpenEnd: true})
			if err := g.Nodes().Delete(ctx, c.ID()); err != nil {
				t.Fatal(err)
			}
			check("current rows after its delete", 0, storepkg.CountTimelineOpts{CurrentRows: true}, want)
			// A rewrite stamped below the pin changes the pin's belief.
			if _, err := g.Temporal().SetNodeVersionIntervalWithTx(ctx, a.ID(), -900, -800, map[string]any{"x": int64(1)}, pin-1); err != nil {
				t.Fatal(err)
			}
			check("pin after a rewrite below it", pin, storepkg.CountTimelineOpts{}, storepkg.ValidHull{From: -900, To: 400})
			if _, _, err := g.Nodes().ValidHullByLabel("P", 0, storepkg.CountTimelineOpts{}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestRound10ClassCountsAtPin (request 3): on every backend, at pins across
// random histories, PropertyTypeClassCountsAt / RelPropertyTypeClassCountsAt
//   - count the indexable classes (Numeric, NaN, String, Bool) exactly as
//     many rows as PropertyValueStats at the pin counts (one rule for the
//     rows at a pin; memory and badger, where it is served);
//   - count at least every distinct row (id, version) the effective scan
//     at the pin shows carrying the label, in its class (a row that wins
//     some valid instant is a row the any-version equality reads);
//   - answer again unchanged after later writes (two-phase) and from the
//     cache;
//   - refuse a non-positive pin (ErrInvalidTimeRange) and answer an unknown
//     label with zero counts.
func TestRound10ClassCountsAtPin(t *testing.T) {
	_, _, seeds := r10Size()
	for _, b := range r10Backends() {
		t.Run(b.name, func(t *testing.T) {
			ops := map[string]int{}
			defer func() { r10CheckCoverage(t, b, ops) }()
			for _, seed := range seeds {
				g := b.open(t)
				if err := g.Index().CreateProperty("Ref", "v"); err != nil {
					t.Fatal(err)
				}
				served := (b.name == "memory" || b.name == "badger" || b.name == "memory-txfrom" || b.name == "badger-txfrom")
				w := r10History(t, b, g, seed)
				for op, n := range w.ops {
					ops[op] += n
				}
				answers := map[types.Instant]storepkg.PropertyTypeClassCounts{}
				for _, pin := range r10ProbePins(w) {
					got, exact, err := g.Stats().PropertyTypeClassCountsAt(w.label, "v", pin)
					if err != nil || !exact {
						t.Fatalf("pin %d: exact %v err %v", pin, exact, err)
					}
					answers[pin] = got
					if served {
						st, sexact, err := g.Stats().PropertyValueStats(w.label, "v", pin)
						if err != nil || !sexact {
							t.Fatalf("pin %d value stats: exact %v err %v", pin, sexact, err)
						}
						if idx := got.Numeric + got.NaN + got.String + got.Bool; idx != st.Rows {
							t.Fatalf("pin %d: indexable classes %d (%+v), value statistics rows %d", pin, idx, got, st.Rows)
						}
					}
					// Lower bound: the rows the effective scan shows.
					var lb storepkg.PropertyTypeClassCounts
					seen := map[string]bool{}
					if err := g.Temporal().ForEachNodeEffectiveByLabel(w.label, pin, func(s temporal.NodeSegment) bool {
						k := fmt.Sprintf("%d/%d", s.Node.ID(), s.Node.Version())
						if seen[k] {
							return true
						}
						seen[k] = true
						v, ok := s.Node.GetProperty("v")
						switch x := v.(type) {
						case nil:
							if !ok {
								lb.Missing++
							}
						case float64:
							if math.IsNaN(x) {
								lb.NaN++
							} else {
								lb.Numeric++
							}
						case float32:
							lb.Numeric++
						case string:
							lb.String++
						case bool:
							lb.Bool++
						case int64, int8, uint64:
							lb.Numeric++
						default:
							lb.Other++
						}
						return true
					}); err != nil {
						t.Fatal(err)
					}
					if got.Numeric < lb.Numeric || got.NaN < lb.NaN || got.String < lb.String || got.Bool < lb.Bool || got.Other < lb.Other || got.Missing < lb.Missing {
						t.Fatalf("pin %d: %+v below the effective scan's rows %+v", pin, got, lb)
					}
					if again, _, _ := g.Stats().PropertyTypeClassCountsAt(w.label, "v", pin); again != got {
						t.Fatalf("pin %d: cached %+v, first %+v", pin, again, got)
					}
					rc, rexact, err := g.Stats().RelPropertyTypeClassCountsAt("R", "w", pin)
					if err != nil || !rexact || rc.Present() == 0 {
						t.Fatalf("pin %d rels: %+v exact %v err %v", pin, rc, rexact, err)
					}
				}
				// Phase 2: later writes leave the pins' answers.
				ctx := context.Background()
				for _, id := range w.nodes[:4] {
					_, _ = g.Nodes().Update(ctx, id, map[string]any{"v": []string{"later"}, "tkg_valid_from": types.Instant(90000)})
				}
				for pin, want := range answers {
					if got, _, err := g.Stats().PropertyTypeClassCountsAt(w.label, "v", pin); err != nil || got != want {
						t.Fatalf("pin %d after later writes: %+v %v, want %+v", pin, got, err, want)
					}
				}
				for _, p := range []types.Instant{0, -1} {
					if _, _, err := g.Stats().PropertyTypeClassCountsAt(w.label, "v", p); !errors.Is(err, graphpkg.ErrInvalidTimeRange) {
						t.Fatalf("pin %d: %v, want ErrInvalidTimeRange", p, err)
					}
					if _, _, err := g.Stats().RelPropertyTypeClassCountsAt("R", "w", p); !errors.Is(err, graphpkg.ErrInvalidTimeRange) {
						t.Fatalf("rel pin %d: %v, want ErrInvalidTimeRange", p, err)
					}
				}
				if c, ok, err := g.Stats().PropertyTypeClassCountsAt("NoSuchLabel", "v", w.pins[0]); err != nil || !ok || c != (storepkg.PropertyTypeClassCounts{}) {
					t.Fatalf("unknown label: %+v %v %v", c, ok, err)
				}
			}
		})
	}
}

// TestRound10ClassCountsExact (request 3, exact scenario, two-phase): one
// row per version recorded at the pin, every class: an int and a float of
// one value both Numeric, NaN, a list and a map in Other, a bool, a string,
// a row without the key in Missing; a deleted node's rows before the delete
// count, a retracted life's rows do not, an excise appends no counted row of
// its own; a rewrite below the pin moves the pin's answer.
func TestRound10ClassCountsExact(t *testing.T) {
	for _, b := range r10Backends()[:4] {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			ctx := context.Background()
			add := func(props map[string]any) types.NodeID {
				t.Helper()
				n, err := g.Nodes().Add(ctx, []string{"P"}, props)
				if err != nil {
					t.Fatal(err)
				}
				return n.ID()
			}
			a := add(map[string]any{"v": int64(3), "tkg_valid_from": types.Instant(-100)})
			if _, err := g.Nodes().Update(ctx, a, map[string]any{"v": 3.0, "tkg_valid_from": types.Instant(50)}); err != nil {
				t.Fatal(err)
			}
			add(map[string]any{"v": math.NaN()})
			add(map[string]any{"v": []string{"x"}})
			add(map[string]any{"v": map[string]any{"k": int64(1)}})
			add(map[string]any{"v": true})
			live := add(map[string]any{"v": "s", "tkg_valid_from": types.Instant(-1000)})
			add(map[string]any{"other": int64(1)})
			d := add(map[string]any{"v": "gone", "tkg_valid_from": types.UnboundedValidFrom})
			if err := g.Nodes().Delete(ctx, d); err != nil {
				t.Fatal(err)
			}
			r := add(map[string]any{"v": "retracted"})
			if err := g.Nodes().Retract(ctx, r); err != nil {
				t.Fatal(err)
			}
			if err := g.Temporal().ExciseNodeValidity(ctx, a, -50, -20); err != nil {
				t.Fatal(err)
			}
			pin, _ := g.Temporal().NowTx()
			got, ok, err := g.Stats().PropertyTypeClassCountsAt("P", "v", pin)
			// a: two versions (int64 3 from -100, float64 3 from 50) plus the
			// excise's resumption row (int64 3 from -20): Numeric 3; the void
			// piece is no row the equality door reads (eachNodeChainDuring
			// skips void rows), so it counts nowhere.
			if err != nil || !ok {
				t.Fatalf("counts: %v %v", ok, err)
			}
			if got.NaN != 1 || got.Other != 2 || got.Bool != 1 || got.Missing != 1 || got.String != 2 || got.Numeric != 3 {
				t.Fatalf("counts %+v", got)
			}
			if err := g.Nodes().Delete(ctx, a); err != nil {
				t.Fatal(err)
			}
			if again, _, _ := g.Stats().PropertyTypeClassCountsAt("P", "v", pin); again != got {
				t.Fatalf("after a later delete: %+v, want %+v", again, got)
			}
			if _, err := g.Temporal().SetNodeVersionIntervalWithTx(ctx, live, -900, -800, map[string]any{"v": "rewritten"}, pin-1); err != nil {
				t.Fatal(err)
			}
			// The correction's rows (the rewritten piece, the resumption that
			// re-asserts "s") are recorded at pin-1: the pin's rows grow by
			// strings only.
			moved, _, err := g.Stats().PropertyTypeClassCountsAt("P", "v", pin)
			if err != nil || moved.String <= got.String || moved.Numeric != got.Numeric || moved.NaN != got.NaN ||
				moved.Bool != got.Bool || moved.Other != got.Other || moved.Missing != got.Missing {
				t.Fatalf("after a rewrite below the pin: %+v %v, before %+v", moved, err, got)
			}
		})
	}
}

// TestRound10ExciseNegativeStart: an excise may start at a negative instant
// and at types.UnboundedValidFrom (a real bound since sigma round 10, as on the read doors since v4.58.1); 0 stays
// refused (no stored piece can start at the unset valid-from).
func TestRound10ExciseNegativeStart(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		ctx := context.Background()
		n, err := g.Nodes().Add(ctx, []string{"P"}, map[string]any{"tkg_valid_from": types.UnboundedValidFrom, "v": int64(1)})
		if err != nil {
			t.Fatal(err)
		}
		r, err := g.Rels().Add(ctx, "R", n, n, map[string]any{"tkg_valid_from": types.Instant(-5000)})
		if err != nil {
			t.Fatal(err)
		}
		if err := g.Temporal().ExciseNodeValidity(ctx, n.ID(), -4500, -4200); err != nil {
			t.Fatalf("negative excise: %v", err)
		}
		if err := g.Temporal().ExciseNodeValidity(ctx, n.ID(), types.UnboundedValidFrom, -9000); err != nil {
			t.Fatalf("excise from -inf: %v", err)
		}
		if err := g.Temporal().ExciseRelValidity(ctx, r.ID(), -4500, -4200); err != nil {
			t.Fatalf("negative rel excise: %v", err)
		}
		if err := g.Temporal().ExciseNodeValidity(ctx, n.ID(), 0, 10); !errors.Is(err, graphpkg.ErrInvalidTimeRange) {
			t.Fatalf("excise from 0: %v, want ErrInvalidTimeRange", err)
		}
		for _, at := range []types.Instant{-4500, -4201, -10000} {
			if _, err := g.Temporal().NodeAtTx(n.ID(), at, 0); !errors.Is(err, graphpkg.ErrNoVersionValidAt) {
				t.Fatalf("node at %d inside an excised range: %v", at, err)
			}
		}
		for _, at := range []types.Instant{-4501, -4200, -8999} {
			if _, err := g.Temporal().NodeAtTx(n.ID(), at, 0); err != nil {
				t.Fatalf("node at %d outside the excised ranges: %v", at, err)
			}
		}
		if _, err := g.Temporal().RelAtTx(r.ID(), -4300, 0); !errors.Is(err, graphpkg.ErrNoVersionValidAt) {
			t.Fatalf("rel inside the excised range: %v", err)
		}
	})
}

// TestRound10IntervalEndIsExclusive pins the one overlap rule
// (storeutil.IntervalMeets) at the doors that share it: a version starting
// exactly at an interval's end does not meet [start, end), one ending
// exactly at its start does not either (the interval resolver of
// ByLabel / ByType, the windowed reads), on every backend, at negative
// instants too.
func TestRound10IntervalEndIsExclusive(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		ctx := context.Background()
		late, err := g.Nodes().Add(ctx, []string{"P"}, map[string]any{"tkg_valid_from": types.Instant(-20)})
		if err != nil {
			t.Fatal(err)
		}
		early, err := g.Nodes().Add(ctx, []string{"P"}, map[string]any{"tkg_valid_from": types.Instant(-90), "tkg_valid_to": types.Instant(-50)})
		if err != nil {
			t.Fatal(err)
		}
		pin, _ := g.Temporal().NowTx()
		for _, c := range []struct {
			start, end types.Instant
			want       []types.NodeID
		}{
			{-50, -20, nil},
			{-51, -20, []types.NodeID{early.ID()}},
			{-50, -19, []types.NodeID{late.ID()}},
		} {
			rows, err := g.Nodes().ByLabel("P", storepkg.QueryOpts{ValidStart: c.start, ValidEnd: c.end, TxAt: pin})
			if err != nil {
				t.Fatal(err)
			}
			var got []types.NodeID
			for _, n := range rows {
				got = append(got, n.ID())
			}
			var seen []types.NodeID
			if err := g.Temporal().ForEachNodeEffectiveByLabelsWindow([]string{"P"}, pin, temporal.Window{{From: c.start, To: c.end}}, func(s temporal.NodeSegment) bool {
				seen = append(seen, s.Node.ID())
				return true
			}); err != nil {
				t.Fatal(err)
			}
			want := slices.Sorted(slices.Values(c.want))
			slices.Sort(got)
			slices.Sort(seen)
			if !slices.Equal(got, want) || !slices.Equal(seen, want) {
				t.Fatalf("[%d, %d): ByLabel %v, window %v, want %v", c.start, c.end, got, seen, want)
			}
		}
	})
}

// TestRound10ByIDsAnyVersion (request 6): on every backend and untimed mode,
// at pins across random histories, ForEachByIDsAnyVersion hands out, per
// node of a shuffled ID set with repeats, every row recorded at the pin in
// ascending ID order: the label's rows among them are exactly the rows
// PropertyTypeClassCountsAt counts (one rule for "every version at a pin"),
// they include every row the effective scan shows winning a valid instant,
// every row was written at or before the pin, a node created after the pin
// gives nothing; the relationship mirror likewise against
// RelPropertyTypeClassCountsAt; a later write leaves a pin's answer
// (two-phase); a bad pin, ID or callback is refused before any row.
func TestRound10ByIDsAnyVersion(t *testing.T) {
	_, _, seeds := r10Size()
	for _, b := range r10Backends() {
		t.Run(b.name, func(t *testing.T) {
			for _, seed := range seeds {
				g := b.open(t)
				w := r10History(t, b, g, seed)
				rng := rand.New(rand.NewPCG(seed, 6))
				ids := slices.Clone(w.nodes)
				ids = append(ids, w.nodes[:3]...)
				rng.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
				rids := slices.Clone(w.rels)
				rids = append(rids, w.rels[len(w.rels)-2:]...)
				rng.Shuffle(len(rids), func(i, j int) { rids[i], rids[j] = rids[j], rids[i] })
				type key struct {
					id      int64
					version uint32
				}
				answers := map[types.Instant][]key{}
				for _, pin := range r10ProbePins(w) {
					var got []key
					var labelRows int64
					var last types.NodeID
					if err := g.Nodes().ForEachByIDsAnyVersion(ids, pin, func(n *types.Node) bool {
						if n.ID() < last {
							t.Fatalf("pin %d: node %d after %d", pin, n.ID(), last)
						}
						last = n.ID()
						if tx, _, _ := n.TxStamps(); tx > pin {
							t.Fatalf("pin %d: row of node %d written at %d", pin, n.ID(), tx)
						}
						got = append(got, key{int64(n.ID()), n.Version()})
						if g.Nodes().HasLabel(n, w.label) {
							labelRows++
						}
						return true
					}); err != nil {
						t.Fatal(err)
					}
					answers[pin] = got
					counts, exact, err := g.Stats().PropertyTypeClassCountsAt(w.label, "v", pin)
					if err != nil || !exact {
						t.Fatalf("pin %d class counts: %v %v", pin, exact, err)
					}
					if want := counts.Present() + counts.Missing; labelRows != want {
						t.Fatalf("pin %d: %d rows carry the label, value classes count %d (%+v)", pin, labelRows, want, counts)
					}
					emitted := map[key]bool{}
					for _, k := range got {
						if emitted[k] {
							t.Fatalf("pin %d: row %v handed out twice", pin, k)
						}
						emitted[k] = true
					}
					if err := g.Temporal().ForEachNodeEffectiveByLabel(w.label, pin, func(s temporal.NodeSegment) bool {
						if !emitted[key{int64(s.Node.ID()), s.Node.Version()}] {
							t.Fatalf("pin %d: winning row %d v%d not handed out", pin, s.Node.ID(), s.Node.Version())
						}
						return true
					}); err != nil {
						t.Fatal(err)
					}
					var relRows int64
					var lastRel types.RelID
					if err := g.Rels().ForEachByIDsAnyVersion(rids, pin, func(r *types.Relationship) bool {
						if r.ID() < lastRel {
							t.Fatalf("pin %d: rel %d after %d", pin, r.ID(), lastRel)
						}
						lastRel = r.ID()
						relRows++
						return true
					}); err != nil {
						t.Fatal(err)
					}
					var wantRel int64
					for _, typ := range []string{"R", "S"} {
						rc, rexact, err := g.Stats().RelPropertyTypeClassCountsAt(typ, "w", pin)
						if err != nil || !rexact {
							t.Fatalf("pin %d rel class counts: %v %v", pin, rexact, err)
						}
						wantRel += rc.Present() + rc.Missing
					}
					if relRows != wantRel {
						t.Fatalf("pin %d: %d relationship rows, value classes count %d", pin, relRows, wantRel)
					}
				}
				// Phase 2: later writes, a node created after every pin.
				ctx := context.Background()
				late, err := g.Nodes().Add(ctx, []string{w.label}, map[string]any{"v": int64(1)})
				if err != nil {
					t.Fatal(err)
				}
				for _, id := range w.nodes[r10Witnesses:] {
					_, _ = g.Nodes().Update(ctx, id, map[string]any{"v": "later", "tkg_valid_from": types.Instant(95000)})
				}
				for pin, want := range answers {
					var got []key
					if err := g.Nodes().ForEachByIDsAnyVersion(append(slices.Clone(ids), late.ID()), pin, func(n *types.Node) bool {
						got = append(got, key{int64(n.ID()), n.Version()})
						return true
					}); err != nil {
						t.Fatal(err)
					}
					if !slices.Equal(got, want) {
						t.Fatalf("pin %d after later writes: %d rows, want the pin's %d", pin, len(got), len(want))
					}
				}
			}
			g := b.open(t)
			pin, _ := g.Temporal().NowTx()
			for _, p := range []types.Instant{0, -1} {
				if err := g.Nodes().ForEachByIDsAnyVersion(nil, p, func(*types.Node) bool { return true }); !errors.Is(err, graphpkg.ErrInvalidTimeRange) {
					t.Fatalf("pin %d: %v", p, err)
				}
			}
			if err := g.Nodes().ForEachByIDsAnyVersion([]types.NodeID{0}, pin, func(*types.Node) bool { t.Fatal("row"); return false }); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
				t.Fatalf("invalid ID: %v", err)
			}
			if err := g.Rels().ForEachByIDsAnyVersion([]types.RelID{-3}, pin, func(*types.Relationship) bool { t.Fatal("row"); return false }); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
				t.Fatalf("invalid rel ID: %v", err)
			}
			if err := g.Nodes().ForEachByIDsAnyVersion(nil, pin, nil); !errors.Is(err, grapherr.ErrNilCallback) {
				t.Fatalf("nil callback: %v", err)
			}
		})
	}
}
