package graph_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/constraints"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// Round 11, zero is an instant (Markus, 2026-10-17). Under
// Config.UntimedValidStart = UntimedWholeAxis the store's rows are on the
// literal valid axis (types.LiteralValidAxis): every stored ValidFrom and
// ValidTo is the instant it names, a write without a valid time claims the
// whole axis (types.UnboundedValidFrom, types.InfiniteValidTo), so the instant
// 0 is an ordinary start and an ordinary end. A legacy store
// (UntimedFromTxFrom, UntimedUnbounded) keeps reading a stored 0 as "no
// claim" / "open" and refuses a claim of the instant 0 with
// ErrZeroInstantLegacyAxis. No row carries a flag: the axis is the store's,
// decided at open.

func r11ZeroCfg(c *graphpkg.Config) { c.UntimedValidStart = graphpkg.UntimedWholeAxis }

// r11ZeroIDs is the world r11ZeroWorld builds.
type r11ZeroIDs struct {
	fromZero, toZero, whole, shift, closed types.NodeID
	rel                                    types.RelID
}

// r11ZeroWorld builds, on a literal-axis graph: a node valid [0, 10) (a plain
// 0 start), a node valid [-10, 0) (an end at the instant 0), an untimed node
// (the whole axis), a node valid from -100 updated from the explicit 0 (its
// first version ends at 0), a node valid from -5 closed at 0, and a
// relationship valid from 0 (types.ExplicitInstant(0), the same claim).
func r11ZeroWorld(t *testing.T, g *graphpkg.Graph) r11ZeroIDs {
	t.Helper()
	ctx := context.Background()
	add := func(props map[string]any) types.NodeID {
		t.Helper()
		n, err := g.Nodes().Add(ctx, []string{"Z"}, props)
		if err != nil {
			t.Fatalf("create %v: %v", props, err)
		}
		return n.ID()
	}
	var w r11ZeroIDs
	w.fromZero = add(map[string]any{"k": int64(1), types.ShadowValidFrom: int64(0), types.ShadowValidTo: int64(10)})
	w.toZero = add(map[string]any{"k": int64(2), types.ShadowValidFrom: int64(-10), types.ShadowValidTo: int64(0)})
	w.whole = add(map[string]any{"k": int64(3)})
	w.shift = add(map[string]any{"k": int64(4), types.ShadowValidFrom: int64(-100)})
	if _, err := g.Nodes().Update(ctx, w.shift, map[string]any{"k": int64(5), types.ShadowValidFrom: int64(0)}); err != nil {
		t.Fatalf("update from the instant 0: %v", err)
	}
	w.closed = add(map[string]any{"k": int64(6), types.ShadowValidFrom: int64(-5)})
	if err := g.Nodes().CloseVersion(ctx, w.closed, 0); err != nil {
		t.Fatalf("close at the instant 0: %v", err)
	}
	hub, err := g.Nodes().Get(ctx, w.whole)
	if err != nil {
		t.Fatal(err)
	}
	end, err := g.Nodes().Get(ctx, w.fromZero)
	if err != nil {
		t.Fatal(err)
	}
	r, err := g.Rels().Add(ctx, "ZR", hub, end, map[string]any{types.ShadowValidFrom: types.ExplicitInstant(0), types.ShadowValidTo: int64(10)})
	if err != nil {
		t.Fatalf("relationship from the explicit 0: %v", err)
	}
	w.rel = r.ID()
	return w
}

// r11ZeroK returns node id's k at the valid instant at (ok false: none).
func r11ZeroK(t *testing.T, g *graphpkg.Graph, id types.NodeID, at types.Instant) (int64, bool) {
	t.Helper()
	n, err := g.Temporal().NodeAt(id, at)
	if errors.Is(err, storepkg.ErrNoVersionValidAt) {
		return 0, false
	}
	if err != nil {
		t.Fatalf("NodeAt(%d, %d): %v", id, at, err)
	}
	v, _ := n.GetProperty("k")
	return v.(int64), true
}

// r11ZeroCheck asserts r11ZeroWorld's answers on every door family.
func r11ZeroCheck(t *testing.T, g *graphpkg.Graph, w r11ZeroIDs, what string) {
	t.Helper()
	ctx := context.Background()
	axis, err := g.Admin().ValidAxis()
	if err != nil || !axis.Literal() {
		t.Fatalf("%s: ValidAxis = %+v, %v; want the literal axis", what, axis, err)
	}
	for _, c := range []struct {
		id   types.NodeID
		at   types.Instant
		want int64 // 0: no version
	}{
		{w.fromZero, -1, 0}, {w.fromZero, 0, 1}, {w.fromZero, 9, 1}, {w.fromZero, 10, 0},
		{w.toZero, -10, 2}, {w.toZero, -1, 2}, {w.toZero, 0, 0}, {w.toZero, 5, 0},
		{w.whole, types.UnboundedValidFrom, 3}, {w.whole, 0, 3}, {w.whole, types.InfiniteValidTo - 1, 3},
		{w.shift, -100, 4}, {w.shift, -1, 4}, {w.shift, 0, 5}, {w.shift, 1 << 40, 5},
		{w.closed, -5, 6}, {w.closed, -1, 6}, {w.closed, 0, 0},
	} {
		k, ok := r11ZeroK(t, g, c.id, c.at)
		if (c.want == 0) == ok || (ok && k != c.want) {
			t.Errorf("%s: node %d at %d: k=%d ok=%v, want %d", what, c.id, c.at, k, ok, c.want)
		}
	}
	// The stored values are literal: no flag, a 0 is the instant 0, the
	// whole axis is [-inf, +inf).
	for _, c := range []struct {
		id       types.NodeID
		from, to types.Instant
	}{
		{w.fromZero, 0, 10}, {w.toZero, -10, 0}, {w.whole, types.UnboundedValidFrom, types.InfiniteValidTo},
		{w.shift, 0, types.InfiniteValidTo}, {w.closed, -5, 0},
	} {
		n, err := g.Nodes().Get(ctx, c.id)
		if err != nil {
			t.Fatal(err)
		}
		if from, to, _ := n.ValidRange(); from != c.from || to != c.to {
			t.Errorf("%s: node %d stores [%d, %d), want [%d, %d)", what, c.id, from, to, c.from, c.to)
		}
		if got := axis.NodeStart(n); got != c.from {
			t.Errorf("%s: node %d starts at %d on the axis, want %d", what, c.id, got, c.from)
		}
	}
	// The push-down and the generic doors.
	for _, c := range []struct {
		opts storepkg.QueryOpts
		want map[types.NodeID]bool
	}{
		{storepkg.QueryOpts{ValidAt: -1, ValidAtSet: true}, map[types.NodeID]bool{w.fromZero: false, w.toZero: true, w.whole: true, w.shift: true, w.closed: true}},
		{storepkg.QueryOpts{ValidAt: 0, ValidAtSet: true}, map[types.NodeID]bool{w.fromZero: true, w.toZero: false, w.whole: true, w.shift: true, w.closed: false}},
		{storepkg.QueryOpts{ValidStart: 0, ValidEnd: 5, ValidWindowSet: true}, map[types.NodeID]bool{w.fromZero: true, w.toZero: false, w.whole: true, w.closed: false}},
	} {
		ns, err := g.Nodes().ByLabel("Z", c.opts)
		if err != nil {
			t.Fatal(err)
		}
		got := map[types.NodeID]bool{}
		for _, n := range ns {
			got[n.ID()] = true
		}
		for id, want := range c.want {
			if got[id] != want {
				t.Errorf("%s: ByLabel(%+v) lists %d = %v, want %v", what, c.opts, id, got[id], want)
			}
		}
	}
	// Relationship: from the explicit 0 (the same claim as a plain 0).
	if _, err := g.Temporal().RelAt(w.rel, -1); !errors.Is(err, storepkg.ErrNoVersionValidAt) {
		t.Errorf("%s: RelAt(-1): %v, want ErrNoVersionValidAt", what, err)
	}
	if _, err := g.Temporal().RelAt(w.rel, 0); err != nil {
		t.Errorf("%s: RelAt(0): %v", what, err)
	}
	now, err := g.Temporal().NowTx()
	if err != nil {
		t.Fatal(err)
	}
	// Timelines: an end at 0 is an instant, the open end InfiniteValidTo.
	for _, c := range []struct {
		id   types.NodeID
		want string
	}{
		{w.fromZero, "[0,10)"},
		{w.toZero, "[-10,0)"},
		{w.shift, fmt.Sprintf("[-100,0)[0,%d)", types.InfiniteValidTo)},
		{w.closed, "[-5,0)"},
	} {
		segs, err := g.Temporal().NodeEffectiveTimeline(c.id, now)
		if err != nil {
			t.Fatal(err)
		}
		got := ""
		for _, s := range segs {
			got += fmt.Sprintf("[%d,%d)", s.ValidFrom, s.ValidTo)
		}
		if got != c.want {
			t.Errorf("%s: timeline of %d: %s, want %s", what, c.id, got, c.want)
		}
	}
	// The count timeline: segments end at the instant 0, only the last is
	// open (InfiniteValidTo), and the counts at -1 and 0 match the point
	// doors.
	cnt, exact, err := g.Nodes().CountTimelineByLabel("Z", now, storepkg.CountTimelineOpts{})
	if err != nil || !exact {
		t.Fatalf("%s: count timeline: %v exact=%v", what, err, exact)
	}
	countAt := func(x types.Instant) int64 {
		for _, s := range cnt {
			if s.From <= x && (axis.Open(s.To) || x < s.To) {
				return s.Count
			}
		}
		return 0
	}
	for x, want := range map[types.Instant]int64{-1: 4, 0: 3, 10: 2} {
		if got := countAt(x); got != want {
			t.Errorf("%s: count at %d = %d, want %d (%+v)", what, x, got, want, cnt)
		}
	}
	for i, s := range cnt {
		if last := i == len(cnt)-1; axis.Open(s.To) != last {
			t.Errorf("%s: count segment %d %+v open=%v (only the last is open)", what, i, s, axis.Open(s.To))
		}
	}
	hull, ok, err := g.Nodes().ValidHullByLabel("Z", now, storepkg.CountTimelineOpts{})
	if err != nil || !ok || !hull.OpenEnd || hull.To != types.InfiniteValidTo || hull.From != types.UnboundedValidFrom {
		t.Errorf("%s: hull %+v ok=%v err=%v, want [-inf, +inf) open", what, hull, ok, err)
	}
	// Column scans report the stored values and filter on the axis.
	for _, c := range []struct {
		opts storepkg.QueryOpts
		want bool // the [0, 10) node is in the scan
	}{
		{storepkg.QueryOpts{}, true},
		{storepkg.QueryOpts{ValidAt: -1, ValidAtSet: true}, false},
		{storepkg.QueryOpts{ValidAt: 0, ValidAtSet: true}, true},
		{storepkg.QueryOpts{ValidAt: 0, ValidAtSet: true, TxAt: now, TxAtSet: true}, true},
	} {
		seen := false
		ok, err := g.ScanNodeColumns("Z", []string{"k"}, c.opts, func(b *graphpkg.ColumnBatch) bool {
			for i, id := range b.IDs {
				if id == w.toZero && c.opts.HasValidAt() && c.opts.ValidAt == 0 {
					t.Errorf("%s: column scan %+v lists the node ending at 0", what, c.opts)
				}
				if id == w.fromZero {
					seen = true
					if b.ValidFrom[i] != 0 || b.ValidTo[i] != 10 {
						t.Errorf("%s: column row of [0, 10): [%d, %d)", what, b.ValidFrom[i], b.ValidTo[i])
					}
				}
			}
			return true
		})
		if err != nil {
			t.Fatalf("%s: column scan %+v: %v", what, c.opts, err)
		}
		if ok && seen != c.want {
			t.Errorf("%s: column scan %+v lists [0, 10) = %v, want %v", what, c.opts, seen, c.want)
		}
	}
}

// TestRound11ZeroIsAnInstant: on every backend an UntimedWholeAxis store
// records the instant 0 as a start and as an end (create, update, close),
// and every door family reads it as the instant 0.
func TestRound11ZeroIsAnInstant(t *testing.T) {
	for _, b := range allStoreBackendsWith(r11ZeroCfg) {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			w := r11ZeroWorld(t, g)
			r11ZeroCheck(t, g, w, "fresh")
			if err := g.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := g.Admin().ValidAxis(); !errors.Is(err, graphpkg.ErrGraphClosed) {
				t.Errorf("ValidAxis on a closed graph: %v, want ErrGraphClosed", err)
			}
		})
	}
}

// TestRound11ZeroCorrectionsAndExcise: a correction and an excise may start
// or end at the instant 0 on a literal-axis store (positional bounds are
// literal: types.InfiniteValidTo is the open end, 0 the instant 0).
func TestRound11ZeroCorrectionsAndExcise(t *testing.T) {
	ctx := context.Background()
	for _, b := range allStoreBackendsWith(r11ZeroCfg) {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			n, err := g.Nodes().Add(ctx, []string{"Z"}, map[string]any{"k": int64(1)})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := g.Temporal().SetNodeVersionInterval(ctx, n.ID(), -10, 0, map[string]any{"k": int64(2)}); err != nil {
				t.Fatalf("correction [-10, 0): %v", err)
			}
			if _, err := g.Temporal().SetNodeVersionInterval(ctx, n.ID(), 0, 5, map[string]any{"k": int64(3)}); err != nil {
				t.Fatalf("correction [0, 5): %v", err)
			}
			if err := g.Temporal().ExciseNodeValidity(ctx, n.ID(), 5, 6); err != nil {
				t.Fatalf("excise [5, 6): %v", err)
			}
			for at, want := range map[types.Instant]int64{-11: 1, -10: 2, -1: 2, 0: 3, 4: 3, 5: 0, 6: 1} {
				k, ok := r11ZeroK(t, g, n.ID(), at)
				if (want == 0) == ok || (ok && k != want) {
					t.Errorf("at %d: k=%d ok=%v, want %d", at, k, ok, want)
				}
			}
			if _, err := g.Temporal().SetNodeVersionInterval(ctx, n.ID(), 7, 0, nil); !errors.Is(err, graphpkg.ErrInvalidTimeRange) {
				t.Errorf("correction [7, 0): %v, want ErrInvalidTimeRange (0 is an instant, not open)", err)
			}
			if _, err := g.Nodes().Add(ctx, []string{"Z"}, map[string]any{types.ShadowValidFrom: int64(types.InfiniteValidTo)}); !errors.Is(err, graphpkg.ErrInvalidTimeRange) {
				t.Errorf("a start at +inf: %v, want ErrInvalidTimeRange", err)
			}
			if _, err := g.Nodes().Add(ctx, []string{"Z"}, map[string]any{types.ShadowValidTo: int64(types.UnboundedValidFrom)}); !errors.Is(err, graphpkg.ErrInvalidTimeRange) {
				t.Errorf("an end at -inf: %v, want ErrInvalidTimeRange", err)
			}
		})
	}
}

// TestRound11ZeroLegacyRefusal: a store on the legacy axis (UntimedUnbounded,
// UntimedFromTxFrom) refuses a claim of the instant 0 on every write door with
// ErrZeroInstantLegacyAxis (wrapping ErrInvalidTimeRange), writes nothing, and
// keeps a plain 0 as "no claim".
func TestRound11ZeroLegacyRefusal(t *testing.T) {
	ctx := context.Background()
	zero := func(key string) map[string]any { return map[string]any{key: types.ExplicitInstant(0)} }
	for _, mode := range []graphpkg.UntimedValidStart{graphpkg.UntimedUnbounded, graphpkg.UntimedFromTxFrom} {
		for _, b := range allStoreBackendsWith(func(c *graphpkg.Config) { c.UntimedValidStart = mode; c.AllowTxBackfill = true }) {
			t.Run(fmt.Sprintf("%s/mode%d", b.name, mode), func(t *testing.T) {
				g := b.open(t)
				refused := func(op string, err error) {
					t.Helper()
					if !errors.Is(err, graphpkg.ErrZeroInstantLegacyAxis) || !errors.Is(err, graphpkg.ErrInvalidTimeRange) {
						t.Errorf("%s: %v, want ErrZeroInstantLegacyAxis", op, err)
					}
				}
				if axis, err := g.Admin().ValidAxis(); err != nil || axis.Literal() {
					t.Fatalf("ValidAxis = %+v, %v; want the legacy axis", axis, err)
				}
				for _, key := range []string{types.ShadowValidFrom, types.ShadowValidTo} {
					_, err := g.Nodes().Add(ctx, []string{"Z"}, zero(key))
					refused("create "+key, err)
				}
				n, err := g.Nodes().Add(ctx, []string{"Z"}, map[string]any{"k": int64(1), types.ShadowValidFrom: int64(-100)})
				if err != nil {
					t.Fatal(err)
				}
				hub, err := g.Nodes().Add(ctx, []string{"H"}, nil)
				if err != nil {
					t.Fatal(err)
				}
				r, err := g.Rels().Add(ctx, "E", hub, n, map[string]any{types.ShadowValidFrom: int64(-50)})
				if err != nil {
					t.Fatal(err)
				}
				_, err = g.Rels().Add(ctx, "E", hub, n, zero(types.ShadowValidFrom))
				refused("relationship create", err)
				for _, key := range []string{types.ShadowValidFrom, types.ShadowValidTo} {
					_, err = g.Nodes().Update(ctx, n.ID(), zero(key))
					refused("update "+key, err)
					_, err = g.Nodes().UpdateInPlace(ctx, n.ID(), zero(key))
					refused("update in place "+key, err)
					_, err = g.Rels().Update(ctx, r.ID(), zero(key))
					refused("relationship update "+key, err)
				}
				now, err := g.Temporal().NowTx()
				if err != nil {
					t.Fatal(err)
				}
				_, err = g.Nodes().UpdateWithTx(ctx, n.ID(), zero(types.ShadowValidFrom), now+1)
				refused("update with a caller instant", err)
				refused("GraphTx UpdateNode", g.Tx().Run(func(tx *graphpkg.GraphTx) error {
					_, err := tx.UpdateNode(n.ID(), zero(types.ShadowValidFrom))
					return err
				}))
				refused("GraphTx AddNode", g.Tx().Run(func(tx *graphpkg.GraphTx) error {
					_, err := tx.AddNode([]string{"Z"}, zero(types.ShadowValidFrom))
					return err
				}))
				bb, err := g.Batch().New()
				if err != nil {
					t.Fatal(err)
				}
				_, err = bb.AddNode([]string{"Z"}, zero(types.ShadowValidFrom))
				refused("Batch AddNode", err)
				refused("Batch UpdateNode", bb.UpdateNode(n.ID(), zero(types.ShadowValidTo)))
				viol, err := g.Constraints().DryRunValidate(ctx, constraints.DryRunFacts{Rels: []constraints.DryRunRel{{
					Ref: "r", TypeName: "E", StartID: hub.ID(), EndID: n.ID(), ValidFromSet: true,
				}}})
				if err != nil {
					t.Fatal(err)
				}
				if len(viol) != 1 || !errors.Is(viol[0].Err, graphpkg.ErrZeroInstantLegacyAxis) {
					t.Errorf("dry run with ValidFromSet at 0: %+v, want one ErrZeroInstantLegacyAxis violation", viol)
				}
				// Nothing changed; a plain 0 is still no claim.
				if k, ok := r11ZeroK(t, g, n.ID(), -100); !ok || k != 1 {
					t.Errorf("after the refusals: k=%d ok=%v at -100, want 1", k, ok)
				}
				p, err := g.Nodes().Add(ctx, []string{"Z"}, map[string]any{types.ShadowValidFrom: int64(0), types.ShadowValidTo: int64(0)})
				if err != nil {
					t.Fatal(err)
				}
				if from, to, _ := p.ValidRange(); to != 0 || (mode == graphpkg.UntimedUnbounded) != (from == types.UnboundedValidFrom) {
					t.Errorf("a plain 0 on mode %d stored [%d, %d)", mode, from, to)
				}
				if _, err := g.Temporal().NodeAt(p.ID(), 1<<50); err != nil {
					t.Errorf("a plain 0 end is open on the legacy axis: %v", err)
				}
			})
		}
	}
}

// TestRound11ZeroDryRun: on a literal-axis store the dry run reads a claim
// at the instant 0 (ValidFromSet / ValidToSet) as the create would.
func TestRound11ZeroDryRun(t *testing.T) {
	ctx := context.Background()
	g := allStoreBackendsWith(r11ZeroCfg)[0].open(t)
	if err := g.Constraints().Add(temporal.TemporalConstraint{Kind: temporal.ConstraintRelWithinEndpoints}); err != nil {
		t.Fatal(err)
	}
	start, err := g.Nodes().Add(ctx, []string{"N"}, map[string]any{types.ShadowValidFrom: int64(0)})
	if err != nil {
		t.Fatal(err)
	}
	end, err := g.Nodes().Add(ctx, []string{"N"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		rel  constraints.DryRunRel
		want bool // violates
	}{
		{constraints.DryRunRel{ValidFrom: 0, ValidFromSet: true}, false},
		{constraints.DryRunRel{ValidFrom: -1, ValidFromSet: true}, true},
		{constraints.DryRunRel{ValidFrom: -5, ValidFromSet: true, ValidTo: 0, ValidToSet: true}, true},
	} {
		r := c.rel
		r.Ref, r.TypeName, r.StartID, r.EndID = "r", "E", start.ID(), end.ID()
		viol, err := g.Constraints().DryRunValidate(ctx, constraints.DryRunFacts{Rels: []constraints.DryRunRel{r}})
		if err != nil {
			t.Fatal(err)
		}
		if (len(viol) > 0) != c.want {
			t.Errorf("dry run %+v: %+v, want violation=%v", c.rel, viol, c.want)
		}
	}
}

// TestRound11ZeroReopen: a durable UntimedWholeAxis store keeps its axis
// across a reopen under UntimedDefault (decided once at open from the
// persisted mode) and answers the instant 0 as before; a legacy store
// reopens on the legacy axis with its rows unchanged; neither opens under
// the other mode.
func TestRound11ZeroReopen(t *testing.T) {
	ctx := context.Background()
	for _, b := range uvsDurable {
		t.Run(b.name, func(t *testing.T) {
			dir := t.TempDir()
			g, _ := uvsMustOpen(t, b, dir, graphpkg.UntimedWholeAxis)
			w := r11ZeroWorld(t, g)
			if err := g.Close(); err != nil {
				t.Fatal(err)
			}
			g, _ = uvsMustOpen(t, b, dir, graphpkg.UntimedDefault)
			r11ZeroCheck(t, g, w, "reopened under UntimedDefault")
			if mode, complete, err := g.Admin().UntimedValidStart(); err != nil || !complete || mode != graphpkg.UntimedWholeAxis {
				t.Errorf("reopened mode %d complete=%v err=%v", mode, complete, err)
			}
			if err := g.Close(); err != nil {
				t.Fatal(err)
			}
			if _, _, err := uvsOpen(t, b, dir, graphpkg.UntimedUnbounded); !errors.Is(err, graphpkg.ErrUntimedModeMismatch) {
				t.Errorf("a whole-axis store opened under UntimedUnbounded: %v, want ErrUntimedModeMismatch", err)
			}

			legacyDir := t.TempDir()
			g, _ = uvsMustOpen(t, b, legacyDir, graphpkg.UntimedUnbounded)
			n, err := g.Nodes().Add(ctx, []string{"Ref"}, map[string]any{types.ShadowValidFrom: int64(0), "k": int64(1)})
			if err != nil {
				t.Fatal(err)
			}
			if err := g.Close(); err != nil {
				t.Fatal(err)
			}
			g, _ = uvsMustOpen(t, b, legacyDir, graphpkg.UntimedDefault)
			if axis, err := g.Admin().ValidAxis(); err != nil || axis.Literal() {
				t.Errorf("a reopened legacy store: axis %+v, %v", axis, err)
			}
			got, err := g.Nodes().Get(ctx, n.ID())
			if err != nil {
				t.Fatal(err)
			}
			if from, to, _ := got.ValidRange(); from != types.UnboundedValidFrom || to != 0 {
				t.Errorf("a legacy plain-0 create reopened as [%d, %d), want [-inf, open 0)", from, to)
			}
			if err := g.Close(); err != nil {
				t.Fatal(err)
			}
			if _, _, err := uvsOpen(t, b, legacyDir, graphpkg.UntimedWholeAxis); !errors.Is(err, graphpkg.ErrUntimedModeMismatch) {
				t.Errorf("a legacy store opened under UntimedWholeAxis: %v, want ErrUntimedModeMismatch", err)
			}
		})
	}
}

// TestRound11ZeroExport: an UntimedWholeAxis store's export carries the
// literal-axis version (a release before it refuses the stream) and the
// mode; an empty legacy-configured target adopts the mode, its axis and the
// rows verbatim; a legacy store with rows refuses it.
func TestRound11ZeroExport(t *testing.T) {
	ctx := context.Background()
	src := allStoreBackendsWith(r11ZeroCfg)[0].open(t)
	w := r11ZeroWorld(t, src)
	var buf bytes.Buffer
	if err := src.IO().Export(&buf); err != nil {
		t.Fatal(err)
	}
	hdr, err := tkgio.HeaderOf(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if hdr.Version != 4 || hdr.IsDelta {
		t.Errorf("header: version %d delta=%v, want 4 (the literal axis), full", hdr.Version, hdr.IsDelta)
	}
	for _, b := range allStoreBackends() {
		t.Run(b.name, func(t *testing.T) {
			dst := b.open(t)
			if err := dst.IO().Import(bytes.NewReader(buf.Bytes()), tkgio.ImportOptions{}); err != nil {
				t.Fatalf("import into an empty store: %v", err)
			}
			r11ZeroCheck(t, dst, w, "imported")
			busy := b.open(t)
			if _, err := busy.Nodes().Add(ctx, []string{"X"}, nil); err != nil {
				t.Fatal(err)
			}
			if err := busy.IO().Import(bytes.NewReader(buf.Bytes()), tkgio.ImportOptions{}); !errors.Is(err, graphpkg.ErrUntimedModeMismatch) {
				t.Errorf("import into a legacy store with rows: %v, want ErrUntimedModeMismatch", err)
			}
		})
	}
	// A legacy store's export keeps its version.
	legacy := allStoreBackends()[0].open(t)
	if _, err := legacy.Nodes().Add(ctx, []string{"X"}, nil); err != nil {
		t.Fatal(err)
	}
	var lb bytes.Buffer
	if err := legacy.IO().Export(&lb); err != nil {
		t.Fatal(err)
	}
	if lh, err := tkgio.HeaderOf(bytes.NewReader(lb.Bytes())); err != nil || lh.Version != 2 {
		t.Errorf("legacy export header: %+v, %v; want version 2", lh, err)
	}
}

// TestRound11ZeroStoreDoors: the store's own valid-time reads take the
// store's axis (store.ValidAxisCapability, set at open): the temporal index
// envelopes (CreateTemporal prunes by them), the property index push-down
// with a time filter, the relationship type temporal index and the
// effective "From" doors (valid-end index) read a start or end at the
// instant 0 as the instant 0 — a store left on the legacy axis would start
// such a row at its TxFrom and prune it at 0.
func TestRound11ZeroStoreDoors(t *testing.T) {
	ctx := context.Background()
	for _, b := range allStoreBackendsWith(r11ZeroCfg) {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			if err := g.Index().CreateTemporal("T"); err != nil {
				t.Fatal(err)
			}
			if err := g.Index().CreateProperty("T", "k"); err != nil {
				t.Fatal(err)
			}
			if err := g.Index().CreateRelTemporal("TR"); err != nil {
				t.Fatal(err)
			}
			from0, err := g.Nodes().Add(ctx, []string{"T"}, map[string]any{"k": int64(1), types.ShadowValidFrom: int64(0), types.ShadowValidTo: int64(10)})
			if err != nil {
				t.Fatal(err)
			}
			to0, err := g.Nodes().Add(ctx, []string{"T"}, map[string]any{"k": int64(1), types.ShadowValidFrom: int64(-10), types.ShadowValidTo: int64(0)})
			if err != nil {
				t.Fatal(err)
			}
			hub, err := g.Nodes().Add(ctx, []string{"H"}, nil) // the whole axis
			if err != nil {
				t.Fatal(err)
			}
			r, err := g.Rels().Add(ctx, "TR", hub, from0, map[string]any{"w": int64(1), types.ShadowValidFrom: int64(0), types.ShadowValidTo: int64(10)})
			if err != nil {
				t.Fatal(err)
			}
			now, err := g.Temporal().NowTx()
			if err != nil {
				t.Fatal(err)
			}
			for _, at := range []types.Instant{-1, 0, 9} {
				want := map[types.NodeID]bool{from0.ID(): at >= 0, to0.ID(): at < 0}
				for door, read := range map[string]func() ([]*types.Node, error){
					"ByLabel": func() ([]*types.Node, error) {
						return g.Nodes().ByLabel("T", storepkg.QueryOpts{ValidAt: at, ValidAtSet: true})
					},
					"ByLabelAndProperty": func() ([]*types.Node, error) {
						return g.Nodes().ByLabelAndProperty("T", "k", int64(1), storepkg.QueryOpts{ValidAt: at, ValidAtSet: true})
					},
					"NodesByLabelAt": func() ([]*types.Node, error) { return g.Temporal().NodesByLabelAt("T", at) },
				} {
					ns, err := read()
					if err != nil {
						t.Fatal(err)
					}
					got := map[types.NodeID]bool{}
					for _, n := range ns {
						got[n.ID()] = true
					}
					for id, w := range want {
						if got[id] != w {
							t.Errorf("%s at %d lists %d = %v, want %v", door, at, id, got[id], w)
						}
					}
				}
				rels, err := g.Rels().ByType("TR", storepkg.QueryOpts{ValidAt: at, ValidAtSet: true})
				if err != nil {
					t.Fatal(err)
				}
				if got := len(rels) == 1 && rels[0].ID() == r.ID(); got != (at >= 0) {
					t.Errorf("Rels ByType at %d: %d rows, want the relationship = %v", at, len(rels), at >= 0)
				}
				adj := 0
				if err := g.Rels().ForEachAdjacentEndpointAt(hub.ID(), "TR", false, storepkg.QueryOpts{ValidAt: at, ValidAtSet: true}, func(types.RelID, types.NodeID) bool {
					adj++
					return true
				}); err != nil {
					t.Fatal(err)
				}
				if got := adj == 1; got != (at >= 0) {
					t.Errorf("ForEachAdjacentEndpointAt at %d: %d rows, want the relationship = %v", at, adj, at >= 0)
				}
				out, err := g.Temporal().OutgoingRelsAt(hub.ID(), at)
				if err != nil {
					t.Fatal(err)
				}
				if got := len(out) == 1; got != (at >= 0) {
					t.Errorf("OutgoingRelsAt %d: %d rows, want the relationship = %v", at, len(out), at >= 0)
				}
				byProp, err := g.Temporal().NodesByLabelPropertyAt("T", "k", int64(1), at)
				if err != nil {
					t.Fatal(err)
				}
				if len(byProp) != 1 || byProp[0].ID() != map[bool]types.NodeID{true: from0.ID(), false: to0.ID()}[at >= 0] {
					t.Errorf("NodesByLabelPropertyAt %d: %d rows", at, len(byProp))
				}
			}
			// The From door lists an entity with a segment ending at or after
			// validFrom: [-10, 0) ends at the instant 0, before 1.
			seen := map[types.NodeID]bool{}
			if err := g.Temporal().ForEachNodeEffectiveByLabelsFrom([]string{"T"}, now, 1, func(s temporal.NodeSegment) bool {
				seen[s.Node.ID()] = true
				return true
			}); err != nil {
				t.Fatal(err)
			}
			if seen[to0.ID()] || !seen[from0.ID()] {
				t.Errorf("From(1) lists %v, want [0, 10) and not [-10, 0)", seen)
			}
		})
	}
}

// TestRound11ZeroBadgerOpenRebuild: a badger store reads its valid axis from
// its own persisted mode at open, before the open-time index rebuild reads a
// row (no trusted open image, temporal index in RAM): the envelope it
// rebuilds starts [0, 10) at the instant 0, so the pruned door finds it at 0.
func TestRound11ZeroBadgerOpenRebuild(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	open := func() *graphpkg.Graph {
		t.Helper()
		bs, err := badger.New(badger.Config{Dir: dir})
		if err != nil {
			t.Fatal(err)
		}
		nodes, err := bs.NodeCount()
		if err != nil {
			t.Fatal(err)
		}
		if a := bs.ValidAxis(); a.Literal() != (nodes > 0) {
			t.Fatalf("badger opened on axis literal=%v with %d nodes", a.Literal(), nodes)
		}
		g, err := graphpkg.New(graphpkg.Config{Store: bs, UntimedValidStart: graphpkg.UntimedWholeAxis})
		if err != nil {
			t.Fatal(err)
		}
		return g
	}
	g := open()
	if err := g.Index().CreateTemporal("T"); err != nil {
		t.Fatal(err)
	}
	n, err := g.Nodes().Add(ctx, []string{"T"}, map[string]any{types.ShadowValidFrom: int64(0), types.ShadowValidTo: int64(10)})
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	rawWrite(t, dir, func(txn *badgerv4.Txn) error { return badger.DeleteOpenStateForTest(txn) })
	g = open()
	defer g.Close()
	for at, want := range map[types.Instant]bool{-1: false, 0: true} {
		ns, err := g.Nodes().ByLabel("T", storepkg.QueryOpts{ValidAt: at, ValidAtSet: true})
		if err != nil {
			t.Fatal(err)
		}
		if got := len(ns) == 1 && ns[0].ID() == n.ID(); got != want {
			t.Errorf("ByLabel at %d after a rebuild: %d rows, want the node = %v", at, len(ns), want)
		}
	}
}
