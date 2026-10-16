package graph_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Valid-time excise (backlog 64, sigma R7). Instants are in thousands
// (k(20) = 20_000) so every read lands on, just before or inside a boundary.
func k(v int64) types.Instant { return types.Instant(v * 1000) }

// exciseProbes are the valid instants every check reads at.
var exciseProbes = []types.Instant{k(15), k(20) - 1, k(20), k(25), k(30) - 1, k(30), k(35), k(45), k(50) - 1, k(50), k(55), k(65)}

// nodeTimeline answers NodeAtTx(id, t, pin) at every probe: "v=<s>" or "_"
// (ErrNoVersionValidAt). Any other error fails the test.
func nodeTimeline(t *testing.T, g *graphpkg.Graph, id types.NodeID, pin types.Instant) string {
	t.Helper()
	out := make([]string, 0, len(exciseProbes))
	for _, at := range exciseProbes {
		n, err := g.Temporal().NodeAtTx(id, at, pin)
		if errors.Is(err, graphpkg.ErrNoVersionValidAt) {
			out = append(out, "_")
			continue
		}
		if err != nil {
			t.Fatalf("NodeAtTx(%d, %d, %d): %v", id, at, pin, err)
		}
		if n.Temporal().Void {
			t.Fatalf("NodeAtTx(%d, %d, %d) returned a void row", id, at, pin)
		}
		s, _ := n.GetProperty("s")
		out = append(out, fmt.Sprint(s))
	}
	return strings.Join(out, " ")
}

// relTimeline mirrors nodeTimeline for relationships.
func relTimeline(t *testing.T, g *graphpkg.Graph, id types.RelID, pin types.Instant) string {
	t.Helper()
	out := make([]string, 0, len(exciseProbes))
	for _, at := range exciseProbes {
		r, err := g.Temporal().RelAtTx(id, at, pin)
		if errors.Is(err, graphpkg.ErrNoVersionValidAt) {
			out = append(out, "_")
			continue
		}
		if err != nil {
			t.Fatalf("RelAtTx(%d, %d, %d): %v", id, at, pin, err)
		}
		if r.Temporal().Void {
			t.Fatalf("RelAtTx(%d, %d, %d) returned a void row", id, at, pin)
		}
		s, _ := r.GetProperty("s")
		out = append(out, fmt.Sprint(s))
	}
	return strings.Join(out, " ")
}

// timeline builds the expected string from (from, to, s) runs over
// exciseProbes ("_" elsewhere); to == 0 is open.
type run struct {
	from, to types.Instant
	s        string
}

func want(runs ...run) string {
	out := make([]string, 0, len(exciseProbes))
	for _, at := range exciseProbes {
		s := "_"
		for _, r := range runs {
			if at >= r.from && (r.to == 0 || at < r.to) {
				s = r.s
			}
		}
		out = append(out, s)
	}
	return strings.Join(out, " ")
}

// voidRows returns the [from,to) of the void rows in a node's chain, sorted,
// and fails when the current row is void.
func nodeVoidRows(t *testing.T, g *graphpkg.Graph, id types.NodeID) ([][2]types.Instant, types.Instant) {
	t.Helper()
	cur, err := g.Nodes().Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get(%d): %v", id, err)
	}
	if cur.Temporal().Void {
		t.Fatalf("current row of %d is void", id)
	}
	h, err := g.Nodes().History(id)
	if err != nil {
		t.Fatalf("History(%d): %v", id, err)
	}
	var out [][2]types.Instant
	var tx types.Instant
	for _, r := range h {
		if tm := r.Temporal(); tm.Void {
			out = append(out, [2]types.Instant{tm.ValidFrom, tm.ValidTo})
			if tx != 0 && tm.TxFrom != tx {
				t.Fatalf("void rows of %d carry two tx instants %d and %d", id, tx, tm.TxFrom)
			}
			tx = tm.TxFrom
		}
	}
	slices.SortFunc(out, func(a, b [2]types.Instant) int { return int(a[0] - b[0]) })
	return out, tx
}

func historyLen(t *testing.T, g *graphpkg.Graph, id types.NodeID) int {
	t.Helper()
	h, err := g.Nodes().History(id)
	if err != nil {
		t.Fatalf("History(%d): %v", id, err)
	}
	return len(h)
}

// TestExciseNodeValidity: Temporal().ExciseNodeValidity on memory, badger,
// tiered and sharded (on disk, reopened). For each shape — inside the current
// open version, inside a closed current version, inside a historical version,
// across two versions, and a later correction over the hole — the reads at
// every probe answer the excised timeline now and the unsplit one at a pin
// before the excise; the void pieces are history rows of one tx instant, never
// the current row; NodeAsOf answers a real row; the effective timeline has the
// hole; the label and interval doors drop the node inside [a,b). Break-the-code
// cases: inverted, empty, b = 0 and a = 0 ranges, a range where nothing is
// valid (also the hole itself), an unknown and a deleted node — each refused
// with nothing written.
//
// Catches: an excise composed of existing rows (the old version still answers
// inside [a,b) — r7-probe.txt), a void row that answers a read or takes the
// current slot, a resumption that is missing (the tail after b disappears) or
// unbounded (a later version is shadowed), pieces stamped at different tx
// instants (a pin between them sees half an excise), a void flag lost on the
// wire (the reopened store reads the version again inside [a,b)), and a
// refusal that still appends rows.
func TestExciseNodeValidity(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	type shape struct {
		name string
		// build creates the node and runs the excise (and later writes);
		// it returns the pin just before the excise and the expected
		// timelines now and before.
		build func(t *testing.T, g *graphpkg.Graph) (id types.NodeID, before types.Instant, wantNow, wantBefore string, voids [][2]types.Instant)
	}
	add := func(t *testing.T, g *graphpkg.Graph, props map[string]any) *types.Node {
		t.Helper()
		n, err := g.Nodes().Add(ctx, []string{"D"}, props)
		if err != nil {
			t.Fatalf("add: %v", err)
		}
		return n
	}
	excise := func(t *testing.T, g *graphpkg.Graph, id types.NodeID, a, b types.Instant) types.Instant {
		t.Helper()
		if err := g.Temporal().ExciseNodeValidity(ctx, id, a, b); err != nil {
			t.Fatalf("ExciseNodeValidity(%d, %d, %d): %v", id, a, b, err)
		}
		_, vtx := nodeVoidRows(t, g, id)
		return vtx - 1 // the pin just before the excise's one instant
	}
	shapes := []shape{
		{"current-open", func(t *testing.T, g *graphpkg.Graph) (types.NodeID, types.Instant, string, string, [][2]types.Instant) {
			n := add(t, g, map[string]any{"tkg_valid_from": k(10), "s": "v"})
			before := excise(t, g, n.ID(), k(20), k(30))
			return n.ID(), before, want(run{k(10), k(20), "v"}, run{k(30), 0, "v"}), want(run{k(10), 0, "v"}), [][2]types.Instant{{k(20), k(30)}}
		}},
		{"current-closed", func(t *testing.T, g *graphpkg.Graph) (types.NodeID, types.Instant, string, string, [][2]types.Instant) {
			n := add(t, g, map[string]any{"tkg_valid_from": k(10), "tkg_valid_to": k(50), "s": "v"})
			before := excise(t, g, n.ID(), k(20), k(30))
			return n.ID(), before, want(run{k(10), k(20), "v"}, run{k(30), k(50), "v"}), want(run{k(10), k(50), "v"}), [][2]types.Instant{{k(20), k(30)}}
		}},
		{"closed-tail", func(t *testing.T, g *graphpkg.Graph) (types.NodeID, types.Instant, string, string, [][2]types.Instant) {
			// b beyond the version's end: no resumption, the void piece covers
			// only the valid part.
			n := add(t, g, map[string]any{"tkg_valid_from": k(10), "tkg_valid_to": k(50), "s": "v"})
			before := excise(t, g, n.ID(), k(45), k(60))
			return n.ID(), before, want(run{k(10), k(45), "v"}), want(run{k(10), k(50), "v"}), [][2]types.Instant{{k(45), k(50)}}
		}},
		{"historical", func(t *testing.T, g *graphpkg.Graph) (types.NodeID, types.Instant, string, string, [][2]types.Instant) {
			n := add(t, g, map[string]any{"tkg_valid_from": k(10), "s": "v1"})
			if _, err := g.Nodes().Update(ctx, n.ID(), map[string]any{"tkg_valid_from": k(40), "s": "v2"}); err != nil {
				t.Fatal(err)
			}
			before := excise(t, g, n.ID(), k(20), k(30))
			return n.ID(), before, want(run{k(10), k(20), "v1"}, run{k(30), k(40), "v1"}, run{k(40), 0, "v2"}), want(run{k(10), k(40), "v1"}, run{k(40), 0, "v2"}), [][2]types.Instant{{k(20), k(30)}}
		}},
		{"across-two-versions", func(t *testing.T, g *graphpkg.Graph) (types.NodeID, types.Instant, string, string, [][2]types.Instant) {
			n := add(t, g, map[string]any{"tkg_valid_from": k(10), "s": "v1"})
			if _, err := g.Nodes().Update(ctx, n.ID(), map[string]any{"tkg_valid_from": k(40), "s": "v2"}); err != nil {
				t.Fatal(err)
			}
			before := excise(t, g, n.ID(), k(30), k(50))
			return n.ID(), before, want(run{k(10), k(30), "v1"}, run{k(50), 0, "v2"}), want(run{k(10), k(40), "v1"}, run{k(40), 0, "v2"}), [][2]types.Instant{{k(30), k(40)}, {k(40), k(50)}}
		}},
		{"refill", func(t *testing.T, g *graphpkg.Graph) (types.NodeID, types.Instant, string, string, [][2]types.Instant) {
			n := add(t, g, map[string]any{"tkg_valid_from": k(10), "s": "v"})
			before := excise(t, g, n.ID(), k(20), k(30))
			if _, err := g.Temporal().SetNodeVersionInterval(ctx, n.ID(), k(20), k(30), map[string]any{"s": "w"}); err != nil {
				t.Fatalf("refill: %v", err)
			}
			return n.ID(), before, want(run{k(10), k(20), "v"}, run{k(20), k(30), "w"}, run{k(30), 0, "v"}), want(run{k(10), 0, "v"}), [][2]types.Instant{{k(20), k(30)}}
		}},
		{"partial-refill", func(t *testing.T, g *graphpkg.Graph) (types.NodeID, types.Instant, string, string, [][2]types.Instant) {
			n := add(t, g, map[string]any{"tkg_valid_from": k(10), "s": "v"})
			before := excise(t, g, n.ID(), k(20), k(30))
			if _, err := g.Temporal().SetNodeVersionInterval(ctx, n.ID(), k(20)-1, k(25)+1, map[string]any{"s": "w"}); err != nil {
				t.Fatalf("refill: %v", err)
			}
			return n.ID(), before, want(run{k(10), k(20) - 1, "v"}, run{k(20) - 1, k(25) + 1, "w"}, run{k(30), 0, "v"}), want(run{k(10), 0, "v"}), [][2]types.Instant{{k(20), k(30)}}
		}},
	}
	for _, b := range hasHistoryBackends() {
		t.Run(b.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			open := func() *graphpkg.Graph {
				st, _ := b.open(t, dir)
				g, err := graphpkg.New(graphpkg.Config{Store: st, AllowTxBackfill: true})
				if err != nil {
					t.Fatal(err)
				}
				return g
			}
			g := open()
			defer func() { _ = g.Close() }()

			type built struct {
				shape               string
				id                  types.NodeID
				before              types.Instant
				wantNow, wantBefore string
				voids               [][2]types.Instant
			}
			var all []built
			for _, s := range shapes {
				id, before, wantNow, wantBefore, voids := s.build(t, g)
				all = append(all, built{s.name, id, before, wantNow, wantBefore, voids})
			}

			// Refusals: nothing written.
			closed := add(t, g, map[string]any{"tkg_valid_from": k(10), "tkg_valid_to": k(50), "s": "v"})
			deleted := add(t, g, map[string]any{"tkg_valid_from": k(10), "s": "v"})
			if err := g.Nodes().Delete(ctx, deleted.ID()); err != nil {
				t.Fatal(err)
			}
			hole := all[0].id
			for _, r := range []struct {
				name string
				id   types.NodeID
				a, b types.Instant
				want error
			}{
				{"inverted", closed.ID(), k(30), k(20), graphpkg.ErrInvalidTimeRange},
				{"empty", closed.ID(), k(20), k(20), graphpkg.ErrInvalidTimeRange},
				{"open tail b=0", closed.ID(), k(20), 0, graphpkg.ErrInvalidTimeRange},
				{"a=0", closed.ID(), 0, k(20), graphpkg.ErrInvalidTimeRange},
				// A negative a is a real start since sigma round 10 (TestRound10ExciseNegativeStart).
				{"nothing valid", closed.ID(), k(60), k(70), graphpkg.ErrNoVersionValidAt},
				{"before the first version", closed.ID(), k(1), k(10), graphpkg.ErrNoVersionValidAt},
				{"the hole again", hole, k(21), k(29), graphpkg.ErrNoVersionValidAt},
				{"unknown", closed.ID() + types.NodeID(1)<<50, k(20), k(30), graphpkg.ErrNodeNotFound},
				{"deleted", deleted.ID(), k(20), k(30), graphpkg.ErrEntityDeleted},
			} {
				var hl int
				if r.name != "unknown" {
					hl = historyLen(t, g, r.id)
				}
				if err := g.Temporal().ExciseNodeValidity(ctx, r.id, r.a, r.b); !errors.Is(err, r.want) {
					t.Fatalf("refusal %s: ExciseNodeValidity(%d, %d) = %v; want %v", r.name, r.a, r.b, err, r.want)
				}
				if r.name != "unknown" && historyLen(t, g, r.id) != hl {
					t.Fatalf("refusal %s appended rows", r.name)
				}
			}

			check := func(step string, g *graphpkg.Graph) {
				t.Helper()
				pinNow, err := g.Temporal().PeekTx()
				if err != nil {
					t.Fatal(err)
				}
				for _, c := range all {
					if got := nodeTimeline(t, g, c.id, 0); got != c.wantNow {
						t.Errorf("%s %s: now\n got  %s\n want %s", step, c.shape, got, c.wantNow)
					}
					if got := nodeTimeline(t, g, c.id, pinNow); got != c.wantNow {
						t.Errorf("%s %s: pin now\n got  %s\n want %s", step, c.shape, got, c.wantNow)
					}
					if got := nodeTimeline(t, g, c.id, c.before); got != c.wantBefore {
						t.Errorf("%s %s: pin before the excise\n got  %s\n want %s", step, c.shape, got, c.wantBefore)
					}
					voids, vtx := nodeVoidRows(t, g, c.id)
					if !slices.Equal(voids, c.voids) {
						t.Errorf("%s %s: void rows %v; want %v", step, c.shape, voids, c.voids)
					}
					if vtx != c.before+1 {
						t.Errorf("%s %s: void rows at tx %d; want %d", step, c.shape, vtx, c.before+1)
					}
					// The as-of door answers a state row, never a void piece.
					if n, err := g.Temporal().NodeAsOf(c.id, pinNow); err != nil || n.Temporal().Void {
						t.Errorf("%s %s: NodeAsOf = %v, %v; want a non-void row", step, c.shape, n, err)
					}
					// The effective timeline matches the point door.
					segs, err := g.Temporal().NodeEffectiveTimeline(c.id, pinNow)
					if err != nil {
						t.Fatalf("%s %s: timeline: %v", step, c.shape, err)
					}
					var runs []run
					for _, s := range segs {
						if s.Node.Temporal().Void {
							t.Errorf("%s %s: timeline segment holds a void row", step, c.shape)
						}
						v, _ := s.Node.GetProperty("s")
						runs = append(runs, run{s.ValidFrom, s.ValidTo, fmt.Sprint(v)})
					}
					if got := want(runs...); got != c.wantNow {
						t.Errorf("%s %s: effective timeline %v\n got  %s\n want %s", step, c.shape, segs, got, c.wantNow)
					}
				}
				// Label and interval doors on the first shape (hole [20,30)).
				id := all[0].id
				has := func(ns []*types.Node) bool {
					for _, n := range ns {
						if n.ID() == id {
							return true
						}
					}
					return false
				}
				for _, p := range []struct {
					name string
					run  func() ([]*types.Node, error)
					want bool
				}{
					{"NodesByLabelAt(25)", func() ([]*types.Node, error) { return g.Temporal().NodesByLabelAt("D", k(25)) }, false},
					{"NodesByLabelAt(15)", func() ([]*types.Node, error) { return g.Temporal().NodesByLabelAt("D", k(15)) }, true},
					{"NodesAt(25)", func() ([]*types.Node, error) { return g.Temporal().NodesAt(k(25)) }, false},
					{"NodesDuring(22,28)", func() ([]*types.Node, error) { return g.Temporal().NodesDuring(k(22), k(28)) }, false},
					{"NodesDuring(15,25)", func() ([]*types.Node, error) { return g.Temporal().NodesDuring(k(15), k(25)) }, true},
					{"NodesDuring(25,35)", func() ([]*types.Node, error) { return g.Temporal().NodesDuring(k(25), k(35)) }, true},
				} {
					ns, err := p.run()
					if err != nil {
						t.Fatalf("%s %s: %v", step, p.name, err)
					}
					if has(ns) != p.want {
						t.Errorf("%s %s: node present %v; want %v", step, p.name, !p.want, p.want)
					}
				}
			}
			check("live", g)
			if !b.reopen {
				return
			}
			if err := g.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}
			g = open()
			check("reopened", g)
		})
	}
}

// TestExciseNodeValidityWithTx: the caller-instant door stamps every appended
// row at txFrom (a pin just below it sees the unsplit version) and keeps the
// caller-instant rules: txFrom 0 or in the future (ErrInvalidTxFrom), at or
// below a stamp of the chain (ErrTxOrder), the gate off
// (ErrTxBackfillDisabled) — each refusal writes nothing.
func TestExciseNodeValidityWithTx(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, b := range allStoreBackendsWith(func(c *graphpkg.Config) { c.AllowTxBackfill = true }) {
		t.Run(b.name, func(t *testing.T) {
			t.Parallel()
			g := b.open(t)
			base := types.Instant(time.Now().Add(-2 * time.Hour).UnixMilli())
			n, err := g.Nodes().AddWithTx(ctx, []string{"D"}, map[string]any{"tkg_valid_from": k(10), "s": "v"}, base)
			if err != nil {
				t.Fatal(err)
			}
			id := n.ID()
			for _, r := range []struct {
				name string
				tx   types.Instant
				want error
			}{
				{"zero", 0, graphpkg.ErrInvalidTxFrom},
				{"future", types.Instant(time.Now().Add(time.Hour).UnixMilli()), graphpkg.ErrInvalidTxFrom},
				{"at the creation", base, graphpkg.ErrTxOrder},
				{"before the creation", base - 1, graphpkg.ErrTxOrder},
			} {
				if err := g.Temporal().ExciseNodeValidityWithTx(ctx, id, k(20), k(30), r.tx); !errors.Is(err, r.want) {
					t.Fatalf("%s: %v; want %v", r.name, err, r.want)
				}
				if historyLen(t, g, id) != 0 {
					t.Fatalf("%s: refusal wrote rows", r.name)
				}
			}
			at := base + 3_600_000
			if err := g.Temporal().ExciseNodeValidityWithTx(ctx, id, k(20), k(30), at); err != nil {
				t.Fatal(err)
			}
			voids, vtx := nodeVoidRows(t, g, id)
			if vtx != at || len(voids) != 1 {
				t.Fatalf("void rows %v at tx %d; want one at %d", voids, vtx, at)
			}
			cur, _ := g.Nodes().Get(ctx, id)
			if cur.Temporal().TxFrom != at {
				t.Fatalf("resumption (current) tx %d; want %d", cur.Temporal().TxFrom, at)
			}
			if got, w := nodeTimeline(t, g, id, at), want(run{k(10), k(20), "v"}, run{k(30), 0, "v"}); got != w {
				t.Fatalf("pin at\n got  %s\n want %s", got, w)
			}
			if got, w := nodeTimeline(t, g, id, at-1), want(run{k(10), 0, "v"}); got != w {
				t.Fatalf("pin at-1\n got  %s\n want %s", got, w)
			}
			// The same instant again: below the chain's newest stamp.
			if err := g.Temporal().ExciseNodeValidityWithTx(ctx, id, k(40), k(45), at); !errors.Is(err, graphpkg.ErrTxOrder) {
				t.Fatalf("same instant again: %v; want ErrTxOrder", err)
			}
		})
	}
	t.Run("gate off", func(t *testing.T) {
		g := allStoreBackends()[0].open(t)
		n, err := g.Nodes().Add(ctx, []string{"D"}, map[string]any{"tkg_valid_from": k(10), "s": "v"})
		if err != nil {
			t.Fatal(err)
		}
		tx := nowTx(t, g)
		if err := g.Temporal().ExciseNodeValidityWithTx(ctx, n.ID(), k(20), k(30), tx); !errors.Is(err, graphpkg.ErrTxBackfillDisabled) {
			t.Fatalf("gate off: %v; want ErrTxBackfillDisabled", err)
		}
		if historyLen(t, g, n.ID()) != 0 {
			t.Fatal("gate refusal wrote rows")
		}
	})
}

// TestExciseNodeValidityTxAndBatch: the GraphTx and BatchBuilder twins.
// GraphTx: an excise commits with the transaction, a rolled-back one leaves
// nothing, a refused one leaves the transaction usable. Batch: an excise
// executes with the unit, a queue-time interval refusal queues nothing, and a
// caller-instant excise refused by the order rule refuses the whole unit (the
// other op of the unit is not applied).
func TestExciseNodeValidityTxAndBatch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	excised := want(run{k(10), k(20), "v"}, run{k(30), 0, "v"})
	unsplit := want(run{k(10), 0, "v"})
	for _, b := range allStoreBackendsWith(func(c *graphpkg.Config) { c.AllowTxBackfill = true }) {
		t.Run(b.name, func(t *testing.T) {
			t.Parallel()
			g := b.open(t)
			add := func() types.NodeID {
				n, err := g.Nodes().Add(ctx, []string{"D"}, map[string]any{"tkg_valid_from": k(10), "s": "v"})
				if err != nil {
					t.Fatal(err)
				}
				return n.ID()
			}
			// GraphTx commit.
			a := add()
			if err := g.Tx().Run(func(tx *graphpkg.GraphTx) error {
				if err := tx.ExciseNodeValidity(a, k(30), k(20)); !errors.Is(err, graphpkg.ErrInvalidTimeRange) {
					return fmt.Errorf("inverted in tx: %v", err)
				}
				return tx.ExciseNodeValidity(a, k(20), k(30))
			}); err != nil {
				t.Fatal(err)
			}
			if got := nodeTimeline(t, g, a, 0); got != excised {
				t.Fatalf("tx commit\n got  %s\n want %s", got, excised)
			}
			// GraphTx rollback.
			r := add()
			boom := errors.New("boom")
			if err := g.Tx().Run(func(tx *graphpkg.GraphTx) error {
				if err := tx.ExciseNodeValidity(r, k(20), k(30)); err != nil {
					return err
				}
				return boom
			}); !errors.Is(err, boom) {
				t.Fatalf("rollback run: %v", err)
			}
			if got := nodeTimeline(t, g, r, 0); got != unsplit || historyLen(t, g, r) != 0 {
				t.Fatalf("tx rollback left rows: %s, history %d", got, historyLen(t, g, r))
			}
			// GraphTx caller instant.
			w := add()
			wAt := nowTx(t, g)
			if err := g.Tx().Run(func(tx *graphpkg.GraphTx) error {
				return tx.ExciseNodeValidityWithTx(w, k(20), k(30), wAt)
			}); err != nil {
				t.Fatal(err)
			}
			if _, vtx := nodeVoidRows(t, g, w); vtx != wAt {
				t.Fatalf("tx WithTx void at %d; want %d", vtx, wAt)
			}

			// Batch.
			x := add()
			bb, err := g.Batch().New()
			if err != nil {
				t.Fatal(err)
			}
			if err := bb.ExciseNodeValidity(x, k(20), 0); !errors.Is(err, graphpkg.ErrInvalidTimeRange) {
				t.Fatalf("batch queue b=0: %v", err)
			}
			if err := bb.ExciseNodeValidity(x, k(20), k(30)); err != nil {
				t.Fatal(err)
			}
			if _, err := bb.Execute(); err != nil {
				t.Fatal(err)
			}
			if got := nodeTimeline(t, g, x, 0); got != excised {
				t.Fatalf("batch\n got  %s\n want %s", got, excised)
			}
			// Batch caller instant refused by the order rule: whole unit.
			y, other := add(), add()
			yNode, _ := g.Nodes().Get(ctx, y)
			bb, err = g.Batch().New()
			if err != nil {
				t.Fatal(err)
			}
			if err := bb.UpdateNode(other, map[string]any{"s": "changed"}); err != nil {
				t.Fatal(err)
			}
			if err := bb.ExciseNodeValidityWithTx(y, k(20), k(30), yNode.Temporal().TxFrom); err != nil {
				t.Fatal(err)
			}
			res, err := bb.Execute()
			if !errors.Is(err, graphpkg.ErrTxOrder) {
				t.Fatalf("batch whole-unit refusal: %v (%+v); want ErrTxOrder", err, res)
			}
			if o, _ := g.Nodes().Get(ctx, other); o != nil {
				if s, _ := o.GetProperty("s"); s != "v" {
					t.Fatalf("refused unit applied its other op: s=%v", s)
				}
			}
			if historyLen(t, g, y) != 0 || historyLen(t, g, other) != 0 {
				t.Fatal("refused unit wrote rows")
			}
			// Batch caller instant accepted.
			z := add()
			zAt := nowTx(t, g)
			bb, err = g.Batch().New()
			if err != nil {
				t.Fatal(err)
			}
			if err := bb.ExciseNodeValidityWithTx(z, k(20), k(30), zAt); err != nil {
				t.Fatal(err)
			}
			if _, err := bb.Execute(); err != nil {
				t.Fatal(err)
			}
			if _, vtx := nodeVoidRows(t, g, z); vtx != zAt {
				t.Fatalf("batch WithTx void at %d; want %d", vtx, zAt)
			}
		})
	}
}

// TestExciseRelValidity is the relationship mirror (Node/Rel parity): the
// current open and closed shapes, a historical version, the refusals, the
// caller-instant door, GraphTx and Batch twins, and reopen on disk.
func TestExciseRelValidity(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, b := range hasHistoryBackends() {
		t.Run(b.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			open := func() *graphpkg.Graph {
				st, _ := b.open(t, dir)
				g, err := graphpkg.New(graphpkg.Config{Store: st, AllowTxBackfill: true, Validation: graphpkg.ValidationLimits{AllowSelfLoops: true}})
				if err != nil {
					t.Fatal(err)
				}
				return g
			}
			g := open()
			defer func() { _ = g.Close() }()
			a, err := g.Nodes().Add(ctx, []string{"D"}, map[string]any{"tkg_valid_from": k(1)})
			if err != nil {
				t.Fatal(err)
			}
			c, err := g.Nodes().Add(ctx, []string{"D"}, map[string]any{"tkg_valid_from": k(1)})
			if err != nil {
				t.Fatal(err)
			}
			rel := func(props map[string]any) types.RelID {
				r, err := g.Rels().Add(ctx, "R", a, c, props)
				if err != nil {
					t.Fatal(err)
				}
				return r.ID()
			}
			type exp struct {
				name    string
				id      types.RelID
				wantNow string
			}
			var all []exp

			open1 := rel(map[string]any{"tkg_valid_from": k(10), "s": "v"})
			pre := relTimeline(t, g, open1, 0)
			if err := g.Temporal().ExciseRelValidity(ctx, open1, k(20), k(30)); err != nil {
				t.Fatal(err)
			}
			cur1, err := g.Rels().Get(ctx, open1)
			if err != nil {
				t.Fatal(err)
			}
			before := cur1.Temporal().TxFrom - 1 // the resumption carries the excise's instant
			if got := relTimeline(t, g, open1, before); got != pre {
				t.Fatalf("pin before\n got  %s\n want %s", got, pre)
			}
			all = append(all, exp{"current-open", open1, want(run{k(10), k(20), "v"}, run{k(30), 0, "v"})})

			closed := rel(map[string]any{"tkg_valid_from": k(10), "tkg_valid_to": k(50), "s": "v"})
			if err := g.Temporal().ExciseRelValidity(ctx, closed, k(20), k(30)); err != nil {
				t.Fatal(err)
			}
			all = append(all, exp{"current-closed", closed, want(run{k(10), k(20), "v"}, run{k(30), k(50), "v"})})

			hist := rel(map[string]any{"tkg_valid_from": k(10), "s": "v1"})
			if _, err := g.Rels().Update(ctx, hist, map[string]any{"tkg_valid_from": k(40), "s": "v2"}); err != nil {
				t.Fatal(err)
			}
			if err := g.Temporal().ExciseRelValidity(ctx, hist, k(30), k(50)); err != nil {
				t.Fatal(err)
			}
			all = append(all, exp{"across-two-versions", hist, want(run{k(10), k(30), "v1"}, run{k(50), 0, "v2"})})

			withTx := rel(map[string]any{"tkg_valid_from": k(10), "s": "v"})
			wr, _ := g.Rels().Get(ctx, withTx)
			if err := g.Temporal().ExciseRelValidityWithTx(ctx, withTx, k(20), k(30), wr.Temporal().TxFrom); !errors.Is(err, graphpkg.ErrTxOrder) {
				t.Fatalf("rel WithTx at creation: %v; want ErrTxOrder", err)
			}
			if err := g.Temporal().ExciseRelValidityWithTx(ctx, withTx, k(20), k(30), nowTx(t, g)); err != nil {
				t.Fatal(err)
			}
			all = append(all, exp{"withTx", withTx, want(run{k(10), k(20), "v"}, run{k(30), 0, "v"})})

			inTx := rel(map[string]any{"tkg_valid_from": k(10), "s": "v"})
			if err := g.Tx().Run(func(tx *graphpkg.GraphTx) error { return tx.ExciseRelValidity(inTx, k(20), k(30)) }); err != nil {
				t.Fatal(err)
			}
			all = append(all, exp{"tx", inTx, want(run{k(10), k(20), "v"}, run{k(30), 0, "v"})})
			txAt := rel(map[string]any{"tkg_valid_from": k(10), "s": "v"})
			txAtInstant := nowTx(t, g)
			if err := g.Tx().Run(func(tx *graphpkg.GraphTx) error {
				return tx.ExciseRelValidityWithTx(txAt, k(20), k(30), txAtInstant)
			}); err != nil {
				t.Fatal(err)
			}
			all = append(all, exp{"txWithTx", txAt, want(run{k(10), k(20), "v"}, run{k(30), 0, "v"})})

			inBatch := rel(map[string]any{"tkg_valid_from": k(10), "s": "v"})
			batchAt := rel(map[string]any{"tkg_valid_from": k(10), "s": "v"})
			bb, err := g.Batch().New()
			if err != nil {
				t.Fatal(err)
			}
			if err := bb.ExciseRelValidity(inBatch, k(30), k(30)); !errors.Is(err, graphpkg.ErrInvalidTimeRange) {
				t.Fatalf("rel batch empty: %v", err)
			}
			if err := bb.ExciseRelValidity(inBatch, k(20), k(30)); err != nil {
				t.Fatal(err)
			}
			if err := bb.ExciseRelValidityWithTx(batchAt, k(20), k(30), nowTx(t, g)); err != nil {
				t.Fatal(err)
			}
			if _, err := bb.Execute(); err != nil {
				t.Fatal(err)
			}
			all = append(all, exp{"batch", inBatch, want(run{k(10), k(20), "v"}, run{k(30), 0, "v"})})
			all = append(all, exp{"batchWithTx", batchAt, want(run{k(10), k(20), "v"}, run{k(30), 0, "v"})})

			for _, r := range []struct {
				name string
				a, b types.Instant
				want error
			}{
				{"inverted", k(30), k(20), graphpkg.ErrInvalidTimeRange},
				{"b=0", k(20), 0, graphpkg.ErrInvalidTimeRange},
				{"nothing valid", k(60), k(70), graphpkg.ErrNoVersionValidAt},
				{"the hole", k(21), k(29), graphpkg.ErrNoVersionValidAt},
			} {
				if err := g.Temporal().ExciseRelValidity(ctx, closed, r.a, r.b); !errors.Is(err, r.want) {
					t.Fatalf("rel refusal %s: %v; want %v", r.name, err, r.want)
				}
			}
			if err := g.Temporal().ExciseRelValidity(ctx, closed+types.RelID(1)<<50, k(20), k(30)); !errors.Is(err, graphpkg.ErrRelNotFound) {
				t.Fatalf("rel unknown: %v", err)
			}

			check := func(step string, g *graphpkg.Graph) {
				t.Helper()
				pinNow, _ := g.Temporal().PeekTx()
				for _, e := range all {
					if got := relTimeline(t, g, e.id, 0); got != e.wantNow {
						t.Errorf("%s %s: now\n got  %s\n want %s", step, e.name, got, e.wantNow)
					}
					cur, err := g.Rels().Get(ctx, e.id)
					if err != nil || cur.Temporal().Void {
						t.Errorf("%s %s: current %v, %v; want a non-void row", step, e.name, cur, err)
					}
					if r, err := g.Temporal().RelAsOf(e.id, pinNow); err != nil || r.Temporal().Void {
						t.Errorf("%s %s: RelAsOf = %v, %v; want a non-void row", step, e.name, r, err)
					}
					segs, err := g.Temporal().RelEffectiveTimeline(e.id, pinNow)
					if err != nil {
						t.Fatal(err)
					}
					var runs []run
					for _, s := range segs {
						v, _ := s.Rel.GetProperty("s")
						runs = append(runs, run{s.ValidFrom, s.ValidTo, fmt.Sprint(v)})
					}
					if got := want(runs...); got != e.wantNow {
						t.Errorf("%s %s: effective timeline\n got  %s\n want %s", step, e.name, got, e.wantNow)
					}
				}
				rs, err := g.Temporal().RelsDuring(k(22), k(28))
				if err != nil {
					t.Fatal(err)
				}
				for _, r := range rs {
					if r.ID() == open1 {
						t.Errorf("%s: RelsDuring(22,28) holds the excised relationship", step)
					}
				}
			}
			check("live", g)
			if !b.reopen {
				return
			}
			if err := g.Close(); err != nil {
				t.Fatal(err)
			}
			g = open()
			check("reopened", g)
		})
	}
}
