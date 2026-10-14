package graph_test

import (
	"context"
	"errors"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestSetNodeVersionIntervalOverGapAppendsPiece: a valid-time correction over an
// interval disjoint from every existing piece (a GAP) appends one new piece and
// leaves the earlier one untouched. Node valid [10,20) s=old, corrected over
// [30,40) s=new: at every pin [10,20) answers old, 25 and 45 nothing, [30,40) new
// from the correction's TxFrom on; History holds both pieces; survives a reopen.
func TestSetNodeVersionIntervalOverGapAppendsPiece(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const (
		vf1, vt1 = types.Instant(10_000), types.Instant(20_000)
		vf2, vt2 = types.Instant(30_000), types.Instant(40_000)
	)
	doors := []struct {
		name   string
		withTx bool
	}{{"SetNodeVersionInterval", false}, {"SetNodeVersionIntervalWithTx", true}}
	for _, b := range hasHistoryBackends() {
		for _, d := range doors {
			t.Run(b.name+"/"+d.name, func(t *testing.T) {
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

				props := map[string]any{"tkg_valid_from": vf1, "tkg_valid_to": vt1, "s": "old", "keep": int64(7)}
				var n *types.Node
				var err error
				txBase := types.Instant(time.Now().Add(-2 * time.Hour).UnixMilli())
				if d.withTx {
					n, err = g.Nodes().AddWithTx(ctx, []string{"Derived"}, props, txBase)
				} else {
					n, err = g.Nodes().Add(ctx, []string{"Derived"}, props)
				}
				if err != nil {
					t.Fatalf("add: %v", err)
				}
				id := n.ID()
				t1 := n.Temporal().TxFrom
				var piece *types.Node
				if d.withTx {
					piece, err = g.Temporal().SetNodeVersionIntervalWithTx(ctx, id, vf2, vt2, map[string]any{"s": "new"}, txBase+3_600_000)
				} else {
					piece, err = g.Temporal().SetNodeVersionInterval(ctx, id, vf2, vt2, map[string]any{"s": "new"})
				}
				if err != nil {
					t.Fatalf("%s: %v", d.name, err)
				}
				t2 := piece.Temporal().TxFrom
				if t2 <= t1 {
					t.Fatalf("correction TxFrom %d <= creation TxFrom %d", t2, t1)
				}
				if pt := piece.Temporal(); pt.ValidFrom != vf2 || pt.ValidTo != vt2 {
					t.Fatalf("returned piece valid [%d,%d); want [%d,%d)", pt.ValidFrom, pt.ValidTo, vf2, vt2)
				}

				check := func(step string, g *graphpkg.Graph) {
					t.Helper()
					// want[validAt] per pin: "" = no version valid.
					type probe struct {
						at   types.Instant
						want string
					}
					for _, pin := range []types.Instant{t1, t2 - 1, t2, t2 + 1, 0} {
						corrected := pin == 0 || pin >= t2
						newWant := ""
						if corrected {
							newWant = "new"
						}
						for _, p := range []probe{{vf1, "old"}, {vt1 - 1, "old"}, {vt1, ""}, {25_000, ""}, {vf2, newWant}, {vt2 - 1, newWant}, {vt2, ""}, {45_000, ""}} {
							got, err := g.Temporal().NodeAtTx(id, p.at, pin)
							if p.want == "" {
								if !errors.Is(err, graphpkg.ErrNoVersionValidAt) {
									t.Fatalf("%s: NodeAtTx(valid %d, tx %d) = %v, %v; want ErrNoVersionValidAt", step, p.at, pin, got, err)
								}
								continue
							}
							if err != nil {
								t.Fatalf("%s: NodeAtTx(valid %d, tx %d): %v", step, p.at, pin, err)
							}
							if s, _ := got.GetProperty("s"); s != p.want {
								t.Fatalf("%s: NodeAtTx(valid %d, tx %d) s=%v; want %s", step, p.at, pin, s, p.want)
							}
							if k, _ := got.GetProperty("keep"); k != int64(7) {
								t.Fatalf("%s: NodeAtTx(valid %d, tx %d) keep=%v; want 7 (patch keeps unnamed keys)", step, p.at, pin, k)
							}
						}
					}
					// Before the node existed in tx time nothing answers.
					if _, err := g.Temporal().NodeAtTx(id, vf1, t1-1); !errors.Is(err, graphpkg.ErrNoVersionValidAt) && !errors.Is(err, graphpkg.ErrNodeNotFound) {
						t.Fatalf("%s: NodeAtTx before creation: %v", step, err)
					}
					// The earlier piece is still the current row byte for byte; the gap piece is a
					// history row (History excludes the current row).
					cur, err := g.Nodes().Get(ctx, id)
					if err != nil {
						t.Fatalf("%s: Get: %v", step, err)
					}
					chain, err := g.Nodes().History(id)
					if err != nil {
						t.Fatalf("%s: History: %v", step, err)
					}
					chain = append(chain, cur)
					var sawOld, sawNew bool
					for _, r := range chain {
						tm := r.Temporal()
						s, _ := r.GetProperty("s")
						switch {
						case tm.ValidFrom == vf1 && tm.ValidTo == vt1 && s == "old":
							sawOld = true
							if r != cur || r.Version() != n.Version() || tm.TxFrom != t1 || tm.TxTo != 0 {
								t.Fatalf("%s: earlier piece rewritten or moved: version %d tx [%d,%d) current=%v; want current version %d tx [%d,0)", step, r.Version(), tm.TxFrom, tm.TxTo, r == cur, n.Version(), t1)
							}
						case tm.ValidFrom == vf2 && tm.ValidTo == vt2 && s == "new":
							sawNew = true
							if tm.TxFrom != t2 || r.Version() <= n.Version() {
								t.Fatalf("%s: gap piece tx from %d version %d; want tx from %d, version above %d", step, tm.TxFrom, r.Version(), t2, n.Version())
							}
						default:
							t.Fatalf("%s: unexpected chain row valid [%d,%d) tx [%d,%d) s=%v", step, tm.ValidFrom, tm.ValidTo, tm.TxFrom, tm.TxTo, s)
						}
					}
					if !sawOld || !sawNew || len(chain) != 2 {
						t.Fatalf("%s: chain has %d rows (old %v, new %v); want exactly the two pieces", step, len(chain), sawOld, sawNew)
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
}
