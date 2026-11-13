package core

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Sigma's group C doors under a read budget (RH list R2): the entity history
// doors (Nodes().HistoryCtx, Rels().HistoryCtx), the point reads (Nodes().GetCtx,
// Temporal().NodeAtCtx) and Temporal().RelsLowestAnsweredPinsCtx. Tests count
// work (rows decoded, history looked at, shard opens, rows read by the plain
// door), never time or the heap. Notes and red output:
// tasks/evidence/READBUDGET/sigma-doors-c.txt.

// scHist is one entity history door, plain and under a context: both return
// the rendered rows (nil rows render "") and how many there are.
type scHist struct {
	name, door string
	plain      func() (string, int, error)
	ctx        func(ctx context.Context) (string, int, error)
}

// scNodeRows renders a node history: version, the transaction interval and
// the properties of every row, in order.
func scNodeRows(h []*types.Node, err error) (string, int, error) {
	var out []string
	for _, n := range h {
		tm := n.Temporal()
		out = append(out, fmt.Sprintf("n%d v%d tx[%d,%d) %v", n.ID(), n.Version(), tm.TxFrom, tm.TxTo, n.PropertiesMap()))
	}
	return strings.Join(out, "\n"), len(h), err
}

func scRelRows(h []*types.Relationship, err error) (string, int, error) {
	var out []string
	for _, r := range h {
		tm := r.Temporal()
		out = append(out, fmt.Sprintf("r%d v%d tx[%d,%d) %v", r.ID(), r.Version(), tm.TxFrom, tm.TxTo, r.PropertiesMap()))
	}
	return strings.Join(out, "\n"), len(h), err
}

func scHistDoors(g *Core, node types.NodeID, rel types.RelID) []scHist {
	return []scHist{
		{"node history", "History",
			func() (string, int, error) { return scNodeRows(g.Nodes.History(node)) },
			func(ctx context.Context) (string, int, error) { return scNodeRows(g.Nodes.HistoryCtx(ctx, node)) }},
		{"rel history", "History",
			func() (string, int, error) { return scRelRows(g.Rels.History(rel)) },
			func(ctx context.Context) (string, int, error) { return scRelRows(g.Rels.HistoryCtx(ctx, rel)) }},
	}
}

// TestReadBudget_SigmaC_History: Nodes().HistoryCtx and Rels().HistoryCtx
// answer the plain door's rows in its order under a budget, or refuse with no
// rows, on badger, sharded and tiered, rows pending and flushed, over a node
// and a relationship with 24 updates:
//   - the plain door reads every version (Core.historyRead) and has no budget to
//     stop at; without a budget, and under an unlimited one, the Ctx door's
//     answer is the plain door's, and the unlimited meter counts one candidate
//     and exactly one history version examined per row;
//   - a limit of 1 on history examined or decoded bytes refuses with
//     *ReadBudgetError at that limit from History and no rows, the meter equal
//     to the error's work, and the read stops there: at most two versions
//     looked at (decoded bytes: at most a quarter of the unlimited bytes);
//   - a limit of exactly the unlimited count answers as without one, one under
//     it refuses; a limit at half the versions refuses within two versions of it.
func TestReadBudget_SigmaC_History(t *testing.T) {
	t.Parallel()
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			g, shut := be.open(t, dir)
			lw := sbBuildLong(t, g)
			probe := func(t *testing.T, g *Core, flushed bool) {
				for _, d := range scHistDoors(g, lw.node, lw.rel) {
					t.Run(d.name, func(t *testing.T) {
						cold := func() {
							if flushed {
								rbdEvict(t, g, []types.NodeID{lw.node}, []types.RelID{lw.rel})
							}
						}
						ctx := context.Background()
						cold()
						plainRows := 0
						g.historyRead = func(n int) { plainRows += n }
						want, rows, err := d.plain()
						g.historyRead = nil
						if err != nil || rows < sbLongVersions || plainRows != rows {
							t.Fatalf("plain = %d rows, %v, %d rows read; want every version (at least %d) read", rows, err, plainRows, sbLongVersions)
						}
						cold()
						if got, _, err := d.ctx(ctx); err != nil || got != want {
							t.Fatalf("no budget = %q, %v; want the plain rows %q", got, err, want)
						}
						cold()
						fctx, full := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
						if got, _, err := d.ctx(fctx); err != nil || got != want {
							t.Fatalf("unlimited = %q, %v; want the plain rows %q", got, err, want)
						}
						total := full.Work()
						if total.Candidates != 1 || total.HistoryExamined != uint64(rows) || total.DecodedBytes == 0 || total.ShardOpens != 0 { // #nosec G115 -- counts
							t.Fatalf("unlimited work %+v; want 1 candidate, %d history versions, decoded bytes and no shard opens", total, rows)
						}
						for _, limit := range []string{"history examined", "decoded bytes"} {
							t.Run(limit, func(t *testing.T) {
								cold()
								pctx, meter := storepkg.WithReadBudget(ctx, sbBudget(limit, 1))
								got, n, err := d.ctx(pctx)
								rb := rbdBudgetErr(t, err, limit)
								work := meter.Work()
								if got != "" || n != 0 || rb.Door != d.door || work != rb.Work {
									t.Fatalf("%s 1: %d rows, door %q, work %+v, meter %+v; want no rows, door %q and the meter's work", limit, n, rb.Door, rb.Work, work, d.door)
								}
								if limit == "history examined" && work.HistoryExamined > 2 || limit == "decoded bytes" && work.DecodedBytes > total.DecodedBytes/4 {
									t.Fatalf("%s 1: refused after %+v (unlimited %+v); want the read stopped near the limit", limit, work, total)
								}
								t.Logf("%s 1: refused after %+v (unlimited %+v, plain door read %d rows)", limit, work, total, plainRows)
								// Exact or refused: at its own count the read answers as
								// without a budget, one under it refuses with no rows.
								cold()
								xctx, _ := storepkg.WithReadBudget(ctx, sbBudget(limit, sbWorkOf(total, limit)))
								if got, _, err := d.ctx(xctx); err != nil || got != want {
									t.Fatalf("%s %d (the unlimited count) = %q, %v; want the plain rows", limit, sbWorkOf(total, limit), got, err)
								}
								cold()
								xctx, _ = storepkg.WithReadBudget(ctx, sbBudget(limit, sbWorkOf(total, limit)-1))
								if got, _, err := d.ctx(xctx); got != "" || !isBudget(err, limit) {
									t.Fatalf("%s %d (one under) = %q, %v; want a refusal and no rows", limit, sbWorkOf(total, limit)-1, got, err)
								}
								// A limit at half the versions stops within two of it.
								half := sbWorkOf(total, limit) / 2
								cold()
								hctx, hmeter := storepkg.WithReadBudget(ctx, sbBudget(limit, half))
								if got, _, err := d.ctx(hctx); got != "" || !isBudget(err, limit) {
									t.Fatalf("%s %d (half) = %q, %v; want a refusal and no rows", limit, half, got, err)
								}
								if limit == "history examined" && hmeter.Work().HistoryExamined > half+2 {
									t.Fatalf("history examined %d: counted %d before the refusal; want at most %d", half, hmeter.Work().HistoryExamined, half+2)
								}
							})
						}
					})
				}
			}
			t.Run("pending", func(t *testing.T) { probe(t, g, false) })
			shut()
			g, shut = be.open(t, dir)
			defer shut()
			t.Run("flushed", func(t *testing.T) { probe(t, g, true) })
		})
	}
}

// TestReadBudget_SigmaC_HistoryGates: a cancelled context is refused before
// anything is counted, an invalid ID and a closed graph before a candidate is
// counted; an unknown (valid) ID answers the plain door's empty history and
// counts its one candidate; the plain door's nil and an empty answer agree.
func TestReadBudget_SigmaC_HistoryGates(t *testing.T) {
	t.Parallel()
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			g, shut := be.open(t, t.TempDir())
			lw := sbBuildLong(t, g)
			ctx := context.Background()
			cctx, cancel := context.WithCancel(ctx)
			cancel()
			if _, err := g.Nodes.HistoryCtx(cctx, lw.node); !errors.Is(err, context.Canceled) {
				t.Fatalf("node cancelled ctx = %v", err)
			}
			if _, err := g.Rels.HistoryCtx(cctx, lw.rel); !errors.Is(err, context.Canceled) {
				t.Fatalf("rel cancelled ctx = %v", err)
			}
			bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 5})
			if h, err := g.Nodes.HistoryCtx(bctx, 0); err == nil || h != nil || meter.Work().Candidates != 0 {
				t.Fatalf("an invalid node ID = %v, %v, work %+v; want an error and nothing counted", h, err, meter.Work())
			}
			if h, err := g.Rels.HistoryCtx(bctx, 0); err == nil || h != nil || meter.Work().Candidates != 0 {
				t.Fatalf("an invalid rel ID = %v, %v, work %+v; want an error and nothing counted", h, err, meter.Work())
			}
			unknownNode, unknownRel := lw.node+1<<30, lw.rel+1<<30
			wantN, errN := g.Nodes.History(unknownNode)
			wantR, errR := g.Rels.History(unknownRel)
			gotN, errGN := g.Nodes.HistoryCtx(bctx, unknownNode)
			gotR, errGR := g.Rels.HistoryCtx(bctx, unknownRel)
			if errN != nil || errR != nil || errGN != nil || errGR != nil || len(gotN) != len(wantN) || len(gotR) != len(wantR) || meter.Work().Candidates != 2 {
				t.Fatalf("an unknown ID: plain %d/%d rows (%v, %v), Ctx %d/%d rows (%v, %v), work %+v; want the same empty answers and 2 candidates", len(wantN), len(wantR), errN, errR, len(gotN), len(gotR), errGN, errGR, meter.Work())
			}
			// A candidate limit counts the one ID: two calls under a budget of 1 refuse the second.
			tctx, tmeter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 1})
			first, err := g.Nodes.HistoryCtx(tctx, lw.node)
			if err != nil || len(first) < sbLongVersions {
				t.Fatalf("one candidate under 1 = %d rows, %v", len(first), err)
			}
			after := tmeter.Work()
			if h, err := g.Nodes.HistoryCtx(tctx, lw.node); !isBudget(err, "candidates") || h != nil || tmeter.Work().HistoryExamined != after.HistoryExamined {
				t.Fatalf("a second candidate under 1 = %d rows, %v, work %+v; want a refusal before any version is looked at (work before %+v)", len(h), err, tmeter.Work(), after)
			}
			shut()
			if _, err := g.Nodes.HistoryCtx(bctx, lw.node); !errors.Is(err, ErrGraphClosed) {
				t.Fatalf("closed graph = %v; want ErrGraphClosed", err)
			}
			if _, err := g.Rels.HistoryCtx(bctx, lw.rel); !errors.Is(err, ErrGraphClosed) {
				t.Fatalf("closed graph (rel) = %v; want ErrGraphClosed", err)
			}
		})
	}
}

// TestReadBudget_SigmaC_HistoryShardOpens: on a tiered store whose cold shards
// are closed before each read, the plain History of a node and of a
// relationship deleted in a cold shard (live nowhere: its history is searched
// on every shard) opens every cold shard; under ShardOpens 1 HistoryCtx opens at
// most one and refuses with no rows; unlimited the rows are the plain door's
// and the meter counts exactly the opens.
func TestReadBudget_SigmaC_HistoryShardOpens(t *testing.T) {
	t.Parallel()
	w := sbBuildTiered(t)
	gone, goneRel := w.nodes[1], w.rels[1] // deleted in the first shard
	ctx := context.Background()
	for _, d := range scHistDoors(w.g, gone, goneRel) {
		t.Run(d.name, func(t *testing.T) {
			w.closeCold(t)
			opens := w.opens()
			want, rows, err := d.plain()
			plainOpens := w.opens() - opens
			if err != nil || rows == 0 || plainOpens < 2 {
				t.Fatalf("plain = %d rows, %v, %d shards opened; want rows and at least 2 opens", rows, err, plainOpens)
			}

			w.closeCold(t)
			opens = w.opens()
			bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{ShardOpens: 1})
			got, _, err := d.ctx(bctx)
			boundedOpened := w.opens() - opens
			rb := rbdBudgetErr(t, err, "shard opens")
			if got != "" || rb.Door != d.door || meter.Work() != rb.Work || boundedOpened > 1 {
				t.Fatalf("ShardOpens 1 = %q, door %q, work %+v, %d shards opened; want no rows, door %q and at most 1 open", got, rb.Door, meter.Work(), boundedOpened, d.door)
			}

			w.closeCold(t)
			opens = w.opens()
			bctx, meter = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
			got, _, err = d.ctx(bctx)
			opened := w.opens() - opens
			if err != nil || got != want || meter.Work().ShardOpens != opened {
				t.Fatalf("unlimited = %q, %v, %d shards opened, %d counted; want the plain rows and every open counted", got, err, opened, meter.Work().ShardOpens)
			}
			t.Logf("%s: the plain read opened %d cold shards; under ShardOpens 1 the read opened %d and refused; unlimited it opened %d and counted %d", d.name, plainOpens, boundedOpened, opened, meter.Work().ShardOpens)
		})
	}
}
