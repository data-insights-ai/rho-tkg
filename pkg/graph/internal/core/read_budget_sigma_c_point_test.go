package core

import (
	"context"
	"errors"
	"fmt"
	"testing"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	shardedpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Sigma's group C point reads under a read budget (RH list R2): Nodes().GetCtx
// and Temporal().NodeAtCtx. Notes and red output:
// tasks/evidence/READBUDGET/sigma-doors-c.txt.

// scNodeRow renders the answer of a point read: the row's version, transaction
// interval and properties, "refused" for a read budget refusal, "nil" for no row and no error.
func scNodeRow(n *types.Node, err error) string {
	switch {
	case errors.Is(err, storepkg.ErrNodeNotFound):
		return "not found"
	case errors.Is(err, storepkg.ErrReadBudget):
		return "refused"
	case errors.Is(err, storepkg.ErrNoVersionValidAt):
		return "no version valid"
	case err != nil:
		return "error: " + err.Error()
	case n == nil:
		return "nil"
	}
	tm := n.Temporal()
	return fmt.Sprintf("n%d v%d tx[%d,%d) %v", n.ID(), n.Version(), tm.TxFrom, tm.TxTo, n.PropertiesMap())
}

// scNodeCached reports whether the node's row sits in the entity cache of the
// badger store that holds it: a row decoded from disk is cached, so a read that
// was refused before its decode leaves it out.
func scNodeCached(t testing.TB, g *Core, id types.NodeID) bool {
	t.Helper()
	var bs *badger.Store
	var err error
	switch st := g.store.(type) {
	case *badger.Store:
		bs = st
	case *shardedpkg.Store:
		bs, err = st.ShardForIDForTest(id.SnowflakeID())
	case *tiered.Store:
		bs, err = st.ShardForNodeIDForTest(id)
	default:
		t.Fatalf("no entity cache to probe on %T", g.store)
	}
	if err != nil {
		t.Fatal(err)
	}
	_, status := bs.NodeCacheForTest().Get(id.SnowflakeID())
	return status == indexpkg.CacheHit
}

// TestReadBudget_SigmaC_Get: Nodes().GetCtx answers Get's row or error under a
// budget, counting one candidate and, cold, the stored bytes of the row it
// decodes, on badger, sharded and tiered, rows pending and flushed, for a live
// node, a deleted one and an unknown ID:
//   - without a budget and under an unlimited one the answer is Get's; the meter
//     counts one candidate, and cold the row's stored bytes (a row the entity cache
//     serves, pending or already read, decodes nothing and counts 0);
//   - a limit of 1 decoded byte refuses with *ReadBudgetError from Get and no row
//     before the row is decoded (the plain Get decodes it and leaves it in the
//     entity cache; the refused read leaves the cache without it); a limit of
//     exactly the bytes answers, one under refuses; a candidate limit counts the
//     call before anything is read.
func TestReadBudget_SigmaC_Get(t *testing.T) {
	t.Parallel()
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			g, shut := be.open(t, dir)
			w := sbBuild(t, g, 12)
			live, gone := w.nodes[0], w.nodes[3] // every fourth is deleted: gone has history and no current row
			unknown := w.nodes[len(w.nodes)-1] + 1<<30
			probe := func(t *testing.T, g *Core, flushed bool) {
				ctx := context.Background()
				cold := func() {
					if flushed {
						rbdEvict(t, g, w.nodes, w.rels)
					}
				}
				for _, c := range []struct {
					name string
					id   types.NodeID
				}{{"live", live}, {"deleted", gone}, {"unknown", unknown}} {
					t.Run(c.name, func(t *testing.T) {
						cold()
						pn, perr := g.Nodes.Get(ctx, c.id)
						want := scNodeRow(pn, perr)
						if (c.name == "live") != (perr == nil) {
							t.Fatalf("plain Get = %q; want a row only for the live node", want)
						}
						if c.name == "live" && flushed && !scNodeCached(t, g, c.id) {
							t.Fatalf("the plain Get left the row it decoded out of the entity cache")
						}
						run := func(ctx context.Context) (string, error) {
							n, err := g.Nodes.GetCtx(ctx, c.id)
							return scNodeRow(n, err), err
						}
						cold()
						if got, _ := run(ctx); got != want {
							t.Fatalf("no budget = %q; want %q", got, want)
						}
						cold()
						fctx, full := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
						if got, _ := run(fctx); got != want {
							t.Fatalf("unlimited = %q; want %q", got, want)
						}
						total := full.Work()
						if total.Candidates != 1 || total.HistoryExamined != 0 || total.ShardOpens != 0 || total.Segments != 0 || total.BufferBytes != 0 || c.name == "live" && flushed != (total.DecodedBytes > 0) || c.name != "live" && total.DecodedBytes != 0 {
							t.Fatalf("unlimited work %+v; want 1 candidate, no history, no opens and %v decoded bytes", total, c.name == "live" && flushed)
						}
						if c.name != "live" || !flushed {
							return
						}
						// Cold: the plain door decoded the row; the Ctx door refuses before it does.
						cold()
						pctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{DecodedBytes: 1})
						got, err := run(pctx)
						rb := rbdBudgetErr(t, err, "decoded bytes")
						if got != "refused" || rb.Door != "Get" || meter.Work() != rb.Work || scNodeCached(t, g, c.id) {
							t.Fatalf("DecodedBytes 1 = %q, door %q, work %+v, meter %+v, row cached %v; want no row from Get, the meter's work and no decode (the row not in the entity cache)", got, rb.Door, rb.Work, meter.Work(), scNodeCached(t, g, c.id))
						}
						t.Logf("DecodedBytes 1: refused after %+v with the row not decoded (the plain Get decoded and cached it; unlimited %+v)", meter.Work(), total)
						rbdExact(t, "Get", "Get", 60, cold, func(ctx context.Context) (bool, error) {
							got, err := run(ctx)
							return err == nil && got == want, err
						})
						// Warm: a read that answers caches the row; the next decodes nothing, so one byte holds.
						if got, err := run(ctx); err != nil || got != want {
							t.Fatalf("warming read = %q, %v; want %q", got, err, want)
						}
						wctx, wmeter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{DecodedBytes: 1})
						if got, err := run(wctx); err != nil || got != want || wmeter.Work().DecodedBytes != 0 {
							t.Fatalf("warm = %q, %v, %+v; want the row and 0 decoded bytes", got, err, wmeter.Work())
						}
					})
				}
				// One meter over three calls: Candidates 2 refuses the third before it reads.
				cold()
				cctx, cmeter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 2})
				for _, id := range []types.NodeID{w.nodes[0], w.nodes[1]} {
					if _, err := g.Nodes.GetCtx(cctx, id); err != nil {
						t.Fatalf("GetCtx(%d) under Candidates 2: %v", id, err)
					}
				}
				n, err := g.Nodes.GetCtx(cctx, w.nodes[2])
				if !isBudget(err, "candidates") || n != nil || flushed && scNodeCached(t, g, w.nodes[2]) || cmeter.Work().Candidates != 3 {
					t.Fatalf("third call under Candidates 2 = %v, %v, work %+v; want a refusal before any read (the row not cached)", n, err, cmeter.Work())
				}
			}
			t.Run("pending", func(t *testing.T) { probe(t, g, false) })
			shut()
			g, shut = be.open(t, dir)
			defer shut()
			w.g = g
			t.Run("flushed", func(t *testing.T) { probe(t, g, true) })
		})
	}
}

// TestReadBudget_SigmaC_GetGates: a cancelled context is refused before
// anything is counted, an invalid ID and a closed graph before a candidate is
// counted.
func TestReadBudget_SigmaC_GetGates(t *testing.T) {
	t.Parallel()
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			g, shut := be.open(t, t.TempDir())
			w := sbBuild(t, g, 4)
			ctx := context.Background()
			cctx, cancel := context.WithCancel(ctx)
			cancel()
			if _, err := g.Nodes.GetCtx(cctx, w.nodes[0]); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled ctx = %v", err)
			}
			bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 5})
			if n, err := g.Nodes.GetCtx(bctx, 0); err == nil || n != nil || meter.Work().Candidates != 0 {
				t.Fatalf("an invalid ID = %v, %v, work %+v; want an error and nothing counted", n, err, meter.Work())
			}
			if n, err := g.Nodes.GetCtx(bctx, w.nodes[0]); err != nil || n == nil || meter.Work().Candidates != 1 {
				t.Fatalf("a live node = %v, %v, work %+v; want the row and 1 candidate", n, err, meter.Work())
			}
			shut()
			if _, err := g.Nodes.GetCtx(bctx, w.nodes[0]); !errors.Is(err, ErrGraphClosed) || meter.Work().Candidates != 1 {
				t.Fatalf("closed graph = %v, work %+v; want ErrGraphClosed and no candidate counted", err, meter.Work())
			}
		})
	}
}

// TestReadBudget_SigmaC_GetShardOpens: on a tiered store whose cold shards are
// closed before each read, GetCtx counts each closed shard it is about to open
// before the open: three live nodes in three cold shards read under one meter
// of ShardOpens 1 open one shard and refuse the second call before its shard
// opens; unlimited the rows are the plain door's and the meter counts exactly
// the opens. A deleted node answers Get's not-found with its opens counted.
func TestReadBudget_SigmaC_GetShardOpens(t *testing.T) {
	t.Parallel()
	w := sbBuildTiered(t)
	ids := []types.NodeID{w.nodes[0], w.nodes[2], w.nodes[4]} // one live node per shard
	gone := w.nodes[1]
	ctx := context.Background()
	readAll := func(ctx context.Context, plain bool) (string, error) {
		var out string
		for _, id := range ids {
			var n *types.Node
			var err error
			if plain {
				n, err = w.g.Nodes.Get(ctx, id)
			} else {
				n, err = w.g.Nodes.GetCtx(ctx, id)
			}
			if err != nil {
				return out, err
			}
			out += scNodeRow(n, nil) + "\n"
		}
		return out, nil
	}
	w.closeCold(t)
	opens := w.opens()
	want, err := readAll(ctx, true)
	plainOpens := w.opens() - opens
	if err != nil || plainOpens < 3 {
		t.Fatalf("plain = %v, %d shards opened; want the rows and 3 opens", err, plainOpens)
	}

	w.closeCold(t)
	opens = w.opens()
	bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{ShardOpens: 1})
	got, err := readAll(bctx, false)
	boundedOpened := w.opens() - opens
	rb := rbdBudgetErr(t, err, "shard opens")
	if rb.Door != "Get" || meter.Work() != rb.Work || boundedOpened != 1 || got != scNodeRowsFirst(want) {
		t.Fatalf("ShardOpens 1 = %q, door %q, work %+v, %d shards opened; want the first row, then a refusal from Get with 1 open", got, rb.Door, meter.Work(), boundedOpened)
	}

	w.closeCold(t)
	opens = w.opens()
	bctx, meter = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
	got, err = readAll(bctx, false)
	opened := w.opens() - opens
	if err != nil || got != want || meter.Work().ShardOpens != opened || opened != plainOpens {
		t.Fatalf("unlimited = %q, %v, %d shards opened, %d counted; want the plain rows and every open (%d) counted", got, err, opened, meter.Work().ShardOpens, plainOpens)
	}
	t.Logf("three nodes in three cold shards: the plain reads opened %d; under ShardOpens 1 the reads opened %d and refused the second; unlimited they opened %d and counted %d", plainOpens, boundedOpened, opened, meter.Work().ShardOpens)

	// A deleted node: Get's not-found, every open it makes counted.
	w.closeCold(t)
	opens = w.opens()
	_, perr := w.g.Nodes.Get(ctx, gone)
	goneOpens := w.opens() - opens
	w.closeCold(t)
	opens = w.opens()
	bctx, meter = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
	_, err = w.g.Nodes.GetCtx(bctx, gone)
	if !errors.Is(perr, storepkg.ErrNodeNotFound) || !errors.Is(err, storepkg.ErrNodeNotFound) || meter.Work().ShardOpens != w.opens()-opens || meter.Work().ShardOpens != goneOpens {
		t.Fatalf("deleted node: plain %v (%d opens), Ctx %v, work %+v (%d opens); want not found both ways and the same opens counted", perr, goneOpens, err, meter.Work(), w.opens()-opens)
	}
	if goneOpens != 1 {
		t.Fatalf("a deleted node: the plain Get opened %d cold shards; want 1 (its own: Get is not a fan-out door)", goneOpens)
	}
}

// scNodeRowsFirst is the first row line of readAll's rendering.
func scNodeRowsFirst(all string) string {
	for i := range all {
		if all[i] == '\n' {
			return all[:i+1]
		}
	}
	return all
}

// TestReadBudget_SigmaC_NodeAt: NodeAtCtx answers NodeAt's row or error at the
// same valid instant under a budget (NodeAtTxCtx with no transaction filter),
// counting one candidate and the history and decoded bytes of the read, and a
// limit of 1 refuses with no row after at most two history versions (the plain
// read looks at the whole chain of 24 updates); exactly the unlimited count
// answers, one under refuses.
func TestReadBudget_SigmaC_NodeAt(t *testing.T) {
	t.Parallel()
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			g, shut := be.open(t, dir)
			lw := sbBuildLong(t, g)
			probe := func(t *testing.T, g *Core, flushed bool) {
				cold := func() {
					if flushed {
						rbdEvict(t, g, []types.NodeID{lw.node}, nil)
					}
				}
				ctx := context.Background()
				cold()
				// The plain read looks at the whole chain: the skeleton rows it lists
				// (headStats.metaRows) and the full rows it reads (historyRead).
				historyRows := 0
				g.historyRead = func(n int) { historyRows += n }
				metaBefore := g.headStats.metaRows.Load()
				pn, perr := g.Temporal.NodeAt(lw.node, lw.from)
				g.historyRead = nil
				plainLooked := historyRows + int(g.headStats.metaRows.Load()-metaBefore) // #nosec G115 -- a small count
				want := scNodeRow(pn, perr)
				if perr != nil || pn == nil || plainLooked < sbLongVersions {
					t.Fatalf("plain NodeAt = %v, %v, %d history rows looked at; want a row and the whole chain (at least %d) looked at", pn, perr, plainLooked, sbLongVersions)
				}
				run := func(ctx context.Context) (string, error) {
					n, err := g.Temporal.NodeAtCtx(ctx, lw.node, lw.from)
					return scNodeRow(n, err), err
				}
				cold()
				if got, err := run(ctx); err != nil || got != want {
					t.Fatalf("no budget = %q, %v; want %q", got, err, want)
				}
				cold()
				fctx, full := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
				if got, err := run(fctx); err != nil || got != want {
					t.Fatalf("unlimited = %q, %v; want %q", got, err, want)
				}
				total := full.Work()
				if total.Candidates != 1 || total.HistoryExamined < sbLongVersions/2 || flushed && total.DecodedBytes == 0 {
					t.Fatalf("unlimited work %+v; want 1 candidate and the chain looked at", total)
				}
				for _, limit := range []string{"history examined", "decoded bytes"} {
					if sbWorkOf(total, limit) <= 1 {
						continue
					}
					cold()
					pctx, meter := storepkg.WithReadBudget(ctx, sbBudget(limit, 1))
					got, err := run(pctx)
					rb := rbdBudgetErr(t, err, limit)
					work := meter.Work()
					if got != "refused" || rb.Door != "NodeAt" || work != rb.Work {
						t.Fatalf("%s 1: %q, door %q, work %+v, meter %+v; want no row from NodeAt and the meter's work", limit, got, rb.Door, rb.Work, work)
					}
					if limit == "history examined" && work.HistoryExamined > 2 || limit == "decoded bytes" && work.DecodedBytes > total.DecodedBytes/4 {
						t.Fatalf("%s 1: refused after %+v (unlimited %+v); want the read stopped near the limit", limit, work, total)
					}
					t.Logf("%s 1: refused after %+v (unlimited %+v; the plain door looked at %d history rows)", limit, work, total, plainLooked)
					cold()
					xctx, _ := storepkg.WithReadBudget(ctx, sbBudget(limit, sbWorkOf(total, limit)))
					if got, err := run(xctx); err != nil || got != want {
						t.Fatalf("%s %d (the unlimited count) = %q, %v; want %q", limit, sbWorkOf(total, limit), got, err, want)
					}
					cold()
					xctx, _ = storepkg.WithReadBudget(ctx, sbBudget(limit, sbWorkOf(total, limit)-1))
					if got, err := run(xctx); got != "refused" || !isBudget(err, limit) {
						t.Fatalf("%s %d (one under) = %q, %v; want a refusal and no row", limit, sbWorkOf(total, limit)-1, got, err)
					}
				}
				// A time before the node existed answers NodeAt's error, the read's work counted.
				bctx, bmeter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 5})
				_, pe := g.Temporal.NodeAt(lw.node, 1)
				_, ce := g.Temporal.NodeAtCtx(bctx, lw.node, 1)
				if (pe == nil) != (ce == nil) || scNodeRow(nil, pe) != scNodeRow(nil, ce) || bmeter.Work().Candidates != 1 {
					t.Fatalf("NodeAt before the node = %v; NodeAtCtx = %v, work %+v; want the same error", pe, ce, bmeter.Work())
				}
				// Gates: a cancelled context and an invalid ID count nothing.
				cctx, cancel := context.WithCancel(ctx)
				cancel()
				if _, err := g.Temporal.NodeAtCtx(cctx, lw.node, lw.from); !errors.Is(err, context.Canceled) {
					t.Fatalf("cancelled ctx = %v", err)
				}
				gctx, gmeter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 5})
				if n, err := g.Temporal.NodeAtCtx(gctx, 0, lw.from); err == nil || n != nil || gmeter.Work().Candidates != 0 {
					t.Fatalf("an invalid ID = %v, %v, work %+v; want an error and nothing counted", n, err, gmeter.Work())
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

// TestReadBudget_SigmaC_NodeAtShardOpens: on a tiered store whose cold shards
// are closed before each read, the plain NodeAt of a node deleted in a cold
// shard (live nowhere: its chain is searched on every shard) opens every cold
// shard; under ShardOpens 1 NodeAtCtx opens at most one and refuses with no
// row; unlimited the row is the plain door's and the meter counts exactly the
// opens.
func TestReadBudget_SigmaC_NodeAtShardOpens(t *testing.T) {
	t.Parallel()
	w := sbBuildTiered(t)
	gone, pre := w.nodes[1], w.preDelete
	ctx := context.Background()
	w.closeCold(t)
	opens := w.opens()
	pn, perr := w.g.Temporal.NodeAt(gone, pre)
	plainOpens := w.opens() - opens
	want := scNodeRow(pn, perr)
	if perr != nil || pn == nil || plainOpens < 2 {
		t.Fatalf("plain NodeAt = %q, %d shards opened; want the row and at least 2 opens", want, plainOpens)
	}

	w.closeCold(t)
	opens = w.opens()
	bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{ShardOpens: 1})
	n, err := w.g.Temporal.NodeAtCtx(bctx, gone, pre)
	boundedOpened := w.opens() - opens
	rb := rbdBudgetErr(t, err, "shard opens")
	if n != nil || rb.Door != "NodeAt" || meter.Work() != rb.Work || boundedOpened > 1 {
		t.Fatalf("ShardOpens 1 = %v, door %q, work %+v, %d shards opened; want no row, door NodeAt and at most 1 open", n, rb.Door, meter.Work(), boundedOpened)
	}

	w.closeCold(t)
	opens = w.opens()
	bctx, meter = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
	n, err = w.g.Temporal.NodeAtCtx(bctx, gone, pre)
	opened := w.opens() - opens
	if got := scNodeRow(n, err); err != nil || got != want || meter.Work().ShardOpens != opened {
		t.Fatalf("unlimited = %q, %v, %d shards opened, %d counted; want the plain row and every open counted", got, err, opened, meter.Work().ShardOpens)
	}
	t.Logf("NodeAt of a deleted node: the plain read opened %d cold shards; under ShardOpens 1 the read opened %d and refused; unlimited it opened %d and counted %d", plainOpens, boundedOpened, opened, meter.Work().ShardOpens)
}
