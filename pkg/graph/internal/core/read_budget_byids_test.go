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

// sbEarlyWorld is n nodes of label D, then a pin, then two updates of each,
// then every fourth one deleted: at the early pin every node has one version
// and the current row no longer answers, so a point read at it walks history.
type sbEarlyWorld struct {
	g     *Core
	nodes []types.NodeID // ascending
	early types.Instant
	pin   types.Instant
}

func sbBuildEarly(t testing.TB, g *Core, n int) *sbEarlyWorld {
	t.Helper()
	ctx := context.Background()
	w := &sbEarlyWorld{g: g}
	for i := range n {
		nd, err := g.Nodes.Add(ctx, []string{sbLabel}, map[string]any{"i": int64(i), "pad": fmt.Sprintf("%0*d", 60, i)})
		if err != nil {
			t.Fatal(err)
		}
		w.nodes = append(w.nodes, nd.ID())
	}
	var err error
	if w.early, err = g.Temporal.NowTx(); err != nil {
		t.Fatal(err)
	}
	for i, id := range w.nodes {
		for v := range 2 {
			if _, err := g.Nodes.Update(ctx, id, map[string]any{"v": int64(v)}); err != nil {
				t.Fatal(err)
			}
		}
		if i%4 == 3 {
			if err := g.Nodes.Delete(ctx, id); err != nil {
				t.Fatal(err)
			}
		}
	}
	if w.pin, err = g.Temporal.NowTx(); err != nil {
		t.Fatal(err)
	}
	return w
}

// ids is the by-ID doors' input: every node in reverse order, the first node
// again and an ID no node has (valid, unknown).
func (w *sbEarlyWorld) ids() []types.NodeID {
	ids := slices.Clone(w.nodes)
	slices.Reverse(ids)
	return append(ids, w.nodes[0], w.nodes[len(w.nodes)-1]+1<<30)
}

// sbRow renders a node row (nil: "nil").
func sbRow(n *types.Node) string {
	if n == nil {
		return "nil"
	}
	return fmt.Sprintf("n%d v%d", n.ID(), n.Version())
}

func sbRows(ns []*types.Node) string {
	var out []string
	for _, n := range ns {
		out = append(out, sbRow(n))
	}
	return strings.Join(out, ", ")
}

// TestReadBudget_SigmaB_NodesEffectiveByIDs: NodesEffectiveByIDsCtx streams the
// plain door's segments in the order of ids (a repeated ID once, an unknown ID
// nothing) under a budget, as the scans do: one candidate per distinct ID before
// its read, the history, decoded bytes and shard opens of the read, each
// segment before fn, 8 buffer bytes per ID of the list before it is copied; a
// limit refuses at that limit after the entities read before it handed out their
// segments (a prefix of the plain stream) and none after, before the next
// entity is read.
func TestReadBudget_SigmaB_NodesEffectiveByIDs(t *testing.T) {
	t.Parallel()
	const n = 12
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			g, shut := be.open(t, dir)
			w := sbBuildEarly(t, g, n)
			probe := func(t *testing.T, g *Core, flushed bool) {
				ids := w.ids()
				distinct := firstOccurrences(ids)
				nf := func(fn func(string) bool) func(s temporal.NodeSegment) bool {
					return func(s temporal.NodeSegment) bool { return fn(sbNodeSeg(s)) }
				}
				plain := func(fn func(string) bool) error { return g.Temporal.NodesEffectiveByIDs(ids, w.pin, nf(fn)) }
				run := func(ctx context.Context, fn func(string) bool) error {
					return g.Temporal.NodesEffectiveByIDsCtx(ctx, ids, w.pin, nf(fn))
				}
				cold := func() {
					if flushed {
						rbdEvict(t, g, w.nodes, nil)
					}
				}
				ctx := context.Background()
				cold()
				want, err := sbCollect(plain)
				if err != nil || len(want) == 0 {
					t.Fatalf("plain = %d segments, %v", len(want), err)
				}
				cold()
				if got, err := sbCollect(func(fn func(string) bool) error { return run(ctx, fn) }); err != nil || !slices.Equal(got, want) {
					t.Fatalf("no budget = %v, %v; want the plain stream\n%v", got, err, want)
				}
				cold()
				fctx, full := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
				if got, err := sbCollect(func(fn func(string) bool) error { return run(fctx, fn) }); err != nil || !slices.Equal(got, want) {
					t.Fatalf("unlimited = %v, %v; want the plain stream\n%v", got, err, want)
				}
				total := full.Work()
				if total.Candidates != uint64(len(distinct)) || total.Segments != uint64(len(want)) || total.BufferBytes != uint64(8*len(ids)) { // #nosec G115 -- counts
					t.Fatalf("unlimited work %+v; want %d candidates (distinct IDs), %d segments, %d buffer bytes", total, len(distinct), len(want), 8*len(ids))
				}
				for _, c := range []struct {
					limit string
					n     uint64
				}{{"candidates", 5}, {"segments", 3}, {"history examined", 1}, {"decoded bytes", 1}, {"buffer bytes", 1}} {
					if sbWorkOf(total, c.limit) <= c.n {
						continue
					}
					t.Run(c.limit, func(t *testing.T) {
						cold()
						pctx, meter := storepkg.WithReadBudget(ctx, sbBudget(c.limit, c.n))
						got, err := sbCollect(func(fn func(string) bool) error { return run(pctx, fn) })
						rb := rbdBudgetErr(t, err, c.limit)
						work := meter.Work()
						if rb.Door != "NodesEffectiveByIDs" || sbWorkOf(work, c.limit) != sbWorkOf(rb.Work, c.limit) || !sbPrefix(got, want) {
							t.Fatalf("refused by %q with work %+v, meter %+v; callbacks %v; want the door, the refused counter equal and a prefix of the plain stream", rb.Door, rb.Work, work, got)
						}
						switch c.limit {
						case "candidates":
							first := map[string]bool{}
							for _, id := range distinct[:c.n] {
								first[fmt.Sprintf("n%d", int64(id))] = true
							}
							var exp []string
							for _, s := range want {
								if first[sbEntity(s)] {
									exp = append(exp, s)
								}
							}
							if !slices.Equal(got, exp) || work.Candidates != c.n+1 {
								t.Fatalf("candidates %d: callbacks %v, work %+v; want the first %d IDs' segments %v and %d counted", c.n, got, work, c.n, exp, c.n+1)
							}
						case "segments":
							if len(got) != 3 || work.Segments != 4 || work.Candidates > 2 {
								t.Fatalf("segments 3: %d callbacks, work %+v; want 3 callbacks, 4 counted, at most 2 entities read", len(got), work)
							}
						case "buffer bytes":
							if len(got) != 0 || work.Candidates != 0 {
								t.Fatalf("buffer bytes 1: %d callbacks, work %+v; want none and no entity read", len(got), work)
							}
						default:
							if work.Candidates > 2 {
								t.Fatalf("%s 1: %d entities read before the refusal (%+v); want at most 2", c.limit, work.Candidates, work)
							}
						}
						t.Logf("%s %d: refused after %+v (unlimited %+v)", c.limit, c.n, work, total)
					})
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

// TestReadBudget_SigmaB_NodeAtTx: NodeAtTxCtx answers the plain door's row at
// the same pins under a budget, counting one candidate and the history and
// decoded bytes of the read, and a limit of 1 refuses with no row after at most
// two history versions (the plain read looks at the whole chain of 24
// updates); exactly the unlimited count answers, one under refuses.
func TestReadBudget_SigmaB_NodeAtTx(t *testing.T) {
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
				pn, err := g.Temporal.NodeAtTx(lw.node, lw.from, lw.from)
				if err != nil || pn == nil {
					t.Fatalf("plain NodeAtTx = %v, %v", pn, err)
				}
				want := sbRow(pn)
				run := func(ctx context.Context) (string, error) {
					n, err := g.Temporal.NodeAtTxCtx(ctx, lw.node, lw.from, lw.from)
					return sbRow(n), err
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
					if got != "nil" || rb.Door != "NodeAtTx" || work != rb.Work {
						t.Fatalf("%s 1: %q, door %q, work %+v, meter %+v; want no row from NodeAtTx and the meter's work", limit, got, rb.Door, rb.Work, work)
					}
					if limit == "history examined" && work.HistoryExamined > 2 || limit == "decoded bytes" && work.DecodedBytes > total.DecodedBytes/4 {
						t.Fatalf("%s 1: refused after %+v (unlimited %+v); want the read stopped near the limit", limit, work, total)
					}
					t.Logf("%s 1: refused after %+v (unlimited %+v)", limit, work, total)
					cold()
					xctx, _ := storepkg.WithReadBudget(ctx, sbBudget(limit, sbWorkOf(total, limit)))
					if got, err := run(xctx); err != nil || got != want {
						t.Fatalf("%s %d (the unlimited count) = %q, %v; want %q", limit, sbWorkOf(total, limit), got, err, want)
					}
					cold()
					xctx, _ = storepkg.WithReadBudget(ctx, sbBudget(limit, sbWorkOf(total, limit)-1))
					if got, err := run(xctx); got != "nil" || !isBudget(err, limit) {
						t.Fatalf("%s %d (one under) = %q, %v; want a refusal and no row", limit, sbWorkOf(total, limit)-1, got, err)
					}
				}
				// Gates: a cancelled context and an invalid ID count nothing.
				cctx, cancel := context.WithCancel(ctx)
				cancel()
				if _, err := g.Temporal.NodeAtTxCtx(cctx, lw.node, lw.from, lw.from); !errors.Is(err, context.Canceled) {
					t.Fatalf("cancelled ctx = %v", err)
				}
				bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 5})
				if n, err := g.Temporal.NodeAtTxCtx(bctx, 0, lw.from, lw.from); err == nil || n != nil || meter.Work().Candidates != 0 {
					t.Fatalf("an invalid ID = %v, %v, work %+v; want an error and nothing counted", n, err, meter.Work())
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

// TestReadBudget_SigmaB_NodesAtTxByIDs: NodesAtTxByIDsCtx answers the plain
// door's aligned rows or refuses with none. The candidates and the result
// buffer count before any ID is read: a limit one under the ID count refuses
// with no history looked at and no byte decoded (the plain door reads every
// distinct ID); a history or decoded-bytes limit of 1 refuses within the first
// entity; exactly the unlimited count answers, one under refuses.
func TestReadBudget_SigmaB_NodesAtTxByIDs(t *testing.T) {
	t.Parallel()
	const n = 12
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			g, shut := be.open(t, dir)
			w := sbBuildEarly(t, g, n)
			probe := func(t *testing.T, g *Core, flushed bool) {
				ids := w.ids()
				cold := func() {
					if flushed {
						rbdEvict(t, g, w.nodes, nil)
					}
				}
				ctx := context.Background()
				cold()
				pl, err := g.Temporal.NodesAtTxByIDs(ids, w.early, w.early)
				if err != nil || len(pl) != len(ids) || pl[len(pl)-1] != nil || pl[0] == nil {
					t.Fatalf("plain = %d rows, %v; want %d aligned rows, the unknown ID nil", len(pl), err, len(ids))
				}
				want := sbRows(pl)
				run := func(ctx context.Context) (string, error) {
					rows, err := g.Temporal.NodesAtTxByIDsCtx(ctx, ids, w.early, w.early)
					if rows == nil {
						return "", err
					}
					return sbRows(rows), err
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
				if total.Candidates != uint64(len(ids)) || total.BufferBytes != uint64(8*len(ids)) || total.HistoryExamined == 0 || flushed && total.DecodedBytes == 0 { // #nosec G115 -- counts
					t.Fatalf("unlimited work %+v; want %d candidates, %d buffer bytes and the history looked at", total, len(ids), 8*len(ids))
				}
				for _, c := range []struct {
					limit string
					n     uint64
				}{{"candidates", uint64(len(ids)) - 1}, {"buffer bytes", uint64(8*len(ids)) - 1}, {"history examined", 1}, {"decoded bytes", 1}} { // #nosec G115 -- counts
					if sbWorkOf(total, c.limit) <= c.n {
						continue
					}
					cold()
					pctx, meter := storepkg.WithReadBudget(ctx, sbBudget(c.limit, c.n))
					got, err := run(pctx)
					rb := rbdBudgetErr(t, err, c.limit)
					work := meter.Work()
					if got != "" || rb.Door != "NodesAtTxByIDs" || work != rb.Work {
						t.Fatalf("%s %d: answered %q, door %q, work %+v, meter %+v; want no rows from NodesAtTxByIDs and the meter's work", c.limit, c.n, got, rb.Door, rb.Work, work)
					}
					if c.limit == "candidates" || c.limit == "buffer bytes" {
						if work.HistoryExamined != 0 || work.DecodedBytes != 0 {
							t.Fatalf("%s %d: %+v read before the refusal; want nothing read", c.limit, c.n, work)
						}
					} else if work.HistoryExamined > 2 || work.DecodedBytes > total.DecodedBytes/4 {
						t.Fatalf("%s 1: refused after %+v (unlimited %+v); want the first entity's read only", c.limit, work, total)
					}
					t.Logf("%s %d: refused after %+v (unlimited %+v)", c.limit, c.n, work, total)
				}
				for _, limit := range []string{"candidates", "history examined"} {
					cold()
					xctx, _ := storepkg.WithReadBudget(ctx, sbBudget(limit, sbWorkOf(total, limit)))
					if got, err := run(xctx); err != nil || got != want {
						t.Fatalf("%s %d (the unlimited count) = %q, %v; want %q", limit, sbWorkOf(total, limit), got, err, want)
					}
					cold()
					xctx, _ = storepkg.WithReadBudget(ctx, sbBudget(limit, sbWorkOf(total, limit)-1))
					if got, err := run(xctx); got != "" || !isBudget(err, limit) {
						t.Fatalf("%s %d (one under) = %q, %v; want a refusal and no rows", limit, sbWorkOf(total, limit)-1, got, err)
					}
				}
				// The call's own checks run before anything is counted.
				bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 100})
				if _, err := g.Temporal.NodesAtTxByIDsCtx(bctx, []types.NodeID{w.nodes[0], 0}, w.early, w.early); err == nil || isBudget(err, "candidates") {
					t.Fatalf("an invalid ID = %v; want ErrInvalidStoreMutation", err)
				}
				if _, err := g.Temporal.NodesAtTxByIDsCtx(bctx, w.nodes[:2], w.early, 0); err == nil || isBudget(err, "candidates") {
					t.Fatalf("a zero pin = %v; want ErrInvalidTimeRange", err)
				}
				if w := meter.Work(); w.Candidates != 0 {
					t.Fatalf("refused calls counted %+v; want nothing", w)
				}
				if rows, err := g.Temporal.NodesAtTxByIDsCtx(bctx, nil, w.early, w.early); err != nil || len(rows) != 0 {
					t.Fatalf("no IDs = %v, %v; want an empty answer", rows, err)
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

// TestReadBudget_SigmaB_NodesLowestAnsweredPins: the door reads no row, so
// NodesLowestAnsweredPinsCtx counts one candidate and 8 buffer bytes per ID
// before computing any answer and nothing else (no history, decoded byte or
// shard open); a limit one under refuses with no answer; the answer is the
// plain door's.
func TestReadBudget_SigmaB_NodesLowestAnsweredPins(t *testing.T) {
	t.Parallel()
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			g, shut := be.open(t, t.TempDir())
			defer shut()
			w := sbBuildEarly(t, g, 12)
			ids := w.ids()
			ctx := context.Background()
			want, err := g.Temporal.NodesLowestAnsweredPins(ids)
			if err != nil || len(want) != len(ids) {
				t.Fatalf("plain = %v, %v", want, err)
			}
			if got, err := g.Temporal.NodesLowestAnsweredPinsCtx(ctx, ids); err != nil || !slices.Equal(got, want) {
				t.Fatalf("no budget = %v, %v; want %v", got, err, want)
			}
			fctx, full := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
			if got, err := g.Temporal.NodesLowestAnsweredPinsCtx(fctx, ids); err != nil || !slices.Equal(got, want) {
				t.Fatalf("unlimited = %v, %v; want %v", got, err, want)
			}
			if total := full.Work(); total != (storepkg.ReadWork{Candidates: uint64(len(ids)), BufferBytes: uint64(8 * len(ids))}) { // #nosec G115 -- counts
				t.Fatalf("unlimited work %+v; want %d candidates and %d buffer bytes only", total, len(ids), 8*len(ids))
			}
			for _, c := range []struct {
				limit string
				n     uint64
			}{{"candidates", uint64(len(ids)) - 1}, {"buffer bytes", uint64(8*len(ids)) - 1}} { // #nosec G115 -- counts
				pctx, meter := storepkg.WithReadBudget(ctx, sbBudget(c.limit, c.n))
				got, err := g.Temporal.NodesLowestAnsweredPinsCtx(pctx, ids)
				rb := rbdBudgetErr(t, err, c.limit)
				if got != nil || rb.Door != "NodesLowestAnsweredPins" || meter.Work() != rb.Work {
					t.Fatalf("%s %d: %v, door %q, work %+v, meter %+v; want no answer from the door and the meter's work", c.limit, c.n, got, rb.Door, rb.Work, meter.Work())
				}
			}
			// The call's own checks run first; a cancelled context first of all.
			bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 100})
			if _, err := g.Temporal.NodesLowestAnsweredPinsCtx(bctx, []types.NodeID{w.nodes[0], 0}); err == nil || isBudget(err, "candidates") || meter.Work().Candidates != 0 {
				t.Fatalf("an invalid ID = %v, work %+v; want ErrInvalidStoreMutation and nothing counted", err, meter.Work())
			}
			cctx, cancel := context.WithCancel(ctx)
			cancel()
			if _, err := g.Temporal.NodesLowestAnsweredPinsCtx(cctx, ids); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled ctx = %v", err)
			}
			if got, err := g.Temporal.NodesLowestAnsweredPinsCtx(bctx, nil); err != nil || len(got) != 0 {
				t.Fatalf("no IDs = %v, %v; want an empty answer", got, err)
			}
		})
	}
}

// TestReadBudget_SigmaB_ByIDsShardOpens: on a tiered store whose cold shards
// are closed before each read, the plain by-ID reads of a node deleted in a
// cold shard (live nowhere: its chain is searched on every shard) open every
// cold shard; under ShardOpens 1 each Ctx door opens at most one and refuses
// (NodeAtTx and NodesAtTxByIDs with no row; NodesEffectiveByIDs after the
// callbacks of the entities read before it); unlimited the answer is the plain
// door's and the meter counts exactly the opens. NodesLowestAnsweredPins opens
// none.
func TestReadBudget_SigmaB_ByIDsShardOpens(t *testing.T) {
	t.Parallel()
	w := sbBuildTiered(t)
	live, gone := w.nodes[0], w.nodes[1]
	ctx := context.Background()
	ids := []types.NodeID{live, gone}
	pre := w.preDelete
	doors := []struct {
		name, door string
		plain      func() (string, error)
		ctx        func(ctx context.Context) (string, error)
		stream     bool
	}{
		{"NodeAtTx", "NodeAtTx", func() (string, error) {
			n, err := w.g.Temporal.NodeAtTx(gone, pre, pre)
			return sbRow(n), err
		}, func(ctx context.Context) (string, error) {
			n, err := w.g.Temporal.NodeAtTxCtx(ctx, gone, pre, pre)
			return sbRow(n), err
		}, false},
		{"NodesAtTxByIDs", "NodesAtTxByIDs", func() (string, error) {
			ns, err := w.g.Temporal.NodesAtTxByIDs(ids, pre, pre)
			return sbRows(ns), err
		}, func(ctx context.Context) (string, error) {
			ns, err := w.g.Temporal.NodesAtTxByIDsCtx(ctx, ids, pre, pre)
			if ns == nil {
				return "", err
			}
			return sbRows(ns), err
		}, false},
	}
	for _, d := range doors {
		t.Run(d.name, func(t *testing.T) {
			w.closeCold(t)
			opens := w.opens()
			want, err := d.plain()
			plainOpens := w.opens() - opens
			if err != nil || strings.Contains(want, "nil") || plainOpens < 2 {
				t.Fatalf("plain = %q, %v, %d shards opened; want rows and at least 2 opens", want, err, plainOpens)
			}
			w.closeCold(t)
			opens = w.opens()
			bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{ShardOpens: 1})
			got, err := d.ctx(bctx)
			boundedOpened := w.opens() - opens
			rb := rbdBudgetErr(t, err, "shard opens")
			if got != "" && got != "nil" || rb.Door != d.door || meter.Work() != rb.Work || boundedOpened > 1 {
				t.Fatalf("ShardOpens 1 = %q, door %q, work %+v, %d shards opened; want no rows, door %q and at most 1 open", got, rb.Door, meter.Work(), boundedOpened, d.door)
			}
			w.closeCold(t)
			opens = w.opens()
			bctx, meter = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
			got, err = d.ctx(bctx)
			opened := w.opens() - opens
			if err != nil || got != want || meter.Work().ShardOpens != opened {
				t.Fatalf("unlimited = %q, %v, %d shards opened, %d counted; want the plain answer and every open counted", got, err, opened, meter.Work().ShardOpens)
			}
			t.Logf("%s: the plain read opened %d cold shards; under ShardOpens 1 it opened %d and refused; unlimited it opened %d and counted %d", d.name, plainOpens, boundedOpened, opened, meter.Work().ShardOpens)
		})
	}

	t.Run("NodesEffectiveByIDs", func(t *testing.T) {
		collect := func(run func(fn func(string) bool) error) ([]string, error) { return sbCollect(run) }
		nf := func(fn func(string) bool) func(temporal.NodeSegment) bool {
			return func(s temporal.NodeSegment) bool { return fn(sbNodeSeg(s)) }
		}
		w.closeCold(t)
		opens := w.opens()
		want, err := collect(func(fn func(string) bool) error { return w.g.Temporal.NodesEffectiveByIDs(ids, w.pin, nf(fn)) })
		plainOpens := w.opens() - opens
		if err != nil || len(want) == 0 || plainOpens < 3 {
			t.Fatalf("plain = %d segments, %v, %d shards opened; want a stream and at least 3 opens", len(want), err, plainOpens)
		}
		w.closeCold(t)
		opens = w.opens()
		bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{ShardOpens: 1})
		got, err := collect(func(fn func(string) bool) error { return w.g.Temporal.NodesEffectiveByIDsCtx(bctx, ids, w.pin, nf(fn)) })
		boundedOpened, boundedCalls := w.opens()-opens, len(got)
		rb := rbdBudgetErr(t, err, "shard opens")
		if rb.Door != "NodesEffectiveByIDs" || !sbPrefix(got, want) || len(got) == len(want) || boundedOpened > 1 || meter.Work().ShardOpens < 2 {
			t.Fatalf("ShardOpens 1: door %q, %d of %d callbacks, %d shards opened, work %+v; want a prefix of the stream, the refusal and at most 1 open", rb.Door, len(got), len(want), boundedOpened, meter.Work())
		}
		w.closeCold(t)
		opens = w.opens()
		bctx, meter = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
		got, err = collect(func(fn func(string) bool) error { return w.g.Temporal.NodesEffectiveByIDsCtx(bctx, ids, w.pin, nf(fn)) })
		opened := w.opens() - opens
		if err != nil || !slices.Equal(got, want) || meter.Work().ShardOpens != opened || opened < 3 {
			t.Fatalf("unlimited = %d segments, %v, %d shards opened, %d counted; want the plain stream and every open counted", len(got), err, opened, meter.Work().ShardOpens)
		}
		t.Logf("NodesEffectiveByIDs: the plain stream opened %d cold shards; under ShardOpens 1 the stream opened %d and refused after %d of %d callbacks; unlimited it opened %d and counted %d", plainOpens, boundedOpened, boundedCalls, len(want), opened, meter.Work().ShardOpens)
	})

	t.Run("NodesLowestAnsweredPins", func(t *testing.T) {
		w.closeCold(t)
		opens := w.opens()
		bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{ShardOpens: 1})
		got, err := w.g.Temporal.NodesLowestAnsweredPinsCtx(bctx, ids)
		want, werr := w.g.Temporal.NodesLowestAnsweredPins(ids)
		if err != nil || werr != nil || !slices.Equal(got, want) || w.opens() != opens || meter.Work().ShardOpens != 0 {
			t.Fatalf("under ShardOpens 1 = %v, %v (plain %v, %v), %d shards opened, work %+v; want the plain answer and no open", got, err, want, werr, w.opens()-opens, meter.Work())
		}
	})
}

// TestReadBudget_SigmaB_ByIDsNilCallback: a nil callback is refused before any
// charge, as the plain door refuses it.
func TestReadBudget_SigmaB_ByIDsNilCallback(t *testing.T) {
	t.Parallel()
	g, shut := rbdBackends()[0].open(t, t.TempDir())
	defer shut()
	w := sbBuildEarly(t, g, 4)
	bctx, meter := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{Candidates: 100})
	if err := g.Temporal.NodesEffectiveByIDsCtx(bctx, w.nodes, w.pin, nil); !errors.Is(err, grapherr.ErrNilCallback) || meter.Work() != (storepkg.ReadWork{}) {
		t.Fatalf("nil callback = %v, work %+v; want ErrNilCallback and nothing counted", err, meter.Work())
	}
	cctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := 0
	if err := g.Temporal.NodesEffectiveByIDsCtx(cctx, w.nodes, w.pin, func(temporal.NodeSegment) bool { calls++; return true }); !errors.Is(err, context.Canceled) || calls != 0 {
		t.Fatalf("cancelled ctx = %v after %d callbacks; want context.Canceled and none", err, calls)
	}
	if err := g.Temporal.NodesEffectiveByIDsCtx(bctx, []types.NodeID{w.nodes[0], 0}, w.pin, func(temporal.NodeSegment) bool { return true }); err == nil || meter.Work() != (storepkg.ReadWork{}) {
		t.Fatalf("an invalid ID = %v, work %+v; want ErrInvalidStoreMutation before anything is counted", err, meter.Work())
	}
}
