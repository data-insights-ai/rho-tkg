package core

import (
	"context"
	"errors"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The last two node doors without a Ctx form (RH list R2, R2-TWINS):
// Temporal().NodesAtOptsByIDsCtx and NodesAtTxCtx. Tests count work (rows
// decoded, history looked at, shard opens, candidates), never time or the heap.
// Notes and red output: tasks/evidence/R2TWINS/red.txt.

// TestReadBudget_R2Twins_NodesAtOptsByIDs: NodesAtOptsByIDsCtx answers the plain
// door's aligned rows (the rows NodesAtTxByIDs answers at the same pins) or
// refuses with none, counted as NodesAtTxByIDsCtx: the candidates and the result
// buffer before any ID is read, so a limit one under the ID count refuses with
// nothing read; a history or decoded-bytes limit of 1 refuses within the first
// entity; exactly the unlimited count answers, one under refuses. The call's own
// checks (a point form of opts, a pin the clock has reached, IDs) run before
// anything is counted. Badger, sharded and tiered, rows pending and flushed.
func TestReadBudget_R2Twins_NodesAtOptsByIDs(t *testing.T) {
	t.Parallel()
	const n = 12
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			g, shut := be.open(t, dir)
			w := sbBuildEarly(t, g, n)
			opts := storepkg.QueryOpts{ValidAt: w.early, ValidAtSet: true, TxAt: w.early}
			probe := func(t *testing.T, g *Core, flushed bool) {
				ids := w.ids()
				cold := func() {
					if flushed {
						rbdEvict(t, g, w.nodes, nil)
					}
				}
				ctx := context.Background()
				cold()
				pl, err := g.Temporal.NodesAtOptsByIDs(ids, opts)
				if err != nil || len(pl) != len(ids) || pl[len(pl)-1] != nil || pl[0] == nil {
					t.Fatalf("plain = %d rows, %v; want %d aligned rows, the unknown ID nil", len(pl), err, len(ids))
				}
				want := sbRows(pl)
				if viaTx, err := g.Temporal.NodesAtTxByIDs(ids, w.early, w.early); err != nil || sbRows(viaTx) != want {
					t.Fatalf("NodesAtTxByIDs at the same pins = %q, %v; want the same rows as NodesAtOptsByIDs %q", sbRows(viaTx), err, want)
				}
				run := func(ctx context.Context) (string, error) {
					rows, err := g.Temporal.NodesAtOptsByIDsCtx(ctx, ids, opts)
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
					if got != "" || rb.Door != "NodesAtOptsByIDs" || work != rb.Work {
						t.Fatalf("%s %d: answered %q, door %q, work %+v, meter %+v; want no rows from NodesAtOptsByIDs and the meter's work", c.limit, c.n, got, rb.Door, rb.Work, work)
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
				if _, err := g.Temporal.NodesAtOptsByIDsCtx(bctx, []types.NodeID{w.nodes[0], 0}, opts); err == nil || isBudget(err, "candidates") {
					t.Fatalf("an invalid ID = %v; want ErrInvalidStoreMutation", err)
				}
				if _, err := g.Temporal.NodesAtOptsByIDsCtx(bctx, w.nodes[:2], storepkg.QueryOpts{TxPin: w.early}); !errors.Is(err, ErrConflictingTemporalOpts) {
					t.Fatalf("a TxPin = %v; want ErrConflictingTemporalOpts", err)
				}
				if _, err := g.Temporal.NodesAtOptsByIDsCtx(bctx, w.nodes[:2], storepkg.QueryOpts{TxAt: w.pin + 1<<40}); !errors.Is(err, ErrTxPinTooNew) {
					t.Fatalf("a pin above the clock = %v; want ErrTxPinTooNew", err)
				}
				if w := meter.Work(); w.Candidates != 0 {
					t.Fatalf("refused calls counted %+v; want nothing", w)
				}
				if rows, err := g.Temporal.NodesAtOptsByIDsCtx(bctx, nil, opts); err != nil || len(rows) != 0 {
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

// TestReadBudget_R2Twins_NodesAtTx: NodesAtTxCtx answers NodesAtTx's rows in ID
// order or refuses with none. Every node ever stored is a candidate, found by the
// store's ID walks, which count each distinct ID (and 24 buffer bytes for its
// set entry) before it joins the candidate set, so a Candidates limit stops the
// walk at the ID that passes it with nothing read; then 8 bytes per ID for the
// list and 8 for the result slice before any node is read, then each node's rows
// decoded and history looked at. A limit of 1 on history or decoded bytes refuses
// within the first node; exactly the unlimited count answers, one under refuses.
// Badger, sharded and tiered, rows pending and flushed.
func TestReadBudget_R2Twins_NodesAtTx(t *testing.T) {
	t.Parallel()
	const n = 12
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			g, shut := be.open(t, dir)
			w := sbBuildEarly(t, g, n)
			probe := func(t *testing.T, g *Core, flushed bool) {
				cold := func() {
					if flushed {
						rbdEvict(t, g, w.nodes, nil)
					}
				}
				ctx := context.Background()
				cold()
				pl, err := g.Temporal.NodesAtTx(w.early, w.early)
				if err != nil || len(pl) != n {
					t.Fatalf("plain = %d rows, %v; want %d (every node, the deleted ones included, existed at the early pin)", len(pl), err, n)
				}
				want := sbRows(pl)
				run := func(ctx context.Context) (string, error) {
					rows, err := g.Temporal.NodesAtTxCtx(ctx, w.early, w.early)
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
				// 24 bytes a set entry, 8 an ID of the list, 8 a slot of the result slice.
				if total.Candidates != n || total.BufferBytes != 40*n || total.HistoryExamined == 0 || flushed && total.DecodedBytes == 0 || total.Segments != 0 || total.ShardOpens != 0 {
					t.Fatalf("unlimited work %+v; want %d candidates (the distinct known nodes), %d buffer bytes and the history looked at", total, n, 40*n)
				}
				for _, c := range []struct {
					limit string
					n     uint64
				}{{"candidates", 5}, {"buffer bytes", 40*n - 1}, {"buffer bytes", 1}, {"history examined", 1}, {"decoded bytes", 1}} {
					if sbWorkOf(total, c.limit) <= c.n {
						continue
					}
					cold()
					pctx, meter := storepkg.WithReadBudget(ctx, sbBudget(c.limit, c.n))
					got, err := run(pctx)
					rb := rbdBudgetErr(t, err, c.limit)
					work := meter.Work()
					if got != "" || rb.Door != "NodesAtTx" || work != rb.Work {
						t.Fatalf("%s %d: answered %q, door %q, work %+v, meter %+v; want no rows from NodesAtTx and the meter's work", c.limit, c.n, got, rb.Door, rb.Work, work)
					}
					switch c.limit {
					case "candidates":
						// The walk stopped at the ID that passed the limit: n+1 counted of 12 known, nothing read.
						if work.Candidates != c.n+1 || work.HistoryExamined != 0 || work.DecodedBytes != 0 {
							t.Fatalf("candidates %d: %+v; want %d counted of %d and nothing read", c.n, work, c.n+1, n)
						}
					case "buffer bytes":
						if work.HistoryExamined != 0 || work.DecodedBytes != 0 || c.n == 1 && work.Candidates > 1 {
							t.Fatalf("buffer bytes %d: %+v; want nothing read (and, at 1, the walk stopped at its first ID)", c.n, work)
						}
					default:
						if work.Candidates != n || work.HistoryExamined > 2 || work.DecodedBytes > total.DecodedBytes/4 {
							t.Fatalf("%s 1: refused after %+v (unlimited %+v); want every ID gathered and the first node's read only", c.limit, work, total)
						}
					}
					t.Logf("%s %d: refused after %+v (unlimited %+v)", c.limit, c.n, work, total)
				}
				for _, limit := range []string{"candidates", "history examined", "buffer bytes"} {
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
				// A pin before every node answers an empty list, the candidates counted.
				ectx, em := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
				if rows, err := g.Temporal.NodesAtTxCtx(ectx, 1, 1); err != nil || len(rows) != 0 || em.Work().Candidates != n {
					t.Fatalf("a time before every node = %d rows, %v, work %+v; want none and %d candidates", len(rows), err, em.Work(), n)
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

// TestReadBudget_R2Twins_NodesAtTxShardOpens: on a tiered store whose cold shards
// are closed before each read, the plain NodesAtTx opens every cold shard (its ID
// walks and its reads); the walks cannot count their opens, so a budget that
// bounds ShardOpens is refused before the walk (Limit "unsupported", no shard
// opened, no row); a budget that does not bound them answers the plain rows.
// NodesAtOptsByIDsCtx counts its reads' opens: over a node live in the first
// shard and one deleted there (live nowhere, searched on every shard) the plain
// read opens every cold shard; under ShardOpens 1 the Ctx door opens at most one
// and refuses with no row; unlimited it counts exactly the opens.
func TestReadBudget_R2Twins_NodesAtTxShardOpens(t *testing.T) {
	t.Parallel()
	w := sbBuildTiered(t)
	ctx := context.Background()
	pre := w.preDelete
	w.closeCold(t)
	opens := w.opens()
	pl, err := w.g.Temporal.NodesAtTx(pre, pre)
	plainOpens := w.opens() - opens
	if err != nil || len(pl) == 0 || plainOpens < 3 {
		t.Fatalf("plain NodesAtTx = %d rows, %v, %d shards opened; want rows and every cold shard (3) opened", len(pl), err, plainOpens)
	}
	want := sbRows(pl)

	w.closeCold(t)
	opens = w.opens()
	bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{ShardOpens: 1})
	rows, err := w.g.Temporal.NodesAtTxCtx(bctx, pre, pre)
	rb := rbdBudgetErr(t, err, "unsupported")
	if rows != nil || rb.Door != "NodesAtTx" || w.opens() != opens || meter.Work().Candidates != 0 {
		t.Fatalf("ShardOpens 1 = %d rows, door %q, %d shards opened, work %+v; want a refusal before the walk: no row, no open, nothing counted", len(rows), rb.Door, w.opens()-opens, meter.Work())
	}

	w.closeCold(t)
	bctx, meter = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 1000})
	rows, err = w.g.Temporal.NodesAtTxCtx(bctx, pre, pre)
	if err != nil || sbRows(rows) != want || meter.Work().Candidates == 0 {
		t.Fatalf("a budget without ShardOpens = %q, %v, work %+v; want the plain rows %q", sbRows(rows), err, meter.Work(), want)
	}

	// NodesAtOptsByIDsCtx: a live node and one deleted in the first shard.
	ids := []types.NodeID{w.nodes[0], w.nodes[1]}
	opts := storepkg.QueryOpts{ValidAt: pre, ValidAtSet: true, TxAt: pre}
	w.closeCold(t)
	opens = w.opens()
	pids, err := w.g.Temporal.NodesAtOptsByIDs(ids, opts)
	idsOpens := w.opens() - opens
	if err != nil || sbRows(pids) == "" || idsOpens < 2 {
		t.Fatalf("plain NodesAtOptsByIDs = %q, %v, %d shards opened; want rows and at least 2 opens", sbRows(pids), err, idsOpens)
	}
	w.closeCold(t)
	opens = w.opens()
	bctx, meter = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{ShardOpens: 1})
	got, err := w.g.Temporal.NodesAtOptsByIDsCtx(bctx, ids, opts)
	boundedOpened := w.opens() - opens
	rb = rbdBudgetErr(t, err, "shard opens")
	if got != nil || rb.Door != "NodesAtOptsByIDs" || meter.Work() != rb.Work || boundedOpened > 1 {
		t.Fatalf("NodesAtOptsByIDs under ShardOpens 1 = %v, door %q, work %+v, %d shards opened; want no rows from the door and at most 1 open", got, rb.Door, meter.Work(), boundedOpened)
	}
	w.closeCold(t)
	opens = w.opens()
	bctx, meter = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
	got, err = w.g.Temporal.NodesAtOptsByIDsCtx(bctx, ids, opts)
	opened := w.opens() - opens
	if err != nil || sbRows(got) != sbRows(pids) || meter.Work().ShardOpens != opened {
		t.Fatalf("unlimited NodesAtOptsByIDs = %q, %v, %d shards opened, %d counted; want the plain rows and every open counted", sbRows(got), err, opened, meter.Work().ShardOpens)
	}
	t.Logf("NodesAtTx: the plain read opened %d cold shards, ShardOpens 1 is refused before the walk (0 opened); NodesAtOptsByIDs: the plain read opened %d, under ShardOpens 1 it opened %d and refused, unlimited it opened %d and counted %d", plainOpens, idsOpens, boundedOpened, opened, meter.Work().ShardOpens)
}

// TestReadBudget_R2Twins_NodeGates: both node doors check their context first
// (cancelled: context.Canceled, nil: ErrNilContext) and a closed graph before a
// candidate is counted.
func TestReadBudget_R2Twins_NodeGates(t *testing.T) {
	t.Parallel()
	g, shut := rbdBackends()[0].open(t, t.TempDir())
	w := sbBuildEarly(t, g, 4)
	ctx := context.Background()
	opts := storepkg.QueryOpts{ValidAt: w.early, ValidAtSet: true, TxAt: w.early}
	doors := []struct {
		name string
		call func(ctx context.Context) error
	}{
		{"NodesAtOptsByIDs", func(ctx context.Context) error {
			_, err := g.Temporal.NodesAtOptsByIDsCtx(ctx, w.nodes, opts)
			return err
		}},
		{"NodesAtTx", func(ctx context.Context) error {
			_, err := g.Temporal.NodesAtTxCtx(ctx, w.early, w.early)
			return err
		}},
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	var nilCtx context.Context
	bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 100})
	for _, d := range doors {
		if err := d.call(cctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("%s: cancelled ctx = %v", d.name, err)
		}
		if err := d.call(nilCtx); !errors.Is(err, ErrNilContext) {
			t.Fatalf("%s: nil ctx = %v", d.name, err)
		}
	}
	shut()
	for _, d := range doors {
		if err := d.call(bctx); !errors.Is(err, ErrGraphClosed) || meter.Work() != (storepkg.ReadWork{}) {
			t.Fatalf("%s: closed graph = %v, work %+v; want ErrGraphClosed and no candidate counted", d.name, err, meter.Work())
		}
	}
}
