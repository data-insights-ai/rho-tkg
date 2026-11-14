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

// The relationship twins of the budgeted node doors (RH list R2, R2-TWINS):
// Temporal().RelAtTxCtx, RelAtCtx, RelsAtTxByIDsCtx, RelsEffectiveByIDsCtx and
// RelLowestAnsweredPinCtx, and Rels().GetCtx. Tests count work (rows decoded,
// history looked at, shard opens, callbacks), never time or the heap. Notes and
// red output: tasks/evidence/R2TWINS/red.txt.

// r2RelWorld is n relationships of type NET, then an early pin, then two
// updates of each, then every fourth one deleted: at the early pin every
// relationship has one version and the current row no longer answers, so a
// point read at it walks history.
type r2RelWorld struct {
	g     *Core
	rels  []types.RelID // ascending
	early types.Instant
	pin   types.Instant
}

func r2BuildRels(t testing.TB, g *Core, n int) *r2RelWorld {
	t.Helper()
	ctx := context.Background()
	s, e := txbEndpointsTB(t, g)
	w := &r2RelWorld{g: g}
	for i := range n {
		r, err := g.Rels.AddByID(ctx, sbType, s, e, map[string]any{"i": int64(i), "pad": fmt.Sprintf("%0*d", 60, i)})
		if err != nil {
			t.Fatal(err)
		}
		w.rels = append(w.rels, r.ID())
	}
	var err error
	if w.early, err = g.Temporal.NowTx(); err != nil {
		t.Fatal(err)
	}
	for i, id := range w.rels {
		for v := range 2 {
			if _, err := g.Rels.Update(ctx, id, map[string]any{"v": int64(v)}); err != nil {
				t.Fatal(err)
			}
		}
		if i%4 == 3 {
			if err := g.Rels.Delete(ctx, id); err != nil {
				t.Fatal(err)
			}
		}
	}
	if w.pin, err = g.Temporal.NowTx(); err != nil {
		t.Fatal(err)
	}
	return w
}

// ids is the by-ID doors' input: every relationship in reverse order, the first
// one again and an ID no relationship has (valid, unknown).
func (w *r2RelWorld) ids() []types.RelID {
	ids := slices.Clone(w.rels)
	slices.Reverse(ids)
	return append(ids, w.rels[0], w.rels[len(w.rels)-1]+1<<30)
}

// r2Row renders a relationship row (nil: "nil").
func r2Row(r *types.Relationship) string {
	if r == nil {
		return "nil"
	}
	return fmt.Sprintf("r%d v%d", r.ID(), r.Version())
}

func r2Rows(rs []*types.Relationship) string {
	var out []string
	for _, r := range rs {
		out = append(out, r2Row(r))
	}
	return strings.Join(out, ", ")
}

// TestReadBudget_R2Twins_RelsAtTxByIDs: RelsAtTxByIDsCtx answers the plain
// door's aligned rows or refuses with none, as NodesAtTxByIDsCtx does. The
// candidates and the result buffer count before any ID is read: a limit one
// under the ID count refuses with no history looked at and no byte decoded (the
// plain door reads every distinct ID); a history or decoded-bytes limit of 1
// refuses within the first entity; exactly the unlimited count answers, one under
// refuses. Badger, sharded and tiered, rows pending and flushed.
func TestReadBudget_R2Twins_RelsAtTxByIDs(t *testing.T) {
	t.Parallel()
	const n = 12
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			g, shut := be.open(t, dir)
			w := r2BuildRels(t, g, n)
			probe := func(t *testing.T, g *Core, flushed bool) {
				ids := w.ids()
				cold := func() {
					if flushed {
						rbdEvict(t, g, nil, w.rels)
					}
				}
				ctx := context.Background()
				cold()
				pl, err := g.Temporal.RelsAtTxByIDs(ids, w.early, w.early)
				if err != nil || len(pl) != len(ids) || pl[len(pl)-1] != nil || pl[0] == nil {
					t.Fatalf("plain = %d rows, %v; want %d aligned rows, the unknown ID nil", len(pl), err, len(ids))
				}
				want := r2Rows(pl)
				run := func(ctx context.Context) (string, error) {
					rows, err := g.Temporal.RelsAtTxByIDsCtx(ctx, ids, w.early, w.early)
					if rows == nil {
						return "", err
					}
					return r2Rows(rows), err
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
					if got != "" || rb.Door != "RelsAtTxByIDs" || work != rb.Work {
						t.Fatalf("%s %d: answered %q, door %q, work %+v, meter %+v; want no rows from RelsAtTxByIDs and the meter's work", c.limit, c.n, got, rb.Door, rb.Work, work)
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
				if _, err := g.Temporal.RelsAtTxByIDsCtx(bctx, []types.RelID{w.rels[0], 0}, w.early, w.early); err == nil || isBudget(err, "candidates") {
					t.Fatalf("an invalid ID = %v; want ErrInvalidStoreMutation", err)
				}
				if _, err := g.Temporal.RelsAtTxByIDsCtx(bctx, w.rels[:2], w.early, 0); err == nil || isBudget(err, "candidates") {
					t.Fatalf("a zero pin = %v; want ErrInvalidTimeRange", err)
				}
				if w := meter.Work(); w.Candidates != 0 {
					t.Fatalf("refused calls counted %+v; want nothing", w)
				}
				if rows, err := g.Temporal.RelsAtTxByIDsCtx(bctx, nil, w.early, w.early); err != nil || len(rows) != 0 {
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

// TestReadBudget_R2Twins_RelsEffectiveByIDs: RelsEffectiveByIDsCtx streams the
// plain door's segments in the order of ids (a repeated ID once, an unknown ID
// nothing) under a budget, as NodesEffectiveByIDsCtx does: one candidate per
// distinct ID before its read, the history, decoded bytes and shard opens of the
// read, each segment before fn, 8 buffer bytes per ID of the list before it is
// copied; a limit refuses at that limit after the entities read before it handed
// out their segments (a prefix of the plain stream) and none after, before the
// next entity is read.
func TestReadBudget_R2Twins_RelsEffectiveByIDs(t *testing.T) {
	t.Parallel()
	const n = 12
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			g, shut := be.open(t, dir)
			w := r2BuildRels(t, g, n)
			probe := func(t *testing.T, g *Core, flushed bool) {
				ids := w.ids()
				distinct := firstOccurrences(ids)
				plain := func(fn func(string) bool) error {
					return g.Temporal.RelsEffectiveByIDs(ids, w.pin, func(s temporal.RelSegment) bool { return fn(sbRelSeg(s)) })
				}
				run := func(ctx context.Context, fn func(string) bool) error {
					return g.Temporal.RelsEffectiveByIDsCtx(ctx, ids, w.pin, func(s temporal.RelSegment) bool { return fn(sbRelSeg(s)) })
				}
				cold := func() {
					if flushed {
						rbdEvict(t, g, nil, w.rels)
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
				if total.Candidates != uint64(len(distinct)) || total.Segments != uint64(len(want)) || total.BufferBytes != uint64(8*len(ids)) || total.HistoryExamined == 0 || flushed && total.DecodedBytes == 0 { // #nosec G115 -- counts
					t.Fatalf("unlimited work %+v; want %d candidates (distinct IDs), %d segments, %d buffer bytes and the history looked at", total, len(distinct), len(want), 8*len(ids))
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
						if rb.Door != "RelsEffectiveByIDs" || sbWorkOf(work, c.limit) != sbWorkOf(rb.Work, c.limit) || !sbPrefix(got, want) {
							t.Fatalf("refused by %q with work %+v, meter %+v; callbacks %v; want the door, the refused counter equal and a prefix of the plain stream", rb.Door, rb.Work, work, got)
						}
						switch c.limit {
						case "candidates":
							first := map[string]bool{}
							for _, id := range distinct[:c.n] {
								first[fmt.Sprintf("r%d", int64(id))] = true
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

// TestReadBudget_R2Twins_RelsEffectiveByIDsGates: a nil callback is refused
// before any charge as the plain door refuses it, a cancelled context and a nil
// one before anything, an invalid ID before anything is counted.
func TestReadBudget_R2Twins_RelsEffectiveByIDsGates(t *testing.T) {
	t.Parallel()
	g, shut := rbdBackends()[0].open(t, t.TempDir())
	defer shut()
	w := r2BuildRels(t, g, 4)
	bctx, meter := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{Candidates: 100})
	if err := g.Temporal.RelsEffectiveByIDsCtx(bctx, w.rels, w.pin, nil); !errors.Is(err, grapherr.ErrNilCallback) || meter.Work() != (storepkg.ReadWork{}) {
		t.Fatalf("nil callback = %v, work %+v; want ErrNilCallback and nothing counted", err, meter.Work())
	}
	calls := 0
	count := func(temporal.RelSegment) bool { calls++; return true }
	cctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := g.Temporal.RelsEffectiveByIDsCtx(cctx, w.rels, w.pin, count); !errors.Is(err, context.Canceled) || calls != 0 {
		t.Fatalf("cancelled ctx = %v after %d callbacks; want context.Canceled and none", err, calls)
	}
	var nilCtx context.Context
	if err := g.Temporal.RelsEffectiveByIDsCtx(nilCtx, w.rels, w.pin, count); !errors.Is(err, ErrNilContext) || calls != 0 {
		t.Fatalf("nil ctx = %v after %d callbacks; want ErrNilContext and none", err, calls)
	}
	if err := g.Temporal.RelsEffectiveByIDsCtx(bctx, []types.RelID{w.rels[0], 0}, w.pin, count); err == nil || meter.Work() != (storepkg.ReadWork{}) {
		t.Fatalf("an invalid ID = %v, work %+v; want ErrInvalidStoreMutation before anything is counted", err, meter.Work())
	}
	if err := g.Temporal.RelsEffectiveByIDsCtx(bctx, w.rels, w.pin+1<<40, count); !errors.Is(err, ErrTxPinTooNew) || meter.Work() != (storepkg.ReadWork{}) {
		t.Fatalf("a pin above the clock = %v, work %+v; want ErrTxPinTooNew before anything is counted", err, meter.Work())
	}
}
