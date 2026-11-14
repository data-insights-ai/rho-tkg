package core

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	shardedpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// r2RelResult renders the answer of a relationship point read: the row, "refused"
// for a read budget refusal, "not found", "no version valid" or "nil".
func r2RelResult(r *types.Relationship, err error) string {
	switch {
	case errors.Is(err, storepkg.ErrRelNotFound):
		return "not found"
	case errors.Is(err, storepkg.ErrReadBudget):
		return "refused"
	case errors.Is(err, storepkg.ErrNoVersionValidAt):
		return "no version valid"
	case err != nil:
		return "error: " + err.Error()
	case r == nil:
		return "nil"
	}
	tm := r.Temporal()
	return fmt.Sprintf("r%d v%d tx[%d,%d) %v", r.ID(), r.Version(), tm.TxFrom, tm.TxTo, r.PropertiesMap())
}

// r2RelCached reports whether the relationship's row sits in the entity cache of
// the badger store that holds it: a row decoded from disk is cached, so a read
// that was refused before its decode leaves it out.
func r2RelCached(t testing.TB, g *Core, id types.RelID) bool {
	t.Helper()
	var bs *badger.Store
	var err error
	switch st := g.store.(type) {
	case *badger.Store:
		bs = st
	case *shardedpkg.Store:
		bs, err = st.ShardForIDForTest(id.SnowflakeID())
	case *tiered.Store:
		bs, err = st.ShardForRelIDForTest(id)
	default:
		t.Fatalf("no entity cache to probe on %T", g.store)
	}
	if err != nil {
		t.Fatal(err)
	}
	_, status := bs.RelCacheForTest().Get(id.SnowflakeID())
	return status == indexpkg.CacheHit
}

// TestReadBudget_R2Twins_RelGet: Rels().GetCtx answers Get's row or error under
// a budget, counting one candidate and, cold, the stored bytes of the row it
// decodes, on badger, sharded and tiered, rows pending and flushed, for a live
// relationship, a deleted one and an unknown ID (the twin of
// TestReadBudget_SigmaC_Get):
//   - without a budget and under an unlimited one the answer is Get's; the meter
//     counts one candidate, and cold the row's stored bytes (a row the entity cache
//     serves decodes nothing and counts 0);
//   - a limit of 1 decoded byte refuses with *ReadBudgetError from Get and no row
//     before the row is decoded (the plain Get decodes it and leaves it in the
//     entity cache; the refused read leaves the cache without it); a limit of
//     exactly the bytes answers, one under refuses; a candidate limit counts the
//     call before anything is read.
func TestReadBudget_R2Twins_RelGet(t *testing.T) {
	t.Parallel()
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			g, shut := be.open(t, dir)
			w := sbBuild(t, g, 12)
			live, gone := w.rels[0], w.rels[3] // every fourth is deleted: gone has history and no current row
			unknown := w.rels[len(w.rels)-1] + 1<<30
			probe := func(t *testing.T, g *Core, flushed bool) {
				ctx := context.Background()
				cold := func() {
					if flushed {
						rbdEvict(t, g, w.nodes, w.rels)
					}
				}
				for _, c := range []struct {
					name string
					id   types.RelID
				}{{"live", live}, {"deleted", gone}, {"unknown", unknown}} {
					t.Run(c.name, func(t *testing.T) {
						cold()
						pr, perr := g.Rels.Get(ctx, c.id)
						want := r2RelResult(pr, perr)
						if (c.name == "live") != (perr == nil) {
							t.Fatalf("plain Get = %q; want a row only for the live relationship", want)
						}
						if c.name == "live" && flushed && !r2RelCached(t, g, c.id) {
							t.Fatalf("the plain Get left the row it decoded out of the entity cache")
						}
						run := func(ctx context.Context) (string, error) {
							r, err := g.Rels.GetCtx(ctx, c.id)
							return r2RelResult(r, err), err
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
						if got != "refused" || rb.Door != "Get" || meter.Work() != rb.Work || r2RelCached(t, g, c.id) {
							t.Fatalf("DecodedBytes 1 = %q, door %q, work %+v, meter %+v, row cached %v; want no row from Get, the meter's work and no decode (the row not in the entity cache)", got, rb.Door, rb.Work, meter.Work(), r2RelCached(t, g, c.id))
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
				for _, id := range []types.RelID{w.rels[0], w.rels[1]} {
					if _, err := g.Rels.GetCtx(cctx, id); err != nil {
						t.Fatalf("GetCtx(%d) under Candidates 2: %v", id, err)
					}
				}
				r, err := g.Rels.GetCtx(cctx, w.rels[2])
				if !isBudget(err, "candidates") || r != nil || flushed && r2RelCached(t, g, w.rels[2]) || cmeter.Work().Candidates != 3 {
					t.Fatalf("third call under Candidates 2 = %v, %v, work %+v; want a refusal before any read (the row not cached)", r, err, cmeter.Work())
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

// TestReadBudget_R2Twins_RelAt: RelAtTxCtx and RelAtCtx answer RelAtTx's and
// RelAt's row or error at the same pins under a budget, counting one candidate
// and the history and decoded bytes of the read, and a limit of 1 refuses with
// no row after at most two history versions (the plain read looks at the whole
// chain of 24 updates); exactly the unlimited count answers, one under refuses
// (the twins of TestReadBudget_SigmaB_NodeAtTx and _SigmaC_NodeAt).
func TestReadBudget_R2Twins_RelAt(t *testing.T) {
	t.Parallel()
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			g, shut := be.open(t, dir)
			lw := sbBuildLong(t, g)
			probe := func(t *testing.T, g *Core, flushed bool) {
				doors := []struct {
					name  string
					plain func(at types.Instant) (*types.Relationship, error)
					ctx   func(ctx context.Context, id types.RelID, at types.Instant) (*types.Relationship, error)
				}{
					{"RelAtTx", func(at types.Instant) (*types.Relationship, error) { return g.Temporal.RelAtTx(lw.rel, at, at) },
						func(ctx context.Context, id types.RelID, at types.Instant) (*types.Relationship, error) {
							return g.Temporal.RelAtTxCtx(ctx, id, at, at)
						}},
					{"RelAt", func(at types.Instant) (*types.Relationship, error) { return g.Temporal.RelAt(lw.rel, at) },
						func(ctx context.Context, id types.RelID, at types.Instant) (*types.Relationship, error) {
							return g.Temporal.RelAtCtx(ctx, id, at)
						}},
				}
				cold := func() {
					if flushed {
						rbdEvict(t, g, nil, []types.RelID{lw.rel})
					}
				}
				ctx := context.Background()
				for _, d := range doors {
					t.Run(d.name, func(t *testing.T) {
						cold()
						pr, perr := d.plain(lw.from)
						want := r2RelResult(pr, perr)
						if perr != nil || pr == nil {
							t.Fatalf("plain %s = %q; want a row", d.name, want)
						}
						run := func(ctx context.Context) (string, error) {
							r, err := d.ctx(ctx, lw.rel, lw.from)
							return r2RelResult(r, err), err
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
							if got != "refused" || rb.Door != d.name || work != rb.Work {
								t.Fatalf("%s 1: %q, door %q, work %+v, meter %+v; want no row from %s and the meter's work", limit, got, rb.Door, rb.Work, work, d.name)
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
							if got, err := run(xctx); got != "refused" || !isBudget(err, limit) {
								t.Fatalf("%s %d (one under) = %q, %v; want a refusal and no row", limit, sbWorkOf(total, limit)-1, got, err)
							}
						}
						// A time before the relationship existed answers the plain door's error, the read's work counted.
						bctx, bmeter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 5})
						_, pe := d.plain(1)
						_, ce := d.ctx(bctx, lw.rel, 1)
						if pe == nil || r2RelResult(nil, pe) != r2RelResult(nil, ce) || bmeter.Work().Candidates != 1 {
							t.Fatalf("%s before the relationship = %v; the Ctx door = %v, work %+v; want the same error", d.name, pe, ce, bmeter.Work())
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

// TestReadBudget_R2Twins_RelLowestAnsweredPin: RelLowestAnsweredPinCtx reads the
// retention gate's rows (with a watermark set: the relationship's current row
// and, if deleted, its history) and answers the plain door's pin under a
// budget, or refuses with none, on badger, sharded and tiered, rows pending and
// flushed, for a live relationship, a deleted one and an unknown ID: one
// candidate and 8 buffer bytes, the history versions of a deleted one and, cold,
// the decoded bytes of the rows read; a buffer limit under 8 refuses before any
// read; a history or decoded-bytes limit of 1 refuses within the first read;
// exactly the unlimited count answers, one under refuses.
func TestReadBudget_R2Twins_RelLowestAnsweredPin(t *testing.T) {
	t.Parallel()
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			g, shut := be.open(t, dir)
			w := sbBuild(t, g, 12)
			_, floor := scRetain(t, g)
			live, gone := w.rels[0], w.rels[3]
			unknown := w.rels[len(w.rels)-1] + 1<<30
			probe := func(t *testing.T, g *Core, flushed bool) {
				ctx := context.Background()
				for _, c := range []struct {
					name string
					id   types.RelID
				}{{"live", live}, {"deleted", gone}, {"unknown", unknown}} {
					t.Run(c.name, func(t *testing.T) {
						cold := func() {
							if flushed {
								rbdEvict(t, g, nil, w.rels)
							}
						}
						cold()
						want, err := g.Temporal.RelLowestAnsweredPin(c.id)
						if err != nil || want == 0 {
							t.Fatalf("plain = %d, %v; want a pin", want, err)
						}
						run := func(ctx context.Context) (types.Instant, error) { return g.Temporal.RelLowestAnsweredPinCtx(ctx, c.id) }
						cold()
						if got, err := run(ctx); err != nil || got != want {
							t.Fatalf("no budget = %d, %v; want %d", got, err, want)
						}
						cold()
						fctx, full := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
						if got, err := run(fctx); err != nil || got != want {
							t.Fatalf("unlimited = %d, %v; want %d", got, err, want)
						}
						total := full.Work()
						if c.name != "unknown" && want != floor {
							t.Fatalf("pin %d; want the type's watermark %d", want, floor)
						}
						if total.Candidates != 1 || total.BufferBytes != 8 || total.ShardOpens != 0 || total.Segments != 0 || c.name == "deleted" && total.HistoryExamined == 0 || c.name == "live" && total.HistoryExamined != 0 || flushed && c.name != "unknown" && total.DecodedBytes == 0 {
							t.Fatalf("unlimited work %+v; want 1 candidate, 8 buffer bytes, the deleted one's history and, cold, the decoded rows", total)
						}
						// A buffer limit under the 8 bytes refuses after the candidate and before any read.
						cold()
						bctx, bmeter := storepkg.WithReadBudget(ctx, sbBudget("buffer bytes", 7))
						if got, err := run(bctx); got != 0 || !isBudget(err, "buffer bytes") || bmeter.Work().DecodedBytes != 0 || bmeter.Work().HistoryExamined != 0 {
							t.Fatalf("buffer bytes 7 = %d, %v, work %+v; want a refusal and nothing read", got, err, bmeter.Work())
						}
						for _, limit := range []string{"history examined", "decoded bytes"} {
							if sbWorkOf(total, limit) <= 1 {
								continue
							}
							cold()
							pctx, meter := storepkg.WithReadBudget(ctx, sbBudget(limit, 1))
							got, err := run(pctx)
							rb := rbdBudgetErr(t, err, limit)
							if got != 0 || rb.Door != "RelLowestAnsweredPin" || meter.Work() != rb.Work || limit == "history examined" && meter.Work().HistoryExamined > 2 {
								t.Fatalf("%s 1: %d, door %q, work %+v, meter %+v; want no answer from RelLowestAnsweredPin and the meter's work", limit, got, rb.Door, rb.Work, meter.Work())
							}
							cold()
							xctx, _ := storepkg.WithReadBudget(ctx, sbBudget(limit, sbWorkOf(total, limit)))
							if got, err := run(xctx); err != nil || got != want {
								t.Fatalf("%s %d (the unlimited count) = %d, %v; want %d", limit, sbWorkOf(total, limit), got, err, want)
							}
							cold()
							xctx, _ = storepkg.WithReadBudget(ctx, sbBudget(limit, sbWorkOf(total, limit)-1))
							if got, err := run(xctx); got != 0 || !isBudget(err, limit) {
								t.Fatalf("%s %d (one under) = %d, %v; want a refusal and no answer", limit, sbWorkOf(total, limit)-1, got, err)
							}
						}
						// One meter over two calls: Candidates 1 refuses the second before it reads.
						cold()
						cctx, cmeter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 1})
						if _, err := run(cctx); err != nil {
							t.Fatalf("first call under Candidates 1: %v", err)
						}
						before := cmeter.Work()
						if got, err := run(cctx); got != 0 || !isBudget(err, "candidates") || cmeter.Work().DecodedBytes != before.DecodedBytes || cmeter.Work().HistoryExamined != before.HistoryExamined {
							t.Fatalf("second call under Candidates 1 = %d, %v, work %+v (after the first %+v); want a refusal before any read", got, err, cmeter.Work(), before)
						}
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

// r2BuildTiered is sbBuildTiered with its relationships in the event shards:
// each shard gets its own two endpoint nodes of an event label, so the
// relationships between them are placed with them (sbBuildTiered's start node
// is a reference node, which keeps its relationships on the always-open
// reference shard). Three shards, two relationships each (rels[0], [2], [4]
// live, rels[1], [3], [5] deleted), all shards demoted to cold; nodes is empty.
func r2BuildTiered(t *testing.T) *sbTieredWorld {
	t.Helper()
	ts, err := tiered.New(tiered.Config{
		DataDir:           t.TempDir(),
		RefLabels:         []string{"Ref"},
		ShardWindow:       7 * 24 * time.Hour,
		FlushInterval:     1<<63 - 1,
		MaxOpenColdShards: 8,
	})
	if err != nil {
		t.Fatalf("tiered.New: %v", err)
	}
	g, err := New(Config{Store: ts, UntimedValidStart: UntimedFromTxFrom})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close(); _ = ts.Close() })
	ctx := context.Background()
	w := &sbTieredWorld{g: g, ts: ts}
	const shards = 3
	for i := range shards {
		s, err := g.Nodes.Add(ctx, []string{"Ev"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		e, err := g.Nodes.Add(ctx, []string{"Ev"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		for j := range 2 {
			r, err := g.Rels.AddByID(ctx, sbType, s.ID(), e.ID(), map[string]any{"i": int64(i*2 + j)})
			if err != nil {
				t.Fatal(err)
			}
			for v := range 2 {
				if _, err := g.Rels.Update(ctx, r.ID(), map[string]any{"v": int64(v)}); err != nil {
					t.Fatal(err)
				}
			}
			if j == 1 { // history only
				if i == 0 {
					if w.preDelete, err = g.Temporal.NowTx(); err != nil {
						t.Fatal(err)
					}
				}
				if err := g.Rels.Delete(ctx, r.ID()); err != nil {
					t.Fatal(err)
				}
			}
			w.rels = append(w.rels, r.ID())
		}
		time.Sleep(2 * time.Millisecond)
		if err := ts.RotateHotShard(); err != nil {
			t.Fatal(err)
		}
	}
	if w.pin, err = g.Temporal.NowTx(); err != nil {
		t.Fatal(err)
	}
	ts.MuForTest().RLock()
	hot := ts.HotShardForTest()
	for _, es := range ts.EventShardsForTest() {
		if es != hot {
			w.cold = append(w.cold, es)
		}
	}
	ts.MuForTest().RUnlock()
	if len(w.cold) < shards {
		t.Fatalf("%d cold shards, want %d", len(w.cold), shards)
	}
	for _, es := range w.cold {
		demoteToCold(ts, es.Name())
	}
	return w
}

// r2DoorCase is one relationship door of the shard-open and gate tests: the plain
// door and the Ctx door over the same list of calls, each rendering what it
// answered (a refused exact-or-refused door renders "").
type r2DoorCase struct {
	name, door string
	plain      func() (string, error)
	ctx        func(ctx context.Context) (string, error)
}

// r2RelDoors lists the relationship doors over the IDs given (one call per ID
// for the point doors, one call for the by-ID doors), at the pins given.
func r2RelDoors(g *Core, ids []types.RelID, pin types.Instant) []r2DoorCase {
	each := func(read func(ctx context.Context, id types.RelID) (string, error)) func(ctx context.Context) (string, error) {
		return func(ctx context.Context) (string, error) {
			var out strings.Builder
			for _, id := range ids {
				row, err := read(ctx, id)
				if err != nil {
					return out.String(), err
				}
				out.WriteString(row + "\n")
			}
			return out.String(), nil
		}
	}
	rel := func(r *types.Relationship, err error) (string, error) { return r2RelResult(r, nil), err }
	plainOf := func(read func(id types.RelID) (*types.Relationship, error)) func() (string, error) {
		return func() (string, error) {
			return each(func(_ context.Context, id types.RelID) (string, error) { return rel(read(id)) })(context.Background())
		}
	}
	return []r2DoorCase{
		{"Get", "Get", plainOf(func(id types.RelID) (*types.Relationship, error) { return g.Rels.Get(context.Background(), id) }),
			each(func(ctx context.Context, id types.RelID) (string, error) { return rel(g.Rels.GetCtx(ctx, id)) })},
		{"RelAtTx", "RelAtTx", plainOf(func(id types.RelID) (*types.Relationship, error) { return g.Temporal.RelAtTx(id, pin, pin) }),
			each(func(ctx context.Context, id types.RelID) (string, error) {
				return rel(g.Temporal.RelAtTxCtx(ctx, id, pin, pin))
			})},
		{"RelAt", "RelAt", plainOf(func(id types.RelID) (*types.Relationship, error) { return g.Temporal.RelAt(id, pin) }),
			each(func(ctx context.Context, id types.RelID) (string, error) {
				return rel(g.Temporal.RelAtCtx(ctx, id, pin))
			})},
		{"RelsAtTxByIDs", "RelsAtTxByIDs", func() (string, error) {
			rs, err := g.Temporal.RelsAtTxByIDs(ids, pin, pin)
			return r2Rows(rs), err
		}, func(ctx context.Context) (string, error) {
			rs, err := g.Temporal.RelsAtTxByIDsCtx(ctx, ids, pin, pin)
			if rs == nil {
				return "", err
			}
			return r2Rows(rs), err
		}},
	}
}

// TestReadBudget_R2Twins_RelShardOpens: on a tiered store whose cold shards are
// closed before each read, three relationships live in three cold shards: the
// plain reads open the three shards; under ShardOpens 1 each Ctx door opens one
// and refuses (the point doors, three calls on one meter, after the first row;
// RelsAtTxByIDs with no row; RelsEffectiveByIDs after the callbacks of the
// entities read before it); unlimited the answer is the plain door's and the
// meter counts exactly the opens.
func TestReadBudget_R2Twins_RelShardOpens(t *testing.T) {
	t.Parallel()
	w := r2BuildTiered(t)
	ids := []types.RelID{w.rels[0], w.rels[2], w.rels[4]} // one live relationship per shard
	ctx := context.Background()
	for _, d := range r2RelDoors(w.g, ids, w.pin) {
		t.Run(d.name, func(t *testing.T) {
			w.closeCold(t)
			opens := w.opens()
			want, err := d.plain()
			plainOpens := w.opens() - opens
			if err != nil || strings.Contains(want, "nil") || plainOpens < 3 {
				t.Fatalf("plain = %q, %v, %d shards opened; want the rows and 3 opens", want, err, plainOpens)
			}
			w.closeCold(t)
			opens = w.opens()
			bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{ShardOpens: 1})
			got, err := d.ctx(bctx)
			boundedOpened := w.opens() - opens
			rb := rbdBudgetErr(t, err, "shard opens")
			if rb.Door != d.door || meter.Work() != rb.Work || boundedOpened > 1 || got == want || !strings.HasPrefix(want, got) {
				t.Fatalf("ShardOpens 1 = %q, door %q, work %+v, %d shards opened; want a prefix of the answer, a refusal from %s and at most 1 open", got, rb.Door, meter.Work(), boundedOpened, d.door)
			}
			w.closeCold(t)
			opens = w.opens()
			bctx, meter = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
			got, err = d.ctx(bctx)
			opened := w.opens() - opens
			if err != nil || got != want || meter.Work().ShardOpens != opened || opened != plainOpens {
				t.Fatalf("unlimited = %q, %v, %d shards opened, %d counted; want the plain rows and every open (%d) counted", got, err, opened, meter.Work().ShardOpens, plainOpens)
			}
			t.Logf("%s: the plain read opened %d cold shards; under ShardOpens 1 the read opened %d and refused; unlimited it opened %d and counted %d", d.name, plainOpens, boundedOpened, opened, meter.Work().ShardOpens)
		})
	}

	t.Run("RelsEffectiveByIDs", func(t *testing.T) {
		stream := func(ctx context.Context, ctxDoor bool) ([]string, error) {
			return sbCollect(func(fn func(string) bool) error {
				f := func(s temporal.RelSegment) bool { return fn(sbRelSeg(s)) }
				if ctxDoor {
					return w.g.Temporal.RelsEffectiveByIDsCtx(ctx, ids, w.pin, f)
				}
				return w.g.Temporal.RelsEffectiveByIDs(ids, w.pin, f)
			})
		}
		w.closeCold(t)
		opens := w.opens()
		want, err := stream(ctx, false)
		plainOpens := w.opens() - opens
		if err != nil || len(want) == 0 || plainOpens < 3 {
			t.Fatalf("plain = %d segments, %v, %d shards opened; want a stream and at least 3 opens", len(want), err, plainOpens)
		}
		w.closeCold(t)
		opens = w.opens()
		bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{ShardOpens: 1})
		got, err := stream(bctx, true)
		boundedOpened, boundedCalls := w.opens()-opens, len(got)
		rb := rbdBudgetErr(t, err, "shard opens")
		if rb.Door != "RelsEffectiveByIDs" || !sbPrefix(got, want) || len(got) == len(want) || boundedOpened > 1 || meter.Work().ShardOpens < 2 {
			t.Fatalf("ShardOpens 1: door %q, %d of %d callbacks, %d shards opened, work %+v; want a prefix of the stream, the refusal and at most 1 open", rb.Door, len(got), len(want), boundedOpened, meter.Work())
		}
		w.closeCold(t)
		opens = w.opens()
		bctx, meter = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
		got, err = stream(bctx, true)
		opened := w.opens() - opens
		if err != nil || !slices.Equal(got, want) || meter.Work().ShardOpens != opened || opened != plainOpens {
			t.Fatalf("unlimited = %d segments, %v, %d shards opened, %d counted; want the plain stream and every open (%d) counted", len(got), err, opened, meter.Work().ShardOpens, plainOpens)
		}
		t.Logf("RelsEffectiveByIDs: the plain stream opened %d cold shards; under ShardOpens 1 it opened %d and refused after %d of %d callbacks; unlimited it opened %d and counted %d", plainOpens, boundedOpened, boundedCalls, len(want), opened, meter.Work().ShardOpens)
	})
}

// TestReadBudget_R2Twins_RelLowestAnsweredPinShardOpens: with a retention
// watermark the door reads each relationship's current row for its type, so on a
// tiered store with its cold shards closed the plain reads of three relationships
// in three shards open three; under ShardOpens 1 the Ctx door opens one and
// refuses on the second call; unlimited it counts exactly the opens.
func TestReadBudget_R2Twins_RelLowestAnsweredPinShardOpens(t *testing.T) {
	t.Parallel()
	w := r2BuildTiered(t)
	scRetain(t, w.g)
	ids := []types.RelID{w.rels[0], w.rels[2], w.rels[4]}
	ctx := context.Background()
	readAll := func(ctx context.Context, plain bool) (string, error) {
		var out []string
		for _, id := range ids {
			var pin types.Instant
			var err error
			if plain {
				pin, err = w.g.Temporal.RelLowestAnsweredPin(id)
			} else {
				pin, err = w.g.Temporal.RelLowestAnsweredPinCtx(ctx, id)
			}
			if err != nil {
				return strings.Join(out, ","), err
			}
			out = append(out, fmt.Sprint(pin))
		}
		return strings.Join(out, ","), nil
	}
	w.closeCold(t)
	opens := w.opens()
	want, err := readAll(ctx, true)
	plainOpens := w.opens() - opens
	if err != nil || plainOpens < 3 {
		t.Fatalf("plain = %q, %v, %d shards opened; want the pins and 3 opens", want, err, plainOpens)
	}
	w.closeCold(t)
	opens = w.opens()
	bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{ShardOpens: 1})
	got, err := readAll(bctx, false)
	boundedOpened := w.opens() - opens
	rb := rbdBudgetErr(t, err, "shard opens")
	if rb.Door != "RelLowestAnsweredPin" || meter.Work() != rb.Work || boundedOpened != 1 || !strings.HasPrefix(want, got) || got == want {
		t.Fatalf("ShardOpens 1 = %q, door %q, work %+v, %d shards opened; want the first pin, then a refusal with 1 open", got, rb.Door, meter.Work(), boundedOpened)
	}
	w.closeCold(t)
	opens = w.opens()
	bctx, meter = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
	got, err = readAll(bctx, false)
	opened := w.opens() - opens
	if err != nil || got != want || meter.Work().ShardOpens != opened || opened != plainOpens {
		t.Fatalf("unlimited = %q, %v, %d shards opened, %d counted; want the plain pins and every open (%d) counted", got, err, opened, meter.Work().ShardOpens, plainOpens)
	}
	t.Logf("three relationships in three cold shards: the plain reads opened %d; under ShardOpens 1 the reads opened %d and refused the second; unlimited they opened %d and counted %d", plainOpens, boundedOpened, opened, meter.Work().ShardOpens)
}
