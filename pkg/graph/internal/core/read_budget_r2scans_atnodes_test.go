package core

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// R2-SCANS: the by-endpoint scans (ForEachRelEffectiveAtNodes, its Window form
// and the typed column scan) under a read budget. Counts only. Notes and red
// output: tasks/evidence/R2SCANS/red.txt.

// r2AtNodesDoors are the three by-endpoint doors seeded from w's start node
// (every relationship of the world starts there), each rendering what it hands
// out as strings: the segments, or the rows of the column batches.
func r2AtNodesDoors(g *Core, w interface{ seed() types.NodeID }, pin types.Instant) []sbScan {
	seeds := []types.NodeID{w.seed()}
	window := temporal.Window{{From: pin, To: math.MaxInt64}}
	rf := func(fn func(string) bool) func(temporal.RelSegment) bool {
		return func(s temporal.RelSegment) bool { return fn(sbRelSeg(s)) }
	}
	rows := func(fn func(string) bool) func(*storepkg.RelColumnBatch) bool {
		return func(b *storepkg.RelColumnBatch) bool {
			for i := range b.IDs {
				if !fn(fmt.Sprintf("r%d[%d,%d)", b.IDs[i], b.ValidFrom[i], b.ValidTo[i])) {
					return false
				}
			}
			return true
		}
	}
	types1 := []string{sbType}
	return []sbScan{
		{"at nodes", "ForEachRelEffectiveAtNodes", false, false,
			func(fn func(string) bool) error {
				return g.Temporal.ForEachRelEffectiveAtNodes(seeds, temporal.Outgoing, types1, pin, rf(fn))
			},
			func(ctx context.Context, fn func(string) bool) error {
				return g.Temporal.ForEachRelEffectiveAtNodesCtx(ctx, seeds, temporal.Outgoing, types1, pin, rf(fn))
			}},
		{"at nodes window", "ForEachRelEffectiveAtNodesWindow", false, false,
			func(fn func(string) bool) error {
				return g.Temporal.ForEachRelEffectiveAtNodesWindow(seeds, temporal.Outgoing, types1, pin, window, rf(fn))
			},
			func(ctx context.Context, fn func(string) bool) error {
				return g.Temporal.ForEachRelEffectiveAtNodesWindowCtx(ctx, seeds, temporal.Outgoing, types1, pin, window, rf(fn))
			}},
		{"columns at nodes", "ScanRelEffectiveColumnsAtNodes", false, false,
			func(fn func(string) bool) error {
				return g.Temporal.ScanRelEffectiveColumnsAtNodes(seeds, temporal.Outgoing, types1, pin, window, []string{"i"}, rows(fn))
			},
			func(ctx context.Context, fn func(string) bool) error {
				return g.Temporal.ScanRelEffectiveColumnsAtNodesCtx(ctx, seeds, temporal.Outgoing, types1, pin, window, []string{"i"}, rows(fn))
			}},
	}
}

func (w *sbWorld) seed() types.NodeID       { return w.start }
func (w *sbTieredWorld) seed() types.NodeID { return w.start }

// TestR2Scans_AtNodes: the by-endpoint scans under a budget, on badger, sharded
// and tiered, rows pending and flushed:
//   - without a budget and under an unlimited one a scan hands out the plain
//     door's segments in its order; the unlimited meter counts one candidate for
//     the seed, one for each entry of its current adjacency (the 9 relationships
//     alive) and of its ex-adjacency (the 3 deleted) as the gather produces them,
//     and one for each relationship read (the 12 the gather lists), the segments
//     handed out, and 8 buffer bytes per listed seed and relationship (the column
//     scan also 24 per segment it holds);
//   - a Candidates limit inside the gather refuses before any relationship is
//     read (no callback, no history looked at); one past the gather refuses at
//     the first read; one inside the reads hands out the entities read before it;
//   - the count is exact: a limit at it answers, one under it refuses;
//   - Segments, HistoryExamined, DecodedBytes and BufferBytes limits refuse with
//     the callbacks a prefix of the plain stream, and a BufferBytes limit of 1
//     before anything is counted (the column scan makes no callback before it has
//     every row: it is exact or refused).
func TestR2Scans_AtNodes(t *testing.T) {
	t.Parallel()
	const n = 12
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			g, shut := be.open(t, dir)
			w := sbBuild(t, g, n)
			probe := func(t *testing.T, g *Core, flushed bool) {
				w.g = g
				for _, d := range r2AtNodesDoors(g, w, w.pin) {
					t.Run(d.name, func(t *testing.T) {
						ctx := context.Background()
						columns := d.door == "ScanRelEffectiveColumnsAtNodes"
						cold := func() {
							if flushed {
								w.evict(t)
							}
						}
						cold()
						want, err := sbCollect(d.plain)
						if err != nil || len(want) == 0 {
							t.Fatalf("plain scan = %d rows, %v", len(want), err)
						}
						cold()
						if got, err := sbCollect(func(fn func(string) bool) error { return d.ctx(ctx, fn) }); err != nil || !slices.Equal(got, want) {
							t.Fatalf("no budget = %v, %v; want the plain stream\n%v", got, err, want)
						}
						cold()
						fctx, full := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
						if got, err := sbCollect(func(fn func(string) bool) error { return d.ctx(fctx, fn) }); err != nil || !slices.Equal(got, want) {
							t.Fatalf("unlimited = %v, %v; want the plain stream\n%v", got, err, want)
						}
						total := full.Work()
						live, gone := uint64(n-len(w.deletedRels)), uint64(len(w.deletedRels))
						gather := 1 + live + gone // the seed, its current adjacency, its ex-adjacency
						wantBuf := uint64(8 * (1 + n))
						if columns {
							wantBuf += 24 * uint64(len(want))
						}
						if total.Candidates != gather+n || total.Segments != uint64(len(want)) || total.BufferBytes != wantBuf { // #nosec G115 -- a count
							t.Fatalf("unlimited work %+v; want %d candidates (%d in the gather, %d reads), %d segments, %d buffer bytes", total, gather+n, gather, n, len(want), wantBuf)
						}
						if flushed && (total.DecodedBytes == 0 || total.HistoryExamined == 0) {
							t.Fatalf("flushed scan counted %+v; want decoded bytes and history examined", total)
						}

						refused := func(t *testing.T, b storepkg.ReadBudget, limit string) ([]string, storepkg.ReadWork, *storepkg.ReadBudgetError) {
							t.Helper()
							cold()
							pctx, meter := storepkg.WithReadBudget(ctx, b)
							got, err := sbCollect(func(fn func(string) bool) error { return d.ctx(pctx, fn) })
							rb := rbdBudgetErr(t, err, limit)
							work := meter.Work()
							if rb.Door != d.door || sbWorkOf(work, limit) != sbWorkOf(rb.Work, limit) || !sbPrefix(got, want) {
								t.Fatalf("%s refused by %q with work %+v, meter %+v, callbacks %v; want door %q, the refused counter equal and a prefix of the plain stream", limit, rb.Door, rb.Work, work, got, d.door)
							}
							return got, work, rb
						}

						// A limit inside the gather refuses before any relationship is read.
						got, work, _ := refused(t, storepkg.ReadBudget{Candidates: 5}, "candidates")
						if len(got) != 0 || work.HistoryExamined != 0 || work.Segments != 0 || work.Candidates > gather {
							t.Fatalf("Candidates 5 (inside the gather of %d): %d callbacks, work %+v; want none, no history looked at and at most %d candidates counted", gather, len(got), work, gather)
						}
						// One past the gather: the first read is refused.
						got, work, _ = refused(t, storepkg.ReadBudget{Candidates: gather}, "candidates")
						if len(got) != 0 || work.Candidates != gather+1 || work.HistoryExamined != 0 {
							t.Fatalf("Candidates %d (the whole gather): %d callbacks, work %+v; want none and the first read refused (%d candidates)", gather, len(got), work, gather+1)
						}
						if !columns {
							// Inside the reads: the entities read before the refusal are handed out.
							got, work, _ = refused(t, storepkg.ReadBudget{Candidates: gather + 4}, "candidates")
							if work.Candidates != gather+5 || len(got) == 0 {
								t.Fatalf("Candidates %d (four reads): %d callbacks, work %+v; want a prefix and %d candidates counted", gather+4, len(got), work, gather+5)
							}
						}
						// Exact at the count.
						cold()
						ectx, _ := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: total.Candidates})
						if got, err := sbCollect(func(fn func(string) bool) error { return d.ctx(ectx, fn) }); err != nil || !slices.Equal(got, want) {
							t.Fatalf("Candidates %d (the unlimited count) = %v, %v; want the plain stream", total.Candidates, got, err)
						}
						// (The last relationship read is a deleted one: the window doors hand out
						// nothing for it, so only the counted work shows the refusal there.)
						got, work, _ = refused(t, storepkg.ReadBudget{Candidates: total.Candidates - 1}, "candidates")
						if work.Candidates != total.Candidates || len(got) > len(want) {
							t.Fatalf("Candidates %d (one under) = %d callbacks, work %+v; want a refusal at the last read", total.Candidates-1, len(got), work)
						}

						// The other limits.
						got, work, _ = refused(t, storepkg.ReadBudget{BufferBytes: 1}, "buffer bytes")
						if len(got) != 0 || work.Candidates != 0 {
							t.Fatalf("BufferBytes 1: %d callbacks, work %+v; want none and nothing counted (the seed list passes the limit first)", len(got), work)
						}
						got, work, _ = refused(t, storepkg.ReadBudget{BufferBytes: 8 + 8*uint64(n) - 1}, "buffer bytes")
						if len(got) != 0 || work.Candidates != gather {
							t.Fatalf("BufferBytes one under the lists: %d callbacks, work %+v; want none, the gather counted (%d candidates) and no relationship read", len(got), work, gather)
						}
						got, work, _ = refused(t, storepkg.ReadBudget{HistoryExamined: 1}, "history examined")
						if len(got) != 0 || work.Candidates > gather+2 {
							t.Fatalf("HistoryExamined 1: %d callbacks, work %+v; want none and at most 2 relationships read", len(got), work)
						}
						if flushed {
							got, work, _ = refused(t, storepkg.ReadBudget{DecodedBytes: 1}, "decoded bytes")
							if len(got) != 0 || work.Candidates > gather+2 {
								t.Fatalf("DecodedBytes 1: %d callbacks, work %+v; want none and at most 2 relationships read after the gather", len(got), work)
							}
						}
						if !columns {
							got, work, _ = refused(t, storepkg.ReadBudget{Segments: 3}, "segments")
							if len(got) != 3 || work.Segments != 4 {
								t.Fatalf("Segments 3: %d callbacks, work %+v; want 3 callbacks and 4 segments counted", len(got), work)
							}
						} else {
							got, work, _ = refused(t, storepkg.ReadBudget{Segments: 3}, "segments")
							if len(got) != 0 || work.Segments != 4 {
								t.Fatalf("Segments 3: %d callbacks, work %+v; want no batch (the scan holds every row first) and 4 segments counted", len(got), work)
							}
							got, work, _ = refused(t, storepkg.ReadBudget{BufferBytes: wantBuf - 1}, "buffer bytes")
							if len(got) != 0 || work.BufferBytes != wantBuf {
								t.Fatalf("BufferBytes one under: %d callbacks, work %+v; want no batch and a refusal at the last held row", len(got), work)
							}
						}
						t.Logf("%s: unlimited %+v", d.name, total)
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

// TestR2Scans_AtNodesGates: a cancelled context is refused before any read, a
// nil callback before any charge, an empty seed list answers nothing with
// nothing counted, and a stopping callback is not a refusal.
func TestR2Scans_AtNodesGates(t *testing.T) {
	t.Parallel()
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			g, shut := be.open(t, t.TempDir())
			defer shut()
			w := sbBuild(t, g, 6)
			for _, d := range r2AtNodesDoors(g, w, w.pin) {
				cctx, cancel := context.WithCancel(context.Background())
				cancel()
				calls := 0
				if err := d.ctx(cctx, func(string) bool { calls++; return true }); !errors.Is(err, context.Canceled) || calls != 0 {
					t.Fatalf("%s: cancelled ctx = %v after %d callbacks; want context.Canceled and none", d.name, err, calls)
				}
				bctx, meter := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{Candidates: 100})
				var err error
				seeds := []types.NodeID{w.start}
				window := temporal.Window{{From: w.pin, To: math.MaxInt64}}
				switch d.door {
				case "ForEachRelEffectiveAtNodes":
					err = g.Temporal.ForEachRelEffectiveAtNodesCtx(bctx, seeds, temporal.Outgoing, nil, w.pin, nil)
				case "ForEachRelEffectiveAtNodesWindow":
					err = g.Temporal.ForEachRelEffectiveAtNodesWindowCtx(bctx, seeds, temporal.Outgoing, nil, w.pin, window, nil)
				default:
					err = g.Temporal.ScanRelEffectiveColumnsAtNodesCtx(bctx, seeds, temporal.Outgoing, nil, w.pin, window, nil, nil)
				}
				if !errors.Is(err, grapherr.ErrNilCallback) || meter.Work() != (storepkg.ReadWork{}) {
					t.Fatalf("%s: nil callback = %v, work %+v; want ErrNilCallback and nothing counted", d.name, err, meter.Work())
				}
				// A direction nobody has, before any charge.
				ictx, imeter := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{Candidates: 100})
				if err := g.Temporal.ForEachRelEffectiveAtNodesCtx(ictx, seeds, temporal.Direction(9), nil, w.pin, func(temporal.RelSegment) bool { return true }); !errors.Is(err, ErrInvalidDirection) || imeter.Work() != (storepkg.ReadWork{}) {
					t.Fatalf("%s: invalid direction = %v, work %+v; want ErrInvalidDirection and nothing counted", d.name, err, imeter.Work())
				}
				// No seed: no relationship, nothing counted.
				ectx, emeter := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{Candidates: 1})
				if err := g.Temporal.ForEachRelEffectiveAtNodesCtx(ectx, nil, temporal.Outgoing, nil, w.pin, func(temporal.RelSegment) bool { return true }); err != nil || emeter.Work() != (storepkg.ReadWork{}) {
					t.Fatalf("%s: no seed = %v, work %+v; want nothing", d.name, err, emeter.Work())
				}
				// fn stopping the scan is the caller's choice.
				sctx, smeter := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{Candidates: 100})
				calls = 0
				if err := d.ctx(sctx, func(string) bool { calls++; return false }); err != nil || calls != 1 || smeter.Work().Candidates == 0 {
					t.Fatalf("%s: a stopping callback = %v after %d calls, work %+v; want no error and one call", d.name, err, calls, smeter.Work())
				}
			}
		})
	}
}

// TestR2Scans_AtNodesWithoutExAdjacency: a store whose ex-adjacency does not
// cover every relationship (the graph then folds the history IDs of every
// relationship, c.exAdj nil) counts each ID the fold produces as a candidate,
// before it is collected, and a Candidates limit stops the fold at that ID; on a
// store that opens shards the fold cannot count its opens, so a ShardOpens
// budget refuses it before the walk (Limit "unsupported") with no shard opened.
func TestR2Scans_AtNodesWithoutExAdjacency(t *testing.T) {
	t.Parallel()
	const n = 12
	for _, be := range rbdBackends() {
		if be.name == "tiered" {
			continue // below: cold shards
		}
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			g, shut := be.open(t, t.TempDir())
			defer shut()
			w := sbBuild(t, g, n)
			ctx := context.Background()
			d := r2AtNodesDoors(g, w, w.pin)[0]
			want, err := sbCollect(d.plain)
			if err != nil || len(want) == 0 {
				t.Fatalf("plain scan = %d rows, %v", len(want), err)
			}
			g.exAdj = nil // the fold of every relationship with history
			bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
			got, err := sbCollect(func(fn func(string) bool) error { return d.ctx(bctx, fn) })
			if err != nil || !slices.Equal(got, want) {
				t.Fatalf("unlimited = %v, %v; want the plain stream\n%v", got, err, want)
			}
			// The seed, its 9 live relationships, the 12 relationships the fold
			// visits (every one has history), then the 12 reads.
			if c := meter.Work().Candidates; c != 1+9+n+n {
				t.Fatalf("unlimited counted %d candidates; want %d (seed, adjacency, the fold's 12 IDs, 12 reads)", c, 1+9+n+n)
			}
			// A limit inside the fold stops it at that ID: no relationship read.
			bctx, meter = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 1 + 9 + 3})
			got, err = sbCollect(func(fn func(string) bool) error { return d.ctx(bctx, fn) })
			rb := rbdBudgetErr(t, err, "candidates")
			if rb.Door != d.door || len(got) != 0 || meter.Work().Candidates != 1+9+3+1 || meter.Work().HistoryExamined != 0 {
				t.Fatalf("Candidates 13 inside the fold = %d callbacks, work %+v; want none, the fold stopped at its fourth ID (14 counted) and nothing read", len(got), meter.Work())
			}
		})
	}
	t.Run("tiered", func(t *testing.T) {
		w := sbBuildTiered(t)
		d := r2AtNodesDoors(w.g, w, w.pin)[0]
		w.g.exAdj = nil
		w.closeCold(t)
		opens := w.opens()
		bctx, meter := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{ShardOpens: 100})
		got, err := sbCollect(func(fn func(string) bool) error { return d.ctx(bctx, fn) })
		rb := rbdBudgetErr(t, err, "unsupported")
		if rb.Door != d.door || len(got) != 0 || w.opens() != opens || meter.Work().ShardOpens != 0 {
			t.Fatalf("a ShardOpens budget over the fold = %d callbacks, %d shards opened, work %+v; want a refusal \"unsupported\" before any shard opened", len(got), w.opens()-opens, meter.Work())
		}
	})
}

// TestR2Scans_AtNodesShardOpens: on a tiered store whose cold shards are
// closed before each read, the plain scan opens the cold shards its gather (the
// seed's ex-adjacency lives on the shard of its mint time) and its reads reach
// (2 of the 3);
// under ShardOpens 1 the scan opens at most one and refuses with "shard opens"
// and no callback after it; unlimited it streams the plain stream and the meter
// counts exactly the shards opened.
func TestR2Scans_AtNodesShardOpens(t *testing.T) {
	t.Parallel()
	w := sbBuildTiered(t)
	ctx := context.Background()
	for _, d := range r2AtNodesDoors(w.g, w, w.pin) {
		t.Run(d.name, func(t *testing.T) {
			w.closeCold(t)
			opens := w.opens()
			want, err := sbCollect(d.plain)
			plainOpens := w.opens() - opens
			// The relationships live where their start node does (the reference shard),
			// so the scan opens the cold shards of the seed's mint time (its
			// ex-adjacency) and of the history it reads, not every cold shard.
			if err != nil || len(want) == 0 || plainOpens < 2 {
				t.Fatalf("plain scan = %d rows, %v, %d cold shards opened; want a stream and at least 2 opens", len(want), err, plainOpens)
			}

			w.closeCold(t)
			opens = w.opens()
			bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{ShardOpens: 1})
			got, err := sbCollect(func(fn func(string) bool) error { return d.ctx(bctx, fn) })
			boundedOpened, boundedCalls := w.opens()-opens, len(got)
			rb := rbdBudgetErr(t, err, "shard opens")
			// (The segments of the entities read before the refusal are handed out, and
			// counted, after it: only the refused counter is the error's and the meter's.)
			if rb.Door != d.door || meter.Work().ShardOpens != rb.Work.ShardOpens || !sbPrefix(got, want) || boundedOpened > 1 {
				t.Fatalf("ShardOpens 1: door %q, work %+v, meter %+v, %d callbacks, %d shards opened; want %q, the refused counter equal, a prefix and at most 1 open", rb.Door, rb.Work, meter.Work(), len(got), boundedOpened, d.door)
			}

			w.closeCold(t)
			opens = w.opens()
			bctx, meter = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
			got, err = sbCollect(func(fn func(string) bool) error { return d.ctx(bctx, fn) })
			opened := w.opens() - opens
			if err != nil || !slices.Equal(got, want) || opened != plainOpens || meter.Work().ShardOpens != opened {
				t.Fatalf("unlimited = %d rows, %v, %d shards opened, %d counted; want the plain stream and every open counted", len(got), err, opened, meter.Work().ShardOpens)
			}
			t.Logf("%s: the plain scan opened %d cold shards; under ShardOpens 1 it opened %d and refused after %d callbacks; unlimited it opened %d and counted %d", d.name, plainOpens, boundedOpened, boundedCalls, opened, meter.Work().ShardOpens)
		})
	}
}
