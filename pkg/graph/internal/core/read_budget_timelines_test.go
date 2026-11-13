package core

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// sbOne is one per-entity door, plain and under a context: both return the
// rendered answer and how many segments it holds (a nil answer renders "").
type sbOne struct {
	name, door string
	segSize    int // the segment struct's size, what BufferBytes charges per segment
	plain      func() (string, int, error)
	ctx        func(ctx context.Context) (string, int, error)
}

func sbOneDoors(g *Core, node types.NodeID, rel types.RelID, pin, from types.Instant, pins []types.Instant) []sbOne {
	nodeSize := int(reflect.TypeFor[temporal.NodeSegment]().Size()) // #nosec G115 -- a struct size
	relSize := int(reflect.TypeFor[temporal.RelSegment]().Size())   // #nosec G115 -- a struct size
	one := func(segs []temporal.NodeSegment, err error) (string, int, error) {
		return sbSegs(segs), len(segs), err
	}
	oneRel := func(segs []temporal.RelSegment, err error) (string, int, error) {
		return sbRelSegs(segs), len(segs), err
	}
	many := func(all [][]temporal.NodeSegment, err error) (string, int, error) {
		var parts []string
		n := 0
		for _, segs := range all {
			parts = append(parts, sbSegs(segs))
			n += len(segs)
		}
		if all == nil {
			return "", 0, err
		}
		return strings.Join(parts, " | "), n, err
	}
	manyRel := func(all [][]temporal.RelSegment, err error) (string, int, error) {
		var parts []string
		n := 0
		for _, segs := range all {
			parts = append(parts, sbRelSegs(segs))
			n += len(segs)
		}
		if all == nil {
			return "", 0, err
		}
		return strings.Join(parts, " | "), n, err
	}
	return []sbOne{
		{"node timeline", "NodeEffectiveTimeline", nodeSize,
			func() (string, int, error) { return one(g.Temporal.NodeEffectiveTimeline(node, pin)) },
			func(ctx context.Context) (string, int, error) {
				return one(g.Temporal.NodeEffectiveTimelineCtx(ctx, node, pin))
			}},
		{"node timeline from", "NodeEffectiveTimelineFrom", nodeSize,
			func() (string, int, error) { return one(g.Temporal.NodeEffectiveTimelineFrom(node, from, pin)) },
			func(ctx context.Context) (string, int, error) {
				return one(g.Temporal.NodeEffectiveTimelineFromCtx(ctx, node, from, pin))
			}},
		{"node timelines", "NodeEffectiveTimelines", nodeSize,
			func() (string, int, error) { return many(g.Temporal.NodeEffectiveTimelines(node, pins)) },
			func(ctx context.Context) (string, int, error) {
				return many(g.Temporal.NodeEffectiveTimelinesCtx(ctx, node, pins))
			}},
		{"rel timeline", "RelEffectiveTimeline", relSize,
			func() (string, int, error) { return oneRel(g.Temporal.RelEffectiveTimeline(rel, pin)) },
			func(ctx context.Context) (string, int, error) {
				return oneRel(g.Temporal.RelEffectiveTimelineCtx(ctx, rel, pin))
			}},
		{"rel timelines", "RelEffectiveTimelines", relSize,
			func() (string, int, error) { return manyRel(g.Temporal.RelEffectiveTimelines(rel, pins)) },
			func(ctx context.Context) (string, int, error) {
				return manyRel(g.Temporal.RelEffectiveTimelinesCtx(ctx, rel, pins))
			}},
	}
}

// sbLongWorld is one node and one relationship with a long history.
type sbLongWorld struct {
	node types.NodeID
	rel  types.RelID
	pin  types.Instant   // after every update
	from types.Instant   // a valid start in the middle of the history
	pins []types.Instant // three pins, one repeated, one at 0 (nothing recorded)
}

const sbLongVersions = 24

func sbBuildLong(t *testing.T, g *Core) *sbLongWorld {
	t.Helper()
	ctx := context.Background()
	s, e := txbEndpoints(t, g)
	n, err := g.Nodes.Add(ctx, []string{"L"}, map[string]any{"pad": fmt.Sprintf("%0*d", 80, 0)})
	if err != nil {
		t.Fatal(err)
	}
	r, err := g.Rels.AddByID(ctx, "LONG", s, e, map[string]any{"pad": fmt.Sprintf("%0*d", 80, 0)})
	if err != nil {
		t.Fatal(err)
	}
	w := &sbLongWorld{node: n.ID(), rel: r.ID()}
	var mid types.Instant
	for v := range sbLongVersions {
		if v == sbLongVersions/2 {
			if mid, err = g.Temporal.NowTx(); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := g.Nodes.Update(ctx, n.ID(), map[string]any{"v": int64(v)}); err != nil {
			t.Fatal(err)
		}
		if _, err := g.Rels.Update(ctx, r.ID(), map[string]any{"v": int64(v)}); err != nil {
			t.Fatal(err)
		}
	}
	if w.pin, err = g.Temporal.NowTx(); err != nil {
		t.Fatal(err)
	}
	w.from = mid
	w.pins = []types.Instant{mid, w.pin, mid, 0}
	return w
}

// TestReadBudget_SigmaB_Timelines: the per-entity effective timelines under a
// budget (group B, step 2), on badger, sharded and tiered, rows pending and
// flushed, over a node and a relationship with 24 updates:
//   - without a budget, and under an unlimited one, the answer is the plain
//     door's; the unlimited meter counts one candidate, the history it looks
//     at, every returned segment and its size as buffer bytes;
//   - a limit of 1 on segments, history, decoded bytes or buffer bytes refuses
//     with *ReadBudgetError at that limit from that door and no segments, the
//     meter equal to the error's work, and the read stops there: the plain
//     read looks at every version, the refused one at most two (decoded bytes:
//     at most a quarter of the unlimited bytes);
//   - a limit of exactly the unlimited count answers as without one, one under
//     it refuses (segments and history).
func TestReadBudget_SigmaB_Timelines(t *testing.T) {
	t.Parallel()
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			g, shut := be.open(t, dir)
			lw := sbBuildLong(t, g)
			probe := func(t *testing.T, g *Core, flushed bool) {
				for _, d := range sbOneDoors(g, lw.node, lw.rel, lw.pin, lw.from, lw.pins) {
					t.Run(d.name, func(t *testing.T) {
						cold := func() {
							if flushed {
								rbdEvict(t, g, []types.NodeID{lw.node}, []types.RelID{lw.rel})
							}
						}
						ctx := context.Background()
						cold()
						want, segs, err := d.plain()
						if err != nil || segs < 2 {
							t.Fatalf("plain = %q (%d segments), %v; want an answer of several segments", want, segs, err)
						}
						cold()
						if got, _, err := d.ctx(ctx); err != nil || got != want {
							t.Fatalf("no budget = %q, %v; want the plain answer %q", got, err, want)
						}
						cold()
						fctx, full := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
						if got, _, err := d.ctx(fctx); err != nil || got != want {
							t.Fatalf("unlimited = %q, %v; want the plain answer %q", got, err, want)
						}
						total := full.Work()
						if total.Candidates != 1 || total.Segments != uint64(segs) || total.BufferBytes != uint64(segs*d.segSize) || total.HistoryExamined < sbLongVersions/2 { // #nosec G115 -- counts
							t.Fatalf("unlimited work %+v; want 1 candidate, %d segments, %d buffer bytes and the history looked at", total, segs, segs*d.segSize)
						}
						if flushed && total.DecodedBytes == 0 {
							t.Fatalf("flushed read decoded nothing: %+v", total)
						}
						for _, limit := range []string{"segments", "history examined", "decoded bytes", "buffer bytes"} {
							if sbWorkOf(total, limit) <= 1 {
								continue
							}
							t.Run(limit, func(t *testing.T) {
								cold()
								pctx, meter := storepkg.WithReadBudget(ctx, sbBudget(limit, 1))
								got, _, err := d.ctx(pctx)
								rb := rbdBudgetErr(t, err, limit)
								work := meter.Work()
								if got != "" || rb.Door != d.door || work != rb.Work {
									t.Fatalf("%s 1: answered %q, door %q, work %+v, meter %+v; want no segments, door %q and the meter's work", limit, got, rb.Door, rb.Work, work, d.door)
								}
								switch limit {
								case "history examined":
									if work.HistoryExamined > 2 {
										t.Fatalf("history 1: %d versions looked at before the refusal (unlimited %d)", work.HistoryExamined, total.HistoryExamined)
									}
								case "decoded bytes":
									if work.DecodedBytes > total.DecodedBytes/4 {
										t.Fatalf("decoded bytes 1: %d bytes decoded before the refusal (unlimited %d)", work.DecodedBytes, total.DecodedBytes)
									}
								case "segments":
									if work.Segments != 2 {
										t.Fatalf("segments 1: %d counted; want the second, which passes the limit", work.Segments)
									}
								}
								t.Logf("%s %s 1: refused after %+v (unlimited %+v)", d.name, limit, work, total)
							})
						}
						// A refusal past the skeleton, among the winners' decodes, stops
						// there too: no fallback walks the chain again after it.
						for _, limit := range []uint64{total.HistoryExamined / 2, total.HistoryExamined * 3 / 4, total.HistoryExamined - 1} {
							cold()
							xctx, meter := storepkg.WithReadBudget(ctx, sbBudget("history examined", limit))
							if got, _, err := d.ctx(xctx); got != "" || !isBudget(err, "history examined") || meter.Work().HistoryExamined > limit+2 {
								t.Fatalf("history %d of %d = %q, %v, %d looked at; want a refusal within %d", limit, total.HistoryExamined, got, err, meter.Work().HistoryExamined, limit+2)
							}
						}
						// Exactness: a limit of exactly the unlimited count answers, one
						// under it refuses.
						for _, limit := range []string{"segments", "history examined"} {
							cold()
							xctx, _ := storepkg.WithReadBudget(ctx, sbBudget(limit, sbWorkOf(total, limit)))
							if got, _, err := d.ctx(xctx); err != nil || got != want {
								t.Fatalf("%s %d (the unlimited count) = %q, %v; want the plain answer", limit, sbWorkOf(total, limit), got, err)
							}
							cold()
							xctx, _ = storepkg.WithReadBudget(ctx, sbBudget(limit, sbWorkOf(total, limit)-1))
							if got, _, err := d.ctx(xctx); got != "" || !isBudget(err, limit) {
								t.Fatalf("%s %d (one under) = %q, %v; want a refusal and no segments", limit, sbWorkOf(total, limit)-1, got, err)
							}
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

// isBudget reports whether err is a read budget refusal at limit.
func isBudget(err error, limit string) bool {
	var rb *storepkg.ReadBudgetError
	return errors.As(err, &rb) && rb.Limit == limit
}

// TestReadBudget_SigmaB_TimelineGates: a cancelled context is refused before
// any read, an invalid ID and a pin above the clock before anything is counted.
func TestReadBudget_SigmaB_TimelineGates(t *testing.T) {
	t.Parallel()
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			g, shut := be.open(t, t.TempDir())
			defer shut()
			lw := sbBuildLong(t, g)
			cctx, cancel := context.WithCancel(context.Background())
			cancel()
			for _, d := range sbOneDoors(g, lw.node, lw.rel, lw.pin, lw.from, lw.pins) {
				if _, _, err := d.ctx(cctx); !errors.Is(err, context.Canceled) {
					t.Fatalf("%s: cancelled ctx = %v; want context.Canceled", d.name, err)
				}
			}
			// A refusal of the call's own checks counts nothing.
			future := lw.pin + 1<<40
			bctx, meter := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{Candidates: 10})
			if _, err := g.Temporal.NodeEffectiveTimelineCtx(bctx, lw.node, future); err == nil || isBudget(err, "candidates") {
				t.Fatalf("a pin above the clock = %v; want ErrTxPinTooNew", err)
			}
			if _, err := g.Temporal.NodeEffectiveTimelineCtx(bctx, 0, lw.pin); err == nil || isBudget(err, "candidates") {
				t.Fatalf("an invalid ID = %v; want ErrInvalidStoreMutation", err)
			}
			if _, err := g.Temporal.RelEffectiveTimelinesCtx(bctx, lw.rel, []types.Instant{lw.pin, future}); err == nil {
				t.Fatal("a pin above the clock among the pins was answered")
			}
			if w := meter.Work(); w.Candidates != 0 {
				t.Fatalf("refused calls counted %+v; want nothing", w)
			}
		})
	}
}

// TestReadBudget_SigmaB_TimelineShardOpens: on a tiered store whose cold
// shards are closed before each read, the timeline of an entity live nowhere
// (a deleted one: its chain is searched on every shard) opens every cold shard
// without a budget; under ShardOpens 1 a Ctx door opens at most one and
// refuses with no segments; unlimited it answers as the plain door and counts
// exactly the shards it opened.
func TestReadBudget_SigmaB_TimelineShardOpens(t *testing.T) {
	t.Parallel()
	w := sbBuildTiered(t)
	gone, goneRel := w.nodes[1], w.rels[1] // deleted in the first shard
	pins := []types.Instant{w.pin, w.pin}
	ctx := context.Background()
	for _, d := range sbOneDoors(w.g, gone, goneRel, w.pin, 1, pins) {
		t.Run(d.name, func(t *testing.T) {
			w.closeCold(t)
			opens := w.opens()
			want, segs, err := d.plain()
			plainOpens := w.opens() - opens
			if err != nil || segs == 0 || plainOpens < 2 {
				t.Fatalf("plain = %q, %v, %d shards opened; want an answer and at least 2 opens", want, err, plainOpens)
			}

			w.closeCold(t)
			opens = w.opens()
			bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{ShardOpens: 1})
			got, _, err := d.ctx(bctx)
			boundedOpened := w.opens() - opens
			rb := rbdBudgetErr(t, err, "shard opens")
			if got != "" || rb.Door != d.door || meter.Work() != rb.Work || boundedOpened > 1 {
				t.Fatalf("ShardOpens 1 = %q, door %q, work %+v, %d shards opened; want no segments, door %q and at most 1 open", got, rb.Door, meter.Work(), boundedOpened, d.door)
			}

			w.closeCold(t)
			opens = w.opens()
			bctx, meter = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
			got, _, err = d.ctx(bctx)
			opened := w.opens() - opens
			if err != nil || got != want || meter.Work().ShardOpens != opened {
				t.Fatalf("unlimited = %q, %v, %d shards opened, %d counted; want the plain answer and every open counted", got, err, opened, meter.Work().ShardOpens)
			}
			t.Logf("%s: the plain read opened %d cold shards; under ShardOpens 1 the read opened %d and refused; unlimited it opened %d and counted %d", d.name, plainOpens, boundedOpened, opened, meter.Work().ShardOpens)
		})
	}
}
