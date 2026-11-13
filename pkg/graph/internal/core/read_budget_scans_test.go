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
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// sbScan is one scan door, plain and under a context; both feed fn the
// rendered segment (sbNodeSeg, sbRelSeg).
type sbScan struct {
	name, door string
	nodes      bool
	plain      func(fn func(string) bool) error
	ctx        func(ctx context.Context, fn func(string) bool) error
}

func sbScanDoors(g *Core, pin types.Instant) []sbScan {
	nf := func(fn func(string) bool) func(temporal.NodeSegment) bool {
		return func(s temporal.NodeSegment) bool { return fn(sbNodeSeg(s)) }
	}
	rf := func(fn func(string) bool) func(temporal.RelSegment) bool {
		return func(s temporal.RelSegment) bool { return fn(sbRelSeg(s)) }
	}
	labels := []string{sbLabel}
	return []sbScan{
		{"node by label", "ForEachNodeEffectiveByLabel", true,
			func(fn func(string) bool) error { return g.Temporal.ForEachNodeEffectiveByLabel(sbLabel, pin, nf(fn)) },
			func(ctx context.Context, fn func(string) bool) error {
				return g.Temporal.ForEachNodeEffectiveByLabelCtx(ctx, sbLabel, pin, nf(fn))
			}},
		{"node by labels", "ForEachNodeEffectiveByLabels", true,
			func(fn func(string) bool) error { return g.Temporal.ForEachNodeEffectiveByLabels(labels, pin, nf(fn)) },
			func(ctx context.Context, fn func(string) bool) error {
				return g.Temporal.ForEachNodeEffectiveByLabelsCtx(ctx, labels, pin, nf(fn))
			}},
		{"node by labels from", "ForEachNodeEffectiveByLabelsFrom", true,
			func(fn func(string) bool) error {
				return g.Temporal.ForEachNodeEffectiveByLabelsFrom(labels, pin, pin, nf(fn))
			},
			func(ctx context.Context, fn func(string) bool) error {
				return g.Temporal.ForEachNodeEffectiveByLabelsFromCtx(ctx, labels, pin, pin, nf(fn))
			}},
		{"rel by type", "ForEachRelEffectiveByType", false,
			func(fn func(string) bool) error { return g.Temporal.ForEachRelEffectiveByType(sbType, pin, rf(fn)) },
			func(ctx context.Context, fn func(string) bool) error {
				return g.Temporal.ForEachRelEffectiveByTypeCtx(ctx, sbType, pin, rf(fn))
			}},
		{"rel by type from", "ForEachRelEffectiveByTypeFrom", false,
			func(fn func(string) bool) error {
				return g.Temporal.ForEachRelEffectiveByTypeFrom(sbType, pin, pin, rf(fn))
			},
			func(ctx context.Context, fn func(string) bool) error {
				return g.Temporal.ForEachRelEffectiveByTypeFromCtx(ctx, sbType, pin, pin, rf(fn))
			}},
	}
}

// sbEntity is the entity part of a rendered segment ("n12" of "n12[3,0)v2").
func sbEntity(seg string) string { return seg[:strings.IndexByte(seg, '[')] }

// sbCollect runs run and returns the segments it handed to fn, in order.
func sbCollect(run func(fn func(string) bool) error) ([]string, error) {
	var got []string
	err := run(func(s string) bool { got = append(got, s); return true })
	return got, err
}

// TestReadBudget_SigmaB_Scans: the effective scans under a budget (group B,
// step 1). On badger, sharded and tiered, rows pending and flushed:
//   - without a budget, and under an unlimited one, a scan streams exactly the
//     plain door's segments in the plain door's order; the unlimited meter
//     counts one candidate per entity read, one segment per segment streamed
//     and 8 buffer bytes per listed candidate;
//   - a limit on any counter refuses with *ReadBudgetError at that limit from
//     that door; the callbacks made before it are a prefix of the plain stream
//     (the entities read before the refusal) and none follows it; the refused
//     counter of the meter equals the error's;
//   - the refusal comes before the next entity is read: a Candidates limit of
//     5 reads 5 entities, a Segments limit of 3 reads at most 2, a limit of 1
//     on history or decoded bytes at most 2, and a BufferBytes limit of 1
//     reads none (the plain scan reads every one of the 12 entities, which
//     the unlimited meter's counts show).
func TestReadBudget_SigmaB_Scans(t *testing.T) {
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
				for _, d := range sbScanDoors(g, w.pin) {
					t.Run(d.name, func(t *testing.T) {
						cold := func() {
							if flushed {
								w.evict(t)
							}
						}
						ctx := context.Background()
						cold()
						want, err := sbCollect(d.plain)
						if err != nil || len(want) == 0 {
							t.Fatalf("plain scan = %d segments, %v", len(want), err)
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
						if total.Candidates != n || total.Segments != uint64(len(want)) || total.BufferBytes != 8*n { // #nosec G115 -- a count
							t.Fatalf("unlimited work %+v; want %d candidates, %d segments, %d buffer bytes", total, n, len(want), 8*n)
						}
						if flushed && (total.DecodedBytes == 0 || total.HistoryExamined == 0) {
							t.Fatalf("flushed scan counted %+v; want decoded bytes and history examined", total)
						}

						// The first K candidates' segments are the stream's prefix of
						// those entities (candidates read in ascending ID order).
						var entities []string
						if d.nodes {
							for _, id := range w.nodes {
								entities = append(entities, fmt.Sprintf("n%d", int64(id)))
							}
						} else {
							for _, id := range w.rels {
								entities = append(entities, fmt.Sprintf("r%d", int64(id)))
							}
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
								got, err := sbCollect(func(fn func(string) bool) error { return d.ctx(pctx, fn) })
								rb := rbdBudgetErr(t, err, c.limit)
								work := meter.Work()
								// The error's work is the meter when the limit was passed; the
								// segments of the entities read before it are handed out (and
								// counted) after it, so only the refused counter must agree.
								if rb.Door != d.door || sbWorkOf(work, c.limit) != sbWorkOf(rb.Work, c.limit) || rb.Work.Candidates > work.Candidates || rb.Work.Segments > work.Segments {
									t.Fatalf("refused by %q with work %+v, meter %+v; want door %q and the refused counter equal", rb.Door, rb.Work, work, d.door)
								}
								t.Logf("%s %s %d: refused after %+v (unlimited: %+v)", d.name, c.limit, c.n, work, total)
								if !sbPrefix(got, want) {
									t.Fatalf("callbacks %v are not a prefix of the plain stream\n%v", got, want)
								}
								switch c.limit {
								case "candidates":
									first := map[string]bool{}
									for _, e := range entities[:c.n] {
										first[e] = true
									}
									var exp []string
									for _, s := range want {
										if first[sbEntity(s)] {
											exp = append(exp, s)
										}
									}
									if !slices.Equal(got, exp) || work.Candidates != c.n+1 {
										t.Fatalf("candidates %d: callbacks %v, work %+v; want the first %d entities' segments %v and %d candidates counted", c.n, got, work, c.n, exp, c.n+1)
									}
								case "segments":
									if len(got) != 3 || work.Segments != 4 || work.Candidates > 2 {
										t.Fatalf("segments 3: %d callbacks, work %+v; want 3 callbacks, 4 segments counted, at most 2 entities read", len(got), work)
									}
								case "buffer bytes":
									if len(got) != 0 || work.Candidates != 0 {
										t.Fatalf("buffer bytes 1: %d callbacks, work %+v; want none and no entity read", len(got), work)
									}
								default:
									if work.Candidates > 2 {
										t.Fatalf("%s 1: %d entities read before the refusal (%+v); want at most 2", c.limit, work.Candidates, work)
									}
									if flushed && c.limit == "decoded bytes" && work.DecodedBytes > 3*total.DecodedBytes/n {
										t.Fatalf("decoded bytes 1: %d bytes decoded before the refusal (unlimited %d over %d entities)", work.DecodedBytes, total.DecodedBytes, n)
									}
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

// TestReadBudget_SigmaB_ScanGates: a cancelled context is refused before any
// read, a nil callback before any charge, and a stopped callback (fn returning
// false) is not a refusal.
func TestReadBudget_SigmaB_ScanGates(t *testing.T) {
	t.Parallel()
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			g, shut := be.open(t, t.TempDir())
			defer shut()
			w := sbBuild(t, g, 6)
			for _, d := range sbScanDoors(g, w.pin) {
				cctx, cancel := context.WithCancel(context.Background())
				cancel()
				calls := 0
				if err := d.ctx(cctx, func(string) bool { calls++; return true }); !errors.Is(err, context.Canceled) || calls != 0 {
					t.Fatalf("%s: cancelled ctx = %v after %d callbacks; want context.Canceled and none", d.name, err, calls)
				}
				bctx, meter := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{Candidates: 100})
				var err error
				switch d.door {
				case "ForEachNodeEffectiveByLabel":
					err = g.Temporal.ForEachNodeEffectiveByLabelCtx(bctx, sbLabel, w.pin, nil)
				case "ForEachNodeEffectiveByLabels":
					err = g.Temporal.ForEachNodeEffectiveByLabelsCtx(bctx, []string{sbLabel}, w.pin, nil)
				case "ForEachNodeEffectiveByLabelsFrom":
					err = g.Temporal.ForEachNodeEffectiveByLabelsFromCtx(bctx, []string{sbLabel}, w.pin, w.pin, nil)
				case "ForEachRelEffectiveByType":
					err = g.Temporal.ForEachRelEffectiveByTypeCtx(bctx, sbType, w.pin, nil)
				default:
					err = g.Temporal.ForEachRelEffectiveByTypeFromCtx(bctx, sbType, w.pin, w.pin, nil)
				}
				if !errors.Is(err, grapherr.ErrNilCallback) || meter.Work() != (storepkg.ReadWork{}) {
					t.Fatalf("%s: nil callback = %v, work %+v; want ErrNilCallback and nothing counted", d.name, err, meter.Work())
				}
				// fn stopping the scan is the caller's choice: no error, and the
				// meter holds only what was read.
				sctx, meter := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{Candidates: 100})
				calls = 0
				if err := d.ctx(sctx, func(string) bool { calls++; return false }); err != nil || calls != 1 || meter.Work().Candidates == 0 {
					t.Fatalf("%s: a stopping callback = %v after %d calls, work %+v; want no error and one call", d.name, err, calls, meter.Work())
				}
			}
		})
	}
}

// TestReadBudget_SigmaB_ScanShardOpens: on a tiered store whose cold shards
// are closed before each read, the plain scan opens every cold shard (the
// member enumeration and the entity reads both fan out); under a ShardOpens
// budget of 1 the scan opens at most one and refuses with "shard opens" and
// no callback after it; unlimited it streams the plain stream and the meter
// counts exactly the shards opened.
func TestReadBudget_SigmaB_ScanShardOpens(t *testing.T) {
	t.Parallel()
	w := sbBuildTiered(t)
	ctx := context.Background()
	for _, d := range sbScanDoors(w.g, w.pin) {
		t.Run(d.name, func(t *testing.T) {
			w.closeCold(t)
			opens := w.opens()
			want, err := sbCollect(d.plain)
			plainOpens := w.opens() - opens
			if err != nil || len(want) == 0 || plainOpens < 3 {
				t.Fatalf("plain scan = %d segments, %v, %d cold shards opened; want a stream and at least 3 opens", len(want), err, plainOpens)
			}

			w.closeCold(t)
			opens = w.opens()
			bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{ShardOpens: 1})
			got, err := sbCollect(func(fn func(string) bool) error { return d.ctx(bctx, fn) })
			boundedOpened, boundedCalls := w.opens()-opens, len(got)
			rb := rbdBudgetErr(t, err, "shard opens")
			if rb.Door != d.door || meter.Work() != rb.Work || !sbPrefix(got, want) || boundedOpened > 1 {
				t.Fatalf("ShardOpens 1: door %q, work %+v, meter %+v, %d callbacks, %d shards opened; want %q, the meter's work, a prefix and at most 1 open", rb.Door, rb.Work, meter.Work(), len(got), boundedOpened, d.door)
			}

			w.closeCold(t)
			opens = w.opens()
			bctx, meter = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
			got, err = sbCollect(func(fn func(string) bool) error { return d.ctx(bctx, fn) })
			opened := w.opens() - opens
			if err != nil || !slices.Equal(got, want) || opened < 3 || meter.Work().ShardOpens != opened {
				t.Fatalf("unlimited = %d segments, %v, %d shards opened, %d counted; want the plain stream and every open counted", len(got), err, opened, meter.Work().ShardOpens)
			}
			t.Logf("%s: the plain scan opened %d cold shards; under ShardOpens 1 the scan opened %d and refused after %d callbacks; unlimited it opened %d and counted %d", d.name, plainOpens, boundedOpened, boundedCalls, opened, meter.Work().ShardOpens)
		})
	}
}

// TestReadBudget_SigmaB_MemberGatherMeteredOpens: each member enumeration a
// scan gathers its candidates from (store.MemberGatherMeteredCapability, the
// tiered store) charges the open of every closed shard it fans out to before
// the open: under ShardOpens 1 it opens at most one and refuses with "shard
// opens" and no member; unlimited it counts exactly the shards it opened; a nil
// tally is the plain form and counts nothing. Alone, because the scans' own
// gather runs two of these in a row and the second finds the shards open.
func TestReadBudget_SigmaB_MemberGatherMeteredOpens(t *testing.T) {
	t.Parallel()
	w := sbBuildTiered(t)
	ltok, ok := w.g.labels.Lookup(sbLabel)
	rtok, ok2 := w.g.relTypes.Lookup(sbType)
	if !ok || !ok2 {
		t.Fatal("fixture: label or type token unknown")
	}
	members := []struct {
		name string
		run  func(tally *storepkg.ReadTally) (int, error)
	}{
		{"NodeIDsByLabel", func(tl *storepkg.ReadTally) (int, error) {
			ids, err := w.ts.NodeIDsByLabelMetered(ltok, tl)
			return len(ids), err
		}},
		{"RelIDsByType", func(tl *storepkg.ReadTally) (int, error) {
			ids, err := w.ts.RelIDsByTypeMetered(rtok, tl)
			return len(ids), err
		}},
		{"ForEachLabelTxMember", func(tl *storepkg.ReadTally) (int, error) {
			n := 0
			err := w.ts.ForEachLabelTxMemberMetered(ltok, tl, func(types.NodeID, types.Instant) bool { n++; return true })
			return n, err
		}},
		{"ForEachRelTypeTxMember", func(tl *storepkg.ReadTally) (int, error) {
			n := 0
			err := w.ts.ForEachRelTypeTxMemberMetered(rtok, tl, func(types.RelID, types.Instant) bool { n++; return true })
			return n, err
		}},
	}
	ctx := context.Background()
	for _, m := range members {
		t.Run(m.name, func(t *testing.T) {
			w.closeCold(t)
			opens := w.opens()
			want, err := m.run(nil)
			plainOpens := w.opens() - opens
			if err != nil || want == 0 || plainOpens < 3 {
				t.Fatalf("plain form = %d members, %v, %d shards opened; want members and at least 3 opens", want, err, plainOpens)
			}

			w.closeCold(t)
			opens = w.opens()
			bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{ShardOpens: 1})
			got, err := m.run(meter.Tally("gather"))
			rb := rbdBudgetErr(t, err, "shard opens")
			if got != 0 || rb.Door != "gather" || w.opens()-opens > 1 || meter.Work().ShardOpens < 2 {
				t.Fatalf("ShardOpens 1 = %d members, %+v, %d shards opened (meter %+v); want no member, the door and at most 1 open", got, rb, w.opens()-opens, meter.Work())
			}
			_ = bctx

			w.closeCold(t)
			opens = w.opens()
			_, meter = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
			got, err = m.run(meter.Tally("gather"))
			opened := w.opens() - opens
			if err != nil || got != want || opened != plainOpens || meter.Work().ShardOpens != opened {
				t.Fatalf("unlimited = %d members, %v, %d shards opened, %d counted; want %d members and %d opens, all counted", got, err, opened, meter.Work().ShardOpens, want, plainOpens)
			}
		})
	}
}

// TestReadBudget_SigmaB_MemoryCountsHistory: the memory store has no read
// snapshot, so a scan's reads count the versions of an entity's chain through
// the entity reads' tally (it counted none before group B: a HistoryExamined
// budget over RelEffectiveTimelinesByProperties on memory never refused).
func TestReadBudget_SigmaB_MemoryCountsHistory(t *testing.T) {
	t.Parallel()
	g, err := New(Config{Store: memory.New(), UntimedValidStart: UntimedFromTxFrom})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	w := newRTBWorld(t, g)
	w.index(t, rtbType, rtbKeys...)
	for i := range 4 {
		id := w.add(t, fmt.Sprintf("r%d", i), rtbType, map[string]any{"estate": "e1", "device": "d1"})
		for v := range 3 {
			if _, err := g.Rels.Update(w.ctx, id, map[string]any{"v": int64(v)}); err != nil {
				t.Fatal(err)
			}
		}
	}
	pin := temporal.Pin{Tx: w.pin(t)}
	tuples := [][]any{{"e1", "d1"}}
	bctx, meter := storepkg.WithReadBudget(w.ctx, storepkg.ReadBudget{})
	if _, err := g.Temporal.RelEffectiveTimelinesByProperties(bctx, rtbType, rtbKeys, tuples, pin); err != nil || meter.Work().HistoryExamined < 4 {
		t.Fatalf("unlimited = %v, work %+v; want at least 4 history versions counted on memory", err, meter.Work())
	}
	bctx, meter = storepkg.WithReadBudget(w.ctx, storepkg.ReadBudget{HistoryExamined: 1})
	got, err := g.Temporal.RelEffectiveTimelinesByProperties(bctx, rtbType, rtbKeys, tuples, pin)
	if rbdBudgetErr(t, err, "history examined"); got != nil {
		t.Fatalf("HistoryExamined 1 on memory answered %v", got)
	}
	_ = meter
}
