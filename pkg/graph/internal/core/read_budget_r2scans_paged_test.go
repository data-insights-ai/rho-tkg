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

// R2-SCANS: the paged effective scans (NodesEffectivePage, RelsEffectivePage,
// ForEachNodeEffectiveByLabelAndProperties and ...PropertyValues) take a ctx
// already; under a read budget in it they count and refuse. Counts only. Notes
// and red output: tasks/evidence/R2SCANS/red.txt.

const (
	r2pN     = 12  // nodes of each label, relationships of each type
	r2pNodeA = "P" // composite index on (a, b), property index on (a) and (i)
	r2pNodeB = "Q" // no index: every lookup is a label scan
)

// r2pWorld is a graph of r2pN nodes of label P and of label Q, each with a="x",
// b="y", i, and two updates (three versions), and r2pN relationships of type RP
// (property index on a) and of type RQ (none) between two E nodes (no property
// a), each with two updates.
type r2pWorld struct {
	g          *Core
	pin        types.Instant
	nodesP     []types.NodeID
	nodesQ     []types.NodeID
	ends       int // nodes of the other label (read by a page, kept by none)
	relsP      []types.RelID
	relsQ      []types.RelID
	pageFilter temporal.Filter
}

func r2pBuild(t testing.TB, g *Core) *r2pWorld {
	t.Helper()
	ctx := context.Background()
	w := &r2pWorld{g: g, pageFilter: temporal.Filter{Prop: "a", Eq: "x"}}
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(g.Index.CreateProperty(r2pNodeA, "a"))
	must(g.Index.CreateProperty(r2pNodeA, "i"))
	must(g.Index.CreateComposite(r2pNodeA, []string{"a", "b"}))
	_ = g.Index.CreateRelProperty("RP", "a") // tiered keeps no relationship property index
	e1, err := g.Nodes.Add(ctx, []string{"E"}, map[string]any{"k": "s"})
	must(err)
	e2, err := g.Nodes.Add(ctx, []string{"E"}, map[string]any{"k": "e"})
	must(err)
	w.ends = 2
	for i := range r2pN {
		for _, label := range []string{r2pNodeA, r2pNodeB} {
			n, err := g.Nodes.Add(ctx, []string{label}, map[string]any{"a": "x", "b": "y", "i": int64(i), "pad": fmt.Sprintf("%0*d", 40, i)})
			must(err)
			for v := range 2 {
				_, err := g.Nodes.Update(ctx, n.ID(), map[string]any{"v": int64(v)})
				must(err)
			}
			if label == r2pNodeA {
				w.nodesP = append(w.nodesP, n.ID())
			} else {
				w.nodesQ = append(w.nodesQ, n.ID())
			}
		}
		for _, typ := range []string{"RP", "RQ"} {
			r, err := g.Rels.AddByID(ctx, typ, e1.ID(), e2.ID(), map[string]any{"a": "x", "i": int64(i)})
			must(err)
			for v := range 2 {
				_, err := g.Rels.Update(ctx, r.ID(), map[string]any{"v": int64(v)})
				must(err)
			}
			if typ == "RP" {
				w.relsP = append(w.relsP, r.ID())
			} else {
				w.relsQ = append(w.relsQ, r.ID())
			}
		}
	}
	w.pin, err = g.Temporal.NowTx()
	must(err)
	return w
}

// evict drops the entity-cache entries of every node and relationship, so the
// next read decodes them again.
func (w *r2pWorld) evict(t testing.TB) {
	t.Helper()
	nodes := append(slices.Clone(w.nodesP), w.nodesQ...)
	rbdEvict(t, w.g, nodes, append(slices.Clone(w.relsP), w.relsQ...))
}

// r2pDoor is one paged door under a context, rendering what it hands out as
// strings, one per segment.
type r2pDoor struct {
	name, door string
	slice      bool // returns its segments (exact or refused), else streams them
	// reads is how many entities the door reads, ends how many of them it reads
	// and drops before the first it keeps, segs how many segments it hands out,
	// scanned the candidates its store lookup counts beyond the reads.
	reads, ends, segs, scanned uint64
	run                        func(ctx context.Context, fn func(string) bool) error
}

func (w *r2pWorld) doors(limit int) []r2pDoor {
	g := w.g
	n := uint64(r2pN) // #nosec G115 -- a count
	nodeSegs := func(segs []temporal.NodeSegment) string {
		var out []string
		for _, s := range segs {
			out = append(out, sbNodeSeg(s))
		}
		return strings.Join(out, " ")
	}
	emit := func(fn func(string) bool) func(types.NodeID, []temporal.NodeSegment) bool {
		return func(id types.NodeID, segs []temporal.NodeSegment) bool {
			return fn(fmt.Sprintf("n%d: %s", int64(id), nodeSegs(segs)))
		}
	}
	values := make([]any, r2pN)
	for i := range values {
		values[i] = int64(i)
	}
	pageOpts := temporal.EffectivePageOpts{}
	// A store with the composite posting list (badger, tiered) lists the
	// composite lookup's IDs without reading a row, counted by the reads alone;
	// one without it (sharded) lists through the rows, counting the matches.
	compositeScanned := n
	if g.compositeIDPage != nil {
		compositeScanned = 0
	}
	ends := uint64(w.ends) // #nosec G115 -- a count
	return []r2pDoor{
		{name: "NodesEffectivePage", door: "NodesEffectivePage", slice: true, reads: 2*n + ends, ends: ends, segs: 2 * n * 3,
			run: func(ctx context.Context, fn func(string) bool) error {
				segs, _, err := g.Temporal.NodesEffectivePage(ctx, temporal.Pin{Tx: w.pin}, w.pageFilter, 0, limit)
				if err != nil {
					return err
				}
				for _, s := range segs {
					if !fn(sbNodeSeg(s)) {
						return nil
					}
				}
				return nil
			}},
		{name: "RelsEffectivePage", door: "RelsEffectivePage", slice: true, reads: 2 * n, segs: 2 * n * 3,
			run: func(ctx context.Context, fn func(string) bool) error {
				segs, _, err := g.Temporal.RelsEffectivePage(ctx, temporal.Pin{Tx: w.pin}, w.pageFilter, 0, limit)
				if err != nil {
					return err
				}
				for _, s := range segs {
					if !fn(sbRelSeg(s)) {
						return nil
					}
				}
				return nil
			}},
		{name: "props composite", door: "ForEachNodeEffectiveByLabelAndProperties", reads: n, segs: n * 3, scanned: compositeScanned,
			run: func(ctx context.Context, fn func(string) bool) error {
				_, err := g.Temporal.ForEachNodeEffectiveByLabelAndProperties(ctx, r2pNodeA, map[string]any{"a": "x", "b": "y"}, w.pin, pageOpts, emit(fn))
				return err
			}},
		{name: "props scan", door: "ForEachNodeEffectiveByLabelAndProperties", reads: n, segs: n * 3, scanned: n,
			run: func(ctx context.Context, fn func(string) bool) error {
				_, err := g.Temporal.ForEachNodeEffectiveByLabelAndProperties(ctx, r2pNodeB, map[string]any{"a": "x", "b": "y"}, w.pin, pageOpts, emit(fn))
				return err
			}},
		{name: "values index", door: "ForEachNodeEffectiveByLabelAndPropertyValues", reads: n, segs: n * 3, scanned: n,
			run: func(ctx context.Context, fn func(string) bool) error {
				_, err := g.Temporal.ForEachNodeEffectiveByLabelAndPropertyValues(ctx, r2pNodeA, map[string]any{"a": "x"}, "i", values, w.pin, pageOpts, emit(fn))
				return err
			}},
		{name: "values scan", door: "ForEachNodeEffectiveByLabelAndPropertyValues", reads: n, segs: n * 3, scanned: n,
			run: func(ctx context.Context, fn func(string) bool) error {
				_, err := g.Temporal.ForEachNodeEffectiveByLabelAndPropertyValues(ctx, r2pNodeB, map[string]any{"a": "x"}, "i", values, w.pin, pageOpts, emit(fn))
				return err
			}},
	}
}

// TestR2Scans_Paged: the paged scans under a budget, on badger, sharded and
// tiered, rows pending and flushed:
//   - a context without a budget, and an unlimited one, answer the same: the
//     unlimited meter counts one candidate for each entity the door reads before
//     it reads it (a page door lists every label's or type's candidates, so it
//     reads the endpoint nodes it then drops), the segments each hands out, and
//     8 buffer bytes per listed candidate (a page door also the segments it
//     collects, a segment's struct size each); a lookup that scans the label (no
//     index) counts its nodes besides;
//   - a Candidates limit refuses at the entity that passes it, the stream's
//     callbacks before it standing and a page door returning nothing (exact or
//     refused), and the count is exact: a limit at it answers, one under it
//     refuses;
//   - HistoryExamined, DecodedBytes, Segments and BufferBytes limits refuse the
//     same way, a BufferBytes limit of 1 before any entity is read;
//   - a page of 5 entities costs 5 reads under a Candidates limit of 5.
func TestR2Scans_Paged(t *testing.T) {
	t.Parallel()
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			g, shut := be.open(t, dir)
			w := r2pBuild(t, g)
			probe := func(t *testing.T, g *Core, flushed bool) {
				w.g = g
				for _, d := range w.doors(1000) {
					t.Run(d.name, func(t *testing.T) {
						ctx := context.Background()
						cold := func() {
							if flushed {
								w.evict(t)
							}
						}
						cold()
						want, err := sbCollect(func(fn func(string) bool) error { return d.run(ctx, fn) })
						if err != nil || uint64(len(want)) == 0 {
							t.Fatalf("no budget = %d rows, %v", len(want), err)
						}
						cold()
						fctx, full := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
						if got, err := sbCollect(func(fn func(string) bool) error { return d.run(fctx, fn) }); err != nil || !slices.Equal(got, want) {
							t.Fatalf("unlimited = %v, %v; want the same answer\n%v", got, err, want)
						}
						total := full.Work()
						// 8 bytes per listed candidate; a page door lists them twice (each
						// source's list, then the merged batch) and also holds the segments
						// it returns, a segment's struct size each.
						wantBuf := 8 * d.reads
						if d.slice {
							size := uint64(nodeSegmentSize) // #nosec G115 -- a struct size
							if d.name == "RelsEffectivePage" {
								size = uint64(relSegmentSize) // #nosec G115 -- a struct size
							}
							wantBuf += 8*d.reads + d.segs*size
						}
						// (Sharded keeps no membership sidecar for a label or a type without
						// the index: its gather is every entity with history, a superset, so
						// it lists more than the page reads.)
						if be.name == "sharded" && d.slice {
							wantBuf = total.BufferBytes
							if total.BufferBytes < 8*d.reads+8*d.reads+d.segs*uint64(nodeSegmentSize) { // #nosec G115 -- a struct size
								t.Fatalf("sharded page held %d buffer bytes; want at least the lists and the segments", total.BufferBytes)
							}
						}
						if total.Candidates != d.reads+d.scanned || total.Segments != d.segs || total.BufferBytes != wantBuf {
							t.Fatalf("unlimited work %+v; want %d candidates (%d reads, %d scanned), %d segments and %d buffer bytes", total, d.reads+d.scanned, d.reads, d.scanned, d.segs, wantBuf)
						}
						if flushed && (total.DecodedBytes == 0 || total.HistoryExamined == 0) {
							t.Fatalf("flushed read counted %+v; want decoded bytes and history examined", total)
						}

						refused := func(t *testing.T, b storepkg.ReadBudget, limit string) ([]string, storepkg.ReadWork) {
							t.Helper()
							cold()
							pctx, meter := storepkg.WithReadBudget(ctx, b)
							got, err := sbCollect(func(fn func(string) bool) error { return d.run(pctx, fn) })
							rb := rbdBudgetErr(t, err, limit)
							work := meter.Work()
							if sbWorkOf(work, limit) != sbWorkOf(rb.Work, limit) {
								t.Fatalf("%s: error work %+v, meter %+v; want the refused counter equal", limit, rb.Work, work)
							}
							if d.slice && len(got) != 0 {
								t.Fatalf("%s: a page door handed out %d segments under a refusal", limit, len(got))
							}
							if !sbPrefix(got, want) {
								t.Fatalf("%s: callbacks %v are not a prefix of the plain answer\n%v", limit, got, want)
							}
							return got, work
						}

						// A limit of 5: a lookup that counts the label or the key (scanned)
						// refuses inside it, before any entity is read; otherwise the sixth
						// read is refused, the stream having handed out the five before it.
						got, work := refused(t, storepkg.ReadBudget{Candidates: 5}, "candidates")
						switch {
						case d.scanned > 0:
							if len(got) != 0 || work.HistoryExamined != 0 || work.Segments != 0 {
								t.Fatalf("Candidates 5 inside the lookup: %d callbacks, work %+v; want none and no entity read", len(got), work)
							}
						case d.slice:
							if work.Candidates != 6 {
								t.Fatalf("Candidates 5: work %+v; want the sixth read refused (6 candidates)", work)
							}
						default:
							if work.Candidates != 6 || len(got) != 5 || work.Segments != 5*3 { // five reads, a callback each, of three segments
								t.Fatalf("Candidates 5: %d callbacks, work %+v; want the first 5 entities (15 segments) and 6 candidates counted", len(got), work)
							}
						}
						cold()
						ectx, _ := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: total.Candidates})
						if got, err := sbCollect(func(fn func(string) bool) error { return d.run(ectx, fn) }); err != nil || !slices.Equal(got, want) {
							t.Fatalf("Candidates %d (the unlimited count) = %v, %v; want the same answer", total.Candidates, got, err)
						}
						_, work = refused(t, storepkg.ReadBudget{Candidates: total.Candidates - 1}, "candidates")
						if work.Candidates != total.Candidates {
							t.Fatalf("Candidates %d (one under): work %+v; want a refusal at the last candidate (%d)", total.Candidates-1, work, total.Candidates)
						}
						_, work = refused(t, storepkg.ReadBudget{BufferBytes: 1}, "buffer bytes")
						if work.HistoryExamined != 0 || work.Segments != 0 {
							t.Fatalf("BufferBytes 1: work %+v; want no entity read", work)
						}
						_, work = refused(t, storepkg.ReadBudget{HistoryExamined: 1}, "history examined")
						if work.Candidates > d.scanned+d.reads/2 {
							t.Fatalf("HistoryExamined 1: work %+v; want a refusal within the first entities", work)
						}
						if flushed {
							_, work = refused(t, storepkg.ReadBudget{DecodedBytes: 1}, "decoded bytes")
							if work.Segments != 0 {
								t.Fatalf("DecodedBytes 1: work %+v; want a refusal before any segment", work)
							}
						}
						// Under a Segments bound an entity is read at a time: the first
						// entity (a callback of three segments, none from a page door, which
						// holds them) is handed out, the second entity's first segment is the
						// fourth, refused, and a lookup that scans the label scans it once.
						got, work = refused(t, storepkg.ReadBudget{Segments: 3}, "segments")
						if work.Segments != 4 || (!d.slice && len(got) != 1) || work.Candidates != d.scanned+d.ends+2 {
							t.Fatalf("Segments 3: %d callbacks, work %+v; want 1 callback (none from a page door), 4 segments counted and %d candidates (the lookup's, the endpoints', two entities)", len(got), work, d.scanned+d.ends+2)
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

// TestR2Scans_PagedLimit: a page of 5 entities costs 5 reads (7 for the node
// page, which reads the two endpoint nodes the filter drops, listed first by
// ID): a Candidates limit at that count answers, one under refuses at the
// last read, and the page lists no more candidates than the budget has room for
// plus the one that passes it (BufferBytes counts only what it lists).
func TestR2Scans_PagedLimit(t *testing.T) {
	t.Parallel()
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			g, shut := be.open(t, t.TempDir())
			defer shut()
			w := r2pBuild(t, g)
			ctx := context.Background()
			for _, d := range w.doors(5)[:2] {
				want, err := sbCollect(func(fn func(string) bool) error { return d.run(ctx, fn) })
				if err != nil || len(want) != 5*3 {
					t.Fatalf("%s: plain page of 5 = %d segments, %v; want 15", d.name, len(want), err)
				}
				reads := 5 + d.ends
				pctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: reads})
				got, err := sbCollect(func(fn func(string) bool) error { return d.run(pctx, fn) })
				if err != nil || !slices.Equal(got, want) || meter.Work().Candidates != reads {
					t.Fatalf("%s: a page of 5 under Candidates %d = %d segments, %v, work %+v; want the plain page and %d candidates", d.name, reads, len(got), err, meter.Work(), reads)
				}
				pctx, meter = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: reads - 1})
				got, err = sbCollect(func(fn func(string) bool) error { return d.run(pctx, fn) })
				rbdBudgetErr(t, err, "candidates")
				if len(got) != 0 || meter.Work().Candidates != reads {
					t.Fatalf("%s: Candidates %d over a page of 5: %d segments, work %+v; want none and a refusal at read %d", d.name, reads-1, len(got), meter.Work(), reads)
				}
			}
			// The node page's sidecar source (label P) and its merged batch list at
			// most the room plus one (7 of 12 and of 26) under Candidates 6: the
			// lists Q and E gather whole (12 and 2) are counted after they are built.
			if g.nodePropTxPage != nil && be.name != "sharded" {
				d := w.doors(1000)[0]
				pctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 6})
				_, err := sbCollect(func(fn func(string) bool) error { return d.run(pctx, fn) })
				rbdBudgetErr(t, err, "candidates")
				const lists = 8 * (7 + 12 + 2 + 7)
				if work := meter.Work(); work.BufferBytes < lists || work.BufferBytes > lists+uint64(nodeSegmentSize)*work.Segments { // #nosec G115 -- a struct size
					t.Fatalf("a page under Candidates 6 held %d buffer bytes with %d segments; want the lists (%d: 7 + 12 + 2 + 7 IDs) and the segments' structs", work.BufferBytes, work.Segments, lists)
				}
			}
		})
	}
}

// TestR2Scans_PagedShardOpens: on a tiered store whose cold shards are closed
// before each read, each paged door opens the cold shards its candidate gather
// and its reads reach; under ShardOpens 1 it opens at most one and refuses with
// "shard opens" (a page door returning nothing, a stream's callbacks before the
// refusal standing); unlimited it answers as the plain door and the meter
// counts exactly the shards opened.
func TestR2Scans_PagedShardOpens(t *testing.T) {
	t.Parallel()
	w := sbBuildTiered(t)
	g := w.g
	pin := w.pin
	ctx := context.Background()
	nodeSegs := func(segs []temporal.NodeSegment) string {
		var out []string
		for _, s := range segs {
			out = append(out, sbNodeSeg(s))
		}
		return strings.Join(out, " ")
	}
	emit := func(fn func(string) bool) func(types.NodeID, []temporal.NodeSegment) bool {
		return func(id types.NodeID, segs []temporal.NodeSegment) bool {
			return fn(fmt.Sprintf("n%d: %s", int64(id), nodeSegs(segs)))
		}
	}
	values := []any{int64(0), int64(1), int64(2), int64(3), int64(4), int64(5)}
	doors := []struct {
		name, door string
		run        func(ctx context.Context, fn func(string) bool) error
	}{
		{"NodesEffectivePage", "NodesEffectivePage", func(ctx context.Context, fn func(string) bool) error {
			segs, _, err := g.Temporal.NodesEffectivePage(ctx, temporal.Pin{Tx: pin}, temporal.Filter{Prop: "i", Eq: int64(0)}, 0, 100)
			for _, s := range segs {
				fn(sbNodeSeg(s))
			}
			return err
		}},
		{"RelsEffectivePage", "RelsEffectivePage", func(ctx context.Context, fn func(string) bool) error {
			segs, _, err := g.Temporal.RelsEffectivePage(ctx, temporal.Pin{Tx: pin}, temporal.Filter{Prop: "i", Eq: int64(0)}, 0, 100)
			for _, s := range segs {
				fn(sbRelSeg(s))
			}
			return err
		}},
		{"props", "ForEachNodeEffectiveByLabelAndProperties", func(ctx context.Context, fn func(string) bool) error {
			_, err := g.Temporal.ForEachNodeEffectiveByLabelAndProperties(ctx, sbLabel, map[string]any{"v": int64(1), "i": int64(0)}, pin, temporal.EffectivePageOpts{}, emit(fn))
			return err
		}},
		{"values", "ForEachNodeEffectiveByLabelAndPropertyValues", func(ctx context.Context, fn func(string) bool) error {
			_, err := g.Temporal.ForEachNodeEffectiveByLabelAndPropertyValues(ctx, sbLabel, nil, "i", values, pin, temporal.EffectivePageOpts{}, emit(fn))
			return err
		}},
	}
	for _, d := range doors {
		t.Run(d.name, func(t *testing.T) {
			w.closeCold(t)
			opens := w.opens()
			want, err := sbCollect(func(fn func(string) bool) error { return d.run(ctx, fn) })
			plainOpens := w.opens() - opens
			if err != nil || len(want) == 0 || plainOpens < 2 {
				t.Fatalf("plain = %d rows, %v, %d cold shards opened; want an answer and at least 2 opens", len(want), err, plainOpens)
			}

			w.closeCold(t)
			opens = w.opens()
			bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{ShardOpens: 1})
			got, err := sbCollect(func(fn func(string) bool) error { return d.run(bctx, fn) })
			boundedOpened := w.opens() - opens
			rb := rbdBudgetErr(t, err, "shard opens")
			if meter.Work().ShardOpens != rb.Work.ShardOpens || !sbPrefix(got, want) || boundedOpened > 1 || (strings.HasSuffix(d.name, "Page") && len(got) != 0) {
				t.Fatalf("ShardOpens 1: door %q, work %+v, meter %+v, %d callbacks, %d shards opened; want the refused counter equal, a prefix (none from a page door) and at most 1 open", rb.Door, rb.Work, meter.Work(), len(got), boundedOpened)
			}

			w.closeCold(t)
			opens = w.opens()
			bctx, meter = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
			got, err = sbCollect(func(fn func(string) bool) error { return d.run(bctx, fn) })
			opened := w.opens() - opens
			if err != nil || !slices.Equal(got, want) || opened != plainOpens || meter.Work().ShardOpens != opened {
				t.Fatalf("unlimited = %d rows, %v, %d shards opened, %d counted; want the plain answer, %d opens, all counted", len(got), err, opened, meter.Work().ShardOpens, plainOpens)
			}
			t.Logf("%s: the plain read opened %d cold shards; under ShardOpens 1 it opened %d and refused; unlimited it opened %d and counted %d", d.name, plainOpens, boundedOpened, opened, meter.Work().ShardOpens)
		})
	}
}

// TestR2Scans_ListCap: a listing asks for no more candidates than the meter has
// room for plus the one that passes the limit, and the room shrinks as the reads
// count.
func TestR2Scans_ListCap(t *testing.T) {
	t.Parallel()
	var none *scanRun
	if got := none.listCap(100); got != 100 {
		t.Fatalf("no budget: listCap(100) = %d, want 100", got)
	}
	ctx, meter := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{Candidates: 5})
	run := &scanRun{meter: storepkg.ReadMeterFrom(ctx), door: "d"}
	for _, c := range []struct{ asked, counted, want int }{{100, 0, 6}, {3, 0, 3}, {100, 4, 2}, {100, 5, 1}} {
		if c.counted > 0 {
			if err := meter.AddCandidates("d", c.counted-int(meter.Work().Candidates)); err != nil { // #nosec G115 -- a count
				t.Fatal(err)
			}
		}
		if got := run.listCap(c.asked); got != c.want {
			t.Fatalf("Candidates 5, %d counted: listCap(%d) = %d, want %d", c.counted, c.asked, got, c.want)
		}
	}
	// Another bound than Candidates does not cap a listing.
	ctx, _ = storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{DecodedBytes: 1})
	if got := (&scanRun{meter: storepkg.ReadMeterFrom(ctx), door: "d"}).listCap(100); got != 100 {
		t.Fatalf("DecodedBytes 1: listCap(100) = %d, want 100", got)
	}
}

// TestR2Scans_PagedGates: a cancelled context is refused before any read and a
// nil callback before any charge; the arguments are checked before anything is
// counted.
func TestR2Scans_PagedGates(t *testing.T) {
	t.Parallel()
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			g, shut := be.open(t, t.TempDir())
			defer shut()
			w := r2pBuild(t, g)
			for _, d := range w.doors(1000) {
				cctx, cancel := context.WithCancel(context.Background())
				cancel()
				calls := 0
				if err := d.run(cctx, func(string) bool { calls++; return true }); !errors.Is(err, context.Canceled) || calls != 0 {
					t.Fatalf("%s: cancelled ctx = %v after %d callbacks; want context.Canceled and none", d.name, err, calls)
				}
			}
			bctx, meter := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{Candidates: 100})
			if _, err := g.Temporal.ForEachNodeEffectiveByLabelAndProperties(bctx, r2pNodeA, map[string]any{"a": "x", "b": "y"}, w.pin, temporal.EffectivePageOpts{}, nil); !errors.Is(err, grapherr.ErrNilCallback) || meter.Work() != (storepkg.ReadWork{}) {
				t.Fatalf("nil callback = %v, work %+v; want ErrNilCallback and nothing counted", err, meter.Work())
			}
			if _, _, err := g.Temporal.NodesEffectivePage(bctx, temporal.Pin{Tx: w.pin}, w.pageFilter, 0, 0); err == nil || errors.Is(err, storepkg.ErrReadBudget) || meter.Work() != (storepkg.ReadWork{}) {
				t.Fatalf("page limit 0 = %v, work %+v; want an argument error and nothing counted", err, meter.Work())
			}
		})
	}
}
