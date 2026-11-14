package core

import (
	"context"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// R2-5 HEADREAD (sigma, 14 Nov): a budgeted single-entity timeline read is
// answered from the effective head entry as the plain read is, and is charged
// what that path reads (the winners' versions and their decodes), not the
// version chain the entry skips.

// hrNode builds a node of versions rows: [1000, 2000), then [2000, 3000), then
// versions-2 rows of [3000, 4000), each superseding the last. Its timeline is
// three segments whatever versions is: two history winners and the current
// row.
func hrNode(tb testing.TB, g *Core, versions int) types.NodeID {
	tb.Helper()
	ctx := context.Background()
	p := r10Props(0)
	p["tkg_valid_from"], p["tkg_valid_to"] = int64(1000), int64(2000)
	n, err := g.Nodes.Add(ctx, []string{"Derived"}, p)
	if err != nil {
		tb.Fatalf("Add: %v", err)
	}
	for i := 1; i < versions; i++ {
		from, to := types.Instant(3000), types.Instant(4000)
		if i == 1 {
			from, to = 2000, 3000
		}
		if _, err := g.Temporal.SetNodeVersionInterval(ctx, n.ID(), from, to, r10Props(i)); err != nil {
			tb.Fatalf("SetNodeVersionInterval: %v", err)
		}
	}
	return n.ID()
}

// hrRel is hrNode for a relationship.
func hrRel(tb testing.TB, g *Core, versions int) types.RelID {
	tb.Helper()
	ctx := context.Background()
	s, e := txbEndpointsTB(tb, g)
	p := r10Props(0)
	p["tkg_valid_from"], p["tkg_valid_to"] = int64(1000), int64(2000)
	r, err := g.Rels.AddByID(ctx, "LINK", s, e, p)
	if err != nil {
		tb.Fatalf("AddByID: %v", err)
	}
	for i := 1; i < versions; i++ {
		from, to := types.Instant(3000), types.Instant(4000)
		if i == 1 {
			from, to = 2000, 3000
		}
		if _, err := g.Temporal.SetRelVersionInterval(ctx, r.ID(), from, to, r10Props(i)); err != nil {
			tb.Fatalf("SetRelVersionInterval: %v", err)
		}
	}
	return r.ID()
}

// hrDoor is one timeline door, plain and under ctx, rendered.
type hrDoor struct {
	name  string
	head  bool // the plain door reads the head entry (a by-IDs door is a scan: it never does)
	plain func() (string, error)
	ctx   func(context.Context) (string, error)
}

func hrDoors(g *Core, node types.NodeID, rel types.RelID, pin types.Instant) []hrDoor {
	pins := []types.Instant{pin, pin}
	many := func(all [][]temporal.NodeSegment, err error) (string, error) {
		s := ""
		for _, segs := range all {
			s += sbSegs(segs) + " | "
		}
		return s, err
	}
	manyRel := func(all [][]temporal.RelSegment, err error) (string, error) {
		s := ""
		for _, segs := range all {
			s += sbRelSegs(segs) + " | "
		}
		return s, err
	}
	nodeByIDs := func(read func(func(temporal.NodeSegment) bool) error) (string, error) {
		var segs []temporal.NodeSegment
		err := read(func(s temporal.NodeSegment) bool { segs = append(segs, s); return true })
		return sbSegs(segs), err
	}
	relByIDs := func(read func(func(temporal.RelSegment) bool) error) (string, error) {
		var segs []temporal.RelSegment
		err := read(func(s temporal.RelSegment) bool { segs = append(segs, s); return true })
		return sbRelSegs(segs), err
	}
	tp := g.Temporal
	return []hrDoor{
		{"node timeline", true,
			func() (string, error) { s, err := tp.NodeEffectiveTimeline(node, pin); return sbSegs(s), err },
			func(c context.Context) (string, error) {
				s, err := tp.NodeEffectiveTimelineCtx(c, node, pin)
				return sbSegs(s), err
			}},
		{"node timeline from", true,
			func() (string, error) { s, err := tp.NodeEffectiveTimelineFrom(node, 2500, pin); return sbSegs(s), err },
			func(c context.Context) (string, error) {
				s, err := tp.NodeEffectiveTimelineFromCtx(c, node, 2500, pin)
				return sbSegs(s), err
			}},
		{"node timelines", true,
			func() (string, error) { return many(tp.NodeEffectiveTimelines(node, pins)) },
			func(c context.Context) (string, error) { return many(tp.NodeEffectiveTimelinesCtx(c, node, pins)) }},
		{"rel timeline", true,
			func() (string, error) { s, err := tp.RelEffectiveTimeline(rel, pin); return sbRelSegs(s), err },
			func(c context.Context) (string, error) {
				s, err := tp.RelEffectiveTimelineCtx(c, rel, pin)
				return sbRelSegs(s), err
			}},
		{"rel timelines", true,
			func() (string, error) { return manyRel(tp.RelEffectiveTimelines(rel, pins)) },
			func(c context.Context) (string, error) { return manyRel(tp.RelEffectiveTimelinesCtx(c, rel, pins)) }},
		{"nodes by IDs", false,
			func() (string, error) {
				return nodeByIDs(func(fn func(temporal.NodeSegment) bool) error {
					return tp.NodesEffectiveByIDs([]types.NodeID{node}, pin, fn)
				})
			},
			func(c context.Context) (string, error) {
				return nodeByIDs(func(fn func(temporal.NodeSegment) bool) error {
					return tp.NodesEffectiveByIDsCtx(c, []types.NodeID{node}, pin, fn)
				})
			}},
		{"rels by IDs", false,
			func() (string, error) {
				return relByIDs(func(fn func(temporal.RelSegment) bool) error {
					return tp.RelsEffectiveByIDs([]types.RelID{rel}, pin, fn)
				})
			},
			func(c context.Context) (string, error) {
				return relByIDs(func(fn func(temporal.RelSegment) bool) error {
					return tp.RelsEffectiveByIDsCtx(c, []types.RelID{rel}, pin, fn)
				})
			}},
	}
}

// hrRun is one budgeted read's figures: the meter's work and two native
// counters the meter does not keep, the reads answered from the head entry
// (Core.headHits) and the node history skeletons read (headStats.metaRows).
type hrRun struct {
	answer   string
	work     storepkg.ReadWork
	hits     uint64
	metaRows uint64
}

// TestBudgetedTimeline_ReadsTheHead: on badger, sharded and tiered, rows
// pending and flushed, a node and a relationship of 10 and of 80 versions
// whose timelines are the same three segments. After the plain door has
// filled the head entry, each head door under an unlimited budget gives the
// plain door's exact answer (the control), from the head entry (one head hit,
// no skeleton read), and counts the same HistoryExamined and DecodedBytes at 80
// versions as at 10: the two history winners it reads, not the chain. Under a
// HistoryExamined limit one below that count it refuses at the second winner
// and walks no chain after the refusal. The by-IDs doors are scans, plain and
// budgeted alike: they never read the head entry, and their figures are
// logged.
func TestBudgetedTimeline_ReadsTheHead(t *testing.T) {
	t.Parallel()
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			g, shut := be.open(t, dir)
			sizes := []int{10, 80}
			nodes := map[int]types.NodeID{}
			rels := map[int]types.RelID{}
			for _, n := range sizes {
				nodes[n], rels[n] = hrNode(t, g, n), hrRel(t, g, n)
			}
			probe := func(t *testing.T, g *Core, flushed bool) {
				pin := g.peekNow()
				cold := func(n int) {
					if flushed {
						rbdEvict(t, g, []types.NodeID{nodes[n]}, []types.RelID{rels[n]})
					}
				}
				for i := range hrDoors(g, 0, 0, pin) {
					name := hrDoors(g, 0, 0, pin)[i].name
					t.Run(name, func(t *testing.T) {
						runs := map[int]hrRun{}
						for _, n := range sizes {
							d := hrDoors(g, nodes[n], rels[n], pin)[i]
							cold(n)
							want, err := d.plain() // fills the head entry
							if err != nil || want == "" {
								t.Fatalf("%d versions: plain = %q, %v", n, want, err)
							}
							cold(n)
							hits, metas := g.headHits.Load(), g.headStats.metaRows.Load()
							ctx, meter := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{})
							got, err := d.ctx(ctx)
							if err != nil || got != want {
								t.Fatalf("%d versions: unlimited = %q, %v; want the plain answer %q", n, got, err, want)
							}
							runs[n] = hrRun{got, meter.Work(), g.headHits.Load() - hits, g.headStats.metaRows.Load() - metas}
						}
						small, big := runs[10], runs[80]
						t.Logf("%s: HistoryExamined %d at 10 versions, %d at 80; DecodedBytes %d / %d; head hits %d / %d; skeletons read %d / %d; answer %s",
							name, small.work.HistoryExamined, big.work.HistoryExamined, small.work.DecodedBytes, big.work.DecodedBytes,
							small.hits, big.hits, small.metaRows, big.metaRows, big.answer)
						d := hrDoors(g, nodes[80], rels[80], pin)[i]
						if !d.head {
							return // a scan: no head entry, plain or budgeted (logged above)
						}
						for _, n := range sizes {
							if r := runs[n]; r.hits != 1 || r.metaRows != 0 {
								t.Errorf("%d versions: %d head hits, %d skeletons read; want 1 and 0 (the budgeted read bypasses the head entry)", n, r.hits, r.metaRows)
							}
						}
						if small.work.HistoryExamined != big.work.HistoryExamined || big.work.HistoryExamined > 2 {
							t.Errorf("HistoryExamined %d at 10 versions, %d at 80; want the same, at most the 2 history winners", small.work.HistoryExamined, big.work.HistoryExamined)
						}
						if small.work.DecodedBytes != big.work.DecodedBytes {
							t.Errorf("DecodedBytes %d at 10 versions, %d at 80; want the same", small.work.DecodedBytes, big.work.DecodedBytes)
						}
						if flushed && big.work.DecodedBytes == 0 {
							t.Errorf("flushed: nothing decoded (%+v)", big.work)
						}
						// A refusal among the winners ends the read: no fold walks
						// the chain after it.
						if h := big.work.HistoryExamined; h >= 2 {
							cold(80)
							ctx, meter := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{HistoryExamined: h - 1})
							got, err := d.ctx(ctx)
							if !isBudget(err, "history examined") || meter.Work().HistoryExamined > h {
								t.Errorf("HistoryExamined %d: %q, %v, %d looked at; want a refusal within %d", h-1, got, err, meter.Work().HistoryExamined, h)
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
