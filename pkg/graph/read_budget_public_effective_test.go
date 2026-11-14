package graph_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Sigma's group B doors through the public API, on every backend including
// memory (the core tests run the disk backends): same answers, the counts a
// budget bounds, the typed refusal.

// pubScan is one scan door, plain and under a context, rendering each segment.
type pubScan struct {
	name string
	// bounded marks a From or Window door: where the store keeps a valid-end
	// index (every backend but sharded) it reads only the entities still open
	// at the pin, not every entity of the label or type.
	bounded bool
	plain   func(fn func(string) bool) error
	ctx     func(ctx context.Context, fn func(string) bool) error
}

func pubScans(g interface {
	Temporal() *temporal.API
}, pin types.Instant) []pubScan {
	nf := func(fn func(string) bool) func(temporal.NodeSegment) bool {
		return func(s temporal.NodeSegment) bool {
			return fn(fmt.Sprintf("n%d[%d,%d)v%d", s.Node.ID(), s.ValidFrom, s.ValidTo, s.Node.Version()))
		}
	}
	rf := func(fn func(string) bool) func(temporal.RelSegment) bool {
		return func(s temporal.RelSegment) bool {
			return fn(fmt.Sprintf("r%d[%d,%d)v%d", s.Rel.ID(), s.ValidFrom, s.ValidTo, s.Rel.Version()))
		}
	}
	tp := g.Temporal()
	labels := []string{"D"}
	window := temporal.Window{{From: pin, To: math.MaxInt64}}
	return []pubScan{
		{"ByLabel", false, func(fn func(string) bool) error { return tp.ForEachNodeEffectiveByLabel("D", pin, nf(fn)) },
			func(ctx context.Context, fn func(string) bool) error {
				return tp.ForEachNodeEffectiveByLabelCtx(ctx, "D", pin, nf(fn))
			}},
		{"ByLabels", false, func(fn func(string) bool) error { return tp.ForEachNodeEffectiveByLabels(labels, pin, nf(fn)) },
			func(ctx context.Context, fn func(string) bool) error {
				return tp.ForEachNodeEffectiveByLabelsCtx(ctx, labels, pin, nf(fn))
			}},
		{"ByLabelsFrom", true, func(fn func(string) bool) error { return tp.ForEachNodeEffectiveByLabelsFrom(labels, pin, pin, nf(fn)) },
			func(ctx context.Context, fn func(string) bool) error {
				return tp.ForEachNodeEffectiveByLabelsFromCtx(ctx, labels, pin, pin, nf(fn))
			}},
		{"ByType", false, func(fn func(string) bool) error { return tp.ForEachRelEffectiveByType("R", pin, rf(fn)) },
			func(ctx context.Context, fn func(string) bool) error {
				return tp.ForEachRelEffectiveByTypeCtx(ctx, "R", pin, rf(fn))
			}},
		{"ByTypeFrom", true, func(fn func(string) bool) error { return tp.ForEachRelEffectiveByTypeFrom("R", pin, pin, rf(fn)) },
			func(ctx context.Context, fn func(string) bool) error {
				return tp.ForEachRelEffectiveByTypeFromCtx(ctx, "R", pin, pin, rf(fn))
			}},
		{"ByLabelsWindow", true, func(fn func(string) bool) error {
			return tp.ForEachNodeEffectiveByLabelsWindow(labels, pin, window, nf(fn))
		},
			func(ctx context.Context, fn func(string) bool) error {
				return tp.ForEachNodeEffectiveByLabelsWindowCtx(ctx, labels, pin, window, nf(fn))
			}},
		{"ByTypeWindow", true, func(fn func(string) bool) error { return tp.ForEachRelEffectiveByTypeWindow("R", pin, window, rf(fn)) },
			func(ctx context.Context, fn func(string) bool) error {
				return tp.ForEachRelEffectiveByTypeWindowCtx(ctx, "R", pin, window, rf(fn))
			}},
	}
}

func pubCollect(run func(fn func(string) bool) error) ([]string, error) {
	var got []string
	err := run(func(s string) bool { got = append(got, s); return true })
	return got, err
}

// TestReadBudgetPublicScans: Temporal().ForEach...Ctx stream the plain
// scan's segments in its order without a budget and under an unlimited one
// (one candidate counted per entity, one segment per segment), refuse at a
// Candidates limit after streaming the entities read before it, and stop at a
// history limit of 1 within two entities, on every backend.
func TestReadBudgetPublicScans(t *testing.T) {
	t.Parallel()
	const n = 8
	for _, b := range allStoreBackends() {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			ctx := context.Background()
			a, err := g.Nodes().Add(ctx, []string{"Ev"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			z, err := g.Nodes().Add(ctx, []string{"Ev"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			for i := range n {
				nd, err := g.Nodes().Add(ctx, []string{"D"}, map[string]any{"i": int64(i)})
				if err != nil {
					t.Fatal(err)
				}
				r, err := g.Rels().Add(ctx, "R", a, z, map[string]any{"i": int64(i)})
				if err != nil {
					t.Fatal(err)
				}
				for v := range 2 {
					if _, err := g.Nodes().Update(ctx, nd.ID(), map[string]any{"v": int64(v)}); err != nil {
						t.Fatal(err)
					}
					if _, err := g.Rels().Update(ctx, r.ID(), map[string]any{"v": int64(v)}); err != nil {
						t.Fatal(err)
					}
				}
				if i%4 == 3 {
					if err := g.Nodes().Delete(ctx, nd.ID()); err != nil {
						t.Fatal(err)
					}
					if err := g.Rels().Delete(ctx, r.ID()); err != nil {
						t.Fatal(err)
					}
				}
			}
			pin, err := g.Temporal().NowTx()
			if err != nil {
				t.Fatal(err)
			}
			for _, d := range pubScans(g, pin) {
				want, err := pubCollect(d.plain)
				if err != nil || len(want) == 0 {
					t.Fatalf("%s: plain scan = %d segments, %v", d.name, len(want), err)
				}
				if got, err := pubCollect(func(fn func(string) bool) error { return d.ctx(ctx, fn) }); err != nil || !slices.Equal(got, want) {
					t.Fatalf("%s: no budget = %v, %v; want the plain stream %v", d.name, got, err, want)
				}
				fctx, all := store.WithReadBudget(ctx, store.ReadBudget{})
				if got, err := pubCollect(func(fn func(string) bool) error { return d.ctx(fctx, fn) }); err != nil || !slices.Equal(got, want) {
					t.Fatalf("%s: unlimited = %v, %v; want the plain stream %v", d.name, got, err, want)
				}
				// A From or Window door reads only what the valid-end index lists, the
				// entities still open at the pin (the plain call above resolved it),
				// where the store keeps one: not the two that were deleted.
				entities := uint64(n)
				if d.bounded && b.name != "sharded" {
					entities = n - 2
				}
				if w := all.Work(); w.Candidates != entities || w.Segments != uint64(len(want)) || w.HistoryExamined == 0 {
					t.Fatalf("%s: unlimited work %+v; want %d candidates, %d segments and history examined", d.name, w, entities, len(want))
				}

				cctx, cm := store.WithReadBudget(ctx, store.ReadBudget{Candidates: 3})
				got, err := pubCollect(func(fn func(string) bool) error { return d.ctx(cctx, fn) })
				if !errors.Is(err, store.ErrReadBudget) || cm.Work().Candidates != 4 || len(got) == 0 || !slices.Equal(got, want[:len(got)]) {
					t.Fatalf("%s: Candidates 3 = %v, %v (work %+v); want a prefix of the plain stream and a refusal at the fourth candidate", d.name, got, err, cm.Work())
				}
				hctx, hm := store.WithReadBudget(ctx, store.ReadBudget{HistoryExamined: 1})
				got, err = pubCollect(func(fn func(string) bool) error { return d.ctx(hctx, fn) })
				if !errors.Is(err, store.ErrReadBudget) || hm.Work().Candidates > 2 || !slices.Equal(got, want[:len(got)]) {
					t.Fatalf("%s: HistoryExamined 1 = %v, %v (work %+v); want a refusal within two entities", d.name, got, err, hm.Work())
				}
			}
		})
	}
}

// TestReadBudgetPublicTimelines: Temporal().NodeEffectiveTimelineCtx, its From
// and many-pin forms and the relationship twins return the plain doors' answer
// without a budget and under an unlimited one (one candidate, the history and
// the segments counted), and refuse with no segments at a limit of 1 on
// history or segments, on every backend (memory counts the versions of the
// chain it reads).
func TestReadBudgetPublicTimelines(t *testing.T) {
	t.Parallel()
	const updates = 12
	for _, b := range allStoreBackends() {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			ctx := context.Background()
			a, err := g.Nodes().Add(ctx, []string{"Ev"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			z, err := g.Nodes().Add(ctx, []string{"Ev"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			r, err := g.Rels().Add(ctx, "R", a, z, nil)
			if err != nil {
				t.Fatal(err)
			}
			var mid types.Instant
			for v := range updates {
				if v == updates/2 {
					if mid, err = g.Temporal().NowTx(); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := g.Nodes().Update(ctx, a.ID(), map[string]any{"v": int64(v)}); err != nil {
					t.Fatal(err)
				}
				if _, err := g.Rels().Update(ctx, r.ID(), map[string]any{"v": int64(v)}); err != nil {
					t.Fatal(err)
				}
			}
			pin, err := g.Temporal().NowTx()
			if err != nil {
				t.Fatal(err)
			}
			tp := g.Temporal()
			pins := []types.Instant{mid, pin, 0}
			nodeStr := func(s []temporal.NodeSegment) string {
				var out []string
				for _, x := range s {
					out = append(out, fmt.Sprintf("[%d,%d)v%d", x.ValidFrom, x.ValidTo, x.Node.Version()))
				}
				return fmt.Sprint(out)
			}
			relStr := func(s []temporal.RelSegment) string {
				var out []string
				for _, x := range s {
					out = append(out, fmt.Sprintf("[%d,%d)v%d", x.ValidFrom, x.ValidTo, x.Rel.Version()))
				}
				return fmt.Sprint(out)
			}
			doors := []struct {
				name  string
				plain func() (string, int, error)
				ctx   func(ctx context.Context) (string, int, error)
			}{
				{"node", func() (string, int, error) {
					s, err := tp.NodeEffectiveTimeline(a.ID(), pin)
					return nodeStr(s), len(s), err
				},
					func(c context.Context) (string, int, error) {
						s, err := tp.NodeEffectiveTimelineCtx(c, a.ID(), pin)
						return nodeStr(s), len(s), err
					}},
				{"node from", func() (string, int, error) {
					s, err := tp.NodeEffectiveTimelineFrom(a.ID(), mid, pin)
					return nodeStr(s), len(s), err
				}, func(c context.Context) (string, int, error) {
					s, err := tp.NodeEffectiveTimelineFromCtx(c, a.ID(), mid, pin)
					return nodeStr(s), len(s), err
				}},
				{"node pins", func() (string, int, error) {
					all, err := tp.NodeEffectiveTimelines(a.ID(), pins)
					n := 0
					for _, s := range all {
						n += len(s)
					}
					return fmt.Sprint(len(all), nodeStr(all[0]), nodeStr(all[1])), n, err
				}, func(c context.Context) (string, int, error) {
					all, err := tp.NodeEffectiveTimelinesCtx(c, a.ID(), pins)
					if err != nil {
						return "", 0, err
					}
					n := 0
					for _, s := range all {
						n += len(s)
					}
					return fmt.Sprint(len(all), nodeStr(all[0]), nodeStr(all[1])), n, nil
				}},
				{"rel", func() (string, int, error) {
					s, err := tp.RelEffectiveTimeline(r.ID(), pin)
					return relStr(s), len(s), err
				},
					func(c context.Context) (string, int, error) {
						s, err := tp.RelEffectiveTimelineCtx(c, r.ID(), pin)
						return relStr(s), len(s), err
					}},
				{"rel pins", func() (string, int, error) {
					all, err := tp.RelEffectiveTimelines(r.ID(), pins)
					n := 0
					for _, s := range all {
						n += len(s)
					}
					return fmt.Sprint(len(all), relStr(all[0]), relStr(all[1])), n, err
				}, func(c context.Context) (string, int, error) {
					all, err := tp.RelEffectiveTimelinesCtx(c, r.ID(), pins)
					if err != nil {
						return "", 0, err
					}
					n := 0
					for _, s := range all {
						n += len(s)
					}
					return fmt.Sprint(len(all), relStr(all[0]), relStr(all[1])), n, nil
				}},
			}
			for _, d := range doors {
				want, segs, err := d.plain()
				if err != nil || segs < 2 {
					t.Fatalf("%s: plain = %q (%d segments), %v", d.name, want, segs, err)
				}
				if got, _, err := d.ctx(ctx); err != nil || got != want {
					t.Fatalf("%s: no budget = %q, %v; want %q", d.name, got, err, want)
				}
				fctx, all := store.WithReadBudget(ctx, store.ReadBudget{})
				if got, _, err := d.ctx(fctx); err != nil || got != want {
					t.Fatalf("%s: unlimited = %q, %v; want %q", d.name, got, err, want)
				}
				if w := all.Work(); w.Candidates != 1 || w.Segments != uint64(segs) || w.HistoryExamined == 0 {
					t.Fatalf("%s: unlimited work %+v; want 1 candidate, %d segments and history examined", d.name, w, segs)
				}
				for _, limit := range []store.ReadBudget{{HistoryExamined: 1}, {Segments: 1}, {BufferBytes: 1}} {
					bctx, _ := store.WithReadBudget(ctx, limit)
					if got, _, err := d.ctx(bctx); !errors.Is(err, store.ErrReadBudget) || got != "[]" && got != "" && got != "0 [] []" {
						t.Fatalf("%s under %+v = %q, %v; want ErrReadBudget and no segments", d.name, limit, got, err)
					}
				}
			}
		})
	}
}

// TestReadBudgetPublicValidEndFirstCall (R2-SCANS): a budgeted From or Window
// scan uses the valid-end index on every backend that keeps one (memory, badger,
// tiered), and the index's first call, which reads every ever-member's temporal
// fields, is counted: a Candidates limit refuses inside it, before any
// callback; the refusal leaves the index exact (the next call streams what the
// unbounded scan's entities that are still open stream); once the index is
// resolved a budgeted scan counts only the entities it lists.
func TestReadBudgetPublicValidEndFirstCall(t *testing.T) {
	t.Parallel()
	const n = 8
	entityOf := func(seg string) string { return seg[:slices.Index([]byte(seg), '[')] }
	for _, b := range allStoreBackends() {
		if b.name == "sharded" {
			continue // no valid-end index: the unbounded gather
		}
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			ctx := context.Background()
			a, err := g.Nodes().Add(ctx, []string{"Ev"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			z, err := g.Nodes().Add(ctx, []string{"Ev"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			deleted := map[string]bool{}
			for i := range n {
				nd, err := g.Nodes().Add(ctx, []string{"D"}, map[string]any{"i": int64(i)})
				if err != nil {
					t.Fatal(err)
				}
				r, err := g.Rels().Add(ctx, "R", a, z, map[string]any{"i": int64(i)})
				if err != nil {
					t.Fatal(err)
				}
				for v := range 2 {
					if _, err := g.Nodes().Update(ctx, nd.ID(), map[string]any{"v": int64(v)}); err != nil {
						t.Fatal(err)
					}
					if _, err := g.Rels().Update(ctx, r.ID(), map[string]any{"v": int64(v)}); err != nil {
						t.Fatal(err)
					}
				}
				if i%4 == 3 {
					if err := g.Nodes().Delete(ctx, nd.ID()); err != nil {
						t.Fatal(err)
					}
					if err := g.Rels().Delete(ctx, r.ID()); err != nil {
						t.Fatal(err)
					}
					deleted[fmt.Sprintf("n%d", nd.ID())], deleted[fmt.Sprintf("r%d", r.ID())] = true, true
				}
			}
			pin, err := g.Temporal().NowTx()
			if err != nil {
				t.Fatal(err)
			}
			byName := map[string]pubScan{}
			for _, d := range pubScans(g, pin) {
				byName[d.name] = d
			}
			for _, c := range []struct {
				door, oracle string
				fresh        bool // the first door of its kind: the index holds no token yet (the Window door follows on the same index)
			}{
				{"ByLabelsFrom", "ByLabels", true}, {"ByLabelsWindow", "ByLabels", false}, {"ByTypeFrom", "ByType", true}, {"ByTypeWindow", "ByType", false},
			} {
				d := byName[c.door]
				// The oracle is the unbounded scan (no index) without the entities that
				// ended before the pin; a window [pin, inf) keeps only the segment
				// still open, the last of each entity.
				all, err := pubCollect(byName[c.oracle].plain)
				if err != nil {
					t.Fatal(err)
				}
				var want []string
				last := map[string]string{}
				var order []string
				window := strings.HasSuffix(c.door, "Window")
				for _, s := range all {
					if deleted[entityOf(s)] {
						continue
					}
					if _, ok := last[entityOf(s)]; !ok {
						order = append(order, entityOf(s))
					}
					last[entityOf(s)] = s
					if !window {
						want = append(want, s)
					}
				}
				if window {
					for _, e := range order {
						want = append(want, last[e])
					}
				}
				if c.fresh {
					// The first call is counted: a limit of 5 refuses inside it.
					pctx, pm := store.WithReadBudget(ctx, store.ReadBudget{Candidates: 5})
					got, err := pubCollect(func(fn func(string) bool) error { return d.ctx(pctx, fn) })
					if !errors.Is(err, store.ErrReadBudget) || len(got) != 0 || pm.Work().Candidates != 6 {
						t.Fatalf("%s: Candidates 5 on the first call = %d callbacks, %v, work %+v; want none and a refusal at the sixth ever-member", d.name, len(got), err, pm.Work())
					}
				}
				uctx, um := store.WithReadBudget(ctx, store.ReadBudget{})
				got, err := pubCollect(func(fn func(string) bool) error { return d.ctx(uctx, fn) })
				if err != nil || !slices.Equal(got, want) {
					t.Fatalf("%s: unlimited = %v, %v; want %v", d.name, got, err, want)
				}
				// Badger and tiered install nothing of a refused first call: it reads
				// every ever-member again. Memory keeps the 5 entities it had read
				// before the refusal and reads the 3 left.
				if first, wantFirst := um.Work().Candidates, uint64(n+(n-2)); c.fresh {
					if b.name == "memory" {
						wantFirst = (n - 5) + (n - 2)
					}
					if first != wantFirst {
						t.Fatalf("%s: the unlimited first call counted %d candidates; want %d (the ever-members it read, then the %d listed)", d.name, first, wantFirst, n-2)
					}
				}
				sctx, sm := store.WithReadBudget(ctx, store.ReadBudget{})
				if got, err := pubCollect(func(fn func(string) bool) error { return d.ctx(sctx, fn) }); err != nil || !slices.Equal(got, want) || sm.Work().Candidates != n-2 {
					t.Fatalf("%s: second call = %v, %v, %d candidates; want the stream and the %d entities the index lists", d.name, got, err, sm.Work().Candidates, n-2)
				}
			}
		})
	}
}
