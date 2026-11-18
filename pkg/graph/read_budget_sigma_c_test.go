package graph_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestReadBudgetPublicHistoryDoors:
// Nodes().HistoryCtx and Rels().HistoryCtx reach the graph's budgeted doors on
// every backend (memory included): without a budget and under an unlimited one
// the plain door's rows in its order, the ID and native routing counted, the versions
// looked at counted, a history limit under the versions refused with
// ErrReadBudget and no rows, and a cancelled context refused first.
func TestReadBudgetPublicHistoryDoors(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	const updates = 6
	for _, b := range allStoreBackends() {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			ctx := context.Background()
			a, err := g.Nodes().Add(ctx, []string{"Ev"}, map[string]any{"v": int64(0)})
			if err != nil {
				t.Fatal(err)
			}
			z, err := g.Nodes().Add(ctx, []string{"Ev"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			r, err := g.Rels().Add(ctx, "R", a, z, map[string]any{"v": int64(0)})
			if err != nil {
				t.Fatal(err)
			}
			for v := 1; v <= updates; v++ {
				if _, err := g.Nodes().Update(ctx, a.ID(), map[string]any{"v": int64(v)}); err != nil {
					t.Fatal(err)
				}
				if _, err := g.Rels().Update(ctx, r.ID(), map[string]any{"v": int64(v)}); err != nil {
					t.Fatal(err)
				}
			}
			nodeRows := func(h []*types.Node) string {
				var out []string
				for _, n := range h {
					out = append(out, fmt.Sprintf("n%d v%d %v", n.ID(), n.Version(), n.PropertiesMap()))
				}
				return strings.Join(out, "; ")
			}
			relRows := func(h []*types.Relationship) string {
				var out []string
				for _, x := range h {
					out = append(out, fmt.Sprintf("r%d v%d %v", x.ID(), x.Version(), x.PropertiesMap()))
				}
				return strings.Join(out, "; ")
			}
			plainNodes, err := g.Nodes().History(a.ID())
			if err != nil || len(plainNodes) != updates {
				t.Fatalf("plain node History = %d rows, %v; want %d", len(plainNodes), err, updates)
			}
			plainRels, err := g.Rels().History(r.ID())
			if err != nil || len(plainRels) != updates {
				t.Fatalf("plain rel History = %d rows, %v; want %d", len(plainRels), err, updates)
			}
			doors := []struct {
				name string
				want string
				call func(ctx context.Context) (string, error)
			}{
				{"Nodes", nodeRows(plainNodes), func(ctx context.Context) (string, error) {
					h, err := g.Nodes().HistoryCtx(ctx, a.ID())
					return nodeRows(h), err
				}},
				{"Rels", relRows(plainRels), func(ctx context.Context) (string, error) {
					h, err := g.Rels().HistoryCtx(ctx, r.ID())
					return relRows(h), err
				}},
			}
			for _, d := range doors {
				if got, err := d.call(ctx); err != nil || got != d.want {
					t.Fatalf("%s: no budget = %q, %v; want %q", d.name, got, err, d.want)
				}
				uctx, meter := store.WithReadBudget(ctx, store.ReadBudget{})
				if got, err := d.call(uctx); err != nil || got != d.want {
					t.Fatalf("%s: unlimited = %q, %v; want %q", d.name, got, err, d.want)
				}
				// Every returned version is examined; pending overlay copies and
				// committed iterator walks are additional native history work.
				historyWork := meter.Work().HistoryExamined
				wantCandidates := uint64(1)
				if b.name == "tiered" {
					wantCandidates++ // reference-owner probe
					if d.name == "Rels" {
						wantCandidates++ // event relationship owner after the reference miss
					}
				}
				if w := meter.Work(); w.Candidates != wantCandidates || historyWork < updates {
					t.Fatalf("%s: unlimited work %+v; want %d candidates and at least %d versions looked at", d.name, w, wantCandidates, updates)
				}
				bctx, meter := store.WithReadBudget(ctx, store.ReadBudget{HistoryExamined: updates - 1})
				got, err := d.call(bctx)
				var rb *store.ReadBudgetError
				if !errors.Is(err, store.ErrReadBudget) || !errors.As(err, &rb) || rb.Limit != "history examined" || rb.Door != "History" || got != "" {
					t.Fatalf("%s: HistoryExamined %d = %q, %v; want ErrReadBudget \"history examined\" from History and no rows", d.name, updates-1, got, err)
				}
				if w := meter.Work(); w != rb.Work {
					t.Fatalf("%s: meter %+v, error work %+v", d.name, w, rb.Work)
				}
				exact, _ := store.WithReadBudget(ctx, store.ReadBudget{HistoryExamined: historyWork, Candidates: wantCandidates})
				if got, err := d.call(exact); err != nil || got != d.want {
					t.Fatalf("%s: a budget of exactly the work = %q, %v; want the rows", d.name, got, err)
				}
				cctx, cancel := context.WithCancel(ctx)
				cancel()
				if _, err := d.call(cctx); !errors.Is(err, context.Canceled) {
					t.Fatalf("%s: cancelled ctx = %v", d.name, err)
				}
			}
		})
	}
}

// TestReadBudgetPublicPointDoors:
// Nodes().GetCtx and Temporal().NodeAtCtx reach the graph's budgeted point
// reads on every backend (memory included): without a budget and under an
// unlimited one the plain door's row (and, for a deleted node, its
// not-found), the ID and native routing counted, NodeAtCtx's history looked at counted, a
// history limit of 1 refused with ErrReadBudget and no row, a second call on a
// spent candidate budget refused, and a cancelled context refused first.
func TestReadBudgetPublicPointDoors(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	for _, b := range allStoreBackendsWith(txFromCfg) {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			ctx := context.Background()
			live, err := g.Nodes().Add(ctx, []string{"Ev"}, map[string]any{"v": int64(0)})
			if err != nil {
				t.Fatal(err)
			}
			dead, err := g.Nodes().Add(ctx, []string{"Ev"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			early, err := g.Temporal().NowTx()
			if err != nil {
				t.Fatal(err)
			}
			for v := 1; v <= 5; v++ {
				if _, err := g.Nodes().Update(ctx, live.ID(), map[string]any{"v": int64(v)}); err != nil {
					t.Fatal(err)
				}
			}
			if err := g.Nodes().Delete(ctx, dead.ID()); err != nil {
				t.Fatal(err)
			}
			row := func(n *types.Node, err error) string {
				switch {
				case errors.Is(err, store.ErrNodeNotFound):
					return "not found"
				case err != nil:
					return "error: " + err.Error()
				}
				return fmt.Sprintf("n%d v%d %v", n.ID(), n.Version(), n.PropertiesMap())
			}

			getCandidates, atCandidates := uint64(1), uint64(1)
			if b.name == "tiered" {
				getCandidates, atCandidates = 2, 4 // core read plus one or three owner probes
				atCandidates++                     // historical resolution also revalidates metadata-owner liveness
			}
			wantLive := row(g.Nodes().Get(ctx, live.ID()))
			wantDead := row(g.Nodes().Get(ctx, dead.ID()))
			if wantDead != "not found" || !strings.Contains(wantLive, "v5") {
				t.Fatalf("plain Get: live %q, deleted %q", wantLive, wantDead)
			}
			for _, c := range []struct {
				name string
				id   types.NodeID
				want string
			}{{"live", live.ID(), wantLive}, {"deleted", dead.ID(), wantDead}} {
				if got := row(g.Nodes().GetCtx(ctx, c.id)); got != c.want {
					t.Fatalf("GetCtx %s: no budget = %q; want %q", c.name, got, c.want)
				}
				uctx, meter := store.WithReadBudget(ctx, store.ReadBudget{})
				if got := row(g.Nodes().GetCtx(uctx, c.id)); got != c.want || meter.Work().Candidates != getCandidates {
					t.Fatalf("GetCtx %s: unlimited = %q, work %+v; want %q and %d candidates", c.name, got, meter.Work(), c.want, getCandidates)
				}
			}
			bctx, meter := store.WithReadBudget(ctx, store.ReadBudget{Candidates: getCandidates})
			if _, err := g.Nodes().GetCtx(bctx, live.ID()); err != nil {
				t.Fatalf("one complete read under %d candidates: %v", getCandidates, err)
			}
			if n, err := g.Nodes().GetCtx(bctx, live.ID()); !errors.Is(err, store.ErrReadBudget) || n != nil || meter.Work().Candidates != getCandidates+1 {
				t.Fatalf("second read under %d candidates = %v, %v, %+v; want ErrReadBudget and no row", getCandidates, n, err, meter.Work())
			}
			cctx, cancel := context.WithCancel(ctx)
			cancel()
			if _, err := g.Nodes().GetCtx(cctx, live.ID()); !errors.Is(err, context.Canceled) {
				t.Fatalf("GetCtx cancelled ctx = %v", err)
			}

			tp := g.Temporal()
			wantAt := row(tp.NodeAt(live.ID(), early))
			if wantAt == wantLive || strings.HasPrefix(wantAt, "error") {
				t.Fatalf("plain NodeAt at the early instant = %q; want an earlier version", wantAt)
			}
			if got := row(tp.NodeAtCtx(ctx, live.ID(), early)); got != wantAt {
				t.Fatalf("NodeAtCtx: no budget = %q; want %q", got, wantAt)
			}
			uctx, meter := store.WithReadBudget(ctx, store.ReadBudget{})
			if got := row(tp.NodeAtCtx(uctx, live.ID(), early)); got != wantAt {
				t.Fatalf("NodeAtCtx: unlimited = %q; want %q", got, wantAt)
			}
			total := meter.Work()
			if total.Candidates != atCandidates || total.HistoryExamined < 2 {
				t.Fatalf("NodeAtCtx: unlimited work %+v; want %d candidates and the chain looked at", total, atCandidates)
			}
			hctx, hmeter := store.WithReadBudget(ctx, store.ReadBudget{HistoryExamined: 1})
			n, err := tp.NodeAtCtx(hctx, live.ID(), early)
			var rb *store.ReadBudgetError
			if !errors.Is(err, store.ErrReadBudget) || !errors.As(err, &rb) || rb.Limit != "history examined" || rb.Door != "NodeAt" || n != nil || hmeter.Work() != rb.Work {
				t.Fatalf("NodeAtCtx under HistoryExamined 1 = %v, %v, work %+v; want ErrReadBudget \"history examined\" from NodeAt and no row", n, err, hmeter.Work())
			}
			exact, _ := store.WithReadBudget(ctx, store.ReadBudget{HistoryExamined: total.HistoryExamined, Candidates: atCandidates})
			if got := row(tp.NodeAtCtx(exact, live.ID(), early)); got != wantAt {
				t.Fatalf("NodeAtCtx under exactly the work = %q; want %q", got, wantAt)
			}
			if _, err := tp.NodeAtCtx(cctx, live.ID(), early); !errors.Is(err, context.Canceled) {
				t.Fatalf("NodeAtCtx cancelled ctx = %v", err)
			}
		})
	}
}

// TestReadBudgetPublicRelsLowestAnsweredPins:
// Temporal().RelsLowestAnsweredPinsCtx reaches the graph's budgeted door on every
// backend (memory included): the plain door's aligned pins without a budget and
// under an unlimited one, one candidate and 8 buffer bytes counted per ID, a
// limit one under either refused with ErrReadBudget, no answer and nothing read,
// and a cancelled context refused first.
func TestReadBudgetPublicRelsLowestAnsweredPins(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
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
			var ids []types.RelID
			for i := range 4 {
				r, err := g.Rels().Add(ctx, "R", a, z, map[string]any{"v": int64(i)})
				if err != nil {
					t.Fatal(err)
				}
				ids = append(ids, r.ID())
			}
			if err := g.Rels().Delete(ctx, ids[3]); err != nil {
				t.Fatal(err)
			}
			ids = append(ids, ids[0]) // a repeated ID
			tp := g.Temporal()
			want, err := tp.RelsLowestAnsweredPins(ids)
			if err != nil || len(want) != len(ids) {
				t.Fatalf("plain = %v, %v", want, err)
			}
			if got, err := tp.RelsLowestAnsweredPinsCtx(ctx, ids); err != nil || !slices.Equal(got, want) {
				t.Fatalf("no budget = %v, %v; want %v", got, err, want)
			}
			uctx, meter := store.WithReadBudget(ctx, store.ReadBudget{})
			if got, err := tp.RelsLowestAnsweredPinsCtx(uctx, ids); err != nil || !slices.Equal(got, want) {
				t.Fatalf("unlimited = %v, %v; want %v", got, err, want)
			}
			n := uint64(len(ids)) // #nosec G115 -- a count
			if w := meter.Work(); w != (store.ReadWork{Candidates: n, BufferBytes: 8 * n}) {
				t.Fatalf("unlimited work %+v; want %d candidates and %d buffer bytes only (no retention watermark, no row read)", w, n, 8*n)
			}
			for _, c := range []struct {
				limit string
				b     store.ReadBudget
			}{{"candidates", store.ReadBudget{Candidates: n - 1}}, {"buffer bytes", store.ReadBudget{BufferBytes: 8*n - 1}}} {
				bctx, meter := store.WithReadBudget(ctx, c.b)
				got, err := tp.RelsLowestAnsweredPinsCtx(bctx, ids)
				var rb *store.ReadBudgetError
				if !errors.Is(err, store.ErrReadBudget) || !errors.As(err, &rb) || rb.Limit != c.limit || rb.Door != "RelsLowestAnsweredPins" || got != nil || meter.Work() != rb.Work || meter.Work().HistoryExamined != 0 || meter.Work().DecodedBytes != 0 {
					t.Fatalf("%s: %v, %v, work %+v; want ErrReadBudget %q from RelsLowestAnsweredPins, no answer and nothing read", c.limit, got, err, meter.Work(), c.limit)
				}
			}
			cctx, cancel := context.WithCancel(ctx)
			cancel()
			if _, err := tp.RelsLowestAnsweredPinsCtx(cctx, ids); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled ctx = %v", err)
			}
		})
	}
}
