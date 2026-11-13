package core

import (
	"context"
	"fmt"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestReadBudget_ProbeMatrix (READBUDGET step 8, René's done criterion): for
// every budgeted door at 8, 32 and 128 matches, each with a history of 3
// versions, on badger, sharded and tiered, with the rows pending (as written)
// and flushed (after a reopen), a budget of 1 on any counter the unlimited read
// passes stops the read: ErrReadBudget at that limit, no rows, the meter equal
// to the error's work, and at most about one row decoded (the check runs before
// the work, nothing runs after the refusal). Without a budget the answer is the
// unbudgeted one. Shard opens, dense keys and unsupported stores have their own
// tests (read_budget_shard_opens_test.go, read_budget_history_probe_test.go).
func TestReadBudget_ProbeMatrix(t *testing.T) {
	t.Parallel()
	sizes := []int{8, 32, 128}
	counters := []struct {
		limit string
		set   func(*storepkg.ReadBudget)
		get   func(storepkg.ReadWork) uint64
	}{
		{"candidates", func(b *storepkg.ReadBudget) { b.Candidates = 1 }, func(w storepkg.ReadWork) uint64 { return w.Candidates }},
		{"segments", func(b *storepkg.ReadBudget) { b.Segments = 1 }, func(w storepkg.ReadWork) uint64 { return w.Segments }},
		{"decoded bytes", func(b *storepkg.ReadBudget) { b.DecodedBytes = 1 }, func(w storepkg.ReadWork) uint64 { return w.DecodedBytes }},
		{"history examined", func(b *storepkg.ReadBudget) { b.HistoryExamined = 1 }, func(w storepkg.ReadWork) uint64 { return w.HistoryExamined }},
		{"buffer bytes", func(b *storepkg.ReadBudget) { b.BufferBytes = 1 }, func(w storepkg.ReadWork) uint64 { return w.BufferBytes }},
	}
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			g, shut := be.open(t, dir)
			ctx := context.Background()
			if err := g.Index.CreateProperty("D", "k"); err != nil {
				t.Fatal(err)
			}
			w := newRTBWorld(t, g)
			w.index(t, rtbType, rtbKeys...)
			groups := map[int][]types.NodeID{}
			var all []types.NodeID
			var rels []types.RelID
			for _, size := range sizes {
				for i := range size {
					n, err := g.Nodes.Add(ctx, []string{"D"}, map[string]any{"k": int64(size), "pad": fmt.Sprintf("%0*d", 100, i)})
					if err != nil {
						t.Fatal(err)
					}
					r := w.add(t, fmt.Sprintf("r%d-%d", size, i), rtbType, map[string]any{"estate": fmt.Sprintf("e%d", size), "device": fmt.Sprintf("d%d", size)})
					for v := range 2 {
						if _, err := g.Nodes.Update(ctx, n.ID(), map[string]any{"v": int64(v)}); err != nil {
							t.Fatal(err)
						}
						if _, err := g.Rels.Update(ctx, r, map[string]any{"v": int64(v)}); err != nil {
							t.Fatal(err)
						}
					}
					groups[size], all, rels = append(groups[size], n.ID()), append(all, n.ID()), append(rels, r)
				}
			}
			pinTx := w.pin(t)
			pin := temporal.Pin{Tx: pinTx}

			probe := func(t *testing.T, g *Core, flushed bool) {
				cold := func() {
					if flushed { // a pending row lives only in the cache: never evicted
						rbdEvict(t, g, all, rels)
					}
				}
				for _, size := range sizes {
					ids := groups[size]
					doors := []struct {
						name string
						call func(ctx context.Context) (int, error)
					}{
						{"LendByIDs", func(ctx context.Context) (int, error) {
							n := 0
							err := g.Nodes.LendByIDs(ctx, ids, func(int, *types.Node) bool { n++; return true })
							return n, err
						}},
						{"GetByIDs", func(ctx context.Context) (int, error) {
							r, err := g.Nodes.GetByIDsCtx(ctx, ids)
							return len(r), err
						}},
						{"property current", func(ctx context.Context) (int, error) {
							r, err := g.Nodes.ByLabelAndPropertyCtx(ctx, "D", "k", int64(size), storepkg.QueryOpts{})
							return len(r), err
						}},
						{"property TxPin", func(ctx context.Context) (int, error) {
							r, err := g.Nodes.ByLabelAndPropertyCtx(ctx, "D", "k", int64(size), storepkg.QueryOpts{TxPin: pinTx})
							return len(r), err
						}},
						{"property ValidAt", func(ctx context.Context) (int, error) {
							r, err := g.Nodes.ByLabelAndPropertyCtx(ctx, "D", "k", int64(size), storepkg.QueryOpts{ValidAt: pinTx, ValidAtSet: true})
							return len(r), err
						}},
						{"rel timelines", func(ctx context.Context) (int, error) {
							r, err := g.Temporal.RelEffectiveTimelinesByProperties(ctx, rtbType, rtbKeys, [][]any{{fmt.Sprintf("e%d", size), fmt.Sprintf("d%d", size)}}, pin)
							n := 0
							for _, segs := range r {
								n += len(segs)
							}
							return n, err
						}},
					}
					for _, d := range doors {
						what := fmt.Sprintf("%s over %d matches", d.name, size)
						cold()
						plain, err := d.call(ctx)
						if err != nil || plain == 0 {
							t.Fatalf("%s: no budget = %d, %v", what, plain, err)
						}
						cold()
						fctx, full := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
						if n, err := d.call(fctx); err != nil || n != plain {
							t.Fatalf("%s: unlimited = %d, %v; want %d", what, n, err, plain)
						}
						total := full.Work()
						for _, c := range counters {
							if c.get(total) <= 1 {
								continue // the door does not pass this counter's limit of 1
							}
							cold()
							var b storepkg.ReadBudget
							c.set(&b)
							pctx, meter := storepkg.WithReadBudget(ctx, b)
							n, err := d.call(pctx)
							rb := rbdBudgetErr(t, err, c.limit)
							got := meter.Work()
							if n != 0 || got != rb.Work {
								t.Fatalf("%s, %s 1: %d rows, error work %+v, meter %+v; want no rows and the meter's work", what, c.limit, n, rb.Work, got)
							}
							// About one row decoded at most, whatever the matches.
							if total.DecodedBytes > 0 && got.DecodedBytes > 3*total.DecodedBytes/uint64(size) {
								t.Fatalf("%s, %s 1: decoded %d bytes before the refusal (unlimited %d over %d matches)", what, c.limit, got.DecodedBytes, total.DecodedBytes, size)
							}
						}
					}
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
