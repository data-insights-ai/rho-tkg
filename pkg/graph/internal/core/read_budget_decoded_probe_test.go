package core

import (
	"context"
	"fmt"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestReadBudget_DecodedBytes_BudgetOneProbeStopsEarly (René's READBUDGET
// criterion): a budget of 1 decoded byte stops a probe at its first decode,
// whatever the number of matches. At 8, 32 and 128 matches each door returns
// ErrReadBudget "decoded bytes" and no rows, having counted one row's bytes
// (about the unlimited count over the matches) and not the matches': the check
// runs before the decode, and nothing is decoded after the refusal.
func TestReadBudget_DecodedBytes_BudgetOneProbeStopsEarly(t *testing.T) {
	t.Parallel()
	sizes := []int{8, 32, 128}
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			dir := t.TempDir()
			g, shut := be.open(t, dir)
			ctx := context.Background()
			if err := g.Index.CreateProperty("D", "k"); err != nil {
				t.Fatal(err)
			}
			w := newRTBWorld(t, g)
			w.index(t, rtbType, rtbKeys...)
			// One group per size: nodes of value k = size, relationships of device d<size>.
			groups := map[int][]types.NodeID{}
			var all []types.NodeID
			var rels []types.RelID
			for _, size := range sizes {
				for i := range size {
					n, err := g.Nodes.Add(ctx, []string{"D"}, map[string]any{"k": int64(size), "pad": fmt.Sprintf("%0*d", 150, i)})
					if err != nil {
						t.Fatal(err)
					}
					groups[size], all = append(groups[size], n.ID()), append(all, n.ID())
					rels = append(rels, w.add(t, fmt.Sprintf("r%d-%d", size, i), rtbType, map[string]any{"estate": "e1", "device": fmt.Sprintf("d%d", size), "pad": fmt.Sprintf("%0*d", 150, i)}))
				}
			}
			pin := temporal.Pin{Tx: w.pin(t)}
			shut()
			g, shut = be.open(t, dir)
			defer shut()
			w.g = g

			for _, size := range sizes {
				ids := groups[size]
				for _, d := range []struct {
					name, door string
					call       func(ctx context.Context) (int, error)
				}{
					{"lend", "LendNodesSorted", func(ctx context.Context) (int, error) {
						n := 0
						err := g.Nodes.LendByIDs(ctx, ids, func(int, *types.Node) bool { n++; return true })
						return n, err
					}},
					{"GetByIDs", "GetByIDs", func(ctx context.Context) (int, error) {
						r, err := g.Nodes.GetByIDsCtx(ctx, ids)
						return len(r), err
					}},
					{"property index", "ByLabelAndProperty", func(ctx context.Context) (int, error) {
						r, err := g.Nodes.ByLabelAndPropertyCtx(ctx, "D", "k", int64(size), storepkg.QueryOpts{})
						return len(r), err
					}},
					{"property scan", "ByLabelAndProperty", func(ctx context.Context) (int, error) { // no index on pad: the label's every row
						r, err := g.Nodes.ByLabelAndPropertyCtx(ctx, "D", "pad", fmt.Sprintf("%0*d", 150, 0), storepkg.QueryOpts{})
						return len(r), err
					}},
					{"rel timelines", "RelEffectiveTimelinesByProperties", func(ctx context.Context) (int, error) {
						r, err := g.Temporal.RelEffectiveTimelinesByProperties(ctx, rtbType, rtbKeys, [][]any{{"e1", fmt.Sprintf("d%d", size)}}, pin)
						n := 0
						for _, segs := range r {
							n += len(segs)
						}
						return n, err
					}},
				} {
					what := fmt.Sprintf("%s over %d matches", d.name, size)
					rbdEvict(t, g, all, rels)
					fctx, full := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{})
					if n, err := d.call(fctx); err != nil || n == 0 {
						t.Fatalf("%s: unlimited = %d, %v", what, n, err)
					}
					total := full.Work().DecodedBytes
					rbdEvict(t, g, all, rels)
					pctx, probe := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{DecodedBytes: 1})
					n, err := d.call(pctx)
					be2 := rbdBudgetErr(t, err, "decoded bytes")
					got := probe.Work().DecodedBytes
					// One row, not the matches: well under the matches' total. The scan
					// door decodes the label's every row, the others the matches'.
					rows := uint64(size)
					if d.name == "property scan" {
						rows = uint64(len(all))
					}
					if n != 0 || be2.Work.DecodedBytes <= 1 || probe.Work() != be2.Work || got > 2*(total/rows) {
						t.Fatalf("%s: budget 1 = %d rows, %+v, meter %+v; want no rows and one row's bytes (unlimited read %d over %d rows)", what, n, be2, probe.Work(), total, rows)
					}
				}
			}
		})
	}
}
