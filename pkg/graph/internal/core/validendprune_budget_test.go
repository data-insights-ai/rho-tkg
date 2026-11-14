package core

import (
	"errors"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The helper used after a property lookup must retain its tally when it
// resolves history. Each store has the same exact five-segment timeline; a
// shared history limit stops the first entity before the second is read.
func TestValidEndReturnedRowAdmitsHistory(t *testing.T) {
	for name, g := range validEndStores(t) {
		t.Run(name, func(t *testing.T) {
			var rows []*types.Node
			for range 2 {
				n, err := g.Nodes.Add(t.Context(), []string{"D"}, map[string]any{"tkg_valid_from": types.Instant(100)})
				if err != nil {
					t.Fatal(err)
				}
				for i := range 4 {
					n, err = g.Nodes.Update(t.Context(), n.ID(), map[string]any{"tkg_valid_from": types.Instant(200 + i*100)})
					if err != nil {
						t.Fatal(err)
					}
				}
				rows = append(rows, n)
			}
			read := func(budget storepkg.ReadBudget) (int, int, storepkg.ReadWork, error) {
				_, m := storepkg.WithReadBudget(t.Context(), budget)
				currentReads, segments := 0, 0
				g.currentNodeReadTestHook = func() { currentReads++ }
				defer func() { g.currentNodeReadTestHook = nil }()
				err := g.readUnderRLock(func() error {
					rd, release := g.meteredEntityReads(m.Tally("valid-end-timeline"))
					defer release()
					rd.scan = true
					pin := g.peekNow()
					for _, row := range rows {
						segs, err := g.nodeTimelineOfReturnedRow(rd, row, pin)
						if err != nil {
							return err
						}
						if len(segs) != 5 {
							t.Errorf("node %d: segments %d; want five", row.ID(), len(segs))
						}
						segments += len(segs)
					}
					return nil
				})
				return segments, currentReads, m.Work(), err
			}
			segments, reads, work, err := read(storepkg.ReadBudget{})
			if err != nil || segments != 10 || reads != 2 || work.HistoryExamined == 0 {
				t.Fatalf("control: segments %d, native current reads %d, work %+v, err %v; want two complete five-segment timelines", segments, reads, work, err)
			}
			segments, reads, work, err = read(storepkg.ReadBudget{HistoryExamined: 1})
			var refusal *storepkg.ReadBudgetError
			if !errors.As(err, &refusal) || refusal.Limit != "history examined" || segments != 0 || reads != 1 || work.HistoryExamined <= 1 {
				t.Errorf("limited: segments %d, native current reads %d, work %+v, err %v; want refusal while reading the first timeline", segments, reads, work, err)
			}
		})
	}
}
