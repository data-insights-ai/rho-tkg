package graph_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Round 11 item 4 (sigma-tkgd, 2026-10-17): the as-of doors at a pin <= 0.
// Every recorded row has a positive TxFrom (a backfill at or below 0 is
// ErrInvalidTxFrom, the commit clock is positive), so a transaction-time pin
// at or below 0 is an ordinary pin at which nothing was recorded yet: the
// effective-timeline doors, the paged effective doors, DocValues*AsOf, the
// value classes and the any-version reads at a pin answer the empty state
// there, as at a pin before the first write, instead of ErrInvalidTimeRange.

// r11PinDoors reads every door of the family at pin and reports what it saw:
// the number of rows / segments each door handed out, per door name.
func r11PinDoors(t *testing.T, g *graphpkg.Graph, pin types.Instant, n types.NodeID, r types.RelID, hub types.NodeID) (map[string]int, error) {
	t.Helper()
	seen := map[string]int{}
	var first error
	note := func(door string, rows int, err error) {
		if err != nil && first == nil {
			first = fmt.Errorf("%s: %w", door, err)
		}
		seen[door] += rows
	}
	countN := func(door string) func(temporal.NodeSegment) bool {
		return func(temporal.NodeSegment) bool { seen[door]++; return true }
	}
	countR := func(door string) func(temporal.RelSegment) bool {
		return func(temporal.RelSegment) bool { seen[door]++; return true }
	}
	tp := g.Temporal()
	segs, err := tp.NodeEffectiveTimeline(n, pin)
	note("NodeEffectiveTimeline", len(segs), err)
	rsegs, err := tp.RelEffectiveTimeline(r, pin)
	note("RelEffectiveTimeline", len(rsegs), err)
	note("ForEachNodeEffectiveByLabel", 0, tp.ForEachNodeEffectiveByLabel("P", pin, countN("ForEachNodeEffectiveByLabel")))
	note("ForEachRelEffectiveByType", 0, tp.ForEachRelEffectiveByType("E", pin, countR("ForEachRelEffectiveByType")))
	note("ForEachNodeEffective", 0, tp.ForEachNodeEffective(pin, countN("ForEachNodeEffective")))
	note("ForEachRelEffective", 0, tp.ForEachRelEffective(pin, countR("ForEachRelEffective")))
	note("NodesEffectiveByIDs", 0, tp.NodesEffectiveByIDs([]types.NodeID{n}, pin, countN("NodesEffectiveByIDs")))
	note("RelsEffectiveByIDs", 0, tp.RelsEffectiveByIDs([]types.RelID{r}, pin, countR("RelsEffectiveByIDs")))
	note("ForEachRelEffectiveAtNodes", 0, tp.ForEachRelEffectiveAtNodes([]types.NodeID{hub}, temporal.Outgoing, nil, pin, countR("ForEachRelEffectiveAtNodes")))
	note("ForEachNodeEffectiveByLabels", 0, tp.ForEachNodeEffectiveByLabels([]string{"P"}, pin, countN("ForEachNodeEffectiveByLabels")))
	note("ForEachNodeEffectiveByLabelsFrom", 0, tp.ForEachNodeEffectiveByLabelsFrom([]string{"P"}, pin, types.UnboundedValidFrom, countN("ForEachNodeEffectiveByLabelsFrom")))
	note("ForEachRelEffectiveByTypeFrom", 0, tp.ForEachRelEffectiveByTypeFrom("E", pin, types.UnboundedValidFrom, countR("ForEachRelEffectiveByTypeFrom")))
	all := temporal.Window{{From: types.UnboundedValidFrom, To: 1 << 62}}
	note("ForEachNodeEffectiveByLabelsWindow", 0, tp.ForEachNodeEffectiveByLabelsWindow([]string{"P"}, pin, all, countN("ForEachNodeEffectiveByLabelsWindow")))
	note("ForEachRelEffectiveByTypeWindow", 0, tp.ForEachRelEffectiveByTypeWindow("E", pin, all, countR("ForEachRelEffectiveByTypeWindow")))
	note("ForEachRelEffectiveAtNodesWindow", 0, tp.ForEachRelEffectiveAtNodesWindow([]types.NodeID{hub}, temporal.Outgoing, nil, pin, all, countR("ForEachRelEffectiveAtNodesWindow")))
	note("ScanRelEffectiveColumnsAtNodes", 0, tp.ScanRelEffectiveColumnsAtNodes([]types.NodeID{hub}, temporal.Outgoing, nil, pin, all, []string{"k"}, func(b *storepkg.RelColumnBatch) bool {
		seen["ScanRelEffectiveColumnsAtNodes"] += len(b.IDs)
		return true
	}))
	ctx := context.Background()
	np, _, err := tp.NodesEffectivePage(ctx, temporal.Pin{Tx: pin}, temporal.Filter{Prop: "k", Eq: int64(1)}, 0, 10)
	note("NodesEffectivePage", len(np), err)
	rp, _, err := tp.RelsEffectivePage(ctx, temporal.Pin{Tx: pin}, temporal.Filter{Prop: "k", Eq: int64(1)}, 0, 10)
	note("RelsEffectivePage", len(rp), err)
	note("Nodes.ForEachByIDsAnyVersion", 0, g.Nodes().ForEachByIDsAnyVersion([]types.NodeID{n}, pin, func(*types.Node) bool { seen["Nodes.ForEachByIDsAnyVersion"]++; return true }))
	note("Rels.ForEachByIDsAnyVersion", 0, g.Rels().ForEachByIDsAnyVersion([]types.RelID{r}, pin, func(*types.Relationship) bool { seen["Rels.ForEachByIDsAnyVersion"]++; return true }))
	cls, exact, err := g.Stats().PropertyTypeClassCountsAt("P", "k", pin)
	note("PropertyTypeClassCountsAt", int(cls.Numeric+cls.Missing), err)
	if err == nil && !exact {
		note("PropertyTypeClassCountsAt", 0, errors.New("not exact"))
	}
	rcls, _, err := g.Stats().RelPropertyTypeClassCountsAt("E", "k", pin)
	note("RelPropertyTypeClassCountsAt", int(rcls.Numeric+rcls.Missing), err)
	snap, _, ok, err := g.Nodes().DocValuesSnapshotAsOf("P", []string{"k"}, pin)
	rows := 0
	if err == nil && ok && snap != nil {
		vals, present := make([]any, 1), make([]bool, 1)
		if snap.Row(n, vals, present) {
			rows = 1
		}
	}
	note("DocValuesSnapshotAsOf", rows, err)
	_, _, err = g.Nodes().ForEachDocValuesAsOf("P", []string{"k"}, pin, func(types.NodeID, []any, []bool) bool { seen["ForEachDocValuesAsOf"]++; return true })
	note("ForEachDocValuesAsOf", 0, err)
	return seen, first
}

// TestRound11PinsAtOrBelowZero: on every backend, at the pins 0 and -5 every
// door of the family answers without an error and hands out nothing, while at
// pin 1 (a row backfilled at TxFrom 1, the first recordable instant) and at
// now the same doors see the rows (the positive control). The batched point
// doors take a negative pin as ordinary (NodeAtTx's rule); their 0 stays
// refused (0 is NodeAtTx's "no pin"; a pin at 0 is NodesAtOptsByIDs).
func TestRound11PinsAtOrBelowZero(t *testing.T) {
	for _, b := range allStoreBackendsWith(func(c *graphpkg.Config) { c.AllowTxBackfill = true }) {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			ctx := context.Background()
			hub, err := g.Nodes().AddWithTx(ctx, []string{"Hub"}, nil, 1)
			if err != nil {
				t.Fatal(err)
			}
			n, err := g.Nodes().AddWithTx(ctx, []string{"P"}, map[string]any{"k": int64(1)}, 1)
			if err != nil {
				t.Fatal(err)
			}
			r, err := g.Rels().AddWithTx(ctx, "E", hub, n, map[string]any{"k": int64(1)}, 1)
			if err != nil {
				t.Fatal(err)
			}
			now, err := g.Temporal().NowTx()
			if err != nil {
				t.Fatal(err)
			}
			for _, pin := range []types.Instant{1, now} {
				seen, err := r11PinDoors(t, g, pin, n.ID(), r.ID(), hub.ID())
				if err != nil {
					t.Fatalf("pin %d (positive control): %v", pin, err)
				}
				for door, rows := range seen {
					if rows == 0 {
						t.Errorf("pin %d (positive control): %s saw nothing", pin, door)
					}
				}
			}
			for _, pin := range []types.Instant{0, -5} {
				seen, err := r11PinDoors(t, g, pin, n.ID(), r.ID(), hub.ID())
				if err != nil {
					t.Errorf("pin %d: %v, want the empty state", pin, err)
				}
				for door, rows := range seen {
					if rows != 0 {
						t.Errorf("pin %d: %s handed out %d rows, want none (nothing was recorded by then)", pin, door, rows)
					}
				}
				if pin < 0 {
					got, err := g.Temporal().NodesAtTxByIDs([]types.NodeID{n.ID()}, 0, pin)
					if err != nil || len(got) != 1 || got[0] != nil {
						t.Errorf("NodesAtTxByIDs at pin %d: %v %v, want one nil", pin, got, err)
					}
					rgot, err := g.Temporal().RelsAtTxByIDs([]types.RelID{r.ID()}, 0, pin)
					if err != nil || len(rgot) != 1 || rgot[0] != nil {
						t.Errorf("RelsAtTxByIDs at pin %d: %v %v, want one nil", pin, rgot, err)
					}
				}
			}
			if _, err := g.Temporal().NodesAtTxByIDs([]types.NodeID{n.ID()}, 0, 0); !errors.Is(err, graphpkg.ErrInvalidTimeRange) {
				t.Errorf("NodesAtTxByIDs at txAt 0: %v, want ErrInvalidTimeRange (0 is NodeAtTx's \"no pin\")", err)
			}
			// A pin above the commit clock stays ErrTxPinTooNew.
			if _, err := g.Temporal().NodeEffectiveTimeline(n.ID(), now+1<<40); !errors.Is(err, graphpkg.ErrTxPinTooNew) {
				t.Errorf("timeline above the clock: %v, want ErrTxPinTooNew", err)
			}
		})
	}
}
