package tiered

import (
	"context"
	"errors"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestReadBudget_LendCountsAndBoundsShardOpens (RH list R2, sigma's S7): a
// by-ID lend under a read budget counts each closed shard it opens; when the
// next open would pass the budget's ShardOpens it refuses before opening, so
// the shard stays closed. A retained shard opens nothing and counts nothing.
func TestReadBudget_LendCountsAndBoundsShardOpens(t *testing.T) {
	t.Parallel()
	cfg := volumeCfg(t.TempDir(), t.TempDir(), "")
	cfg.MaxOpenColdShards = 1
	ts, _, n, old, _ := closedColdFixtureWith(t, cfg)
	defer func() { _ = ts.Close() }()
	ids := []types.NodeID{n.ID()}
	out := make([]*types.Node, 1)

	// The budget is spent: refused, the shard not opened.
	ctx, spent := storecontract.WithReadBudget(context.Background(), storecontract.ReadBudget{ShardOpens: 1})
	if err := storecontract.ReadMeterFrom(ctx).AddShardOpens("earlier", 1); err != nil {
		t.Fatal(err)
	}
	err := ts.LendNodesSortedMetered(ids, out, spent)
	var be *storecontract.ReadBudgetError
	if !errors.Is(err, storecontract.ErrReadBudget) || !errors.As(err, &be) || be.Door != "LendNodesSorted" || be.Limit != "shard opens" {
		t.Fatalf("spent budget: %v", err)
	}
	if ts.EventShardsForTest()[old].Store() != nil || coldOpensOf(t, ts, old) != 0 {
		t.Fatal("a refused lend opened the shard")
	}

	// Room for one open: lent and counted once; the retained shard again: no open.
	_, meter := storecontract.WithReadBudget(context.Background(), storecontract.ReadBudget{ShardOpens: 1})
	for range 2 {
		clear(out)
		if err := ts.LendNodesSortedMetered(ids, out, meter); err != nil || out[0] == nil || out[0].ID() != n.ID() {
			t.Fatalf("lend = %v, %v", out[0], err)
		}
	}
	if w := meter.Work(); w.ShardOpens != 1 || coldOpensOf(t, ts, old) != 1 {
		t.Fatalf("work %+v, ColdOpens %d; want 1 open", w, coldOpensOf(t, ts, old))
	}

	// No meter: today's door.
	clear(out)
	if err := ts.LendNodesSorted(ids, out); err != nil || out[0] == nil {
		t.Fatalf("unmetered lend = %v, %v", out[0], err)
	}
}
