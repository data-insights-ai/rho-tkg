package tiered

import (
	"context"
	"errors"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestReadBudget_ShardWindowAtEveryChargeSite (CUTCOST M3): each place that
// charges a closed event shard's open hands the shard's own window to the
// meter: the read-only open, the writable open and the lend's pre-count. Under
// a limited meter a window that misses the shard refuses each before the open
// and one that meets it opens the shard, counted, with the limit untouched.
func TestReadBudget_ShardWindowAtEveryChargeSite(t *testing.T) {
	t.Parallel()
	ts, _, n, old, _ := closedColdFixtureWith(t, volumeCfg(t.TempDir(), t.TempDir(), ""))
	defer func() { _ = ts.Close() }()
	es := ts.EventShardsForTest()[old]
	start, end := types.Instant(es.timeStart.UnixMilli()), types.Instant(es.timeEnd.UnixMilli())
	if start >= end {
		t.Fatalf("fixture: the cold shard's window is [%d, %d)", start, end)
	}
	closeIt := func() {
		t.Helper()
		es.LockShardMuForTest()
		defer es.UnlockShardMuForTest()
		if es.Store() == nil {
			return
		}
		if err := es.Store().Close(); err != nil {
			t.Fatal(err)
		}
		es.SetStoreForTest(nil)
	}
	out := make([]*types.Node, 1)
	sites := map[string]func(*storecontract.ReadMeter) error{
		"read-only open": func(m *storecontract.ReadMeter) error {
			_, release, err := es.checkoutForReadCharged(ts, true, m.Tally("Door"))
			release()
			return err
		},
		"writable open": func(m *storecontract.ReadMeter) error {
			_, release, err := es.checkoutForReadCharged(ts, false, m.Tally("Door"))
			release()
			return err
		},
		"lend": func(m *storecontract.ReadMeter) error {
			clear(out)
			return ts.LendNodesSortedMetered([]types.NodeID{n.ID()}, out, m)
		},
	}
	for name, open := range sites {
		closeIt()
		before := coldOpensOf(t, ts, old)
		_, miss := storecontract.WithReadBudget(context.Background(), storecontract.ReadBudget{})
		miss.LimitShardOpens()
		miss.AllowShardWindows([2]types.Instant{end, end + 1000}, [2]types.Instant{start - 1000, start - 1})
		var be *storecontract.ReadBudgetError
		if err := open(miss); !errors.As(err, &be) || be.Limit != "shard opens" || !be.ShardOpensLimited || be.ShardWindows != 2 {
			t.Fatalf("%s under windows beside the shard's [%d, %d): %v; want a refusal at shard opens naming the limit and 2 windows", name, start, end, err)
		}
		if es.Store() != nil || coldOpensOf(t, ts, old) != before {
			t.Fatalf("%s: the refused open opened the shard", name)
		}
		_, hit := storecontract.WithReadBudget(context.Background(), storecontract.ReadBudget{})
		hit.LimitShardOpens()
		hit.AllowShardWindows([2]types.Instant{start - 1000, start - 1}, [2]types.Instant{end - 1, end - 1})
		if err := open(hit); err != nil || hit.Work().ShardOpens != 1 {
			t.Fatalf("%s under a window meeting the shard's last instant: %v, work %+v; want it opened and counted once", name, err, hit.Work())
		}
		if err := hit.AddShardOpens("Door", 1); !errors.Is(err, storecontract.ErrReadBudget) {
			t.Fatalf("%s: an open without a window after the window's open: %v; want the limit of 0 still whole", name, err)
		}
	}
}
