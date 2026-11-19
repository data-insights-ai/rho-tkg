package tiered

import (
	"context"
	"errors"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestPurgeNodesByIDs_MeterChargesBeforeOpen (PURGEIDS): a purge of nodes on
// two closed cold shards under a meter with room for one open is refused
// before any open and removes nothing; with room it opens each shard once,
// counted by the meter as by the store, and the nodes are gone.
func TestPurgeNodesByIDs_MeterChargesBeforeOpen(t *testing.T) {
	ts, shards, nodes := holdFixture(t, 2)
	ids := []types.NodeID{nodes[0].ID(), nodes[1].ID()}
	coldOpens := func() (n int64) {
		for _, es := range shards {
			n += es.coldOpens.Load()
		}
		return n
	}
	_, lm := storecontract.WithReadBudget(context.Background(), storecontract.ReadBudget{ShardOpens: 1})
	_, err := ts.PurgeNodesByIDs(ids, lm.Tally("PurgeByIDs"))
	var be *storecontract.ReadBudgetError
	if !errors.As(err, &be) {
		t.Fatalf("limited meter: err %v, want a read budget refusal", err)
	}
	if n := coldOpens(); n != 0 {
		t.Fatalf("refused purge opened %d cold shards, want 0", n)
	}

	_, m := storecontract.WithReadBudget(context.Background(), storecontract.ReadBudget{})
	res, err := ts.PurgeNodesByIDs(ids, m.Tally("PurgeByIDs"))
	if err != nil {
		t.Fatal(err)
	}
	if got, counted := coldOpens(), int64(m.Work().ShardOpens); res.NodesPurged != 2 || got != 2 || counted != 2 {
		t.Fatalf("purged %d, cold opens %d, meter counted %d; want 2, 2, 2 (the refusal removed nothing)", res.NodesPurged, got, counted)
	}
	for _, id := range ids {
		if _, err := ts.GetNode(id); !errors.Is(err, ErrNodeNotFound) {
			t.Fatalf("purged node: %v", err)
		}
	}
}
