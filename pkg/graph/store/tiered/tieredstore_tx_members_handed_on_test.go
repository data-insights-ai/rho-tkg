package tiered

import (
	"context"
	"errors"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestTieredMeteredMembershipCountsWhatItHandsOn (R2, ai-soc's host lookup,
// which calls the store capability itself, and the graph doors, which call it
// the same way): on tiered each shard counts the members it collects as
// candidates, across value keys and shards, before the union hands them on, and
// nothing above counts them again. Two keys of one member each under Candidates
// 1 refuse with no callback (the union hands on only after every shard
// answered); under Candidates 2 the read answers both members and counts
// exactly 2.
func TestTieredMeteredMembershipCountsWhatItHandsOn(t *testing.T) {
	ts := newTestTieredStore(t)
	caseTok, _, _ := installDefaultTestLabelRegistry(t, ts)
	if err := ts.CreatePropertyIndex(caseTok, "seat"); err != nil {
		t.Fatal(err)
	}
	gen := tieredNodeGen(t)
	for _, v := range []int64{1, 2} {
		n := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
		_ = n.SetProperty("seat", v)
		if err := ts.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	keys := []string{types.IndexablePropertyValueKey(int64(1)), types.IndexablePropertyValueKey(int64(2))}
	_, meter := storecontract.WithReadBudget(context.Background(), storecontract.ReadBudget{Candidates: 1})
	callbacks := 0
	err := ts.ForEachNodePropertyTxMemberMetered(caseTok, "seat", keys, nil, meter.Tally("direct"), func(types.NodeID, types.Instant) bool {
		callbacks++
		return true
	})
	var rb *storecontract.ReadBudgetError
	if callbacks != 0 || !errors.As(err, &rb) || rb.Limit != "candidates" || meter.Work() != rb.Work || meter.Work().Candidates != 2 {
		t.Fatalf("direct: %d callbacks, %v, work %+v; want the candidates refusal, 2 counted, no callback", callbacks, err, meter.Work())
	}
	_, meter = storecontract.WithReadBudget(context.Background(), storecontract.ReadBudget{Candidates: 2})
	callbacks = 0
	err = ts.ForEachNodePropertyTxMemberMetered(caseTok, "seat", keys, nil, meter.Tally("direct"), func(types.NodeID, types.Instant) bool {
		callbacks++
		return true
	})
	if err != nil || callbacks != 2 || meter.Work().Candidates != 2 {
		t.Fatalf("Candidates 2: %d callbacks, %v, work %+v; want both members handed on, each counted once", callbacks, err, meter.Work())
	}
}
