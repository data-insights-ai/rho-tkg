package tiered

import (
	"context"
	"errors"
	"slices"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestTieredMeteredMembershipCountsWhatItHandsOn (R2, ai-soc's host lookup,
// which calls the store capability itself, and the graph doors, which call it
// the same way): after its separately tested first build, each shard counts
// the members it collects as candidates, across value keys and shards, before
// the union hands them on, and
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
	var want []types.NodeID
	for _, v := range []int64{1, 2} {
		n := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
		want = append(want, n.ID())
		_ = n.SetProperty("seat", v)
		if err := ts.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	keys := []string{types.IndexablePropertyValueKey(int64(1)), types.IndexablePropertyValueKey(int64(2))}
	_, buildMeter := storecontract.WithReadBudget(context.Background(), storecontract.ReadBudget{})
	var control []types.NodeID
	err := ts.ForEachNodePropertyTxMemberMetered(caseTok, "seat", keys, nil, buildMeter.Tally("build"), func(id types.NodeID, _ types.Instant) bool {
		control = append(control, id)
		return true
	})
	slices.Sort(control)
	slices.Sort(want)
	if err != nil || !slices.Equal(control, want) {
		t.Fatalf("first-build control: %v, %v; want %v", control, err, want)
	}
	_, meter := storecontract.WithReadBudget(context.Background(), storecontract.ReadBudget{Candidates: 1})
	callbacks := 0
	err = ts.ForEachNodePropertyTxMemberMetered(caseTok, "seat", keys, nil, meter.Tally("direct"), func(types.NodeID, types.Instant) bool {
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
