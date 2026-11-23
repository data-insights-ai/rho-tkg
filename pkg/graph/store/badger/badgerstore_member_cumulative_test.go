package badger

import (
	"context"
	"errors"
	"testing"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestMemberWindowAdmissionCumulativeAcrossKeys (R2, ai-soc review 14 Nov §3.2,
// TestRH02WindowAdmissionCumulativeAcrossKeys): three value keys with one
// entry each on disk; the window [100, 101) rejects the first two (valid
// later) and keeps the third. Unlimited, the read answers the one member. With
// Candidates 1 the walk judges three entries, two rejected, so it must refuse
// with the typed ReadBudgetError: the entries a walk judges count against the
// one limit of the read, across keys, and the walk stops at the entry that
// passes it (the iterator steps, which the meter does not keep, are 2 of the 3
// of the unlimited control). Node and relationship twins.
func TestMemberWindowAdmissionCumulativeAcrossKeys(t *testing.T) {
	keys := []string{types.IndexablePropertyValueKey("a"), types.IndexablePropertyValueKey("b"), types.IndexablePropertyValueKey("c")}
	window := &storecontract.PropertyTxMemberWindow{ValidStart: 100, ValidEnd: 101, TxAt: 0}
	valid := map[string]types.Instant{"a": 500, "b": 600, "c": 100}
	tm := func(v string) *types.TemporalMetadata {
		from := valid[v]
		return &types.TemporalMetadata{TxFrom: 1, ValidFrom: from, ValidTo: from + 1}
	}
	for _, kind := range []string{"node", "rel"} {
		t.Run(kind, func(t *testing.T) {
			// The relationship twin keeps its membership on disk only for a type
			// listed in RelPropertyIndexOnDiskTypes (else its RAM sidecar answers).
			relTypes := registrypkg.NewRelTypeRegistry()
			rt, _ := relTypes.GetOrCreate("R")
			bs, err := New(Config{InMemory: true, PropertyIndexOnDisk: true, PropertyKeyRegistry: registrypkg.NewPropertyKeyRegistry(), MaxPendingWrites: -1, FlushInterval: 1<<63 - 1,
				RelPropertyIndexOnDiskTypes: []string{"R"}, RelTypeNames: relTypes})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = bs.Close() })
			var read func(tally *storecontract.ReadTally, fn func()) error
			if kind == "node" {
				if err = bs.CreatePropertyIndex(1, "k"); err != nil {
					t.Fatal(err)
				}
				for i, v := range []string{"a", "b", "c"} {
					n := types.NewNode(types.NodeID(i+1), 1, []uint16{1})
					_ = n.SetProperty("k", v)
					n.SetTemporal(tm(v))
					if err = bs.PutNode(n); err != nil {
						t.Fatal(err)
					}
				}
				read = func(tally *storecontract.ReadTally, fn func()) error {
					return bs.ForEachNodePropertyTxMemberMetered(1, "k", keys, window, tally, func(types.NodeID, types.Instant) bool { fn(); return true })
				}
			} else {
				if err = bs.CreateRelPropertyIndex(rt, "k"); err != nil {
					t.Fatal(err)
				}
				for i := 1; i <= 2; i++ {
					if err = bs.PutNode(types.NewNode(types.NodeID(i), 1, []uint16{2})); err != nil {
						t.Fatal(err)
					}
				}
				for i, v := range []string{"a", "b", "c"} {
					r := types.NewRelationship(types.RelID(100+i), rt, 1, 2)
					_ = r.SetProperty("k", v)
					r.SetTemporal(tm(v))
					if err = bs.PutRelationship(r); err != nil {
						t.Fatal(err)
					}
				}
				read = func(tally *storecontract.ReadTally, fn func()) error {
					return bs.ForEachRelPropertyTxMemberMetered(rt, "k", keys, window, tally, func(types.RelID, types.Instant) bool { fn(); return true })
				}
			}
			if err = bs.Flush(); err != nil {
				t.Fatal(err)
			}
			// The native boundary: the iterator steps of the walk, which the meter does not keep.
			steps := 0
			bs.entityIndexWalkTestHook = func() { steps++ }
			got := 0
			if err := read(nil, func() { got++ }); err != nil || got != 1 || steps != 3 {
				t.Fatalf("unlimited: %d members, %v, %d iterator steps; want the one member of key c after the 3 entries", got, err, steps)
			}
			steps = 0
			_, meter := storecontract.WithReadBudget(context.Background(), storecontract.ReadBudget{Candidates: 1})
			got = 0
			err = read(meter.Tally("d"), func() { got++ })
			var rb *storecontract.ReadBudgetError
			if !errors.As(err, &rb) || rb.Limit != "candidates" || got != 0 {
				t.Fatalf("Candidates 1: %d members, %v, work %+v; want the candidates refusal, no member: three entries judged", got, err, meter.Work())
			}
			if steps != 2 {
				t.Fatalf("Candidates 1: %d iterator steps; want 2: the walk stops at the second rejected entry and never reads key c", steps)
			}
		})
	}
}
