package tiered

import (
	"errors"
	"slices"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	badgerstore "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestReadBudget_ColdExAdjRepairRefusesBeforeScan(t *testing.T) {
	// Serial: the native row decoder counter is process-wide.
	ts, node, end, shard, tok := coldROFixture(t)
	id := types.RelID(tieredRelGen(t).Generate())
	if err := ts.NoteExAdjacentRel(id, node.ID(), end); err != nil {
		t.Fatal(err)
	}
	closeCold := func() {
		es := ts.EventShardsForTest()[shard]
		es.shardMu.Lock()
		defer es.shardMu.Unlock()
		if es.store != nil {
			if err := es.store.Close(); err != nil {
				t.Fatal(err)
			}
			es.clearStoreLocked(ts)
		}
	}
	run := func(tally *storecontract.ReadTally) ([]types.RelID, error) {
		var ids []types.RelID
		err := ts.ForEachExAdjacentRelIDMetered(node.ID(), false, tally, func(id types.RelID) bool {
			ids = append(ids, id)
			return true
		})
		return ids, err
	}
	want := []types.RelID{id}
	closeCold()
	_, all := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{})
	if ids, err := run(all.Tally("exadj")); err != nil || !slices.Equal(ids, want) {
		t.Fatalf("unlimited exadj control = %v, %v; want %v", ids, err, want)
	}
	closeCold()
	if err := ts.refShard.CreatePropertyIndex(tok, "late"); err != nil {
		t.Fatal(err)
	}
	_, meter := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{DecodedBytes: 1})
	var ids []types.RelID
	var err error
	decoded := badgerstore.CountRowDecodesForTest(func() { ids, err = run(meter.Tally("exadj")) })
	var rb *storecontract.ReadBudgetError
	if len(ids) != 0 || !errors.As(err, &rb) || rb.Limit != "unsupported" || decoded != 0 || meter.Exceeded() == nil {
		t.Fatalf("exadj with pending repair = %v, %v, %d native decodes; want shared refusal before repair", ids, err, decoded)
	}
	if ids, err := run(nil); err != nil || !slices.Equal(ids, want) {
		t.Fatalf("plain exadj repair = %v, %v; want %v", ids, err, want)
	}
	closeCold()
	_, exact := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{ShardOpens: 1})
	if ids, err := run(exact.Tally("exadj")); err != nil || !slices.Equal(ids, want) || exact.Work().ShardOpens != 1 {
		t.Fatalf("bounded exadj retry = %v, %v, work %+v; want %v with one open", ids, err, exact.Work(), want)
	}
}

// A cold read must not repair interrupted index DDL by scanning rows outside
// its budget. A plain read can finish the repair; a metered retry then returns
// the same row without doing that maintenance work.
func TestReadBudget_ColdIndexRepairRefusesBeforeScan(t *testing.T) {
	// Serial: the row decoder counter is process-wide.
	for _, kind := range []string{"property", "composite"} {
		t.Run(kind, func(t *testing.T) {
			ts, want, _, shard, tok := coldROFixture(t)
			var err error
			if kind == "property" {
				err = ts.refShard.CreatePropertyIndex(tok, "late")
			} else {
				err = ts.refShard.CreateCompositePropertyIndex(tok, []string{"late", "other"})
			}
			if err != nil {
				t.Fatal(err)
			}
			ctx, outer := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{DecodedBytes: 1})
			_, nested := storecontract.WithReadBudget(ctx, storecontract.ReadBudget{})
			// Use an outer limit and an unlimited inner meter: maintenance must
			// refuse on the shared read, not only on the innermost budget.
			snap := ts.NewReadSnapshot()
			snap.(storecontract.ReadSnapshotMeterCapability).MeterReads(nested.Tally("cold repair"))
			var got *types.Node
			decoded := badgerstore.CountRowDecodesForTest(func() { got, err = snap.GetNode(want.ID()) })
			snap.Close()
			var refused *storecontract.ReadBudgetError
			if got != nil || !errors.As(err, &refused) || refused.Limit != "unsupported" || refused.Door != "cold repair" {
				t.Errorf("read = %v, %v; want typed unsupported refusal before index repair", got, err)
			}
			if decoded != 0 {
				t.Errorf("refused read decoded %d rows while repairing the index", decoded)
			}
			if outer.Exceeded() == nil || nested.Exceeded() == nil {
				t.Errorf("refusal not shared: outer %v, inner %v", outer.Exceeded(), nested.Exceeded())
			}
			if open, _ := storeIsRO(ts.EventShardsForTest()[shard]); open {
				t.Error("refused maintenance left a cold handle installed")
			}

			if got, err := ts.GetNode(want.ID()); err != nil || got == nil || got.ID() != want.ID() {
				t.Fatalf("plain repair read = %v, %v; want node %d", got, err, want.ID())
			}
			_, control := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{})
			snap = ts.NewReadSnapshot()
			snap.(storecontract.ReadSnapshotMeterCapability).MeterReads(control.Tally("cold repair"))
			got, err = snap.GetNode(want.ID())
			snap.Close()
			if err != nil || got == nil || got.ID() != want.ID() {
				t.Fatalf("metered retry after repair = %v, %v; want node %d", got, err, want.ID())
			}
			_, bounded := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{DecodedBytes: max(1, control.Work().DecodedBytes)})
			snap = ts.NewReadSnapshot()
			snap.(storecontract.ReadSnapshotMeterCapability).MeterReads(bounded.Tally("cold repair"))
			got, err = snap.GetNode(want.ID())
			snap.Close()
			if err != nil || got == nil || got.ID() != want.ID() {
				t.Fatalf("bounded retry after repair = %v, %v; want node %d", got, err, want.ID())
			}
		})
	}
}
