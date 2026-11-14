package sharded

import (
	"errors"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

type snapshotAsOfWalk interface {
	NodeAsOfWalk(types.NodeID, types.Instant) (*types.Node, bool, error)
	RelAsOfWalk(types.RelID, types.Instant) (*types.Relationship, bool, error)
}

func TestShardedSnapshotAsOfSharesAdmission(t *testing.T) {
	s := newMemStore(t, 0, 2)
	for slot := uint8(0); slot < 2; slot++ {
		n := types.NewNode(mkNodeID(slot, 1), 1, nil)
		n.SetTemporal(&types.TemporalMetadata{TxFrom: 10, ValidFrom: 1})
		if err := s.PutNode(n); err != nil {
			t.Fatal(err)
		}
		r := types.NewRelationship(mkRelID(slot, 2), 1, n.ID(), n.ID())
		r.SetTemporal(&types.TemporalMetadata{TxFrom: 10, ValidFrom: 1})
		if err := s.PutRelationship(r); err != nil {
			t.Fatal(err)
		}
	}
	snap := s.NewReadSnapshot()
	defer snap.Close()
	walk, ok := snap.(snapshotAsOfWalk)
	if !ok {
		t.Fatal("snapshot has no admitted owner as-of walk")
	}
	for _, rel := range []bool{false, true} {
		calls := 0
		for _, bs := range s.shards {
			bs.SetAsOfAfterCurrentTestHookForTest(func() { calls++ })
			defer bs.SetAsOfAfterCurrentTestHookForTest(nil)
		}
		read := func(slot uint8) error {
			if rel {
				r, ok, err := walk.RelAsOfWalk(mkRelID(slot, 2), 20)
				if err == nil && (!ok || r == nil || r.ID() != mkRelID(slot, 2)) {
					t.Fatalf("slot %d: rel %v, ok %v", slot, r, ok)
				}
				return err
			}
			n, ok, err := walk.NodeAsOfWalk(mkNodeID(slot, 1), 20)
			if err == nil && (!ok || n == nil || n.ID() != mkNodeID(slot, 1)) {
				t.Fatalf("slot %d: node %v, ok %v", slot, n, ok)
			}
			return err
		}
		_, full := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{})
		snap.(storecontract.ReadSnapshotMeterCapability).MeterReads(full.Tally("as-of"))
		for slot := uint8(0); slot < 2; slot++ {
			if err := read(slot); err != nil {
				t.Fatal(err)
			}
		}
		if calls != 2 || full.Work().Candidates != 4 {
			t.Fatalf("control rel=%v: %d native reads, work %+v; want two current reads and two proof probes", rel, calls, full.Work())
		}
		calls = 0
		_, limited := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{Candidates: 2})
		snap.(storecontract.ReadSnapshotMeterCapability).MeterReads(limited.Tally("as-of"))
		if err := read(0); err != nil {
			t.Fatal(err)
		}
		if err := read(1); !errors.Is(err, storecontract.ErrReadBudget) || calls != 1 || limited.Work().Candidates != 3 {
			t.Fatalf("two slots rel=%v: %d native reads, work %+v, err %v; want refusal before second owner read", rel, calls, limited.Work(), err)
		}
	}
}
