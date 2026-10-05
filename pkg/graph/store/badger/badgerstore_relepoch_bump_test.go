package badger

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Every relationship-writing store door moves RelMutationEpoch, and
// RelMutationEpochForType for the written type (the relationship columns'
// freshness stamp), and a shared counter handed in through
// Config.SharedMutationEpochs moves with them. Before v4.41 the history doors
// (a property write, a delete with a tombstone, a version write), the
// node-delete cascades and the cross-shard split helpers moved neither.
func TestRelationshipDoorsBumpRelMutationEpochs(t *testing.T) {
	type door struct {
		name string
		run  func(t *testing.T, bs *Store, r *types.Relationship) error
	}
	tomb := func(r *types.Relationship) *types.Relationship {
		c := r.DeepCopy()
		c.SetTemporal(&types.TemporalMetadata{DeletedAt: 5, ValidTo: 5, TxFrom: 5, TxTo: 5})
		return c
	}
	doors := []door{
		{"ReplaceRelWithHistory", func(_ *testing.T, bs *Store, r *types.Relationship) error {
			next := r.DeepCopy()
			next.SetVersion(r.Version() + 1)
			if err := next.SetProperty("w", int64(2)); err != nil {
				return err
			}
			return bs.ReplaceRelWithHistory(next, r.Version(), r)
		}},
		{"DeleteRelWithHistory", func(_ *testing.T, bs *Store, r *types.Relationship) error {
			return bs.DeleteRelWithHistory(r.ID(), r.Version(), tomb(r))
		}},
		{"PutRelVersion", func(_ *testing.T, bs *Store, r *types.Relationship) error {
			return bs.PutRelVersion(r.ID(), r.Version(), r)
		}},
		{"DeleteNodeCascade", func(_ *testing.T, bs *Store, r *types.Relationship) error {
			return bs.DeleteNodeCascade(r.StartNodeID())
		}},
		{"DeleteNodeWithHistory", func(t *testing.T, bs *Store, r *types.Relationship) error {
			n, err := bs.GetNode(r.StartNodeID())
			if err != nil {
				return err
			}
			nt := n.DeepCopy()
			nt.SetTemporal(&types.TemporalMetadata{DeletedAt: 5, ValidTo: 5, TxFrom: 5, TxTo: 5})
			return bs.DeleteNodeWithHistory(n.ID(), n.Version(), nt,
				[]RelTombstone{{ID: r.ID(), PrevVersion: r.Version(), Tombstone: tomb(r)}})
		}},
		{"DeleteRelEntityAndOut", func(_ *testing.T, bs *Store, r *types.Relationship) error {
			_, err := bs.DeleteRelEntityAndOut(r.ID().SnowflakeID())
			return err
		}},
		{"DeleteRelIncoming", func(_ *testing.T, bs *Store, r *types.Relationship) error {
			return bs.DeleteRelIncoming(relDeleteInfoFromRelationship(r))
		}},
		{"DeleteIncomingByRelID", func(_ *testing.T, bs *Store, r *types.Relationship) error {
			return bs.DeleteIncomingByRelID(r.EndNodeID().SnowflakeID(), r.ID().SnowflakeID())
		}},
		{"ScanAndDeleteIncoming", func(_ *testing.T, bs *Store, r *types.Relationship) error {
			return bs.ScanAndDeleteIncoming(r.EndNodeID().SnowflakeID(), r.ID().SnowflakeID())
		}},
		{"PutRelEntityAndOut", func(t *testing.T, bs *Store, r *types.Relationship) error {
			fresh := types.NewRelationship(types.RelID(newTestGen(t, 3).Generate()), r.TypeToken().Value(), r.StartNodeID(), r.EndNodeID())
			return bs.PutRelEntityAndOut(fresh)
		}},
		{"PutRelIncoming", func(t *testing.T, bs *Store, r *types.Relationship) error {
			return bs.PutRelIncoming(r.EndNodeID().SnowflakeID(), r.StartNodeID().SnowflakeID(), r.TypeToken().Value(), newTestGen(t, 5).Generate())
		}},
	}
	for _, d := range doors {
		t.Run(d.name, func(t *testing.T) {
			shared := &SharedMutationEpochs{}
			bs, err := New(Config{InMemory: true, FlushInterval: 1<<63 - 1, SharedMutationEpochs: shared})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = bs.Close() })
			gen := newTestGen(t, 0)
			n1 := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
			n2 := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
			for _, n := range []*types.Node{n1, n2} {
				if err := bs.PutNode(n); err != nil {
					t.Fatal(err)
				}
			}
			r := types.NewRelationship(types.RelID(newTestGen(t, 1).Generate()), 7, n1.ID(), n2.ID())
			if err := r.SetProperty("w", int64(1)); err != nil {
				t.Fatal(err)
			}
			if err := bs.PutRelationship(r); err != nil {
				t.Fatal(err)
			}
			global, perType, sharedBefore := bs.RelMutationEpoch(), bs.RelMutationEpochForType(7), shared.RelMutationEpoch()
			if err := d.run(t, bs, r); err != nil {
				t.Fatalf("%s: %v", d.name, err)
			}
			if bs.RelMutationEpoch() == global {
				t.Errorf("%s left RelMutationEpoch at %d", d.name, global)
			}
			if bs.RelMutationEpochForType(7) == perType {
				t.Errorf("%s left RelMutationEpochForType(7) at %d", d.name, perType)
			}
			if shared.RelMutationEpoch() == sharedBefore {
				t.Errorf("%s left the shared relationship epoch at %d", d.name, sharedBefore)
			}
		})
	}
}

// A nil *SharedMutationEpochs reads as zero and is never written.
func TestSharedMutationEpochsNil(t *testing.T) {
	var e *SharedMutationEpochs
	e.node()
	e.rel()
	if e.NodeMutationEpoch() != 0 || e.RelMutationEpoch() != 0 {
		t.Fatal("nil SharedMutationEpochs must read 0")
	}
	var z SharedMutationEpochs
	z.node()
	z.rel()
	z.rel()
	if z.NodeMutationEpoch() != 1 || z.RelMutationEpoch() != 2 {
		t.Fatalf("zero value counts = %d/%d, want 1/2", z.NodeMutationEpoch(), z.RelMutationEpoch())
	}
}
