package badger

import (
	"errors"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	registrypkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/internal/registry"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
)

type readOnlySeed struct {
	n1  *types.Node
	n2  *types.Node
	n3  *types.Node
	n4  *types.Node
	rel *types.Relationship
}

func newReadOnlySeededBadgerStore(t *testing.T) (*Store, readOnlySeed) {
	t.Helper()

	dir := t.TempDir()
	seedStore, err := New(Config{Dir: dir, SyncWrites: true})
	if err != nil {
		t.Fatalf("New seed store: %v", err)
	}

	seed := readOnlySeed{
		n1: types.NewNode(types.NodeID(snowflake.ID(1)), 1, nil),
		n2: types.NewNode(types.NodeID(snowflake.ID(2)), 1, nil),
		n3: types.NewNode(types.NodeID(snowflake.ID(3)), 1, nil),
		n4: types.NewNode(types.NodeID(snowflake.ID(4)), 1, []uint16{2}),
	}
	for _, n := range []*types.Node{seed.n1, seed.n2, seed.n3, seed.n4} {
		if err := seedStore.PutNode(n); err != nil {
			t.Fatalf("PutNode seed %d: %v", n.ID(), err)
		}
	}
	seed.rel = types.NewRelationship(types.RelID(snowflake.ID(100)), 1, seed.n1.ID(), seed.n2.ID())
	if err := seedStore.PutRelationship(seed.rel); err != nil {
		t.Fatalf("PutRelationship seed: %v", err)
	}
	if err := seedStore.Close(); err != nil {
		t.Fatalf("Close seed store: %v", err)
	}

	readOnlyStore, err := New(Config{Dir: dir, ReadOnly: true})
	if err != nil {
		t.Fatalf("New read-only store: %v", err)
	}
	t.Cleanup(func() {
		_ = readOnlyStore.Close()
	})
	return readOnlyStore, seed
}

func assertReadOnlySeedUnchanged(t *testing.T, bs *Store, seed readOnlySeed) {
	t.Helper()

	if got, err := bs.NodeCount(); err != nil || got != 4 {
		t.Fatalf("NodeCount after read-only mutation = %d, %v; want 4, nil", got, err)
	}
	if got, err := bs.RelationshipCount(); err != nil || got != 1 {
		t.Fatalf("RelationshipCount after read-only mutation = %d, %v; want 1, nil", got, err)
	}
	if _, err := bs.GetNode(types.NodeID(snowflake.ID(99))); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("GetNode(new ID) after read-only mutation = %v, want ErrNodeNotFound", err)
	}
	if _, err := bs.GetRelationship(types.RelID(snowflake.ID(101))); !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("GetRelationship(new ID) after read-only mutation = %v, want ErrRelNotFound", err)
	}
	if got, err := bs.GetNode(seed.n3.ID()); err != nil || got.ID() != seed.n3.ID() {
		t.Fatalf("GetNode(seed n3) after read-only mutation = (%v, %v), want original", got, err)
	}
	if got, err := bs.GetRelationship(seed.rel.ID()); err != nil || got.ID() != seed.rel.ID() {
		t.Fatalf("GetRelationship(seed rel) after read-only mutation = (%v, %v), want original", got, err)
	}
	incoming := bs.IncomingRelIDs(seed.n2.ID().SnowflakeID(), 1)
	if len(incoming) != 1 || incoming[0] != seed.rel.ID().SnowflakeID() {
		t.Fatalf("IncomingRelIDs after read-only mutation = %v, want [%d]", incoming, seed.rel.ID().SnowflakeID())
	}
}

func TestBadgerStoreReadOnlyRejectsMutatingMethods(t *testing.T) {
	labels := registrypkg.NewLabelRegistry()
	relTypes := registrypkg.NewRelTypeRegistry()

	tests := []struct {
		name string
		run  func(*Store, readOnlySeed) error
	}{
		{name: "Clear", run: func(bs *Store, _ readOnlySeed) error { return bs.Clear() }},
		{name: "PutNode", run: func(bs *Store, _ readOnlySeed) error {
			return bs.PutNode(types.NewNode(types.NodeID(snowflake.ID(99)), 1, nil))
		}},
		{name: "ReplaceNode", run: func(bs *Store, seed readOnlySeed) error {
			return bs.ReplaceNode(seed.n3.DeepCopy())
		}},
		{name: "DeleteNode", run: func(bs *Store, seed readOnlySeed) error { return bs.DeleteNode(seed.n3.ID()) }},
		{name: "DeleteNodeCascade", run: func(bs *Store, seed readOnlySeed) error { return bs.DeleteNodeCascade(seed.n3.ID()) }},
		{name: "PutNodesBatch", run: func(bs *Store, _ readOnlySeed) error {
			return bs.PutNodesBatch([]*types.Node{types.NewNode(types.NodeID(snowflake.ID(99)), 1, nil)})
		}},
		{name: "DeleteNodesBatch", run: func(bs *Store, seed readOnlySeed) error {
			return bs.DeleteNodesBatch([]types.NodeID{seed.n3.ID()})
		}},
		{name: "RemoveNodeLabelToken", run: func(bs *Store, seed readOnlySeed) error {
			updated := seed.n4.DeepCopy()
			updated.RemoveLabelTokenRaw(2)
			return bs.RemoveNodeLabelToken(seed.n4.ID(), 2, updated)
		}},
		{name: "AddNodeLabelToken", run: func(bs *Store, seed readOnlySeed) error {
			updated := seed.n3.DeepCopy()
			updated.AddLabelTokenRaw(2)
			return bs.AddNodeLabelToken(seed.n3.ID(), 2, updated)
		}},
		{name: "PutRelationship", run: func(bs *Store, seed readOnlySeed) error {
			r := types.NewRelationship(types.RelID(snowflake.ID(101)), 1, seed.n1.ID(), seed.n2.ID())
			return bs.PutRelationship(r)
		}},
		{name: "ReplaceRelationship", run: func(bs *Store, seed readOnlySeed) error {
			return bs.ReplaceRelationship(seed.rel.DeepCopy())
		}},
		{name: "DeleteRelationship", run: func(bs *Store, seed readOnlySeed) error {
			return bs.DeleteRelationship(seed.rel.ID())
		}},
		{name: "PutRelationshipsBatch", run: func(bs *Store, seed readOnlySeed) error {
			r := types.NewRelationship(types.RelID(snowflake.ID(101)), 1, seed.n1.ID(), seed.n2.ID())
			return bs.PutRelationshipsBatch([]*types.Relationship{r})
		}},
		{name: "DeleteRelationshipsBatch", run: func(bs *Store, seed readOnlySeed) error {
			return bs.DeleteRelationshipsBatch([]types.RelID{seed.rel.ID()})
		}},
		{name: "RemoveNodeLabelTokenWithHistory", run: func(bs *Store, seed readOnlySeed) error {
			updated := seed.n4.DeepCopy()
			updated.RemoveLabelTokenRaw(2)
			return bs.RemoveNodeLabelTokenWithHistory(seed.n4.ID(), 2, updated, seed.n4.Version(), seed.n4.DeepCopy())
		}},
		{name: "AddNodeLabelTokenWithHistory", run: func(bs *Store, seed readOnlySeed) error {
			updated := seed.n3.DeepCopy()
			updated.AddLabelTokenRaw(2)
			return bs.AddNodeLabelTokenWithHistory(seed.n3.ID(), 2, updated, seed.n3.Version(), seed.n3.DeepCopy())
		}},
		{name: "ReplaceNodeWithHistory", run: func(bs *Store, seed readOnlySeed) error {
			return bs.ReplaceNodeWithHistory(seed.n3.DeepCopy(), seed.n3.Version(), seed.n3.DeepCopy())
		}},
		{name: "DeleteNodeWithHistory", run: func(bs *Store, seed readOnlySeed) error {
			return bs.DeleteNodeWithHistory(seed.n3.ID(), seed.n3.Version(), seed.n3.DeepCopy(), nil)
		}},
		{name: "PutNodeVersion", run: func(bs *Store, seed readOnlySeed) error {
			return bs.PutNodeVersion(seed.n3.ID(), seed.n3.Version(), seed.n3.DeepCopy())
		}},
		{name: "TruncateNodeHistory", run: func(bs *Store, seed readOnlySeed) error {
			return bs.TruncateNodeHistory(seed.n3.ID(), 1)
		}},
		{name: "TrimNodeHistoryFrom", run: func(bs *Store, seed readOnlySeed) error {
			return bs.TrimNodeHistoryFrom(seed.n3.ID(), 1)
		}},
		{name: "ReplaceRelWithHistory", run: func(bs *Store, seed readOnlySeed) error {
			return bs.ReplaceRelWithHistory(seed.rel.DeepCopy(), seed.rel.Version(), seed.rel.DeepCopy())
		}},
		{name: "DeleteRelWithHistory", run: func(bs *Store, seed readOnlySeed) error {
			return bs.DeleteRelWithHistory(seed.rel.ID(), seed.rel.Version(), seed.rel.DeepCopy())
		}},
		{name: "PutRelVersion", run: func(bs *Store, seed readOnlySeed) error {
			return bs.PutRelVersion(seed.rel.ID(), seed.rel.Version(), seed.rel.DeepCopy())
		}},
		{name: "TruncateRelHistory", run: func(bs *Store, seed readOnlySeed) error {
			return bs.TruncateRelHistory(seed.rel.ID(), 1)
		}},
		{name: "TrimRelHistoryFrom", run: func(bs *Store, seed readOnlySeed) error {
			return bs.TrimRelHistoryFrom(seed.rel.ID(), 1)
		}},
		{name: "CreatePropertyIndex", run: func(bs *Store, _ readOnlySeed) error {
			return bs.CreatePropertyIndex(1, "status")
		}},
		{name: "DropPropertyIndex", run: func(bs *Store, _ readOnlySeed) error {
			return bs.DropPropertyIndex(1, "status")
		}},
		{name: "CreateTemporalIndex", run: func(bs *Store, _ readOnlySeed) error {
			return bs.CreateTemporalIndex(1)
		}},
		{name: "DropTemporalIndex", run: func(bs *Store, _ readOnlySeed) error {
			return bs.DropTemporalIndex(1)
		}},
		{name: "CreateHighFrequencyIndex", run: func(bs *Store, _ readOnlySeed) error {
			return bs.CreateHighFrequencyIndex(1, time.Second)
		}},
		{name: "DropHighFrequencyIndex", run: func(bs *Store, _ readOnlySeed) error {
			return bs.DropHighFrequencyIndex(1)
		}},
		{name: "CreateVectorIndex", run: func(bs *Store, _ readOnlySeed) error {
			return bs.CreateVectorIndex(1, "embedding", 3, DistanceCosine)
		}},
		{name: "DropVectorIndex", run: func(bs *Store, _ readOnlySeed) error {
			return bs.DropVectorIndex(1, "embedding")
		}},
		{name: "SaveRegistries", run: func(bs *Store, _ readOnlySeed) error {
			return bs.SaveRegistries(labels, relTypes)
		}},
		{name: "SaveLabelRegistry", run: func(bs *Store, _ readOnlySeed) error {
			return bs.SaveLabelRegistry(labels)
		}},
		{name: "SaveRelTypeRegistry", run: func(bs *Store, _ readOnlySeed) error {
			return bs.SaveRelTypeRegistry(relTypes)
		}},
		{name: "PutRelEntityAndOut", run: func(bs *Store, seed readOnlySeed) error {
			r := types.NewRelationship(types.RelID(snowflake.ID(101)), 1, seed.n1.ID(), seed.n2.ID())
			return bs.PutRelEntityAndOut(r)
		}},
		{name: "PutRelIncoming", run: func(bs *Store, seed readOnlySeed) error {
			return bs.PutRelIncoming(seed.n2.ID().SnowflakeID(), seed.n1.ID().SnowflakeID(), 1, snowflake.ID(101))
		}},
		{name: "DeleteRelEntityAndOut", run: func(bs *Store, seed readOnlySeed) error {
			_, err := bs.DeleteRelEntityAndOut(seed.rel.ID().SnowflakeID())
			return err
		}},
		{name: "DeleteRelIncoming", run: func(bs *Store, seed readOnlySeed) error {
			info := RelDeleteInfo{
				ID:      seed.rel.ID().SnowflakeID(),
				RelType: seed.rel.TypeToken().Value(),
				StartID: seed.rel.StartNodeID().SnowflakeID(),
				EndID:   seed.rel.EndNodeID().SnowflakeID(),
			}
			return bs.DeleteRelIncoming(info)
		}},
		{name: "DeleteIncomingByRelID", run: func(bs *Store, seed readOnlySeed) error {
			return bs.DeleteIncomingByRelID(seed.n2.ID().SnowflakeID(), snowflake.ID(101))
		}},
		{name: "ScanAndDeleteIncoming", run: func(bs *Store, seed readOnlySeed) error {
			return bs.ScanAndDeleteIncoming(seed.n2.ID().SnowflakeID(), snowflake.ID(101))
		}},
		{name: "PurgeOrphanRelationshipIndexes", run: func(bs *Store, _ readOnlySeed) error {
			return bs.PurgeOrphanRelationshipIndexes(types.RelID(snowflake.ID(101)))
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bs, seed := newReadOnlySeededBadgerStore(t)
			if err := tc.run(bs, seed); !errors.Is(err, ErrInvalidStoreMutation) {
				t.Fatalf("%s on read-only store = %v, want ErrInvalidStoreMutation", tc.name, err)
			}
			assertReadOnlySeedUnchanged(t, bs, seed)
		})
	}
}

func TestBadgerStoreReadOnlyAllowsEmptyBatches(t *testing.T) {
	bs, seed := newReadOnlySeededBadgerStore(t)

	tests := []struct {
		name string
		run  func() error
	}{
		{name: "PutNodesBatch nil", run: func() error { return bs.PutNodesBatch(nil) }},
		{name: "PutNodesBatch empty", run: func() error { return bs.PutNodesBatch([]*types.Node{}) }},
		{name: "DeleteNodesBatch nil", run: func() error { return bs.DeleteNodesBatch(nil) }},
		{name: "DeleteNodesBatch empty", run: func() error { return bs.DeleteNodesBatch([]types.NodeID{}) }},
		{name: "PutRelationshipsBatch nil", run: func() error { return bs.PutRelationshipsBatch(nil) }},
		{name: "PutRelationshipsBatch empty", run: func() error {
			return bs.PutRelationshipsBatch([]*types.Relationship{})
		}},
		{name: "DeleteRelationshipsBatch nil", run: func() error { return bs.DeleteRelationshipsBatch(nil) }},
		{name: "DeleteRelationshipsBatch empty", run: func() error {
			return bs.DeleteRelationshipsBatch([]types.RelID{})
		}},
	}

	for _, tc := range tests {
		if err := tc.run(); err != nil {
			t.Fatalf("%s on read-only store returned error: %v", tc.name, err)
		}
	}
	assertReadOnlySeedUnchanged(t, bs, seed)
}
