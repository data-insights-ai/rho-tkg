package sharded

import (
	"errors"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestShardedPropertyTxMembersUnion: members of a value come from every slot,
// and an entity a re-sharding move left on two shards is emitted once with the
// lower first transaction time. Faulty fan-outs caught: asking one shard only
// (the slot-1 member), emitting duplicates, keeping the first shard's bound
// instead of the minimum, ignoring an early stop, hiding a shard's
// ErrIndexNotFound.
func TestShardedPropertyTxMembersUnion(t *testing.T) {
	st := newMemStore(t, 0, 2)
	const typ, label = uint16(3), uint16(4)
	vk := types.IndexablePropertyValueKey(int64(1))
	if err := st.ForEachRelPropertyTxMember(typ, "seat", vk, func(types.RelID, types.Instant) bool { return true }); !errors.Is(err, storecontract.ErrIndexNotFound) {
		t.Fatalf("undeclared: %v, want ErrIndexNotFound", err)
	}
	if err := st.CreateRelPropertyIndex(typ, "seat"); err != nil {
		t.Fatal(err)
	}
	if err := st.CreatePropertyIndex(label, "seat"); err != nil {
		t.Fatal(err)
	}
	put := func(slot uint8, seq int64, tx types.Instant) types.RelID {
		r := types.NewRelationship(mkRelID(slot, seq), typ, mkNodeID(slot, 1), mkNodeID(slot, 2))
		_ = r.SetProperty("seat", int64(1))
		r.SetTemporal(&types.TemporalMetadata{TxFrom: tx})
		if err := st.PutRelVersion(r.ID(), 0, r); err != nil {
			t.Fatal(err)
		}
		n := types.NewNode(mkNodeID(slot, seq), label, nil)
		_ = n.SetProperty("seat", int64(1))
		n.SetTemporal(&types.TemporalMetadata{TxFrom: tx})
		if err := st.PutNodeVersion(n.ID(), 0, n); err != nil {
			t.Fatal(err)
		}
		return r.ID()
	}
	a := put(0, 11, 50)
	b := put(1, 12, 60)
	// A copy of a's rows on the other shard (as a move leaves its source
	// behind), with a later stamp.
	dup := types.NewRelationship(a, typ, mkNodeID(0, 1), mkNodeID(0, 2))
	_ = dup.SetProperty("seat", int64(1))
	dup.SetTemporal(&types.TemporalMetadata{TxFrom: 90})
	dup.SetVersion(3)
	if err := st.shards[1].PutRelVersion(a, 3, dup); err != nil {
		t.Fatal(err)
	}
	got := map[types.RelID]types.Instant{}
	calls := 0
	if err := st.ForEachRelPropertyTxMember(typ, "seat", vk, func(id types.RelID, tx types.Instant) bool {
		calls++
		got[id] = tx
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if calls != 2 || got[a] != 50 || got[b] != 60 {
		t.Fatalf("union = %v after %d calls, want {a:50 b:60} in 2 calls", got, calls)
	}
	nodes := map[types.NodeID]types.Instant{}
	if err := st.ForEachNodePropertyTxMember(label, "seat", vk, func(id types.NodeID, tx types.Instant) bool {
		nodes[id] = tx
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 2 || nodes[mkNodeID(0, 11)] != 50 || nodes[mkNodeID(1, 12)] != 60 {
		t.Fatalf("node union = %v", nodes)
	}
	calls = 0
	if err := st.ForEachRelPropertyTxMember(typ, "seat", vk, func(types.RelID, types.Instant) bool { calls++; return false }); err != nil || calls != 1 {
		t.Fatalf("early stop: %d calls, err %v", calls, err)
	}
	calls = 0
	if err := st.ForEachNodePropertyTxMember(label, "seat", vk, func(types.NodeID, types.Instant) bool { calls++; return false }); err != nil || calls != 1 {
		t.Fatalf("node early stop: %d calls, err %v", calls, err)
	}
	stats, err := st.PropertyTxMembershipStats()
	if err != nil || stats.RelSidecars != 2 || stats.NodeSidecars != 2 || stats.RelPostings != 3 || stats.NodePostings != 2 {
		t.Fatalf("stats = %+v, %v", stats, err)
	}
	// One shard loses the index: the store must not serve a partial union.
	if err := st.shards[1].DropRelPropertyIndex(typ, "seat"); err != nil {
		t.Fatal(err)
	}
	if err := st.ForEachRelPropertyTxMember(typ, "seat", vk, func(types.RelID, types.Instant) bool { return true }); !errors.Is(err, storecontract.ErrIndexNotFound) {
		t.Fatalf("one shard without the index: %v, want ErrIndexNotFound", err)
	}
	if err := st.ForEachRelPropertyTxMember(typ, "seat", vk, nil); !errors.Is(err, storecontract.ErrInvalidStoreMutation) {
		t.Fatalf("nil fn: %v", err)
	}
	if err := st.ForEachNodePropertyTxMember(label, "seat", vk, nil); !errors.Is(err, storecontract.ErrInvalidStoreMutation) {
		t.Fatalf("node nil fn: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if err := st.ForEachRelPropertyTxMember(typ, "seat", vk, func(types.RelID, types.Instant) bool { return true }); !errors.Is(err, storecontract.ErrStoreClosed) {
		t.Fatalf("closed: %v", err)
	}
	if err := st.ForEachNodePropertyTxMember(label, "seat", vk, func(types.NodeID, types.Instant) bool { return true }); !errors.Is(err, storecontract.ErrStoreClosed) {
		t.Fatalf("node closed: %v", err)
	}
	if _, err := st.PropertyTxMembershipStats(); !errors.Is(err, storecontract.ErrStoreClosed) {
		t.Fatalf("stats closed: %v", err)
	}
}
