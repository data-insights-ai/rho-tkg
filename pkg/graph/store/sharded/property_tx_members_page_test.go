package sharded

import (
	"errors"
	"fmt"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestShardedPropertyTxMemberPageMerge (backlog 69): a page is the smallest
// IDs above the cursor across both slots, ascending, an entity a move left on
// two shards once with the lower bound. Faulty fan-outs caught: one shard
// only, shard order instead of ID order, duplicates, a limit applied per
// shard but not to the union, a shard's ErrIndexNotFound hidden.
func TestShardedPropertyTxMemberPageMerge(t *testing.T) {
	st := newMemStore(t, 0, 2)
	const typ, label = uint16(3), uint16(4)
	vk := types.IndexablePropertyValueKey(int64(1))
	if err := st.CreateRelPropertyIndex(typ, "seat"); err != nil {
		t.Fatal(err)
	}
	if err := st.CreatePropertyIndex(label, "seat"); err != nil {
		t.Fatal(err)
	}
	put := func(slot uint8, seq int64, tx types.Instant) {
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
	}
	// Interleaved IDs: slot 1 IDs sit between slot 0 IDs of neighbouring seq.
	for seq := int64(10); seq < 16; seq++ {
		put(uint8(seq%2), seq, types.Instant(seq))
	}
	// A copy of slot 0's seq 10 rows on shard 1 with a later stamp.
	dup := types.NewRelationship(mkRelID(0, 10), typ, mkNodeID(0, 1), mkNodeID(0, 2))
	_ = dup.SetProperty("seat", int64(1))
	dup.SetTemporal(&types.TemporalMetadata{TxFrom: 90})
	dup.SetVersion(3)
	if err := st.shards[1].PutRelVersion(dup.ID(), 3, dup); err != nil {
		t.Fatal(err)
	}
	want := func(seqs ...int64) string {
		var out []string
		for _, s := range seqs {
			out = append(out, fmt.Sprintf("%d:%d", mkID(uint8(s%2), s), s))
		}
		return fmt.Sprint(out)
	}
	relPage := func(after types.RelID, limit int) string {
		var out []string
		if err := st.ForEachRelPropertyTxMemberAfter(typ, "seat", vk, after, limit, func(id types.RelID, tx types.Instant) bool {
			out = append(out, fmt.Sprintf("%d:%d", id, tx))
			return true
		}); err != nil {
			t.Fatal(err)
		}
		return fmt.Sprint(out)
	}
	nodePage := func(after types.NodeID, limit int) string {
		var out []string
		if err := st.ForEachNodePropertyTxMemberAfter(label, "seat", vk, after, limit, func(id types.NodeID, tx types.Instant) bool {
			out = append(out, fmt.Sprintf("%d:%d", id, tx))
			return true
		}); err != nil {
			t.Fatal(err)
		}
		return fmt.Sprint(out)
	}
	if got, w := relPage(0, 3), want(10, 11, 12); got != w {
		t.Fatalf("rel page 1 = %s, want %s", got, w)
	}
	if got, w := relPage(mkRelID(0, 12), 10), want(13, 14, 15); got != w {
		t.Fatalf("rel page 2 = %s, want %s", got, w)
	}
	if got, w := nodePage(mkNodeID(1, 11), 2), want(12, 13); got != w {
		t.Fatalf("node page = %s, want %s", got, w)
	}
	if err := st.ForEachRelPropertyTxMemberAfter(typ, "seat", vk, 0, 1, nil); !errors.Is(err, storecontract.ErrInvalidStoreMutation) {
		t.Fatalf("nil fn: %v", err)
	}
	if err := st.ForEachNodePropertyTxMemberAfter(label, "seat", vk, 0, 1, nil); !errors.Is(err, storecontract.ErrInvalidStoreMutation) {
		t.Fatalf("nil node fn: %v", err)
	}
	if err := st.shards[1].DropRelPropertyIndex(typ, "seat"); err != nil {
		t.Fatal(err)
	}
	if err := st.ForEachRelPropertyTxMemberAfter(typ, "seat", vk, 0, 5, func(types.RelID, types.Instant) bool { return true }); !errors.Is(err, storecontract.ErrIndexNotFound) {
		t.Fatalf("one shard without the index: %v, want ErrIndexNotFound", err)
	}
}
