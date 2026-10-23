package sharded

import (
	"errors"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 42: the current-row stamps ask the slot the ID routes to. Each slot
// holds a row under the same sequence number with its own stamps, so a router
// that asks a non-owner slot (or always the first) answers the other slot's
// numbers; a foreign slot fails like GetNode (ErrSlotNotLocal), invalid IDs
// with ErrInvalidStoreMutation, a closed store with ErrStoreClosed, a nil
// store with ErrNilStore.
func TestCurrentStampsSharded(t *testing.T) {
	t.Parallel()
	st := newMemStore(t, 0, 2)
	for slot := uint8(0); slot < 2; slot++ {
		base := types.Instant(1000 * (int64(slot) + 1))
		for _, seq := range []int64{1, 2} {
			if err := st.PutNode(types.NewNode(mkNodeID(slot, seq), 1, nil)); err != nil {
				t.Fatal(err)
			}
		}
		n := types.NewNode(mkNodeID(slot, 7), 1, nil)
		n.SetVersion(uint32(slot) + 3)
		n.SetTemporal(&types.TemporalMetadata{TxFrom: base, TxTo: base + 1})
		if err := st.PutNode(n); err != nil {
			t.Fatal(err)
		}
		r := types.NewRelationship(mkRelID(slot, 9), 1, mkNodeID(slot, 1), mkNodeID(slot, 2))
		r.SetVersion(uint32(slot) + 5)
		r.SetTemporal(&types.TemporalMetadata{TxFrom: base + 2, DeletedAt: base + 3})
		if err := st.PutRelationship(r); err != nil {
			t.Fatal(err)
		}
	}
	for slot := uint8(0); slot < 2; slot++ {
		base := types.Instant(1000 * (int64(slot) + 1))
		want := storecontract.RowStamps{TxFrom: base, TxTo: base + 1, Version: uint32(slot) + 3}
		if got, err := st.NodeCurrentStamps(mkNodeID(slot, 7)); err != nil || got != want {
			t.Fatalf("slot %d node = (%+v, %v), want %+v", slot, got, err, want)
		}
		wantRel := storecontract.RowStamps{TxFrom: base + 2, DeletedAt: base + 3, Version: uint32(slot) + 5}
		if got, err := st.RelCurrentStamps(mkRelID(slot, 9)); err != nil || got != wantRel {
			t.Fatalf("slot %d rel = (%+v, %v), want %+v", slot, got, err, wantRel)
		}
		if _, err := st.NodeCurrentStamps(mkNodeID(slot, 8)); !errors.Is(err, ErrNodeNotFound) {
			t.Fatalf("slot %d unknown node = %v, want ErrNodeNotFound", slot, err)
		}
		if _, err := st.RelCurrentStamps(mkRelID(slot, 8)); !errors.Is(err, ErrRelNotFound) {
			t.Fatalf("slot %d unknown rel = %v, want ErrRelNotFound", slot, err)
		}
	}
	_, gerr := st.GetNode(mkNodeID(5, 7))
	if _, err := st.NodeCurrentStamps(mkNodeID(5, 7)); !errors.Is(err, ErrSlotNotLocal) || !errors.Is(gerr, ErrSlotNotLocal) {
		t.Fatalf("foreign slot: stamps %v, GetNode %v; want ErrSlotNotLocal from both", err, gerr)
	}
	if _, err := st.RelCurrentStamps(mkRelID(5, 9)); !errors.Is(err, ErrSlotNotLocal) {
		t.Fatalf("foreign slot rel: %v", err)
	}
	for _, bad := range []int64{0, -1} {
		if _, err := st.NodeCurrentStamps(types.NodeID(bad)); !errors.Is(err, ErrInvalidStoreMutation) {
			t.Fatalf("NodeCurrentStamps(%d) = %v, want ErrInvalidStoreMutation", bad, err)
		}
		if _, err := st.RelCurrentStamps(types.RelID(bad)); !errors.Is(err, ErrInvalidStoreMutation) {
			t.Fatalf("RelCurrentStamps(%d) = %v, want ErrInvalidStoreMutation", bad, err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := st.NodeCurrentStamps(mkNodeID(0, 7)); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("closed = %v, want ErrStoreClosed", err)
	}
	if _, err := st.RelCurrentStamps(mkRelID(0, 9)); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("closed rel = %v, want ErrStoreClosed", err)
	}
}
