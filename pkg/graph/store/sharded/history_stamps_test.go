package sharded

import (
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 30: the history stamps ask the slot the ID routes to. Each slot
// holds rows with its own stamps under the same sequence number, so a router
// that asks the wrong slot answers the other slot's numbers; a foreign slot
// fails like GetNodeHistory, invalid IDs like the history doors, a closed
// store with ErrStoreClosed.
func TestHistoryStampsSharded(t *testing.T) {
	st := newMemStore(t, 0, 2)
	for slot := uint8(0); slot < 2; slot++ {
		base := types.Instant(1000 * (int64(slot) + 1))
		n := types.NewNode(mkNodeID(slot, 7), 1, nil)
		n.SetTemporal(&types.TemporalMetadata{TxFrom: base, TxTo: base + 1})
		if err := st.PutNodeVersion(n.ID(), 0, n); err != nil {
			t.Fatal(err)
		}
		r := types.NewRelationship(mkRelID(slot, 9), 1, mkNodeID(slot, 1), mkNodeID(slot, 2))
		r.SetTemporal(&types.TemporalMetadata{TxFrom: base + 2, DeletedAt: base + 3})
		if err := st.PutRelVersion(r.ID(), 0, r); err != nil {
			t.Fatal(err)
		}
	}
	for slot := uint8(0); slot < 2; slot++ {
		base := types.Instant(1000 * (int64(slot) + 1))
		if f, to, has, err := st.NodeHistoryStamps(mkNodeID(slot, 7)); err != nil || !has || f != base || to != base+1 {
			t.Fatalf("slot %d node = (%d, %d, %v, %v), want (%d, %d, true)", slot, f, to, has, err, base, base+1)
		}
		if f, to, has, err := st.RelHistoryStamps(mkRelID(slot, 9)); err != nil || !has || f != base+2 || to != base+3 {
			t.Fatalf("slot %d rel = (%d, %d, %v, %v), want (%d, %d, true)", slot, f, to, has, err, base+2, base+3)
		}
		if f, to, has, err := st.NodeHistoryStamps(mkNodeID(slot, 8)); err != nil || has || f != 0 || to != 0 {
			t.Fatalf("slot %d plain node = (%d, %d, %v, %v)", slot, f, to, has, err)
		}
	}
	_, herr := st.GetNodeHistory(mkNodeID(5, 7))
	if _, _, _, err := st.NodeHistoryStamps(mkNodeID(5, 7)); !errors.Is(err, ErrSlotNotLocal) || !errors.Is(herr, ErrSlotNotLocal) {
		t.Fatalf("foreign slot: stamps %v, GetNodeHistory %v; want ErrSlotNotLocal from both", err, herr)
	}
	if _, _, _, err := st.RelHistoryStamps(mkRelID(5, 9)); !errors.Is(err, ErrSlotNotLocal) {
		t.Fatalf("foreign slot rel: %v", err)
	}
	sameClass := func(a, b error) bool {
		return a != nil && b != nil &&
			errors.Is(a, ErrInvalidStoreMutation) == errors.Is(b, ErrInvalidStoreMutation) &&
			errors.Is(a, ErrSlotNotLocal) == errors.Is(b, ErrSlotNotLocal)
	}
	for _, bad := range []int64{0, -1} {
		_, herr := st.GetNodeHistory(types.NodeID(bad))
		if _, _, _, err := st.NodeHistoryStamps(types.NodeID(bad)); !sameClass(err, herr) {
			t.Fatalf("NodeHistoryStamps(%d) = %v, GetNodeHistory %v", bad, err, herr)
		}
		_, rerr := st.GetRelHistory(types.RelID(bad))
		if _, _, _, err := st.RelHistoryStamps(types.RelID(bad)); !sameClass(err, rerr) {
			t.Fatalf("RelHistoryStamps(%d) = %v, GetRelHistory %v", bad, err, rerr)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := st.NodeHistoryStamps(mkNodeID(0, 7)); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("closed = %v, want ErrStoreClosed", err)
	}
	if _, _, _, err := st.RelHistoryStamps(mkRelID(0, 9)); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("closed rel = %v, want ErrStoreClosed", err)
	}
	var nilStore *Store
	if _, _, _, err := nilStore.NodeHistoryStamps(1); !errors.Is(err, ErrNilStore) {
		t.Fatalf("nil NodeHistoryStamps = %v", err)
	}
	if _, _, _, err := nilStore.RelHistoryStamps(1); !errors.Is(err, ErrNilStore) {
		t.Fatalf("nil RelHistoryStamps = %v", err)
	}
}
