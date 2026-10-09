package sharded

import (
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// HasHistory asks the slot the ID routes to: history written to slot 0 and
// slot 1 reads true, the same sequence number on the other slot (another ID)
// reads false, an ID of a slot this store does not own fails like
// GetNodeHistory, invalid IDs fail like the history doors.
func TestHistoryPresenceSharded(t *testing.T) {
	st := newMemStore(t, 0, 2)
	for slot := uint8(0); slot < 2; slot++ {
		n := types.NewNode(mkNodeID(slot, 7), 1, nil)
		if err := st.PutNodeVersion(n.ID(), 0, n); err != nil {
			t.Fatal(err)
		}
		r := types.NewRelationship(mkRelID(slot, 9), 1, mkNodeID(slot, 1), mkNodeID(slot, 2))
		if err := st.PutRelVersion(r.ID(), 0, r); err != nil {
			t.Fatal(err)
		}
	}
	for slot := uint8(0); slot < 2; slot++ {
		if got, err := st.HasNodeHistory(mkNodeID(slot, 7)); err != nil || !got {
			t.Fatalf("slot %d node with history = %v, %v", slot, got, err)
		}
		if got, err := st.HasRelHistory(mkRelID(slot, 9)); err != nil || !got {
			t.Fatalf("slot %d rel with history = %v, %v", slot, got, err)
		}
		if got, err := st.HasNodeHistory(mkNodeID(slot, 8)); err != nil || got {
			t.Fatalf("slot %d plain node = %v, %v", slot, got, err)
		}
		if got, err := st.HasRelHistory(mkRelID(slot, 10)); err != nil || got {
			t.Fatalf("slot %d plain rel = %v, %v", slot, got, err)
		}
	}
	_, herr := st.GetNodeHistory(mkNodeID(5, 7))
	if _, err := st.HasNodeHistory(mkNodeID(5, 7)); !errors.Is(err, ErrSlotNotLocal) || !errors.Is(herr, ErrSlotNotLocal) {
		t.Fatalf("foreign slot: HasNodeHistory %v, GetNodeHistory %v; want ErrSlotNotLocal from both", err, herr)
	}
	if _, err := st.HasRelHistory(mkRelID(5, 9)); !errors.Is(err, ErrSlotNotLocal) {
		t.Fatalf("foreign slot rel: %v", err)
	}
	// Invalid IDs fail exactly like the sibling history doors (the zero ID
	// routes to slot 0 and fails validation there; a negative one decomposes to
	// a foreign slot). The graph core validates before either door runs.
	sameClass := func(a, b error) bool {
		return a != nil && b != nil &&
			errors.Is(a, ErrInvalidStoreMutation) == errors.Is(b, ErrInvalidStoreMutation) &&
			errors.Is(a, ErrSlotNotLocal) == errors.Is(b, ErrSlotNotLocal)
	}
	for _, bad := range []int64{0, -1} {
		_, herr := st.GetNodeHistory(types.NodeID(bad))
		if _, err := st.HasNodeHistory(types.NodeID(bad)); !sameClass(err, herr) {
			t.Fatalf("HasNodeHistory(%d) = %v, GetNodeHistory %v", bad, err, herr)
		}
		_, rerr := st.GetRelHistory(types.RelID(bad))
		if _, err := st.HasRelHistory(types.RelID(bad)); !sameClass(err, rerr) {
			t.Fatalf("HasRelHistory(%d) = %v, GetRelHistory %v", bad, err, rerr)
		}
	}
	if _, err := st.HasNodeHistory(0); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("HasNodeHistory(0) = %v, want ErrInvalidStoreMutation", err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := st.HasNodeHistory(mkNodeID(0, 7)); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("closed HasNodeHistory = %v", err)
	}
	if _, err := st.HasRelHistory(mkRelID(0, 9)); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("closed HasRelHistory = %v", err)
	}
	var nilStore *Store
	if _, err := nilStore.HasNodeHistory(1); !errors.Is(err, ErrNilStore) {
		t.Fatalf("nil HasNodeHistory = %v", err)
	}
}
