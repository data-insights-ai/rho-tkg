package memory

import (
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The memory history-stamps cache (backlog 30) outside the store-level
// matrix: a row rewritten by the test-only tamper door (no history-row seam)
// drops the cached entry, so the next read folds the rewritten row (a cache
// that kept the entry answers the old max); the zero-value store answers no
// history; a closed store fails.
func TestMemoryHistoryStampsTamperDoorAndZeroValue(t *testing.T) {
	ms := New()
	id := types.NodeID(77)
	put := func(ver uint32, tf, tt types.Instant) {
		t.Helper()
		n := types.NewNode(id, 1, nil)
		n.SetVersion(ver)
		n.SetTemporal(&types.TemporalMetadata{TxFrom: tf, TxTo: tt})
		if err := ms.PutNodeVersion(id, ver, n); err != nil {
			t.Fatal(err)
		}
	}
	put(0, 100, 200)
	put(1, 300, 400)
	if f, to, has, err := ms.NodeHistoryStamps(id); err != nil || !has || f != 300 || to != 400 {
		t.Fatalf("cached = (%d, %d, %v, %v), want (300, 400, true)", f, to, has, err)
	}
	low := types.NewNode(id, 1, nil)
	low.SetVersion(1)
	low.SetTemporal(&types.TemporalMetadata{TxFrom: 10, TxTo: 20})
	ms.SetNodeHistoryEntryForTest(id, 1, low)
	if f, to, has, err := ms.NodeHistoryStamps(id); err != nil || !has || f != 100 || to != 200 {
		t.Fatalf("after the tamper rewrite = (%d, %d, %v, %v), want (100, 200, true)", f, to, has, err)
	}

	var zero Store
	if f, to, has, err := zero.NodeHistoryStamps(id); err != nil || has || f != 0 || to != 0 {
		t.Fatalf("zero-value node = (%d, %d, %v, %v)", f, to, has, err)
	}
	if f, to, has, err := zero.RelHistoryStamps(types.RelID(5)); err != nil || has || f != 0 || to != 0 {
		t.Fatalf("zero-value rel = (%d, %d, %v, %v)", f, to, has, err)
	}
	if err := ms.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := ms.NodeHistoryStamps(id); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("closed node = %v, want ErrStoreClosed", err)
	}
	if _, _, _, err := ms.RelHistoryStamps(types.RelID(5)); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("closed rel = %v, want ErrStoreClosed", err)
	}
}
