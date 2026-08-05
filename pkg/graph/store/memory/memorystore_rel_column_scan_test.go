package memory

import (
	"errors"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

const memRelScanType uint16 = 11

func newMemRelScanStore(t *testing.T) *Store {
	t.Helper()
	ms := New()
	for _, nid := range []int64{1, 2, 3} {
		n := types.NewNode(types.NodeID(nid), memRelScanType, nil)
		n.SetTemporal(&types.TemporalMetadata{ValidFrom: types.Instant(1)})
		if err := ms.PutNode(n); err != nil {
			t.Fatalf("PutNode %d: %v", nid, err)
		}
	}
	return ms
}

func putMemRel(t *testing.T, ms *Store, id, start, end int64, weight any) {
	t.Helper()
	r := types.NewRelationship(types.RelID(id), memRelScanType,
		types.NodeID(start), types.NodeID(end))
	if weight != nil {
		if err := r.SetProperty("weight", weight); err != nil {
			t.Fatalf("SetProperty: %v", err)
		}
	}
	r.SetTemporal(&types.TemporalMetadata{ValidFrom: types.Instant(10)})
	if err := ms.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship %d: %v", id, err)
	}
}

// TestMemRelColumnScan_MatchesRowPath is the memory-side oracle. The memory scanner
// delegates to the SHARED row builder, so this asserts the door reaches it with the
// right rows and endpoints rather than re-testing the builder.
func TestMemRelColumnScan_MatchesRowPath(t *testing.T) {
	ms := newMemRelScanStore(t)
	putMemRel(t, ms, 1, 1, 2, int64(10))
	putMemRel(t, ms, 2, 2, 3, int64(20))
	putMemRel(t, ms, 3, 3, 1, nil) // absent weight

	want := map[int64][2]int64{1: {1, 2}, 2: {2, 3}, 3: {3, 1}}

	rows := 0
	err := ms.ScanRelColumns(memRelScanType, []string{"weight"}, storecontract.QueryOpts{},
		func(b *storecontract.RelColumnBatch) bool {
			for i := range b.IDs {
				rows++
				w, ok := want[int64(b.IDs[i])]
				if !ok {
					t.Errorf("unexpected rel id %d", b.IDs[i])
					continue
				}
				if int64(b.StartIDs[i]) != w[0] || int64(b.EndIDs[i]) != w[1] {
					t.Errorf("rel %d: got (%d->%d), want (%d->%d)",
						b.IDs[i], b.StartIDs[i], b.EndIDs[i], w[0], w[1])
				}
			}
			return true
		})
	if err != nil {
		t.Fatalf("ScanRelColumns: %v", err)
	}
	if rows != 3 {
		t.Fatalf("got %d rows, want 3", rows)
	}
}

// TestMemRelColumnScan_MixedNumericRefuses proves the memory door reaches the SHARED
// refusal. The rules live in ColumnData.appendRow with the node path; a rel-specific
// copy would be free to drift, and this is what would catch that.
func TestMemRelColumnScan_MixedNumericRefuses(t *testing.T) {
	ms := newMemRelScanStore(t)
	putMemRel(t, ms, 1, 1, 2, int64(2))
	putMemRel(t, ms, 2, 2, 3, 2.5)

	err := ms.ScanRelColumns(memRelScanType, []string{"weight"}, storecontract.QueryOpts{},
		func(b *storecontract.RelColumnBatch) bool { return true })
	if !errors.Is(err, storecontract.ErrMixedNumericColumn) {
		t.Fatalf("got err=%v, want ErrMixedNumericColumn", err)
	}
}

// TestMemRelColumnScan_RejectsNilInputs — a nil callback is a caller bug, not an
// empty scan; the callback is the only way results leave this door.
func TestMemRelColumnScan_RejectsNilInputs(t *testing.T) {
	ms := newMemRelScanStore(t)
	if err := ms.ScanRelColumns(memRelScanType, []string{"weight"},
		storecontract.QueryOpts{}, nil); err == nil {
		t.Fatal("nil callback accepted; want an error")
	}
	var nilStore *Store
	if err := nilStore.ScanRelColumns(memRelScanType, nil, storecontract.QueryOpts{},
		func(b *storecontract.RelColumnBatch) bool { return true }); err == nil {
		t.Fatal("nil store accepted; want an error")
	}
}

// TestMemRelColumnScan_EarlyStopHonoured — returning false must stop the scan; a
// caller using it as a limit would otherwise pay for the whole type.
func TestMemRelColumnScan_EarlyStopHonoured(t *testing.T) {
	ms := newMemRelScanStore(t)
	putMemRel(t, ms, 1, 1, 2, int64(1))
	putMemRel(t, ms, 2, 2, 3, int64(2))

	calls := 0
	if err := ms.ScanRelColumns(memRelScanType, []string{"weight"}, storecontract.QueryOpts{},
		func(b *storecontract.RelColumnBatch) bool { calls++; return false }); err != nil {
		t.Fatalf("ScanRelColumns: %v", err)
	}
	if calls != 1 {
		t.Fatalf("callback ran %d times after returning false; want 1", calls)
	}
}
