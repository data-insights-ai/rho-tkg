package badger

import (
	"strings"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestReadSnapshotChargesTheSameBufferedOrCommitted (REOPENBUF): a metered
// history read charges the same work whether its rows are still in the write
// buffer or committed, so a figure such as BufferBytes follows the read and
// not the flush ticker (ai-soc's reopen gate dropped BufferBytes for this:
// 3,208 to 4,462 B on one input). One node with eight delta-encoded history
// rows (anchor every 4), read through every version door, the history list
// and the skeleton list of a metered snapshot, and the newest-first walk,
// before and after Flush.
func TestReadSnapshotChargesTheSameBufferedOrCommitted(t *testing.T) {
	bs, err := New(Config{InMemory: true, FlushInterval: time.Hour, HistoryDeltaEncoding: true, HistoryAnchorInterval: 4})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	id := types.NodeID(snowflake.ID(1 << 30))
	prev := types.NewNode(id, 1, nil)
	if err := prev.SetProperty("p", strings.Repeat("x", 64)); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNode(prev); err != nil {
		t.Fatal(err)
	}
	for v := range 8 {
		next := prev.DeepCopy()
		next.SetVersion(prev.Version() + 1)
		if err := next.SetProperty("p", strings.Repeat("y", 64+v)); err != nil {
			t.Fatal(err)
		}
		if err := bs.ReplaceNodeWithHistory(next, prev.Version(), prev); err != nil {
			t.Fatal(err)
		}
		prev = next
	}
	read := func() storecontract.ReadWork {
		t.Helper()
		_, m := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{})
		snap := bs.NewReadSnapshot()
		defer snap.Close()
		snap.(storecontract.ReadSnapshotMeterCapability).MeterReads(m.Tally("snapshot"))
		for v := prev.Version() - 1; v >= 1; v-- {
			if _, err := snap.GetNodeVersion(id, v); err != nil {
				t.Fatalf("v%d: %v", v, err)
			}
		}
		rows, err := snap.GetNodeHistory(id)
		if err != nil || len(rows) != 8 {
			t.Fatalf("history = %d rows, %v; want 8", len(rows), err)
		}
		metas, err := snap.(storecontract.TemporalMetaHistoryCapability).NodeHistoryTemporalMeta(id)
		if err != nil || len(metas) != 8 {
			t.Fatalf("skeletons = %d, %v; want 8", len(metas), err)
		}
		visited := 0
		if _, err := bs.NodeHistoryNewestFirst(id, m.Tally("newest first"), func(storecontract.VersionTemporalMeta, bool) bool {
			visited++
			return true
		}); err != nil || visited != 8 {
			t.Fatalf("newest first visited %d, %v; want 8", visited, err)
		}
		return m.Work()
	}
	buffered := read()
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	committed := read()
	if buffered != committed {
		t.Fatalf("the same reads charged %+v with the rows buffered and %+v committed; want equal work", buffered, committed)
	}
}
