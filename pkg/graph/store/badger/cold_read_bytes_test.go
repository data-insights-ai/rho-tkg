package badger

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// coldReadNodes is the row count seedColdReadNodes writes.
const coldReadNodes = 64

// seedColdReadNodes writes coldReadNodes nodes of 13 string columns each
// (with withVersion also each as history version 0) and flushes, so a read
// after a cache reset is cold.
func seedColdReadNodes(t *testing.T, bs *Store, withVersion bool) []types.NodeID {
	t.Helper()
	ids := make([]types.NodeID, coldReadNodes)
	for i := range ids {
		ps := types.PropertySlice{}
		for c := 0; c < 13; c++ {
			if err := ps.Set("ov_c"+string(rune('a'+c)), "term-value-"+string(rune('a'+c))); err != nil {
				t.Fatal(err)
			}
		}
		n := types.NewNode(types.NodeID(snowflake.ID(1000+i)), 1, nil)
		if err := n.SetProperties(ps); err != nil {
			t.Fatal(err)
		}
		if err := bs.PutNode(n); err != nil {
			t.Fatal(err)
		}
		if withVersion {
			if err := bs.PutNodeVersion(n.ID(), 0, n); err != nil {
				t.Fatal(err)
			}
		}
		ids[i] = n.ID()
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	return ids
}

// TestColdReadKeepsEveryProperty (row 121): a cold LendNode and a
// GetNodeVersion decode build the property slice once, with no canonical
// re-copy; the decode must keep every property (an install without the copy
// must not drop them). The bytes those reads allocate are
// TestColdLendNodeBytesBoundedByRowSize and TestGetNodeVersionBytesMatchColdRead,
// at the measurement level (cold_read_bytes_measure_test.go, MEMGATE).
func TestColdReadKeepsEveryProperty(t *testing.T) {
	bs := newTestBadgerStore(t)
	ids := seedColdReadNodes(t, bs, true)
	bs.NodeCacheForTest().ResetForTest()
	got, err := bs.LendNode(ids[0])
	if err != nil {
		t.Fatal(err)
	}
	if got.PropertyCount() != 13 {
		t.Fatalf("cold read returned %d properties, want 13", got.PropertyCount())
	}
	if v, ok := got.GetProperty("ov_cb"); !ok || v != "term-value-b" {
		t.Fatalf("cold read ov_cb = %v, %v; want term-value-b", v, ok)
	}
	v, err := bs.GetNodeVersion(ids[1], 0)
	if err != nil {
		t.Fatal(err)
	}
	if v.PropertyCount() != 13 {
		t.Fatalf("version has %d properties, want 13", v.PropertyCount())
	}
	if p, ok := v.GetProperty("ov_cm"); !ok || p != "term-value-m" {
		t.Fatalf("version ov_cm = %v, %v; want term-value-m", p, ok)
	}
}
