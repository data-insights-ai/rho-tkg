package badger

import (
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestLabelTxMembersOfOnlyProbesRequestedCurrentIDs(t *testing.T) {
	for _, mode := range []string{"pending", "flushed", "transient"} {
		t.Run(mode, func(t *testing.T) {
			cfg := Config{InMemory: mode != "transient", LabelIndexOnDisk: mode != "transient", MaxPendingWrites: -1}
			if mode == "transient" {
				cfg.Dir = t.TempDir()
			}
			bs, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			for id := types.NodeID(1); id <= 64; id++ {
				if err := bs.PutNode(types.NewNode(id, 1, nil)); err != nil {
					t.Fatal(err)
				}
			}
			if err := bs.PutNode(types.NewNode(99, 2, nil)); err != nil {
				t.Fatal(err)
			}
			if mode == "flushed" {
				if err := bs.Flush(); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "transient" {
				if err := bs.Close(); err != nil {
					t.Fatal(err)
				}
				cfg.ReadOnly, cfg.TransientRead = true, true
				bs, err = New(cfg)
				if err != nil {
					t.Fatal(err)
				}
				if !bs.labelLazy {
					t.Fatal("fixture did not open with lazy labels")
				}
			}
			t.Cleanup(func() { _ = bs.Close() })
			// Isolate the point lookup from the separate history-sidecar build.
			if err := bs.ensureLabelTxMembersBuilt(); err != nil {
				t.Fatal(err)
			}
			copied, stopCopy := CountCurrentMembersCopiedForTest()
			defer stopCopy()
			visited, stopVisit := CountIndexEntriesVisitedForTest()
			defer stopVisit()
			pending, stopPending := CountPendingKeysVisitedForTest()
			defer stopPending()
			lazyBefore := bs.lazyLabelKeysWalked.Load()
			var got []types.NodeID
			err = bs.LabelTxMembersOf(1, []types.NodeID{3, 61, 99, 100}, func(id types.NodeID, first types.Instant) bool {
				if first != 0 {
					t.Errorf("current member %d has first tx %d", id, first)
				}
				got = append(got, id)
				return true
			})
			if err != nil || !slices.Equal(got, []types.NodeID{3, 61}) {
				t.Fatalf("lookup: ids %v, err %v; want only the two requested current members", got, err)
			}
			if copied() != 0 || visited()+pending() != 2 || bs.lazyLabelKeysWalked.Load() != lazyBefore {
				t.Errorf("lookup copied %d current IDs, visited %d native keys and %d lazy keys; must only probe the four requested IDs", copied(), visited()+pending(), bs.lazyLabelKeysWalked.Load()-lazyBefore)
			}
		})
	}
}
