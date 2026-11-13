package badger

import (
	"fmt"
	"slices"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestPropTxWindow_DiskNarrowsLikeRAM (RELDISKWIN, R2's second half; ai-soc's
// native_test case): n rows of one type carry seat=1, each valid [1000+1000 i,
// 1010+1000 i); the window [1000, 2010) meets two of them. The windowed
// membership door (store.RelPropertyTxMemberWindowCapability) of a definition
// kept on disk (RelPropertyIndexOnDiskTypes, PropertyIndexOnDiskLabels) must
// answer the members whose valid-time envelope meets the window, as the RAM
// sidecar does, and nothing of the n-2 others: the read budget counts every
// member it hands on as a candidate (TestReadBudget_SelectiveDoorRefusesBroadRead).
// Before RELDISKWIN the disk door handed on every member of the value.
func TestPropTxWindow_DiskNarrowsLikeRAM(t *testing.T) {
	t.Parallel()
	for _, n := range []int{8, 32, 128} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			t.Parallel()
			labels := registrypkg.NewLabelRegistry()
			d, _ := labels.GetOrCreate("D")
			relTypes := registrypkg.NewRelTypeRegistry()
			r, _ := relTypes.GetOrCreate("R")
			open := func(disk bool) *Store {
				t.Helper()
				cfg := Config{InMemory: true, PropertyKeyRegistry: registrypkg.NewPropertyKeyRegistry(), LabelNames: labels, RelTypeNames: relTypes}
				if disk {
					cfg.PropertyIndexOnDiskLabels, cfg.RelPropertyIndexOnDiskTypes = []string{"D"}, []string{"R"}
				}
				bs, err := New(cfg)
				if err != nil {
					t.Fatalf("open disk=%t: %v", disk, err)
				}
				t.Cleanup(func() { _ = bs.Close() })
				if err := bs.CreatePropertyIndex(d, "seat"); err != nil {
					t.Fatal(err)
				}
				if err := bs.CreateRelPropertyIndex(r, "seat"); err != nil {
					t.Fatal(err)
				}
				putTestNode(t, bs, 1, 9, nil)
				putTestNode(t, bs, 2, 9, nil)
				for i := range n {
					vf := types.Instant(1000 + 1000*i)
					tm := &types.TemporalMetadata{TxFrom: 100, ValidFrom: vf, ValidTo: vf + 10}
					x := types.NewRelationship(types.RelID(snowflake.ID(100+i)), r, 1, 2)
					_ = x.SetProperty("seat", int64(1))
					x.SetTemporal(tm)
					if err := bs.PutRelationship(x); err != nil {
						t.Fatalf("put rel %d: %v", i, err)
					}
					nd := types.NewNode(types.NodeID(snowflake.ID(1000+i)), d, nil)
					_ = nd.SetProperty("seat", int64(1))
					nd.SetTemporal(&types.TemporalMetadata{TxFrom: 100, ValidFrom: vf, ValidTo: vf + 10})
					if err := bs.PutNode(nd); err != nil {
						t.Fatalf("put node %d: %v", i, err)
					}
				}
				if err := bs.Flush(); err != nil {
					t.Fatal(err)
				}
				return bs
			}
			vk := winVK(t)
			rels := func(bs *Store, start, end, pin types.Instant) []int64 {
				t.Helper()
				var ids []int64
				if err := bs.ForEachRelPropertyTxMemberDuring(r, "seat", vk, start, end, pin, func(id types.RelID, _ types.Instant) bool {
					ids = append(ids, int64(id))
					return true
				}); err != nil {
					t.Fatalf("rel window: %v", err)
				}
				slices.Sort(ids)
				return ids
			}
			nodes := func(bs *Store, start, end, pin types.Instant) []int64 {
				t.Helper()
				var ids []int64
				if err := bs.ForEachNodePropertyTxMemberDuring(d, "seat", vk, start, end, pin, func(id types.NodeID, _ types.Instant) bool {
					ids = append(ids, int64(id))
					return true
				}); err != nil {
					t.Fatalf("node window: %v", err)
				}
				slices.Sort(ids)
				return ids
			}
			ram, disk := open(false), open(true)
			for _, w := range []struct {
				start, end, pin types.Instant
				want            int
			}{
				{1000, 2010, 0, 2},                        // ai-soc's window: two rows meet it
				{1000, 2010, 1 << 40, 2},                  // pinned after every write: the same two
				{1005, 1006, 0, 1},                        // one row
				{types.UnboundedValidFrom, 1 << 40, 0, n}, // every row
			} {
				if got := rels(ram, w.start, w.end, w.pin); len(got) != w.want {
					t.Fatalf("RAM rel window [%d,%d)@%d = %v, want %d members", w.start, w.end, w.pin, got, w.want)
				}
				if got, want := rels(disk, w.start, w.end, w.pin), rels(ram, w.start, w.end, w.pin); !slices.Equal(got, want) {
					t.Errorf("disk rel window [%d,%d)@%d = %d members %v, want RAM's %d %v", w.start, w.end, w.pin, len(got), got, len(want), want)
				}
				if got, want := nodes(disk, w.start, w.end, w.pin), nodes(ram, w.start, w.end, w.pin); !slices.Equal(got, want) {
					t.Errorf("disk node window [%d,%d)@%d = %d members %v, want RAM's %d %v", w.start, w.end, w.pin, len(got), got, len(want), want)
				}
			}
		})
	}
}
