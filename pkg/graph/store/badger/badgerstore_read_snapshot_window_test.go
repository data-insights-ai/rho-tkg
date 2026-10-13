package badger

import (
	"fmt"
	"sync"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestReadSnapshotCommitWindows (backlog 55): a snapshot whose transaction
// predates a flush reads a history row in each window the flush opens.
//   - "overlay then read": the flush commits the row and clears the overlay
//     between the snapshot's overlay capture and its Badger read
//     (historyScanTestHook): the overlay must be captured first, so the read
//     re-pins.
//   - "after the overlay cleared": the read runs inside flush right after the
//     overlay dropped the row (flushClearedTestHook): commitGen must already
//     have moved.
//
// Node and relationship.
func TestReadSnapshotCommitWindows(t *testing.T) {
	for _, window := range []string{"overlay then read", "after the overlay cleared"} {
		for _, rel := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/rel=%v", window, rel), func(t *testing.T) {
				bs := newFlushParkStore(t, nil)
				putTestNode(t, bs, 2, 10, nil)
				putTestNode(t, bs, 3, 10, nil)
				v0 := &types.TemporalMetadata{ValidFrom: 11, TxFrom: 22, TxTo: 33}
				var put func() error
				var meta func(storecontract.ReadSnapshot) ([]storecontract.VersionTemporalMeta, error)
				if rel {
					r := types.NewRelationship(types.RelID(7), 5, types.NodeID(2), types.NodeID(3))
					r.SetVersion(0)
					r.SetTemporal(v0)
					put = func() error { return bs.PutRelVersion(types.RelID(7), 0, r) }
					meta = func(s storecontract.ReadSnapshot) ([]storecontract.VersionTemporalMeta, error) {
						return s.RelHistoryTemporalMeta(types.RelID(7))
					}
				} else {
					n := types.NewNode(types.NodeID(1), 10, nil)
					n.SetVersion(0)
					n.SetTemporal(v0)
					put = func() error { return bs.PutNodeVersion(types.NodeID(1), 0, n) }
					meta = func(s storecontract.ReadSnapshot) ([]storecontract.VersionTemporalMeta, error) {
						return s.NodeHistoryTemporalMeta(types.NodeID(1))
					}
				}
				if err := bs.Flush(); err != nil {
					t.Fatal(err)
				}
				snap := bs.NewReadSnapshot()
				defer snap.Close()
				if _, err := snap.NodeHistoryTemporalMeta(types.NodeID(99)); err != nil { // opens the transaction
					t.Fatal(err)
				}
				if err := put(); err != nil {
					t.Fatal(err)
				}
				var (
					got  []storecontract.VersionTemporalMeta
					err  error
					once sync.Once
				)
				if window == "overlay then read" {
					parkPendingIntoFlushing(t, bs)
					bs.historyScanTestHook = func() { once.Do(func() { commitFlushingToBadger(t, bs) }) }
					got, err = meta(snap)
					bs.historyScanTestHook = nil
				} else {
					bs.flushClearedTestHook = func() { once.Do(func() { got, err = meta(snap) }) }
					if ferr := bs.Flush(); ferr != nil {
						t.Fatal(ferr)
					}
					bs.flushClearedTestHook = nil
				}
				if err != nil || len(got) != 1 || got[0].Temporal == nil || got[0].Temporal.TxFrom != 22 {
					t.Fatalf("history skeletons read in the window: %+v, %v; want v0 (TxFrom 22)", got, err)
				}
			})
		}
	}
}
