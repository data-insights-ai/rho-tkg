package tiered

import (
	"math"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestTxMembersFanoutListsNodeRestoredMidRead: a RestoreNode that runs
// between the fan-out's reference-shard read and its archive read (the
// archive's sidecar and valid-end index built for the first time in that
// call) must not drop the node; the history fold lists it, so the label
// membership and the valid-end candidates must too. The restore runs
// concurrently and the hook waits a bounded time for it, so a fan-out that
// holds restores off can still finish.
func TestTxMembersFanoutListsNodeRestoredMidRead(t *testing.T) {
	for _, tc := range []struct {
		name string
		list func(ts *Store, label uint16) (map[types.NodeID]bool, error)
	}{
		{"LabelTxMembership", func(ts *Store, label uint16) (map[types.NodeID]bool, error) {
			got := map[types.NodeID]bool{}
			err := ts.ForEachLabelTxMember(label, func(id types.NodeID, _ types.Instant) bool {
				got[id] = true
				return true
			})
			return got, err
		}},
		{"NodeValidEndCandidates", func(ts *Store, label uint16) (map[types.NodeID]bool, error) {
			cs, err := ts.NodeValidEndCandidates([]uint16{label}, types.Instant(math.MaxInt64), 0)
			got := map[types.NodeID]bool{}
			for _, c := range cs {
				got[types.NodeID(c.ID)] = true
			}
			return got, err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts := newTestTieredStore(t)
			caseTok, _, _ := installDefaultTestLabelRegistry(t, ts)
			n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), caseTok, nil)
			if err := ts.PutNode(n); err != nil {
				t.Fatal(err)
			}
			if err := ts.ArchiveNode(n.ID()); err != nil {
				t.Fatal(err)
			}
			restored := make(chan error, 1)
			fired := false
			ts.fanoutRefReadHook = func() {
				if fired {
					return
				}
				fired = true
				go func() { restored <- ts.RestoreNode(n.ID()) }()
				select {
				case err := <-restored:
					restored <- err
				case <-time.After(time.Second):
				}
			}
			got, err := tc.list(ts, caseTok)
			if err != nil {
				t.Fatal(err)
			}
			if err := <-restored; err != nil {
				t.Fatalf("RestoreNode: %v", err)
			}
			if !fired {
				t.Fatal("hook never ran")
			}
			if !got[n.ID()] {
				t.Fatalf("node %d restored during the fan-out is missing: got %v", n.ID(), got)
			}
		})
	}
}
