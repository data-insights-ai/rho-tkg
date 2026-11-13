package badger

import (
	"context"
	"errors"
	"math"
	"slices"
	"testing"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestMemberWalkFoldStopsAtTheCandidateRoom (R2, gap B): the on-disk member
// walk of a value admits its fold under a read budget as it grows. With room
// for 3 candidates the walk, exact and windowed, with the entries on disk
// (flushed) or in the write buffer (pending), stops at the 4th member it folds
// and returns the typed ReadBudgetError before it hands a member on (fn is
// never called): the counts of entries it visited and emitted, from the work
// diagnostic, stay at the room, not the value's 64 members. A room as wide as
// the value answers the unbudgeted members; one less refuses.
func TestMemberWalkFoldStopsAtTheCandidateRoom(t *testing.T) {
	const members, room = 64, 3
	key := indexpkg.PropertyIndexKey{LabelToken: 1, PropertyKey: "k"}
	vk := types.IndexablePropertyValueKey("v")
	for _, flushed := range []bool{false, true} {
		for _, windowed := range []bool{false, true} {
			name := map[bool]string{false: "pending", true: "flushed"}[flushed] + "/" + map[bool]string{false: "exact", true: "window"}[windowed]
			t.Run(name, func(t *testing.T) {
				bs, err := New(Config{InMemory: true, PropertyIndexOnDisk: true, PropertyKeyRegistry: registrypkg.NewPropertyKeyRegistry(), MaxPendingWrites: -1})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = bs.Close() })
				if err = bs.CreatePropertyIndex(1, "k"); err != nil {
					t.Fatal(err)
				}
				for i := 1; i <= members; i++ {
					if err = bs.PutNode(memberWorkNode(types.NodeID(i))); err != nil {
						t.Fatal(err)
					}
				}
				if flushed {
					if err = bs.Flush(); err != nil {
						t.Fatal(err)
					}
				}
				query := func(tally *storecontract.ReadTally, work *storecontract.PropertyMemberWork) ([]indexpkg.TxMember[types.NodeID], error) {
					q := memberQuery{valueKeys: []string{vk}, tally: tally, work: work}
					if windowed {
						q.window = &memberWindow{0, math.MaxInt64, 0}
					}
					got, handled, err := bs.nodeDiskMembers(key, q)
					if !handled {
						t.Fatal("the definition is not kept on disk")
					}
					return got, err
				}
				want, err := query(nil, nil)
				if err != nil || len(want) != members {
					t.Fatalf("no budget = %d members, %v; want %d", len(want), err, members)
				}

				tight := func(candidates uint64) (*storecontract.ReadMeter, *storecontract.ReadTally) {
					_, m := storecontract.WithReadBudget(context.Background(), storecontract.ReadBudget{Candidates: candidates})
					return m, m.Tally("d")
				}
				var work storecontract.PropertyMemberWork
				meter, tally := tight(room)
				got, err := query(tally, &work)
				var rb *storecontract.ReadBudgetError
				if !errors.As(err, &rb) || rb.Limit != "candidates" || got != nil || meter.Work() != rb.Work {
					t.Fatalf("room %d = %d members, %v; want ErrReadBudget at candidates and no members", room, len(got), err)
				}
				if emitted := work.DiskEntriesEmitted + work.OverlaySetsEmitted; emitted > room+1 {
					t.Fatalf("room %d: the walk emitted %d entries (%+v); want at most %d: it folded the value before it admitted a member", room, emitted, work, room+1)
				}
				if flushed && work.IteratorSteps > 2*(room+1) {
					t.Fatalf("room %d: %d iterator steps (%+v) over a value of %d members", room, work.IteratorSteps, work, members)
				}

				_, tally = tight(members)
				if got, err := query(tally, nil); err != nil || !slices.Equal(got, want) {
					t.Fatalf("room %d = %d members, %v; want the unbudgeted %d", members, len(got), err, len(want))
				}
				_, tally = tight(members - 1)
				if got, err := query(tally, nil); !errors.Is(err, storecontract.ErrReadBudget) || got != nil {
					t.Fatalf("room %d = %d members, %v; want ErrReadBudget", members-1, len(got), err)
				}
				// A meter that already counted candidates leaves the room that is left.
				used, tally := tight(members + 10)
				if err := used.AddCandidates("d", 11); err != nil {
					t.Fatal(err)
				}
				if got, err := query(tally, nil); !errors.Is(err, storecontract.ErrReadBudget) || got != nil {
					t.Fatalf("room %d after 11 counted = %d members, %v; want ErrReadBudget", members-1, len(got), err)
				}
			})
		}
	}
}
