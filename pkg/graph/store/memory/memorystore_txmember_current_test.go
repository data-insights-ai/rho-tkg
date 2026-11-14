package memory

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/generatedcreate"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestTxMemberCopy_CountsEveryNativeMember covers every acquisition door:
// after building the sidecar, add k members and delete m. The native snapshot
// must contain and charge all k entries, with exactly m former members. The
// control and budget k agree; k-1 refuses before any copy or callback.
func TestTxMemberCopy_CountsEveryNativeMember(t *testing.T) {
	const k, m = 4, 2
	const tokL, tokK, tokT = uint16(1), uint16(2), uint16(5)
	const start, end = types.NodeID(1), types.NodeID(2)
	tx := types.Instant(100)
	next := func() types.Instant { tx += 10; return tx }
	rel := func(id int64) *types.Relationship {
		r := types.NewRelationship(types.RelID(id), tokT, start, end)
		at := next()
		r.SetTemporal(&types.TemporalMetadata{TxFrom: at, ValidFrom: at})
		return r
	}
	ids := func(base int64) []int64 {
		out := make([]int64, k)
		for i := range out {
			out[i] = base + int64(i)
		}
		return out
	}
	cases := []struct {
		name    string
		rels    bool
		acquire func(t *testing.T, ms *Store) []int64
	}{
		{"PutNode", false, func(t *testing.T, ms *Store) []int64 {
			out := ids(100)
			for _, id := range out {
				if err := ms.PutNode(mustNode(t, id, []uint16{tokL}, next())); err != nil {
					t.Fatal(err)
				}
			}
			return out
		}},
		{"AddNodeLabelToken", false, func(t *testing.T, ms *Store) []int64 {
			out := ids(100)
			for _, id := range out {
				if err := ms.PutNode(mustNode(t, id, []uint16{tokK}, next())); err != nil {
					t.Fatal(err)
				}
				if err := ms.AddNodeLabelToken(types.NodeID(id), tokL, mustNode(t, id, []uint16{tokK, tokL}, next())); err != nil {
					t.Fatal(err)
				}
			}
			return out
		}},
		{"PutNodesBatch", false, func(t *testing.T, ms *Store) []int64 {
			out := ids(100)
			var batch []*types.Node
			for _, id := range out {
				batch = append(batch, mustNode(t, id, []uint16{tokL}, next()))
			}
			if err := ms.PutNodesBatch(batch); err != nil {
				t.Fatal(err)
			}
			return out
		}},
		{"AddNodeLabelTokenWithHistory", false, func(t *testing.T, ms *Store) []int64 {
			out := ids(100)
			for _, id := range out {
				old := mustNode(t, id, []uint16{tokK}, next())
				if err := ms.PutNode(old); err != nil {
					t.Fatal(err)
				}
				updated := mustNode(t, id, []uint16{tokK, tokL}, next())
				updated.SetVersion(old.Version() + 1)
				if err := ms.AddNodeLabelTokenWithHistory(types.NodeID(id), tokL, updated, old.Version(), old); err != nil {
					t.Fatal(err)
				}
			}
			return out
		}},
		{"PutRelationship", true, func(t *testing.T, ms *Store) []int64 {
			out := ids(200)
			for _, id := range out {
				if err := ms.PutRelationship(rel(id)); err != nil {
					t.Fatal(err)
				}
			}
			return out
		}},
		{"PutRelationshipGeneratedIDWithEndpointHashes", true, func(t *testing.T, ms *Store) []int64 {
			out := ids(200)
			for _, id := range out {
				if _, _, err := ms.PutRelationshipGeneratedIDWithEndpointHashes(rel(id), generatedcreate.FreshGraphID()); err != nil {
					t.Fatal(err)
				}
			}
			return out
		}},
		{"PutRelationshipsBatch", true, func(t *testing.T, ms *Store) []int64 {
			out := ids(200)
			var batch []*types.Relationship
			for _, id := range out {
				batch = append(batch, rel(id))
			}
			if err := ms.PutRelationshipsBatch(batch); err != nil {
				t.Fatal(err)
			}
			return out
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ms := New()
			defer ms.Close()
			for _, id := range []types.NodeID{start, end} {
				if err := ms.PutNode(mustNode(t, int64(id), []uint16{tokK}, next())); err != nil {
					t.Fatal(err)
				}
			}
			gather := func(tl *storecontract.ReadTally, fn func(int64) bool) error {
				if c.rels {
					return ms.ForEachRelTypeTxMemberMetered(tokT, tl, func(id types.RelID, _ types.Instant) bool { return fn(int64(id)) })
				}
				return ms.ForEachLabelTxMemberMetered(tokL, tl, func(id types.NodeID, _ types.Instant) bool { return fn(int64(id)) })
			}
			current := func() map[int64]bool {
				out := map[int64]bool{}
				if c.rels {
					rs, err := ms.RelationshipsByType(tokT, QueryOpts{})
					if err != nil {
						t.Fatal(err)
					}
					for _, r := range rs {
						out[int64(r.ID())] = true
					}
					return out
				}
				ns, err := ms.NodesByLabel(tokL, QueryOpts{})
				if err != nil {
					t.Fatal(err)
				}
				for _, n := range ns {
					out[int64(n.ID())] = true
				}
				return out
			}
			if err := gather(nil, func(int64) bool { return true }); err != nil { // builds the sidecar: the doors below record into it
				t.Fatal(err)
			}
			acquired := c.acquire(t, ms)
			for _, id := range acquired[:m] {
				var err error
				if c.rels {
					err = ms.DeleteRelationship(types.RelID(id))
				} else {
					err = ms.DeleteNode(types.NodeID(id))
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			if cur := current(); len(cur) != k-m {
				t.Fatalf("fixture: %d current members, want %d", len(cur), k-m)
			} else {
				for i, id := range acquired {
					if cur[id] != (i >= m) {
						t.Fatalf("fixture: current membership of %d is %v, want %v", id, cur[id], i >= m)
					}
				}
			}
			copied, stop := CountTxMembersCopiedForTest()
			defer stop()
			run := func(budget uint64) (handed, nonCurrent int, n int64, work storecontract.ReadWork, err error) {
				cur := current()
				_, meter := storecontract.WithReadBudget(context.Background(), storecontract.ReadBudget{Candidates: budget})
				tally := meter.Tally("gather")
				before := copied()
				var seen []int64
				err = gather(tally, func(id int64) bool {
					seen = append(seen, id)
					handed++
					if !cur[id] {
						nonCurrent++
					}
					return true
				})
				if err == nil {
					err = tally.Settle()
				}
				if err == nil {
					slices.Sort(seen)
					if !slices.Equal(seen, acquired) {
						t.Fatalf("gather IDs = %v, want exactly %v", seen, acquired)
					}
				}
				return handed, nonCurrent, copied() - before, meter.Work(), err
			}
			for _, budget := range []uint64{0, k} {
				handed, nonCurrent, n, work, err := run(budget)
				if err != nil || handed != k || n != k || nonCurrent != m || work.Candidates != k {
					t.Fatalf("Candidates %d (0: unlimited): %v, %d handed, %d not current, %d copied, work %+v; want %d handed and copied, %d not current", budget, err, handed, nonCurrent, n, work, k, m)
				}
			}
			handed, _, n, work, err := run(k - 1)
			if !errors.Is(err, storecontract.ErrReadBudget) || handed != 0 || n != 0 {
				t.Fatalf("Candidates %d: %v, %d handed, %d copied, work %+v; want the refusal with nothing copied or handed", k-1, err, handed, n, work)
			}
		})
	}
}
