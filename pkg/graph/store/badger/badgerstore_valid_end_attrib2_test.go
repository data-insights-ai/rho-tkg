package badger

import (
	"errors"
	"slices"
	"sync"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// R2-2 VEATTRIB, review of 83e3093b: the attribution pass's cost, the overlay
// path's committed read, writers between the rows' read and the attribution.

// vaFormerFixture writes labels 1..3 of 20 nodes each, plus 2 nodes per label
// that carried it with label 8 and lost it (former members), and label 9 of
// 50 nodes (IDs 9001..9050) plus 2 former members (9051, 9052, the same way);
// it returns label 9's 52 ever-members.
func vaFormerFixture(t *testing.T, bs *Store) []int64 {
	t.Helper()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	for l := uint16(1); l <= 3; l++ {
		base := types.NodeID(l) * 1000
		for i := types.NodeID(1); i <= 20; i++ {
			must(bs.PutNode(vaNode(base+i, 0, 10, 0, l)))
		}
		for i := types.NodeID(21); i <= 22; i++ {
			prev := vaNode(base+i, 0, 10, 0, l, 8)
			must(bs.PutNode(prev))
			must(bs.RemoveNodeLabelTokenWithHistory(base+i, l, vaNode(base+i, 1, 20, 0, 8), 0, prev))
		}
	}
	var b []int64
	for i := int64(9001); i <= 9050; i++ {
		must(bs.PutNode(vaNode(types.NodeID(i), 0, 10, 0, 9)))
		b = append(b, i)
	}
	for i := types.NodeID(9051); i <= 9052; i++ {
		prev := vaNode(i, 0, 10, 0, 9, 8)
		must(bs.PutNode(prev))
		must(bs.RemoveNodeLabelTokenWithHistory(i, 9, vaNode(i, 1, 20, 0, 8), 0, prev))
		b = append(b, int64(i))
	}
	return b
}

// TestValidEndAttribution_WalksFormerSetsNotEntities (review finding 1, fix
// round 3 D1): the attribution asked every tracked label about every entity
// read, a RAM lookup each under idxMu.RLock, uncounted: label 9's first call
// (50 entities) after labels 1..3 made 4 x 50 = 200 lookups. It now walks, per
// tracked label in token order, the smaller of its former set and the entities
// read: 2 + 2 + 2 + 2 = 8 lookups, whatever the number of entities. An entity of
// the call is counted once, at its read; a former-set entry that is not one (the
// 2 of each of labels 1..3) counts one candidate as it is visited: 6. Label 9's
// own former members (9051, 9052) are entities of the call: not counted again.
// The native counter is the pass's own lookups (validEndAttribLookups), which
// the meter does not keep. The unlimited control answers label 9's 52
// ever-members and counts its listing (50 on disk, none from a RAM map), the 52
// entities and the 6 rejected entries; one candidate less is refused with the
// typed error at label 3's second entry, the 6th lookup: label 9's walk is not
// made, and the refusal counts the control's figure.
func TestValidEndAttribution_WalksFormerSetsNotEntities(t *testing.T) {
	t.Parallel()
	for _, m := range []vaMode{{name: "ram"}, {name: "disk", disk: true}} {
		for _, flushed := range []bool{true, false} {
			t.Run(m.name+"/"+map[bool]string{true: "flushed", false: "pending"}[flushed], func(t *testing.T) {
				t.Parallel()
				var want []int64
				open := func() *Store {
					bs := vaOpen(t, m, flushed, func(bs *Store) { want = vaFormerFixture(t, bs) })
					for l := uint16(1); l <= 3; l++ {
						if _, got, err := vaCall(bs, l, storecontract.ReadBudget{}); err != nil || len(got) != 22 {
							t.Fatalf("label %d = %d candidates, %v; want 22 (20 current, 2 former)", l, len(got), err)
						}
					}
					return bs
				}
				bs := open()
				before := bs.validEndAttribLookups.Load()
				control, got, err := vaCall(bs, 9, storecontract.ReadBudget{})
				lookups := bs.validEndAttribLookups.Load() - before
				if err != nil || !slices.Equal(got, want) {
					t.Fatalf("control = %d candidates, %v; want %d", len(got), err, len(want))
				}
				listing := uint64(0)
				if m.disk {
					listing = 50
				}
				const walk, rejected = 2 + 2 + 2 + 2, 2 + 2 + 2
				count := listing + 52 + rejected
				if c := control.Work().Candidates; lookups != walk || c != count {
					t.Fatalf("label 9's first call made %d attribution lookups and counted %d candidates; want %d and %d (%d listed, 52 entities, %d rejected former entries)", lookups, c, walk, count, listing, rejected)
				}

				bs = open()
				before = bs.validEndAttribLookups.Load()
				_, nodesBefore := bs.ValidEndStats()
				refused, got, err := vaCall(bs, 9, storecontract.ReadBudget{Candidates: count - 1})
				if !errors.Is(err, storecontract.ErrReadBudget) || len(got) != 0 || refused.Work().Candidates != count {
					t.Fatalf("one under the control = %d candidates, %v, %d counted; want ErrReadBudget counting %d", len(got), err, refused.Work().Candidates, count)
				}
				if l := bs.validEndAttribLookups.Load() - before; l != rejected {
					t.Fatalf("the refused call made %d lookups; want %d (refused at the last rejected entry, label 9's walk not made)", l, rejected)
				}
				if _, nodes := bs.ValidEndStats(); nodes.Entities != nodesBefore.Entities {
					t.Fatalf("the refused call installed %+v; before it %+v", nodes, nodesBefore)
				}
			})
		}
	}
}

// TestValidEndOverlay_CommittedRowLabelsOnCacheMiss (review finding 2): an
// entity with only a history row buffered and no cache entry is read by the
// overlay path, whose current row comes from committedTemporal: its labels
// must come with it. Node 1's current row (label 9) is flushed and evicted
// from the cache; an older version carrying label 3 only is then buffered. Label
// 9's first call reads node 1's committed row once (the native count,
// validEndCommittedReads) and lists it; without the row's labels it would be
// dropped (no former membership of 9). Node 2 (label 9, flushed) is the
// control member read by the bulk path.
func TestValidEndOverlay_CommittedRowLabelsOnCacheMiss(t *testing.T) {
	t.Parallel()
	for _, m := range []vaMode{{name: "ram"}, {name: "disk", disk: true}} {
		t.Run(m.name, func(t *testing.T) {
			t.Parallel()
			bs := vaOpen(t, m, false, func(bs *Store) {
				must := func(err error) {
					t.Helper()
					if err != nil {
						t.Fatal(err)
					}
				}
				must(bs.PutNode(vaNode(1, 1, 20, 0, 9)))
				must(bs.PutNode(vaNode(2, 0, 10, 0, 9)))
				must(bs.Flush())
				must(bs.PutNodeVersion(1, 0, vaNode(1, 0, 10, 0, 3)))
			})
			bs.nodeCache.EvictForTest(snowflake.ID(1))
			if _, status := bs.nodeCache.GetNoPromote(snowflake.ID(1)); status != indexpkg.CacheMiss {
				t.Fatal("node 1 is still cached")
			}
			reads := bs.validEndCommittedReads.Load()
			_, got, err := vaCall(bs, 9, storecontract.ReadBudget{})
			if err != nil || !slices.Equal(got, []int64{1, 2}) {
				t.Fatalf("label 9 = %v, %v; want [1 2]", got, err)
			}
			if r := bs.validEndCommittedReads.Load() - reads; r != 1 {
				t.Fatalf("the overlay path read %d committed current rows; want 1 (node 1's, on the cache miss)", r)
			}
			if _, nodes := bs.ValidEndStats(); nodes.Entities != 2 || nodes.Unknown != 0 {
				t.Fatalf("index after the call: %+v; want 2 entities installed", nodes)
			}
		})
	}
}

// TestValidEndAttribution_WriterInTheGap (review finding 4): a writer lands
// between the rows' read and the attribution (validEndAttribHook): node 1
// (label 9) gains label 3 and loses it again. Its fold read before the writes is
// stale, so it must not be installed: the call lists it as unknown, the next
// call re-reads and installs it, and label 3's first call lists it (a former
// member). The control (no writer) installs both nodes.
func TestValidEndAttribution_WriterInTheGap(t *testing.T) {
	t.Parallel()
	for _, m := range []vaMode{{name: "ram"}, {name: "disk", disk: true}} {
		for _, flushed := range []bool{true, false} {
			t.Run(m.name+"/"+map[bool]string{true: "flushed", false: "pending"}[flushed], func(t *testing.T) {
				t.Parallel()
				open := func() *Store {
					return vaOpen(t, m, flushed, func(bs *Store) {
						for i := types.NodeID(1); i <= 2; i++ {
							if err := bs.PutNode(vaNode(i, 0, 10, 0, 9)); err != nil {
								t.Fatal(err)
							}
						}
					})
				}
				want := []int64{1, 2}
				ctl := open()
				if _, got, err := vaCall(ctl, 9, storecontract.ReadBudget{}); err != nil || !slices.Equal(got, want) {
					t.Fatalf("control = %v, %v; want %v", got, err, want)
				}
				if _, nodes := ctl.ValidEndStats(); nodes.Entities != 2 || nodes.Unknown != 0 {
					t.Fatalf("control index %+v; want 2 entities", nodes)
				}
				bs := open()
				fired := 0
				bs.validEndAttribHook = func() {
					fired++
					v0 := vaNode(1, 0, 10, 0, 9)
					v1 := vaNode(1, 1, 20, 0, 9, 3)
					if err := bs.AddNodeLabelTokenWithHistory(1, 3, v1, 0, v0); err != nil {
						t.Error(err)
					}
					if err := bs.RemoveNodeLabelTokenWithHistory(1, 3, vaNode(1, 2, 30, 0, 9), 1, v1); err != nil {
						t.Error(err)
					}
				}
				_, got, err := vaCall(bs, 9, storecontract.ReadBudget{})
				bs.validEndAttribHook = nil
				if fired != 1 || err != nil || !slices.Equal(got, want) {
					t.Fatalf("writer in the gap: hook %d, label 9 = %v, %v; want once, %v", fired, got, err, want)
				}
				if _, nodes := bs.ValidEndStats(); nodes.Entities != 1 || nodes.Unknown != 1 {
					t.Fatalf("after the writer: %+v; want node 2 installed, node 1 unknown", nodes)
				}
				if _, got, err := vaCall(bs, 9, storecontract.ReadBudget{}); err != nil || !slices.Equal(got, want) {
					t.Fatalf("next call = %v, %v; want %v", got, err, want)
				}
				if _, nodes := bs.ValidEndStats(); nodes.Entities != 2 || nodes.Unknown != 0 {
					t.Fatalf("after the next call: %+v; want 2 installed", nodes)
				}
				if _, got, err := vaCall(bs, 3, storecontract.ReadBudget{}); err != nil || !slices.Equal(got, []int64{1}) {
					t.Fatalf("label 3 = %v, %v; want [1] (a former member)", got, err)
				}
			})
		}
	}
}

// TestValidEndAttribution_ConcurrentWriters (review finding 4, run with -race
// by hand): four writers each toggle label 3 on their own 5 nodes of label 9
// while label 9's and label 3's candidates are read in a loop. Every read lists
// all 20 nodes of label 9; after the writers stop, label 3 lists exactly the
// nodes that carried it (the plain scan of their rows).
func TestValidEndAttribution_ConcurrentWriters(t *testing.T) {
	t.Parallel()
	for _, m := range []vaMode{{name: "ram"}, {name: "disk", disk: true}} {
		t.Run(m.name, func(t *testing.T) {
			t.Parallel()
			var all []int64
			bs := vaOpen(t, m, true, func(bs *Store) {
				for i := types.NodeID(1); i <= 20; i++ {
					if err := bs.PutNode(vaNode(i, 0, 10, 0, 9)); err != nil {
						t.Fatal(err)
					}
					all = append(all, int64(i))
				}
			})
			if _, got, err := vaCall(bs, 9, storecontract.ReadBudget{}); err != nil || !slices.Equal(got, all) {
				t.Fatalf("before the writers: %v, %v", got, err)
			}
			var (
				wg      sync.WaitGroup
				stop    = make(chan struct{})
				touched sync.Map
			)
			for w := 0; w < 4; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					cur := map[types.NodeID]*types.Node{}
					for i := 1; i <= 5; i++ {
						id := types.NodeID(w*5 + i)
						cur[id] = vaNode(id, 0, 10, 0, 9)
					}
					for round := 0; ; round++ {
						select {
						case <-stop:
							return
						default:
						}
						id := types.NodeID(w*5 + 1 + round%5)
						prev := cur[id]
						ver := prev.Version() + 1
						tx := types.Instant(100 + int64(ver))
						var err error
						if prev.HasLabelTokenRaw(3) {
							next := vaNode(id, ver, tx, 0, 9)
							err = bs.RemoveNodeLabelTokenWithHistory(id, 3, next, prev.Version(), prev)
							cur[id] = next
						} else {
							next := vaNode(id, ver, tx, 0, 9, 3)
							err = bs.AddNodeLabelTokenWithHistory(id, 3, next, prev.Version(), prev)
							cur[id] = next
							touched.Store(int64(id), true)
						}
						if err != nil {
							t.Error(err)
							return
						}
						if round%7 == 0 {
							if err := bs.Flush(); err != nil {
								t.Error(err)
								return
							}
						}
					}
				}(w)
			}
			for i := 0; i < 30; i++ {
				if _, got, err := vaCall(bs, 9, storecontract.ReadBudget{}); err != nil || !slices.Equal(got, all) {
					t.Errorf("read %d of label 9 = %v, %v; want all 20", i, got, err)
					break
				}
				if _, _, err := vaCall(bs, 3, storecontract.ReadBudget{}); err != nil {
					t.Errorf("read %d of label 3: %v", i, err)
					break
				}
			}
			close(stop)
			wg.Wait()
			var want3 []int64
			touched.Range(func(k, _ any) bool { want3 = append(want3, k.(int64)); return true })
			slices.Sort(want3)
			if _, got, err := vaCall(bs, 3, storecontract.ReadBudget{}); err != nil || !slices.Equal(got, want3) {
				t.Fatalf("label 3 after the writers = %v, %v; want %v (the nodes that carried it)", got, err, want3)
			}
			if _, got, err := vaCall(bs, 9, storecontract.ReadBudget{}); err != nil || !slices.Equal(got, all) {
				t.Fatalf("label 9 after the writers = %v, %v; want all 20", got, err)
			}
		})
	}
}
