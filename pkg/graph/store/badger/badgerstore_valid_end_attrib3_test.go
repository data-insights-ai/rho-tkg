package badger

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	badgerv4 "github.com/dgraph-io/badger/v4"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// R2-2 VEATTRIB, fix round 3 (design-veattrib3.txt): the attribution walk's
// count (an entity read is counted once; a former-set entry that is not an
// entity of the call counts one candidate as it is visited), its two sides,
// its lock holds, and the valid-end read's DecodedBytes charged before each
// value is fetched.

var vaRAMDisk = []vaMode{{name: "ram"}, {name: "disk", disk: true}}

func vaFlushedName(flushed bool) string {
	return map[bool]string{true: "flushed", false: "pending"}[flushed]
}

// vaMust fails t on err.
func vaMust(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// vaDeleted writes node id with labels and deletes it (a tombstone at version
// 0, deleted at 30): a former member of each label.
func vaDeleted(t *testing.T, bs *Store, id types.NodeID, labels ...uint16) {
	t.Helper()
	vaMust(t, bs.PutNode(vaNode(id, 0, 10, 0, labels...)))
	vaMust(t, bs.DeleteNodeWithHistory(id, 0, vaNode(id, 0, 10, 30, labels...), nil))
}

// vaIDs is from..to.
func vaIDs(from, to int64) []int64 {
	var out []int64
	for i := from; i <= to; i++ {
		out = append(out, i)
	}
	return out
}

// vaCounters is the native counts of one call: attribution lookups, lock holds
// and row decodes (the bulk read's and the history overlay's).
type vaCounters struct{ lookups, holds, decodes, metaDecodes, steps int64 }

func vaCount(bs *Store) vaCounters {
	return vaCounters{bs.validEndAttribLookups.Load(), bs.validEndAttribHolds.Load(), bs.validEndRowDecodes.Load(), bs.historyMetaDecodes.Load(), bs.validEndIteratorSteps.Load()}
}

func (c vaCounters) since(b vaCounters) vaCounters {
	return vaCounters{c.lookups - b.lookups, c.holds - b.holds, c.decodes - b.decodes, c.metaDecodes - b.metaDecodes, c.steps - b.steps}
}

// TestValidEndAttribution_OwnFormerMembersCountOnce (D1): label 9 has 6
// current members and 2 former ones (deleted). Its first call reads the 8
// ever-members; the attribution walks label 9's former set
// (2 < 8 entities: the former side), 2 lookups, both entities of this call,
// already counted: no candidate more. The control counts the label listing,
// enlistment, pending capture, entity reads, iterator steps and result copies,
// with one attribution lock hold. A tight candidate limit refuses enlistment:
// no attribution lookup and nothing installed.
func TestValidEndAttribution_OwnFormerMembersCountOnce(t *testing.T) {
	t.Parallel()
	want := vaIDs(1, 8)
	for _, m := range vaRAMDisk {
		for _, flushed := range []bool{true, false} {
			t.Run(m.name+"/"+vaFlushedName(flushed), func(t *testing.T) {
				t.Parallel()
				open := func() *Store {
					return vaOpen(t, m, flushed, func(bs *Store) {
						for i := types.NodeID(1); i <= 6; i++ {
							vaMust(t, bs.PutNode(vaNode(i, 0, 10, 0, 9)))
						}
						vaDeleted(t, bs, 7, 9)
						vaDeleted(t, bs, 8, 9)
					})
				}
				listing := uint64(0)
				if m.disk {
					listing = 6
					if !flushed {
						listing += 2 // the two pending label deletes are examined too
					}
				}
				bs := open()
				b := vaCount(bs)
				control, got, err := vaCall(bs, 9, storecontract.ReadBudget{})
				d := vaCount(bs).since(b)
				if err != nil || !slices.Equal(got, want) {
					t.Fatalf("control = %v, %v; want %v", got, err, want)
				}
				if c := control.Work().Candidates; c != listing+4*8+uint64(d.steps) || d.lookups != 2 || d.holds != 1 {
					t.Fatalf("control: %d candidates, %d lookups, %d holds; want %d (%d listed, 8 entities, the 2 former members not again), 2, 1", c, d.lookups, d.holds, listing+4*8+uint64(d.steps), listing)
				}
				bs = open()
				b = vaCount(bs)
				if _, got, err := vaCall(bs, 9, storecontract.ReadBudget{Candidates: listing + 8 - 1}); !errors.Is(err, storecontract.ErrReadBudget) || len(got) != 0 {
					t.Fatalf("one under the control = %v, %v; want ErrReadBudget", got, err)
				}
				if _, nodes := bs.ValidEndStats(); vaCount(bs).since(b).lookups != 0 || nodes.Entities != 0 || nodes.Unknown != 0 || nodes.Tokens != 0 {
					t.Fatalf("the refused call: %d lookups, index %+v; want none and nothing installed", vaCount(bs).since(b).lookups, nodes)
				}
			})
		}
	}
}

// TestValidEndAttribution_EntitySide (D2): a token whose former set is larger
// than the entities read is walked on the entity side, each entity asked once,
// no candidate more (every entity is counted at its read).
//
// (a) Steady state: label 3 (20 former members, deleted) and label 9 (5
// nodes) tracked; 3 of label 3's members are made unknown (their tombstone
// history row written again). Label 3's call reads the 3; token
// 3's walk: 20 > 3, the entity side, 3 lookups; token 9's former set is empty
// (the former side, 0 lookups). It lists the 20, nothing unknown after. Under 2
// candidates it is refused before any read: no lookup, no row decoded, 3
// unknown.
//
// (b) First call: label 3 with 60 former members tracked, label 9 with 50
// nodes: token 3 is walked on the entity side, 50 lookups. Candidate admission
// includes each native listing, copy, row read and forward step.
func TestValidEndAttribution_EntitySide(t *testing.T) {
	t.Parallel()
	for _, m := range vaRAMDisk {
		for _, flushed := range []bool{true, false} {
			t.Run(m.name+"/"+vaFlushedName(flushed), func(t *testing.T) {
				t.Parallel()
				t.Run("steady", func(t *testing.T) {
					want3 := vaIDs(3001, 3020)
					open := func() *Store {
						bs := vaOpen(t, m, flushed, func(bs *Store) {
							for _, id := range want3 {
								vaDeleted(t, bs, types.NodeID(id), 3)
							}
							for i := types.NodeID(9001); i <= 9005; i++ {
								vaMust(t, bs.PutNode(vaNode(i, 0, 10, 0, 9)))
							}
						})
						for _, l := range []uint16{3, 9} {
							if _, _, err := vaCall(bs, l, storecontract.ReadBudget{}); err != nil {
								t.Fatal(err)
							}
						}
						for i := types.NodeID(3001); i <= 3003; i++ {
							vaMust(t, bs.PutNodeVersion(i, 0, vaNode(i, 0, 10, 30, 3)))
						}
						if _, nodes := bs.ValidEndStats(); nodes.Unknown != 3 {
							t.Fatalf("after the writes: %+v; want 3 unknown", nodes)
						}
						return bs
					}
					bs := open()
					b := vaCount(bs)
					control, got, err := vaCall(bs, 3, storecontract.ReadBudget{})
					d := vaCount(bs).since(b)
					if err != nil || !slices.Equal(got, want3) {
						t.Fatalf("control = %v, %v; want %v", got, err, want3)
					}
					if c := control.Work().Candidates; c != 2*3+20+uint64(d.steps) || d.lookups != 3+0 {
						t.Fatalf("control: %d candidates, %d lookups; want %d native admissions and 3 attribution lookups", c, d.lookups, 2*3+20+uint64(d.steps))
					}
					if _, nodes := bs.ValidEndStats(); nodes.Unknown != 0 {
						t.Fatalf("after the control: %+v; want nothing unknown", nodes)
					}
					bs = open()
					b = vaCount(bs)
					if _, got, err := vaCall(bs, 3, storecontract.ReadBudget{Candidates: 2}); !errors.Is(err, storecontract.ErrReadBudget) || len(got) != 0 {
						t.Fatalf("under 2 = %v, %v; want ErrReadBudget", got, err)
					}
					d = vaCount(bs).since(b)
					if _, nodes := bs.ValidEndStats(); d.lookups != 0 || d.decodes != 0 || d.metaDecodes != 0 || nodes.Unknown != 3 {
						t.Fatalf("the refused call: %+v, index %+v; want no lookup, no row decoded, 3 unknown", d, nodes)
					}
				})
				t.Run("first", func(t *testing.T) {
					open := func() *Store { return vaEntitySideOpen(t, m, flushed, 60) }
					listing := uint64(0)
					if m.disk {
						listing = 50
					}
					bs := open()
					b := vaCount(bs)
					control, got, err := vaCall(bs, 9, storecontract.ReadBudget{})
					d := vaCount(bs).since(b)
					if err != nil || !slices.Equal(got, vaIDs(9001, 9050)) {
						t.Fatalf("control = %d candidates, %v; want label 9's 50", len(got), err)
					}
					if c := control.Work().Candidates; c != listing+4*50+uint64(d.steps) || d.lookups != 50 {
						t.Fatalf("control: %d candidates, %d lookups; want %d (%d listed, 50 entities) and 50 (token 3's entity side)", c, d.lookups, listing+4*50+uint64(d.steps), listing)
					}
					bs = open()
					b = vaCount(bs)
					_, before := bs.ValidEndStats()
					if _, got, err := vaCall(bs, 9, storecontract.ReadBudget{Candidates: listing + 50 - 1}); !errors.Is(err, storecontract.ErrReadBudget) || len(got) != 0 {
						t.Fatalf("one under the control = %v, %v; want ErrReadBudget", got, err)
					}
					if _, after := bs.ValidEndStats(); vaCount(bs).since(b).lookups != 0 || after.Entities != before.Entities || after.Unknown != before.Unknown || after.Tokens != before.Tokens {
						t.Fatalf("the refused call: %d lookups, index %+v; before %+v", vaCount(bs).since(b).lookups, after, before)
					}
				})
			})
		}
	}
}

// vaEntitySideOpen writes label 3 of `former` deleted nodes (3001..) and label
// 9 of 50 nodes (9001..9050), and tracks label 3.
func vaEntitySideOpen(t *testing.T, m vaMode, flushed bool, former int64) *Store {
	t.Helper()
	bs := vaOpen(t, m, flushed, func(bs *Store) {
		for i := int64(3001); i < 3001+former; i++ {
			vaDeleted(t, bs, types.NodeID(i), 3)
		}
		for i := types.NodeID(9001); i <= 9050; i++ {
			vaMust(t, bs.PutNode(vaNode(i, 0, 10, 0, 9)))
		}
	})
	if _, got, err := vaCall(bs, 3, storecontract.ReadBudget{}); err != nil || int64(len(got)) != former {
		t.Fatalf("label 3 = %d candidates, %v; want %d", len(got), err, former)
	}
	return bs
}

// TestValidEndAttribution_HoldsBounded (D6): the attribution pass holds the
// index lock for at most validEndAttribHoldMax lookups at a time (4 here).
// (a) 4 tracked labels with empty former sets, 5 entities: one hold. (b) The
// entity side of TestValidEndAttribution_EntitySide's first call, 50 lookups:
// the lock is retaken before lookups 5, 9, ..., 49, 13 holds; the answer and the
// count equal the default's (one hold). (c) A former set of 5 against 50
// entities: 5 is more than one hold, so the entity side, 50 lookups, no
// candidate; the default walks the former side, 5 lookups, 5 rejected entries
// counted.
func TestValidEndAttribution_HoldsBounded(t *testing.T) {
	t.Parallel()
	for _, m := range vaRAMDisk {
		for _, flushed := range []bool{true, false} {
			t.Run(m.name+"/"+vaFlushedName(flushed), func(t *testing.T) {
				t.Parallel()
				listing := uint64(0)
				if m.disk {
					listing = 50
				}
				t.Run("empty", func(t *testing.T) {
					bs := vaOpen(t, m, flushed, func(bs *Store) {
						for l := uint16(1); l <= 3; l++ {
							for i := types.NodeID(1); i <= 2; i++ {
								vaMust(t, bs.PutNode(vaNode(types.NodeID(l)*100+i, 0, 10, 0, l)))
							}
						}
						for i := types.NodeID(9001); i <= 9005; i++ {
							vaMust(t, bs.PutNode(vaNode(i, 0, 10, 0, 9)))
						}
					})
					for l := uint16(1); l <= 3; l++ {
						if _, _, err := vaCall(bs, l, storecontract.ReadBudget{}); err != nil {
							t.Fatal(err)
						}
					}
					bs.validEndAttribHoldMax = 4
					b := vaCount(bs)
					if _, got, err := vaCall(bs, 9, storecontract.ReadBudget{}); err != nil || !slices.Equal(got, vaIDs(9001, 9005)) {
						t.Fatalf("label 9 = %v, %v", got, err)
					}
					if d := vaCount(bs).since(b); d.holds != 1 || d.lookups != 0 {
						t.Fatalf("4 tracked labels, empty former sets: %d holds, %d lookups; want 1 and 0", d.holds, d.lookups)
					}
				})
				for _, c := range []struct {
					name                 string
					former               int64
					holds, lookups       int64
					extra                uint64 // candidates beyond the listing and the 50 entities
					ctlHolds, ctlLookups int64
					ctlExtra             uint64
				}{
					{"entity", 60, 13, 50, 0, 1, 50, 0},
					{"former-over-hold", 5, 13, 50, 0, 1, 5, 5},
				} {
					t.Run(c.name, func(t *testing.T) {
						ctl := vaEntitySideOpen(t, m, flushed, c.former)
						b := vaCount(ctl)
						cm, want, err := vaCall(ctl, 9, storecontract.ReadBudget{})
						d := vaCount(ctl).since(b)
						if err != nil || !slices.Equal(want, vaIDs(9001, 9050)) || cm.Work().Candidates != listing+4*50+c.ctlExtra+uint64(d.steps) || d.holds != c.ctlHolds || d.lookups != c.ctlLookups {
							t.Fatalf("default hold: %d candidates, %v, %d counted, %+v; want 50, %d, %d holds, %d lookups", len(want), err, cm.Work().Candidates, d, listing+4*50+c.ctlExtra+uint64(d.steps), c.ctlHolds, c.ctlLookups)
						}
						bs := vaEntitySideOpen(t, m, flushed, c.former)
						bs.validEndAttribHoldMax = 4
						b = vaCount(bs)
						bm, got, err := vaCall(bs, 9, storecontract.ReadBudget{})
						d = vaCount(bs).since(b)
						if err != nil || !slices.Equal(got, want) {
							t.Fatalf("hold 4 = %v, %v; want the default's %v", got, err, want)
						}
						if cand := bm.Work().Candidates; cand != listing+4*50+c.extra+uint64(d.steps) || d.holds != c.holds || d.lookups != c.lookups {
							t.Fatalf("hold 4: %d candidates, %d holds, %d lookups; want %d, %d, %d", cand, d.holds, d.lookups, listing+4*50+c.extra+uint64(d.steps), c.holds, c.lookups)
						}
					})
				}
			})
		}
	}
}

// vaRel is relationship id of type typ from node 1001 to 1002, version ver,
// written at tx, deleted at deleted (0: live).
func vaRel(id types.RelID, typ uint16, ver uint32, tx, deleted types.Instant) *types.Relationship {
	r := types.NewRelationship(id, typ, 1001, 1002)
	r.SetVersion(ver)
	r.SetTemporal(&types.TemporalMetadata{TxFrom: tx, DeletedAt: deleted, TxTo: deleted})
	return r
}

// vaEndpoints writes the relationships' endpoints, nodes 1001 and 1002 of label 2.
func vaEndpoints(t *testing.T, bs *Store) {
	t.Helper()
	for _, id := range []types.NodeID{1001, 1002} {
		vaMust(t, bs.PutNode(vaNode(id, 0, 10, 0, 2)))
	}
}

// vaRelCall is RelValidEndCandidatesMetered of typ under budget, the IDs sorted.
func vaRelCall(bs *Store, typ uint16, budget storecontract.ReadBudget) (*storecontract.ReadMeter, []int64, error) {
	_, m := storecontract.WithReadBudget(context.Background(), budget)
	cs, err := bs.RelValidEndCandidatesMetered(typ, 1_000, 0, m.Tally("probe"))
	var ids []int64
	for _, c := range cs {
		ids = append(ids, c.ID)
	}
	slices.Sort(ids)
	return m, ids, err
}

// vaN5 is TestValidEndRows_ChargeEachValueBeforeItsFetch's store of one kind:
// entities 1 and 2 with a history row (version 0) and a current row (version
// 1), flushed (the bulk path); entity 3 with a current row (version 2) and a
// history row (version 0) flushed, its cache entry evicted, and a history row
// of version 1 buffered (the overlay path). It returns the store and the
// rows' sizes in the order the call charges them: 1h 1c 2h 2c (the bulk view),
// 3c (the committed current row) 3h (the committed history) 3h' (the buffered
// row, from RAM).
func vaN5(t *testing.T, node bool) (*Store, []int) {
	t.Helper()
	bs, err := New(Config{Dir: t.TempDir(), FlushInterval: time.Hour})
	vaMust(t, err)
	t.Cleanup(func() { _ = bs.Close() })
	if !node {
		vaEndpoints(t, bs)
	}
	put := func(id int64, cur uint32, hist ...uint32) {
		t.Helper()
		if node {
			vaMust(t, bs.PutNode(vaNode(types.NodeID(id), cur, types.Instant(10+cur), 0, 1)))
			for _, v := range hist {
				vaMust(t, bs.PutNodeVersion(types.NodeID(id), v, vaNode(types.NodeID(id), v, types.Instant(10+v), 0, 1)))
			}
			return
		}
		vaMust(t, bs.PutRelationship(vaRel(types.RelID(id), 1, cur, types.Instant(10+cur), 0)))
		for _, v := range hist {
			vaMust(t, bs.PutRelVersion(types.RelID(id), v, vaRel(types.RelID(id), 1, v, types.Instant(10+v), 0)))
		}
	}
	put(1, 1, 0)
	put(2, 1, 0)
	put(3, 2, 0)
	vaMust(t, bs.Flush())
	put3 := func() {
		if node {
			vaMust(t, bs.PutNodeVersion(3, 1, vaNode(3, 1, 11, 0, 1)))
		} else {
			vaMust(t, bs.PutRelVersion(3, 1, vaRel(3, 1, 1, 11, 0)))
		}
	}
	put3()
	var status indexpkg.CacheStatus
	if node {
		bs.nodeCache.EvictForTest(snowflake.ID(3))
		_, status = bs.nodeCache.GetNoPromote(snowflake.ID(3))
	} else {
		bs.relCache.EvictForTest(snowflake.ID(3))
		_, status = bs.relCache.GetNoPromote(snowflake.ID(3))
	}
	if status != indexpkg.CacheMiss {
		t.Fatal("entity 3 is still cached")
	}
	curKey, histKey := storepkg.RelKey, storepkg.HistRelKey
	if node {
		curKey, histKey = storepkg.NodeKey, storepkg.HistNodeKey
	}
	var sizes []int
	vaMust(t, bs.db.View(func(txn *badgerv4.Txn) error {
		for _, k := range [][]byte{histKey(1, 0), curKey(1), histKey(2, 0), curKey(2), curKey(3), histKey(3, 0)} {
			item, err := txn.Get(k)
			if err != nil {
				return err
			}
			sizes = append(sizes, int(item.ValueSize()))
		}
		return nil
	}))
	op, found := bs.lookupPending(string(histKey(3, 1)))
	if !found {
		t.Fatal("entity 3's history row of version 1 is not buffered")
	}
	vaMust(t, bs.ensureLabelTxMembersBuilt())
	vaMust(t, bs.ensureRelTypeTxMembersBuilt())
	return bs, append(sizes, len(op.value))
}

// TestValidEndRows_ChargeEachValueBeforeItsFetch (D3): the valid-end read
// charges each row's stored size (Item.ValueSize) as DecodedBytes before it
// fetches the value, at all three fetch sites: the bulk view's add, the
// overlay path's committed current row (committedTemporal) and its committed
// history rows (historyTemporalMetaByPrefixTallied); a buffered history row,
// already in RAM, is charged its length before its decode. The native counters
// are the values fetched from badger and their bytes, which the meter does not
// keep. The control counts the fetched bytes and the buffered row's, 6 fetches,
// 4 history versions plus one pending capture, one committed current row read;
// a DecodedBytes limit one
// under the first k rows' sizes refuses at the k-th row with k-1 values
// fetched (k = 7, the buffered row: 6 fetched, one history row less decoded).
// Nodes and relationships.
func TestValidEndRows_ChargeEachValueBeforeItsFetch(t *testing.T) {
	t.Parallel()
	for _, node := range []bool{true, false} {
		t.Run(map[bool]string{true: "nodes", false: "rels"}[node], func(t *testing.T) {
			t.Parallel()
			call := func(bs *Store, b storecontract.ReadBudget) (*storecontract.ReadMeter, []int64, error) {
				if node {
					return vaCall(bs, 1, b)
				}
				return vaRelCall(bs, 1, b)
			}
			stats := func(bs *Store) storecontract.ValidEndStats {
				rels, nodes := bs.ValidEndStats()
				if node {
					return nodes
				}
				return rels
			}
			bs, sizes := vaN5(t, node)
			fetches, fetched, reads, metas := bs.validEndValueFetches.Load(), bs.validEndFetchedBytes.Load(), bs.validEndCommittedReads.Load(), bs.historyMetaDecodes.Load()
			control, got, err := call(bs, storecontract.ReadBudget{})
			if err != nil || !slices.Equal(got, []int64{1, 2, 3}) {
				t.Fatalf("control = %v, %v; want [1 2 3]", got, err)
			}
			fetches, fetched, reads, metas = bs.validEndValueFetches.Load()-fetches, bs.validEndFetchedBytes.Load()-fetched, bs.validEndCommittedReads.Load()-reads, bs.historyMetaDecodes.Load()-metas
			w := control.Work()
			// The pending row is examined when captured and again when decoded.
			if w.DecodedBytes != uint64(fetched)+uint64(sizes[6]) || fetches != 6 || w.HistoryExamined != 5 || reads != 1 {
				t.Fatalf("control: %+v, %d values fetched (%d bytes), %d committed current rows read; want DecodedBytes %d (fetched + the buffered row's %d), 6 fetched, 5 history examinations, 1 committed read", w, fetches, fetched, reads, uint64(fetched)+uint64(sizes[6]), sizes[6])
			}
			sum := 0
			for k := 1; k <= 7; k++ {
				sum += sizes[k-1]
				t.Run(fmt.Sprintf("k=%d", k), func(t *testing.T) {
					bs, again := vaN5(t, node)
					if !slices.Equal(again, sizes) {
						t.Fatalf("sizes %v; the control's %v", again, sizes)
					}
					f0, m0 := bs.validEndValueFetches.Load(), bs.historyMetaDecodes.Load()
					_, got, err := call(bs, storecontract.ReadBudget{DecodedBytes: uint64(sum - 1)})
					var rb *storecontract.ReadBudgetError
					if !errors.As(err, &rb) || rb.Limit != "decoded bytes" || len(got) != 0 {
						t.Fatalf("limit %d: %v, %v; want ErrReadBudget at decoded bytes", sum-1, got, err)
					}
					f := bs.validEndValueFetches.Load() - f0
					if f != int64(k-1) {
						t.Fatalf("limit %d (one under the first %d rows): %d values fetched; want %d (charged before the fetch)", sum-1, k, f, k-1)
					}
					if k == 7 {
						if md := bs.historyMetaDecodes.Load() - m0; md != metas-1 {
							t.Fatalf("refused at the buffered row: %d history rows decoded; want %d", md, metas-1)
						}
					}
					if st := stats(bs); st.Entities != 0 {
						t.Fatalf("the refused call installed %+v", st)
					}
				})
			}
		})
	}
}

// TestValidEndOverlay_CommittedRowTypeOnCacheMiss (D4): relationship 1 (type
// 9) is flushed, evicted from the cache, and one older history version is
// buffered: the overlay path reads its committed current row
// (committedTemporal) and must take the type from it; a current member is not
// in the type's former set, so without it relationship 1 is dropped.
// Relationship 2 (type 9, flushed) is read on the bulk path.
func TestValidEndOverlay_CommittedRowTypeOnCacheMiss(t *testing.T) {
	t.Parallel()
	for _, m := range vaRAMDisk {
		t.Run(m.name, func(t *testing.T) {
			t.Parallel()
			bs := vaOpen(t, m, false, func(bs *Store) {
				vaEndpoints(t, bs)
				vaMust(t, bs.PutRelationship(vaRel(1, 9, 1, 20, 0)))
				vaMust(t, bs.PutRelationship(vaRel(2, 9, 0, 10, 0)))
				vaMust(t, bs.Flush())
				vaMust(t, bs.PutRelVersion(1, 0, vaRel(1, 9, 0, 10, 0)))
			})
			bs.relCache.EvictForTest(snowflake.ID(1))
			if _, status := bs.relCache.GetNoPromote(snowflake.ID(1)); status != indexpkg.CacheMiss {
				t.Fatal("relationship 1 is still cached")
			}
			reads := bs.validEndCommittedReads.Load()
			if _, got, err := vaRelCall(bs, 9, storecontract.ReadBudget{}); err != nil || !slices.Equal(got, []int64{1, 2}) {
				t.Fatalf("type 9 = %v, %v; want [1 2]", got, err)
			}
			if r := bs.validEndCommittedReads.Load() - reads; r != 1 {
				t.Fatalf("%d committed current rows read; want 1 (relationship 1's, on the cache miss)", r)
			}
			if rels, _ := bs.ValidEndStats(); rels.Entities != 2 || rels.Unknown != 0 {
				t.Fatalf("index after the call: %+v; want 2 entities", rels)
			}
		})
	}
}

// TestValidEndAttribution_RelFormerWalkCounts (D8): type 1 has 22
// relationships, 2 of them deleted (its former set); type 9 53, 3 deleted.
// Type 1 is tracked first. Type 9's first call reads its 53 relationships;
// the attribution walks type 1's former set (2, neither read: 2
// rejected entries, 2 candidates) and type 9's (3, all read: none): 5 lookups,
// 75 relationships held. Admission includes enlistment, capture, row reads,
// iterator steps and result copies. A limit inside attribution refuses type
// 1's second entry: 2 lookups, nothing installed.
func TestValidEndAttribution_RelFormerWalkCounts(t *testing.T) {
	t.Parallel()
	for _, m := range vaRAMDisk {
		for _, flushed := range []bool{true, false} {
			t.Run(m.name+"/"+vaFlushedName(flushed), func(t *testing.T) {
				t.Parallel()
				open := func() *Store {
					bs := vaOpen(t, m, flushed, func(bs *Store) {
						vaEndpoints(t, bs)
						write := func(typ uint16, from, n, deleted int64) {
							for i := from; i < from+n; i++ {
								vaMust(t, bs.PutRelationship(vaRel(types.RelID(i), typ, 0, 10, 0)))
								if i >= from+n-deleted {
									vaMust(t, bs.DeleteRelWithHistory(types.RelID(i), 0, vaRel(types.RelID(i), typ, 0, 10, 30)))
								}
							}
						}
						write(1, 1001, 22, 2)
						write(9, 9001, 53, 3)
					})
					if _, got, err := vaRelCall(bs, 1, storecontract.ReadBudget{}); err != nil || len(got) != 22 {
						t.Fatalf("type 1 = %d candidates, %v; want 22", len(got), err)
					}
					return bs
				}
				bs := open()
				b := vaCount(bs)
				control, got, err := vaRelCall(bs, 9, storecontract.ReadBudget{})
				d := vaCount(bs).since(b)
				if err != nil || !slices.Equal(got, vaIDs(9001, 9053)) {
					t.Fatalf("control = %d candidates, %v; want type 9's 53", len(got), err)
				}
				rels, _ := bs.ValidEndStats()
				if c := control.Work().Candidates; c != 4*53+2+uint64(d.steps) || d.lookups != 2+3 || rels.Entities != 75 {
					t.Fatalf("control: %d candidates, %d lookups, %d held; want %d native admissions, 5 lookups, 75 held", c, d.lookups, rels.Entities, 4*53+2+uint64(d.steps))
				}
				beforeAttribution := uint64(3*53) + uint64(d.steps)
				bs = open()
				b = vaCount(bs)
				before, _ := bs.ValidEndStats()
				if _, got, err := vaRelCall(bs, 9, storecontract.ReadBudget{Candidates: beforeAttribution + 1}); !errors.Is(err, storecontract.ErrReadBudget) || len(got) != 0 {
					t.Fatalf("inside attribution = %v, %v; want ErrReadBudget", got, err)
				}
				if after, _ := bs.ValidEndStats(); vaCount(bs).since(b).lookups != 2 || after.Entities != before.Entities {
					t.Fatalf("the refused call: %d lookups, %+v held; want 2 (refused at type 1's second entry), %+v", vaCount(bs).since(b).lookups, after, before)
				}
			})
		}
	}
}
