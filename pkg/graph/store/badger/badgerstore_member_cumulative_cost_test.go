package badger

import (
	"context"
	"errors"
	"math"
	"testing"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// cumulativeStore opens a store whose node definition (label 1, "k") is kept on
// disk and puts n nodes of value "v" valid [from(i), from(i)+1), flushed or
// left in the write buffer.
func cumulativeStore(t *testing.T, n int, flushed bool, from func(i int) types.Instant) *Store {
	t.Helper()
	bs, err := New(Config{InMemory: true, PropertyIndexOnDisk: true, PropertyKeyRegistry: registrypkg.NewPropertyKeyRegistry(), MaxPendingWrites: -1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	if err = bs.CreatePropertyIndex(1, "k"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		nd := memberWorkNode(types.NodeID(i + 1))
		nd.SetTemporal(&types.TemporalMetadata{TxFrom: 1, ValidFrom: from(i), ValidTo: from(i) + 1})
		if err = bs.PutNode(nd); err != nil {
			t.Fatal(err)
		}
	}
	if flushed {
		if err = bs.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	return bs
}

func cumulativeTally(b storecontract.ReadBudget) (*storecontract.ReadMeter, *storecontract.ReadTally) {
	_, m := storecontract.WithReadBudget(context.Background(), b)
	return m, m.Tally("d")
}

// TestMemberWindowWalkCountsRejectsAndAcceptedOnce (R2 cumulative, 3.2): a
// window [100, 101) over a value of 4 members, 2 valid in it and 2 valid after
// it. The 2 rejected entries are charged to the meter as the walk judges them,
// the 2 accepted members once (by the store when it hands them on, by the door
// when DoorCounts), so the read costs 4 candidates: Candidates 3 refuses
// (the fold of 2 against the room the 2 charges left, 1), 4 answers the
// unbudgeted members. Pending and flushed.
func TestMemberWindowWalkCountsRejectsAndAcceptedOnce(t *testing.T) {
	key := indexpkg.PropertyIndexKey{LabelToken: 1, PropertyKey: "k"}
	vk := types.IndexablePropertyValueKey("v")
	for _, flushed := range []bool{false, true} {
		t.Run(map[bool]string{false: "pending", true: "flushed"}[flushed], func(t *testing.T) {
			bs := cumulativeStore(t, 4, flushed, func(i int) types.Instant { return []types.Instant{100, 500, 100, 600}[i] })
			query := func(tally *storecontract.ReadTally) ([]indexpkg.TxMember[types.NodeID], error) {
				got, handled, err := bs.nodeDiskMembers(key, memberQuery{valueKeys: []string{vk}, tally: tally, window: &memberWindow{100, 101, 0}})
				if !handled {
					t.Fatal("the definition is not kept on disk")
				}
				return got, err
			}
			want, err := query(nil)
			if err != nil || len(want) != 2 {
				t.Fatalf("no budget: %d members, %v; want 2", len(want), err)
			}
			m, tally := cumulativeTally(storecontract.ReadBudget{Candidates: 3})
			var rb *storecontract.ReadBudgetError
			if got, err := query(tally); !errors.As(err, &rb) || rb.Limit != "candidates" || got != nil {
				t.Fatalf("Candidates 3: %d members, %v, work %+v; want the candidates refusal (2 rejected + 2 accepted)", len(got), err, m.Work())
			}
			m, tally = cumulativeTally(storecontract.ReadBudget{Candidates: 4})
			if got, err := query(tally); err != nil || len(got) != 2 || m.Work().Candidates != 2 {
				t.Fatalf("Candidates 4: %d members, %v, work %+v; want the 2 members, the 2 rejected entries counted (the door counts the 2 it is handed)", len(got), err, m.Work())
			}
			// A direct caller: the store counts the 2 members it hands on as well.
			m, tally = cumulativeTally(storecontract.ReadBudget{Candidates: 4})
			handed := 0
			err = bs.ForEachNodePropertyTxMemberMetered(1, "k", []string{vk}, &storecontract.PropertyTxMemberWindow{ValidStart: 100, ValidEnd: 101}, tally, func(types.NodeID, types.Instant) bool { handed++; return true })
			if err != nil || handed != 2 || m.Work().Candidates != 4 {
				t.Fatalf("direct, Candidates 4: %d members, %v, work %+v; want 2 members and 4 counted", handed, err, m.Work())
			}
		})
	}
}

// TestMemberWindowWalkChargesStaleSteps (R2 cumulative, 3.3): every node of a
// value is replaced after a flush (its first row demoted), so the late axis
// holds, per node, the entries that stand and the older versions and tombstones
// of the key the demotion deleted, which the walk steps over. Each such step is charged as a candidate
// judged: the meter's Candidates equal the iterator steps that did not emit an
// entry, and a budget of exactly the members refuses.
func TestMemberWindowWalkChargesStaleSteps(t *testing.T) {
	const n = 8
	key := indexpkg.PropertyIndexKey{LabelToken: 1, PropertyKey: "k"}
	vk := types.IndexablePropertyValueKey("v")
	bs := cumulativeStore(t, n, true, func(int) types.Instant { return 100 })
	for i := 0; i < n; i++ {
		id := types.NodeID(i + 1)
		cur, prev := memberWorkNode(id), memberWorkNode(id)
		cur.SetVersion(1)
		cur.SetTemporal(&types.TemporalMetadata{TxFrom: 2, ValidFrom: 300, ValidTo: 400})
		prev.SetTemporal(&types.TemporalMetadata{TxFrom: 1, TxTo: 2, ValidFrom: 100, ValidTo: 101})
		if err := bs.ReplaceNodeWithHistory(cur, 0, prev); err != nil { // demotes the row: its late-axis entry is rewritten
			t.Fatal(err)
		}
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	query := func(tally *storecontract.ReadTally, work *storecontract.PropertyMemberWork) ([]indexpkg.TxMember[types.NodeID], error) {
		got, _, err := bs.nodeDiskMembers(key, memberQuery{valueKeys: []string{vk}, tally: tally, work: work, window: &memberWindow{0, math.MaxInt64, 0}})
		return got, err
	}
	var work storecontract.PropertyMemberWork
	m, tally := cumulativeTally(storecontract.ReadBudget{Candidates: 1 << 20})
	if got, err := query(tally, &work); err != nil || len(got) != n {
		t.Fatalf("unbudgeted: %d members, %v; want %d", len(got), err, n)
	}
	stale := work.IteratorSteps - work.DiskEntriesEmitted
	if stale == 0 || m.Work().Candidates != stale {
		t.Fatalf("Candidates %d, iterator steps %d, entries emitted %d (%+v); want the stale steps (%d, not 0) counted", m.Work().Candidates, work.IteratorSteps, work.DiskEntriesEmitted, work, stale)
	}
	_, tally = cumulativeTally(storecontract.ReadBudget{Candidates: n})
	var rb *storecontract.ReadBudgetError
	if got, err := query(tally, nil); !errors.As(err, &rb) || rb.Limit != "candidates" || got != nil {
		t.Fatalf("Candidates %d over %d stale steps: %d members, %v; want the candidates refusal", n, stale, len(got), err)
	}
}

// TestMemberWalkChargesBufferBytes (R2 cumulative, 3.3): the buffers a member
// walk holds are charged as BufferBytes before they grow: each pending value
// cloned for the window walk's overlay (EntityIndexSpanValueLen bytes) and the
// list the fold becomes (txMemberBytes per member, before it is allocated and
// sorted), exact and windowed. Counts are exact: n pending entries cost 32n for
// the clones and 16n for the list; flushed, only the list; a limit one byte
// short of either refuses at it.
func TestMemberWalkChargesBufferBytes(t *testing.T) {
	const n = 8
	key := indexpkg.PropertyIndexKey{LabelToken: 1, PropertyKey: "k"}
	vk := types.IndexablePropertyValueKey("v")
	list := uint64(n * txMemberBytes)
	clones := uint64(n * storepkg.EntityIndexSpanValueLen)
	for _, windowed := range []bool{false, true} {
		for _, flushed := range []bool{false, true} {
			name := map[bool]string{false: "exact", true: "window"}[windowed] + "/" + map[bool]string{false: "pending", true: "flushed"}[flushed]
			t.Run(name, func(t *testing.T) {
				bs := cumulativeStore(t, n, flushed, func(int) types.Instant { return 100 })
				query := func(tally *storecontract.ReadTally) ([]indexpkg.TxMember[types.NodeID], error) {
					q := memberQuery{valueKeys: []string{vk}, tally: tally}
					if windowed {
						q.window = &memberWindow{0, math.MaxInt64, 0}
					}
					got, _, err := bs.nodeDiskMembers(key, q)
					return got, err
				}
				want := list
				if windowed && !flushed {
					want += clones
				}
				m, tally := cumulativeTally(storecontract.ReadBudget{BufferBytes: 1 << 20})
				if got, err := query(tally); err != nil || len(got) != n || m.Work().BufferBytes != want {
					t.Fatalf("unbudgeted: %d members, %v, %d buffer bytes; want %d members and exactly %d bytes", len(got), err, m.Work().BufferBytes, n, want)
				}
				_, tally = cumulativeTally(storecontract.ReadBudget{BufferBytes: want})
				if got, err := query(tally); err != nil || len(got) != n {
					t.Fatalf("limit %d: %d members, %v; want the %d members", want, len(got), err, n)
				}
				m, tally = cumulativeTally(storecontract.ReadBudget{BufferBytes: want - 1})
				var rb *storecontract.ReadBudgetError
				if got, err := query(tally); !errors.As(err, &rb) || rb.Limit != "buffer bytes" || got != nil || m.Work().BufferBytes != want {
					t.Fatalf("limit %d: %d members, %v, work %+v; want the buffer bytes refusal at the %d-byte charge", want-1, len(got), err, m.Work(), want)
				}
				if windowed && !flushed {
					// One byte short of the clones: refused at the clone that passes, before the list.
					m, tally = cumulativeTally(storecontract.ReadBudget{BufferBytes: clones - 1})
					if got, err := query(tally); !errors.As(err, &rb) || rb.Limit != "buffer bytes" || got != nil || m.Work().BufferBytes != clones {
						t.Fatalf("limit %d: %d members, %v, work %+v; want the refusal at the last clone, %d bytes counted", clones-1, len(got), err, m.Work(), clones)
					}
				}
			})
		}
	}
}
