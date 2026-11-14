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

// TestMemberWindowWalkCountsRejectsAndCollectedOnce (R2 cumulative, 3.2): a
// window [100, 101) over a value of 4 members, 2 valid in it and 2 valid after
// it. The store is the one owner of the cost: it counts the 2 rejected entries
// as it judges them and the 2 members as it collects them, once each, so the
// read costs 4 candidates, and what it hands on is not counted again, through
// the internal walk and through the capability a host calls. The walk stops at
// the entry that passes the limit: the entries it visits (the native counters
// of the work diagnostic, which the meter does not keep) are the limit plus the
// one that refused, of the control's 4. Pending and flushed.
func TestMemberWindowWalkCountsRejectsAndCollectedOnce(t *testing.T) {
	key := indexpkg.PropertyIndexKey{LabelToken: 1, PropertyKey: "k"}
	vk := types.IndexablePropertyValueKey("v")
	for _, flushed := range []bool{false, true} {
		t.Run(map[bool]string{false: "pending", true: "flushed"}[flushed], func(t *testing.T) {
			bs := cumulativeStore(t, 4, flushed, func(i int) types.Instant { return []types.Instant{100, 500, 100, 600}[i] })
			query := func(tally *storecontract.ReadTally, work *storecontract.PropertyMemberWork) ([]indexpkg.TxMember[types.NodeID], error) {
				got, handled, err := bs.nodeDiskMembers(key, memberQuery{valueKeys: []string{vk}, tally: tally, work: work, window: &memberWindow{100, 101, 0}})
				if !handled {
					t.Fatal("the definition is not kept on disk")
				}
				return got, err
			}
			visits := func(w storecontract.PropertyMemberWork) uint64 { return w.DiskEntriesEmitted + w.OverlaySetsEmitted }
			var control storecontract.PropertyMemberWork
			want, err := query(nil, &control)
			if err != nil || len(want) != 2 || visits(control) != 4 {
				t.Fatalf("no budget: %d members, %v, %d entries visited; want 2 members after 4 entries", len(want), err, visits(control))
			}
			for _, limit := range []uint64{2, 3} {
				var work storecontract.PropertyMemberWork
				m, tally := cumulativeTally(storecontract.ReadBudget{Candidates: limit})
				var rb *storecontract.ReadBudgetError
				if got, err := query(tally, &work); !errors.As(err, &rb) || rb.Limit != "candidates" || got != nil || m.Work().Candidates != limit+1 {
					t.Fatalf("Candidates %d: %d members, %v, work %+v; want the candidates refusal at the entry past it", limit, len(got), err, m.Work())
				}
				if visits(work) != limit+1 {
					t.Fatalf("Candidates %d: %d entries visited (%+v); want %d, the walk stopping at the entry that refused", limit, visits(work), work, limit+1)
				}
			}
			m, tally := cumulativeTally(storecontract.ReadBudget{Candidates: 4})
			if got, err := query(tally, nil); err != nil || len(got) != 2 || m.Work().Candidates != 4 {
				t.Fatalf("Candidates 4: %d members, %v, work %+v; want the 2 members and 4 counted: 2 rejected entries, 2 members collected", len(got), err, m.Work())
			}
			// A host calling the capability: the same count, the hand-on counted by no one.
			window := &storecontract.PropertyTxMemberWindow{ValidStart: 100, ValidEnd: 101}
			m, tally = cumulativeTally(storecontract.ReadBudget{Candidates: 4})
			handed := 0
			err = bs.ForEachNodePropertyTxMemberMetered(1, "k", []string{vk}, window, tally, func(types.NodeID, types.Instant) bool { handed++; return true })
			if err != nil || handed != 2 || m.Work().Candidates != 4 {
				t.Fatalf("capability, Candidates 4: %d members, %v, work %+v; want 2 members and 4 counted", handed, err, m.Work())
			}
			m, tally = cumulativeTally(storecontract.ReadBudget{Candidates: 3})
			handed = 0
			err = bs.ForEachNodePropertyTxMemberMetered(1, "k", []string{vk}, window, tally, func(types.NodeID, types.Instant) bool { handed++; return true })
			var rb *storecontract.ReadBudgetError
			if !errors.As(err, &rb) || handed != 0 {
				t.Fatalf("capability, Candidates 3: %d members, %v, work %+v; want the refusal and no member handed on", handed, err, m.Work())
			}
		})
	}
}

// TestMemberWindowWalkChargesStaleSteps (R2 cumulative, 3.3): every node of a
// value is replaced after a flush (its first row demoted), so the late axis
// holds, per node, the entries that stand and the older versions and tombstones
// of the key the demotion deleted, which the walk steps over. Each such step is
// charged as a candidate, and each member once as it is collected: the meter's
// Candidates equal the native iterator steps that did not emit an entry (the
// work diagnostic, which the meter does not keep) plus the members, and a
// budget of exactly the members refuses.
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
	if stale == 0 || m.Work().Candidates != stale+n {
		t.Fatalf("Candidates %d, iterator steps %d, entries emitted %d (%+v); want %d stale steps (not 0) and %d members counted", m.Work().Candidates, work.IteratorSteps, work.DiskEntriesEmitted, work, stale, n)
	}
	var cut storecontract.PropertyMemberWork
	_, tally = cumulativeTally(storecontract.ReadBudget{Candidates: n})
	var rb *storecontract.ReadBudgetError
	if got, err := query(tally, &cut); !errors.As(err, &rb) || rb.Limit != "candidates" || got != nil {
		t.Fatalf("Candidates %d over %d stale steps: %d members, %v; want the candidates refusal", n, stale, len(got), err)
	}
	if cut.IteratorSteps >= work.IteratorSteps {
		t.Fatalf("refused read took %d iterator steps, the control %d: the walk did not stop at the refusal", cut.IteratorSteps, work.IteratorSteps)
	}
}

// TestMemberWalkChargesBufferBytes (R2 cumulative, 3.3): the buffers a member
// walk holds are charged as BufferBytes before they grow: each pending value
// cloned for the window walk's overlay (EntityIndexSpanValueLen bytes) and the
// list the fold becomes (txMemberBytes per member, before it is allocated and
// sorted), exact and windowed. Counts are exact: n pending entries cost 32n for
// the clones and 16n for the list; flushed, only the list; a limit one byte
// short of either refuses at it. The native counters of the work diagnostic,
// which the meter does not keep, show the buffer was not made: the clones made
// stop one short of the refused one, and a list refused is never allocated
// (MemberSliceLen 0, the control's n; the exact walk: the window walk reports
// its fold there itself).
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
				query := func(tally *storecontract.ReadTally, work *storecontract.PropertyMemberWork) ([]indexpkg.TxMember[types.NodeID], error) {
					q := memberQuery{valueKeys: []string{vk}, tally: tally, work: work}
					if windowed {
						q.window = &memberWindow{0, math.MaxInt64, 0}
					}
					got, _, err := bs.nodeDiskMembers(key, q)
					return got, err
				}
				want, wantClones := list, uint64(0)
				if windowed && !flushed {
					want, wantClones = want+clones, n
				}
				var control storecontract.PropertyMemberWork
				m, tally := cumulativeTally(storecontract.ReadBudget{BufferBytes: 1 << 20})
				if got, err := query(tally, &control); err != nil || len(got) != n || m.Work().BufferBytes != want || control.OverlayValuesCloned != wantClones || control.MemberSliceLen != n {
					t.Fatalf("unbudgeted: %d members, %v, %d buffer bytes, %+v; want %d members, exactly %d bytes, %d clones, a list of %d", len(got), err, m.Work().BufferBytes, control, n, want, wantClones, n)
				}
				_, tally = cumulativeTally(storecontract.ReadBudget{BufferBytes: want})
				if got, err := query(tally, nil); err != nil || len(got) != n {
					t.Fatalf("limit %d: %d members, %v; want the %d members", want, len(got), err, n)
				}
				var work storecontract.PropertyMemberWork
				m, tally = cumulativeTally(storecontract.ReadBudget{BufferBytes: want - 1})
				var rb *storecontract.ReadBudgetError
				if got, err := query(tally, &work); !errors.As(err, &rb) || rb.Limit != "buffer bytes" || got != nil || m.Work().BufferBytes != want || (!windowed && work.MemberSliceLen != 0) { // the window walk reports its fold in MemberSliceLen itself
					t.Fatalf("limit %d: %d members, %v, work %+v, %+v; want the buffer bytes refusal at the %d-byte charge, the list never allocated", want-1, len(got), err, m.Work(), work, want)
				}
				if windowed && !flushed {
					// One byte short of the clones: refused at the clone that passes, before the list.
					work = storecontract.PropertyMemberWork{}
					m, tally = cumulativeTally(storecontract.ReadBudget{BufferBytes: clones - 1})
					if got, err := query(tally, &work); !errors.As(err, &rb) || rb.Limit != "buffer bytes" || got != nil || m.Work().BufferBytes != clones || work.OverlayValuesCloned != n-1 {
						t.Fatalf("limit %d: %d members, %v, work %+v, %+v; want the refusal at the last clone: %d bytes counted, %d values cloned", clones-1, len(got), err, m.Work(), work, clones, n-1)
					}
				}
			})
		}
	}
}
