package badger

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// replacedNodeStore is cumulativeStore with every node replaced `replacements`
// times (each replacement demotes the row before it, so the member entries of a
// node pile up: one live entry per version, the older versions of the keys the
// demotions rewrote, and the time-ordered entries they moved), flushed or left
// in the write buffer.
func replacedNodeStore(t *testing.T, n, replacements int, flushed bool) *Store {
	t.Helper()
	bs := cumulativeStore(t, n, false, func(int) types.Instant { return 100 })
	row := func(id types.NodeID, v int, txTo types.Instant) *types.Node {
		nd := memberWorkNode(id)
		nd.SetVersion(uint32(v)) // #nosec G115 -- a small test count
		if v == 0 {
			nd.SetTemporal(&types.TemporalMetadata{TxFrom: 1, TxTo: txTo, ValidFrom: 100, ValidTo: 101})
		} else {
			from := types.Instant(100 + 10*v)
			nd.SetTemporal(&types.TemporalMetadata{TxFrom: types.Instant(v + 1), TxTo: txTo, ValidFrom: from, ValidTo: from + 5})
		}
		return nd
	}
	for i := 0; i < n; i++ {
		id := types.NodeID(i + 1)
		for r := 1; r <= replacements; r++ {
			if err := bs.ReplaceNodeWithHistory(row(id, r, 0), uint32(r-1), row(id, r-1, types.Instant(r+1))); err != nil { // #nosec G115 -- a small test count
				t.Fatal(err)
			}
			if flushed && r == 1 {
				if err := bs.Flush(); err != nil { // the first versions on disk, the later ones rewrite them
					t.Fatal(err)
				}
			}
		}
	}
	if flushed {
		if err := bs.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	return bs
}

// TestMemberWalkChargesEveryEntryExamined (R2 cumulative, Fable review 2 and 3,
// 5): a disk walk of a value charges every entry it examines once, before it
// reads or parses the entry: the live ones (a repeat of an entity already
// collected too: a node replaced 5 times has 6 live entries), the older
// versions of a key and the tombstones, exact and windowed; the write buffer's
// ops are examined entries too. 4 nodes replaced 5 times: the meter's
// Candidates equal the native count of the walk (the iterator steps flushed,
// the buffered ops inspected pending), not the 4 members. The exact, non-empty
// unlimited control answers the 4 members. A limit of 10 refuses at the 11th
// entry: that entry is charged and then not read (flushed: 11 iterator steps,
// at most 10 entries passed on; pending: 10 ops taken into the overlay).
func TestMemberWalkChargesEveryEntryExamined(t *testing.T) {
	testtier.Slow(t)
	const n, replacements, limit = 4, 5, 10
	key := indexpkg.PropertyIndexKey{LabelToken: 1, PropertyKey: "k"}
	vk := types.IndexablePropertyValueKey("v")
	for _, windowed := range []bool{false, true} {
		for _, flushed := range []bool{false, true} {
			name := map[bool]string{false: "exact", true: "window"}[windowed] + "/" + map[bool]string{false: "pending", true: "flushed"}[flushed]
			t.Run(name, func(t *testing.T) {
				bs := replacedNodeStore(t, n, replacements, flushed)
				query := func(tally *storecontract.ReadTally, work *storecontract.PropertyMemberWork) ([]indexpkg.TxMember[types.NodeID], error) {
					q := memberQuery{valueKeys: []string{vk}, tally: tally, work: work}
					if windowed {
						q.window = &memberWindow{0, math.MaxInt64, 0}
					}
					got, _, err := bs.nodeDiskMembers(key, q)
					return got, err
				}
				native := func(w storecontract.PropertyMemberWork) uint64 { // the entries the walk looked at
					if flushed {
						return w.IteratorSteps
					}
					return w.OverlayOpsInspected
				}
				var control storecontract.PropertyMemberWork
				m, tally := cumulativeTally(storecontract.ReadBudget{Candidates: 1 << 20})
				got, err := query(tally, &control)
				if err != nil || len(got) != n {
					t.Fatalf("unlimited: %d members, %v; want %d", len(got), err, n)
				}
				live := uint64(n * (replacements + 1))
				if native(control) < live || (flushed && native(control) == live) || m.Work().Candidates != native(control) {
					t.Fatalf("unlimited: %d candidates, %d entries looked at (%+v); want every entry charged once: the %d live ones, and flushed the stale steps too", m.Work().Candidates, native(control), control, live)
				}

				var work storecontract.PropertyMemberWork
				m, tally = cumulativeTally(storecontract.ReadBudget{Candidates: limit})
				var rb *storecontract.ReadBudgetError
				if got, err := query(tally, &work); !errors.As(err, &rb) || rb.Limit != "candidates" || got != nil || m.Work().Candidates != limit+1 {
					t.Fatalf("Candidates %d: %d members, %v, work %+v; want the refusal at the %dth entry charged", limit, len(got), err, m.Work(), limit+1)
				}
				if flushed && (work.IteratorSteps != limit+1 || work.DiskEntriesEmitted > limit) {
					t.Fatalf("Candidates %d: %d iterator steps, %d entries passed on (%+v); want %d steps and none passed on past the limit", limit, work.IteratorSteps, work.DiskEntriesEmitted, work, limit+1)
				}
				if !flushed && work.OverlayOpsMatched != limit {
					t.Fatalf("Candidates %d: %d ops taken into the overlay (%+v); want %d, the one past the limit charged and left", limit, work.OverlayOpsMatched, work, limit)
				}

				_, tally = cumulativeTally(storecontract.ReadBudget{Candidates: native(control)})
				if got, err := query(tally, nil); err != nil || len(got) != n {
					t.Fatalf("Candidates %d (the unlimited count): %d members, %v; want the %d members", native(control), len(got), err, n)
				}
			})
		}
	}
}

// TestWindowFirstLookupDoesNotRaceWithWrites (Fable review 1; run with -race):
// the first windowed lookup of a RAM sidecar value opens its window view, and
// the lookup sized the value after it had released the index lock, reading the
// sidecar's value map while a write to the same (label, key) updated it. Every
// round writes a new value and looks it up, the first windowed lookup of that
// value, while another goroutine writes values of the same definition.
func TestWindowFirstLookupDoesNotRaceWithWrites(t *testing.T) {
	const rounds = 300
	bs, err := New(Config{InMemory: true, PropertyKeyRegistry: registrypkg.NewPropertyKeyRegistry(), MaxPendingWrites: -1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	if err = bs.CreatePropertyIndex(1, "k"); err != nil {
		t.Fatal(err)
	}
	put := func(id int, value string) {
		n := types.NewNode(types.NodeID(id), 1, []uint16{1})
		_ = n.SetProperty("k", value)
		n.SetTemporal(&types.TemporalMetadata{TxFrom: 1, ValidFrom: 100, ValidTo: 200})
		if err := bs.PutNode(n); err != nil {
			t.Error(err)
		}
	}
	put(1, "seed")
	if err = bs.ForEachNodePropertyTxMember(1, "k", types.IndexablePropertyValueKey("seed"), func(types.NodeID, types.Instant) bool { return true }); err != nil { // builds the sidecar
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			put(10_000+i, fmt.Sprintf("w%d", i))
		}
	}()
	for i := 0; i < rounds; i++ {
		put(100+i, fmt.Sprintf("y%d", i))
		members := 0
		if err := bs.ForEachNodePropertyTxMemberDuring(1, "k", types.IndexablePropertyValueKey(fmt.Sprintf("y%d", i)), 0, 1000, 0, func(types.NodeID, types.Instant) bool { members++; return true }); err != nil || members != 1 {
			t.Fatalf("round %d: %d members, %v; want 1", i, members, err)
		}
	}
	wg.Wait()
}
