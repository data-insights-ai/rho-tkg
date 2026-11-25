package tiered

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestTieredMemberCollectionCumulativeAcrossShards (R2, ai-soc review 14 Nov
// 3.2, the rule: one allowance for the whole read, across shards): the
// members of one value spread over three event shards, perShard each, so no
// shard's share passes the limit but the shards together do. Each shard counts
// the members it collects (disk: as its walk folds them; RAM: as its sidecar
// snapshots them) on the one meter, so the read refuses with the typed error
// before any member is handed on, whichever shard passes the limit, and the
// shards stop at their next charge. The control is the exact, non-empty
// unlimited read of all the members; a limit that covers them answers it and
// counts each member once. On disk the native counter, the iterator steps of
// every index walk in the process, which the meter does not keep, shows the
// refused read visited fewer entries than the control, at most the limit and
// one past it per shard. Node (RAM and disk) and relationship twins, the exact
// walk and a window over everything.
func TestTieredMemberCollectionCumulativeAcrossShards(t *testing.T) {
	testtier.Slow(t)
	const perShard, shards = 20, 3
	const total = perShard * shards
	value := types.IndexablePropertyValueKey(int64(1))
	for _, variant := range []struct {
		kind string
		disk bool
	}{{"node", false}, {"node", true}, {"rel", false}} {
		for _, windowed := range []bool{false, true} {
			name := variant.kind + map[bool]string{false: "/ram", true: "/disk"}[variant.disk] + map[bool]string{false: "/exact", true: "/window"}[windowed]
			t.Run(name, func(t *testing.T) {
				ts := newCumulativeTieredStore(t, variant.disk)
				installDefaultTestLabelRegistry(t, ts) // Case=1, User=2, Signal=3
				var window *storecontract.PropertyTxMemberWindow
				if windowed {
					window = &storecontract.PropertyTxMemberWindow{ValidStart: 0, ValidEnd: types.Instant(math.MaxInt64)}
				}
				read := spreadMembers(t, ts, variant.kind, perShard, shards)
				visited, stop := badger.CountIndexEntriesVisitedForTest()
				defer stop()
				budget := func(limit uint64) (*storecontract.ReadMeter, *storecontract.ReadTally) {
					_, m := storecontract.WithReadBudget(context.Background(), storecontract.ReadBudget{Candidates: limit})
					return m, m.Tally("d")
				}

				before := visited()
				var members int // the unlimited, non-empty control
				if err := read(value, window, nil, func() { members++ }); err != nil || members != total {
					t.Fatalf("unlimited: %d members, %v; want %d", members, err, total)
				}
				control := visited() - before
				if variant.disk && control < total {
					t.Fatalf("unlimited visited %d index entries, want at least the %d members", control, total)
				}

				for _, limit := range []uint64{perShard + 5, total - 1} { // above one shard's share, below the total
					m, tally := budget(limit)
					members = 0
					before = visited()
					err := read(value, window, tally, func() { members++ })
					var rb *storecontract.ReadBudgetError
					if !errors.As(err, &rb) || rb.Limit != "candidates" || members != 0 {
						t.Fatalf("Candidates %d: %d members, %v, work %+v; want the candidates refusal and no member handed on", limit, members, err, m.Work())
					}
					if got := m.Work().Candidates; got <= limit || got > limit+shards {
						t.Fatalf("Candidates %d: %d counted at the refusal; want past the limit by at most one per shard", limit, got)
					}
					steps := visited() - before
					t.Logf("Candidates %d: refused at %d counted, %d index entries visited (the control %d)", limit, m.Work().Candidates, steps, control)
					if variant.disk && ((limit < total-1 && steps >= control) || steps > int64(limit)+shards) {
						t.Fatalf("Candidates %d: %d index entries visited, the control %d; want the walks stopped at the refusal (fewer than the control unless the last entry refused, at most %d)", limit, steps, control, limit+shards)
					}
				}

				m, tally := budget(total)
				members = 0
				if err := read(value, window, tally, func() { members++ }); err != nil || members != total || m.Work().Candidates != total {
					t.Fatalf("Candidates %d: %d members, %v, work %+v; want all %d handed on, each counted once", total, members, err, m.Work(), total)
				}
			})
		}
	}
}

// newCumulativeTieredStore is newTestTieredStore with the node property index
// kept on disk (disk) or in RAM.
func newCumulativeTieredStore(t *testing.T, disk bool) *Store {
	t.Helper()
	ts, err := New(Config{
		InMemory:            true,
		RefLabels:           []string{"Case", "User"},
		ShardWindow:         7 * 24 * time.Hour,
		FlushInterval:       1<<63 - 1, // disable periodic flush
		PropertyIndexOnDisk: disk,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close() })
	ts.SetPropertyKeyRegistry(registrypkg.NewPropertyKeyRegistry()) // the on-disk index needs one
	return ts
}

// spreadMembers puts perShard members of the value 1 on each of shards event
// shards (the hot shard rotated between the batches) and returns a read of
// them through the store's metered membership door: tally nil is the unmetered
// form, fn runs once per member handed on.
func spreadMembers(t *testing.T, ts *Store, kind string, perShard, shards int) func(valueKey string, window *storecontract.PropertyTxMemberWindow, tally *storecontract.ReadTally, fn func()) error {
	t.Helper()
	const signal, relType = uint16(3), uint16(5)
	gen := tieredNodeGen(t)
	if kind == "node" {
		if err := ts.CreatePropertyIndex(signal, "sev"); err != nil {
			t.Fatal(err)
		}
	} else if err := ts.CreateRelPropertyIndex(relType, "w"); err != nil {
		t.Fatal(err)
	}
	var to *types.Node
	for s := 0; s < shards; s++ {
		if s > 0 {
			forceRotation(t, ts)
		}
		var from *types.Node // a relationship sits on the shard of its source node
		if kind == "rel" {
			from = types.NewNode(types.NodeID(gen.Generate()), signal, nil)
			if err := ts.PutNode(from); err != nil {
				t.Fatal(err)
			}
			if to == nil {
				to = from
			}
		}
		for i := 0; i < perShard; i++ {
			if kind == "node" {
				n := types.NewNode(types.NodeID(gen.Generate()), signal, nil)
				_ = n.SetProperty("sev", int64(1))
				if err := ts.PutNode(n); err != nil {
					t.Fatal(err)
				}
				continue
			}
			r := types.NewRelationship(types.RelID(tieredRelGen(t).Generate()), relType, from.ID(), to.ID())
			_ = r.SetProperty("w", int64(1))
			if err := ts.PutRelationship(r); err != nil {
				t.Fatal(err)
			}
		}
	}
	time.Sleep(2 * time.Millisecond)
	if err := ts.Flush(); err != nil { // the hot shard's members on disk too, so every walk steps an iterator
		t.Fatal(err)
	}
	// The fixture: each shard holds exactly its share, so no shard's own room can see the total.
	holding := 0
	for name, bs := range allShardStoresForTest(t, ts) {
		got := 0
		vk := types.IndexablePropertyValueKey(int64(1))
		var err error
		if kind == "node" {
			err = bs.ForEachNodePropertyTxMember(signal, "sev", vk, func(types.NodeID, types.Instant) bool { got++; return true })
		} else {
			err = bs.ForEachRelPropertyTxMember(relType, "w", vk, func(types.RelID, types.Instant) bool { got++; return true })
		}
		if err != nil || (got != 0 && got != perShard) {
			t.Fatalf("fixture: shard %s holds %d members, %v; want 0 or %d", name, got, err, perShard)
		}
		if got != 0 {
			holding++
		}
	}
	if holding != shards {
		t.Fatalf("fixture: %d shards hold members, want %d", holding, shards)
	}
	return func(valueKey string, window *storecontract.PropertyTxMemberWindow, tally *storecontract.ReadTally, fn func()) error {
		if kind == "node" {
			return ts.ForEachNodePropertyTxMemberMetered(signal, "sev", []string{valueKey}, window, tally, func(types.NodeID, types.Instant) bool { fn(); return true })
		}
		return ts.ForEachRelPropertyTxMemberMetered(relType, "w", []string{valueKey}, window, tally, func(types.RelID, types.Instant) bool { fn(); return true })
	}
}
