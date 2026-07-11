package store_test

import (
	"math/rand/v2"
	"sort"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// propertyStatsStore is any backend exposing the optional
// store.NodePropertyStatsCapability alongside the mandatory node-mutation
// surface — the narrow interface this parity harness needs from either
// backend.
type propertyStatsStore interface {
	storecontract.NodeCRUDCapability
	storecontract.NodePropertyStatsCapability
}

// TestNodePropertyStatsParity_BadgerVsMemory drives an IDENTICAL randomized
// sequence of node creates/replaces/deletes through memory.Store,
// badger.Store, AND tiered.Store (ADR-0005 §3.1) and asserts NodePropertyStats
// agrees EXACTLY across all three at every checkpoint — Count and Min/Max are
// exact by construction, and since every backend shares the SAME
// index.PropertyStatsAccumulator/HyperLogLog implementation fed the
// identical sequence of ever-added values (tiered's cross-shard fold
// register-max-merges each shard's sketch before calling Estimate() ONCE, so
// a single-shard population — as this test's tiny synthetic IDs all decode
// to — degenerates to the same computation the other two backends do
// directly), NDV must also match bit-for-bit (HyperLogLog's per-register max
// is a deterministic function of the set of hashed inputs, independent of
// insertion order). The test name is kept for history; it now covers all
// three backends.
func TestNodePropertyStatsParity_BadgerVsMemory(t *testing.T) {
	t.Parallel()

	const labelToken = uint16(1)
	const propertyKey = "v"
	const nNodes = 400
	const nCheckpoints = 8

	rng := rand.New(rand.NewPCG(42, 4242))

	type op struct {
		kind  string // "put", "replace", "delete"
		id    int64
		value int64
	}
	var ops []op
	live := make(map[int64]bool)
	nextID := int64(1)
	for len(ops) < nNodes {
		switch {
		case len(live) == 0 || rng.IntN(3) == 0:
			id := nextID
			nextID++
			ops = append(ops, op{kind: "put", id: id, value: rng.Int64N(1000)})
			live[id] = true
		case rng.IntN(2) == 0:
			// replace a random live node
			ids := liveIDs(live)
			id := ids[rng.IntN(len(ids))]
			ops = append(ops, op{kind: "replace", id: id, value: rng.Int64N(1000)})
		default:
			// delete a random live node
			ids := liveIDs(live)
			id := ids[rng.IntN(len(ids))]
			ops = append(ops, op{kind: "delete", id: id})
			delete(live, id)
		}
	}

	dir := t.TempDir()
	bs, err := badger.New(badger.Config{Dir: dir})
	if err != nil {
		t.Fatalf("badger.New: %v", err)
	}
	defer bs.Close()
	ms := memory.New()
	defer ms.Close()
	// ADR-0005 §3.1 tiered arm: tiered.Store now implements
	// NodePropertyStatsCapability (a cross-shard NDV/min/max/count fold), so
	// it must agree with memory/badger on this backend-agnostic randomized
	// sequence exactly like the two single-shard backends agree with each
	// other. FlushInterval disables the periodic async flush (deterministic,
	// no background-goroutine interference with the checkpointed assertions).
	ts, err := tiered.New(tiered.Config{InMemory: true, ShardWindow: 7 * 24 * time.Hour, FlushInterval: 1<<63 - 1})
	if err != nil {
		t.Fatalf("tiered.New: %v", err)
	}
	defer ts.Close()

	stores := []propertyStatsStore{bs, ms, ts}
	names := []string{"badger", "memory", "tiered"}

	checkpointEvery := len(ops) / nCheckpoints
	if checkpointEvery == 0 {
		checkpointEvery = 1
	}

	applyOp := func(s propertyStatsStore, o op) {
		t.Helper()
		switch o.kind {
		case "put":
			n := types.NewNode(types.NodeID(snowflake.ID(o.id)), labelToken, nil)
			if err := n.SetProperty(propertyKey, o.value); err != nil {
				t.Fatalf("SetProperty: %v", err)
			}
			if err := s.PutNode(n); err != nil {
				t.Fatalf("PutNode(%d): %v", o.id, err)
			}
		case "replace":
			n := types.NewNode(types.NodeID(snowflake.ID(o.id)), labelToken, nil)
			if err := n.SetProperty(propertyKey, o.value); err != nil {
				t.Fatalf("SetProperty: %v", err)
			}
			if err := s.ReplaceNode(n); err != nil {
				t.Fatalf("ReplaceNode(%d): %v", o.id, err)
			}
		case "delete":
			if err := s.DeleteNode(types.NodeID(snowflake.ID(o.id))); err != nil {
				t.Fatalf("DeleteNode(%d): %v", o.id, err)
			}
		}
	}

	for i, o := range ops {
		for _, s := range stores {
			applyOp(s, o)
		}
		if (i+1)%checkpointEvery != 0 && i != len(ops)-1 {
			continue
		}
		var results []storecontract.PropertyStats
		for _, s := range stores {
			stats, err := s.NodePropertyStats(labelToken, propertyKey)
			if err != nil {
				t.Fatalf("NodePropertyStats: %v", err)
			}
			results = append(results, stats)
		}
		for j := 1; j < len(results); j++ {
			if results[j] != results[0] {
				t.Fatalf("checkpoint after op %d: %s stats = %+v, %s stats = %+v — backends diverged",
					i, names[0], results[0], names[j], results[j])
			}
		}
	}
}

func liveIDs(live map[int64]bool) []int64 {
	ids := make([]int64, 0, len(live))
	for id := range live {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}
