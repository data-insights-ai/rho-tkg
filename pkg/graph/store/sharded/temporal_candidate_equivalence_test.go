package sharded

import (
	"sort"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// nodePutter is satisfied by both *badger.Store and *sharded.Store, letting
// putWindowNode build the identical scenario on either backend.
type nodePutter interface {
	PutNode(n *types.Node) error
}

func putWindowNode(t *testing.T, st nodePutter, id types.NodeID, label uint16, from, to types.Instant) {
	t.Helper()
	n := types.NewNode(id, label, nil)
	n.SetTemporal(&types.TemporalMetadata{ValidFrom: from, ValidTo: to})
	if err := st.PutNode(n); err != nil {
		t.Fatalf("PutNode(%d): %v", id, err)
	}
}

func idNames(ids []types.NodeID, names map[types.NodeID]string) []string {
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		out = append(out, names[id])
	}
	sort.Strings(out)
	return out
}

// TestPruneTemporalCandidatesCrossBackendEquivalence closes BACKLOG 20j: sharded's
// PruneTemporalCandidates (temporal_index.go) partitions candidates by OWNING
// shard and folds each shard's local badger.PruneTemporalCandidates result back
// together — a routing layer with no prune logic of its own. The existing
// sharded-only test (TestShardedPruneTemporalCandidatesRoutesAcrossShards) proves
// the routing fires across shards but never checks the RESULT against an
// unsharded oracle, and the existing core-level equivalence test
// (TestTemporalCandidatePruneEquivalence, internal/core) exercises sharded only
// through the default single-slot node generator — every node lands on ONE
// shard, so the byShard partition-and-fold path (temporal_index.go:70-96) is
// never exercised for EQUIVALENCE, only for isolated correctness.
//
// This test builds the identical multi-node scenario — deliberately spanning
// FOUR distinct shards, with one shard holding two nodes — on both a plain
// single-instance badger.Store (the oracle: no sharding, no routing) and a
// four-shard sharded.Store, using the SAME snowflake IDs on both (badger does
// not interpret the slot bits, so this is a legitimate direct ID reuse). Every
// probe's kept-ID set must be IDENTICAL between the two backends, and is also
// pinned to a hand-reasoned expected set (rule 16) so a bug that corrupts both
// backends identically cannot hide behind an equivalence-only check.
func TestPruneTemporalCandidatesCrossBackendEquivalence(t *testing.T) {
	t.Parallel()
	const label = uint16(9)

	// (name, slot, n, from, to) — deliberately spread across shards 0..3, with
	// shard 2 holding two nodes (D and E) to exercise a multi-node bucket.
	type spec struct {
		name     string
		slot     uint8
		n        int64
		from, to types.Instant
	}
	specs := []spec{
		{"A", 0, 1, 1000, 0},    // open [1000,∞)
		{"B", 0, 2, 1000, 2000}, // bounded [1000,2000)
		{"C", 1, 1, 2000, 5000}, // bounded [2000,5000)
		{"D", 2, 1, 9000, 0},    // future-open [9000,∞) — phantom at early probes
		{"E", 2, 2, 100, 500},   // bounded [100,500)
		{"F", 3, 1, 500, 0},     // open [500,∞)
	}

	badgerStore, err := badger.New(badger.Config{InMemory: true})
	if err != nil {
		t.Fatalf("badger.New: %v", err)
	}
	t.Cleanup(func() { _ = badgerStore.Close() })

	shardedStore := newMemStore(t, 0, 4)

	names := make(map[types.NodeID]string, len(specs))
	allIDs := make([]types.NodeID, 0, len(specs))
	for _, s := range specs {
		id := mkNodeID(s.slot, s.n)
		names[id] = s.name
		allIDs = append(allIDs, id)
		putWindowNode(t, badgerStore, id, label, s.from, s.to)
		putWindowNode(t, shardedStore, id, label, s.from, s.to)
	}

	if err := badgerStore.CreateTemporalIndex(label); err != nil {
		t.Fatalf("badger CreateTemporalIndex: %v", err)
	}
	if err := shardedStore.CreateTemporalIndex(label); err != nil {
		t.Fatalf("sharded CreateTemporalIndex: %v", err)
	}

	probes := []struct {
		name string
		opts storecontract.QueryOpts
		want []string
	}{
		{"point@300", storecontract.QueryOpts{ValidAt: 300}, []string{"E"}},
		{"point@1500", storecontract.QueryOpts{ValidAt: 1500}, []string{"A", "B", "F"}},
		{"point@3000", storecontract.QueryOpts{ValidAt: 3000}, []string{"A", "C", "F"}},
		{"point@6000", storecontract.QueryOpts{ValidAt: 6000}, []string{"A", "F"}},
		{"point@9500", storecontract.QueryOpts{ValidAt: 9500}, []string{"A", "D", "F"}},
		{"interval@1200-1800", storecontract.QueryOpts{ValidStart: 1200, ValidEnd: 1800}, []string{"A", "B", "F"}},
	}

	for _, p := range probes {
		p := p
		t.Run(p.name, func(t *testing.T) {
			badgerKept, badgerOK := badgerStore.PruneTemporalCandidates(label, allIDs, p.opts)
			shardedKept, shardedOK := shardedStore.PruneTemporalCandidates(label, allIDs, p.opts)

			if !badgerOK || !shardedOK {
				t.Fatalf("prune did not fire: badgerOK=%v shardedOK=%v (both must be true with a live index)", badgerOK, shardedOK)
			}

			badgerNames := idNames(badgerKept, names)
			shardedNames := idNames(shardedKept, names)
			wantSorted := append([]string(nil), p.want...)
			sort.Strings(wantSorted)

			if !equalStrs(badgerNames, wantSorted) {
				t.Errorf("badger kept = %v, want %v", badgerNames, wantSorted)
			}
			if !equalStrs(shardedNames, wantSorted) {
				t.Errorf("sharded kept = %v, want %v", shardedNames, wantSorted)
			}
			if !equalStrs(badgerNames, shardedNames) {
				t.Errorf("CROSS-BACKEND DIVERGENCE: badger kept %v, sharded kept %v", badgerNames, shardedNames)
			}
		})
	}
}

func equalStrs(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
