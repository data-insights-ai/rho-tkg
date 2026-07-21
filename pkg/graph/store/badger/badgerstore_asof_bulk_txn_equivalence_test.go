package badger

import (
	"math/rand/v2"
	"sort"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BACKLOG 18k: NodesAsOf/RelsAsOf were refactored from one badger transaction
// PER ENTITY to ONE transaction for the whole scan. These tests are the primary
// safety net: for every pin across a randomized population (mirroring
// genAsofChain's generator from badgerstore_asof_equivalence_test.go, extended
// with long chains to exercise HistoryDeltaEncoding's anchor+delta path), the
// NEW single-txn NodesAsOf/RelsAsOf must return the exact same multiset as (a)
// calling the OLD per-entity primitive (NodeAsOf/RelAsOf, themselves UNCHANGED
// by this refactor) in a loop, and (b) storeutil.SelectAsOf computed directly
// against the materialized chain — the canonical oracle. Agreement with (a)
// proves the transaction-batching refactor didn't change behavior; agreement
// with (b) ties the whole thing back to ground truth so a bug shared by both
// old and new paths can't hide behind an old-vs-new-only comparison.

// buildNodeChainAndPopulate builds a genAsofChain-shaped chain (reusing the
// generator/builders from badgerstore_asof_equivalence_test.go) for nid, writes
// it to bs, and returns the full materialized chain for the SelectAsOf oracle.
func buildNodeChainAndPopulate(t *testing.T, bs *Store, nid types.NodeID, rng *rand.Rand) []*types.Node {
	t.Helper()
	chain := genAsofChain(rng)
	full := make([]*types.Node, 0, len(chain))
	for _, v := range chain {
		node := buildNode(nid, v)
		full = append(full, node)
		if v.current {
			if err := bs.PutNode(node); err != nil {
				t.Fatalf("PutNode(%d) v%d: %v", nid, v.version, err)
			}
		} else {
			if err := bs.PutNodeVersion(nid, v.version, node); err != nil {
				t.Fatalf("PutNodeVersion(%d) v%d: %v", nid, v.version, err)
			}
		}
	}
	return full
}

// buildLongNodeChain builds a chain of `versions` strictly-increasing-TxFrom
// versions (a simpler, monotonic shape than genAsofChain, but long enough to
// cross HistoryAnchorInterval=16 and force a real anchor+delta boundary under
// HistoryDeltaEncoding) and returns the materialized chain for the oracle.
func buildLongNodeChain(t *testing.T, bs *Store, nid types.NodeID, versions int, open bool) []*types.Node {
	t.Helper()
	full := make([]*types.Node, 0, versions)
	for i := 0; i < versions; i++ {
		txFrom := types.Instant(10 * (i + 1))
		var txTo types.Instant
		isCurrent := open && i == versions-1
		if i < versions-1 {
			txTo = types.Instant(10 * (i + 2))
		} else if !open {
			txTo = types.Instant(10 * (i + 2))
		}
		v := asofVersion{version: uint32(i), txFrom: txFrom, txTo: txTo, current: isCurrent}
		node := buildNode(nid, v)
		full = append(full, node)
		if isCurrent {
			if err := bs.PutNode(node); err != nil {
				t.Fatalf("PutNode(%d) v%d: %v", nid, i, err)
			}
		} else {
			if err := bs.PutNodeVersion(nid, v.version, node); err != nil {
				t.Fatalf("PutNodeVersion(%d) v%d: %v", nid, i, err)
			}
		}
	}
	return full
}

func buildRelChainAndPopulate(t *testing.T, bs *Store, rid types.RelID, rng *rand.Rand) []*types.Relationship {
	t.Helper()
	chain := genAsofChain(rng)
	full := make([]*types.Relationship, 0, len(chain))
	for _, v := range chain {
		rel := buildRel(rid, v)
		full = append(full, rel)
		if v.current {
			if err := bs.PutRelationship(rel); err != nil {
				t.Fatalf("PutRelationship(%d) v%d: %v", rid, v.version, err)
			}
		} else {
			if err := bs.PutRelVersion(rid, v.version, rel); err != nil {
				t.Fatalf("PutRelVersion(%d) v%d: %v", rid, v.version, err)
			}
		}
	}
	return full
}

// oldStyleNodesAsOf reproduces the PRE-BACKLOG-18k algorithm: one call to the
// (unchanged) per-entity NodeAsOf per candidate ID, each opening its own
// transaction. Used as the "old path" reference for the equivalence check.
func oldStyleNodesAsOf(t *testing.T, bs *Store, ids []types.NodeID, txTime types.Instant) []*types.Node {
	t.Helper()
	var result []*types.Node
	for _, id := range ids {
		n, err := bs.NodeAsOf(id, txTime)
		if err == ErrVersionNotFound {
			continue
		}
		if err != nil {
			t.Fatalf("NodeAsOf(%d, %d): %v", id, txTime, err)
		}
		result = append(result, n)
	}
	storeutil.SortNodesByID(result)
	return result
}

func oldStyleRelsAsOf(t *testing.T, bs *Store, ids []types.RelID, txTime types.Instant) []*types.Relationship {
	t.Helper()
	var result []*types.Relationship
	for _, id := range ids {
		r, err := bs.RelAsOf(id, txTime)
		if err == ErrVersionNotFound {
			continue
		}
		if err != nil {
			t.Fatalf("RelAsOf(%d, %d): %v", id, txTime, err)
		}
		result = append(result, r)
	}
	storeutil.SortRelsByID(result)
	return result
}

func nodeVersionSet(nodes []*types.Node) map[snowflake.ID]uint32 {
	m := make(map[snowflake.ID]uint32, len(nodes))
	for _, n := range nodes {
		m[n.ID().SnowflakeID()] = n.Version()
	}
	return m
}

func relVersionSet(rels []*types.Relationship) map[snowflake.ID]uint32 {
	m := make(map[snowflake.ID]uint32, len(rels))
	for _, r := range rels {
		m[r.ID().SnowflakeID()] = r.Version()
	}
	return m
}

func oracleNodeVersionSet(t *testing.T, chains map[types.NodeID][]*types.Node, pin types.Instant) map[snowflake.ID]uint32 {
	t.Helper()
	m := make(map[snowflake.ID]uint32)
	for nid, full := range chains {
		wantV, wantOK := storeutil.SelectAsOf(full, pin)
		if wantOK {
			m[nid.SnowflakeID()] = wantV.Version()
		}
	}
	return m
}

func oracleRelVersionSet(t *testing.T, chains map[types.RelID][]*types.Relationship, pin types.Instant) map[snowflake.ID]uint32 {
	t.Helper()
	m := make(map[snowflake.ID]uint32)
	for rid, full := range chains {
		wantV, wantOK := storeutil.SelectAsOf(full, pin)
		if wantOK {
			m[rid.SnowflakeID()] = wantV.Version()
		}
	}
	return m
}

func runNodesAsOfBulkEquivalence(t *testing.T, bs *Store) {
	t.Helper()
	rng := rand.New(rand.NewPCG(0xC0FFEE, 0xBEEF))

	const entities = 320
	chains := make(map[types.NodeID][]*types.Node, entities)
	var allIDs []types.NodeID
	for e := 0; e < entities; e++ {
		nid := types.NodeID(snowflake.ID(2_000_000 + e*2))
		chains[nid] = buildNodeChainAndPopulate(t, bs, nid, rng)
		allIDs = append(allIDs, nid)
	}
	// A handful of long chains (>16 versions) to force real anchor+delta
	// boundaries when HistoryDeltaEncoding is on (harmless no-op shape when off).
	for i := 0; i < 5; i++ {
		nid := types.NodeID(snowflake.ID(3_000_000 + i*2))
		open := i%2 == 0
		chains[nid] = buildLongNodeChain(t, bs, nid, 20+i, open)
		allIDs = append(allIDs, nid)
	}

	sort.Slice(allIDs, func(i, j int) bool { return allIDs[i].SnowflakeID() < allIDs[j].SnowflakeID() })

	// ForEachDeletedNodeID (used internally by NodesAsOf's own candidate
	// discovery, and reused here to build allIDs consistently) scans
	// badger-PERSISTED state directly, not the async pending-write overlay —
	// an explicit Flush() before any assertion that depends on durable state is
	// the established convention in this package (see newTestBadgerStore's doc
	// comment). Without it, a history-only entity whose writes are still
	// sitting in the flush buffer at query time is invisible to
	// ForEachDeletedNodeID (though still correctly visible via NodeAsOf's own
	// reverse-scan, which DOES merge the pending overlay) — a timing artifact
	// of the missing Flush, not a NodesAsOf behavior difference.
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Pins across the full range, including a FUTURE pin beyond any TxFrom used
	// above (max TxFrom used is ~250) to exercise the single-snapshot-consistency
	// contract path (no concurrent writer here, so it must still equal the
	// oracle — this only proves the static/no-contention case; the concurrent
	// case is covered by a separate test).
	pins := []types.Instant{0, 1, 7, 30, 60, 90, 120, 150, 180, 210, 250, 500, 10_000}

	for _, pin := range pins {
		got, err := bs.NodesAsOf(pin)
		if err != nil {
			t.Fatalf("pin %d: NodesAsOf: %v", pin, err)
		}
		gotSet := nodeVersionSet(got)

		oldResult := oldStyleNodesAsOf(t, bs, allIDs, pin)
		oldSet := nodeVersionSet(oldResult)

		oracleSet := oracleNodeVersionSet(t, chains, pin)

		if len(gotSet) != len(oldSet) {
			for id, v := range oldSet {
				if _, ok := gotSet[id]; !ok {
					t.Logf("MISSING in new: id=%d oldV=%d", id, v)
				}
			}
			for id, v := range gotSet {
				if _, ok := oldSet[id]; !ok {
					t.Logf("EXTRA in new: id=%d newV=%d", id, v)
				}
			}
			t.Fatalf("pin %d: new NodesAsOf returned %d entities, old per-entity loop returned %d", pin, len(gotSet), len(oldSet))
		}
		for id, v := range oldSet {
			if gotV, ok := gotSet[id]; !ok || gotV != v {
				t.Fatalf("pin %d: entity %d: new NodesAsOf = (v=%d,ok=%v), old per-entity loop = v%d", pin, id, gotV, ok, v)
			}
		}
		if len(gotSet) != len(oracleSet) {
			t.Fatalf("pin %d: new NodesAsOf returned %d entities, SelectAsOf oracle wants %d", pin, len(gotSet), len(oracleSet))
		}
		for id, v := range oracleSet {
			if gotV, ok := gotSet[id]; !ok || gotV != v {
				t.Fatalf("pin %d: entity %d: new NodesAsOf = (v=%d,ok=%v), SelectAsOf oracle wants v%d", pin, id, gotV, ok, v)
			}
		}
	}
}

func runRelsAsOfBulkEquivalence(t *testing.T, bs *Store) {
	t.Helper()
	rng := rand.New(rand.NewPCG(0xD15EA5E, 0xCAFE))

	for _, endpoint := range []types.NodeID{types.NodeID(snowflake.ID(7)), types.NodeID(snowflake.ID(9))} {
		ep := types.NewNode(endpoint, 1, nil)
		if err := bs.PutNode(ep); err != nil {
			t.Fatalf("PutNode endpoint %d: %v", endpoint, err)
		}
	}

	const entities = 320
	chains := make(map[types.RelID][]*types.Relationship, entities)
	var allIDs []types.RelID
	for e := 0; e < entities; e++ {
		rid := types.RelID(snowflake.ID(2_000_001 + e*2))
		chains[rid] = buildRelChainAndPopulate(t, bs, rid, rng)
		allIDs = append(allIDs, rid)
	}

	sort.Slice(allIDs, func(i, j int) bool { return allIDs[i].SnowflakeID() < allIDs[j].SnowflakeID() })

	// See the matching comment in runNodesAsOfBulkEquivalence: Flush before any
	// assertion depending on durable/badger-persisted state.
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	pins := []types.Instant{0, 1, 7, 30, 60, 90, 120, 150, 180, 210, 500}

	for _, pin := range pins {
		got, err := bs.RelsAsOf(pin)
		if err != nil {
			t.Fatalf("pin %d: RelsAsOf: %v", pin, err)
		}
		gotSet := relVersionSet(got)

		oldResult := oldStyleRelsAsOf(t, bs, allIDs, pin)
		oldSet := relVersionSet(oldResult)

		oracleSet := oracleRelVersionSet(t, chains, pin)

		if len(gotSet) != len(oldSet) {
			t.Fatalf("pin %d: new RelsAsOf returned %d entities, old per-entity loop returned %d", pin, len(gotSet), len(oldSet))
		}
		for id, v := range oldSet {
			if gotV, ok := gotSet[id]; !ok || gotV != v {
				t.Fatalf("pin %d: entity %d: new RelsAsOf = (v=%d,ok=%v), old per-entity loop = v%d", pin, id, gotV, ok, v)
			}
		}
		if len(gotSet) != len(oracleSet) {
			t.Fatalf("pin %d: new RelsAsOf returned %d entities, SelectAsOf oracle wants %d", pin, len(gotSet), len(oracleSet))
		}
		for id, v := range oracleSet {
			if gotV, ok := gotSet[id]; !ok || gotV != v {
				t.Fatalf("pin %d: entity %d: new RelsAsOf = (v=%d,ok=%v), SelectAsOf oracle wants v%d", pin, id, gotV, ok, v)
			}
		}
	}
}

func TestNodesAsOfSingleTxnEquivalence(t *testing.T) {
	t.Parallel()
	t.Run("delta_off", func(t *testing.T) {
		t.Parallel()
		bs := newTestBadgerStore(t)
		runNodesAsOfBulkEquivalence(t, bs)
	})
	t.Run("delta_on", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		bs, err := New(Config{Dir: dir, HistoryDeltaEncoding: true, HistoryAnchorInterval: 4})
		if err != nil {
			t.Fatalf("New(HistoryDeltaEncoding): %v", err)
		}
		t.Cleanup(func() { bs.Close() })
		runNodesAsOfBulkEquivalence(t, bs)
	})
}

func TestRelsAsOfSingleTxnEquivalence(t *testing.T) {
	t.Parallel()
	t.Run("delta_off", func(t *testing.T) {
		t.Parallel()
		bs := newTestBadgerStore(t)
		runRelsAsOfBulkEquivalence(t, bs)
	})
	t.Run("delta_on", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		bs, err := New(Config{Dir: dir, HistoryDeltaEncoding: true, HistoryAnchorInterval: 4})
		if err != nil {
			t.Fatalf("New(HistoryDeltaEncoding): %v", err)
		}
		t.Cleanup(func() { bs.Close() })
		runRelsAsOfBulkEquivalence(t, bs)
	})
}
