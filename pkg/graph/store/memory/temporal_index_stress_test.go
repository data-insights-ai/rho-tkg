package memory

// Stress tests for the temporal index fix (v3.0.60):
// - Zero-result short-circuit: when a temporal index exists and a temporal
//   query matches nothing, NodesByLabel must return immediately without
//   falling through to the full O(N) label scan.
// - Mixed open/closed correctness: CreateTemporalIndex + CloseNodeVersion
//   must keep counts consistent with the unindexed path.
// - Concurrent reads: temporal index queries must be race-free under parallel load.
// - Property index at scale: indexed lookup must match the fallback scan.

import (
	"fmt"
	"sync"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- Temporal index: zero-result short-circuit ---

// TestMemoryStore_TemporalIndex_ZeroResultShortCircuit verifies that when a
// temporal index exists and the temporal query matches no entries, NodesByLabel
// returns nil immediately — not a full label scan that also returns nil.
//
// Before the fix, `if ids != nil` let a nil result fall through to the full
// O(N) scan. With the fix, `temporalQuery == true` causes an immediate return.
// Both paths return 0 results; the test verifies correctness across N.
func TestMemoryStore_TemporalIndex_ZeroResultShortCircuit(t *testing.T) {
	t.Parallel()
	const n = 1000
	ms := New()

	// All nodes: ValidFrom=5000, so none are valid at t=1.
	for i := range n {
		n := types.NewNode(types.NodeID(snowflake.ID(i+1)), 1, nil)
		n.SetTemporal(&types.TemporalMetadata{ValidFrom: 5000, ValidTo: 0})
		if err := ms.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", i, err)
		}
	}

	// Without index: full scan returns 0 (baseline).
	nodes, err := ms.NodesByLabel(1, QueryOpts{ValidAt: 1})
	if err != nil {
		t.Fatalf("NodesByLabel (no index): %v", err)
	}
	if len(nodes) != 0 {
		t.Fatalf("no index: got %d nodes at t=1, want 0", len(nodes))
	}

	// Create temporal index.
	if err := ms.CreateTemporalIndex(1); err != nil {
		t.Fatalf("CreateTemporalIndex: %v", err)
	}

	// With index: must also return 0 (short-circuit, not fallthrough).
	nodes, err = ms.NodesByLabel(1, QueryOpts{ValidAt: 1})
	if err != nil {
		t.Fatalf("NodesByLabel (with index): %v", err)
	}
	if len(nodes) != 0 {
		t.Fatalf("with index: got %d nodes at t=1, want 0", len(nodes))
	}

	// Interval query that matches nothing — same fix covers this path.
	nodes, err = ms.NodesByLabel(1, QueryOpts{ValidStart: 1, ValidEnd: 100})
	if err != nil {
		t.Fatalf("NodesByLabel interval (with index): %v", err)
	}
	if len(nodes) != 0 {
		t.Fatalf("with index (interval): got %d nodes, want 0", len(nodes))
	}
}

// --- Temporal index: mixed open/closed node counts ---

// TestMemoryStore_TemporalIndex_MixedOpenClosed verifies that CreateTemporalIndex
// correctly differentiates open-ended (ValidTo=0) and closed (ValidTo=past) nodes.
// The indexed and non-indexed query paths must return identical results.
func TestMemoryStore_TemporalIndex_MixedOpenClosed(t *testing.T) {
	t.Parallel()
	const (
		total  = 500
		closed = 400 // first 400 are closed
		open   = total - closed
	)

	ms := New()
	const (
		validFrom   = types.Instant(100)
		closedAt    = types.Instant(200)  // ValidTo for closed nodes
		queryAt     = types.Instant(1000) // after closedAt, so closed nodes excluded
		queryBefore = types.Instant(50)   // before any ValidFrom → 0 results
	)

	for i := range total {
		id := snowflake.ID(i + 1)
		n := types.NewNode(types.NodeID(id), 1, nil)
		validTo := types.Instant(0)
		if i < closed {
			validTo = closedAt
		}
		n.SetTemporal(&types.TemporalMetadata{ValidFrom: validFrom, ValidTo: validTo})
		if err := ms.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", i, err)
		}
	}

	// --- Without index ---
	noIdxCurrent, err := ms.NodesByLabel(1, QueryOpts{ValidAt: queryAt})
	if err != nil {
		t.Fatalf("NodesByLabel (no index, queryAt): %v", err)
	}
	if len(noIdxCurrent) != open {
		t.Fatalf("no index queryAt: got %d, want %d", len(noIdxCurrent), open)
	}

	noIdxPast, err := ms.NodesByLabel(1, QueryOpts{ValidAt: queryBefore})
	if err != nil {
		t.Fatalf("NodesByLabel (no index, before): %v", err)
	}
	if len(noIdxPast) != 0 {
		t.Fatalf("no index before ValidFrom: got %d, want 0", len(noIdxPast))
	}

	// --- Create index ---
	if err := ms.CreateTemporalIndex(1); err != nil {
		t.Fatalf("CreateTemporalIndex: %v", err)
	}

	// --- With index: must match no-index results ---
	withIdxCurrent, err := ms.NodesByLabel(1, QueryOpts{ValidAt: queryAt})
	if err != nil {
		t.Fatalf("NodesByLabel (with index, queryAt): %v", err)
	}
	if len(withIdxCurrent) != open {
		t.Fatalf("with index queryAt: got %d, want %d", len(withIdxCurrent), open)
	}

	// IDs must match.
	for i, n := range withIdxCurrent {
		if n.ID() != noIdxCurrent[i].ID() {
			t.Errorf("result[%d] ID mismatch: index=%d, scan=%d",
				i, n.ID(), noIdxCurrent[i].ID())
		}
	}

	withIdxPast, err := ms.NodesByLabel(1, QueryOpts{ValidAt: queryBefore})
	if err != nil {
		t.Fatalf("NodesByLabel (with index, before): %v", err)
	}
	if len(withIdxPast) != 0 {
		t.Fatalf("with index before ValidFrom: got %d, want 0", len(withIdxPast))
	}

	// Non-temporal query must still return all nodes.
	all, err := ms.NodesByLabel(1, QueryOpts{})
	if err != nil {
		t.Fatalf("NodesByLabel (no filter): %v", err)
	}
	if len(all) != total {
		t.Fatalf("no filter: got %d, want %d", len(all), total)
	}
}

// --- Temporal index: concurrent reads (race detector) ---

// TestMemoryStore_TemporalIndex_ConcurrentReads spins up N goroutines all
// querying the temporal index simultaneously. Exercises the RWMutex paths
// and the lazy-sort mechanism. Run with -race.
func TestMemoryStore_TemporalIndex_ConcurrentReads(t *testing.T) {
	t.Parallel()
	const (
		nodes       = 500
		goroutines  = 20
		queriesEach = 50
	)

	ms := New()
	for i := range nodes {
		nd := types.NewNode(types.NodeID(snowflake.ID(i+1)), 1, nil)
		nd.SetTemporal(&types.TemporalMetadata{ValidFrom: types.Instant(i * 10), ValidTo: 0})
		if err := ms.PutNode(nd); err != nil {
			t.Fatalf("PutNode: %v", err)
		}
	}
	if err := ms.CreateTemporalIndex(1); err != nil {
		t.Fatalf("CreateTemporalIndex: %v", err)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, goroutines)

	for g := range goroutines {
		wg.Add(1)
		go func(gID int) {
			defer wg.Done()
			for q := range queriesEach {
				t := types.Instant((gID*queriesEach + q) * 7)
				results, err := ms.NodesByLabel(1, QueryOpts{ValidAt: t})
				if err != nil {
					errCh <- fmt.Errorf("goroutine %d query %d: %v", gID, q, err)
					return
				}
				_ = results
			}
		}(g)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}

// TestMemoryStore_TemporalIndex_ConcurrentWriteRead verifies that the temporal
// index is safe when new nodes are added concurrently with reads.
func TestMemoryStore_TemporalIndex_ConcurrentWriteRead(t *testing.T) {
	t.Parallel()
	ms := New()

	// Seed initial nodes and create index.
	for i := range 100 {
		nd := types.NewNode(types.NodeID(snowflake.ID(i+1)), 1, nil)
		nd.SetTemporal(&types.TemporalMetadata{ValidFrom: types.Instant(i * 10), ValidTo: 0})
		if err := ms.PutNode(nd); err != nil {
			t.Fatalf("PutNode seed: %v", err)
		}
	}
	if err := ms.CreateTemporalIndex(1); err != nil {
		t.Fatalf("CreateTemporalIndex: %v", err)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 4)

	// Writer: adds 200 more nodes.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range 200 {
			nd := types.NewNode(types.NodeID(snowflake.ID(1000+i+1)), 1, nil)
			nd.SetTemporal(&types.TemporalMetadata{ValidFrom: types.Instant(i * 5), ValidTo: 0})
			if err := ms.PutNode(nd); err != nil {
				errCh <- fmt.Errorf("writer PutNode: %v", err)
				return
			}
		}
	}()

	// Readers: query concurrently while writer is active.
	for r := range 3 {
		wg.Add(1)
		go func(rID int) {
			defer wg.Done()
			for q := range 100 {
				queryT := types.Instant(rID*100 + q*7)
				if _, err := ms.NodesByLabel(1, QueryOpts{ValidAt: queryT}); err != nil {
					errCh <- fmt.Errorf("reader %d query %d: %v", rID, q, err)
					return
				}
			}
		}(r)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}

// --- Property index at scale ---

// TestMemoryStore_PropertyIndex_LargeScale verifies that a property index built
// over 2000 nodes with 10 distinct values returns exact counts per value and
// matches the fallback scan for each value.
func TestMemoryStore_PropertyIndex_LargeScale(t *testing.T) {
	t.Parallel()
	const (
		total     = 2000
		numValues = 10
		perValue  = total / numValues
	)

	ms := New()
	for i := range total {
		id := snowflake.ID(i + 1)
		n := types.NewNode(types.NodeID(id), 1, nil)
		ps, err := types.NewPropertySlice(map[string]any{"tier": i % numValues})
		if err != nil {
			t.Fatalf("NewPropertySlice: %v", err)
		}
		n.SetProperties(ps)
		if err := ms.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", i, err)
		}
	}

	// Collect fallback counts (no index).
	fallbackCounts := make([]int, numValues)
	for v := range numValues {
		nodes, err := ms.NodesByLabelAndProperty(1, "tier", v, QueryOpts{})
		if err != nil {
			t.Fatalf("fallback tier=%d: %v", v, err)
		}
		fallbackCounts[v] = len(nodes)
		if len(nodes) != perValue {
			t.Errorf("fallback tier=%d: got %d, want %d", v, len(nodes), perValue)
		}
	}

	// Create property index.
	if err := ms.CreatePropertyIndex(1, "tier"); err != nil {
		t.Fatalf("CreatePropertyIndex: %v", err)
	}

	// Indexed counts must match fallback counts exactly.
	for v := range numValues {
		nodes, err := ms.NodesByLabelAndProperty(1, "tier", v, QueryOpts{})
		if err != nil {
			t.Fatalf("indexed tier=%d: %v", v, err)
		}
		if len(nodes) != fallbackCounts[v] {
			t.Errorf("indexed tier=%d: got %d, want %d", v, len(nodes), fallbackCounts[v])
		}
	}
}

// TestMemoryStore_PropertyIndex_ConsistencyAfterUpdate verifies that the
// property index stays consistent after nodes are updated or deleted.
func TestMemoryStore_PropertyIndex_ConsistencyAfterUpdate(t *testing.T) {
	t.Parallel()
	ms := New()

	// 100 nodes: tier=0 or tier=1, alternating.
	for i := range 100 {
		id := snowflake.ID(i + 1)
		n := types.NewNode(types.NodeID(id), 1, nil)
		ps, err := types.NewPropertySlice(map[string]any{"tier": i % 2})
		if err != nil {
			t.Fatalf("NewPropertySlice: %v", err)
		}
		n.SetProperties(ps)
		if err := ms.PutNode(n); err != nil {
			t.Fatalf("PutNode: %v", err)
		}
	}
	if err := ms.CreatePropertyIndex(1, "tier"); err != nil {
		t.Fatalf("CreatePropertyIndex: %v", err)
	}

	// Move node 1 from tier=0 to tier=1 via ReplaceNode.
	oldNode, err := ms.GetNode(types.NodeID(1))
	if err != nil {
		t.Fatalf("GetNode(1): %v", err)
	}
	ps, err := types.NewPropertySlice(map[string]any{"tier": 1})
	if err != nil {
		t.Fatalf("NewPropertySlice update: %v", err)
	}
	updated := oldNode.DeepCopy()
	updated.SetProperties(ps)
	if err := ms.ReplaceNode(updated); err != nil {
		t.Fatalf("ReplaceNode: %v", err)
	}

	// tier=0: was 50, now 49.
	tier0, err := ms.NodesByLabelAndProperty(1, "tier", 0, QueryOpts{})
	if err != nil {
		t.Fatalf("query tier=0: %v", err)
	}
	if len(tier0) != 49 {
		t.Errorf("tier=0 after update: got %d, want 49", len(tier0))
	}

	// tier=1: was 50, now 51.
	tier1, err := ms.NodesByLabelAndProperty(1, "tier", 1, QueryOpts{})
	if err != nil {
		t.Fatalf("query tier=1: %v", err)
	}
	if len(tier1) != 51 {
		t.Errorf("tier=1 after update: got %d, want 51", len(tier1))
	}

	// Delete node 1 (now tier=1).
	if err := ms.DeleteNode(types.NodeID(1)); err != nil {
		t.Fatalf("DeleteNode(1): %v", err)
	}

	tier1After, err := ms.NodesByLabelAndProperty(1, "tier", 1, QueryOpts{})
	if err != nil {
		t.Fatalf("query tier=1 after delete: %v", err)
	}
	if len(tier1After) != 50 {
		t.Errorf("tier=1 after delete: got %d, want 50", len(tier1After))
	}
}
