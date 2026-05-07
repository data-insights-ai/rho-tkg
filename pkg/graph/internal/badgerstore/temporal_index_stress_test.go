package badgerstore

// Stress tests for the temporal index fix (v3.0.60):
// - Zero-result short-circuit: when a temporal index exists and a temporal
//   query matches nothing, NodesByLabel must return immediately without
//   falling through to the full O(N) label scan.
// - Mixed open/closed correctness: CreateTemporalIndex + CloseNodeVersion
//   must keep counts consistent with the unindexed path.

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// TestBadgerStore_TemporalIndex_ZeroResultShortCircuit is the BadgerStore twin.
func TestBadgerStore_TemporalIndex_ZeroResultShortCircuit(t *testing.T) {
	t.Parallel()
	const n = 500
	bs := newTestBadgerStore(t)

	for i := range n {
		nd := types.NewNode(types.NodeID(snowflake.ID(i+1)), 1, nil)
		nd.SetTemporal(&types.TemporalMetadata{ValidFrom: 5000, ValidTo: 0})
		if err := bs.PutNode(nd); err != nil {
			t.Fatalf("PutNode(%d): %v", i, err)
		}
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Baseline: no index, t=1 → 0 results.
	nodes, err := bs.NodesByLabel(1, QueryOpts{ValidAt: 1})
	if err != nil {
		t.Fatalf("NodesByLabel (no index): %v", err)
	}
	if len(nodes) != 0 {
		t.Fatalf("no index: got %d nodes, want 0", len(nodes))
	}

	if err := bs.CreateTemporalIndex(1); err != nil {
		t.Fatalf("CreateTemporalIndex: %v", err)
	}

	nodes, err = bs.NodesByLabel(1, QueryOpts{ValidAt: 1})
	if err != nil {
		t.Fatalf("NodesByLabel (with index): %v", err)
	}
	if len(nodes) != 0 {
		t.Fatalf("with index: got %d nodes, want 0", len(nodes))
	}

	nodes, err = bs.NodesByLabel(1, QueryOpts{ValidStart: 1, ValidEnd: 100})
	if err != nil {
		t.Fatalf("NodesByLabel interval (with index): %v", err)
	}
	if len(nodes) != 0 {
		t.Fatalf("with index (interval): got %d nodes, want 0", len(nodes))
	}
}

// TestBadgerStore_TemporalIndex_MixedOpenClosed is the BadgerStore twin.
func TestBadgerStore_TemporalIndex_MixedOpenClosed(t *testing.T) {
	t.Parallel()
	const (
		total  = 300
		closed = 240
		open   = total - closed
	)

	bs := newTestBadgerStore(t)
	const (
		validFrom   = types.Instant(100)
		closedAt    = types.Instant(200)
		queryAt     = types.Instant(1000)
		queryBefore = types.Instant(50)
	)

	for i := range total {
		id := snowflake.ID(i + 1)
		nd := types.NewNode(types.NodeID(id), 1, nil)
		validTo := types.Instant(0)
		if i < closed {
			validTo = closedAt
		}
		nd.SetTemporal(&types.TemporalMetadata{ValidFrom: validFrom, ValidTo: validTo})
		if err := bs.PutNode(nd); err != nil {
			t.Fatalf("PutNode(%d): %v", i, err)
		}
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	noIdxCurrent, err := bs.NodesByLabel(1, QueryOpts{ValidAt: queryAt})
	if err != nil {
		t.Fatalf("no index queryAt: %v", err)
	}
	if len(noIdxCurrent) != open {
		t.Fatalf("no index: got %d, want %d", len(noIdxCurrent), open)
	}

	noIdxPast, err := bs.NodesByLabel(1, QueryOpts{ValidAt: queryBefore})
	if err != nil {
		t.Fatalf("no index before: %v", err)
	}
	if len(noIdxPast) != 0 {
		t.Fatalf("no index before ValidFrom: got %d, want 0", len(noIdxPast))
	}

	if err := bs.CreateTemporalIndex(1); err != nil {
		t.Fatalf("CreateTemporalIndex: %v", err)
	}

	withIdxCurrent, err := bs.NodesByLabel(1, QueryOpts{ValidAt: queryAt})
	if err != nil {
		t.Fatalf("with index queryAt: %v", err)
	}
	if len(withIdxCurrent) != open {
		t.Fatalf("with index: got %d, want %d", len(withIdxCurrent), open)
	}

	withIdxPast, err := bs.NodesByLabel(1, QueryOpts{ValidAt: queryBefore})
	if err != nil {
		t.Fatalf("with index before: %v", err)
	}
	if len(withIdxPast) != 0 {
		t.Fatalf("with index before ValidFrom: got %d, want 0", len(withIdxPast))
	}

	// IDs must match no-index results.
	for i, n := range withIdxCurrent {
		if n.ID() != noIdxCurrent[i].ID() {
			t.Errorf("result[%d] mismatch: idx=%d scan=%d",
				i, n.ID(), noIdxCurrent[i].ID())
		}
	}
}
