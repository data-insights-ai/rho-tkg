package badger

// Stress tests for the temporal index fix (v3.0.60):
// - Zero-result short-circuit: when a temporal index exists and a temporal
//   query matches nothing, NodesByLabel must return immediately without
//   falling through to the full O(N) label scan.
// - Mixed open/closed correctness: CreateTemporalIndex over nodes with and
//   without a ValidTo must keep counts consistent with the unindexed path.

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestBadgerStore_TemporalIndex_ZeroResultShortCircuit is the Store twin. The
// entity cache is emptied before each query so every row the query looks at is
// decoded from Badger: the unindexed scan decodes rows to find nothing, the
// indexed point and interval queries must decode none.
func TestBadgerStore_TemporalIndex_ZeroResultShortCircuit(t *testing.T) {
	t.Parallel()
	const n = 50
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

	query := func(name string, opts QueryOpts) int64 {
		t.Helper()
		bs.NodeCacheForTest().ResetForTest()
		before := bs.NodeRowDecodesForTest()
		nodes, err := bs.NodesByLabel(1, opts)
		if err != nil {
			t.Fatalf("NodesByLabel (%s): %v", name, err)
		}
		if len(nodes) != 0 {
			t.Fatalf("%s: got %d nodes, want 0", name, len(nodes))
		}
		return bs.NodeRowDecodesForTest() - before
	}

	if d := query("no index", QueryOpts{ValidAt: 1}); d == 0 {
		t.Fatal("no index: the scan decoded no rows, so the indexed zero below proves nothing")
	}
	if err := bs.CreateTemporalIndex(1); err != nil {
		t.Fatalf("CreateTemporalIndex: %v", err)
	}
	if d := query("with index", QueryOpts{ValidAt: 1}); d != 0 {
		t.Fatalf("with index: decoded %d rows, want 0 (short-circuit)", d)
	}
	if d := query("with index (interval)", QueryOpts{ValidStart: 1, ValidEnd: 100}); d != 0 {
		t.Fatalf("with index (interval): decoded %d rows, want 0 (short-circuit)", d)
	}
}

// TestBadgerStore_TemporalIndex_MixedOpenClosed is the Store twin.
func TestBadgerStore_TemporalIndex_MixedOpenClosed(t *testing.T) {
	testtier.Slow(t)
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
