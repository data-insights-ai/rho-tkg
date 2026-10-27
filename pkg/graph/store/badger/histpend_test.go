package badger

import (
	"slices"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// HISTPEND (SCALE, sigma's BenchmarkFoldInputs rows8 on v4.89.2: a cut grows
// from 4 to 12 ms, 14 % of the CPU in the history overlay under
// SetNodeVersionInterval's cascade). A history read found its entity's
// buffered rows by walking the whole pending-write overlay, so every
// interval write paid for every write buffered beside it. The read now finds
// its entity's buffered rows by the entity's key prefix.
func TestHistPendHistoryReadIgnoresUnrelatedPending(t *testing.T) {
	const label, id = uint16(1), existMapBase - 1
	// perRead buffers n unrelated nodes and three versions of node id, and
	// returns the time of one history read of id.
	perRead := func(t *testing.T, n int) time.Duration {
		bs, err := New(Config{InMemory: true, FlushInterval: time.Hour, MaxPendingWrites: -1})
		if err != nil {
			t.Fatal(err)
		}
		defer bs.Close()
		batch := make([]*types.Node, 0, 1000)
		for i := range n {
			batch = append(batch, types.NewNode(types.NodeID(existMapBase+int64(i)), label, nil))
			if len(batch) == cap(batch) || i == n-1 {
				if err := bs.PutNodesBatch(batch); err != nil {
					t.Fatal(err)
				}
				batch = batch[:0]
			}
		}
		version := func(v uint32) *types.Node {
			nd := types.NewNode(types.NodeID(id), label, nil)
			_ = nd.SetProperty("v", int64(v))
			nd.SetVersion(v)
			return nd
		}
		if err := bs.PutNode(version(0)); err != nil {
			t.Fatal(err)
		}
		for v := uint32(1); v <= 3; v++ {
			if err := bs.ReplaceNodeWithHistory(version(v), v-1, version(v-1)); err != nil {
				t.Fatal(err)
			}
		}
		const reads = 300
		best := time.Duration(1 << 62)
		for range 3 {
			start := time.Now()
			for range reads {
				if metas, err := bs.NodeHistoryTemporalMeta(types.NodeID(id)); err != nil || len(metas) != 3 {
					t.Fatalf("history: %d versions, %v", len(metas), err)
				}
			}
			best = min(best, time.Since(start)/reads)
		}
		return best
	}
	const small, large = 1_000, 64_000
	ts, tl := perRead(t, small), perRead(t, large)
	t.Logf("one history read: %v with %d unrelated pending writes, %v with %d", ts, small, tl, large)
	if tl > 8*ts {
		t.Fatalf("a history read takes %v with %d unrelated pending writes against %v with %d: it walks the pending buffer", tl, large, ts, small)
	}
}

// TestHistPendKeyspaceOverlayStillWalksBuffer: a read of a whole history
// keyspace (the exact erasure closure, whose prefix is one byte) still finds
// a buffered row: only an entity's prefix is listed.
func TestHistPendKeyspaceOverlayStillWalksBuffer(t *testing.T) {
	bs, err := New(Config{InMemory: true, FlushInterval: time.Hour, MaxPendingWrites: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer bs.Close()
	for _, id := range []types.NodeID{1, 3} {
		if err := bs.PutNode(types.NewNode(id, 1, nil)); err != nil {
			t.Fatal(err)
		}
	}
	const rid = types.RelID(102)
	if err := bs.PutRelVersion(rid, 0, types.NewRelationship(rid, 1, 1, 3)); err != nil {
		t.Fatal(err)
	}
	bs.idxMu.Lock()
	closure, err := bs.exactErasureRelationshipClosureLocked([]types.NodeID{1}, badgerExactErasureBounds)
	bs.idxMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(closure.RelationshipIDs, []types.RelID{rid}) {
		t.Fatalf("closure of a buffered history row: %v, want [%d]", closure.RelationshipIDs, rid)
	}
}
