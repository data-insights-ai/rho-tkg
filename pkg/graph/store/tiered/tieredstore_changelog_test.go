package tiered

import (
	"testing"
	"time"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// newChangeLogTieredStore builds an in-memory tiered store with the change-log
// enabled and Case/User as reference labels, plus a wired label registry so
// routing resolves token classes.
func newChangeLogTieredStore(t *testing.T) (*Store, uint16, uint16, uint16) {
	t.Helper()
	ts, err := New(Config{
		InMemory:      true,
		RefLabels:     []string{"Case", "User"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
		ChangeLog:     true,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close() })
	caseTok, userTok, signalTok := installDefaultTestLabelRegistry(t, ts)
	return ts, caseTok, userTok, signalTok
}

// TestTieredChangeLogEnabled proves ChangeLogStatusCapability reports the intent.
func TestTieredChangeLogEnabled(t *testing.T) {
	ts, _, _, _ := newChangeLogTieredStore(t)
	if !ts.ChangeLogEnabled() {
		t.Fatal("ChangeLogEnabled() = false, want true")
	}
	off := newTestTieredStore(t)
	if off.ChangeLogEnabled() {
		t.Fatal("ChangeLogEnabled() = true on a store opened without the log")
	}
}

// TestTieredChangeFeedAscendingCrossShard proves cross-shard records emerge in
// ascending store-global LSN order and LastCommittedLSN folds the global max.
func TestTieredChangeFeedAscendingCrossShard(t *testing.T) {
	ts, caseTok, _, signalTok := newChangeLogTieredStore(t)
	nodeGen := tieredNodeGen(t)

	// Interleave reference-shard and event-shard writes so a per-shard-only feed
	// would emit them grouped, not globally ordered.
	ids := make([]types.NodeID, 0, 6)
	for i := 0; i < 3; i++ {
		ref := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil)
		if err := ts.PutNode(ref); err != nil {
			t.Fatalf("PutNode ref: %v", err)
		}
		ids = append(ids, ref.ID())
		evt := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
		if err := ts.PutNode(evt); err != nil {
			t.Fatalf("PutNode evt: %v", err)
		}
		ids = append(ids, evt.ID())
	}

	last, err := ts.LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	}
	if last != 6 {
		t.Fatalf("LastCommittedLSN = %d, want 6", last)
	}

	feed, err := ts.ChangeFeed(0, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(feed) != 6 {
		t.Fatalf("feed length %d, want 6", len(feed))
	}
	var prev uint64
	for i, rec := range feed {
		if rec.LSN <= prev {
			t.Errorf("record[%d] LSN %d not strictly ascending after %d", i, rec.LSN, prev)
		}
		prev = rec.LSN
		if rec.Tag != storecontract.ChangeNodePut {
			t.Errorf("record[%d] tag = %v, want NodePut", i, rec.Tag)
		}
	}
	if feed[0].LSN != 1 || feed[5].LSN != 6 {
		t.Errorf("feed LSN span = [%d,%d], want [1,6]", feed[0].LSN, feed[5].LSN)
	}

	// afterLSN paging: resume from the middle.
	tail, err := ts.ChangeFeed(3, 0)
	if err != nil {
		t.Fatalf("ChangeFeed(3): %v", err)
	}
	if len(tail) != 3 || tail[0].LSN != 4 {
		t.Fatalf("ChangeFeed(3) = %d records starting at %d, want 3 starting at 4", len(tail), tail[0].LSN)
	}
}

// TestTieredChangeFeedForEachStops proves ForEachChange honors early stop.
func TestTieredChangeFeedForEachStops(t *testing.T) {
	ts, caseTok, _, _ := newChangeLogTieredStore(t)
	nodeGen := tieredNodeGen(t)
	for i := 0; i < 5; i++ {
		n := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil)
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode: %v", err)
		}
	}
	seen := 0
	err := ts.ForEachChange(0, func(storecontract.ChangeRecord) bool {
		seen++
		return seen < 2
	})
	if err != nil {
		t.Fatalf("ForEachChange: %v", err)
	}
	if seen != 2 {
		t.Fatalf("saw %d records before stop, want 2", seen)
	}
}
