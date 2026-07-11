package tiered

import (
	"sort"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// mergeChangeFeedUnbounded is a DELIBERATELY BROKEN feed used only to demonstrate
// the ADR-0005 Finding-1 hazard: it reads each shard's durably-committed feed with
// NO flush-before-read barrier and NO W-bound, then merges by LSN. Under a
// cross-shard flush reorder (a lower LSN buffered on a slow shard while a higher
// LSN is already durable on another) it treats the slow shard as exhausted and
// emits the higher LSN, so a tail cursor skips the lower one FOREVER. The real
// ForEachChange must NOT do this — that is what this file's tests assert.
func (ts *Store) mergeChangeFeedUnbounded(afterLSN uint64) ([]storecontract.ChangeRecord, error) {
	var all []storecontract.ChangeRecord
	err := ts.forEachOpenShard(func(bs *BadgerStore) error {
		recs, err := bs.ChangeFeed(afterLSN, 0)
		if err != nil {
			return err
		}
		all = append(all, recs...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(all, func(i, j int) bool { return all[i].LSN < all[j].LSN })
	return all, nil
}

// TestTieredChangeFeedReorderBarrierNoSkip is the load-bearing adversarial test
// (ADR-0005 §2.1-barrier / Finding-1). A record with a LOWER store-global LSN is
// left BUFFERED on the reference shard while a record with a HIGHER LSN is flushed
// durable on the hot event shard first. The naive unbounded merge (no barrier)
// SKIPS the lower record — proven RED here. The real ForEachChange runs the
// flush-before-read barrier, so it emits BOTH in ascending order and skips
// nothing.
func TestTieredChangeFeedReorderBarrierNoSkip(t *testing.T) {
	ts, caseTok, _, signalTok := newChangeLogTieredStore(t)
	nodeGen := tieredNodeGen(t)

	// LSN 1 → reference shard (buffered, NOT flushed).
	refNode := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil)
	if err := ts.PutNode(refNode); err != nil {
		t.Fatalf("PutNode ref: %v", err)
	}
	// LSN 2 → hot event shard, then flush ONLY that shard so LSN 2 is durable
	// while LSN 1 is still buffered — the cross-shard flush reorder.
	evtNode := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	if err := ts.PutNode(evtNode); err != nil {
		t.Fatalf("PutNode evt: %v", err)
	}
	if err := ts.HotShardForTest().Store().Flush(); err != nil {
		t.Fatalf("flush hot shard only: %v", err)
	}

	// RED: the unbounded merge sees the reference shard as empty (LSN 1 buffered)
	// and emits only LSN 2 — the exact silent-loss the barrier must prevent.
	broken, err := ts.mergeChangeFeedUnbounded(0)
	if err != nil {
		t.Fatalf("unbounded merge: %v", err)
	}
	if len(broken) != 1 || broken[0].LSN != 2 {
		t.Fatalf("unbounded merge = %v; expected the hazard: only LSN 2 (LSN 1 skipped)", lsns(broken))
	}

	// GREEN: the real feed's barrier flushes the reference shard first, so LSN 1
	// becomes durable and both records emit in ascending order — LSN 1 is NOT
	// skipped.
	var got []storecontract.ChangeRecord
	if err := ts.ForEachChange(0, func(rec storecontract.ChangeRecord) bool {
		got = append(got, rec)
		return true
	}); err != nil {
		t.Fatalf("ForEachChange: %v", err)
	}
	if len(got) != 2 || got[0].LSN != 1 || got[1].LSN != 2 {
		t.Fatalf("barriered feed = %v, want [1 2] (LSN 1 must NOT be skipped)", lsns(got))
	}
}

// TestTieredChangeFeedWBound proves the emission bound: mergeChangeFeed emits only
// records with LSN <= W, deferring higher LSNs (allocated during a drain) to the
// next poll. This is the second half of the Finding-1 defense — the barrier makes
// LSNs allocated BEFORE the call durable, the W-bound stops the merge from
// crossing an LSN allocated DURING the drain (ADR-0005 §2.2).
func TestTieredChangeFeedWBound(t *testing.T) {
	ts, caseTok, _, signalTok := newChangeLogTieredStore(t)
	nodeGen := tieredNodeGen(t)
	for i := 0; i < 3; i++ {
		if err := ts.PutNode(types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil)); err != nil {
			t.Fatalf("PutNode ref: %v", err)
		}
		if err := ts.PutNode(types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)); err != nil {
			t.Fatalf("PutNode evt: %v", err)
		}
	}
	if err := ts.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	// Six records (LSN 1..6) are durable. Bound the merge at W=4: LSN 5 and 6 must
	// be deferred (not emitted), even though they are durable.
	var got []storecontract.ChangeRecord
	if err := ts.mergeChangeFeed(0, 4, func(rec storecontract.ChangeRecord) bool {
		got = append(got, rec)
		return true
	}); err != nil {
		t.Fatalf("mergeChangeFeed(W=4): %v", err)
	}
	want := []uint64{1, 2, 3, 4}
	if g := lsns(got); !equalLSNs(g, want) {
		t.Fatalf("W-bounded merge = %v, want %v (LSN 5,6 deferred)", g, want)
	}
}

func lsns(recs []storecontract.ChangeRecord) []uint64 {
	out := make([]uint64, len(recs))
	for i, r := range recs {
		out[i] = r.LSN
	}
	return out
}

func equalLSNs(a, b []uint64) bool {
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
