package tiered

import (
	"errors"
	"slices"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestTieredRelPurge_ColdStartRefEnd (backlog 21): the split-write layout keeps
// a cross-shard relationship's row and out-leg on its start node's shard and
// its in-leg on its end node's shard. A relationship purge with the event
// shard COLD (closed) must still remove both directions — event->ref (row on
// the cold shard, in-leg on the ref shard) and ref->event (row on the ref
// shard, in-leg on the cold shard) — and an ended one's history and
// ex-adjacency legs, keep another type, and leave the cold shard closed.
// Faulty implementations caught: a purge of open shards only, a missing
// leg-residue sweep (an orphan in-leg on the ref shard), a cold shard left open.
func TestTieredRelPurge_ColdStartRefEnd(t *testing.T) {
	if testing.Short() && !raceEnabled {
		t.Skip("long tier: release gate")
	}
	t.Parallel()
	ts := newDiskTestTieredStore(t)
	caseTok, _, signalTok := installDefaultTestLabelRegistry(t, ts)
	const evType, keepType = uint16(5), uint16(6)
	nodeGen, relGen := tieredNodeGen(t), tieredRelGen(t)
	ref := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil)
	ev := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	for _, n := range []*types.Node{ref, ev} {
		if err := ts.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	newRel := func(typ uint16, s, e *types.Node) *types.Relationship {
		t.Helper()
		r := types.NewRelationship(types.RelID(relGen.Generate()), typ, s.ID(), e.ID())
		r.SetVersion(1)
		if err := ts.PutRelationship(r); err != nil {
			t.Fatal(err)
		}
		return r
	}
	evToRef := newRel(evType, ev, ref)
	refToEv := newRel(evType, ref, ev)
	ended := newRel(evType, ev, ref)
	hist := types.NewRelationship(ended.ID(), evType, ev.ID(), ref.ID())
	hist.SetVersion(0)
	if err := ts.PutRelVersion(ended.ID(), 0, hist); err != nil {
		t.Fatal(err)
	}
	if err := ts.DeleteRelationship(ended.ID()); err != nil {
		t.Fatal(err)
	}
	keep := newRel(keepType, ev, ref)

	coldName := ts.HotShardForTest().Name()
	if err := ts.ForceRotate(); err != nil {
		t.Fatal(err)
	}
	demoteToCold(ts, coldName)
	cold := ts.EventShardsForTest()[coldName]
	cold.LockShardMuForTest()
	if cold.Store() != nil {
		if err := cold.Store().Close(); err != nil {
			cold.UnlockShardMuForTest()
			t.Fatal(err)
		}
		cold.SetStoreForTest(nil)
	}
	cold.UnlockShardMuForTest()

	total := 0
	for i := 0; ; i++ {
		res, err := ts.PurgeRelsByTypeBefore(evType, types.Instant(1<<50), 2)
		if err != nil {
			t.Fatal(err)
		}
		total += res.RelsPurged
		if !res.More {
			break
		}
		if i > 10 {
			t.Fatal("More never cleared")
		}
	}
	if total != 3 {
		t.Fatalf("purged %d, want 3", total)
	}
	assertColdShardClosedForTest(t, cold, "PurgeRelsByTypeBefore")

	for _, r := range []*types.Relationship{evToRef, refToEv, ended} {
		if _, err := ts.GetRelationship(r.ID()); !errors.Is(err, storecontract.ErrRelNotFound) {
			t.Fatalf("rel %d: %v, want ErrRelNotFound", r.ID(), err)
		}
		if h, err := ts.GetRelHistory(r.ID()); err != nil || len(h) != 0 {
			t.Fatalf("rel %d: %d history rows, %v", r.ID(), len(h), err)
		}
	}
	if _, err := ts.GetRelationship(keep.ID()); err != nil {
		t.Fatalf("other type: %v", err)
	}
	sorted := func(ids []snowflake.ID) []snowflake.ID { slices.Sort(ids); return ids }
	if got := sorted(ts.RefShardForTest().IncomingRelIDs(ref.ID().SnowflakeID(), 0)); !slices.Equal(got, []snowflake.ID{keep.ID().SnowflakeID()}) {
		t.Fatalf("ref shard in-legs of ref = %v, want only %d", got, keep.ID())
	}
	if got := ts.RefShardForTest().OutgoingRelIDs(ref.ID().SnowflakeID()); len(got) != 0 {
		t.Fatalf("ref shard out-legs of ref = %v, want none", got)
	}
	store, err := cold.CheckoutStoreForTest(ts)
	if err != nil {
		t.Fatal(err)
	}
	in := store.IncomingRelIDs(ev.ID().SnowflakeID(), 0)
	out := store.OutgoingRelIDs(ev.ID().SnowflakeID())
	cold.CheckinStoreForTest()
	if len(in) != 0 || !slices.Equal(out, []snowflake.ID{keep.ID().SnowflakeID()}) {
		t.Fatalf("cold shard legs of ev: in %v out %v, want none and only %d", in, out, keep.ID())
	}
	for _, leg := range []struct {
		n        types.NodeID
		incoming bool
	}{{ref.ID(), true}, {ev.ID(), false}} {
		if err := ts.ForEachExAdjacentRelID(leg.n, leg.incoming, func(id types.RelID) bool {
			if id == ended.ID() {
				t.Errorf("ex-adjacency of %d (incoming=%v) still names the purged ended rel", leg.n, leg.incoming)
			}
			return true
		}); err != nil {
			t.Fatal(err)
		}
	}
}
