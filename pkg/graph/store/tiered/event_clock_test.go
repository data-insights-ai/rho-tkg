package tiered

import (
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// EVENTCLOCK (SCALE, ai-soc's Flux run k10262212: EventShard.checkoutForRead
// was 36 % of the time.Now calls). Every hot and warm checkout stamped
// lastAccess with the wall clock, though only cold shards read it (idle-close,
// the open-handle cap). A hot or warm read now stamps nothing, and a shard
// turning cold counts as accessed then: the idle sweep right after a demotion
// keeps it open, and closes it once its idle time passes.
func TestEventClockHotWarmReadsStampNoAccess(t *testing.T) {
	ts, err := New(Config{
		DataDir:       t.TempDir(),
		RefLabels:     []string{"Case", "User"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
		ColdAfter:     time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ts.Close() })
	_, _, signalTok := installDefaultTestLabelRegistry(t, ts)
	gen := tieredNodeGen(t)
	put := func() types.NodeID {
		id := types.NodeID(gen.Generate())
		if err := ts.PutNode(types.NewNode(id, signalTok, nil)); err != nil {
			t.Fatal(err)
		}
		return id
	}
	read := func(id types.NodeID) {
		if _, err := ts.GetNode(id); err != nil {
			t.Fatalf("GetNode(%v): %v", id, err)
		}
	}
	// touch checks out es through a read, a write (hot only) and a durable
	// flush (the open-store checkout) and fails if any stamped lastAccess.
	touch := func(es *EventShard, id types.NodeID, write bool) {
		t.Helper()
		es.SetLastAccessForTest(1)
		read(id)
		if write {
			put()
		}
		if err := ts.DurableFlush(); err != nil {
			t.Fatal(err)
		}
		if got := es.lastAccess.Load(); got != 1 {
			t.Fatalf("a %s checkout stamped lastAccess %d", es.currentTier(), got)
		}
	}
	ts.mu.RLock()
	first := ts.hotShard.name
	ts.mu.RUnlock()
	old := put()
	shard := eventShardNamed(t, ts, first)

	touch(shard, old, true)
	forceRotation(t, ts) // first -> warm
	if tier := shard.currentTier(); tier != TierWarm {
		t.Fatalf("shard %s is %s, want warm", first, tier)
	}
	touch(shard, old, false)

	time.Sleep(5 * time.Millisecond)
	before := time.Now().UnixMilli()
	forceRotation(t, ts) // first -> cold
	if tier := shard.currentTier(); tier != TierCold || shard.Store() == nil {
		t.Fatalf("shard %s is %s, open %v: want an open cold shard", first, tier, shard.Store() != nil)
	}
	if got := shard.lastAccess.Load(); got < before {
		t.Fatalf("the demotion left lastAccess %d, before it was %d", got, before)
	}
	ts.SetIdleTimeoutForTest(time.Hour)
	ts.CloseIdleShardsForTest()
	shard.LockShardMuForTest()
	open := shard.Store() != nil
	shard.UnlockShardMuForTest()
	if !open {
		t.Fatal("the idle sweep right after the demotion closed the shard")
	}
	ts.SetIdleTimeoutForTest(0)
	ts.CloseIdleShardsForTest()
	shard.LockShardMuForTest()
	open = shard.Store() != nil
	shard.UnlockShardMuForTest()
	if open {
		t.Fatal("the idle sweep past the idle time kept the shard open")
	}
	read(old)
}
