package tiered

import (
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// EVENTCLOCK (SCALE, ai-soc's Flux run k10262212: EventShard.checkoutForRead
// was 36 % of the time.Now calls). Every hot and warm checkout stamped
// lastAccess with the wall clock, though only cold shards read it (idle-close,
// the open-handle cap). A hot or warm read now stamps nothing, so a shard's
// idle time counts from its last cold access: a demoted shard no cold read
// touched closes at the next idle sweep, whatever its idle timeout (v4.87.1
// stamped the demotion instead, which kept a shard open through a sweep in the
// demotion's millisecond with IdleTimeout 0: sigma's TestFoldReadDepthHot);
// a cold read keeps it open for its idle timeout.
func TestEventClockHotWarmReadsStampNoAccess(t *testing.T) {
	ts, err := New(Config{
		DataDir:       t.TempDir(),
		RefLabels:     []string{"Case", "User"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
		ColdAfter:     time.Millisecond,
		// A cold read keeps its shard open (up to the cap) for the idle sweep.
		MaxOpenColdShards: 2,
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

	forceRotation(t, ts) // first -> cold
	if tier := shard.currentTier(); tier != TierCold || shard.Store() == nil {
		t.Fatalf("shard %s is %s, open %v: want an open cold shard", first, tier, shard.Store() != nil)
	}
	isOpen := func() bool {
		shard.LockShardMuForTest()
		defer shard.UnlockShardMuForTest()
		return shard.Store() != nil
	}
	ts.SetIdleTimeoutForTest(time.Hour)
	ts.CloseIdleShardsForTest()
	if isOpen() {
		t.Fatal("the idle sweep after the demotion kept a shard no cold read touched")
	}
	read(old) // reopens it and stamps the access
	if !isOpen() {
		t.Fatal("the cold read left the shard closed")
	}
	ts.CloseIdleShardsForTest()
	if !isOpen() {
		t.Fatal("the idle sweep closed a shard read within its idle timeout")
	}
	time.Sleep(2 * time.Millisecond)
	ts.SetIdleTimeoutForTest(0)
	ts.CloseIdleShardsForTest()
	if isOpen() {
		t.Fatal("the idle sweep past the idle time kept the shard open")
	}
	read(old)
}
