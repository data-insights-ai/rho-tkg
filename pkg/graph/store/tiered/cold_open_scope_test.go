package tiered

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestColdOpen_LateWriteJoinsOpenLogScope: a closed cold shard that a late
// write opens while a change-log scope is open joins the scope, so the write's
// record is buffered and a DiscardLogScope drops it. The scope's begin skipped
// the closed shard; without the hand-off the record self-commits where no
// rollback reaches it (R3 sweep review, finding 1; the sweep closes every
// demoted handle, which makes this path common).
func TestColdOpen_LateWriteJoinsOpenLogScope(t *testing.T) {
	cfg := mintCfg(t.TempDir(), true)
	cfg.ChangeLog = true
	ts := openMintStore(t, cfg)
	late := types.NodeID(tieredNodeGen(t).Generate())
	name := hotShardName(ts)
	rotate(t, ts)
	demoteToCold(ts, name)
	closeColdStores(t, ts)
	ts.mu.RLock()
	es := ts.eventShards[name]
	ts.mu.RUnlock()
	if ts.timestampToEventShardEntry(late.SnowflakeID()) != es {
		t.Fatal("setup: the late ID was not minted in the closed shard's window")
	}
	before, err := ts.ChangeFeed(0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := ts.BeginLogScope(); err != nil {
		t.Fatal(err)
	}
	ts.SetLogDivert(true)
	if err := ts.PutNode(types.NewNode(late, mintSignalTok, nil)); err != nil {
		t.Fatalf("late write in the scope: %v", err)
	}
	ts.SetLogDivert(false)
	if err := ts.DiscardLogScope(); err != nil {
		t.Fatal(err)
	}
	after, err := ts.ChangeFeed(0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Fatalf("change feed %d records after a discarded scope, want %d: the cold-opened shard missed the scope", len(after), len(before))
	}
}
