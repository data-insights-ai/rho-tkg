package tiered

import (
	"encoding/binary"
	"testing"

	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestTieredRecoverChangeLogReenablesEventShard proves RecoverChangeLog closes
// the silent-feed-loss gap on an event shard that opened WHILE the change-log
// allocator was poisoned.
//
// Sequence: poisoning (an unreadable reseed watermark) can only happen at
// store-open, BEFORE the hot event shard opens (New() calls
// changeLogAllocator.reseedFromRefShard() right after the reference shard
// opens and before the hot event shard does). So a hot/warm event shard that
// opens while poisoned never receives Config.ChangeLogSeqSource — badgerCfg
// gates that wiring on the allocator's poisoned state at open time. Before
// the fix, RecoverChangeLog only called refShard.EnableChangeLog(), which is
// documented to stay inert on a shard "opened without it" (never configured)
// — so this already-open event shard's mutations stayed silently absent from
// the feed forever, even after the store-level fence cleared. The fix wires
// EnableChangeLogWithSource into every currently-open shard so it resumes
// proper change-log production without a close/reopen.
func TestTieredRecoverChangeLogReenablesEventShard(t *testing.T) {
	dir := t.TempDir()
	nodeGen := tieredNodeGen(t)

	ts, err := New(diskChangeLogConfig(dir))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	caseTok, signalTok := wireRegistry(t, ts)

	// Baseline: ref + event (hot) shard both take a committed record.
	refNode := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil)
	if err := ts.PutNode(refNode); err != nil {
		t.Fatalf("PutNode ref: %v", err)
	}
	evtNode := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	if err := ts.PutNode(evtNode); err != nil {
		t.Fatalf("PutNode evt: %v", err)
	}
	if err := ts.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	preLSN, err := ts.LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	}

	// Corrupt the reseed watermark so the NEXT open poisons the change-log
	// (reused from TestTieredChangeLogUnreadableWatermarkFailsClosed) — this
	// fires before the hot event shard reopens, so that shard reopens with NO
	// change-log wiring at all: the reachable "already-open, never wired"
	// state RecoverChangeLog must repair.
	if err := ts.MetaSet(changeLogWatermarkKey, []byte{0x01, 0x02, 0x03}); err != nil {
		t.Fatalf("corrupt watermark: %v", err)
	}
	if err := ts.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	ts2, err := New(diskChangeLogConfig(dir))
	if err != nil {
		t.Fatalf("reopen New: %v", err)
	}
	defer ts2.Close()
	caseTok2, signalTok2 := wireRegistry(t, ts2)

	if ts2.ChangeLogEnabled() {
		t.Fatal("expected change-log fenced after reopen with corrupt watermark")
	}

	// Repair the watermark and recover in place (no close/reopen).
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, preLSN+100)
	if err := ts2.MetaSet(changeLogWatermarkKey, buf); err != nil {
		t.Fatalf("repair watermark: %v", err)
	}
	if err := ts2.RecoverChangeLog(); err != nil {
		t.Fatalf("RecoverChangeLog: %v", err)
	}
	if !ts2.ChangeLogEnabled() {
		t.Fatal("expected change-log enabled after recovery")
	}

	// Write MORE mutations to ref AND the SAME already-open event (hot) shard.
	refNode2 := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok2, nil)
	if err := ts2.PutNode(refNode2); err != nil {
		t.Fatalf("PutNode ref2: %v", err)
	}
	evtNode2 := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok2, nil)
	if err := ts2.PutNode(evtNode2); err != nil {
		t.Fatalf("PutNode evt2: %v", err)
	}
	if err := ts2.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	feed, err := ts2.ChangeFeed(0, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}

	var sawRef2, sawEvt2 bool
	for _, r := range feed {
		if r.Tag != storecontract.ChangeNodePut {
			continue
		}
		body, err := storeutil.DecodeNodePut(r.Payload)
		if err != nil {
			t.Fatalf("DecodeNodePut: %v", err)
		}
		switch types.NodeID(body.Wire.ID) {
		case refNode2.ID():
			sawRef2 = true
		case evtNode2.ID():
			sawEvt2 = true
		}
	}
	if !sawRef2 {
		t.Error("post-recovery reference-shard write missing from feed")
	}
	if !sawEvt2 {
		t.Fatal("post-recovery EVENT-shard write missing from feed — RecoverChangeLog did not re-enable the already-open event shard")
	}
}

// TestTieredRecoverChangeLogThenRotationWiresNewHotShard proves a shard opened
// LAZILY AFTER RecoverChangeLog (here, a fresh hot shard created by rotation)
// picks up the change-log wiring straight from store config — no special
// handling needed, since badgerCfg reads the allocator's (now-cleared)
// poisoned state at THAT shard's own open time.
func TestTieredRecoverChangeLogThenRotationWiresNewHotShard(t *testing.T) {
	dir := t.TempDir()
	nodeGen := tieredNodeGen(t)

	ts, err := New(diskChangeLogConfig(dir))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	_, signalTok := wireRegistry(t, ts)

	e := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	if err := ts.PutNode(e); err != nil {
		t.Fatalf("PutNode evt: %v", err)
	}
	if err := ts.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	preLSN, err := ts.LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	}

	if err := ts.MetaSet(changeLogWatermarkKey, []byte{0x01, 0x02, 0x03}); err != nil {
		t.Fatalf("corrupt watermark: %v", err)
	}
	if err := ts.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	ts2, err := New(diskChangeLogConfig(dir))
	if err != nil {
		t.Fatalf("reopen New: %v", err)
	}
	defer ts2.Close()
	caseTok2, _ := wireRegistry(t, ts2)

	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, preLSN+100)
	if err := ts2.MetaSet(changeLogWatermarkKey, buf); err != nil {
		t.Fatalf("repair watermark: %v", err)
	}
	if err := ts2.RecoverChangeLog(); err != nil {
		t.Fatalf("RecoverChangeLog: %v", err)
	}

	// Rotate: the NEW hot shard is opened lazily, after recovery, via the
	// normal store-config path (not touched by RecoverChangeLog at all).
	forceRotation(t, ts2)

	n := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok2, nil)
	if err := ts2.PutNode(n); err != nil {
		t.Fatalf("PutNode ref: %v", err)
	}
	e2 := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	if err := ts2.PutNode(e2); err != nil {
		t.Fatalf("PutNode evt2 (new hot shard): %v", err)
	}
	if err := ts2.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	feed, err := ts2.ChangeFeed(0, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	var sawE2 bool
	for _, r := range feed {
		if r.Tag != storecontract.ChangeNodePut {
			continue
		}
		body, err := storeutil.DecodeNodePut(r.Payload)
		if err != nil {
			t.Fatalf("DecodeNodePut: %v", err)
		}
		if types.NodeID(body.Wire.ID) == e2.ID() {
			sawE2 = true
		}
	}
	if !sawE2 {
		t.Fatal("post-recovery, post-rotation new-hot-shard write missing from feed")
	}
}
