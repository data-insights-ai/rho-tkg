package tiered

import (
	"encoding/binary"
	"errors"
	"testing"
	"time"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func diskChangeLogConfig(dir string) Config {
	return Config{
		DataDir:       dir,
		RefLabels:     []string{"Case", "User"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
		ChangeLog:     true,
	}
}

// wireRegistry installs a Case/User/Signal label registry on a disk store (the
// helper installDefaultTestLabelRegistry does the same but is _test-scoped to a
// *testing.T setup; here we need it after a reopen too).
func wireRegistry(t *testing.T, ts *Store) (caseTok, signalTok uint16) {
	t.Helper()
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	var err error
	caseTok, err = reg.GetOrCreate("Case")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = reg.GetOrCreate("User"); err != nil {
		t.Fatal(err)
	}
	signalTok, err = reg.GetOrCreate("Signal")
	if err != nil {
		t.Fatal(err)
	}
	return caseTok, signalTok
}

// TestTieredChangeLogReseedAcrossReopen proves the store-global allocator resumes
// strictly above every durable LSN after a Close/reopen — reading only the
// refShard watermark (no cold-shard opens) — so no LSN is ever reissued.
func TestTieredChangeLogReseedAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	nodeGen := tieredNodeGen(t)

	ts, err := New(diskChangeLogConfig(dir))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	caseTok, signalTok := wireRegistry(t, ts)
	for i := 0; i < 4; i++ {
		n := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil)
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode ref: %v", err)
		}
		e := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
		if err := ts.PutNode(e); err != nil {
			t.Fatalf("PutNode evt: %v", err)
		}
	}
	last, err := ts.LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	}
	if last != 8 {
		t.Fatalf("LastCommittedLSN = %d, want 8", last)
	}
	// The refShard watermark must be persisted (reseed reads ONLY this key).
	raw, err := ts.MetaGet(changeLogWatermarkKey)
	if err != nil {
		t.Fatalf("MetaGet watermark: %v", err)
	}
	if len(raw) != 8 || binary.BigEndian.Uint64(raw) < 8 {
		t.Fatalf("refShard watermark = %x, want >= 8 persisted", raw)
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
	// A fresh write must draw an LSN strictly above the durable max (8).
	n := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok2, nil)
	if err := ts2.PutNode(n); err != nil {
		t.Fatalf("post-reopen PutNode: %v", err)
	}
	feed, err := ts2.ChangeFeed(8, 0)
	if err != nil {
		t.Fatalf("ChangeFeed(8): %v", err)
	}
	if len(feed) != 1 || feed[0].LSN <= 8 {
		t.Fatalf("post-reopen feed = %+v; want one record with LSN > 8 (no reuse)", feed)
	}
}

// TestTieredChangeLogReseedAfterRotation proves a shard that was hot, took
// records, then rotated to warm still contributes its max to the reseed via the
// monotonic refShard watermark — the reopened allocator lands above it without
// opening that (now cold/warm) shard.
func TestTieredChangeLogReseedAfterRotation(t *testing.T) {
	dir := t.TempDir()
	nodeGen := tieredNodeGen(t)

	ts, err := New(diskChangeLogConfig(dir))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	caseTok, signalTok := wireRegistry(t, ts)
	// Write event records into the current hot shard.
	for i := 0; i < 3; i++ {
		e := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
		if err := ts.PutNode(e); err != nil {
			t.Fatalf("PutNode evt: %v", err)
		}
	}
	if err := ts.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	// Rotate: the retiring shard keeps its committed log records; a new hot shard
	// starts empty. The allocator is store-level and unaffected.
	forceRotation(t, ts)
	// Write into the NEW hot shard + a reference record.
	e := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	if err := ts.PutNode(e); err != nil {
		t.Fatalf("PutNode evt2: %v", err)
	}
	r := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil)
	if err := ts.PutNode(r); err != nil {
		t.Fatalf("PutNode ref: %v", err)
	}
	last, err := ts.LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	}
	if last != 5 {
		t.Fatalf("LastCommittedLSN = %d, want 5 across the rotation boundary", last)
	}
	if err := ts.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	ts2, err := New(diskChangeLogConfig(dir))
	if err != nil {
		t.Fatalf("reopen New: %v", err)
	}
	defer ts2.Close()
	_, signalTok2 := wireRegistry(t, ts2)
	e2 := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok2, nil)
	if err := ts2.PutNode(e2); err != nil {
		t.Fatalf("post-reopen PutNode: %v", err)
	}
	feed, err := ts2.ChangeFeed(5, 0)
	if err != nil {
		t.Fatalf("ChangeFeed(5): %v", err)
	}
	if len(feed) != 1 || feed[0].LSN <= 5 {
		t.Fatalf("post-rotation-reopen feed = %+v; want one record LSN > 5", feed)
	}
}

// TestTieredChangeLogUnreadableWatermarkFailsClosed proves an unreadable reseed
// watermark fences ONLY the change-log (sticky ErrChangeLogWatermarkUnreadable);
// the store still serves its primary reads/writes, and RecoverChangeLog clears
// the gate once the watermark is repaired.
func TestTieredChangeLogUnreadableWatermarkFailsClosed(t *testing.T) {
	dir := t.TempDir()
	nodeGen := tieredNodeGen(t)

	ts, err := New(diskChangeLogConfig(dir))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	caseTok, _ := wireRegistry(t, ts)
	n := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := ts.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	// Corrupt the refShard watermark to a wrong-length value.
	if err := ts.MetaSet(changeLogWatermarkKey, []byte{0x01, 0x02, 0x03}); err != nil {
		t.Fatalf("corrupt watermark: %v", err)
	}
	if err := ts.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	ts2, err := New(diskChangeLogConfig(dir))
	if err != nil {
		t.Fatalf("reopen New should still succeed (fence is at the capability): %v", err)
	}
	defer ts2.Close()
	caseTok2, _ := wireRegistry(t, ts2)

	// Change-log capability fenced.
	if ts2.ChangeLogEnabled() {
		t.Error("ChangeLogEnabled() = true, want false (fenced)")
	}
	if _, err := ts2.LastCommittedLSN(); !errors.Is(err, ErrChangeLogWatermarkUnreadable) {
		t.Errorf("LastCommittedLSN err = %v, want ErrChangeLogWatermarkUnreadable", err)
	}
	if err := ts2.ForEachChange(0, func(storecontract.ChangeRecord) bool { return true }); err == nil {
		t.Error("ForEachChange returned nil, want fenced error")
	} else if !errors.Is(err, ErrChangeLogWatermarkUnreadable) {
		t.Errorf("ForEachChange err = %v, want ErrChangeLogWatermarkUnreadable", err)
	}

	// The store still serves its PRIMARY job: reads/writes proceed.
	n2 := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok2, nil)
	if err := ts2.PutNode(n2); err != nil {
		t.Fatalf("PutNode under fenced change-log: %v", err)
	}
	got, err := ts2.GetNode(n2.ID())
	if err != nil || got == nil {
		t.Fatalf("GetNode under fenced change-log: %v", err)
	}

	// Repair the watermark and recover in place.
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, 100)
	if err := ts2.MetaSet(changeLogWatermarkKey, buf); err != nil {
		t.Fatalf("repair watermark: %v", err)
	}
	if err := ts2.RecoverChangeLog(); err != nil {
		t.Fatalf("RecoverChangeLog: %v", err)
	}
	if !ts2.ChangeLogEnabled() {
		t.Error("ChangeLogEnabled() = false after recovery, want true")
	}
	if _, err := ts2.LastCommittedLSN(); err != nil {
		t.Errorf("LastCommittedLSN after recovery: %v", err)
	}
}
