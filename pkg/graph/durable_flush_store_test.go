package graph_test

import (
	"context"
	"errors"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
)

// Direct tests of store.DurableFlushCapability on every implementing backend
// (Testing Rule 1).

var (
	_ storepkg.DurableFlushCapability = (*badger.Store)(nil)
	_ storepkg.DurableFlushCapability = (*tiered.Store)(nil)
	_ storepkg.DurableFlushCapability = (*sharded.Store)(nil)
)

// Catches: a DurableFlush that never fsyncs (process-crash safe only), one that
// fsyncs although nothing was written since the last sync (a read-only commit
// would pay a disk sync), and one that syncs only what its OWN flush wrote — a
// group drained earlier by a plain background Flush must still be fsynced.
func TestDurableFlush_Badger_SyncsExactlyWhatWasWritten(t *testing.T) {
	bs, err := badger.New(badger.Config{Dir: t.TempDir(), FlushInterval: time.Hour})
	if err != nil {
		t.Fatalf("badger.New: %v", err)
	}
	g, err := graphpkg.New(graphpkg.Config{Store: bs})
	if err != nil {
		t.Fatalf("graph.New: %v", err)
	}
	defer func() { _ = g.Close() }()
	ctx := context.Background()

	if err := bs.DurableFlush(); err != nil {
		t.Fatalf("DurableFlush on an idle store: %v", err)
	}
	if n := bs.DurableSyncCountForTest(); n != 0 {
		t.Fatalf("idle DurableFlush issued %d fsyncs, want 0", n)
	}

	if _, err := g.Nodes().Add(ctx, []string{"Signal"}, nil); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if bs.PendingWriteCount() == 0 {
		t.Fatal("setup: the write was flushed before DurableFlush")
	}
	if err := bs.DurableFlush(); err != nil {
		t.Fatalf("DurableFlush: %v", err)
	}
	if n := bs.PendingWriteCount(); n != 0 {
		t.Fatalf("%d writes pending after DurableFlush, want 0", n)
	}
	if n := bs.DurableSyncCountForTest(); n != 1 {
		t.Fatalf("DurableFlush after a write issued %d fsyncs, want 1", n)
	}
	if err := bs.DurableFlush(); err != nil {
		t.Fatalf("second DurableFlush: %v", err)
	}
	if n := bs.DurableSyncCountForTest(); n != 1 {
		t.Fatalf("a DurableFlush with nothing new fsynced again: %d, want 1", n)
	}

	if _, err := g.Nodes().Add(ctx, []string{"Signal"}, nil); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := bs.Flush(); err != nil { // the background flush: WriteBatch, no fsync
		t.Fatalf("Flush: %v", err)
	}
	if err := bs.DurableFlush(); err != nil {
		t.Fatalf("DurableFlush after a plain Flush: %v", err)
	}
	if n := bs.DurableSyncCountForTest(); n != 2 {
		t.Fatalf("rows written by a plain Flush were not fsynced by the next DurableFlush: %d fsyncs, want 2", n)
	}
}

// Catches: a second fsync per commit under SyncWrites, where Badger already
// fsynced every WriteBatch.
func TestDurableFlush_Badger_SyncWritesNeedsNoExtraFsync(t *testing.T) {
	bs, err := badger.New(badger.Config{Dir: t.TempDir(), SyncWrites: true})
	if err != nil {
		t.Fatalf("badger.New: %v", err)
	}
	g, err := graphpkg.New(graphpkg.Config{Store: bs, DurableCommit: true})
	if err != nil {
		t.Fatalf("graph.New: %v", err)
	}
	defer func() { _ = g.Close() }()
	if err := dcWriteGroup(g, "tx"); err != nil {
		t.Fatalf("tx: %v", err)
	}
	if n := bs.DurableSyncCountForTest(); n != 0 {
		t.Fatalf("SyncWrites store: DurableFlush issued %d extra fsyncs, want 0", n)
	}
}

// Catches: claiming durability for an in-memory instance (Badger's Sync is a
// silent no-op there, so a nil answer would be a false promise), and a closed
// store answering nil.
func TestDurableFlush_Badger_SupportAndRefusals(t *testing.T) {
	disk, err := badger.New(badger.Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("badger.New dir: %v", err)
	}
	if !disk.DurableFlushSupported() {
		t.Fatal("disk store: DurableFlushSupported = false")
	}
	if err := disk.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := disk.DurableFlush(); !errors.Is(err, badger.ErrStoreClosed) {
		t.Fatalf("DurableFlush on a closed store: got %v, want ErrStoreClosed", err)
	}

	mem, err := badger.New(badger.Config{InMemory: true})
	if err != nil {
		t.Fatalf("badger.New in-memory: %v", err)
	}
	defer func() { _ = mem.Close() }()
	if mem.DurableFlushSupported() {
		t.Fatal("in-memory store: DurableFlushSupported = true")
	}
	if err := mem.DurableFlush(); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Fatalf("in-memory DurableFlush: got %v, want ErrCapabilityNotSupported", err)
	}
}

// Catches: a tiered DurableFlush that leaves one of the two touched shards
// (reference shard, hot event shard) unflushed.
func TestDurableFlush_Tiered_DrainsEveryTouchedShard(t *testing.T) {
	ts, err := tiered.New(tiered.Config{DataDir: t.TempDir(), RefLabels: []string{"Ref"}, FlushInterval: time.Hour})
	if err != nil {
		t.Fatalf("tiered.New: %v", err)
	}
	g, err := graphpkg.New(graphpkg.Config{Store: ts})
	if err != nil {
		t.Fatalf("graph.New: %v", err)
	}
	defer func() { _ = g.Close() }()
	if !ts.DurableFlushSupported() {
		t.Fatal("disk tiered store: DurableFlushSupported = false")
	}
	if err := dcWriteGroup(g, "tx"); err != nil {
		t.Fatalf("tx: %v", err)
	}
	ref := ts.RefShardForTest()
	hot := ts.HotShardForTest().Store()
	if ref.PendingWriteCount() == 0 || hot.PendingWriteCount() == 0 {
		t.Fatalf("setup: the group did not leave writes pending on both shards (ref %d, hot %d)", ref.PendingWriteCount(), hot.PendingWriteCount())
	}
	if err := ts.DurableFlush(); err != nil {
		t.Fatalf("DurableFlush: %v", err)
	}
	if r, h := ref.PendingWriteCount(), hot.PendingWriteCount(); r != 0 || h != 0 {
		t.Fatalf("after DurableFlush: ref %d, hot %d writes pending, want 0/0", r, h)
	}
	if ref.DurableSyncCountForTest() == 0 || hot.DurableSyncCountForTest() == 0 {
		t.Fatalf("a touched shard was not fsynced (ref %d, hot %d)", ref.DurableSyncCountForTest(), hot.DurableSyncCountForTest())
	}
}

// Catches: claiming durability for an in-memory tiered store, and a closed
// tiered store answering nil.
func TestDurableFlush_Tiered_SupportAndRefusals(t *testing.T) {
	mem, err := tiered.New(tiered.Config{InMemory: true, RefLabels: []string{"Ref"}})
	if err != nil {
		t.Fatalf("tiered.New in-memory: %v", err)
	}
	if mem.DurableFlushSupported() {
		t.Fatal("in-memory tiered: DurableFlushSupported = true")
	}
	if err := mem.DurableFlush(); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Fatalf("in-memory tiered DurableFlush: got %v, want ErrCapabilityNotSupported", err)
	}
	_ = mem.Close()

	disk, err := tiered.New(tiered.Config{DataDir: t.TempDir(), RefLabels: []string{"Ref"}})
	if err != nil {
		t.Fatalf("tiered.New dir: %v", err)
	}
	if err := disk.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := disk.DurableFlush(); !errors.Is(err, tiered.ErrStoreClosed) {
		t.Fatalf("DurableFlush on a closed tiered store: got %v, want ErrStoreClosed", err)
	}
}

// Catches: a sharded DurableFlush that reaches only the anchor slot, an
// in-memory sharded store claiming durability, and a closed store answering nil.
func TestDurableFlush_Sharded_DrainsEverySlotAndRefuses(t *testing.T) {
	st, err := sharded.New(sharded.Config{Dir: t.TempDir(), BaseSlot: 0, SlotCount: 2, FlushInterval: time.Hour})
	if err != nil {
		t.Fatalf("sharded.New: %v", err)
	}
	g, err := graphpkg.New(graphpkg.Config{Store: st})
	if err != nil {
		t.Fatalf("graph.New: %v", err)
	}
	if !st.DurableFlushSupported() {
		t.Fatal("disk sharded store: DurableFlushSupported = false")
	}
	if err := dcWriteGroup(g, "tx"); err != nil {
		t.Fatalf("tx: %v", err)
	}
	pending := st.ShardPendingWritesForTest()
	if len(pending) != 2 || pending[0] == 0 || pending[1] == 0 {
		t.Fatalf("setup: the group did not leave writes pending on both slots: %v", pending)
	}
	if err := st.DurableFlush(); err != nil {
		t.Fatalf("DurableFlush: %v", err)
	}
	for i, n := range st.ShardPendingWritesForTest() {
		if n != 0 {
			t.Fatalf("slot %d: %d writes pending after DurableFlush, want 0", i, n)
		}
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := st.DurableFlush(); !errors.Is(err, sharded.ErrStoreClosed) {
		t.Fatalf("DurableFlush on a closed sharded store: got %v, want ErrStoreClosed", err)
	}

	mem, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
	if err != nil {
		t.Fatalf("sharded.New in-memory: %v", err)
	}
	defer func() { _ = mem.Close() }()
	if mem.DurableFlushSupported() {
		t.Fatal("in-memory sharded: DurableFlushSupported = true")
	}
	if err := mem.DurableFlush(); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Fatalf("in-memory sharded DurableFlush: got %v, want ErrCapabilityNotSupported", err)
	}
}
