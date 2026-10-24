package badger

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// FLUSHPART (docs/badger.md rule 1). A flush larger than one badger
// transaction (MaxBatchCount entries or MaxBatchSize bytes) was one WriteBatch,
// which badger splits into independent transactions: a crash between them kept
// a random part of the flush (rows without their change-log records; after a
// removal flush, counters above the live rows and a store that refuses to
// open). Red on the WriteBatch flush: tasks/evidence/FLUSHPART/red.txt.
//
// Now a flush that fits one transaction commits as one Txn, and a larger one is
// staged: written under the 0x0E keyspace, made the commit by a small marker
// (which raises the store format marker), applied, unmarked and cleaned; New
// re-applies a marked flush and discards unmarked staging. The tests crash the
// store at each step through the stagedFlushFault seam and reopen it.

// splitFlushConfig is a store whose badger transaction limit is small (8 MB
// memtable: about 1.26 MB and 13,107 entries per transaction), so a test flush
// outgrows it quickly.
func splitFlushConfig(dir string) Config {
	return Config{Dir: dir, ChangeLog: true, MemTableSize: 8 << 20, FlushInterval: time.Hour}
}

const splitFlushNodeCount = 16_000 // creations or deletions of 16,000 nodes: above 13,107 entries

func splitFlushNodes(n int) []*types.Node {
	nodes := make([]*types.Node, n)
	for i := range nodes {
		nodes[i] = types.NewNode(types.NodeID(snowflake.ID(1_000_000+i)), 7, nil)
		_ = nodes[i].SetProperty("k", int64(i))
	}
	return nodes
}

// inOneFlush runs writes and commits them as one flush: under SyncWrites
// (rho flushes per mutation) inside a group commit, as ai-soc's commit groups
// do, else buffered and flushed by Flush. It returns that flush's error.
func inOneFlush(bs *Store, sync bool, writes func()) error {
	if sync {
		bs.BeginGroupCommit()
		writes()
		return bs.EndGroupCommit()
	}
	writes()
	return bs.Flush()
}

func putNodes(t *testing.T, bs *Store, nodes []*types.Node) {
	t.Helper()
	for _, node := range nodes {
		if err := bs.PutNode(node); err != nil {
			t.Fatal(err)
		}
	}
}

// crashStop stops the store the way a dead process leaves it: the flush loop's
// final flush dies before writing (FailNextFlushForTest), Store.Close's own
// final flush never runs, and badger is closed directly (no open image).
func crashStop(bs *Store) {
	bs.FailNextFlushForTest(errors.New("crash"))
	bs.closeOnce.Do(func() {
		bs.closing.Store(true)
		close(bs.stopCh)
		<-bs.flushDone
		<-bs.gcDone
		bs.dbClosed.Store(true)
		_ = bs.db.Close()
	})
}

// failAt makes the staged flush fail right after step (a crash point when the
// test then calls crashStop) and returns the injected error.
func failAt(bs *Store, step stagedFlushStep) error {
	injected := fmt.Errorf("injected failure after %v", step)
	bs.stagedFlushFault = func(s stagedFlushStep) error {
		if s == step {
			return injected
		}
		return nil
	}
	return injected
}

// keysWithPrefix counts the keys of an open store under prefix.
func keysWithPrefix(t *testing.T, bs *Store, prefix []byte) int {
	t.Helper()
	n := 0
	if err := bs.DBForTest().View(func(txn *badgerv4.Txn) error {
		it := txn.NewIterator(badgerv4.IteratorOptions{Prefix: prefix})
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			n++
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return n
}

// storedFormatMarker reads the store-level format marker of an open store.
func storedFormatMarker(t *testing.T, bs *Store) uint16 {
	t.Helper()
	var v uint16
	if err := bs.DBForTest().View(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(storepkg.WireFormatVersionKey)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error { v = uint16(val[0])<<8 | uint16(val[1]); return nil })
	}); err != nil {
		t.Fatal(err)
	}
	return v
}

func liveNodes(t *testing.T, bs *Store, nodes []*types.Node) int {
	t.Helper()
	live := 0
	for _, node := range nodes {
		if _, err := bs.GetNode(node.ID()); err == nil {
			live++
		} else if !errors.Is(err, storecontract.ErrNodeNotFound) {
			t.Fatal(err)
		}
	}
	return live
}

var stagedFlushSteps = []stagedFlushStep{stagedStepStaged, stagedStepMarked, stagedStepApplyWritten, stagedStepApplied, stagedStepUnmarked, stagedStepCleanWritten}

// flushReportsFailure is whether a fault after step makes the flush fail: up
// to the unmark it does (before the marker it is requeued, after it the store
// fails closed); after the unmark the flush is applied and only its clean-up
// is left, so it succeeds.
func flushReportsFailure(step stagedFlushStep) bool { return step < stagedStepUnmarked }

// TestStagedFlushCrashMatrix pins FLUSHPART at every crash point, with and
// without SyncWrites. Creations (change log on): a flush of 8,000 new nodes
// reopens with all of them and all their records, or none, never a part. The
// flush is lost when the crash comes before the marker and complete from the
// marker on. Removals (change log off: the counters are the batch's last
// entries): a flush deleting 8,000 nodes reopens, with the node count equal to
// the live rows. After every reopen no staging key and no marker remain, and
// the format marker is StagedFlushStoreFormatVersion exactly when the marker
// was committed. Mutation: the plain WriteBatch keeps a random part (red.txt);
// recovery skipped leaves the marked flush unapplied; staging applied without a
// marker resurrects a flush that never committed.
func TestStagedFlushCrashMatrix(t *testing.T) {
	for _, sync := range []bool{false, true} {
		for _, step := range stagedFlushSteps {
			for _, removal := range []bool{false, true} {
				name := fmt.Sprintf("sync=%v/%v/removal=%v", sync, step, removal)
				t.Run(name, func(t *testing.T) {
					t.Parallel()
					cfg := splitFlushConfig(t.TempDir())
					cfg.SyncWrites = sync
					cfg.ChangeLog = !removal
					bs, err := New(cfg)
					if err != nil {
						t.Fatal(err)
					}
					nodes := splitFlushNodes(splitFlushNodeCount)
					write := func() { putNodes(t, bs, nodes) }
					before := 0
					if removal {
						if err := inOneFlush(bs, sync, write); err != nil {
							t.Fatal(err)
						}
						before = len(nodes)
						write = func() {
							for _, node := range nodes {
								if err := bs.DeleteNode(node.ID()); err != nil {
									t.Fatal(err)
								}
							}
						}
					}
					markerBefore := storedFormatMarker(t, bs)
					injected := failAt(bs, step)
					err = inOneFlush(bs, sync, write)
					if flushReportsFailure(step) && !errors.Is(err, injected) {
						t.Fatalf("the flush = %v, want the injected failure", err)
					}
					if !flushReportsFailure(step) && err != nil {
						t.Fatalf("the flush = %v; it applied, only its clean-up failed", err)
					}
					crashStop(bs)

					bs, err = New(cfg)
					if err != nil {
						t.Fatalf("reopen after a crash at %v: %v", step, err)
					}
					t.Cleanup(func() { _ = bs.Close() })
					after := 0
					if !removal {
						after = len(nodes)
					}
					want := before
					if step >= stagedStepMarked {
						want = after
					}
					if got := liveNodes(t, bs, nodes); got != want {
						t.Fatalf("crash at %v: %d of %d nodes live, want %d", step, got, len(nodes), want)
					}
					count, err := bs.NodeCount()
					if err != nil {
						t.Fatal(err)
					}
					if count != want {
						t.Fatalf("node count %d, want %d", count, want)
					}
					if !removal {
						records := 0
						if err := bs.ForEachChange(0, func(storecontract.ChangeRecord) bool { records++; return true }); err != nil {
							t.Fatal(err)
						}
						if records != want {
							t.Fatalf("%d change-log records, want %d", records, want)
						}
					}
					if n := keysWithPrefix(t, bs, []byte{storepkg.KeyFlushStage}); n != 0 {
						t.Fatalf("%d staging keys left after recovery", n)
					}
					if n := keysWithPrefix(t, bs, storepkg.FlushStageMarkerKey); n != 0 {
						t.Fatal("the staged-flush marker is left after recovery")
					}
					wantMarker := markerBefore
					if step >= stagedStepMarked {
						wantMarker = storepkg.StagedFlushStoreFormatVersion
					}
					if marker := storedFormatMarker(t, bs); marker != wantMarker {
						t.Fatalf("crash at %v: format marker %d, want %d", step, marker, wantMarker)
					}
				})
			}
		}
	}
}

// TestStagedApplyFailureFailsClosed pins the rule after the marker: an apply
// that fails without a crash is never requeued (the marker already promises
// the staged values, and a requeued flush could later be overwritten by a
// stale re-apply); the store fails closed and the next open applies the flush.
// Mutation: requeue after the marker lets a write through, and the reopen's
// re-apply of the staged value hides it.
func TestStagedApplyFailureFailsClosed(t *testing.T) {
	cfg := splitFlushConfig(t.TempDir())
	bs, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	nodes := splitFlushNodes(splitFlushNodeCount)
	putNodes(t, bs, nodes)
	injected := failAt(bs, stagedStepApplyWritten)
	if err := bs.Flush(); !errors.Is(err, injected) {
		t.Fatalf("Flush = %v, want the injected failure", err)
	}
	newer := types.NewNode(nodes[0].ID(), 7, nil)
	_ = newer.SetProperty("k", int64(-1))
	if err := bs.ReplaceNode(newer); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("a write after a failed staged apply = %v, want ErrStoreClosed (the store fails closed)", err)
	}
	_ = bs.Close()

	bs, err = New(cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	if got := liveNodes(t, bs, nodes); got != len(nodes) {
		t.Fatalf("%d of %d nodes live after the reopen applied the flush", got, len(nodes))
	}
	n, err := bs.GetNode(nodes[0].ID())
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := n.GetProperty("k"); v != int64(0) {
		t.Fatalf("node 0 k = %v, want the staged 0", v)
	}
}

// tornAtMarker leaves dir with a committed staged flush of n new nodes whose
// staging lost one entry, written with or without SyncWrites.
func tornAtMarker(t *testing.T, cfg Config, nodes []*types.Node) {
	t.Helper()
	bs, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	injected := failAt(bs, stagedStepMarked)
	if err := inOneFlush(bs, cfg.SyncWrites, func() { putNodes(t, bs, nodes) }); !errors.Is(err, injected) {
		t.Fatalf("the flush = %v, want the injected failure", err)
	}
	crashStop(bs)
	updateRawBadgerDir(t, cfg.Dir, func(txn *badgerv4.Txn) error {
		it := txn.NewIterator(badgerv4.IteratorOptions{Prefix: []byte{storepkg.KeyFlushStage}})
		defer it.Close()
		it.Rewind()
		if !it.Valid() {
			return errors.New("no staging key to remove")
		}
		return txn.Delete(it.Item().KeyCopy(nil))
	})
}

// wantFlushAbsent: the store holds none of nodes, counts none, logs none and
// has no staged flush left.
func wantFlushAbsent(t *testing.T, bs *Store, nodes []*types.Node) {
	t.Helper()
	if got := liveNodes(t, bs, nodes); got != 0 {
		t.Fatalf("%d nodes of a discarded flush live", got)
	}
	if c, err := bs.NodeCount(); err != nil || c != 0 {
		t.Fatalf("node count %d (%v), want 0", c, err)
	}
	if n := keysWithPrefix(t, bs, []byte{storepkg.KeyFlushStage}); n != 0 {
		t.Fatalf("%d staging keys left", n)
	}
	if n := keysWithPrefix(t, bs, storepkg.FlushStageMarkerKey); n != 0 {
		t.Fatal("marker left")
	}
}

// TestStagedFlushRecoveryCountMismatchFailsClosed: a SyncWrites staged flush
// whose staging lost an entry (only corruption can do that: every part was
// durable before the marker) fails the open, naming the way out; after
// DiscardStagedFlush the store opens without the flush. Mutation: apply what
// is there; discard it silently; a refusal that DiscardStagedFlush cannot
// clear.
func TestStagedFlushRecoveryCountMismatchFailsClosed(t *testing.T) {
	cfg := splitFlushConfig(t.TempDir())
	cfg.SyncWrites = true
	nodes := splitFlushNodes(splitFlushNodeCount)
	tornAtMarker(t, cfg, nodes)
	bs, err := New(cfg)
	if err == nil {
		_ = bs.Close()
		t.Fatal("New applied (or dropped) a synced staged flush that lost an entry")
	}
	if !errors.Is(err, ErrInvalidStoreMutation) || !strings.Contains(err.Error(), "its marker says") || !strings.Contains(err.Error(), "DiscardStagedFlush") {
		t.Fatalf("New = %v, want the entry-count refusal naming DiscardStagedFlush", err)
	}
	dropped, err := DiscardStagedFlush(cfg)
	if err != nil || !dropped {
		t.Fatalf("DiscardStagedFlush = %v, %v", dropped, err)
	}
	if dropped, err := DiscardStagedFlush(cfg); err != nil || dropped {
		t.Fatalf("a second DiscardStagedFlush = %v, %v; want nothing to drop", dropped, err)
	}
	bs, err = New(cfg)
	if err != nil {
		t.Fatalf("open after DiscardStagedFlush: %v", err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	wantFlushAbsent(t, bs, nodes)
}

// TestStagedTornUnsyncedFlushIsDiscarded: without SyncWrites a power loss can
// keep the marker and lose earlier staging (badger syncs no rotated
// memtable's WAL). Such a flush was never durable: the open discards it and
// opens, as it would after losing any unsynced flush. Mutation: refuse it (a
// power loss would leave a store that does not open).
func TestStagedTornUnsyncedFlushIsDiscarded(t *testing.T) {
	cfg := splitFlushConfig(t.TempDir())
	nodes := splitFlushNodes(splitFlushNodeCount)
	tornAtMarker(t, cfg, nodes)
	bs, err := New(cfg)
	if err != nil {
		t.Fatalf("open after a torn unsynced staged flush: %v", err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	wantFlushAbsent(t, bs, nodes)
	records := 0
	if err := bs.ForEachChange(0, func(storecontract.ChangeRecord) bool { records++; return true }); err != nil {
		t.Fatal(err)
	}
	if records != 0 {
		t.Fatalf("%d change-log records of a discarded flush", records)
	}
}

// TestStagedFlushAbortedAttemptsDoNotMix: two flushes that fail before their
// markers leave staging of two attempts; a later flush that succeeds, and the
// reopen, keep exactly the final state and no staging. Mutation: a reused
// sequence number lets recovery apply an older attempt's entries.
func TestStagedFlushAbortedAttemptsDoNotMix(t *testing.T) {
	cfg := splitFlushConfig(t.TempDir())
	bs, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	nodes := splitFlushNodes(splitFlushNodeCount)
	putNodes(t, bs, nodes)
	for attempt := 0; attempt < 2; attempt++ {
		injected := failAt(bs, stagedStepStaged)
		if err := bs.Flush(); !errors.Is(err, injected) {
			t.Fatalf("attempt %d: Flush = %v", attempt, err)
		}
	}
	bs.stagedFlushFault = nil
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	if n := keysWithPrefix(t, bs, []byte{storepkg.KeyFlushStage}); n != 0 {
		t.Fatalf("%d staging keys left after a completed flush", n)
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	bs, err = New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	if got := liveNodes(t, bs, nodes); got != len(nodes) {
		t.Fatalf("%d of %d nodes live", got, len(nodes))
	}
}

// TestStagedFlushReadOnlyOpenNeedsRecovery: a read-only open of a store with a
// pending marker cannot apply it and fails with an error matching badger's
// ErrTruncateNeeded (the tiered store's recovery probe then falls through to a
// writable open); the writable open recovers. Mutation: a read-only open that
// ignores the marker answers without the committed flush.
func TestStagedFlushReadOnlyOpenNeedsRecovery(t *testing.T) {
	cfg := splitFlushConfig(t.TempDir())
	bs, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	nodes := splitFlushNodes(splitFlushNodeCount)
	putNodes(t, bs, nodes)
	injected := failAt(bs, stagedStepMarked)
	if err := bs.Flush(); !errors.Is(err, injected) {
		t.Fatalf("Flush = %v, want the injected failure", err)
	}
	crashStop(bs)
	ro := cfg
	ro.ReadOnly = true
	if bs, err := New(ro); err == nil {
		_ = bs.Close()
		t.Fatal("a read-only open of a store with a pending staged flush succeeded")
	} else if !errors.Is(err, badgerv4.ErrTruncateNeeded) {
		t.Fatalf("read-only open = %v, want an error matching badger's ErrTruncateNeeded", err)
	}
	bs, err = New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	if got := liveNodes(t, bs, nodes); got != len(nodes) {
		t.Fatalf("%d of %d nodes live after recovery", got, len(nodes))
	}
}

// TestClearDropsStagingKeys: Clear removes staging leftovers with the data,
// with and without the change log. Mutation: 0x0E missing from Clear's drop
// list.
func TestClearDropsStagingKeys(t *testing.T) {
	for _, changeLog := range []bool{true, false} {
		t.Run(fmt.Sprintf("changeLog=%v", changeLog), func(t *testing.T) {
			cfg := splitFlushConfig(t.TempDir())
			cfg.ChangeLog = changeLog
			bs, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = bs.Close() })
			putNodes(t, bs, splitFlushNodes(splitFlushNodeCount))
			injected := failAt(bs, stagedStepStaged)
			if err := bs.Flush(); !errors.Is(err, injected) {
				t.Fatalf("Flush = %v, want the injected failure", err)
			}
			bs.stagedFlushFault = nil
			if n := keysWithPrefix(t, bs, []byte{storepkg.KeyFlushStage}); n == 0 {
				t.Fatal("fixture left no staging keys")
			}
			if err := bs.Clear(); err != nil {
				t.Fatal(err)
			}
			if n := keysWithPrefix(t, bs, []byte{storepkg.KeyFlushStage}); n != 0 {
				t.Fatalf("Clear left %d staging keys", n)
			}
		})
	}
}

// TestStagedFlushFormatMarker: a flush under the transaction limit leaves the
// format marker as it was and writes no staging; a staged flush raises it to
// StagedFlushStoreFormatVersion, which a binary reading only up to the previous
// version refuses at open. Mutation: the marker not raised lets an older rho
// open the store, ignore a pending marker and keep writing.
func TestStagedFlushFormatMarker(t *testing.T) {
	cfg := splitFlushConfig(t.TempDir())
	bs, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNode(types.NewNode(types.NodeID(snowflake.ID(42)), 7, nil)); err != nil {
		t.Fatal(err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	if small := storedFormatMarker(t, bs); small >= storepkg.StagedFlushStoreFormatVersion {
		t.Fatalf("a flush under the limit raised the marker to %d", small)
	}
	if n := keysWithPrefix(t, bs, []byte{storepkg.KeyFlushStage}); n != 0 {
		t.Fatalf("a flush under the limit wrote %d staging keys", n)
	}
	putNodes(t, bs, splitFlushNodes(splitFlushNodeCount))
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := storedFormatMarker(t, bs); got != storepkg.StagedFlushStoreFormatVersion {
		t.Fatalf("after a staged flush the marker is %d, want %d", got, storepkg.StagedFlushStoreFormatVersion)
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	old := maxReadableStoreFormatVersion
	maxReadableStoreFormatVersion = storepkg.StagedFlushStoreFormatVersion - 1
	t.Cleanup(func() { maxReadableStoreFormatVersion = old })
	if bs, err := New(cfg); err == nil {
		_ = bs.Close()
		t.Fatal("an older reader opened a store with a staged flush")
	} else if !errors.Is(err, storecontract.ErrWireFormatVersionUnsupported) {
		t.Fatalf("older reader: %v, want ErrWireFormatVersionUnsupported", err)
	}
}

// TestStagedFlushKeepsFormatMarkerRaisedMidFlush: a feature raises the format
// marker (here to KeptValidTo, 8) while a staged flush runs, after its snapshot:
// the raise's write lands in the next flush. The staged flush's marker
// transaction stores 9; the next flush must not lower it to 8 (an older
// release would then open a store whose staged-flush marker it ignores).
// Mutation: the flush writes the raise's own value instead of the tracker's.
func TestStagedFlushKeepsFormatMarkerRaisedMidFlush(t *testing.T) {
	cfg := splitFlushConfig(t.TempDir())
	bs, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	putNodes(t, bs, splitFlushNodes(splitFlushNodeCount))
	bs.stagedFlushFault = func(s stagedFlushStep) error {
		if s == stagedStepBegin { // after the snapshot, before the staged flush raises the tracker
			bs.wbMu.Lock()
			bs.raiseFormatMarkerPendingLocked(storepkg.KeptValidToStoreFormatVersion)
			bs.wbMu.Unlock()
		}
		return nil
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	bs.stagedFlushFault = nil
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := storedFormatMarker(t, bs); got != storepkg.StagedFlushStoreFormatVersion {
		t.Fatalf("format marker %d after a mid-flush raise to %d, want %d", got, storepkg.KeptValidToStoreFormatVersion, storepkg.StagedFlushStoreFormatVersion)
	}
}

// TestDiscardStagedFlushRefusesWhatIsNotItsStore: DiscardStagedFlush on a
// missing directory, or a directory that holds no badger store, fails with
// fs.ErrNotExist and creates nothing; on a store of a newer format it refuses
// (ErrWireFormatVersionUnsupported) and leaves the staged flush alone.
// Catches a discard that creates an empty store at a mistyped path and
// answers "nothing to drop", or edits a store this binary cannot read.
func TestDiscardStagedFlushRefusesWhatIsNotItsStore(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "no-such-store")
	if _, err := DiscardStagedFlush(splitFlushConfig(missing)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("missing dir: %v, want fs.ErrNotExist", err)
	}
	if _, err := os.Stat(missing); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("DiscardStagedFlush created the missing directory")
	}
	empty := t.TempDir()
	if _, err := DiscardStagedFlush(splitFlushConfig(empty)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("dir without a store: %v, want fs.ErrNotExist", err)
	}
	if entries, _ := os.ReadDir(empty); len(entries) != 0 {
		t.Fatalf("DiscardStagedFlush wrote %d files into a dir without a store", len(entries))
	}

	cfg := splitFlushConfig(t.TempDir())
	cfg.SyncWrites = true
	tornAtMarker(t, cfg, splitFlushNodes(splitFlushNodeCount))
	old := maxReadableStoreFormatVersion
	maxReadableStoreFormatVersion = storepkg.StagedFlushStoreFormatVersion - 1
	_, err := DiscardStagedFlush(cfg)
	maxReadableStoreFormatVersion = old
	if !errors.Is(err, storecontract.ErrWireFormatVersionUnsupported) {
		t.Fatalf("newer-format store: %v, want ErrWireFormatVersionUnsupported", err)
	}
	if dropped, err := DiscardStagedFlush(cfg); err != nil || !dropped {
		t.Fatalf("the staged flush must still be there to drop: %v, %v", dropped, err)
	}
}

// TestDiscardedFlushLeavesCountersReconcilable: a torn staged flush whose
// counter keys were already applied (a part of the apply survived) leaves
// counters above the live rows; dropping the flush (DiscardStagedFlush when it
// was synced, the open itself when not) also drops the counters, so the open
// counts the live rows instead of refusing. Catches a discard after which the
// store still does not open.
func TestDiscardedFlushLeavesCountersReconcilable(t *testing.T) {
	for _, sync := range []bool{true, false} {
		t.Run(fmt.Sprintf("sync=%v", sync), func(t *testing.T) {
			cfg := splitFlushConfig(t.TempDir())
			cfg.SyncWrites = sync
			nodes := splitFlushNodes(splitFlushNodeCount)
			tornAtMarker(t, cfg, nodes)
			updateRawBadgerDir(t, cfg.Dir, func(txn *badgerv4.Txn) error {
				v := make([]byte, 8)
				binary.BigEndian.PutUint64(v, uint64(len(nodes)))
				return txn.Set(counterNodeCountKey, v) // the counter part of the apply survived
			})
			if sync {
				if dropped, err := DiscardStagedFlush(cfg); err != nil || !dropped {
					t.Fatalf("DiscardStagedFlush = %v, %v", dropped, err)
				}
			}
			bs, err := New(cfg)
			if err != nil {
				t.Fatalf("open after the discard: %v", err)
			}
			t.Cleanup(func() { _ = bs.Close() })
			wantFlushAbsent(t, bs, nodes)
		})
	}
}
