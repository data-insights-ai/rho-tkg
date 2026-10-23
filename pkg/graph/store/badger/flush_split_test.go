package badger

import (
	"errors"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// FLUSHPART (docs/badger.md rule 1): a flush larger than one badger transaction
// (MaxBatchCount entries or MaxBatchSize bytes) must not leave a part of itself
// behind after a crash. The crash is simulated with the flush seam: badger
// commits a batch's first transaction on its own as soon as the batch outgrows
// it, so a flush that fails before its final commit has, with a plain
// WriteBatch, already made that part durable; the store is then closed with its
// final flush failing too (no open image, nothing requeued is written), which
// is what a process that died after the first part leaves on disk.

// splitFlushConfig is a store whose badger transaction limit is small (8 MB
// memtable: about 1.26 MB and 13,100 entries per transaction), so a test flush
// outgrows it quickly.
func splitFlushConfig(dir string) Config {
	return Config{Dir: dir, ChangeLog: true, MemTableSize: 8 << 20, FlushInterval: time.Hour}
}

// crashAfterSplitFlush makes the next flush die before its final commit and
// then stops the store the way a dead process leaves it: the flush loop's final
// flush dies the same way, and Store.Close's own final flush never runs (badger
// is closed directly, no open image).
func crashAfterSplitFlush(t *testing.T, bs *Store) {
	t.Helper()
	injected := errors.New("crash during flush")
	bs.FailNextFlushForTest(injected)
	if err := bs.Flush(); !errors.Is(err, injected) {
		t.Fatalf("Flush = %v, want the injected crash", err)
	}
	bs.FailNextFlushForTest(injected) // for the flush loop's final flush
	bs.closeOnce.Do(func() {
		bs.closing.Store(true)
		close(bs.stopCh)
		<-bs.flushDone
		<-bs.gcDone
		bs.dbClosed.Store(true)
		_ = bs.db.Close()
	})
}

func splitFlushNodes(n int) []*types.Node {
	nodes := make([]*types.Node, n)
	for i := range nodes {
		nodes[i] = types.NewNode(types.NodeID(snowflake.ID(1_000_000+i)), 7, nil)
		_ = nodes[i].SetProperty("k", int64(i))
	}
	return nodes
}

// TestFlushAboveBatchLimitIsAllOrNothingAfterCrash pins FLUSHPART for
// creations: a flush of 20,000 new nodes (several badger transactions' worth)
// that dies before its final commit reopens with either every node and every
// change-log record of the flush, or none of them, and the persisted node count
// equals the live rows. Mutation: a plain WriteBatch (today) keeps its first
// transaction: thousands of nodes without a single change-log record.
func TestFlushAboveBatchLimitIsAllOrNothingAfterCrash(t *testing.T) {
	dir := t.TempDir()
	cfg := splitFlushConfig(dir)
	bs, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	const n = 20_000
	for _, node := range splitFlushNodes(n) {
		if err := bs.PutNode(node); err != nil {
			t.Fatal(err)
		}
	}
	if maxCount := int(bs.DBForTest().MaxBatchCount()); n < maxCount {
		t.Fatalf("fixture too small: %d nodes against a %d-entry transaction limit", n, maxCount)
	}
	crashAfterSplitFlush(t, bs)

	bs, err = New(cfg)
	if err != nil {
		t.Fatalf("reopen after the crash: %v", err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	live := 0
	for _, node := range splitFlushNodes(n) {
		if _, err := bs.GetNode(node.ID()); err == nil {
			live++
		} else if !errors.Is(err, storecontract.ErrNodeNotFound) {
			t.Fatal(err)
		}
	}
	records := 0
	if err := bs.ForEachChange(0, func(storecontract.ChangeRecord) bool { records++; return true }); err != nil {
		t.Fatal(err)
	}
	count, err := bs.NodeCount()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("after the crash: %d of %d nodes live, %d change-log records, node count %d", live, n, records, count)
	if live != 0 && live != n {
		t.Fatalf("a part of the flush survived: %d of %d nodes", live, n)
	}
	if records != live {
		t.Fatalf("%d live nodes but %d change-log records", live, records)
	}
	if count != live {
		t.Fatalf("node count %d, live rows %d", count, live)
	}
}

// TestFlushAboveBatchLimitWithRemovalsReopensAfterCrash pins FLUSHPART for
// removals: 20,000 nodes flushed cleanly, then one flush deleting all of them
// dies before its final commit; the store must open again with a node count
// equal to its live rows. Without the change log the counters are the batch's
// last entries, so they fall in the transaction the crash loses. Mutation: a
// plain WriteBatch keeps the first transactions' deletes but not the counter,
// the persisted count exceeds the live rows, and New refuses the store
// (reconcilePersistedCounter).
func TestFlushAboveBatchLimitWithRemovalsReopensAfterCrash(t *testing.T) {
	dir := t.TempDir()
	cfg := splitFlushConfig(dir)
	cfg.ChangeLog = false
	bs, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	const n = 20_000
	nodes := splitFlushNodes(n)
	for _, node := range nodes {
		if err := bs.PutNode(node); err != nil {
			t.Fatal(err)
		}
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	for _, node := range nodes {
		if err := bs.DeleteNode(node.ID()); err != nil {
			t.Fatal(err)
		}
	}
	crashAfterSplitFlush(t, bs)

	bs, err = New(cfg)
	if err != nil {
		t.Fatalf("reopen after a crashed removal flush: %v", err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	live := 0
	for _, node := range nodes {
		if _, err := bs.GetNode(node.ID()); err == nil {
			live++
		}
	}
	count, err := bs.NodeCount()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("after the crash: %d of %d nodes live, node count %d", live, n, count)
	if live != 0 && live != n {
		t.Fatalf("a part of the removal flush survived: %d of %d nodes live", live, n)
	}
	if count != live {
		t.Fatalf("node count %d, live rows %d", count, live)
	}
}
