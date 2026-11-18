// Tests in this file pin the snapshot-style tx methods: the standalone
// IO/Temporal/Admin entry points take c.mu themselves, and sync.RWMutex is not
// reentrant, so GraphTx exposes Export, Snapshot, and VerifyShard methods that
// take c.mu.RLock once and call the lock-free internal variants (exportLocked,
// snapshotAt, verifyShardLocked).
// These tests prove that the tx-scoped paths complete within a tight
// timeout and that standalone Export excludes standalone mutations while
// its streamed snapshot is in progress.
package core

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// withTimeout runs fn in a goroutine and fails the test if it takes
// longer than d. Lets the deadlock-detection tests fail loud rather
// than hang forever.
func withTimeout(t *testing.T, d time.Duration, name string, fn func() error) error {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		done <- fn()
	}()
	select {
	case err := <-done:
		return err
	case <-time.After(d):
		t.Fatalf("%s: exceeded %v — likely deadlocked", name, d)
		return nil
	}
}

// (*GraphTx).Export must NOT deadlock inside an open tx. Calling
// (*IOOps).Export from there would self-deadlock because it takes c.mu.Lock
// while the tx call already holds c.mu.RLock. The tx path therefore calls
// exportLocked directly.
func TestR5_TxExport_DoesNotDeadlock(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	if _, err := g.Nodes.Add(context.Background(), []string{"X"}, nil); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	err := withTimeout(t, 5*time.Second, "BeginTx + tx.Export", func() error {
		tx, err := g.BeginTx()
		if err != nil {
			return err
		}
		if exportErr := tx.Export(&buf); exportErr != nil {
			_ = tx.Rollback()
			return exportErr
		}
		return tx.Commit()
	})
	if err != nil {
		t.Fatalf("tx.Export: %v", err)
	}
	if buf.Len() == 0 {
		t.Errorf("tx.Export wrote 0 bytes")
	}
}

// coreMuExclusive reports whether c.mu is held for writing: every stripe
// refuses a read lock. A standalone mutation reads under one stripe, so it
// cannot run while this holds.
func coreMuExclusive(c *Core) bool {
	for i := range c.mu.stripes {
		if c.mu.stripes[i].mu.TryRLock() {
			c.mu.stripes[i].mu.RUnlock()
			return false
		}
	}
	return true
}

// Standalone Export holds c.mu for writing while it streams, so a standalone
// mutation started during the stream waits for it and completes after it.
func TestR9_StandaloneExportBlocksStandaloneMutation(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	if _, err := g.Nodes.Add(context.Background(), []string{"X"}, nil); err != nil {
		t.Fatal(err)
	}

	w := &blockingExportWriter{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	exportDone := make(chan error, 1)
	go func() {
		exportDone <- g.IO.Export(w)
	}()

	select {
	case <-w.started:
	case err := <-exportDone:
		t.Fatalf("Export returned before the writer blocked: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Export to enter the blocking writer")
	}

	// The writer is blocked mid-stream: c.mu must be held exclusively.
	if !coreMuExclusive(g) {
		w.unblock()
		t.Fatal("c.mu admits a reader while Export is streaming; a standalone mutation could interleave")
	}
	mutationDone := make(chan error, 1)
	go func() {
		_, err := g.Nodes.Add(context.Background(), []string{"DuringExport"}, nil)
		mutationDone <- err
	}()
	select {
	case err := <-mutationDone:
		w.unblock()
		t.Fatalf("standalone mutation completed while Export was still streaming: %v", err)
	default:
	}

	w.unblock()

	select {
	case err := <-exportDone:
		if err != nil {
			t.Fatalf("Export: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Export to finish")
	}
	select {
	case err := <-mutationDone:
		if err != nil {
			t.Fatalf("mutation after Export release: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for blocked mutation to finish")
	}
}

type blockingExportWriter struct {
	buf         bytes.Buffer
	started     chan struct{}
	release     chan struct{}
	startOnce   sync.Once
	releaseOnce sync.Once
}

func (w *blockingExportWriter) Write(p []byte) (int, error) {
	w.startOnce.Do(func() {
		close(w.started)
		<-w.release
	})
	return w.buf.Write(p)
}

func (w *blockingExportWriter) unblock() {
	w.releaseOnce.Do(func() { close(w.release) })
}

// (*GraphTx).Snapshot must complete inside an open tx without deadlocking on
// c.mu.
func TestR5_TxSnapshot_DoesNotDeadlock(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	if _, err := g.Nodes.Add(context.Background(), []string{"X"}, nil); err != nil {
		t.Fatal(err)
	}

	err := withTimeout(t, 5*time.Second, "BeginTx + tx.Snapshot", func() error {
		tx, err := g.BeginTx()
		if err != nil {
			return err
		}
		snap, err := tx.Snapshot(types.Instant(time.Now().UnixMilli() + 1000))
		if err != nil {
			_ = tx.Rollback()
			return err
		}
		if snap == nil {
			_ = tx.Rollback()
			return errors.New("tx.Snapshot returned nil snapshot")
		}
		if snap.NodeCount != 1 {
			_ = tx.Rollback()
			return fmt.Errorf("tx.Snapshot: NodeCount = %d, want 1", snap.NodeCount)
		}
		return tx.Commit()
	})
	if err != nil {
		t.Fatalf("tx.Snapshot: %v", err)
	}
}

// Tx.VerifyShard must use the lock-free verify implementation. The default
// memory store is not tiered, so this pins the wrapper path and its sentinel
// without needing a shard setup.
func TestR5_TxVerifyShard_NonTieredReturnsErrNotTieredStore(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	tx, err := g.BeginTx()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.VerifyShard("hot"); !errors.Is(err, ErrNotTieredStore) {
		_ = tx.Rollback()
		t.Fatalf("tx.VerifyShard: %v, want ErrNotTieredStore", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
}

// Tx.Export after Commit/Rollback returns ErrTxDone — same lifecycle
// contract as every other tx method.
func TestR5_TxExport_AfterCommit_ReturnsErrTxDone(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	tx, err := g.BeginTx()
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := tx.Export(&buf); !errors.Is(err, storepkg.ErrTxDone) {
		t.Fatalf("post-commit tx.Export: %v, want ErrTxDone", err)
	}
}
