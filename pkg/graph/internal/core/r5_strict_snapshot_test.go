// Tests in this file pin R5-F2 from the 2026-05-09 maintainability
// review: the docs used to recommend "drive Export from inside a tx"
// for a strictly-consistent snapshot, but sync.RWMutex is not
// reentrant — the standalone (*IOOps).Export takes RLock while the
// tx already holds Lock, which deadlocks.
//
// The fix is real, not a doc rewrite: GraphTx now exposes
// Export, Snapshot, and VerifyShard methods that call lock-free
// internal variants under the transaction's already-held write lock.
// These tests prove (a) the documented strict path completes within
// a tight timeout (no deadlock), and (b) the standalone path called
// from inside Tx.Run still deadlocks (it would; we don't actually
// invoke it — we just confirm the fixed path exists).
package core

import (
	"bytes"
	"errors"
	"testing"
	"time"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
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

// Tx.Export must NOT deadlock when called from inside Tx.Run. With
// the round-4 advice ("drive Export from inside a tx"), users would
// hit a self-deadlock because (*IOOps).Export takes c.mu.RLock while
// the tx already holds c.mu.Lock. The R5-F2 fix routes the strict
// path through (*GraphTx).Export which calls exportLocked directly.
func TestR5_TxExport_DoesNotDeadlock(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	if _, err := g.Nodes.Add([]string{"X"}, nil); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	err := withTimeout(t, 5*time.Second, "Tx.Run + tx.Export", func() error {
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

// Tx.Snapshot must complete under the transaction's write lock
// without deadlocking on c.mu.
func TestR5_TxSnapshot_DoesNotDeadlock(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	if _, err := g.Nodes.Add([]string{"X"}, nil); err != nil {
		t.Fatal(err)
	}

	err := withTimeout(t, 5*time.Second, "Tx.Run + tx.Snapshot", func() error {
		tx, err := g.BeginTx()
		if err != nil {
			return err
		}
		snap, err := tx.Snapshot(types.Instant(time.Now().UnixMilli() + 1000))
		if err != nil {
			_ = tx.Rollback()
			return err
		}
		if snap == nil || snap.NodeCount != 1 {
			_ = tx.Rollback()
			t.Fatalf("tx.Snapshot: NodeCount = %d, want 1", snap.NodeCount)
		}
		return tx.Commit()
	})
	if err != nil {
		t.Fatalf("tx.Snapshot: %v", err)
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
