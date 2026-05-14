// Tests in this file pin R4-F2 from the 2026-05-08 maintainability
// review: GraphTx must serialize across goroutines so Commit/Rollback
// cannot race in-flight mutation methods. Pre-fix, the lifecycle
// methods checked tx.done, released tx.mu around the mutation, then
// reacquired tx.mu to record rollback metadata — leaving a window where
// Commit/Rollback could fire between the done check and the mutation
// completing.
//
// Post-fix, every public tx method holds tx.mu for its entire body, so
// Commit/Rollback either fires entirely before the method observes
// tx.done (and the method returns ErrTxDone immediately) or entirely
// after (and the method's rollback log entry is recorded before
// Commit/Rollback runs).
package core

import (
	"errors"
	"sync"
	"testing"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/store"
)

// Drives concurrent AddNode + Rollback on the same *GraphTx and
// verifies (a) the race detector stays clean and (b) no AddNode call
// returns success without its node being recorded for rollback.
//
// Without the R4-F2 fix, the race detector reliably flags the
// concurrent mutation of tx.createdNodes / tx.snapshotSet across
// goroutines.
func TestR4_GraphTx_ConcurrentAddNode_AndRollback_NoRace(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	defer g.Close()

	const adders = 4
	const perGoroutine = 50

	tx, _ := g.BeginTx()

	// Track which nodes successfully reported success — they must all
	// be reflected in CreatedNodeIDs OR they were created after
	// Rollback fired (in which case AddNode should have failed with
	// ErrTxDone, not nil). We verify after rollback completes.
	var (
		successMu sync.Mutex
		succeeded int
	)
	var wg sync.WaitGroup
	wg.Add(adders)
	for w := 0; w < adders; w++ {
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				_, err := tx.AddNode([]string{"X"}, nil)
				if err == nil {
					successMu.Lock()
					succeeded++
					successMu.Unlock()
					continue
				}
				if errors.Is(err, storepkg.ErrTxDone) {
					return
				}
				t.Errorf("AddNode unexpected error: %v", err)
				return
			}
		}()
	}

	// Snapshot CreatedNodeIDs *before* rollback so the count reflects
	// the same critical section as the AddNode return values. Without
	// the lock-for-whole-call fix, this snapshot can miss
	// just-completed-but-not-yet-recorded creations.
	wg.Wait()
	created := len(tx.CreatedNodeIDs())
	if got := succeeded; got != created {
		t.Errorf("succeeded AddNode = %d, CreatedNodeIDs = %d; entries diverge — torn rollback log", got, created)
	}

	if err := tx.Rollback(); err != nil {
		t.Errorf("Rollback after concurrent adds: %v", err)
	}
}

// Concurrent AddNode against an already-rolled-back tx must always
// return ErrTxDone — never succeed and never panic.
func TestR4_GraphTx_AddNodeAfterRollback_AlwaysErrTxDone(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	defer g.Close()

	tx, _ := g.BeginTx()
	if err := tx.Rollback(); err != nil {
		t.Fatalf("initial rollback: %v", err)
	}

	const goroutines = 8
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			_, err := tx.AddNode([]string{"X"}, nil)
			if !errors.Is(err, storepkg.ErrTxDone) {
				t.Errorf("post-rollback AddNode: got %v, want ErrTxDone", err)
			}
		}()
	}
	wg.Wait()
}
