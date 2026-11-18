// Tests in this file pin that GraphTx serializes across goroutines so
// Commit/Rollback cannot race in-flight mutation methods. Before the
// fix, the lifecycle
// methods checked tx.done, released tx.mu around the mutation, then
// reacquired tx.mu to record rollback metadata — leaving a window where
// Commit/Rollback could fire between the done check and the mutation
// completing.
//
// Now every public tx method holds tx.mu for its entire body, so
// Commit/Rollback either fires entirely before the method observes
// tx.done (and the method returns ErrTxDone immediately) or entirely
// after (and the method's rollback log entry is recorded before
// Commit/Rollback runs).
package core

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// Rolls back a *GraphTx from its own goroutine while several goroutines are
// still calling AddNode on it. Every AddNode must return nil or ErrTxDone;
// every nil return must be in CreatedNodeIDs (recorded before Rollback ran),
// and after Rollback no node is left in the graph. A method that let Rollback
// fire between its done check and its rollback-log entry would leave a node
// behind or a CreatedNodeIDs count below the nil count.
func TestR4_GraphTx_ConcurrentAddNode_AndRollback_NoRace(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	const adders = 4
	const maxPerAdder = 10_000 // bound only; each adder stops at ErrTxDone

	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}

	var succeeded, refused atomic.Int64
	firstAdd := make(chan struct{})
	var once sync.Once
	var wg sync.WaitGroup
	wg.Add(adders)
	for w := 0; w < adders; w++ {
		go func() {
			defer wg.Done()
			for i := 0; i < maxPerAdder; i++ {
				_, err := tx.AddNode([]string{"X"}, nil)
				if err == nil {
					succeeded.Add(1)
					once.Do(func() { close(firstAdd) })
					continue
				}
				if errors.Is(err, storepkg.ErrTxDone) {
					refused.Add(1)
					return
				}
				t.Errorf("AddNode: got %v, want nil or ErrTxDone", err)
				return
			}
		}()
	}

	// Rollback races the adders: it starts once one add has succeeded and the
	// others are still in flight.
	rbErr := make(chan error, 1)
	go func() {
		<-firstAdd
		rbErr <- tx.Rollback()
	}()
	wg.Wait()
	once.Do(func() { close(firstAdd) }) // no add succeeded: unblock Rollback
	if err := <-rbErr; err != nil {
		t.Fatalf("Rollback during concurrent adds: %v", err)
	}

	if got := refused.Load(); got != adders {
		t.Errorf("adders stopped by ErrTxDone = %d, want %d (an adder ran past Rollback without refusal)", got, adders)
	}
	if got, want := len(tx.CreatedNodeIDs()), int(succeeded.Load()); got != want {
		t.Errorf("CreatedNodeIDs = %d, AddNode nil returns = %d; rollback log torn", got, want)
	}
	if nc, err := g.Nodes.Count(); err != nil || nc != 0 {
		t.Errorf("node count after Rollback = %d (err %v), want 0", nc, err)
	}
}
