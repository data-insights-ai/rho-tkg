package graph_test

import (
	"context"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
)

// TestTxRunWithLSN proves the transactional commit-LSN: RunWithLSN returns the
// exact max LSN this tx's commit assigned — a read-your-writes write-bookmark
// that is monotonic across commits and never exceeds the durable head.
func TestTxRunWithLSN(t *testing.T) {
	ctx := context.Background()
	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 1, BadgerInMemory: true, ChangeLog: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	lsn1, err := g.Tx().RunWithLSN(func(tx *graphpkg.GraphTx) error {
		_, e := tx.AddNode([]string{"A"}, map[string]any{"n": int64(1)})
		return e
	})
	if err != nil {
		t.Fatalf("tx1: %v", err)
	}
	if lsn1 == 0 {
		t.Fatal("tx1 committed a mutation but returned LSN 0")
	}
	// The write-bookmark never exceeds the durable head right after commit.
	head1, _ := g.Replication().LastCommittedLSN()
	if lsn1 > head1 {
		t.Fatalf("commit LSN %d exceeds durable head %d", lsn1, head1)
	}

	// A second committing tx assigns a strictly higher LSN.
	lsn2, err := g.Tx().RunWithLSN(func(tx *graphpkg.GraphTx) error {
		_, e := tx.AddNode([]string{"A"}, map[string]any{"n": int64(2)})
		return e
	})
	if err != nil {
		t.Fatalf("tx2: %v", err)
	}
	if lsn2 <= lsn1 {
		t.Fatalf("tx2 LSN %d not > tx1 LSN %d", lsn2, lsn1)
	}

	// A tx that mutates nothing burns no LSN.
	lsn3, err := g.Tx().RunWithLSN(func(tx *graphpkg.GraphTx) error { return nil })
	if err != nil {
		t.Fatalf("empty tx: %v", err)
	}
	if lsn3 != 0 {
		t.Fatalf("empty tx returned LSN %d, want 0", lsn3)
	}
	_ = ctx
}

// TestTxRunWithLSN_NoChangeLog: without a change-log, the commit-LSN is 0 (there
// is no feed to bookmark) but the tx still commits successfully.
func TestTxRunWithLSN_NoChangeLog(t *testing.T) {
	g, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	lsn, err := g.Tx().RunWithLSN(func(tx *graphpkg.GraphTx) error {
		_, e := tx.AddNode([]string{"A"}, nil)
		return e
	})
	if err != nil {
		t.Fatalf("tx: %v", err)
	}
	if lsn != 0 {
		t.Fatalf("no-change-log tx returned LSN %d, want 0", lsn)
	}
}

// TestBatchCommittedLSN proves the batch path surfaces its commit-LSN too.
func TestBatchCommittedLSN(t *testing.T) {
	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 2, BadgerInMemory: true, ChangeLog: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	res, err := g.Batch().Run(func(bb *graphpkg.BatchBuilder) error {
		bb.AddNode([]string{"A"}, map[string]any{"n": int64(1)})
		bb.AddNode([]string{"A"}, map[string]any{"n": int64(2)})
		return nil
	})
	if err != nil {
		t.Fatalf("batch: %v", err)
	}
	if res.CommittedLSN == 0 {
		t.Fatal("batch committed 2 nodes but CommittedLSN is 0")
	}
	head, _ := g.Replication().LastCommittedLSN()
	if res.CommittedLSN > head {
		t.Fatalf("batch commit LSN %d exceeds durable head %d", res.CommittedLSN, head)
	}
}
