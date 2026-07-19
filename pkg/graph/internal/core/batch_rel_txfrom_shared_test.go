package core

import (
	"context"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BACKLOG 11d: Batch.Execute stamped TxFrom ONCE for the whole node-creation
// phase (txNow computed before the node loop, shared by every node) but
// called b.g.now() FRESH inside the per-relationship loop for rel creates —
// contradicting CLAUDE.md's documented Ingest Pipeline invariant "a whole
// commit group shares one TxFrom." Since c.now() is a monotonic-floor
// counter (context.go: next = max(observed, last+1) — every call returns a
// value strictly greater than the previous), distinct relationships created
// in the SAME batch could get DIFFERENT (increasing) TxFrom stamps.
func TestBatchExecute_RelationshipsShareOneTxFromPerCommitGroup(t *testing.T) {
	g := newTestGraph(t)
	b, err := NewBatchBuilder(g)
	if err != nil {
		t.Fatalf("NewBatchBuilder: %v", err)
	}

	n1, err := b.AddNode([]string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode n1: %v", err)
	}
	n2, err := b.AddNode([]string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode n2: %v", err)
	}
	n3, err := b.AddNode([]string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode n3: %v", err)
	}

	const n = 5
	rels := make([]*types.Relationship, n)
	for i := 0; i < n; i++ {
		endNode := n2
		if i%2 == 0 {
			endNode = n3
		}
		r, err := b.AddRelationship("KNOWS", n1, endNode, nil)
		if err != nil {
			t.Fatalf("AddRelationship[%d]: %v", i, err)
		}
		rels[i] = r
	}

	result, err := b.Execute()
	if err != nil {
		t.Fatalf("Execute: %v (result=%+v)", err, result)
	}
	if result.Created != n+3 { // 3 nodes + n rels
		t.Fatalf("result.Created = %d, want %d", result.Created, n+3)
	}

	var wantTxFrom types.Instant
	for i, r := range rels {
		got, err := g.Rels.Get(context.Background(), r.ID())
		if err != nil {
			t.Fatalf("Get rel[%d]: %v", i, err)
		}
		tm := got.Temporal()
		if tm == nil || tm.TxFrom == 0 {
			t.Fatalf("rel[%d] has no TxFrom stamped: %+v", i, tm)
		}
		if i == 0 {
			wantTxFrom = tm.TxFrom
			continue
		}
		if tm.TxFrom != wantTxFrom {
			t.Fatalf("rel[%d] TxFrom = %d, want %d (same as rel[0]) — BACKLOG 11d regression: the batch's relationships must share ONE TxFrom per commit group", i, tm.TxFrom, wantTxFrom)
		}
	}
}
