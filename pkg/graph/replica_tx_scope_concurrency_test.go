package graph_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/replication"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// A transaction routes its change-log records into its own scope (keyed by the
// tx's scope token) and discards them on Rollback. This pins that a standalone
// mutation made WHILE a tx scope is open is never misrouted into that scope: its
// record reaches the feed even though the tx rolls back and discards ITS records.
// Each round opens a tx, adds a node in it, makes a standalone create while the
// scope is still open, adds a second tx node and rolls back, so the interleaving
// is fixed, not left to the scheduler. The oracle is the set of node IDs the
// feed's NodePut records carry: exactly the seed plus every standalone node, and
// no rolled-back tx node.
func TestTxScope_ConcurrentStandaloneNotMisrouted(t *testing.T) {
	ctx := context.Background()
	// SyncWrites so each committed record is durable (ChangeFeed reads durable
	// records only).
	g, err := graph.New(graph.Config{SnowflakeNodeID: 1, BadgerInMemory: true, ChangeLog: true, SyncWrites: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	// Seed label "S" so the creates need no new token.
	seed := mustAdd(t, g, []string{"S"}, map[string]any{"n": "seed"})

	const rounds = 20
	want := map[int64]bool{int64(seed.ID().SnowflakeID()): true}
	rolledBack := map[int64]bool{}
	for i := 0; i < rounds; i++ {
		tx, err := g.Tx().Begin()
		if err != nil {
			t.Fatalf("round %d: Begin: %v", i, err)
		}
		r1, err := tx.AddNode([]string{"S"}, map[string]any{"r": fmt.Sprintf("rollback-%d", i)})
		if err != nil {
			t.Fatalf("round %d: tx AddNode: %v", i, err)
		}
		n, err := g.Nodes().Add(ctx, []string{"S"}, map[string]any{"k": fmt.Sprintf("standalone-%d", i)})
		if err != nil {
			t.Fatalf("round %d: standalone Add with the tx scope open: %v", i, err)
		}
		r2, err := tx.AddNode([]string{"S"}, map[string]any{"r": fmt.Sprintf("rollback-%d-b", i)})
		if err != nil {
			t.Fatalf("round %d: tx AddNode b: %v", i, err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatalf("round %d: Rollback: %v", i, err)
		}
		want[int64(n.ID().SnowflakeID())] = true
		rolledBack[int64(r1.ID().SnowflakeID())] = true
		rolledBack[int64(r2.ID().SnowflakeID())] = true
	}

	got := map[int64]bool{}
	if err := g.Replication().ForEachChange(0, func(rec store.ChangeRecord) bool {
		if rec.Tag == store.ChangeMeta {
			return true // registry records (label tokens), not entity writes
		}
		if rec.Tag != store.ChangeNodePut {
			t.Errorf("LSN %d: tag %v, want only NodePut and Meta records", rec.LSN, rec.Tag)
			return true
		}
		kind, id, err := replication.DecodeChangeIdentity(rec)
		if err != nil || kind != replication.EntityKindNode {
			t.Errorf("LSN %d: DecodeChangeIdentity = %v, %v, want a node", rec.LSN, kind, err)
			return true
		}
		if got[int64(id)] {
			t.Errorf("LSN %d: node %d put twice", rec.LSN, id)
		}
		got[int64(id)] = true
		return true
	}); err != nil {
		t.Fatalf("ForEachChange: %v", err)
	}
	for id := range got {
		if rolledBack[id] {
			t.Fatalf("feed carries rolled-back tx node %d: the tx scope's records leaked", id)
		}
		if !want[id] {
			t.Fatalf("feed carries node %d, which is neither the seed nor a standalone node", id)
		}
	}
	for id := range want {
		if !got[id] {
			t.Fatalf("standalone node %d has no NodePut in the feed: its record was misrouted into the rolled-back tx scope", id)
		}
	}
}
