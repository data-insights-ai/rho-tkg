package core

import (
	"context"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestGraphTx_ImportWithID_UnderChangeLog exercises a transaction importing
// nodes AND a relationship by explicit ID with a store-global change-log
// enabled: the tx's writes must be buffered into a scope (never emitted
// eagerly) and their LSNs minted contiguously at Commit. As of BACKLOG 11f
// Batch F, badger (this test's backend) implements the full
// storepkg.ScopedTxCapability, so lockActiveCoreWriteContext takes the
// token-routed shared RLock path (tx.g.scopedChangeLog != nil), not the
// legacy exclusive-lock SetLogDivert path this test originally exercised —
// see tx_scoped_lock_test.go for dedicated coverage of the mechanism
// selection and both lock behaviors directly. This test's own assertions
// (imports persist, commit emitted the change records) are mechanism-
// agnostic and remain valid either way.
func TestGraphTx_ImportWithID_UnderChangeLog(t *testing.T) {
	t.Parallel()
	g, err := New(Config{BadgerInMemory: true, ChangeLog: true, SnowflakeNodeID: 3})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	ctx := context.Background()

	lsn0, err := g.Repl.LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	}

	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}

	startID := types.NodeID(snowflake.ID(700001))
	endID := types.NodeID(snowflake.ID(700002))
	relID := types.RelID(snowflake.ID(700003))

	start, err := tx.ImportNodeWithID(ctx, startID, []string{"Person"}, map[string]any{"name": "Ada"})
	if err != nil {
		t.Fatalf("ImportNodeWithID start: %v", err)
	}
	end, err := tx.ImportNodeWithID(ctx, endID, []string{"Person"}, map[string]any{"name": "Bob"})
	if err != nil {
		t.Fatalf("ImportNodeWithID end: %v", err)
	}
	if _, err := tx.ImportRelationshipWithID(ctx, relID, "KNOWS", start, end, map[string]any{"since": int64(2026)}); err != nil {
		t.Fatalf("ImportRelationshipWithID: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// The imported entities persisted with their caller-specified IDs.
	if n, err := g.Nodes.Get(ctx, startID); err != nil || n.ID() != startID {
		t.Fatalf("start node after commit: got %v (id %v), err %v", n, n.ID(), err)
	}
	if r, err := g.Rels.Get(ctx, relID); err != nil || r.ID() != relID {
		t.Fatalf("rel after commit: got %v (id %v), err %v", r, r.ID(), err)
	}

	// The scope buffered records and minted their LSNs at commit — the feed
	// advanced past the pre-commit watermark and carries every committed change.
	lsn1, err := g.Repl.LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN after commit: %v", err)
	}
	if lsn1 <= lsn0 {
		t.Fatalf("LSN did not advance across the change-logged tx commit: %d -> %d", lsn0, lsn1)
	}
	var recs int
	if err := g.Repl.ForEachChange(lsn0, func(storepkg.ChangeRecord) bool {
		recs++
		return true
	}); err != nil {
		t.Fatalf("ForEachChange: %v", err)
	}
	if recs < 3 {
		t.Fatalf("change records after tx = %d, want >= 3 (2 nodes + 1 rel)", recs)
	}
}
