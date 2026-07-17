package graph_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestModelA_ForeignIncomingConvergence (ADR-0010 Model A, increments 2+3) drives
// the full end-side path: the graph door records a cross-machine incoming
// half-edge stub through the registry (re-tokenized type, recomputed hash), it is
// locally visible via IncomingRelationships(END) while a slot-routed point read
// still fails closed, and — the crux — a replica of the END machine reproduces the
// stub BYTE-EXACT from the ChangeForeignIncoming feed record (the foreign-slot
// stub is routed by the END-node slot on apply, not the rel slot).
func TestModelA_ForeignIncomingConvergence(t *testing.T) {
	ctx := context.Background()

	// END machine (sharded, change-log ON).
	eStore, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2, ChangeLog: true})
	if err != nil {
		t.Fatalf("end sharded.New: %v", err)
	}
	e, err := graph.New(graph.Config{SnowflakeNodeID: 0, Store: eStore})
	if err != nil {
		t.Fatalf("end New: %v", err)
	}
	defer e.Close()

	// Seed the rel-type KNOWS + the local END node BEFORE the snapshot so the
	// replica's registry knows KNOWS (no post-bootstrap token refetch needed).
	end, err := e.Nodes().Add(ctx, []string{"Person"}, map[string]any{"name": "Ada"})
	if err != nil {
		t.Fatalf("Add end: %v", err)
	}
	x, _ := e.Nodes().Add(ctx, []string{"Person"}, nil)
	y, _ := e.Nodes().Add(ctx, []string{"Person"}, nil)
	if _, err := e.Rels().AddByID(ctx, "KNOWS", x.ID(), y.ID(), nil); err != nil {
		t.Fatalf("seed KNOWS: %v", err)
	}

	var snap bytes.Buffer
	if err := e.IO().Export(&snap); err != nil {
		t.Fatalf("Export: %v", err)
	}
	lsn0, _ := e.Replication().LastCommittedLSN()

	// Replica of the END machine (same 2-slot topology, read-only).
	repStore, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
	if err != nil {
		t.Fatalf("replica sharded.New: %v", err)
	}
	rep, err := graph.New(graph.Config{SnowflakeNodeID: 6, Store: repStore, ReadOnlyReplica: true})
	if err != nil {
		t.Fatalf("replica New: %v", err)
	}
	defer rep.Close()
	if err := rep.IO().Import(bytes.NewReader(snap.Bytes()), tkgio.ImportOptions{}); err != nil {
		t.Fatalf("replica Import: %v", err)
	}
	if err := rep.Replication().SetAppliedLSN(lsn0); err != nil {
		t.Fatalf("replica SetAppliedLSN: %v", err)
	}

	// Record a cross-machine incoming edge into `end` (start + rel-ID on foreign slot 11).
	edge := store.ForeignIncomingEdge{
		RelID:      types.RelID(snowflake.ID(700003)),
		TypeName:   "KNOWS",
		StartID:    types.NodeID(snowflake.ID(700001)),
		EndID:      end.ID(),
		Properties: map[string]any{"w": int64(7)},
		FromHash:   "aa11",
		ToHash:     "bb22",
		TxFrom:     1234,
		Version:    0,
		AttestTx:   1,
	}
	if err := e.Rels().RecordForeignIncoming(ctx, edge); err != nil {
		t.Fatalf("RecordForeignIncoming: %v", err)
	}

	// Local visibility on the END machine.
	in, err := e.Rels().Incoming(end.ID(), "KNOWS")
	if err != nil {
		t.Fatalf("end Incoming: %v", err)
	}
	if len(in) != 1 || in[0].ID() != edge.RelID {
		t.Fatalf("end Incoming(END) = %d rels, want the stub %d", len(in), edge.RelID.SnowflakeID())
	}
	if in[0].Integrity().ToNodeHash != "bb22" {
		t.Fatalf("stub ToNodeHash = %q, want bb22", in[0].Integrity().ToNodeHash)
	}
	// The foreign-slot stub is NOT a slot-routed point read on the end machine.
	if _, err := e.Rels().Get(ctx, edge.RelID); err == nil {
		t.Fatal("end Get(stub relID) succeeded, want fail-closed (foreign slot)")
	}

	// Tail the END machine's feed and apply to its replica.
	var recs []store.ChangeRecord
	if err := e.Replication().ForEachChange(lsn0, func(rec store.ChangeRecord) bool {
		recs = append(recs, rec)
		return true
	}); err != nil {
		t.Fatalf("ForEachChange: %v", err)
	}
	if _, err := rep.Replication().ApplyChanges(recs); err != nil {
		t.Fatalf("replica ApplyChanges: %v", err)
	}

	// The replica reproduced the stub byte-exact: Incoming(END) returns it with
	// identical rel-ID, content hash, and to-hash.
	rin, err := rep.Rels().Incoming(end.ID(), "KNOWS")
	if err != nil {
		t.Fatalf("replica Incoming: %v", err)
	}
	if len(rin) != 1 || rin[0].ID() != edge.RelID {
		t.Fatalf("replica Incoming(END) = %d rels, want the stub", len(rin))
	}
	if rin[0].Integrity().Hash != in[0].Integrity().Hash {
		t.Fatalf("stub content hash diverged: primary %q vs replica %q", in[0].Integrity().Hash, rin[0].Integrity().Hash)
	}
	if rin[0].Integrity().ToNodeHash != "bb22" || rin[0].Integrity().FromNodeHash != "aa11" {
		t.Fatalf("replica stub endpoint hashes diverged: from=%q to=%q", rin[0].Integrity().FromNodeHash, rin[0].Integrity().ToNodeHash)
	}
	// Idempotent re-apply is a no-op.
	if _, err := rep.Replication().ApplyChanges(recs); err != nil {
		t.Fatalf("replica re-ApplyChanges (idempotency): %v", err)
	}
	if again, _ := rep.Rels().Incoming(end.ID(), "KNOWS"); len(again) != 1 {
		t.Fatalf("re-apply changed replica incoming set to %d, want 1 (idempotent)", len(again))
	}
}

// TestModelA_ForeignIncomingDeleteConvergence (ADR-0010 Model A, increment 4 —
// cascade) drives the DELETE side: with an incoming half-edge stub present on the
// END machine, deleting the END node must (1) SUCCEED — before this increment it
// failed closed with ErrSlotNotLocal because the cascade tried to route the
// foreign-slot stub by its rel slot — (2) remove the stub locally, and (3)
// replicate the removal so a replica of the END machine reaches the same
// stub-free state via the dedicated ChangeForeignIncomingDelete feed record
// (routed by the END-node slot, idempotently).
func TestModelA_ForeignIncomingDeleteConvergence(t *testing.T) {
	ctx := context.Background()

	eStore, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2, ChangeLog: true})
	if err != nil {
		t.Fatalf("end sharded.New: %v", err)
	}
	e, err := graph.New(graph.Config{SnowflakeNodeID: 0, Store: eStore})
	if err != nil {
		t.Fatalf("end New: %v", err)
	}
	defer e.Close()

	end, err := e.Nodes().Add(ctx, []string{"Person"}, map[string]any{"name": "Ada"})
	if err != nil {
		t.Fatalf("Add end: %v", err)
	}
	// A co-located ordinary KNOWS edge into `end` too, so the cascade removes BOTH
	// a normal rel (with a history tombstone) AND the adjacency-only stub.
	other, _ := e.Nodes().Add(ctx, []string{"Person"}, nil)
	if _, err := e.Rels().AddByID(ctx, "KNOWS", other.ID(), end.ID(), nil); err != nil {
		t.Fatalf("seed co-located KNOWS: %v", err)
	}

	var snap bytes.Buffer
	if err := e.IO().Export(&snap); err != nil {
		t.Fatalf("Export: %v", err)
	}
	lsn0, _ := e.Replication().LastCommittedLSN()

	repStore, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
	if err != nil {
		t.Fatalf("replica sharded.New: %v", err)
	}
	rep, err := graph.New(graph.Config{SnowflakeNodeID: 6, Store: repStore, ReadOnlyReplica: true})
	if err != nil {
		t.Fatalf("replica New: %v", err)
	}
	defer rep.Close()
	if err := rep.IO().Import(bytes.NewReader(snap.Bytes()), tkgio.ImportOptions{}); err != nil {
		t.Fatalf("replica Import: %v", err)
	}
	if err := rep.Replication().SetAppliedLSN(lsn0); err != nil {
		t.Fatalf("replica SetAppliedLSN: %v", err)
	}

	// Record the incoming stub (foreign start + rel-ID on slot 11).
	edge := store.ForeignIncomingEdge{
		RelID:      types.RelID(snowflake.ID(700003)),
		TypeName:   "KNOWS",
		StartID:    types.NodeID(snowflake.ID(700001)),
		EndID:      end.ID(),
		Properties: map[string]any{"w": int64(7)},
		FromHash:   "aa11",
		ToHash:     "bb22",
		TxFrom:     1234,
		Version:    0,
		AttestTx:   1,
	}
	if err := e.Rels().RecordForeignIncoming(ctx, edge); err != nil {
		t.Fatalf("RecordForeignIncoming: %v", err)
	}
	if in, _ := e.Rels().Incoming(end.ID(), "KNOWS"); len(in) != 2 {
		t.Fatalf("pre-delete Incoming(END) = %d, want 2 (stub + co-located)", len(in))
	}

	// DELETE the END node. The crux: this must not fail closed on the foreign stub.
	if err := e.Nodes().Delete(ctx, end.ID()); err != nil {
		t.Fatalf("Delete(END) with a foreign-incoming stub: %v", err)
	}
	if _, err := e.Nodes().Get(ctx, end.ID()); err == nil {
		t.Fatal("END node still present after Delete")
	}

	// Exactly one ChangeForeignIncomingDelete record was emitted for the stub.
	var recs []store.ChangeRecord
	var fidDeletes int
	if err := e.Replication().ForEachChange(lsn0, func(rec store.ChangeRecord) bool {
		recs = append(recs, rec)
		if rec.Tag == store.ChangeForeignIncomingDelete {
			fidDeletes++
		}
		return true
	}); err != nil {
		t.Fatalf("ForEachChange: %v", err)
	}
	if fidDeletes != 1 {
		t.Fatalf("emitted %d ChangeForeignIncomingDelete records, want exactly 1", fidDeletes)
	}

	// The replica reaches the same stub-free, node-free state from the feed.
	if _, err := rep.Replication().ApplyChanges(recs); err != nil {
		t.Fatalf("replica ApplyChanges: %v", err)
	}
	if _, err := rep.Nodes().Get(ctx, end.ID()); err == nil {
		t.Fatal("replica END node still present after applying the delete feed")
	}
	if rin, _ := rep.Rels().Incoming(end.ID(), "KNOWS"); len(rin) != 0 {
		t.Fatalf("replica Incoming(END) = %d after delete, want 0 (stub + co-located gone)", len(rin))
	}
	// Idempotent re-apply.
	if _, err := rep.Replication().ApplyChanges(recs); err != nil {
		t.Fatalf("replica re-ApplyChanges (idempotency): %v", err)
	}
}

// TestModelA_TxRollbackRestoresStub (ADR-0010 Model A, increment 4 follow-up)
// proves the tx-rollback path: deleting an END node that carries a foreign-
// incoming stub inside a transaction, then ROLLING BACK, restores BOTH the node
// AND the stub. The stub's rel-ID slot is foreign, so restoreDeletedRelRow must
// route its restore through the foreign-incoming capability (by END slot), not a
// slot-routed PutRelationship that would fail closed.
func TestModelA_TxRollbackRestoresStub(t *testing.T) {
	ctx := context.Background()
	st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
	if err != nil {
		t.Fatalf("sharded.New: %v", err)
	}
	g, err := graph.New(graph.Config{SnowflakeNodeID: 0, Store: st})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	end, err := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"name": "Ada"})
	if err != nil {
		t.Fatalf("add end: %v", err)
	}
	edge := store.ForeignIncomingEdge{
		RelID:      types.RelID(snowflake.ID(700003)),
		TypeName:   "KNOWS",
		StartID:    types.NodeID(snowflake.ID(700001)),
		EndID:      end.ID(),
		Properties: map[string]any{"w": int64(7)},
		FromHash:   "aa11",
		ToHash:     "bb22",
		TxFrom:     1234,
		Version:    0,
		AttestTx:   1,
	}
	if err := g.Rels().RecordForeignIncoming(ctx, edge); err != nil {
		t.Fatalf("RecordForeignIncoming: %v", err)
	}
	if in, _ := g.Rels().Incoming(end.ID(), "KNOWS"); len(in) != 1 {
		t.Fatalf("pre-tx Incoming = %d, want 1 (stub)", len(in))
	}

	// Delete the end node inside a tx, then force ROLLBACK by returning an error.
	forceRollback := errors.New("force rollback")
	rerr := g.Tx().Run(func(tx *graph.GraphTx) error {
		if e := tx.DeleteNode(end.ID()); e != nil {
			return e
		}
		return forceRollback
	})
	if !errors.Is(rerr, forceRollback) {
		t.Fatalf("tx Run err=%v, want forceRollback", rerr)
	}

	// After rollback the end node is back AND the stub is restored via the
	// foreign-incoming capability (byte-identical: same rel-ID and content hash).
	if _, err := g.Nodes().Get(ctx, end.ID()); err != nil {
		t.Fatalf("end node not restored after rollback: %v", err)
	}
	in, err := g.Rels().Incoming(end.ID(), "KNOWS")
	if err != nil {
		t.Fatalf("post-rollback Incoming: %v", err)
	}
	if len(in) != 1 || in[0].ID() != edge.RelID {
		t.Fatalf("stub NOT restored after rollback: got %d rels", len(in))
	}
	if in[0].Integrity().ToNodeHash != "bb22" {
		t.Fatalf("restored stub ToNodeHash = %q, want bb22", in[0].Integrity().ToNodeHash)
	}
}
