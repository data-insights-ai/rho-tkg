package core

import (
	"context"
	"errors"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BACKLOG 9h: RecordForeignIncoming (ADR-0010 §3.3 — the directly callable
// public door for recording a cross-machine incoming half-edge stub) skipped
// c.validateName/c.validateProperties — the ValidationLimits checks every
// OTHER create door funnels through via prepareRelCreate. edge.Validate()
// only checks STRUCTURAL well-formedness (nonzero IDs, nonempty type name,
// nonzero attest time), never this graph instance's configured
// MaxNameLength/MaxPropertiesPerEntity/etc. Unlike apply_record.go's
// replica-apply path (which has a documented exemption because it
// reproduces a PRIMARY's already-validated state verbatim), RecordForeignIncoming
// is a normal public door a caller can invoke directly — so a cross-machine
// stub from a machine with looser local limits could exceed THIS machine's
// caps unrejected.

func newForeignIncomingTestGraph(t *testing.T, validation ValidationLimits) (*Core, types.NodeID) {
	t.Helper()
	st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
	if err != nil {
		t.Fatalf("sharded.New: %v", err)
	}
	g, err := New(Config{SnowflakeNodeID: 0, Store: st, Validation: validation})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { g.Close() })

	end, err := g.Nodes.Add(context.Background(), []string{"P"}, nil)
	if err != nil {
		t.Fatalf("Add end: %v", err)
	}
	return g, end.ID()
}

// baseForeignEdge returns a structurally-valid edge (passes edge.Validate())
// targeting the given local end node, with a foreign start ID on an
// unclaimed slot (mirrors the literal IDs pkg/graph's
// TestModelA_ForeignIncomingConvergence already uses successfully).
func baseForeignEdge(endID types.NodeID) storepkg.ForeignIncomingEdge {
	return storepkg.ForeignIncomingEdge{
		RelID:      types.RelID(snowflake.ID(700003)),
		TypeName:   "KNOWS",
		StartID:    types.NodeID(snowflake.ID(700001)),
		EndID:      endID,
		Properties: map[string]any{"w": int64(7)},
		FromHash:   "aa11",
		ToHash:     "bb22",
		TxFrom:     1234,
		Version:    0,
		AttestTx:   1,
	}
}

func TestRecordForeignIncoming_RejectsOversizedTypeName(t *testing.T) {
	g, endID := newForeignIncomingTestGraph(t, ValidationLimits{MaxNameLength: 5})
	edge := baseForeignEdge(endID)
	edge.TypeName = "MUCH_LONGER_THAN_FIVE_CHARS"

	if err := g.Rels.RecordForeignIncoming(context.Background(), edge); !errors.Is(err, ErrNameTooLong) {
		t.Fatalf("RecordForeignIncoming with oversized type name = %v, want ErrNameTooLong — BACKLOG 9h regression", err)
	}
	// typeName="" bypasses Incoming's own name-length validation, so this
	// checks for ANY recorded incoming rel regardless of type.
	if in, err := g.Rels.Incoming(endID, ""); err != nil || len(in) != 0 {
		t.Fatalf("stub was recorded despite oversized type name: in=%v err=%v", in, err)
	}
}

func TestRecordForeignIncoming_RejectsTooManyProperties(t *testing.T) {
	g, endID := newForeignIncomingTestGraph(t, ValidationLimits{MaxPropertiesPerEntity: 1})
	edge := baseForeignEdge(endID)
	edge.Properties = map[string]any{"a": int64(1), "b": int64(2)}

	if err := g.Rels.RecordForeignIncoming(context.Background(), edge); !errors.Is(err, ErrTooManyProperties) {
		t.Fatalf("RecordForeignIncoming with too many properties = %v, want ErrTooManyProperties — BACKLOG 9h regression", err)
	}
	if in, err := g.Rels.Incoming(endID, "KNOWS"); err != nil || len(in) != 0 {
		t.Fatalf("stub was recorded despite exceeding MaxPropertiesPerEntity: in=%v err=%v", in, err)
	}
}

// TestRecordForeignIncoming_SucceedsWithinLimits is the non-regression
// counterpart: an edge within the configured limits must still succeed.
func TestRecordForeignIncoming_SucceedsWithinLimits(t *testing.T) {
	g, endID := newForeignIncomingTestGraph(t, ValidationLimits{})
	edge := baseForeignEdge(endID)

	if err := g.Rels.RecordForeignIncoming(context.Background(), edge); err != nil {
		t.Fatalf("RecordForeignIncoming within limits: %v", err)
	}
	in, err := g.Rels.Incoming(endID, "KNOWS")
	if err != nil {
		t.Fatalf("Incoming: %v", err)
	}
	if len(in) != 1 || in[0].ID() != edge.RelID {
		t.Fatalf("Incoming = %v, want exactly [%v]", in, edge.RelID)
	}
}
