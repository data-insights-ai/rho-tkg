package sharded_test

import (
	"context"
	"errors"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/replication"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	temporalpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// foreignEndTestGraph builds a graph over a 2-slot sharded store (slots 0,1
// local — SnowflakeNodeID 0 mints nodes on slot 0, rels on slot 1). Any node ID
// whose slot is >= 2 is therefore FOREIGN to this store, letting us simulate a
// cross-machine endpoint in-process (ADR-0010 §6): no second machine is needed,
// only an ID outside the claimed slot range plus a caller-attested descriptor.
func foreignEndTestGraph(t *testing.T) *graph.Graph {
	t.Helper()
	st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
	if err != nil {
		t.Fatalf("sharded.New: %v", err)
	}
	g, err := graph.New(graph.Config{Store: st, SnowflakeNodeID: 0})
	if err != nil {
		t.Fatalf("graph.New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g
}

// foreignNodeID returns a node ID on slot 11 (700001 decodes to slot 11), which
// is outside the 2-slot store's claimed range {0,1} — i.e. FOREIGN.
func foreignNodeID() types.NodeID { return types.NodeID(snowflake.ID(700001)) }

func TestForeignEndCreate_HappyPath(t *testing.T) {
	t.Parallel()
	g := foreignEndTestGraph(t)
	ctx := context.Background()

	start, err := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"name": "Ada"})
	if err != nil {
		t.Fatalf("Add start: %v", err)
	}
	startHash := start.Integrity().Hash
	if startHash == "" {
		t.Fatal("start node has empty integrity hash")
	}

	const attestedToHash = "ffeeddccbbaa99887766554433221100"
	fe := storepkg.ForeignEndpoint{
		NodeID:   foreignNodeID(),
		Hash:     attestedToHash,
		AttestTx: types.Instant(1_700_000_000_000),
	}

	rel, err := g.Rels().AddByIDForeignEnd(ctx, "KNOWS", start.ID(), fe, map[string]any{"since": int64(2020)})
	if err != nil {
		t.Fatalf("AddByIDForeignEnd: %v", err)
	}
	if rel == nil {
		t.Fatal("AddByIDForeignEnd returned nil rel")
	}

	// Endpoints: local start, foreign end — recorded verbatim.
	if rel.StartNodeID() != start.ID() {
		t.Fatalf("start = %d, want %d", rel.StartNodeID().SnowflakeID(), start.ID().SnowflakeID())
	}
	if rel.EndNodeID() != fe.NodeID {
		t.Fatalf("end = %d, want foreign %d", rel.EndNodeID().SnowflakeID(), fe.NodeID.SnowflakeID())
	}

	// tkg_to_hash is the ATTESTED value (not fetched locally — the foreign node
	// does not exist on this machine); tkg_from_hash is the LOCAL start's hash.
	ig := rel.Integrity()
	if ig.ToNodeHash != attestedToHash {
		t.Fatalf("ToNodeHash = %q, want attested %q", ig.ToNodeHash, attestedToHash)
	}
	if ig.FromNodeHash != startHash {
		t.Fatalf("FromNodeHash = %q, want local start hash %q", ig.FromNodeHash, startHash)
	}

	// The rel persisted and reads back with the same attested to-hash.
	got, err := g.Rels().Get(ctx, rel.ID())
	if err != nil {
		t.Fatalf("Get rel: %v", err)
	}
	if got.Integrity().ToNodeHash != attestedToHash {
		t.Fatalf("persisted ToNodeHash = %q, want %q", got.Integrity().ToNodeHash, attestedToHash)
	}

	// Verify*Chain passes: the attested to-hash is NOT part of the content hash,
	// so a foreign-attested endpoint does not weaken tamper-evidence.
	ok, err := g.Hash().VerifyRelChain(rel.ID())
	if err != nil {
		t.Fatalf("VerifyRelChain: %v", err)
	}
	if !ok {
		t.Fatal("VerifyRelChain = false, want true for a cross-machine edge")
	}

	// Outgoing adjacency from the local start finds the edge.
	out, err := g.Rels().Outgoing(start.ID(), "")
	if err != nil {
		t.Fatalf("Outgoing: %v", err)
	}
	if len(out) != 1 || out[0].ID() != rel.ID() {
		t.Fatalf("Outgoing = %d rels, want 1 (the cross-machine KNOWS)", len(out))
	}
}

// TestForeignEndCreate_PlainDoorFailsClosed proves the foreign-end door is the
// ONLY door that accepts a foreign endpoint: the ordinary AddByID still fails
// closed with ErrSlotNotLocal for the same foreign end (no silent widening).
func TestForeignEndCreate_PlainDoorFailsClosed(t *testing.T) {
	t.Parallel()
	g := foreignEndTestGraph(t)
	ctx := context.Background()

	start, err := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"name": "Ada"})
	if err != nil {
		t.Fatalf("Add start: %v", err)
	}

	_, err = g.Rels().AddByID(ctx, "KNOWS", start.ID(), foreignNodeID(), nil)
	if err == nil {
		t.Fatal("plain AddByID with a foreign end succeeded — want fail-closed")
	}
	if !errors.Is(err, sharded.ErrSlotNotLocal) {
		t.Fatalf("plain AddByID error = %v, want ErrSlotNotLocal", err)
	}
}

// TestForeignEndCreate_LocalEndMisuse: routing the foreign-end door at a node
// that is actually LOCAL is a misuse the store rejects (it would skip a local
// existence check it can and must perform).
func TestForeignEndCreate_LocalEndMisuse(t *testing.T) {
	t.Parallel()
	g := foreignEndTestGraph(t)
	ctx := context.Background()

	start, err := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"name": "Ada"})
	if err != nil {
		t.Fatalf("Add start: %v", err)
	}
	localEnd, err := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"name": "Bob"})
	if err != nil {
		t.Fatalf("Add localEnd: %v", err)
	}

	fe := storepkg.ForeignEndpoint{NodeID: localEnd.ID(), Hash: "deadbeef", AttestTx: 1}
	_, err = g.Rels().AddByIDForeignEnd(ctx, "KNOWS", start.ID(), fe, nil)
	if err == nil {
		t.Fatal("foreign-end door with a LOCAL end succeeded — want ErrForeignEndpointLocal")
	}
	if !errors.Is(err, sharded.ErrForeignEndpointLocal) {
		t.Fatalf("error = %v, want ErrForeignEndpointLocal", err)
	}
}

func TestForeignEndCreate_InvalidDescriptor(t *testing.T) {
	t.Parallel()
	g := foreignEndTestGraph(t)
	ctx := context.Background()

	start, err := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"name": "Ada"})
	if err != nil {
		t.Fatalf("Add start: %v", err)
	}

	cases := map[string]storepkg.ForeignEndpoint{
		"empty hash":     {NodeID: foreignNodeID(), Hash: "", AttestTx: 1},
		"zero attest tx": {NodeID: foreignNodeID(), Hash: "h", AttestTx: 0},
		"zero node id":   {NodeID: types.NodeID(0), Hash: "h", AttestTx: 1},
	}
	for name, fe := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := g.Rels().AddByIDForeignEnd(ctx, "KNOWS", start.ID(), fe, nil)
			if !errors.Is(err, graph.ErrInvalidForeignEndpoint) {
				t.Fatalf("error = %v, want ErrInvalidForeignEndpoint", err)
			}
		})
	}
}

// TestForeignEndCreate_EmitsReplicableRecord proves a cross-machine edge emits
// one co-committed change-log record identifying the relationship — the wire
// substrate a replica of this machine tails to reproduce the edge (with its
// attested to-hash, an ordinary RelWire field) verbatim.
func TestForeignEndCreate_EmitsReplicableRecord(t *testing.T) {
	t.Parallel()
	st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2, ChangeLog: true})
	if err != nil {
		t.Fatalf("sharded.New: %v", err)
	}
	g, err := graph.New(graph.Config{Store: st, SnowflakeNodeID: 0})
	if err != nil {
		t.Fatalf("graph.New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	ctx := context.Background()

	start, err := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"name": "Ada"})
	if err != nil {
		t.Fatalf("Add start: %v", err)
	}

	lsn0, err := g.Replication().LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	}

	fe := storepkg.ForeignEndpoint{NodeID: foreignNodeID(), Hash: "abc123", AttestTx: 1}
	rel, err := g.Rels().AddByIDForeignEnd(ctx, "KNOWS", start.ID(), fe, nil)
	if err != nil {
		t.Fatalf("AddByIDForeignEnd: %v", err)
	}

	var relRecords int
	if err := g.Replication().ForEachChange(lsn0, func(rec storepkg.ChangeRecord) bool {
		kind, id, derr := replication.DecodeChangeIdentity(rec)
		if derr != nil {
			return true // skip records without an entity identity
		}
		if kind == replication.EntityKindRelationship && types.RelID(id) == rel.ID() {
			relRecords++
		}
		return true
	}); err != nil {
		t.Fatalf("ForEachChange: %v", err)
	}
	if relRecords != 1 {
		t.Fatalf("change records for the cross-machine rel = %d, want exactly 1", relRecords)
	}
}

// TestForeignEndCreate_MissingLocalStart: the LOCAL start must still exist — a
// foreign-end create attests only the FOREIGN end, not the local start.
func TestForeignEndCreate_MissingLocalStart(t *testing.T) {
	t.Parallel()
	g := foreignEndTestGraph(t)
	ctx := context.Background()

	// A local-slot (slot 0) node ID that was never created.
	phantomStart := types.NodeID(snowflake.ID((1 << 15) | 1))
	fe := storepkg.ForeignEndpoint{NodeID: foreignNodeID(), Hash: "h", AttestTx: 1}
	_, err := g.Rels().AddByIDForeignEnd(ctx, "KNOWS", phantomStart, fe, nil)
	if !errors.Is(err, graph.ErrNodeNotFound) {
		t.Fatalf("error = %v, want ErrNodeNotFound for a missing local start", err)
	}
}

// TestForeignEndCreate_ConstraintsFailClosed: temporal constraints need the
// live foreign end node (unavailable locally), so a foreign-end create fails
// closed rather than silently skipping the check (ADR-0010 §4.1).
func TestForeignEndCreate_ConstraintsFailClosed(t *testing.T) {
	t.Parallel()
	g := foreignEndTestGraph(t)
	ctx := context.Background()

	if err := g.Constraints().Add(temporalpkg.TemporalConstraint{Kind: temporalpkg.ConstraintRelWithinEndpoints}); err != nil {
		t.Fatalf("Constraints().Add: %v", err)
	}

	start, err := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"name": "Ada"})
	if err != nil {
		t.Fatalf("Add start: %v", err)
	}
	fe := storepkg.ForeignEndpoint{NodeID: foreignNodeID(), Hash: "h", AttestTx: 1}
	_, err = g.Rels().AddByIDForeignEnd(ctx, "KNOWS", start.ID(), fe, nil)
	if !errors.Is(err, graph.ErrForeignEndpointConstraint) {
		t.Fatalf("error = %v, want ErrForeignEndpointConstraint", err)
	}
}

// TestForeignEndCreate_UnsupportedStore: a non-partitioned (single-machine)
// store has no foreign partition, so the door fails closed.
func TestForeignEndCreate_UnsupportedStore(t *testing.T) {
	t.Parallel()
	g, err := graph.New(graph.Config{BadgerInMemory: true, SnowflakeNodeID: 0})
	if err != nil {
		t.Fatalf("graph.New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	ctx := context.Background()

	start, err := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"name": "Ada"})
	if err != nil {
		t.Fatalf("Add start: %v", err)
	}
	fe := storepkg.ForeignEndpoint{NodeID: foreignNodeID(), Hash: "h", AttestTx: 1}
	_, err = g.Rels().AddByIDForeignEnd(ctx, "KNOWS", start.ID(), fe, nil)
	if !errors.Is(err, graph.ErrForeignEndpointUnsupported) {
		t.Fatalf("error = %v, want ErrForeignEndpointUnsupported", err)
	}
}
