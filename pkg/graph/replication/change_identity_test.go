package replication_test

import (
	"context"
	"errors"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	adminpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/admin"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/replication"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Ask 4 — DecodeChangeIdentity: extract (kind, Snowflake ID) from a real
// change-log record without the internal wire codec. Driven end-to-end against
// REAL records tailed from a change-log-enabled store (not hand-crafted
// payloads), so the msgpack body keys the decoder reads are the ones the
// encoders actually write.

func TestDecodeChangeIdentity_RealRecords(t *testing.T) {
	g, err := graphpkg.New(graphpkg.Config{Store: memory.New(memory.WithChangeLog())})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	ctx := context.Background()

	n1, err := g.Nodes().Add(ctx, []string{"A"}, map[string]any{"x": int64(1)})
	if err != nil {
		t.Fatalf("add n1: %v", err)
	}
	n2, err := g.Nodes().Add(ctx, []string{"A"}, map[string]any{"x": int64(2)})
	if err != nil {
		t.Fatalf("add n2: %v", err)
	}
	r1, err := g.Rels().AddByID(ctx, "KNOWS", n1.ID(), n2.ID(), map[string]any{"w": int64(1)})
	if err != nil {
		t.Fatalf("add r1: %v", err)
	}
	// Version-history put records for both kinds.
	if _, err := g.Nodes().Update(ctx, n1.ID(), map[string]any{"x": int64(11)}); err != nil {
		t.Fatalf("update n1: %v", err)
	}
	if _, err := g.Rels().Update(ctx, r1.ID(), map[string]any{"w": int64(2)}); err != nil {
		t.Fatalf("update r1: %v", err)
	}
	// Cascade delete: removes n2 and (as tombstones inside the node-delete record)
	// the connected rel r1.
	if err := g.Nodes().Delete(ctx, n2.ID()); err != nil {
		t.Fatalf("delete n2: %v", err)
	}

	feed, err := g.Replication().ChangeFeed(0, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(feed) == 0 {
		t.Fatal("empty change feed")
	}

	known := map[snowflake.ID]replication.EntityKind{
		n1.ID().SnowflakeID(): replication.EntityKindNode,
		n2.ID().SnowflakeID(): replication.EntityKindNode,
		r1.ID().SnowflakeID(): replication.EntityKindRelationship,
	}

	// Every entity record decodes to a KNOWN id whose kind matches both the tag
	// family and the entity's true kind.
	sawNodePut, sawRelPut, sawNodeDelete := false, false, false
	for _, rec := range feed {
		kind, id, err := replication.DecodeChangeIdentity(rec)
		if errors.Is(err, replication.ErrNoEntityIdentity) {
			continue // control record (meta/clear) — none expected here, tolerated
		}
		if err != nil {
			t.Fatalf("DecodeChangeIdentity(%s @LSN %d): %v", rec.Tag, rec.LSN, err)
		}
		wantKind, ok := known[id]
		if !ok {
			t.Fatalf("record %s @LSN %d decoded to unknown id %d", rec.Tag, rec.LSN, id)
		}
		if kind != wantKind {
			t.Fatalf("record %s @LSN %d kind = %s, want %s (id %d)", rec.Tag, rec.LSN, kind, wantKind, id)
		}
		switch rec.Tag {
		case store.ChangeNodePut:
			sawNodePut = true
		case store.ChangeRelPut:
			sawRelPut = true
		case store.ChangeNodeDelete:
			sawNodeDelete = true
			if id != n2.ID().SnowflakeID() {
				t.Fatalf("node-delete decoded to %d, want n2 %d", id, n2.ID().SnowflakeID())
			}
		}
	}
	if !sawNodePut || !sawRelPut || !sawNodeDelete {
		t.Fatalf("missing record kinds: nodePut=%v relPut=%v nodeDelete=%v", sawNodePut, sawRelPut, sawNodeDelete)
	}

	// The first three records are, in LSN order, the two node creates and the rel
	// create — an exact spot-check of the put decode.
	assertIdentity(t, feed[0], replication.EntityKindNode, n1.ID().SnowflakeID())
	assertIdentity(t, feed[1], replication.EntityKindNode, n2.ID().SnowflakeID())
	assertIdentity(t, feed[2], replication.EntityKindRelationship, r1.ID().SnowflakeID())
}

func assertIdentity(t *testing.T, rec store.ChangeRecord, wantKind replication.EntityKind, wantID snowflake.ID) {
	t.Helper()
	kind, id, err := replication.DecodeChangeIdentity(rec)
	if err != nil {
		t.Fatalf("DecodeChangeIdentity(%s): %v", rec.Tag, err)
	}
	if kind != wantKind || id != wantID {
		t.Fatalf("DecodeChangeIdentity(%s) = (%s, %d), want (%s, %d)", rec.Tag, kind, id, wantKind, wantID)
	}
}

// A corrupt/hostile payload fails closed with store.ErrCorruptWire (rule 4 —
// errors.Is at the public boundary), never a panic.
func TestDecodeChangeIdentity_CorruptFailsClosed(t *testing.T) {
	garbage := []byte{0xc1, 0xff, 0xff, 0xff, 0xff} // 0xc1 is msgpack "never used"
	for _, tag := range []store.ChangeTag{
		store.ChangeNodePut, store.ChangeRelPut,
		store.ChangeNodeDelete, store.ChangeRelDelete,
		store.ChangeNodeHistoryVersion, store.ChangeRelHistoryVersion,
		store.ChangeNodeHistoryTruncate, store.ChangeRelHistoryTruncate,
	} {
		rec := store.ChangeRecord{LSN: 1, Tag: tag, Payload: garbage}
		_, _, err := replication.DecodeChangeIdentity(rec)
		if !errors.Is(err, store.ErrCorruptWire) {
			t.Fatalf("tag %s corrupt err = %v, want ErrCorruptWire", tag, err)
		}
	}
}

// The store-global control tags name no single entity.
func TestDecodeChangeIdentity_ControlTags(t *testing.T) {
	for _, tag := range []store.ChangeTag{store.ChangeMeta, store.ChangeClear} {
		_, _, err := replication.DecodeChangeIdentity(store.ChangeRecord{Tag: tag})
		if !errors.Is(err, replication.ErrNoEntityIdentity) {
			t.Fatalf("tag %s err = %v, want ErrNoEntityIdentity", tag, err)
		}
	}
}

// An unrecognized tag fails closed with ErrCorruptWire.
func TestDecodeChangeIdentity_UnknownTag(t *testing.T) {
	_, _, err := replication.DecodeChangeIdentity(store.ChangeRecord{Tag: store.ChangeTag(200)})
	if !errors.Is(err, store.ErrCorruptWire) {
		t.Fatalf("unknown tag err = %v, want ErrCorruptWire", err)
	}
}

// TestDecodeChangeIdentity_ForeignIncomingRecords guards BACKLOG 8a:
// ChangeForeignIncoming and ChangeForeignIncomingDelete (ADR-0010 Model A,
// shipped) must decode as relationship identities, not fall into the
// unknown-tag default and misreport a well-formed record as
// store.ErrCorruptWire. Driven against REAL records from RecordForeignIncoming
// + a subsequent node delete (which cascades a ChangeForeignIncomingDelete).
func TestDecodeChangeIdentity_ForeignIncomingRecords(t *testing.T) {
	ctx := context.Background()

	eStore, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2, ChangeLog: true})
	if err != nil {
		t.Fatalf("sharded.New: %v", err)
	}
	e, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 0, Store: eStore})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer e.Close()

	end, err := e.Nodes().Add(ctx, []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("Add end: %v", err)
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
	if err := e.Rels().RecordForeignIncoming(ctx, edge); err != nil {
		t.Fatalf("RecordForeignIncoming: %v", err)
	}
	if err := e.Nodes().Delete(ctx, end.ID()); err != nil {
		t.Fatalf("Delete(end): %v", err)
	}

	feed, err := e.Replication().ChangeFeed(0, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}

	sawForeignIncoming, sawForeignIncomingDelete := false, false
	for _, rec := range feed {
		switch rec.Tag {
		case store.ChangeForeignIncoming:
			sawForeignIncoming = true
			kind, id, err := replication.DecodeChangeIdentity(rec)
			if err != nil {
				t.Fatalf("DecodeChangeIdentity(ChangeForeignIncoming): %v", err)
			}
			if kind != replication.EntityKindRelationship {
				t.Fatalf("ChangeForeignIncoming kind = %s, want Relationship", kind)
			}
			if id != edge.RelID.SnowflakeID() {
				t.Fatalf("ChangeForeignIncoming id = %d, want %d", id, edge.RelID.SnowflakeID())
			}
			op, err := replication.ChangeOpOf(rec)
			if err != nil || op != replication.ChangeOpUpsert {
				t.Fatalf("ChangeOpOf(ChangeForeignIncoming) = (%s, %v), want (Upsert, nil)", op, err)
			}
		case store.ChangeForeignIncomingDelete:
			sawForeignIncomingDelete = true
			kind, id, err := replication.DecodeChangeIdentity(rec)
			if err != nil {
				t.Fatalf("DecodeChangeIdentity(ChangeForeignIncomingDelete): %v", err)
			}
			if kind != replication.EntityKindRelationship {
				t.Fatalf("ChangeForeignIncomingDelete kind = %s, want Relationship", kind)
			}
			if id != edge.RelID.SnowflakeID() {
				t.Fatalf("ChangeForeignIncomingDelete id = %d, want %d", id, edge.RelID.SnowflakeID())
			}
			op, err := replication.ChangeOpOf(rec)
			if err != nil || op != replication.ChangeOpDelete {
				t.Fatalf("ChangeOpOf(ChangeForeignIncomingDelete) = (%s, %v), want (Delete, nil)", op, err)
			}
		}
	}
	if !sawForeignIncoming || !sawForeignIncomingDelete {
		t.Fatalf("missing record kinds: foreignIncoming=%v foreignIncomingDelete=%v", sawForeignIncoming, sawForeignIncomingDelete)
	}
}

// TestDecodeChangeIdentity_RangePurgeRecord guards BACKLOG 8a: ChangeRangePurge
// (ADR-0008 R3, shipped) must classify as a no-entity-identity control record
// (like ChangeMeta/ChangeClear), not the unknown-tag default
// store.ErrCorruptWire — it names a predicate, not one entity.
func TestDecodeChangeIdentity_RangePurgeRecord(t *testing.T) {
	ctx := context.Background()
	g, err := graphpkg.New(graphpkg.Config{Store: memory.New(memory.WithChangeLog()), AllowRetentionPurge: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	if _, err := g.Nodes().Add(ctx, []string{"Event"}, nil); err != nil {
		t.Fatalf("add event: %v", err)
	}
	if _, err := g.Admin().PurgeExpiredNodes(ctx, adminpkg.PurgePolicy{Label: "Event", Mode: adminpkg.PurgeByAge, Before: types.Instant(1 << 50)}); err != nil {
		t.Fatalf("PurgeExpiredNodes: %v", err)
	}

	feed, err := g.Replication().ChangeFeed(0, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	sawRangePurge := false
	for _, rec := range feed {
		if rec.Tag != store.ChangeRangePurge {
			continue
		}
		sawRangePurge = true
		if _, _, err := replication.DecodeChangeIdentity(rec); !errors.Is(err, replication.ErrNoEntityIdentity) {
			t.Fatalf("DecodeChangeIdentity(ChangeRangePurge) err = %v, want ErrNoEntityIdentity", err)
		}
		if _, err := replication.ChangeOpOf(rec); !errors.Is(err, replication.ErrNoEntityIdentity) {
			t.Fatalf("ChangeOpOf(ChangeRangePurge) err = %v, want ErrNoEntityIdentity", err)
		}
	}
	if !sawRangePurge {
		t.Fatal("no ChangeRangePurge record found in feed — test setup broken")
	}
}

// Ask 4 (op) — ChangeOpOf classifies every real record's tag into a normalized
// mutation op, and every entity op pairs with a decodable (kind, ID). Driven
// against the SAME real feed shape as the identity test so tag→op stays aligned
// with what the encoders actually emit.
func TestChangeOpOf_RealRecords(t *testing.T) {
	g, err := graphpkg.New(graphpkg.Config{Store: memory.New(memory.WithChangeLog())})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	ctx := context.Background()

	n1, err := g.Nodes().Add(ctx, []string{"A"}, map[string]any{"x": int64(1)})
	if err != nil {
		t.Fatalf("add n1: %v", err)
	}
	n2, err := g.Nodes().Add(ctx, []string{"A"}, map[string]any{"x": int64(2)})
	if err != nil {
		t.Fatalf("add n2: %v", err)
	}
	if _, err := g.Nodes().Update(ctx, n1.ID(), map[string]any{"x": int64(11)}); err != nil {
		t.Fatalf("update n1: %v", err)
	}
	if err := g.Nodes().Delete(ctx, n2.ID()); err != nil {
		t.Fatalf("delete n2: %v", err)
	}

	feed, err := g.Replication().ChangeFeed(0, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(feed) == 0 {
		t.Fatal("empty change feed")
	}

	sawUpsert, sawDelete := false, false
	for _, rec := range feed {
		op, err := replication.ChangeOpOf(rec)
		if errors.Is(err, replication.ErrNoEntityIdentity) {
			continue // control record — none expected here, tolerated
		}
		if err != nil {
			t.Fatalf("ChangeOpOf(%s @LSN %d): %v", rec.Tag, rec.LSN, err)
		}
		// op must agree with the tag family.
		switch rec.Tag {
		case store.ChangeNodePut, store.ChangeRelPut:
			if op != replication.ChangeOpUpsert {
				t.Fatalf("put record %s got op %s, want Upsert", rec.Tag, op)
			}
			sawUpsert = true
		case store.ChangeNodeDelete, store.ChangeRelDelete:
			if op != replication.ChangeOpDelete {
				t.Fatalf("delete record %s got op %s, want Delete", rec.Tag, op)
			}
			sawDelete = true
		}
		// Every entity op pairs with a decodable identity — the (kind, ID, op)
		// triple a mirror consumes.
		if _, _, idErr := replication.DecodeChangeIdentity(rec); idErr != nil {
			t.Fatalf("op %s record %s has no identity: %v", op, rec.Tag, idErr)
		}
	}
	if !sawUpsert || !sawDelete {
		t.Fatalf("missing ops: upsert=%v delete=%v", sawUpsert, sawDelete)
	}
}

// ChangeOpOf is a pure tag classifier: control tags name no op, unknown tags fail
// closed, and — unlike DecodeChangeIdentity — a corrupt PAYLOAD is irrelevant
// because the body is never decoded.
func TestChangeOpOf_ControlUnknownAndCorruptPayload(t *testing.T) {
	for _, tag := range []store.ChangeTag{store.ChangeMeta, store.ChangeClear} {
		if _, err := replication.ChangeOpOf(store.ChangeRecord{Tag: tag}); !errors.Is(err, replication.ErrNoEntityIdentity) {
			t.Fatalf("control tag %s err = %v, want ErrNoEntityIdentity", tag, err)
		}
	}
	if _, err := replication.ChangeOpOf(store.ChangeRecord{Tag: store.ChangeTag(200)}); !errors.Is(err, store.ErrCorruptWire) {
		t.Fatalf("unknown tag err = %v, want ErrCorruptWire", err)
	}
	// A put tag with a garbage payload still classifies (op is tag-only).
	op, err := replication.ChangeOpOf(store.ChangeRecord{Tag: store.ChangeNodePut, Payload: []byte{0xc1}})
	if err != nil || op != replication.ChangeOpUpsert {
		t.Fatalf("put with garbage payload = (%s, %v), want (Upsert, nil)", op, err)
	}
}

func TestChangeOp_String(t *testing.T) {
	cases := map[replication.ChangeOp]string{
		replication.ChangeOpUpsert:          "Upsert",
		replication.ChangeOpDelete:          "Delete",
		replication.ChangeOpHistoryVersion:  "HistoryVersion",
		replication.ChangeOpHistoryTruncate: "HistoryTruncate",
		replication.ChangeOpUnknown:         "Unknown",
		replication.ChangeOp(99):            "Unknown",
	}
	for o, want := range cases {
		if got := o.String(); got != want {
			t.Fatalf("ChangeOp(%d).String() = %q, want %q", o, got, want)
		}
	}
}

func TestEntityKind_String(t *testing.T) {
	cases := map[replication.EntityKind]string{
		replication.EntityKindNode:         "Node",
		replication.EntityKindRelationship: "Relationship",
		replication.EntityKindUnknown:      "Unknown",
		replication.EntityKind(99):         "Unknown",
	}
	for k, want := range cases {
		if got := k.String(); got != want {
			t.Fatalf("EntityKind(%d).String() = %q, want %q", k, got, want)
		}
	}
}
