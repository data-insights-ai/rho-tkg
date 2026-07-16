package replication_test

import (
	"context"
	"errors"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/replication"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
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
