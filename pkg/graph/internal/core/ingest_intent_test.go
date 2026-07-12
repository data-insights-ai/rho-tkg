package core

import (
	"errors"
	"reflect"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/integrity"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// preparedNode builds a single pendingNode via the batch prepare path (the same
// prepare a producer session runs on its caller thread).
func preparedNode(t *testing.T, c *Core, labels []string, props map[string]any) pendingNode {
	t.Helper()
	b, err := NewBatchBuilder(c)
	if err != nil {
		t.Fatalf("NewBatchBuilder: %v", err)
	}
	if _, err := b.AddNode(labels, props); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	return b.nodes[0]
}

func preparedRel(t *testing.T, c *Core, typeName string, start, end *types.Node, props map[string]any) pendingRel {
	t.Helper()
	b, err := NewBatchBuilder(c)
	if err != nil {
		t.Fatalf("NewBatchBuilder: %v", err)
	}
	if _, err := b.AddRelationship(typeName, start, end, props); err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	return b.rels[0]
}

// TestIntentHashPrecomputeEquivalence: the content hash the ingest prepare path
// carries (PrecomputeNodeHashSuffixChecked → ComputeNodeHashFromSuffix) equals
// the standalone door's hash (ComputeNodeHashChecked) for identical inputs.
// This is the §4.4 enabler — the hash keys on label STRINGS, so the applier's
// token re-stamp does not invalidate it.
func TestIntentHashPrecomputeEquivalence(t *testing.T) {
	t.Parallel()
	c := newTestGraph(t)

	labels := []string{"Person", "Employee"}
	props := map[string]any{"name": "Ada", "age": int64(37), "score": 1.5}
	pn := preparedNode(t, c, labels, props)

	carried := pn.node.Integrity().Hash
	standalone, err := integrity.ComputeNodeHashChecked(pn.node, pn.labels)
	if err != nil {
		t.Fatalf("ComputeNodeHashChecked: %v", err)
	}
	if carried != standalone {
		t.Fatalf("precomputed hash %q != standalone-door hash %q", carried, standalone)
	}
	if carried == "" {
		t.Fatalf("empty hash")
	}
}

// TestIntentCodecRoundTripNode: a node-create intent survives Encode→Decode with
// its ordering header, labels, and full content payload (incl. the precomputed
// hash) intact.
func TestIntentCodecRoundTripNode(t *testing.T) {
	t.Parallel()
	c := newTestGraph(t)

	labels := []string{"Account", "External"}
	props := map[string]any{"balance": 42.0, "iban": "DE00", "flag": true}
	pn := preparedNode(t, c, labels, props)

	hdr := intentHeader{epoch: 7, lane: 0, seq: 99}
	ir, err := nodeCreateIntentFrom(hdr, pn)
	if err != nil {
		t.Fatalf("nodeCreateIntentFrom: %v", err)
	}
	blob, err := EncodeIntent(ir)
	if err != nil {
		t.Fatalf("EncodeIntent: %v", err)
	}
	got, err := DecodeIntent(blob)
	if err != nil {
		t.Fatalf("DecodeIntent: %v", err)
	}

	if got.Epoch != 7 || got.Lane != 0 || got.Seq != 99 {
		t.Fatalf("header lost: %+v", got)
	}
	if got.Kind != IntentNodeCreate {
		t.Fatalf("kind = %d, want NodeCreate", got.Kind)
	}
	if got.Half != HalfWhole {
		t.Fatalf("half = %d, want Whole", got.Half)
	}
	if !reflect.DeepEqual(got.Labels, ir.Labels) {
		t.Fatalf("labels: got %v want %v", got.Labels, ir.Labels)
	}
	if got.nodeWire.ID != ir.nodeWire.ID {
		t.Fatalf("id: got %d want %d", got.nodeWire.ID, ir.nodeWire.ID)
	}
	if got.nodeWire.Hash != ir.nodeWire.Hash {
		t.Fatalf("hash: got %q want %q", got.nodeWire.Hash, ir.nodeWire.Hash)
	}
	if len(got.nodeWire.Properties) != len(ir.nodeWire.Properties) {
		t.Fatalf("props len: got %d want %d", len(got.nodeWire.Properties), len(ir.nodeWire.Properties))
	}
}

// TestIntentCodecRoundTripRel: a rel-create intent survives Encode→Decode with
// its type name, endpoints, edge id, and content hash intact.
func TestIntentCodecRoundTripRel(t *testing.T) {
	t.Parallel()
	c := newTestGraph(t)

	a := preparedNode(t, c, []string{"N"}, nil)
	bnode := preparedNode(t, c, []string{"N"}, nil)
	pr := preparedRel(t, c, "KNOWS", a.node, bnode.node, map[string]any{"since": int64(2020)})

	hdr := intentHeader{epoch: 1, lane: 0, seq: 5}
	ir, err := relCreateIntentFrom(hdr, pr)
	if err != nil {
		t.Fatalf("relCreateIntentFrom: %v", err)
	}
	blob, err := EncodeIntent(ir)
	if err != nil {
		t.Fatalf("EncodeIntent: %v", err)
	}
	got, err := DecodeIntent(blob)
	if err != nil {
		t.Fatalf("DecodeIntent: %v", err)
	}
	if got.Kind != IntentRelCreate {
		t.Fatalf("kind = %d, want RelCreate", got.Kind)
	}
	if got.TypeName != "KNOWS" {
		t.Fatalf("type: got %q", got.TypeName)
	}
	if got.EdgeID != pr.rel.ID() {
		t.Fatalf("edge id: got %d want %d", got.EdgeID, pr.rel.ID())
	}
	if got.relWire.StartID != int64(a.node.ID().SnowflakeID()) || got.relWire.EndID != int64(bnode.node.ID().SnowflakeID()) {
		t.Fatalf("endpoints lost: %+v", got.relWire)
	}
	if got.relWire.Hash != ir.relWire.Hash {
		t.Fatalf("hash: got %q want %q", got.relWire.Hash, ir.relWire.Hash)
	}
}

// TestDecodeIntentCorruptFailsClosed: a hostile/garbage buffer fails closed with
// store.ErrCorruptWire at the trust boundary, never panics.
func TestDecodeIntentCorruptFailsClosed(t *testing.T) {
	t.Parallel()
	_, err := DecodeIntent([]byte{0xff, 0xff, 0xff, 0xff, 0x01, 0x02})
	if err == nil {
		t.Fatalf("expected decode error on garbage")
	}
	if !errors.Is(err, storepkg.ErrCorruptWire) {
		t.Fatalf("want ErrCorruptWire, got %v", err)
	}
}

// TestDecodeIntentUnknownKind: an envelope with an unknown kind fails closed.
func TestDecodeIntentUnknownKind(t *testing.T) {
	t.Parallel()
	c := newTestGraph(t)
	pn := preparedNode(t, c, []string{"N"}, nil)
	ir, err := nodeCreateIntentFrom(intentHeader{seq: 1}, pn)
	if err != nil {
		t.Fatalf("nodeCreateIntentFrom: %v", err)
	}
	ir.Kind = 250 // unknown
	// Re-encode manually through the envelope with the unknown kind but a
	// payload, then decode.
	blob, err := EncodeIntent(IntentRecord{Epoch: 1, Kind: 250})
	if err != nil {
		t.Fatalf("EncodeIntent: %v", err)
	}
	if _, err := DecodeIntent(blob); !errors.Is(err, storepkg.ErrCorruptWire) {
		t.Fatalf("want ErrCorruptWire for unknown kind, got %v", err)
	}
}

// TestIntentToPendingNodeRoundTrip: a decoded node-create intent reconstructs a
// pendingNode whose node carries the same id/hash, ready for the applier.
func TestIntentToPendingNodeRoundTrip(t *testing.T) {
	t.Parallel()
	c := newTestGraph(t)

	pn := preparedNode(t, c, []string{"Widget"}, map[string]any{"k": int64(1)})
	ir, err := nodeCreateIntentFrom(intentHeader{seq: 3}, pn)
	if err != nil {
		t.Fatalf("nodeCreateIntentFrom: %v", err)
	}
	blob, err := EncodeIntent(ir)
	if err != nil {
		t.Fatalf("EncodeIntent: %v", err)
	}
	got, err := DecodeIntent(blob)
	if err != nil {
		t.Fatalf("DecodeIntent: %v", err)
	}
	pn2, err := got.toPendingNode(c)
	if err != nil {
		t.Fatalf("toPendingNode: %v", err)
	}
	if pn2.node.ID() != pn.node.ID() {
		t.Fatalf("id: got %d want %d", pn2.node.ID(), pn.node.ID())
	}
	if pn2.node.Integrity().Hash != pn.node.Integrity().Hash {
		t.Fatalf("hash mismatch after round-trip")
	}
	if pn2.temporal.TxFrom != 0 {
		t.Fatalf("TxFrom must be applier-owned (0), got %d", pn2.temporal.TxFrom)
	}
}
