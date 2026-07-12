package core

import (
	"bytes"
	"context"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestIntentWireBodyIsZeroTailedV2: the prepare-side pre-encoded wireBody a
// node-create intent carries is a v2 buffer with a ZERO transaction-time tail —
// exactly what the applier patches (ADR-0006 §4.5).
func TestIntentWireBodyIsZeroTailedV2(t *testing.T) {
	t.Parallel()
	c := newTestGraph(t)

	pn := preparedNode(t, c, []string{"Person"}, map[string]any{"name": "Ada", "age": int64(37)})
	ir, err := nodeCreateIntentFrom(intentHeader{seq: 1}, pn)
	if err != nil {
		t.Fatalf("nodeCreateIntentFrom: %v", err)
	}
	if len(ir.wireBody) == 0 {
		t.Fatal("node-create intent carries no pre-encoded wireBody")
	}
	if !storeutil.HasWireTemporalTail(ir.wireBody) {
		t.Fatalf("wireBody has no v2 fixed tail: % x", ir.wireBody)
	}
	var w storeutil.NodeWire
	if err := storeutil.SafeUnmarshal(ir.wireBody, &w); err != nil {
		t.Fatalf("decode wireBody: %v", err)
	}
	if w.FormatVersion != storeutil.CurrentWireFormatVersion {
		t.Fatalf("wireBody fv = %d, want %d", w.FormatVersion, storeutil.CurrentWireFormatVersion)
	}
	if w.TxFrom != 0 || w.TxTo != 0 {
		t.Fatalf("wireBody tail not zero: tf=%d tt=%d", w.TxFrom, w.TxTo)
	}
}

// TestIntentWireBodyPatchEquivalenceNode: the CROWN property at the intent
// level. Patching the carried zero-tail wireBody with an applier TxFrom yields
// byte-for-byte the same wire as encoding the payload with TxFrom set — so the
// apply-side consumption path (Stage C) is correct by construction: it may
// consume the patched buffer OR fall back to encoding, indistinguishably.
func TestIntentWireBodyPatchEquivalenceNode(t *testing.T) {
	t.Parallel()
	c := newTestGraph(t)

	pn := preparedNode(t, c, []string{"Person", "Employee"}, map[string]any{"name": "Ada", "score": 1.5})
	ir, err := nodeCreateIntentFrom(intentHeader{seq: 1}, pn)
	if err != nil {
		t.Fatalf("nodeCreateIntentFrom: %v", err)
	}

	// A create stamps only TxFrom (TxTo stays the queued 0), mirroring
	// batch_execute.go's pn.temporal.TxFrom = ts; SetTemporal(pn.temporal).
	const txFrom, txTo int64 = 1_700_000_000_123, 0
	patched := append([]byte(nil), ir.wireBody...)
	if err := storeutil.PatchWireTemporalTail(patched, txFrom, txTo); err != nil {
		t.Fatalf("patch: %v", err)
	}

	// The fallback encode: the finalized node is pn.node with the queued
	// temporal applied and TxFrom stamped — exactly what Execute produces.
	stamped := pn.node.DeepCopy()
	z := *pn.temporal
	z.TxFrom = types.Instant(txFrom)
	z.TxTo = types.Instant(txTo)
	stamped.SetTemporal(&z)
	direct, err := storeutil.MarshalNodeWire(stamped)
	if err != nil {
		t.Fatalf("MarshalNodeWire: %v", err)
	}

	if !bytes.Equal(patched, direct) {
		t.Fatalf("intent wireBody patch != fallback encode:\n patched=% x\n direct =% x", patched, direct)
	}
}

// TestIntentWireBodyPatchEquivalenceRel is the relationship mirror.
func TestIntentWireBodyPatchEquivalenceRel(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	c := newTestGraph(t)
	a, err := c.Nodes.Add(ctx, []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("add a: %v", err)
	}
	b, err := c.Nodes.Add(ctx, []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("add b: %v", err)
	}

	pr := preparedRel(t, c, "KNOWS", a, b, map[string]any{"w": int64(3)})
	ir, err := relCreateIntentFrom(intentHeader{seq: 1}, pr)
	if err != nil {
		t.Fatalf("relCreateIntentFrom: %v", err)
	}
	if !storeutil.HasWireTemporalTail(ir.wireBody) {
		t.Fatalf("rel wireBody has no v2 fixed tail")
	}

	const txFrom, txTo int64 = 42, 0
	patched := append([]byte(nil), ir.wireBody...)
	if err := storeutil.PatchWireTemporalTail(patched, txFrom, txTo); err != nil {
		t.Fatalf("patch: %v", err)
	}

	stamped := pr.rel.DeepCopy()
	z := *pr.temporal
	z.TxFrom = types.Instant(txFrom)
	z.TxTo = types.Instant(txTo)
	stamped.SetTemporal(&z)
	direct, err := storeutil.MarshalRelWire(stamped)
	if err != nil {
		t.Fatalf("MarshalRelWire: %v", err)
	}
	if !bytes.Equal(patched, direct) {
		t.Fatalf("rel intent wireBody patch != fallback encode:\n patched=% x\n direct =% x", patched, direct)
	}
}

// TestIntentWireBodyRoundTripsThroughCodec: the pre-encoded buffer survives the
// EncodeIntent/DecodeIntent projection (the stage-3 wire path) intact.
func TestIntentWireBodyRoundTripsThroughCodec(t *testing.T) {
	t.Parallel()
	c := newTestGraph(t)
	pn := preparedNode(t, c, []string{"Person"}, map[string]any{"name": "Ada"})
	ir, err := nodeCreateIntentFrom(intentHeader{epoch: 7, lane: 0, seq: 9}, pn)
	if err != nil {
		t.Fatalf("nodeCreateIntentFrom: %v", err)
	}
	data, err := EncodeIntent(ir)
	if err != nil {
		t.Fatalf("EncodeIntent: %v", err)
	}
	back, err := DecodeIntent(data)
	if err != nil {
		t.Fatalf("DecodeIntent: %v", err)
	}
	if !bytes.Equal(back.wireBody, ir.wireBody) {
		t.Fatalf("wireBody did not survive codec round-trip")
	}
}
