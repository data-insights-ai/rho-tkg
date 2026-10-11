package storeutil

import (
	"errors"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestDecodeWireTemporalMeta pins the partial decode against the full decode:
// for any marshalled row, (version, numeric temporal instants) must agree with
// WireToNodeChecked / WireToRelChecked exactly.
func TestDecodeWireTemporalMeta(t *testing.T) {
	t.Parallel()

	n := goldenNodeFullForTail(t)
	buf, err := MarshalNodeWire(n)
	if err != nil {
		t.Fatalf("MarshalNodeWire: %v", err)
	}
	version, tm, err := DecodeWireTemporalMeta(buf)
	if err != nil {
		t.Fatalf("DecodeWireTemporalMeta: %v", err)
	}
	if version != n.Version() {
		t.Fatalf("version = %d, want %d", version, n.Version())
	}
	want := n.Temporal()
	if tm == nil {
		t.Fatal("temporal = nil, want block")
	}
	if tm.ValidFrom != want.ValidFrom || tm.ValidTo != want.ValidTo ||
		tm.TxFrom != want.TxFrom || tm.TxTo != want.TxTo ||
		tm.CreatedAt != want.CreatedAt || tm.UpdatedAt != want.UpdatedAt ||
		tm.DeletedAt != want.DeletedAt {
		t.Fatalf("numeric temporal diverges from full decode: got %+v, want %+v", tm, want)
	}
	// Selection scope: provenance strings deliberately NOT decoded.
	if tm.CreatedBy != "" || tm.UpdatedBy != "" {
		t.Fatalf("selection-scope meta must not carry provenance strings, got %q/%q", tm.CreatedBy, tm.UpdatedBy)
	}

	// A row without a temporal block partially decodes to nil temporal.
	bare := types.NewNode(types.NodeID(snowflake.ID(7)), 1, nil)
	bare.SetVersion(3)
	bareBuf, err := MarshalNodeWire(bare)
	if err != nil {
		t.Fatalf("MarshalNodeWire(bare): %v", err)
	}
	v2, tm2, err := DecodeWireTemporalMeta(bareBuf)
	if err != nil || v2 != 3 || tm2 != nil {
		t.Fatalf("bare row = (%d, %v, %v), want (3, nil, nil)", v2, tm2, err)
	}

	// v1 golden rows (legacy omitempty tail) decode fine — the partial decode
	// reads map fields, not the fixed tail.
	v1 := mustHex(t, goldenV1NodeFull)
	_, tmV1, err := DecodeWireTemporalMeta(v1)
	if err != nil {
		t.Fatalf("v1 golden partial decode: %v", err)
	}
	var full NodeWire
	if err := SafeUnmarshal(v1, &full); err != nil {
		t.Fatalf("v1 golden full decode: %v", err)
	}
	if tmV1 == nil || int64(tmV1.TxFrom) != full.TxFrom || int64(tmV1.ValidFrom) != full.ValidFrom {
		t.Fatalf("v1 partial decode diverges from full: %+v vs tf=%d vf=%d", tmV1, full.TxFrom, full.ValidFrom)
	}

	// A future-format row fails closed like the checked decode.
	future := append([]byte(nil), buf...)
	var fw NodeWire
	if err := SafeUnmarshal(future, &fw); err != nil {
		t.Fatalf("decode for future-stamp: %v", err)
	}
	fw.FormatVersion = CurrentWireFormatVersion + 1
	futureBuf, err := marshalWirePooled(fw)
	if err != nil {
		t.Fatalf("marshal future: %v", err)
	}
	if _, _, err := DecodeWireTemporalMeta(futureBuf); !errors.Is(err, storepkg.ErrWireFormatVersionUnsupported) {
		t.Fatalf("future row = %v, want ErrWireFormatVersionUnsupported", err)
	}

	// Corrupt bytes fail closed via SafeUnmarshal.
	if _, _, err := DecodeWireTemporalMeta([]byte{0xc1}); err == nil {
		t.Fatal("corrupt row: want error, got nil")
	}
}

// TestSelectionTemporalMetaOfWires pins the wire-fields helpers (used for a
// delta row's Meta) against the same construction rule.
func TestSelectionTemporalMetaOfWires(t *testing.T) {
	t.Parallel()

	nw := NodeWire{HasTemporal: true, ValidFrom: 10, ValidTo: 20, TxFrom: 30, TxTo: 40, CreatedAt: 50, UpdatedAt: 60, DeletedAt: 70, CreatedBy: "x"}
	tm := SelectionTemporalMetaOfNodeWire(nw)
	if tm == nil || tm.ValidFrom != 10 || tm.DeletedAt != 70 || tm.CreatedBy != "" {
		t.Fatalf("node wire selection meta = %+v", tm)
	}
	if SelectionTemporalMetaOfNodeWire(NodeWire{}) != nil {
		t.Fatal("no-temporal node wire must yield nil")
	}

	rw := RelWire{HasTemporal: true, ValidFrom: 1, TxFrom: 2}
	rtm := SelectionTemporalMetaOfRelWire(rw)
	if rtm == nil || rtm.ValidFrom != 1 || rtm.TxFrom != 2 {
		t.Fatalf("rel wire selection meta = %+v", rtm)
	}
	if SelectionTemporalMetaOfRelWire(RelWire{}) != nil {
		t.Fatal("no-temporal rel wire must yield nil")
	}
}
