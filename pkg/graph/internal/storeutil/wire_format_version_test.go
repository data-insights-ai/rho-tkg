package storeutil

import (
	"errors"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	"github.com/vmihailenco/msgpack/v5"
)

// wireMapInt extracts an integer field from a raw msgpack map decode, where
// the concrete Go type depends on the encoded width.
func wireMapInt(t *testing.T, m map[string]any, key string) (int64, bool) {
	t.Helper()
	v, ok := m[key]
	if !ok {
		return 0, false
	}
	switch x := v.(type) {
	case int64:
		return x, true
	case uint64:
		return int64(x), true // #nosec G115 — test values are tiny
	case int32:
		return int64(x), true
	case uint32:
		return int64(x), true
	case int16:
		return int64(x), true
	case uint16:
		return int64(x), true
	case int8:
		return int64(x), true
	case uint8:
		return int64(x), true
	case int:
		return int64(x), true
	default:
		t.Fatalf("field %q has unexpected type %T", key, v)
		return 0, false
	}
}

// The custom EncodeMsgpack implementations bypass struct tags (lesson 39).
// These tests decode the RAW bytes into a generic map so they fail if the
// field is added to the struct but not to the hand-written encoder — a
// struct-level round-trip would pass in that case and hide the bug.
func TestNodeWireCustomEncoderEmitsFormatVersion(t *testing.T) {
	t.Parallel()

	n := types.NewNode(types.NodeID(snowflake.ID(1001)), 1, []uint16{2})
	data, err := MarshalNodeWire(n)
	if err != nil {
		t.Fatalf("MarshalNodeWire: %v", err)
	}

	var m map[string]any
	if err := msgpack.Unmarshal(data, &m); err != nil {
		t.Fatalf("raw unmarshal: %v", err)
	}
	fv, ok := wireMapInt(t, m, "fv")
	if !ok {
		t.Fatalf("custom NodeWire encoder did not emit the fv field; raw map keys: %v", mapKeys(m))
	}
	if fv != CurrentWireFormatVersion {
		t.Fatalf("fv on the wire = %d, want %d", fv, CurrentWireFormatVersion)
	}

	// The map-length bookkeeping in the custom encoder must stay consistent:
	// a wrong EncodeMapLen corrupts every field after it. Prove the full row
	// still decodes and carries the exact original content.
	var w NodeWire
	if err := msgpack.Unmarshal(data, &w); err != nil {
		t.Fatalf("typed unmarshal: %v", err)
	}
	if w.FormatVersion != CurrentWireFormatVersion {
		t.Fatalf("decoded FormatVersion = %d, want %d", w.FormatVersion, CurrentWireFormatVersion)
	}
	back, err := WireToNodeChecked(w)
	if err != nil {
		t.Fatalf("WireToNodeChecked: %v", err)
	}
	if back.ID() != n.ID() || back.PrimaryLabelToken() != n.PrimaryLabelToken() || !back.HasLabelToken(2) {
		t.Fatalf("round-trip mutated the node: got id=%v primary=%v", back.ID(), back.PrimaryLabelToken())
	}
}

func TestRelWireCustomEncoderEmitsFormatVersion(t *testing.T) {
	t.Parallel()

	r := types.NewRelationship(types.RelID(snowflake.ID(77)), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
	data, err := MarshalRelWire(r)
	if err != nil {
		t.Fatalf("MarshalRelWire: %v", err)
	}

	var m map[string]any
	if err := msgpack.Unmarshal(data, &m); err != nil {
		t.Fatalf("raw unmarshal: %v", err)
	}
	fv, ok := wireMapInt(t, m, "fv")
	if !ok {
		t.Fatalf("custom RelWire encoder did not emit the fv field; raw map keys: %v", mapKeys(m))
	}
	if fv != CurrentWireFormatVersion {
		t.Fatalf("fv on the wire = %d, want %d", fv, CurrentWireFormatVersion)
	}

	var w RelWire
	if err := msgpack.Unmarshal(data, &w); err != nil {
		t.Fatalf("typed unmarshal: %v", err)
	}
	back, err := WireToRelChecked(w)
	if err != nil {
		t.Fatalf("WireToRelChecked: %v", err)
	}
	if back.ID() != r.ID() || back.StartNodeID() != r.StartNodeID() || back.EndNodeID() != r.EndNodeID() {
		t.Fatalf("round-trip mutated the relationship")
	}
}

func mapKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// A row claiming a FUTURE format version must fail closed with the sentinel —
// never decode into an entity with zero-filled unknown fields.
func TestWireFormatVersionFutureRowFailsClosed(t *testing.T) {
	t.Parallel()

	nw := NodeWire{FormatVersion: CurrentWireFormatVersion + 1, ID: 100, PrimaryLabel: 1}
	if err := ValidateNodeWire(nw); !errors.Is(err, storecontract.ErrWireFormatVersionUnsupported) {
		t.Fatalf("ValidateNodeWire(future) = %v, want ErrWireFormatVersionUnsupported", err)
	}
	if n, err := WireToNodeChecked(nw); err == nil || n != nil {
		t.Fatalf("WireToNodeChecked(future) returned (%v, %v), want (nil, error)", n, err)
	}

	rw := RelWire{FormatVersion: CurrentWireFormatVersion + 1, ID: 100, RelType: 1, StartID: 1, EndID: 2}
	if err := ValidateRelWire(rw); !errors.Is(err, storecontract.ErrWireFormatVersionUnsupported) {
		t.Fatalf("ValidateRelWire(future) = %v, want ErrWireFormatVersionUnsupported", err)
	}
	if r, err := WireToRelChecked(rw); err == nil || r != nil {
		t.Fatalf("WireToRelChecked(future) returned (%v, %v), want (nil, error)", r, err)
	}

	// Hostile raw bytes: a hand-encoded row with fv far in the future must
	// fail at the checked boundary, not be silently accepted.
	data, err := msgpack.Marshal(NodeWire{FormatVersion: 99, ID: 100, PrimaryLabel: 1})
	if err != nil {
		t.Fatalf("marshal hostile row: %v", err)
	}
	var decoded NodeWire
	if err := msgpack.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal hostile row: %v", err)
	}
	if decoded.FormatVersion != 99 {
		t.Fatalf("hostile fv did not survive the wire: got %d", decoded.FormatVersion)
	}
	if _, err := WireToNodeChecked(decoded); !errors.Is(err, storecontract.ErrWireFormatVersionUnsupported) {
		t.Fatalf("checked decode of hostile fv=99 row = %v, want ErrWireFormatVersionUnsupported", err)
	}
}

// Rows written before versioning existed have NO fv key on the wire and must
// keep decoding (FormatVersion 0 == legacy == version 1 semantics).
func TestWireFormatVersionLegacyRowStillDecodes(t *testing.T) {
	t.Parallel()

	legacy := NodeWire{ID: 4242, PrimaryLabel: 3, Version: 7} // FormatVersion deliberately zero
	data, err := msgpack.Marshal(legacy)
	if err != nil {
		t.Fatalf("marshal legacy row: %v", err)
	}

	// The legacy encoding must really omit the key — otherwise this test
	// would not be exercising the absent-field path at all.
	var m map[string]any
	if err := msgpack.Unmarshal(data, &m); err != nil {
		t.Fatalf("raw unmarshal: %v", err)
	}
	if _, ok := m["fv"]; ok {
		t.Fatalf("legacy encoding unexpectedly contains fv; the absent-field path is untested")
	}

	var w NodeWire
	if err := msgpack.Unmarshal(data, &w); err != nil {
		t.Fatalf("typed unmarshal: %v", err)
	}
	if w.FormatVersion != 0 {
		t.Fatalf("legacy row decoded FormatVersion = %d, want 0", w.FormatVersion)
	}
	n, err := WireToNodeChecked(w)
	if err != nil {
		t.Fatalf("WireToNodeChecked(legacy) = %v, want success", err)
	}
	if int64(n.ID().SnowflakeID()) != 4242 || n.Version() != 7 {
		t.Fatalf("legacy row content mutated: id=%v version=%d", n.ID(), n.Version())
	}
}
