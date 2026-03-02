package graph

import (
	"reflect"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/vmihailenco/msgpack/v5"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3/pkg/types"
)

func TestNodeToWireAndBack(t *testing.T) {
	t.Parallel()

	n := types.NewNode(snowflake.ID(1001), 1, []uint16{2, 3})
	n.SetVersion(5)
	n.SetProperties(mustPropertySlice(t, map[string]any{
		"name": "Alice",
		"age":  int64(30),
	}))
	n.SetTemporal(&types.TemporalMetadata{
		ValidFrom: 100,
		ValidTo:   200,
		TxFrom:    300,
		TxTo:      400,
		CreatedAt: 500,
		UpdatedAt: 600,
		DeletedAt: 700,
		CreatedBy: "admin",
		UpdatedBy: "system",
	})
	n.Temporal().SetBaseEntityID(snowflake.ID(999))
	n.SetIntegrity(&types.NodeIntegrity{
		Hash:     "abc123",
		PrevHash: "def456",
	})

	w := nodeToWire(n)
	got := wireToNode(w)

	if int64(got.InternalID().SnowflakeID()) != 1001 {
		t.Fatalf("ID mismatch: got %d", int64(got.InternalID().SnowflakeID()))
	}
	if got.PrimaryLabelToken().Value() != 1 {
		t.Fatalf("primary label mismatch: got %d", got.PrimaryLabelToken().Value())
	}
	extras := got.ExtraLabelTokens()
	if len(extras) != 2 {
		t.Fatalf("extras len: got %d", len(extras))
	}
	if got.Version() != 5 {
		t.Fatalf("version: got %d", got.Version())
	}
	v, ok := got.GetProperty("name")
	if !ok || v != "Alice" {
		t.Fatalf("property name: got %v %v", v, ok)
	}
	v, ok = got.GetProperty("age")
	if !ok || v != int64(30) {
		t.Fatalf("property age: got %v %v", v, ok)
	}
	tm := got.Temporal()
	if tm == nil {
		t.Fatal("temporal is nil")
	}
	if tm.ValidFrom != 100 || tm.ValidTo != 200 {
		t.Fatalf("temporal validity: %d-%d", tm.ValidFrom, tm.ValidTo)
	}
	if tm.CreatedBy != "admin" || tm.UpdatedBy != "system" {
		t.Fatal("temporal provenance mismatch")
	}
	if int64(tm.BaseEntityID().SnowflakeID()) != 999 {
		t.Fatalf("base entity: got %d", int64(tm.BaseEntityID().SnowflakeID()))
	}
	ig := got.Integrity()
	if ig == nil {
		t.Fatal("integrity is nil")
	}
	if ig.Hash != "abc123" || ig.PrevHash != "def456" {
		t.Fatal("integrity mismatch")
	}
}

func TestNodeWireNoExtras(t *testing.T) {
	t.Parallel()

	n := types.NewNode(snowflake.ID(42), 1, nil)
	w := nodeToWire(n)
	got := wireToNode(w)

	if got.ExtraLabelTokens() != nil {
		t.Fatal("expected no extra labels")
	}
	if got.Properties().Len() != 0 {
		t.Fatal("expected no properties")
	}
}

func TestNodeWireNilTemporal(t *testing.T) {
	t.Parallel()

	n := types.NewNode(snowflake.ID(42), 1, nil)
	w := nodeToWire(n)

	if w.HasTemporal {
		t.Fatal("HasTemporal should be false")
	}

	got := wireToNode(w)
	if got.Temporal() != nil {
		t.Fatal("temporal should be nil")
	}
}

func TestNodeWireNilIntegrity(t *testing.T) {
	t.Parallel()

	n := types.NewNode(snowflake.ID(42), 1, nil)
	w := nodeToWire(n)

	if w.Hash != "" || w.PrevHash != "" {
		t.Fatal("hash fields should be empty")
	}

	got := wireToNode(w)
	if got.Integrity() != nil {
		t.Fatal("integrity should be nil")
	}
}

func TestRelToWireAndBack(t *testing.T) {
	t.Parallel()

	r := types.NewRelationship(snowflake.ID(500), 3, snowflake.ID(100), snowflake.ID(200))
	r.SetVersion(2)
	r.SetProperties(mustPropertySlice(t, map[string]any{
		"weight": float64(1.5),
	}))
	r.SetTemporal(&types.TemporalMetadata{
		ValidFrom: 10,
		CreatedBy: "test",
	})
	r.SetIntegrity(&types.RelIntegrity{
		Hash: "rel-hash",
	})

	w := relToWire(r)
	got := wireToRel(w)

	if int64(got.InternalID().SnowflakeID()) != 500 {
		t.Fatalf("ID mismatch: got %d", int64(got.InternalID().SnowflakeID()))
	}
	if got.TypeToken().Value() != 3 {
		t.Fatalf("type token: got %d", got.TypeToken().Value())
	}
	if int64(got.StartNodeID().SnowflakeID()) != 100 {
		t.Fatal("start ID mismatch")
	}
	if int64(got.EndNodeID().SnowflakeID()) != 200 {
		t.Fatal("end ID mismatch")
	}
	if got.Version() != 2 {
		t.Fatalf("version: got %d", got.Version())
	}
	v, ok := got.GetProperty("weight")
	if !ok || v != float64(1.5) {
		t.Fatalf("property weight: got %v", v)
	}
	if got.Temporal() == nil {
		t.Fatal("temporal is nil")
	}
	if got.Temporal().ValidFrom != 10 {
		t.Fatal("temporal validfrom mismatch")
	}
	if got.Integrity() == nil || got.Integrity().Hash != "rel-hash" {
		t.Fatal("integrity mismatch")
	}
}

func TestRelWireNoProperties(t *testing.T) {
	t.Parallel()

	r := types.NewRelationship(snowflake.ID(1), 1, snowflake.ID(2), snowflake.ID(3))
	w := relToWire(r)
	got := wireToRel(w)

	if got.Properties().Len() != 0 {
		t.Fatal("expected no properties")
	}
}

func TestRelWireNilTemporalIntegrity(t *testing.T) {
	t.Parallel()

	r := types.NewRelationship(snowflake.ID(1), 1, snowflake.ID(2), snowflake.ID(3))
	w := relToWire(r)
	got := wireToRel(w)

	if got.Temporal() != nil {
		t.Fatal("temporal should be nil")
	}
	if got.Integrity() != nil {
		t.Fatal("integrity should be nil")
	}
}

func TestPropertyWireRoundTrip(t *testing.T) {
	t.Parallel()

	ps := mustPropertySlice(t, map[string]any{
		"bool":    true,
		"int64":   int64(42),
		"float64": float64(3.14),
		"string":  "hello",
	})

	pw := propertiesToWire(ps)
	got := wireToProperties(pw)

	if got.Len() != 4 {
		t.Fatalf("expected 4 properties, got %d", got.Len())
	}
	v, ok := got.Get("bool")
	if !ok || v != true {
		t.Fatal("bool mismatch")
	}
	v, ok = got.Get("int64")
	if !ok || v != int64(42) {
		t.Fatal("int64 mismatch")
	}
	v, ok = got.Get("float64")
	if !ok || v != float64(3.14) {
		t.Fatal("float64 mismatch")
	}
	v, ok = got.Get("string")
	if !ok || v != "hello" {
		t.Fatal("string mismatch")
	}
}

func TestPropertyWireSliceValues(t *testing.T) {
	t.Parallel()

	ps := mustPropertySlice(t, map[string]any{
		"tags":  []string{"a", "b"},
		"nums":  []int64{1, 2, 3},
		"mixed": []any{"x", int64(1)},
	})

	pw := propertiesToWire(ps)
	got := wireToProperties(pw)

	if got.Len() != 3 {
		t.Fatalf("expected 3, got %d", got.Len())
	}
}

func TestPropertyWireMapValues(t *testing.T) {
	t.Parallel()

	ps := mustPropertySlice(t, map[string]any{
		"nested": map[string]any{"key": "value"},
	})

	pw := propertiesToWire(ps)
	got := wireToProperties(pw)

	v, ok := got.Get("nested")
	if !ok {
		t.Fatal("missing nested")
	}
	m, ok := v.(map[string]any)
	if !ok || m["key"] != "value" {
		t.Fatal("nested map mismatch")
	}
}

func TestPropertyWireEmpty(t *testing.T) {
	t.Parallel()

	pw := propertiesToWire(nil)
	if pw != nil {
		t.Fatal("expected nil for nil input")
	}

	got := wireToProperties(nil)
	if got != nil {
		t.Fatal("expected nil for nil input")
	}

	pw2 := propertiesToWire(types.PropertySlice{})
	if pw2 != nil {
		t.Fatal("expected nil for empty input")
	}
}

func TestNodeWireMsgpackMarshalUnmarshal(t *testing.T) {
	t.Parallel()

	n := types.NewNode(snowflake.ID(1001), 1, []uint16{2})
	n.SetVersion(3)
	n.SetProperties(mustPropertySlice(t, map[string]any{
		"name": "Bob",
	}))

	w := nodeToWire(n)
	data, err := msgpack.Marshal(w)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var w2 nodeWire
	if err := msgpack.Unmarshal(data, &w2); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	got := wireToNode(w2)
	if int64(got.InternalID().SnowflakeID()) != 1001 {
		t.Fatal("ID mismatch after msgpack round-trip")
	}
	if got.Version() != 3 {
		t.Fatal("version mismatch after msgpack round-trip")
	}
	v, ok := got.GetProperty("name")
	if !ok || v != "Bob" {
		t.Fatalf("property mismatch after msgpack round-trip: %v", v)
	}
}

func TestRelWireMsgpackMarshalUnmarshal(t *testing.T) {
	t.Parallel()

	r := types.NewRelationship(snowflake.ID(500), 3, snowflake.ID(100), snowflake.ID(200))
	r.SetVersion(7)

	w := relToWire(r)
	data, err := msgpack.Marshal(w)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var w2 relWire
	if err := msgpack.Unmarshal(data, &w2); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	got := wireToRel(w2)
	if int64(got.InternalID().SnowflakeID()) != 500 {
		t.Fatal("ID mismatch")
	}
	if got.TypeToken().Value() != 3 {
		t.Fatal("type token mismatch")
	}
	if got.Version() != 7 {
		t.Fatal("version mismatch")
	}
}

func TestPropertyWireTypeNormalization(t *testing.T) {
	t.Parallel()

	// With type tags, small ints that msgpack encodes as int8 are
	// reconstructed back to their original Go type.
	w := nodeWire{
		ID:           1,
		PrimaryLabel: 1,
		Properties: []propertyWire{
			{Key: "count", Value: int(42), Type: ptInt},        // int → int8 after msgpack → reconstructed to int
			{Key: "big", Value: int64(1 << 40), Type: ptInt64}, // stays int64
			{Key: "rate", Value: float64(1.5), Type: ptFloat64},
		},
	}

	data, err := msgpack.Marshal(w)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var w2 nodeWire
	if err := msgpack.Unmarshal(data, &w2); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// After reconstruction, int(42) should be restored.
	ps := wireToProperties(w2.Properties)
	v0 := ps[0].Value
	if _, ok := v0.(int); !ok {
		t.Fatalf("expected int, got %T(%v)", v0, v0)
	}
	if v0.(int) != 42 {
		t.Fatalf("expected 42, got %v", v0)
	}

	// Large int64 stays int64.
	v1 := ps[1].Value
	if v, ok := v1.(int64); !ok || v != 1<<40 {
		t.Fatalf("expected int64(1<<40), got %T(%v)", v1, v1)
	}

	// float64 stays float64.
	v2 := ps[2].Value
	if v, ok := v2.(float64); !ok || v != 1.5 {
		t.Fatalf("expected float64(1.5), got %T(%v)", v2, v2)
	}
}

// ─── Type fidelity tests ─────────────────────────────────────────────────────

func TestPropertyWireTypeFidelityPrimitives(t *testing.T) {
	t.Parallel()

	// Table-driven: every scalar type tag must survive a full msgpack round-trip.
	tests := []struct {
		name string
		val  any
		tag  byte
	}{
		{"bool", true, ptBool},
		{"int", int(42), ptInt},
		{"int8", int8(7), ptInt8},
		{"int16", int16(300), ptInt16},
		{"int32", int32(70000), ptInt32},
		{"int64", int64(1 << 40), ptInt64},
		{"uint", uint(42), ptUint},
		{"uint8", uint8(200), ptUint8},
		{"uint16", uint16(50000), ptUint16},
		{"uint32", uint32(3000000000), ptUint32},
		{"uint64", uint64(1 << 50), ptUint64},
		{"float32", float32(1.5), ptFloat32},
		{"float64", float64(3.14), ptFloat64},
		{"string", "hello", ptString},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pw := []propertyWire{{Key: "k", Value: tc.val, Type: tc.tag}}
			data, err := msgpack.Marshal(pw)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			var pw2 []propertyWire
			if err := msgpack.Unmarshal(data, &pw2); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			ps := wireToProperties(pw2)
			got := ps[0].Value
			wantType := reflect.TypeOf(tc.val)
			gotType := reflect.TypeOf(got)
			if gotType != wantType {
				t.Fatalf("type mismatch: want %v, got %v (value: %v)", wantType, gotType, got)
			}
		})
	}
}

func TestPropertyWireTypeFidelityStringSlice(t *testing.T) {
	t.Parallel()

	ps := mustPropertySlice(t, map[string]any{"tags": []string{"a", "b", "c"}})
	pw := propertiesToWire(ps)
	data, err := msgpack.Marshal(pw)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var pw2 []propertyWire
	if err := msgpack.Unmarshal(data, &pw2); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	got := wireToProperties(pw2)
	v, ok := got.Get("tags")
	if !ok {
		t.Fatal("missing tags")
	}
	ss, ok := v.([]string)
	if !ok {
		t.Fatalf("expected []string, got %T", v)
	}
	if len(ss) != 3 || ss[0] != "a" || ss[1] != "b" || ss[2] != "c" {
		t.Fatalf("unexpected value: %v", ss)
	}
}

func TestPropertyWireTypeFidelityInt64Slice(t *testing.T) {
	t.Parallel()

	ps := mustPropertySlice(t, map[string]any{"ids": []int64{10, 20, 30}})
	pw := propertiesToWire(ps)
	data, err := msgpack.Marshal(pw)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var pw2 []propertyWire
	if err := msgpack.Unmarshal(data, &pw2); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	got := wireToProperties(pw2)
	v, ok := got.Get("ids")
	if !ok {
		t.Fatal("missing ids")
	}
	is, ok := v.([]int64)
	if !ok {
		t.Fatalf("expected []int64, got %T", v)
	}
	if len(is) != 3 || is[0] != 10 || is[1] != 20 || is[2] != 30 {
		t.Fatalf("unexpected value: %v", is)
	}
}

func TestPropertyWireTypeFidelityFloat64Slice(t *testing.T) {
	t.Parallel()

	ps := mustPropertySlice(t, map[string]any{"scores": []float64{1.1, 2.2}})
	pw := propertiesToWire(ps)
	data, err := msgpack.Marshal(pw)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var pw2 []propertyWire
	if err := msgpack.Unmarshal(data, &pw2); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	got := wireToProperties(pw2)
	v, ok := got.Get("scores")
	if !ok {
		t.Fatal("missing scores")
	}
	fs, ok := v.([]float64)
	if !ok {
		t.Fatalf("expected []float64, got %T", v)
	}
	if len(fs) != 2 || fs[0] != 1.1 || fs[1] != 2.2 {
		t.Fatalf("unexpected value: %v", fs)
	}
}

func TestPropertyWireTypeFidelityBoolSlice(t *testing.T) {
	t.Parallel()

	ps := mustPropertySlice(t, map[string]any{"flags": []bool{true, false, true}})
	pw := propertiesToWire(ps)
	data, err := msgpack.Marshal(pw)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var pw2 []propertyWire
	if err := msgpack.Unmarshal(data, &pw2); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	got := wireToProperties(pw2)
	v, ok := got.Get("flags")
	if !ok {
		t.Fatal("missing flags")
	}
	bs, ok := v.([]bool)
	if !ok {
		t.Fatalf("expected []bool, got %T", v)
	}
	if len(bs) != 3 || !bs[0] || bs[1] || !bs[2] {
		t.Fatalf("unexpected value: %v", bs)
	}
}

func TestPropertyWireTypeFidelityByteSlice(t *testing.T) {
	t.Parallel()

	ps := mustPropertySlice(t, map[string]any{"data": []byte{0xDE, 0xAD}})
	pw := propertiesToWire(ps)
	data, err := msgpack.Marshal(pw)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var pw2 []propertyWire
	if err := msgpack.Unmarshal(data, &pw2); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	got := wireToProperties(pw2)
	v, ok := got.Get("data")
	if !ok {
		t.Fatal("missing data")
	}
	bs, ok := v.([]byte)
	if !ok {
		t.Fatalf("expected []byte, got %T", v)
	}
	if len(bs) != 2 || bs[0] != 0xDE || bs[1] != 0xAD {
		t.Fatalf("unexpected value: %x", bs)
	}
}

func TestPropertyWireTypeFidelityMapStringAny(t *testing.T) {
	t.Parallel()

	ps := mustPropertySlice(t, map[string]any{
		"meta": map[string]any{"key": "val", "num": int64(42)},
	})
	pw := propertiesToWire(ps)
	data, err := msgpack.Marshal(pw)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var pw2 []propertyWire
	if err := msgpack.Unmarshal(data, &pw2); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	got := wireToProperties(pw2)
	v, ok := got.Get("meta")
	if !ok {
		t.Fatal("missing meta")
	}
	m, ok := v.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any, got %T", v)
	}
	if m["key"] != "val" {
		t.Fatal("key mismatch")
	}
	// int64(42) may have been decoded as int8 by msgpack; integer normalization
	// should restore to int64.
	if n, ok := m["num"].(int64); !ok || n != 42 {
		t.Fatalf("expected int64(42), got %T(%v)", m["num"], m["num"])
	}
}

func TestPropertyWireTypeFidelityMapStringString(t *testing.T) {
	t.Parallel()

	ps := mustPropertySlice(t, map[string]any{
		"headers": map[string]string{"Content-Type": "text/plain"},
	})
	pw := propertiesToWire(ps)
	data, err := msgpack.Marshal(pw)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var pw2 []propertyWire
	if err := msgpack.Unmarshal(data, &pw2); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	got := wireToProperties(pw2)
	v, ok := got.Get("headers")
	if !ok {
		t.Fatal("missing headers")
	}
	m, ok := v.(map[string]string)
	if !ok {
		t.Fatalf("expected map[string]string, got %T", v)
	}
	if m["Content-Type"] != "text/plain" {
		t.Fatal("value mismatch")
	}
}

func TestPropertyWireTypeFidelitySliceAny(t *testing.T) {
	t.Parallel()

	ps := mustPropertySlice(t, map[string]any{
		"mixed": []any{"x", int64(99), true},
	})
	pw := propertiesToWire(ps)
	data, err := msgpack.Marshal(pw)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var pw2 []propertyWire
	if err := msgpack.Unmarshal(data, &pw2); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	got := wireToProperties(pw2)
	v, ok := got.Get("mixed")
	if !ok {
		t.Fatal("missing mixed")
	}
	s, ok := v.([]any)
	if !ok {
		t.Fatalf("expected []any, got %T", v)
	}
	if len(s) != 3 {
		t.Fatalf("expected 3 elements, got %d", len(s))
	}
	if s[0] != "x" {
		t.Fatal("element 0 mismatch")
	}
	// int64(99) may have been decoded as int8; normalization should restore.
	if n, ok := s[1].(int64); !ok || n != 99 {
		t.Fatalf("expected int64(99), got %T(%v)", s[1], s[1])
	}
	if s[2] != true {
		t.Fatal("element 2 mismatch")
	}
}

func TestPropertyWireTypeFidelityBackwardCompat(t *testing.T) {
	t.Parallel()

	// Simulate old data without type tag (Type=0/ptUnknown).
	pw := []propertyWire{
		{Key: "count", Value: int(42), Type: ptUnknown},
		{Key: "tags", Value: []string{"a"}, Type: ptUnknown},
	}
	data, err := msgpack.Marshal(pw)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var pw2 []propertyWire
	if err := msgpack.Unmarshal(data, &pw2); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	ps := wireToProperties(pw2)

	// ptUnknown: small int8 → normalized to int64 (best-effort).
	v0 := ps[0].Value
	if _, ok := v0.(int64); !ok {
		t.Fatalf("backward compat: expected int64, got %T", v0)
	}

	// ptUnknown: []string → decoded as []any → stays []any (no tag to restore).
	v1 := ps[1].Value
	if _, ok := v1.([]any); !ok {
		t.Fatalf("backward compat: expected []any (no tag), got %T", v1)
	}
}

func TestPropertyWireTypeFidelityNilValue(t *testing.T) {
	t.Parallel()

	// Nil values should survive reconstruction.
	pw := []propertyWire{{Key: "empty", Value: nil, Type: ptString}}
	ps := wireToProperties(pw)
	if ps[0].Value != nil {
		t.Fatalf("expected nil, got %T(%v)", ps[0].Value, ps[0].Value)
	}
}

func TestNodeWireBaseEntityID(t *testing.T) {
	t.Parallel()

	// Non-zero base entity.
	n := types.NewNode(snowflake.ID(1), 1, nil)
	n.SetTemporal(&types.TemporalMetadata{})
	n.Temporal().SetBaseEntityID(snowflake.ID(777))

	w := nodeToWire(n)
	if w.BaseEntityID != 777 {
		t.Fatalf("wire base entity: got %d", w.BaseEntityID)
	}

	got := wireToNode(w)
	if int64(got.Temporal().BaseEntityID().SnowflakeID()) != 777 {
		t.Fatal("base entity round-trip failed")
	}

	// Zero base entity.
	n2 := types.NewNode(snowflake.ID(2), 1, nil)
	n2.SetTemporal(&types.TemporalMetadata{})
	w2 := nodeToWire(n2)
	got2 := wireToNode(w2)
	if int64(got2.Temporal().BaseEntityID().SnowflakeID()) != 0 {
		t.Fatal("zero base entity should remain zero")
	}
}

func TestNodeWireTemporalZeroInstants(t *testing.T) {
	t.Parallel()

	// All-zero temporal with HasTemporal=true.
	n := types.NewNode(snowflake.ID(1), 1, nil)
	n.SetTemporal(&types.TemporalMetadata{})

	w := nodeToWire(n)
	if !w.HasTemporal {
		t.Fatal("HasTemporal should be true")
	}

	got := wireToNode(w)
	tm := got.Temporal()
	if tm == nil {
		t.Fatal("temporal should not be nil")
	}
	if tm.ValidFrom != 0 || tm.ValidTo != 0 || tm.TxFrom != 0 || tm.TxTo != 0 {
		t.Fatal("all temporal instants should be zero")
	}
}

func TestWireRoundTripIntSlice(t *testing.T) {
	t.Parallel()

	// Exercise toIntSlice via full marshal/unmarshal round-trip.
	n := types.NewNode(snowflake.ID(42), 1, nil)
	n.SetProperties(mustPropertySlice(t, map[string]any{
		"counts": []int{1, 2, 3},
	}))

	w := nodeToWire(n)
	data, err := msgpack.Marshal(w)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var w2 nodeWire
	if err := msgpack.Unmarshal(data, &w2); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	got := wireToNode(w2)
	v, ok := got.GetProperty("counts")
	if !ok {
		t.Fatal("missing counts property")
	}
	is, ok := v.([]int)
	if !ok {
		t.Fatalf("expected []int, got %T", v)
	}
	if len(is) != 3 || is[0] != 1 || is[1] != 2 || is[2] != 3 {
		t.Fatalf("unexpected value: %v", is)
	}
}

// ─── Direct unit tests for low-level helpers ──────────────────────────────────

// TestPropertyTypeTagAllBranches exercises every branch in propertyTypeTag
// directly (without going through msgpack). The function is called by
// propertiesToWire, but most wire tests construct propertyWire literals and
// bypass it; this test provides explicit coverage.
func TestPropertyTypeTagAllBranches(t *testing.T) {
	t.Parallel()

	cases := []struct {
		val  any
		want byte
	}{
		{true, ptBool},
		{int(1), ptInt},
		{int8(1), ptInt8},
		{int16(1), ptInt16},
		{int32(1), ptInt32},
		{int64(1), ptInt64},
		{uint(1), ptUint},
		{uint8(1), ptUint8},
		{uint16(1), ptUint16},
		{uint32(1), ptUint32},
		{uint64(1), ptUint64},
		{float32(1), ptFloat32},
		{float64(1), ptFloat64},
		{"s", ptString},
		{[]string{"a"}, ptSliceStr},
		{[]int{1}, ptSliceInt},
		{[]int64{1}, ptSliceInt64},
		{[]float64{1}, ptSliceF64},
		{[]byte{1}, ptSliceByte},
		{[]bool{true}, ptSliceBool},
		{[]any{1}, ptSliceAny},
		{map[string]any{"k": "v"}, ptMapStrAny},
		{map[string]string{"k": "v"}, ptMapStrStr},
		{struct{}{}, ptUnknown}, // unknown type → default branch
	}

	for _, tc := range cases {
		got := propertyTypeTag(tc.val)
		if got != tc.want {
			t.Errorf("propertyTypeTag(%T) = %d, want %d", tc.val, got, tc.want)
		}
	}
}

// TestToInt64AllBranches covers every type case in toInt64 directly.
func TestToInt64AllBranches(t *testing.T) {
	t.Parallel()

	cases := []struct {
		val  any
		want int64
	}{
		{int8(-5), -5},
		{int16(300), 300},
		{int32(70000), 70000},
		{int64(1 << 40), 1 << 40},
		{int(42), 42},
		{uint8(200), 200},
		{uint16(50000), 50000},
		{uint32(3000000000), 3000000000},
		{uint64(999), 999},
		{"unsupported", 0}, // default → 0
	}

	for _, tc := range cases {
		got := toInt64(tc.val)
		if got != tc.want {
			t.Errorf("toInt64(%T(%v)) = %d, want %d", tc.val, tc.val, got, tc.want)
		}
	}
}

// TestToUint64AllBranches covers every type case in toUint64 directly.
func TestToUint64AllBranches(t *testing.T) {
	t.Parallel()

	cases := []struct {
		val  any
		want uint64
	}{
		{uint8(200), 200},
		{uint16(50000), 50000},
		{uint32(3000000000), 3000000000},
		{uint64(1 << 50), 1 << 50},
		{int8(7), 7},
		{int16(300), 300},
		{int32(70000), 70000},
		{int64(999), 999},
		{int(42), 42},
		{"unsupported", 0}, // default → 0
	}

	for _, tc := range cases {
		got := toUint64(tc.val)
		if got != tc.want {
			t.Errorf("toUint64(%T(%v)) = %d, want %d", tc.val, tc.val, got, tc.want)
		}
	}
}

// TestNormalizeIntegersRecursiveAllBranches covers every branch in
// normalizeIntegersRecursive including the integer narrow-int cases and
// the default passthrough for non-integer types.
func TestNormalizeIntegersRecursiveAllBranches(t *testing.T) {
	t.Parallel()

	cases := []struct {
		val     any
		wantVal any
	}{
		{int8(-3), int64(-3)},
		{int16(300), int64(300)},
		{int32(70000), int64(70000)},
		{uint8(200), uint64(200)},
		{uint16(50000), uint64(50000)},
		{uint32(3000000000), uint64(3000000000)},
		// default passthrough — types that are already normalized
		{int64(42), int64(42)},
		{uint64(99), uint64(99)},
		{float64(1.5), float64(1.5)},
		{"hello", "hello"},
		{true, true},
	}

	for _, tc := range cases {
		got := normalizeIntegersRecursive(tc.val)
		if got != tc.wantVal {
			t.Errorf("normalizeIntegersRecursive(%T(%v)) = %T(%v), want %T(%v)",
				tc.val, tc.val, got, got, tc.wantVal, tc.wantVal)
		}
	}

	// []any and map[string]any recursion is already covered by
	// normalizeIntegersInSlice / normalizeIntegersInMap (100%), but
	// call through normalizeIntegersRecursive explicitly for branch coverage.
	sliceResult := normalizeIntegersRecursive([]any{int8(1), int16(2)})
	s, ok := sliceResult.([]any)
	if !ok || len(s) != 2 {
		t.Fatalf("[]any branch: got %T(%v)", sliceResult, sliceResult)
	}
	if s[0] != int64(1) || s[1] != int64(2) {
		t.Fatalf("[]any branch values: %v %v", s[0], s[1])
	}

	mapResult := normalizeIntegersRecursive(map[string]any{"x": int32(99)})
	m, ok := mapResult.(map[string]any)
	if !ok || m["x"] != int64(99) {
		t.Fatalf("map[string]any branch: got %T(%v)", mapResult, mapResult)
	}
}

// mustPropertySlice is a test helper that creates a PropertySlice from a map.
func mustPropertySlice(t *testing.T, m map[string]any) types.PropertySlice {
	t.Helper()
	ps, err := types.NewPropertySlice(m)
	if err != nil {
		t.Fatalf("NewPropertySlice: %v", err)
	}
	return ps
}
