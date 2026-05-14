package storeutil

import (
	"errors"
	"math"
	"reflect"
	"strconv"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/vmihailenco/msgpack/v5"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
)

func TestNodeToWireAndBack(t *testing.T) {
	t.Parallel()

	n := types.NewNode(types.NodeID(snowflake.ID(1001)), 1, []uint16{2, 3})
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
	n.Temporal().SetBaseEntityID(types.EntityID(999))
	n.SetIntegrity(&types.NodeIntegrity{
		Hash:     "abc123",
		PrevHash: "def456",
	})

	w := NodeToWire(n)
	got := WireToNode(w)

	if int64(got.ID()) != 1001 {
		t.Fatalf("ID mismatch: got %d", int64(got.ID()))
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

	n := types.NewNode(types.NodeID(snowflake.ID(42)), 1, nil)
	w := NodeToWire(n)
	got := WireToNode(w)

	if got.ExtraLabelTokens() != nil {
		t.Fatal("expected no extra labels")
	}
	if got.Properties().Len() != 0 {
		t.Fatal("expected no properties")
	}
}

func TestNodeWireNilTemporal(t *testing.T) {
	t.Parallel()

	n := types.NewNode(types.NodeID(snowflake.ID(42)), 1, nil)
	w := NodeToWire(n)

	if w.HasTemporal {
		t.Fatal("HasTemporal should be false")
	}

	got := WireToNode(w)
	if got.Temporal() != nil {
		t.Fatal("temporal should be nil")
	}
}

func TestNodeWireNilIntegrity(t *testing.T) {
	t.Parallel()

	n := types.NewNode(types.NodeID(snowflake.ID(42)), 1, nil)
	w := NodeToWire(n)

	if w.Hash != "" || w.PrevHash != "" {
		t.Fatal("hash fields should be empty")
	}

	got := WireToNode(w)
	if got.Integrity() != nil {
		t.Fatal("integrity should be nil")
	}
}

func TestRelToWireAndBack(t *testing.T) {
	t.Parallel()

	r := types.NewRelationship(types.RelID(snowflake.ID(500)), 3, types.NodeID(snowflake.ID(100)), types.NodeID(snowflake.ID(200)))
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

	w := RelToWire(r)
	got := WireToRel(w)

	if int64(got.ID()) != 500 {
		t.Fatalf("ID mismatch: got %d", int64(got.ID()))
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

	r := types.NewRelationship(types.RelID(snowflake.ID(1)), 1, types.NodeID(snowflake.ID(2)), types.NodeID(snowflake.ID(3)))
	w := RelToWire(r)
	got := WireToRel(w)

	if got.Properties().Len() != 0 {
		t.Fatal("expected no properties")
	}
}

func TestRelWireNilTemporalIntegrity(t *testing.T) {
	t.Parallel()

	r := types.NewRelationship(types.RelID(snowflake.ID(1)), 1, types.NodeID(snowflake.ID(2)), types.NodeID(snowflake.ID(3)))
	w := RelToWire(r)
	got := WireToRel(w)

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

type wireCustomProperty struct {
	Name  string
	Count int
}

func (w wireCustomProperty) HashBytes() []byte {
	return []byte{byte(w.Count), byte(len(w.Name))}
}

func (w wireCustomProperty) DeepCopyValue() any { return w }

func TestPropertyWireRegisteredCustomValueRoundTrip(t *testing.T) {
	if err := types.RegisterPropertyStructType(wireCustomProperty{}); err != nil {
		t.Fatalf("RegisterPropertyStructType: %v", err)
	}

	for _, tc := range []struct {
		name    string
		value   any
		wantPtr bool
	}{
		{name: "value", value: wireCustomProperty{Name: "point", Count: 7}},
		{name: "pointer", value: &wireCustomProperty{Name: "point", Count: 8}, wantPtr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var ps types.PropertySlice
			if err := ps.Set("custom", tc.value); err != nil {
				t.Fatalf("Set: %v", err)
			}

			pw, err := propertiesToWireChecked(ps)
			if err != nil {
				t.Fatalf("propertiesToWireChecked: %v", err)
			}
			if got := pw[0].Type; got != ptCustom {
				t.Fatalf("wire type = %d, want ptCustom", got)
			}
			if pw[0].CustomType == "" {
				t.Fatal("custom property type name was not persisted")
			}
			if pw[0].CustomPointer != tc.wantPtr {
				t.Fatalf("custom pointer flag = %v, want %v", pw[0].CustomPointer, tc.wantPtr)
			}

			data, err := msgpack.Marshal(pw)
			if err != nil {
				t.Fatalf("Marshal property wire: %v", err)
			}
			var decoded []PropertyWire
			if err := msgpack.Unmarshal(data, &decoded); err != nil {
				t.Fatalf("Unmarshal property wire: %v", err)
			}
			if err := ValidatePropertyWireSlice(decoded); err != nil {
				t.Fatalf("ValidatePropertyWireSlice: %v", err)
			}

			got := wireToProperties(decoded)
			v, ok := got.Get("custom")
			if !ok {
				t.Fatal("custom property missing after wire round-trip")
			}
			if tc.wantPtr {
				gotPtr, ok := v.(*wireCustomProperty)
				if !ok {
					t.Fatalf("round-trip type = %T, want *wireCustomProperty", v)
				}
				if gotPtr.Name != "point" || gotPtr.Count != 8 {
					t.Fatalf("round-trip pointer value = %#v", gotPtr)
				}
			} else {
				gotValue, ok := v.(wireCustomProperty)
				if !ok {
					t.Fatalf("round-trip type = %T, want wireCustomProperty", v)
				}
				if gotValue.Name != "point" || gotValue.Count != 7 {
					t.Fatalf("round-trip value = %#v", gotValue)
				}
			}
		})
	}
}

type wireUnmarshalableCustomProperty struct {
	Ch chan int
}

func (w wireUnmarshalableCustomProperty) HashBytes() []byte  { return []byte("bad") }
func (w wireUnmarshalableCustomProperty) DeepCopyValue() any { return w }

func TestMarshalNodeWireRejectsUnmarshalableCustomProperty(t *testing.T) {
	if err := types.RegisterPropertyStructType(wireUnmarshalableCustomProperty{}); err != nil {
		t.Fatalf("RegisterPropertyStructType: %v", err)
	}
	n := types.NewNode(types.NodeID(snowflake.ID(1)), 1, nil)
	if err := n.SetProperties(types.PropertySlice{{
		Key:   "bad",
		Value: wireUnmarshalableCustomProperty{Ch: make(chan int)},
	}}); err != nil {
		t.Fatalf("SetProperties: %v", err)
	}

	if _, err := MarshalNodeWire(n); err == nil {
		t.Fatal("MarshalNodeWire returned nil error for unmarshalable custom property")
	}
}

type wirePanicHashCustomProperty struct {
	Name string
}

func (w wirePanicHashCustomProperty) HashBytes() []byte {
	panic("wire hash panic")
}

func (w wirePanicHashCustomProperty) DeepCopyValue() any { return w }

func TestMarshalNodeWireRejectsPanicHashCustomProperty(t *testing.T) {
	if err := types.RegisterPropertyStructType(wirePanicHashCustomProperty{}); err != nil {
		t.Fatalf("RegisterPropertyStructType: %v", err)
	}
	n := types.NewNode(types.NodeID(snowflake.ID(1)), 1, nil)
	if err := n.SetProperties(types.PropertySlice{{
		Key:   "bad",
		Value: wirePanicHashCustomProperty{Name: "boom"},
	}}); err != nil {
		t.Fatalf("SetProperties: %v", err)
	}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("MarshalNodeWire panicked: %v", r)
		}
	}()
	_, err := MarshalNodeWire(n)
	if !errors.Is(err, types.ErrUnsupportedValueType) {
		t.Fatalf("MarshalNodeWire error = %v, want ErrUnsupportedValueType", err)
	}
}

type wirePanicCopyCustomProperty struct {
	Name string
}

func (w wirePanicCopyCustomProperty) HashBytes() []byte { return []byte(w.Name) }

func (w wirePanicCopyCustomProperty) DeepCopyValue() any {
	panic("wire copy panic")
}

func panicCopyPropertyWire(t *testing.T) []PropertyWire {
	t.Helper()
	if err := types.RegisterPropertyStructType(wirePanicCopyCustomProperty{}); err != nil {
		t.Fatalf("RegisterPropertyStructType: %v", err)
	}
	value := wirePanicCopyCustomProperty{Name: "boom"}
	typeName, pointer, ok := types.RegisteredPropertyStructWireType(value)
	if !ok {
		t.Fatalf("RegisteredPropertyStructWireType(%T) returned ok=false", value)
	}
	data, err := msgpack.Marshal(value)
	if err != nil {
		t.Fatalf("marshal custom property fixture: %v", err)
	}
	return []PropertyWire{{
		Key:           "bad",
		Value:         data,
		Type:          ptCustom,
		CustomType:    typeName,
		CustomPointer: pointer,
	}}
}

func TestCheckedWireRejectsPanicDeepCopyCustomProperty(t *testing.T) {
	properties := panicCopyPropertyWire(t)

	if err := ValidatePropertyWireSlice(properties); !errors.Is(err, types.ErrUnsupportedValueType) {
		t.Fatalf("ValidatePropertyWireSlice error = %v, want ErrUnsupportedValueType", err)
	}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("checked wire reconstruction panicked: %v", r)
		}
	}()
	_, nodeErr := WireToNodeChecked(NodeWire{
		ID:           1,
		PrimaryLabel: 1,
		Properties:   properties,
	})
	if !errors.Is(nodeErr, types.ErrUnsupportedValueType) {
		t.Fatalf("WireToNodeChecked error = %v, want ErrUnsupportedValueType", nodeErr)
	}

	_, relErr := WireToRelChecked(RelWire{
		ID:         2,
		RelType:    1,
		StartID:    1,
		EndID:      3,
		Properties: properties,
	})
	if !errors.Is(relErr, types.ErrUnsupportedValueType) {
		t.Fatalf("WireToRelChecked error = %v, want ErrUnsupportedValueType", relErr)
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

	n := types.NewNode(types.NodeID(snowflake.ID(1001)), 1, []uint16{2})
	n.SetVersion(3)
	n.SetProperties(mustPropertySlice(t, map[string]any{
		"name": "Bob",
	}))

	w := NodeToWire(n)
	data, err := msgpack.Marshal(w)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var w2 NodeWire
	if err := msgpack.Unmarshal(data, &w2); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	got := WireToNode(w2)
	if int64(got.ID()) != 1001 {
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

	r := types.NewRelationship(types.RelID(snowflake.ID(500)), 3, types.NodeID(snowflake.ID(100)), types.NodeID(snowflake.ID(200)))
	r.SetVersion(7)

	w := RelToWire(r)
	data, err := msgpack.Marshal(w)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var w2 RelWire
	if err := msgpack.Unmarshal(data, &w2); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	got := WireToRel(w2)
	if int64(got.ID()) != 500 {
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
	w := NodeWire{
		ID:           1,
		PrimaryLabel: 1,
		Properties: []PropertyWire{
			{Key: "count", Value: int(42), Type: ptInt},        // int → int8 after msgpack → reconstructed to int
			{Key: "big", Value: int64(1 << 40), Type: ptInt64}, // stays int64
			{Key: "rate", Value: float64(1.5), Type: ptFloat64},
		},
	}

	data, err := msgpack.Marshal(w)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var w2 NodeWire
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
			pw := []PropertyWire{{Key: "k", Value: tc.val, Type: tc.tag}}
			data, err := msgpack.Marshal(pw)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			var pw2 []PropertyWire
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
	var pw2 []PropertyWire
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
	var pw2 []PropertyWire
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
	var pw2 []PropertyWire
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
	var pw2 []PropertyWire
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
	var pw2 []PropertyWire
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
	var pw2 []PropertyWire
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
	var pw2 []PropertyWire
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
	var pw2 []PropertyWire
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
	pw := []PropertyWire{
		{Key: "count", Value: int(42), Type: ptUnknown},
		{Key: "tags", Value: []string{"a"}, Type: ptUnknown},
	}
	data, err := msgpack.Marshal(pw)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var pw2 []PropertyWire
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
	pw := []PropertyWire{{Key: "empty", Value: nil, Type: ptString}}
	ps := wireToProperties(pw)
	if ps[0].Value != nil {
		t.Fatalf("expected nil, got %T(%v)", ps[0].Value, ps[0].Value)
	}
}

func TestPropertyWireTypeFidelityTypedNilContainers(t *testing.T) {
	t.Parallel()

	var nilStrings []string
	var nilInts []int
	var nilInt64s []int64
	var nilFloat32s []float32
	var nilFloat64s []float64
	var nilBytes []byte
	var nilBools []bool
	var nilAny []any
	var nilMapAny map[string]any
	var nilMapString map[string]string

	tests := []struct {
		name  string
		value any
		empty any
		tag   byte
	}{
		{name: "[]string", value: nilStrings, empty: []string{}, tag: ptSliceStr},
		{name: "[]int", value: nilInts, empty: []int{}, tag: ptSliceInt},
		{name: "[]int64", value: nilInt64s, empty: []int64{}, tag: ptSliceInt64},
		{name: "[]float32", value: nilFloat32s, empty: []float32{}, tag: ptSliceF32},
		{name: "[]float64", value: nilFloat64s, empty: []float64{}, tag: ptSliceF64},
		{name: "[]byte", value: nilBytes, empty: []byte{}, tag: ptSliceByte},
		{name: "[]bool", value: nilBools, empty: []bool{}, tag: ptSliceBool},
		{name: "[]any", value: nilAny, empty: []any{}, tag: ptSliceAny},
		{name: "map[string]any", value: nilMapAny, empty: map[string]any{}, tag: ptMapStrAny},
		{name: "map[string]string", value: nilMapString, empty: map[string]string{}, tag: ptMapStrStr},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ps := mustPropertySlice(t, map[string]any{"k": tc.value})
			pw := propertiesToWire(ps)
			if len(pw) != 1 {
				t.Fatalf("propertiesToWire count = %d, want 1", len(pw))
			}
			if pw[0].Type != tc.tag || !pw[0].Nil || pw[0].Value != nil {
				t.Fatalf("typed nil wire = %#v, want tag %d, nil marker, nil value", pw[0], tc.tag)
			}

			data, err := msgpack.Marshal(pw)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			var decoded []PropertyWire
			if err := msgpack.Unmarshal(data, &decoded); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if err := ValidatePropertyWireSlice(decoded); err != nil {
				t.Fatalf("ValidatePropertyWireSlice: %v", err)
			}
			got := wireToProperties(decoded)
			v, ok := got.Get("k")
			if !ok {
				t.Fatal("missing typed nil property after wire round trip")
			}
			assertWireTypedNilContainer(t, v, tc.value, tc.empty)
		})
	}
}

func assertWireTypedNilContainer(t *testing.T, got, wantNil, wantEmpty any) {
	t.Helper()
	if got == nil {
		t.Fatalf("round trip returned untyped nil for %T", wantNil)
	}
	gotValue := reflect.ValueOf(got)
	wantType := reflect.TypeOf(wantNil)
	if gotValue.Type() != wantType {
		t.Fatalf("round trip type = %T, want %v", got, wantType)
	}
	if !gotValue.IsNil() {
		t.Fatalf("round trip value = %#v, want typed nil %v", got, wantType)
	}
	if !types.PropertyValueEqual(got, wantNil) {
		t.Fatalf("round trip value does not equal typed nil %v", wantType)
	}
	if types.PropertyValueEqual(got, wantEmpty) {
		t.Fatalf("round trip typed nil compares equal to empty %T", wantEmpty)
	}
}

func TestNodeWireBaseEntityID(t *testing.T) {
	t.Parallel()

	// Non-zero base entity.
	n := types.NewNode(types.NodeID(snowflake.ID(1)), 1, nil)
	n.SetTemporal(&types.TemporalMetadata{})
	n.Temporal().SetBaseEntityID(types.EntityID(777))

	w := NodeToWire(n)
	if w.BaseEntityID != 777 {
		t.Fatalf("wire base entity: got %d", w.BaseEntityID)
	}

	got := WireToNode(w)
	if int64(got.Temporal().BaseEntityID().SnowflakeID()) != 777 {
		t.Fatal("base entity round-trip failed")
	}

	// Zero base entity.
	n2 := types.NewNode(types.NodeID(snowflake.ID(2)), 1, nil)
	n2.SetTemporal(&types.TemporalMetadata{})
	w2 := NodeToWire(n2)
	got2 := WireToNode(w2)
	if int64(got2.Temporal().BaseEntityID().SnowflakeID()) != 0 {
		t.Fatal("zero base entity should remain zero")
	}
}

func TestNodeWireTemporalZeroInstants(t *testing.T) {
	t.Parallel()

	// All-zero temporal with HasTemporal=true.
	n := types.NewNode(types.NodeID(snowflake.ID(1)), 1, nil)
	n.SetTemporal(&types.TemporalMetadata{})

	w := NodeToWire(n)
	if !w.HasTemporal {
		t.Fatal("HasTemporal should be true")
	}

	got := WireToNode(w)
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
	n := types.NewNode(types.NodeID(snowflake.ID(42)), 1, nil)
	n.SetProperties(mustPropertySlice(t, map[string]any{
		"counts": []int{1, 2, 3},
	}))

	w := NodeToWire(n)
	data, err := msgpack.Marshal(w)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var w2 NodeWire
	if err := msgpack.Unmarshal(data, &w2); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	got := WireToNode(w2)
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

// TestPropertyTypeTagAllBranches exercises every branch in PropertyTypeTag
// directly (without going through msgpack). The function is called by
// propertiesToWire, but most wire tests construct PropertyWire literals and
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
		got := PropertyTypeTag(tc.val)
		if got != tc.want {
			t.Errorf("PropertyTypeTag(%T) = %d, want %d", tc.val, got, tc.want)
		}
	}
}

func TestValidateWireBoolSlice(t *testing.T) {
	t.Parallel()

	if err := validatePropertyWireValue([]bool{true, false}, ptSliceBool); err != nil {
		t.Fatalf("validate []bool: %v", err)
	}
	if err := validatePropertyWireValue([]any{true, false}, ptSliceBool); err != nil {
		t.Fatalf("validate decoded []any bools: %v", err)
	}
	if err := validatePropertyWireValue([]any{true, "false"}, ptSliceBool); err == nil {
		t.Fatal("validate decoded []any with non-bool returned nil error")
	}
	if err := validatePropertyWireValue([]string{"true"}, ptSliceBool); err == nil {
		t.Fatal("validate []string as []bool returned nil error")
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

func TestWireInt64AllBranches(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		val  any
		want int64
		ok   bool
	}{
		{name: "int8", val: int8(-5), want: -5, ok: true},
		{name: "int16", val: int16(300), want: 300, ok: true},
		{name: "int32", val: int32(70000), want: 70000, ok: true},
		{name: "int64", val: int64(1 << 40), want: 1 << 40, ok: true},
		{name: "int", val: int(42), want: 42, ok: true},
		{name: "uint8", val: uint8(200), want: 200, ok: true},
		{name: "uint16", val: uint16(50000), want: 50000, ok: true},
		{name: "uint32", val: uint32(3000000000), want: 3000000000, ok: true},
		{name: "uint64", val: uint64(999), want: 999, ok: true},
		{name: "uint64 overflow", val: uint64(maxInt64) + 1, ok: false},
		{name: "uint", val: uint(77), want: 77, ok: true},
		{name: "unsupported", val: "unsupported", ok: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := wireInt64(tc.val)
			if ok != tc.ok || got != tc.want {
				t.Fatalf("wireInt64(%T(%v)) = %d, %v; want %d, %v", tc.val, tc.val, got, ok, tc.want, tc.ok)
			}
		})
	}

	if strconv.IntSize == 64 {
		overflow64 := uint64(maxInt64)
		if got, ok := wireInt64(uint(overflow64 + 1)); ok || got != 0 {
			t.Fatalf("wireInt64(uint overflow) = %d, %v; want 0, false", got, ok)
		}
	}
}

func TestWireUint64AllBranches(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		val  any
		want uint64
		ok   bool
	}{
		{name: "uint8", val: uint8(200), want: 200, ok: true},
		{name: "uint16", val: uint16(50000), want: 50000, ok: true},
		{name: "uint32", val: uint32(3000000000), want: 3000000000, ok: true},
		{name: "uint64", val: uint64(1 << 50), want: 1 << 50, ok: true},
		{name: "uint", val: uint(42), want: 42, ok: true},
		{name: "int8 positive", val: int8(7), want: 7, ok: true},
		{name: "int8 negative", val: int8(-1), ok: false},
		{name: "int16 positive", val: int16(300), want: 300, ok: true},
		{name: "int16 negative", val: int16(-1), ok: false},
		{name: "int32 positive", val: int32(70000), want: 70000, ok: true},
		{name: "int32 negative", val: int32(-1), ok: false},
		{name: "int64 positive", val: int64(999), want: 999, ok: true},
		{name: "int64 negative", val: int64(-1), ok: false},
		{name: "int positive", val: int(123), want: 123, ok: true},
		{name: "int negative", val: int(-1), ok: false},
		{name: "unsupported", val: "unsupported", ok: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := wireUint64(tc.val)
			if ok != tc.ok || got != tc.want {
				t.Fatalf("wireUint64(%T(%v)) = %d, %v; want %d, %v", tc.val, tc.val, got, ok, tc.want, tc.ok)
			}
		})
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

func TestValidatePropertyWireSliceRejectsLossyTaggedValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		pw   []PropertyWire
	}{
		{
			name: "negative value with unsigned tag",
			pw:   []PropertyWire{{Key: "a", Value: int64(-1), Type: ptUint8}},
		},
		{
			name: "out of range int8",
			pw:   []PropertyWire{{Key: "a", Value: int64(200), Type: ptInt8}},
		},
		{
			name: "string slice tag with mixed element",
			pw:   []PropertyWire{{Key: "a", Value: []any{"ok", int64(1)}, Type: ptSliceStr}},
		},
		{
			name: "string map tag with non-string value",
			pw:   []PropertyWire{{Key: "a", Value: map[string]any{"k": int64(1)}, Type: ptMapStrStr}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if err := ValidatePropertyWireSlice(tc.pw); err == nil {
				t.Fatal("ValidatePropertyWireSlice returned nil, want error")
			}
		})
	}
}

func TestValidatePropertyWireSliceAcceptsCanonicalTaggedValues(t *testing.T) {
	t.Parallel()

	pw := []PropertyWire{
		{Key: "a", Value: int64(-8), Type: ptInt8},
		{Key: "b", Value: uint64(255), Type: ptUint8},
		{Key: "c", Value: []any{"x", "y"}, Type: ptSliceStr},
		{Key: "d", Value: map[string]any{"k": "v"}, Type: ptMapStrStr},
		{Key: "e", Value: []any{float64(1), float64(2)}, Type: ptSliceF32},
	}

	if err := ValidatePropertyWireSlice(pw); err != nil {
		t.Fatalf("ValidatePropertyWireSlice: %v", err)
	}
}

func TestValidatePropertyWireSliceAcceptsFloat32SpecialValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		pw   []PropertyWire
	}{
		{
			name: "scalar nan",
			pw:   []PropertyWire{{Key: "a", Value: math.NaN(), Type: ptFloat32}},
		},
		{
			name: "scalar infinity",
			pw:   []PropertyWire{{Key: "a", Value: math.Inf(1), Type: ptFloat32}},
		},
		{
			name: "any slice nan and infinity",
			pw:   []PropertyWire{{Key: "a", Value: []any{math.NaN(), math.Inf(-1)}, Type: ptSliceF32}},
		},
		{
			name: "typed slice nan and infinity",
			pw:   []PropertyWire{{Key: "a", Value: []float32{float32(math.NaN()), float32(math.Inf(1))}, Type: ptSliceF32}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if err := ValidatePropertyWireSlice(tc.pw); err != nil {
				t.Fatalf("ValidatePropertyWireSlice: %v", err)
			}
		})
	}
}

func TestValidatePropertyWireSliceRejectsFloat32FiniteOverflow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		pw   []PropertyWire
	}{
		{
			name: "scalar overflow",
			pw:   []PropertyWire{{Key: "a", Value: maxFloat32 * 2, Type: ptFloat32}},
		},
		{
			name: "slice overflow",
			pw:   []PropertyWire{{Key: "a", Value: []any{maxFloat32 * 2}, Type: ptSliceF32}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if err := ValidatePropertyWireSlice(tc.pw); err == nil {
				t.Fatal("ValidatePropertyWireSlice returned nil, want error")
			}
		})
	}
}

func TestValidatePropertyWireSliceRejectsPrecisionLosingFloat32DecodedValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		pw   []PropertyWire
	}{
		{
			name: "scalar precision loss",
			pw:   []PropertyWire{{Key: "a", Value: float64(0.1), Type: ptFloat32}},
		},
		{
			name: "slice precision loss",
			pw:   []PropertyWire{{Key: "a", Value: []any{float64(0.1)}, Type: ptSliceF32}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if err := ValidatePropertyWireSlice(tc.pw); err == nil {
				t.Fatal("ValidatePropertyWireSlice returned nil, want precision-loss error")
			}
		})
	}
}

func TestWireToNodeCheckedReconstructsFloat64TagsFromFloat32DecodedValues(t *testing.T) {
	t.Parallel()

	n, err := WireToNodeChecked(NodeWire{
		ID:           1,
		PrimaryLabel: 1,
		Properties: []PropertyWire{
			{Key: "scalar", Value: float32(1.25), Type: ptFloat64},
			{Key: "slice", Value: []any{float32(2.5), float64(3.5)}, Type: ptSliceF64},
		},
	})
	if err != nil {
		t.Fatalf("WireToNodeChecked: %v", err)
	}

	scalar, ok := n.GetProperty("scalar")
	if !ok {
		t.Fatal("missing scalar property")
	}
	if _, ok := scalar.(float64); !ok {
		t.Fatalf("scalar type = %T, want float64", scalar)
	}
	if scalar != float64(1.25) {
		t.Fatalf("scalar = %v, want 1.25", scalar)
	}

	sliceValue, ok := n.GetProperty("slice")
	if !ok {
		t.Fatal("missing slice property")
	}
	slice, ok := sliceValue.([]float64)
	if !ok {
		t.Fatalf("slice type = %T, want []float64", sliceValue)
	}
	if len(slice) != 2 || slice[0] != 2.5 || slice[1] != 3.5 {
		t.Fatalf("slice = %v, want [2.5 3.5]", slice)
	}
}

func TestWireToNodeCheckedReconstructsIntSliceTagFromInt64DecodedValues(t *testing.T) {
	t.Parallel()

	n, err := WireToNodeChecked(NodeWire{
		ID:           1,
		PrimaryLabel: 1,
		Properties: []PropertyWire{
			{Key: "ints", Value: []int64{1, 2, 3}, Type: ptSliceInt},
		},
	})
	if err != nil {
		t.Fatalf("WireToNodeChecked: %v", err)
	}

	value, ok := n.GetProperty("ints")
	if !ok {
		t.Fatal("missing ints property")
	}
	ints, ok := value.([]int)
	if !ok {
		t.Fatalf("ints type = %T, want []int", value)
	}
	if len(ints) != 3 || ints[0] != 1 || ints[1] != 2 || ints[2] != 3 {
		t.Fatalf("ints = %v, want [1 2 3]", ints)
	}
}

func TestWireToNodeCheckedReconstructsUintDecodedValues(t *testing.T) {
	t.Parallel()

	n, err := WireToNodeChecked(NodeWire{
		ID:           1,
		PrimaryLabel: 1,
		Properties: []PropertyWire{
			{Key: "int_scalar", Value: uint(7), Type: ptInt},
			{Key: "int_slice", Value: []any{uint(8), uint16(9)}, Type: ptSliceInt64},
			{Key: "uint_scalar", Value: uint(10), Type: ptUint},
		},
	})
	if err != nil {
		t.Fatalf("WireToNodeChecked: %v", err)
	}

	intScalar, ok := n.GetProperty("int_scalar")
	if !ok {
		t.Fatal("missing int_scalar property")
	}
	if got, ok := intScalar.(int); !ok || got != 7 {
		t.Fatalf("int_scalar = %#v (%T), want int(7)", intScalar, intScalar)
	}

	intSliceValue, ok := n.GetProperty("int_slice")
	if !ok {
		t.Fatal("missing int_slice property")
	}
	intSlice, ok := intSliceValue.([]int64)
	if !ok {
		t.Fatalf("int_slice type = %T, want []int64", intSliceValue)
	}
	if len(intSlice) != 2 || intSlice[0] != 8 || intSlice[1] != 9 {
		t.Fatalf("int_slice = %v, want [8 9]", intSlice)
	}

	uintScalar, ok := n.GetProperty("uint_scalar")
	if !ok {
		t.Fatal("missing uint_scalar property")
	}
	if got, ok := uintScalar.(uint); !ok || got != 10 {
		t.Fatalf("uint_scalar = %#v (%T), want uint(10)", uintScalar, uintScalar)
	}
}

func TestNodeToWireCheckedRejectsNilNode(t *testing.T) {
	t.Parallel()
	if _, err := NodeToWireChecked(nil); err == nil {
		t.Fatal("NodeToWireChecked(nil) returned nil error")
	}
}

func TestRelToWireCheckedRejectsNilRelationship(t *testing.T) {
	t.Parallel()
	if _, err := RelToWireChecked(nil); err == nil {
		t.Fatal("RelToWireChecked(nil) returned nil error")
	}
}

func TestWireToNodeCheckedRejectsSemanticCorruption(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		wire NodeWire
	}{
		{name: "zero primary label", wire: NodeWire{ID: 1, PrimaryLabel: 0}},
		{name: "token truncation", wire: NodeWire{ID: 1, PrimaryLabel: 1 << 16}},
		{name: "duplicate extra label", wire: NodeWire{ID: 1, PrimaryLabel: 1, ExtraLabels: []int{2, 2}}},
		{name: "temporal payload without flag", wire: NodeWire{ID: 1, PrimaryLabel: 1, TxFrom: 10}},
		{name: "temporal author without flag", wire: NodeWire{ID: 1, PrimaryLabel: 1, CreatedBy: "writer"}},
		{name: "empty explicit temporal range", wire: NodeWire{ID: 1, PrimaryLabel: 1, ValidFrom: 20, ValidTo: 20}},
		{name: "reversed explicit temporal range", wire: NodeWire{ID: 1, PrimaryLabel: 1, ValidFrom: 30, ValidTo: 20}},
		{name: "reserved property", wire: NodeWire{ID: 1, PrimaryLabel: 1, Properties: []PropertyWire{{Key: "tkg_hash", Value: "x", Type: ptString}}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if _, err := WireToNodeChecked(tc.wire); err == nil {
				t.Fatal("WireToNodeChecked returned nil error")
			}
		})
	}
}

func TestWireToRelCheckedRejectsSemanticCorruption(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		wire RelWire
	}{
		{name: "zero rel type", wire: RelWire{ID: 1, RelType: 0, StartID: 2, EndID: 3}},
		{name: "missing start", wire: RelWire{ID: 1, RelType: 1, StartID: 0, EndID: 3}},
		{name: "token truncation", wire: RelWire{ID: 1, RelType: 1 << 16, StartID: 2, EndID: 3}},
		{name: "temporal payload without flag", wire: RelWire{ID: 1, RelType: 1, StartID: 2, EndID: 3, TxFrom: 10}},
		{name: "temporal author without flag", wire: RelWire{ID: 1, RelType: 1, StartID: 2, EndID: 3, UpdatedBy: "writer"}},
		{name: "empty explicit temporal range", wire: RelWire{ID: 1, RelType: 1, StartID: 2, EndID: 3, ValidFrom: 20, ValidTo: 20}},
		{name: "reversed explicit temporal range", wire: RelWire{ID: 1, RelType: 1, StartID: 2, EndID: 3, ValidFrom: 30, ValidTo: 20}},
		{name: "reserved property", wire: RelWire{ID: 1, RelType: 1, StartID: 2, EndID: 3, Properties: []PropertyWire{{Key: "tkg_hash", Value: "x", Type: ptString}}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if _, err := WireToRelChecked(tc.wire); err == nil {
				t.Fatal("WireToRelChecked returned nil error")
			}
		})
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
