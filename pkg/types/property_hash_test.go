package types

import (
	"bytes"
	"encoding/binary"
	"testing"
)

type propertyHashCustom struct {
	X int
}

func (p propertyHashCustom) HashBytes() []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(p.X))
	return buf[:]
}

func (p propertyHashCustom) DeepCopyValue() any { return p }

type propertyHashUnregistered struct{}

func (p propertyHashUnregistered) HashBytes() []byte { return []byte("unregistered") }

func (p propertyHashUnregistered) DeepCopyValue() any { return p }

type propertyHashOtherCustom struct {
	X int
}

func (p propertyHashOtherCustom) HashBytes() []byte {
	return propertyHashCustom(p).HashBytes()
}

func (p propertyHashOtherCustom) DeepCopyValue() any { return p }

func TestPropertyHashTypeTagAllBranches(t *testing.T) {
	t.Cleanup(resetRegistry)
	resetRegistry()
	if err := RegisterPropertyStructType(propertyHashCustom{}); err != nil {
		t.Fatalf("RegisterPropertyStructType: %v", err)
	}

	tests := []struct {
		name string
		val  any
		want byte
	}{
		{"nil", nil, propertyHashTypeUnknown},
		{"bool", true, propertyHashTypeBool},
		{"int", int(1), propertyHashTypeInt},
		{"int8", int8(1), propertyHashTypeInt8},
		{"int16", int16(1), propertyHashTypeInt16},
		{"int32", int32(1), propertyHashTypeInt32},
		{"int64", int64(1), propertyHashTypeInt64},
		{"uint", uint(1), propertyHashTypeUint},
		{"uint8", uint8(1), propertyHashTypeUint8},
		{"uint16", uint16(1), propertyHashTypeUint16},
		{"uint32", uint32(1), propertyHashTypeUint32},
		{"uint64", uint64(1), propertyHashTypeUint64},
		{"float32", float32(1), propertyHashTypeFloat32},
		{"float64", float64(1), propertyHashTypeFloat64},
		{"string", "x", propertyHashTypeString},
		{"slice string", []string{"x"}, propertyHashTypeSliceStr},
		{"slice int", []int{1}, propertyHashTypeSliceInt},
		{"slice int64", []int64{1}, propertyHashTypeSliceInt64},
		{"slice float32", []float32{1}, propertyHashTypeSliceF32},
		{"slice float64", []float64{1}, propertyHashTypeSliceF64},
		{"slice byte", []byte("x"), propertyHashTypeSliceByte},
		{"slice bool", []bool{true}, propertyHashTypeSliceBool},
		{"slice any", []any{int64(1)}, propertyHashTypeSliceAny},
		{"map string any", map[string]any{"x": int64(1)}, propertyHashTypeMapStrAny},
		{"map string string", map[string]string{"x": "y"}, propertyHashTypeMapStrStr},
		{"custom", propertyHashCustom{X: 1}, propertyHashTypeCustom},
		{"unknown", struct{ X int }{X: 1}, propertyHashTypeUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := PropertyHashTypeTag(tt.val); got != tt.want {
				t.Fatalf("PropertyHashTypeTag(%T) = %d, want %d", tt.val, got, tt.want)
			}
		})
	}
}

func TestAppendPropertyValueHashBytesAllBranches(t *testing.T) {
	t.Cleanup(resetRegistry)
	resetRegistry()
	if err := RegisterPropertyStructType(propertyHashCustom{}); err != nil {
		t.Fatalf("RegisterPropertyStructType: %v", err)
	}

	values := []struct {
		name string
		val  any
	}{
		{"nil", nil},
		{"bool", true},
		{"int", int(1)},
		{"int8", int8(1)},
		{"int16", int16(1)},
		{"int32", int32(1)},
		{"int64", int64(1)},
		{"uint", uint(1)},
		{"uint8", uint8(1)},
		{"uint16", uint16(1)},
		{"uint32", uint32(1)},
		{"uint64", uint64(1)},
		{"float32", float32(1.25)},
		{"float64", float64(1.25)},
		{"string", "x"},
		{"slice string", []string{"x", "y"}},
		{"slice int", []int{1, 2}},
		{"slice int64", []int64{1, 2}},
		{"slice float32", []float32{1, 2}},
		{"slice float64", []float64{1, 2}},
		{"slice byte", []byte("xy")},
		{"slice bool", []bool{true, false}},
		{"slice any", []any{int64(1), "x", true}},
		{"map string any", map[string]any{"b": int64(2), "a": "one"}},
		{"map string string", map[string]string{"b": "2", "a": "1"}},
		{"custom", propertyHashCustom{X: 7}},
	}
	for _, tt := range values {
		t.Run(tt.name, func(t *testing.T) {
			first := AppendPropertyValueHashBytes(nil, tt.val)
			second := AppendPropertyValueHashBytes(nil, tt.val)
			if !bytes.Equal(first, second) {
				t.Fatalf("AppendPropertyValueHashBytes(%T) nondeterministic: %v vs %v", tt.val, first, second)
			}
			if len(first) == 0 {
				t.Fatalf("AppendPropertyValueHashBytes(%T) returned empty encoding", tt.val)
			}
		})
	}
}

func TestAppendPropertyValueHashBytesDistinguishesTypedNilContainers(t *testing.T) {
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
		nil   any
		empty any
	}{
		{name: "[]string", nil: nilStrings, empty: []string{}},
		{name: "[]int", nil: nilInts, empty: []int{}},
		{name: "[]int64", nil: nilInt64s, empty: []int64{}},
		{name: "[]float32", nil: nilFloat32s, empty: []float32{}},
		{name: "[]float64", nil: nilFloat64s, empty: []float64{}},
		{name: "[]byte", nil: nilBytes, empty: []byte{}},
		{name: "[]bool", nil: nilBools, empty: []bool{}},
		{name: "[]any", nil: nilAny, empty: []any{}},
		{name: "map[string]any", nil: nilMapAny, empty: map[string]any{}},
		{name: "map[string]string", nil: nilMapString, empty: map[string]string{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nilHash := AppendPropertyValueHashBytes(nil, tt.nil)
			emptyHash := AppendPropertyValueHashBytes(nil, tt.empty)
			if bytes.Equal(nilHash, emptyHash) {
				t.Fatalf("typed nil and empty %s produced identical hash bytes %v", tt.name, nilHash)
			}
		})
	}
}

func TestAppendPropertyValueHashBytesUnsupportedPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic")
		}
	}()
	_ = AppendPropertyValueHashBytes(nil, struct{ X int }{X: 1})
}

func TestAppendPropertyValueHashBytesUnregisteredHashablePanics(t *testing.T) {
	t.Cleanup(resetRegistry)
	resetRegistry()

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic")
		}
	}()
	_ = AppendPropertyValueHashBytes(nil, propertyHashUnregistered{})
}

func TestAppendPropertyValueHashBytesCustomIncludesTypeAndPointerShape(t *testing.T) {
	t.Cleanup(resetRegistry)
	resetRegistry()
	if err := RegisterPropertyStructType(propertyHashCustom{}); err != nil {
		t.Fatalf("RegisterPropertyStructType custom: %v", err)
	}
	if err := RegisterPropertyStructType(propertyHashOtherCustom{}); err != nil {
		t.Fatalf("RegisterPropertyStructType other: %v", err)
	}

	value := AppendPropertyValueHashBytes(nil, propertyHashCustom{X: 7})
	sameHashDifferentType := AppendPropertyValueHashBytes(nil, propertyHashOtherCustom{X: 7})
	if bytes.Equal(value, sameHashDifferentType) {
		t.Fatal("custom property hash bytes ignored registered type name")
	}

	pointer := AppendPropertyValueHashBytes(nil, &propertyHashCustom{X: 7})
	if bytes.Equal(value, pointer) {
		t.Fatal("custom property hash bytes ignored pointer/value shape")
	}
}

func TestPropertySliceAppendHashBytesDirect(t *testing.T) {
	var nilProps PropertySlice
	if got := nilProps.AppendHashBytes([]byte("prefix")); string(got) != "prefix" {
		t.Fatalf("nil PropertySlice AppendHashBytes = %q, want prefix", got)
	}

	ps, err := NewPropertySlice(map[string]any{
		"b": int64(2),
		"a": "one",
	})
	if err != nil {
		t.Fatalf("NewPropertySlice: %v", err)
	}
	first := ps.AppendHashBytes(nil)
	second := ps.AppendHashBytes(nil)
	if !bytes.Equal(first, second) {
		t.Fatalf("PropertySlice AppendHashBytes nondeterministic: %v vs %v", first, second)
	}

	changed, err := NewPropertySlice(map[string]any{
		"b": int64(3),
		"a": "one",
	})
	if err != nil {
		t.Fatalf("NewPropertySlice changed: %v", err)
	}
	if bytes.Equal(first, changed.AppendHashBytes(nil)) {
		t.Fatal("PropertySlice AppendHashBytes ignored property value change")
	}
}

func TestPropertyValueHashNeedsRecoverBranches(t *testing.T) {
	tests := []struct {
		name string
		val  any
		want bool
	}{
		{"nil", nil, false},
		{"primitive", int64(1), false},
		{"slice any primitive", []any{int64(1), map[string]any{"ok": true}}, false},
		{"slice any hashable", []any{propertyHashCustom{X: 1}}, true},
		{"map string any primitive", map[string]any{"ok": []any{"yes"}}, false},
		{"map string any hashable", map[string]any{"h": propertyHashCustom{X: 1}}, true},
		{"map string any unsupported", map[string]any{"bad": struct{ X int }{X: 1}}, true},
		{"map string string", map[string]string{"x": "y"}, false},
		{"hashable", propertyHashCustom{X: 1}, true},
		{"unsupported non hashable", struct{ X int }{X: 1}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := PropertyValueHashNeedsRecover(tt.val); got != tt.want {
				t.Fatalf("PropertyValueHashNeedsRecover(%T) = %v, want %v", tt.val, got, tt.want)
			}
		})
	}
}

func TestPropertyValueHashNeedsRecoverBoundsCyclicContainers(t *testing.T) {
	cyclicSlice := make([]any, 1)
	cyclicSlice[0] = cyclicSlice
	if !PropertyValueHashNeedsRecover(cyclicSlice) {
		t.Fatal("PropertyValueHashNeedsRecover(cyclic []any) = false, want true")
	}

	cyclicMap := map[string]any{}
	cyclicMap["self"] = cyclicMap
	if !PropertyValueHashNeedsRecover(cyclicMap) {
		t.Fatal("PropertyValueHashNeedsRecover(cyclic map[string]any) = false, want true")
	}
}

func TestAppendPropertyValueHashBytesBoundsCyclicContainers(t *testing.T) {
	tests := []struct {
		name string
		val  any
	}{
		{
			name: "slice",
			val: func() any {
				v := make([]any, 1)
				v[0] = v
				return v
			}(),
		},
		{
			name: "map",
			val: func() any {
				v := map[string]any{}
				v["self"] = v
				return v
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatal("expected recoverable panic for cyclic property value")
				}
			}()
			_ = AppendPropertyValueHashBytes(nil, tt.val)
		})
	}
}

func TestNodeRelationshipAppendPropertyHashBytesParityAndNil(t *testing.T) {
	var nilNode *Node
	var nilRel *Relationship
	if got := nilNode.AppendPropertyHashBytes([]byte("prefix")); string(got) != "prefix" {
		t.Fatalf("nil node append = %q, want prefix", got)
	}
	if nilNode.PropertyHashNeedsRecover() {
		t.Fatal("nil node PropertyHashNeedsRecover = true")
	}
	if got := nilRel.AppendPropertyHashBytes([]byte("prefix")); string(got) != "prefix" {
		t.Fatalf("nil rel append = %q, want prefix", got)
	}
	if nilRel.PropertyHashNeedsRecover() {
		t.Fatal("nil rel PropertyHashNeedsRecover = true")
	}

	props := map[string]any{
		"a": int64(1),
		"b": []float32{1, 2, 3},
	}
	owned, err := NewOwnedPropertySlice(props)
	if err != nil {
		t.Fatalf("NewOwnedPropertySlice: %v", err)
	}
	n := NewNode(1, 1, nil)
	if err := n.SetOwnedProperties(owned); err != nil {
		t.Fatalf("Node.SetOwnedProperties: %v", err)
	}
	owned, err = NewOwnedPropertySlice(props)
	if err != nil {
		t.Fatalf("NewOwnedPropertySlice relationship: %v", err)
	}
	r := NewRelationship(2, 1, 1, 3)
	if err := r.SetOwnedProperties(owned); err != nil {
		t.Fatalf("Relationship.SetOwnedProperties: %v", err)
	}
	if got, want := n.AppendPropertyHashBytes(nil), r.AppendPropertyHashBytes(nil); !bytes.Equal(got, want) {
		t.Fatalf("node/rel property hash bytes differ for same properties: %v vs %v", got, want)
	}
}

func TestNodeRelationshipPropertyHashNeedsRecoverForBrokenInvariant(t *testing.T) {
	unsupported := struct{ X int }{X: 1}
	n := NewNode(1, 1, nil)
	n.properties = PropertySlice{{Key: "bad", Value: unsupported}}
	if !n.PropertyHashNeedsRecover() {
		t.Fatal("node with unsupported property did not request checked hash recovery")
	}

	r := NewRelationship(1, 1, 1, 2)
	r.properties = PropertySlice{{Key: "bad", Value: unsupported}}
	if !r.PropertyHashNeedsRecover() {
		t.Fatal("relationship with unsupported property did not request checked hash recovery")
	}
}
