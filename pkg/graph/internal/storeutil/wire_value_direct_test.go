package storeutil

import (
	"errors"
	"reflect"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
)

type wireValueDirectCustom struct {
	X int
}

func (v wireValueDirectCustom) HashBytes() []byte {
	return []byte{byte(v.X)}
}

func (v wireValueDirectCustom) DeepCopyValue() any { return v }

func TestValidatePropertyWireValueDirectBranches(t *testing.T) {
	tests := []struct {
		name    string
		value   any
		tag     byte
		wantErr bool
	}{
		{name: "unknown accepts nil", value: nil, tag: ptUnknown},
		{name: "nonzero tag rejects nil", value: nil, tag: ptString, wantErr: true},
		{name: "bool accepts bool", value: true, tag: ptBool},
		{name: "bool rejects string", value: "true", tag: ptBool, wantErr: true},
		{name: "int accepts decoded int64", value: int64(7), tag: ptInt},
		{name: "int rejects string", value: "7", tag: ptInt, wantErr: true},
		{name: "int8 accepts decoded int64", value: int64(127), tag: ptInt8},
		{name: "int8 rejects overflow", value: int64(128), tag: ptInt8, wantErr: true},
		{name: "uint8 accepts decoded int64", value: int64(255), tag: ptUint8},
		{name: "uint8 rejects negative", value: int64(-1), tag: ptUint8, wantErr: true},
		{name: "float32 accepts decoded float64", value: float64(1.5), tag: ptFloat32},
		{name: "float32 rejects string", value: "1.5", tag: ptFloat32, wantErr: true},
		{name: "float64 accepts float32", value: float32(1.5), tag: ptFloat64},
		{name: "string accepts string", value: "x", tag: ptString},
		{name: "slice byte accepts bytes", value: []byte("x"), tag: ptSliceByte},
		{name: "slice any accepts any slice", value: []any{int64(1)}, tag: ptSliceAny},
		{name: "map string any accepts map", value: map[string]any{"k": int64(1)}, tag: ptMapStrAny},
		{name: "custom accepts bytes", value: []byte("payload"), tag: ptCustom},
		{name: "custom rejects string", value: "payload", tag: ptCustom, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validatePropertyWireValue(tt.value, tt.tag)
			if tt.wantErr && err == nil {
				t.Fatal("validatePropertyWireValue returned nil, want error")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("validatePropertyWireValue: %v", err)
			}
		})
	}
}

func TestPropertyToWireRejectsUnsupportedUnknownValue(t *testing.T) {
	_, err := propertyToWire(types.Property{
		Key:   "bad",
		Value: struct{ X int }{X: 1},
	})
	if !errors.Is(err, types.ErrUnsupportedValueType) {
		t.Fatalf("propertyToWire unsupported value error = %v, want ErrUnsupportedValueType", err)
	}

	pw, err := propertyToWire(types.Property{Key: "nil", Value: nil})
	if err != nil {
		t.Fatalf("propertyToWire nil: %v", err)
	}
	if pw.Type != ptUnknown || pw.Value != nil {
		t.Fatalf("propertyToWire nil = %#v, want ptUnknown nil value", pw)
	}
}

func TestValidatePropertyWireSliceRejectsInvalidTypedNilMarkers(t *testing.T) {
	tests := []struct {
		name string
		wire PropertyWire
	}{
		{
			name: "non-nillable tag",
			wire: PropertyWire{Key: "a", Type: ptString, Nil: true},
		},
		{
			name: "value present",
			wire: PropertyWire{Key: "a", Type: ptSliceF32, Value: []float32{}, Nil: true},
		},
		{
			name: "custom tag",
			wire: PropertyWire{Key: "a", Type: ptCustom, Nil: true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidatePropertyWireSlice([]PropertyWire{tt.wire}); err == nil {
				t.Fatal("ValidatePropertyWireSlice returned nil, want typed nil marker error")
			}
		})
	}
}

func TestValidateWireSliceAndMapDirectBranches(t *testing.T) {
	tests := []struct {
		name    string
		run     func() error
		wantErr bool
	}{
		{name: "string slice typed", run: func() error { return validateWireStringSlice([]string{"a"}) }},
		{name: "string slice decoded", run: func() error { return validateWireStringSlice([]any{"a"}) }},
		{name: "string slice bad decoded element", run: func() error { return validateWireStringSlice([]any{"a", int64(1)}) }, wantErr: true},
		{name: "string slice bad shape", run: func() error { return validateWireStringSlice("a") }, wantErr: true},
		{name: "int slice typed int", run: func() error { return validateWireIntSlice([]int{1}, minInt64, maxInt64, "int") }},
		{name: "int64 tag rejects typed int slice", run: func() error { return validateWireIntSlice([]int{1}, minInt64, maxInt64, "int64") }, wantErr: true},
		{name: "int slice accepts decoded int64", run: func() error { return validateWireIntSlice([]int64{1}, minInt64, maxInt64, "int") }},
		{name: "int slice rejects typed out of range", run: func() error { return validateWireIntSlice([]int64{128}, minInt8, maxInt8, "int8") }, wantErr: true},
		{name: "int slice accepts decoded any integers", run: func() error { return validateWireIntSlice([]any{int8(1), uint16(2)}, minInt64, maxInt64, "int64") }},
		{name: "int slice rejects decoded bad type", run: func() error { return validateWireIntSlice([]any{int64(1), "2"}, minInt64, maxInt64, "int64") }, wantErr: true},
		{name: "int slice rejects decoded out of range", run: func() error { return validateWireIntSlice([]any{int64(128)}, minInt8, maxInt8, "int8") }, wantErr: true},
		{name: "int slice rejects bad shape", run: func() error { return validateWireIntSlice("1", minInt64, maxInt64, "int64") }, wantErr: true},
		{name: "float64 slice typed", run: func() error { return validateWireFloat64Slice([]float64{1.5}) }},
		{name: "float64 slice decoded", run: func() error { return validateWireFloat64Slice([]any{float32(1.5), float64(2.5)}) }},
		{name: "float64 slice bad decoded element", run: func() error { return validateWireFloat64Slice([]any{float64(1), "2"}) }, wantErr: true},
		{name: "float64 slice bad shape", run: func() error { return validateWireFloat64Slice("1") }, wantErr: true},
		{name: "string map typed", run: func() error { return validateWireStringMap(map[string]string{"k": "v"}) }},
		{name: "string map decoded", run: func() error { return validateWireStringMap(map[string]any{"k": "v"}) }},
		{name: "string map bad decoded value", run: func() error { return validateWireStringMap(map[string]any{"k": int64(1)}) }, wantErr: true},
		{name: "string map bad shape", run: func() error { return validateWireStringMap("v") }, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.run()
			if tt.wantErr && err == nil {
				t.Fatal("validator returned nil, want error")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("validator: %v", err)
			}
		})
	}
}

func TestWireFloatAndCollectionConversionFallbacks(t *testing.T) {
	if got, ok := wireFloat64(float32(1.25)); !ok || got != 1.25 {
		t.Fatalf("wireFloat64(float32) = (%v, %v), want (1.25, true)", got, ok)
	}
	if got, ok := wireFloat64("1.25"); ok || got != 0 {
		t.Fatalf("wireFloat64(string) = (%v, %v), want (0, false)", got, ok)
	}

	if got := toFloat64Slice([]any{float32(1.25), "bad", float64(2.5)}); !reflect.DeepEqual(got, []float64{1.25, 0, 2.5}) {
		t.Fatalf("toFloat64Slice decoded = %#v", got)
	}
	if got := toFloat64Slice("bad"); got != nil {
		t.Fatalf("toFloat64Slice bad shape = %#v, want nil", got)
	}
	if got := toBoolSlice([]any{true, "false", false}); !reflect.DeepEqual(got, []bool{true, false, false}) {
		t.Fatalf("toBoolSlice decoded = %#v", got)
	}
	if got := toBoolSlice("bad"); got != nil {
		t.Fatalf("toBoolSlice bad shape = %#v, want nil", got)
	}
	if got := toStringStringMap(map[string]any{"a": "b", "c": int64(1)}); !reflect.DeepEqual(got, map[string]string{"a": "b", "c": ""}) {
		t.Fatalf("toStringStringMap decoded = %#v", got)
	}
	if got := toStringStringMap("bad"); got != nil {
		t.Fatalf("toStringStringMap bad shape = %#v, want nil", got)
	}
}

func TestReconstructCustomPropertyValueDirectBranches(t *testing.T) {
	if err := types.RegisterPropertyStructType(wireValueDirectCustom{}); err != nil {
		t.Fatalf("RegisterPropertyStructType: %v", err)
	}

	if _, err := reconstructCustomPropertyValue(nil, "", false); err == nil {
		t.Fatal("missing custom type name returned nil error")
	}
	if _, err := reconstructCustomPropertyValue(nil, "not/registered", false); err == nil {
		t.Fatal("unregistered custom type returned nil error")
	}
	if _, err := reconstructCustomPropertyValue([]byte{0xff}, "storeutil.wireValueDirectCustom", false); err == nil {
		t.Fatal("invalid custom payload returned nil error")
	}

	data, err := msgpack.Marshal(wireValueDirectCustom{X: 7})
	if err != nil {
		t.Fatalf("msgpack.Marshal: %v", err)
	}
	value, err := reconstructCustomPropertyValue(data, "storeutil.wireValueDirectCustom", false)
	if err != nil {
		t.Fatalf("reconstruct value: %v", err)
	}
	if got, ok := value.(wireValueDirectCustom); !ok || got.X != 7 {
		t.Fatalf("reconstruct value = %#v, want wireValueDirectCustom{X:7}", value)
	}

	ptr, err := reconstructCustomPropertyValue(data, "storeutil.wireValueDirectCustom", true)
	if err != nil {
		t.Fatalf("reconstruct pointer: %v", err)
	}
	if got, ok := ptr.(*wireValueDirectCustom); !ok || got.X != 7 {
		t.Fatalf("reconstruct pointer = %#v, want *wireValueDirectCustom{X:7}", ptr)
	}
}

func TestValidatePropertyWireSliceRejectsCustomWireShape(t *testing.T) {
	err := ValidatePropertyWireSlice([]PropertyWire{{
		Key:        "custom",
		Value:      "not bytes",
		Type:       ptCustom,
		CustomType: "storeutil.wireValueDirectCustom",
	}})
	if err == nil {
		t.Fatal("ValidatePropertyWireSlice returned nil, want custom shape error")
	}
	if errors.Is(err, types.ErrUnsupportedValueType) {
		t.Fatalf("ValidatePropertyWireSlice error = %v, did not expect unsupported type sentinel for shape mismatch", err)
	}
}

func TestValidatePropertyWireSliceRejectsCustomMetadataOnNonCustomTags(t *testing.T) {
	tests := []struct {
		name string
		wire PropertyWire
	}{
		{
			name: "custom type on string tag",
			wire: PropertyWire{
				Key:        "name",
				Value:      "Ada",
				Type:       ptString,
				CustomType: "storeutil.wireValueDirectCustom",
			},
		},
		{
			name: "custom pointer on unknown legacy tag",
			wire: PropertyWire{
				Key:           "legacy",
				Value:         int64(1),
				Type:          ptUnknown,
				CustomPointer: true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidatePropertyWireSlice([]PropertyWire{tt.wire})
			if err == nil {
				t.Fatal("ValidatePropertyWireSlice returned nil, want metadata mismatch error")
			}
		})
	}
}
