package types

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// ─── Security tests ──────────────────────────────────────────────────────────

func TestRelSetPropertyRejectsTKGPrefix(t *testing.T) {
	t.Parallel()

	r := NewRelationship(RelID(snowflake.ID(1)), 5, NodeID(snowflake.ID(100)), NodeID(snowflake.ID(200)))
	if err := r.SetProperty("tkg_labels", "hack"); err == nil {
		t.Fatal("SetProperty(\"tkg_labels\", ...) should return error")
	}
}

func TestRelSetPropertyRejectsTKGPrefixVariants(t *testing.T) {
	t.Parallel()

	keys := []string{"tkg_type", "tkg_version", "tkg_", "tkg_hash"}
	r := NewRelationship(RelID(snowflake.ID(1)), 5, NodeID(snowflake.ID(100)), NodeID(snowflake.ID(200)))
	for _, key := range keys {
		err := r.SetProperty(key, "x")
		if err == nil {
			t.Errorf("SetProperty(%q, ...) should return error", key)
			continue
		}
		if !errors.Is(err, ErrReservedPrefix) {
			t.Errorf("SetProperty(%q): errors.Is(err, ErrReservedPrefix) = false; err = %v", key, err)
		}
	}
}

func TestRelSetPropertyRejectsNestedPointer(t *testing.T) {
	t.Parallel()

	type myStruct struct{ X int }
	r := NewRelationship(RelID(snowflake.ID(1)), 5, NodeID(snowflake.ID(100)), NodeID(snowflake.ID(200)))
	err := r.SetProperty("bad", []any{&myStruct{X: 1}})
	if err == nil {
		t.Fatal("SetProperty should reject nested pointer in []any")
	}
	if !errors.Is(err, ErrUnsupportedValueType) {
		t.Errorf("errors.Is(err, ErrUnsupportedValueType) = false; err = %v", err)
	}
}

func TestNewRelationshipAllowsZeroRelTypeForStoreValidation(t *testing.T) {
	t.Parallel()

	r := NewRelationship(RelID(snowflake.ID(1)), 0, NodeID(snowflake.ID(100)), NodeID(snowflake.ID(200)))
	if r == nil {
		t.Fatal("NewRelationship returned nil")
	}
	if r.TypeToken() != relTypeToken(0) {
		t.Fatalf("TypeToken() = %d, want reserved token 0", r.TypeToken())
	}
	if r.HasTypeTokenRaw(0) {
		t.Fatal("HasTypeTokenRaw(0) should still return false")
	}
}

func TestRelationshipNilReceiverMethodsFailClosed(t *testing.T) {
	t.Parallel()

	var r *Relationship
	if r.ID() != 0 {
		t.Fatalf("ID() = %v, want 0", r.ID())
	}
	if r.InternalID() != 0 {
		t.Fatalf("InternalID() = %v, want 0", r.InternalID())
	}
	if r.TypeToken() != 0 {
		t.Fatalf("TypeToken() = %v, want 0", r.TypeToken())
	}
	if r.HasTypeToken(1) {
		t.Fatal("HasTypeToken(1) = true, want false")
	}
	if r.HasTypeTokenRaw(1) {
		t.Fatal("HasTypeTokenRaw(1) = true, want false")
	}
	if r.StartNodeID() != 0 {
		t.Fatalf("StartNodeID() = %v, want 0", r.StartNodeID())
	}
	if r.EndNodeID() != 0 {
		t.Fatalf("EndNodeID() = %v, want 0", r.EndNodeID())
	}
	if err := r.SetProperties(nil); !errors.Is(err, ErrNilRelationship) {
		t.Fatalf("SetProperties(nil) = %v, want ErrNilRelationship", err)
	}
	if err := r.SetOwnedProperties(OwnedPropertySlice{}); !errors.Is(err, ErrNilRelationship) {
		t.Fatalf("SetOwnedProperties(nil receiver) = %v, want ErrNilRelationship", err)
	}
	if err := r.SetProperty("x", int64(1)); !errors.Is(err, ErrNilRelationship) {
		t.Fatalf("SetProperty = %v, want ErrNilRelationship", err)
	}
	if got, ok := r.GetProperty("x"); got != nil || ok {
		t.Fatalf("GetProperty = (%v, %v), want (nil, false)", got, ok)
	}
	if deleted, err := r.DeleteProperty("x"); !errors.Is(err, ErrNilRelationship) || deleted {
		t.Fatalf("DeleteProperty = (%v, %v), want (false, ErrNilRelationship)", deleted, err)
	}
	if r.PropertyCount() != 0 {
		t.Fatalf("PropertyCount() = %d, want 0", r.PropertyCount())
	}
	if got := r.Properties(); got != nil {
		t.Fatalf("Properties() = %v, want nil", got)
	}
	if got := r.PropertiesMap(); got != nil {
		t.Fatalf("PropertiesMap() = %v, want nil", got)
	}
	if r.Version() != 0 {
		t.Fatalf("Version() = %d, want 0", r.Version())
	}
	r.SetVersion(7)
	if r.Temporal() != nil {
		t.Fatal("Temporal() != nil, want nil")
	}
	r.SetTemporal(&TemporalMetadata{})
	if r.Integrity() != nil {
		t.Fatal("Integrity() != nil, want nil")
	}
	r.SetIntegrity(&RelIntegrity{})
	r.SetTypeTokenRaw(9)
	if r.TypeToken() != 0 {
		t.Fatalf("SetTypeTokenRaw mutated nil receiver")
	}
	if cp := r.DeepCopy(); cp != nil {
		t.Fatalf("DeepCopy() = %v, want nil", cp)
	}
}

func TestRelationshipSetOwnedProperties(t *testing.T) {
	t.Parallel()

	r := NewRelationship(RelID(snowflake.ID(1)), 5, NodeID(snowflake.ID(100)), NodeID(snowflake.ID(200)))
	owned, err := NewOwnedPropertySlice(map[string]any{"name": "rel-1"})
	if err != nil {
		t.Fatalf("NewOwnedPropertySlice: %v", err)
	}
	if err := r.SetOwnedProperties(owned); err != nil {
		t.Fatalf("SetOwnedProperties: %v", err)
	}
	got, ok := r.GetProperty("name")
	if !ok || got != "rel-1" {
		t.Fatalf("GetProperty(name) = (%v, %v), want (rel-1, true)", got, ok)
	}
}

func TestRelationshipSetOwnedPropertiesConsumesOwnership(t *testing.T) {
	t.Parallel()

	owned, err := NewOwnedPropertySlice(map[string]any{"name": []string{"rel-1"}})
	if err != nil {
		t.Fatalf("NewOwnedPropertySlice: %v", err)
	}
	alias := owned

	first := NewRelationship(RelID(snowflake.ID(1)), 5, NodeID(snowflake.ID(100)), NodeID(snowflake.ID(200)))
	second := NewRelationship(RelID(snowflake.ID(2)), 5, NodeID(snowflake.ID(100)), NodeID(snowflake.ID(201)))
	if err := first.SetOwnedProperties(owned); err != nil {
		t.Fatalf("first SetOwnedProperties: %v", err)
	}
	if err := second.SetOwnedProperties(alias); err != nil {
		t.Fatalf("second SetOwnedProperties: %v", err)
	}
	if _, ok := second.GetProperty("name"); ok {
		t.Fatal("reused OwnedPropertySlice installed properties on second relationship")
	}
	if err := first.SetProperty("name", []string{"mutated"}); err != nil {
		t.Fatalf("mutate first: %v", err)
	}
	if _, ok := second.GetProperty("name"); ok {
		t.Fatal("mutating first relationship affected second relationship after owned reuse")
	}
}

func TestTokenZeroReservedRel(t *testing.T) {
	t.Parallel()

	r := NewRelationship(RelID(snowflake.ID(1)), 5, NodeID(snowflake.ID(100)), NodeID(snowflake.ID(200))) // valid token
	if r.HasTypeToken(relTypeToken(0)) {
		t.Fatal("HasTypeToken(0) should always return false (reserved)")
	}
}

func TestRelationshipSetTypeTokenRaw(t *testing.T) {
	t.Parallel()

	r := NewRelationship(RelID(snowflake.ID(1)), 5, NodeID(snowflake.ID(100)), NodeID(snowflake.ID(200)))
	r.SetTypeTokenRaw(9)
	if got := r.TypeToken(); got != relTypeToken(9) {
		t.Fatalf("TypeToken() = %d, want 9", got)
	}
	if !r.HasTypeTokenRaw(9) {
		t.Fatal("HasTypeTokenRaw(9) = false, want true")
	}
	r.SetTypeTokenRaw(0)
	if r.TypeToken() != 0 {
		t.Fatalf("TypeToken() after zero = %d, want 0", r.TypeToken())
	}
	if r.HasTypeTokenRaw(0) {
		t.Fatal("HasTypeTokenRaw(0) = true, want false")
	}
}

func TestRelPropertiesReturnsCopy(t *testing.T) {
	t.Parallel()

	r := NewRelationship(RelID(snowflake.ID(1)), 5, NodeID(snowflake.ID(100)), NodeID(snowflake.ID(200)))
	if err := r.SetProperty("weight", 42); err != nil {
		t.Fatal(err)
	}

	props := r.Properties()
	if err := props.Set("injected", "bad"); err != nil {
		t.Fatal(err)
	}

	// The internal state must not have changed.
	if _, found := r.GetProperty("injected"); found {
		t.Fatal("Properties() returned an alias to internal state")
	}
}

// ─── Functional tests ────────────────────────────────────────────────────────

func TestNewRelationship(t *testing.T) {
	t.Parallel()

	r := NewRelationship(RelID(snowflake.ID(42)), 5, NodeID(snowflake.ID(100)), NodeID(snowflake.ID(200)))
	if r.ID() != RelID(snowflake.ID(42)) {
		t.Errorf("InternalID() = %d, want 42", r.ID())
	}
	if r.TypeToken() != relTypeToken(5) {
		t.Errorf("TypeToken() = %d, want 5", r.TypeToken())
	}
	if r.StartNodeID() != NodeID(snowflake.ID(100)) {
		t.Errorf("StartNodeID() = %d, want 100", r.StartNodeID())
	}
	if r.EndNodeID() != NodeID(snowflake.ID(200)) {
		t.Errorf("EndNodeID() = %d, want 200", r.EndNodeID())
	}
}

func TestRelTypeToken(t *testing.T) {
	t.Parallel()

	r := NewRelationship(RelID(snowflake.ID(1)), 7, NodeID(snowflake.ID(100)), NodeID(snowflake.ID(200)))
	if r.TypeToken() != relTypeToken(7) {
		t.Errorf("TypeToken() = %d, want 7", r.TypeToken())
	}
}

func TestRelHasTypeToken(t *testing.T) {
	t.Parallel()

	r := NewRelationship(RelID(snowflake.ID(1)), 5, NodeID(snowflake.ID(100)), NodeID(snowflake.ID(200)))
	if !r.HasTypeToken(relTypeToken(5)) {
		t.Error("HasTypeToken(5) = false, want true")
	}
	if r.HasTypeToken(relTypeToken(99)) {
		t.Error("HasTypeToken(99) = true, want false")
	}
}

func TestRelStartNodeID(t *testing.T) {
	t.Parallel()

	r := NewRelationship(RelID(snowflake.ID(1)), 5, NodeID(snowflake.ID(100)), NodeID(snowflake.ID(200)))
	if r.StartNodeID() != NodeID(snowflake.ID(100)) {
		t.Errorf("StartNodeID() = %d, want 100", r.StartNodeID())
	}
}

func TestRelEndNodeID(t *testing.T) {
	t.Parallel()

	r := NewRelationship(RelID(snowflake.ID(1)), 5, NodeID(snowflake.ID(100)), NodeID(snowflake.ID(200)))
	if r.EndNodeID() != NodeID(snowflake.ID(200)) {
		t.Errorf("EndNodeID() = %d, want 200", r.EndNodeID())
	}
}

func TestRelInternalID(t *testing.T) {
	t.Parallel()

	r := NewRelationship(RelID(snowflake.ID(999)), 5, NodeID(snowflake.ID(100)), NodeID(snowflake.ID(200)))
	if r.ID() != RelID(snowflake.ID(999)) {
		t.Errorf("InternalID() = %d, want 999", r.ID())
	}
}

func TestRelSetPropertyAcceptsNormalKeys(t *testing.T) {
	t.Parallel()

	r := NewRelationship(RelID(snowflake.ID(1)), 5, NodeID(snowflake.ID(100)), NodeID(snowflake.ID(200)))
	if err := r.SetProperty("weight", 1.5); err != nil {
		t.Fatalf("SetProperty(\"weight\", 1.5) returned unexpected error: %v", err)
	}
}

func TestRelSetPropertyCopiesCallerValue(t *testing.T) {
	t.Parallel()

	r := NewRelationship(RelID(snowflake.ID(1)), 5, NodeID(snowflake.ID(100)), NodeID(snowflake.ID(200)))
	tags := []string{"alpha", "beta"}
	if err := r.SetProperty("tags", tags); err != nil {
		t.Fatalf("SetProperty: %v", err)
	}

	tags[0] = "mutated"
	got, ok := r.GetProperty("tags")
	if !ok {
		t.Fatal("GetProperty(\"tags\") missing")
	}
	if got.([]string)[0] != "alpha" {
		t.Fatalf("SetProperty retained caller slice alias: %q", got.([]string)[0])
	}
}

func TestRelGetProperty(t *testing.T) {
	t.Parallel()

	r := NewRelationship(RelID(snowflake.ID(1)), 5, NodeID(snowflake.ID(100)), NodeID(snowflake.ID(200)))
	if err := r.SetProperty("weight", 1.5); err != nil {
		t.Fatal(err)
	}

	val, found := r.GetProperty("weight")
	if !found {
		t.Fatal("GetProperty(\"weight\") not found")
	}
	if val != 1.5 {
		t.Errorf("GetProperty(\"weight\") = %v, want 1.5", val)
	}

	_, found = r.GetProperty("missing")
	if found {
		t.Error("GetProperty(\"missing\") found, want not found")
	}
}

func TestRelGetPropertyReturnsIndependentValue(t *testing.T) {
	t.Parallel()

	r := NewRelationship(RelID(snowflake.ID(1)), 5, NodeID(snowflake.ID(100)), NodeID(snowflake.ID(200)))
	if err := r.SetProperty("meta", map[string]any{
		"tags": []any{"alpha", "beta"},
	}); err != nil {
		t.Fatal(err)
	}

	got, ok := r.GetProperty("meta")
	if !ok {
		t.Fatal("GetProperty(\"meta\") missing")
	}
	got.(map[string]any)["tags"].([]any)[0] = "mutated"

	again, ok := r.GetProperty("meta")
	if !ok {
		t.Fatal("GetProperty(\"meta\") missing after returned-value mutation")
	}
	if again.(map[string]any)["tags"].([]any)[0] != "alpha" {
		t.Fatalf("GetProperty returned internal mutable state: %v", again)
	}
}

func TestRelPropertyValueEqual(t *testing.T) {
	t.Parallel()

	var nilRel *Relationship
	if found, equal := nilRel.PropertyValueEqual("score", math.NaN()); found || equal {
		t.Fatalf("nil PropertyValueEqual = (%v, %v), want (false, false)", found, equal)
	}

	r := NewRelationship(RelID(snowflake.ID(1)), 5, NodeID(snowflake.ID(100)), NodeID(snowflake.ID(200)))
	if err := r.SetProperty("score", math.NaN()); err != nil {
		t.Fatal(err)
	}
	if err := r.SetProperty("trail", []float32{1, float32(math.NaN())}); err != nil {
		t.Fatal(err)
	}

	if found, equal := r.PropertyValueEqual("score", math.NaN()); !found || !equal {
		t.Fatalf("PropertyValueEqual(score) = (%v, %v), want (true, true)", found, equal)
	}
	if found, equal := r.PropertyValueEqual("trail", []float32{1, float32(math.NaN())}); !found || !equal {
		t.Fatalf("PropertyValueEqual(trail) = (%v, %v), want (true, true)", found, equal)
	}
	if found, equal := r.PropertyValueEqual("trail", []float64{1, math.NaN()}); !found || equal {
		t.Fatalf("PropertyValueEqual(type mismatch) = (%v, %v), want (true, false)", found, equal)
	}
	if found, equal := r.PropertyValueEqual("missing", nil); found || equal {
		t.Fatalf("PropertyValueEqual(missing) = (%v, %v), want (false, false)", found, equal)
	}

	var nilTrail []float32
	if err := r.SetProperty("nil_trail", nilTrail); err != nil {
		t.Fatal(err)
	}
	if found, equal := r.PropertyValueEqual("nil_trail", nilTrail); !found || !equal {
		t.Fatalf("PropertyValueEqual(nil_trail typed nil) = (%v, %v), want (true, true)", found, equal)
	}
	if found, equal := r.PropertyValueEqual("nil_trail", []float32{}); !found || equal {
		t.Fatalf("PropertyValueEqual(nil_trail empty) = (%v, %v), want (true, false)", found, equal)
	}

	var nilMeta map[string]any
	if err := r.SetProperty("nil_meta", nilMeta); err != nil {
		t.Fatal(err)
	}
	if found, equal := r.PropertyValueEqual("nil_meta", nilMeta); !found || !equal {
		t.Fatalf("PropertyValueEqual(nil_meta typed nil) = (%v, %v), want (true, true)", found, equal)
	}
	if found, equal := r.PropertyValueEqual("nil_meta", map[string]any{}); !found || equal {
		t.Fatalf("PropertyValueEqual(nil_meta empty) = (%v, %v), want (true, false)", found, equal)
	}
}

func TestRelIndexablePropertyValueKey(t *testing.T) {
	t.Parallel()

	var nilRel *Relationship
	if got, ok := nilRel.IndexablePropertyValueKey("weight"); got != "" || ok {
		t.Fatalf("nil IndexablePropertyValueKey = (%q, %v), want (\"\", false)", got, ok)
	}

	r := NewRelationship(RelID(snowflake.ID(1)), 5, NodeID(snowflake.ID(100)), NodeID(snowflake.ID(200)))
	if err := r.SetProperty("weight", 1.5); err != nil {
		t.Fatal(err)
	}
	if err := r.SetProperty("meta", map[string]any{"tags": []any{"alpha"}}); err != nil {
		t.Fatal(err)
	}
	if err := r.SetProperty("zero", math.Copysign(0, -1)); err != nil {
		t.Fatal(err)
	}

	if got, ok := r.IndexablePropertyValueKey("weight"); got != "f64:1.5" || !ok {
		t.Fatalf("IndexablePropertyValueKey(weight) = (%q, %v), want (f64:1.5, true)", got, ok)
	}
	if got, ok := r.IndexablePropertyValueKey("zero"); got != "f64:0" || !ok {
		t.Fatalf("IndexablePropertyValueKey(zero) = (%q, %v), want (f64:0, true)", got, ok)
	}
	if got, ok := r.IndexablePropertyValueKey("meta"); got != "" || !ok {
		t.Fatalf("IndexablePropertyValueKey(meta) = (%q, %v), want (\"\", true)", got, ok)
	}
	if got, ok := r.IndexablePropertyValueKey("missing"); got != "" || ok {
		t.Fatalf("IndexablePropertyValueKey(missing) = (%q, %v), want (\"\", false)", got, ok)
	}
}

func TestRelForEachIndexablePropertyValueKey(t *testing.T) {
	t.Parallel()

	var nilRel *Relationship
	nilRel.ForEachIndexablePropertyValueKey(func(string, string) bool {
		t.Fatal("nil relationship invoked callback")
		return false
	})

	r := NewRelationship(RelID(snowflake.ID(1)), 5, NodeID(snowflake.ID(100)), NodeID(snowflake.ID(200)))
	if err := r.SetProperty("since", int64(2026)); err != nil {
		t.Fatal(err)
	}
	if err := r.SetProperty("weight", 1.5); err != nil {
		t.Fatal(err)
	}
	if err := r.SetProperty("meta", map[string]any{"tags": []any{"alpha"}}); err != nil {
		t.Fatal(err)
	}
	r.ForEachIndexablePropertyValueKey(nil)

	got := make(map[string]string)
	r.ForEachIndexablePropertyValueKey(func(propertyKey, valueKey string) bool {
		got[propertyKey] = valueKey
		return true
	})
	want := map[string]string{"since": "i64:2026", "weight": "f64:1.5"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ForEachIndexablePropertyValueKey = %v, want %v", got, want)
	}

	calls := 0
	r.ForEachIndexablePropertyValueKey(func(string, string) bool {
		calls++
		return false
	})
	if calls != 1 {
		t.Fatalf("early-stop callback calls = %d, want 1", calls)
	}
}

func TestRelFloat32SlicePropertyCopy(t *testing.T) {
	t.Parallel()

	var nilRel *Relationship
	if got, ok := nilRel.Float32SlicePropertyCopy("embedding"); got != nil || ok {
		t.Fatalf("nil Float32SlicePropertyCopy = (%v, %v), want (nil, false)", got, ok)
	}

	r := NewRelationship(RelID(snowflake.ID(1)), 5, NodeID(snowflake.ID(100)), NodeID(snowflake.ID(200)))
	if err := r.SetProperty("embedding", []float32{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	if err := r.SetProperty("wire", []any{float32(1), float64(2)}); err != nil {
		t.Fatal(err)
	}
	if err := r.SetProperty("bad", []any{float32(1), "x"}); err != nil {
		t.Fatal(err)
	}
	if err := r.SetProperty("tags", []string{"a"}); err != nil {
		t.Fatal(err)
	}
	var nilEmbedding []float32
	if err := r.SetProperty("nil_embedding", nilEmbedding); err != nil {
		t.Fatal(err)
	}
	var nilWire []any
	if err := r.SetProperty("nil_wire", nilWire); err != nil {
		t.Fatal(err)
	}

	got, ok := r.Float32SlicePropertyCopy("embedding")
	if !ok || len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Fatalf("Float32SlicePropertyCopy(embedding) = (%v, %v), want ([1 2 3], true)", got, ok)
	}
	got[0] = 99
	again, ok := r.Float32SlicePropertyCopy("embedding")
	if !ok || again[0] != 1 {
		t.Fatalf("Float32SlicePropertyCopy returned alias; second read = (%v, %v), want first value 1", again, ok)
	}

	wire, ok := r.Float32SlicePropertyCopy("wire")
	if !ok || len(wire) != 2 || wire[0] != 1 || wire[1] != 2 {
		t.Fatalf("Float32SlicePropertyCopy(wire) = (%v, %v), want ([1 2], true)", wire, ok)
	}
	if got, ok := r.Float32SlicePropertyCopy("nil_embedding"); got != nil || !ok {
		t.Fatalf("Float32SlicePropertyCopy(nil_embedding) = (%v, %v), want (nil, true)", got, ok)
	}
	if got, ok := r.Float32SlicePropertyCopy("nil_wire"); got != nil || !ok {
		t.Fatalf("Float32SlicePropertyCopy(nil_wire) = (%v, %v), want (nil, true)", got, ok)
	}
	for _, key := range []string{"bad", "tags", "missing"} {
		if got, ok := r.Float32SlicePropertyCopy(key); got != nil || ok {
			t.Fatalf("Float32SlicePropertyCopy(%s) = (%v, %v), want (nil, false)", key, got, ok)
		}
	}
}

func TestRelDeleteProperty(t *testing.T) {
	t.Parallel()

	r := NewRelationship(RelID(snowflake.ID(1)), 5, NodeID(snowflake.ID(100)), NodeID(snowflake.ID(200)))
	_ = r.SetProperty("weight", 1.5)
	_ = r.SetProperty("since", "2025")

	found, err := r.DeleteProperty("weight")
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("DeleteProperty(\"weight\") should return true")
	}
	if _, ok := r.GetProperty("weight"); ok {
		t.Fatal("GetProperty(\"weight\") should return false after delete")
	}
}

func TestRelPropertiesMapIsIndependent(t *testing.T) {
	t.Parallel()

	r := NewRelationship(RelID(snowflake.ID(1)), 5, NodeID(snowflake.ID(100)), NodeID(snowflake.ID(200)))
	_ = r.SetProperty("tags", []string{"x", "y"})

	m := r.PropertiesMap()
	m["tags"].([]string)[0] = "MUTATED"

	val, _ := r.GetProperty("tags")
	origSlice := val.([]string)
	if origSlice[0] == "MUTATED" {
		t.Fatal("PropertiesMap: mutating returned map affected internal relationship state")
	}
}

func TestRelTemporalDefaultNil(t *testing.T) {
	t.Parallel()

	r := NewRelationship(RelID(snowflake.ID(1)), 5, NodeID(snowflake.ID(100)), NodeID(snowflake.ID(200)))
	if r.Temporal() != nil {
		t.Fatal("Temporal() should default to nil")
	}
}

func TestRelTemporalRoundTrip(t *testing.T) {
	t.Parallel()

	r := NewRelationship(RelID(snowflake.ID(1)), 5, NodeID(snowflake.ID(100)), NodeID(snowflake.ID(200)))
	tm := &TemporalMetadata{}
	r.SetTemporal(tm)
	if r.Temporal() != tm {
		t.Fatal("Temporal() should return the value set by SetTemporal()")
	}
}

func TestRelIntegrityDefaultNil(t *testing.T) {
	t.Parallel()

	r := NewRelationship(RelID(snowflake.ID(1)), 5, NodeID(snowflake.ID(100)), NodeID(snowflake.ID(200)))
	if r.Integrity() != nil {
		t.Fatal("Integrity() should default to nil")
	}
}

func TestRelIntegrityRoundTrip(t *testing.T) {
	t.Parallel()

	r := NewRelationship(RelID(snowflake.ID(1)), 5, NodeID(snowflake.ID(100)), NodeID(snowflake.ID(200)))
	ig := &RelIntegrity{}
	r.SetIntegrity(ig)
	if r.Integrity() != ig {
		t.Fatal("Integrity() should return the value set by SetIntegrity()")
	}
}

func TestRelTypeTokenValue(t *testing.T) {
	t.Parallel()

	r := NewRelationship(RelID(snowflake.ID(1)), 7, NodeID(snowflake.ID(100)), NodeID(snowflake.ID(200)))
	tok := r.TypeToken()
	if tok.Value() != 7 {
		t.Errorf("relTypeToken(7).Value() = %d, want 7", tok.Value())
	}
}

func TestRelVersion(t *testing.T) {
	t.Parallel()

	r := NewRelationship(RelID(snowflake.ID(1)), 5, NodeID(snowflake.ID(100)), NodeID(snowflake.ID(200)))
	if r.Version() != 0 {
		t.Errorf("default Version() = %d, want 0", r.Version())
	}
	r.SetVersion(5)
	if r.Version() != 5 {
		t.Errorf("after SetVersion(5), Version() = %d", r.Version())
	}
}

func TestRelTemporalFieldsRoundTrip(t *testing.T) {
	t.Parallel()

	r := NewRelationship(RelID(snowflake.ID(1)), 5, NodeID(snowflake.ID(100)), NodeID(snowflake.ID(200)))
	tm := &TemporalMetadata{
		ValidFrom: 1000,
		ValidTo:   2000,
		TxFrom:    3000,
		TxTo:      4000,
		CreatedAt: 5000,
		UpdatedAt: 6000,
		DeletedAt: 7000,
		CreatedBy: "alice",
		UpdatedBy: "bob",
	}
	tm.SetBaseEntityID(EntityID(42))
	r.SetTemporal(tm)
	got := r.Temporal()
	if got.ValidFrom != 1000 || got.ValidTo != 2000 {
		t.Errorf("ValidFrom/To = %d/%d, want 1000/2000", got.ValidFrom, got.ValidTo)
	}
	if got.TxFrom != 3000 || got.TxTo != 4000 {
		t.Errorf("TxFrom/To = %d/%d, want 3000/4000", got.TxFrom, got.TxTo)
	}
	if got.CreatedAt != 5000 || got.UpdatedAt != 6000 || got.DeletedAt != 7000 {
		t.Errorf("CreatedAt/UpdatedAt/DeletedAt = %d/%d/%d, want 5000/6000/7000",
			got.CreatedAt, got.UpdatedAt, got.DeletedAt)
	}
	if got.CreatedBy != "alice" || got.UpdatedBy != "bob" {
		t.Errorf("CreatedBy/UpdatedBy = %q/%q, want alice/bob", got.CreatedBy, got.UpdatedBy)
	}
	if got.BaseEntityID() != EntityID(snowflake.ID(42)) {
		t.Errorf("BaseEntityID() = %v, want 42", got.BaseEntityID())
	}
}

func TestRelIntegrityFieldsRoundTrip(t *testing.T) {
	t.Parallel()

	r := NewRelationship(RelID(snowflake.ID(1)), 5, NodeID(snowflake.ID(100)), NodeID(snowflake.ID(200)))
	ig := &RelIntegrity{
		Hash:     "abc123",
		PrevHash: "def456",
	}
	r.SetIntegrity(ig)
	got := r.Integrity()
	if got.Hash != "abc123" || got.PrevHash != "def456" {
		t.Errorf("Hash/PrevHash = %q/%q, want abc123/def456", got.Hash, got.PrevHash)
	}
}

func TestRelHasTypeTokenRaw(t *testing.T) {
	t.Parallel()

	r := NewRelationship(RelID(snowflake.ID(1)), 5, NodeID(snowflake.ID(100)), NodeID(snowflake.ID(200)))

	tests := []struct {
		name string
		tok  uint16
		want bool
	}{
		{"type hit", 5, true},
		{"type miss", 99, false},
		{"token 0 reserved", 0, false},
	}

	for _, tt := range tests {
		if got := r.HasTypeTokenRaw(tt.tok); got != tt.want {
			t.Errorf("HasTypeTokenRaw(%d) [%s] = %v, want %v", tt.tok, tt.name, got, tt.want)
		}
	}
}

func TestRelPropertiesMap(t *testing.T) {
	t.Parallel()

	r := NewRelationship(RelID(snowflake.ID(1)), 5, NodeID(snowflake.ID(100)), NodeID(snowflake.ID(200)))
	_ = r.SetProperty("weight", 1.5)
	_ = r.SetProperty("since", "2025")

	m := r.PropertiesMap()
	if len(m) != 2 {
		t.Fatalf("PropertiesMap() len = %d, want 2", len(m))
	}
	if m["weight"] != 1.5 || m["since"] != "2025" {
		t.Errorf("PropertiesMap() = %v, unexpected values", m)
	}
}

// ─── SnowflakeID bridge tests ────────────────────────────────────────────────

func TestRelIDSnowflakeIDRoundTrip(t *testing.T) {
	t.Parallel()

	r := NewRelationship(RelID(snowflake.ID(42)), 5, NodeID(snowflake.ID(100)), NodeID(snowflake.ID(200)))
	got := r.ID().SnowflakeID()
	if got != snowflake.ID(42) {
		t.Errorf("relID.SnowflakeID() = %d, want 42", got)
	}
}

func TestNodeIDSnowflakeIDRoundTripFromRel(t *testing.T) {
	t.Parallel()

	r := NewRelationship(RelID(snowflake.ID(1)), 5, NodeID(snowflake.ID(100)), NodeID(snowflake.ID(200)))
	startID := r.StartNodeID().SnowflakeID()
	endID := r.EndNodeID().SnowflakeID()

	if startID != snowflake.ID(100) {
		t.Errorf("StartNodeID().SnowflakeID() = %d, want 100", startID)
	}
	if endID != snowflake.ID(200) {
		t.Errorf("EndNodeID().SnowflakeID() = %d, want 200", endID)
	}
}

// ─── SetProperties tests ────────────────────────────────────────────────────

func TestRelSetProperties(t *testing.T) {
	t.Parallel()

	r := NewRelationship(RelID(snowflake.ID(1)), 5, NodeID(snowflake.ID(100)), NodeID(snowflake.ID(200)))
	ps, err := NewPropertySlice(map[string]any{"weight": 1.5, "since": "2025"})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.SetProperties(ps); err != nil {
		t.Fatalf("SetProperties: %v", err)
	}

	val, ok := r.GetProperty("weight")
	if !ok || val != 1.5 {
		t.Errorf("GetProperty(\"weight\") = (%v, %v), want (1.5, true)", val, ok)
	}
	val, ok = r.GetProperty("since")
	if !ok || val != "2025" {
		t.Errorf("GetProperty(\"since\") = (%v, %v), want (\"2025\", true)", val, ok)
	}
}

func TestRelSetPropertiesCanonicalizesAndCopies(t *testing.T) {
	t.Parallel()

	r := NewRelationship(RelID(snowflake.ID(1)), 5, NodeID(snowflake.ID(100)), NodeID(snowflake.ID(200)))
	tags := []string{"alpha", "beta"}
	ps := PropertySlice{
		{Key: "z", Value: "old"},
		{Key: "tags", Value: tags},
		{Key: "z", Value: "new"},
		{Key: "weight", Value: 1.5},
	}
	if err := r.SetProperties(ps); err != nil {
		t.Fatalf("SetProperties: %v", err)
	}

	if _, ok := r.GetProperty("z"); !ok {
		t.Fatal("GetProperty(\"z\") missing after unsorted SetProperties input")
	}
	if val, _ := r.GetProperty("z"); val != "new" {
		t.Fatalf("duplicate key value = %v, want last value", val)
	}

	tags[0] = "mutated"
	got, ok := r.GetProperty("tags")
	if !ok {
		t.Fatal("GetProperty(\"tags\") missing")
	}
	gotTags := got.([]string)
	if gotTags[0] != "alpha" {
		t.Fatalf("SetProperties retained caller slice alias: %q", gotTags[0])
	}
}

func TestRelSetPropertiesDuplicateKeyValidatesWinningValueOnly(t *testing.T) {
	t.Parallel()

	r := NewRelationship(RelID(snowflake.ID(1)), 5, NodeID(snowflake.ID(100)), NodeID(snowflake.ID(200)))
	ps := PropertySlice{
		{Key: "z", Value: make(chan int)},
		{Key: "a", Value: "kept"},
		{Key: "z", Value: "winner"},
	}
	if err := r.SetProperties(ps); err != nil {
		t.Fatalf("SetProperties duplicate overwritten invalid value: %v", err)
	}
	if val, ok := r.GetProperty("z"); !ok || val != "winner" {
		t.Fatalf("duplicate key value = (%v, %v), want (winner, true)", val, ok)
	}
	if got := r.PropertyCount(); got != 2 {
		t.Fatalf("PropertyCount = %d, want 2", got)
	}
}

func TestRelSetPropertiesRejectsInvalidAndKeepsPrevious(t *testing.T) {
	t.Parallel()

	r := NewRelationship(RelID(snowflake.ID(1)), 5, NodeID(snowflake.ID(100)), NodeID(snowflake.ID(200)))
	if err := r.SetProperties(PropertySlice{{Key: "since", Value: "2025"}}); err != nil {
		t.Fatalf("initial SetProperties: %v", err)
	}

	err := r.SetProperties(PropertySlice{{Key: "bad", Value: make(chan int)}})
	if !errors.Is(err, ErrUnsupportedValueType) {
		t.Fatalf("SetProperties error = %v, want ErrUnsupportedValueType", err)
	}
	if _, ok := r.GetProperty("bad"); ok {
		t.Fatal("invalid property was installed")
	}
	if got, ok := r.GetProperty("since"); !ok || got != "2025" {
		t.Fatalf("previous property after rejected SetProperties = (%v, %v), want (2025, true)", got, ok)
	}
}

func TestRelSetPropertiesRejectsReservedKey(t *testing.T) {
	t.Parallel()

	r := NewRelationship(RelID(snowflake.ID(1)), 5, NodeID(snowflake.ID(100)), NodeID(snowflake.ID(200)))
	err := r.SetProperties(PropertySlice{{Key: "tkg_hash", Value: "x"}})
	if !errors.Is(err, ErrReservedPrefix) {
		t.Fatalf("SetProperties error = %v, want ErrReservedPrefix", err)
	}
	if r.PropertyCount() != 0 {
		t.Fatalf("reserved property was installed, count=%d", r.PropertyCount())
	}
}

// ─── Edge case tests ────────────────────────────────────────────────────────

func TestRelTemporalSharedPointerMutation(t *testing.T) {
	t.Parallel()

	r := NewRelationship(RelID(snowflake.ID(1)), 5, NodeID(snowflake.ID(100)), NodeID(snowflake.ID(200)))
	tm := &TemporalMetadata{ValidFrom: 1000}
	r.SetTemporal(tm)

	// Mutate through the returned pointer.
	r.Temporal().ValidFrom = 2000

	// Relationship must reflect the change (shared pointer).
	if r.Temporal().ValidFrom != 2000 {
		t.Errorf("Temporal().ValidFrom = %d, want 2000 (shared pointer mutation)", r.Temporal().ValidFrom)
	}
}

func TestRelSetTemporalNil(t *testing.T) {
	t.Parallel()

	r := NewRelationship(RelID(snowflake.ID(1)), 5, NodeID(snowflake.ID(100)), NodeID(snowflake.ID(200)))
	tm := &TemporalMetadata{ValidFrom: 1000}
	r.SetTemporal(tm)
	r.SetTemporal(nil)

	if r.Temporal() != nil {
		t.Fatal("Temporal() should be nil after SetTemporal(nil)")
	}
}

func TestRelTemporalOverwrite(t *testing.T) {
	t.Parallel()

	r := NewRelationship(RelID(snowflake.ID(1)), 5, NodeID(snowflake.ID(100)), NodeID(snowflake.ID(200)))
	old := &TemporalMetadata{ValidFrom: 1000}
	r.SetTemporal(old)

	replacement := &TemporalMetadata{ValidFrom: 9999}
	r.SetTemporal(replacement)

	if r.Temporal() != replacement {
		t.Fatal("Temporal() should return the replacement pointer")
	}
	// Old pointer must be detached — mutating it doesn't affect relationship.
	old.ValidFrom = 5555
	if r.Temporal().ValidFrom != 9999 {
		t.Errorf("old pointer mutation affected relationship: ValidFrom = %d, want 9999", r.Temporal().ValidFrom)
	}
}

func TestRelStressManyProperties(t *testing.T) {
	t.Parallel()

	r := NewRelationship(RelID(snowflake.ID(1)), 5, NodeID(snowflake.ID(100)), NodeID(snowflake.ID(200)))
	for i := range 1000 {
		key := fmt.Sprintf("prop_%04d", i)
		if err := r.SetProperty(key, i); err != nil {
			t.Fatalf("SetProperty(%q) failed: %v", key, err)
		}
	}

	// All 1000 retrievable.
	for i := range 1000 {
		key := fmt.Sprintf("prop_%04d", i)
		val, ok := r.GetProperty(key)
		if !ok || val != i {
			t.Fatalf("GetProperty(%q) = (%v, %v), want (%d, true)", key, val, ok, i)
		}
	}

	// Sorted invariant on internal properties.
	props := r.Properties()
	if !sort.SliceIsSorted(props, func(i, j int) bool { return props[i].Key < props[j].Key }) {
		t.Fatal("Relationship properties are not sorted after 1000 insertions")
	}
}

// ─── DeepCopy tests ──────────────────────────────────────────────────────────

func TestRelDeepCopy(t *testing.T) {
	t.Parallel()

	r := NewRelationship(RelID(snowflake.ID(42)), 5, NodeID(snowflake.ID(100)), NodeID(snowflake.ID(200)))
	_ = r.SetProperty("weight", 1.5)
	_ = r.SetProperty("tags", []string{"x", "y"})
	r.SetVersion(3)
	r.SetTemporal(&TemporalMetadata{ValidFrom: 2000, CreatedBy: "bob"})
	r.SetIntegrity(&RelIntegrity{Hash: "abc", PrevHash: "def"})

	cp := r.DeepCopy()

	// All fields copied correctly.
	if cp.ID() != r.ID() {
		t.Errorf("ID: got %v, want %v", cp.ID(), r.ID())
	}
	if cp.TypeToken() != r.TypeToken() {
		t.Errorf("TypeToken: got %v, want %v", cp.TypeToken(), r.TypeToken())
	}
	if cp.StartNodeID() != r.StartNodeID() {
		t.Errorf("StartNodeID: got %v, want %v", cp.StartNodeID(), r.StartNodeID())
	}
	if cp.EndNodeID() != r.EndNodeID() {
		t.Errorf("EndNodeID: got %v, want %v", cp.EndNodeID(), r.EndNodeID())
	}
	if cp.Version() != 3 {
		t.Errorf("Version: got %d, want 3", cp.Version())
	}
	if v, ok := cp.GetProperty("weight"); !ok || v != 1.5 {
		t.Errorf("Property weight: got (%v, %v), want (1.5, true)", v, ok)
	}
	if cp.Temporal().ValidFrom != 2000 || cp.Temporal().CreatedBy != "bob" {
		t.Error("Temporal fields not copied correctly")
	}
	if cp.Integrity().Hash != "abc" || cp.Integrity().PrevHash != "def" {
		t.Error("Integrity fields not copied correctly")
	}

	// Mutation independence: properties.
	_ = cp.SetProperty("weight", 9.9)
	if v, _ := r.GetProperty("weight"); v != 1.5 {
		t.Fatal("DeepCopy properties: mutation affected original")
	}

	// Mutation independence: slice property values.
	cpTags, _ := cp.GetProperty("tags")
	cpTags.([]string)[0] = "MUTATED"
	origTags, _ := r.GetProperty("tags")
	if origTags.([]string)[0] == "MUTATED" {
		t.Fatal("DeepCopy property slice value: mutation affected original")
	}

	// Mutation independence: temporal.
	cp.Temporal().ValidFrom = 9999
	if r.Temporal().ValidFrom != 2000 {
		t.Fatal("DeepCopy temporal: mutation affected original")
	}

	// Mutation independence: integrity.
	cp.Integrity().Hash = "MUTATED"
	if r.Integrity().Hash != "abc" {
		t.Fatal("DeepCopy integrity: mutation affected original")
	}
}

func TestRelDeepCopyNilTemporalIntegrity(t *testing.T) {
	t.Parallel()

	r := NewRelationship(RelID(snowflake.ID(1)), 5, NodeID(snowflake.ID(100)), NodeID(snowflake.ID(200)))
	cp := r.DeepCopy()
	if cp.Temporal() != nil {
		t.Fatal("DeepCopy should preserve nil temporal")
	}
	if cp.Integrity() != nil {
		t.Fatal("DeepCopy should preserve nil integrity")
	}
}

func TestRelPropertyCount(t *testing.T) {
	t.Parallel()
	r := NewRelationship(RelID(snowflake.ID(1)), 1, NodeID(snowflake.ID(2)), NodeID(snowflake.ID(3)))

	if r.PropertyCount() != 0 {
		t.Fatalf("PropertyCount = %d, want 0", r.PropertyCount())
	}

	r.SetProperty("a", "1")
	r.SetProperty("b", "2")
	if r.PropertyCount() != 2 {
		t.Fatalf("PropertyCount = %d, want 2", r.PropertyCount())
	}

	r.DeleteProperty("a")
	if r.PropertyCount() != 1 {
		t.Fatalf("PropertyCount after delete = %d, want 1", r.PropertyCount())
	}
}
