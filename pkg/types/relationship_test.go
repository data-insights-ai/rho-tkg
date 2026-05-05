package types

import (
	"errors"
	"fmt"
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

func TestNewRelationshipPanicsOnZeroRelType(t *testing.T) {
	t.Parallel()

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("NewRelationship(RelID(id), 0, NodeID(start), NodeID(end)) should panic on reserved token 0")
		}
	}()
	NewRelationship(RelID(snowflake.ID(1)), 0, NodeID(snowflake.ID(100)), NodeID(snowflake.ID(200)))
}

func TestTokenZeroReservedRel(t *testing.T) {
	t.Parallel()

	r := NewRelationship(RelID(snowflake.ID(1)), 5, NodeID(snowflake.ID(100)), NodeID(snowflake.ID(200))) // valid token
	if r.HasTypeToken(relTypeToken(0)) {
		t.Fatal("HasTypeToken(0) should always return false (reserved)")
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
	tm.SetBaseEntityID(snowflake.ID(42))
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
	r.SetProperties(ps)

	val, ok := r.GetProperty("weight")
	if !ok || val != 1.5 {
		t.Errorf("GetProperty(\"weight\") = (%v, %v), want (1.5, true)", val, ok)
	}
	val, ok = r.GetProperty("since")
	if !ok || val != "2025" {
		t.Errorf("GetProperty(\"since\") = (%v, %v), want (\"2025\", true)", val, ok)
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
