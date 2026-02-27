package types

import (
	"errors"
	"testing"

	snowflake "gitlab2024.bds421-cloud.com/bds421/rho/snowflake-2026"
)

// ─── Security tests ──────────────────────────────────────────────────────────

func TestRelSetPropertyRejectsTKGPrefix(t *testing.T) {
	t.Parallel()

	r := NewRelationship(snowflake.ID(1), 5, snowflake.ID(100), snowflake.ID(200))
	if err := r.SetProperty("tkg_labels", "hack"); err == nil {
		t.Fatal("SetProperty(\"tkg_labels\", ...) should return error")
	}
}

func TestRelSetPropertyRejectsTKGPrefixVariants(t *testing.T) {
	t.Parallel()

	keys := []string{"tkg_type", "tkg_version", "tkg_", "tkg_hash"}
	r := NewRelationship(snowflake.ID(1), 5, snowflake.ID(100), snowflake.ID(200))
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
	r := NewRelationship(snowflake.ID(1), 5, snowflake.ID(100), snowflake.ID(200))
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
			t.Fatal("NewRelationship(id, 0, start, end) should panic on reserved token 0")
		}
	}()
	NewRelationship(snowflake.ID(1), 0, snowflake.ID(100), snowflake.ID(200))
}

func TestTokenZeroReservedRel(t *testing.T) {
	t.Parallel()

	r := NewRelationship(snowflake.ID(1), 5, snowflake.ID(100), snowflake.ID(200)) // valid token
	if r.HasTypeToken(relTypeToken(0)) {
		t.Fatal("HasTypeToken(0) should always return false (reserved)")
	}
}

func TestRelPropertiesReturnsCopy(t *testing.T) {
	t.Parallel()

	r := NewRelationship(snowflake.ID(1), 5, snowflake.ID(100), snowflake.ID(200))
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

	r := NewRelationship(snowflake.ID(42), 5, snowflake.ID(100), snowflake.ID(200))
	if r.InternalID() != relID(snowflake.ID(42)) {
		t.Errorf("InternalID() = %d, want 42", r.InternalID())
	}
	if r.TypeToken() != relTypeToken(5) {
		t.Errorf("TypeToken() = %d, want 5", r.TypeToken())
	}
	if r.StartNodeID() != nodeID(snowflake.ID(100)) {
		t.Errorf("StartNodeID() = %d, want 100", r.StartNodeID())
	}
	if r.EndNodeID() != nodeID(snowflake.ID(200)) {
		t.Errorf("EndNodeID() = %d, want 200", r.EndNodeID())
	}
}

func TestRelTypeToken(t *testing.T) {
	t.Parallel()

	r := NewRelationship(snowflake.ID(1), 7, snowflake.ID(100), snowflake.ID(200))
	if r.TypeToken() != relTypeToken(7) {
		t.Errorf("TypeToken() = %d, want 7", r.TypeToken())
	}
}

func TestRelHasTypeToken(t *testing.T) {
	t.Parallel()

	r := NewRelationship(snowflake.ID(1), 5, snowflake.ID(100), snowflake.ID(200))
	if !r.HasTypeToken(relTypeToken(5)) {
		t.Error("HasTypeToken(5) = false, want true")
	}
	if r.HasTypeToken(relTypeToken(99)) {
		t.Error("HasTypeToken(99) = true, want false")
	}
}

func TestRelStartNodeID(t *testing.T) {
	t.Parallel()

	r := NewRelationship(snowflake.ID(1), 5, snowflake.ID(100), snowflake.ID(200))
	if r.StartNodeID() != nodeID(snowflake.ID(100)) {
		t.Errorf("StartNodeID() = %d, want 100", r.StartNodeID())
	}
}

func TestRelEndNodeID(t *testing.T) {
	t.Parallel()

	r := NewRelationship(snowflake.ID(1), 5, snowflake.ID(100), snowflake.ID(200))
	if r.EndNodeID() != nodeID(snowflake.ID(200)) {
		t.Errorf("EndNodeID() = %d, want 200", r.EndNodeID())
	}
}

func TestRelInternalID(t *testing.T) {
	t.Parallel()

	r := NewRelationship(snowflake.ID(999), 5, snowflake.ID(100), snowflake.ID(200))
	if r.InternalID() != relID(snowflake.ID(999)) {
		t.Errorf("InternalID() = %d, want 999", r.InternalID())
	}
}

func TestRelSetPropertyAcceptsNormalKeys(t *testing.T) {
	t.Parallel()

	r := NewRelationship(snowflake.ID(1), 5, snowflake.ID(100), snowflake.ID(200))
	if err := r.SetProperty("weight", 1.5); err != nil {
		t.Fatalf("SetProperty(\"weight\", 1.5) returned unexpected error: %v", err)
	}
}

func TestRelGetProperty(t *testing.T) {
	t.Parallel()

	r := NewRelationship(snowflake.ID(1), 5, snowflake.ID(100), snowflake.ID(200))
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

	r := NewRelationship(snowflake.ID(1), 5, snowflake.ID(100), snowflake.ID(200))
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

	r := NewRelationship(snowflake.ID(1), 5, snowflake.ID(100), snowflake.ID(200))
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

	r := NewRelationship(snowflake.ID(1), 5, snowflake.ID(100), snowflake.ID(200))
	if r.Temporal() != nil {
		t.Fatal("Temporal() should default to nil")
	}
}

func TestRelTemporalRoundTrip(t *testing.T) {
	t.Parallel()

	r := NewRelationship(snowflake.ID(1), 5, snowflake.ID(100), snowflake.ID(200))
	tm := &TemporalMetadata{}
	r.SetTemporal(tm)
	if r.Temporal() != tm {
		t.Fatal("Temporal() should return the value set by SetTemporal()")
	}
}

func TestRelIntegrityDefaultNil(t *testing.T) {
	t.Parallel()

	r := NewRelationship(snowflake.ID(1), 5, snowflake.ID(100), snowflake.ID(200))
	if r.Integrity() != nil {
		t.Fatal("Integrity() should default to nil")
	}
}

func TestRelIntegrityRoundTrip(t *testing.T) {
	t.Parallel()

	r := NewRelationship(snowflake.ID(1), 5, snowflake.ID(100), snowflake.ID(200))
	ig := &RelIntegrity{}
	r.SetIntegrity(ig)
	if r.Integrity() != ig {
		t.Fatal("Integrity() should return the value set by SetIntegrity()")
	}
}

func TestRelTypeTokenValue(t *testing.T) {
	t.Parallel()

	r := NewRelationship(snowflake.ID(1), 7, snowflake.ID(100), snowflake.ID(200))
	tok := r.TypeToken()
	if tok.Value() != 7 {
		t.Errorf("relTypeToken(7).Value() = %d, want 7", tok.Value())
	}
}

func TestRelVersion(t *testing.T) {
	t.Parallel()

	r := NewRelationship(snowflake.ID(1), 5, snowflake.ID(100), snowflake.ID(200))
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

	r := NewRelationship(snowflake.ID(1), 5, snowflake.ID(100), snowflake.ID(200))
	tm := &TemporalMetadata{
		ValidFrom:    1000,
		ValidTo:      2000,
		TxFrom:       3000,
		TxTo:         4000,
		CreatedAt:    5000,
		UpdatedAt:    6000,
		DeletedAt:    7000,
		CreatedBy:    "alice",
		UpdatedBy:    "bob",
		BaseEntityID: 42,
	}
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
	if got.BaseEntityID != 42 {
		t.Errorf("BaseEntityID = %d, want 42", got.BaseEntityID)
	}
}

func TestRelIntegrityFieldsRoundTrip(t *testing.T) {
	t.Parallel()

	r := NewRelationship(snowflake.ID(1), 5, snowflake.ID(100), snowflake.ID(200))
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

	r := NewRelationship(snowflake.ID(1), 5, snowflake.ID(100), snowflake.ID(200))

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

	r := NewRelationship(snowflake.ID(1), 5, snowflake.ID(100), snowflake.ID(200))
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
