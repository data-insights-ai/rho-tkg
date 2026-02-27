package types

import (
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
		if err := r.SetProperty(key, "x"); err == nil {
			t.Errorf("SetProperty(%q, ...) should return error", key)
		}
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
	if r.InternalID() != snowflake.ID(42) {
		t.Errorf("InternalID() = %d, want 42", r.InternalID())
	}
	if r.TypeToken() != relTypeToken(5) {
		t.Errorf("TypeToken() = %d, want 5", r.TypeToken())
	}
	if r.StartNodeID() != snowflake.ID(100) {
		t.Errorf("StartNodeID() = %d, want 100", r.StartNodeID())
	}
	if r.EndNodeID() != snowflake.ID(200) {
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
	if r.StartNodeID() != snowflake.ID(100) {
		t.Errorf("StartNodeID() = %d, want 100", r.StartNodeID())
	}
}

func TestRelEndNodeID(t *testing.T) {
	t.Parallel()

	r := NewRelationship(snowflake.ID(1), 5, snowflake.ID(100), snowflake.ID(200))
	if r.EndNodeID() != snowflake.ID(200) {
		t.Errorf("EndNodeID() = %d, want 200", r.EndNodeID())
	}
}

func TestRelInternalID(t *testing.T) {
	t.Parallel()

	r := NewRelationship(snowflake.ID(999), 5, snowflake.ID(100), snowflake.ID(200))
	if r.InternalID() != snowflake.ID(999) {
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
