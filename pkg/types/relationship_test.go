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

	keys := []string{"tkg_id", "tkg_version", "tkg_", "tkg_rel_type"}
	r := NewRelationship(snowflake.ID(1), 5, snowflake.ID(100), snowflake.ID(200))
	for _, key := range keys {
		if err := r.SetProperty(key, "x"); err == nil {
			t.Errorf("SetProperty(%q, ...) should return error", key)
		}
	}
}

func TestTokenZeroReservedRel(t *testing.T) {
	t.Parallel()

	r := NewRelationship(snowflake.ID(1), 0, snowflake.ID(100), snowflake.ID(200))
	if r.HasTypeToken(0) {
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
	if r.TypeToken() != 5 {
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
	if r.TypeToken() != 7 {
		t.Errorf("TypeToken() = %d, want 7", r.TypeToken())
	}
}

func TestRelHasTypeToken(t *testing.T) {
	t.Parallel()

	r := NewRelationship(snowflake.ID(1), 5, snowflake.ID(100), snowflake.ID(200))
	if !r.HasTypeToken(5) {
		t.Error("HasTypeToken(5) = false, want true")
	}
	if r.HasTypeToken(99) {
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
