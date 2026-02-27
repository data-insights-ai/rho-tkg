package types

import (
	"errors"
	"testing"

	snowflake "gitlab2024.bds421-cloud.com/bds421/rho/snowflake-2026"
)

// ─── Security tests ──────────────────────────────────────────────────────────

func TestNodeSetPropertyRejectsTKGPrefix(t *testing.T) {
	t.Parallel()

	n := NewNode(snowflake.ID(1), 10, nil)
	if err := n.SetProperty("tkg_labels", "hack"); err == nil {
		t.Fatal("SetProperty(\"tkg_labels\", ...) should return error")
	}
}

func TestNodeSetPropertyRejectsTKGPrefixVariants(t *testing.T) {
	t.Parallel()

	keys := []string{"tkg_type", "tkg_version", "tkg_", "tkg_anything"}
	n := NewNode(snowflake.ID(1), 10, nil)
	for _, key := range keys {
		err := n.SetProperty(key, "x")
		if err == nil {
			t.Errorf("SetProperty(%q, ...) should return error", key)
			continue
		}
		if !errors.Is(err, ErrReservedPrefix) {
			t.Errorf("SetProperty(%q): errors.Is(err, ErrReservedPrefix) = false; err = %v", key, err)
		}
	}
}

func TestNodeSetPropertyRejectsNestedPointer(t *testing.T) {
	t.Parallel()

	type myStruct struct{ X int }
	n := NewNode(snowflake.ID(1), 10, nil)
	err := n.SetProperty("bad", []any{&myStruct{X: 1}})
	if err == nil {
		t.Fatal("SetProperty should reject nested pointer in []any")
	}
	if !errors.Is(err, ErrUnsupportedValueType) {
		t.Errorf("errors.Is(err, ErrUnsupportedValueType) = false; err = %v", err)
	}
}

func TestNewNodePanicsOnZeroPrimaryLabel(t *testing.T) {
	t.Parallel()

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("NewNode(id, 0, nil) should panic on reserved token 0")
		}
	}()
	NewNode(snowflake.ID(1), 0, nil)
}

func TestNewNodePanicsOnZeroExtraLabel(t *testing.T) {
	t.Parallel()

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("NewNode(id, 1, []uint16{5, 0}) should panic on reserved token 0 in extras")
		}
	}()
	NewNode(snowflake.ID(1), 1, []uint16{5, 0})
}

func TestNewNodeDeduplicatesExtraLabels(t *testing.T) {
	t.Parallel()

	n := NewNode(snowflake.ID(1), 10, []uint16{20, 30, 20})
	extras := n.ExtraLabelTokens()
	if len(extras) != 2 {
		t.Fatalf("ExtraLabelTokens() len = %d, want 2 (deduped)", len(extras))
	}
	if n.LabelTokenCount() != 3 {
		t.Errorf("LabelTokenCount() = %d, want 3", n.LabelTokenCount())
	}
}

func TestNewNodeRemovesPrimaryFromExtras(t *testing.T) {
	t.Parallel()

	n := NewNode(snowflake.ID(1), 10, []uint16{10, 20})
	extras := n.ExtraLabelTokens()
	if len(extras) != 1 {
		t.Fatalf("ExtraLabelTokens() len = %d, want 1 (primary removed)", len(extras))
	}
	if extras[0] != labelToken(20) {
		t.Errorf("ExtraLabelTokens()[0] = %d, want 20", extras[0])
	}
	if n.LabelTokenCount() != 2 {
		t.Errorf("LabelTokenCount() = %d, want 2", n.LabelTokenCount())
	}
}

func TestTokenZeroReservedNode(t *testing.T) {
	t.Parallel()

	n := NewNode(snowflake.ID(1), 5, nil) // valid token
	if n.HasLabelToken(labelToken(0)) {
		t.Fatal("HasLabelToken(0) should always return false (reserved)")
	}
}

func TestNodeExtraLabelTokensReturnsCopy(t *testing.T) {
	t.Parallel()

	n := NewNode(snowflake.ID(1), 10, []uint16{20, 30})
	extras := n.ExtraLabelTokens()
	extras[0] = labelToken(999) // Mutate the returned slice.

	// The internal state must not have changed.
	got := n.ExtraLabelTokens()
	if got[0] == labelToken(999) {
		t.Fatal("ExtraLabelTokens returned an alias to internal state")
	}
}

// ─── Functional tests ────────────────────────────────────────────────────────

func TestNewNode(t *testing.T) {
	t.Parallel()

	n := NewNode(snowflake.ID(42), 10, []uint16{20, 30})
	if n.InternalID() != nodeID(snowflake.ID(42)) {
		t.Errorf("InternalID() = %d, want 42", n.InternalID())
	}
	if n.PrimaryLabelToken() != labelToken(10) {
		t.Errorf("PrimaryLabelToken() = %d, want 10", n.PrimaryLabelToken())
	}
	extras := n.ExtraLabelTokens()
	if len(extras) != 2 || extras[0] != labelToken(20) || extras[1] != labelToken(30) {
		t.Errorf("ExtraLabelTokens() = %v, want [20 30]", extras)
	}
}

func TestNodePrimaryLabelToken(t *testing.T) {
	t.Parallel()

	n := NewNode(snowflake.ID(1), 7, nil)
	if n.PrimaryLabelToken() != labelToken(7) {
		t.Errorf("PrimaryLabelToken() = %d, want 7", n.PrimaryLabelToken())
	}
}

func TestNodeExtraLabelTokens(t *testing.T) {
	t.Parallel()

	// Single-label node: extras should be nil.
	single := NewNode(snowflake.ID(1), 10, nil)
	if extras := single.ExtraLabelTokens(); extras != nil {
		t.Errorf("single-label ExtraLabelTokens() = %v, want nil", extras)
	}

	// Multi-label node: extras should match.
	multi := NewNode(snowflake.ID(2), 10, []uint16{20, 30})
	extras := multi.ExtraLabelTokens()
	if len(extras) != 2 {
		t.Fatalf("multi-label ExtraLabelTokens() len = %d, want 2", len(extras))
	}
}

func TestNodeHasLabelToken(t *testing.T) {
	t.Parallel()

	n := NewNode(snowflake.ID(1), 10, []uint16{20, 30})

	if !n.HasLabelToken(labelToken(10)) {
		t.Error("HasLabelToken(10) = false, want true (primary)")
	}
	if !n.HasLabelToken(labelToken(20)) {
		t.Error("HasLabelToken(20) = false, want true (extra)")
	}
	if n.HasLabelToken(labelToken(99)) {
		t.Error("HasLabelToken(99) = true, want false (absent)")
	}
}

func TestNodeLabelTokenCount(t *testing.T) {
	t.Parallel()

	single := NewNode(snowflake.ID(1), 10, nil)
	if single.LabelTokenCount() != 1 {
		t.Errorf("single-label LabelTokenCount() = %d, want 1", single.LabelTokenCount())
	}

	multi := NewNode(snowflake.ID(2), 10, []uint16{20, 30})
	if multi.LabelTokenCount() != 3 {
		t.Errorf("multi-label LabelTokenCount() = %d, want 3", multi.LabelTokenCount())
	}
}

func TestNodeAllLabelTokens(t *testing.T) {
	t.Parallel()

	n := NewNode(snowflake.ID(1), 10, []uint16{20, 30})
	all := n.AllLabelTokens()
	if len(all) != 3 || all[0] != labelToken(10) || all[1] != labelToken(20) || all[2] != labelToken(30) {
		t.Errorf("AllLabelTokens() = %v, want [10 20 30]", all)
	}
}

func TestNodeInternalID(t *testing.T) {
	t.Parallel()

	n := NewNode(snowflake.ID(123456789), 1, nil)
	if n.InternalID() != nodeID(snowflake.ID(123456789)) {
		t.Errorf("InternalID() = %d, want 123456789", n.InternalID())
	}
}

func TestNodeSetPropertyAcceptsNormalKeys(t *testing.T) {
	t.Parallel()

	n := NewNode(snowflake.ID(1), 10, nil)
	if err := n.SetProperty("name", "Alice"); err != nil {
		t.Fatalf("SetProperty(\"name\", \"Alice\") returned unexpected error: %v", err)
	}
}

func TestNodeGetProperty(t *testing.T) {
	t.Parallel()

	n := NewNode(snowflake.ID(1), 10, nil)
	if err := n.SetProperty("name", "Alice"); err != nil {
		t.Fatal(err)
	}

	val, found := n.GetProperty("name")
	if !found {
		t.Fatal("GetProperty(\"name\") not found")
	}
	if val != "Alice" {
		t.Errorf("GetProperty(\"name\") = %v, want \"Alice\"", val)
	}

	// Missing key.
	_, found = n.GetProperty("missing")
	if found {
		t.Error("GetProperty(\"missing\") found, want not found")
	}
}

func TestNodePureDataStruct(t *testing.T) {
	t.Parallel()

	// Node must work immediately after construction — no graph needed.
	n := NewNode(snowflake.ID(1), 10, nil)
	if err := n.SetProperty("x", 1); err != nil {
		t.Fatal(err)
	}
	if v, ok := n.GetProperty("x"); !ok || v != 1 {
		t.Errorf("round-trip failed: got (%v, %v)", v, ok)
	}
}

func TestNodePropertiesMap(t *testing.T) {
	t.Parallel()

	n := NewNode(snowflake.ID(1), 10, nil)
	_ = n.SetProperty("name", "Alice")
	_ = n.SetProperty("age", 30)

	m := n.PropertiesMap()
	if len(m) != 2 {
		t.Fatalf("PropertiesMap() len = %d, want 2", len(m))
	}
	if m["name"] != "Alice" || m["age"] != 30 {
		t.Errorf("PropertiesMap() = %v, unexpected values", m)
	}
}

func TestNodeDeleteProperty(t *testing.T) {
	t.Parallel()

	n := NewNode(snowflake.ID(1), 10, nil)
	_ = n.SetProperty("name", "Alice")
	_ = n.SetProperty("age", 30)

	found, err := n.DeleteProperty("name")
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("DeleteProperty(\"name\") should return true")
	}
	if _, ok := n.GetProperty("name"); ok {
		t.Fatal("GetProperty(\"name\") should return false after delete")
	}
}

func TestNodePropertiesMapIsIndependent(t *testing.T) {
	t.Parallel()

	n := NewNode(snowflake.ID(1), 10, nil)
	_ = n.SetProperty("tags", []string{"a", "b"})

	m := n.PropertiesMap()
	m["tags"].([]string)[0] = "MUTATED"

	val, _ := n.GetProperty("tags")
	origSlice := val.([]string)
	if origSlice[0] == "MUTATED" {
		t.Fatal("PropertiesMap: mutating returned map affected internal node state")
	}
}

func TestNodeTemporalDefaultNil(t *testing.T) {
	t.Parallel()

	n := NewNode(snowflake.ID(1), 10, nil)
	if n.Temporal() != nil {
		t.Fatal("Temporal() should default to nil")
	}
}

func TestNodeTemporalRoundTrip(t *testing.T) {
	t.Parallel()

	n := NewNode(snowflake.ID(1), 10, nil)
	tm := &TemporalMetadata{}
	n.SetTemporal(tm)
	if n.Temporal() != tm {
		t.Fatal("Temporal() should return the value set by SetTemporal()")
	}
}

func TestNodeIntegrityDefaultNil(t *testing.T) {
	t.Parallel()

	n := NewNode(snowflake.ID(1), 10, nil)
	if n.Integrity() != nil {
		t.Fatal("Integrity() should default to nil")
	}
}

func TestNodeIntegrityRoundTrip(t *testing.T) {
	t.Parallel()

	n := NewNode(snowflake.ID(1), 10, nil)
	ig := &NodeIntegrity{}
	n.SetIntegrity(ig)
	if n.Integrity() != ig {
		t.Fatal("Integrity() should return the value set by SetIntegrity()")
	}
}

func TestNodeTemporalFieldsRoundTrip(t *testing.T) {
	t.Parallel()

	n := NewNode(snowflake.ID(1), 10, nil)
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
	n.SetTemporal(tm)
	got := n.Temporal()
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

func TestNodeIntegrityFieldsRoundTrip(t *testing.T) {
	t.Parallel()

	n := NewNode(snowflake.ID(1), 10, nil)
	ig := &NodeIntegrity{
		Hash:     "abc123",
		PrevHash: "def456",
	}
	n.SetIntegrity(ig)
	got := n.Integrity()
	if got.Hash != "abc123" || got.PrevHash != "def456" {
		t.Errorf("Hash/PrevHash = %q/%q, want abc123/def456", got.Hash, got.PrevHash)
	}
}

func TestNodePropertiesReturnsCopy(t *testing.T) {
	t.Parallel()

	n := NewNode(snowflake.ID(1), 10, nil)
	if err := n.SetProperty("weight", 42); err != nil {
		t.Fatal(err)
	}

	props := n.Properties()
	if err := props.Set("injected", "bad"); err != nil {
		t.Fatal(err)
	}

	// The internal state must not have changed.
	if _, found := n.GetProperty("injected"); found {
		t.Fatal("Properties() returned an alias to internal state")
	}
}

func TestNodeHasLabelTokenRaw(t *testing.T) {
	t.Parallel()

	n := NewNode(snowflake.ID(1), 10, []uint16{20, 30})

	cases := []struct {
		name string
		tok  uint16
		want bool
	}{
		{"primary hit", 10, true},
		{"extra hit", 20, true},
		{"extra hit second", 30, true},
		{"miss", 99, false},
		{"token 0 reserved", 0, false},
	}
	for _, tc := range cases {
		if got := n.HasLabelTokenRaw(tc.tok); got != tc.want {
			t.Errorf("HasLabelTokenRaw(%d) [%s] = %v, want %v", tc.tok, tc.name, got, tc.want)
		}
	}
}

func TestLabelTokenValue(t *testing.T) {
	t.Parallel()

	n := NewNode(snowflake.ID(1), 42, nil)
	tok := n.PrimaryLabelToken()
	if tok.Value() != 42 {
		t.Errorf("labelToken(42).Value() = %d, want 42", tok.Value())
	}
}

func TestNodeVersion(t *testing.T) {
	t.Parallel()

	n := NewNode(snowflake.ID(1), 10, nil)
	if n.Version() != 0 {
		t.Errorf("default Version() = %d, want 0", n.Version())
	}
	n.SetVersion(5)
	if n.Version() != 5 {
		t.Errorf("after SetVersion(5), Version() = %d", n.Version())
	}
}
