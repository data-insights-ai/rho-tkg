package types

import (
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

	keys := []string{"tkg_id", "tkg_version", "tkg_", "tkg_anything"}
	n := NewNode(snowflake.ID(1), 10, nil)
	for _, key := range keys {
		if err := n.SetProperty(key, "x"); err == nil {
			t.Errorf("SetProperty(%q, ...) should return error", key)
		}
	}
}

func TestTokenZeroReservedNode(t *testing.T) {
	t.Parallel()

	n := NewNode(snowflake.ID(1), 0, nil)
	if n.HasLabelToken(0) {
		t.Fatal("HasLabelToken(0) should always return false (reserved)")
	}
}

func TestNodeExtraLabelTokensReturnsCopy(t *testing.T) {
	t.Parallel()

	n := NewNode(snowflake.ID(1), 10, []uint16{20, 30})
	extras := n.ExtraLabelTokens()
	extras[0] = 999 // Mutate the returned slice.

	// The internal state must not have changed.
	got := n.ExtraLabelTokens()
	if got[0] == 999 {
		t.Fatal("ExtraLabelTokens returned an alias to internal state")
	}
}

// ─── Functional tests ────────────────────────────────────────────────────────

func TestNewNode(t *testing.T) {
	t.Parallel()

	n := NewNode(snowflake.ID(42), 10, []uint16{20, 30})
	if n.InternalID() != snowflake.ID(42) {
		t.Errorf("InternalID() = %d, want 42", n.InternalID())
	}
	if n.PrimaryLabelToken() != 10 {
		t.Errorf("PrimaryLabelToken() = %d, want 10", n.PrimaryLabelToken())
	}
	extras := n.ExtraLabelTokens()
	if len(extras) != 2 || extras[0] != 20 || extras[1] != 30 {
		t.Errorf("ExtraLabelTokens() = %v, want [20 30]", extras)
	}
}

func TestNodePrimaryLabelToken(t *testing.T) {
	t.Parallel()

	n := NewNode(snowflake.ID(1), 7, nil)
	if n.PrimaryLabelToken() != 7 {
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

	if !n.HasLabelToken(10) {
		t.Error("HasLabelToken(10) = false, want true (primary)")
	}
	if !n.HasLabelToken(20) {
		t.Error("HasLabelToken(20) = false, want true (extra)")
	}
	if n.HasLabelToken(99) {
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
	if len(all) != 3 || all[0] != 10 || all[1] != 20 || all[2] != 30 {
		t.Errorf("AllLabelTokens() = %v, want [10 20 30]", all)
	}
}

func TestNodeInternalID(t *testing.T) {
	t.Parallel()

	n := NewNode(snowflake.ID(123456789), 1, nil)
	if n.InternalID() != snowflake.ID(123456789) {
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
