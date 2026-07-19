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

func TestNodeSetPropertyRejectsTKGPrefix(t *testing.T) {
	t.Parallel()

	n := NewNode(NodeID(snowflake.ID(1)), 10, nil)
	if err := n.SetProperty("tkg_labels", "hack"); err == nil {
		t.Fatal("SetProperty(\"tkg_labels\", ...) should return error")
	}
}

func TestNodeSetPropertyRejectsTKGPrefixVariants(t *testing.T) {
	t.Parallel()

	keys := []string{"tkg_type", "tkg_version", "tkg_", "tkg_anything"}
	n := NewNode(NodeID(snowflake.ID(1)), 10, nil)
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
	n := NewNode(NodeID(snowflake.ID(1)), 10, nil)
	err := n.SetProperty("bad", []any{&myStruct{X: 1}})
	if err == nil {
		t.Fatal("SetProperty should reject nested pointer in []any")
	}
	if !errors.Is(err, ErrUnsupportedValueType) {
		t.Errorf("errors.Is(err, ErrUnsupportedValueType) = false; err = %v", err)
	}
}

func TestNewNodeAllowsZeroPrimaryLabelForStoreValidation(t *testing.T) {
	t.Parallel()

	n := NewNode(NodeID(snowflake.ID(1)), 0, nil)
	if n == nil {
		t.Fatal("NewNode returned nil")
	}
	if n.PrimaryLabelToken() != labelToken(0) {
		t.Fatalf("PrimaryLabelToken() = %d, want reserved token 0", n.PrimaryLabelToken())
	}
	if n.HasLabelTokenRaw(0) {
		t.Fatal("HasLabelTokenRaw(0) should still return false")
	}
}

func TestNodeNilReceiverMethodsFailClosed(t *testing.T) {
	t.Parallel()

	var n *Node
	if n.ID() != 0 {
		t.Fatalf("ID() = %v, want 0", n.ID())
	}
	if n.InternalID() != 0 {
		t.Fatalf("InternalID() = %v, want 0", n.InternalID())
	}
	if n.PrimaryLabelToken() != 0 {
		t.Fatalf("PrimaryLabelToken() = %v, want 0", n.PrimaryLabelToken())
	}
	if got := n.ExtraLabelTokens(); got != nil {
		t.Fatalf("ExtraLabelTokens() = %v, want nil", got)
	}
	if got := n.AllLabelTokens(); got != nil {
		t.Fatalf("AllLabelTokens() = %v, want nil", got)
	}
	if n.HasLabelToken(1) {
		t.Fatal("HasLabelToken(1) = true, want false")
	}
	if n.HasLabelTokenRaw(1) {
		t.Fatal("HasLabelTokenRaw(1) = true, want false")
	}
	if n.LabelTokenCount() != 0 {
		t.Fatalf("LabelTokenCount() = %d, want 0", n.LabelTokenCount())
	}
	if n.LabelTokenRawAt(0) != 0 {
		t.Fatalf("LabelTokenRawAt(0) = %d, want 0", n.LabelTokenRawAt(0))
	}
	if err := n.SetProperties(nil); !errors.Is(err, ErrNilNode) {
		t.Fatalf("SetProperties(nil) = %v, want ErrNilNode", err)
	}
	if err := n.SetOwnedProperties(OwnedPropertySlice{}); !errors.Is(err, ErrNilNode) {
		t.Fatalf("SetOwnedProperties(nil receiver) = %v, want ErrNilNode", err)
	}
	if err := n.SetProperty("x", int64(1)); !errors.Is(err, ErrNilNode) {
		t.Fatalf("SetProperty = %v, want ErrNilNode", err)
	}
	if got, ok := n.GetProperty("x"); got != nil || ok {
		t.Fatalf("GetProperty = (%v, %v), want (nil, false)", got, ok)
	}
	if deleted, err := n.DeleteProperty("x"); !errors.Is(err, ErrNilNode) || deleted {
		t.Fatalf("DeleteProperty = (%v, %v), want (false, ErrNilNode)", deleted, err)
	}
	if n.PropertyCount() != 0 {
		t.Fatalf("PropertyCount() = %d, want 0", n.PropertyCount())
	}
	if got := n.Properties(); got != nil {
		t.Fatalf("Properties() = %v, want nil", got)
	}
	if got := n.PropertiesMap(); got != nil {
		t.Fatalf("PropertiesMap() = %v, want nil", got)
	}
	if n.Version() != 0 {
		t.Fatalf("Version() = %d, want 0", n.Version())
	}
	n.SetVersion(7)
	if n.Temporal() != nil {
		t.Fatal("Temporal() != nil, want nil")
	}
	n.SetTemporal(&TemporalMetadata{})
	if n.Integrity() != nil {
		t.Fatal("Integrity() != nil, want nil")
	}
	n.SetIntegrity(&NodeIntegrity{})
	if n.AddLabelTokenRaw(2) {
		t.Fatal("AddLabelTokenRaw = true, want false")
	}
	if n.RemoveLabelTokenRaw(2) {
		t.Fatal("RemoveLabelTokenRaw = true, want false")
	}
	n.SetLabelTokensRaw(1, []uint16{2})
	if n.PrimaryLabelToken() != 0 {
		t.Fatalf("SetLabelTokensRaw mutated nil receiver")
	}
	if cp := n.DeepCopy(); cp != nil {
		t.Fatalf("DeepCopy() = %v, want nil", cp)
	}
}

func TestNodeSetOwnedProperties(t *testing.T) {
	t.Parallel()

	n := NewNode(NodeID(snowflake.ID(1)), 10, nil)
	owned, err := NewOwnedPropertySlice(map[string]any{"name": "case-1"})
	if err != nil {
		t.Fatalf("NewOwnedPropertySlice: %v", err)
	}
	if err := n.SetOwnedProperties(owned); err != nil {
		t.Fatalf("SetOwnedProperties: %v", err)
	}
	got, ok := n.GetProperty("name")
	if !ok || got != "case-1" {
		t.Fatalf("GetProperty(name) = (%v, %v), want (case-1, true)", got, ok)
	}
}

func TestNodeSetOwnedPropertiesConsumesOwnership(t *testing.T) {
	t.Parallel()

	owned, err := NewOwnedPropertySlice(map[string]any{"name": []string{"case-1"}})
	if err != nil {
		t.Fatalf("NewOwnedPropertySlice: %v", err)
	}
	alias := owned

	first := NewNode(NodeID(snowflake.ID(1)), 10, nil)
	second := NewNode(NodeID(snowflake.ID(2)), 10, nil)
	if err := first.SetOwnedProperties(owned); err != nil {
		t.Fatalf("first SetOwnedProperties: %v", err)
	}
	if err := second.SetOwnedProperties(alias); err != nil {
		t.Fatalf("second SetOwnedProperties: %v", err)
	}
	if _, ok := second.GetProperty("name"); ok {
		t.Fatal("reused OwnedPropertySlice installed properties on second node")
	}
	if err := first.SetProperty("name", []string{"mutated"}); err != nil {
		t.Fatalf("mutate first: %v", err)
	}
	if _, ok := second.GetProperty("name"); ok {
		t.Fatal("mutating first node affected second node after owned reuse")
	}
}

func TestNewNodeAllowsZeroExtraLabelForStoreValidation(t *testing.T) {
	t.Parallel()

	n := NewNode(NodeID(snowflake.ID(1)), 1, []uint16{5, 0})
	extras := n.ExtraLabelTokens()
	if len(extras) != 2 {
		t.Fatalf("ExtraLabelTokens() len = %d, want 2", len(extras))
	}
	if extras[1] != labelToken(0) {
		t.Fatalf("second extra label = %d, want reserved token 0", extras[1])
	}
	if n.HasLabelTokenRaw(0) {
		t.Fatal("HasLabelTokenRaw(0) should still return false")
	}
}

func TestNewNodeDeduplicatesExtraLabels(t *testing.T) {
	t.Parallel()

	n := NewNode(NodeID(snowflake.ID(1)), 10, []uint16{20, 30, 20})
	extras := n.ExtraLabelTokens()
	if len(extras) != 2 {
		t.Fatalf("ExtraLabelTokens() len = %d, want 2 (deduped)", len(extras))
	}
	if n.LabelTokenCount() != 3 {
		t.Errorf("LabelTokenCount() = %d, want 3", n.LabelTokenCount())
	}
}

func TestNodeSetLabelTokensRawCanonicalizesExtras(t *testing.T) {
	t.Parallel()

	n := NewNode(NodeID(snowflake.ID(1)), 10, []uint16{20, 30})
	n.SetLabelTokensRaw(40, []uint16{40, 50, 50, 0})

	if got := n.PrimaryLabelToken(); got != labelToken(40) {
		t.Fatalf("PrimaryLabelToken() = %d, want 40", got)
	}
	extras := n.ExtraLabelTokens()
	if len(extras) != 2 {
		t.Fatalf("ExtraLabelTokens len = %d, want 2", len(extras))
	}
	if extras[0] != labelToken(50) || extras[1] != labelToken(0) {
		t.Fatalf("ExtraLabelTokens = %v, want [50 0]", extras)
	}
	if n.HasLabelTokenRaw(40) != true || n.HasLabelTokenRaw(50) != true {
		t.Fatal("SetLabelTokensRaw did not install expected labels")
	}
	if n.HasLabelTokenRaw(0) {
		t.Fatal("HasLabelTokenRaw(0) = true, want false")
	}
}

func TestNewNodeRemovesPrimaryFromExtras(t *testing.T) {
	t.Parallel()

	n := NewNode(NodeID(snowflake.ID(1)), 10, []uint16{10, 20})
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

func TestNodeLabelTokenRawAt(t *testing.T) {
	t.Parallel()

	n := NewNode(NodeID(snowflake.ID(1)), 10, []uint16{20, 30})
	for _, tc := range []struct {
		index int
		want  uint16
	}{
		{index: -1, want: 0},
		{index: 0, want: 10},
		{index: 1, want: 20},
		{index: 2, want: 30},
		{index: 3, want: 0},
	} {
		if got := n.LabelTokenRawAt(tc.index); got != tc.want {
			t.Fatalf("LabelTokenRawAt(%d) = %d, want %d", tc.index, got, tc.want)
		}
	}
}

func TestAddLabelTokenRawBranches(t *testing.T) {
	t.Parallel()

	n := NewNode(NodeID(snowflake.ID(1)), 10, []uint16{20})
	if n.AddLabelTokenRaw(0) {
		t.Fatal("AddLabelTokenRaw(0) = true, want false")
	}
	if n.AddLabelTokenRaw(10) {
		t.Fatal("AddLabelTokenRaw(primary) = true, want false")
	}
	if n.AddLabelTokenRaw(20) {
		t.Fatal("AddLabelTokenRaw(existing extra) = true, want false")
	}
	if !n.AddLabelTokenRaw(30) {
		t.Fatal("AddLabelTokenRaw(new extra) = false, want true")
	}
	if n.AddLabelTokenRaw(30) {
		t.Fatal("AddLabelTokenRaw(duplicate added extra) = true, want false")
	}
	if !n.HasLabelTokenRaw(30) {
		t.Fatal("HasLabelTokenRaw(30) = false after add, want true")
	}
	if n.LabelTokenCount() != 3 {
		t.Fatalf("LabelTokenCount() = %d, want 3", n.LabelTokenCount())
	}
}

func TestRemoveLabelTokenRawRefusesLastLabel(t *testing.T) {
	t.Parallel()

	n := NewNode(NodeID(snowflake.ID(1)), 10, nil)
	if n.RemoveLabelTokenRaw(10) {
		t.Fatal("RemoveLabelTokenRaw(primary) = true for single-label node, want false")
	}
	if n.PrimaryLabelToken() != labelToken(10) {
		t.Errorf("PrimaryLabelToken() = %d, want 10", n.PrimaryLabelToken())
	}
	if n.LabelTokenCount() != 1 {
		t.Errorf("LabelTokenCount() = %d, want 1", n.LabelTokenCount())
	}
}

// TestRemoveLabelTokenRawSkipsZeroTokenWhenPromoting is BACKLOG 6d:
// RemoveLabelTokenRaw's promotion step used to promote extraLabels[0]
// unconditionally. NewNode's own documented/tested contract
// (TestNewNodeAllowsZeroExtraLabelForStoreValidation) permits token 0 to sit
// in the extra-label set, so a node built as
// NewNode(id, primary, []uint16{0, realLabel}) had reserved token 0 as its
// FIRST extra. Removing the primary label promoted that 0 to primary —
// HasLabelToken(0) always returns false (token 0 reserved), so the node was
// left with a primary label that could never again be observed as present,
// even though LabelTokenCount() still reported it. A perfectly good
// non-zero candidate (realLabel) sat right behind it in the extra list.
func TestRemoveLabelTokenRawSkipsZeroTokenWhenPromoting(t *testing.T) {
	t.Parallel()

	n := NewNode(NodeID(snowflake.ID(1)), 10, []uint16{0, 20})
	if !n.RemoveLabelTokenRaw(10) {
		t.Fatal("RemoveLabelTokenRaw(primary) = false, want true — BACKLOG 6d regression")
	}
	if n.PrimaryLabelToken() != labelToken(20) {
		t.Fatalf("PrimaryLabelToken() = %d, want 20 (the non-zero extra), not the reserved token 0 — BACKLOG 6d regression", n.PrimaryLabelToken())
	}
	if !n.HasLabelToken(labelToken(20)) {
		t.Fatal("HasLabelToken(20) = false after promotion, want true")
	}
	// The skipped token-0 extra must survive as an extra label, not vanish.
	extras := n.ExtraLabelTokens()
	if len(extras) != 1 || extras[0] != labelToken(0) {
		t.Fatalf("ExtraLabelTokens() = %v, want [0] (the skipped zero token retained as an extra)", extras)
	}
	if n.LabelTokenCount() != 2 {
		t.Fatalf("LabelTokenCount() = %d, want 2", n.LabelTokenCount())
	}
}

// TestRemoveLabelTokenRawRefusesPromotionWhenOnlyZeroExtraRemains covers the
// edge the skip-token-0 fix introduces: if the ONLY remaining extra is token
// 0, there is no valid non-zero candidate to promote, so the removal must be
// refused — exactly like the pre-existing "no extras at all" refusal
// (TestRemoveLabelTokenRawRefusesLastLabel), not silently promote 0 anyway.
func TestRemoveLabelTokenRawRefusesPromotionWhenOnlyZeroExtraRemains(t *testing.T) {
	t.Parallel()

	n := NewNode(NodeID(snowflake.ID(1)), 10, []uint16{0})
	if n.RemoveLabelTokenRaw(10) {
		t.Fatal("RemoveLabelTokenRaw(primary) = true when only a reserved zero extra remains, want false — BACKLOG 6d regression")
	}
	if n.PrimaryLabelToken() != labelToken(10) {
		t.Fatalf("PrimaryLabelToken() = %d, want unchanged 10 after refused removal", n.PrimaryLabelToken())
	}
	extras := n.ExtraLabelTokens()
	if len(extras) != 1 || extras[0] != labelToken(0) {
		t.Fatalf("ExtraLabelTokens() = %v, want unchanged [0] after refused removal", extras)
	}
}

func TestTokenZeroReservedNode(t *testing.T) {
	t.Parallel()

	n := NewNode(NodeID(snowflake.ID(1)), 5, nil) // valid token
	if n.HasLabelToken(labelToken(0)) {
		t.Fatal("HasLabelToken(0) should always return false (reserved)")
	}
}

func TestNodeExtraLabelTokensReturnsCopy(t *testing.T) {
	t.Parallel()

	n := NewNode(NodeID(snowflake.ID(1)), 10, []uint16{20, 30})
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

	n := NewNode(NodeID(snowflake.ID(42)), 10, []uint16{20, 30})
	if n.ID() != NodeID(snowflake.ID(42)) {
		t.Errorf("InternalID() = %d, want 42", n.ID())
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

	n := NewNode(NodeID(snowflake.ID(1)), 7, nil)
	if n.PrimaryLabelToken() != labelToken(7) {
		t.Errorf("PrimaryLabelToken() = %d, want 7", n.PrimaryLabelToken())
	}
}

func TestNodeExtraLabelTokens(t *testing.T) {
	t.Parallel()

	// Single-label node: extras should be nil.
	single := NewNode(NodeID(snowflake.ID(1)), 10, nil)
	if extras := single.ExtraLabelTokens(); extras != nil {
		t.Errorf("single-label ExtraLabelTokens() = %v, want nil", extras)
	}

	// Multi-label node: extras should match.
	multi := NewNode(NodeID(snowflake.ID(2)), 10, []uint16{20, 30})
	extras := multi.ExtraLabelTokens()
	if len(extras) != 2 {
		t.Fatalf("multi-label ExtraLabelTokens() len = %d, want 2", len(extras))
	}
}

func TestNodeHasLabelToken(t *testing.T) {
	t.Parallel()

	n := NewNode(NodeID(snowflake.ID(1)), 10, []uint16{20, 30})

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

	single := NewNode(NodeID(snowflake.ID(1)), 10, nil)
	if single.LabelTokenCount() != 1 {
		t.Errorf("single-label LabelTokenCount() = %d, want 1", single.LabelTokenCount())
	}

	multi := NewNode(NodeID(snowflake.ID(2)), 10, []uint16{20, 30})
	if multi.LabelTokenCount() != 3 {
		t.Errorf("multi-label LabelTokenCount() = %d, want 3", multi.LabelTokenCount())
	}
}

func TestNodeAllLabelTokens(t *testing.T) {
	t.Parallel()

	n := NewNode(NodeID(snowflake.ID(1)), 10, []uint16{20, 30})
	all := n.AllLabelTokens()
	if len(all) != 3 || all[0] != labelToken(10) || all[1] != labelToken(20) || all[2] != labelToken(30) {
		t.Errorf("AllLabelTokens() = %v, want [10 20 30]", all)
	}
}

func TestNodeInternalID(t *testing.T) {
	t.Parallel()

	n := NewNode(NodeID(snowflake.ID(123456789)), 1, nil)
	if n.ID() != NodeID(snowflake.ID(123456789)) {
		t.Errorf("InternalID() = %d, want 123456789", n.ID())
	}
}

func TestNodeSetPropertyAcceptsNormalKeys(t *testing.T) {
	t.Parallel()

	n := NewNode(NodeID(snowflake.ID(1)), 10, nil)
	if err := n.SetProperty("name", "Alice"); err != nil {
		t.Fatalf("SetProperty(\"name\", \"Alice\") returned unexpected error: %v", err)
	}
}

func TestNodeSetPropertyCopiesCallerValue(t *testing.T) {
	t.Parallel()

	n := NewNode(NodeID(snowflake.ID(1)), 10, nil)
	tags := []string{"alpha", "beta"}
	if err := n.SetProperty("tags", tags); err != nil {
		t.Fatalf("SetProperty: %v", err)
	}

	tags[0] = "mutated"
	got, ok := n.GetProperty("tags")
	if !ok {
		t.Fatal("GetProperty(\"tags\") missing")
	}
	if got.([]string)[0] != "alpha" {
		t.Fatalf("SetProperty retained caller slice alias: %q", got.([]string)[0])
	}
}

func TestNodeGetProperty(t *testing.T) {
	t.Parallel()

	n := NewNode(NodeID(snowflake.ID(1)), 10, nil)
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

func TestNodeGetPropertyReturnsIndependentValue(t *testing.T) {
	t.Parallel()

	n := NewNode(NodeID(snowflake.ID(1)), 10, nil)
	if err := n.SetProperty("meta", map[string]any{
		"tags": []any{"alpha", "beta"},
	}); err != nil {
		t.Fatal(err)
	}

	got, ok := n.GetProperty("meta")
	if !ok {
		t.Fatal("GetProperty(\"meta\") missing")
	}
	got.(map[string]any)["tags"].([]any)[0] = "mutated"

	again, ok := n.GetProperty("meta")
	if !ok {
		t.Fatal("GetProperty(\"meta\") missing after returned-value mutation")
	}
	if again.(map[string]any)["tags"].([]any)[0] != "alpha" {
		t.Fatalf("GetProperty returned internal mutable state: %v", again)
	}
}

func TestNodePropertyValueEqual(t *testing.T) {
	t.Parallel()

	var nilNode *Node
	if found, equal := nilNode.PropertyValueEqual("score", math.NaN()); found || equal {
		t.Fatalf("nil PropertyValueEqual = (%v, %v), want (false, false)", found, equal)
	}

	n := NewNode(NodeID(snowflake.ID(1)), 10, nil)
	if err := n.SetProperty("score", math.NaN()); err != nil {
		t.Fatal(err)
	}
	if err := n.SetProperty("trail", []float32{1, float32(math.NaN())}); err != nil {
		t.Fatal(err)
	}

	if found, equal := n.PropertyValueEqual("score", math.NaN()); !found || !equal {
		t.Fatalf("PropertyValueEqual(score) = (%v, %v), want (true, true)", found, equal)
	}
	if found, equal := n.PropertyValueEqual("trail", []float32{1, float32(math.NaN())}); !found || !equal {
		t.Fatalf("PropertyValueEqual(trail) = (%v, %v), want (true, true)", found, equal)
	}
	if found, equal := n.PropertyValueEqual("trail", []float64{1, math.NaN()}); !found || equal {
		t.Fatalf("PropertyValueEqual(type mismatch) = (%v, %v), want (true, false)", found, equal)
	}
	if found, equal := n.PropertyValueEqual("missing", nil); found || equal {
		t.Fatalf("PropertyValueEqual(missing) = (%v, %v), want (false, false)", found, equal)
	}

	var nilTrail []float32
	if err := n.SetProperty("nil_trail", nilTrail); err != nil {
		t.Fatal(err)
	}
	if found, equal := n.PropertyValueEqual("nil_trail", nilTrail); !found || !equal {
		t.Fatalf("PropertyValueEqual(nil_trail typed nil) = (%v, %v), want (true, true)", found, equal)
	}
	if found, equal := n.PropertyValueEqual("nil_trail", []float32{}); !found || equal {
		t.Fatalf("PropertyValueEqual(nil_trail empty) = (%v, %v), want (true, false)", found, equal)
	}

	var nilMeta map[string]any
	if err := n.SetProperty("nil_meta", nilMeta); err != nil {
		t.Fatal(err)
	}
	if found, equal := n.PropertyValueEqual("nil_meta", nilMeta); !found || !equal {
		t.Fatalf("PropertyValueEqual(nil_meta typed nil) = (%v, %v), want (true, true)", found, equal)
	}
	if found, equal := n.PropertyValueEqual("nil_meta", map[string]any{}); !found || equal {
		t.Fatalf("PropertyValueEqual(nil_meta empty) = (%v, %v), want (true, false)", found, equal)
	}
}

func TestNodeIndexablePropertyValueKey(t *testing.T) {
	t.Parallel()

	var nilNode *Node
	if got, ok := nilNode.IndexablePropertyValueKey("name"); got != "" || ok {
		t.Fatalf("nil IndexablePropertyValueKey = (%q, %v), want (\"\", false)", got, ok)
	}

	n := NewNode(NodeID(snowflake.ID(1)), 10, nil)
	if err := n.SetProperty("name", "Alice"); err != nil {
		t.Fatal(err)
	}
	if err := n.SetProperty("meta", map[string]any{"tags": []any{"alpha"}}); err != nil {
		t.Fatal(err)
	}
	if err := n.SetProperty("zero", math.Copysign(0, -1)); err != nil {
		t.Fatal(err)
	}

	if got, ok := n.IndexablePropertyValueKey("name"); got != "s:Alice" || !ok {
		t.Fatalf("IndexablePropertyValueKey(name) = (%q, %v), want (s:Alice, true)", got, ok)
	}
	if got, ok := n.IndexablePropertyValueKey("zero"); got != "f64:0" || !ok {
		t.Fatalf("IndexablePropertyValueKey(zero) = (%q, %v), want (f64:0, true)", got, ok)
	}
	if got, ok := n.IndexablePropertyValueKey("meta"); got != "" || !ok {
		t.Fatalf("IndexablePropertyValueKey(meta) = (%q, %v), want (\"\", true)", got, ok)
	}
	if got, ok := n.IndexablePropertyValueKey("missing"); got != "" || ok {
		t.Fatalf("IndexablePropertyValueKey(missing) = (%q, %v), want (\"\", false)", got, ok)
	}
}

func TestNodeForEachIndexablePropertyValueKey(t *testing.T) {
	t.Parallel()

	var nilNode *Node
	nilNode.ForEachIndexablePropertyValueKey(func(string, string) bool {
		t.Fatal("nil node invoked callback")
		return false
	})

	n := NewNode(NodeID(snowflake.ID(1)), 10, nil)
	if err := n.SetProperty("name", "Alice"); err != nil {
		t.Fatal(err)
	}
	if err := n.SetProperty("age", int64(42)); err != nil {
		t.Fatal(err)
	}
	if err := n.SetProperty("meta", map[string]any{"tags": []any{"alpha"}}); err != nil {
		t.Fatal(err)
	}
	n.ForEachIndexablePropertyValueKey(nil)

	got := make(map[string]string)
	n.ForEachIndexablePropertyValueKey(func(propertyKey, valueKey string) bool {
		got[propertyKey] = valueKey
		return true
	})
	want := map[string]string{"age": "i64:42", "name": "s:Alice"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ForEachIndexablePropertyValueKey = %v, want %v", got, want)
	}

	calls := 0
	n.ForEachIndexablePropertyValueKey(func(string, string) bool {
		calls++
		return false
	})
	if calls != 1 {
		t.Fatalf("early-stop callback calls = %d, want 1", calls)
	}
}

func TestNodeFloat32SlicePropertyCopy(t *testing.T) {
	t.Parallel()

	var nilNode *Node
	if got, ok := nilNode.Float32SlicePropertyCopy("embedding"); got != nil || ok {
		t.Fatalf("nil Float32SlicePropertyCopy = (%v, %v), want (nil, false)", got, ok)
	}

	n := NewNode(NodeID(snowflake.ID(1)), 10, nil)
	if err := n.SetProperty("embedding", []float32{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	if err := n.SetProperty("wire", []any{float32(1), float64(2)}); err != nil {
		t.Fatal(err)
	}
	if err := n.SetProperty("bad", []any{float32(1), "x"}); err != nil {
		t.Fatal(err)
	}
	if err := n.SetProperty("tags", []string{"a"}); err != nil {
		t.Fatal(err)
	}
	var nilEmbedding []float32
	if err := n.SetProperty("nil_embedding", nilEmbedding); err != nil {
		t.Fatal(err)
	}
	var nilWire []any
	if err := n.SetProperty("nil_wire", nilWire); err != nil {
		t.Fatal(err)
	}

	got, ok := n.Float32SlicePropertyCopy("embedding")
	if !ok || len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Fatalf("Float32SlicePropertyCopy(embedding) = (%v, %v), want ([1 2 3], true)", got, ok)
	}
	got[0] = 99
	again, ok := n.Float32SlicePropertyCopy("embedding")
	if !ok || again[0] != 1 {
		t.Fatalf("Float32SlicePropertyCopy returned alias; second read = (%v, %v), want first value 1", again, ok)
	}

	wire, ok := n.Float32SlicePropertyCopy("wire")
	if !ok || len(wire) != 2 || wire[0] != 1 || wire[1] != 2 {
		t.Fatalf("Float32SlicePropertyCopy(wire) = (%v, %v), want ([1 2], true)", wire, ok)
	}
	if got, ok := n.Float32SlicePropertyCopy("nil_embedding"); got != nil || !ok {
		t.Fatalf("Float32SlicePropertyCopy(nil_embedding) = (%v, %v), want (nil, true)", got, ok)
	}
	if got, ok := n.Float32SlicePropertyCopy("nil_wire"); got != nil || !ok {
		t.Fatalf("Float32SlicePropertyCopy(nil_wire) = (%v, %v), want (nil, true)", got, ok)
	}
	for _, key := range []string{"bad", "tags", "missing"} {
		if got, ok := n.Float32SlicePropertyCopy(key); got != nil || ok {
			t.Fatalf("Float32SlicePropertyCopy(%s) = (%v, %v), want (nil, false)", key, got, ok)
		}
	}
}

func TestNodePureDataStruct(t *testing.T) {
	t.Parallel()

	// Node must work immediately after construction — no graph needed.
	n := NewNode(NodeID(snowflake.ID(1)), 10, nil)
	if err := n.SetProperty("x", 1); err != nil {
		t.Fatal(err)
	}
	if v, ok := n.GetProperty("x"); !ok || v != 1 {
		t.Errorf("round-trip failed: got (%v, %v)", v, ok)
	}
}

func TestNodePropertiesMap(t *testing.T) {
	t.Parallel()

	n := NewNode(NodeID(snowflake.ID(1)), 10, nil)
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

	n := NewNode(NodeID(snowflake.ID(1)), 10, nil)
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

	n := NewNode(NodeID(snowflake.ID(1)), 10, nil)
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

	n := NewNode(NodeID(snowflake.ID(1)), 10, nil)
	if n.Temporal() != nil {
		t.Fatal("Temporal() should default to nil")
	}
}

func TestNodeTemporalRoundTrip(t *testing.T) {
	t.Parallel()

	n := NewNode(NodeID(snowflake.ID(1)), 10, nil)
	tm := &TemporalMetadata{}
	n.SetTemporal(tm)
	if n.Temporal() != tm {
		t.Fatal("Temporal() should return the value set by SetTemporal()")
	}
}

func TestNodeIntegrityDefaultNil(t *testing.T) {
	t.Parallel()

	n := NewNode(NodeID(snowflake.ID(1)), 10, nil)
	if n.Integrity() != nil {
		t.Fatal("Integrity() should default to nil")
	}
}

func TestNodeIntegrityRoundTrip(t *testing.T) {
	t.Parallel()

	n := NewNode(NodeID(snowflake.ID(1)), 10, nil)
	ig := &NodeIntegrity{}
	n.SetIntegrity(ig)
	if n.Integrity() != ig {
		t.Fatal("Integrity() should return the value set by SetIntegrity()")
	}
}

func TestNodeTemporalFieldsRoundTrip(t *testing.T) {
	t.Parallel()

	n := NewNode(NodeID(snowflake.ID(1)), 10, nil)
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
	if got.BaseEntityID() != EntityID(snowflake.ID(42)) {
		t.Errorf("BaseEntityID() = %v, want 42", got.BaseEntityID())
	}
}

func TestNodeIntegrityFieldsRoundTrip(t *testing.T) {
	t.Parallel()

	n := NewNode(NodeID(snowflake.ID(1)), 10, nil)
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

	n := NewNode(NodeID(snowflake.ID(1)), 10, nil)
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

	n := NewNode(NodeID(snowflake.ID(1)), 10, []uint16{20, 30})

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

	n := NewNode(NodeID(snowflake.ID(1)), 42, nil)
	tok := n.PrimaryLabelToken()
	if tok.Value() != 42 {
		t.Errorf("labelToken(42).Value() = %d, want 42", tok.Value())
	}
}

func TestNodeVersion(t *testing.T) {
	t.Parallel()

	n := NewNode(NodeID(snowflake.ID(1)), 10, nil)
	if n.Version() != 0 {
		t.Errorf("default Version() = %d, want 0", n.Version())
	}
	n.SetVersion(5)
	if n.Version() != 5 {
		t.Errorf("after SetVersion(5), Version() = %d", n.Version())
	}
}

// ─── SnowflakeID bridge tests ────────────────────────────────────────────────

func TestNodeIDSnowflakeIDRoundTrip(t *testing.T) {
	t.Parallel()

	n := NewNode(NodeID(snowflake.ID(42)), 10, nil)
	got := n.ID().SnowflakeID()
	if got != snowflake.ID(42) {
		t.Errorf("nodeID.SnowflakeID() = %d, want 42", got)
	}
}

// ─── SetProperties tests ────────────────────────────────────────────────────

func TestNodeSetProperties(t *testing.T) {
	t.Parallel()

	n := NewNode(NodeID(snowflake.ID(1)), 10, nil)
	ps, err := NewPropertySlice(map[string]any{"name": "Alice", "age": 30})
	if err != nil {
		t.Fatal(err)
	}
	if err := n.SetProperties(ps); err != nil {
		t.Fatalf("SetProperties: %v", err)
	}

	val, ok := n.GetProperty("name")
	if !ok || val != "Alice" {
		t.Errorf("GetProperty(\"name\") = (%v, %v), want (\"Alice\", true)", val, ok)
	}
	val, ok = n.GetProperty("age")
	if !ok || val != 30 {
		t.Errorf("GetProperty(\"age\") = (%v, %v), want (30, true)", val, ok)
	}
}

func TestNodeSetPropertiesCanonicalizesAndCopies(t *testing.T) {
	t.Parallel()

	n := NewNode(NodeID(snowflake.ID(1)), 10, nil)
	tags := []string{"alpha", "beta"}
	ps := PropertySlice{
		{Key: "z", Value: "old"},
		{Key: "tags", Value: tags},
		{Key: "z", Value: "new"},
		{Key: "age", Value: 30},
	}
	if err := n.SetProperties(ps); err != nil {
		t.Fatalf("SetProperties: %v", err)
	}

	if _, ok := n.GetProperty("z"); !ok {
		t.Fatal("GetProperty(\"z\") missing after unsorted SetProperties input")
	}
	if val, _ := n.GetProperty("z"); val != "new" {
		t.Fatalf("duplicate key value = %v, want last value", val)
	}

	tags[0] = "mutated"
	got, ok := n.GetProperty("tags")
	if !ok {
		t.Fatal("GetProperty(\"tags\") missing")
	}
	gotTags := got.([]string)
	if gotTags[0] != "alpha" {
		t.Fatalf("SetProperties retained caller slice alias: %q", gotTags[0])
	}
}

func TestNodeSetPropertiesDuplicateKeyValidatesWinningValueOnly(t *testing.T) {
	t.Parallel()

	n := NewNode(NodeID(snowflake.ID(1)), 10, nil)
	ps := PropertySlice{
		{Key: "z", Value: make(chan int)},
		{Key: "a", Value: "kept"},
		{Key: "z", Value: "winner"},
	}
	if err := n.SetProperties(ps); err != nil {
		t.Fatalf("SetProperties duplicate overwritten invalid value: %v", err)
	}
	if val, ok := n.GetProperty("z"); !ok || val != "winner" {
		t.Fatalf("duplicate key value = (%v, %v), want (winner, true)", val, ok)
	}
	if got := n.PropertyCount(); got != 2 {
		t.Fatalf("PropertyCount = %d, want 2", got)
	}
}

func TestNodeSetPropertiesRejectsInvalidAndKeepsPrevious(t *testing.T) {
	t.Parallel()

	n := NewNode(NodeID(snowflake.ID(1)), 10, nil)
	if err := n.SetProperties(PropertySlice{{Key: "name", Value: "Alice"}}); err != nil {
		t.Fatalf("initial SetProperties: %v", err)
	}

	err := n.SetProperties(PropertySlice{{Key: "bad", Value: make(chan int)}})
	if !errors.Is(err, ErrUnsupportedValueType) {
		t.Fatalf("SetProperties error = %v, want ErrUnsupportedValueType", err)
	}
	if _, ok := n.GetProperty("bad"); ok {
		t.Fatal("invalid property was installed")
	}
	if got, ok := n.GetProperty("name"); !ok || got != "Alice" {
		t.Fatalf("previous property after rejected SetProperties = (%v, %v), want (Alice, true)", got, ok)
	}
}

func TestNodeSetPropertiesRejectsReservedKey(t *testing.T) {
	t.Parallel()

	n := NewNode(NodeID(snowflake.ID(1)), 10, nil)
	err := n.SetProperties(PropertySlice{{Key: "tkg_hash", Value: "x"}})
	if !errors.Is(err, ErrReservedPrefix) {
		t.Fatalf("SetProperties error = %v, want ErrReservedPrefix", err)
	}
	if n.PropertyCount() != 0 {
		t.Fatalf("reserved property was installed, count=%d", n.PropertyCount())
	}
}

// ─── Edge case tests ────────────────────────────────────────────────────────

func TestNodeHasLabelTokenRawHighCardinality(t *testing.T) {
	t.Parallel()

	extras := make([]uint16, 15)
	for i := range extras {
		extras[i] = uint16(100 + i) // tokens 100..114
	}
	n := NewNode(NodeID(snowflake.ID(1)), 10, extras)

	// Hit on last extra label (full scan).
	if !n.HasLabelTokenRaw(114) {
		t.Error("HasLabelTokenRaw(114) = false, want true (last extra label)")
	}
	// Miss on absent token.
	if n.HasLabelTokenRaw(999) {
		t.Error("HasLabelTokenRaw(999) = true, want false (absent)")
	}
}

func TestNodeTemporalSharedPointerMutation(t *testing.T) {
	t.Parallel()

	n := NewNode(NodeID(snowflake.ID(1)), 10, nil)
	tm := &TemporalMetadata{ValidFrom: 1000}
	n.SetTemporal(tm)

	// Mutate through the returned pointer.
	n.Temporal().ValidFrom = 2000

	// Node must reflect the change (shared pointer).
	if n.Temporal().ValidFrom != 2000 {
		t.Errorf("Temporal().ValidFrom = %d, want 2000 (shared pointer mutation)", n.Temporal().ValidFrom)
	}
}

func TestNodeSetTemporalNil(t *testing.T) {
	t.Parallel()

	n := NewNode(NodeID(snowflake.ID(1)), 10, nil)
	tm := &TemporalMetadata{ValidFrom: 1000}
	n.SetTemporal(tm)
	n.SetTemporal(nil)

	if n.Temporal() != nil {
		t.Fatal("Temporal() should be nil after SetTemporal(nil)")
	}
}

func TestNodeTemporalOverwrite(t *testing.T) {
	t.Parallel()

	n := NewNode(NodeID(snowflake.ID(1)), 10, nil)
	old := &TemporalMetadata{ValidFrom: 1000}
	n.SetTemporal(old)

	replacement := &TemporalMetadata{ValidFrom: 9999}
	n.SetTemporal(replacement)

	if n.Temporal() != replacement {
		t.Fatal("Temporal() should return the replacement pointer")
	}
	// Old pointer must be detached — mutating it doesn't affect node.
	old.ValidFrom = 5555
	if n.Temporal().ValidFrom != 9999 {
		t.Errorf("old pointer mutation affected node: ValidFrom = %d, want 9999", n.Temporal().ValidFrom)
	}
}

func TestNodeStressManyProperties(t *testing.T) {
	t.Parallel()

	n := NewNode(NodeID(snowflake.ID(1)), 10, nil)
	for i := range 1000 {
		key := fmt.Sprintf("prop_%04d", i)
		if err := n.SetProperty(key, i); err != nil {
			t.Fatalf("SetProperty(%q) failed: %v", key, err)
		}
	}

	// All 1000 retrievable.
	for i := range 1000 {
		key := fmt.Sprintf("prop_%04d", i)
		val, ok := n.GetProperty(key)
		if !ok || val != i {
			t.Fatalf("GetProperty(%q) = (%v, %v), want (%d, true)", key, val, ok, i)
		}
	}

	// Sorted invariant on internal properties.
	props := n.Properties()
	if !sort.SliceIsSorted(props, func(i, j int) bool { return props[i].Key < props[j].Key }) {
		t.Fatal("Node properties are not sorted after 1000 insertions")
	}
}

// ─── DeepCopy tests ──────────────────────────────────────────────────────────

func TestNodeDeepCopy(t *testing.T) {
	t.Parallel()

	n := NewNode(NodeID(snowflake.ID(42)), 10, []uint16{20, 30})
	_ = n.SetProperty("name", "Alice")
	_ = n.SetProperty("tags", []string{"a", "b"})
	n.SetVersion(7)
	n.SetTemporal(&TemporalMetadata{ValidFrom: 1000, CreatedBy: "alice"})
	n.SetIntegrity(&NodeIntegrity{Hash: "abc", PrevHash: "def"})

	cp := n.DeepCopy()

	// All fields copied correctly.
	if cp.ID() != n.ID() {
		t.Errorf("ID: got %v, want %v", cp.ID(), n.ID())
	}
	if cp.PrimaryLabelToken() != n.PrimaryLabelToken() {
		t.Errorf("PrimaryLabel: got %v, want %v", cp.PrimaryLabelToken(), n.PrimaryLabelToken())
	}
	if cp.Version() != 7 {
		t.Errorf("Version: got %d, want 7", cp.Version())
	}
	extras := cp.ExtraLabelTokens()
	if len(extras) != 2 || extras[0] != labelToken(20) || extras[1] != labelToken(30) {
		t.Errorf("ExtraLabels: got %v, want [20 30]", extras)
	}
	if v, ok := cp.GetProperty("name"); !ok || v != "Alice" {
		t.Errorf("Property name: got (%v, %v), want (Alice, true)", v, ok)
	}
	if cp.Temporal().ValidFrom != 1000 || cp.Temporal().CreatedBy != "alice" {
		t.Error("Temporal fields not copied correctly")
	}
	if cp.Integrity().Hash != "abc" || cp.Integrity().PrevHash != "def" {
		t.Error("Integrity fields not copied correctly")
	}

	// Mutation independence: extraLabels.
	extras[0] = labelToken(999)
	if n.ExtraLabelTokens()[0] == labelToken(999) {
		t.Fatal("DeepCopy extraLabels: mutation affected original")
	}

	// Mutation independence: properties.
	_ = cp.SetProperty("name", "Bob")
	if v, _ := n.GetProperty("name"); v != "Alice" {
		t.Fatal("DeepCopy properties: mutation affected original")
	}

	// Mutation independence: slice property values.
	cpTags, _ := cp.GetProperty("tags")
	cpTags.([]string)[0] = "MUTATED"
	origTags, _ := n.GetProperty("tags")
	if origTags.([]string)[0] == "MUTATED" {
		t.Fatal("DeepCopy property slice value: mutation affected original")
	}

	// Mutation independence: temporal.
	cp.Temporal().ValidFrom = 9999
	if n.Temporal().ValidFrom != 1000 {
		t.Fatal("DeepCopy temporal: mutation affected original")
	}

	// Mutation independence: integrity.
	cp.Integrity().Hash = "MUTATED"
	if n.Integrity().Hash != "abc" {
		t.Fatal("DeepCopy integrity: mutation affected original")
	}
}

func TestNodeDeepCopyNilTemporalIntegrity(t *testing.T) {
	t.Parallel()

	n := NewNode(NodeID(snowflake.ID(1)), 10, nil)
	cp := n.DeepCopy()
	if cp.Temporal() != nil {
		t.Fatal("DeepCopy should preserve nil temporal")
	}
	if cp.Integrity() != nil {
		t.Fatal("DeepCopy should preserve nil integrity")
	}
}

func TestNodePropertyCount(t *testing.T) {
	t.Parallel()
	n := NewNode(NodeID(snowflake.ID(1)), 1, nil)

	if n.PropertyCount() != 0 {
		t.Fatalf("PropertyCount = %d, want 0", n.PropertyCount())
	}

	n.SetProperty("a", "1")
	n.SetProperty("b", "2")
	if n.PropertyCount() != 2 {
		t.Fatalf("PropertyCount = %d, want 2", n.PropertyCount())
	}

	n.DeleteProperty("a")
	if n.PropertyCount() != 1 {
		t.Fatalf("PropertyCount after delete = %d, want 1", n.PropertyCount())
	}
}
