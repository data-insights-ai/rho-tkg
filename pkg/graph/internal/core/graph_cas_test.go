package core

import (
	"errors"
	"testing"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/memory"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func TestCAS_Match(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"status": "draft"})
	id := n.ID()

	ok, err := g.Nodes.CompareAndSetProperty(id, "status", "draft", "published")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("CAS should return true on match")
	}

	got, _ := g.Nodes.Get(id)
	v, _ := got.GetProperty("status")
	if v != "published" {
		t.Fatalf("status = %v, want published", v)
	}
}

func TestCAS_Mismatch(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"status": "draft"})
	id := n.ID()

	ok, err := g.Nodes.CompareAndSetProperty(id, "status", "archived", "published")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("CAS should return false on mismatch")
	}

	got, _ := g.Nodes.Get(id)
	v, _ := got.GetProperty("status")
	if v != "draft" {
		t.Fatalf("status = %v, want draft (unchanged)", v)
	}
}

func TestCAS_NilExpected_Absent(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add([]string{"Person"}, nil)
	id := n.ID()

	ok, err := g.Nodes.CompareAndSetProperty(id, "status", nil, "active")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("CAS should return true when expected=nil and property absent")
	}

	got, _ := g.Nodes.Get(id)
	v, found := got.GetProperty("status")
	if !found || v != "active" {
		t.Fatalf("status = (%v, %v), want (active, true)", v, found)
	}
}

func TestCAS_AddPropertyRejectsFinalPropertyCountOverLimit(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{
		Store:      memory.New(),
		Validation: ValidationLimits{MaxPropertiesPerEntity: 1},
	})
	n, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"a": 1})
	id := n.ID()

	ok, err := g.Nodes.CompareAndSetProperty(id, "b", nil, 2)
	if err == nil {
		t.Fatal("expected property count limit error")
	}
	if ok {
		t.Fatal("CAS should not report success when final property count exceeds limit")
	}
	if !errors.Is(err, ErrTooManyProperties) {
		t.Fatalf("expected ErrTooManyProperties, got: %v", err)
	}

	got, getErr := g.Nodes.Get(id)
	if getErr != nil {
		t.Fatal(getErr)
	}
	if got.PropertyCount() != 1 {
		t.Fatalf("property count = %d, want 1", got.PropertyCount())
	}
	if _, found := got.GetProperty("b"); found {
		t.Fatal("overflow property should not be persisted")
	}
}

func TestCAS_NilExpected_Present(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"status": "draft"})
	id := n.ID()

	ok, err := g.Nodes.CompareAndSetProperty(id, "status", nil, "active")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("CAS should return false when expected=nil but property exists")
	}
}

func TestCAS_DeleteOnMatch(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"status": "draft"})
	id := n.ID()

	ok, err := g.Nodes.CompareAndSetProperty(id, "status", "draft", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("CAS should return true on match+delete")
	}

	got, _ := g.Nodes.Get(id)
	if _, found := got.GetProperty("status"); found {
		t.Fatal("property should be deleted")
	}
}

func TestCAS_NilBoth_Absent(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add([]string{"Person"}, nil)
	id := n.ID()

	ok, err := g.Nodes.CompareAndSetProperty(id, "status", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("CAS should return true for no-op (nil/nil, absent)")
	}
}

func TestCAS_ShadowKey(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add([]string{"Person"}, nil)
	id := n.ID()

	_, err := g.Nodes.CompareAndSetProperty(id, "tkg_labels", nil, "hack")
	if err == nil {
		t.Fatal("CAS should reject tkg_ prefix")
	}
	if !errors.Is(err, types.ErrReservedPrefix) {
		t.Errorf("errors.Is(err, ErrReservedPrefix) = false; err = %v", err)
	}
}

func TestCAS_NodeNotFound(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Store: memory.New()})

	_, err := g.Nodes.CompareAndSetProperty(types.NodeID(999999), "status", nil, "x")
	if err == nil {
		t.Fatal("CAS should return error for non-existent node")
	}
	if !errors.Is(err, storepkg.ErrNodeNotFound) {
		t.Errorf("errors.Is(err, storepkg.ErrNodeNotFound) = false; err = %v", err)
	}
}

func TestCAS_VersionBump(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"v": 1})
	id := n.ID()
	before, _ := g.Nodes.Get(id)
	vBefore := before.Version()

	ok, _ := g.Nodes.CompareAndSetProperty(id, "v", 1, 2)
	if !ok {
		t.Fatal("CAS should succeed")
	}

	after, _ := g.Nodes.Get(id)
	if after.Version() != vBefore+1 {
		t.Fatalf("version = %d, want %d", after.Version(), vBefore+1)
	}
}

func TestCAS_NoVersionBumpOnMismatch(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"v": 1})
	id := n.ID()
	before, _ := g.Nodes.Get(id)
	vBefore := before.Version()

	ok, _ := g.Nodes.CompareAndSetProperty(id, "v", 999, 2)
	if ok {
		t.Fatal("CAS should fail on mismatch")
	}

	after, _ := g.Nodes.Get(id)
	if after.Version() != vBefore {
		t.Fatalf("version = %d, want %d (unchanged)", after.Version(), vBefore)
	}
}

func TestCAS_History(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"v": 1})
	id := n.ID()

	ok, _ := g.Nodes.CompareAndSetProperty(id, "v", 1, 2)
	if !ok {
		t.Fatal("CAS should succeed")
	}

	hist, err := g.Nodes.History(id)
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 1 {
		t.Fatalf("history len = %d, want 1", len(hist))
	}
	// History entry should have old value.
	hv, _ := hist[0].GetProperty("v")
	if hv != 1 {
		t.Fatalf("history v = %v, want 1", hv)
	}
}

func TestCAS_TypeMismatch(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"v": int(42)})
	id := n.ID()

	// int64(42) != int(42) — type must match exactly.
	ok, err := g.Nodes.CompareAndSetProperty(id, "v", int64(42), int(99))
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("CAS should fail: int64(42) != int(42)")
	}

	got, _ := g.Nodes.Get(id)
	v, _ := got.GetProperty("v")
	if v != int(42) {
		t.Fatalf("v = %v, want 42 (unchanged)", v)
	}
}

func TestCAS_DeleteMismatch(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"status": "draft"})
	id := n.ID()

	// Try to delete with wrong expected value.
	ok, err := g.Nodes.CompareAndSetProperty(id, "status", "archived", nil)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("CAS delete should fail on mismatch")
	}

	got, _ := g.Nodes.Get(id)
	v, found := got.GetProperty("status")
	if !found || v != "draft" {
		t.Fatalf("status = (%v, %v), want (draft, true) — unchanged", v, found)
	}
}

// ─── OutgoingRelationshipsForNodes ───────────────────────────────────────────
