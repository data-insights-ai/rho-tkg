package core

import (
	"errors"
	"testing"
)

func TestAddNodeTooManyLabels(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxLabelsPerNode: 2}})

	_, err := g.Nodes.Add([]string{"A", "B", "C"}, nil)
	if err == nil {
		t.Fatal("expected error for too many labels")
	}
	if !errors.Is(err, ErrTooManyLabels) {
		t.Fatalf("expected ErrTooManyLabels, got: %v", err)
	}
}

func TestAddNodeMaxLabels(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxLabelsPerNode: 2}})

	n, err := g.Nodes.Add([]string{"A", "B"}, nil)
	if err != nil {
		t.Fatalf("at-limit should succeed: %v", err)
	}
	if n == nil {
		t.Fatal("node should not be nil")
	}
}

func TestAddNodeTooManyProperties(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxPropertiesPerEntity: 2}})

	props := map[string]any{"a": 1, "b": 2, "c": 3}
	_, err := g.Nodes.Add([]string{"X"}, props)
	if err == nil {
		t.Fatal("expected error for too many properties")
	}
	if !errors.Is(err, ErrTooManyProperties) {
		t.Fatalf("expected ErrTooManyProperties, got: %v", err)
	}
}

func TestAddNodeMaxProperties(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxPropertiesPerEntity: 2}})

	props := map[string]any{"a": 1, "b": 2}
	n, err := g.Nodes.Add([]string{"X"}, props)
	if err != nil {
		t.Fatalf("at-limit should succeed: %v", err)
	}
	if n == nil {
		t.Fatal("node should not be nil")
	}
}

func TestAddNodePropertyKeyTooLong(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxPropertyKeyLength: 5}})

	props := map[string]any{"toolong": "val"}
	_, err := g.Nodes.Add([]string{"X"}, props)
	if err == nil {
		t.Fatal("expected error for key too long")
	}
	if !errors.Is(err, ErrKeyTooLong) {
		t.Fatalf("expected ErrKeyTooLong, got: %v", err)
	}
}

func TestAddNodePropertyKeyMaxLength(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxPropertyKeyLength: 5}})

	props := map[string]any{"abcde": "val"}
	n, err := g.Nodes.Add([]string{"X"}, props)
	if err != nil {
		t.Fatalf("at-limit key should succeed: %v", err)
	}
	if n == nil {
		t.Fatal("node should not be nil")
	}
}

func TestAddNodePropertyValueTooLarge(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxPropertyValueSize: 10}})

	props := map[string]any{"key": "12345678901"} // 11 bytes
	_, err := g.Nodes.Add([]string{"X"}, props)
	if err == nil {
		t.Fatal("expected error for value too large")
	}
	if !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("expected ErrValueTooLarge, got: %v", err)
	}
}

func TestAddNodePropertyValueMaxSize(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxPropertyValueSize: 10}})

	props := map[string]any{"key": "1234567890"} // exactly 10 bytes
	n, err := g.Nodes.Add([]string{"X"}, props)
	if err != nil {
		t.Fatalf("at-limit value should succeed: %v", err)
	}
	if n == nil {
		t.Fatal("node should not be nil")
	}
}

func TestAddNodePropertyValueNonStringIgnored(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxPropertyValueSize: 1}})

	// Non-string values should not be checked against MaxPropertyValueSize.
	props := map[string]any{"key": 99999}
	n, err := g.Nodes.Add([]string{"X"}, props)
	if err != nil {
		t.Fatalf("non-string value should be ignored: %v", err)
	}
	if n == nil {
		t.Fatal("node should not be nil")
	}
}

func TestAddNodeLabelNameTooLong(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxNameLength: 5}})

	_, err := g.Nodes.Add([]string{"TooLong"}, nil)
	if err == nil {
		t.Fatal("expected error for label name too long")
	}
	if !errors.Is(err, ErrNameTooLong) {
		t.Fatalf("expected ErrNameTooLong, got: %v", err)
	}
}

func TestAddNodeLabelNameMaxLength(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxNameLength: 5}})

	n, err := g.Nodes.Add([]string{"ABCDE"}, nil)
	if err != nil {
		t.Fatalf("at-limit name should succeed: %v", err)
	}
	if n == nil {
		t.Fatal("node should not be nil")
	}
}

func TestUpdateNodePropertyCountExceedsLimit(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxPropertiesPerEntity: 2}})

	n, _ := g.Nodes.Add([]string{"X"}, map[string]any{"a": 1, "b": 2})

	// Try to add a 3rd property via update — should fail.
	_, err := g.Nodes.Update(n.ID(), map[string]any{"c": 3})
	if err == nil {
		t.Fatal("expected error for exceeding property limit on update")
	}
	if !errors.Is(err, ErrTooManyProperties) {
		t.Fatalf("expected ErrTooManyProperties, got: %v", err)
	}
}

func TestUpdateNodePropertyKeyTooLong(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxPropertyKeyLength: 3}})

	n, _ := g.Nodes.Add([]string{"X"}, nil)

	_, err := g.Nodes.Update(n.ID(), map[string]any{"toolong": "v"})
	if err == nil {
		t.Fatal("expected error for key too long on update")
	}
	if !errors.Is(err, ErrKeyTooLong) {
		t.Fatalf("expected ErrKeyTooLong, got: %v", err)
	}
}

func TestUpdateNodePropertyValueTooLarge(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxPropertyValueSize: 3}})

	n, _ := g.Nodes.Add([]string{"X"}, nil)

	_, err := g.Nodes.Update(n.ID(), map[string]any{"k": "toolong"})
	if err == nil {
		t.Fatal("expected error for value too large on update")
	}
	if !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("expected ErrValueTooLarge, got: %v", err)
	}
}

func TestUpdateNodeReplacementWithinLimit(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxPropertiesPerEntity: 2}})

	n, _ := g.Nodes.Add([]string{"X"}, map[string]any{"a": 1, "b": 2})

	// Replacing an existing property should not trip the limit.
	updated, err := g.Nodes.Update(n.ID(), map[string]any{"a": 99})
	if err != nil {
		t.Fatalf("replacement should succeed: %v", err)
	}
	v, _ := updated.GetProperty("a")
	if v != 99 {
		t.Fatalf("expected 99, got %v", v)
	}
}

func TestUpdateNodeDeleteThenAddWithinLimit(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxPropertiesPerEntity: 2}})

	n, _ := g.Nodes.Add([]string{"X"}, map[string]any{"a": 1, "b": 2})

	// Delete one and add one — should still be at limit.
	updated, err := g.Nodes.Update(n.ID(), map[string]any{"a": nil, "c": 3})
	if err != nil {
		t.Fatalf("delete+add should succeed: %v", err)
	}
	if updated.PropertyCount() != 2 {
		t.Fatalf("PropertyCount = %d, want 2", updated.PropertyCount())
	}
}
