package core

import (
	"context"
	"errors"
	"strings"
	"testing"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
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

func TestAddNodeDuplicateLabelsCountCanonicalForLimit(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxLabelsPerNode: 2}})

	n, err := g.Nodes.Add([]string{"A", "B", "A"}, nil)
	if err != nil {
		t.Fatalf("duplicate labels should count by canonical label set: %v", err)
	}

	labels := g.Nodes.Labels(n)
	if len(labels) != 2 || labels[0] != "A" || labels[1] != "B" {
		t.Fatalf("labels = %v, want [A B]", labels)
	}
}

func TestImportNodeDuplicateLabelsCountCanonicalForLimit(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxLabelsPerNode: 2}})

	n, err := g.Nodes.Import(context.Background(), types.NodeID(12345), []string{"A", "B", "A"}, nil)
	if err != nil {
		t.Fatalf("duplicate import labels should count by canonical label set: %v", err)
	}

	labels := g.Nodes.Labels(n)
	if len(labels) != 2 || labels[0] != "A" || labels[1] != "B" {
		t.Fatalf("labels = %v, want [A B]", labels)
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

func TestAddNodeInvalidLabelPrecedesPropertyValidation(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxPropertiesPerEntity: 1}})

	_, err := g.Nodes.Add([]string{" "}, map[string]any{"a": int64(1), "b": int64(2)})
	if !errors.Is(err, ErrEmptyName) {
		t.Fatalf("Add invalid label with invalid props = %v, want ErrEmptyName", err)
	}

	_, err = g.Nodes.Add([]string{strings.Repeat("x", defaultMaxNameLength+1)}, map[string]any{"a": int64(1), "b": int64(2)})
	if !errors.Is(err, ErrNameTooLong) {
		t.Fatalf("Add overlong label with invalid props = %v, want ErrNameTooLong", err)
	}

	_, err = g.Nodes.Add([]string{" "}, map[string]any{"tkg_signature": "not bytes"})
	if !errors.Is(err, ErrEmptyName) {
		t.Fatalf("Add invalid label with invalid reserved props = %v, want ErrEmptyName", err)
	}

	_, err = g.Nodes.Import(context.Background(), types.NodeID(12345), []string{" "}, map[string]any{"tkg_signature": "not bytes"})
	if !errors.Is(err, ErrEmptyName) {
		t.Fatalf("Import invalid label with invalid reserved props = %v, want ErrEmptyName", err)
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

func TestAddNodeNestedPropertyStringValueTooLarge(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		props map[string]any
	}{
		{
			name:  "slice string",
			props: map[string]any{"key": []string{"toolong"}},
		},
		{
			name:  "any slice string",
			props: map[string]any{"key": []any{map[string]any{"nested": "toolong"}}},
		},
		{
			name:  "string map value",
			props: map[string]any{"key": map[string]string{"ok": "toolong"}},
		},
		{
			name:  "nested map key",
			props: map[string]any{"key": map[string]any{"toolong": true}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			g, _ := New(Config{Validation: ValidationLimits{MaxPropertyValueSize: 3}})

			_, err := g.Nodes.Add([]string{"X"}, tc.props)
			if err == nil {
				t.Fatal("expected error for nested string value too large")
			}
			if !errors.Is(err, ErrValueTooLarge) {
				t.Fatalf("expected ErrValueTooLarge, got: %v", err)
			}
		})
	}
}

func TestAddNodeNestedPropertyStringValueMaxSize(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxPropertyValueSize: 3}})

	props := map[string]any{
		"key": []any{
			"abc",
			map[string]string{"def": "ghi"},
		},
	}
	n, err := g.Nodes.Add([]string{"X"}, props)
	if err != nil {
		t.Fatalf("nested at-limit strings should succeed: %v", err)
	}
	if n == nil {
		t.Fatal("node should not be nil")
	}
}

func TestAddNodePropertyValueNonStringIgnored(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxPropertyValueSize: 1}})

	// Numeric values should not be checked against the string size limit.
	props := map[string]any{"key": 99999}
	n, err := g.Nodes.Add([]string{"X"}, props)
	if err != nil {
		t.Fatalf("non-string value should be ignored: %v", err)
	}
	if n == nil {
		t.Fatal("node should not be nil")
	}
}

func TestAddNodePropertyValueNonStringContainersIgnored(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		val  any
	}{
		{name: "int slice", val: []int{1, 2, 3}},
		{name: "int64 slice", val: []int64{1, 2, 3}},
		{name: "float32 slice", val: []float32{1, 2, 3}},
		{name: "float64 slice", val: []float64{1, 2, 3}},
		{name: "byte slice", val: []byte("longer than limit")},
		{name: "bool slice", val: []bool{true, false}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			g, _ := New(Config{Validation: ValidationLimits{MaxPropertyValueSize: 1}})

			n, err := g.Nodes.Add([]string{"X"}, map[string]any{"key": tc.val})
			if err != nil {
				t.Fatalf("non-string container should be ignored: %v", err)
			}
			if n == nil {
				t.Fatal("node should not be nil")
			}
		})
	}
}

func TestAddNodeRejectsPropertyTypesOutsideHashWireAllowlist(t *testing.T) {
	t.Parallel()

	type namedString string
	type namedSlice []string

	tests := []struct {
		name string
		val  any
		want error
	}{
		{name: "named scalar", val: namedString("x"), want: types.ErrUnsupportedValueType},
		{name: "unsupported slice", val: []uint{1, 2}, want: types.ErrUnsupportedValueType},
		{name: "named slice", val: namedSlice{"x"}, want: types.ErrUnsupportedValueType},
		{name: "unsupported map", val: map[string]int{"x": 1}, want: types.ErrUnsupportedMapType},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			defer func() {
				if rec := recover(); rec != nil {
					t.Fatalf("AddNode should reject %T with an error, not panic: %v", tc.val, rec)
				}
			}()

			g, _ := New(Config{})
			_, err := g.Nodes.Add([]string{"X"}, map[string]any{"k": tc.val})
			if err == nil {
				t.Fatal("expected validation error")
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("expected %v, got: %v", tc.want, err)
			}
		})
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

func TestNodeMutationsRejectNestedPropertyStringValueTooLarge(t *testing.T) {
	t.Parallel()

	oversized := map[string]any{"nested": []any{"toolong"}}
	tests := []struct {
		name string
		run  func(*Core, types.NodeID) error
	}{
		{
			name: "update",
			run: func(g *Core, id types.NodeID) error {
				_, err := g.Nodes.Update(id, map[string]any{"k": oversized})
				return err
			},
		},
		{
			name: "update in place",
			run: func(g *Core, id types.NodeID) error {
				_, err := g.Nodes.UpdateInPlace(id, map[string]any{"k": oversized})
				return err
			},
		},
		{
			name: "set property",
			run: func(g *Core, id types.NodeID) error {
				return g.Nodes.SetProperty(id, "k", oversized)
			},
		},
		{
			name: "compare and set",
			run: func(g *Core, id types.NodeID) error {
				_, err := g.Nodes.CompareAndSetProperty(id, "k", nil, oversized)
				return err
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			g, _ := New(Config{Validation: ValidationLimits{MaxPropertyValueSize: 3}})
			n, _ := g.Nodes.Add([]string{"X"}, nil)

			err := tc.run(g, n.ID())
			if err == nil {
				t.Fatal("expected error for nested string value too large")
			}
			if !errors.Is(err, ErrValueTooLarge) {
				t.Fatalf("expected ErrValueTooLarge, got: %v", err)
			}
		})
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
