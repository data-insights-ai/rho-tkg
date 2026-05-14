package core

import (
	"errors"
	"testing"
)

func TestBatchAddNodeTooManyLabels(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxLabelsPerNode: 1}})

	batch, _ := NewBatchBuilder(g)
	_, err := batch.AddNode([]string{"A", "B"}, nil)
	if err == nil {
		t.Fatal("expected error for too many labels in batch")
	}
	if !errors.Is(err, ErrTooManyLabels) {
		t.Fatalf("expected ErrTooManyLabels, got: %v", err)
	}
}

func TestBatchAddNodeDuplicateLabelsCountCanonicalForLimit(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxLabelsPerNode: 2}})

	batch, _ := NewBatchBuilder(g)
	n, err := batch.AddNode([]string{"A", "B", "A"}, nil)
	if err != nil {
		t.Fatalf("duplicate labels should count by canonical label set: %v", err)
	}
	if _, err := batch.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	labels := g.Nodes.Labels(n)
	if len(labels) != 2 || labels[0] != "A" || labels[1] != "B" {
		t.Fatalf("labels = %v, want [A B]", labels)
	}
}

func TestBatchAddNodeInvalidLabelPrecedesReservedPropertyValidation(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{})

	batch, _ := NewBatchBuilder(g)
	_, err := batch.AddNode([]string{" "}, map[string]any{"tkg_signature": "not bytes"})
	if !errors.Is(err, ErrEmptyName) {
		t.Fatalf("AddNode invalid label with invalid reserved props = %v, want ErrEmptyName", err)
	}
}

func TestBatchAddRelationshipTypeNameTooLong(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxNameLength: 3}})

	batch, _ := NewBatchBuilder(g)
	n, _ := batch.AddNode([]string{"X"}, nil)
	m, _ := batch.AddNode([]string{"X"}, nil)
	_, err := batch.AddRelationship("TOOLONG", n, m, nil)
	if err == nil {
		t.Fatal("expected error for type name too long in batch")
	}
	if !errors.Is(err, ErrNameTooLong) {
		t.Fatalf("expected ErrNameTooLong, got: %v", err)
	}
}

func TestBatchAddRelationshipInvalidTypePrecedesReservedPropertyValidation(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{})

	a, _ := g.Nodes.Add([]string{"X"}, nil)
	bn, _ := g.Nodes.Add([]string{"X"}, nil)
	batch, _ := NewBatchBuilder(g)
	_, err := batch.AddRelationship(" ", a, bn, map[string]any{"tkg_signature": "not bytes"})
	if !errors.Is(err, ErrEmptyName) {
		t.Fatalf("AddRelationship invalid type with invalid reserved props = %v, want ErrEmptyName", err)
	}
}

func TestBatchUpdateNodePropertyKeyTooLong(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxPropertyKeyLength: 3}})

	n, _ := g.Nodes.Add([]string{"X"}, nil)

	batch, _ := NewBatchBuilder(g)
	err := batch.UpdateNode(n.ID(), map[string]any{"toolong": "v"})
	if err == nil {
		t.Fatal("expected error for key too long in batch update")
	}
	if !errors.Is(err, ErrKeyTooLong) {
		t.Fatalf("expected ErrKeyTooLong, got: %v", err)
	}
}

func TestBatchUpdateRelPropertyValueTooLarge(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxPropertyValueSize: 3}})

	a, _ := g.Nodes.Add([]string{"X"}, nil)
	b, _ := g.Nodes.Add([]string{"X"}, nil)
	r, _ := g.Rels.Add("REL", a, b, nil)

	batch, _ := NewBatchBuilder(g)
	err := batch.UpdateRelationship(r.ID(), map[string]any{"k": "toolong"})
	if err == nil {
		t.Fatal("expected error for value too large in batch update")
	}
	if !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("expected ErrValueTooLarge, got: %v", err)
	}
}

func TestBatchBuilderRejectsNestedPropertyStringValueTooLarge(t *testing.T) {
	t.Parallel()

	oversized := map[string]any{"nested": []any{"toolong"}}
	tests := []struct {
		name string
		run  func(*Core) error
	}{
		{
			name: "add node",
			run: func(g *Core) error {
				batch, _ := NewBatchBuilder(g)
				_, err := batch.AddNode([]string{"X"}, map[string]any{"k": oversized})
				return err
			},
		},
		{
			name: "add relationship",
			run: func(g *Core) error {
				batch, _ := NewBatchBuilder(g)
				a, _ := batch.AddNode([]string{"X"}, nil)
				b, _ := batch.AddNode([]string{"X"}, nil)
				_, err := batch.AddRelationship("REL", a, b, map[string]any{"k": oversized})
				return err
			},
		},
		{
			name: "update node",
			run: func(g *Core) error {
				n, _ := g.Nodes.Add([]string{"X"}, nil)
				batch, _ := NewBatchBuilder(g)
				return batch.UpdateNode(n.ID(), map[string]any{"k": oversized})
			},
		},
		{
			name: "update relationship",
			run: func(g *Core) error {
				a, _ := g.Nodes.Add([]string{"X"}, nil)
				b, _ := g.Nodes.Add([]string{"X"}, nil)
				r, _ := g.Rels.Add("REL", a, b, nil)
				batch, _ := NewBatchBuilder(g)
				return batch.UpdateRelationship(r.ID(), map[string]any{"k": oversized})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			g, _ := New(Config{Validation: ValidationLimits{MaxPropertyValueSize: 3}})

			err := tc.run(g)
			if err == nil {
				t.Fatal("expected error for nested string value too large")
			}
			if !errors.Is(err, ErrValueTooLarge) {
				t.Fatalf("expected ErrValueTooLarge, got: %v", err)
			}
		})
	}
}

// --- MemoryStore NodeCountByLabel / RelCountByType ---
