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

// --- MemoryStore NodeCountByLabel / RelCountByType ---
