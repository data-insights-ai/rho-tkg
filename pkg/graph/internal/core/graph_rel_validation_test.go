package core

import (
	"errors"
	"testing"
)

func TestAddRelationshipTypeNameTooLong(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxNameLength: 5}})

	a, _ := g.Nodes.Add([]string{"X"}, nil)
	b, _ := g.Nodes.Add([]string{"X"}, nil)
	_, err := g.Rels.Add("TOOLONG", a, b, nil)
	if err == nil {
		t.Fatal("expected error for type name too long")
	}
	if !errors.Is(err, ErrNameTooLong) {
		t.Fatalf("expected ErrNameTooLong, got: %v", err)
	}
}

func TestAddRelationshipTooManyProperties(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxPropertiesPerEntity: 1}})

	a, _ := g.Nodes.Add([]string{"X"}, nil)
	b, _ := g.Nodes.Add([]string{"X"}, nil)
	_, err := g.Rels.Add("REL", a, b, map[string]any{"a": 1, "b": 2})
	if err == nil {
		t.Fatal("expected error for too many properties")
	}
	if !errors.Is(err, ErrTooManyProperties) {
		t.Fatalf("expected ErrTooManyProperties, got: %v", err)
	}
}

func TestAddRelationshipPropertyKeyTooLong(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxPropertyKeyLength: 3}})

	a, _ := g.Nodes.Add([]string{"X"}, nil)
	b, _ := g.Nodes.Add([]string{"X"}, nil)
	_, err := g.Rels.Add("REL", a, b, map[string]any{"toolong": "v"})
	if err == nil {
		t.Fatal("expected error for key too long")
	}
	if !errors.Is(err, ErrKeyTooLong) {
		t.Fatalf("expected ErrKeyTooLong, got: %v", err)
	}
}

func TestAddRelationshipPropertyValueTooLarge(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxPropertyValueSize: 3}})

	a, _ := g.Nodes.Add([]string{"X"}, nil)
	b, _ := g.Nodes.Add([]string{"X"}, nil)
	_, err := g.Rels.Add("REL", a, b, map[string]any{"k": "toolong"})
	if err == nil {
		t.Fatal("expected error for value too large")
	}
	if !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("expected ErrValueTooLarge, got: %v", err)
	}
}

func TestUpdateRelPropertyCountExceedsLimit(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxPropertiesPerEntity: 1}})

	a, _ := g.Nodes.Add([]string{"X"}, nil)
	b, _ := g.Nodes.Add([]string{"X"}, nil)
	r, _ := g.Rels.Add("REL", a, b, map[string]any{"a": 1})

	_, err := g.Rels.Update(r.ID(), map[string]any{"b": 2})
	if err == nil {
		t.Fatal("expected error for exceeding property limit on rel update")
	}
	if !errors.Is(err, ErrTooManyProperties) {
		t.Fatalf("expected ErrTooManyProperties, got: %v", err)
	}
}
