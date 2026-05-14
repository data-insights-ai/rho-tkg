package core

import (
	"context"
	"errors"
	"testing"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func TestAddRelationshipTypeNameTooLong(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxNameLength: 5}})

	a, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	b, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	_, err := g.Rels.Add(context.Background(), "TOOLONG", a, b, nil)
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

	a, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	b, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	_, err := g.Rels.Add(context.Background(), "REL", a, b, map[string]any{"a": 1, "b": 2})
	if err == nil {
		t.Fatal("expected error for too many properties")
	}
	if !errors.Is(err, ErrTooManyProperties) {
		t.Fatalf("expected ErrTooManyProperties, got: %v", err)
	}
}

func TestAddRelationshipInvalidTypePrecedesEntityAndPropertyValidation(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxPropertiesPerEntity: 1}})
	missingStart := types.NewNode(types.NodeID(999), 1, nil)
	missingEnd := types.NewNode(types.NodeID(1000), 1, nil)
	props := map[string]any{"tkg_signature": "not bytes", "a": int64(1), "b": int64(2)}

	checks := []struct {
		name string
		run  func() error
	}{
		{name: "Add", run: func() error {
			_, err := g.Rels.Add(context.Background(), " ", missingStart, missingEnd, props)
			return err
		}},
		{name: "AddByID", run: func() error {
			_, err := g.Rels.AddByID(context.Background(), " ", missingStart.ID(), missingEnd.ID(), props)
			return err
		}},
		{name: "AddByIDIfAbsent", run: func() error {
			_, _, err := g.Rels.AddByIDIfAbsent(context.Background(), " ", missingStart.ID(), missingEnd.ID(), props)
			return err
		}},
		{name: "Import", run: func() error {
			_, err := g.Rels.Import(context.Background(), types.RelID(12345), " ", missingStart, missingEnd, props)
			return err
		}},
	}

	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			if err := check.run(); !errors.Is(err, ErrEmptyName) {
				t.Fatalf("err = %v, want ErrEmptyName", err)
			}
		})
	}
}

func TestAddRelationshipPropertyKeyTooLong(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxPropertyKeyLength: 3}})

	a, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	b, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	_, err := g.Rels.Add(context.Background(), "REL", a, b, map[string]any{"toolong": "v"})
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

	a, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	b, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	_, err := g.Rels.Add(context.Background(), "REL", a, b, map[string]any{"k": "toolong"})
	if err == nil {
		t.Fatal("expected error for value too large")
	}
	if !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("expected ErrValueTooLarge, got: %v", err)
	}
}

func TestRelationshipMutationsRejectNestedPropertyStringValueTooLarge(t *testing.T) {
	t.Parallel()

	oversized := map[string]any{"nested": []any{"toolong"}}
	tests := []struct {
		name string
		run  func(*Core, types.RelID) error
	}{
		{
			name: "add",
			run: func(g *Core, _ types.RelID) error {
				a, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
				b, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
				_, err := g.Rels.Add(context.Background(), "REL", a, b, map[string]any{"k": oversized})
				return err
			},
		},
		{
			name: "update",
			run: func(g *Core, id types.RelID) error {
				_, err := g.Rels.Update(context.Background(), id, map[string]any{"k": oversized})
				return err
			},
		},
		{
			name: "update in place",
			run: func(g *Core, id types.RelID) error {
				_, err := g.Rels.UpdateInPlace(context.Background(), id, map[string]any{"k": oversized})
				return err
			},
		},
		{
			name: "set property",
			run: func(g *Core, id types.RelID) error {
				return g.Rels.SetProperty(context.Background(), id, "k", oversized)
			},
		},
		{
			name: "compare and set property",
			run: func(g *Core, id types.RelID) error {
				_, err := g.Rels.CompareAndSetProperty(context.Background(), id, "k", nil, oversized)
				return err
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			g, _ := New(Config{Validation: ValidationLimits{MaxPropertyValueSize: 3}})
			a, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
			b, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
			r, _ := g.Rels.Add(context.Background(), "REL", a, b, nil)

			err := tc.run(g, r.ID())
			if err == nil {
				t.Fatal("expected error for nested string value too large")
			}
			if !errors.Is(err, ErrValueTooLarge) {
				t.Fatalf("expected ErrValueTooLarge, got: %v", err)
			}
		})
	}
}

func TestUpdateRelPropertyCountExceedsLimit(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxPropertiesPerEntity: 1}})

	a, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	b, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	r, _ := g.Rels.Add(context.Background(), "REL", a, b, map[string]any{"a": 1})

	_, err := g.Rels.Update(context.Background(), r.ID(), map[string]any{"b": 2})
	if err == nil {
		t.Fatal("expected error for exceeding property limit on rel update")
	}
	if !errors.Is(err, ErrTooManyProperties) {
		t.Fatalf("expected ErrTooManyProperties, got: %v", err)
	}
}
