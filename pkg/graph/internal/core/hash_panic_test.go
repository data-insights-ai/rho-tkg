package core

import (
	"context"
	"errors"
	"testing"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
)

type panicHashProperty struct {
	V int
}

func (p panicHashProperty) HashBytes() []byte {
	panic("hash bytes panic")
}

func (p panicHashProperty) DeepCopyValue() any {
	return p
}

func registerPanicHashProperty(t *testing.T) {
	t.Helper()
	if err := types.RegisterPropertyStructType(panicHashProperty{}); err != nil {
		t.Fatalf("RegisterPropertyStructType: %v", err)
	}
}

func runWithoutPanic(t *testing.T, fn func() error) (err error) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("operation panicked: %v", r)
		}
	}()
	return fn()
}

func requireUnsupportedHashError(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, types.ErrUnsupportedValueType) {
		t.Fatalf("error = %v, want errors.Is ErrUnsupportedValueType", err)
	}
}

func TestHashPanicProperty_CreatePathsReturnError(t *testing.T) {
	registerPanicHashProperty(t)

	t.Run("node add", func(t *testing.T) {
		g := newTestGraph(t)
		err := runWithoutPanic(t, func() error {
			_, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"bad": panicHashProperty{V: 1}})
			return err
		})
		requireUnsupportedHashError(t, err)
		if count, countErr := g.Nodes.Count(); countErr != nil || count != 0 {
			t.Fatalf("node count = %d, err = %v, want 0 nil", count, countErr)
		}
	})

	t.Run("node import", func(t *testing.T) {
		g := newTestGraph(t)
		err := runWithoutPanic(t, func() error {
			_, err := g.Nodes.Import(context.Background(), types.NodeID(1001), []string{"Person"}, map[string]any{"bad": panicHashProperty{V: 1}})
			return err
		})
		requireUnsupportedHashError(t, err)
		if count, countErr := g.Nodes.Count(); countErr != nil || count != 0 {
			t.Fatalf("node count = %d, err = %v, want 0 nil", count, countErr)
		}
	})

	t.Run("batch node add", func(t *testing.T) {
		g := newTestGraph(t)
		b, err := NewBatchBuilder(g)
		if err != nil {
			t.Fatalf("NewBatchBuilder: %v", err)
		}
		err = runWithoutPanic(t, func() error {
			_, err := b.AddNode([]string{"Person"}, map[string]any{"bad": panicHashProperty{V: 1}})
			return err
		})
		requireUnsupportedHashError(t, err)
	})

	t.Run("relationship add", func(t *testing.T) {
		g := newTestGraph(t)
		a, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
		if err != nil {
			t.Fatalf("add start node: %v", err)
		}
		b, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
		if err != nil {
			t.Fatalf("add end node: %v", err)
		}
		err = runWithoutPanic(t, func() error {
			_, err := g.Rels.Add(context.Background(), "KNOWS", a, b, map[string]any{"bad": panicHashProperty{V: 1}})
			return err
		})
		requireUnsupportedHashError(t, err)
		if count, countErr := g.Rels.Count(); countErr != nil || count != 0 {
			t.Fatalf("relationship count = %d, err = %v, want 0 nil", count, countErr)
		}
	})

	t.Run("relationship add by id", func(t *testing.T) {
		g := newTestGraph(t)
		a, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
		if err != nil {
			t.Fatalf("add start node: %v", err)
		}
		b, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
		if err != nil {
			t.Fatalf("add end node: %v", err)
		}
		err = runWithoutPanic(t, func() error {
			_, err := g.Rels.AddByID(context.Background(), "KNOWS", a.ID(), b.ID(), map[string]any{"bad": panicHashProperty{V: 1}})
			return err
		})
		requireUnsupportedHashError(t, err)
		if count, countErr := g.Rels.Count(); countErr != nil || count != 0 {
			t.Fatalf("relationship count = %d, err = %v, want 0 nil", count, countErr)
		}
	})

	t.Run("relationship add by id if absent", func(t *testing.T) {
		g := newTestGraph(t)
		a, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
		if err != nil {
			t.Fatalf("add start node: %v", err)
		}
		b, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
		if err != nil {
			t.Fatalf("add end node: %v", err)
		}
		err = runWithoutPanic(t, func() error {
			_, _, err := g.Rels.AddByIDIfAbsent(context.Background(), "KNOWS", a.ID(), b.ID(), map[string]any{"bad": panicHashProperty{V: 1}})
			return err
		})
		requireUnsupportedHashError(t, err)
		if count, countErr := g.Rels.Count(); countErr != nil || count != 0 {
			t.Fatalf("relationship count = %d, err = %v, want 0 nil", count, countErr)
		}
	})

	t.Run("relationship import", func(t *testing.T) {
		g := newTestGraph(t)
		a, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
		if err != nil {
			t.Fatalf("add start node: %v", err)
		}
		b, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
		if err != nil {
			t.Fatalf("add end node: %v", err)
		}
		err = runWithoutPanic(t, func() error {
			_, err := g.Rels.Import(context.Background(), types.RelID(2001), "KNOWS", a, b, map[string]any{"bad": panicHashProperty{V: 1}})
			return err
		})
		requireUnsupportedHashError(t, err)
		if count, countErr := g.Rels.Count(); countErr != nil || count != 0 {
			t.Fatalf("relationship count = %d, err = %v, want 0 nil", count, countErr)
		}
	})

	t.Run("batch relationship add", func(t *testing.T) {
		g := newTestGraph(t)
		a, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
		if err != nil {
			t.Fatalf("add start node: %v", err)
		}
		bNode, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
		if err != nil {
			t.Fatalf("add end node: %v", err)
		}
		builder, err := NewBatchBuilder(g)
		if err != nil {
			t.Fatalf("NewBatchBuilder: %v", err)
		}
		err = runWithoutPanic(t, func() error {
			_, err := builder.AddRelationship("KNOWS", a, bNode, map[string]any{"bad": panicHashProperty{V: 1}})
			return err
		})
		requireUnsupportedHashError(t, err)
	})
}

func TestHashPanicProperty_UpdateAndVerifyPathsReturnError(t *testing.T) {
	registerPanicHashProperty(t)

	t.Run("node update", func(t *testing.T) {
		g := newTestGraph(t)
		n, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
		if err != nil {
			t.Fatalf("add node: %v", err)
		}
		err = runWithoutPanic(t, func() error {
			_, err := g.Nodes.Update(context.Background(), n.ID(), map[string]any{"bad": panicHashProperty{V: 1}})
			return err
		})
		requireUnsupportedHashError(t, err)
		stored, err := g.Nodes.Get(context.Background(), n.ID())
		if err != nil {
			t.Fatalf("get node: %v", err)
		}
		if _, ok := stored.GetProperty("bad"); ok {
			t.Fatal("failed node update wrote bad property")
		}
	})

	t.Run("node update in place", func(t *testing.T) {
		g := newTestGraph(t)
		n, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
		if err != nil {
			t.Fatalf("add node: %v", err)
		}
		err = runWithoutPanic(t, func() error {
			_, err := g.Nodes.UpdateInPlace(context.Background(), n.ID(), map[string]any{"bad": panicHashProperty{V: 1}})
			return err
		})
		requireUnsupportedHashError(t, err)
		stored, err := g.Nodes.Get(context.Background(), n.ID())
		if err != nil {
			t.Fatalf("get node: %v", err)
		}
		if _, ok := stored.GetProperty("bad"); ok {
			t.Fatal("failed in-place node update wrote bad property")
		}
	})

	t.Run("node compare and set", func(t *testing.T) {
		g := newTestGraph(t)
		n, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"state": "old"})
		if err != nil {
			t.Fatalf("add node: %v", err)
		}
		err = runWithoutPanic(t, func() error {
			_, err := g.Nodes.CompareAndSetProperty(context.Background(), n.ID(), "state", "old", panicHashProperty{V: 1})
			return err
		})
		requireUnsupportedHashError(t, err)
		stored, err := g.Nodes.Get(context.Background(), n.ID())
		if err != nil {
			t.Fatalf("get node: %v", err)
		}
		if got, _ := stored.GetProperty("state"); got != "old" {
			t.Fatalf("state = %v, want old", got)
		}
	})

	t.Run("relationship update", func(t *testing.T) {
		g := newTestGraph(t)
		a, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
		if err != nil {
			t.Fatalf("add start node: %v", err)
		}
		b, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
		if err != nil {
			t.Fatalf("add end node: %v", err)
		}
		r, err := g.Rels.Add(context.Background(), "KNOWS", a, b, map[string]any{"since": int64(1)})
		if err != nil {
			t.Fatalf("add relationship: %v", err)
		}
		err = runWithoutPanic(t, func() error {
			_, err := g.Rels.Update(context.Background(), r.ID(), map[string]any{"bad": panicHashProperty{V: 1}})
			return err
		})
		requireUnsupportedHashError(t, err)
		stored, err := g.Rels.Get(context.Background(), r.ID())
		if err != nil {
			t.Fatalf("get relationship: %v", err)
		}
		if _, ok := stored.GetProperty("bad"); ok {
			t.Fatal("failed relationship update wrote bad property")
		}
	})

	t.Run("relationship update in place", func(t *testing.T) {
		g := newTestGraph(t)
		a, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
		if err != nil {
			t.Fatalf("add start node: %v", err)
		}
		b, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
		if err != nil {
			t.Fatalf("add end node: %v", err)
		}
		r, err := g.Rels.Add(context.Background(), "KNOWS", a, b, map[string]any{"since": int64(1)})
		if err != nil {
			t.Fatalf("add relationship: %v", err)
		}
		err = runWithoutPanic(t, func() error {
			_, err := g.Rels.UpdateInPlace(context.Background(), r.ID(), map[string]any{"bad": panicHashProperty{V: 1}})
			return err
		})
		requireUnsupportedHashError(t, err)
		stored, err := g.Rels.Get(context.Background(), r.ID())
		if err != nil {
			t.Fatalf("get relationship: %v", err)
		}
		if _, ok := stored.GetProperty("bad"); ok {
			t.Fatal("failed in-place relationship update wrote bad property")
		}
	})

	t.Run("hash verify node", func(t *testing.T) {
		g := newTestGraph(t)
		n, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
		if err != nil {
			t.Fatalf("add node: %v", err)
		}
		if err := n.SetProperty("bad", panicHashProperty{V: 1}); err != nil {
			t.Fatalf("set direct bad property: %v", err)
		}
		if err := g.store.ReplaceNode(n); err != nil {
			t.Fatalf("replace node directly: %v", err)
		}
		err = runWithoutPanic(t, func() error {
			_, err := g.Hash.VerifyNodeChain(n.ID())
			return err
		})
		requireUnsupportedHashError(t, err)
	})

	t.Run("hash verify relationship", func(t *testing.T) {
		g := newTestGraph(t)
		a, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
		if err != nil {
			t.Fatalf("add start node: %v", err)
		}
		b, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
		if err != nil {
			t.Fatalf("add end node: %v", err)
		}
		r, err := g.Rels.Add(context.Background(), "KNOWS", a, b, nil)
		if err != nil {
			t.Fatalf("add relationship: %v", err)
		}
		if err := r.SetProperty("bad", panicHashProperty{V: 1}); err != nil {
			t.Fatalf("set direct bad property: %v", err)
		}
		if err := g.store.ReplaceRelationship(r); err != nil {
			t.Fatalf("replace relationship directly: %v", err)
		}
		err = runWithoutPanic(t, func() error {
			_, err := g.Hash.VerifyRelChain(r.ID())
			return err
		})
		requireUnsupportedHashError(t, err)
	})
}
