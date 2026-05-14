// Tests in this file pin R4-F3 from the 2026-05-08 maintainability
// review: Core.Close must serialize with live operations and post-close
// public mutations / index registrations must short-circuit with
// ErrGraphClosed instead of touching the (possibly-closed) store.
package core

import (
	"context"
	"errors"
	"testing"

	eventspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/events"
	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/index"
)

func TestR4_PostClose_NodeAdd_ReturnsErrGraphClosed(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	_, err := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	if !errors.Is(err, ErrGraphClosed) {
		t.Fatalf("post-close NodeAdd: got %v, want errors.Is ErrGraphClosed", err)
	}
}

func TestR4_PostClose_NodeUpdate_ReturnsErrGraphClosed(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	n, err := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	_, err = g.Nodes.Update(context.Background(), n.ID(), map[string]any{"k": 1})
	if !errors.Is(err, ErrGraphClosed) {
		t.Fatalf("post-close NodeUpdate: got %v, want errors.Is ErrGraphClosed", err)
	}
}

func TestR4_PostClose_RelAdd_ReturnsErrGraphClosed(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	a, err := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"B"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	_, err = g.Rels.Add(context.Background(), "LINK", a, b, nil)
	if !errors.Is(err, ErrGraphClosed) {
		t.Fatalf("post-close RelAdd: got %v, want errors.Is ErrGraphClosed", err)
	}
}

func TestR4_PostClose_RelDelete_ReturnsErrGraphClosed(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	a, err := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"B"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := g.Rels.Add(context.Background(), "LINK", a, b, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := g.Rels.Delete(context.Background(), r.ID()); !errors.Is(err, ErrGraphClosed) {
		t.Fatalf("post-close RelDelete: got %v, want errors.Is ErrGraphClosed", err)
	}
}

// Index provider registration after Close must also be rejected — an
// unrejected provider would observe events that never fire and be
// orphaned because the indexProviders map was already drained.
func TestR4_PostClose_RegisterProvider_ReturnsErrGraphClosed(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	provider := &noopIndexProvider{name: "post-close"}
	if err := g.Index.RegisterProvider(provider); !errors.Is(err, ErrGraphClosed) {
		t.Fatalf("post-close RegisterProvider: got %v, want errors.Is ErrGraphClosed", err)
	}
}

// noopIndexProvider is a minimal IndexProvider used only to drive the
// post-close registration path.
type noopIndexProvider struct {
	name string
}

func (p *noopIndexProvider) Name() string                  { return p.name }
func (p *noopIndexProvider) OnEvent(eventspkg.Event) error { return nil }
func (p *noopIndexProvider) Close() error                  { return nil }

var _ indexpkg.IndexProvider = (*noopIndexProvider)(nil)
