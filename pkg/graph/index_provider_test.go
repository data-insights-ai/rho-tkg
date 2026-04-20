package graph

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
)

// mockIndexProvider captures events it receives and tracks Close calls.
// Tests assert on its observable state.
type mockIndexProvider struct {
	name    string
	mu      sync.Mutex
	events  []Event
	closed  atomic.Bool
	closeFn func() error // optional; returns nil if unset
}

func (m *mockIndexProvider) Name() string { return m.name }

func (m *mockIndexProvider) OnEvent(ev Event, _ *Graph) {
	m.mu.Lock()
	m.events = append(m.events, ev)
	m.mu.Unlock()
}

func (m *mockIndexProvider) Close() error {
	m.closed.Store(true)
	if m.closeFn != nil {
		return m.closeFn()
	}
	return nil
}

func (m *mockIndexProvider) capturedEvents() []Event {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Event, len(m.events))
	copy(out, m.events)
	return out
}

func newProviderTestGraph(t *testing.T) *Graph {
	t.Helper()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("graph.New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g
}

func TestIndexProvider_RegisterAndListIsOrdered(t *testing.T) {
	g := newProviderTestGraph(t)

	providers := []IndexProvider{
		&mockIndexProvider{name: "charlie"},
		&mockIndexProvider{name: "alpha"},
		&mockIndexProvider{name: "bravo"},
	}
	for _, p := range providers {
		if err := g.RegisterIndexProvider(p); err != nil {
			t.Fatalf("register %q: %v", p.Name(), err)
		}
	}

	got := g.IndexProviders()
	want := []string{"alpha", "bravo", "charlie"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("IndexProviders order: got %v, want %v", got, want)
	}
}

func TestIndexProvider_DuplicateNameRejected(t *testing.T) {
	g := newProviderTestGraph(t)
	p := &mockIndexProvider{name: "spatial"}
	if err := g.RegisterIndexProvider(p); err != nil {
		t.Fatalf("first register: %v", err)
	}
	err := g.RegisterIndexProvider(&mockIndexProvider{name: "spatial"})
	if !errors.Is(err, ErrIndexProviderExists) {
		t.Errorf("expected ErrIndexProviderExists, got %v", err)
	}
}

func TestIndexProvider_EmptyNameRejected(t *testing.T) {
	g := newProviderTestGraph(t)
	err := g.RegisterIndexProvider(&mockIndexProvider{name: ""})
	if !errors.Is(err, ErrIndexProviderEmptyName) {
		t.Errorf("expected ErrIndexProviderEmptyName, got %v", err)
	}
}

func TestIndexProvider_NilRejected(t *testing.T) {
	g := newProviderTestGraph(t)
	if err := g.RegisterIndexProvider(nil); err == nil {
		t.Error("expected error for nil provider")
	}
}

func TestIndexProvider_AutoCreatesEventBus(t *testing.T) {
	g := newProviderTestGraph(t)

	if g.GetEventBus() != nil {
		t.Fatal("fresh Graph should not have an event bus yet")
	}
	p := &mockIndexProvider{name: "spatial"}
	if err := g.RegisterIndexProvider(p); err != nil {
		t.Fatalf("register: %v", err)
	}
	if g.GetEventBus() == nil {
		t.Error("RegisterIndexProvider should auto-create an EventBus when none is attached")
	}
}

func TestIndexProvider_ReceivesNodeEvents(t *testing.T) {
	g := newProviderTestGraph(t)
	p := &mockIndexProvider{name: "spatial"}
	if err := g.RegisterIndexProvider(p); err != nil {
		t.Fatalf("register: %v", err)
	}

	n, err := g.AddNode([]string{"Gemeinde"}, map[string]any{"gkz": "60201"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	events := p.capturedEvents()
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	if events[0].Type != EventNodeCreate {
		t.Errorf("event type: got %v, want EventNodeCreate", events[0].Type)
	}
	if events[0].EntityID != n.InternalID().SnowflakeID() {
		t.Errorf("event entity id: got %v, want %v", events[0].EntityID, n.InternalID().SnowflakeID())
	}
}

func TestIndexProvider_UnregisterStopsEvents(t *testing.T) {
	g := newProviderTestGraph(t)
	p := &mockIndexProvider{name: "spatial"}
	if err := g.RegisterIndexProvider(p); err != nil {
		t.Fatalf("register: %v", err)
	}

	_, err := g.AddNode([]string{"A"}, map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if got := len(p.capturedEvents()); got != 1 {
		t.Fatalf("expected 1 event after first AddNode, got %d", got)
	}

	if err := g.UnregisterIndexProvider("spatial"); err != nil {
		t.Fatalf("unregister: %v", err)
	}
	if !p.closed.Load() {
		t.Error("Close should have been called on unregister")
	}

	_, err = g.AddNode([]string{"B"}, map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if got := len(p.capturedEvents()); got != 1 {
		t.Errorf("expected still 1 event after unregister, got %d", got)
	}
}

func TestIndexProvider_UnregisterUnknown(t *testing.T) {
	g := newProviderTestGraph(t)
	err := g.UnregisterIndexProvider("nope")
	if !errors.Is(err, ErrIndexProviderNotFound) {
		t.Errorf("expected ErrIndexProviderNotFound, got %v", err)
	}
}

func TestIndexProvider_CloseCalledFromGraphClose(t *testing.T) {
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	p := &mockIndexProvider{name: "spatial"}
	if err := g.RegisterIndexProvider(p); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !p.closed.Load() {
		t.Error("provider Close should have been called from Graph.Close")
	}
}

func TestIndexProvider_CloseErrorsAreJoined(t *testing.T) {
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	boom := fmt.Errorf("spatial-close-failed")
	p := &mockIndexProvider{name: "spatial", closeFn: func() error { return boom }}
	if err := g.RegisterIndexProvider(p); err != nil {
		t.Fatalf("register: %v", err)
	}
	err = g.Close()
	if err == nil || !errors.Is(err, boom) {
		t.Errorf("expected Close error to wrap provider error; got %v", err)
	}
}

func TestIndexProvider_AsyncBusIncompatible(t *testing.T) {
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	ab := NewAsyncEventBus(AsyncEventBusConfig{QueueSize: 8, Workers: 1})
	g.SetAsyncEventBus(ab)

	err = g.RegisterIndexProvider(&mockIndexProvider{name: "spatial"})
	if err == nil {
		t.Fatal("expected registration to fail with AsyncEventBus attached")
	}
	// Error message should mention the mismatch so the caller can diagnose.
	if !containsStr(err.Error(), "synchronous EventBus") {
		t.Errorf("error should mention synchronous EventBus; got %q", err.Error())
	}
}

func containsStr(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
