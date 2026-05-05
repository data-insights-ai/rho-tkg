package graph

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
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
	if events[0].EntityID != types.EntityID(n.ID()) {
		t.Errorf("event entity id: got %v, want %v", events[0].EntityID, types.EntityID(n.ID()))
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
	if !strings.Contains(err.Error(), "synchronous EventBus") {
		t.Errorf("error should mention synchronous EventBus; got %q", err.Error())
	}
}

// TestIndexProvider_ConcurrentRegisterRaceSafe exercises the TOCTOU window
// between the dup-check and the entry insertion in RegisterIndexProvider.
// Pre-fix code unlocked g.mu between the dup check and the Subscribe call,
// allowing N goroutines registering the same Name to all pass the dup check
// and all subscribe; the entry insertion would then overwrite the prior
// goroutines' map entry, leaving their bus subscriptions orphaned. Post-fix
// holds g.mu through Subscribe so exactly one goroutine succeeds and exactly
// one subscription is created.
//
// Run with -race to surface any residual races on g.indexProviders.
func TestIndexProvider_ConcurrentRegisterRaceSafe(t *testing.T) {
	g := newProviderTestGraph(t)

	const N = 50
	providers := make([]*mockIndexProvider, N)
	for i := range providers {
		providers[i] = &mockIndexProvider{name: "spatial"}
	}

	var (
		wg        sync.WaitGroup
		successes atomic.Int32
		dups      atomic.Int32
		other     atomic.Int32
	)
	for i := range providers {
		wg.Add(1)
		go func(p *mockIndexProvider) {
			defer wg.Done()
			err := g.RegisterIndexProvider(p)
			switch {
			case err == nil:
				successes.Add(1)
			case errors.Is(err, ErrIndexProviderExists):
				dups.Add(1)
			default:
				other.Add(1)
				t.Errorf("unexpected error: %v", err)
			}
		}(providers[i])
	}
	wg.Wait()

	if successes.Load() != 1 {
		t.Errorf("successes = %d, want 1", successes.Load())
	}
	if dups.Load() != N-1 {
		t.Errorf("ErrIndexProviderExists count = %d, want %d", dups.Load(), N-1)
	}
	if other.Load() != 0 {
		t.Errorf("unexpected errors = %d", other.Load())
	}
	if got := len(g.IndexProviders()); got != 1 {
		t.Errorf("registered providers = %d, want 1", got)
	}

	// Fire one event; only the single registered provider should observe it.
	// If orphan subscriptions leaked (pre-fix behaviour), multiple providers
	// would have received the event because all N closures subscribed to bus.
	if _, err := g.AddNode([]string{"X"}, nil); err != nil {
		t.Fatal(err)
	}
	fired := 0
	for _, p := range providers {
		if len(p.capturedEvents()) > 0 {
			fired++
		}
	}
	if fired != 1 {
		t.Errorf("providers receiving event = %d, want 1 (orphan subscription leak)", fired)
	}
}
