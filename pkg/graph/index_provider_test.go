package graph

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// mockIndexProvider implements the new (Phase 6) IndexProvider interface.
// Captures events it receives and tracks Close calls. Tests assert on its
// observable state.
type mockIndexProvider struct {
	name    string
	mu      sync.Mutex
	events  []Event
	closed  atomic.Bool
	closeFn func() error // optional; returns nil if unset
	onErr   error        // optional; returned from OnEvent when set
}

func (m *mockIndexProvider) Name() string { return m.name }

func (m *mockIndexProvider) OnEvent(ev Event) error {
	m.mu.Lock()
	m.events = append(m.events, ev)
	m.mu.Unlock()
	return m.onErr
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

// mockLegacyIndexProvider implements the LegacyIndexProvider shape so the
// backward-compat path through RegisterLegacyIndexProvider can be exercised.
type mockLegacyIndexProvider struct {
	name      string
	mu        sync.Mutex
	events    []Event
	graphSeen *Graph // captured from OnEvent's graph argument
	closed    atomic.Bool
}

func (m *mockLegacyIndexProvider) Name() string { return m.name }

func (m *mockLegacyIndexProvider) OnEvent(ev Event, g *Graph) {
	m.mu.Lock()
	m.events = append(m.events, ev)
	m.graphSeen = g
	m.mu.Unlock()
}

func (m *mockLegacyIndexProvider) Close() error {
	m.closed.Store(true)
	return nil
}

func (m *mockLegacyIndexProvider) capturedEvents() []Event {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Event, len(m.events))
	copy(out, m.events)
	return out
}

// initializableProvider implements both IndexProvider and Initializable.
// Tests verify the bulk-load callback receives a usable GraphReader and
// that errors from Init roll back the registration.
type initializableProvider struct {
	mockIndexProvider
	initCalled atomic.Int32
	initErr    error
	seenNodes  []*types.Node
	seenRels   []*types.Relationship
	initMu     sync.Mutex
}

func (p *initializableProvider) Init(g GraphReader) error {
	p.initCalled.Add(1)
	if p.initErr != nil {
		return p.initErr
	}
	nodes, err := g.AllNodes(QueryOpts{})
	if err != nil {
		return err
	}
	rels, err := g.AllRelationships(QueryOpts{})
	if err != nil {
		return err
	}
	p.initMu.Lock()
	p.seenNodes = nodes
	p.seenRels = rels
	p.initMu.Unlock()
	return nil
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

// TestIndexProvider_AsyncBusSupported verifies that the Phase 6 redesign
// removed the "synchronous EventBus only" restriction. Both publisher
// types must accept new IndexProviders.
func TestIndexProvider_AsyncBusSupported(t *testing.T) {
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	ab := NewAsyncEventBus(AsyncEventBusConfig{QueueSize: 8, Workers: 1})
	g.SetAsyncEventBus(ab)

	p := &mockIndexProvider{name: "spatial"}
	if err := g.RegisterIndexProvider(p); err != nil {
		t.Fatalf("register on async bus: %v", err)
	}

	if _, err := g.AddNode([]string{"X"}, nil); err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	// Async dispatch is not synchronous with the mutation; close the bus
	// to drain pending events before asserting.
	ab.Close()
	if got := len(p.capturedEvents()); got != 1 {
		t.Errorf("async-bus delivery: got %d events, want 1", got)
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

// --- Phase 6 redesign: legacy provider, Initializable, GraphReader ---

func TestIndexProvider_LegacyAdapterReceivesEvents(t *testing.T) {
	g := newProviderTestGraph(t)
	p := &mockLegacyIndexProvider{name: "legacy-spatial"}
	if err := g.RegisterLegacyIndexProvider(p); err != nil {
		t.Fatalf("RegisterLegacyIndexProvider: %v", err)
	}

	if names := g.IndexProviders(); len(names) != 1 || names[0] != "legacy-spatial" {
		t.Fatalf("registry: got %v, want [legacy-spatial]", names)
	}

	n, err := g.AddNode([]string{"Gemeinde"}, map[string]any{"gkz": "60201"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	events := p.capturedEvents()
	if len(events) != 1 {
		t.Fatalf("legacy provider got %d events, want 1", len(events))
	}
	if events[0].EntityID != types.EntityID(n.ID()) {
		t.Errorf("event entity id: got %v, want %v", events[0].EntityID, types.EntityID(n.ID()))
	}
	if p.graphSeen != g {
		t.Error("legacy adapter should pass through *Graph reference to OnEvent")
	}
}

func TestIndexProvider_LegacyUnregisterClosesProvider(t *testing.T) {
	g := newProviderTestGraph(t)
	p := &mockLegacyIndexProvider{name: "legacy-spatial"}
	if err := g.RegisterLegacyIndexProvider(p); err != nil {
		t.Fatalf("RegisterLegacyIndexProvider: %v", err)
	}
	if err := g.UnregisterIndexProvider("legacy-spatial"); err != nil {
		t.Fatalf("Unregister: %v", err)
	}
	if !p.closed.Load() {
		t.Error("legacy provider Close should be invoked on unregister")
	}
}

func TestIndexProvider_LegacyNilRejected(t *testing.T) {
	g := newProviderTestGraph(t)
	if err := g.RegisterLegacyIndexProvider(nil); err == nil {
		t.Error("expected error for nil legacy provider")
	}
}

func TestIndexProvider_InitializableBulkLoad(t *testing.T) {
	g := newProviderTestGraph(t)
	// Seed graph state BEFORE registering the provider so Init has
	// something to bulk-load.
	n1, err := g.AddNode([]string{"Gemeinde"}, map[string]any{"gkz": "60201"})
	if err != nil {
		t.Fatalf("AddNode 1: %v", err)
	}
	n2, err := g.AddNode([]string{"Gemeinde"}, map[string]any{"gkz": "60202"})
	if err != nil {
		t.Fatalf("AddNode 2: %v", err)
	}
	if _, err := g.AddRelationshipByID("RELATED", n1.ID(), n2.ID(), nil); err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	p := &initializableProvider{mockIndexProvider: mockIndexProvider{name: "spatial"}}
	if err := g.RegisterIndexProvider(p); err != nil {
		t.Fatalf("register: %v", err)
	}

	if got := p.initCalled.Load(); got != 1 {
		t.Errorf("Init call count = %d, want 1", got)
	}
	p.initMu.Lock()
	gotNodes := len(p.seenNodes)
	gotRels := len(p.seenRels)
	p.initMu.Unlock()
	if gotNodes != 2 {
		t.Errorf("Init saw %d nodes, want 2", gotNodes)
	}
	if gotRels != 1 {
		t.Errorf("Init saw %d rels, want 1", gotRels)
	}
}

func TestIndexProvider_InitializableErrorRollsBackRegistration(t *testing.T) {
	g := newProviderTestGraph(t)
	boom := errors.New("init-failed")
	p := &initializableProvider{
		mockIndexProvider: mockIndexProvider{name: "spatial"},
		initErr:           boom,
	}

	err := g.RegisterIndexProvider(p)
	if err == nil {
		t.Fatal("expected register to fail when Init errors")
	}
	if !errors.Is(err, boom) {
		t.Errorf("error chain should wrap Init error; got %v", err)
	}
	if names := g.IndexProviders(); len(names) != 0 {
		t.Errorf("provider should be removed after Init failure; got registry %v", names)
	}

	// Subscription must have been torn down — subsequent events must not
	// reach the provider, otherwise we leaked a subscription closure.
	if _, err := g.AddNode([]string{"X"}, nil); err != nil {
		t.Fatal(err)
	}
	if got := len(p.capturedEvents()); got != 0 {
		t.Errorf("expected 0 events after Init failure rollback, got %d (subscription leak)", got)
	}
}

func TestIndexProvider_InitializableSeesAddedAfterEvents(t *testing.T) {
	// Two-phase: Init populates from current state, then subsequent
	// mutations arrive via OnEvent. Verify the provider can stitch
	// bulk-load + incremental updates without missing or double-counting.
	g := newProviderTestGraph(t)
	if _, err := g.AddNode([]string{"A"}, nil); err != nil {
		t.Fatal(err)
	}

	p := &initializableProvider{mockIndexProvider: mockIndexProvider{name: "spatial"}}
	if err := g.RegisterIndexProvider(p); err != nil {
		t.Fatal(err)
	}
	p.initMu.Lock()
	bulkNodes := len(p.seenNodes)
	p.initMu.Unlock()
	if bulkNodes != 1 {
		t.Errorf("Init bulk-load saw %d nodes, want 1", bulkNodes)
	}

	// Mutation after registration should reach OnEvent (not Init).
	if _, err := g.AddNode([]string{"B"}, nil); err != nil {
		t.Fatal(err)
	}
	if got := len(p.capturedEvents()); got != 1 {
		t.Errorf("expected 1 OnEvent after Init, got %d", got)
	}
}

func TestIndexProvider_OnEventErrorDoesNotAbortMutation(t *testing.T) {
	g := newProviderTestGraph(t)
	p := &mockIndexProvider{name: "spatial", onErr: errors.New("provider-failed")}
	if err := g.RegisterIndexProvider(p); err != nil {
		t.Fatal(err)
	}

	// AddNode must succeed even when the provider's OnEvent reports an
	// error — provider failures are best-effort diagnostics, not
	// mutation veto.
	if _, err := g.AddNode([]string{"X"}, nil); err != nil {
		t.Errorf("AddNode should succeed when provider returns OnEvent error; got %v", err)
	}
	if got := len(p.capturedEvents()); got != 1 {
		t.Errorf("expected 1 event observed by provider, got %d", got)
	}
}

// graphReaderProbe is a minimal Initializable that records that the
// GraphReader handed to Init really is restricted (no mutation surface).
// The compiler enforces this — we cannot call g.AddNode on a GraphReader
// — so the test exists to lock the contract: any future attempt to widen
// GraphReader will break this test by allowing the recorded type to
// expose mutation methods.
type graphReaderProbe struct {
	mockIndexProvider
	receivedReader GraphReader
}

func (p *graphReaderProbe) Init(g GraphReader) error {
	p.receivedReader = g
	return nil
}

func TestIndexProvider_InitReceivesGraphReaderInterface(t *testing.T) {
	g := newProviderTestGraph(t)
	p := &graphReaderProbe{mockIndexProvider: mockIndexProvider{name: "probe"}}
	if err := g.RegisterIndexProvider(p); err != nil {
		t.Fatalf("register: %v", err)
	}
	if p.receivedReader == nil {
		t.Fatal("Init did not receive a GraphReader")
	}
	// Confirm the reader is a GraphReader (not *Graph) — type assertion
	// to *Graph must FAIL because graphReaderView is unexported.
	if _, ok := p.receivedReader.(*Graph); ok {
		t.Error("GraphReader should not be a *Graph (would expose mutation surface)")
	}
}
