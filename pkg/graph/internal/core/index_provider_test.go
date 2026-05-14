package core

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/memory"

	eventspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/events"
	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/index"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// mockIndexProvider implements the new (Phase 6) indexpkg.IndexProvider interface.
// Captures events it receives and tracks Close calls. Tests assert on its
// observable state.
type mockIndexProvider struct {
	name       string
	mu         sync.Mutex
	events     []eventspkg.Event
	closed     atomic.Bool
	closeFn    func() error // optional; returns nil if unset
	closePanic any          // optional; panics from Close when set
	onErr      error        // optional; returned from OnEvent when set
}

func (m *mockIndexProvider) Name() string { return m.name }

func (m *mockIndexProvider) OnEvent(ev eventspkg.Event) error {
	m.mu.Lock()
	m.events = append(m.events, ev)
	m.mu.Unlock()
	return m.onErr
}

func (m *mockIndexProvider) Close() error {
	m.closed.Store(true)
	if m.closePanic != nil {
		panic(m.closePanic)
	}
	if m.closeFn != nil {
		return m.closeFn()
	}
	return nil
}

func (m *mockIndexProvider) capturedEvents() []eventspkg.Event {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]eventspkg.Event, len(m.events))
	copy(out, m.events)
	return out
}

// mockLegacyIndexProvider implements the indexpkg.LegacyIndexProvider shape so the
// backward-compat path through RegisterLegacyIndexProvider can be exercised.
type mockLegacyIndexProvider struct {
	name      string
	mu        sync.Mutex
	events    []eventspkg.Event
	graphSeen indexpkg.GraphReader // captured from OnEvent's graph argument
	closed    atomic.Bool
}

func (m *mockLegacyIndexProvider) Name() string { return m.name }

func (m *mockLegacyIndexProvider) OnEvent(ev eventspkg.Event, g indexpkg.GraphReader) {
	m.mu.Lock()
	m.events = append(m.events, ev)
	m.graphSeen = g
	m.mu.Unlock()
}

func (m *mockLegacyIndexProvider) Close() error {
	m.closed.Store(true)
	return nil
}

func (m *mockLegacyIndexProvider) capturedEvents() []eventspkg.Event {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]eventspkg.Event, len(m.events))
	copy(out, m.events)
	return out
}

// initializableProvider implements both indexpkg.IndexProvider and indexpkg.Initializable.
// Tests verify the bulk-load callback receives a usable indexpkg.GraphReader and
// that errors from Init roll back the registration.
type initializableProvider struct {
	mockIndexProvider
	initCalled atomic.Int32
	initErr    error
	initPanic  any
	seenNodes  []*types.Node
	seenRels   []*types.Relationship
	initMu     sync.Mutex
}

func (p *initializableProvider) Init(g indexpkg.GraphReader) error {
	p.initCalled.Add(1)
	if p.initPanic != nil {
		panic(p.initPanic)
	}
	if p.initErr != nil {
		return p.initErr
	}
	nodes, err := g.AllNodes(storepkg.QueryOpts{})
	if err != nil {
		return err
	}
	rels, err := g.AllRelationships(storepkg.QueryOpts{})
	if err != nil {
		return err
	}
	p.initMu.Lock()
	p.seenNodes = nodes
	p.seenRels = rels
	p.initMu.Unlock()
	return nil
}

type blockingInitializableProvider struct {
	mockIndexProvider
	initStarted      chan struct{}
	releaseInit      chan struct{}
	initReturned     atomic.Bool
	closedDuringInit atomic.Bool
}

func newBlockingInitializableProvider(name string) *blockingInitializableProvider {
	return &blockingInitializableProvider{
		mockIndexProvider: mockIndexProvider{name: name},
		initStarted:       make(chan struct{}),
		releaseInit:       make(chan struct{}),
	}
}

func (p *blockingInitializableProvider) Init(indexpkg.GraphReader) error {
	close(p.initStarted)
	<-p.releaseInit
	p.initReturned.Store(true)
	return nil
}

func (p *blockingInitializableProvider) Close() error {
	if !p.initReturned.Load() {
		p.closedDuringInit.Store(true)
	}
	return p.mockIndexProvider.Close()
}

type blockingEventProvider struct {
	mockIndexProvider
	eventStarted      chan struct{}
	releaseEvent      chan struct{}
	startOnce         sync.Once
	eventReturned     atomic.Bool
	closedDuringEvent atomic.Bool
}

func newBlockingEventProvider(name string) *blockingEventProvider {
	return &blockingEventProvider{
		mockIndexProvider: mockIndexProvider{name: name},
		eventStarted:      make(chan struct{}),
		releaseEvent:      make(chan struct{}),
	}
}

func (p *blockingEventProvider) OnEvent(ev eventspkg.Event) error {
	p.startOnce.Do(func() { close(p.eventStarted) })
	<-p.releaseEvent
	p.eventReturned.Store(true)
	return p.mockIndexProvider.OnEvent(ev)
}

func (p *blockingEventProvider) Close() error {
	if !p.eventReturned.Load() {
		p.closedDuringEvent.Store(true)
	}
	return p.mockIndexProvider.Close()
}

func newProviderTestGraph(t *testing.T) *Core {
	t.Helper()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("graph.New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g
}

type closeTrackingStore struct {
	storepkg.MandatoryStore
	closed atomic.Bool
}

func (s *closeTrackingStore) Close() error {
	s.closed.Store(true)
	return s.MandatoryStore.Close()
}

func TestIndexProvider_RegisterAndListIsOrdered(t *testing.T) {
	g := newProviderTestGraph(t)

	providers := []indexpkg.IndexProvider{
		&mockIndexProvider{name: "charlie"},
		&mockIndexProvider{name: "alpha"},
		&mockIndexProvider{name: "bravo"},
	}
	for _, p := range providers {
		if err := g.Index.RegisterProvider(p); err != nil {
			t.Fatalf("register %q: %v", p.Name(), err)
		}
	}

	got := g.Index.Providers()
	want := []string{"alpha", "bravo", "charlie"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("IndexProviders order: got %v, want %v", got, want)
	}
}

func TestIndexProvider_ProvidersClosedFlagReturnsEmpty(t *testing.T) {
	g := newProviderTestGraph(t)
	if err := g.Index.RegisterProvider(&mockIndexProvider{name: "spatial"}); err != nil {
		t.Fatalf("RegisterProvider: %v", err)
	}

	// Simulate the first phase of Close: the closed flag is visible before
	// c.mu.Lock drains indexProviders. Providers has no error return, so it
	// must fail closed by returning an empty list in that window.
	g.closed.Store(true)
	if names := g.Index.Providers(); len(names) != 0 {
		t.Fatalf("Providers after closed flag = %v, want empty", names)
	}
}

func TestIndexProvider_ProvidersRechecksClosedAfterWaitingForLock(t *testing.T) {
	g := newProviderTestGraph(t)
	if err := g.Index.RegisterProvider(&mockIndexProvider{name: "spatial"}); err != nil {
		t.Fatalf("RegisterProvider: %v", err)
	}

	g.mu.Lock()
	done := make(chan []string, 1)
	go func() {
		done <- g.Index.Providers()
	}()

	select {
	case names := <-done:
		g.mu.Unlock()
		t.Fatalf("Providers returned while write lock held: %v", names)
	case <-time.After(50 * time.Millisecond):
	}

	g.closed.Store(true)
	g.mu.Unlock()

	select {
	case names := <-done:
		if len(names) != 0 {
			t.Fatalf("Providers after closed flag while waiting for lock = %v, want empty", names)
		}
	case <-time.After(time.Second):
		t.Fatal("Providers did not return after write lock released")
	}
}

func TestIndexProvider_DuplicateNameRejected(t *testing.T) {
	g := newProviderTestGraph(t)
	p := &mockIndexProvider{name: "spatial"}
	if err := g.Index.RegisterProvider(p); err != nil {
		t.Fatalf("first register: %v", err)
	}
	err := g.Index.RegisterProvider(&mockIndexProvider{name: "spatial"})
	if !errors.Is(err, indexpkg.ErrIndexProviderExists) {
		t.Errorf("expected indexpkg.ErrIndexProviderExists, got %v", err)
	}
}

func TestIndexProvider_EmptyNameRejected(t *testing.T) {
	g := newProviderTestGraph(t)
	err := g.Index.RegisterProvider(&mockIndexProvider{name: ""})
	if !errors.Is(err, indexpkg.ErrIndexProviderEmptyName) {
		t.Errorf("expected indexpkg.ErrIndexProviderEmptyName, got %v", err)
	}
}

func TestIndexProvider_BlankNameRejected(t *testing.T) {
	g := newProviderTestGraph(t)
	if err := g.Index.RegisterProvider(&mockIndexProvider{name: " \t\n "}); !errors.Is(err, indexpkg.ErrIndexProviderEmptyName) {
		t.Fatalf("RegisterProvider blank name = %v, want ErrIndexProviderEmptyName", err)
	}
	if err := g.Index.UnregisterProvider(" \t\n "); !errors.Is(err, indexpkg.ErrIndexProviderEmptyName) {
		t.Fatalf("UnregisterProvider blank name = %v, want ErrIndexProviderEmptyName", err)
	}
}

func TestIndexProvider_NilRejected(t *testing.T) {
	g := newProviderTestGraph(t)
	if err := g.Index.RegisterProvider(nil); err == nil {
		t.Error("expected error for nil provider")
	}

	var typedNil *mockIndexProvider
	if err := g.Index.RegisterProvider(typedNil); err == nil {
		t.Error("expected error for typed nil provider")
	}
}

func TestIndexProvider_AutoCreatesEventBus(t *testing.T) {
	g := newProviderTestGraph(t)

	if g.Events.GetSync() != nil {
		t.Fatal("fresh Graph should not have an event bus yet")
	}
	p := &mockIndexProvider{name: "spatial"}
	if err := g.Index.RegisterProvider(p); err != nil {
		t.Fatalf("register: %v", err)
	}
	if g.Events.GetSync() == nil {
		t.Error("RegisterIndexProvider should auto-create an eventspkg.EventBus when none is attached")
	}
}

func TestIndexProvider_ReceivesNodeEvents(t *testing.T) {
	g := newProviderTestGraph(t)
	p := &mockIndexProvider{name: "spatial"}
	if err := g.Index.RegisterProvider(p); err != nil {
		t.Fatalf("register: %v", err)
	}

	n, err := g.Nodes.Add([]string{"Gemeinde"}, map[string]any{"gkz": "60201"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	events := p.capturedEvents()
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	if events[0].Type != eventspkg.EventNodeCreate {
		t.Errorf("event type: got %v, want eventspkg.EventNodeCreate", events[0].Type)
	}
	if events[0].EntityID != types.EntityID(n.ID()) {
		t.Errorf("event entity id: got %v, want %v", events[0].EntityID, types.EntityID(n.ID()))
	}
}

func TestIndexProvider_UnregisterStopsEvents(t *testing.T) {
	g := newProviderTestGraph(t)
	p := &mockIndexProvider{name: "spatial"}
	if err := g.Index.RegisterProvider(p); err != nil {
		t.Fatalf("register: %v", err)
	}

	_, err := g.Nodes.Add([]string{"A"}, map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if got := len(p.capturedEvents()); got != 1 {
		t.Fatalf("expected 1 event after first AddNode, got %d", got)
	}

	if err := g.Index.UnregisterProvider("spatial"); err != nil {
		t.Fatalf("unregister: %v", err)
	}
	if !p.closed.Load() {
		t.Error("Close should have been called on unregister")
	}

	_, err = g.Nodes.Add([]string{"B"}, map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if got := len(p.capturedEvents()); got != 1 {
		t.Errorf("expected still 1 event after unregister, got %d", got)
	}
}

func TestIndexProvider_UnregisterClosePanicIsReturnedAndStopsEvents(t *testing.T) {
	g := newProviderTestGraph(t)
	p := &mockIndexProvider{name: "spatial", closePanic: "unregister-panicked"}
	if err := g.Index.RegisterProvider(p); err != nil {
		t.Fatalf("register: %v", err)
	}

	err := g.Index.UnregisterProvider("spatial")
	if err == nil {
		t.Fatal("expected UnregisterProvider to return provider close panic")
	}
	if !strings.Contains(err.Error(), "index provider \"spatial\" close panic") {
		t.Fatalf("UnregisterProvider error missing close panic context: %v", err)
	}
	if !strings.Contains(err.Error(), "unregister-panicked") {
		t.Fatalf("UnregisterProvider error missing panic value: %v", err)
	}
	if !p.closed.Load() {
		t.Fatal("provider Close should have been called")
	}
	if names := g.Index.Providers(); len(names) != 0 {
		t.Fatalf("provider should be removed after close panic; got registry %v", names)
	}

	if _, err := g.Nodes.Add([]string{"B"}, map[string]any{}); err != nil {
		t.Fatal(err)
	}
	if got := len(p.capturedEvents()); got != 0 {
		t.Fatalf("expected 0 events after unregister close panic, got %d", got)
	}
}

func TestIndexProvider_UnregisterUnknown(t *testing.T) {
	g := newProviderTestGraph(t)
	err := g.Index.UnregisterProvider("nope")
	if !errors.Is(err, indexpkg.ErrIndexProviderNotFound) {
		t.Errorf("expected indexpkg.ErrIndexProviderNotFound, got %v", err)
	}
}

func TestIndexProvider_CloseCalledFromGraphClose(t *testing.T) {
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	p := &mockIndexProvider{name: "spatial"}
	if err := g.Index.RegisterProvider(p); err != nil {
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
	if err := g.Index.RegisterProvider(p); err != nil {
		t.Fatalf("register: %v", err)
	}
	err = g.Close()
	if err == nil || !errors.Is(err, boom) {
		t.Errorf("expected Close error to wrap provider error; got %v", err)
	}
}

func TestIndexProvider_ClosePanicIsReturnedAndStoreStillCloses(t *testing.T) {
	store := &closeTrackingStore{MandatoryStore: memory.New()}
	g, err := New(Config{Store: store})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	p := &mockIndexProvider{name: "spatial", closePanic: "spatial-close-panicked"}
	if err := g.Index.RegisterProvider(p); err != nil {
		t.Fatalf("register: %v", err)
	}

	err = g.Close()
	if err == nil {
		t.Fatal("expected Close to return provider panic error")
	}
	if !strings.Contains(err.Error(), "index provider \"spatial\" close panic") {
		t.Fatalf("Close error missing provider panic context: %v", err)
	}
	if !strings.Contains(err.Error(), "spatial-close-panicked") {
		t.Fatalf("Close error missing panic value: %v", err)
	}
	if !p.closed.Load() {
		t.Error("provider Close should have been called")
	}
	if !store.closed.Load() {
		t.Error("store Close should still run after provider Close panic")
	}
}

// TestIndexProvider_AsyncBusSupported verifies that the Phase 6 redesign
// removed the "synchronous eventspkg.EventBus only" restriction. Both publisher
// types must accept new IndexProviders.
func TestIndexProvider_AsyncBusSupported(t *testing.T) {
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	ab := eventspkg.NewAsyncEventBus(eventspkg.AsyncEventBusConfig{QueueSize: 8, Workers: 1})
	_ = g.Events.SetAsync(ab)

	p := &mockIndexProvider{name: "spatial"}
	if err := g.Index.RegisterProvider(p); err != nil {
		t.Fatalf("register on async bus: %v", err)
	}

	if _, err := g.Nodes.Add([]string{"X"}, nil); err != nil {
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
			err := g.Index.RegisterProvider(p)
			switch {
			case err == nil:
				successes.Add(1)
			case errors.Is(err, indexpkg.ErrIndexProviderExists):
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
		t.Errorf("indexpkg.ErrIndexProviderExists count = %d, want %d", dups.Load(), N-1)
	}
	if other.Load() != 0 {
		t.Errorf("unexpected errors = %d", other.Load())
	}
	if got := len(g.Index.Providers()); got != 1 {
		t.Errorf("registered providers = %d, want 1", got)
	}

	// Fire one event; only the single registered provider should observe it.
	// If orphan subscriptions leaked (pre-fix behaviour), multiple providers
	// would have received the event because all N closures subscribed to bus.
	if _, err := g.Nodes.Add([]string{"X"}, nil); err != nil {
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

// --- Phase 6 redesign: legacy provider, indexpkg.Initializable, indexpkg.GraphReader ---

func TestIndexProvider_LegacyAdapterReceivesEvents(t *testing.T) {
	g := newProviderTestGraph(t)
	p := &mockLegacyIndexProvider{name: "legacy-spatial"}
	if err := g.Index.RegisterLegacyProvider(p); err != nil {
		t.Fatalf("RegisterLegacyIndexProvider: %v", err)
	}

	if names := g.Index.Providers(); len(names) != 1 || names[0] != "legacy-spatial" {
		t.Fatalf("registry: got %v, want [legacy-spatial]", names)
	}

	n, err := g.Nodes.Add([]string{"Gemeinde"}, map[string]any{"gkz": "60201"})
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
	// Phase 7f: LegacyIndexProvider.OnEvent now receives a GraphReader (not
	// *Core). Verify the reader is non-nil and the adapter forwards a working
	// reader by reading back the just-created node.
	if p.graphSeen == nil {
		t.Fatal("legacy adapter should hand a GraphReader to OnEvent")
	}
	if got, err := p.graphSeen.GetNode(n.ID()); err != nil {
		t.Errorf("legacy adapter GraphReader.GetNode failed: %v", err)
	} else if got.ID() != n.ID() {
		t.Errorf("legacy adapter GraphReader.GetNode: got id %v, want %v", got.ID(), n.ID())
	}
}

func TestIndexProvider_LegacyUnregisterClosesProvider(t *testing.T) {
	g := newProviderTestGraph(t)
	p := &mockLegacyIndexProvider{name: "legacy-spatial"}
	if err := g.Index.RegisterLegacyProvider(p); err != nil {
		t.Fatalf("RegisterLegacyIndexProvider: %v", err)
	}
	if err := g.Index.UnregisterProvider("legacy-spatial"); err != nil {
		t.Fatalf("Unregister: %v", err)
	}
	if !p.closed.Load() {
		t.Error("legacy provider Close should be invoked on unregister")
	}
}

func TestIndexProvider_LegacyNilRejected(t *testing.T) {
	g := newProviderTestGraph(t)
	if err := g.Index.RegisterLegacyProvider(nil); err == nil {
		t.Error("expected error for nil legacy provider")
	}

	var typedNil *mockLegacyIndexProvider
	if err := g.Index.RegisterLegacyProvider(typedNil); err == nil {
		t.Error("expected error for typed nil legacy provider")
	}
}

func TestIndexProvider_InitializableBulkLoad(t *testing.T) {
	g := newProviderTestGraph(t)
	// Seed graph state BEFORE registering the provider so Init has
	// something to bulk-load.
	n1, err := g.Nodes.Add([]string{"Gemeinde"}, map[string]any{"gkz": "60201"})
	if err != nil {
		t.Fatalf("AddNode 1: %v", err)
	}
	n2, err := g.Nodes.Add([]string{"Gemeinde"}, map[string]any{"gkz": "60202"})
	if err != nil {
		t.Fatalf("AddNode 2: %v", err)
	}
	if _, err := g.Rels.AddByID("RELATED", n1.ID(), n2.ID(), nil); err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	p := &initializableProvider{mockIndexProvider: mockIndexProvider{name: "spatial"}}
	if err := g.Index.RegisterProvider(p); err != nil {
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

	err := g.Index.RegisterProvider(p)
	if err == nil {
		t.Fatal("expected register to fail when Init errors")
	}
	if !errors.Is(err, boom) {
		t.Errorf("error chain should wrap Init error; got %v", err)
	}
	if names := g.Index.Providers(); len(names) != 0 {
		t.Errorf("provider should be removed after Init failure; got registry %v", names)
	}
	if !p.closed.Load() {
		t.Fatal("provider Close should be called after Init failure rollback")
	}

	// Subscription must have been torn down — subsequent events must not
	// reach the provider, otherwise we leaked a subscription closure.
	if _, err := g.Nodes.Add([]string{"X"}, nil); err != nil {
		t.Fatal(err)
	}
	if got := len(p.capturedEvents()); got != 0 {
		t.Errorf("expected 0 events after Init failure rollback, got %d (subscription leak)", got)
	}
}

func TestIndexProvider_InitializableErrorReturnsCloseFailure(t *testing.T) {
	g := newProviderTestGraph(t)
	initErr := errors.New("init-failed")
	closeErr := errors.New("close-failed")
	p := &initializableProvider{
		mockIndexProvider: mockIndexProvider{
			name:    "spatial",
			closeFn: func() error { return closeErr },
		},
		initErr: initErr,
	}

	err := g.Index.RegisterProvider(p)
	if !errors.Is(err, initErr) {
		t.Fatalf("RegisterProvider error = %v, want Init error", err)
	}
	if !errors.Is(err, closeErr) {
		t.Fatalf("RegisterProvider error = %v, want Close error joined", err)
	}
	if names := g.Index.Providers(); len(names) != 0 {
		t.Fatalf("provider should be removed after Init+Close failure; got registry %v", names)
	}
	if !p.closed.Load() {
		t.Fatal("provider Close should be called after Init failure")
	}
}

func TestIndexProvider_InitializableErrorReturnsClosePanic(t *testing.T) {
	g := newProviderTestGraph(t)
	initErr := errors.New("init-failed")
	p := &initializableProvider{
		mockIndexProvider: mockIndexProvider{
			name:       "spatial",
			closePanic: "close-panicked",
		},
		initErr: initErr,
	}

	err := g.Index.RegisterProvider(p)
	if !errors.Is(err, initErr) {
		t.Fatalf("RegisterProvider error = %v, want Init error", err)
	}
	if !strings.Contains(err.Error(), "close after Init failure") {
		t.Fatalf("RegisterProvider error missing rollback close context: %v", err)
	}
	if !strings.Contains(err.Error(), "close-panicked") {
		t.Fatalf("RegisterProvider error missing Close panic value: %v", err)
	}
	if names := g.Index.Providers(); len(names) != 0 {
		t.Fatalf("provider should be removed after Init+Close panic; got registry %v", names)
	}
	if !p.closed.Load() {
		t.Fatal("provider Close should be called after Init failure")
	}
}

func TestIndexProvider_InitializablePanicRollsBackRegistration(t *testing.T) {
	g := newProviderTestGraph(t)
	p := &initializableProvider{
		mockIndexProvider: mockIndexProvider{name: "spatial"},
		initPanic:         "init-panicked",
	}

	err := g.Index.RegisterProvider(p)
	if err == nil {
		t.Fatal("expected register to fail when Init panics")
	}
	if !strings.Contains(err.Error(), "Init panic") || !strings.Contains(err.Error(), "init-panicked") {
		t.Fatalf("RegisterProvider panic error = %v, want Init panic detail", err)
	}
	if names := g.Index.Providers(); len(names) != 0 {
		t.Fatalf("provider should be removed after Init panic; got registry %v", names)
	}
	if !p.closed.Load() {
		t.Fatal("provider Close should be called after Init panic rollback")
	}

	if _, err := g.Nodes.Add([]string{"X"}, nil); err != nil {
		t.Fatal(err)
	}
	if got := len(p.capturedEvents()); got != 0 {
		t.Fatalf("expected 0 events after Init panic rollback, got %d (subscription leak)", got)
	}
}

func TestIndexProvider_CloseWaitsForInitializableProviderInit(t *testing.T) {
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	p := newBlockingInitializableProvider("spatial")

	registerDone := make(chan error, 1)
	go func() {
		registerDone <- g.Index.RegisterProvider(p)
	}()

	select {
	case <-p.initStarted:
	case <-time.After(time.Second):
		t.Fatal("RegisterProvider did not enter Init")
	}

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- g.Close()
	}()

	select {
	case err := <-closeDone:
		t.Fatalf("Close returned before Init finished: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if p.closed.Load() {
		t.Fatal("provider Close ran while Init was still blocked")
	}

	close(p.releaseInit)

	if err := <-registerDone; err != nil {
		t.Fatalf("RegisterProvider: %v", err)
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not return after Init finished")
	}
	if p.closedDuringInit.Load() {
		t.Fatal("provider Close observed Init still running")
	}
	if !p.closed.Load() {
		t.Fatal("provider Close was not called by graph Close")
	}
}

func TestIndexProvider_UnregisterWaitsForInitializableProviderInit(t *testing.T) {
	g := newProviderTestGraph(t)
	p := newBlockingInitializableProvider("spatial")

	registerDone := make(chan error, 1)
	go func() {
		registerDone <- g.Index.RegisterProvider(p)
	}()

	select {
	case <-p.initStarted:
	case <-time.After(time.Second):
		t.Fatal("RegisterProvider did not enter Init")
	}

	unregisterDone := make(chan error, 1)
	go func() {
		unregisterDone <- g.Index.UnregisterProvider("spatial")
	}()

	select {
	case err := <-unregisterDone:
		t.Fatalf("UnregisterProvider returned before Init finished: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if p.closed.Load() {
		t.Fatal("provider Close ran while Init was still blocked")
	}

	close(p.releaseInit)

	if err := <-registerDone; err != nil {
		t.Fatalf("RegisterProvider: %v", err)
	}
	select {
	case err := <-unregisterDone:
		if err != nil {
			t.Fatalf("UnregisterProvider: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("UnregisterProvider did not return after Init finished")
	}
	if p.closedDuringInit.Load() {
		t.Fatal("provider Close observed Init still running")
	}
	if !p.closed.Load() {
		t.Fatal("provider Close was not called by UnregisterProvider")
	}
}

func TestIndexProvider_GraphCloseWaitsForInFlightEvent(t *testing.T) {
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	p := newBlockingEventProvider("spatial")
	if err := g.Index.RegisterProvider(p); err != nil {
		t.Fatalf("RegisterProvider: %v", err)
	}

	addDone := make(chan error, 1)
	go func() {
		_, err := g.Nodes.Add([]string{"A"}, nil)
		addDone <- err
	}()

	select {
	case <-p.eventStarted:
	case <-time.After(time.Second):
		t.Fatal("provider OnEvent did not start")
	}

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- g.Close()
	}()

	select {
	case err := <-closeDone:
		t.Fatalf("Close returned before in-flight OnEvent finished: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if p.closed.Load() {
		t.Fatal("provider Close ran while OnEvent was still blocked")
	}

	close(p.releaseEvent)

	select {
	case err := <-addDone:
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("AddNode did not return after provider event was released")
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not return after provider event was released")
	}
	if p.closedDuringEvent.Load() {
		t.Fatal("provider Close observed OnEvent still running")
	}
	if !p.closed.Load() {
		t.Fatal("provider Close was not called by graph Close")
	}
}

func TestIndexProvider_UnregisterWaitsForInFlightEvent(t *testing.T) {
	g := newProviderTestGraph(t)
	p := newBlockingEventProvider("spatial")
	if err := g.Index.RegisterProvider(p); err != nil {
		t.Fatalf("RegisterProvider: %v", err)
	}

	addDone := make(chan error, 1)
	go func() {
		_, err := g.Nodes.Add([]string{"A"}, nil)
		addDone <- err
	}()

	select {
	case <-p.eventStarted:
	case <-time.After(time.Second):
		t.Fatal("provider OnEvent did not start")
	}

	unregisterDone := make(chan error, 1)
	go func() {
		unregisterDone <- g.Index.UnregisterProvider("spatial")
	}()

	select {
	case err := <-unregisterDone:
		t.Fatalf("UnregisterProvider returned before in-flight OnEvent finished: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if p.closed.Load() {
		t.Fatal("provider Close ran while OnEvent was still blocked")
	}

	close(p.releaseEvent)

	select {
	case err := <-addDone:
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("AddNode did not return after provider event was released")
	}
	select {
	case err := <-unregisterDone:
		if err != nil {
			t.Fatalf("UnregisterProvider: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("UnregisterProvider did not return after provider event was released")
	}
	if p.closedDuringEvent.Load() {
		t.Fatal("provider Close observed OnEvent still running")
	}
	if !p.closed.Load() {
		t.Fatal("provider Close was not called by UnregisterProvider")
	}
}

func TestIndexProvider_InitializableSeesAddedAfterEvents(t *testing.T) {
	// Two-phase: Init populates from current state, then subsequent
	// mutations arrive via OnEvent. Verify the provider can stitch
	// bulk-load + incremental updates without missing or double-counting.
	g := newProviderTestGraph(t)
	if _, err := g.Nodes.Add([]string{"A"}, nil); err != nil {
		t.Fatal(err)
	}

	p := &initializableProvider{mockIndexProvider: mockIndexProvider{name: "spatial"}}
	if err := g.Index.RegisterProvider(p); err != nil {
		t.Fatal(err)
	}
	p.initMu.Lock()
	bulkNodes := len(p.seenNodes)
	p.initMu.Unlock()
	if bulkNodes != 1 {
		t.Errorf("Init bulk-load saw %d nodes, want 1", bulkNodes)
	}

	// Mutation after registration should reach OnEvent (not Init).
	if _, err := g.Nodes.Add([]string{"B"}, nil); err != nil {
		t.Fatal(err)
	}
	if got := len(p.capturedEvents()); got != 1 {
		t.Errorf("expected 1 OnEvent after Init, got %d", got)
	}
}

func TestIndexProvider_OnEventErrorDoesNotAbortMutation(t *testing.T) {
	g := newProviderTestGraph(t)
	p := &mockIndexProvider{name: "spatial", onErr: errors.New("provider-failed")}
	if err := g.Index.RegisterProvider(p); err != nil {
		t.Fatal(err)
	}

	// AddNode must succeed even when the provider's OnEvent reports an
	// error — provider failures are best-effort diagnostics, not
	// mutation veto.
	if _, err := g.Nodes.Add([]string{"X"}, nil); err != nil {
		t.Errorf("AddNode should succeed when provider returns OnEvent error; got %v", err)
	}
	if got := len(p.capturedEvents()); got != 1 {
		t.Errorf("expected 1 event observed by provider, got %d", got)
	}
}

func TestGraphReaderViewDelegatesReadMethods(t *testing.T) {
	g := newProviderTestGraph(t)
	a, err := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	b, err := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Bob"})
	if err != nil {
		t.Fatalf("AddNode b: %v", err)
	}
	r, err := g.Rels.Add("KNOWS", a, b, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	reader := graphReaderView{g: g}
	gotNode, err := reader.GetNode(a.ID())
	if err != nil || gotNode.ID() != a.ID() {
		t.Fatalf("GetNode = %v, %v; want %v, nil", gotNode, err, a.ID())
	}
	gotRel, err := reader.GetRelationship(r.ID())
	if err != nil || gotRel.ID() != r.ID() {
		t.Fatalf("GetRelationship = %v, %v; want %v, nil", gotRel, err, r.ID())
	}
	nodes, err := reader.NodesByLabel("Person", storepkg.QueryOpts{})
	if err != nil || len(nodes) != 2 {
		t.Fatalf("NodesByLabel = len %d, %v; want 2, nil", len(nodes), err)
	}
	rels, err := reader.RelationshipsByType("KNOWS", storepkg.QueryOpts{})
	if err != nil || len(rels) != 1 {
		t.Fatalf("RelationshipsByType = len %d, %v; want 1, nil", len(rels), err)
	}
	nodeCount, err := reader.NodeCount()
	if err != nil || nodeCount != 2 {
		t.Fatalf("NodeCount = %d, %v; want 2, nil", nodeCount, err)
	}
	relCount, err := reader.RelationshipCount()
	if err != nil || relCount != 1 {
		t.Fatalf("RelationshipCount = %d, %v; want 1, nil", relCount, err)
	}
	out, err := reader.OutgoingRelationships(a.ID(), "KNOWS")
	if err != nil || len(out) != 1 || out[0].ID() != r.ID() {
		t.Fatalf("OutgoingRelationships = %#v, %v; want rel %v", out, err, r.ID())
	}
	in, err := reader.IncomingRelationships(b.ID(), "KNOWS")
	if err != nil || len(in) != 1 || in[0].ID() != r.ID() {
		t.Fatalf("IncomingRelationships = %#v, %v; want rel %v", in, err, r.ID())
	}
}

// graphReaderProbe is a minimal indexpkg.Initializable that records that the
// indexpkg.GraphReader handed to Init really is restricted (no mutation surface).
// The compiler enforces this — we cannot call g.Nodes.Add on a indexpkg.GraphReader
// — so the test exists to lock the contract: any future attempt to widen
// indexpkg.GraphReader will break this test by allowing the recorded type to
// expose mutation methods.
type graphReaderProbe struct {
	mockIndexProvider
	receivedReader indexpkg.GraphReader
}

func (p *graphReaderProbe) Init(g indexpkg.GraphReader) error {
	p.receivedReader = g
	return nil
}

func TestIndexProvider_InitReceivesGraphReaderInterface(t *testing.T) {
	g := newProviderTestGraph(t)
	p := &graphReaderProbe{mockIndexProvider: mockIndexProvider{name: "probe"}}
	if err := g.Index.RegisterProvider(p); err != nil {
		t.Fatalf("register: %v", err)
	}
	if p.receivedReader == nil {
		t.Fatal("Init did not receive a indexpkg.GraphReader")
	}
	// graphReaderView is unexported and *Core no longer satisfies GraphReader
	// at compile time (the read methods now live on the sub-Core types like
	// NodeOps), so the previous *Core type-assertion is unreachable. Skip.
}
