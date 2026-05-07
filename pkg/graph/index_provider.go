package graph

import (
	"errors"
	"fmt"
	"sort"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// IndexProvider is the contract for an auxiliary index that lives outside
// Store's built-in index types (property, temporal, high-frequency, vector)
// and maintains its own structures by reacting to graph lifecycle events.
//
// Unlike Store-embedded indexes, IndexProviders plug in from outside the
// graph package and are not persisted or queried through Store. The graph
// forwards lifecycle events; persistence, query routing, and threading are
// the provider's responsibility. This is the extension point used by
// tkgd's spatial R-tree.
//
// The Phase 6 redesign narrows the surface area in three ways relative to
// LegacyIndexProvider:
//
//  1. OnEvent no longer receives *Graph. Providers that need to read entity
//     data should keep a GraphReader handed to them via Init (see
//     Initializable). This prevents providers from mutating the graph or
//     registering more providers from inside an event handler.
//  2. OnEvent returns an error so dispatch loops can surface diagnostic
//     information. The graph currently logs errors but does not abort the
//     mutation that produced the event — semantics match the "best effort
//     auxiliary index" model.
//  3. Providers may optionally implement Initializable to receive a
//     bulk-load callback at registration time, removing the need to seed
//     the index out-of-band before wiring in events.
//
// Providers run in-process. When the attached publisher is the synchronous
// EventBus, OnEvent runs inline with the originating mutation goroutine.
// When the attached publisher is AsyncEventBus, OnEvent runs on a worker
// goroutine. In either case, long-running work should be dispatched to
// the provider's own goroutine pool — blocking on a sync bus stalls the
// caller; saturating an async bus produces backpressure.
type IndexProvider interface {
	// Name is a unique identifier for admin and registry listing. Must be
	// non-empty and stable across the provider's lifetime; the graph uses
	// it as the registration key and rejects duplicates with
	// ErrIndexProviderExists.
	Name() string

	// OnEvent receives lifecycle events after the mutation commits. The
	// Event carries only a types.EntityID and event Type — providers that
	// need full entity state should fetch via the GraphReader handed to
	// them through Init (see Initializable).
	//
	// Returning a non-nil error does NOT abort the originating mutation
	// (the event has already committed). The error is captured for
	// diagnostic purposes only. Implementations MUST NOT panic — a panic
	// is caught by the bus' safeInvoke but generates an error log entry.
	OnEvent(ev Event) error

	// Close releases resources held by the provider. Called from
	// UnregisterIndexProvider and from Graph.Close. Returning an error
	// does not block shutdown — the graph still closes the store.
	Close() error
}

// Initializable is an optional extension implemented by IndexProviders
// that need to populate themselves from the existing graph state at
// registration time. When a provider also satisfies Initializable, the
// graph calls Init synchronously after wiring the provider into the event
// stream.
//
// Init is called AFTER Subscribe returns, so any event fired during Init
// may or may not be observed by the provider depending on goroutine
// interleaving. Implementations should design their bulk-load to be
// idempotent (re-applying the same event has no effect) or guard via
// their own version/state machinery.
//
// If Init returns an error, the provider is unsubscribed and removed from
// the registry; RegisterIndexProvider returns the Init error.
//
// Implementations should not retain references to *Graph — accept only
// the GraphReader supplied here.
type Initializable interface {
	Init(g GraphReader) error
}

// LegacyIndexProvider is the pre-Phase-6 IndexProvider shape, retained for
// external callers (notably tkgd's spatial R-tree) whose providers still
// take *Graph in OnEvent. New providers should implement IndexProvider
// (and optionally Initializable) instead.
//
// Existing implementations remain bit-for-bit source-compatible — the
// only change required is to call RegisterLegacyIndexProvider in place
// of RegisterIndexProvider at the registration site.
//
// Deprecated: Use IndexProvider (the redesigned, narrowed shape) and
// Initializable for bulk-load. LegacyIndexProvider will be removed in a
// future major version.
type LegacyIndexProvider interface {
	Name() string
	OnEvent(ev Event, g *Graph)
	Close() error
}

// GraphReader is the read-only Graph surface exposed to IndexProviders.
// The interface deliberately omits every mutation, every registration
// helper, and Close — providers that only need to read a node by ID can
// receive a GraphReader instead of *Graph and lose the ability to
// accidentally mutate the graph from inside an event handler or an Init
// callback.
//
// Method signatures intentionally mirror the corresponding *Graph methods
// (lookup-by-string is convenient for providers that index over labels or
// relationship types). All returned slices are owned by the caller and
// may be modified freely.
type GraphReader interface {
	GetNode(id types.NodeID) (*types.Node, error)
	GetRelationship(id types.RelID) (*types.Relationship, error)
	NodesByLabel(label string, opts QueryOpts) ([]*types.Node, error)
	RelationshipsByType(typeName string, opts QueryOpts) ([]*types.Relationship, error)
	AllNodes(opts QueryOpts) ([]*types.Node, error)
	AllRelationships(opts QueryOpts) ([]*types.Relationship, error)
	NodeCount() (int, error)
	RelationshipCount() (int, error)
	OutgoingRelationships(nodeID types.NodeID, typeName string) ([]*types.Relationship, error)
	IncomingRelationships(nodeID types.NodeID, typeName string) ([]*types.Relationship, error)
}

// graphReaderView wraps *Graph as a GraphReader. The wrapper exists only
// to ensure providers cannot type-assert their way back to *Graph and
// reach the mutation surface — graphReaderView is unexported, so the
// interface is the only handle a provider sees.
type graphReaderView struct{ g *Graph }

func (r graphReaderView) GetNode(id types.NodeID) (*types.Node, error) {
	return r.g.GetNode(id)
}

func (r graphReaderView) GetRelationship(id types.RelID) (*types.Relationship, error) {
	return r.g.GetRelationship(id)
}

func (r graphReaderView) NodesByLabel(label string, opts QueryOpts) ([]*types.Node, error) {
	return r.g.NodesByLabel(label, opts)
}

func (r graphReaderView) RelationshipsByType(typeName string, opts QueryOpts) ([]*types.Relationship, error) {
	return r.g.RelationshipsByType(typeName, opts)
}

func (r graphReaderView) AllNodes(opts QueryOpts) ([]*types.Node, error) {
	return r.g.AllNodes(opts)
}

func (r graphReaderView) AllRelationships(opts QueryOpts) ([]*types.Relationship, error) {
	return r.g.AllRelationships(opts)
}

func (r graphReaderView) NodeCount() (int, error)         { return r.g.NodeCount() }
func (r graphReaderView) RelationshipCount() (int, error) { return r.g.RelationshipCount() }

func (r graphReaderView) OutgoingRelationships(nodeID types.NodeID, typeName string) ([]*types.Relationship, error) {
	return r.g.OutgoingRelationships(nodeID, typeName)
}

func (r graphReaderView) IncomingRelationships(nodeID types.NodeID, typeName string) ([]*types.Relationship, error) {
	return r.g.IncomingRelationships(nodeID, typeName)
}

// legacyAdapter wraps a LegacyIndexProvider so it can be stored uniformly
// alongside new-shape IndexProviders. Forwards OnEvent(ev) to the legacy
// provider's OnEvent(ev, g) and discards the return value.
//
// The adapter holds a *Graph reference captured at registration time —
// legacy providers received unrestricted *Graph access by design, so the
// adapter preserves that. New IndexProvider implementations receive only
// a GraphReader via Init.
type legacyAdapter struct {
	legacy LegacyIndexProvider
	g      *Graph
}

func (a *legacyAdapter) Name() string { return a.legacy.Name() }

func (a *legacyAdapter) OnEvent(ev Event) error {
	a.legacy.OnEvent(ev, a.g)
	return nil
}

func (a *legacyAdapter) Close() error { return a.legacy.Close() }

// indexProviderEntry bundles a registered provider with the unsubscribe
// closure returned by Subscribe. Stored in Graph.indexProviders under the
// provider's Name. The provider field is always the new IndexProvider
// shape — legacy providers are wrapped in legacyAdapter at registration.
type indexProviderEntry struct {
	provider    IndexProvider
	unsubscribe func()
}

// ErrIndexProviderExists is returned by RegisterIndexProvider /
// RegisterLegacyIndexProvider when a provider with the same Name is
// already registered.
var ErrIndexProviderExists = errors.New("graph: index provider already registered")

// ErrIndexProviderNotFound is returned by UnregisterIndexProvider when no
// provider with the given name is registered.
var ErrIndexProviderNotFound = errors.New("graph: index provider not found")

// ErrIndexProviderEmptyName is returned by RegisterIndexProvider /
// RegisterLegacyIndexProvider when the provider's Name() is the empty
// string. Names are the registry key.
var ErrIndexProviderEmptyName = errors.New("graph: index provider Name must be non-empty")

// subscribeFunc is the abstraction over EventBus.Subscribe and
// AsyncEventBus.Subscribe so registerProvider can support both publishers
// without type-switching at every call site.
type subscribeFunc func(EventHandler) func()

// resolveSubscribeLocked returns a subscribe function that wires a handler
// into the currently attached publisher, auto-creating a synchronous
// EventBus if none is attached. Caller must hold g.mu.Lock.
//
// The returned subscribe func captures the bus reference at call time, so
// later calls to SetEventBus / SetAsyncEventBus do not affect already
// subscribed providers. This matches the behaviour external callers
// observed before the redesign.
func (g *Graph) resolveSubscribeLocked() (subscribeFunc, error) {
	if g.events == nil {
		g.events = NewEventBus()
	}
	switch bus := g.events.(type) {
	case *EventBus:
		return bus.Subscribe, nil
	case *AsyncEventBus:
		return bus.Subscribe, nil
	default:
		// Unknown publisher implementation — surface rather than silently miss events.
		return nil, fmt.Errorf("graph: attached event publisher %T does not support Subscribe", g.events)
	}
}

// RegisterIndexProvider wires p into the graph's event dispatch.
//
// If no event publisher is attached, a synchronous EventBus is created and
// attached so the provider receives events. External callers who already
// manage an AsyncEventBus or a pre-existing EventBus should attach it
// (SetAsyncEventBus / SetEventBus) before registering — both publisher
// types are supported.
//
// If p also implements Initializable, Init is called synchronously after
// the provider is wired into the event stream. A non-nil Init error
// causes the subscription to be torn down and the provider to be removed
// from the registry; the error is returned to the caller.
//
// Returns ErrIndexProviderExists on duplicate Name and
// ErrIndexProviderEmptyName on empty Name.
//
// All registry mutations (dup check, auto-bus creation, Subscribe, entry
// insertion) happen under a single g.mu.Lock() critical section.
// Subscribe is non-reentrant w.r.t. graph mutations (it just appends to
// an internal handler slice under the bus' own mu), so holding g.mu
// through it is deadlock-safe and closes a TOCTOU race where two
// concurrent Register("name") calls could both pass the dup check and
// produce orphaned subscriptions.
//
// Init runs OUTSIDE g.mu so the bulk-load callback may freely call back
// into the GraphReader without re-entering the lock. Events fired
// concurrently with Init may or may not be observed by the provider —
// see Initializable for the recommended idempotency pattern.
func (g *Graph) RegisterIndexProvider(p IndexProvider) error {
	if p == nil {
		return fmt.Errorf("graph: index provider is nil")
	}
	name := p.Name()
	if name == "" {
		return ErrIndexProviderEmptyName
	}

	g.mu.Lock()
	if _, exists := g.indexProviders[name]; exists {
		g.mu.Unlock()
		return fmt.Errorf("%w: %q", ErrIndexProviderExists, name)
	}
	subscribe, err := g.resolveSubscribeLocked()
	if err != nil {
		g.mu.Unlock()
		return err
	}
	unsub := subscribe(func(ev Event) {
		// OnEvent errors are best-effort diagnostics; we deliberately do
		// not surface them to the originating mutation goroutine because
		// the mutation has already committed.
		_ = p.OnEvent(ev)
	})
	g.indexProviders[name] = &indexProviderEntry{provider: p, unsubscribe: unsub}
	g.mu.Unlock()

	if init, ok := p.(Initializable); ok {
		if err := init.Init(graphReaderView{g: g}); err != nil {
			// Roll back the registration so the caller does not have to
			// worry about a half-wired provider observing future events.
			g.mu.Lock()
			if entry, present := g.indexProviders[name]; present && entry.provider == p {
				delete(g.indexProviders, name)
				g.mu.Unlock()
				entry.unsubscribe()
			} else {
				g.mu.Unlock()
			}
			return fmt.Errorf("graph: index provider %q Init failed: %w", name, err)
		}
	}
	return nil
}

// RegisterLegacyIndexProvider registers a provider that uses the legacy
// OnEvent(Event, *Graph) shape. Internally the provider is wrapped in an
// adapter that satisfies the new IndexProvider interface.
//
// All semantics (auto-bus creation, sync/async support, duplicate-name
// rejection, race safety) match RegisterIndexProvider. Legacy providers
// cannot use Initializable — the adapter does not implement it because
// the old API never had a bulk-load hook. Callers needing bulk-load
// should migrate to the new IndexProvider interface.
//
// Deprecated: Migrate providers to IndexProvider (and optionally
// Initializable). This entry point will be removed in a future major
// version.
func (g *Graph) RegisterLegacyIndexProvider(p LegacyIndexProvider) error {
	if p == nil {
		return fmt.Errorf("graph: index provider is nil")
	}
	return g.RegisterIndexProvider(&legacyAdapter{legacy: p, g: g})
}

// UnregisterIndexProvider detaches the provider by name and calls its
// Close. Returns ErrIndexProviderNotFound if not registered. Close errors
// are returned; the provider is removed from the registry either way.
func (g *Graph) UnregisterIndexProvider(name string) error {
	g.mu.Lock()
	entry, ok := g.indexProviders[name]
	if !ok {
		g.mu.Unlock()
		return fmt.Errorf("%w: %q", ErrIndexProviderNotFound, name)
	}
	delete(g.indexProviders, name)
	g.mu.Unlock()

	entry.unsubscribe()
	return entry.provider.Close()
}

// IndexProviders returns the names of registered providers in
// lexicographic order. Stable ordering helps with admin UIs and snapshot
// tests.
func (g *Graph) IndexProviders() []string {
	g.mu.RLock()
	out := make([]string, 0, len(g.indexProviders))
	for name := range g.indexProviders {
		out = append(out, name)
	}
	g.mu.RUnlock()
	sort.Strings(out)
	return out
}

// closeIndexProviders closes all registered providers and clears the
// registry. Called from Graph.Close before the store is closed. Errors
// from individual Close calls are joined; all providers are attempted
// regardless of earlier failures.
func (g *Graph) closeIndexProviders() error {
	g.mu.Lock()
	entries := make([]*indexProviderEntry, 0, len(g.indexProviders))
	for _, e := range g.indexProviders {
		entries = append(entries, e)
	}
	g.indexProviders = make(map[string]*indexProviderEntry)
	g.mu.Unlock()

	var err error
	for _, e := range entries {
		e.unsubscribe()
		if cerr := e.provider.Close(); cerr != nil {
			err = errors.Join(err, fmt.Errorf("index provider %q close: %w", e.provider.Name(), cerr))
		}
	}
	return err
}
