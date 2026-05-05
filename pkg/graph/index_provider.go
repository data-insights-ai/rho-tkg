package graph

import (
	"errors"
	"fmt"
	"sort"
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
// Providers run in-process. OnEvent is called synchronously in the same
// goroutine as the originating mutation (via the attached EventBus). For
// long-running work, dispatch to the provider's own goroutine pool.
type IndexProvider interface {
	// Name is a unique identifier for admin and registry listing. Must be
	// non-empty and stable across the provider's lifetime; the graph uses
	// it as the registration key and panics on duplicate.
	Name() string

	// OnEvent receives lifecycle events after the mutation commits. The
	// Event carries only a types.EntityID; implementations that need entity
	// properties should fetch via g.GetNode(types.NodeID(ev.EntityID)) for
	// node events or g.GetRelationship(types.RelID(ev.EntityID)) for rel
	// events (the Event.Type discriminates the kind).
	// Implementations MUST NOT panic — a panic is caught by the event bus'
	// safeInvoke but will produce an error log entry.
	OnEvent(ev Event, g *Graph)

	// Close releases resources held by the provider. Called from
	// UnregisterIndexProvider and from Graph.Close. Returning an error
	// does not block shutdown — the graph still closes the store.
	Close() error
}

// indexProviderEntry bundles a registered provider with the unsubscribe
// closure returned by EventBus.Subscribe. Stored in Graph.indexProviders
// under the provider's Name.
type indexProviderEntry struct {
	provider    IndexProvider
	unsubscribe func()
}

// ErrIndexProviderExists is returned by RegisterIndexProvider when a
// provider with the same Name is already registered.
var ErrIndexProviderExists = errors.New("graph: index provider already registered")

// ErrIndexProviderNotFound is returned by UnregisterIndexProvider when no
// provider with the given name is registered.
var ErrIndexProviderNotFound = errors.New("graph: index provider not found")

// ErrIndexProviderEmptyName is returned by RegisterIndexProvider when the
// provider's Name() is the empty string. Names are the registry key.
var ErrIndexProviderEmptyName = errors.New("graph: index provider Name must be non-empty")

// RegisterIndexProvider wires p into the graph's event dispatch. If no
// EventBus is attached, a synchronous EventBus is created and attached so
// the provider receives events. External callers who already manage an
// AsyncEventBus should attach it before registering providers — this
// method will not overwrite an existing publisher.
//
// Returns ErrIndexProviderExists on duplicate Name, ErrIndexProviderEmptyName
// on empty Name. p's OnEvent is called synchronously per mutation; do not
// block inside it for extended durations.
//
// All registry mutations (dup check, auto-bus creation, type assertion,
// Subscribe, entry insertion) happen under a single g.mu.Lock() critical
// section. EventBus.Subscribe is non-reentrant w.r.t. graph mutations
// (it just appends to an internal handler slice under bus.mu), so holding
// g.mu through it is deadlock-safe and closes a TOCTOU race where two
// concurrent Register("name") calls could both pass the dup check and
// produce orphaned subscriptions.
func (g *Graph) RegisterIndexProvider(p IndexProvider) error {
	if p == nil {
		return fmt.Errorf("graph: index provider is nil")
	}
	name := p.Name()
	if name == "" {
		return ErrIndexProviderEmptyName
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	if _, exists := g.indexProviders[name]; exists {
		return fmt.Errorf("%w: %q", ErrIndexProviderExists, name)
	}
	// Auto-create a synchronous EventBus if none is attached. Callers who
	// prefer async dispatch should SetAsyncEventBus before registering.
	if g.events == nil {
		g.events = NewEventBus()
	}
	bus, ok := g.events.(*EventBus)
	if !ok {
		// An AsyncEventBus (or other eventPublisher) is attached. Index
		// providers need Subscribe, which only the sync EventBus currently
		// exposes. Surface this rather than silently missing events.
		return fmt.Errorf(
			"graph: index provider %q requires a synchronous EventBus; "+
				"attach one via SetEventBus before registering, or use the async "+
				"bus's Subscribe API directly",
			name,
		)
	}

	unsub := bus.Subscribe(func(ev Event) {
		p.OnEvent(ev, g)
	})
	g.indexProviders[name] = &indexProviderEntry{provider: p, unsubscribe: unsub}
	return nil
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
