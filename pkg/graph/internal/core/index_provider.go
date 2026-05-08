package core

import (
	"errors"
	"fmt"
	"sort"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"

	eventspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/events"
	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/index"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// graphReaderView wraps *Core as an indexpkg.GraphReader. The wrapper
// exists only to ensure providers cannot type-assert their way back to
// *Core and reach the mutation surface — graphReaderView is unexported,
// so the interface is the only handle a provider sees.
type graphReaderView struct{ g *Core }

func (r graphReaderView) GetNode(id types.NodeID) (*types.Node, error) {
	return r.g.Nodes.Get(id)
}

func (r graphReaderView) GetRelationship(id types.RelID) (*types.Relationship, error) {
	return r.g.Rels.Get(id)
}

func (r graphReaderView) NodesByLabel(label string, opts storepkg.QueryOpts) ([]*types.Node, error) {
	return r.g.Nodes.ByLabel(label, opts)
}

func (r graphReaderView) RelationshipsByType(typeName string, opts storepkg.QueryOpts) ([]*types.Relationship, error) {
	return r.g.Rels.ByType(typeName, opts)
}

func (r graphReaderView) AllNodes(opts storepkg.QueryOpts) ([]*types.Node, error) {
	return r.g.Nodes.All(opts)
}

func (r graphReaderView) AllRelationships(opts storepkg.QueryOpts) ([]*types.Relationship, error) {
	return r.g.Rels.All(opts)
}

func (r graphReaderView) NodeCount() (int, error)         { return r.g.Nodes.Count() }
func (r graphReaderView) RelationshipCount() (int, error) { return r.g.Rels.Count() }

func (r graphReaderView) OutgoingRelationships(nodeID types.NodeID, typeName string) ([]*types.Relationship, error) {
	return r.g.Rels.Outgoing(nodeID, typeName)
}

func (r graphReaderView) IncomingRelationships(nodeID types.NodeID, typeName string) ([]*types.Relationship, error) {
	return r.g.Rels.Incoming(nodeID, typeName)
}

// legacyAdapter wraps an indexpkg.LegacyIndexProvider so it can be stored
// uniformly alongside new-shape IndexProviders. Forwards OnEvent(ev) to
// the legacy provider's OnEvent(ev, g) where g is a GraphReader.
type legacyAdapter struct {
	legacy indexpkg.LegacyIndexProvider //nolint:staticcheck // intentional bridge for the deprecated provider shape
	reader indexpkg.GraphReader
}

func (a *legacyAdapter) Name() string { return a.legacy.Name() }

func (a *legacyAdapter) OnEvent(ev eventspkg.Event) error {
	a.legacy.OnEvent(ev, a.reader)
	return nil
}

func (a *legacyAdapter) Close() error { return a.legacy.Close() }

// indexProviderEntry bundles a registered provider with the unsubscribe
// closure returned by Subscribe. Stored in Graph.indexProviders under the
// provider's Name. The provider field is always the new IndexProvider
// shape — legacy providers are wrapped in legacyAdapter at registration.
type indexProviderEntry struct {
	provider    indexpkg.IndexProvider
	unsubscribe func()
}

// subscribeFunc is the abstraction over eventspkg.EventBus.Subscribe and
// eventspkg.AsyncEventBus.Subscribe so registerProvider can support both
// publishers without type-switching at every call site.
type subscribeFunc func(eventspkg.EventHandler) func()

// resolveSubscribeLocked returns a subscribe function that wires a handler
// into the currently attached publisher, auto-creating a synchronous
// eventspkg.EventBus if none is attached. Caller must hold c.mu.Lock.
//
// The returned subscribe func captures the bus reference at call time, so
// later calls to Events.SetSync / Events.SetAsync do not affect already
// subscribed providers. This matches the behaviour external callers
// observed before the redesign.
func (c *Core) resolveSubscribeLocked() (subscribeFunc, error) {
	if c.events == nil {
		c.events = eventspkg.NewEventBus()
	}
	switch bus := c.events.(type) {
	case *eventspkg.EventBus:
		return bus.Subscribe, nil
	case *eventspkg.AsyncEventBus:
		return bus.Subscribe, nil
	default:
		// Unknown publisher implementation — surface rather than silently miss events.
		return nil, fmt.Errorf("graph: attached event publisher %T does not support Subscribe", c.events)
	}
}

// RegisterProvider wires p into the graph's event dispatch.
//
// If no event publisher is attached, a synchronous eventspkg.EventBus is
// created and attached so the provider receives events. External callers
// who already manage an eventspkg.AsyncEventBus or a pre-existing
// eventspkg.EventBus should attach it (Events.SetAsync / Events.SetSync)
// before registering — both publisher types are supported.
//
// If p also implements indexpkg.Initializable, Init is called synchronously
// after the provider is wired into the event stream. A non-nil Init error
// causes the subscription to be torn down and the provider to be removed
// from the registry; the error is returned to the caller.
//
// Returns indexpkg.ErrIndexProviderExists on duplicate Name and
// indexpkg.ErrIndexProviderEmptyName on empty Name.
//
// All registry mutations (dup check, auto-bus creation, Subscribe, entry
// insertion) happen under a single c.mu.Lock() critical section.
// Subscribe is non-reentrant w.r.t. graph mutations (it just appends to
// an internal handler slice under the bus' own mu), so holding c.mu
// through it is deadlock-safe and closes a TOCTOU race where two
// concurrent Register("name") calls could both pass the dup check and
// produce orphaned subscriptions.
//
// RegisterProvider runs OUTSIDE c.mu so the bulk-load callback may freely call back
// into the GraphReader without re-entering the lock. Events fired
// concurrently with Init may or may not be observed by the provider —
// see indexpkg.Initializable for the recommended idempotency pattern.
func (i *IndexOps) RegisterProvider(p indexpkg.IndexProvider) error {
	c := i.c
	if p == nil {
		return fmt.Errorf("graph: index provider is nil")
	}
	name := p.Name()
	if name == "" {
		return indexpkg.ErrIndexProviderEmptyName
	}

	c.mu.Lock()
	if _, exists := c.indexProviders[name]; exists {
		c.mu.Unlock()
		return fmt.Errorf("%w: %q", indexpkg.ErrIndexProviderExists, name)
	}
	subscribe, err := c.resolveSubscribeLocked()
	if err != nil {
		c.mu.Unlock()
		return err
	}
	unsub := subscribe(func(ev eventspkg.Event) {
		// OnEvent errors are best-effort diagnostics; we deliberately do
		// not surface them to the originating mutation goroutine because
		// the mutation has already committed.
		_ = p.OnEvent(ev)
	})
	c.indexProviders[name] = &indexProviderEntry{provider: p, unsubscribe: unsub}
	c.mu.Unlock()

	if init, ok := p.(indexpkg.Initializable); ok {
		if err := init.Init(graphReaderView{g: c}); err != nil {
			// Roll back the registration so the caller does not have to
			// worry about a half-wired provider observing future events.
			c.mu.Lock()
			if entry, present := c.indexProviders[name]; present && entry.provider == p {
				delete(c.indexProviders, name)
				c.mu.Unlock()
				entry.unsubscribe()
			} else {
				c.mu.Unlock()
			}
			return fmt.Errorf("graph: index provider %q Init failed: %w", name, err)
		}
	}
	return nil
}

// RegisterLegacyProvider registers a provider that uses the legacy
// OnEvent(eventspkg.Event, indexpkg.GraphReader) shape. Internally the
// provider is wrapped in an adapter that satisfies the new IndexProvider
// interface.
//
// All semantics (auto-bus creation, sync/async support, duplicate-name
// rejection, race safety) match IndexOps.RegisterProvider. Legacy providers
// cannot use Initializable — the adapter does not implement it because
// the old API never had a bulk-load hook. Callers needing bulk-load
// should migrate to the new IndexProvider interface.
//
// Deprecated: Migrate providers to indexpkg.IndexProvider (and optionally
// Initializable). This entry point will be removed in a future major
// version.
func (i *IndexOps) RegisterLegacyProvider(p indexpkg.LegacyIndexProvider) error {
	c := i.c
	if p == nil {
		return fmt.Errorf("graph: index provider is nil")
	}
	return i.RegisterProvider(&legacyAdapter{legacy: p, reader: graphReaderView{g: c}})
}

// UnregisterProvider detaches the provider by name and calls its
// Close. Returns indexpkg.ErrIndexProviderNotFound if not registered.
// Close errors are returned; the provider is removed from the registry
// either way.
func (i *IndexOps) UnregisterProvider(name string) error {
	c := i.c
	c.mu.Lock()
	entry, ok := c.indexProviders[name]
	if !ok {
		c.mu.Unlock()
		return fmt.Errorf("%w: %q", indexpkg.ErrIndexProviderNotFound, name)
	}
	delete(c.indexProviders, name)
	c.mu.Unlock()

	entry.unsubscribe()
	return entry.provider.Close()
}

// Providers returns the names of registered providers in
// lexicographic order. Stable ordering helps with admin UIs and snapshot
// tests.
func (i *IndexOps) Providers() []string {
	c := i.c
	c.mu.RLock()
	out := make([]string, 0, len(c.indexProviders))
	for name := range c.indexProviders {
		out = append(out, name)
	}
	c.mu.RUnlock()
	sort.Strings(out)
	return out
}

// closeIndexProviders closes all registered providers and clears the
// registry. Called from Graph.Close before the store is closed. Errors
// from individual Close calls are joined; all providers are attempted
// regardless of earlier failures.
func (c *Core) closeIndexProviders() error {
	c.mu.Lock()
	entries := make([]*indexProviderEntry, 0, len(c.indexProviders))
	for _, e := range c.indexProviders {
		entries = append(entries, e)
	}
	c.indexProviders = make(map[string]*indexProviderEntry)
	c.mu.Unlock()

	var err error
	for _, e := range entries {
		e.unsubscribe()
		if cerr := e.provider.Close(); cerr != nil {
			err = errors.Join(err, fmt.Errorf("index provider %q close: %w", e.provider.Name(), cerr))
		}
	}
	return err
}
