// Package index defines the public extension surface for auxiliary indexes
// (e.g. tkgd's spatial R-tree) that live outside the graph's built-in
// property/temporal/high-frequency/vector indexes and maintain their own
// structures by reacting to graph lifecycle events.
//
// The package contains only the contract types (interfaces and sentinel
// errors). The Graph-coupled registration and dispatch wiring lives on
// the graph index sub-API (g.Index.RegisterProvider,
// g.Index.UnregisterProvider, g.Index.Providers).
package index

import (
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/events"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
)

// IndexProvider is the contract for an auxiliary index that lives outside
// Store's built-in index types (property, temporal, high-frequency, vector)
// and maintains its own structures by reacting to graph lifecycle events.
//
// Unlike Store-embedded indexes, IndexProviders plug in from outside the
// graph package and are not persisted or queried through Store. The graph
// forwards lifecycle events; providers supply their own persistence, query
// routing, and threading. This is the extension point used by
// tkgd's spatial R-tree.
//
// Design constraints:
//
//  1. OnEvent does NOT receive *Graph. Providers that need to read entity
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
// events.EventBus, OnEvent runs inline with the originating mutation
// goroutine. When the attached publisher is events.AsyncEventBus, OnEvent
// runs on a worker goroutine. In either case, long-running work should be
// dispatched to the provider's own goroutine pool — blocking on a sync bus
// stalls the caller; saturating an async bus produces backpressure.
type IndexProvider interface {
	// Name is a unique identifier for admin and registry listing. Must be
	// non-empty and stable across the provider's lifetime; the graph uses
	// it as the registration key and rejects duplicates with
	// ErrIndexProviderExists.
	Name() string

	// OnEvent receives lifecycle events after the mutation commits. The
	// events.Event carries only a types.EntityID and event Type — providers
	// that need full entity state should fetch via the GraphReader handed
	// to them through Init (see Initializable).
	//
	// Returning a non-nil error does NOT abort the originating mutation
	// (the event has already committed). The error is captured for
	// diagnostic purposes only. Implementations MUST NOT panic — a panic
	// is caught by the bus' safeInvoke but generates an error log entry.
	OnEvent(ev events.Event) error

	// Close releases resources held by the provider. Called from
	// g.Index.UnregisterProvider and from Graph.Close. Returning an error
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
// the registry; g.Index.RegisterProvider returns the Init error.
// Graph.Close and g.Index.UnregisterProvider wait for an in-flight Init
// callback to finish before calling provider Close.
//
// Implementations should not retain references to *Graph — accept only
// the GraphReader supplied here.
type Initializable interface {
	Init(g GraphReader) error
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
	NodesByLabel(label string, opts storepkg.QueryOpts) ([]*types.Node, error)
	RelationshipsByType(typeName string, opts storepkg.QueryOpts) ([]*types.Relationship, error)
	AllNodes(opts storepkg.QueryOpts) ([]*types.Node, error)
	AllRelationships(opts storepkg.QueryOpts) ([]*types.Relationship, error)
	NodeCount() (int, error)
	RelationshipCount() (int, error)
	OutgoingRelationships(nodeID types.NodeID, typeName string) ([]*types.Relationship, error)
	IncomingRelationships(nodeID types.NodeID, typeName string) ([]*types.Relationship, error)
}
