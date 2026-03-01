package graph

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3/pkg/types"
)

// Sentinel errors for entity management.
var (
	ErrNoLabels = errors.New("graph: node requires at least one label")
	ErrNilNode  = errors.New("graph: node must not be nil")
)

// snowflakeEpoch is the custom epoch for all snowflake ID generation (2026-01-01 UTC).
var snowflakeEpoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// Config holds configuration for the Graph.
type Config struct {
	// SnowflakeNodeID identifies this graph instance (0-511).
	// Internally mapped to even/odd generator pair (nodeGen=ID*2, relGen=ID*2+1)
	// to guarantee value-level uniqueness across node and relationship IDs.
	// Each concurrent instance must use a different value.
	SnowflakeNodeID int64

	// Store is the persistence backend. If nil, NewMemoryStore() is used
	// unless BadgerDir or BadgerInMemory is set.
	Store Store

	// BadgerDir is the Badger data directory. If set and Store is nil,
	// a BadgerStore is created. Ignored if Store is non-nil.
	BadgerDir string

	// BadgerInMemory enables in-memory Badger mode (useful for testing).
	// If true and Store is nil, a BadgerStore with InMemory=true is created.
	BadgerInMemory bool
}

// Graph is the central graph layer. It owns the label and relationship type
// registries, snowflake ID generators, store, and provides string resolution
// for token-based entities.
//
// Entity locks serialize AddRelationship and DeleteNode on overlapping entities
// to prevent write-skew (concurrent AddRelationship(→X) + DeleteNodeCascade(X)
// producing a dangling edge).
type Graph struct {
	labels      *labelRegistry
	relTypes    *relTypeRegistry
	nodeIDGen   *snowflake.Node
	relIDGen    *snowflake.Node
	store       Store
	entityLocks *entityLockManager
	closeOnce   sync.Once
}

// New creates a new Graph with the given configuration.
// Returns an error if SnowflakeNodeID is out of range (0-511).
// The ID is mapped to an even/odd pair (ID*2 for nodes, ID*2+1 for rels)
// to guarantee value-level uniqueness across entity types.
//
// Store selection priority:
//  1. config.Store (explicit injection)
//  2. BadgerStore (if BadgerDir or BadgerInMemory is set)
//  3. MemoryStore (default)
//
// When a BadgerStore is created, registries are loaded from persisted data.
// Call Close() when done to save registries and close the store.
func New(config Config) (*Graph, error) {
	if config.SnowflakeNodeID < 0 || config.SnowflakeNodeID > 511 {
		return nil, fmt.Errorf("graph: SnowflakeNodeID must be 0-511, got %d", config.SnowflakeNodeID)
	}

	nodeGen, err := snowflake.NewNode(config.SnowflakeNodeID*2,
		snowflake.WithEpoch(snowflakeEpoch),
		snowflake.WithNodeBits(10),
		snowflake.WithStepBits(12),
	)
	if err != nil {
		return nil, fmt.Errorf("graph: node ID generator: %w", err)
	}
	relGen, err := snowflake.NewNode(config.SnowflakeNodeID*2+1,
		snowflake.WithEpoch(snowflakeEpoch),
		snowflake.WithNodeBits(10),
		snowflake.WithStepBits(12),
	)
	if err != nil {
		return nil, fmt.Errorf("graph: rel ID generator: %w", err)
	}

	g := &Graph{
		labels:      newLabelRegistry(),
		relTypes:    newRelTypeRegistry(),
		nodeIDGen:   nodeGen,
		relIDGen:    relGen,
		entityLocks: newEntityLockManager(),
	}

	// Validate BadgerDir: reject whitespace-only strings (silent fallback hazard).
	if config.Store == nil && config.BadgerDir != "" {
		if strings.TrimSpace(config.BadgerDir) == "" {
			return nil, fmt.Errorf("graph: BadgerDir is whitespace-only; use a valid path or omit for MemoryStore")
		}
	}

	store := config.Store
	if store == nil {
		if config.BadgerDir != "" || config.BadgerInMemory {
			bs, err := NewBadgerStore(BadgerStoreConfig{
				Dir:      config.BadgerDir,
				InMemory: config.BadgerInMemory,
			})
			if err != nil {
				return nil, fmt.Errorf("graph: badger store: %w", err)
			}

			// Load persisted registries. Fail fast if the saved data is corrupt.
			if _, err := bs.LoadLabelRegistry(g.labels); err != nil {
				_ = bs.Close() // best-effort cleanup; returning primary error
				return nil, fmt.Errorf("graph: load label registry: %w", err)
			}
			if _, err := bs.LoadRelTypeRegistry(g.relTypes); err != nil {
				_ = bs.Close() // best-effort cleanup; returning primary error
				return nil, fmt.Errorf("graph: load reltype registry: %w", err)
			}

			store = bs
		} else {
			store = NewMemoryStore()
		}
	}

	g.store = store
	return g, nil
}

// Close saves registries (if Badger) and closes the underlying store.
// Safe to call concurrently and multiple times.
//
// store.Close() always runs even if registry saves fail — prevents resource leaks.
// Returns all errors joined; subsequent calls return nil.
func (g *Graph) Close() error {
	var closeErr error
	g.closeOnce.Do(func() {
		// Save registries if the store supports it (Badger-specific).
		if bs, ok := g.store.(*BadgerStore); ok {
			if err := bs.SaveLabelRegistry(g.labels); err != nil {
				closeErr = fmt.Errorf("graph: save label registry: %w", err)
			}
			if err := bs.SaveRelTypeRegistry(g.relTypes); err != nil {
				closeErr = errors.Join(closeErr, fmt.Errorf("graph: save reltype registry: %w", err))
			}
		}
		// Always close the store — even if registry saves failed.
		closeErr = errors.Join(closeErr, g.store.Close())
	})
	return closeErr
}

// NextNodeID generates a unique snowflake ID for a new node.
func (g *Graph) NextNodeID() snowflake.ID {
	return g.nodeIDGen.Generate()
}

// NextRelID generates a unique snowflake ID for a new relationship.
func (g *Graph) NextRelID() snowflake.ID {
	return g.relIDGen.Generate()
}

// --- Registry passthrough ---

// GetOrCreateLabel returns the token for a label name, creating it if needed.
func (g *Graph) GetOrCreateLabel(name string) (uint16, error) {
	return g.labels.GetOrCreate(name)
}

// GetOrCreateRelType returns the token for a relationship type name, creating it if needed.
func (g *Graph) GetOrCreateRelType(name string) (uint16, error) {
	return g.relTypes.GetOrCreate(name)
}

// LookupLabel returns the token for a label name without creating it.
func (g *Graph) LookupLabel(name string) (uint16, bool) {
	return g.labels.Lookup(name)
}

// LookupRelType returns the token for a relationship type name without creating it.
func (g *Graph) LookupRelType(name string) (uint16, bool) {
	return g.relTypes.Lookup(name)
}

// --- Resolution methods ---

// NodeLabels resolves all label tokens on the node to strings.
func (g *Graph) NodeLabels(n *types.Node) []string {
	tokens := n.AllLabelTokens()
	raw := make([]uint16, len(tokens))
	for i, t := range tokens {
		raw[i] = t.Value()
	}
	return g.labels.ResolveAll(raw)
}

// NodePrimaryLabel resolves the node's primary label token to a string.
func (g *Graph) NodePrimaryLabel(n *types.Node) string {
	return g.labels.Resolve(n.PrimaryLabelToken().Value())
}

// NodeHasLabel checks if the node has the given label (by name).
// Returns false if the label is not registered.
func (g *Graph) NodeHasLabel(n *types.Node, label string) bool {
	tok, ok := g.labels.Lookup(label)
	if !ok {
		return false
	}
	return n.HasLabelTokenRaw(tok)
}

// RelationshipType resolves the relationship's type token to a string.
func (g *Graph) RelationshipType(r *types.Relationship) string {
	return g.relTypes.Resolve(r.TypeToken().Value())
}

// RelationshipHasType checks if the relationship has the given type (by name).
// Returns false if the type is not registered.
func (g *Graph) RelationshipHasType(r *types.Relationship, typ string) bool {
	tok, ok := g.relTypes.Lookup(typ)
	if !ok {
		return false
	}
	return r.HasTypeTokenRaw(tok)
}

// --- Entity management ---

// AddNode creates a new node with the given labels and properties.
// Labels are resolved to tokens (created if needed). Properties are bulk-validated
// and sorted in O(N log N). Returns the created node with a generated snowflake ID.
func (g *Graph) AddNode(labels []string, props map[string]any) (*types.Node, error) {
	if len(labels) == 0 {
		return nil, ErrNoLabels
	}

	// Bulk-build properties first — fail fast before generating an ID.
	ps, err := types.NewPropertySlice(props)
	if err != nil {
		return nil, fmt.Errorf("graph: node properties: %w", err)
	}

	// Resolve labels to tokens.
	primaryToken, err := g.labels.GetOrCreate(labels[0])
	if err != nil {
		return nil, fmt.Errorf("graph: primary label: %w", err)
	}

	var extraTokens []uint16
	for _, label := range labels[1:] {
		tok, err := g.labels.GetOrCreate(label)
		if err != nil {
			return nil, fmt.Errorf("graph: extra label %q: %w", label, err)
		}
		extraTokens = append(extraTokens, tok)
	}

	id := g.NextNodeID()
	n := types.NewNode(id, primaryToken, extraTokens)
	n.SetProperties(ps)

	if err := g.store.PutNode(n); err != nil {
		return nil, err
	}

	return n, nil
}

// AddRelationship creates a new directed relationship between two nodes.
// The type name is resolved to a token (created if needed). Properties are
// bulk-validated and sorted in O(N log N).
func (g *Graph) AddRelationship(typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error) {
	if startNode == nil || endNode == nil {
		return nil, ErrNilNode
	}

	// Bulk-build properties first — fail fast before generating an ID.
	ps, err := types.NewPropertySlice(props)
	if err != nil {
		return nil, fmt.Errorf("graph: relationship properties: %w", err)
	}

	typeToken, err := g.relTypes.GetOrCreate(typeName)
	if err != nil {
		return nil, fmt.Errorf("graph: relationship type: %w", err)
	}

	startID := startNode.InternalID().SnowflakeID()
	endID := endNode.InternalID().SnowflakeID()

	// Lock both endpoints to prevent write-skew with concurrent DeleteNode.
	// Lock ordering: ascending shard index — deadlock-free.
	g.entityLocks.LockTwo(startID, endID)
	defer g.entityLocks.UnlockTwo(startID, endID)

	id := g.NextRelID()
	r := types.NewRelationship(id, typeToken, startID, endID)
	r.SetProperties(ps)

	if err := g.store.PutRelationship(r); err != nil {
		return nil, err
	}

	return r, nil
}

// DeleteNode atomically removes a node and all connected relationships.
// Acquires the entity lock for the node to prevent write-skew with concurrent
// AddRelationship targeting the same node.
// Returns ErrNodeNotFound if the node does not exist.
func (g *Graph) DeleteNode(id snowflake.ID) error {
	g.entityLocks.LockEntity(id)
	defer g.entityLocks.UnlockEntity(id)
	return g.store.DeleteNodeCascade(id)
}

// DeleteRelationship removes a relationship from the store.
// Returns ErrRelNotFound if the relationship does not exist.
func (g *Graph) DeleteRelationship(id snowflake.ID) error {
	return g.store.DeleteRelationship(id)
}

// --- Store passthrough queries ---

// GetNode retrieves a node by snowflake ID.
func (g *Graph) GetNode(id snowflake.ID) (*types.Node, error) {
	return g.store.GetNode(id)
}

// GetRelationship retrieves a relationship by snowflake ID.
func (g *Graph) GetRelationship(id snowflake.ID) (*types.Relationship, error) {
	return g.store.GetRelationship(id)
}

// NodesByLabel returns all nodes with the given label (resolved from string).
// Returns nil if the label is not registered.
func (g *Graph) NodesByLabel(label string) ([]*types.Node, error) {
	tok, ok := g.labels.Lookup(label)
	if !ok {
		return nil, nil
	}
	return g.store.NodesByLabel(tok)
}

// RelationshipsByType returns all relationships with the given type (resolved from string).
// Returns nil if the type is not registered.
func (g *Graph) RelationshipsByType(typeName string) ([]*types.Relationship, error) {
	tok, ok := g.relTypes.Lookup(typeName)
	if !ok {
		return nil, nil
	}
	return g.store.RelationshipsByType(tok)
}

// OutgoingRelationships returns all outgoing relationships from the given node.
// If typeName is empty, all types are returned. If typeName is non-empty, only
// relationships of that type are returned (nil if the type is not registered).
func (g *Graph) OutgoingRelationships(nodeID snowflake.ID, typeName string) ([]*types.Relationship, error) {
	var tok uint16
	if typeName != "" {
		t, ok := g.relTypes.Lookup(typeName)
		if !ok {
			return nil, nil
		}
		tok = t
	}
	return g.store.OutgoingRelationships(nodeID, tok)
}

// IncomingRelationships returns all incoming relationships to the given node.
// If typeName is empty, all types are returned. If typeName is non-empty, only
// relationships of that type are returned (nil if the type is not registered).
func (g *Graph) IncomingRelationships(nodeID snowflake.ID, typeName string) ([]*types.Relationship, error) {
	var tok uint16
	if typeName != "" {
		t, ok := g.relTypes.Lookup(typeName)
		if !ok {
			return nil, nil
		}
		tok = t
	}
	return g.store.IncomingRelationships(nodeID, tok)
}

// NodeCount returns the number of nodes in the store.
func (g *Graph) NodeCount() (int, error) {
	return g.store.NodeCount()
}

// RelationshipCount returns the number of relationships in the store.
func (g *Graph) RelationshipCount() (int, error) {
	return g.store.RelationshipCount()
}
