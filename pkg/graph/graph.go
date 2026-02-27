package graph

import (
	"errors"
	"fmt"
	"time"

	snowflake "gitlab2024.bds421-cloud.com/bds421/rho/snowflake-2026"
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
	// SnowflakeNodeID identifies this graph instance (0-1023).
	// Each concurrent instance must use a different value.
	SnowflakeNodeID int64

	// Store is the persistence backend. If nil, NewMemoryStore() is used.
	Store Store
}

// Graph is the central graph layer. It owns the label and relationship type
// registries, snowflake ID generators, store, and provides string resolution
// for token-based entities.
type Graph struct {
	labels    *labelRegistry
	relTypes  *relTypeRegistry
	nodeIDGen *snowflake.Node
	relIDGen  *snowflake.Node
	store     Store
}

// New creates a new Graph with the given configuration.
// Returns an error if SnowflakeNodeID is out of range (0-1023 for 10-bit node).
func New(config Config) (*Graph, error) {
	nodeGen, err := snowflake.NewNode(config.SnowflakeNodeID,
		snowflake.WithEpoch(snowflakeEpoch),
		snowflake.WithNodeBits(10),
		snowflake.WithStepBits(12),
	)
	if err != nil {
		return nil, fmt.Errorf("graph: node ID generator: %w", err)
	}
	// relGen uses the same parameters as nodeGen. If nodeGen succeeded,
	// relGen will too. The error handling remains for defensive correctness
	// in case the snowflake library adds non-deterministic validation.
	relGen, err := snowflake.NewNode(config.SnowflakeNodeID,
		snowflake.WithEpoch(snowflakeEpoch),
		snowflake.WithNodeBits(10),
		snowflake.WithStepBits(12),
	)
	if err != nil {
		return nil, fmt.Errorf("graph: rel ID generator: %w", err)
	}
	store := config.Store
	if store == nil {
		store = NewMemoryStore()
	}

	return &Graph{
		labels:    newLabelRegistry(),
		relTypes:  newRelTypeRegistry(),
		nodeIDGen: nodeGen,
		relIDGen:  relGen,
		store:     store,
	}, nil
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

	id := g.NextRelID()
	r := types.NewRelationship(id, typeToken, startID, endID)
	r.SetProperties(ps)

	if err := g.store.PutRelationship(r); err != nil {
		return nil, err
	}

	return r, nil
}

// DeleteNode cascade-deletes all connected relationships, then removes the node.
// Returns ErrNodeNotFound if the node does not exist.
func (g *Graph) DeleteNode(id snowflake.ID) error {
	// Collect all connected relationships before deleting.
	outgoing := g.store.OutgoingRelationships(id, 0)
	incoming := g.store.IncomingRelationships(id, 0)

	// Delete each connected relationship.
	for _, r := range outgoing {
		if err := g.store.DeleteRelationship(r.InternalID().SnowflakeID()); err != nil {
			return fmt.Errorf("graph: cascade delete outgoing rel: %w", err)
		}
	}
	for _, r := range incoming {
		// Skip if already deleted (self-loop: same rel in both outgoing and incoming).
		if err := g.store.DeleteRelationship(r.InternalID().SnowflakeID()); err != nil {
			if errors.Is(err, ErrRelNotFound) {
				continue
			}
			return fmt.Errorf("graph: cascade delete incoming rel: %w", err)
		}
	}

	return g.store.DeleteNode(id)
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
func (g *Graph) NodesByLabel(label string) []*types.Node {
	tok, ok := g.labels.Lookup(label)
	if !ok {
		return nil
	}
	return g.store.NodesByLabel(tok)
}

// RelationshipsByType returns all relationships with the given type (resolved from string).
// Returns nil if the type is not registered.
func (g *Graph) RelationshipsByType(typeName string) []*types.Relationship {
	tok, ok := g.relTypes.Lookup(typeName)
	if !ok {
		return nil
	}
	return g.store.RelationshipsByType(tok)
}

// NodeCount returns the number of nodes in the store.
func (g *Graph) NodeCount() int {
	return g.store.NodeCount()
}

// RelationshipCount returns the number of relationships in the store.
func (g *Graph) RelationshipCount() int {
	return g.store.RelationshipCount()
}
