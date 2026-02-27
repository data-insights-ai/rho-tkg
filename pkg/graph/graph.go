package graph

import (
	"fmt"

	snowflake "gitlab2024.bds421-cloud.com/bds421/rho/snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3/pkg/types"
)

// Config holds configuration for the Graph.
type Config struct {
	// SnowflakeNodeID identifies this graph instance (0-1023).
	// Each concurrent instance must use a different value.
	SnowflakeNodeID int64
}

// Graph is the central graph layer. It owns the label and relationship type
// registries, snowflake ID generators, and provides string resolution for
// token-based entities.
type Graph struct {
	labels    *labelRegistry
	relTypes  *relTypeRegistry
	nodeIDGen *snowflake.Node
	relIDGen  *snowflake.Node
}

// New creates a new Graph with the given configuration.
// Returns an error if SnowflakeNodeID is out of range (0-1023 for 10-bit node).
func New(config Config) (*Graph, error) {
	nodeGen, err := snowflake.NewNode(config.SnowflakeNodeID)
	if err != nil {
		return nil, fmt.Errorf("graph: node ID generator: %w", err)
	}
	relGen, err := snowflake.NewNode(config.SnowflakeNodeID)
	if err != nil {
		return nil, fmt.Errorf("graph: rel ID generator: %w", err)
	}
	return &Graph{
		labels:    newLabelRegistry(),
		relTypes:  newRelTypeRegistry(),
		nodeIDGen: nodeGen,
		relIDGen:  relGen,
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
	// We need to check all tokens on the node. AllLabelTokens returns
	// opaque labelToken values, so we compare via Value().
	for _, lt := range n.AllLabelTokens() {
		if lt.Value() == tok {
			return true
		}
	}
	return false
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
	return r.TypeToken().Value() == tok
}
