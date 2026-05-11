// Package generatedcreate defines the internal fast path used only after the
// graph layer has generated a fresh snowflake ID itself.
package generatedcreate

import "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"

// Proof is intentionally defined in an internal package. Public Store callers
// outside pkg/graph cannot name this type, so the generated-ID fast path cannot
// become a public duplicate-check bypass.
type Proof struct {
	freshGraphID bool
}

// FreshGraphID is passed by core create paths immediately after generating a
// snowflake ID from the graph's configured generators.
var FreshGraphID = Proof{freshGraphID: true}

// Valid reports whether this proof came from this package's fresh-ID marker.
func (p Proof) Valid() bool {
	return p.freshGraphID
}

// Capability is implemented by backends that can optimize graph-generated
// create paths without exposing that weaker duplicate-probe contract publicly.
type Capability interface {
	PutNodeGeneratedID(n *types.Node, proof Proof) error
	PutRelationshipGeneratedID(r *types.Relationship, proof Proof) error
	PutNodesBatchGeneratedID(nodes []*types.Node, proof Proof) error
}

// RelationshipEndpointHashCapability is an optional generated-ID fast path for
// stores that can verify relationship endpoints, capture their integrity
// hashes, and persist the relationship in one routed write path. The returned
// hashes are the values persisted on the relationship.
type RelationshipEndpointHashCapability interface {
	PutRelationshipGeneratedIDWithEndpointHashes(r *types.Relationship, proof Proof) (string, string, error)
}
