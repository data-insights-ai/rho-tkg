// Package generatedcreate defines the internal fast path used only after the
// graph layer has generated a fresh snowflake ID itself.
package generatedcreate

import "github.com/data-insights-ai/rho-tkg/v4/pkg/types"

// Proof is intentionally defined in an internal package. Public Store callers
// outside pkg/graph cannot name this type, so the generated-ID fast path cannot
// become a public duplicate-check bypass.
type Proof struct {
	freshGraphID bool
}

// FreshGraphID is passed by core create paths immediately after generating a
// snowflake ID from the graph's configured generators.
//
// BACKLOG 15n: a function, not a package-level var. A mutable exported var
// holding this process-wide sentinel could be accidentally reassigned by any
// importer (e.g. `generatedcreate.FreshGraphID = generatedcreate.Proof{}`,
// legal since Proof's zero value is externally constructible even though its
// field is unexported) — zeroing freshGraphID for every subsequent call
// across the whole process for the lifetime of the binary. A function
// returning a fresh value each call has no such mutable global to corrupt.
func FreshGraphID() Proof { return Proof{freshGraphID: true} }

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

// RelationshipEndpointHashScopedCapability is the BACKLOG 11f Batch A scoped
// counterpart of RelationshipEndpointHashCapability — FOUNDATION ONLY, see
// store.ScopedTxChangeLog's doc comment for the full design rationale. It
// behaves exactly like PutRelationshipGeneratedIDWithEndpointHashes except the
// change-log record it produces is routed into the store.ScopedTxChangeLog
// buffer named by token instead of the eager pending log. token == 0 is
// exactly PutRelationshipGeneratedIDWithEndpointHashes. A store implementing
// this MUST also implement both RelationshipEndpointHashCapability and
// store.ScopedTxChangeLog.
type RelationshipEndpointHashScopedCapability interface {
	PutRelationshipGeneratedIDWithEndpointHashesScoped(r *types.Relationship, token uint64, proof Proof) (fromHash, toHash string, err error)
}

// ForeignEndpointRelCapability is an optional generated-ID create path for a
// PARTITIONED store: it persists a relationship whose END node lives on a
// FOREIGN partition — a slot owned by another machine and not present in this
// store (ADR-0010). The rel row and BOTH adjacency legs are written on the
// rel's shard; the END node's existence is NOT validated locally (the caller
// attests it via an out-of-band RPC), while the START (local) endpoint IS
// validated. The relationship already carries its attested tkg_to_hash and its
// locally-captured tkg_from_hash, so no endpoint-hash capture happens here.
//
// Only the slot-sharded store implements this; single-machine backends
// (memory/badger/tiered) have no foreign partition and decline it, which makes
// the graph-level foreign-endpoint door fail closed on a non-partitioned store.
type ForeignEndpointRelCapability interface {
	PutRelationshipForeignEnd(r *types.Relationship, proof Proof) error
}

// ForeignIncomingRelCapability is the ADR-0010 Model A companion to
// ForeignEndpointRelCapability: it records a cross-machine incoming half-edge
// STUB on the END node's machine so IncomingRelationships(END) is locally
// complete. The stub's rel-ID belongs to a FOREIGN slot; the store writes it
// co-located on the END node's shard (reachable via that shard's adjacency fold,
// never a slot-routed point read) and co-commits a distinct ChangeForeignIncoming
// record so a replica routes apply by the END-node slot. Sharded-only.
type ForeignIncomingRelCapability interface {
	RecordForeignIncoming(r *types.Relationship, proof Proof) error
	// DeleteForeignIncoming removes the stub identified by relID, routed by the
	// LOCAL end node endID (the rel-ID's own slot is foreign). It co-commits a
	// ChangeForeignIncomingDelete record so a replica routes the delete by the
	// END slot too. Idempotent — an already-absent stub is not an error. Called
	// by the cascade (END-node delete) and the replica-apply path.
	DeleteForeignIncoming(relID types.RelID, endID types.NodeID) error
}
