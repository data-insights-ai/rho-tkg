package types

import "strings"

// Shadow property keys — read-only virtual properties dispatched by the graph
// layer from internal metadata. They are never stored in the entity's
// PropertySlice; PropertySlice.Set rejects any key with the "tkg_" prefix.
//
// Final 21 shadow keys per spec.
const (
	// Structural
	ShadowLabels = "tkg_labels" // Node: []string
	ShadowType   = "tkg_type"   // Relationship: string

	// Temporal
	ShadowValidFrom = "tkg_valid_from" // Both: Instant
	ShadowValidTo   = "tkg_valid_to"   // Both: Instant
	ShadowTxFrom    = "tkg_tx_from"    // Both: Instant
	ShadowTxTo      = "tkg_tx_to"      // Both: Instant
	ShadowCreatedAt = "tkg_created_at" // Both: Instant
	ShadowUpdatedAt = "tkg_updated_at" // Both: Instant
	ShadowDeletedAt = "tkg_deleted_at" // Both: Instant

	// Provenance
	ShadowCreatedBy = "tkg_created_by" // Both: string
	ShadowUpdatedBy = "tkg_updated_by" // Both: string
	ShadowVersion   = "tkg_version"    // Both: uint32

	// Integrity
	ShadowHash     = "tkg_hash"      // Both: string
	ShadowPrevHash = "tkg_prev_hash" // Both: string

	// Integrity — endpoint cross-references (relationship only)
	ShadowFromHash = "tkg_from_hash" // Relationship: string — start-node hash at write time
	ShadowToHash   = "tkg_to_hash"   // Relationship: string — end-node hash at write time

	// Integrity — provenance
	ShadowAuthorID  = "tkg_author_id" // Both: string  — caller-supplied author identifier
	ShadowSignature = "tkg_signature" // Both: []byte  — caller-supplied cryptographic signature

	// Authorization
	ShadowAuthorizedBy = "tkg_authorized_by" // Both: string — caller-supplied authorization entity
	ShadowAuthLevel    = "tkg_auth_level"    // Both: uint8  — caller-supplied authorization level

	// Version chain
	ShadowBaseEntity = "tkg_base_entity" // Both: entityID
)

// IsShadowKey returns true if key is a reserved tkg_ shadow property.
func IsShadowKey(key string) bool {
	return strings.HasPrefix(key, "tkg_")
}
