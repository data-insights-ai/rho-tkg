package types

import "strings"

// Shadow property keys — read-only virtual properties dispatched by the graph
// layer from internal metadata. They are never stored in the entity's
// PropertySlice; PropertySlice.Set rejects any key with the "tkg_" prefix.
const (
	ShadowLabels          = "tkg_labels"
	ShadowPrimaryLabel    = "tkg_primary_label"
	ShadowID              = "tkg_id"
	ShadowValidFrom       = "tkg_valid_from"
	ShadowValidTo         = "tkg_valid_to"
	ShadowTransactionFrom = "tkg_transaction_from"
	ShadowTransactionTo   = "tkg_transaction_to"
	ShadowCreatedAt       = "tkg_created_at"
	ShadowUpdatedAt       = "tkg_updated_at"
	ShadowVersion         = "tkg_version"
	ShadowPreviousVersion = "tkg_previous_version"
	ShadowNextVersion     = "tkg_next_version"
	ShadowBaseEntity      = "tkg_base_entity"
	ShadowRelType         = "tkg_rel_type"
	ShadowIntegrity       = "tkg_integrity"
)

// IsShadowKey returns true if key is a reserved tkg_ shadow property.
func IsShadowKey(key string) bool {
	return strings.HasPrefix(key, "tkg_")
}
