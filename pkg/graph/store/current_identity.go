package store

import "github.com/data-insights-ai/rho-tkg/v4/pkg/types"

// CurrentIdentityCapability distinguishes current-row absence from storage
// uncertainty. It includes pending writes and excludes history-only identities.
// Mutation preflight must propagate errors rather than treating them as absent.
type CurrentIdentityCapability interface {
	NodeExists(types.NodeID) (bool, error)
	RelationshipExists(types.RelID) (bool, error)
}
