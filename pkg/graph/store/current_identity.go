package store

import "github.com/data-insights-ai/rho-tkg/v4/pkg/types"

// CurrentIdentityCapability distinguishes current-row absence from storage
// uncertainty. It includes pending writes and excludes history-only identities.
// Mutation preflight must propagate errors rather than treating them as absent.
type CurrentIdentityCapability interface {
	NodeExists(types.NodeID) (bool, error)
	RelationshipExists(types.RelID) (bool, error)
}

// CloseAdmissionCapability reserves backend lifecycle admission before a graph
// closes itself or persists final metadata. A refusal changes no lifecycle state.
// The caller must defer release, and close the backend before releasing on success.
// Releasing without closing restores read/callback admission.
type CloseAdmissionCapability interface {
	AdmitClose() (release func(), err error)
}
