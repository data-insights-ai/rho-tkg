package store

import "github.com/data-insights-ai/rho-tkg/v4/pkg/types"

// CurrentIdentityCapability distinguishes current-row absence from storage
// uncertainty. It includes pending writes and excludes history-only identities.
// Mutation preflight must propagate errors rather than treating them as absent.
type CurrentIdentityCapability interface {
	NodeExists(types.NodeID) (bool, error)
	RelationshipExists(types.RelID) (bool, error)
}

// CloseAdmissionCapability reserves a backend close before a graph changes its
// lifecycle. Reservation leaves reads available while accepted ingest drains.
// A refusal changes no lifecycle state. Default profiles need no reservation.
type CloseAdmissionCapability interface {
	CloseAdmissionRequired() bool
	AdmitClose() (CloseAdmissionLease, error)
}

// CloseAdmissionLease separates accepted-work drain from reader sealing. Seal
// fences new iteration/callback admission and waits for admitted snapshots.
// Close closes the sealed backend; direct backend Close refuses an active lease.
// Release cancels an unused lease or releases a closed lease, and is idempotent.
// Advanced backend callers must invoke lease methods outside their iteration
// callbacks: Seal waits for admitted snapshots, including the caller's own.
type CloseAdmissionLease interface {
	Seal() error
	Close() error
	Release()
}
