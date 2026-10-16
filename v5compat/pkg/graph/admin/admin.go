// Package admin mirrors the v4 import path pkg/graph/admin: maintenance, ID decomposition and erasure. Every method keeps its
// v4 signature and reports ErrNotSupportedInV5 (declined or not in the first
// batch of WP-11); the types are placeholders so consumers compile.
package admin

import (
	"context"
	"fmt"
	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/types"
)

// ErrNotSupportedInV5 is the class of every declined door.
var ErrNotSupportedInV5 = store.ErrNotSupportedInV5

func declined(door string) error { return fmt.Errorf("%w: "+door, ErrNotSupportedInV5) }

// API is the sub-API handle returned by the graph accessor.
type API struct{}

// UntimedValidStart is a placeholder for the v4 type of the same name.
type UntimedValidStart struct{}

// IDComponents is a placeholder for the v4 type of the same name.
type IDComponents struct{}

// RetentionPolicy is a placeholder for the v4 type of the same name.
type RetentionPolicy struct{}

// CompactReport is a placeholder for the v4 type of the same name.
type CompactReport struct{}

// PurgePolicy is a placeholder for the v4 type of the same name.
type PurgePolicy struct{}

// PurgeReport is a placeholder for the v4 type of the same name.
type PurgeReport struct{}

// RelPurgePolicy is a placeholder for the v4 type of the same name.
type RelPurgePolicy struct{}

// RelPurgeReport is a placeholder for the v4 type of the same name.
type RelPurgeReport struct{}

// ExactErasureRequest is a placeholder for the v4 type of the same name.
type ExactErasureRequest struct{}

// ExactErasureResolution is a placeholder for the v4 type of the same name.
type ExactErasureResolution struct{}

// ExactErasureReceipt is a placeholder for the v4 type of the same name.
type ExactErasureReceipt struct{}

// RelSegmentStats is a placeholder for the v4 type of the same name.
type RelSegmentStats struct{}

// Reset is declined.
func (a *API) Reset() error {
	return declined("admin.Reset")
}

// SetUntimedValidStart is declined.
func (a *API) SetUntimedValidStart(ctx context.Context, mode UntimedValidStart) error {
	return declined("admin.SetUntimedValidStart")
}

// DecomposeNodeID is declined.
func (a *API) DecomposeNodeID(id types.NodeID) (r0 IDComponents) {
	return
}

// DecomposeRelID is declined.
func (a *API) DecomposeRelID(id types.RelID) (r0 IDComponents) {
	return
}

// CompactHistoryNodes is declined.
func (a *API) CompactHistoryNodes(ctx context.Context, policy RetentionPolicy) (r0 CompactReport, err error) {
	err = declined("admin.CompactHistoryNodes")
	return
}

// CompactHistoryRels is declined.
func (a *API) CompactHistoryRels(ctx context.Context, policy RetentionPolicy) (r0 CompactReport, err error) {
	err = declined("admin.CompactHistoryRels")
	return
}

// PurgeExpiredNodes is declined.
func (a *API) PurgeExpiredNodes(ctx context.Context, policy PurgePolicy) (r0 PurgeReport, err error) {
	err = declined("admin.PurgeExpiredNodes")
	return
}

// PurgeExpiredRels is declined.
func (a *API) PurgeExpiredRels(ctx context.Context, policy RelPurgePolicy) (r0 RelPurgeReport, err error) {
	err = declined("admin.PurgeExpiredRels")
	return
}

// ResolveExactErasure is declined.
func (a *API) ResolveExactErasure(ctx context.Context, request ExactErasureRequest) (r0 ExactErasureResolution, err error) {
	err = declined("admin.ResolveExactErasure")
	return
}

// ExactErase is declined.
func (a *API) ExactErase(ctx context.Context, request ExactErasureRequest) (r0 ExactErasureReceipt, err error) {
	err = declined("admin.ExactErase")
	return
}

// SealRelSegments is declined.
func (a *API) SealRelSegments(relType string) error {
	return declined("admin.SealRelSegments")
}

// RelSegmentStats is declined.
func (a *API) RelSegmentStats(relType string) (r0 RelSegmentStats, err error) {
	err = declined("admin.RelSegmentStats")
	return
}
