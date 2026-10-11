package temporal

import (
	"context"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Optional capability preserves compatibility with existing Ops implementers.
type versionBoundsOps interface {
	SetNodeVersionBounds(context.Context, types.NodeID, types.TemporalRange, map[string]any) (*types.Node, error)
	SetRelVersionBounds(context.Context, types.RelID, types.TemporalRange, map[string]any) (*types.Relationship, error)
}

// SetNodeVersionBounds records an append-only valid-time correction with exact
// finite/unbounded endpoints. Finite zero is the Unix epoch. This is the typed
// counterpart of SetNodeVersionInterval, whose numeric defaults are unchanged.
func (a *API) SetNodeVersionBounds(ctx context.Context, id types.NodeID, bounds types.TemporalRange, props map[string]any) (*types.Node, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	b, ok := ops.(versionBoundsOps)
	if !ok {
		return nil, storepkg.ErrCapabilityNotSupported
	}
	return b.SetNodeVersionBounds(ctx, id, bounds, props)
}

// SetRelVersionBounds is the relationship counterpart.
func (a *API) SetRelVersionBounds(ctx context.Context, id types.RelID, bounds types.TemporalRange, props map[string]any) (*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	b, ok := ops.(versionBoundsOps)
	if !ok {
		return nil, storepkg.ErrCapabilityNotSupported
	}
	return b.SetRelVersionBounds(ctx, id, bounds, props)
}
