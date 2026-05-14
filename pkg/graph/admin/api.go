// Package admin is a sub-API accessor for generic graph admin operations
// (Reset and ID decomposition) that work on every store backend.
//
// API 4.0 change: the tiered-only methods that previously lived here
// (Archive, Restore, ForceRotate, ListShards, RebuildCatalog, Repair,
// VerifyShard) moved to pkg/graph/tier (reached via g.Tier). The split
// turns the tiered-only contract into a compile-time hint instead of a
// hidden runtime trap.
package admin

import (
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/grapherr"
	snowflakepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/snowflake"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// IDComponents is the snowflake decomposition (alias of internal/snowflake.IDComponents).
type IDComponents = snowflakepkg.IDComponents

// Ops is the subset of *core.AdminOps the admin sub-API forwards to.
type Ops interface {
	Reset() error
	DecomposeNodeID(id types.NodeID) IDComponents
	DecomposeRelID(id types.RelID) IDComponents
}

// API is the admin sub-API accessor.
type API struct {
	ops Ops
	ok  bool
}

// New constructs an admin sub-API.
func New(ops Ops) *API { return &API{ops: ops, ok: !grapherr.IsNil(ops)} }

func (a *API) ready() (Ops, error) {
	if a == nil || !a.ok {
		return nil, grapherr.ErrNilGraph
	}
	return a.ops, nil
}

// Reset clears all entities, indexes, history, and counters from the graph.
func (a *API) Reset() error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.Reset()
}

// DecomposeNodeID extracts time/node/step from a node ID.
func (a *API) DecomposeNodeID(id types.NodeID) IDComponents {
	if a == nil || !a.ok {
		return IDComponents{}
	}
	return a.ops.DecomposeNodeID(id)
}

// DecomposeRelID extracts time/node/step from a relationship ID.
func (a *API) DecomposeRelID(id types.RelID) IDComponents {
	if a == nil || !a.ok {
		return IDComponents{}
	}
	return a.ops.DecomposeRelID(id)
}
