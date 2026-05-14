// Package hash is a sub-API accessor for hash-chain verification.
package hash

import (
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/internal/grapherr"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
)

// Ops is the subset of *core.HashOps the hash sub-API forwards to.
type Ops interface {
	VerifyNodeChain(id types.NodeID) (bool, error)
	VerifyRelChain(id types.RelID) (bool, error)
}

// API is the hash sub-API accessor.
type API struct {
	ops Ops
	ok  bool
}

// New constructs a hash sub-API.
func New(ops Ops) *API { return &API{ops: ops, ok: !grapherr.IsNil(ops)} }

func (a *API) ready() (Ops, error) {
	if a == nil || !a.ok {
		return nil, grapherr.ErrNilGraph
	}
	return a.ops, nil
}

// VerifyNodeChain verifies the hash chain for a node's version history.
func (a *API) VerifyNodeChain(id types.NodeID) (bool, error) {
	ops, err := a.ready()
	if err != nil {
		return false, err
	}
	return ops.VerifyNodeChain(id)
}

// VerifyRelChain verifies the hash chain for a relationship's version history.
func (a *API) VerifyRelChain(id types.RelID) (bool, error) {
	ops, err := a.ready()
	if err != nil {
		return false, err
	}
	return ops.VerifyRelChain(id)
}
