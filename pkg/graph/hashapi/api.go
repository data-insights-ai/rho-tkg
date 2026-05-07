// Package hashapi is a sub-API accessor for hash-chain verification.
package hashapi

import (
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// Core is the subset of *graph.Graph hash methods the hash sub-API forwards to.
type Core interface {
	VerifyNodeHashChain(id types.NodeID) (bool, error)
	VerifyRelHashChain(id types.RelID) (bool, error)
}

// API is the hash sub-API accessor.
type API struct{ c Core }

// New constructs a hash sub-API.
func New(c Core) *API { return &API{c: c} }

// VerifyNodeChain verifies the hash chain for a node's version history. Forwards to Graph.VerifyNodeHashChain.
func (a *API) VerifyNodeChain(id types.NodeID) (bool, error) { return a.c.VerifyNodeHashChain(id) }

// VerifyRelChain verifies the hash chain for a relationship's version history. Forwards to Graph.VerifyRelHashChain.
func (a *API) VerifyRelChain(id types.RelID) (bool, error) { return a.c.VerifyRelHashChain(id) }
