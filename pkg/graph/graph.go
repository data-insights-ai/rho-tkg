// Package graph is the public façade over pkg/graph/internal/core.
//
// As of v3.4.0 the 130+ public methods that historically lived directly on
// *Graph have been removed. Customers must reach the implementation through
// the sub-API accessors: g.Nodes.Add(...), g.Rels.Add(...), g.Temporal.NodesAt(...),
// etc. The list below is the complete public surface on *Graph itself:
//
//	New(cfg Config) (*Graph, error)
//	(*Graph).Close() error
//
// Plus the sub-API field set declared on Graph itself.
package graph

import (
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/adminapi"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/constraintsapi"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/eventsapi"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/hashapi"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/indexapi"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/core"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/ioapi"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/nodes"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/rels"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/resolveapi"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/statsapi"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/temporalapi"
)

// Graph is the thin façade providing sub-API accessors over an internal Core.
// All methods that historically lived on *Graph have been moved to *core.Core
// and are reachable through the sub-API fields below.
type Graph struct {
	core *core.Core

	Nodes       *nodes.API
	Rels        *rels.API
	Temporal    *temporalapi.API
	Index       *indexapi.API
	Events      *eventsapi.API
	Constraints *constraintsapi.API
	IO          *ioapi.API
	Admin       *adminapi.API
	Statistics  *statsapi.API
	Hash        *hashapi.API
	Resolve     *resolveapi.API
	Tx          *TxAPI
	Batch       *BatchAPI
}

// Config aliases core.Config for the public API.
type Config = core.Config

// ValidationLimits aliases core.ValidationLimits for the public API.
type ValidationLimits = core.ValidationLimits

// GraphStats aliases core.GraphStats for the public API.
type GraphStats = core.GraphStats

// StoreStats aliases core.StoreStats for the public API.
type StoreStats = core.StoreStats

// GraphTx aliases core.GraphTx for the public API. It is the value returned
// by g.Tx.Begin().
type GraphTx = core.GraphTx

// BatchBuilder aliases core.BatchBuilder for the public API. It is the value
// returned by NewBatchBuilder(g) and g.Batch.New().
type BatchBuilder = core.BatchBuilder

// BatchResult aliases core.BatchResult.
type BatchResult = core.BatchResult

// BatchError aliases core.BatchError.
type BatchError = core.BatchError

// IDComponents aliases core.IDComponents.
type IDComponents = core.IDComponents

// ConstraintSet aliases core.ConstraintSet.
type ConstraintSet = core.ConstraintSet

// New creates a new Graph with the given configuration. Delegates to core.New
// and wires every sub-API accessor to the same *Core instance.
func New(cfg Config) (*Graph, error) {
	c, err := core.New(cfg)
	if err != nil {
		return nil, err
	}
	g := &Graph{core: c}
	g.Nodes = nodes.New(c)
	g.Rels = rels.New(c)
	g.Temporal = temporalapi.New(c)
	g.Index = indexapi.New(c)
	g.Events = eventsapi.New(c)
	g.Constraints = constraintsapi.New(c)
	g.IO = ioapi.New(c)
	g.Admin = adminapi.New(c)
	g.Statistics = statsapi.New(c)
	g.Hash = hashapi.New(c)
	g.Resolve = resolveapi.New(c)
	g.Tx = &TxAPI{c: c}
	g.Batch = &BatchAPI{c: c}
	return g, nil
}

// Close flushes registries (when applicable) and closes the underlying store.
func (g *Graph) Close() error { return g.core.Close() }

// Core returns the internal *core.Core. Used by package-internal helpers and
// transitionally by tests; not part of the stable customer surface.
func (g *Graph) Core() *core.Core { return g.core }

// NewBatchBuilder constructs a BatchBuilder bound to g. Equivalent to g.Batch.New().
func NewBatchBuilder(g *Graph) *BatchBuilder { return core.NewBatchBuilder(g.core) }

// DecomposeID extracts creation time, node ID, sequence number from a
// snowflake ID. Pure helper; does not touch any graph instance.
var DecomposeID = core.DecomposeID
