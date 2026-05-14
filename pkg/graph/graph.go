// Package graph is the public façade over pkg/graph/internal/core.
//
// As of v3.4.0 the 130+ public methods that historically lived directly on
// *Graph have been removed. Customers must reach the implementation through
// the sub-API accessors: g.Nodes.Add(context.Background(), ...), g.Rels.Add(context.Background(), ...), g.Temporal.NodesAt(...),
// etc. The complete public surface on *Graph itself is:
//
//	New(cfg Config) (*Graph, error)
//	(*Graph).Close() error
//
// Plus the sub-API field set declared on Graph itself.
package graph

import (
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/admin"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/constraints"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/events"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/hash"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/index"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/internal/core"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/io"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/nodes"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/rels"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/resolve"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/stats"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/temporal"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/tier"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
)

// Graph is the thin façade providing sub-API accessors over an internal Core.
// All methods that historically lived on *Graph have been moved to *core.Core
// and are reachable through the sub-API fields below.
type Graph struct {
	core *core.Core

	Nodes       *nodes.API
	Rels        *rels.API
	Temporal    *temporal.API
	Index       *index.API
	Events      *events.API
	Constraints *constraints.API
	IO          *io.API
	Admin       *admin.API
	Tier        *tier.API
	Stats       *stats.API
	Hash        *hash.API
	Resolve     *resolve.API
	Tx          *TxAPI
	Batch       *BatchAPI
}

// Config aliases core.Config for the public API.
type Config = core.Config

// ValidationLimits aliases core.ValidationLimits for the public API.
type ValidationLimits = core.ValidationLimits

// GraphStats aliases the public stats snapshot returned by g.Stats.Get().
type GraphStats = stats.GraphStats

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
	g.Nodes = nodes.New(c.Nodes)
	g.Rels = rels.New(c.Rels)
	g.Temporal = temporal.New(c.Temporal)
	g.Index = index.New(c.Index)
	g.Events = events.New(c.Events)
	g.Constraints = constraints.New(c.Constraints)
	g.IO = io.New(c.IO)
	g.Admin = admin.New(c.Admin)
	g.Tier = tier.New(c.Admin)
	g.Stats = stats.New(c.Stats)
	g.Hash = hash.New(c.Hash)
	g.Resolve = resolve.New(c.Resolve)
	g.Tx = &TxAPI{c: c}
	g.Batch = &BatchAPI{c: c}
	return g, nil
}

// Close flushes registries (when applicable) and closes the underlying store.
func (g *Graph) Close() error {
	if g == nil || g.core == nil {
		return ErrNilGraph
	}
	return g.core.Close()
}

// NewBatchBuilder constructs a BatchBuilder bound to g. Equivalent to
// g.Batch.New(). Returns ErrGraphClosed if the graph has already been
// closed.
func NewBatchBuilder(g *Graph) (*BatchBuilder, error) {
	if g == nil || g.core == nil {
		return nil, ErrNilGraph
	}
	return core.NewBatchBuilder(g.core)
}

// DecomposeNodeID extracts creation time, node ID, sequence number from a
// NodeID. Pure helper; does not touch any graph instance.
func DecomposeNodeID(id types.NodeID) IDComponents {
	return core.DecomposeID(id.SnowflakeID())
}

// DecomposeRelID extracts creation time, node ID, sequence number from a
// RelID. Pure helper; does not touch any graph instance.
func DecomposeRelID(id types.RelID) IDComponents {
	return core.DecomposeID(id.SnowflakeID())
}
