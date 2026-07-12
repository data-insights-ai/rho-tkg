// Package graph is the public façade over pkg/graph/internal/core.
//
// As of v3.4.0 the 130+ public methods that historically lived directly on
// *Graph have been removed. Customers must reach the implementation through
// the sub-API accessors. From v4.2.0 the sub-APIs are exposed as accessor
// methods (g.Nodes(), g.Rels(), …) rather than exported fields — see the
// CHANGELOG for the migration recipe. The complete public surface on
// *Graph itself is:
//
//	New(cfg Config) (*Graph, error)
//	(*Graph).Close() error
//	(*Graph).Nodes() *nodes.API
//	(*Graph).Rels() *rels.API
//	(*Graph).Temporal() *temporal.API
//	(*Graph).Index() *index.API
//	(*Graph).Events() *events.API
//	(*Graph).Constraints() *constraints.API
//	(*Graph).IO() *io.API
//	(*Graph).Admin() *admin.API
//	(*Graph).Tier() *tier.API
//	(*Graph).Stats() *stats.API
//	(*Graph).Hash() *hash.API
//	(*Graph).Ingest() *ingest.API
//	(*Graph).Resolve() *resolve.API
//	(*Graph).Tx() *TxAPI
//	(*Graph).Batch() *BatchAPI
//
// All accessors are nil-safe: (*Graph)(nil).Nodes() returns nil, and every
// sub-API method handles a nil receiver by returning ErrNilGraph (or a zero
// value for no-error variants).
package graph

import (
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/admin"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/constraints"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/events"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/hash"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/index"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/ingest"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/core"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/nodes"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/rels"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/replication"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/resolve"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/stats"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/tier"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Graph is the thin façade providing sub-API accessors over an internal Core.
// All methods that historically lived on *Graph have been moved to *core.Core
// and are reachable through the sub-API accessor methods below.
type Graph struct {
	core *core.Core

	nodes       *nodes.API
	rels        *rels.API
	temporal    *temporal.API
	index       *index.API
	events      *events.API
	constraints *constraints.API
	io          *io.API
	admin       *admin.API
	tier        *tier.API
	stats       *stats.API
	hash        *hash.API
	resolve     *resolve.API
	replication *replication.API
	ingest      *ingest.API
	tx          *TxAPI
	batch       *BatchAPI
}

// Nodes returns the node sub-API. Nil-safe: returns nil when g is nil
// (every method on the returned *nodes.API also handles a nil receiver).
func (g *Graph) Nodes() *nodes.API {
	if g == nil {
		return nil
	}
	return g.nodes
}

// Rels returns the relationship sub-API. Nil-safe.
func (g *Graph) Rels() *rels.API {
	if g == nil {
		return nil
	}
	return g.rels
}

// Temporal returns the temporal-query sub-API. Nil-safe.
func (g *Graph) Temporal() *temporal.API {
	if g == nil {
		return nil
	}
	return g.temporal
}

// Index returns the index-management sub-API. Nil-safe.
func (g *Graph) Index() *index.API {
	if g == nil {
		return nil
	}
	return g.index
}

// Events returns the event-bus sub-API. Nil-safe.
func (g *Graph) Events() *events.API {
	if g == nil {
		return nil
	}
	return g.events
}

// Constraints returns the constraint-management sub-API. Nil-safe.
func (g *Graph) Constraints() *constraints.API {
	if g == nil {
		return nil
	}
	return g.constraints
}

// IO returns the export/import sub-API. Nil-safe.
func (g *Graph) IO() *io.API {
	if g == nil {
		return nil
	}
	return g.io
}

// Admin returns the backend-agnostic admin sub-API. Nil-safe.
func (g *Graph) Admin() *admin.API {
	if g == nil {
		return nil
	}
	return g.admin
}

// Tier returns the tiered-store admin sub-API. Nil-safe. Methods on the
// returned API fail closed with ErrNotTieredStore on non-tiered backends.
func (g *Graph) Tier() *tier.API {
	if g == nil {
		return nil
	}
	return g.tier
}

// Stats returns the statistics sub-API. Nil-safe.
func (g *Graph) Stats() *stats.API {
	if g == nil {
		return nil
	}
	return g.stats
}

// Hash returns the hash-chain verification sub-API. Nil-safe.
func (g *Graph) Hash() *hash.API {
	if g == nil {
		return nil
	}
	return g.hash
}

// Replication returns the change-log / replication sub-API (durable op-log:
// CDC, audit, point-in-time recovery, read-replica streaming foundation).
// Nil-safe. Its methods return ErrCapabilityNotSupported on a backend without a
// change-log (e.g. tiered, or badger/memory opened without it enabled).
func (g *Graph) Replication() *replication.API {
	if g == nil {
		return nil
	}
	return g.replication
}

// Ingest returns the ingest-pipeline sub-API (ADR-0006) — the
// prepare-parallel / apply-sequential write door for insert-dominated
// throughput, beside the interactive g.Tx() door. Nil-safe.
func (g *Graph) Ingest() *ingest.API {
	if g == nil {
		return nil
	}
	return g.ingest
}

// Resolve returns the shadow-property resolution sub-API. Nil-safe.
func (g *Graph) Resolve() *resolve.API {
	if g == nil {
		return nil
	}
	return g.resolve
}

// Tx returns the transaction sub-API. Nil-safe.
func (g *Graph) Tx() *TxAPI {
	if g == nil {
		return nil
	}
	return g.tx
}

// Batch returns the batch sub-API. Nil-safe.
func (g *Graph) Batch() *BatchAPI {
	if g == nil {
		return nil
	}
	return g.batch
}

// SetReplicationSource sets (or clears, with nil) this graph's handle to its
// primary's token registry, used by the replica apply path to resolve a token
// the primary allocated after the bootstrap snapshot. A primary's
// g.Replication() satisfies store.ReplicationSource, so a replica can be wired
// with replica.SetReplicationSource(primary.Replication()) in-process. Nil-safe.
func (g *Graph) SetReplicationSource(src store.ReplicationSource) {
	if g == nil || g.core == nil {
		return
	}
	g.core.SetReplicationSource(src)
}

// ReplicationSource aliases store.ReplicationSource — the primary handle a
// replica resolves unknown tokens through. See SetReplicationSource.
type ReplicationSource = store.ReplicationSource

// RegistrySnapshot aliases store.RegistrySnapshot — a primary's token registries
// at a known change-log LSN, returned by g.Replication().RegistrySnapshot().
type RegistrySnapshot = store.RegistrySnapshot

// IDSlotLeaseRecord aliases store.IDSlotLeaseRecord — the durable snowflake-slot
// failover hint persisted/read via g.Replication().SetIDSlotLease/IDSlotLease.
type IDSlotLeaseRecord = store.IDSlotLeaseRecord

// Config aliases core.Config for the public API.
type Config = core.Config

// ValidationLimits aliases core.ValidationLimits for the public API.
type ValidationLimits = core.ValidationLimits

// GraphStats aliases the public stats snapshot returned by g.Stats.Get().
type GraphStats = stats.GraphStats

// PropertyStats aliases store.PropertyStats — the NDV/min/max/count planner
// statistics returned by g.Stats().PropertyStats(...).
type PropertyStats = store.PropertyStats

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

// QueryOpts aliases store.QueryOpts so callers can configure indexed,
// temporal, and paginated reads via the *graph.Graph sub-APIs without
// importing pkg/graph/store directly.
type QueryOpts = store.QueryOpts

// ShardDepth aliases store.ShardDepth — the Depth field of QueryOpts.
// Use the storepkg.Depth* constants (DepthAll, DepthHot, …) — they keep
// their original location since they're tiered-store-specific knobs.
type ShardDepth = store.ShardDepth

// DistanceMetric aliases store.DistanceMetric — accepted by
// g.Index().SearchNearest.
type DistanceMetric = store.DistanceMetric

// Distance metrics for vector indexes — re-exports of the store-canonical
// store.DistanceCosine / store.DistanceEuclidean values (same identity, not
// just the same numeric value), so a caller using only graph.DistanceMetric
// (via g.Index().CreateVector/CreateVectorWithOptions/SearchNearest) is not
// forced to import pkg/graph/store directly just to name a metric constant.
const (
	DistanceCosine    = store.DistanceCosine
	DistanceEuclidean = store.DistanceEuclidean
)

// VectorIndexOptions aliases store.VectorIndexOptions — accepted by
// g.Index().CreateVectorWithOptions to choose the vector-index engine
// (default approximate HNSW, or the UseBruteForce exact-recall escape
// hatch) and its tuning. Zero value = documented defaults.
type VectorIndexOptions = store.VectorIndexOptions

// New creates a new Graph with the given configuration. Delegates to core.New
// and wires every sub-API accessor to the same *Core instance.
func New(cfg Config) (*Graph, error) {
	c, err := core.New(cfg)
	if err != nil {
		return nil, err
	}
	g := &Graph{core: c}
	g.nodes = nodes.New(c.Nodes)
	g.rels = rels.New(c.Rels)
	g.temporal = temporal.New(c.Temporal)
	g.index = index.New(c.Index)
	g.events = events.New(c.Events)
	g.constraints = constraints.New(c.Constraints)
	g.io = io.New(c.IO)
	g.admin = admin.New(c.Admin)
	g.tier = tier.New(c.Admin)
	g.stats = stats.New(c.Stats)
	g.hash = hash.New(c.Hash)
	g.resolve = resolve.New(c.Resolve)
	g.replication = replication.New(c.Repl)
	g.ingest = ingest.New(c.Ingest)
	g.tx = &TxAPI{c: c}
	g.batch = &BatchAPI{c: c}
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
