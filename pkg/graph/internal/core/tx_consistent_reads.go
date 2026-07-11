package core

import (
	"context"
	"io"
	"sort"

	eventspkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/events"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	temporalpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Tx-scoped consistent-read APIs (R5-F2).
//
// The standalone IO/Temporal/Admin snapshot-style entry points acquire
// c.mu.Lock themselves. Code that is already inside g.Tx.Run /
// g.Tx.RunContext must call these tx-scoped methods instead: the tx
// already holds c.mu.Lock, so the underlying lock-free implementations
// run without re-entering c.mu.
//
// Calling the standalone (*IOOps).Export, (*TempOps).Snapshot, or
// (*AdminOps).VerifyShard from inside g.Tx.Run would deadlock because
// sync.RWMutex is not reentrant — those methods try to Lock while
// the tx holds Lock. The tx-scoped methods below avoid that trap.

// Export writes a portable graph snapshot to w under the transaction's
// write lock. Strict consistency: no concurrent mutations can
// interleave with the export's reads while the transaction is live.
// See (*IOOps).Export for stream format details.
func (tx *GraphTx) Export(w io.Writer) error {
	if err := tx.lockActiveCore(); err != nil {
		return err
	}
	defer tx.unlockActiveCore()
	return tx.g.exportLocked(w)
}

// Snapshot returns the graph state at the given instant under the
// transaction's write lock. Strict consistency: no concurrent
// mutations can interleave with the snapshot's reads while the
// transaction is live.
func (tx *GraphTx) Snapshot(at types.Instant) (*temporalpkg.GraphSnapshot, error) {
	if err := tx.lockActiveCore(); err != nil {
		return nil, err
	}
	defer tx.unlockActiveCore()
	return tx.g.snapshotAt(at)
}

// VerifyShard runs hash chain verification on a tiered-store shard
// under the transaction's write lock. Strict consistency: no
// concurrent mutations can flip an entity's hash chain mid-scan.
// Returns ErrNotTieredStore if the underlying store does not support
// shard-level verification.
func (tx *GraphTx) VerifyShard(shardName string) (*tiered.VerifyResult, error) {
	if err := tx.lockActiveCore(); err != nil {
		return nil, err
	}
	defer tx.unlockActiveCore()
	return tx.g.verifyShardLocked(shardName)
}

// Tx-side resolution and shadow-property mirrors.
//
// Calling g.Nodes.Labels, g.Nodes.HasLabel, g.Nodes.PrimaryLabel,
// g.Rels.Type, g.Rels.HasType, g.Resolve.NodeProperty, or
// g.Resolve.RelProperty from inside g.Tx.Run / g.Tx.RunContext deadlocks
// the caller goroutine because each acquires c.mu.RLock while the tx
// already holds c.mu.Lock (sync.RWMutex is not reentrant — lesson 9).
// The mirrors below call the lock-free *Unlocked helpers directly and
// guard only the tx-state mutex.
//
// After Commit/Rollback every method returns the zero value of its
// return type — matching the silently-fail-closed contract of the
// non-tx accessors, which return zero values when c.closed is set.

// Labels resolves all label tokens on the node to strings within the
// transaction. Returns nil after Commit/Rollback.
func (tx *GraphTx) Labels(n *types.Node) []string {
	if tx == nil || tx.g == nil {
		return nil
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return nil
	}
	return tx.g.nodeLabelsUnlocked(n)
}

// PrimaryLabel resolves the node's primary label token within the
// transaction. Returns "" after Commit/Rollback.
func (tx *GraphTx) PrimaryLabel(n *types.Node) string {
	if tx == nil || tx.g == nil || n == nil {
		return ""
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return ""
	}
	return tx.g.nodePrimaryLabelUnlocked(n)
}

// HasLabel reports whether the node carries the given label name within
// the transaction. Returns false after Commit/Rollback or for an invalid
// or unregistered label.
func (tx *GraphTx) HasLabel(n *types.Node, label string) bool {
	if tx == nil || tx.g == nil || n == nil {
		return false
	}
	if err := tx.g.validateName(label); err != nil {
		return false
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return false
	}
	return tx.g.nodeHasLabelUnlocked(n, label)
}

// RelType resolves the relationship's type token to a string within the
// transaction. Returns "" after Commit/Rollback.
func (tx *GraphTx) RelType(r *types.Relationship) string {
	if tx == nil || tx.g == nil || r == nil {
		return ""
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return ""
	}
	return tx.g.relTypeUnlocked(r)
}

// HasType reports whether the relationship has the given type name
// within the transaction. Returns false after Commit/Rollback or for an
// invalid or unregistered type name.
func (tx *GraphTx) HasType(r *types.Relationship, typ string) bool {
	if tx == nil || tx.g == nil || r == nil {
		return false
	}
	if err := tx.g.validateName(typ); err != nil {
		return false
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return false
	}
	return tx.g.relHasTypeUnlocked(r, typ)
}

// NodeProperty resolves a property key (including tkg_* shadow keys) on a
// node within the transaction. Returns (nil, false) after Commit/Rollback.
func (tx *GraphTx) NodeProperty(n *types.Node, key string) (any, bool) {
	if tx == nil || tx.g == nil {
		return nil, false
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return nil, false
	}
	return tx.g.nodePropertyUnlocked(n, key)
}

// RelProperty resolves a property key (including tkg_* shadow keys) on a
// relationship within the transaction. Returns (nil, false) after
// Commit/Rollback.
func (tx *GraphTx) RelProperty(r *types.Relationship, key string) (any, bool) {
	if tx == nil || tx.g == nil {
		return nil, false
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return nil, false
	}
	return tx.g.relPropertyUnlocked(r, key)
}

// =============================================================================
// Tx-side bulk read mirrors (v4.0.2)
//
// Every public sub-API read method that opens with c.mu.RLock or
// c.readUnderRLock(…) deadlocks if called inside an open *GraphTx (BeginTx
// holds c.mu.Lock for the tx lifetime; sync.RWMutex is not reentrant —
// lesson 31). The mirrors below dispatch to the matching c.fooLocked
// helpers under tx.mu only, inheriting c.mu.Lock from BeginTx. Each
// returns the zero value of its return type after Commit/Rollback
// (matching the non-tx accessors' fail-closed contract for closed graphs).
// =============================================================================

// --- NodeOps mirrors ---

// GetByIDs is a tx-side mirror for g.Nodes.GetByIDs.
func (tx *GraphTx) GetNodesByIDs(ids []types.NodeID) ([]*types.Node, error) {
	if err := tx.lockActiveCore(); err != nil {
		return nil, err
	}
	defer tx.unlockActiveCore()
	if len(ids) == 0 {
		return nil, nil
	}
	for _, id := range ids {
		if err := storepkg.ValidateNodeID(id); err != nil {
			return nil, err
		}
	}
	return tx.g.nodesByIDsLocked(ids)
}

// AllNodes is a tx-side mirror for g.Nodes.All.
func (tx *GraphTx) AllNodes(opts storepkg.QueryOpts) ([]*types.Node, error) {
	if err := tx.lockActiveCore(); err != nil {
		return nil, err
	}
	defer tx.unlockActiveCore()
	if err := tx.g.validateTemporalQueryOptsScan(opts); err != nil {
		return nil, err
	}
	return tx.g.allNodesLocked(opts)
}

// NodesByLabel is a tx-side mirror for g.Nodes.ByLabel.
func (tx *GraphTx) NodesByLabel(label string, opts storepkg.QueryOpts) ([]*types.Node, error) {
	if err := tx.lockActiveCore(); err != nil {
		return nil, err
	}
	defer tx.unlockActiveCore()
	if err := tx.g.validateIndexLabel(label); err != nil {
		return nil, err
	}
	if err := tx.g.validateTemporalQueryOptsScan(opts); err != nil {
		return nil, err
	}
	return tx.g.nodesByLabelLocked(label, opts)
}

// NodesByLabelAndProperty is a tx-side mirror for g.Nodes.ByLabelAndProperty.
func (tx *GraphTx) NodesByLabelAndProperty(label, key string, value any, opts storepkg.QueryOpts) ([]*types.Node, error) {
	if err := tx.lockActiveCore(); err != nil {
		return nil, err
	}
	defer tx.unlockActiveCore()
	if err := tx.g.validateTemporalQueryOptsScan(opts); err != nil {
		return nil, err
	}
	if err := tx.g.validateIndexLabel(label); err != nil {
		return nil, err
	}
	if err := tx.g.validateIndexPropertyKey(key); err != nil {
		return nil, err
	}
	if err := types.ValidatePropertyValue(value); err != nil {
		return nil, err
	}
	return tx.g.nodesByLabelAndPropertyLocked(label, key, value, opts)
}

// NodeCount is a tx-side mirror for g.Nodes.Count.
func (tx *GraphTx) NodeCount() (int, error) {
	if err := tx.lockActiveCore(); err != nil {
		return 0, err
	}
	defer tx.unlockActiveCore()
	return tx.g.nodeCount()
}

// NodeCountByLabel is a tx-side mirror for g.Nodes.CountByLabel.
func (tx *GraphTx) NodeCountByLabel(label string) (int, error) {
	if err := tx.lockActiveCore(); err != nil {
		return 0, err
	}
	defer tx.unlockActiveCore()
	if err := tx.g.validateIndexLabel(label); err != nil {
		return 0, err
	}
	return tx.g.countByLabelLocked(label)
}

// --- RelOps mirrors ---

// GetRelsByIDs is a tx-side mirror for g.Rels.GetByIDs.
func (tx *GraphTx) GetRelsByIDs(ids []types.RelID) ([]*types.Relationship, error) {
	if err := tx.lockActiveCore(); err != nil {
		return nil, err
	}
	defer tx.unlockActiveCore()
	if len(ids) == 0 {
		return nil, nil
	}
	for _, id := range ids {
		if err := storepkg.ValidateRelID(id); err != nil {
			return nil, err
		}
	}
	return tx.g.relsByIDsLocked(ids)
}

// AllRels is a tx-side mirror for g.Rels.All.
func (tx *GraphTx) AllRels(opts storepkg.QueryOpts) ([]*types.Relationship, error) {
	if err := tx.lockActiveCore(); err != nil {
		return nil, err
	}
	defer tx.unlockActiveCore()
	if err := tx.g.validateTemporalQueryOptsScan(opts); err != nil {
		return nil, err
	}
	return tx.g.allRelsLocked(opts)
}

// RelsByType is a tx-side mirror for g.Rels.ByType.
func (tx *GraphTx) RelsByType(typeName string, opts storepkg.QueryOpts) ([]*types.Relationship, error) {
	if err := tx.lockActiveCore(); err != nil {
		return nil, err
	}
	defer tx.unlockActiveCore()
	if err := tx.g.validateRelTypeQueryName(typeName); err != nil {
		return nil, err
	}
	if err := tx.g.validateTemporalQueryOptsScan(opts); err != nil {
		return nil, err
	}
	return tx.g.relsByTypeLocked(typeName, opts)
}

// OutgoingRels is a tx-side mirror for g.Rels.Outgoing.
func (tx *GraphTx) OutgoingRels(nodeID types.NodeID, typeName string) ([]*types.Relationship, error) {
	if err := tx.lockActiveCore(); err != nil {
		return nil, err
	}
	defer tx.unlockActiveCore()
	if typeName != "" {
		if err := tx.g.validateRelTypeQueryName(typeName); err != nil {
			return nil, err
		}
	}
	if err := storepkg.ValidateNodeID(nodeID); err != nil {
		return nil, err
	}
	return tx.g.outgoingRelsLocked(nodeID, typeName)
}

// IncomingRels is a tx-side mirror for g.Rels.Incoming.
func (tx *GraphTx) IncomingRels(nodeID types.NodeID, typeName string) ([]*types.Relationship, error) {
	if err := tx.lockActiveCore(); err != nil {
		return nil, err
	}
	defer tx.unlockActiveCore()
	if typeName != "" {
		if err := tx.g.validateRelTypeQueryName(typeName); err != nil {
			return nil, err
		}
	}
	if err := storepkg.ValidateNodeID(nodeID); err != nil {
		return nil, err
	}
	return tx.g.incomingRelsLocked(nodeID, typeName)
}

// OutgoingRelsForNodes is a tx-side mirror for g.Rels.OutgoingForNodes.
func (tx *GraphTx) OutgoingRelsForNodes(nodeIDs []types.NodeID, typeName string) (map[types.NodeID][]*types.Relationship, error) {
	if err := tx.lockActiveCore(); err != nil {
		return nil, err
	}
	defer tx.unlockActiveCore()
	if typeName != "" {
		if err := tx.g.validateRelTypeQueryName(typeName); err != nil {
			return nil, err
		}
	}
	if len(nodeIDs) == 0 {
		return nil, nil
	}
	for _, id := range nodeIDs {
		if err := storepkg.ValidateNodeID(id); err != nil {
			return nil, err
		}
	}
	return tx.g.outgoingRelsForNodesLocked(nodeIDs, typeName)
}

// IncomingRelsForNodes is a tx-side mirror for g.Rels.IncomingForNodes.
func (tx *GraphTx) IncomingRelsForNodes(nodeIDs []types.NodeID, typeName string) (map[types.NodeID][]*types.Relationship, error) {
	if err := tx.lockActiveCore(); err != nil {
		return nil, err
	}
	defer tx.unlockActiveCore()
	if typeName != "" {
		if err := tx.g.validateRelTypeQueryName(typeName); err != nil {
			return nil, err
		}
	}
	if len(nodeIDs) == 0 {
		return nil, nil
	}
	for _, id := range nodeIDs {
		if err := storepkg.ValidateNodeID(id); err != nil {
			return nil, err
		}
	}
	return tx.g.incomingRelsForNodesLocked(nodeIDs, typeName)
}

// RelCount is a tx-side mirror for g.Rels.Count.
func (tx *GraphTx) RelCount() (int, error) {
	if err := tx.lockActiveCore(); err != nil {
		return 0, err
	}
	defer tx.unlockActiveCore()
	return tx.g.relCount()
}

// RelCountByType is a tx-side mirror for g.Rels.CountByType.
func (tx *GraphTx) RelCountByType(typeName string) (int, error) {
	if err := tx.lockActiveCore(); err != nil {
		return 0, err
	}
	defer tx.unlockActiveCore()
	if err := tx.g.validateRelTypeQueryName(typeName); err != nil {
		return 0, err
	}
	return tx.g.countByTypeLocked(typeName)
}

// --- TempOps mirrors ---

// NodesAt is a tx-side mirror for g.Temporal.NodesAt.
func (tx *GraphTx) NodesAt(at types.Instant) ([]*types.Node, error) {
	if err := tx.lockActiveCore(); err != nil {
		return nil, err
	}
	defer tx.unlockActiveCore()
	return tx.g.nodesAtLocked(at)
}

// RelsAt is a tx-side mirror for g.Temporal.RelsAt.
func (tx *GraphTx) RelsAt(at types.Instant) ([]*types.Relationship, error) {
	if err := tx.lockActiveCore(); err != nil {
		return nil, err
	}
	defer tx.unlockActiveCore()
	return tx.g.relationshipsAtLocked(at)
}

// NodesByLabelAt is a tx-side mirror for g.Temporal.NodesByLabelAt.
func (tx *GraphTx) NodesByLabelAt(label string, at types.Instant) ([]*types.Node, error) {
	if err := tx.lockActiveCore(); err != nil {
		return nil, err
	}
	defer tx.unlockActiveCore()
	if err := tx.g.validateIndexLabel(label); err != nil {
		return nil, err
	}
	return tx.g.nodesByLabelAtLocked(label, at)
}

// NodesDuring is a tx-side mirror for g.Temporal.NodesDuring.
func (tx *GraphTx) NodesDuring(start, end types.Instant) ([]*types.Node, error) {
	if err := tx.lockActiveCore(); err != nil {
		return nil, err
	}
	defer tx.unlockActiveCore()
	resolvedEnd, err := normalizeDuringRange(start, end)
	if err != nil {
		return nil, err
	}
	return tx.g.nodesDuringLocked(start, resolvedEnd)
}

// RelsDuring is a tx-side mirror for g.Temporal.RelsDuring.
func (tx *GraphTx) RelsDuring(start, end types.Instant) ([]*types.Relationship, error) {
	if err := tx.lockActiveCore(); err != nil {
		return nil, err
	}
	defer tx.unlockActiveCore()
	resolvedEnd, err := normalizeDuringRange(start, end)
	if err != nil {
		return nil, err
	}
	return tx.g.relsDuringLocked(start, resolvedEnd)
}

// NodeAt is a tx-side mirror for g.Temporal.NodeAt.
func (tx *GraphTx) NodeAt(id types.NodeID, at types.Instant) (*types.Node, error) {
	if err := tx.lockActiveCore(); err != nil {
		return nil, err
	}
	defer tx.unlockActiveCore()
	if err := storepkg.ValidateNodeID(id); err != nil {
		return nil, err
	}
	return tx.g.nodeAtLocked(id, at)
}

// RelAt is a tx-side mirror for g.Temporal.RelAt.
func (tx *GraphTx) RelAt(id types.RelID, at types.Instant) (*types.Relationship, error) {
	if err := tx.lockActiveCore(); err != nil {
		return nil, err
	}
	defer tx.unlockActiveCore()
	if err := storepkg.ValidateRelID(id); err != nil {
		return nil, err
	}
	return tx.g.relAtLocked(id, at)
}

// NeighborsAt is a tx-side mirror for g.Temporal.NeighborsAt.
func (tx *GraphTx) NeighborsAt(nodeID types.NodeID, at types.Instant) ([]*types.Node, error) {
	if err := tx.lockActiveCore(); err != nil {
		return nil, err
	}
	defer tx.unlockActiveCore()
	if err := storepkg.ValidateNodeID(nodeID); err != nil {
		return nil, err
	}
	return tx.g.neighborsAtLocked(nodeID, at)
}

// OutgoingRelsAt is a tx-side mirror for g.Temporal.OutgoingRelsAt.
func (tx *GraphTx) OutgoingRelsAt(nodeID types.NodeID, at types.Instant) ([]*types.Relationship, error) {
	if err := tx.lockActiveCore(); err != nil {
		return nil, err
	}
	defer tx.unlockActiveCore()
	if err := storepkg.ValidateNodeID(nodeID); err != nil {
		return nil, err
	}
	return tx.g.directionalRelsAtLocked(nodeID, at, true)
}

// IncomingRelsAt is a tx-side mirror for g.Temporal.IncomingRelsAt.
func (tx *GraphTx) IncomingRelsAt(nodeID types.NodeID, at types.Instant) ([]*types.Relationship, error) {
	if err := tx.lockActiveCore(); err != nil {
		return nil, err
	}
	defer tx.unlockActiveCore()
	if err := storepkg.ValidateNodeID(nodeID); err != nil {
		return nil, err
	}
	return tx.g.directionalRelsAtLocked(nodeID, at, false)
}

// NodesByLabelPropertyAt is a tx-side mirror for g.Temporal.NodesByLabelPropertyAt.
func (tx *GraphTx) NodesByLabelPropertyAt(label, key string, value any, at types.Instant) ([]*types.Node, error) {
	if err := tx.lockActiveCore(); err != nil {
		return nil, err
	}
	defer tx.unlockActiveCore()
	if err := tx.g.validateIndexLabel(label); err != nil {
		return nil, err
	}
	if err := tx.g.validateIndexPropertyKey(key); err != nil {
		return nil, err
	}
	if err := types.ValidatePropertyValue(value); err != nil {
		return nil, err
	}
	return tx.g.nodesByLabelPropertyAtLocked(label, key, value, at)
}

// NodesByLabelPropertyDuring is a tx-side mirror for g.Temporal.NodesByLabelPropertyDuring.
func (tx *GraphTx) NodesByLabelPropertyDuring(label, key string, value any, start, end types.Instant) ([]*types.Node, error) {
	if err := tx.lockActiveCore(); err != nil {
		return nil, err
	}
	defer tx.unlockActiveCore()
	resolvedEnd, err := normalizeDuringRange(start, end)
	if err != nil {
		return nil, err
	}
	if err := tx.g.validateIndexLabel(label); err != nil {
		return nil, err
	}
	if err := tx.g.validateIndexPropertyKey(key); err != nil {
		return nil, err
	}
	if err := types.ValidatePropertyValue(value); err != nil {
		return nil, err
	}
	return tx.g.nodesByLabelPropertyDuringLocked(label, key, value, start, resolvedEnd)
}

// RelsByTypePropertyAt is a tx-side mirror for g.Temporal.RelsByTypePropertyAt.
func (tx *GraphTx) RelsByTypePropertyAt(relType, key string, value any, at types.Instant) ([]*types.Relationship, error) {
	if err := tx.lockActiveCore(); err != nil {
		return nil, err
	}
	defer tx.unlockActiveCore()
	if err := tx.g.validateRelTypeQueryName(relType); err != nil {
		return nil, err
	}
	if err := tx.g.validateIndexPropertyKey(key); err != nil {
		return nil, err
	}
	if err := types.ValidatePropertyValue(value); err != nil {
		return nil, err
	}
	return tx.g.relsByTypePropertyAtLocked(relType, key, value, at)
}

// RelsByTypePropertyDuring is a tx-side mirror for g.Temporal.RelsByTypePropertyDuring.
func (tx *GraphTx) RelsByTypePropertyDuring(relType, key string, value any, start, end types.Instant) ([]*types.Relationship, error) {
	if err := tx.lockActiveCore(); err != nil {
		return nil, err
	}
	defer tx.unlockActiveCore()
	resolvedEnd, err := normalizeDuringRange(start, end)
	if err != nil {
		return nil, err
	}
	if err := tx.g.validateRelTypeQueryName(relType); err != nil {
		return nil, err
	}
	if err := tx.g.validateIndexPropertyKey(key); err != nil {
		return nil, err
	}
	if err := types.ValidatePropertyValue(value); err != nil {
		return nil, err
	}
	return tx.g.relsByTypePropertyDuringLocked(relType, key, value, start, resolvedEnd)
}

// --- TempOps AsOf mirrors ---

// NodeAsOf is a tx-side mirror for g.Temporal.NodeAsOf.
func (tx *GraphTx) NodeAsOf(id types.NodeID, txTime types.Instant) (*types.Node, error) {
	if err := tx.lockActiveCore(); err != nil {
		return nil, err
	}
	defer tx.unlockActiveCore()
	if err := storepkg.ValidateNodeID(id); err != nil {
		return nil, err
	}
	return tx.g.nodeAsOfLocked(id, txTime)
}

// RelAsOf is a tx-side mirror for g.Temporal.RelAsOf.
func (tx *GraphTx) RelAsOf(id types.RelID, txTime types.Instant) (*types.Relationship, error) {
	if err := tx.lockActiveCore(); err != nil {
		return nil, err
	}
	defer tx.unlockActiveCore()
	if err := storepkg.ValidateRelID(id); err != nil {
		return nil, err
	}
	return tx.g.relAsOfLocked(id, txTime)
}

// NodesAsOf is a tx-side mirror for g.Temporal.NodesAsOf.
func (tx *GraphTx) NodesAsOf(txTime types.Instant) ([]*types.Node, error) {
	if err := tx.lockActiveCore(); err != nil {
		return nil, err
	}
	defer tx.unlockActiveCore()
	return tx.g.nodesAsOfLocked(txTime)
}

// RelsAsOf is a tx-side mirror for g.Temporal.RelsAsOf.
func (tx *GraphTx) RelsAsOf(txTime types.Instant) ([]*types.Relationship, error) {
	if err := tx.lockActiveCore(); err != nil {
		return nil, err
	}
	defer tx.unlockActiveCore()
	return tx.g.relsAsOfLocked(txTime)
}

// --- StatOps mirrors ---

// Stats is a tx-side mirror for g.Stats.Get.
func (tx *GraphTx) Stats() (GraphStats, error) {
	if err := tx.lockActiveCore(); err != nil {
		return GraphStats{}, err
	}
	defer tx.unlockActiveCore()
	return tx.g.statsLocked()
}

// AllLabelCounts is a tx-side mirror for g.Stats.AllLabelCounts.
func (tx *GraphTx) AllLabelCounts() (map[string]int, error) {
	if err := tx.lockActiveCore(); err != nil {
		return nil, err
	}
	defer tx.unlockActiveCore()
	return tx.g.allLabelCountsLocked()
}

// AllRelTypeCounts is a tx-side mirror for g.Stats.AllRelTypeCounts.
func (tx *GraphTx) AllRelTypeCounts() (map[string]int, error) {
	if err := tx.lockActiveCore(); err != nil {
		return nil, err
	}
	defer tx.unlockActiveCore()
	return tx.g.allRelTypeCountsLocked()
}

// --- IndexOps mirrors ---

// SearchNearest is a tx-side mirror for g.Index.SearchNearest.
func (tx *GraphTx) SearchNearest(label, propertyKey string, query []float32, k int, opts storepkg.QueryOpts) ([]*types.Node, error) {
	if err := tx.lockActiveCore(); err != nil {
		return nil, err
	}
	defer tx.unlockActiveCore()
	if err := tx.g.validateTemporalQueryOptsScan(opts); err != nil {
		return nil, err
	}
	if err := tx.g.validateIndexLabel(label); err != nil {
		return nil, err
	}
	if err := tx.g.validateIndexPropertyKey(propertyKey); err != nil {
		return nil, err
	}
	return tx.g.searchNearestLocked(label, propertyKey, query, k, opts)
}

// IndexProviders is a tx-side mirror for g.Index.Providers.
func (tx *GraphTx) IndexProviders() []string {
	if tx == nil || tx.g == nil {
		return nil
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return nil
	}
	out := make([]string, 0, len(tx.g.indexProviders))
	for name := range tx.g.indexProviders {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// --- AdminOps / EventOps / ConstraintOps mirrors ---

// ListShards is a tx-side mirror for g.Tier.ListShards.
func (tx *GraphTx) ListShards() ([]tiered.ShardInfo, error) {
	if err := tx.lockActiveCore(); err != nil {
		return nil, err
	}
	defer tx.unlockActiveCore()
	if ts, ok := tx.g.store.(tieredAdminStore); ok {
		return ts.ListShards()
	}
	return nil, ErrNotTieredStore
}

// EventBus is a tx-side mirror for g.Events.GetSync.
func (tx *GraphTx) EventBus() *eventspkg.EventBus {
	if tx == nil || tx.g == nil {
		return nil
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return nil
	}
	if eb, ok := tx.g.events.(*eventspkg.EventBus); ok {
		return eb
	}
	return nil
}

// AsyncEventBus is a tx-side mirror for g.Events.GetAsync.
func (tx *GraphTx) AsyncEventBus() *eventspkg.AsyncEventBus {
	if tx == nil || tx.g == nil {
		return nil
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return nil
	}
	if eb, ok := tx.g.events.(*eventspkg.AsyncEventBus); ok {
		return eb
	}
	return nil
}

// Constraints is a tx-side mirror for g.Constraints.Get.
func (tx *GraphTx) Constraints() ConstraintSet {
	if tx == nil || tx.g == nil {
		return ConstraintSet{}
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return ConstraintSet{}
	}
	return tx.g.constraints
}

// _ = context.Background ensures the imported context package compiles even if
// no future tx-mirror method directly references it; the import is here so
// the file compiles regardless of which paths add ctx-aware tx mirrors next.
var _ = context.Background
