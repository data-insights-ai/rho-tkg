package store

import (
	"errors"
	"fmt"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Capability interfaces: narrowed views of the full Store contract, all
// embedded into Store (store.go). Optional capabilities are type-asserted by
// consumers before use; MandatoryStore lists the required ones.

// Lifecycle is the always-mandatory housekeeping subset every backend must
// implement. Close releases resources; Clear truncates without removing
// registries (a graph-layer concern). Nil concrete store receivers return
// ErrNilStore from lifecycle methods rather than panicking.
type Lifecycle interface {
	// Clear removes all entities, indexes, history, and counters.
	// Registries (label/reltype tokens) are a graph-layer concern and
	// untouched.
	Clear() error
	// Close releases any resources held by the store. Idempotent.
	Close() error
}

// NodeCRUDCapability is the node-mutation surface (mandatory), including the
// label-mutation helpers; they validate that the supplied current row adds or
// removes exactly the requested token.
type NodeCRUDCapability interface {
	PutNode(n *types.Node) error
	GetNode(id types.NodeID) (*types.Node, error)
	ReplaceNode(n *types.Node) error
	// DeleteNode removes only an unconnected node row. Backends return
	// ErrInvalidStoreMutation instead of orphaning connected relationships.
	DeleteNode(id types.NodeID) error

	// DeleteNodeCascade removes the node and every relationship it
	// participates in. Used when the graph layer needs a single-call
	// delete that the store can implement atomically.
	DeleteNodeCascade(id types.NodeID) error

	RemoveNodeLabelToken(id types.NodeID, tok uint16, updatedNode *types.Node) error
	AddNodeLabelToken(id types.NodeID, tok uint16, updatedNode *types.Node) error
}

// NodeIntegrityHashCapability is OPTIONAL: a live node's integrity hash without
// the full node. It still validates that the node exists.
type NodeIntegrityHashCapability interface {
	NodeIntegrityHash(id types.NodeID) (string, error)
}

// EndpointIntegrityHashCapability is OPTIONAL: both endpoint hashes under one read window.
type EndpointIntegrityHashCapability interface {
	EndpointIntegrityHashes(startID, endID types.NodeID) (string, string, error)
}

// EntityLendCapability is OPTIONAL. LendNode / LendRelationship return the
// store's current row by ID WITHOUT the defensive copy: the shared, FROZEN
// pointer the plural reads hand out, a snapshot at the read that later writes
// do not refresh. Frozen mutators fail (ErrFrozenNode / ErrFrozenRelationship, or
// panic for the no-error ones); DeepCopy thaws. The store never mutates a lent row
// (a write replaces it). Errors: typed not-found, ErrInvalidStoreMutation for a
// malformed ID, ErrStoreClosed.
type EntityLendCapability interface {
	LendNode(id types.NodeID) (*types.Node, error)
	LendRelationship(id types.RelID) (*types.Relationship, error)
}

// EntityLendBatchCapability is OPTIONAL: EntityLendCapability for many IDs
// in one read. LendNodesSorted sets out[i] to the row LendNode(ids[i]) lends,
// or nil where the entity does not exist; ids are ascending (duplicates
// allowed), already validated, and len(out) == len(ids). Any other read
// error aborts the call. Badger serves a call from one read snapshot (rows
// missing from its cache are read in key order and not cached); memory under
// one read lock. LendRelationshipsSorted is the relationship mirror.
type EntityLendBatchCapability interface {
	LendNodesSorted(ids []types.NodeID, out []*types.Node) error
	LendRelationshipsSorted(ids []types.RelID, out []*types.Relationship) error
}

// ScanOrderCapability is OPTIONAL: whether the streaming label and type scans
// (ForEachNodeByLabel, ForEachRelByType) walk members in ascending ID order from
// a list kept between scans (true: no per-scan collect or sort) or collect and
// sort per scan unless NoSort (false). A store without it is assumed to sort.
type ScanOrderCapability interface {
	LabelScanKeepsOrder(labelToken uint16) bool
	TypeScanKeepsOrder(relTypeToken uint16) bool
}

// AdjacentEndpointOrdinalCapability is OPTIONAL: ForEachAdjacentEndpoint with the
// dense ordinals (OrdinalCapability) of each relationship and its other endpoint;
// 0 where none is numbered or the other endpoint has no current row any more. Order and isolation as ForEachAdjacentEndpoint.
type AdjacentEndpointOrdinalCapability interface {
	ForEachAdjacentEndpointOrdinal(nid types.NodeID, typeToken uint16, incoming bool,
		fn func(rel types.RelID, relOrdinal uint32, other types.NodeID, otherOrdinal uint32) bool) error
}

// ReadCosts states what a store's read doors cost through the graph, in
// nanoseconds per row: nominal magnitudes for a planner to price access paths
// per backend, not a promise. "Held" rows stay decoded in RAM, "decoded" rows
// are read from the backing store. HeldRows: -1 every row, 0 a byte budget
// governs, n up to n rows per kind (per shard on tiered and sharded).
type ReadCosts struct {
	Backend  string // "memory", "badger", "tiered", "sharded"
	HeldRows int

	LendHeld, LendDecoded                 float64 // a by-ID read (Lend)
	ScanRowHeld, ScanRowDecoded           float64 // a streaming label or type scan, per row
	AdjacencyRelHeld, AdjacencyRelDecoded float64 // ForEachOutgoing / ForEachIncoming, per relationship
}

// HistoryCounts is how many nodes and relationships have history rows (the IDs
// ForEachNodeHistoryID / ForEachRelHistoryID visit, deleted entities included).
// A temporal read by property value resolves every node with history, so a
// planner prices it with Nodes.
type HistoryCounts struct {
	Nodes int
	Rels  int
}

// HistoryCountCapability is OPTIONAL: the store's HistoryCounts, exact, without
// a pass per call. Tiered states none (an archive or restore could be counted twice).
type HistoryCountCapability interface {
	NodeHistoryCount() (int, error)
	RelHistoryCount() (int, error)
}

// HistoryPresenceCapability is OPTIONAL: whether one entity has at least one
// history row, without reading the rows. Equals len(GetNodeHistory(id)) > 0
// (len(GetRelHistory(id)) > 0) at every moment, pending writes included; an
// invalid id returns the same error.
type HistoryPresenceCapability interface {
	HasNodeHistory(id types.NodeID) (bool, error)
	HasRelHistory(id types.RelID) (bool, error)
}

// ReadCostCapability is OPTIONAL: the store's ReadCosts. A store without it
// states none.
type ReadCostCapability interface {
	ReadCosts() ReadCosts
}

// RelationshipCRUDCapability is the relationship-mutation surface.
type RelationshipCRUDCapability interface {
	PutRelationship(r *types.Relationship) error
	GetRelationship(id types.RelID) (*types.Relationship, error)
	ReplaceRelationship(r *types.Relationship) error
	DeleteRelationship(id types.RelID) error
}

// AdjacencyCapability is the outgoing/incoming relationship lookup surface plus
// the batch ForNodes variants (mandatory). Explicit node IDs are all-or-error:
// a missing requested node returns ErrNodeNotFound; an existing node without
// matches yields an empty result.
type AdjacencyCapability interface {
	OutgoingRelationships(nodeID types.NodeID, typeToken uint16) ([]*types.Relationship, error)
	IncomingRelationships(nodeID types.NodeID, typeToken uint16) ([]*types.Relationship, error)

	OutgoingRelationshipsForNodes(nodeIDs []types.NodeID, typeToken uint16) (map[types.NodeID][]*types.Relationship, error)
	IncomingRelationshipsForNodes(nodeIDs []types.NodeID, typeToken uint16) (map[types.NodeID][]*types.Relationship, error)
}

// DegreeCapability is OPTIONAL: a node's type-filtered relationship count from
// the adjacency index, without materializing. Equals the length of the matching
// AdjacencyCapability result except for transient crash-induced orphan index
// entries, which repair/restart reconciles. Without it the graph layer uses
// len(Outgoing/IncomingRelationships).
type DegreeCapability interface {
	OutgoingDegree(nodeID types.NodeID, typeToken uint16) (int, error)
	IncomingDegree(nodeID types.NodeID, typeToken uint16) (int, error)
}

// BulkReadCapability is the unfiltered/label-filtered listing surface (export,
// snapshot, the ByLabel / ByType query shapes).
type BulkReadCapability interface {
	NodesByLabel(token uint16, opts QueryOpts) ([]*types.Node, error)
	RelationshipsByType(token uint16, opts QueryOpts) ([]*types.Relationship, error)

	AllNodes(opts QueryOpts) ([]*types.Node, error)
	AllRelationships(opts QueryOpts) ([]*types.Relationship, error)
	GetNodesByIDs(ids []types.NodeID) ([]*types.Node, error)
	GetRelationshipsByIDs(ids []types.RelID) ([]*types.Relationship, error)
}

// BatchCapability is the batched-mutation surface (BatchBuilder, import).
// All-or-nothing: empty/nil input returns nil. Delete batches treat duplicate IDs
// as one deletion. DeleteNodesBatch removes only unconnected node rows. Duplicate
// IDs are coalesced before validation and counter/index updates. Connected node
// deletes must use graph-level cascade or history-aware delete flows.
type BatchCapability interface {
	PutNodesBatch(nodes []*types.Node) error
	PutRelationshipsBatch(rels []*types.Relationship) error
	DeleteNodesBatch(ids []types.NodeID) error
	DeleteRelationshipsBatch(ids []types.RelID) error
}

// PreEncodedPutCapability is OPTIONAL: the ingest apply-side fast path (ADR-0006
// §4.5). PutNodesBatchPreEncoded behaves as PutNodesBatch for nodes whose v2
// entity-row wire was pre-encoded (transaction-time tail patched with the
// applier's TxFrom), skipping the second msgpack pass. wireBodies[i] nil means
// "re-encode node i"; a backend MUST then treat it exactly as PutNodesBatch, so
// persisted bytes are identical either way. Provenance is the typed in-process
// buffer, never sniffed from stored bytes. Only the native memory and badger
// stores implement it; the change-log put body stays the untokenized
// encode-at-flush form (feed parity).
type PreEncodedPutCapability interface {
	PutNodesBatchPreEncoded(nodes []*types.Node, wireBodies [][]byte) error
}

// PreEncodedPutLogCapability is OPTIONAL: the change-log extension of
// PreEncodedPutCapability. logBodies[i], when non-nil, is the pre-encoded
// ChangeNodePut payload for nodes[i] and MUST be used verbatim as the record
// body; nil means "build at the door" (byte-identical result). With the
// change-log off it behaves as PutNodesBatchPreEncoded.
type PreEncodedPutLogCapability interface {
	PutNodesBatchPreEncodedLog(nodes []*types.Node, wireBodies, logBodies [][]byte) error
}

// OwnedPreEncodedPutCapability is OPTIONAL: PutNodesBatchPreEncodedLog with
// ownership transfer. The caller never reads or mutates the nodes again, so the
// store MAY freeze them in place and cache them without a deep copy. Behaviour
// is undefined if the caller touches a node afterwards; use only for write-only
// creates whose results are not returned (the ingest bulk door, gated by the
// graph layer). A pure allocation optimization.
type OwnedPreEncodedPutCapability interface {
	PutNodesBatchOwnedPreEncoded(nodes []*types.Node, wireBodies, logBodies [][]byte) error
}

// HistoryCapability is the version-history surface (mandatory). The atomic
// ReplaceWith*History and Delete*WithHistory methods avoid a crash window between
// the history write and the data write. DeleteNodeWithHistory relTombstones must
// cover every live connected relationship exactly once.
type HistoryCapability interface {
	ReplaceNodeWithHistory(current *types.Node, prevVersion uint32, prevState *types.Node) error
	ReplaceRelWithHistory(current *types.Relationship, prevVersion uint32, prevState *types.Relationship) error

	PutNodeVersion(id types.NodeID, version uint32, n *types.Node) error
	GetNodeVersion(id types.NodeID, version uint32) (*types.Node, error)
	GetNodeHistory(id types.NodeID) ([]*types.Node, error)
	TruncateNodeHistory(id types.NodeID, keepVersions int) error

	PutRelVersion(id types.RelID, version uint32, r *types.Relationship) error
	GetRelVersion(id types.RelID, version uint32) (*types.Relationship, error)
	GetRelHistory(id types.RelID) ([]*types.Relationship, error)
	TruncateRelHistory(id types.RelID, keepVersions int) error

	RemoveNodeLabelTokenWithHistory(id types.NodeID, tok uint16, updatedNode *types.Node,
		prevVersion uint32, prevState *types.Node) error
	AddNodeLabelTokenWithHistory(id types.NodeID, tok uint16, updatedNode *types.Node,
		prevVersion uint32, prevState *types.Node) error

	DeleteNodeWithHistory(id types.NodeID, prevNodeVersion uint32, nodeTombstone *types.Node, relTombstones []RelTombstone) error
	DeleteRelWithHistory(id types.RelID, prevVersion uint32, tombstone *types.Relationship) error
}

// TransactionTimeQueryCapability is OPTIONAL: transaction-time reads under one
// read window without materializing every history slice. Single-entity misses
// return ErrVersionNotFound; bulk queries omit misses and return nil, nil when
// nothing existed at txTime.
type TransactionTimeQueryCapability interface {
	NodeAsOf(id types.NodeID, txTime types.Instant) (*types.Node, error)
	RelAsOf(id types.RelID, txTime types.Instant) (*types.Relationship, error)
	NodesAsOf(txTime types.Instant) ([]*types.Node, error)
	RelsAsOf(txTime types.Instant) ([]*types.Relationship, error)
}

// LabelTxMembershipCapability is OPTIONAL: enumerates the node IDs that EVER
// carried a label (current row or any version), each with firstTxFrom, a lower
// bound on the transaction time of its earliest acquisition, so a pinned label
// scan costs O(matches) instead of the whole history fold.
//
// Membership is append-only (removing the label or deleting the node keeps the
// member): the set is a sound superset, the chain resolver stays the authority
// and rejects over-included candidates. firstTxFrom 0 = unknown (never prune);
// pruning is sound only as `pin < firstTxFrom -> skip`.
type LabelTxMembershipCapability interface {
	// ForEachLabelTxMember streams (id, firstTxFrom) for every node that ever carried
	// token; fn returning false stops. Order unspecified. The sidecar is built lazily
	// on first use (after open or Clear).
	ForEachLabelTxMember(token uint16, fn func(id types.NodeID, firstTxFrom types.Instant) bool) error
}

// RelTypeTxMembershipCapability is the relationship mirror of
// LabelTxMembershipCapability; a rel's type is immutable, so its ever-members are
// the rels ever created with the type. Same superset and prune contract.
type RelTypeTxMembershipCapability interface {
	ForEachRelTypeTxMember(token uint16, fn func(id types.RelID, firstTxFrom types.Instant) bool) error
}

// LabelTxMemberLookupCapability is OPTIONAL: the point form of
// LabelTxMembershipCapability over the same sidecar: for each of ids that ever
// carried token, fn receives (id, firstTxFrom); others are skipped. Order
// unspecified; fn returning false stops.
type LabelTxMemberLookupCapability interface {
	LabelTxMembersOf(token uint16, ids []types.NodeID, fn func(id types.NodeID, firstTxFrom types.Instant) bool) error
}

// RelTypeTxMemberLookupCapability is the relationship mirror of
// LabelTxMemberLookupCapability over RelTypeTxMembershipCapability's sidecar.
type RelTypeTxMemberLookupCapability interface {
	RelTypeTxMembersOf(token uint16, ids []types.RelID, fn func(id types.RelID, firstTxFrom types.Instant) bool) error
}

// RelPropertyTxMembershipCapability is OPTIONAL (backlog 8): for a declared
// relationship property index (relType, key) and one canonical value key
// (types.IndexablePropertyValueKey), enumerates the relationships whose rows EVER
// carried that value (current or history, deleted included) with a lower bound
// on the earliest transaction time.
//
// Same contract as RelTypeTxMembershipCapability: append-only sound superset,
// firstTxFrom 0 = unknown. Returns store.ErrIndexNotFound, before calling fn,
// when no rel property index is declared (or it is still being created); the
// caller then folds the full history. The sidecar is built lazily per
// (relType, key) and maintained at every row write; fn runs outside store locks
// and may re-enter the store; returning false stops. Order unspecified.
type RelPropertyTxMembershipCapability interface {
	ForEachRelPropertyTxMember(relTypeToken uint16, propertyKey, valueKey string, fn func(id types.RelID, firstTxFrom types.Instant) bool) error
}

// NodePropertyTxMembershipCapability is the node twin of
// RelPropertyTxMembershipCapability, keyed (label, key): nodes whose rows EVER
// carried the label and the value in the same row. Same contract, including
// ErrIndexNotFound.
type NodePropertyTxMembershipCapability interface {
	ForEachNodePropertyTxMember(labelToken uint16, propertyKey, valueKey string, fn func(id types.NodeID, firstTxFrom types.Instant) bool) error
}

// RelPropertyTxMemberWindowCapability is OPTIONAL (ledger row 62): the members
// RelPropertyTxMembershipCapability enumerates for one value, narrowed to those
// whose rows carrying the value may be valid somewhere in [validStart,
// validEnd) as read at transaction time txAt (a TxAt pin; 0 = no pin, every
// row) — a sound superset of the members a valid-time lookup at that pin can
// match — in about O(log n + k) instead of the whole value. validStart >=
// validEnd answers nothing. Same contract otherwise (append-only sound
// superset, firstTxFrom 0 = unknown, store.ErrIndexNotFound before fn when no
// index is declared, each member once, order unspecified); fn runs outside
// store locks.
type RelPropertyTxMemberWindowCapability interface {
	ForEachRelPropertyTxMemberDuring(relTypeToken uint16, propertyKey, valueKey string, validStart, validEnd, txAt types.Instant, fn func(id types.RelID, firstTxFrom types.Instant) bool) error
}

// NodePropertyTxMemberWindowCapability is the node twin of
// RelPropertyTxMemberWindowCapability, keyed (label, key) like
// NodePropertyTxMembershipCapability.
type NodePropertyTxMemberWindowCapability interface {
	ForEachNodePropertyTxMemberDuring(labelToken uint16, propertyKey, valueKey string, validStart, validEnd, txAt types.Instant, fn func(id types.NodeID, firstTxFrom types.Instant) bool) error
}

// RelPropertyTxMemberPageCapability is OPTIONAL (backlog 69): the members
// RelPropertyTxMembershipCapability enumerates for one value, in ascending ID
// order and resumed above a cursor — at most limit members with ID > after —
// so a keyset-paged read costs about one page, not the whole value. Same
// contract otherwise (append-only sound superset, firstTxFrom 0 = unknown,
// store.ErrIndexNotFound before fn when no index is declared); fn runs
// outside store locks.
type RelPropertyTxMemberPageCapability interface {
	ForEachRelPropertyTxMemberAfter(relTypeToken uint16, propertyKey, valueKey string, after types.RelID, limit int, fn func(id types.RelID, firstTxFrom types.Instant) bool) error
}

// NodePropertyTxMemberPageCapability is the node twin of
// RelPropertyTxMemberPageCapability, keyed (label, key).
type NodePropertyTxMemberPageCapability interface {
	ForEachNodePropertyTxMemberAfter(labelToken uint16, propertyKey, valueKey string, after types.NodeID, limit int, fn func(id types.NodeID, firstTxFrom types.Instant) bool) error
}

// PropertyTxMembershipStats reports the property membership sidecars (backlog 8):
// sidecars built, postings held, completed lazy builds since open and their
// summed wall time. On badger (and per slot on sharded) one build is one store
// scan that builds every declared sidecar of its kind; sidecars restored from
// the open image count no build (backlog 74). Memory builds one per sidecar.
type PropertyTxMembershipStats struct {
	RelSidecars   int
	NodeSidecars  int
	RelPostings   int64
	NodePostings  int64
	Builds        int64
	BuildDuration time.Duration
}

// PropertyTxMembershipStatsCapability is OPTIONAL: sidecar sizes and build cost.
type PropertyTxMembershipStatsCapability interface {
	PropertyTxMembershipStats() (PropertyTxMembershipStats, error)
}

// NodeBeliefWatermarkCapability is OPTIONAL (backlog 10c): the maximum TxFrom ever
// recorded across a node's entire version chain (current row plus every history
// row). When the current row's TxFrom equals it, current is the newest belief in
// the chain, so a point-in-time query may skip the history fetch. A bounded
// bitemporal correction can append a history row newer than an untouched current
// row, which is why the check is explicit.
//
// (0, false) means unknown (sidecar not built, or entity not written since the
// last build): the caller MUST fall back to the full scan, never read it as 0.
// Lazy, RAM-only, rebuilt after reopen, maintained at every door that persists a
// row with a TxFrom (max is idempotent, so rollback restores are no-ops).
type NodeBeliefWatermarkCapability interface {
	NodeBeliefWatermark(id types.NodeID) (types.Instant, bool)
}

// RelBeliefWatermarkCapability mirrors NodeBeliefWatermarkCapability for relationships.
type RelBeliefWatermarkCapability interface {
	RelBeliefWatermark(id types.RelID) (types.Instant, bool)
}

// VersionTemporalMeta is one history version's selection-scope temporal skeleton
// (version plus numeric instants). Temporal is nil when the row had no temporal
// block. CreatedBy/UpdatedBy/BaseEntityID are absent: hydrate the full row
// (GetNodeVersion/GetRelVersion) before returning a version to a caller.
type VersionTemporalMeta struct {
	Version  uint32
	Temporal *types.TemporalMetadata
}

// TemporalMetaHistoryCapability is OPTIONAL: an entity's history temporal metadata
// without materializing full rows, so historical-pin resolution decodes only the
// winner. Ascending by version, every history row (tombstones included), exactly
// GetNodeHistory/GetRelHistory's row set. Without it the graph layer falls back
// to the full fetch with identical results.
type TemporalMetaHistoryCapability interface {
	NodeHistoryTemporalMeta(id types.NodeID) ([]VersionTemporalMeta, error)
	RelHistoryTemporalMeta(id types.RelID) ([]VersionTemporalMeta, error)
}

// HistoryEpochCapability is OPTIONAL: a token per entity that changes whenever
// that entity's history rows change, so a reader can keep a value derived from
// the rows (the graph's head-pin effective timeline, backlog 68) and tell in
// O(1) whether it still holds. NodeHistoryEpoch returns a nonzero token; two
// calls on one open store return the same token only if no history row of the
// ID was written, overwritten or removed between them. Tokens are never
// reused within a process (a reopened store or another shard hands out new
// ones). 0 means "no token": the caller must not reuse a derived value. The
// current row is not covered.
type HistoryEpochCapability interface {
	NodeHistoryEpoch(id types.NodeID) (uint64, error)
	RelHistoryEpoch(id types.RelID) (uint64, error)
}

// HistoryRollbackTrimCapability is OPTIONAL: transaction rollback without deep
// copies of history chains. Superseded versions are appended at the previous
// Version(), so rollback restores the current row and trims history at or after it.
type HistoryRollbackTrimCapability interface {
	TrimNodeHistoryFrom(id types.NodeID, minVersion uint32) error
	TrimRelHistoryFrom(id types.RelID, minVersion uint32) error
}

// MetaKVCapability is OPTIONAL: persisted key/value metadata (schema versions,
// migration markers). The key namespace is shared with backend entries, so
// callers MUST use distinct keys. MetaGet on a missing key returns nil, nil.
type MetaKVCapability interface {
	MetaGet(key string) ([]byte, error)
	MetaSet(key string, value []byte) error
}

// MetaWrite is one key/value entry for HistoryCompactionCapability: Key is the
// graph-layer meta key (MetaKVCapability namespace); a nil Value deletes it.
type MetaWrite struct {
	Key   string
	Value []byte
}

// ExactErasureBounds makes closure discovery finite: every relationship row and
// version inspected counts against MaxRelationshipVersions (the global scan, not
// only matches), every distinct relationship touching a declared node against
// MaxRelationshipIdentities, every distinct endpoint against MaxEndpointNodeIdentities.
// All must be positive for node erasure.
type ExactErasureBounds struct {
	MaxRelationshipIdentities int
	MaxRelationshipVersions   int
	MaxEndpointNodeIdentities int
}

// ExactErasureClosureRequest asks for every relationship identity whose current
// row or any version references NodeIDs (canonical: ascending, duplicate-free).
type ExactErasureClosureRequest struct {
	NodeIDs []types.NodeID
	Bounds  ExactErasureBounds
}

// ExactErasureRelationshipBinding is one current or historical relationship
// version touching the requested nodes, with endpoints and type so the semantic
// owner can classify the boundary. Version is the persisted version, not an ordering.
type ExactErasureRelationshipBinding struct {
	RelationshipID types.RelID
	TypeToken      uint16
	StartNodeID    types.NodeID
	EndNodeID      types.NodeID
	Version        uint32
	IntegrityHash  string
}

// ExactErasureClosure is the bounded historical endpoint proof for NodeIDs.
// RelationshipIDs and EndpointNodeIDs are canonical; Bindings are canonical by
// relationship, version, type and endpoints. EndpointNodeIDs is evidence, not a
// deletion request. Bindings keep every distinct matching persisted shape;
// EndpointNodeIDs includes the requested nodes named by a matching binding.
type ExactErasureClosure struct {
	RelationshipIDs []types.RelID
	EndpointNodeIDs []types.NodeID
	Bindings        []ExactErasureRelationshipBinding
}

// ExactErasureRequest is the plan for one bounded, caller-scoped legal erasure.
// NodeIDs and RelIDs are canonical and positive. MetaWrites (UniqueForever
// ownership, compaction stubs) commit atomically with the removal.
type ExactErasureRequest struct {
	NodeIDs    []types.NodeID
	RelIDs     []types.RelID
	Bounds     ExactErasureBounds
	MetaWrites []MetaWrite
}

// ExactErasureResult counts live rows removed only; callers use the canonical
// request digest as the idempotency receipt.
type ExactErasureResult struct {
	NodesRemoved int
	RelsRemoved  int
}

// ExactErasureCapability is OPTIONAL. It hard-removes exactly the declared
// node/relationship IDs: current rows, every history row, index and membership
// residue, and the metadata writes, in one atomic commit without tombstones or
// change records.
//
// ExactErasureRelationshipClosure resolves the relationship set for planning;
// ExactErase recomputes it under the destructive write exclusion and fails
// closed with ErrExactErasureRelationshipEscape when a relationship touching a
// declared node is outside RelIDs. Either fails with ErrExactErasureClosureLimit
// before mutation when the bound is exhausted, and ExactErase with
// ErrExactErasureChangeLogRetained when any change-log material exists (even
// with recording off). Missing declared rows are success (idempotent retry).
type ExactErasureCapability interface {
	ExactErasureRelationshipClosure(ExactErasureClosureRequest) (ExactErasureClosure, error)
	ExactErase(ExactErasureRequest) (ExactErasureResult, error)
}

// HistoryCompactionCapability is OPTIONAL: trims an entity's oldest history
// (keepVersions as TruncateNodeHistory) AND persists the per-entity compaction
// stub (metaWrites). Memory and badger commit both in one atomic write. Tiered
// writes the stub BEFORE the trim (different shards), so a crash fails closed
// and an idempotent re-run repairs. The graph watermark is not part of
// metaWrites (core/compaction.go advanceCompactionWatermark). (Tiered: the chain
// is single-shard (B33) but the stub lives on the reference shard where MetaGet
// reads it; see tiered/tieredstore_compaction.go.)
type HistoryCompactionCapability interface {
	CompactNodeHistory(id types.NodeID, keepVersions int, metaWrites []MetaWrite) error
	CompactRelHistory(id types.RelID, keepVersions int, metaWrites []MetaWrite) error
}

// RetentionPurgeResult reports what one purge chunk removed. More is true when
// purgeable nodes may remain (the caller loops until false). PurgedNodeIDs lets a
// partitioned backend (sharded) sweep edges minted in another slot that point at
// a purged node; nil on single-machine backends. PurgedRels lists relationships
// touched, so tiered (split-write layout) can sweep residue on the other endpoint
// shard; nil on memory, unused by sharded.
type RetentionPurgeResult struct {
	NodesPurged   int
	RelsPurged    int
	More          bool
	PurgedNodeIDs []types.NodeID
	PurgedRels    []PurgedRel
}

// PurgedRel is the routing descriptor of a relationship a purge touched, enough
// to clean its residue on either endpoint's shard without re-reading the row.
type PurgedRel struct {
	ID        types.RelID
	TypeToken uint16
	StartID   types.NodeID
	EndID     types.NodeID
}

// RetentionPurgeCapability is OPTIONAL (ADR-0008 R2): hard-removes aged-out nodes
// of a label without tombstones.
//
// PurgeNodesByLabelBefore removes up to `chunk` nodes carrying labelToken whose
// immutable snowflake mint-time is < before, plus every connected relationship
// (both adjacency legs), all index entries and the entire version history. Each
// call is one atomic batch; the caller loops on More. It emits no per-entity
// change-log record: the graph layer emits one ChangeRangePurge and advances the
// retention watermark once the range is clean. Idempotent. The predicate is
// mint-time (clustered key range), not ValidFrom or a backfilled TxFrom.
type RetentionPurgeCapability interface {
	PurgeNodesByLabelBefore(labelToken uint16, before types.Instant, chunk int) (RetentionPurgeResult, error)
}

// RetentionPurgeByValidToCapability is OPTIONAL (ADR-0008 R5): the ByValidTo
// sibling of RetentionPurgeCapability. Hard-removes nodes of a label whose
// current-version ValidTo != 0 && ValidTo < before, with the same cascade,
// history, atomic-batch and More contract. A closed entity is frozen against
// interactive mutation, so the predicate cannot change under a chunked
// selection; open nodes (ValidTo == 0) are never purged.
type RetentionPurgeByValidToCapability interface {
	PurgeNodesByLabelValidToBefore(labelToken uint16, before types.Instant, chunk int) (RetentionPurgeResult, error)
}

// RangePurgeLogCapability is OPTIONAL (ADR-0008 R3): appends one ChangeRangePurge
// record (the predicate a replica re-executes) to the change-log, co-committed;
// nil no-op when the log is off. Called once per purge.
type RangePurgeLogCapability interface {
	LogRangePurge(labelToken uint16, before types.Instant, mode uint8) error
}

// RelRetentionPurgeCapability is OPTIONAL (backlog 21), the relationship sibling
// of RetentionPurgeCapability. PurgeRelsByTypeBefore hard-removes up to `chunk`
// relationships of typeToken whose immutable snowflake mint-time is < before,
// live and ended alike: rows, both adjacency legs and ex-adjacency legs, index
// entries, counters, sidecars and the whole version history. Nodes are never
// touched. One atomic batch per call on a single-machine backend; reports
// RelsPurged, More and PurgedRels. It emits no change-log record (the graph layer
// owns ChangeRelRangePurge and the watermark). Idempotent. Candidates: the type's
// live rows plus IDs with history rows of the type and no live row; the removed
// history includes the tombstone.
type RelRetentionPurgeCapability interface {
	PurgeRelsByTypeBefore(typeToken uint16, before types.Instant, chunk int) (RetentionPurgeResult, error)
}

// RelRangePurgeLogCapability is OPTIONAL (backlog 21): appends one
// ChangeRelRangePurge record to the change-log; nil no-op when the log is off.
type RelRangePurgeLogCapability interface {
	LogRelRangePurge(typeToken uint16, before types.Instant, mode uint8) error
}

// HistoryVersionPageCapability is OPTIONAL: pages one entity's history chain for
// export and streaming readers. startVersion is inclusive, limit 0 returns all
// remaining; ascending by Version().
type HistoryVersionPageCapability interface {
	NodeHistoryVersionsFrom(id types.NodeID, startVersion uint32, limit int) ([]*types.Node, error)
	RelHistoryVersionsFrom(id types.RelID, startVersion uint32, limit int) ([]*types.Relationship, error)
}

// StatsCapability is the counter surface (g.Stats(), graph accounting).
type StatsCapability interface {
	NodeCount() (int, error)
	RelationshipCount() (int, error)
	NodeCountByLabel(token uint16) (int, error)
	RelCountByType(token uint16) (int, error)
}

// NodePropertyKeyStatsCapability is OPTIONAL: per-label property-key presence
// counts over current nodes with an indexable scalar value. Presence only, not
// value selectivity.
type NodePropertyKeyStatsCapability interface {
	NodeCountByLabelAndPropertyKey(labelToken uint16, propertyKey string) (int, error)
}

// IterationCapability is the ID iteration surface (ForEach + paginated AllIDs),
// no entity deserialisation. Nil ForEach callbacks return ErrInvalidStoreMutation;
// callbacks run outside backend locks, Badger transactions and Tiered shard
// checkouts, so they may call back into Store. History iterators must not let
// callback-created higher IDs extend the iteration; async-buffered stores make
// pending history deletes visible to ID scans before flush.
type IterationCapability interface {
	AllNodeIDs(opts QueryOpts) ([]types.NodeID, error)
	AllRelIDs(opts QueryOpts) ([]types.RelID, error)

	AllNodeHistoryIDs() ([]types.NodeID, error)
	AllRelHistoryIDs() ([]types.RelID, error)

	AllNodeHistoryIDsFrom(after types.NodeID, limit int) ([]types.NodeID, error)
	AllRelHistoryIDsFrom(after types.RelID, limit int) ([]types.RelID, error)

	ForEachNodeID(fn func(types.NodeID) bool) error
	ForEachRelID(fn func(types.RelID) bool) error
	ForEachNodeHistoryID(fn func(types.NodeID) bool) error
	ForEachRelHistoryID(fn func(types.RelID) bool) error
}

// DepthHistoryIterationCapability is OPTIONAL: tiered backends iterate history IDs
// filtered by shard depth. Single-shard backends omit it. Nil callbacks return
// ErrInvalidStoreMutation.
type DepthHistoryIterationCapability interface {
	ForEachNodeHistoryIDByDepth(depth ShardDepth, fn func(types.NodeID) bool) error
	ForEachRelHistoryIDByDepth(depth ShardDepth, fn func(types.RelID) bool) error
}

// DeletedIterationCapability is OPTIONAL: visits every ID with at least one
// history version and no current row (and no existing ID), so temporal queries
// fold only deleted IDs onto a narrow candidate list; without it the graph
// iterates all history. Nil callbacks return ErrInvalidStoreMutation; callbacks
// run outside backend locks.
type DeletedIterationCapability interface {
	ForEachDeletedNodeID(fn func(types.NodeID) bool) error
	ForEachDeletedRelID(fn func(types.RelID) bool) error
}

// ExAdjacencyCapability is OPTIONAL (backlog 50). A node's ex-adjacency holds every
// relationship ID that named it as start (incoming=false) or end (incoming=true)
// in a row the live adjacency does not tie to it (a deleted relationship, or an
// earlier life re-imported with other endpoints). The store maintains it in the
// same write as every relationship delete and every non-live-endpoint history
// row, so adjacency doors gather candidates in O(ever-degree), not O(history).
//
// A sound superset: entries may outlive their justification (compaction, history
// trim); retention purge and exact erasure remove them. NoteExAdjacentRel adds
// both legs of one row (one-time backfill). Nil callbacks return
// ErrInvalidStoreMutation; callbacks run outside locks; unordered; an ID may repeat.
type ExAdjacencyCapability interface {
	ForEachExAdjacentRelID(nodeID types.NodeID, incoming bool, fn func(types.RelID) bool) error
	NoteExAdjacentRel(id types.RelID, startID, endID types.NodeID) error
}

// RelCreateUndoCapability is OPTIONAL, beside ExAdjacencyCapability:
// UndoRelationshipCreate removes a relationship whose create is being undone
// (failed create, rollback, failed import). It is DeleteRelationship (same errors,
// same change-log record) without the ex-adjacency legs, since the relationship
// never committed. Without it DeleteRelationship is used (sound, one extra
// candidate per endpoint).
type RelCreateUndoCapability interface {
	UndoRelationshipCreate(id types.RelID) error
}

// ScopedRelCreateUndoCapability is UndoRelationshipCreate with the change-log
// record routed into a ScopedTxChangeLog buffer, as DeleteRelationshipScoped
// (token 0: UndoRelationshipCreate).
type ScopedRelCreateUndoCapability interface {
	UndoRelationshipCreateScoped(id types.RelID, token uint64) error
}

// DepthDeletedIterationCapability is the depth-aware DeletedIterationCapability
// (tiered). Nil callbacks return ErrInvalidStoreMutation.
type DepthDeletedIterationCapability interface {
	ForEachDeletedNodeIDByDepth(depth ShardDepth, fn func(types.NodeID) bool) error
	ForEachDeletedRelIDByDepth(depth ShardDepth, fn func(types.RelID) bool) error
}

// PropertyIndexCapability is OPTIONAL: property index management and equality
// lookup. Consumers type-assert and surface ErrCapabilityNotSupported when absent.
type PropertyIndexCapability interface {
	CreatePropertyIndex(labelToken uint16, propertyKey string) error
	DropPropertyIndex(labelToken uint16, propertyKey string) error
	NodesByLabelAndProperty(labelToken uint16, key string, value any, opts QueryOpts) ([]*types.Node, error)
}

// PropertyIndexOptions configures a single-key property index (node or
// relationship) at creation; the zero value is the plain index. Options are fixed
// at creation and persisted with the definition (see docs/api.md "Property index
// options" and "Elements index").
type PropertyIndexOptions struct {
	// RangeCounts keeps per-value multiplicity and per-chunk prefix sums so
	// RangeCardinality is O(log chunks) independent of range width, at a small
	// write and memory cost. Not available with badger's PropertyIndexOnDisk
	// (ErrCapabilityNotSupported).
	RangeCounts bool
	// Elements (backlog 63) indexes each scalar element of a list value, so an
	// equality lookup means "the list contains the value". A non-list value is a
	// one-element list; duplicates count once; empty lists, nil and non-scalar
	// elements add no key; []byte is a blob. No ordered view: range and prefix doors
	// decline. Refused with ErrElementsIndexUnsupported together with RangeCounts, a
	// unique constraint or GetOrCreateByKey on the key, and ByLabelAndProperties over
	// it; not available with PropertyIndexOnDisk (ErrCapabilityNotSupported).
	Elements bool
}

// ValidatePropertyIndexOptions refuses Elements with RangeCounts
// (ErrElementsIndexUnsupported).
func ValidatePropertyIndexOptions(opts PropertyIndexOptions) error {
	if opts.Elements && opts.RangeCounts {
		return fmt.Errorf("%w: Elements with RangeCounts", ErrElementsIndexUnsupported)
	}
	return nil
}

// PropertyIndexOptionsCapability is OPTIONAL: creates a node property index with
// PropertyIndexOptions and reads the options back. Without it the graph refuses
// non-zero options with ErrCapabilityNotSupported.
type PropertyIndexOptionsCapability interface {
	CreatePropertyIndexWithOptions(labelToken uint16, propertyKey string, opts PropertyIndexOptions) error
	// PropertyIndexOptions returns the options of the index on (labelToken,
	// propertyKey), or ok=false when there is none.
	PropertyIndexOptions(labelToken uint16, propertyKey string) (opts PropertyIndexOptions, ok bool, err error)
}

// RelPropertyIndexOptionsCapability is the relationship mirror of PropertyIndexOptionsCapability.
type RelPropertyIndexOptionsCapability interface {
	CreateRelPropertyIndexWithOptions(relTypeToken uint16, propertyKey string, opts PropertyIndexOptions) error
	RelPropertyIndexOptions(relTypeToken uint16, propertyKey string) (opts PropertyIndexOptions, ok bool, err error)
}

// RelPropertyIndexCapability is the relationship mirror of PropertyIndexCapability,
// keyed by rel-type token. OPTIONAL: without it the graph answers
// RelsByTypeAndProperty by type scan + filter. Every in-tree backend creates it
// (tiered and sharded on every shard since v4.58.0).
type RelPropertyIndexCapability interface {
	CreateRelPropertyIndex(relTypeToken uint16, propertyKey string) error
	DropRelPropertyIndex(relTypeToken uint16, propertyKey string) error
	RelationshipsByTypeAndProperty(relTypeToken uint16, key string, value any, opts QueryOpts) ([]*types.Relationship, error)
}

// CompositePropertyIndexCapability is OPTIONAL: O(matches) EQUALITY lookup across
// an ordered tuple of 2..4 property keys under one label (no prefix or range;
// every declared key needs a value). Planner guidance: docs/query-planners.md
// "Composite property indexes". Not embedded in Store: without it the graph
// answers NodesByLabelAndProperties by label scan + post-filter.
type CompositePropertyIndexCapability interface {
	// CreateCompositePropertyIndex creates a composite index over the ordered keys
	// (2..4) under labelToken. ErrIndexExists for the same ordered tuple; a different
	// order of the same key set is a distinct definition.
	CreateCompositePropertyIndex(labelToken uint16, keys []string) error
	// DropCompositePropertyIndex removes the index over the exact ordered keys;
	// ErrIndexNotFound if absent.
	DropCompositePropertyIndex(labelToken uint16, keys []string) error
	// NodesByLabelAndProperties returns nodes carrying labelToken whose current row
	// matches EVERY (key, value) pair (AND, equality). A matching index accelerates
	// the call only when values has all its keys; otherwise it falls back to label
	// scan + post-filter.
	NodesByLabelAndProperties(labelToken uint16, values map[string]any, opts QueryOpts) ([]*types.Node, error)
}

// CompositeIndexIntrospectionCapability is OPTIONAL: the planner existence door for
// composite definitions (without one, NodesByLabelAndProperties is a scan that can
// be worse than a single-key plan). Separate from CompositePropertyIndexCapability
// for source compatibility.
type CompositeIndexIntrospectionCapability interface {
	// ListCompositePropertyIndexes returns the declared ordered key tuple of every
	// composite index under labelToken (empty for unregistered labels, caller-owned
	// copies), O(definitions on the label). The graph's g.Index().InventoryEpoch is
	// the cache invalidation signal.
	ListCompositePropertyIndexes(labelToken uint16) ([][]string, error)
}

// PropertyIndexIntrospectionCapability is OPTIONAL (BACKLOG 21b): the planner
// existence door for single-key property index definitions.
type PropertyIndexIntrospectionCapability interface {
	// HasPropertyIndex reports whether a property index exists on (labelToken,
	// propertyKey), O(1); false for unregistered labels. Invalidation signal:
	// g.Index().InventoryEpoch.
	HasPropertyIndex(labelToken uint16, propertyKey string) (bool, error)
}

// TemporalIndexIntrospectionCapability is OPTIONAL (BACKLOG 21b): the planner
// existence door for temporal interval index definitions. Answers "interval kind"
// only; one temporal index kind (interval or high-frequency) exists per label.
type TemporalIndexIntrospectionCapability interface {
	// HasTemporalIndex reports whether a temporal interval index exists on labelToken;
	// false for unregistered labels.
	HasTemporalIndex(labelToken uint16) (bool, error)
}

// TemporalIndexListingCapability is OPTIONAL: the label and relationship-type
// tokens carrying a temporal interval index, ascending (SHOW INDEXES), instead of
// probing every label. A store without rel-type temporal indexes lists none.
type TemporalIndexListingCapability interface {
	TemporalIndexLabels() ([]uint16, error)
	RelTemporalIndexTypes() ([]uint16, error)
}

// VectorIndexInfo is the declared configuration of a vector index: Dims and
// Metric (fixed at creation) plus VectorIndexOptions.
type VectorIndexInfo struct {
	Dims    int
	Metric  DistanceMetric
	Options VectorIndexOptions
}

// VectorIndexIntrospectionCapability is OPTIONAL (BACKLOG 21b): the planner
// existence and configuration door for vector index definitions, so dimension
// mismatches and brute-force indexes are routing decisions.
type VectorIndexIntrospectionCapability interface {
	// VectorIndexInfo returns the declared configuration of the vector index on
	// (labelToken, propertyKey), or (zero, false). Definition only, no rebuild status
	// (AGENTS.md "Vector Indexes").
	VectorIndexInfo(labelToken uint16, propertyKey string) (VectorIndexInfo, bool, error)
}

// RelPropertyIndexIntrospectionCapability is OPTIONAL (BACKLOG 21b): the rel-side
// mirror of PropertyIndexIntrospectionCapability.
type RelPropertyIndexIntrospectionCapability interface {
	// HasRelPropertyIndex reports whether a relationship property index exists on
	// (relTypeToken, propertyKey); false for unregistered rel types.
	HasRelPropertyIndex(relTypeToken uint16, propertyKey string) (bool, error)
}

// PropertyTypeClassCounts is the exact per-(label, key) partition of a label's
// current nodes by the type class of the value (types.PropertyTypeClass). O(1)
// maintained counters, adjusted on the same mutation doors as the
// NodeCountByLabelAndPropertyKey presence counter, so they cannot drift. Each
// node counts in exactly one class per key it carries. Missing is computed by
// the graph layer (NodeCountByLabel minus the classes); stores return 0.
type PropertyTypeClassCounts struct {
	Numeric int64 // finite ints/uints/floats and ±Inf (orderable numbers)
	NaN     int64 // float NaN values (numeric kind, unorderable)
	String  int64
	Bool    int64
	Other   int64 // slices (incl. []float32/[]byte), maps, registered structs
	Missing int64 // graph-layer computed; always 0 at the store boundary
}

// Present returns the number of nodes carrying the key at all (any class).
func (c PropertyTypeClassCounts) Present() int64 {
	return c.Numeric + c.NaN + c.String + c.Bool + c.Other
}

// DocValuesColumn is what a label's DocValues snapshot holds for one property
// key, as nodes.API.DocValuesColumn states it without building the column.
type DocValuesColumn uint8

const (
	// DocValuesNone means no column: the values mix numbers and strings or hold
	// a bool, list, map or struct, the label is empty or over the column cap,
	// or the store has no column path. The DocValues doors decline (ok=false).
	DocValuesNone DocValuesColumn = iota
	// DocValuesNumeric is a numeric column (int64 and float64 kept apart; NaN
	// and ±Inf included). Also a key no member carries: every row absent.
	DocValuesNumeric
	// DocValuesString is a dictionary-encoded string column.
	DocValuesString
)

// DocValuesColumnOf is the DocValues build rule over a key's value-class counts on
// a label of labelCount members (TestDocValuesColumnAgreesWithTheBuild pins it to
// ForEachDocValues). maxRows is the column cap (index.MaxDocValuesNodes).
func DocValuesColumnOf(counts PropertyTypeClassCounts, labelCount, maxRows int) DocValuesColumn {
	switch {
	case labelCount <= 0 || labelCount > maxRows:
		return DocValuesNone
	case counts.Bool > 0 || counts.Other > 0:
		return DocValuesNone
	case counts.String > 0 && counts.Numeric+counts.NaN > 0:
		return DocValuesNone
	case counts.String > 0:
		return DocValuesString
	default:
		return DocValuesNumeric
	}
}

// NodePropertyTypeClassCountsCapability is OPTIONAL: exact O(1) type-class
// cardinalities for planners (the HLL NDV statistics are not exact enough for
// ordering-soundness gates). See PropertyTypeClassCounts.
type NodePropertyTypeClassCountsCapability interface {
	NodePropertyTypeClassCounts(labelToken uint16, propertyKey string) (PropertyTypeClassCounts, error)
}

// RelPropertyTypeClassCountsCapability is OPTIONAL: the relationship mirror, the
// gate for the rel ORDER BY r.prop LIMIT k push-down. Memory, badger and sharded
// (slot counters summed) implement it; tiered declines.
type RelPropertyTypeClassCountsCapability interface {
	RelPropertyTypeClassCounts(relTypeToken uint16, propertyKey string) (PropertyTypeClassCounts, error)
}

// TemporalIndexCapability is OPTIONAL, like PropertyIndexCapability.
type TemporalIndexCapability interface {
	CreateTemporalIndex(labelToken uint16) error
	DropTemporalIndex(labelToken uint16) error
}

// TemporalCandidateCapability is OPTIONAL (B4): prunes a temporal query's candidate
// node IDs against the per-label valid-time envelope index (docs/architecture.md
// "Temporal envelope indexes"). Sound for any store: an id the index does not
// cover is kept, so an incomplete index costs recall, never correctness.
type TemporalCandidateCapability interface {
	// PruneTemporalCandidates returns the ids that may still satisfy the valid-time
	// filter in opts (ValidAt or ValidStart/ValidEnd); an id with a present envelope
	// that provably cannot overlap is dropped. ok is false when no temporal index
	// covers labelToken or opts has no valid-time filter; the caller keeps ids unchanged.
	PruneTemporalCandidates(labelToken uint16, ids []types.NodeID, opts QueryOpts) (kept []types.NodeID, ok bool)
}

// RelTypeTemporalIndexCapability is OPTIONAL: the relationship-type mirror of
// TemporalIndexCapability (BACKLOG 21c), in its own index namespace.
type RelTypeTemporalIndexCapability interface {
	CreateRelTemporalIndex(relType uint16) error
	DropRelTemporalIndex(relType uint16) error
}

// RelTypeTemporalCandidateCapability is OPTIONAL (BACKLOG 21c): the rel-type mirror
// of TemporalCandidateCapability, same sound-superset contract.
type RelTypeTemporalCandidateCapability interface {
	// PruneRelTypeTemporalCandidates returns the ids that may still satisfy the
	// valid-time filter in opts; ok is false when no temporal index covers relType
	// or opts has no valid-time filter.
	PruneRelTypeTemporalCandidates(relType uint16, ids []types.RelID, opts QueryOpts) (kept []types.RelID, ok bool)
}

// VectorIndexCapability is OPTIONAL: vector index management and k-NN search.
// Implementations must reject indexed vectors and queries containing NaN or
// infinity with ErrInvalidVectorValue.
type VectorIndexCapability interface {
	CreateVectorIndex(labelToken uint16, propertyKey string, dims int, metric DistanceMetric) error
	DropVectorIndex(labelToken uint16, propertyKey string) error
	SearchNearestNodes(labelToken uint16, propertyKey string, query []float32, k int, opts QueryOpts) ([]*types.Node, error)
}

// VectorIndexOptions configures the engine and tuning at creation. Zero values
// select the defaults: approximate HNSW with M=16, EfConstruction=200,
// EfSearch=64 (AGENTS.md "Vector Indexes"). A backend without
// VectorIndexOptionsCapability keeps its own default engine.
type VectorIndexOptions struct {
	// UseBruteForce selects the exact linear-scan engine instead of HNSW (exact
	// recall, small indexes, oracles).
	UseBruteForce bool

	// HNSW tuning; ignored with UseBruteForce, zero = default.
	M              int // Max bidirectional links per node above layer 0. Default 16.
	EfConstruction int // Candidate list size while building. Default 200.
	EfSearch       int // Candidate list size while searching. Default 64.
}

// VectorIndexOptionsCapability is OPTIONAL: accepts VectorIndexOptions at creation
// (CreateVectorIndex keeps working with defaults). Every in-tree backend
// implements it.
type VectorIndexOptionsCapability interface {
	CreateVectorIndexWithOptions(labelToken uint16, propertyKey string, dims int, metric DistanceMetric, opts VectorIndexOptions) error
}

// FilteredVectorSearchCapability is OPTIONAL but recommended: the graph pushes its
// eligibility filter (temporal filters etc.) into the search BEFORE the k-cut.
// Without it the graph over-fetches iteratively (k, 2k, 4k ... up to the internal
// overfetchCeiling = 65536), so larger k needs this capability. snowflake.ID keeps
// the hook independent of NodeID/RelID wrappers.
type FilteredVectorSearchCapability interface {
	SearchNearestFiltered(labelToken uint16, propertyKey string, query []float32, k int, filter func(rawID snowflake.ID) bool) ([]snowflake.ID, error)
}

// HighFrequencyIndexCapability is OPTIONAL, like PropertyIndexCapability: the
// time-bucketed insertion path. Bucket sizes that are not positive whole
// milliseconds are rejected with ErrInvalidTemporalIndexConfig.
type HighFrequencyIndexCapability interface {
	CreateHighFrequencyIndex(labelToken uint16, bucketSize time.Duration) error
	DropHighFrequencyIndex(labelToken uint16) error
}

// MandatoryStore is the composition every implementation MUST satisfy. The graph
// Core narrows its store to it and type-asserts optional capabilities, returning
// ErrCapabilityNotSupported (naming the capability) on a miss.
type MandatoryStore interface {
	Lifecycle
	NodeCRUDCapability
	RelationshipCRUDCapability
	AdjacencyCapability
	BulkReadCapability
	BatchCapability
	HistoryCapability
	StatsCapability
	IterationCapability
}

// ColumnKind names the concrete Go type a scanned column carries.
type ColumnKind uint8

// The column kinds a ColumnBatch can carry. Exactly one is live per column.
const (
	ColInt64 ColumnKind = iota
	ColFloat64
	ColString
	ColBool
)

// ColumnBatch is a block of node rows delivered as typed columns, converted in
// the store where the stored type is known, so consumers read slices without
// boxing each value through GetProperty.
//
// Exactly one of Ints/Flts/Strs/Bools is populated per column (selected by
// Kinds), one slot per row. Null marks a row without a value (zero typed slot).
// Other (on ColumnData) tells a row holding the property in a shape the column
// cannot carry from one that does not hold it.
type ColumnBatch struct {
	IDs []types.NodeID
	// Ordinals is parallel to IDs: each node's dense ordinal (types.Node.Ordinal),
	// 0 where the store numbers none (tiered, badger with DisableOrdinals).
	Ordinals []uint32
	// ValidFrom/ValidTo are parallel to IDs. Zero means no temporal metadata; ValidTo
	// zero also means open-ended.
	ValidFrom []int64
	ValidTo   []int64
	ColumnData
}

// ColumnData is the typed column payload shared by ColumnBatch and RelColumnBatch,
// so kind resolution and refusal rules have one implementation across entity
// kinds. Embedded, so batch.Ints / batch.Kinds read unchanged.
type ColumnData struct {
	Kinds []ColumnKind
	Ints  [][]int64
	Flts  [][]float64
	Strs  [][]string
	Bools [][]bool
	Null  [][]bool
	// Other mirrors SegmentColumnValues.Other: Other[c][k] means row k HOLDS property
	// c, but not as a Kinds[c] value (list, map, struct, a scalar of another kind);
	// read it from the row. Such a cell is also Null[c][k] == true with a zero typed
	// slot. Not the PropertyTypeClassCounts.Other partition. nil when no cell is
	// Other; otherwise len(Other) == len(Kinds), parallel to Null.
	Other [][]bool
}

// RelColumnBatch is the relationship sibling of ColumnBatch, with StartIDs/EndIDs
// as structure (traversal aggregation needs (start, end, weight) together).
// Same contract: ID order, fn MUST NOT retain the batch (slices are reused),
// returning false stops.
type RelColumnBatch struct {
	// RelType names the relationship type of the batch's rows. Set by the graph's
	// ScanRelColumns door (all types for an empty name); empty at the store boundary.
	RelType string
	IDs     []types.RelID
	// Ordinals is parallel to IDs: each relationship's dense ordinal
	// (types.Relationship.Ordinal, store.OrdinalCapability); 0 where the store
	// numbers none (tiered, badger with DisableOrdinals, a declared segment type).
	Ordinals []uint32
	StartIDs []types.NodeID
	EndIDs   []types.NodeID
	// StartOrdinals/EndOrdinals are parallel to IDs: the endpoints' dense ordinals;
	// 0 where the store numbers none.
	StartOrdinals []uint32
	EndOrdinals   []uint32
	// ValidFrom/ValidTo are parallel to IDs as on ColumnBatch; zero ValidTo = open-ended.
	ValidFrom []int64
	ValidTo   []int64
	ColumnData
}

// ErrMixedNumericColumn reports that a requested column holds both integral and
// floating values, so it has no single type. The scan refuses rather than
// widening: a consumer deciding from a mixed column (a planner widening literals)
// would otherwise see a uniform one and decide the opposite.
var ErrMixedNumericColumn = errors.New("graph: column holds both integral and floating values")

// NodeColumnScanCapability is OPTIONAL: delivers node rows as typed columns, in ID
// order; fn MUST NOT retain the batch and returning false stops. Consumers
// type-assert and fall back to NodesByLabel. Refuses with ErrMixedNumericColumn.
type NodeColumnScanCapability interface {
	ScanNodeColumns(token uint16, props []string, opts QueryOpts,
		fn func(*ColumnBatch) bool) error
}

// RelColumnScanCapability is OPTIONAL: the relationship sibling of
// NodeColumnScanCapability; fallback RelationshipsByType, same refusal.
type RelColumnScanCapability interface {
	ScanRelColumns(token uint16, props []string, opts QueryOpts,
		fn func(*RelColumnBatch) bool) error
}

// GroupCommitCapability coalesces the strong ingest applier's per-mutation flushes.
// The caller serializes the Begin/End window with graph readers and writers and
// bounds its memory. EndGroupCommit flushes before acknowledgement; a failed End
// must be reported to every submitter. Not a rollback transaction, and crash
// atomicity is not promised (backend batch limits can need several writes). After a
// failed End the buffered ops stay pending for the next flush. Stores without this
// capability keep per-mutation flushing.
type GroupCommitCapability interface {
	BeginGroupCommit()
	EndGroupCommit() error
}

// MutationFlushHoldCapability lets Config.DurableCommit commit a group on a store
// that flushes every mutation (Badger SyncWrites) with one synced write (backlog
// 52). Between HoldMutationFlushes and ReleaseMutationFlushes the store skips the
// per-mutation flush (still flushing at its pending-write bound); the caller holds
// the graph's exclusive write lock for the whole hold and calls DurableFlush
// before reporting durable. FlushesEveryMutation says whether a hold is worth it.
type MutationFlushHoldCapability interface {
	FlushesEveryMutation() bool
	HoldMutationFlushes()
	ReleaseMutationFlushes()
}

// DurableFlushCapability makes every accepted write durable on demand:
// Config.DurableCommit calls DurableFlush once per commit group before returning
// success. It writes the whole pending buffer and fsyncs the WAL on every open
// shard (a clean or closed shard is skipped). On error the operations stay pending
// and the group is not durable. A buffer over the transaction size limit is split,
// so crash atomicity of a group is not promised.
//
// Power-loss limit (Badger v4): Sync fsyncs only the active memtable's WAL and
// current value-log file, so part of a group can be unsynced after return: it
// survives a process crash, not necessarily power loss (SyncWrites is the strict
// setting). DurableFlushSupported is false for in-memory instances; a store
// without the capability or reporting false makes New with DurableCommit fail closed.
type DurableFlushCapability interface {
	DurableFlushSupported() bool
	DurableFlush() error
}

// MetaFormatMarkerCapability is OPTIONAL: a store with a store-level format
// marker writes a meta entry durably together with a raise of that marker
// (never a lowering), so a release that cannot honour the entry refuses the
// store at open (backlog 73: the untimed valid-start mode raises marker 7).
// badger writes both in one transaction on the store; tiered and sharded on
// the shard that holds the meta keyspace (reference / anchor shard), whose
// refusal fails the whole store's open. A marker above the store's readable
// cap is ErrInvalidStoreMutation.
type MetaFormatMarkerCapability interface {
	MetaSetDurableRaising(key string, value []byte, marker uint16) error
}
