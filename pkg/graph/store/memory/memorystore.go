// Package memory provides memory.Store — the thread-safe in-memory
// implementation of the pkg/graph/store.Store interface. Used as the
// default backend by pkg/graph and also as a building block in tests.
package memory

import (
	"sync"
	"sync/atomic"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Store-contract sentinel error aliases. Re-exporting them as package-local
// names keeps the moved file readable. The canonical sentinel-error
// declarations live in pkg/graph/store (public contract).
var (
	ErrNodeExists                 = storecontract.ErrNodeExists
	ErrNodeNotFound               = storecontract.ErrNodeNotFound
	ErrRelExists                  = storecontract.ErrRelExists
	ErrRelNotFound                = storecontract.ErrRelNotFound
	ErrVersionNotFound            = storecontract.ErrVersionNotFound
	ErrIndexExists                = storecontract.ErrIndexExists
	ErrIndexNotFound              = storecontract.ErrIndexNotFound
	ErrTemporalIndexExists        = storecontract.ErrTemporalIndexExists
	ErrTemporalIndexNotFound      = storecontract.ErrTemporalIndexNotFound
	ErrInvalidTemporalIndexConfig = storecontract.ErrInvalidTemporalIndexConfig
	ErrVectorIndexExists          = storecontract.ErrVectorIndexExists
	ErrVectorIndexNotFound        = storecontract.ErrVectorIndexNotFound
	ErrDimensionMismatch          = storecontract.ErrDimensionMismatch
	ErrInvalidVectorIndexConfig   = storecontract.ErrInvalidVectorIndexConfig
	ErrInvalidVectorValue         = storecontract.ErrInvalidVectorValue
	ErrInvalidStoreMutation       = storecontract.ErrInvalidStoreMutation
	ErrNilStore                   = storecontract.ErrNilStore
	ErrStoreClosed                = storecontract.ErrStoreClosed
)

// QueryOpts is a Store-contract alias; canonical declaration lives in
// pkg/graph/store (the public contract).
type QueryOpts = storecontract.QueryOpts

// DistanceMetric is a Store-contract alias; canonical declaration lives in
// pkg/graph/store.
type DistanceMetric = storecontract.DistanceMetric

// RelTombstone is a Store-contract alias; canonical declaration lives in
// pkg/graph/store.
type RelTombstone = storecontract.RelTombstone

// Store is a thread-safe in-memory Store implementation. Its zero value is a
// usable empty store; maps are initialized lazily at the lifecycle gate.
// Uses maps for O(1) entity lookup and nested hash-sets for O(1) index maintenance.
type Store struct {
	initOnce    sync.Once
	initialized atomic.Bool
	mu          sync.RWMutex
	closed      bool
	nodes       map[types.NodeID]*types.Node
	rels        map[types.RelID]*types.Relationship

	// Label index: labelToken → set of node IDs.
	labelIdx map[uint16]map[types.NodeID]struct{}

	// RelType index: relTypeToken → set of rel IDs.
	typeIdx map[uint16]map[types.RelID]struct{}

	// Adjacency indexes — nested hash sets for O(1) insert/delete.
	outIdx map[types.NodeID]map[types.RelID]struct{} // startNodeID → set(relID)
	inIdx  map[types.NodeID]map[types.RelID]struct{} // endNodeID → set(relID)

	// Version history — pre-mutation snapshots keyed by entity ID and version.
	nodeHistory map[types.NodeID]map[uint32]*types.Node
	relHistory  map[types.RelID]map[uint32]*types.Relationship

	// Property indexes — label+property → value → set of node IDs.
	propertyIndexes map[indexpkg.PropertyIndexKey]*indexpkg.PropertyIndex

	// Relationship property indexes — relType+property → value → set of rel IDs.
	// RAM-only mirror of propertyIndexes (K3b); rebuilt from current rels at open.
	relPropertyIndexes map[indexpkg.RelPropertyIndexKey]*indexpkg.PropertyIndex

	// Composite property indexes — (label, declared ordered key tuple) →
	// concatenated per-component value key → set of node IDs. RAM-only, v1
	// equality-only (see docs/query-planners.md "Composite property
	// indexes"). compositeIndexesByLabel is the label->definitions secondary
	// index node-mutation maintenance needs (a node carries a label, not a
	// specific composite key tuple, so maintenance must enumerate every
	// definition registered on each label the node carries).
	compositeIndexes        map[indexpkg.CompositeIndexKey]*indexpkg.CompositePropertyIndex
	compositeIndexesByLabel map[uint16][]indexpkg.CompositeIndexKey

	// Property-key presence counts — label+property → current node count.
	// Counts only indexable scalar property values because that is the lookup
	// surface used by property equality indexes and planner pruning.
	propertyKeyCounts map[uint16]map[string]int

	// Property-key type-class counts — label+property → per-class node counts
	// (types.PropertyTypeClass order). Unlike propertyKeyCounts this covers
	// EVERY property value (non-indexable ones classify as ClassOther), so
	// present+missing partitions the label exactly. Maintained on the SAME
	// node-mutation doors as propertyKeyCounts (same call — see
	// adjustNodePropertyKeyCounts); backs the optional
	// store.NodePropertyTypeClassCountsCapability.
	propertyTypeClassCounts map[uint16]map[string]*[types.NumPropertyTypeClasses]int64

	// relPropertyTypeClassCounts is the RELATIONSHIP mirror (BACKLOG 5B): exact
	// per-(relTypeToken, propertyKey) rel counts by class, maintained on the same rel
	// mutation doors as the rel property index. The memory store holds live rels, so
	// the delete path decrements with the old rel directly — no memoized-contribution
	// sidecar (unlike badger's read-free deleteRelByInfo). Backs the optional
	// store.RelPropertyTypeClassCountsCapability.
	relPropertyTypeClassCounts map[uint16]map[string]*[types.NumPropertyTypeClasses]int64

	// Property-key NDV + exact min/max stats — label+property → accumulator.
	// Maintained on the SAME node-mutation doors as propertyKeyCounts (see
	// adjustNodePropertyKeyCounts in memorystore_property_key_counts.go);
	// backs the optional store.NodePropertyStatsCapability.
	propertyStats map[uint16]map[string]*indexpkg.PropertyStatsAccumulator

	// relPropertyKeyCounts / relPropertyStats are the RELATIONSHIP mirror of
	// propertyKeyCounts / propertyStats (BACKLOG 21a), following the same
	// live-object-at-delete shape as relPropertyTypeClassCounts above: the
	// memory store holds live rels, so the delete path decrements with the
	// old rel directly — no memoized-contribution sidecar (unlike badger's
	// read-free deleteRelByInfo). Maintained on the same rel-mutation doors
	// as relPropertyTypeClassCounts (see adjustRelPropertyKeyCounts in
	// memorystore_rel_property_stats.go); backs the optional
	// store.RelPropertyStatsCapability.
	relPropertyKeyCounts map[uint16]map[string]int
	relPropertyStats     map[uint16]map[string]*indexpkg.PropertyStatsAccumulator

	// Temporal indexes — labelToken → interval index for temporal push-down.
	temporalIndexes map[uint16]*indexpkg.TemporalIndex

	// Relationship-type temporal indexes (BACKLOG 21c) — relType → interval
	// index, the rel-side mirror of temporalIndexes. Independent map: a
	// label token and a rel-type token are different registries and may
	// numerically collide, so these must never share temporalIndexes.
	relTypeTemporalIndexes map[uint16]*indexpkg.TemporalIndex

	// High-frequency indexes — labelToken → time-bucketed index for O(1) insertion.
	// Separate map from temporalIndexes; only one type can exist per label at a time.
	hfIndexes map[uint16]*indexpkg.HighFrequencyIndex

	// Vector indexes — in-memory brute-force k-NN index on node properties.
	vectorIndexes map[indexpkg.VectorIndexKey]*indexpkg.VectorIndex

	// Meta KV — schema-version marker and other graph-layer bookkeeping.
	metaKV map[string][]byte

	// X5 DocValues: cached per-label columnar snapshots (labelToken -> immutable
	// LabelDocValues), and a global node-mutation epoch bumped on EVERY node write
	// so a cached column can detect staleness. The epoch is deliberately coarse
	// (any node mutation invalidates every column): a per-label counter is an
	// optimization, but a single missed mutation path would silently serve a stale
	// aggregate, so correctness picks the impossible-to-under-fire
	// global counter. atomic so ForEachDocValues can read it without the lock.
	nodeEpoch atomic.Uint64
	// relEpoch is a DISTINCT generation counter bumped on every relationship write
	// (add/replace/delete/batch). The X5 expand-aggregation column path reads
	// ADJACENCY (not just node membership), so its staleness re-check (Gate 2) must
	// see edge mutations too — nodeEpoch alone would wave through a torn aggregate
	// from a concurrent edge insert. Kept separate from nodeEpoch so node-only
	// scan/projection column caches do not rebuild on edge-heavy writes.
	relEpoch   atomic.Uint64
	docColumns map[uint16]*indexpkg.LabelDocValues
	// docColumnsMulti caches columns for a LABEL INTERSECTION (multi-label
	// patterns like (p:A:B)), keyed by the order-independent token-tuple key
	// (indexpkg.MultiLabelKey). Same epoch-validated immutable-snapshot model as
	// docColumns; separate map so the single-label path stays keyed by uint16.
	docColumnsMulti map[string]*indexpkg.LabelDocValues

	// Change-log (op-log) — opt-in via WithChangeLog. logEnabled gates all record
	// production. logSeq is the monotonic LSN, changeLog the ordered in-RAM record
	// slice; both are guarded by ms.mu (every mutation door holds it), so LSN
	// assignment and append are atomic and totally ordered. Not durable — a
	// parity/testing facility (see memorystore_changelog.go).
	logEnabled bool
	logSeq     uint64
	changeLog  []storecontract.ChangeRecord

	// disablePlannerStats skips maintenance of the query-planner statistics
	// (presence/NDV/min-max/type-class) on every write; the stat methods then
	// fail closed with ErrCapabilityNotSupported. Opt-in via WithoutPlannerStats.
	disablePlannerStats bool

	// Per-transaction change-log scope (store.TxChangeLogScope), parallel to the
	// badger backend. While scopeActive (toggled by the core's SetLogDivert under
	// its exclusive write lock), logChangeLocked buffers the record into scopeLog
	// (LSN zero) WITHOUT advancing logSeq; CommitLogScope splices them into
	// changeLog with contiguous LSNs minted at commit; DiscardLogScope drops them
	// (a rolled-back tx emits nothing). All under ms.mu.
	scopeActive bool
	scopeLog    []storecontract.ChangeRecord

	// Scoped (multi-token) change-log buffers — store.ScopedTxChangeLog
	// (BACKLOG 11f Batch A, foundation only; nothing wires a nonzero token
	// into a tx yet). Unlike scopeActive/scopeLog above (a single implicit
	// buffer requiring provable exclusion of every other writer for its
	// entire open duration), each scope here is independently addressed by
	// its token, so multiple scopes can be open concurrently with no shared
	// "which scope is active" flag to race on. Both fields guarded by ms.mu.
	scopedTokenSeq uint64
	scopedLogs     map[uint64][]storecontract.ChangeRecord

	// transaction-time membership sidecars (store.LabelTxMembershipCapability /
	// RelTypeTxMembershipCapability). labelTxMembers maps a label token to the set
	// of node IDs that EVER carried it (current OR any historical version), each
	// tagged with a lower bound on the transaction time of its earliest
	// acquisition; relTypeTxMembers is the rel-type mirror. Both are APPEND-ONLY
	// (removal/delete never drops a member) and nil until lazily built on the first
	// ForEach*TxMember call (mirrors the badger relValidIdx / OPT15 precedent), so a
	// graph that never runs a pinned label/type scan pays nothing. All under ms.mu.
	labelTxMembers   map[uint16]map[types.NodeID]types.Instant
	relTypeTxMembers map[uint16]map[types.RelID]types.Instant
}

// bumpNodeEpoch marks every cached DocValues column potentially stale. Called by
// every node-mutation path (add/replace/label-change/delete/batch). A spurious
// bump (on a no-op or errored mutation) is safe — it only forces a rebuild.
func (ms *Store) bumpNodeEpoch() { ms.nodeEpoch.Add(1) }

// bumpRelEpoch marks the adjacency view stale for the X5 expand-aggregation column
// path. Called by every relationship-mutation path. A spurious bump is safe.
func (ms *Store) bumpRelEpoch() { ms.relEpoch.Add(1) }

// New creates an empty Store with all indexes initialized. Options (e.g.
// WithChangeLog) are applied before initialization. The variadic signature is
// backward-compatible with existing New() callers.
func New(opts ...Option) *Store {
	s := &Store{}
	for _, opt := range opts {
		opt(s)
	}
	s.ensureInitialized()
	return s
}

func (ms *Store) ensureInitialized() {
	ms.initOnce.Do(func() {
		if ms.nodes == nil {
			ms.nodes = make(map[types.NodeID]*types.Node)
		}
		if ms.rels == nil {
			ms.rels = make(map[types.RelID]*types.Relationship)
		}
		if ms.labelIdx == nil {
			ms.labelIdx = make(map[uint16]map[types.NodeID]struct{})
		}
		if ms.typeIdx == nil {
			ms.typeIdx = make(map[uint16]map[types.RelID]struct{})
		}
		if ms.outIdx == nil {
			ms.outIdx = make(map[types.NodeID]map[types.RelID]struct{})
		}
		if ms.inIdx == nil {
			ms.inIdx = make(map[types.NodeID]map[types.RelID]struct{})
		}
		if ms.nodeHistory == nil {
			ms.nodeHistory = make(map[types.NodeID]map[uint32]*types.Node)
		}
		if ms.relHistory == nil {
			ms.relHistory = make(map[types.RelID]map[uint32]*types.Relationship)
		}
		if ms.propertyIndexes == nil {
			ms.propertyIndexes = make(map[indexpkg.PropertyIndexKey]*indexpkg.PropertyIndex)
		}
		if ms.relPropertyIndexes == nil {
			ms.relPropertyIndexes = make(map[indexpkg.RelPropertyIndexKey]*indexpkg.PropertyIndex)
		}
		if ms.compositeIndexes == nil {
			ms.compositeIndexes = make(map[indexpkg.CompositeIndexKey]*indexpkg.CompositePropertyIndex)
		}
		if ms.compositeIndexesByLabel == nil {
			ms.compositeIndexesByLabel = make(map[uint16][]indexpkg.CompositeIndexKey)
		}
		if ms.propertyKeyCounts == nil {
			ms.propertyKeyCounts = make(map[uint16]map[string]int)
		}
		if ms.propertyTypeClassCounts == nil {
			ms.propertyTypeClassCounts = make(map[uint16]map[string]*[types.NumPropertyTypeClasses]int64)
		}
		if ms.propertyStats == nil {
			ms.propertyStats = make(map[uint16]map[string]*indexpkg.PropertyStatsAccumulator)
		}
		if ms.relPropertyKeyCounts == nil {
			ms.relPropertyKeyCounts = make(map[uint16]map[string]int)
		}
		if ms.relPropertyStats == nil {
			ms.relPropertyStats = make(map[uint16]map[string]*indexpkg.PropertyStatsAccumulator)
		}
		if ms.temporalIndexes == nil {
			ms.temporalIndexes = make(map[uint16]*indexpkg.TemporalIndex)
		}
		if ms.relTypeTemporalIndexes == nil {
			ms.relTypeTemporalIndexes = make(map[uint16]*indexpkg.TemporalIndex)
		}
		if ms.hfIndexes == nil {
			ms.hfIndexes = make(map[uint16]*indexpkg.HighFrequencyIndex)
		}
		if ms.vectorIndexes == nil {
			ms.vectorIndexes = make(map[indexpkg.VectorIndexKey]*indexpkg.VectorIndex)
		}
		if ms.metaKV == nil {
			ms.metaKV = make(map[string][]byte)
		}
		if ms.docColumns == nil {
			ms.docColumns = make(map[uint16]*indexpkg.LabelDocValues)
		}
		if ms.docColumnsMulti == nil {
			ms.docColumnsMulti = make(map[string]*indexpkg.LabelDocValues)
		}
		ms.initialized.Store(true)
	})
}

// Clear removes all entities, indexes, history, and property indexes.
// After Clear(), the Store is empty (same state as New()).
func (ms *Store) Clear() error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if err := ms.checkOpenLocked(); err != nil {
		return err
	}

	ms.nodes = make(map[types.NodeID]*types.Node)
	ms.rels = make(map[types.RelID]*types.Relationship)
	ms.labelIdx = make(map[uint16]map[types.NodeID]struct{})
	ms.typeIdx = make(map[uint16]map[types.RelID]struct{})
	ms.outIdx = make(map[types.NodeID]map[types.RelID]struct{})
	ms.inIdx = make(map[types.NodeID]map[types.RelID]struct{})
	ms.nodeHistory = make(map[types.NodeID]map[uint32]*types.Node)
	ms.relHistory = make(map[types.RelID]map[uint32]*types.Relationship)
	ms.propertyIndexes = make(map[indexpkg.PropertyIndexKey]*indexpkg.PropertyIndex)
	ms.relPropertyIndexes = make(map[indexpkg.RelPropertyIndexKey]*indexpkg.PropertyIndex)
	ms.compositeIndexes = make(map[indexpkg.CompositeIndexKey]*indexpkg.CompositePropertyIndex)
	ms.compositeIndexesByLabel = make(map[uint16][]indexpkg.CompositeIndexKey)
	ms.propertyKeyCounts = make(map[uint16]map[string]int)
	ms.propertyTypeClassCounts = make(map[uint16]map[string]*[types.NumPropertyTypeClasses]int64)
	ms.relPropertyTypeClassCounts = make(map[uint16]map[string]*[types.NumPropertyTypeClasses]int64)
	ms.propertyStats = make(map[uint16]map[string]*indexpkg.PropertyStatsAccumulator)
	ms.relPropertyKeyCounts = make(map[uint16]map[string]int)
	ms.relPropertyStats = make(map[uint16]map[string]*indexpkg.PropertyStatsAccumulator)
	ms.temporalIndexes = make(map[uint16]*indexpkg.TemporalIndex)
	ms.relTypeTemporalIndexes = make(map[uint16]*indexpkg.TemporalIndex)
	ms.hfIndexes = make(map[uint16]*indexpkg.HighFrequencyIndex)
	ms.vectorIndexes = make(map[indexpkg.VectorIndexKey]*indexpkg.VectorIndex)
	ms.docColumns = make(map[uint16]*indexpkg.LabelDocValues)
	ms.docColumnsMulti = make(map[string]*indexpkg.LabelDocValues)
	ms.labelTxMembers = nil   // drop the lazy membership sidecar; rebuilt on next pinned scan
	ms.relTypeTxMembers = nil // rel-type mirror
	ms.bumpNodeEpoch()        // any cached column from before Clear is now invalid
	ms.bumpRelEpoch()         // and the adjacency view (X5 expand path)
	// Drop the change-log records (the store is now empty) and re-anchor with a
	// ChangeClear marker at a fresh, still-monotonic LSN — mirrors badger.Clear.
	ms.changeLog = nil
	ms.logChangeLocked(storecontract.ChangeClear, nil)
	return nil
}

// MetaGet returns the bytes stored under key, or (nil, nil) if absent.
func (ms *Store) MetaGet(key string) ([]byte, error) {
	if ms == nil {
		return nil, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return nil, err
	}
	v, ok := ms.metaKV[key]
	if !ok {
		return nil, nil
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, nil
}

// MetaSet stores value under key, overwriting any previous value.
func (ms *Store) MetaSet(key string, value []byte) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	cp := make([]byte, len(value))
	copy(cp, value)
	ms.metaKV[key] = cp
	return nil
}

// Close marks the Store closed. It is idempotent.
func (ms *Store) Close() error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()

	ms.closed = true
	return nil
}

// checkOpenLocked returns ErrStoreClosed after Close has run.
// Caller must hold ms.mu for either reading or writing. It also initializes the
// zero-value Store before any map access; sync.Once makes the initialization
// safe even when several readers reach the lifecycle gate together.
func (ms *Store) checkOpenLocked() error {
	if ms == nil {
		return ErrNilStore
	}
	if ms.closed {
		return ErrStoreClosed
	}
	if !ms.initialized.Load() {
		ms.ensureInitialized()
	}
	return nil
}
