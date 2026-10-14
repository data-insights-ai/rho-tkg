// Package memory provides memory.Store — the thread-safe in-memory
// implementation of the pkg/graph/store.Store interface. Used as the
// default backend by pkg/graph and also as a building block in tests.
package memory

import (
	"sync"
	"sync/atomic"
	"time"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/segdir"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/segment"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
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
	// labelOrder / typeOrder: members in ascending ID order for streaming scans
	// (storeutil.MemberOrder); maintained with the label/type index under mu.
	labelOrder storeutil.MemberOrder[types.NodeID]
	typeOrder  storeutil.MemberOrder[types.RelID]

	// RelType index: relTypeToken → set of rel IDs.
	typeIdx map[uint16]map[types.RelID]struct{}

	// Adjacency indexes — nested hash sets for O(1) insert/delete.
	outIdx map[types.NodeID]*adjSet // startNodeID → set(relID) (memorystore_adjset.go)
	inIdx  map[types.NodeID]*adjSet // endNodeID → set(relID)

	// Ex-adjacency (backlog 50, memorystore_exadj.go): per node, the
	// relationships that named it in a row the live adjacency no longer holds.
	exOutIdx map[types.NodeID]*adjSet
	exInIdx  map[types.NodeID]*adjSet

	// Version history — pre-mutation snapshots keyed by entity ID and version.
	nodeHistory map[types.NodeID]map[uint32]*types.Node
	relHistory  map[types.RelID]map[uint32]*types.Relationship
	// nodeHistoryIDs / relHistoryIDs: the sorted key sets of nodeHistory /
	// relHistory for the All*HistoryIDsFrom cursor (memorystore_history_ids.go).
	nodeHistoryIDs atomic.Pointer[[]types.NodeID]
	relHistoryIDs  atomic.Pointer[[]types.RelID]

	// Property indexes — label+property → value → set of node IDs.
	propertyIndexes map[indexpkg.PropertyIndexKey]*indexpkg.PropertyIndex

	// Relationship property indexes — relType+property → value → set of rel IDs.
	// RAM-only mirror of propertyIndexes (K3b); rebuilt from current rels at open.
	relPropertyIndexes map[indexpkg.RelPropertyIndexKey]*indexpkg.PropertyIndex

	// Composite property indexes: (label, key tuple) -> value key -> node IDs.
	// RAM-only, equality-only (docs/query-planners.md "Composite property
	// indexes"). compositeIndexesByLabel lets node mutation enumerate the
	// definitions registered on each label the node carries.
	compositeIndexes        map[indexpkg.CompositeIndexKey]*indexpkg.CompositePropertyIndex
	compositeIndexesByLabel map[uint16][]indexpkg.CompositeIndexKey

	// Property-key presence counts — label+property → current node count.
	// Counts only indexable scalar property values because that is the lookup
	// surface used by property equality indexes and planner pruning.
	propertyKeyCounts map[uint16]map[string]int

	// Property-key type-class counts: label+property -> per-class node counts.
	// Covers every value (non-indexable ones are ClassOther), so present+missing
	// partitions the label. Maintained with propertyKeyCounts
	// (adjustNodePropertyKeyCounts); backs NodePropertyTypeClassCountsCapability.
	propertyTypeClassCounts map[uint16]map[string]*[types.NumPropertyTypeClasses]int64

	// relPropertyTypeClassCounts: relationship mirror (backlog 5B), per
	// (relTypeToken, propertyKey). Delete decrements with the live old rel
	// (backlog 53). Backs RelPropertyTypeClassCountsCapability.
	relPropertyTypeClassCounts map[uint16]map[string]*[types.NumPropertyTypeClasses]int64

	// Property-key NDV and exact min/max stats: label+property -> accumulator.
	// Maintained with propertyKeyCounts; backs NodePropertyStatsCapability.
	propertyStats map[uint16]map[string]*indexpkg.PropertyStatsAccumulator

	// relPropertyKeyCounts / relPropertyStats: relationship mirror of
	// propertyKeyCounts / propertyStats (backlog 21a), maintained with
	// relPropertyTypeClassCounts (adjustRelPropertyKeyCounts); backs
	// RelPropertyStatsCapability.
	relPropertyKeyCounts map[uint16]map[string]int
	relPropertyStats     map[uint16]map[string]*indexpkg.PropertyStatsAccumulator

	// Temporal indexes — labelToken → interval index for temporal push-down.
	temporalIndexes map[uint16]*indexpkg.TemporalIndex

	// Relationship-type temporal indexes (backlog 21c): rel-side mirror of
	// temporalIndexes. A separate map: label and rel-type tokens are different
	// registries and may collide numerically.
	relTypeTemporalIndexes map[uint16]*indexpkg.TemporalIndex

	// High-frequency indexes — labelToken → time-bucketed index for O(1) insertion.
	// Separate map from temporalIndexes; only one type can exist per label at a time.
	hfIndexes map[uint16]*indexpkg.HighFrequencyIndex

	// Vector indexes — in-memory brute-force k-NN index on node properties.
	vectorIndexes map[indexpkg.VectorIndexKey]*indexpkg.VectorIndex

	// Meta KV — schema-version marker and other graph-layer bookkeeping.
	metaKV map[string][]byte

	// X5 DocValues: cached per-label columnar snapshots plus a global node
	// mutation epoch bumped on EVERY node write. Deliberately coarse: a missed
	// mutation path with a per-label counter would serve stale aggregates.
	// Atomic so ForEachDocValues reads it without the lock.
	nodeEpoch atomic.Uint64
	// relEpoch: separate generation counter bumped on every relationship write.
	// The expand-aggregation column path reads adjacency, so its staleness
	// re-check must see edge mutations; kept apart from nodeEpoch so node-only
	// column caches do not rebuild on edge writes.
	relEpoch atomic.Uint64
	// relTypeEpochs / relEpochCoarse: per-type stamp for RelMutationEpochForType
	// (type stripe plus coarse term). Doors holding the relationship bump only
	// their stripe (bumpRelEpochForType); all others bump the coarse term.
	relTypeEpochs  [relTypeEpochStripes]atomic.Uint64
	relEpochCoarse atomic.Uint64
	// maxNodeOrdinal / maxRelOrdinal: the last dense ordinal handed out
	// (memorystore_ordinal.go); advanced under mu, read lock-free by Max*.
	maxNodeOrdinal atomic.Uint32
	maxRelOrdinal  atomic.Uint32
	// appendDelta records pure inserts since the last non-append write, letting a
	// read EXTEND a cached column instead of rebuilding it. Guarded by ms.mu.
	appendDelta    appendDeltaState
	columnExtends  atomic.Uint64
	columnRebuilds atomic.Uint64
	// multiFromMember counts the label-intersection snapshots taken from a
	// member label's columns instead of the nodes (round 7 request 6).
	multiFromMember atomic.Uint64
	docColumns      map[uint16]*indexpkg.LabelDocValues
	// relColumns is the rel-type keyed sibling of docColumns, carrying endpoint
	// columns alongside properties. Stamped with the store-wide relEpoch.
	relColumns map[uint16]*indexpkg.DocValues[types.RelID]
	// docColumnsMulti: columns for a label intersection, keyed by the
	// order-independent indexpkg.MultiLabelKey. Same epoch-validated model as
	// docColumns.
	docColumnsMulti map[string]*indexpkg.LabelDocValues
	// docOrdinals: node ordinals of a docColumns / docColumnsMulti snapshot's
	// rows, valid while that snapshot is. Guarded by mu. A node keeps its ordinal
	// while it has a current row.
	docOrdinals docOrdinalCache

	// Change-log (op-log), opt-in via WithChangeLog. logSeq (monotonic LSN) and
	// changeLog are guarded by ms.mu so LSN assignment and append are atomic.
	// Not durable (memorystore_changelog.go).
	logEnabled bool
	logSeq     uint64
	changeLog  []storecontract.ChangeRecord

	// disablePlannerStats skips maintenance of the query-planner statistics
	// (presence/NDV/min-max/type-class) on every write; the stat methods then
	// fail closed with ErrCapabilityNotSupported. Opt-in via WithoutPlannerStats.
	disablePlannerStats bool

	// Per-transaction change-log scope (store.TxChangeLogScope). While
	// scopeActive (set by SetLogDivert under the core's exclusive lock),
	// logChangeLocked buffers into scopeLog without advancing logSeq;
	// CommitLogScope splices with LSNs minted at commit, DiscardLogScope drops
	// them. All under ms.mu.
	scopeActive bool
	scopeLog    []storecontract.ChangeRecord

	// Scoped change-log buffers (store.ScopedTxChangeLog, backlog 11f Batch A,
	// foundation only). Each scope is addressed by token, so several can be open
	// concurrently with no shared active flag. Guarded by ms.mu.
	scopedTokenSeq uint64
	scopedLogs     map[uint64][]storecontract.ChangeRecord

	// Transaction-time membership sidecars (Label/RelTypeTxMembershipCapability):
	// node IDs that EVER carried a label (current or historical), tagged with a
	// lower bound of the earliest acquisition tx time; relTypeTxMembers mirrors
	// it. Append-only; nil until the first ForEach*TxMember call. Under ms.mu.
	labelTxMembers   map[uint16]map[types.NodeID]types.Instant
	relTypeTxMembers map[uint16]map[types.RelID]types.Instant

	// Property membership sidecars (backlog 8): one per declared property index
	// a temporal lookup asked for, built lazily, recorded at the row seams
	// (memorystore_propertytxmembers.go). Under ms.mu.
	relPropTxMembers  map[indexpkg.RelPropertyIndexKey]*indexpkg.PropertyTxMembers[types.RelID]
	nodePropTxMembers map[indexpkg.PropertyIndexKey]*indexpkg.PropertyTxMembers[types.NodeID]
	propTxBuilds      int64
	propTxBuildTime   time.Duration

	// Belief watermarks (Node/RelBeliefWatermarkCapability, backlog 10c): max
	// TxFrom across an ID's whole version chain. Lazily built (nil until first
	// use), maintained at every door that persists a current or history row.
	// Invariant: see NodeBeliefWatermarkCapability. Under ms.mu.
	nodeBeliefWatermark map[types.NodeID]types.Instant
	relBeliefWatermark  map[types.RelID]types.Instant

	// Valid-end indexes (backlog 51, memorystore_valid_end.go): lazy, RAM,
	// maintained under mu after their first build.
	relValidEnds  indexpkg.ValidEndIndex
	nodeValidEnds indexpkg.ValidEndIndex

	// History stamps (HistoryStampsCapability, backlog 30,
	// memorystore_history_stamps.go): per ID, cached fold of history rows'
	// newest TxFrom / TxTo-or-DeletedAt plus the row count it was computed at.
	// Writers hold ms.mu exclusively; readers share it and take histStampsMu.
	histStampsMu   sync.RWMutex
	nodeHistStamps map[types.NodeID]histStampsEntry
	relHistStamps  map[types.RelID]histStampsEntry
	// History epochs (HistoryEpochCapability, backlog 68,
	// memorystore_history_epoch.go): count-checked watch tables.
	nodeHistEpochs storeutil.HistoryEpochs[types.NodeID]
	relHistEpochs  storeutil.HistoryEpochs[types.RelID]

	// ADR-0011 S2: declared bulk relationship types and their in-RAM column
	// segments (see memorystore_segments.go). All nil/zero until a type is
	// declared; guarded by ms.mu except segDue (atomic).
	segTypes        map[uint16]*segType
	segDead         map[types.RelID]uint16 // sealed rows that are no longer current -> type token
	segNodeDict     *segment.NodeDict      // endpoint identities and hashes shared by every segment of the store
	segBudget       int64                  // memtable budget (bytes, all declared types)
	segUnsealed     int64                  // unsealed bytes of all declared types
	segRefusedBytes int64                  // unsealed bytes the codec refused (excluded from the trigger)
	segMaxID        types.RelID            // largest sealed ID (0 = no segment)
	segEpoch        uint64                 // bumped by Clear; a seal started before it is discarded
	segSeq          uint64                 // last segment id handed out
	segDue          atomic.Bool            // a write pushed the unsealed bytes over the budget
	relsPeak        int                    // len(rels) high-water mark since the last shrink (declared stores only)
	sealEncodeHook  func()                 // test seam: runs in a seal's encode window (nil in production)
	sealLog         func(sealRecord)       // measurement seam: one record per seal that installed a segment (nil in production)
	sealerRunning   bool                   // the background sealer goroutine is live (guarded by ms.mu)
	sealers         sync.WaitGroup         // background sealers; Close waits for them
	sealedRowBuilds atomic.Uint64          // Relationships built from sealed rows (measurement)
	segDir          *segdir.Dir            // ADR-0011 S3: the segment directory (nil = in-RAM segments)
	segCommitFloor  types.Instant          // newest transaction stamp of the rows loaded from segDir (memorystore_commit_floor.go)
}

// bumpNodeEpoch invalidates every cached DocValues column and poisons the
// append record. Poison is the default: only a path that opts in via
// bumpNodeEpochAppend keeps the append delta (memorystore_append_delta.go).
// A spurious bump only forces a rebuild.
func (ms *Store) bumpNodeEpoch() {
	ms.bumpNodeEpochRaw()
	ms.appendDelta.poisoned = true
	ms.appendDelta.byLabel = nil
}

// bumpNodeEpochRaw advances the epoch without touching the append record. Only the
// two append-aware helpers call it.
func (ms *Store) bumpNodeEpochRaw() { ms.nodeEpoch.Add(1) }

// bumpRelEpoch marks the adjacency view stale and moves every type's
// RelMutationEpochForType (coarse term): the default for a write that does
// not name its type. A spurious bump is safe.
func (ms *Store) bumpRelEpoch() {
	ms.relEpoch.Add(1)
	ms.relEpochCoarse.Add(1)
}

// relTypeEpochStripes is the size of the per-type stamp array (badger's
// count): a power of two, so a collision between two types over-invalidates
// and costs nothing else.
const relTypeEpochStripes = 256

// bumpRelEpochForTypes is bumpRelEpoch for a write whose relationships are
// in hand: only their types' stamps move. A nil relationship or token 0
// falls back to the coarse bump.
func (ms *Store) bumpRelEpochForTypes(rels ...*types.Relationship) {
	for _, r := range rels {
		if r == nil || r.TypeToken().Value() == 0 {
			ms.bumpRelEpoch()
			return
		}
	}
	ms.relEpoch.Add(1)
	for _, r := range rels {
		ms.relTypeEpochs[r.TypeToken().Value()%relTypeEpochStripes].Add(1)
	}
}

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
			ms.relColumns = nil // Clear invalidates every rel-type column
		}
		if ms.outIdx == nil {
			ms.outIdx = make(map[types.NodeID]*adjSet)
		}
		if ms.inIdx == nil {
			ms.inIdx = make(map[types.NodeID]*adjSet)
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
	ms.labelOrder.Reset()
	ms.typeOrder.Reset()
	ms.outIdx = make(map[types.NodeID]*adjSet)
	ms.inIdx = make(map[types.NodeID]*adjSet)
	ms.exOutIdx, ms.exInIdx = nil, nil
	ms.nodeHistory = make(map[types.NodeID]map[uint32]*types.Node)
	ms.relHistory = make(map[types.RelID]map[uint32]*types.Relationship)
	ms.histStampsMu.Lock()
	ms.nodeHistStamps, ms.relHistStamps = nil, nil
	ms.histStampsMu.Unlock()
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
	ms.docOrdinals = docOrdinalCache{}
	ms.labelTxMembers = nil            // drop the lazy membership sidecar; rebuilt on next pinned scan
	ms.relTypeTxMembers = nil          // rel-type mirror
	ms.dropPropertyTxMembersLocked()   // property sidecars; rebuilt on next temporal lookup
	ms.nodeBeliefWatermark = nil       // drop the lazy belief-watermark sidecar; rebuilt on next use
	ms.relBeliefWatermark = nil        // rel mirror
	segErr := ms.clearSegmentsLocked() // ADR-0011: segments go (files too, S3), declarations stay
	ms.bumpNodeEpoch()                 // any cached column from before Clear is now invalid
	ms.bumpRelEpoch()                  // and the adjacency view (X5 expand path)
	ms.relValidEnds.Reset()            // backlog 51: rebuilt on the next bounded scan
	ms.nodeValidEnds.Reset()
	// Drop the change-log records (the store is now empty) and re-anchor with a
	// ChangeClear marker at a fresh, still-monotonic LSN — mirrors badger.Clear.
	ms.changeLog = nil
	ms.logChangeLocked(storecontract.ChangeClear, nil)
	return segErr
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
	ms.closed = true
	ms.mu.Unlock()
	// ADR-0011: wait for an in-flight seal (it sees closed and discards its
	// segment) so no work outlives the store, then release segment files; a
	// mapping a lock-free scan holds is unmapped when that scan returns.
	ms.sealers.Wait()
	ms.mu.Lock()
	ms.releaseSegmentDirLocked()
	ms.mu.Unlock()
	return nil
}

// checkOpenLocked returns ErrStoreClosed after Close. Caller holds ms.mu
// (read or write). Also initializes the zero-value Store; sync.Once makes
// that safe for concurrent readers.
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
