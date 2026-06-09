// Package core holds the implementation behind *graph.Graph. It is internal
// because customers only ever see the thin facade in pkg/graph; everything
// load-bearing lives here.
//
// All fields, all methods, all package-level helpers (validation, snowflake,
// resolution helpers, sentinel errors needed by methods) live here. The
// pkg/graph package re-exports the few symbols customers must reach.
package core

import (
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/dgraph-io/badger/v4/options"

	eventspkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/events"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/generatedcreate"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/locks"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	snowflakepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/snowflake"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// =============================================================================
// Core struct (was *graph.Graph)
// =============================================================================

// Core is the central graph implementation. Customers see *graph.Graph, which
// is a thin facade holding *Core plus sub-API accessors.
type Core struct {
	labels             *registrypkg.LabelRegistry
	relTypes           *registrypkg.RelTypeRegistry
	propKeys           *registrypkg.PropertyKeyRegistry
	nodeIDGen          *snowflake.Node
	relIDGen           *snowflake.Node
	store              storepkg.MandatoryStore
	generatedCreate    generatedcreate.Capability
	endpointHash       storepkg.EndpointIntegrityHashCapability
	endpointHashWrite  generatedcreate.RelationshipEndpointHashCapability
	nodeHash           storepkg.NodeIntegrityHashCapability
	txTimeQuery        storepkg.TransactionTimeQueryCapability
	txTimeQueryCopy    bool
	historyTrim        storepkg.HistoryRollbackTrimCapability
	propertyQuery      storepkg.PropertyIndexCapability
	propertyQueryTrust bool
	filteredVector     storepkg.FilteredVectorSearchCapability
	depthHistory       storepkg.DepthHistoryIterationCapability
	deletedIter        storepkg.DeletedIterationCapability
	deletedDepthIter   storepkg.DepthDeletedIterationCapability
	vectorRowsTrust    bool
	storeRowsTrust     bool
	nativeAdjacency    bool
	// bitemporalMigrated is true after the one-shot inherited-ValidFrom
	// migration has run successfully on this store. When false, the resolver
	// keeps the legacy inheritance heuristic active for back-compat.
	bitemporalMigrated bool
	entityLocks        *locks.Manager
	validation         ValidationLimits
	constraints        ConstraintSet
	events             eventspkg.Publisher
	txEventBuffer      *[]eventspkg.Event
	mu                 sync.RWMutex
	// txMu serializes transaction/batch lifecycles (BeginTx → Commit/Rollback
	// and BatchExecute). Held for the entire tx/batch lifetime instead of
	// c.mu.Lock (which was held in v3.4 / v4.0.x). Standalone mutations and
	// reads acquire c.mu.RLock and proceed concurrently with an open tx —
	// only entity-level conflicts now block, not the whole graph. This
	// closes the "read accessor inside a tx deadlocks against c.mu" bug
	// class introduced in v3.4 (see lesson 31). The tx code path takes a
	// brief c.mu.RLock around each mutation/read so the *Internal/*Locked
	// helpers run with a non-zero lock context they expect.
	txMu           sync.Mutex
	registryMu     sync.Mutex
	registryDirty  atomic.Bool
	relTypeCache   map[string]uint16
	relTypeCacheMu sync.RWMutex
	closeOnce      sync.Once
	closed         atomic.Bool

	// clock is the time source used by every mutation path that stamps
	// TxFrom / UpdatedAt / DeletedAt / event.Timestamp. Defaults to
	// time.Now in New(); c.now() makes the observed instant monotonic
	// per Core so fast same-millisecond mutations still get ordered
	// transaction intervals. Test helpers swap it for a deterministic
	// counter without relying on wall-clock sleeps (R4-F20). Only ever
	// read from goroutines that hold the appropriate Core lock — the
	// value is set once in New and (in tests) replaced under exclusive
	// access.
	clock func() time.Time
	// lastInstant stores the last millisecond instant handed out by c.now().
	// It is atomic because event publishing and read-side helpers can call
	// c.now() from different goroutines even though mutation state is guarded.
	lastInstant atomic.Int64

	indexProviders map[string]*indexProviderEntry

	opNodeAdds    atomic.Int64
	opNodeReads   atomic.Int64
	opNodeUpdates atomic.Int64
	opNodeDeletes atomic.Int64
	opRelAdds     atomic.Int64
	opRelReads    atomic.Int64
	opRelUpdates  atomic.Int64
	opRelDeletes  atomic.Int64

	// Sub-Core groupings — narrow operation surfaces that mirror the public
	// sub-API field grouping on *graph.Graph. Wired in New().
	Nodes       *NodeOps
	Rels        *RelOps
	Temporal    *TempOps
	Index       *IndexOps
	Events      *EventOps
	Admin       *AdminOps
	Constraints *ConstraintOps
	Hash        *HashOps
	IO          *IOOps
	Resolve     *ResolveOps
	Stats       *StatOps
}

// =============================================================================
// Errors
// =============================================================================

var (
	ErrNoLabels        = errors.New("graph: node requires at least one label")
	ErrNilNode         = types.ErrNilNode
	ErrNilRelationship = types.ErrNilRelationship
	ErrZeroID          = errors.New("graph: zero ID is not valid for import")
	ErrInvalidID       = errors.New("graph: invalid ID is not valid for import")
	ErrVersionOverflow = errors.New("graph: entity version overflow")
	ErrNotTieredStore  = errors.New("graph: operation requires tiered.Store")
	ErrAlreadyClosed   = errors.New("graph: entity already closed")
	ErrGraphClosed     = errors.New("graph: graph is closed")
	ErrNilGraph        = grapherr.ErrNilGraph
	ErrNilStore        = storepkg.ErrNilStore
	ErrNilContext      = errors.New("graph: context must not be nil")
	ErrNilTxCallback   = errors.New("graph: transaction callback must not be nil")
	ErrLabelNotFound   = errors.New("graph: node does not have the specified label")
	ErrLastLabel       = errors.New("graph: cannot remove the last label from a node")
	ErrBatchFailed     = errors.New("graph: batch execution had failed operations")
	ErrBatchDone       = errors.New("graph: batch already executed")
)

var (
	ErrTooManyLabels     = errors.New("graph: too many labels")
	ErrTooManyProperties = errors.New("graph: too many properties")
	ErrKeyTooLong        = errors.New("graph: property key too long")
	ErrValueTooLarge     = errors.New("graph: property value too large")
	ErrNameTooLong       = errors.New("graph: name too long")
	ErrSelfLoop          = errors.New("graph: self-loop relationship not allowed; set AllowSelfLoops in ValidationLimits to permit")

	// ErrValidFromBeforePrevious is returned by Update when the caller-supplied
	// tkg_valid_from is <= the previous version's effective ValidFrom. This
	// would create a backwards interval on the previous version's tile and is
	// rejected. Bitemporally correct backdating must use Phase 3's cascade
	// edit (SetVersionInterval) instead.
	ErrValidFromBeforePrevious = errors.New("graph: tkg_valid_from must be greater than previous version's effective ValidFrom")
)

// Re-exports of registry errors and index errors used by methods on *Core.
var (
	ErrEmptyName                  = registrypkg.ErrEmptyName
	ErrRegistryNotEmpty           = registrypkg.ErrRegistryNotEmpty
	ErrVectorIndexExists          = storepkg.ErrVectorIndexExists
	ErrVectorIndexNotFound        = storepkg.ErrVectorIndexNotFound
	ErrDimensionMismatch          = storepkg.ErrDimensionMismatch
	ErrInvalidTemporalIndexConfig = storepkg.ErrInvalidTemporalIndexConfig
	ErrInvalidVectorIndexConfig   = storepkg.ErrInvalidVectorIndexConfig
	ErrInvalidVectorValue         = storepkg.ErrInvalidVectorValue
	ErrInvalidTimeRange           = storepkg.ErrInvalidTimeRange
	ErrInvalidQueryLimit          = storepkg.ErrInvalidQueryLimit
	ErrInvalidQueryCursor         = storepkg.ErrInvalidQueryCursor
)

// =============================================================================
// Config & ValidationLimits
// =============================================================================

const (
	defaultMaxLabelsPerNode       = 50
	defaultMaxPropertiesPerEntity = 1000
	defaultMaxPropertyKeyLength   = 256
	defaultMaxPropertyValueSize   = 65536
	defaultMaxNameLength          = 256
)

// ValidationLimits configures limits on entity structure.
type ValidationLimits struct {
	MaxLabelsPerNode       int
	MaxPropertiesPerEntity int
	MaxPropertyKeyLength   int
	MaxPropertyValueSize   int
	MaxNameLength          int
	AllowSelfLoops         bool
}

// Config holds configuration for the Core.
type Config struct {
	SnowflakeNodeID int64
	// Store is the persistence backend. The accepted type is the
	// MandatoryStore composite (the union of always-required capabilities
	// from pkg/graph/store/capabilities.go); optional capabilities
	// (PropertyIndex / TemporalIndex / VectorIndex / HighFrequencyIndex)
	// are type-asserted at the call sites that need them and produce
	// ErrCapabilityNotSupported when missing. Every in-tree backend
	// (memory.Store, badger.Store, tiered.Store) satisfies the full
	// composition.
	Store                storepkg.MandatoryStore
	BadgerDir            string
	BadgerInMemory       bool
	Validation           ValidationLimits
	SyncWrites           bool
	Compression          options.CompressionType
	ZSTDCompressionLevel int
}

// ValidationDefaults returns the resolved validation limits (for testing).
func (c *Core) ValidationDefaults() ValidationLimits {
	return c.validation
}

func isExactNativeStore(store storepkg.MandatoryStore) bool {
	switch store.(type) {
	case *memory.Store, *badger.Store, *tiered.Store:
		return true
	default:
		return false
	}
}

var nativeStoreTypes = [...]reflect.Type{
	reflect.TypeOf((*memory.Store)(nil)),
	reflect.TypeOf((*badger.Store)(nil)),
	reflect.TypeOf((*tiered.Store)(nil)),
}

func embedsNativeCapability(store storepkg.MandatoryStore, capability reflect.Type, directMethods ...string) bool {
	if typeDeclaresMethods(reflect.TypeOf(store), directMethods...) {
		return false
	}
	return valueEmbedsNativeCapability(reflect.ValueOf(store), capability, make(map[reflect.Type]bool))
}

func typeDeclaresMethods(t reflect.Type, names ...string) bool {
	if len(names) == 0 {
		return false
	}
	for _, name := range names {
		if !typeDeclaresMethod(t, name) {
			return false
		}
	}
	return true
}

func typeDeclaresMethod(t reflect.Type, name string) bool {
	if t == nil {
		return false
	}
	candidates := []reflect.Type{t}
	if t.Kind() != reflect.Pointer {
		candidates = append(candidates, reflect.PointerTo(t))
	}
	for _, candidate := range candidates {
		method, ok := candidate.MethodByName(name)
		if !ok {
			continue
		}
		fn := runtime.FuncForPC(method.Func.Pointer())
		if fn == nil {
			continue
		}
		file, _ := fn.FileLine(method.Func.Pointer())
		// Promoted embedded-field methods are compiler wrappers reported from
		// <autogenerated>; source-backed methods are declared by the wrapper.
		if !strings.Contains(file, "<autogenerated>") {
			return true
		}
	}
	return false
}

func typeCanPromoteCapability(t, capability reflect.Type) bool {
	if t == nil {
		return false
	}
	if t.Implements(capability) {
		return true
	}
	if t.Kind() == reflect.Pointer {
		return false
	}
	return reflect.PointerTo(t).Implements(capability)
}

func nativeTypeCanPromoteCapability(t, capability reflect.Type) bool {
	for _, native := range nativeStoreTypes {
		if t == native {
			return native.Implements(capability)
		}
		if t.Kind() != reflect.Pointer && reflect.PointerTo(t) == native {
			return native.Implements(capability)
		}
	}
	return false
}

func typeEmbedsNativeCapability(t, capability reflect.Type, seen map[reflect.Type]bool) bool {
	if t == nil {
		return false
	}
	if nativeTypeCanPromoteCapability(t, capability) {
		return true
	}
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return false
	}
	if seen[t] {
		return false
	}
	seen[t] = true

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if !field.Anonymous {
			continue
		}
		ft := field.Type
		if !typeCanPromoteCapability(ft, capability) {
			continue
		}
		if nativeTypeCanPromoteCapability(ft, capability) {
			return true
		}
		if typeEmbedsNativeCapability(ft, capability, seen) {
			return true
		}
	}
	return false
}

func valueEmbedsNativeCapability(v reflect.Value, capability reflect.Type, seen map[reflect.Type]bool) bool {
	if !v.IsValid() {
		return false
	}
	t := v.Type()
	for t.Kind() == reflect.Interface {
		if v.IsNil() {
			return false
		}
		v = v.Elem()
		t = v.Type()
	}
	if nativeTypeCanPromoteCapability(t, capability) {
		return true
	}
	for t.Kind() == reflect.Pointer {
		if v.IsNil() {
			return typeEmbedsNativeCapability(t, capability, seen)
		}
		v = v.Elem()
		t = v.Type()
	}
	if t.Kind() != reflect.Struct {
		return false
	}
	if seen[t] {
		return false
	}
	seen[t] = true

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if !field.Anonymous {
			continue
		}
		ft := field.Type
		if !typeCanPromoteCapability(ft, capability) {
			continue
		}
		if nativeTypeCanPromoteCapability(ft, capability) {
			return true
		}
		fv := v.Field(i)
		if ft.Kind() == reflect.Interface {
			if !fv.IsNil() && fv.CanInterface() {
				if valueEmbedsNativeCapability(reflect.ValueOf(fv.Interface()), capability, seen) {
					return true
				}
			}
			continue
		}
		if valueEmbedsNativeCapability(fv, capability, seen) {
			return true
		}
	}
	return false
}

func nativeRelationshipEndpointHashWrite(store storepkg.MandatoryStore) generatedcreate.RelationshipEndpointHashCapability {
	cap, ok := store.(generatedcreate.RelationshipEndpointHashCapability)
	if !ok {
		return nil
	}
	// Wrapper stores often embed an in-tree backend but override PutRelationship
	// for instrumentation or fault injection. Do not route around that override
	// through an embedded generated-create method.
	switch store.(type) {
	case *memory.Store, *tiered.Store:
		return cap
	default:
		return nil
	}
}

func nativeEndpointIntegrityHash(store storepkg.MandatoryStore) storepkg.EndpointIntegrityHashCapability {
	cap, ok := store.(storepkg.EndpointIntegrityHashCapability)
	if !ok {
		return nil
	}
	// Endpoint hash reads replace GetNode calls on relationship create/update.
	// Keep that shortcut on exact in-tree stores so wrappers that override
	// GetNode for fault injection, stale reads, or policy checks stay visible.
	if isExactNativeStore(store) {
		return cap
	}
	if embedsNativeCapability(store, reflect.TypeOf((*storepkg.EndpointIntegrityHashCapability)(nil)).Elem(), "EndpointIntegrityHashes") {
		return nil
	}
	return cap
}

func filteredVectorSearchCapability(store storepkg.MandatoryStore) storepkg.FilteredVectorSearchCapability {
	cap, ok := store.(storepkg.FilteredVectorSearchCapability)
	if !ok {
		return nil
	}
	// This is both a correctness and performance hook. External backends may
	// implement it directly, but concrete wrappers that merely inherit an
	// in-tree method should still exercise their SearchNearestNodes override
	// through the graph-layer over-fetch fallback.
	if isExactNativeStore(store) {
		return cap
	}
	if embedsNativeCapability(store, reflect.TypeOf((*storepkg.FilteredVectorSearchCapability)(nil)).Elem(), "SearchNearestFiltered") {
		return nil
	}
	return cap
}

func propertyQueryCapability(store storepkg.MandatoryStore) storepkg.PropertyIndexCapability {
	cap, ok := store.(storepkg.PropertyIndexCapability)
	if !ok {
		return nil
	}
	// The graph can answer property equality by scanning NodesByLabel, so the
	// store capability is an acceleration/extension path. Let exact in-tree and
	// direct external implementations use it, but keep concrete wrappers on the
	// graph fallback so their NodesByLabel overrides stay visible.
	if isExactNativeStore(store) {
		return cap
	}
	if embedsNativeCapability(store, reflect.TypeOf((*storepkg.PropertyIndexCapability)(nil)).Elem(), "NodesByLabelAndProperty") {
		return nil
	}
	return cap
}

func nativeNodeIntegrityHash(store storepkg.MandatoryStore) storepkg.NodeIntegrityHashCapability {
	cap, ok := store.(storepkg.NodeIntegrityHashCapability)
	if !ok {
		return nil
	}
	// Node hash reads are the one-by-one fallback for endpoint hash reads and
	// must obey the same wrapper boundary as nativeEndpointIntegrityHash.
	if isExactNativeStore(store) {
		return cap
	}
	if embedsNativeCapability(store, reflect.TypeOf((*storepkg.NodeIntegrityHashCapability)(nil)).Elem(), "NodeIntegrityHash") {
		return nil
	}
	return cap
}

func nativeGeneratedCreate(store storepkg.MandatoryStore) generatedcreate.Capability {
	cap, ok := store.(generatedcreate.Capability)
	if !ok {
		return nil
	}
	// Keep generated-ID duplicate-probe shortcuts on exact in-tree stores only.
	// Embedded wrappers may override PutNode/PutRelationship/PutNodesBatch for
	// instrumentation or fault injection, and the fast path must not bypass them.
	switch store.(type) {
	case *tiered.Store:
		return cap
	default:
		return nil
	}
}

func nativeTransactionTimeQuery(store storepkg.MandatoryStore) storepkg.TransactionTimeQueryCapability {
	cap, ok := store.(storepkg.TransactionTimeQueryCapability)
	if !ok {
		return nil
	}
	// Transaction-time queries replace the mandatory Get*/history/iteration
	// path. Keep them native-only so wrapper stores can still inject read and
	// history faults through the mandatory Store methods.
	switch store.(type) {
	case *memory.Store:
		return cap
	default:
		if embedsNativeCapability(store, reflect.TypeOf((*storepkg.TransactionTimeQueryCapability)(nil)).Elem(),
			"NodeAsOf", "RelAsOf", "NodesAsOf", "RelsAsOf") {
			return nil
		}
		return cap
	}
}

func nativeHistoryRollbackTrim(store storepkg.MandatoryStore) storepkg.HistoryRollbackTrimCapability {
	cap, ok := store.(storepkg.HistoryRollbackTrimCapability)
	if !ok {
		return nil
	}
	// Rollback trimming replaces the generic truncate+restore history path.
	// Only exact native stores get that shortcut; wrappers must observe their
	// Truncate*/Put*Version hooks during rollback.
	switch store.(type) {
	case *memory.Store, *badger.Store:
		return cap
	default:
		if embedsNativeCapability(store, reflect.TypeOf((*storepkg.HistoryRollbackTrimCapability)(nil)).Elem(),
			"TrimNodeHistoryFrom", "TrimRelHistoryFrom") {
			return nil
		}
		return cap
	}
}

func historyVersionPageCapability(store storepkg.MandatoryStore) storepkg.HistoryVersionPageCapability {
	cap, ok := store.(storepkg.HistoryVersionPageCapability)
	if !ok {
		return nil
	}
	// Export can fall back to mandatory Get*History reads. Keep inherited
	// in-tree pagers off concrete wrappers so tests, fault injectors, and
	// policy wrappers that override Get*History stay observable.
	if isExactNativeStore(store) {
		return cap
	}
	if embedsNativeCapability(store, reflect.TypeOf((*storepkg.HistoryVersionPageCapability)(nil)).Elem(),
		"NodeHistoryVersionsFrom", "RelHistoryVersionsFrom") {
		return nil
	}
	return cap
}

func depthHistoryIterationCapability(store storepkg.MandatoryStore) storepkg.DepthHistoryIterationCapability {
	cap, ok := store.(storepkg.DepthHistoryIterationCapability)
	if !ok {
		return nil
	}
	// Depth-scoped history iteration is a tiered-store optimization. Concrete
	// wrappers that only inherit tiered's methods must fall back through their
	// mandatory ForEach*HistoryID hooks so policy/fault wrappers remain visible.
	if isExactNativeStore(store) {
		return cap
	}
	if embedsNativeCapability(store, reflect.TypeOf((*storepkg.DepthHistoryIterationCapability)(nil)).Elem(),
		"ForEachNodeHistoryIDByDepth", "ForEachRelHistoryIDByDepth") {
		return nil
	}
	return cap
}

func deletedIterationCapability(store storepkg.MandatoryStore) storepkg.DeletedIterationCapability {
	cap, ok := store.(storepkg.DeletedIterationCapability)
	if !ok {
		return nil
	}
	// Deleted iteration is an optional acceleration. Wrappers that only inherit
	// the native methods must fall through so wrapper-injected ForEach*HistoryID
	// behaviour remains observable on the candidate-fold path.
	if isExactNativeStore(store) {
		return cap
	}
	if embedsNativeCapability(store, reflect.TypeOf((*storepkg.DeletedIterationCapability)(nil)).Elem(),
		"ForEachDeletedNodeID", "ForEachDeletedRelID") {
		return nil
	}
	return cap
}

func depthDeletedIterationCapability(store storepkg.MandatoryStore) storepkg.DepthDeletedIterationCapability {
	cap, ok := store.(storepkg.DepthDeletedIterationCapability)
	if !ok {
		return nil
	}
	if isExactNativeStore(store) {
		return cap
	}
	if embedsNativeCapability(store, reflect.TypeOf((*storepkg.DepthDeletedIterationCapability)(nil)).Elem(),
		"ForEachDeletedNodeIDByDepth", "ForEachDeletedRelIDByDepth") {
		return nil
	}
	return cap
}

func nativeAdjacencyReadsValidateNodeExistence(store storepkg.MandatoryStore) bool {
	switch store.(type) {
	case *memory.Store, *badger.Store, *tiered.Store:
		return true
	default:
		return false
	}
}

// =============================================================================
// Snowflake helpers
// =============================================================================

// IDComponents holds the decomposed fields of a snowflake ID. Re-exported
// here so methods on *Core can use the type without importing pkg/graph.
type IDComponents = snowflakepkg.IDComponents

// DecomposeID extracts creation time, node ID, sequence number.
func DecomposeID(id snowflake.ID) IDComponents {
	return snowflakepkg.DecomposeID(id)
}

var snowflakeEpoch = snowflakepkg.Epoch

// =============================================================================
// Lifecycle
// =============================================================================

type registriesPersister interface {
	SaveRegistries(*registrypkg.LabelRegistry, *registrypkg.RelTypeRegistry) error
}

func (c *Core) persistRegistries() error {
	if rp, ok := c.store.(registriesPersister); ok {
		if err := rp.SaveRegistries(c.labels, c.relTypes); err != nil {
			c.registryDirty.Store(true)
			return fmt.Errorf("graph: save registries: %w", err)
		}
	}
	if pk, ok := c.store.(propertyKeyPersister); ok {
		if err := pk.SavePropertyKeyRegistry(c.propKeys); err != nil {
			c.registryDirty.Store(true)
			return fmt.Errorf("graph: save property-key registry: %w", err)
		}
	}
	c.registryDirty.Store(false)
	return nil
}

// propertyKeyPersister matches the badger.Store / tiered.Store property-key
// persistence shape. OPTIONAL — backends without persisted property-key
// registries are skipped.
type propertyKeyPersister interface {
	SavePropertyKeyRegistry(*registrypkg.PropertyKeyRegistry) error
	LoadPropertyKeyRegistry(*registrypkg.PropertyKeyRegistry) (bool, error)
}

// propertyKeyRegistrySetter matches the badger.Store / tiered.Store shape
// for installing the property-key registry on the store. The store uses
// the registry to tokenize property keys on the wire. OPTIONAL.
type propertyKeyRegistrySetter interface {
	SetPropertyKeyRegistry(*registrypkg.PropertyKeyRegistry)
}

func (c *Core) persistRegistriesIfDirtyLocked() error {
	if !c.registryDirty.Load() {
		return nil
	}
	return c.persistRegistries()
}

func (c *Core) persistRegistriesIfDirtyLockedPanicSafe() error {
	if !c.registryDirty.Load() {
		return nil
	}
	defer func() {
		if r := recover(); r != nil {
			c.registryMu.Unlock()
			panic(r)
		}
	}()
	return c.persistRegistries()
}

func (c *Core) checkpointDirtyRegistriesBeforeMutation(op string) error {
	if !c.registryDirty.Load() {
		return nil
	}
	c.registryMu.Lock()
	defer c.registryMu.Unlock()
	err := c.persistRegistriesIfDirtyLocked()
	if err != nil {
		return fmt.Errorf("graph: %s: %w", op, err)
	}
	return nil
}

// New creates a new Core with the given configuration. See pkg/graph.New for docs.
func New(config Config) (*Core, error) {
	if config.SnowflakeNodeID < 0 || config.SnowflakeNodeID > 15 {
		return nil, fmt.Errorf("graph: SnowflakeNodeID must be 0-15, got %d", config.SnowflakeNodeID)
	}
	if config.Store != nil && isNilInterfaceValue(config.Store) {
		return nil, ErrNilStore
	}

	nodeGen, err := snowflake.NewNode(config.SnowflakeNodeID*2,
		snowflake.WithEpoch(snowflakeEpoch),
		snowflake.WithMicroseconds(),
		snowflake.WithNodeBits(5),
		snowflake.WithStepBits(10),
	)
	if err != nil {
		return nil, fmt.Errorf("graph: node ID generator: %w", err)
	}
	relGen, err := snowflake.NewNode(config.SnowflakeNodeID*2+1,
		snowflake.WithEpoch(snowflakeEpoch),
		snowflake.WithMicroseconds(),
		snowflake.WithNodeBits(5),
		snowflake.WithStepBits(10),
	)
	if err != nil {
		return nil, fmt.Errorf("graph: rel ID generator: %w", err)
	}

	v := config.Validation
	if v.MaxLabelsPerNode == 0 {
		v.MaxLabelsPerNode = defaultMaxLabelsPerNode
	}
	if v.MaxPropertiesPerEntity == 0 {
		v.MaxPropertiesPerEntity = defaultMaxPropertiesPerEntity
	}
	if v.MaxPropertyKeyLength == 0 {
		v.MaxPropertyKeyLength = defaultMaxPropertyKeyLength
	}
	if v.MaxPropertyValueSize == 0 {
		v.MaxPropertyValueSize = defaultMaxPropertyValueSize
	}
	if v.MaxNameLength == 0 {
		v.MaxNameLength = defaultMaxNameLength
	}

	if v.MaxLabelsPerNode < 0 || v.MaxPropertiesPerEntity < 0 ||
		v.MaxPropertyKeyLength < 0 || v.MaxPropertyValueSize < 0 ||
		v.MaxNameLength < 0 {
		return nil, fmt.Errorf("graph: validation limits must not be negative")
	}

	c := &Core{
		labels:         registrypkg.NewLabelRegistry(),
		relTypes:       registrypkg.NewRelTypeRegistry(),
		propKeys:       registrypkg.NewPropertyKeyRegistry(),
		nodeIDGen:      nodeGen,
		relIDGen:       relGen,
		entityLocks:    locks.NewManager(),
		validation:     v,
		indexProviders: make(map[string]*indexProviderEntry),
		relTypeCache:   make(map[string]uint16),
		clock:          time.Now,
	}
	c.Nodes = &NodeOps{c: c}
	c.Rels = &RelOps{c: c}
	c.Temporal = &TempOps{c: c}
	c.Index = &IndexOps{c: c}
	c.Events = &EventOps{c: c}
	c.Admin = &AdminOps{c: c}
	c.Constraints = &ConstraintOps{c: c}
	c.Hash = &HashOps{c: c}
	c.IO = &IOOps{c: c}
	c.Resolve = &ResolveOps{c: c}
	c.Stats = &StatOps{c: c}

	if config.Store == nil && config.BadgerDir != "" {
		if strings.TrimSpace(config.BadgerDir) == "" {
			return nil, fmt.Errorf("graph: BadgerDir is whitespace-only; use a valid path or omit for MemoryStore")
		}
	}

	store := config.Store
	if store == nil {
		if config.BadgerDir != "" || config.BadgerInMemory {
			bs, err := badger.New(badger.Config{
				Dir:                  config.BadgerDir,
				InMemory:             config.BadgerInMemory,
				SyncWrites:           config.SyncWrites,
				Compression:          config.Compression,
				ZSTDCompressionLevel: config.ZSTDCompressionLevel,
			})
			if err != nil {
				return nil, fmt.Errorf("graph: badger store: %w", err)
			}
			loadedLabels := registrypkg.NewLabelRegistry()
			if found, err := bs.LoadLabelRegistry(loadedLabels); err != nil {
				_ = bs.Close()
				return nil, fmt.Errorf("graph: load label registry: %w", err)
			} else if found {
				if err := c.validateRegistryNames("label", loadedLabels.ExportNames()); err != nil {
					_ = bs.Close()
					return nil, fmt.Errorf("graph: load label registry: %w", err)
				}
				c.labels = loadedLabels
			}
			loadedRelTypes := registrypkg.NewRelTypeRegistry()
			if found, err := bs.LoadRelTypeRegistry(loadedRelTypes); err != nil {
				_ = bs.Close()
				return nil, fmt.Errorf("graph: load reltype registry: %w", err)
			} else if found {
				if err := c.validateRegistryNames("reltype", loadedRelTypes.ExportNames()); err != nil {
					_ = bs.Close()
					return nil, fmt.Errorf("graph: load reltype registry: %w", err)
				}
				c.relTypes = loadedRelTypes
			}
			store = bs
		} else {
			store = memory.New()
		}
	}

	c.store = store
	c.generatedCreate = nativeGeneratedCreate(store)
	c.endpointHash = nativeEndpointIntegrityHash(store)
	c.endpointHashWrite = nativeRelationshipEndpointHashWrite(store)
	c.nodeHash = nativeNodeIntegrityHash(store)
	c.txTimeQuery = nativeTransactionTimeQuery(store)
	_, txTimeQueryIsNativeMemory := store.(*memory.Store)
	c.txTimeQueryCopy = c.txTimeQuery != nil && !txTimeQueryIsNativeMemory
	c.historyTrim = nativeHistoryRollbackTrim(store)
	c.propertyQuery = propertyQueryCapability(store)
	c.propertyQueryTrust = isExactNativeStore(store)
	c.filteredVector = filteredVectorSearchCapability(store)
	c.depthHistory = depthHistoryIterationCapability(store)
	c.deletedIter = deletedIterationCapability(store)
	c.deletedDepthIter = depthDeletedIterationCapability(store)
	c.vectorRowsTrust = isExactNativeStore(store)
	c.storeRowsTrust = isExactNativeStore(store)
	c.nativeAdjacency = nativeAdjacencyReadsValidateNodeExistence(store)

	// Registry rehydration for caller-injected stores. The Core-
	// constructed badger.Store path above already loads registries; the
	// inject path also has to (R4-F1). Without this, opening an
	// existing badger.Store separately and passing it via
	// `Config{Store: bs}` would start the graph with empty in-memory
	// registries even though the persisted entities use tokenised
	// labels and reltypes — Close would then save the empty registry
	// state back over the persisted mappings.
	//
	// Two interface shapes exist in-tree because badger and tiered
	// landed at different times — badger.Store returns (bool, error)
	// from its loaders while tiered.Store returns (int, error). Both
	// are matched here so the rehydration is dispatched by capability
	// rather than concrete type.
	if config.Store != nil {
		if lr, ok := store.(badgerRegistryLoader); ok {
			loadedLabels := registrypkg.NewLabelRegistry()
			if found, err := lr.LoadLabelRegistry(loadedLabels); err != nil {
				return nil, fmt.Errorf("graph: load label registry from injected store: %w", err)
			} else if found {
				if err := c.validateRegistryNames("label", loadedLabels.ExportNames()); err != nil {
					return nil, fmt.Errorf("graph: load label registry from injected store: %w", err)
				}
				c.labels = loadedLabels
			}
			loadedRelTypes := registrypkg.NewRelTypeRegistry()
			if found, err := lr.LoadRelTypeRegistry(loadedRelTypes); err != nil {
				return nil, fmt.Errorf("graph: load reltype registry from injected store: %w", err)
			} else if found {
				if err := c.validateRegistryNames("reltype", loadedRelTypes.ExportNames()); err != nil {
					return nil, fmt.Errorf("graph: load reltype registry from injected store: %w", err)
				}
				c.relTypes = loadedRelTypes
			}
		}
		if ts, ok := store.(tieredRegistryLoader); ok {
			loadedLabels := registrypkg.NewLabelRegistry()
			if n, err := ts.LoadLabelRegistry(loadedLabels); err != nil {
				return nil, fmt.Errorf("graph: load label registry from injected tiered store: %w", err)
			} else if n > 0 {
				if err := c.validateRegistryNames("label", loadedLabels.ExportNames()); err != nil {
					return nil, fmt.Errorf("graph: load label registry from injected tiered store: %w", err)
				}
				c.labels = loadedLabels
			}
			loadedRelTypes := registrypkg.NewRelTypeRegistry()
			if n, err := ts.LoadRelTypeRegistry(loadedRelTypes); err != nil {
				return nil, fmt.Errorf("graph: load reltype registry from injected tiered store: %w", err)
			} else if n > 0 {
				if err := c.validateRegistryNames("reltype", loadedRelTypes.ExportNames()); err != nil {
					return nil, fmt.Errorf("graph: load reltype registry from injected tiered store: %w", err)
				}
				c.relTypes = loadedRelTypes
			}
			setTieredLabelRegistryIfSupported(store, c.labels)
		}
	}

	if pk, ok := store.(propertyKeyPersister); ok {
		loaded := registrypkg.NewPropertyKeyRegistry()
		if found, err := pk.LoadPropertyKeyRegistry(loaded); err != nil {
			return nil, fmt.Errorf("graph: load property-key registry: %w", err)
		} else if found {
			c.propKeys = loaded
		}
	}
	// Install the registry on the store so the wire encoder / decoder
	// dictionary-encode property keys. Optional capability — backends
	// without it keep the pre-tokenization on-disk format.
	if setter, ok := store.(propertyKeyRegistrySetter); ok {
		setter.SetPropertyKeyRegistry(c.propKeys)
	}

	c.runBitemporalMigrationBestEffort()

	return c, nil
}

// badgerRegistryLoader matches the badger.Store rehydration shape.
// Backends that persist registries with the same `(found bool, err
// error)` signature as badger satisfy this interface and get
// automatic rehydration on graph construction. Tiered stores have a
// different signature and are dispatched by tieredRegistryLoader above.
type badgerRegistryLoader interface {
	LoadLabelRegistry(*registrypkg.LabelRegistry) (bool, error)
	LoadRelTypeRegistry(*registrypkg.RelTypeRegistry) (bool, error)
}

// Close saves registries (if Badger) and closes the underlying store.
//
// Close is serialized against in-flight standalone mutations and reads
// (R4-F3). The closed-state flag is set BEFORE acquiring c.mu.Lock so
// that any RLock acquired after Close releases its Lock observes
// closed=true and short-circuits with ErrGraphClosed (see
// runUnderRLock). Provider drain happens under c.mu.Lock so it cannot
// race with concurrent RegisterProvider; provider Close is then run
// outside the lock so a slow Close cannot block the lifecycle lock.
func (c *Core) Close() error {
	var closeErr error
	c.closeOnce.Do(func() {
		c.closed.Store(true)

		// Drain in-flight RLock holders. New mutations that win the
		// race to RLock between Store and Lock observe closed=true via
		// the defer-protected check inside runUnderRLock and return
		// ErrGraphClosed without touching the store.
		c.mu.Lock()
		entries := make([]*indexProviderEntry, 0, len(c.indexProviders))
		for _, e := range c.indexProviders {
			entries = append(entries, e)
		}
		c.indexProviders = make(map[string]*indexProviderEntry)
		c.mu.Unlock()

		// Provider Close runs outside the lifecycle lock — providers
		// may flush/close their own backends and we do not want to
		// hold the graph lock for that latency. Initializable
		// providers are registered before Init runs, so wait for any
		// in-flight Init callback before invoking Close.
		for _, e := range entries {
			e.unsubscribe()
			e.waitInit()
			closeErr = errors.Join(closeErr, closeIndexProvider(e))
		}

		closeErr = errors.Join(closeErr, c.persistRegistries())
		closeErr = errors.Join(closeErr, c.store.Close())
	})
	return closeErr
}

func closeIndexProvider(e *indexProviderEntry) (err error) {
	providerName := "<unknown>"
	defer func() {
		if r := recover(); r != nil {
			err = errors.Join(err, fmt.Errorf("index provider %q close panic: %v", providerName, r))
		}
	}()

	e.stopEvents()
	providerName = e.provider.Name()
	if closeErr := e.provider.Close(); closeErr != nil {
		err = errors.Join(err, fmt.Errorf("index provider %q close: %w", providerName, closeErr))
	}
	return err
}
