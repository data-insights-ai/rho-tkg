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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/dgraph-io/badger/v4/options"

	eventspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/events"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/generatedcreate"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/grapherr"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/locks"
	registrypkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/registry"
	snowflakepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/snowflake"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/badger"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/memory"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/tiered"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// =============================================================================
// Core struct (was *graph.Graph)
// =============================================================================

// Core is the central graph implementation. Customers see *graph.Graph, which
// is a thin facade holding *Core plus sub-API accessors.
type Core struct {
	labels            *registrypkg.LabelRegistry
	relTypes          *registrypkg.RelTypeRegistry
	nodeIDGen         *snowflake.Node
	relIDGen          *snowflake.Node
	store             storepkg.MandatoryStore
	endpointHash      storepkg.EndpointIntegrityHashCapability
	endpointHashWrite generatedcreate.RelationshipEndpointHashCapability
	nodeHash          storepkg.NodeIntegrityHashCapability
	txTimeQuery       storepkg.TransactionTimeQueryCapability
	historyTrim       storepkg.HistoryRollbackTrimCapability
	nativeAdjacency   bool
	entityLocks       *locks.Manager
	validation        ValidationLimits
	constraints       ConstraintSet
	events            eventspkg.Publisher
	txEventBuffer     *[]eventspkg.Event
	mu                sync.RWMutex
	registryMu        sync.Mutex
	relTypeCache      map[string]uint16
	relTypeCacheMu    sync.RWMutex
	closeOnce         sync.Once
	closed            atomic.Bool

	// clock is the time source used by every mutation path that stamps
	// TxFrom / UpdatedAt / DeletedAt / event.Timestamp. Defaults to
	// time.Now in New(); test helpers swap it for a deterministic
	// counter so two consecutive mutations yield strictly-increasing
	// timestamps without the wall-clock sleeps that otherwise flake on
	// loaded CI hardware (R4-F20). Only ever read from goroutines that
	// hold the appropriate Core lock — the value is set once in New
	// and (in tests) replaced under exclusive access.
	clock func() time.Time

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
	ErrNoLabels                 = errors.New("graph: node requires at least one label")
	ErrNilNode                  = types.ErrNilNode
	ErrNilRelationship          = types.ErrNilRelationship
	ErrZeroID                   = errors.New("graph: zero ID is not valid for import")
	ErrInvalidID                = errors.New("graph: invalid ID is not valid for import")
	ErrVersionOverflow          = errors.New("graph: entity version overflow")
	ErrNotTieredStore           = errors.New("graph: operation requires tiered.Store")
	ErrAlreadyClosed            = errors.New("graph: entity already closed")
	ErrGraphClosed              = errors.New("graph: graph is closed")
	ErrNilGraph                 = grapherr.ErrNilGraph
	ErrNilStore                 = storepkg.ErrNilStore
	ErrNilContext               = errors.New("graph: context must not be nil")
	ErrNilTxCallback            = errors.New("graph: transaction callback must not be nil")
	ErrLabelNotFound            = errors.New("graph: node does not have the specified label")
	ErrLastLabel                = errors.New("graph: cannot remove the last label from a node")
	ErrDepthTemporalUnsupported = errors.New("graph: legacy depth/temporal sentinel")
	ErrBatchFailed              = errors.New("graph: batch execution had failed operations")
	ErrBatchDone                = errors.New("graph: batch already executed")
)

var (
	ErrTooManyLabels     = errors.New("graph: too many labels")
	ErrTooManyProperties = errors.New("graph: too many properties")
	ErrKeyTooLong        = errors.New("graph: property key too long")
	ErrValueTooLarge     = errors.New("graph: property value too large")
	ErrNameTooLong       = errors.New("graph: name too long")
	ErrSelfLoop          = errors.New("graph: self-loop relationship not allowed; set AllowSelfLoops in ValidationLimits to permit")
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
			return fmt.Errorf("graph: save registries: %w", err)
		}
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
	c.endpointHash, _ = store.(storepkg.EndpointIntegrityHashCapability)
	c.endpointHashWrite = nativeRelationshipEndpointHashWrite(store)
	c.nodeHash, _ = store.(storepkg.NodeIntegrityHashCapability)
	c.txTimeQuery, _ = store.(storepkg.TransactionTimeQueryCapability)
	c.historyTrim, _ = store.(storepkg.HistoryRollbackTrimCapability)
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
		if ts, ok := store.(*tiered.Store); ok {
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
			ts.SetLabelRegistry(c.labels)
		}
	}

	return c, nil
}

// badgerRegistryLoader matches the badger.Store rehydration shape.
// Backends that persist registries with the same `(found bool, err
// error)` signature as badger satisfy this interface and get
// automatic rehydration on graph construction. Tiered stores have a
// different signature and are dispatched by concrete type just above.
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
			if err := e.provider.Close(); err != nil {
				closeErr = errors.Join(closeErr, fmt.Errorf("index provider %q close: %w", e.provider.Name(), err))
			}
		}

		closeErr = errors.Join(closeErr, c.persistRegistries())
		closeErr = errors.Join(closeErr, c.store.Close())
	})
	return closeErr
}
