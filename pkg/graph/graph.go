package graph

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/badger"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/memory"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/tiered"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/dgraph-io/badger/v4/options"

	eventspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/events"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/locks"
	registrypkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/registry"
)

// =============================================================================
// Graph struct
// =============================================================================

// Graph is the central graph layer. It owns the label and relationship type
// registries, snowflake ID generators, store, and provides string resolution
// for token-based entities.
//
// Entity locks serialize AddRelationship and DeleteNode on overlapping entities
// to prevent write-skew (concurrent AddRelationship(→X) + DeleteNodeCascade(X)
// producing a dangling edge).
type Graph struct {
	labels        *registrypkg.LabelRegistry
	relTypes      *registrypkg.RelTypeRegistry
	nodeIDGen     *snowflake.Node
	relIDGen      *snowflake.Node
	store         storepkg.Store
	entityLocks   *locks.Manager
	validation    ValidationLimits
	constraints   ConstraintSet       // temporal constraints checked at relationship write time
	events        eventspkg.Publisher // nil = no event publishing; set via SetEventBus/SetAsyncEventBus
	txEventBuffer *[]eventspkg.Event  // non-nil while a tx holds g.mu.Lock — events buffered, not dispatched
	mu            sync.RWMutex        // serializes batch/tx writes vs standalone mutations and reads
	closeOnce     sync.Once

	// Index providers registered via RegisterIndexProvider. Keyed by Name().
	// Each entry holds an unsubscribe closure so UnregisterIndexProvider can
	// detach cleanly. See index_provider.go for semantics.
	indexProviders map[string]*indexProviderEntry

	// Operation counters — incremented atomically on every successful operation.
	opNodeAdds    atomic.Int64
	opNodeReads   atomic.Int64
	opNodeUpdates atomic.Int64
	opNodeDeletes atomic.Int64
	opRelAdds     atomic.Int64
	opRelReads    atomic.Int64
	opRelUpdates  atomic.Int64
	opRelDeletes  atomic.Int64
}

// =============================================================================
// Errors
// =============================================================================

// Sentinel errors for entity management.
var (
	ErrNoLabels         = errors.New("graph: node requires at least one label")
	ErrNilNode          = errors.New("graph: node must not be nil")
	ErrZeroID           = errors.New("graph: zero ID is not valid for import")
	ErrNotTieredStore   = errors.New("graph: operation requires tiered.Store")
	ErrAlreadyClosed    = errors.New("graph: entity already closed")
	ErrInvalidTimeRange = errors.New("graph: invalid time range")
	ErrLabelNotFound    = errors.New("graph: node does not have the specified label")
	ErrLastLabel        = errors.New("graph: cannot remove the last label from a node")
	// ErrDepthTemporalUnsupported is returned when storepkg.QueryOpts combines a
	// non-default Depth with a temporal filter. The history-aware
	// resolution path enumerates IDs through ForEach* iterators that have
	// no storepkg.QueryOpts, so the underlying Store cannot honor Depth in that
	// path — surface the limitation to the caller rather than silently
	// returning entities the caller asked to exclude.
	ErrDepthTemporalUnsupported = errors.New("graph: opts.Depth is not supported with a temporal filter")
)

// Sentinel errors for validation limits.
var (
	ErrTooManyLabels     = errors.New("graph: too many labels")
	ErrTooManyProperties = errors.New("graph: too many properties")
	ErrKeyTooLong        = errors.New("graph: property key too long")
	ErrValueTooLarge     = errors.New("graph: property value too large")
	ErrNameTooLong       = errors.New("graph: name too long")
	ErrSelfLoop          = errors.New("graph: self-loop relationship not allowed; set AllowSelfLoops in ValidationLimits to permit")
)

// =============================================================================
// Config & ValidationLimits
// =============================================================================

// Default validation limits — generous enough for normal use, restrictive enough
// to catch runaway callers.
const (
	defaultMaxLabelsPerNode       = 50
	defaultMaxPropertiesPerEntity = 1000
	defaultMaxPropertyKeyLength   = 256
	defaultMaxPropertyValueSize   = 65536 // 64 KiB, string values only
	defaultMaxNameLength          = 256   // label and reltype names
)

// ValidationLimits configures limits on entity structure.
// Zero values are resolved to defaults in New().
type ValidationLimits struct {
	MaxLabelsPerNode       int  // Default: 50
	MaxPropertiesPerEntity int  // Default: 1000
	MaxPropertyKeyLength   int  // Default: 256
	MaxPropertyValueSize   int  // Default: 65536 (string values only)
	MaxNameLength          int  // Default: 256 (label and reltype names)
	AllowSelfLoops         bool // Default: false — reject relationships where startNode == endNode
}

// Config holds configuration for the Graph.
type Config struct {
	// SnowflakeNodeID identifies this graph instance (0-15).
	// Internally mapped to even/odd generator pair (nodeGen=ID*2, relGen=ID*2+1)
	// to guarantee value-level uniqueness across node and relationship IDs.
	// Each concurrent instance must use a different value.
	SnowflakeNodeID int64

	// Store is the persistence backend. If nil, memory.New() is used
	// unless BadgerDir or BadgerInMemory is set.
	Store storepkg.Store

	// BadgerDir is the Badger data directory. If set and Store is nil,
	// a BadgerStore is created. Ignored if Store is non-nil.
	BadgerDir string

	// BadgerInMemory enables in-memory Badger mode (useful for testing).
	// If true and Store is nil, a BadgerStore with InMemory=true is created.
	BadgerInMemory bool

	// Validation configures limits on entity structure.
	// Zero fields use defaults.
	Validation ValidationLimits

	// SyncWrites enables synchronous disk writes for every mutation.
	// When true, each write is flushed to stable storage before returning.
	// Ignored for MemoryStore or when Store is explicitly provided.
	// Default false (async background flush via FlushInterval).
	SyncWrites bool

	// Compression sets the SSTable compression algorithm for the convenience
	// BadgerStore path (BadgerDir / BadgerInMemory). Ignored when Store is set.
	// Valid values: options.None (0), options.Snappy (1), options.ZSTD (2).
	// Zero keeps the Badger default (Snappy).
	Compression options.CompressionType
	// ZSTDCompressionLevel sets the ZSTD compression level (1-15) for the
	// convenience BadgerStore path. Ignored when Store is set.
	// Only effective when Compression is options.ZSTD.
	// Zero keeps the Badger default (1).
	ZSTDCompressionLevel int
}

// ValidationDefaults returns the resolved validation limits (for testing).
func (g *Graph) ValidationDefaults() ValidationLimits {
	return g.validation
}

// =============================================================================
// Lifecycle
// =============================================================================

// registriesPersister is the optional interface implemented by Store
// backends that can persist both the label and reltype registries atomically.
// Both BadgerStore (single-txn) and tiered.Store (single registry-file write)
// satisfy this interface; MemoryStore does not need to.
type registriesPersister interface {
	SaveRegistries(*registrypkg.LabelRegistry, *registrypkg.RelTypeRegistry) error
}

// New creates a new Graph with the given configuration.
// Returns an error if SnowflakeNodeID is out of range (0-15).
// The ID is mapped to an even/odd pair (ID*2 for nodes, ID*2+1 for rels)
// to guarantee value-level uniqueness across entity types.
//
// Store selection priority:
//  1. config.Store (explicit injection)
//  2. BadgerStore (if BadgerDir or BadgerInMemory is set)
//  3. MemoryStore (default)
//
// When a BadgerStore is created, registries are loaded from persisted data.
// Call Close() when done to save registries and close the store.
func New(config Config) (*Graph, error) {
	if config.SnowflakeNodeID < 0 || config.SnowflakeNodeID > 15 {
		return nil, fmt.Errorf("graph: SnowflakeNodeID must be 0-15, got %d", config.SnowflakeNodeID)
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

	// Resolve zero validation limits to defaults.
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

	g := &Graph{
		labels:         registrypkg.NewLabelRegistry(),
		relTypes:       registrypkg.NewRelTypeRegistry(),
		nodeIDGen:      nodeGen,
		relIDGen:       relGen,
		entityLocks:    locks.NewManager(),
		validation:     v,
		indexProviders: make(map[string]*indexProviderEntry),
	}

	// Validate BadgerDir: reject whitespace-only strings (silent fallback hazard).
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

			// Load persisted registries. Fail fast if the saved data is corrupt.
			if _, err := bs.LoadLabelRegistry(g.labels); err != nil {
				_ = bs.Close() // best-effort cleanup; returning primary error
				return nil, fmt.Errorf("graph: load label registry: %w", err)
			}
			if _, err := bs.LoadRelTypeRegistry(g.relTypes); err != nil {
				_ = bs.Close() // best-effort cleanup; returning primary error
				return nil, fmt.Errorf("graph: load reltype registry: %w", err)
			}

			store = bs
		} else {
			store = memory.New()
		}
	}

	g.store = store

	// Wire tiered.Store to the label registry for ontology token resolution.
	if ts, ok := store.(*tiered.Store); ok {
		ts.SetLabelRegistry(g.labels)
		if _, err := ts.LoadLabelRegistry(g.labels); err != nil {
			_ = ts.Close() // best-effort cleanup; returning primary error
			return nil, fmt.Errorf("graph: load label registry: %w", err)
		}
		if _, err := ts.LoadRelTypeRegistry(g.relTypes); err != nil {
			_ = ts.Close() // best-effort cleanup; returning primary error
			return nil, fmt.Errorf("graph: load reltype registry: %w", err)
		}
	}

	return g, nil
}

// Close saves registries (if Badger) and closes the underlying store.
// Safe to call concurrently and multiple times.
//
// store.Close() always runs even if registry saves fail — prevents resource leaks.
// Returns all errors joined; subsequent calls return nil.
func (g *Graph) Close() error {
	var closeErr error
	g.closeOnce.Do(func() {
		// Close index providers before the store so they can flush their
		// own state. Errors are collected; store close still runs.
		closeErr = errors.Join(closeErr, g.closeIndexProviders())

		// Save registries if the store supports atomic persistence.
		// Both BadgerStore and tiered.Store satisfy registriesPersister; the
		// type-assertion lets us go through a single uniform path that writes
		// label and reltype registries atomically.
		if rp, ok := g.store.(registriesPersister); ok {
			if err := rp.SaveRegistries(g.labels, g.relTypes); err != nil {
				closeErr = errors.Join(closeErr, fmt.Errorf("graph: save registries: %w", err))
			}
		}
		// Always close the store — even if registry saves failed.
		closeErr = errors.Join(closeErr, g.store.Close())
	})
	return closeErr
}
