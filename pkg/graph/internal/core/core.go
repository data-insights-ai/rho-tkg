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
	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/index"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/locks"
	registrypkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/registry"
	snowflakepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/snowflake"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/badger"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/memory"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/tiered"
)

// =============================================================================
// Core struct (was *graph.Graph)
// =============================================================================

// Core is the central graph implementation. Customers see *graph.Graph, which
// is a thin facade holding *Core plus sub-API accessors.
type Core struct {
	labels        *registrypkg.LabelRegistry
	relTypes      *registrypkg.RelTypeRegistry
	nodeIDGen     *snowflake.Node
	relIDGen      *snowflake.Node
	store         storepkg.Store
	entityLocks   *locks.Manager
	validation    ValidationLimits
	constraints   ConstraintSet
	events        eventspkg.Publisher
	txEventBuffer *[]eventspkg.Event
	mu            sync.RWMutex
	closeOnce     sync.Once

	indexProviders map[string]*indexProviderEntry

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

var (
	ErrNoLabels                 = errors.New("graph: node requires at least one label")
	ErrNilNode                  = errors.New("graph: node must not be nil")
	ErrZeroID                   = errors.New("graph: zero ID is not valid for import")
	ErrNotTieredStore           = errors.New("graph: operation requires tiered.Store")
	ErrAlreadyClosed            = errors.New("graph: entity already closed")
	ErrInvalidTimeRange         = errors.New("graph: invalid time range")
	ErrLabelNotFound            = errors.New("graph: node does not have the specified label")
	ErrLastLabel                = errors.New("graph: cannot remove the last label from a node")
	ErrDepthTemporalUnsupported = errors.New("graph: opts.Depth is not supported with a temporal filter")
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
	ErrEmptyName           = registrypkg.ErrEmptyName
	ErrRegistryNotEmpty    = registrypkg.ErrRegistryNotEmpty
	ErrVectorIndexExists   = indexpkg.ErrVectorIndexExists
	ErrVectorIndexNotFound = indexpkg.ErrVectorIndexNotFound
	ErrDimensionMismatch   = indexpkg.ErrDimensionMismatch
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
	SnowflakeNodeID      int64
	Store                storepkg.Store
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

var (
	snowflakeEpoch  time.Time        = snowflakepkg.Epoch
	snowflakeLayout snowflake.Layout = snowflakepkg.Layout
)

// =============================================================================
// Lifecycle
// =============================================================================

type registriesPersister interface {
	SaveRegistries(*registrypkg.LabelRegistry, *registrypkg.RelTypeRegistry) error
}

// New creates a new Core with the given configuration. See pkg/graph.New for docs.
func New(config Config) (*Core, error) {
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
	}

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
			if _, err := bs.LoadLabelRegistry(c.labels); err != nil {
				_ = bs.Close()
				return nil, fmt.Errorf("graph: load label registry: %w", err)
			}
			if _, err := bs.LoadRelTypeRegistry(c.relTypes); err != nil {
				_ = bs.Close()
				return nil, fmt.Errorf("graph: load reltype registry: %w", err)
			}
			store = bs
		} else {
			store = memory.New()
		}
	}

	c.store = store

	if ts, ok := store.(*tiered.Store); ok {
		ts.SetLabelRegistry(c.labels)
		if _, err := ts.LoadLabelRegistry(c.labels); err != nil {
			_ = ts.Close()
			return nil, fmt.Errorf("graph: load label registry: %w", err)
		}
		if _, err := ts.LoadRelTypeRegistry(c.relTypes); err != nil {
			_ = ts.Close()
			return nil, fmt.Errorf("graph: load reltype registry: %w", err)
		}
	}

	return c, nil
}

// Close saves registries (if Badger) and closes the underlying store.
func (c *Core) Close() error {
	var closeErr error
	c.closeOnce.Do(func() {
		closeErr = errors.Join(closeErr, c.closeIndexProviders())

		if rp, ok := c.store.(registriesPersister); ok {
			if err := rp.SaveRegistries(c.labels, c.relTypes); err != nil {
				closeErr = errors.Join(closeErr, fmt.Errorf("graph: save registries: %w", err))
			}
		}
		closeErr = errors.Join(closeErr, c.store.Close())
	})
	return closeErr
}
