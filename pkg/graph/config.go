package graph

import (
	"errors"

	"github.com/dgraph-io/badger/v4/options"
)

// Sentinel errors for entity management.
var (
	ErrNoLabels         = errors.New("graph: node requires at least one label")
	ErrNilNode          = errors.New("graph: node must not be nil")
	ErrZeroID           = errors.New("graph: zero ID is not valid for import")
	ErrNotTieredStore   = errors.New("graph: operation requires TieredStore")
	ErrAlreadyClosed    = errors.New("graph: entity already closed")
	ErrInvalidTimeRange = errors.New("graph: invalid time range")
	ErrLabelNotFound    = errors.New("graph: node does not have the specified label")
	ErrLastLabel        = errors.New("graph: cannot remove the last label from a node")
	// ErrDepthTemporalUnsupported is returned when QueryOpts combines a
	// non-default Depth with a temporal filter. The history-aware
	// resolution path enumerates IDs through ForEach* iterators that have
	// no QueryOpts, so the underlying Store cannot honor Depth in that
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

// snowflakeEpoch and snowflakeLayout are defined in aliases.go as
// references to internal/store. They preserve the package-level
// identifiers used throughout pkg/graph after the v3.1.17 restructure
// pulled the canonical definitions into pkg/graph/internal/store.

// Config holds configuration for the Graph.
type Config struct {
	// SnowflakeNodeID identifies this graph instance (0-15).
	// Internally mapped to even/odd generator pair (nodeGen=ID*2, relGen=ID*2+1)
	// to guarantee value-level uniqueness across node and relationship IDs.
	// Each concurrent instance must use a different value.
	SnowflakeNodeID int64

	// Store is the persistence backend. If nil, NewMemoryStore() is used
	// unless BadgerDir or BadgerInMemory is set.
	Store Store

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
