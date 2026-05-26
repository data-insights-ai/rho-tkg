package graph

import (
	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/index"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/internal/core"
	registrypkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/internal/registry"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/store"
)

// ErrCapabilityNotSupported is the capability-missing sentinel — returned
// (wrapped with the missing-capability name) when a graph operation needs
// an optional Store capability that the configured backend does not
// implement. Callers MUST check via errors.Is(err, ErrCapabilityNotSupported);
// the wrapping message is diagnostic only.
var ErrCapabilityNotSupported = storepkg.ErrCapabilityNotSupported

// Store entity sentinels re-exported for public Graph API callers.
// Graph methods return ErrNodeNotFound / ErrRelNotFound for missing current
// entities (including bulk GetByIDs requests with any missing explicit ID),
// and ErrNodeExists / ErrRelExists for caller-supplied-ID create paths
// (Import / AddByIDIfAbsent) when an entity at that ID is already present.
var (
	ErrNodeNotFound = storepkg.ErrNodeNotFound
	ErrRelNotFound  = storepkg.ErrRelNotFound
	ErrNodeExists   = storepkg.ErrNodeExists
	ErrRelExists    = storepkg.ErrRelExists
)

// Index sentinel errors. Returned from the index sub-API.
var (
	ErrIndexExists                = storepkg.ErrIndexExists
	ErrIndexNotFound              = storepkg.ErrIndexNotFound
	ErrTemporalIndexExists        = storepkg.ErrTemporalIndexExists
	ErrTemporalIndexNotFound      = storepkg.ErrTemporalIndexNotFound
	ErrVectorIndexExists          = storepkg.ErrVectorIndexExists
	ErrVectorIndexNotFound        = storepkg.ErrVectorIndexNotFound
	ErrDimensionMismatch          = storepkg.ErrDimensionMismatch
	ErrInvalidTemporalIndexConfig = storepkg.ErrInvalidTemporalIndexConfig
	ErrInvalidVectorIndexConfig   = storepkg.ErrInvalidVectorIndexConfig
	ErrInvalidVectorValue         = storepkg.ErrInvalidVectorValue
	ErrInvalidShardDepth          = storepkg.ErrInvalidShardDepth
	ErrInvalidQueryLimit          = storepkg.ErrInvalidQueryLimit
	ErrInvalidQueryCursor         = storepkg.ErrInvalidQueryCursor
	ErrIndexProviderExists        = indexpkg.ErrIndexProviderExists
	ErrIndexProviderNotFound      = indexpkg.ErrIndexProviderNotFound
	ErrIndexProviderEmptyName     = indexpkg.ErrIndexProviderEmptyName
)

// Registry sentinel errors. Surface through node/rel mutations.
var (
	ErrEmptyName        = registrypkg.ErrEmptyName
	ErrRegistryNotEmpty = registrypkg.ErrRegistryNotEmpty
)

// Sentinel errors re-exported from pkg/graph/internal/core for the public API.
var (
	ErrNoLabels         = core.ErrNoLabels
	ErrNilNode          = core.ErrNilNode
	ErrNilRelationship  = core.ErrNilRelationship
	ErrZeroID           = core.ErrZeroID
	ErrInvalidID        = core.ErrInvalidID
	ErrVersionOverflow  = core.ErrVersionOverflow
	ErrNotTieredStore   = core.ErrNotTieredStore
	ErrAlreadyClosed    = core.ErrAlreadyClosed
	ErrGraphClosed      = core.ErrGraphClosed
	ErrNilGraph         = core.ErrNilGraph
	ErrNilStore         = core.ErrNilStore
	ErrNilContext       = core.ErrNilContext
	ErrNilTxCallback    = core.ErrNilTxCallback
	ErrInvalidTimeRange = core.ErrInvalidTimeRange
	ErrLabelNotFound    = core.ErrLabelNotFound
	ErrLastLabel        = core.ErrLastLabel
	ErrBatchFailed      = core.ErrBatchFailed
	ErrBatchDone        = core.ErrBatchDone
	ErrTooManyLabels            = core.ErrTooManyLabels
	ErrTooManyProperties        = core.ErrTooManyProperties
	ErrKeyTooLong               = core.ErrKeyTooLong
	ErrValueTooLarge            = core.ErrValueTooLarge
	ErrNameTooLong              = core.ErrNameTooLong
	ErrSelfLoop                 = core.ErrSelfLoop
	ErrValidFromBeforePrevious  = core.ErrValidFromBeforePrevious
)

// IO sentinels (R4-F8). Re-exported so external callers can write
// `errors.Is(err, ErrImportSizeLimit)` without dipping into internal/core.
// Mirrored on pkg/graph/io as well — pick whichever import the caller
// already has.
var (
	ErrNilReader            = core.ErrNilReader
	ErrNilWriter            = core.ErrNilWriter
	ErrIncompatibleExport   = core.ErrIncompatibleExport
	ErrIncompatibleRegistry = core.ErrIncompatibleRegistry
	ErrCorruptExport        = core.ErrCorruptExport
	ErrImportSizeLimit      = core.ErrImportSizeLimit
)

// ErrNoVersionAsOf is the bitemporal sentinel returned by g.Temporal().NodeAsOf
// / RelAsOf when no version was committed at or before the supplied
// transaction time.
var ErrNoVersionAsOf = core.ErrNoVersionAsOf

// ErrTxDone is the transaction-completion sentinel returned by g.Tx().Run /
// RunContext / the imperative *GraphTx methods when the transaction has
// already committed or rolled back. Aliases store.ErrTxDone so external
// callers can use either qualifier.
var ErrTxDone = storepkg.ErrTxDone
