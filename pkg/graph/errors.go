// This file is the CANONICAL consumer surface for sentinel errors: every
// sentinel a public Graph operation can return is re-exported here, so
// `errors.Is(err, graph.ErrX)` always works with a single import. Each
// variable below is a pure alias of the one canonical declaration (store,
// internal/core, io, index, or registry — see the import each block aliases
// from); sub-packages that re-export the same sentinels alias those same
// values. NEVER redeclare a sentinel with errors.New on a second surface —
// the message would match while errors.Is silently stops working.
// TestSentinelAliasesShareIdentity enforces this.
package graph

import (
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/index"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/core"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	iopkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// ErrCapabilityNotSupported is the capability-missing sentinel — returned
// (wrapped with the missing-capability name) when a graph operation needs
// an optional Store capability that the configured backend does not
// implement. Callers MUST check via errors.Is(err, ErrCapabilityNotSupported);
// the wrapping message is diagnostic only.
var ErrCapabilityNotSupported = storepkg.ErrCapabilityNotSupported

// ErrPrimaryRegistryStale (RETRYABLE) and ErrRegistryDiverged (FATAL) classify
// a replica token-refetch failure during ApplyChange/ApplyChanges: stale means
// the primary's registry snapshot has not yet caught up to the record (pause and
// retry); diverged means the replica's registry is not a prefix of the primary's
// (re-bootstrap required). Check with errors.Is to branch retry-vs-abort.
var (
	ErrPrimaryRegistryStale = storepkg.ErrPrimaryRegistryStale
	ErrRegistryDiverged     = storepkg.ErrRegistryDiverged
)

// ErrWireFormatVersionUnsupported is the on-disk format sentinel — returned
// when a store directory (or an individual persisted row) declares a wire
// format version newer than this binary supports. The store fails closed at
// open instead of misdecoding data written by a newer release. Aliases
// store.ErrWireFormatVersionUnsupported.
var ErrWireFormatVersionUnsupported = storepkg.ErrWireFormatVersionUnsupported

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
	ErrNoLabels                = core.ErrNoLabels
	ErrNilNode                 = core.ErrNilNode
	ErrNilRelationship         = core.ErrNilRelationship
	ErrZeroID                  = core.ErrZeroID
	ErrInvalidID               = core.ErrInvalidID
	ErrVersionOverflow         = core.ErrVersionOverflow
	ErrNotTieredStore          = core.ErrNotTieredStore
	ErrAlreadyClosed           = core.ErrAlreadyClosed
	ErrGraphClosed             = core.ErrGraphClosed
	ErrReadOnlyReplica         = core.ErrReadOnlyReplica
	ErrNilGraph                = core.ErrNilGraph
	ErrNilStore                = core.ErrNilStore
	ErrNilContext              = core.ErrNilContext
	ErrNilTxCallback           = core.ErrNilTxCallback
	ErrInvalidTimeRange        = core.ErrInvalidTimeRange
	ErrLabelNotFound           = core.ErrLabelNotFound
	ErrLastLabel               = core.ErrLastLabel
	ErrBatchFailed             = core.ErrBatchFailed
	ErrBatchDone               = core.ErrBatchDone
	ErrTooManyLabels           = core.ErrTooManyLabels
	ErrTooManyProperties       = core.ErrTooManyProperties
	ErrKeyTooLong              = core.ErrKeyTooLong
	ErrValueTooLarge           = core.ErrValueTooLarge
	ErrNameTooLong             = core.ErrNameTooLong
	ErrSelfLoop                = core.ErrSelfLoop
	ErrValidFromBeforePrevious = core.ErrValidFromBeforePrevious
	// Generic-door belief-state pin conflict (QueryOpts.TxPin).
	ErrConflictingTemporalOpts = core.ErrConflictingTemporalOpts
	// §4.1 transaction-time backfill.
	ErrTxBackfillDisabled = core.ErrTxBackfillDisabled
	ErrInvalidTxFrom      = core.ErrInvalidTxFrom
	// §4.2 named as-of (Erkenntniszeit) tags.
	ErrInvalidAsOfTag  = core.ErrInvalidAsOfTag
	ErrTooManyAsOfTags = core.ErrTooManyAsOfTags
	// Unique property constraints (ADR-0002).
	ErrUniqueViolation             = core.ErrUniqueViolation
	ErrUniqueViolationExisting     = core.ErrUniqueViolationExisting
	ErrUniqueConstraintExists      = core.ErrUniqueConstraintExists
	ErrUniqueConstraintNotFound    = core.ErrUniqueConstraintNotFound
	ErrUniqueUnsupportedType       = core.ErrUniqueUnsupportedType
	ErrUniqueEventLabelUnsupported = core.ErrUniqueEventLabelUnsupported
	// History retention & compaction (ADR-0001).
	ErrHistoryCompacted           = core.ErrHistoryCompacted
	ErrCompactionProtectedTag     = core.ErrCompactionProtectedTag
	ErrInvalidRetentionPolicy     = core.ErrInvalidRetentionPolicy
	ErrCompactionChangeLogEnabled = core.ErrCompactionChangeLogEnabled
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

// Delta export/merge sentinels (ExportSince / ImportMerge). Re-exported so
// callers can `errors.Is(err, ErrCursorUnknown)` from pkg/graph directly.
var (
	ErrCursorUnknown     = core.ErrCursorUnknown
	ErrDeltaBaseMismatch = core.ErrDeltaBaseMismatch
)

// ErrBackupExists is returned by g.IO().BackupTo / BackupDeltaTo when the
// deterministic target filename already exists in the backup directory — the
// one-call backup ergonomics layer over Export/ExportSince never silently
// overwrites a prior backup. Aliases io.ErrBackupExists; there is no
// internal/core declaration to mirror because BackupTo/BackupDeltaTo are pure
// filesystem orchestration over the existing Export/ExportSince doors.
var ErrBackupExists = iopkg.ErrBackupExists

// ErrNoVersionAsOf is the bitemporal sentinel returned by g.Temporal().NodeAsOf
// / RelAsOf when no version was committed at or before the supplied
// transaction time.
var ErrNoVersionAsOf = core.ErrNoVersionAsOf

// ErrTxDone is the transaction-completion sentinel returned by g.Tx().Run /
// RunContext / the imperative *GraphTx methods when the transaction has
// already committed or rolled back. Aliases store.ErrTxDone so external
// callers can use either qualifier.
var ErrTxDone = storepkg.ErrTxDone
