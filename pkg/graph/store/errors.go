package store

import "errors"

// Sentinel errors for store operations.
var (
	ErrNodeNotFound     = errors.New("graph: node not found")
	ErrRelNotFound      = errors.New("graph: relationship not found")
	ErrNodeExists       = errors.New("graph: node already exists")
	ErrRelExists        = errors.New("graph: relationship already exists")
	ErrVersionNotFound  = errors.New("graph: version not found")
	ErrNoVersionValidAt = errors.New("graph: no version valid at the given time")
	ErrIndexExists      = errors.New("graph: property index already exists")
	ErrIndexNotFound    = errors.New("graph: property index not found")
	// ErrRelPropertyIndexUnsupported is returned by a backend that recognizes the
	// relationship property-index capability but cannot serve it in its current
	// configuration (it then reports RelPropertyIndexSupported() == false; no
	// in-tree backend declines since v4.58.0). Distinct from
	// ErrCapabilityNotSupported; queries still work via the type-scan fallback,
	// only index creation declines.
	ErrRelPropertyIndexUnsupported = errors.New("graph: relationship property indexes are not supported on this backend")
	// ErrElementsIndexUnsupported is returned for an operation that cannot use an
	// Elements property index (backlog 63): Elements with RangeCounts, a unique
	// constraint or GetOrCreateByKey on the key, ByLabelAndProperties over it.
	ErrElementsIndexUnsupported   = errors.New("graph: not supported with an elements property index")
	ErrTemporalIndexExists        = errors.New("graph: temporal index already exists")
	ErrTemporalIndexNotFound      = errors.New("graph: temporal index not found")
	ErrInvalidTemporalIndexConfig = errors.New("graph: invalid temporal index configuration")
	ErrVectorIndexExists          = errors.New("graph: vector index already exists")
	ErrVectorIndexNotFound        = errors.New("graph: vector index not found")
	ErrDimensionMismatch          = errors.New("graph: vector dimension mismatch")
	ErrInvalidVectorIndexConfig   = errors.New("graph: invalid vector index configuration")
	ErrInvalidVectorValue         = errors.New("graph: invalid vector value")
	ErrInvalidShardDepth          = errors.New("graph: invalid shard depth")
	ErrInvalidTimeRange           = errors.New("graph: invalid time range")
	ErrInvalidQueryLimit          = errors.New("graph: invalid query limit")
	ErrInvalidQueryCursor         = errors.New("graph: invalid query cursor")
	ErrInvalidStoreMutation       = errors.New("graph: invalid store mutation")
	ErrNilStore                   = errors.New("graph: store must not be nil")
	ErrTxDone                     = errors.New("graph: transaction already committed or rolled back")
	ErrStoreClosed                = errors.New("graph: store already closed")
	// ErrExactErasureRelationshipEscape is returned before any mutation when a
	// relationship version touching a declared node is outside the declared
	// relationship set; exact erasure never widens scope.
	ErrExactErasureRelationshipEscape = errors.New("graph: exact erasure scope excludes a relationship touching a declared node")
	// ErrExactErasureClosureLimit is returned before mutation when the closure
	// exhausts a caller-declared identity or version bound.
	ErrExactErasureClosureLimit = errors.New("graph: exact erasure relationship closure exceeds its bound")
	// ErrExactErasureChangeLogRetained is returned before any mutation when the
	// backend holds any change-log material (records carry full payloads).
	ErrExactErasureChangeLogRetained = errors.New("graph: exact erasure is unavailable while change-log material is retained")

	// ErrCapabilityNotSupported is returned when an operation depends on an optional
	// Store capability the backend lacks. Match with errors.Is; the wrapping message
	// names the capability.
	ErrCapabilityNotSupported = errors.New("graph: store does not support this capability")

	// ErrSlotNotLocal is returned by a partitioned store when an entity's snowflake
	// slot is not owned by this store (ADR-0007/0010). sharded re-exports it as
	// sharded.ErrSlotNotLocal (same value); declared here so the graph layer can
	// errors.Is against it (e.g. a Model-A stub during tx rollback).
	ErrSlotNotLocal = errors.New("graph: entity slot not local to this partitioned store")

	// ErrOrderedScanTemporal is a legacy sentinel, no longer returned (ordered and
	// prefix scans serve temporal opts by a full fold); kept for errors.Is callers.
	ErrOrderedScanTemporal = errors.New("graph: ordered range scan is current-state only; temporal QueryOpts are not supported")

	// ErrPrimaryRegistryStale is returned by the replica apply path when the primary's
	// refetched RegistrySnapshot has CapturedAtLSN below the record. Retryable, unlike
	// ErrRegistryDiverged.
	ErrPrimaryRegistryStale = errors.New("graph: primary registry snapshot is stale for this record")

	// ErrRegistryDiverged is returned by the replica apply path when the primary
	// registry does not append-only-extend the replica's. Fatal, not retryable:
	// re-bootstrap the replica.
	ErrRegistryDiverged = errors.New("graph: replica registry diverged from primary")

	// ErrChangesNotAscending is returned by ApplyChanges when the batch is not in
	// strictly ascending LSN order (a lower record would be swallowed by the
	// watermark skip, leaving a gap). The successful prefix is still flushed.
	ErrChangesNotAscending = errors.New("graph: change records not in ascending LSN order")

	// ErrWireFormatVersionUnsupported is returned when persisted data declares a
	// format version (store marker or per-row) newer than this binary supports. The
	// store fails closed; upgrade the binary, never strip the marker.
	ErrWireFormatVersionUnsupported = errors.New("graph: on-disk wire format version not supported by this binary")

	// ErrHistoryAnchorIntervalMismatch is returned at open when the configured
	// HistoryAnchorInterval differs from the one the existing delta history was
	// written at (persisted marker; it is baked into the on-disk layout). Reopen at
	// the original interval or rewrite history.
	ErrHistoryAnchorIntervalMismatch = errors.New("graph: configured HistoryAnchorInterval differs from the store's persisted delta-history interval")

	// ErrCorruptWire is returned when msgpack decoding of a persisted or imported row
	// fails on corrupt or hostile bytes, including a decoder panic (e.g. a duplicate
	// map key for an interface-typed field), which the store boundary recovers so one
	// bad row cannot crash the process. Wraps the panic value; match with errors.Is.
	ErrCorruptWire = errors.New("graph: corrupt wire data")
)
