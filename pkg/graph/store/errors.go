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
	// ErrRelPropertyIndexUnsupported is returned by a backend that recognizes
	// the relationship property-index capability but cannot serve it — the
	// tiered store declines it (v1): relationships live on event shards by
	// timestamp routing, so a rel-value equality index would be shard-local
	// while the values it must find are spread across every shard. Distinct
	// from ErrCapabilityNotSupported (which means the backend does not
	// implement the capability at all). Query still works everywhere via the
	// graph-layer type-scan fallback; only index creation declines.
	ErrRelPropertyIndexUnsupported = errors.New("graph: relationship property indexes are not supported on this backend")
	ErrTemporalIndexExists         = errors.New("graph: temporal index already exists")
	ErrTemporalIndexNotFound       = errors.New("graph: temporal index not found")
	ErrInvalidTemporalIndexConfig  = errors.New("graph: invalid temporal index configuration")
	ErrVectorIndexExists           = errors.New("graph: vector index already exists")
	ErrVectorIndexNotFound         = errors.New("graph: vector index not found")
	ErrDimensionMismatch           = errors.New("graph: vector dimension mismatch")
	ErrInvalidVectorIndexConfig    = errors.New("graph: invalid vector index configuration")
	ErrInvalidVectorValue          = errors.New("graph: invalid vector value")
	ErrInvalidShardDepth           = errors.New("graph: invalid shard depth")
	ErrInvalidTimeRange            = errors.New("graph: invalid time range")
	ErrInvalidQueryLimit           = errors.New("graph: invalid query limit")
	ErrInvalidQueryCursor          = errors.New("graph: invalid query cursor")
	ErrInvalidStoreMutation        = errors.New("graph: invalid store mutation")
	ErrNilStore                    = errors.New("graph: store must not be nil")
	ErrTxDone                      = errors.New("graph: transaction already committed or rolled back")
	ErrStoreClosed                 = errors.New("graph: store already closed")

	// ErrCapabilityNotSupported is returned by graph operations that depend
	// on an optional Store capability when the configured backend does not
	// implement it. Callers MUST use errors.Is(err, ErrCapabilityNotSupported)
	// — the wrapping message identifies the missing capability for
	// diagnostics, but the sentinel is the contract.
	ErrCapabilityNotSupported = errors.New("graph: store does not support this capability")

	// ErrOrderedScanTemporal is a LEGACY sentinel retained for backward
	// compatibility. The ordered / prefix scan doors used to DECLINE temporal
	// QueryOpts with this error (the value ordering came from the valid-time-
	// agnostic property index). As of Stage B those doors SERVE temporal opts via
	// a sound full fold (values resolved at the pin, then sorted), so this error is
	// no longer returned. It is kept exported so a consumer that still matches it
	// with errors.Is keeps compiling.
	ErrOrderedScanTemporal = errors.New("graph: ordered range scan is current-state only; temporal QueryOpts are not supported")

	// ErrPrimaryRegistryStale is returned by the replica apply path when a
	// refetched primary RegistrySnapshot has CapturedAtLSN < the record being
	// applied — the primary's registry has not yet caught up to the record. It is
	// RETRYABLE: a replication driver should pause and retry after the primary
	// commits. Distinct from ErrRegistryDiverged (which is fatal).
	ErrPrimaryRegistryStale = errors.New("graph: primary registry snapshot is stale for this record")

	// ErrRegistryDiverged is returned by the replica apply path when a refetched
	// primary registry does NOT append-only-extend the replica's (the replica's
	// names are not a prefix of the primary's). This is FATAL — the two
	// registries have irreconcilably diverged; the replica must be re-bootstrapped.
	// Not retryable.
	ErrRegistryDiverged = errors.New("graph: replica registry diverged from primary")

	// ErrChangesNotAscending is returned by ApplyChanges when the supplied batch
	// is not in strictly ascending LSN order. Ascending order is a caller
	// contract; the batch path enforces it so a record below the running maximum
	// is not silently swallowed by the watermark skip (which would leave a
	// permanent gap in the replica's coverage). The successful prefix before the
	// out-of-order record is still flushed and watermarked.
	ErrChangesNotAscending = errors.New("graph: change records not in ascending LSN order")

	// ErrWireFormatVersionUnsupported is returned when persisted data declares
	// an on-disk format version newer than this binary supports — either a
	// store-level format marker written by a newer release, or an individual
	// entity row carrying a newer per-row format version. The store fails
	// closed instead of misdecoding fields it does not know about. Upgrade the
	// binary; never strip the marker by hand.
	ErrWireFormatVersionUnsupported = errors.New("graph: on-disk wire format version not supported by this binary")

	// ErrCorruptWire is returned when msgpack decoding of a persisted or
	// imported row fails in a way that indicates corrupt or hostile bytes —
	// in particular when the underlying msgpack decoder PANICS rather than
	// returning an error. The vmihailenco/msgpack decoder can panic via reflect
	// on certain malformed inputs (e.g. a duplicate map key for an
	// interface-typed struct field such as PropertyWire.Value), so the store
	// trust boundary recovers and converts that panic into this sentinel: a
	// single corrupt or adversarial row must never crash the process. Wraps the
	// recovered panic value for diagnostics; callers match with errors.Is.
	ErrCorruptWire = errors.New("graph: corrupt wire data")
)
