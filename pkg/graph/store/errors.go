package store

import "errors"

// Sentinel errors for store operations.
var (
	ErrNodeNotFound               = errors.New("graph: node not found")
	ErrRelNotFound                = errors.New("graph: relationship not found")
	ErrNodeExists                 = errors.New("graph: node already exists")
	ErrRelExists                  = errors.New("graph: relationship already exists")
	ErrVersionNotFound            = errors.New("graph: version not found")
	ErrNoVersionValidAt           = errors.New("graph: no version valid at the given time")
	ErrIndexExists                = errors.New("graph: property index already exists")
	ErrIndexNotFound              = errors.New("graph: property index not found")
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

	// ErrCapabilityNotSupported is returned by graph operations that depend
	// on an optional Store capability when the configured backend does not
	// implement it. Callers MUST use errors.Is(err, ErrCapabilityNotSupported)
	// — the wrapping message identifies the missing capability for
	// diagnostics, but the sentinel is the contract.
	ErrCapabilityNotSupported = errors.New("graph: store does not support this capability")

	// ErrWireFormatVersionUnsupported is returned when persisted data declares
	// an on-disk format version newer than this binary supports — either a
	// store-level format marker written by a newer release, or an individual
	// entity row carrying a newer per-row format version. The store fails
	// closed instead of misdecoding fields it does not know about. Upgrade the
	// binary; never strip the marker by hand.
	ErrWireFormatVersionUnsupported = errors.New("graph: on-disk wire format version not supported by this binary")
)
