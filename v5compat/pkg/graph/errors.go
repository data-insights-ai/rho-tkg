package graph

import (
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/types"
)

// Sentinels by their v4 names; consumers match them with errors.Is.
var (
	ErrNodeNotFound            = errors.New("graph: node not found")
	ErrRelNotFound             = errors.New("graph: relationship not found")
	ErrNoVersionAsOf           = errors.New("graph: no version recorded at the transaction pin")
	ErrNoVersionValidAt        = errors.New("graph: no version valid at the instant")
	ErrInvalidTimeRange        = errors.New("graph: invalid time range")
	ErrEntityDeleted           = errors.New("graph: entity is deleted")
	ErrAlreadyClosed           = errors.New("graph: version already closed")
	ErrGraphClosed             = errors.New("graph: graph closed")
	ErrNilGraph                = errors.New("graph: nil graph")
	ErrNilContext              = errors.New("graph: nil context")
	ErrNilTxCallback           = errors.New("graph: nil transaction callback")
	ErrTxBackfillDisabled      = errors.New("graph: caller transaction instants need Config.AllowTxBackfill")
	ErrInvalidTxFrom           = errors.New("graph: invalid caller transaction instant")
	ErrTxOrder                 = errors.New("graph: caller transaction instant out of order")
	ErrConflictingTemporalOpts = errors.New("graph: conflicting temporal query options")
	ErrTxPinTooNew             = errors.New("graph: transaction pin above the commit clock")
	ErrNoLabels                = errors.New("graph: a node needs at least one label")
	ErrZeroID                  = errors.New("graph: zero ID")
	ErrInvalidID               = errors.New("graph: invalid ID")
	ErrInvalidQueryLimit       = errors.New("graph: invalid query limit")
	ErrInvalidQueryCursor      = errors.New("graph: invalid query cursor")
	ErrEmptyName               = errors.New("graph: empty name")
	ErrUniqueUnsupportedType   = errors.New("graph: unique key value type not supported")
	ErrSelfLoop                = errors.New("graph: self-loop relationship")
	ErrDeclinedConfig          = errors.New("graph: config knob declined")

	// ErrNotSupportedInV5 is the class of every door v5compat declines (the X
	// list of WP-11): store backends, Config knobs without a v5 meaning,
	// DecomposeNodeID/IDSlotLease, the async LSN follower, UntimedFromTxFrom,
	// runtime caller IDs, the v4 export format, tier admin, the vector index and
	// MintInstant. The wrapped error names the door.
	ErrNotSupportedInV5 = store.ErrNotSupportedInV5
)

// Re-exports so errors.Is(err, graph.ErrX) holds for the types/store sentinels.
var (
	ErrUnsupportedValueType = types.ErrUnsupportedValueType
	ErrEmptyPropertyKey     = types.ErrEmptyPropertyKey
	ErrInvalidStoreMutation = store.ErrInvalidStoreMutation
)
