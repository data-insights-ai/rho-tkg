package graph

import (
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

// Exported sentinels. Match with errors.Is; returned errors may wrap them with
// detail that never echoes paths or property values.
var (
	// ErrNotImplemented is returned by every door while no engine is attached.
	// Temporary: it is removed (a breaking change) when the engine lands, unless
	// it is kept for a capability the engine still lacks.
	ErrNotImplemented = errors.New("graph: not implemented: no engine attached")
	// ErrInvalidConfig reports a Config that Open refuses.
	ErrInvalidConfig = errors.New("graph: invalid config")
	// ErrInvalidScope reports a zero, empty (on write) or wrong-axis Scope.
	ErrInvalidScope = errors.New("graph: invalid scope")
	// ErrInvalidID reports a zero or negative node/relationship ID. It is the
	// types sentinel, so errors.Is matches both.
	ErrInvalidID = types.ErrInvalidID
	// ErrInvalidBound reports a zero Bound or an infinity on the wrong side.
	ErrInvalidBound = errors.New("graph: invalid bound")
	// ErrInvalidOp reports a malformed write op (empty label, key or type, no payload).
	ErrInvalidOp = errors.New("graph: invalid operation")
	// ErrInvalidSelector reports a selector without a view, scope or valid filter.
	ErrInvalidSelector = errors.New("graph: invalid selector")
	// ErrInvalidSnapshotSpec reports a zero SnapshotSpec or a zero Cut/Receipt.
	ErrInvalidSnapshotSpec = errors.New("graph: invalid snapshot spec")
	// ErrInvalidValue reports a zero, non-finite or otherwise malformed Value.
	ErrInvalidValue = errors.New("graph: invalid value")
	// ErrInvalidCut reports a zero, empty or oversized Cut encoding.
	ErrInvalidCut = errors.New("graph: invalid cut")
	// ErrInvalidReceipt reports a zero, empty or oversized Receipt encoding.
	ErrInvalidReceipt = errors.New("graph: invalid receipt")
	// ErrInvalidCursor reports a zero, oversized or foreign Cursor.
	ErrInvalidCursor = errors.New("graph: invalid cursor")
	// ErrInvalidRequest reports a malformed request (limits, nil arguments), and
	// an Update larger than one commit can hold (see Tx).
	ErrInvalidRequest = errors.New("graph: invalid request")
	// ErrClosed reports use of a nil or closed Graph or Snapshot.
	ErrClosed = errors.New("graph: closed")
	// ErrTxDone reports use of a Tx after its Update function returned.
	ErrTxDone = errors.New("graph: transaction finished")
	// ErrNotFound reports an ID that does not exist in the named view.
	ErrNotFound = errors.New("graph: not found")
	// ErrConflict reports a serialization conflict; the Update may be retried.
	ErrConflict = errors.New("graph: conflict")
	// ErrUniqueViolation reports a unique-claim violation (gap rows 6-7).
	ErrUniqueViolation = errors.New("graph: unique violation")
	// ErrCommitNotDurable reports a commit applied but not yet durable (gap row 20).
	ErrCommitNotDurable = errors.New("graph: commit not durable")
	// ErrSnapshotExpired reports a snapshot or cursor whose cut was released.
	ErrSnapshotExpired = errors.New("graph: snapshot expired")
)
