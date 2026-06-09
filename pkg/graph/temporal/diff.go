package temporal

import (
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// NodeUpdate pairs the before and after states of an updated node.
type NodeUpdate struct {
	Before *types.Node
	After  *types.Node
}

// RelUpdate pairs the before and after states of an updated relationship.
type RelUpdate struct {
	Before *types.Relationship
	After  *types.Relationship
}

// SnapshotDiff describes entity changes between two points in time.
// Entities valid at T2 but not T1 → Created.
// Entities valid at both with a different integrity hash → Updated.
// Entities valid at T1 but not T2 → Deleted.
// Returned by Graph.DiffSnapshots.
type SnapshotDiff struct {
	T1, T2       types.Instant
	NodesCreated []*types.Node
	NodesUpdated []NodeUpdate
	NodesDeleted []*types.Node
	RelsCreated  []*types.Relationship
	RelsUpdated  []RelUpdate
	RelsDeleted  []*types.Relationship
}

// DiffHandlers is the streaming callback set delivered by
// Graph.Temporal().DiffCallback (forwards to core.DiffSnapshotsCallback). For
// each entity whose state differs between t1 and t2, the matching handler is
// invoked with the relevant before/after pointer(s). Any handler may be nil;
// nil handlers are skipped without invoking any work for that change class.
//
// Returning a non-nil error from any handler aborts iteration; the error is
// returned verbatim. Handlers MUST treat the supplied pointers as
// caller-owned snapshots — implementations may reuse buffers between
// invocations on some Store backends, so retain a deep copy if the value is
// kept beyond the handler's return.
//
// Order of delivery is implementation-defined; do not rely on it.
type DiffHandlers struct {
	OnNodeCreated func(after *types.Node) error
	OnNodeUpdated func(before, after *types.Node) error
	OnNodeDeleted func(before *types.Node) error
	OnRelCreated  func(after *types.Relationship) error
	OnRelUpdated  func(before, after *types.Relationship) error
	OnRelDeleted  func(before *types.Relationship) error
}
