package temporal

import "github.com/data-insights-ai/rho-tkg/v4/pkg/types"

// NodeSegment is one piece of a node's effective timeline
// (API.NodeEffectiveTimeline): over the half-open valid interval
// [ValidFrom, ValidTo) the node's state, as recorded at the pin, is Node —
// the row NodeAtTx(id, t, pin) returns for every t in it. ValidTo == 0 is
// open-ended. ValidFrom is never 0: a row without a recorded valid-from starts
// at its derived start (the ID's mint instant for the first row).
//
// Node is a shared frozen row like the rows of the other plural reads: it
// rejects mutation; call DeepCopy to get a mutable copy. Adjacent segments of
// one entity hold different rows; two segments of one entity may alias the
// same row when another row lies between them.
type NodeSegment struct {
	ValidFrom, ValidTo types.Instant
	Node               *types.Node
}

// RelSegment is NodeSegment for relationships (API.RelEffectiveTimeline).
type RelSegment struct {
	ValidFrom, ValidTo types.Instant
	Rel                *types.Relationship
}
