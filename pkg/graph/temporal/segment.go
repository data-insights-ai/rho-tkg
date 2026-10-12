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
// same row when another row lies between them. A segment names the stored
// record that answers it: distinct rows with equal property content (a burst
// grown by SetNodeVersionInterval with identical props) are never merged;
// merge on content in the consumer if one interval is wanted.
type NodeSegment struct {
	ValidFrom, ValidTo types.Instant
	Node               *types.Node
}

// RelSegment is NodeSegment for relationships (API.RelEffectiveTimeline).
type RelSegment struct {
	ValidFrom, ValidTo types.Instant
	Rel                *types.Relationship
}

// Direction names the endpoint of a relationship a seed node of
// API.ForEachRelEffectiveAtNodes matches: Outgoing the start node, Incoming
// the end node. The zero value is invalid (ErrInvalidDirection).
type Direction uint8

const (
	// Outgoing matches a relationship whose start node is a seed.
	Outgoing Direction = iota + 1
	// Incoming matches a relationship whose end node is a seed.
	Incoming
)
