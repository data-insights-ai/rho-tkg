package temporal

import (
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// GraphSnapshot represents the complete graph state at a point in time.
// Returned by Graph.Snapshot.
type GraphSnapshot struct {
	Timestamp     types.Instant
	Nodes         []*types.Node
	Relationships []*types.Relationship
	NodeCount     int
	RelCount      int
}
