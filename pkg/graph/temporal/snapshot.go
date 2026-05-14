package temporal

import (
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
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
