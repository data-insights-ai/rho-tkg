package graph

import (
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// IDComponents holds the decomposed fields of a snowflake ID.
type IDComponents struct {
	CreatedAt time.Time // creation time (ms or us precision depending on layout)
	NodeID    int64     // snowflake generator node (0-31 in us mode)
	Sequence  int64     // step counter within time tick (0-1023 in us mode)
}

// DecomposeID extracts the creation time, node ID, and sequence number from a
// snowflake ID using the package-level snowflakeLayout.
func DecomposeID(id snowflake.ID) IDComponents {
	parts := snowflakeLayout.Decompose(id)
	return IDComponents{
		CreatedAt: snowflakeLayout.CreatedAt(id),
		NodeID:    parts.Node,
		Sequence:  parts.Step,
	}
}
