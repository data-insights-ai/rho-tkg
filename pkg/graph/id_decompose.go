package graph

import (
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// IDComponents holds the decomposed fields of a snowflake ID.
type IDComponents struct {
	CreatedAt time.Time // millisecond-precision creation time
	NodeID    int64     // snowflake generator node (0-1023)
	Sequence  int64     // step counter within ms (0-4095)
}

// DecomposeID extracts the creation time, node ID, and sequence number from a
// snowflake ID. Uses the same epoch as the graph's snowflake generators.
func DecomposeID(id snowflake.ID) IDComponents {
	raw := uint64(id)
	timeMs := int64(raw >> 22)
	nodeID := int64((raw >> 12) & 0x3FF)
	seq := int64(raw & 0xFFF)

	return IDComponents{
		CreatedAt: time.UnixMilli(snowflakeEpoch.UnixMilli() + timeMs),
		NodeID:    nodeID,
		Sequence:  seq,
	}
}
