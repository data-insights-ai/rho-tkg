// Package snowflake provides the package-level Snowflake epoch + layout used by
// every TKG subpackage that needs to decompose a snowflake.ID without coupling
// on pkg/graph itself. Hosts ID-decomposition helpers (DecomposeID,
// IDComponents) and the canonical Layout configuration.
package snowflake

import (
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// Epoch is the custom epoch for all snowflake ID generation
// (2026-01-01 UTC). Lives in this package so the Layout that depends on
// it can be shared by every package in pkg/graph (graph layer, locks,
// indexes, backends) without an import cycle on pkg/graph itself.
var Epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// Layout is the package-level snowflake.Layout matching the graph's
// snowflake generators. Used by standalone functions
// (entityValidFrom, shardIndex, DecomposeID) that don't have access to a
// *Node. Lives in pkg/graph/internal/snowflake so all subpackages can share
// a single layout without coupling on pkg/graph.
var Layout = func() snowflake.Layout {
	l, err := snowflake.NewLayout(
		snowflake.WithEpoch(Epoch),
		snowflake.WithMicroseconds(),
		snowflake.WithNodeBits(5),
		snowflake.WithStepBits(10),
	)
	if err != nil {
		panic("graph: SnowflakeLayout: " + err.Error())
	}
	return l
}()

// IDComponents holds the decomposed fields of a snowflake ID.
type IDComponents struct {
	CreatedAt time.Time // creation time (ms or us precision depending on layout)
	NodeID    int64     // snowflake generator node (0-31 in us mode)
	Sequence  int64     // step counter within time tick (0-1023 in us mode)
}

// DecomposeID extracts the creation time, node ID, and sequence number from a
// snowflake ID using the package-level Layout.
func DecomposeID(id snowflake.ID) IDComponents {
	parts := Layout.Decompose(id)
	return IDComponents{
		CreatedAt: Layout.CreatedAt(id),
		NodeID:    parts.Node,
		Sequence:  parts.Step,
	}
}
