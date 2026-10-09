// Package snowflake provides the package-level Snowflake epoch + layout used by
// every TKG subpackage that needs to decompose a snowflake.ID without coupling
// on pkg/graph itself. Hosts ID-decomposition helpers (DecomposeID,
// IDComponents) and the canonical Layout configuration.
package snowflake

import (
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/internal/idlayout"
)

// Epoch is the custom epoch for all snowflake ID generation
// (2026-01-01 UTC). Its single definition lives in pkg/internal/idlayout so
// pkg/types (the public NodeID/RelID.MintInstant doors) and every package in
// pkg/graph share one epoch without an import cycle on pkg/graph itself.
var Epoch = idlayout.Epoch

// Layout is the package-level snowflake.Layout matching the graph's
// snowflake generators, shared with pkg/types through pkg/internal/idlayout.
// Used by standalone functions (shardIndex, DecomposeID) that don't have
// access to a *Node.
var Layout = idlayout.Layout

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
