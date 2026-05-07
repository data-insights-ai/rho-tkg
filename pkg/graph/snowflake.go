package graph

import (
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	snowflakepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/snowflake"
)

// IDComponents holds the decomposed fields of a snowflake ID. The canonical
// declaration lives in `pkg/graph/internal/snowflake`; this alias is the
// public API surface.
type IDComponents = snowflakepkg.IDComponents

// DecomposeID extracts the creation time, node ID, and sequence number
// from a snowflake ID.
func DecomposeID(id snowflake.ID) IDComponents {
	return snowflakepkg.DecomposeID(id)
}

// snowflakeEpoch and snowflakeLayout are package-level helpers used by the
// rest of the pkg/graph code to instantiate generators and decompose IDs.
// Both forward to the canonical definition in internal/snowflake.
var (
	snowflakeEpoch  time.Time        = snowflakepkg.Epoch
	snowflakeLayout snowflake.Layout = snowflakepkg.Layout
)
