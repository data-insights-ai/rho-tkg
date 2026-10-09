package types

import "github.com/data-insights-ai/rho-tkg/v4/pkg/internal/idlayout"

// MintInstant returns the instant this node ID was minted, in Unix
// milliseconds: the derived valid-from of a node row that carries no explicit
// ValidFrom (TemporalMetadata.ValidFrom == 0 means unset, and the derived start
// is the ID's mint instant). It is exactly the start the graph's resolver
// applies on every temporal read, so a consumer that reads a row without its
// derived start (an unpinned read, a column scan) can apply the same rule
// without decoding snowflake bits itself.
//
// The result depends only on the ID and the package-level epoch and layout
// (48-bit microseconds since 2026-01-01 UTC, floored to milliseconds), which
// every graph in a process shares; it does not consult any graph's Config. It
// is monotone in ID order. A zero or negative ID, which no generator mints,
// returns 0 (unset). It never panics and does not allocate.
func (id NodeID) MintInstant() Instant { return Instant(idlayout.MintInstantMillis(id.SnowflakeID())) }

// MintInstant is the relationship counterpart of NodeID.MintInstant. Relationships
// mint with the odd node field where nodes use the even one; the time bits, and
// therefore the instant, are derived identically.
func (id RelID) MintInstant() Instant { return Instant(idlayout.MintInstantMillis(id.SnowflakeID())) }
