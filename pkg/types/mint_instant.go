package types

import "github.com/data-insights-ai/rho-tkg/v4/pkg/internal/idlayout"

// MintInstant returns the instant this node ID was minted, in Unix
// milliseconds (the snowflake wall clock). Since v4.52.3 (backlog 62) the graph
// starts a row without a valid-from at its TxFrom, so this instant is the
// derived start only of a legacy row that carries neither a ValidFrom nor a
// TxFrom; to tell an untimed create from its first segment, compare the
// segment's start with the first version's TxFrom (Node.TxStamps).
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
