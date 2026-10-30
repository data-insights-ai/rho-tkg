package store

import (
	"errors"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ErrValidFromOutsideIDRange is returned when a store minting IDs at valid time
// (ValidTimeMintCapability) is given a ValidFrom an ID cannot carry: before the
// snowflake epoch (2026-01-01 UTC) or past the end of its 48-bit microsecond
// field (2034-12-02 19:29 UTC).
var ErrValidFromOutsideIDRange = errors.New("graph: ValidFrom outside the ID time range 2026-01-01 to 2034-12-02 cannot be minted")

// ValidTimeMintField is the snowflake node field reserved for IDs minted at
// valid time, the highest.
const ValidTimeMintField = 31

// ValidTimeMintCapability is OPTIONAL: a store that places event data by valid
// time mints its IDs (ADR-0013, tiered Config.MintAtValidFrom). ok is false
// when the store does not mint that entity (the option is off, a reference
// label): the caller's generator does. at is the entity's ValidFrom; set false
// (or at 0) mints at the store's clock. A relationship is minted inside its
// start node's window, so its ID routes to the shard that holds it.
type ValidTimeMintCapability interface {
	// MintsAtValidTime reports whether the store mints (the option is on): its
	// IDs then take node field ValidTimeMintField, which core's generators
	// must keep clear of.
	MintsAtValidTime() bool
	MintNodeIDAt(primaryLabel uint16, at types.Instant, set bool) (id types.NodeID, ok bool, err error)
	MintRelIDAt(start types.NodeID, at types.Instant, set bool) (id types.RelID, ok bool, err error)
}
