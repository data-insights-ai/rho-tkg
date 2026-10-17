package core

import (
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Valid-time claims (round 11). A write claims a valid start with
// tkg_valid_from (and an end with tkg_valid_to); which values are claims is
// the graph's valid axis (types.ValidAxis, parseValidClaim), fixed at open
// by the store's untimed mode:
//
//   - On the literal axis (UntimedWholeAxis) every value is an instant, the
//     instant 0 included, and no stored value means "unset": a write without
//     a claim stores the whole axis (types.UnboundedValidFrom, the open end
//     types.InfiniteValidTo) or, for an untimed update, every segment's own
//     bounds (cascadeExisting). Nothing on this axis writes a 0 that is not a
//     claimed instant.
//   - On the legacy axis (UntimedFromTxFrom, UntimedUnbounded) a stored
//     ValidFrom of 0 is "no claim" (the row starts at its TxFrom) and a stored
//     ValidTo of 0 the open end, as before; the instant 0 cannot be claimed
//     (ErrZeroInstantLegacyAxis).
//
// A row carries no flag and no format version for either: the axis is a
// property of the store, decided once at open.

// stampValidFrom sets a version's valid start on tm: vf when set (a claim;
// on the literal axis the instant 0 is one), else the valid axis a's start of
// no claim (ValidAxis.NoClaimStart: the legacy axis's stored 0, the literal
// axis's -inf). The one writer of a door's ValidFrom from a claim.
func stampValidFrom(a types.ValidAxis, tm *types.TemporalMetadata, vf types.Instant, set bool) {
	if !set {
		vf = a.NoClaimStart()
	}
	tm.ValidFrom = vf
}

// claimedStart is untimedStart for a create's start that may be claimed: a
// claim (set) is kept as it is, else the untimed mode's start of a create
// (first) applies (UnboundedValidFrom under UntimedUnbounded and
// UntimedWholeAxis, a claim from then on).
func (c *Core) claimedStart(vf types.Instant, set, first bool) (types.Instant, bool, error) {
	if set {
		return vf, true, nil
	}
	vf, err := c.untimedStart(first)
	return vf, c.axis().Claimed(vf), err
}

// claimedStartMode is claimedStart without the unfinished-conversion guard (a
// dry run writes nothing).
func (c *Core) claimedStartMode(vf types.Instant, set, first bool) (types.Instant, bool) {
	if set {
		return vf, true
	}
	vf = c.untimedStartMode(first)
	return vf, c.axis().Claimed(vf)
}
