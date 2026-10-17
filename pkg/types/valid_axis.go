package types

import "math"

// InfiniteValidTo is +inf on the valid-time axis: the smallest Instant no
// instant reaches, the stored ValidTo of an open end under LiteralValidAxis.
// Together with UnboundedValidFrom (-inf) it is one of the axis's two ends;
// neither can be claimed as an instant (tkg_valid_from / tkg_valid_to refuse
// them as instants, a start of +inf or an end of -inf is empty).
const InfiniteValidTo Instant = math.MaxInt64

// ValidAxis is how a store's rows record valid time (round 11). It is fixed
// per store when the store opens, from the store's persisted untimed mode
// (graph Config.UntimedValidStart), and never changes per row:
//
//   - LegacyValidAxis (the zero value; UntimedFromTxFrom and UntimedUnbounded
//     stores): a stored ValidFrom of 0 makes no claim (the row starts at its
//     TxFrom, TemporalMetadata.UntimedValidFrom) and a stored ValidTo of 0 is
//     the open end. The instant 0 cannot be claimed.
//   - LiteralValidAxis (UntimedWholeAxis stores): every stored value is the
//     instant it names. A write without a valid time holds over the whole
//     axis, stored as UnboundedValidFrom and InfiniteValidTo, so no stored
//     value means "unset" and 0 is an ordinary instant.
//
// The axis is a pair of stored values, not a branch: the start a row makes no
// claim with (0 on the legacy axis; none on the literal axis, whose field
// holds InfiniteValidTo, which no stored start equals) and the stored end
// that is open (0, or InfiniteValidTo). Read a row's bounds through Start /
// End / Open; compare ValidFrom or ValidTo with 0 only on the legacy axis.
type ValidAxis struct {
	// unclaimed is the stored ValidFrom of a row without a start claim.
	unclaimed Instant
	// openEnd is the stored ValidTo of an open end.
	openEnd Instant
	// noClaimStart is the ValidFrom a write without a start claim stores:
	// unclaimed on the legacy axis, -inf on the literal axis.
	noClaimStart Instant
}

// LegacyValidAxis is the axis of UntimedFromTxFrom and UntimedUnbounded
// stores (and the zero ValidAxis): 0 = no start claim, 0 = open end.
var LegacyValidAxis = ValidAxis{}

// LiteralValidAxis is the axis of UntimedWholeAxis stores: every stored
// instant is literal, the open end is InfiniteValidTo.
var LiteralValidAxis = ValidAxis{unclaimed: InfiniteValidTo, openEnd: InfiniteValidTo, noClaimStart: UnboundedValidFrom}

// Literal reports whether a is LiteralValidAxis (a store-level property, read
// once at open; not a per-row test).
func (a ValidAxis) Literal() bool { return a.openEnd != 0 }

// OpenEnd is the ValidTo a writer stores for an open end: 0 on the legacy
// axis, InfiniteValidTo on the literal axis.
func (a ValidAxis) OpenEnd() Instant { return a.openEnd }

// NoClaimStart is the ValidFrom a writer stores for a version without a
// start claim: 0 ("no claim") on the legacy axis, UnboundedValidFrom (-inf,
// the whole axis) on the literal axis. (A legacy UntimedUnbounded store
// stores UnboundedValidFrom on an untimed first version: a mode rule, not
// the axis's.)
func (a ValidAxis) NoClaimStart() Instant { return a.noClaimStart }

// Open reports whether the stored ValidTo vt is the open end.
func (a ValidAxis) Open(vt Instant) bool { return vt == a.openEnd }

// End is the stored ValidTo vt as an instant: InfiniteValidTo for the open
// end, else vt. Interval tests compare it directly (at < End(vt)).
func (a ValidAxis) End(vt Instant) Instant {
	if vt == a.openEnd {
		return InfiniteValidTo
	}
	return vt
}

// Claimed reports whether the stored ValidFrom vf is a start claim: always on
// the literal axis, vf != 0 on the legacy axis.
func (a ValidAxis) Claimed(vf Instant) bool { return vf != a.unclaimed }

// Start is THE start rule of a row on the axis (backlog 62): its stored
// ValidFrom when that is a claim, else UntimedValidFrom(mint) (legacy axis
// only: TxFrom, UpdatedAt, then mint). A nil tm starts at mint.
func (a ValidAxis) Start(tm *TemporalMetadata, mint Instant) Instant {
	if tm != nil && tm.ValidFrom != a.unclaimed {
		return tm.ValidFrom
	}
	return tm.UntimedValidFrom(mint)
}

// NodeStart is Start for a materialized node, allocation-free on a frozen
// row (Node.EffectiveValidFrom is NodeStart on the legacy axis).
func (a ValidAxis) NodeStart(n *Node) Instant {
	if n == nil {
		return 0
	}
	if vf, _, ok := n.ValidRange(); ok && vf != a.unclaimed {
		return vf
	}
	if tx, _, _ := n.TxStamps(); tx != 0 {
		return tx
	}
	var tm *TemporalMetadata // a frozen row's meta carries no UpdatedAt
	if n.meta == nil {
		tm = n.temporal
	}
	return tm.UntimedValidFrom(n.ID().MintInstant())
}

// RelStart mirrors NodeStart for relationships.
func (a ValidAxis) RelStart(r *Relationship) Instant {
	if r == nil {
		return 0
	}
	if vf, _, ok := r.ValidRange(); ok && vf != a.unclaimed {
		return vf
	}
	if tx, _, _ := r.TxStamps(); tx != 0 {
		return tx
	}
	var tm *TemporalMetadata
	if r.meta == nil {
		tm = r.temporal
	}
	return tm.UntimedValidFrom(r.ID().MintInstant())
}

// ValidToAtTxTime is TemporalMetadata.ValidToAtTxTime on the axis: a row's
// own stored valid end as believed at pin, the axis's open end (OpenEnd)
// where a delete recorded after pin wrote the ValidTo, and for a nil row.
func (a ValidAxis) ValidToAtTxTime(tm *TemporalMetadata, pin Instant) Instant {
	switch {
	case tm == nil:
		return a.openEnd
	case tm.DeletedAt == 0:
		return tm.ValidTo
	case tm.DeletedAt > pin:
		if tm.DeleteWroteValidTo() {
			return a.openEnd
		}
		return tm.ValidTo
	case tm.DeleteKeptValidTo:
		return tm.DeletedAt
	default:
		return tm.ValidTo
	}
}

// RelateVersion is RelateOpen for a version's valid interval [vStart, vEnd)
// stored on the axis (vEnd open by Open) against a query interval
// [qStart, qEnd) in the query convention (qEnd 0: open). A start that is not
// an instant on the axis (0 on the legacy axis) and a query start of 0 are
// ErrOpenInterval, as in RelateOpen; an empty interval is ErrInvalidInterval.
// On the legacy axis it answers exactly as RelateOpen.
func (a ValidAxis) RelateVersion(vStart, vEnd, qStart, qEnd Instant) (AllenRelation, error) {
	if vStart == a.unclaimed || qStart == 0 {
		return 0, ErrOpenInterval
	}
	vEnd = a.End(vEnd)
	if qEnd == 0 {
		qEnd = InfiniteValidTo
	}
	if vStart >= vEnd || qStart >= qEnd {
		return 0, ErrInvalidInterval
	}
	return relate(vStart, vEnd, qStart, qEnd), nil
}

// Relate is types.Relate for two intervals stored on the axis: a start
// that is not an instant on the axis (0 on the legacy axis) or an open end
// is ErrOpenInterval, an empty interval ErrInvalidInterval. On the legacy
// axis it answers exactly as Relate; on the literal axis 0 is an ordinary
// bound.
func (a ValidAxis) Relate(aStart, aEnd, bStart, bEnd Instant) (AllenRelation, error) {
	if aStart == a.unclaimed || bStart == a.unclaimed || aEnd == a.openEnd || bEnd == a.openEnd {
		return 0, ErrOpenInterval
	}
	if aStart >= aEnd || bStart >= bEnd {
		return 0, ErrInvalidInterval
	}
	return relate(aStart, aEnd, bStart, bEnd), nil
}
