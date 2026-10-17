package core

import (
	"errors"
	"fmt"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Explicit presence of a written valid-from (round 11). A caller claims a
// valid start with a non-zero tkg_valid_from or with types.ExplicitInstant
// (the instant 0 included); the stored row says so through
// types.TemporalMetadata.ValidFromZero when the instant is 0, and every reader
// asks HasValidFrom / ValidFromClaim. A row valid from the explicit 0 carries
// row format version storeutil.ValidFromZeroWireFormatVersion; before the
// first one is written the store's format marker is raised to
// storeutil.ValidFromZeroStoreFormatVersion, so a release that cannot read
// them refuses the store at open.
//
// Only an entity's first version may start at the explicit 0: a valid END of
// 0 is "open" in every stored row (ValidTo), segment and timeline, so no
// version may end at the instant 0, and a later version (an update), an
// import of an ID with an earlier life, or a correction or excise crossing
// 0 on such an entity would end one there. Those are refused with
// ErrValidEndAtZero instead of being stored as open. Ends at 0 need their own
// presence (stored and derived ends), a later step.

// validFromZeroMeta is the MetaKV key whose write raises the marker.
const validFromZeroMeta = "valid_from_zero"

// stampValidFrom sets a version's valid start on tm: set says the caller
// claims one (vf may then be 0, the instant 0); no claim makes tm's start
// unset. The one writer of ValidFrom / ValidFromZero from a claim, so a row
// never keeps a stale marker. The format marker was raised where the claim
// was accepted (claimedStart, noteClaimedUpdate).
func stampValidFrom(tm *types.TemporalMetadata, vf types.Instant, set bool) {
	if !set {
		tm.ValidFrom, tm.ValidFromZero = 0, false
		return
	}
	tm.ValidFrom, tm.ValidFromZero = vf, vf == 0
}

// checkClaimedUpdate refuses an update that claims the explicit instant 0:
// the version before it would end at 0 (ErrValidEndAtZero). In-place
// rewrites refuse it alike (one rule for every update door).
func checkClaimedUpdate(tmp updateTemporal) error {
	if tmp.validFromSet && tmp.validFrom == 0 {
		return fmt.Errorf("%w: an update valid from the explicit 0", ErrValidEndAtZero)
	}
	return nil
}

// crossesZero reports whether a correction or excise span [vf, vt) (vt 0:
// open) holds instants on both sides of 0.
func crossesZero(vf, vt types.Instant) bool {
	return vf < 0 && (vt == 0 || vt > 0)
}

// startsAtZero reports whether tm is a row valid from the explicit 0.
func startsAtZero(tm *types.TemporalMetadata) bool {
	return tm != nil && tm.ValidFrom == 0 && tm.ValidFromZero
}

// checkCorrectionAtZero refuses a correction or excise over [vf, vt) that
// crosses 0 on an entity with a version valid from the explicit 0: its pieces
// would split at that version's start and one would end at 0. Only an
// entity's first version (version 0) and its closes can start at the
// explicit 0, so current and version 0 decide.
func checkCorrectionAtZero[T interface {
	Version() uint32
	Temporal() *types.TemporalMetadata
	comparable
}](vf, vt types.Instant, current T, first func() (T, error)) error {
	if !crossesZero(vf, vt) {
		return nil
	}
	var zero T
	if current != zero && startsAtZero(current.Temporal()) {
		return fmt.Errorf("%w: the span [%d, %d) crosses 0 on an entity valid from the explicit 0", ErrValidEndAtZero, vf, vt)
	}
	if current != zero && current.Version() == 0 {
		return nil
	}
	v0, err := first()
	if errors.Is(err, storepkg.ErrVersionNotFound) || errors.Is(err, storepkg.ErrNodeNotFound) || errors.Is(err, storepkg.ErrRelNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if startsAtZero(v0.Temporal()) {
		return fmt.Errorf("%w: the span [%d, %d) crosses 0 on an entity valid from the explicit 0", ErrValidEndAtZero, vf, vt)
	}
	return nil
}

// claimedStart is untimedStart for a create's start that may be claimed: a
// claim (set) is kept as it is (raising the format marker for the instant 0),
// else the untimed mode's start of a create (first) applies
// (UnboundedValidFrom under UntimedUnbounded, a claim from then on).
func (c *Core) claimedStart(vf types.Instant, set, first bool) (types.Instant, bool, error) {
	if set {
		if vf == 0 {
			if !first {
				return 0, false, fmt.Errorf("%w: tkg_valid_from = the explicit 0 on an ID with an earlier life", ErrValidEndAtZero)
			}
			if err := c.noteValidFromZero(); err != nil {
				return 0, false, err
			}
		}
		return vf, true, nil
	}
	vf, err := c.untimedStart(0, first)
	return vf, vf != 0, err
}

// claimedStartMode is claimedStart without the unfinished-conversion guard
// and without a marker raise (a dry run writes nothing).
func (c *Core) claimedStartMode(vf types.Instant, set, first bool) (types.Instant, bool) {
	if set {
		return vf, true
	}
	vf = c.untimedStartMode(0, first)
	return vf, vf != 0
}

// noteValidFromZero raises the store's format marker once per process before
// the first row valid from the explicit 0 is written (a raise is idempotent,
// so a reopen raises again on its first such write). A store without a format
// marker needs none: the rows' format version fails an older reader closed.
func (c *Core) noteValidFromZero() error {
	if c.validFromZeroMarked.Load() {
		return nil
	}
	fm, ok := c.store.(storepkg.MetaFormatMarkerCapability)
	if ok {
		if err := fm.MetaSetDurableRaising(validFromZeroMeta, []byte{1}, storeutil.ValidFromZeroStoreFormatVersion); err != nil {
			return fmt.Errorf("graph: raise the format marker for a row valid from the explicit 0: %w", err)
		}
	}
	c.validFromZeroMarked.Store(true)
	return nil
}
