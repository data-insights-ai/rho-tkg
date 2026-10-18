package core

import (
	"context"
	"fmt"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// =============================================================================
// UntimedWholeAxis writes (round 11, René and Markus 2026-10-17: atemporal is
// "just a flag")
//
// Under Config.UntimedValidStart = UntimedWholeAxis a write without a valid
// time makes no valid-time claim. Transaction time is not valid time: no
// such write derives a valid start from its TxFrom.
//
//   - An untimed create holds over the whole axis (untimedStart stores
//     types.UnboundedValidFrom, open), as under UntimedUnbounded.
//   - An untimed update (every update door: Nodes()/Rels().Update and
//     UpdateWithTx, the GraphTx, Batch and ingest twins — they all funnel into
//     updateNodePreparedInternal / updateRelationshipPreparedInternal) applies
//     to EVERY valid-time segment of the entity and keeps the segment
//     boundaries, SQL:2011's rule for an UPDATE without FOR PORTION OF
//     (ISO/IEC 9075-2:2011, an UPDATE of an application-time period table without FOR PORTION OF): each segment
//     gets a new version in the new belief, with the named properties changed
//     and the others kept per segment; the old versions end in transaction
//     time only (the newer belief wins); existence does not change (no piece
//     where no version is valid). The cascade kernel writes it
//     (cascadeExisting): a pure whole-axis entity is one whole-axis segment,
//     planned from its current row in O(1) (backlog 67); an entity with
//     explicit valid-time segments costs O(segments) plus its history read.
//     A relationship's cascade takes the relationship's entity lock only
//     (as SetRelVersionInterval); deletes serialize on it too.
//   - A label change applies to every valid-time segment like an untimed
//     update (whole_axis_label.go; a whole-axis node keeps the one-version
//     label door, which writes the same rows). Compare-and-set applies to a
//     whole-axis entity (stamped UnboundedValidFrom) and is refused with
//     ErrUntimedModeMismatch on one with explicit valid-time segments.
//   - An untimed delete ends the entity in transaction time only: its
//     tombstones are retractions (stampDeleteTombstone).
//   - A write with a valid time (tkg_valid_from / tkg_valid_to on an update,
//     SetVersionInterval, CloseVersion, excise) keeps its rules: a timed
//     write onto a whole-axis entity splits its whole-axis segment (FOR
//     PORTION OF).
// =============================================================================

// wholeAxisUntimedUpdate reports whether an update is an untimed write of an
// UntimedWholeAxis graph: no valid-time claim — no tkg_valid_from claim (on
// this axis every given value is one, 0 included) and no tkg_valid_to other
// than the open end (types.InfiniteValidTo, the whole axis on that side).
func wholeAxisUntimedUpdate(c *Core, tmp updateTemporal) bool {
	return c.untimedWholeAxis.Load() && !tmp.validFromSet && (!tmp.hasValidTo || c.axis().Open(tmp.validTo))
}

// wholeAxisUpdateNode is the untimed update of an UntimedWholeAxis graph:
// every valid-time segment of the node patched in one cascade at the clock or
// the caller instant tmp.txAt. It returns the current row after the write
// and whether anything changed (no segment changed: a no-op, as Update; at a
// caller instant that is ErrTxOrder, as Update).
func (c *Core) wholeAxisUpdateNode(ctx context.Context, id types.NodeID, prov updateProvenance, tmp updateTemporal, updates map[string]any) (*types.Node, bool, error) {
	written, err := c.cascadeNodeMode(ctx, id, types.UnboundedValidFrom, c.axis().OpenEnd(), updates, tmp.txAt, cascadeExisting, &prov, nil)
	if err != nil {
		return nil, false, err
	}
	if written == nil && tmp.txAt != 0 {
		return nil, false, fmt.Errorf("%w: an update at t %d with no changes records nothing", ErrTxOrder, tmp.txAt)
	}
	current, err := c.getCurrentNode(id)
	if err != nil {
		return nil, false, err
	}
	if written == nil {
		c.opNodeReads.Add(1)
	}
	return current, written != nil, nil
}

// wholeAxisUpdateRel mirrors wholeAxisUpdateNode for relationships.
func (c *Core) wholeAxisUpdateRel(ctx context.Context, id types.RelID, prov updateProvenance, tmp updateTemporal, updates map[string]any) (*types.Relationship, bool, error) {
	written, err := c.cascadeRelMode(ctx, id, types.UnboundedValidFrom, c.axis().OpenEnd(), updates, tmp.txAt, cascadeExisting, &prov)
	if err != nil {
		return nil, false, err
	}
	if written == nil && tmp.txAt != 0 {
		return nil, false, fmt.Errorf("%w: an update at t %d with no changes records nothing", ErrTxOrder, tmp.txAt)
	}
	current, err := c.getCurrentRelationship(id)
	if err != nil {
		return nil, false, err
	}
	if written == nil {
		c.opRelReads.Add(1)
	}
	return current, written != nil, nil
}

// wholeAxisSupersession decides whether a whole-axis untimed update
// (cascadeExisting) stamps the current row it moves to history with TxTo =
// the update's instant, as Update does on the legacy axis (sigma-tkgd
// 2026-10-18: History showed the replaced row as tx[T, _)).
//
// A stamped TxTo makes the row a replaced row for the read-time supersession
// rule (chain_supersession.go): every row the update appends then ends, at
// its start S, every older belief that started at or before S. That is the
// update's meaning wherever an appended row covers, and changes nothing where
// no version was valid. It would be wrong in two shapes, so neither stamps:
//
//   - a segment the update left unchanged (it already held the values) after
//     the first changed one: its row, older than an appended row starting at
//     or after the row's own start, would end there and lose its later
//     segment;
//   - a gap after the first changed segment on a chain that may hold void
//     rows (an excise): a void row that wins there would end, and an older
//     row below it would answer again. A chain planned from the current row
//     or the head entry (no history read) counts as one that may.
//
// Segments are fed in ascending valid order (note); replaces is asked after.
type wholeAxisSupersession struct {
	voidFree bool // the pre-update chain holds no void row (known only from a history read)
	seen     bool // a changed segment was fed
	unsafe   bool // a shape above was fed
}

// note feeds one segment: gap (no version valid there) or changed (the
// update appends a row for it).
func (s *wholeAxisSupersession) note(gap, changed bool) {
	if changed {
		s.seen = true
		return
	}
	if s.seen && (!gap || !s.voidFree) {
		s.unsafe = true
	}
}

// replaces reports whether the current row with stamps tm is stamped TxTo =
// now when the update moves it to history: no unsafe shape was fed and now
// follows the row's TxFrom (a replaced row has its TxTo after its TxFrom).
func (s *wholeAxisSupersession) replaces(tm *types.TemporalMetadata, now types.Instant) bool {
	return s.seen && !s.unsafe && (tm == nil || now > tm.TxFrom)
}

// deleteRetracts reports whether a delete's tombstones are retractions: a
// Retract door, or a Delete of an UntimedWholeAxis graph (an untimed delete
// ends the entity in transaction time only: belief in its life ends at the
// tombstone's instant for every valid time). An EndValidity door
// (spec.keepPast) never retracts: its tombstone is the legacy axis's Delete
// tombstone on every axis.
func (c *Core) deleteRetracts(spec tombstoneSpec) bool {
	return spec.retract || !spec.keepPast && c.untimedWholeAxis.Load()
}

// checkWholeAxisVersionedNode admits a door that writes one new version from
// the current row without a valid time (compare-and-set) under
// UntimedWholeAxis on a whole-axis node only (its current row starts at -inf,
// is open, live, its top version and newest belief); a node with explicit
// valid-time segments is refused with ErrUntimedModeMismatch: a per-segment
// compare has no one value to compare (a property update and a label change
// apply per segment). Admitted, its version is stamped UnboundedValidFrom
// (stampValidFrom with no claim on the literal axis).
func (c *Core) checkWholeAxisVersionedNode(id types.NodeID, current *types.Node, door string) error {
	if !c.untimedWholeAxis.Load() {
		return nil
	}
	whole, err := c.nodeWholeAxis(id, current)
	if err != nil {
		return err
	}
	if !whole {
		return fmt.Errorf("graph: an untimed %s of node %v, whose valid time is not one whole-axis segment, is not supported under UntimedWholeAxis: %w", door, id, ErrUntimedModeMismatch)
	}
	return nil
}

// checkWholeAxisVersionedRel mirrors checkWholeAxisVersionedNode.
func (c *Core) checkWholeAxisVersionedRel(id types.RelID, current *types.Relationship, door string) error {
	if !c.untimedWholeAxis.Load() {
		return nil
	}
	whole, err := c.relWholeAxis(id, current)
	if err != nil {
		return err
	}
	if !whole {
		return fmt.Errorf("graph: an untimed %s of relationship %v, whose valid time is not one whole-axis segment, is not supported under UntimedWholeAxis: %w", door, id, ErrUntimedModeMismatch)
	}
	return nil
}

// relWholeAxis mirrors nodeWholeAxis for relationships.
func (c *Core) relWholeAxis(id types.RelID, current *types.Relationship) (bool, error) {
	tm := current.Temporal()
	if tm == nil || tm.ValidFrom != types.UnboundedValidFrom || !c.axis().Open(tm.ValidTo) || tm.Void || tm.Retracted || tm.DeletedAt != 0 || tm.TxTo != 0 {
		return false, nil
	}
	if above, err := c.relHasHistoryAbove(id, current.Version()); err != nil || above {
		return false, err
	}
	var newest types.Instant
	if stamps := c.relHistoryStampsFunc(id); stamps != nil {
		f, t, _, err := stamps()
		if err != nil {
			return false, err
		}
		newest = max(f, t)
	} else {
		history, err := c.getRelHistory(id)
		if err != nil {
			return false, err
		}
		for _, h := range history {
			if htm := h.Temporal(); htm != nil {
				newest = max(newest, htm.TxFrom, htm.TxTo, htm.DeletedAt)
			}
		}
	}
	return newest <= tm.TxFrom, nil
}

// nodeWholeAxis reports whether node id's valid time is one whole-axis
// segment held by its current row: the row starts at -inf (a claim), is open,
// live, not void, the chain's top version, and no history row is a newer
// belief (stamped after it). The history's stamps come from the store's
// capability, else from a history read.
func (c *Core) nodeWholeAxis(id types.NodeID, current *types.Node) (bool, error) {
	tm := current.Temporal()
	if tm == nil || tm.ValidFrom != types.UnboundedValidFrom || !c.axis().Open(tm.ValidTo) || tm.Void || tm.Retracted || tm.DeletedAt != 0 || tm.TxTo != 0 {
		return false, nil
	}
	if above, err := c.nodeHasHistoryAbove(id, current.Version()); err != nil || above {
		return false, err
	}
	var newest types.Instant
	if stamps := c.nodeHistoryStampsFunc(id); stamps != nil {
		f, t, _, err := stamps()
		if err != nil {
			return false, err
		}
		newest = max(f, t)
	} else {
		history, err := c.getNodeHistory(id)
		if err != nil {
			return false, err
		}
		for _, h := range history {
			if htm := h.Temporal(); htm != nil {
				newest = max(newest, htm.TxFrom, htm.TxTo, htm.DeletedAt)
			}
		}
	}
	return newest <= tm.TxFrom, nil
}

// nodeHeadSegmentRows returns the rows of node id's effective timeline at the
// clock (one per segment), folded from its chain without the entity lock: a
// caller-instant pre-flight holds it already (checkNodeCallerUpdate). A
// whole-axis node is its current row alone, decided without the history
// read (O(1), as the kernel plans it, backlog 67).
func (c *Core) nodeHeadSegmentRows(id types.NodeID, current *types.Node) ([]*types.Node, error) {
	if whole, err := c.nodeWholeAxis(id, current); err != nil || whole {
		return []*types.Node{current}, err
	}
	history, err := c.getNodeHistory(id)
	if err != nil {
		return nil, err
	}
	pieces := c.nodeEffectivePieces(chainOf(history, current), c.peekNow())
	rows := make([]*types.Node, 0, len(pieces))
	for _, p := range pieces {
		rows = append(rows, p.row)
	}
	return rows, nil
}

// relHeadSegmentRows mirrors nodeHeadSegmentRows.
func (c *Core) relHeadSegmentRows(id types.RelID, current *types.Relationship) ([]*types.Relationship, error) {
	if whole, err := c.relWholeAxis(id, current); err != nil || whole {
		return []*types.Relationship{current}, err
	}
	history, err := c.getRelHistory(id)
	if err != nil {
		return nil, err
	}
	pieces := c.relEffectivePieces(chainOf(history, current), c.peekNow())
	rows := make([]*types.Relationship, 0, len(pieces))
	for _, p := range pieces {
		rows = append(rows, p.row)
	}
	return rows, nil
}
