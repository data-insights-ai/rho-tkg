package core

import (
	"context"
	"fmt"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// =============================================================================
// UntimedAtemporal writes (round 11, René and Markus 2026-10-17: atemporal is
// "just a flag")
//
// Under Config.UntimedValidStart = UntimedAtemporal a write without a valid
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
//     (cascadeExisting): a pure atemporal entity is one whole-axis segment,
//     planned from its current row in O(1) (backlog 67); an entity with
//     explicit valid-time segments costs O(segments) plus its history read.
//     A relationship's cascade takes the relationship's entity lock only
//     (as SetRelVersionInterval); deletes serialize on it too.
//   - The one-version doors (a label change, compare-and-set) apply to a
//     whole-axis entity (stamped UnboundedValidFrom); on an entity whose
//     valid time is not one whole-axis segment they are refused with
//     ErrUntimedModeMismatch (an open item).
//   - An untimed delete ends the entity in transaction time only: its
//     tombstones are retractions (stampDeleteTombstone).
//   - A write with a valid time (tkg_valid_from / tkg_valid_to on an update,
//     SetVersionInterval, CloseVersion, excise) keeps its rules: a timed
//     write onto an atemporal entity splits its whole-axis segment (FOR
//     PORTION OF).
// =============================================================================

// atemporalUntimedUpdate reports whether an update is an untimed write of an
// UntimedAtemporal graph: no valid-time claim (a tkg_valid_from claim or a
// tkg_valid_to); a plain tkg_valid_from 0 is no claim.
func atemporalUntimedUpdate(c *Core, tmp updateTemporal) bool {
	return c.untimedAtemporal.Load() && !tmp.validFromSet && !tmp.hasValidTo
}

// atemporalUpdateNode is the untimed update of an UntimedAtemporal graph:
// every valid-time segment of the node patched in one cascade at the clock or
// the caller instant tmp.txAt. It returns the current row after the write
// and whether anything changed (no segment changed: a no-op, as Update; at a
// caller instant that is ErrTxOrder, as Update).
func (c *Core) atemporalUpdateNode(ctx context.Context, id types.NodeID, prov updateProvenance, tmp updateTemporal, updates map[string]any) (*types.Node, bool, error) {
	written, err := c.cascadeNodeMode(ctx, id, types.UnboundedValidFrom, 0, updates, tmp.txAt, cascadeExisting, &prov)
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

// atemporalUpdateRel mirrors atemporalUpdateNode for relationships.
func (c *Core) atemporalUpdateRel(ctx context.Context, id types.RelID, prov updateProvenance, tmp updateTemporal, updates map[string]any) (*types.Relationship, bool, error) {
	written, err := c.cascadeRelMode(ctx, id, types.UnboundedValidFrom, 0, updates, tmp.txAt, cascadeExisting, &prov)
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

// deleteRetracts reports whether a delete's tombstones are retractions: a
// Retract door, or any delete of an UntimedAtemporal graph (an untimed delete
// ends the entity in transaction time only: belief in its life ends at the
// tombstone's instant for every valid time).
func (c *Core) deleteRetracts(spec tombstoneSpec) bool {
	return spec.retract || c.untimedAtemporal.Load()
}

// labelChangeStart is the valid-from a label change stamps on its new
// version (and compare-and-set, the other one-version untimed door): none
// (0, the version starts at its TxFrom) in the other modes; under
// UntimedAtemporal types.UnboundedValidFrom, the whole axis of the entity it
// applies to (checkAtemporalLabelChange / checkAtemporalVersioned* admitted
// it).
func (c *Core) labelChangeStart() types.Instant {
	if c.untimedAtemporal.Load() {
		return types.UnboundedValidFrom
	}
	return 0
}

// checkAtemporalLabelChange admits a label change of an UntimedAtemporal
// graph on a node whose valid time is one whole-axis segment (its current row
// starts at -inf, is open, live, its top version and newest belief): the new
// version replaces it everywhere in the new belief, SQL:2011's rule for that
// one segment. A node with explicit valid-time segments is refused with
// ErrUntimedModeMismatch: the label index follows the current row only, so a
// per-segment label change needs a store door this release does not have
// (an open item; a property update applies per segment).
func (c *Core) checkAtemporalLabelChange(id types.NodeID, current *types.Node) error {
	if !c.untimedAtemporal.Load() {
		return nil
	}
	whole, err := c.nodeWholeAxis(id, current)
	if err != nil {
		return err
	}
	if !whole {
		return fmt.Errorf("graph: an untimed label change of node %v, whose valid time is not one whole-axis segment, is not supported under UntimedAtemporal: %w", id, ErrUntimedModeMismatch)
	}
	return nil
}

// checkAtemporalVersionedNode is checkAtemporalLabelChange for another door
// that writes one new version from the current row without a valid time
// (compare-and-set): under UntimedAtemporal it is admitted on a whole-axis
// node only (its version is stamped UnboundedValidFrom, labelChangeStart).
func (c *Core) checkAtemporalVersionedNode(id types.NodeID, current *types.Node, door string) error {
	if !c.untimedAtemporal.Load() {
		return nil
	}
	whole, err := c.nodeWholeAxis(id, current)
	if err != nil {
		return err
	}
	if !whole {
		return fmt.Errorf("graph: an untimed %s of node %v, whose valid time is not one whole-axis segment, is not supported under UntimedAtemporal: %w", door, id, ErrUntimedModeMismatch)
	}
	return nil
}

// checkAtemporalVersionedRel mirrors checkAtemporalVersionedNode.
func (c *Core) checkAtemporalVersionedRel(id types.RelID, current *types.Relationship, door string) error {
	if !c.untimedAtemporal.Load() {
		return nil
	}
	whole, err := c.relWholeAxis(id, current)
	if err != nil {
		return err
	}
	if !whole {
		return fmt.Errorf("graph: an untimed %s of relationship %v, whose valid time is not one whole-axis segment, is not supported under UntimedAtemporal: %w", door, id, ErrUntimedModeMismatch)
	}
	return nil
}

// relWholeAxis mirrors nodeWholeAxis for relationships.
func (c *Core) relWholeAxis(id types.RelID, current *types.Relationship) (bool, error) {
	vf, claimed := current.ValidFromClaim()
	tm := current.Temporal()
	if !claimed || vf != types.UnboundedValidFrom || tm == nil || tm.ValidTo != 0 || tm.Void || tm.Retracted || tm.DeletedAt != 0 || tm.TxTo != 0 {
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
	vf, claimed := current.ValidFromClaim()
	tm := current.Temporal()
	if !claimed || vf != types.UnboundedValidFrom || tm == nil || tm.ValidTo != 0 || tm.Void || tm.Retracted || tm.DeletedAt != 0 || tm.TxTo != 0 {
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
