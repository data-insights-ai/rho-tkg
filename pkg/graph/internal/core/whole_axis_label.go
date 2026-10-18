package core

import (
	"context"
	"fmt"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// =============================================================================
// Untimed label changes of UntimedWholeAxis nodes with valid-time segments
// (sigma-tkgd 2026-10-18; round 11 refused them with ErrUntimedModeMismatch)
//
// A label change carries no valid time, so under UntimedWholeAxis it follows
// the rule the untimed property update follows (SQL:2011, an UPDATE without
// FOR PORTION OF): it applies to EVERY valid-time segment of the node and
// keeps the segment boundaries. Each segment whose version does not already
// hold the change gets a new version in the new belief (its labels plus or
// minus the one label, every property kept); the old versions end in
// transaction time only; existence does not change. Reads at any valid
// instant inside any segment then see the new label set, reads pinned before
// the change see the old label set per segment, and the current label index
// follows the current row (the store's label door writes it).
//
// The cascade kernel writes it (cascadeNodeMode with cascadeExisting and a
// nodeLabelOp in place of a property patch): one planning, one version
// allocation, one supersession rule for both kinds of untimed change. A node
// whose valid time is one whole-axis segment keeps the one-version label door
// (node_label.go), which writes the same rows.
//
// Errors per segment: AddLabel refuses with ErrTooManyLabels when a segment
// would exceed MaxLabelsPerNode; RemoveLabel refuses with ErrLabelNotFound
// when no segment carries the label and with ErrLastLabel when a segment
// would be left without a label. A node whose segments all hold the change
// is a no-op (AddLabel returns without writing, as on a node that has the
// label).
// =============================================================================

// nodeLabelOp is a whole-axis label change: add or remove token tok on every
// segment. Its methods are defined on a nil receiver (no label change: a
// property cascade).
type nodeLabelOp struct {
	tok uint16
	add bool
}

// segmentMutates reports whether the segment row base changes: for a label
// op, whether base lacks (add) or carries (remove) the token; else the
// property patch's rule.
func (lop *nodeLabelOp) segmentMutates(base *types.Node, props func() bool) bool {
	if lop == nil {
		return props()
	}
	return base.HasLabelTokenRaw(lop.tok) != lop.add
}

// added is the token the op adds (0: none), for the unique-constraint check.
func (lop *nodeLabelOp) added() uint16 {
	if lop == nil || !lop.add {
		return 0
	}
	return lop.tok
}

// applyNodeLabelOp changes the built row's labels (a private copy of its
// segment's base): the token added or removed, with the label-count limits.
func (c *Core) applyNodeLabelOp(row *types.Node, lop *nodeLabelOp) error {
	if lop == nil {
		return nil
	}
	if lop.add {
		if row.LabelTokenCount()+1 > c.validation.MaxLabelsPerNode {
			return fmt.Errorf("%w: %d > %d", ErrTooManyLabels, row.LabelTokenCount()+1, c.validation.MaxLabelsPerNode)
		}
		if !row.AddLabelTokenRaw(lop.tok) {
			return fmt.Errorf("%w: node %d already has label token %d", storepkg.ErrInvalidStoreMutation, row.ID(), lop.tok)
		}
		return nil
	}
	if row.LabelTokenCount() == 1 {
		return ErrLastLabel
	}
	row.RemoveLabelTokenRaw(lop.tok)
	return nil
}

// slotChanges reports whether the row taking the store's current slot
// changes the slot's label set, and checks that it changes it by exactly the
// op's token (the store's label door's contract).
func (lop *nodeLabelOp) slotChanges(current, newCurrent *types.Node) (bool, error) {
	if lop == nil || current.HasLabelTokenRaw(lop.tok) == newCurrent.HasLabelTokenRaw(lop.tok) {
		return false, nil
	}
	if lop.add {
		return true, storepkg.ValidateNodeLabelAddition(current, newCurrent, lop.tok)
	}
	return true, storepkg.ValidateNodeLabelRemoval(current, newCurrent, lop.tok)
}

// writeSlot writes newCurrent into the store's current slot through the
// label door (the label index follows), after the kernel moved the replaced
// row to history. A replica applies the same pair of records (a history put,
// then the label door without history).
func (lop *nodeLabelOp) writeSlot(c *Core, id types.NodeID, newCurrent *types.Node) error {
	if lop.add {
		return c.store.AddNodeLabelToken(id, lop.tok, newCurrent)
	}
	return c.store.RemoveNodeLabelToken(id, lop.tok, newCurrent)
}

// labelChangePerSegment reports whether a label change of node id (current
// row current) applies per segment: an UntimedWholeAxis graph and a node
// whose valid time is not one whole-axis segment. Caller holds the entity
// lock and releases it before wholeAxisLabelChange.
func (c *Core) labelChangePerSegment(id types.NodeID, current *types.Node) (bool, error) {
	if !c.untimedWholeAxis.Load() {
		return false, nil
	}
	whole, err := c.nodeWholeAxis(id, current)
	return !whole && err == nil, err
}

// wholeAxisLabelChange adds (add) or removes label on every valid-time
// segment of node id in one cascade at the clock. It returns whether
// anything was written. Callers hold c.mu (RLock standalone, Lock in a tx)
// and not the entity lock (the kernel takes it).
func (c *Core) wholeAxisLabelChange(ctx context.Context, id types.NodeID, label string, add bool) (bool, error) {
	var tok uint16
	if add {
		var err error
		// Persisted at once: an allocated but unused token is harmless, and
		// the kernel checkpoints the registries before its first write.
		if tok, err = c.getOrCreateLabelPersisted(label); err != nil {
			return false, fmt.Errorf("graph: add label: %w", err)
		}
	} else {
		var ok bool
		if tok, ok = c.labels.Lookup(label); !ok {
			return false, ErrLabelNotFound
		}
	}
	written, err := c.cascadeNodeMode(ctx, id, types.UnboundedValidFrom, c.axis().OpenEnd(), nil, 0, cascadeExisting, nil, &nodeLabelOp{tok: tok, add: add})
	if err != nil {
		return false, err
	}
	if written == nil && !add {
		return false, ErrLabelNotFound // no segment carries the label
	}
	return written != nil, nil
}
