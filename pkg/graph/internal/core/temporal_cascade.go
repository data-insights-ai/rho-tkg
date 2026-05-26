package core

import (
	"context"
	"errors"
	"fmt"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/store"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/internal/integrity"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
)

// =============================================================================
// Cascade timeline edit — full implementation
//
// Five overlap classifications between an existing version's valid interval
// [vf, vt) and the cascade target [newVF, newVT):
//
//   - keep            no overlap, version untouched
//   - closeRight      vf < newVF < vt — close existing at newVF (vt becomes newVF)
//   - openLeft        newVF <= vf < newVT < vt — open existing from newVT
//   - eclipse         newVF <= vf < vt <= newVT — version fully contained,
//                     marked zero-length (ValidFrom == ValidTo) so the
//                     resolver never matches it for any VT query
//   - split           vf < newVF < newVT < vt — version spans the target;
//                     becomes two rows: [vf, newVF) at original version
//                     number, [newVT, vt) at a freshly-allocated version
//
// Per-row hashes are unchanged because TemporalMetadata is NOT part of the
// content hash (see integrity.computeNodeHashWithBuffer). The new version
// itself gets a fresh hash and joins the chain via PrevHash from whichever
// row it directly supersedes on the VT axis.
//
// "Current" semantics: the row that has the latest open-ended interval
// (max(effectiveValidFrom) where ValidTo == 0) is the new current. If the
// cascade leaves no open-ended row, the entity has no current — the store
// still holds history rows, queries by ID return ErrNodeNotFound, but
// temporal queries find the appropriate version.
// =============================================================================

type cascadeAction int

const (
	cascadeKeep cascadeAction = iota
	cascadeCloseRight
	cascadeOpenLeft
	cascadeEclipse
	cascadeSplit
)

type nodeCascadeOp struct {
	action        cascadeAction
	origVersion   uint32
	original      *types.Node // for diagnostics only
	modified      *types.Node // close-right / open-left / eclipse / split-left
	splitRight    *types.Node // split only
	splitRightVer uint32
}

type relCascadeOp struct {
	action        cascadeAction
	origVersion   uint32
	original      *types.Relationship
	modified      *types.Relationship
	splitRight    *types.Relationship
	splitRightVer uint32
}

// classifyNodeOverlap categorises an existing version vs the cascade target.
// Caller-supplied newVT == 0 means open-ended on the right.
func classifyNodeOverlap(vf, vt, newVF, newVT types.Instant) cascadeAction {
	rightOpen := vt == 0
	newRightOpen := newVT == 0

	// no overlap: existing ends before newVF
	if !rightOpen && vt <= newVF {
		return cascadeKeep
	}
	// no overlap: existing starts after newVT
	if !newRightOpen && vf >= newVT {
		return cascadeKeep
	}

	startedBefore := vf < newVF
	startedAtOrAfter := vf >= newVF
	endedAtOrBefore := !rightOpen && (newRightOpen || vt <= newVT)
	endedAfter := rightOpen || (!newRightOpen && vt > newVT)

	switch {
	case startedBefore && endedAfter && !newRightOpen:
		return cascadeSplit
	case startedBefore && endedAtOrBefore:
		return cascadeCloseRight
	case startedBefore && endedAfter && newRightOpen:
		// vf < newVF, new open-ended → existing must close at newVF
		return cascadeCloseRight
	case startedAtOrAfter && endedAtOrBefore:
		return cascadeEclipse
	case startedAtOrAfter && endedAfter:
		return cascadeOpenLeft
	}
	return cascadeKeep
}

// nodeEffectiveBounds returns the explicit (vf, vt) on the version's
// TemporalMetadata, falling back to nodeValidFrom(snowflake-derived) when
// ValidFrom is unset.
func (c *Core) nodeEffectiveBounds(n *types.Node) (types.Instant, types.Instant) {
	tm := n.Temporal()
	vf := c.nodeValidFrom(n)
	var vt types.Instant
	if tm != nil {
		if tm.ValidFrom != 0 {
			vf = tm.ValidFrom
		}
		vt = tm.ValidTo
	}
	return vf, vt
}

func (c *Core) relEffectiveBounds(r *types.Relationship) (types.Instant, types.Instant) {
	tm := r.Temporal()
	vf := c.relValidFrom(r)
	var vt types.Instant
	if tm != nil {
		if tm.ValidFrom != 0 {
			vf = tm.ValidFrom
		}
		vt = tm.ValidTo
	}
	return vf, vt
}

// eclipsedNodeBounds returns whether a node's temporal metadata represents
// a cascade-eclipsed (near-zero-length, 1-instant) interval. Sentinel:
// ValidTo == ValidFrom + 1. The store rejects ValidFrom == ValidTo, so we
// use +1 as the smallest tile the store accepts. The resolver explicitly
// skips eclipsed rows during version selection so the 1-instant width does
// not cause spurious matches at t == ValidFrom.
func eclipsedNodeBounds(n *types.Node) bool {
	tm := n.Temporal()
	if tm == nil {
		return false
	}
	return tm.ValidFrom != 0 && tm.ValidTo != 0 && tm.ValidTo == tm.ValidFrom+1
}

func eclipsedRelBounds(r *types.Relationship) bool {
	tm := r.Temporal()
	if tm == nil {
		return false
	}
	return tm.ValidFrom != 0 && tm.ValidTo != 0 && tm.ValidTo == tm.ValidFrom+1
}

// =============================================================================
// Node cascade
// =============================================================================

func (c *Core) cascadeNodeVersionInterval(ctx context.Context, id types.NodeID, newVF, newVT types.Instant, props map[string]any) (*types.Node, error) {
	if err := storepkg.ValidateNodeID(id); err != nil {
		return nil, err
	}
	if newVF == 0 {
		return nil, fmt.Errorf("%w: cascade requires non-zero validFrom", ErrInvalidTimeRange)
	}
	if newVT != 0 && newVF >= newVT {
		return nil, fmt.Errorf("%w: validFrom %d >= validTo %d", ErrInvalidTimeRange, newVF, newVT)
	}
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	c.entityLocks.LockEntity(id.SnowflakeID())
	defer c.entityLocks.UnlockEntity(id.SnowflakeID())

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}
	if err := c.checkpointDirtyRegistriesBeforeMutation("cascade node"); err != nil {
		return nil, err
	}

	current, err := c.getCurrentNode(id)
	if err != nil && !errors.Is(err, storepkg.ErrNodeNotFound) {
		return nil, err
	}
	history, err := c.getNodeHistory(id)
	if err != nil {
		return nil, err
	}
	if current == nil && len(history) == 0 {
		return nil, storepkg.ErrNodeNotFound
	}

	maxVersion := uint32(0)
	for _, h := range history {
		if h.Version() > maxVersion {
			maxVersion = h.Version()
		}
	}
	if current != nil && current.Version() > maxVersion {
		maxVersion = current.Version()
	}
	nextVersion := maxVersion + 1

	// Pick the most recent non-eclipsed version as the template for
	// labels/integrity carry-over for the new row. Current preferred; else
	// the latest history row.
	var template *types.Node
	if current != nil && !eclipsedNodeBounds(current) {
		template = current
	} else {
		for i := len(history) - 1; i >= 0; i-- {
			if !eclipsedNodeBounds(history[i]) {
				template = history[i]
				break
			}
		}
	}
	if template == nil {
		return nil, fmt.Errorf("%w: cascade requires at least one non-eclipsed version", storepkg.ErrNodeNotFound)
	}

	// Build chain (history first, ascending by version, then current) so we
	// can derive each version's EFFECTIVE bounds via the resolver — same
	// algorithm as nodeVersionBounds (vEnd uses next.ValidFrom when set,
	// skipping eclipsed rows).
	chain := make([]*types.Node, 0, len(history)+1)
	chain = append(chain, history...)
	if current != nil {
		chain = append(chain, current)
	}

	ops := make([]nodeCascadeOp, 0)
	for i, v := range chain {
		if eclipsedNodeBounds(v) {
			continue
		}
		vf, vt := c.nodeVersionBounds(chain, i)
		action := classifyNodeOverlap(vf, vt, newVF, newVT)
		if action == cascadeKeep {
			continue
		}
		op := nodeCascadeOp{action: action, origVersion: v.Version(), original: v}
		switch action {
		case cascadeCloseRight:
			modified := v.DeepCopy()
			ensureNodeTemporal(modified)
			modified.Temporal().ValidFrom = vf
			modified.Temporal().ValidTo = newVF
			op.modified = modified
		case cascadeOpenLeft:
			modified := v.DeepCopy()
			ensureNodeTemporal(modified)
			modified.Temporal().ValidFrom = newVT
			modified.Temporal().ValidTo = vt
			op.modified = modified
		case cascadeEclipse:
			modified := v.DeepCopy()
			ensureNodeTemporal(modified)
			// Eclipse marker: keep original ValidFrom, set ValidTo = VF + 1.
			// This preserves the row's identity for the legacy inheritance
			// heuristic (which compares to prev's ValidFrom) and lets the
			// resolver skip the row via eclipsedNodeBounds() == VT == VF+1.
			modified.Temporal().ValidFrom = vf
			modified.Temporal().ValidTo = vf + 1
			op.modified = modified
		case cascadeSplit:
			left := v.DeepCopy()
			ensureNodeTemporal(left)
			left.Temporal().ValidFrom = vf
			left.Temporal().ValidTo = newVF
			op.modified = left

			right := v.DeepCopy()
			ensureNodeTemporal(right)
			right.Temporal().ValidFrom = newVT
			right.Temporal().ValidTo = vt
			right.SetVersion(nextVersion)
			op.splitRight = right
			op.splitRightVer = nextVersion
			nextVersion++
		}
		ops = append(ops, op)
	}

	// Build the new version from template, applying caller props.
	newVer := template.DeepCopy()
	newVer.SetVersion(nextVersion)
	nextVersion++
	for key, val := range props {
		if val == nil {
			if _, err := newVer.DeleteProperty(key); err != nil {
				return nil, fmt.Errorf("graph: cascade prop %q: %w", key, err)
			}
		} else {
			if err := newVer.SetProperty(key, val); err != nil {
				return nil, fmt.Errorf("graph: cascade prop %q: %w", key, err)
			}
		}
	}
	if newVer.PropertyCount() > c.validation.MaxPropertiesPerEntity {
		return nil, fmt.Errorf("%w: %d > %d", ErrTooManyProperties, newVer.PropertyCount(), c.validation.MaxPropertiesPerEntity)
	}
	ensureNodeTemporal(newVer)
	newVer.Temporal().ValidFrom = newVF
	newVer.Temporal().ValidTo = newVT
	now := c.now()
	newVer.Temporal().UpdatedAt = now
	newVer.Temporal().TxFrom = now

	// Compute hash + chain from the row this new version directly supersedes
	// on the VT axis (latest non-eclipsed predecessor).
	prevHash := ""
	if ig := template.Integrity(); ig != nil {
		prevHash = ig.Hash
	}
	labels := c.nodeLabelsUnlocked(newVer)
	hash, err := integrity.ComputeNodeHashChecked(newVer, labels)
	if err != nil {
		return nil, fmt.Errorf("graph: cascade compute hash: %w", err)
	}
	newVer.SetIntegrity(nodeIntegrityWithHash(newVer.Integrity(), hash, prevHash))

	// Decide whether the new version becomes the new current row. It does
	// if its interval is the rightmost open-ended (newVT == 0) AND its
	// ValidFrom is the latest among all post-cascade open-ended intervals.
	becomesCurrent := newVT == 0
	if becomesCurrent {
		for _, op := range ops {
			if op.modified == nil {
				continue
			}
			tm := op.modified.Temporal()
			if tm != nil && tm.ValidTo == 0 && tm.ValidFrom > newVF {
				becomesCurrent = false
				break
			}
		}
	}

	// Write all modifications + the new version. Sequential under the entity
	// lock; not atomic at the wire level (a crash mid-cascade leaves partial
	// state). Acceptable trade-off pending a future ReplaceNodeWithCascade
	// store API.
	for _, op := range ops {
		if op.modified == nil {
			continue
		}
		// If the modified row IS the current row, we'd need ReplaceNode
		// (which goes through the "current" KV slot, not history).
		// Detect by Version() matching current's version. Otherwise it's
		// strictly a history row write.
		if current != nil && op.origVersion == current.Version() {
			if becomesCurrent && newVT == 0 {
				// new version takes current; demote modified to history
				if err := c.store.PutNodeVersion(id, op.origVersion, op.modified); err != nil {
					return nil, fmt.Errorf("graph: cascade demote current to history: %w", err)
				}
			} else {
				// modified row stays as current
				if err := c.store.ReplaceNode(op.modified); err != nil {
					return nil, fmt.Errorf("graph: cascade replace current: %w", err)
				}
			}
		} else {
			if err := c.store.PutNodeVersion(id, op.origVersion, op.modified); err != nil {
				return nil, fmt.Errorf("graph: cascade put history: %w", err)
			}
		}
		if op.splitRight != nil {
			if err := c.store.PutNodeVersion(id, op.splitRightVer, op.splitRight); err != nil {
				return nil, fmt.Errorf("graph: cascade put split-right: %w", err)
			}
		}
	}

	// Write the new version itself.
	if becomesCurrent {
		// If current wasn't modified by the cascade ops (i.e., current's
		// interval was outside the cascade target), we still need to demote
		// it to history because the new version takes over the current slot.
		if current != nil && !cascadeModifiedVersion(ops, current.Version()) {
			if err := c.store.PutNodeVersion(id, current.Version(), current); err != nil {
				return nil, fmt.Errorf("graph: cascade archive existing current: %w", err)
			}
		}
		if err := c.store.ReplaceNode(newVer); err != nil {
			return nil, fmt.Errorf("graph: cascade replace with new current: %w", err)
		}
	} else {
		if err := c.store.PutNodeVersion(id, newVer.Version(), newVer); err != nil {
			return nil, fmt.Errorf("graph: cascade put new history row: %w", err)
		}
	}

	c.opNodeUpdates.Add(1)
	return newVer, nil
}

func cascadeModifiedVersion(ops []nodeCascadeOp, v uint32) bool {
	for _, op := range ops {
		if op.modified != nil && op.origVersion == v {
			return true
		}
	}
	return false
}

func ensureNodeTemporal(n *types.Node) {
	if n.Temporal() == nil {
		n.SetTemporal(&types.TemporalMetadata{})
	}
}

func ensureRelTemporal(r *types.Relationship) {
	if r.Temporal() == nil {
		r.SetTemporal(&types.TemporalMetadata{})
	}
}

// =============================================================================
// Relationship cascade (mirror of node cascade, rule 2 parity)
// =============================================================================

func (c *Core) cascadeRelVersionInterval(ctx context.Context, id types.RelID, newVF, newVT types.Instant, props map[string]any) (*types.Relationship, error) {
	if err := storepkg.ValidateRelID(id); err != nil {
		return nil, err
	}
	if newVF == 0 {
		return nil, fmt.Errorf("%w: cascade requires non-zero validFrom", ErrInvalidTimeRange)
	}
	if newVT != 0 && newVF >= newVT {
		return nil, fmt.Errorf("%w: validFrom %d >= validTo %d", ErrInvalidTimeRange, newVF, newVT)
	}
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	c.entityLocks.LockEntity(id.SnowflakeID())
	defer c.entityLocks.UnlockEntity(id.SnowflakeID())

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}
	if err := c.checkpointDirtyRegistriesBeforeMutation("cascade rel"); err != nil {
		return nil, err
	}

	current, err := c.getCurrentRelationship(id)
	if err != nil && !errors.Is(err, storepkg.ErrRelNotFound) {
		return nil, err
	}
	history, err := c.getRelHistory(id)
	if err != nil {
		return nil, err
	}
	if current == nil && len(history) == 0 {
		return nil, storepkg.ErrRelNotFound
	}

	maxVersion := uint32(0)
	for _, h := range history {
		if h.Version() > maxVersion {
			maxVersion = h.Version()
		}
	}
	if current != nil && current.Version() > maxVersion {
		maxVersion = current.Version()
	}
	nextVersion := maxVersion + 1

	var template *types.Relationship
	if current != nil && !eclipsedRelBounds(current) {
		template = current
	} else {
		for i := len(history) - 1; i >= 0; i-- {
			if !eclipsedRelBounds(history[i]) {
				template = history[i]
				break
			}
		}
	}
	if template == nil {
		return nil, fmt.Errorf("%w: cascade requires at least one non-eclipsed version", storepkg.ErrRelNotFound)
	}

	chain := make([]*types.Relationship, 0, len(history)+1)
	chain = append(chain, history...)
	if current != nil {
		chain = append(chain, current)
	}

	ops := make([]relCascadeOp, 0)
	for i, v := range chain {
		if eclipsedRelBounds(v) {
			continue
		}
		vf, vt := c.relVersionBounds(chain, i)
		action := classifyNodeOverlap(vf, vt, newVF, newVT)
		if action == cascadeKeep {
			continue
		}
		op := relCascadeOp{action: action, origVersion: v.Version(), original: v}
		switch action {
		case cascadeCloseRight:
			modified := v.DeepCopy()
			ensureRelTemporal(modified)
			modified.Temporal().ValidFrom = vf
			modified.Temporal().ValidTo = newVF
			op.modified = modified
		case cascadeOpenLeft:
			modified := v.DeepCopy()
			ensureRelTemporal(modified)
			modified.Temporal().ValidFrom = newVT
			modified.Temporal().ValidTo = vt
			op.modified = modified
		case cascadeEclipse:
			modified := v.DeepCopy()
			ensureRelTemporal(modified)
			modified.Temporal().ValidFrom = vf
			modified.Temporal().ValidTo = vf + 1
			op.modified = modified
		case cascadeSplit:
			left := v.DeepCopy()
			ensureRelTemporal(left)
			left.Temporal().ValidFrom = vf
			left.Temporal().ValidTo = newVF
			op.modified = left

			right := v.DeepCopy()
			ensureRelTemporal(right)
			right.Temporal().ValidFrom = newVT
			right.Temporal().ValidTo = vt
			right.SetVersion(nextVersion)
			op.splitRight = right
			op.splitRightVer = nextVersion
			nextVersion++
		}
		ops = append(ops, op)
	}

	newVer := template.DeepCopy()
	newVer.SetVersion(nextVersion)
	nextVersion++
	for key, val := range props {
		if val == nil {
			if _, err := newVer.DeleteProperty(key); err != nil {
				return nil, fmt.Errorf("graph: cascade prop %q: %w", key, err)
			}
		} else {
			if err := newVer.SetProperty(key, val); err != nil {
				return nil, fmt.Errorf("graph: cascade prop %q: %w", key, err)
			}
		}
	}
	if newVer.PropertyCount() > c.validation.MaxPropertiesPerEntity {
		return nil, fmt.Errorf("%w: %d > %d", ErrTooManyProperties, newVer.PropertyCount(), c.validation.MaxPropertiesPerEntity)
	}
	ensureRelTemporal(newVer)
	newVer.Temporal().ValidFrom = newVF
	newVer.Temporal().ValidTo = newVT
	now := c.now()
	newVer.Temporal().UpdatedAt = now
	newVer.Temporal().TxFrom = now

	prevHash := ""
	if ig := template.Integrity(); ig != nil {
		prevHash = ig.Hash
	}
	relTypeName := c.relTypeUnlocked(newVer)
	hash, err := integrity.ComputeRelHashChecked(newVer, relTypeName)
	if err != nil {
		return nil, fmt.Errorf("graph: cascade compute rel hash: %w", err)
	}
	relIG := relIntegrityWithHash(newVer.Integrity(), hash, prevHash)
	if err := c.refreshRelationshipEndpointHashes(newVer, relIG); err != nil {
		return nil, fmt.Errorf("graph: cascade refresh endpoint hashes: %w", err)
	}
	newVer.SetIntegrity(relIG)

	becomesCurrent := newVT == 0
	if becomesCurrent {
		for _, op := range ops {
			if op.modified == nil {
				continue
			}
			tm := op.modified.Temporal()
			if tm != nil && tm.ValidTo == 0 && tm.ValidFrom > newVF {
				becomesCurrent = false
				break
			}
		}
	}

	for _, op := range ops {
		if op.modified == nil {
			continue
		}
		if current != nil && op.origVersion == current.Version() {
			if becomesCurrent && newVT == 0 {
				if err := c.store.PutRelVersion(id, op.origVersion, op.modified); err != nil {
					return nil, fmt.Errorf("graph: cascade demote rel current: %w", err)
				}
			} else {
				if err := c.store.ReplaceRelationship(op.modified); err != nil {
					return nil, fmt.Errorf("graph: cascade replace rel current: %w", err)
				}
			}
		} else {
			if err := c.store.PutRelVersion(id, op.origVersion, op.modified); err != nil {
				return nil, fmt.Errorf("graph: cascade put rel history: %w", err)
			}
		}
		if op.splitRight != nil {
			if err := c.store.PutRelVersion(id, op.splitRightVer, op.splitRight); err != nil {
				return nil, fmt.Errorf("graph: cascade put rel split-right: %w", err)
			}
		}
	}

	if becomesCurrent {
		if current != nil && !cascadeModifiedRelVersion(ops, current.Version()) {
			if err := c.store.PutRelVersion(id, current.Version(), current); err != nil {
				return nil, fmt.Errorf("graph: cascade archive rel current: %w", err)
			}
		}
		if err := c.store.ReplaceRelationship(newVer); err != nil {
			return nil, fmt.Errorf("graph: cascade replace rel: %w", err)
		}
	} else {
		if err := c.store.PutRelVersion(id, newVer.Version(), newVer); err != nil {
			return nil, fmt.Errorf("graph: cascade put new rel history: %w", err)
		}
	}

	c.opRelUpdates.Add(1)
	return newVer, nil
}

func cascadeModifiedRelVersion(ops []relCascadeOp, v uint32) bool {
	for _, op := range ops {
		if op.modified != nil && op.origVersion == v {
			return true
		}
	}
	return false
}
