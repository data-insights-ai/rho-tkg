package core

import (
	"slices"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// =============================================================================
// Corrections on a head miss from history skeletons (CASCADEMETA, sigma's
// RELHIST profile, 3 Nov)
//
// Without a valid head entry (none kept: a chain with a delete, a retraction
// or a shared version never qualifies, gatherHeadFacts) a correction plans
// on the whole pre-correction chain. The plan reads of each row only its
// version and temporal block (the highest version, the order rule's stamps,
// the resolver's order and intervals, every cut, the resumption end, the
// void rows, the newest own-open belief, the head entry the plan leaves
// behind); full content is read only for the rows a correction copies —
// the base of each non-gap segment and the resumption source when a
// resumption is written — and the current row, which the correction holds
// already. So on a store with
// skeleton reads (store.TemporalMetaHistoryCapability: memory, badger,
// tiered) the miss plans on skeletons built as nodeEffectiveViaTemporalMeta
// builds them (version and temporal block only), runs every planning step
// unchanged on them, and then swaps each skeleton the plan hands on for its
// full row by a point read (missPlan hydrate), before any row is copied,
// hashed, compared or written: O(distinct winners) full rows instead of
// O(versions).
//
// The plan falls back to the whole history when the skeletons cannot stand
// for the rows: versions not strictly ascending, or one equal to the current
// row's (a chain stored before backlog 38, where the current row shares a
// version with a history row and a point read could not tell them apart), a
// skeleton read that fails, or a point read that fails or finds nothing (a
// compaction between the two reads). sharded has no skeleton door on the
// store (only on its read snapshot) and keeps the whole-history read. The
// twin tests (cascade_meta_test.go) run both on twin graphs.
// =============================================================================

// missPlan is a correction planned on the pre-correction chain (a head
// miss): the chain facts, the chain and its preparation, and the order
// rule's stamps.
type missPlan[T chainRow] struct {
	hc       headChain[T]
	preChain []T
	prep     *correctionChain[T]
	stamps   []*types.TemporalMetadata
}

// planMiss plans a correction of [newVF, newVT) on history ‖ current
// (current non-nil): what the cascade kernels compute on a head miss.
// resumptionEnd is nodeResumptionEnd or relResumptionEnd.
func planMiss[T chainRow](c *Core, k *chainKernel[T], history []T, current T, newVF, newVT types.Instant, splitGaps bool,
	resumptionEnd func(*Core, []T, types.Instant) types.Instant) missPlan[T] {
	p := missPlan[T]{stamps: temporalsOf(history, current)}
	p.hc.maxVersion = current.Version()
	for _, h := range history {
		p.hc.maxVersion = max(p.hc.maxVersion, h.Version())
	}
	p.preChain = preCorrectionChain(history, current)
	p.prep = prepareCorrection(c, k, p.preChain)
	if !c.axis().Open(newVT) {
		if src, ok := p.prep.winnerAt(newVT); ok {
			p.hc.src, p.hc.hasSrc, p.hc.srcEnd = src, true, resumptionEnd(c, p.preChain, newVT)
		}
	}
	// The template — the current row — is the content of a gap piece.
	p.hc.segments = p.prep.segments(current, newVF, newVT, splitGaps)
	p.hc.voidFree = !slices.ContainsFunc(p.preChain, isVoidRow[T])
	return p
}

// hydrate swaps every row the plan hands on to the row builders — the
// resumption source when one is copied (srcCopied false: it starts at newVT,
// the kernels write no resumption and read only its start) and each non-gap
// segment base — for its full row, read by version once each (current
// stands for itself; a gap's base is current). False when a read fails or
// finds nothing: the caller plans on the whole history.
func (p *missPlan[T]) hydrate(current T, srcCopied bool, read func(uint32) (T, error)) bool {
	var zero T
	rows := make(map[T]T, len(p.hc.segments)+1)
	full := func(s T) (T, bool) {
		if s == current {
			return current, true
		}
		if r, ok := rows[s]; ok {
			return r, true
		}
		r, err := read(s.Version())
		if err != nil || r == zero || r.Version() != s.Version() {
			return zero, false
		}
		rows[s] = r
		return r, true
	}
	if p.hc.hasSrc && srcCopied {
		src, ok := full(p.hc.src)
		if !ok {
			return false
		}
		p.hc.src = src
	}
	for i := range p.hc.segments {
		if p.hc.segments[i].gap {
			continue
		}
		base, ok := full(p.hc.segments[i].base)
		if !ok {
			return false
		}
		p.hc.segments[i].base = base
	}
	return true
}

// skeletonsAscending reports whether metas can stand for the history rows:
// versions strictly ascending and none equal to the current row's.
func skeletonsAscending(metas []storepkg.VersionTemporalMeta, current uint32) bool {
	for i, m := range metas {
		if m.Version == current || (i > 0 && m.Version <= metas[i-1].Version) {
			return false
		}
	}
	return true
}

// nodeMissPlan plans a node correction on a head miss: on skeletons when the
// store offers them (nodeMetaPlan), else on the whole history, refusing an
// unknown or hard-deleted node.
func (c *Core) nodeMissPlan(id types.NodeID, current *types.Node, newVF, newVT types.Instant, splitGaps bool) (missPlan[*types.Node], error) {
	c.headStats.missPlans.Add(1)
	if p, ok := c.nodeMetaPlan(id, current, newVF, newVT, splitGaps); ok {
		return p, nil
	}
	history, err := c.getNodeHistory(id)
	if err != nil {
		return missPlan[*types.Node]{}, err
	}
	if current == nil && len(history) == 0 {
		return missPlan[*types.Node]{}, storepkg.ErrNodeNotFound
	}
	if current == nil {
		// Hard-deleted: the chain ends in a tombstone. A correction appended
		// after it would be a belief no door reads consistently (backlog 14).
		return missPlan[*types.Node]{}, nodeDeletedErr(id)
	}
	return planMiss(c, nodeKernel, history, current, newVF, newVT, splitGaps, nodeResumptionEnd), nil
}

// nodeMetaPlan is the plan on skeletons, ok false when it must not stand
// (see the section comment).
func (c *Core) nodeMetaPlan(id types.NodeID, current *types.Node, newVF, newVT types.Instant, splitGaps bool) (missPlan[*types.Node], bool) {
	if c.temporalMetaHistory == nil || c.cascadeMetaOff || current == nil {
		return missPlan[*types.Node]{}, false
	}
	metas, err := c.temporalMetaHistory.NodeHistoryTemporalMeta(id)
	if err == nil {
		c.noteNodeMeta(len(metas))
		if c.skeletonRead != nil {
			c.skeletonRead(len(metas))
		}
	}
	if err != nil || !skeletonsAscending(metas, current.Version()) {
		return missPlan[*types.Node]{}, false
	}
	stubs := make([]*types.Node, 0, len(metas))
	for _, m := range metas {
		s := types.NewNode(id, 0, nil)
		s.SetVersion(m.Version)
		if m.Temporal != nil {
			s.SetTemporal(m.Temporal)
		}
		stubs = append(stubs, s)
	}
	p := planMiss(c, nodeKernel, stubs, current, newVF, newVT, splitGaps, nodeResumptionEnd)
	if c.cascadeMetaHook != nil {
		c.cascadeMetaHook()
	}
	srcCopied := p.hc.hasSrc && !c.nodeStartsAt(p.hc.src, newVT) // the kernel's RESUME test
	if !p.hydrate(current, srcCopied, func(v uint32) (*types.Node, error) { return c.getNodeVersion(id, v) }) {
		return missPlan[*types.Node]{}, false
	}
	c.metaWrites.Add(1)
	return p, true
}

// relMissPlan mirrors nodeMissPlan for relationships.
func (c *Core) relMissPlan(id types.RelID, current *types.Relationship, newVF, newVT types.Instant, splitGaps bool) (missPlan[*types.Relationship], error) {
	if p, ok := c.relMetaPlan(id, current, newVF, newVT, splitGaps); ok {
		return p, nil
	}
	history, err := c.getRelHistory(id)
	if err != nil {
		return missPlan[*types.Relationship]{}, err
	}
	if current == nil && len(history) == 0 {
		return missPlan[*types.Relationship]{}, storepkg.ErrRelNotFound
	}
	if current == nil {
		// Hard-deleted — see nodeMissPlan (backlog 14).
		return missPlan[*types.Relationship]{}, relDeletedErr(id)
	}
	return planMiss(c, relKernel, history, current, newVF, newVT, splitGaps, relResumptionEnd), nil
}

// relMetaPlan mirrors nodeMetaPlan for relationships.
func (c *Core) relMetaPlan(id types.RelID, current *types.Relationship, newVF, newVT types.Instant, splitGaps bool) (missPlan[*types.Relationship], bool) {
	if c.temporalMetaHistory == nil || c.cascadeMetaOff || current == nil {
		return missPlan[*types.Relationship]{}, false
	}
	metas, err := c.temporalMetaHistory.RelHistoryTemporalMeta(id)
	if c.skeletonRead != nil && err == nil {
		c.skeletonRead(len(metas))
	}
	if err != nil || !skeletonsAscending(metas, current.Version()) {
		return missPlan[*types.Relationship]{}, false
	}
	stubs := make([]*types.Relationship, 0, len(metas))
	for _, m := range metas {
		s := types.NewRelationship(id, 0, 0, 0)
		s.SetVersion(m.Version)
		if m.Temporal != nil {
			s.SetTemporal(m.Temporal)
		}
		stubs = append(stubs, s)
	}
	p := planMiss(c, relKernel, stubs, current, newVF, newVT, splitGaps, relResumptionEnd)
	if c.cascadeMetaHook != nil {
		c.cascadeMetaHook()
	}
	srcCopied := p.hc.hasSrc && !c.relStartsAt(p.hc.src, newVT) // the kernel's RESUME test
	if !p.hydrate(current, srcCopied, func(v uint32) (*types.Relationship, error) { return c.getRelVersion(id, v) }) {
		return missPlan[*types.Relationship]{}, false
	}
	c.metaWrites.Add(1)
	return p, true
}
