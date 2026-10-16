package core

import (
	"cmp"
	"errors"
	"slices"
	"sort"

	snowflake "github.com/bds421/rho-snowflake-2026"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// =============================================================================
// Label and type reads at a temporal coordinate (sigma-tkgd round 6 request
// 12; EnvelopePruneLarge: 5.02 of 5.02 s in ByLabel at a valid-time instant,
// each candidate read a second time through GetNode and its version
// resolved, then the materialized slice sorted).
//
// The candidates of such a read (nodeLabelCandidatesFrom, relTypeCandidatesFrom)
// come in ascending ID order, as the label's current rows do, so one forward
// walk pairs a candidate with the row the label scan already read. A
// candidate is then answered, in this order, by
//   - its current row where that row alone decides (nodeCurrentDecides, the
//     shortcut CountByLabelAt always used),
//   - its current row as its only version where the store states it has no
//     history rows (singleVersionNodes; round 5 request 6's adjacency rule),
//   - the chain resolver's own door, findNodeVersionForOpts, otherwise.
// The answers are the resolver's (TestLabelReadAtAgreesWithTheResolver); the
// order is ascending ID with or without NoSort, so nothing is sorted after.
// ForEachByLabel / ForEachByType resolve labelScanAtChunk candidates per hold
// of the graph read lock and call fn with no lock held.
// =============================================================================

// labelScanAtChunk is how many candidates a streaming read at a coordinate
// resolves per hold of the graph read lock.
const labelScanAtChunk = 1024

// sortedCandidates returns current ∪ extra in ascending order without
// duplicates: current is the label's (type's) current member list, extra the
// IDs the history gather adds. Neither needs to be sorted; a sorted current
// list (the stores' order) is not copied when extra adds nothing.
func sortedCandidates[T cmp.Ordered](current, extra []T) []T {
	if !slices.IsSorted(current) {
		current = slices.Sorted(slices.Values(current))
	}
	if len(extra) == 0 {
		return current
	}
	slices.Sort(extra)
	extra = slices.Compact(extra)
	out := make([]T, 0, len(current)+len(extra))
	i, j := 0, 0
	for i < len(current) || j < len(extra) {
		switch {
		case j == len(extra) || (i < len(current) && current[i] < extra[j]):
			out = append(out, current[i])
			i++
		case i == len(current) || extra[j] < current[i]:
			out = append(out, extra[j])
			j++
		default: // equal
			out = append(out, current[i])
			i++
			j++
		}
	}
	return out
}

// inSorted reports whether x is in the ascending slice s.
func inSorted[T cmp.Ordered](s []T, x T) bool {
	_, ok := slices.BinarySearch(s, x)
	return ok
}

// skipAfter drops the candidates at or below a keyset cursor (ascending ids).
func skipAfter[T interface{ SnowflakeID() snowflake.ID }](ids []T, after types.EntityID) []T {
	a := after.SnowflakeID()
	if a <= 0 {
		return ids
	}
	return ids[sort.Search(len(ids), func(i int) bool { return ids[i].SnowflakeID() > a }):]
}

// -----------------------------------------------------------------------------
// Nodes
// -----------------------------------------------------------------------------

// nodeLabelScanAt answers the candidates of one label read at a temporal
// coordinate in ascending ID order. Callers hold c.mu (R or W) around each
// resolve.
type nodeLabelScanAt struct {
	c       *Core
	opts    storepkg.QueryOpts // normalized (normalizeTxAtOnlyOpts)
	pred    func(*types.Node) bool
	single  singleVersionNodeResolver
	current []*types.Node // the label's current rows, ascending ID
	next    int
}

func (c *Core) newNodeLabelScanAt(tok uint16, current []*types.Node, opts storepkg.QueryOpts) *nodeLabelScanAt {
	byID := func(a, b *types.Node) int { return cmp.Compare(a.ID(), b.ID()) }
	if !slices.IsSortedFunc(current, byID) {
		current = slices.Clone(current)
		slices.SortFunc(current, byID)
	}
	resolveOpts := c.normalizeTxAtOnlyOpts(opts)
	return &nodeLabelScanAt{
		c:       c,
		opts:    resolveOpts,
		pred:    func(n *types.Node) bool { return n.HasLabelTokenRaw(tok) },
		single:  c.singleVersionNodes(resolveOpts),
		current: current,
	}
}

// currentRow is the label's current row of id (nil if none); ids must be
// asked in ascending order.
func (s *nodeLabelScanAt) currentRow(id types.NodeID) *types.Node {
	for s.next < len(s.current) && s.current[s.next].ID() < id {
		s.next++
	}
	if s.next < len(s.current) && s.current[s.next].ID() == id {
		return s.current[s.next]
	}
	return nil
}

// resolve returns the version of id the read answers, nil for none.
func (s *nodeLabelScanAt) resolve(id types.NodeID) (*types.Node, error) {
	if cur := s.currentRow(id); cur != nil {
		decided, matched, err := s.c.nodeCurrentDecides(id, cur, s.opts, s.pred)
		if err != nil {
			return nil, err
		}
		if decided {
			if !matched {
				return nil, nil
			}
			return s.c.outNode(cur), nil
		}
		n, decided, err := s.single.resolve(cur, s.pred)
		if err != nil {
			return nil, err
		}
		if decided {
			return n, nil
		}
	}
	n, err := s.c.findNodeVersionForOpts(id, s.opts, s.pred)
	if err != nil {
		if errors.Is(err, storepkg.ErrNoVersionValidAt) || errors.Is(err, storepkg.ErrNodeNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return n, nil
}

// outNode is a row of the store as a door hands it out: an untrusted store's
// rows leave the core as copies (store_fetch.go).
func (c *Core) outNode(n *types.Node) *types.Node {
	if !c.storeRowsTrust {
		return n.DeepCopy()
	}
	return n
}

func (c *Core) outRel(r *types.Relationship) *types.Relationship {
	if !c.storeRowsTrust {
		return r.DeepCopy()
	}
	return r
}

// singleVersionNodeResolver is singleVersionAdjacencyResolver for nodes: a
// live node the store states has no history rows has exactly one version,
// its current row, so the chain resolver over that one-row chain gives what
// findNodeVersionForOpts gives after reading it again. Only under a pure
// valid-time filter (ValidAt, or ValidStart and ValidEnd); a
// transaction-time pin keeps the full path (compaction and retention
// watermarks). The presence check runs after the row was read: a write that
// replaced or deleted the node since left a history row, so the check sends
// it to the full path.
type singleVersionNodeResolver struct {
	c        *Core
	presence storepkg.HistoryPresenceCapability
	probe    chainProbe
}

func (c *Core) singleVersionNodes(opts storepkg.QueryOpts) singleVersionNodeResolver {
	probe, ok := pureValidTimeProbe(opts)
	if !ok {
		return singleVersionNodeResolver{}
	}
	presence, ok := c.store.(storepkg.HistoryPresenceCapability)
	if !ok {
		return singleVersionNodeResolver{}
	}
	return singleVersionNodeResolver{c: c, presence: presence, probe: probe}
}

// pureValidTimeProbe is the probe of a read whose only filter is valid time.
func pureValidTimeProbe(opts storepkg.QueryOpts) (chainProbe, bool) {
	if opts.HasTxAt() || opts.HasTxPin() {
		return chainProbe{}, false
	}
	switch {
	case opts.HasValidAt():
		return chainProbe{kind: probePoint, validAt: opts.ValidAt}, true
	case opts.HasValidInterval():
		return chainProbe{kind: probeInterval, validStart: opts.ValidStart, validEnd: opts.ValidEnd}, true
	}
	return chainProbe{}, false
}

// resolve reports decided=false when cur must take the full path (no
// capability, or a node with history); decided=true with n == nil when the
// one version does not answer the probe or fails pred.
func (s singleVersionNodeResolver) resolve(cur *types.Node, pred func(*types.Node) bool) (n *types.Node, decided bool, err error) {
	if s.presence == nil {
		return nil, false, nil
	}
	has, err := s.presence.HasNodeHistory(cur.ID())
	if err != nil {
		return nil, false, err
	}
	if has {
		return nil, false, nil
	}
	if covered, ok := s.firstVersionCovers(cur); ok {
		if !covered || !pred(cur) {
			return nil, true, nil
		}
		return s.c.outNode(cur), true, nil
	}
	n, err = s.c.resolveNodeChain([]*types.Node{cur}, s.probe, pred)
	if err != nil {
		if errors.Is(err, storepkg.ErrNoVersionValidAt) {
			return nil, true, nil
		}
		return nil, false, err
	}
	if !pred(n) {
		return nil, true, nil
	}
	if n == cur {
		n = s.c.outNode(cur)
	}
	return n, true, nil
}

// firstVersionCovers is singleVersionAdjacencyResolver.firstVersionCovers for
// a node: a first version (version 0) without a delete stamp answers the
// probe by its bounds [effective valid-from (storeutil.NodeValidFrom), valid-to or open).
// ok=false for any other row: the caller runs the resolver.
// TestSingleVersionNodeMatchesResolver holds the two equal.
func (s singleVersionNodeResolver) firstVersionCovers(n *types.Node) (covered, ok bool) {
	if n.Version() != 0 {
		return false, false
	}
	if _, _, deletedAt := n.TxStamps(); deletedAt != 0 {
		return false, false
	}
	_, vEnd, _ := n.ValidRange()
	vStart := storeutil.NodeValidFrom(n)
	switch s.probe.kind {
	case probePoint:
		t := s.probe.validAt
		return vStart <= t && (vEnd == 0 || vEnd > t), true
	case probeInterval:
		return vStart < s.probe.validEnd && (vEnd == 0 || vEnd > s.probe.validStart), true
	}
	return false, false
}

// nodesByLabelTemporalLocked is nodesByLabelLocked under a temporal filter: every
// answer in ascending ID order, paginated. Callers hold c.mu.
func (c *Core) nodesByLabelTemporalLocked(tok uint16, opts storepkg.QueryOpts) ([]*types.Node, error) {
	current, candIDs, err := c.nodeLabelCandidatesAt(tok, opts)
	if err != nil {
		return nil, err
	}
	candIDs = skipAfter(candIDs, opts.After)
	scan := c.newNodeLabelScanAt(tok, current, opts)
	var result []*types.Node
	for _, id := range candIDs {
		n, err := scan.resolve(id)
		if err != nil {
			return nil, err
		}
		if n == nil {
			continue
		}
		result = append(result, n)
		if opts.Limit > 0 && len(result) == opts.Limit {
			break
		}
	}
	return result, nil
}

// forEachNodeByLabelAt is ForEachByLabel under a temporal filter: the
// candidates gathered under one hold of the read lock, then resolved
// labelScanAtChunk at a time, each chunk under its own hold, its answers
// handed to fn with no lock held, in ascending ID order. A node is answered
// as of a moment between the call and its chunk's resolution.
func (c *Core) forEachNodeByLabelAt(label string, opts storepkg.QueryOpts, fn func(*types.Node) bool) error {
	var scan *nodeLabelScanAt
	var candIDs []types.NodeID
	if err := c.readUnderRLock(func() error {
		tok, ok := c.labels.Lookup(label)
		if !ok {
			return nil
		}
		current, ids, err := c.nodeLabelCandidatesAt(tok, opts)
		if err != nil {
			return err
		}
		candIDs = skipAfter(ids, opts.After)
		scan = c.newNodeLabelScanAt(tok, current, opts)
		return nil
	}); err != nil || scan == nil {
		return err
	}
	emitted := 0
	buf := make([]*types.Node, 0, min(labelScanAtChunk, len(candIDs)))
	for start := 0; start < len(candIDs); start += labelScanAtChunk {
		chunk := candIDs[start:min(start+labelScanAtChunk, len(candIDs))]
		buf = buf[:0]
		if err := c.readUnderRLock(func() error {
			for _, id := range chunk {
				n, err := scan.resolve(id)
				if err != nil {
					return err
				}
				if n != nil {
					buf = append(buf, n)
				}
			}
			return nil
		}); err != nil {
			return err
		}
		for _, n := range buf {
			if !fn(n) {
				return nil
			}
			emitted++
			if opts.Limit > 0 && emitted == opts.Limit {
				return nil
			}
		}
	}
	return nil
}

// -----------------------------------------------------------------------------
// Relationships (the mirror)
// -----------------------------------------------------------------------------

// relTypeScanAt is nodeLabelScanAt for a relationship type.
type relTypeScanAt struct {
	c       *Core
	opts    storepkg.QueryOpts
	pred    func(*types.Relationship) bool
	single  singleVersionAdjacencyResolver
	current []*types.Relationship
	next    int
}

func (c *Core) newRelTypeScanAt(tok uint16, current []*types.Relationship, opts storepkg.QueryOpts) *relTypeScanAt {
	byID := func(a, b *types.Relationship) int { return cmp.Compare(a.ID(), b.ID()) }
	if !slices.IsSortedFunc(current, byID) {
		current = slices.Clone(current)
		slices.SortFunc(current, byID)
	}
	resolveOpts := c.normalizeTxAtOnlyOpts(opts)
	return &relTypeScanAt{
		c:       c,
		opts:    resolveOpts,
		pred:    func(r *types.Relationship) bool { return r.HasTypeTokenRaw(tok) },
		single:  c.singleVersionAdjacency(resolveOpts),
		current: current,
	}
}

func (s *relTypeScanAt) currentRow(id types.RelID) *types.Relationship {
	for s.next < len(s.current) && s.current[s.next].ID() < id {
		s.next++
	}
	if s.next < len(s.current) && s.current[s.next].ID() == id {
		return s.current[s.next]
	}
	return nil
}

func (s *relTypeScanAt) resolve(id types.RelID) (*types.Relationship, error) {
	if cur := s.currentRow(id); cur != nil {
		decided, matched, err := s.c.relCurrentDecides(id, cur, s.opts, s.pred)
		if err != nil {
			return nil, err
		}
		if decided {
			if !matched {
				return nil, nil
			}
			return s.c.outRel(cur), nil
		}
		r, decided, err := s.single.resolve(cur, s.pred)
		if err != nil {
			return nil, err
		}
		if decided {
			if r != nil && !s.pred(r) {
				return nil, nil
			}
			return r, nil
		}
	}
	r, err := s.c.findRelVersionForOpts(id, s.opts, s.pred)
	if err != nil {
		if errors.Is(err, storepkg.ErrNoVersionValidAt) || errors.Is(err, storepkg.ErrRelNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return r, nil
}

// relsByTypeTemporalLocked is nodesByLabelTemporalLocked for a relationship type.
func (c *Core) relsByTypeTemporalLocked(tok uint16, opts storepkg.QueryOpts) ([]*types.Relationship, error) {
	current, candIDs, err := c.relTypeCandidatesAt(tok, opts)
	if err != nil {
		return nil, err
	}
	candIDs = skipAfter(candIDs, opts.After)
	scan := c.newRelTypeScanAt(tok, current, opts)
	var result []*types.Relationship
	for _, id := range candIDs {
		r, err := scan.resolve(id)
		if err != nil {
			return nil, err
		}
		if r == nil {
			continue
		}
		result = append(result, r)
		if opts.Limit > 0 && len(result) == opts.Limit {
			break
		}
	}
	return result, nil
}

// forEachRelByTypeAt is forEachNodeByLabelAt for a relationship type.
func (c *Core) forEachRelByTypeAt(typeName string, opts storepkg.QueryOpts, fn func(*types.Relationship) bool) error {
	var scan *relTypeScanAt
	var candIDs []types.RelID
	if err := c.readUnderRLock(func() error {
		tok, ok := c.lookupRelTypeQueryToken(typeName)
		if !ok {
			return nil
		}
		current, ids, err := c.relTypeCandidatesAt(tok, opts)
		if err != nil {
			return err
		}
		candIDs = skipAfter(ids, opts.After)
		scan = c.newRelTypeScanAt(tok, current, opts)
		return nil
	}); err != nil || scan == nil {
		return err
	}
	emitted := 0
	buf := make([]*types.Relationship, 0, min(labelScanAtChunk, len(candIDs)))
	for start := 0; start < len(candIDs); start += labelScanAtChunk {
		chunk := candIDs[start:min(start+labelScanAtChunk, len(candIDs))]
		buf = buf[:0]
		if err := c.readUnderRLock(func() error {
			for _, id := range chunk {
				r, err := scan.resolve(id)
				if err != nil {
					return err
				}
				if r != nil {
					buf = append(buf, r)
				}
			}
			return nil
		}); err != nil {
			return err
		}
		for _, r := range buf {
			if !fn(r) {
				return nil
			}
			emitted++
			if opts.Limit > 0 && emitted == opts.Limit {
				return nil
			}
		}
	}
	return nil
}
