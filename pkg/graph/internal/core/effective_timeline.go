package core

import (
	"cmp"
	"errors"
	"fmt"
	"slices"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// =============================================================================
// Effective timeline (handover effective-read-cost fix 1c, backlog 27)
//
// NodeEffectiveTimeline(id, pin) answers, in one call, what NodeAtTx(id, t,
// pin) answers for every valid instant t: the entity's state over valid time
// as recorded at the pin, as half-open segments. It is the state door's
// interval form, not the record door's: NodeAsOf answers the newest row
// recorded by a pin; NodeAtTx / the timeline answer the row valid at t as
// believed at the pin.
//
// Cut-and-resolve (the cascade kernel's nodeCorrectionSegments idea, applied
// to reads): the point resolver's answer over a TxAt-filtered chain is
// piecewise constant between the chain's bounds — every row's own and
// positional [vStart, vEnd), its life end (lifeEnds) and its supersession end
// (supersessionEnds). The chain is loaded once (selection skeletons where the
// store offers them), cut at those bounds, and each piece is resolved by THE
// point resolver (resolveNodeVersionAtCapped, on its own copy of the chain —
// lesson 73) at its start; pieces with the same winner merge, pieces with no
// winner are gaps. Only the winners are decoded. Pointwise equality with
// NodeAtTx therefore holds by construction, and the oracle test checks it.
// =============================================================================

// ErrTxPinTooNew is returned by the effective-timeline doors when the pin is
// ahead of the commit clock (above PeekTx): no write, NowTx or CommittedTx
// has handed it out yet, so a write stamped at or below it may still land and
// the answer would not be repeatable.
var ErrTxPinTooNew = errors.New("graph: transaction-time pin is ahead of the commit clock")

// validateEffectivePin checks a timeline pin: positive, not ahead of the
// commit clock.
func (c *Core) validateEffectivePin(pin types.Instant) error {
	if pin <= 0 {
		return fmt.Errorf("%w: transaction-time pin %d must be positive", ErrInvalidTimeRange, pin)
	}
	if now := c.peekNow(); pin > now {
		return fmt.Errorf("%w: pin %d, commit clock %d", ErrTxPinTooNew, pin, now)
	}
	return nil
}

// effPiece is one resolved piece [from, to) (to == 0: open) of a timeline;
// src is the index, in the chain handed to effectivePieces, of the row the
// winner came from (the winner may be the resolver's normalized copy of it).
type effPiece[T any] struct {
	from, to types.Instant
	row      T
	src      int
}

// chainKernel is the node or relationship arm of the point resolver: the
// pieces resolveNodeVersionAtCapped (resolveRelVersionAtCapped) selects with.
type chainKernel[T interface {
	comparable
	storeutil.TemporalRow
}] struct {
	rowAtTx   func(T, types.Instant) (T, bool)
	sortVF    func(T) types.Instant
	sortChain func([]T) bool
	ownBounds func(T) (types.Instant, types.Instant)
	posBounds func([]T, int) (types.Instant, types.Instant)
}

// effInterval is one row's covering interval [s, e) (e == 0: open) in the
// arm the resolver takes, with its selection priority (higher wins).
type effInterval struct {
	s, e types.Instant
	pos  int // position in the resolver-ordered chain
	prio int
}

// effectivePieces answers the point resolver for every valid instant at once:
// nil when nothing was recorded by the pin.
//
// The resolver's input is filterNodeChainByTxAt(versionOrdered(chain), pin)
// with its lifeEnds; built here row by row so each kept row remembers where it
// came from. The resolver then classifies the chain once
// (sortNodeChainForResolve) and, in either arm, answers at t the covering row
// of highest priority: the monotonic arm the highest position whose positional
// [vStart, vEnd) covers t, the own-bounds arm the newest belief whose own
// interval, capped by supersessionCaps, covers t (both capped by the life
// end). Those intervals are computed once here with the resolver's own bounds
// functions, and one sweep over their bounds with a max-heap by priority yields
// every piece: O(n log n) for a chain of n rows. Pieces with the same winner
// merge; instants no interval covers are gaps.
func effectivePieces[T interface {
	comparable
	storeutil.TemporalRow
}](k *chainKernel[T], chain []T, pin types.Instant) []effPiece[T] {
	order := make([]int, len(chain))
	for i := range order {
		order[i] = i
	}
	byWrite := chainWriteOrder(chain)
	slices.SortStableFunc(order, func(a, b int) int { return byWrite(chain[a], chain[b]) })
	filtered := make([]T, 0, len(chain))
	origin := make(map[T]int, len(chain))
	for _, i := range order {
		if r, ok := k.rowAtTx(chain[i], pin); ok {
			filtered = append(filtered, r)
			origin[r] = i
		}
	}
	if len(filtered) == 0 {
		return nil
	}
	caps := chainLifeEnds(filtered)

	// The resolver's classification and order (it sorts its argument).
	arr := slices.Clone(filtered)
	cascade := k.sortChain(arr)
	var superseded []types.Instant
	if cascade {
		sc := getSupersessionScratch()
		defer putSupersessionScratch(sc)
		superseded = supersessionCaps(arr, k.sortVF, sc)
	}
	ivs := make([]effInterval, 0, len(arr))
	for i, r := range arr {
		var s, e types.Instant
		if cascade {
			s, e = k.ownBounds(r)
			if superseded != nil {
				e = capEnd(e, superseded[i])
			}
		} else {
			s, e = k.posBounds(arr, i)
		}
		e = caps.end(r, e)
		if e != 0 && e <= s {
			continue // covers nothing
		}
		ivs = append(ivs, effInterval{s: s, e: e, pos: i, prio: i})
	}
	if cascade {
		// Newest belief wins: (TxFrom, version); among equals the earlier
		// position (the resolver keeps the first it meets).
		rank := make([]int, len(ivs))
		for i := range rank {
			rank[i] = i
		}
		slices.SortFunc(rank, func(a, b int) int {
			ra, rb := arr[ivs[a].pos], arr[ivs[b].pos]
			if c := cmp.Compare(beliefTx(ra.Temporal()), beliefTx(rb.Temporal())); c != 0 {
				return c
			}
			if c := cmp.Compare(ra.Version(), rb.Version()); c != 0 {
				return c
			}
			return cmp.Compare(ivs[b].pos, ivs[a].pos)
		})
		for p, i := range rank {
			ivs[i].prio = p
		}
	}

	cuts := make([]types.Instant, 0, 2*len(ivs))
	for _, iv := range ivs {
		cuts = append(cuts, iv.s)
		if iv.e != 0 {
			cuts = append(cuts, iv.e)
		}
	}
	slices.Sort(cuts)
	cuts = slices.Compact(cuts)
	slices.SortFunc(ivs, func(a, b effInterval) int { return cmp.Compare(a.s, b.s) })

	var out []effPiece[T]
	h := effHeap{}
	next := 0
	for j, from := range cuts {
		for next < len(ivs) && ivs[next].s <= from {
			h.push(ivs[next])
			next++
		}
		for len(h) > 0 && h[0].e != 0 && h[0].e <= from {
			h.pop()
		}
		if len(h) == 0 {
			continue // a gap
		}
		var to types.Instant
		if j+1 < len(cuts) {
			to = cuts[j+1]
		}
		w := arr[h[0].pos]
		if n := len(out); n > 0 && out[n-1].row == w && out[n-1].to == from {
			out[n-1].to = to
			continue
		}
		out = append(out, effPiece[T]{from: from, to: to, row: w, src: origin[w]})
	}
	return out
}

// effHeap is a max-heap of intervals by priority.
type effHeap []effInterval

func (h *effHeap) push(iv effInterval) {
	*h = append(*h, iv)
	a := *h
	for i := len(a) - 1; i > 0; {
		p := (i - 1) / 2
		if a[p].prio >= a[i].prio {
			break
		}
		a[p], a[i] = a[i], a[p]
		i = p
	}
}

func (h *effHeap) pop() {
	a := *h
	n := len(a) - 1
	a[0] = a[n]
	a = a[:n]
	for i := 0; ; {
		l, r, m := 2*i+1, 2*i+2, i
		if l < n && a[l].prio > a[m].prio {
			m = l
		}
		if r < n && a[r].prio > a[m].prio {
			m = r
		}
		if m == i {
			break
		}
		a[i], a[m] = a[m], a[i]
		i = m
	}
	*h = a
}

func (c *Core) nodeKernel() *chainKernel[*types.Node] {
	return &chainKernel[*types.Node]{
		rowAtTx:   nodeRowAtTx,
		sortVF:    c.nodeSortValidFrom,
		sortChain: c.sortNodeChainForResolve,
		ownBounds: c.nodeOwnBounds,
		posBounds: c.nodeVersionBounds,
	}
}

func (c *Core) relKernel() *chainKernel[*types.Relationship] {
	return &chainKernel[*types.Relationship]{
		rowAtTx:   relRowAtTx,
		sortVF:    c.relSortValidFrom,
		sortChain: c.sortRelChainForResolve,
		ownBounds: c.relOwnBounds,
		posBounds: c.relVersionBounds,
	}
}

func (c *Core) nodeEffectivePieces(chain []*types.Node, pin types.Instant) []effPiece[*types.Node] {
	return effectivePieces(c.nodeKernel(), chain, pin)
}

func (c *Core) relEffectivePieces(chain []*types.Relationship, pin types.Instant) []effPiece[*types.Relationship] {
	return effectivePieces(c.relKernel(), chain, pin)
}

// NodeEffectiveTimeline returns node id's state over valid time as recorded at
// transaction-time pin: segments ascending by ValidFrom, non-overlapping,
// half-open [ValidFrom, ValidTo) (ValidTo 0 = open), gaps omitted, adjacent
// segments holding different rows. For every valid instant t the segment
// containing t holds the row NodeAtTx(id, t, pin) returns, and no segment
// contains t when NodeAtTx answers ErrNoVersionValidAt. ValidFrom is the
// effective start, never 0 (a row without a recorded valid-from starts at the
// ID's mint instant, or at its UpdatedAt for a later version); a deleted
// node's last segment ends at the delete instant.
//
// Rows are shared frozen pointers (DeepCopy to mutate). A node created after
// the pin: nil, nil. Errors: ErrGraphClosed; an invalid ID
// (ErrInvalidStoreMutation); pin <= 0 (ErrInvalidTimeRange); a pin above the
// commit clock (ErrTxPinTooNew); a pin below compacted knowledge
// (ErrHistoryCompacted) or a retention watermark (ErrRetentionExpired), as
// NodeAtTx; an unknown ID (ErrNodeNotFound).
//
// Cost: an entity without history reads its current row and the history
// presence bit; one with history reads per-version temporal skeletons where
// the store offers them (badger) and decodes only the rows that answer.
func (t *TempOps) NodeEffectiveTimeline(id types.NodeID, pin types.Instant) ([]temporal.NodeSegment, error) {
	c := t.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if err := storepkg.ValidateNodeID(id); err != nil {
		return nil, err
	}
	if err := c.validateEffectivePin(pin); err != nil {
		return nil, err
	}
	var out []temporal.NodeSegment
	err := c.readUnderRLock(func() error {
		var err error
		out, err = c.nodeEffectiveTimelineLocked(id, pin)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// RelEffectiveTimeline is NodeEffectiveTimeline for relationships (the row
// RelAtTx(id, t, pin) returns; ErrRelNotFound for an unknown ID). It is a
// DECLARED view: the relationship's own validity, not masked by its
// endpoints'.
func (t *TempOps) RelEffectiveTimeline(id types.RelID, pin types.Instant) ([]temporal.RelSegment, error) {
	c := t.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if err := storepkg.ValidateRelID(id); err != nil {
		return nil, err
	}
	if err := c.validateEffectivePin(pin); err != nil {
		return nil, err
	}
	var out []temporal.RelSegment
	err := c.readUnderRLock(func() error {
		var err error
		out, err = c.relEffectiveTimelineLocked(id, pin)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// nodeEffectiveTimelineLocked is the per-entity body (c.mu held).
func (c *Core) nodeEffectiveTimelineLocked(id types.NodeID, pin types.Instant) ([]temporal.NodeSegment, error) {
	// The entity lock makes the current row, the presence bit and the
	// history one snapshot: a concurrent write (Update, Delete, CloseVersion)
	// moves the current row into history in more than one store step, and a
	// read between them saw neither (the entity read as unknown).
	c.entityLocks.LockEntity(id.SnowflakeID())
	defer c.entityLocks.UnlockEntity(id.SnowflakeID())
	if err := c.checkNodePointCompaction(id, pin); err != nil {
		return nil, err
	}
	if err := c.checkNodePointRetention(id, pin); err != nil {
		return nil, err
	}
	current, err := c.getCurrentNode(id)
	if err != nil && !errors.Is(err, storepkg.ErrNodeNotFound) {
		return nil, err
	}
	if current != nil {
		if p, ok := c.store.(storepkg.HistoryPresenceCapability); ok {
			has, err := p.HasNodeHistory(id)
			if err != nil {
				return nil, err
			}
			if !has {
				return nodeSegments(c.nodeEffectivePieces([]*types.Node{current}, pin), nil), nil
			}
		}
	}
	if c.temporalMetaHistory != nil {
		if segs, handled, err := c.nodeEffectiveViaTemporalMeta(id, current, pin); handled {
			return segs, err
		}
	}
	history, err := c.getNodeHistory(id)
	if err != nil {
		return nil, err
	}
	if current == nil && len(history) == 0 {
		return nil, storepkg.ErrNodeNotFound
	}
	chain := make([]*types.Node, 0, len(history)+1)
	chain = append(chain, history...)
	if current != nil {
		chain = append(chain, current)
	}
	return nodeSegments(c.nodeEffectivePieces(chain, pin), nil), nil
}

// relEffectiveTimelineLocked mirrors nodeEffectiveTimelineLocked.
func (c *Core) relEffectiveTimelineLocked(id types.RelID, pin types.Instant) ([]temporal.RelSegment, error) {
	// The entity lock makes the current row, the presence bit and the
	// history one snapshot: a concurrent write (Update, Delete, CloseVersion)
	// moves the current row into history in more than one store step, and a
	// read between them saw neither (the entity read as unknown).
	c.entityLocks.LockEntity(id.SnowflakeID())
	defer c.entityLocks.UnlockEntity(id.SnowflakeID())
	if err := c.checkRelPointCompaction(id, pin); err != nil {
		return nil, err
	}
	if err := c.checkRelPointRetention(pin); err != nil {
		return nil, err
	}
	current, err := c.getCurrentRelationship(id)
	if err != nil && !errors.Is(err, storepkg.ErrRelNotFound) {
		return nil, err
	}
	if current != nil {
		if p, ok := c.store.(storepkg.HistoryPresenceCapability); ok {
			has, err := p.HasRelHistory(id)
			if err != nil {
				return nil, err
			}
			if !has {
				return relSegments(c.relEffectivePieces([]*types.Relationship{current}, pin), nil), nil
			}
		}
	}
	if c.temporalMetaHistory != nil {
		if segs, handled, err := c.relEffectiveViaTemporalMeta(id, current, pin); handled {
			return segs, err
		}
	}
	history, err := c.getRelHistory(id)
	if err != nil {
		return nil, err
	}
	if current == nil && len(history) == 0 {
		return nil, storepkg.ErrRelNotFound
	}
	chain := make([]*types.Relationship, 0, len(history)+1)
	chain = append(chain, history...)
	if current != nil {
		chain = append(chain, current)
	}
	return relSegments(c.relEffectivePieces(chain, pin), nil), nil
}

// nodeEffectiveViaTemporalMeta cuts and resolves on selection skeletons
// (store.TemporalMetaHistoryCapability: version + temporal only) and decodes
// only the winning history rows — nodeAtViaTemporalMeta's discipline: a
// skeleton never leaves this function; a winner is hydrated by a point read
// and given the same TxAt normalization the resolver applies (lesson 60).
// handled=false (a winner vanished between the reads, e.g. a concurrent trim)
// falls back to the full-chain fold.
func (c *Core) nodeEffectiveViaTemporalMeta(id types.NodeID, current *types.Node, pin types.Instant) ([]temporal.NodeSegment, bool, error) {
	metas, err := c.temporalMetaHistory.NodeHistoryTemporalMeta(id)
	if err != nil {
		return nil, true, err
	}
	if current == nil && len(metas) == 0 {
		return nil, true, storepkg.ErrNodeNotFound
	}
	chain := make([]*types.Node, 0, len(metas)+1)
	for _, m := range metas {
		s := types.NewNode(id, 0, nil)
		s.SetVersion(m.Version)
		if m.Temporal != nil {
			s.SetTemporal(m.Temporal)
		}
		chain = append(chain, s)
	}
	if current != nil {
		chain = append(chain, current)
	}
	pieces := c.nodeEffectivePieces(chain, pin)
	// Winners are told apart by origin, not by version: a re-imported ID's
	// current row shares version numbers with the earlier life's history.
	hydrated := make(map[int]*types.Node)
	hydrate := func(p effPiece[*types.Node]) (*types.Node, bool) {
		if p.src >= len(metas) {
			return p.row, true // the current row (or the resolver's normalized copy of it)
		}
		if full, ok := hydrated[p.src]; ok {
			return full, true
		}
		full, err := c.getNodeVersion(id, metas[p.src].Version)
		if err != nil {
			return nil, false
		}
		row, ok := nodeRowAtTx(full, pin)
		if !ok {
			return nil, false
		}
		hydrated[p.src] = row
		return row, true
	}
	segs := make([]temporal.NodeSegment, 0, len(pieces))
	for _, p := range pieces {
		row, ok := hydrate(p)
		if !ok {
			return nil, false, nil
		}
		segs = append(segs, nodeSegment(p.from, p.to, row))
	}
	return segs, true, nil
}

// relEffectiveViaTemporalMeta mirrors nodeEffectiveViaTemporalMeta.
func (c *Core) relEffectiveViaTemporalMeta(id types.RelID, current *types.Relationship, pin types.Instant) ([]temporal.RelSegment, bool, error) {
	metas, err := c.temporalMetaHistory.RelHistoryTemporalMeta(id)
	if err != nil {
		return nil, true, err
	}
	if current == nil && len(metas) == 0 {
		return nil, true, storepkg.ErrRelNotFound
	}
	chain := make([]*types.Relationship, 0, len(metas)+1)
	for _, m := range metas {
		s := types.NewRelationship(id, 0, 0, 0)
		s.SetVersion(m.Version)
		if m.Temporal != nil {
			s.SetTemporal(m.Temporal)
		}
		chain = append(chain, s)
	}
	if current != nil {
		chain = append(chain, current)
	}
	pieces := c.relEffectivePieces(chain, pin)
	// Winners are told apart by origin, not by version: a re-imported ID's
	// current row shares version numbers with the earlier life's history.
	hydrated := make(map[int]*types.Relationship)
	hydrate := func(p effPiece[*types.Relationship]) (*types.Relationship, bool) {
		if p.src >= len(metas) {
			return p.row, true // the current row (or the resolver's normalized copy of it)
		}
		if full, ok := hydrated[p.src]; ok {
			return full, true
		}
		full, err := c.getRelVersion(id, metas[p.src].Version)
		if err != nil {
			return nil, false
		}
		row, ok := relRowAtTx(full, pin)
		if !ok {
			return nil, false
		}
		hydrated[p.src] = row
		return row, true
	}
	segs := make([]temporal.RelSegment, 0, len(pieces))
	for _, p := range pieces {
		row, ok := hydrate(p)
		if !ok {
			return nil, false, nil
		}
		segs = append(segs, relSegment(p.from, p.to, row))
	}
	return segs, true, nil
}

// nodeSegment freezes row (a private copy unless already frozen: a shared
// store row is frozen and only read here) and wraps it.
func nodeSegment(from, to types.Instant, row *types.Node) temporal.NodeSegment {
	if !row.IsFrozen() {
		row.Freeze()
	}
	return temporal.NodeSegment{ValidFrom: from, ValidTo: to, Node: row}
}

func relSegment(from, to types.Instant, row *types.Relationship) temporal.RelSegment {
	if !row.IsFrozen() {
		row.Freeze()
	}
	return temporal.RelSegment{ValidFrom: from, ValidTo: to, Rel: row}
}

func nodeSegments(pieces []effPiece[*types.Node], out []temporal.NodeSegment) []temporal.NodeSegment {
	for _, p := range pieces {
		out = append(out, nodeSegment(p.from, p.to, p.row))
	}
	return out
}

func relSegments(pieces []effPiece[*types.Relationship], out []temporal.RelSegment) []temporal.RelSegment {
	for _, p := range pieces {
		out = append(out, relSegment(p.from, p.to, p.row))
	}
	return out
}

// ForEachNodeEffectiveByLabel streams the effective timeline at pin of every
// node that carried label in a row recorded by the pin — live, closed, and
// deleted before or after the pin alike (the pinned ByLabel{TxPin} door drops
// a node deleted by the pin; this one does not) — as NodeSegments, limited to
// the segments whose row carries the label: for every valid instant t, the
// segments containing t are exactly ByLabel(label, {ValidAt: t, TxAt: pin}).
// The segments of one node are contiguous and ascending; node order is
// unspecified; fn returning false stops the scan.
//
// Candidates are the ones ByLabel with a TxPin gathers (the label's
// ever-members, or every node with history where the store keeps no
// membership sidecar), so the same compaction and retention gates apply:
// pin <= 0 (ErrInvalidTimeRange), a pin above the commit clock
// (ErrTxPinTooNew), below the graph's compaction or retention watermark
// (ErrHistoryCompacted, ErrRetentionExpired) — checked before an unknown label
// returns nothing. Isolation is relaxed like ForEachByLabel: the candidate set
// is gathered under the read lock, each node's timeline is read under its own
// read lock, and fn runs without graph locks (it may call back into the
// graph). A pinned answer is stable while writers commit above the pin.
// Errors: ErrGraphClosed, ErrNilCallback, an invalid label.
func (t *TempOps) ForEachNodeEffectiveByLabel(label string, pin types.Instant, fn func(temporal.NodeSegment) bool) error {
	c := t.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return grapherr.ErrNilCallback
	}
	if err := c.validateIndexLabel(label); err != nil {
		return err
	}
	if err := c.validateEffectiveScanPin(pin); err != nil {
		return err
	}
	var (
		tok   uint16
		known bool
		ids   []types.NodeID
	)
	if err := c.readUnderRLock(func() error {
		tok, known = c.labels.Lookup(label)
		if !known {
			return nil
		}
		var err error
		_, ids, err = c.nodeLabelCandidatesAt(tok, storepkg.QueryOpts{TxPin: pin})
		return err
	}); err != nil {
		return err
	}
	for _, id := range ids {
		var segs []temporal.NodeSegment
		err := c.readUnderRLock(func() error {
			var err error
			segs, err = c.nodeEffectiveTimelineLocked(id, pin)
			return err
		})
		if errors.Is(err, storepkg.ErrNodeNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		for _, s := range segs {
			if s.Node.HasLabelTokenRaw(tok) && !fn(s) {
				return nil
			}
		}
	}
	return nil
}

// ForEachRelEffectiveByType is ForEachNodeEffectiveByLabel for relationships of
// a type (rows equal ByType(typeName, {ValidAt: t, TxAt: pin}) at every t; a
// relationship's type never changes, so each listed relationship streams its
// whole timeline). DECLARED view: not masked by endpoint validity.
func (t *TempOps) ForEachRelEffectiveByType(typeName string, pin types.Instant, fn func(temporal.RelSegment) bool) error {
	c := t.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return grapherr.ErrNilCallback
	}
	if err := c.validateRelTypeQueryName(typeName); err != nil {
		return err
	}
	if err := c.validateEffectiveScanPin(pin); err != nil {
		return err
	}
	var (
		tok   uint16
		known bool
		ids   []types.RelID
	)
	if err := c.readUnderRLock(func() error {
		tok, known = c.lookupRelTypeQueryToken(typeName)
		if !known {
			return nil
		}
		var err error
		_, ids, err = c.relTypeCandidatesAt(tok, storepkg.QueryOpts{TxPin: pin})
		return err
	}); err != nil {
		return err
	}
	for _, id := range ids {
		var segs []temporal.RelSegment
		err := c.readUnderRLock(func() error {
			var err error
			segs, err = c.relEffectiveTimelineLocked(id, pin)
			return err
		})
		if errors.Is(err, storepkg.ErrRelNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		for _, s := range segs {
			if s.Rel.HasTypeTokenRaw(tok) && !fn(s) {
				return nil
			}
		}
	}
	return nil
}

// validateEffectiveScanPin is validateEffectivePin plus the scan doors'
// graph-watermark gates (validateTemporalQueryOptsScan with a TxPin).
func (c *Core) validateEffectiveScanPin(pin types.Instant) error {
	if err := c.validateEffectivePin(pin); err != nil {
		return err
	}
	if err := c.checkScanCompactionAt(pin); err != nil {
		return err
	}
	return c.checkScanRetentionAt(pin)
}
