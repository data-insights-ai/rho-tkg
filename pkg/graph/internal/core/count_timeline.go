package core

import (
	"cmp"
	"fmt"
	"math"
	"slices"

	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Count timelines (sigma-tkgd round 9 request R9-2).
//
// CountTimelineByLabel / CountTimelineByType answer, for every valid instant
// t, the number of the label's (type's) members holding at t as recorded at
// a pin: CountByLabelAt(label, {ValidAt: t, TxAt: pin}) for every t at
// once. The state cut (state_columns.go) holds exactly those segments: for
// every t the segments containing t are ByLabel(label, {ValidAt: t, TxAt:
// pin}). The timeline reads the same segments (the label's effective scan
// at the pin, buildStateCut's source) and sweeps their ends: one sort of
// the 2s ends and a pass, O(s log s) for s segments. It is cached per
// (kind, token, pin, untimed option) under the state cuts' invalidation
// (cutEpoch: a history rewrite at or below a noted pin, an in-place update,
// a clamping delete) and only once every write stamped at or below the pin
// has landed (pinLanded); forward writes leave it.

// countTimelineKey names a cached timeline.
type countTimelineKey struct {
	cut     stateCutKey
	untimed bool
}

// timelineSpan is one member segment's valid span.
type timelineSpan struct{ from, to types.Instant }

// sweepCountTimeline turns member spans into maximal constant-count
// segments of positive count; ok=false when more than max (> 0) segments
// would result.
func sweepCountTimeline(spans []timelineSpan, max int) ([]storepkg.CountSegment, bool) {
	type end struct {
		at    types.Instant
		delta int64
	}
	ends := make([]end, 0, 2*len(spans))
	for _, s := range spans {
		if s.to != 0 && s.to <= s.from {
			continue
		}
		ends = append(ends, end{s.from, 1})
		if s.to != 0 {
			ends = append(ends, end{s.to, -1})
		}
	}
	slices.SortFunc(ends, func(a, b end) int { return cmp.Compare(a.at, b.at) })
	var out []storepkg.CountSegment
	var count int64
	open := -1 // the segment the sweep is in
	for i := 0; i < len(ends); {
		at := ends[i].at
		for ; i < len(ends) && ends[i].at == at; i++ {
			count += ends[i].delta
		}
		if open >= 0 {
			if out[open].Count == count {
				continue // equal counts merge
			}
			out[open].To, open = at, -1
		}
		if count > 0 {
			if max > 0 && len(out) >= max {
				return nil, false
			}
			out = append(out, storepkg.CountSegment{From: at, Count: count})
			open = len(out) - 1
		}
	}
	return out, true
}

// untimedSegmentStart reports whether an entity's first segment of a stream,
// starting at segFrom on a row of version version with valid-from rowFrom
// and TxFrom txFrom, starts at an untimed create (store.CountTimelineOpts).
// The first version (0) without a valid-from starts at its TxFrom; a close
// stamps that start on the closing row (version > 0, valid-from = segFrom),
// untimed iff the genesis row has no valid-from and that TxFrom.
// needGenesis reports that the genesis row decides.
func untimedSegmentStart(segFrom, rowFrom, txFrom types.Instant, version uint32) (untimed, needGenesis bool) {
	if version == 0 {
		return rowFrom == 0 && segFrom == txFrom, false
	}
	return false, rowFrom == segFrom
}

// genesisUntimed reports whether chain's first version is untimed
// (untimedFirst, the conversion's rule) with the TxFrom at.
func genesisUntimed[T interface {
	Version() uint32
	Temporal() *types.TemporalMetadata
}](chain []T, at types.Instant) bool {
	for _, v := range chain {
		if tm := v.Temporal(); v.Version() == 0 {
			return untimedFirst(0, tm) && tm.TxFrom == at
		}
	}
	return false
}

// countTimeline reads the timeline of key at its pin: the memoised one,
// else built from the effective scan (cut_memo.go). ok=false declines. The
// segments may be the memo's own: read-only (the doors copy them out).
func (c *Core) countTimeline(key stateCutKey, opts storepkg.CountTimelineOpts) ([]storepkg.CountSegment, bool, error) {
	tkey := countTimelineKey{cut: key, untimed: opts.UntimedFromMinusInf}
	segs, ok, err := memoAtPin(c, &c.asOfColumns.timelines, key.rel, key.pin, tkey, func() ([]storepkg.CountSegment, bool, error) {
		spans, err := c.timelineSpans(key, opts.UntimedFromMinusInf)
		if err != nil {
			return nil, false, err
		}
		segs, ok := sweepCountTimeline(spans, opts.MaxSegments)
		return segs, ok, nil
	})
	if err != nil || !ok || (opts.MaxSegments > 0 && len(segs) > opts.MaxSegments) {
		return nil, false, err
	}
	return segs, true, nil
}

// timelineRow is what the untimed rule reads of a row (Node, Relationship).
type timelineRow interface {
	ValidRange() (from, to types.Instant, ok bool)
	TxStamps() (txFrom, txTo, deletedAt types.Instant)
	Version() uint32
}

// timelineSpanSet collects member spans; under untimed an entity's first
// span that starts at an untimed create starts at -inf instead
// (store.CountTimelineOpts.UntimedFromMinusInf), the spans whose start the
// genesis row decides listed for resolveGenesis.
type timelineSpanSet struct {
	untimed    bool
	spans      []timelineSpan
	genesis    []int // spans whose start the genesis row decides
	genesisIDs []int64
}

// add appends the span [from, to) of entity id on row; first marks the
// entity's first span of the stream.
func (s *timelineSpanSet) add(id int64, first bool, from, to types.Instant, row timelineRow) {
	s.spans = append(s.spans, timelineSpan{from, to})
	if !s.untimed || !first {
		return
	}
	vf, _, _ := row.ValidRange()
	tx, _, _ := row.TxStamps()
	yes, ask := untimedSegmentStart(from, vf, tx, row.Version())
	if yes {
		s.spans[len(s.spans)-1].from = math.MinInt64
	} else if ask {
		s.genesis, s.genesisIDs = append(s.genesis, len(s.spans)-1), append(s.genesisIDs, id)
	}
}

// resolveGenesis starts at -inf every listed span whose entity's genesis
// row is an untimed create at the span's start.
func (c *Core) resolveGenesis(rel bool, s *timelineSpanSet) error {
	if len(s.genesis) == 0 {
		return nil
	}
	return c.readUnderRLock(func() error {
		for k, i := range s.genesis {
			var yes bool
			if rel {
				h, err := c.getRelHistory(types.RelID(s.genesisIDs[k]))
				if err != nil {
					return err
				}
				yes = genesisUntimed(h, s.spans[i].from)
			} else {
				h, err := c.getNodeHistory(types.NodeID(s.genesisIDs[k]))
				if err != nil {
					return err
				}
				yes = genesisUntimed(h, s.spans[i].from)
			}
			if yes {
				s.spans[i].from = math.MinInt64
			}
		}
		return nil
	})
}

// timelineSpans is every segment of key's effective scan at its pin, an
// untimed first segment from -inf under untimed.
func (c *Core) timelineSpans(key stateCutKey, untimed bool) ([]timelineSpan, error) {
	set := timelineSpanSet{untimed: untimed}
	var last int64
	first := func(id int64) bool {
		f := id != last
		last = id
		return f
	}
	if key.rel {
		var ids []types.RelID
		if err := c.readUnderRLock(func() error {
			var err error
			ids, err = c.relTypeCandidateIDsAt(key.tok, storepkg.QueryOpts{TxPin: key.pin})
			return err
		}); err != nil {
			return nil, err
		}
		err := c.streamRelTimelines(ascending(ids), key.pin, func(r *types.Relationship) bool { return r.HasTypeTokenRaw(key.tok) },
			func(s temporal.RelSegment) bool {
				id := int64(s.Rel.ID())
				set.add(id, first(id), s.ValidFrom, s.ValidTo, s.Rel)
				return true
			})
		if err != nil {
			return nil, err
		}
	} else {
		var ids []types.NodeID
		if err := c.readUnderRLock(func() error {
			var err error
			ids, err = c.nodeLabelCandidateIDsAt(key.tok, storepkg.QueryOpts{TxPin: key.pin})
			return err
		}); err != nil {
			return nil, err
		}
		err := c.streamNodeTimelines(ascending(ids), key.pin, func(n *types.Node) bool { return n.HasLabelTokenRaw(key.tok) },
			func(s temporal.NodeSegment) bool {
				id := int64(s.Node.ID())
				set.add(id, first(id), s.ValidFrom, s.ValidTo, s.Node)
				return true
			})
		if err != nil {
			return nil, err
		}
	}
	err := c.resolveGenesis(key.rel, &set)
	return set.spans, err
}

// currentRowsTimeline is the timeline of the current rows of a label
// (type), the rows ForEachByLabel(label, QueryOpts{}) / ForEachByType hand
// out, each over its own valid interval: from its valid-from (an unset one
// is the row's derived start, storeutil.NodeValidFrom, its TxFrom), to its
// valid-to (open when unset), under untimed from -inf where the row starts
// at an untimed create (the pinned timeline's rule, timelineSpanSet). Not
// memoised: every write of a member changes it.
func (c *Core) currentRowsTimeline(rel bool, name string, opts storepkg.CountTimelineOpts) ([]storepkg.CountSegment, bool, error) {
	set := timelineSpanSet{untimed: opts.UntimedFromMinusInf}
	var err error
	if rel {
		err = c.Rels.ForEachByType(name, storepkg.QueryOpts{}, func(r *types.Relationship) bool {
			_, to, _ := r.ValidRange()
			set.add(int64(r.ID()), true, storeutil.RelValidFrom(r), to, r)
			return true
		})
	} else {
		err = c.Nodes.ForEachByLabel(name, storepkg.QueryOpts{}, func(n *types.Node) bool {
			_, to, _ := n.ValidRange()
			set.add(int64(n.ID()), true, storeutil.NodeValidFrom(n), to, n)
			return true
		})
	}
	if err == nil {
		err = c.resolveGenesis(rel, &set)
	}
	if err != nil {
		return nil, false, err
	}
	segs, ok := sweepCountTimeline(set.spans, opts.MaxSegments)
	if !ok {
		return nil, false, nil
	}
	return segs, true, nil
}

// countTimelinePin is the pin a timeline reads: pin, or the commit clock
// for 0. ok=false for a negative pin (nothing recorded).
func (c *Core) countTimelinePin(pin types.Instant) (types.Instant, bool, error) {
	if pin < 0 {
		return 0, false, nil
	}
	if pin > 0 {
		return pin, true, nil
	}
	now, err := (&TempOps{c: c}).CommittedTx()
	return now, err == nil, err
}

// CountTimelineByLabel is the number of label's members holding at each
// valid instant as recorded at pin (see nodes.API.CountTimelineByLabel).
func (n *NodeOps) CountTimelineByLabel(label string, pin types.Instant, opts storepkg.CountTimelineOpts) ([]storepkg.CountSegment, bool, error) {
	segs, ok, err := n.c.countTimelineDoor(false, label, pin, opts)
	return slices.Clone(segs), ok, err
}

// CountTimelineByType is CountTimelineByLabel for a relationship type.
func (r *RelOps) CountTimelineByType(typeName string, pin types.Instant, opts storepkg.CountTimelineOpts) ([]storepkg.CountSegment, bool, error) {
	segs, ok, err := r.c.countTimelineDoor(true, typeName, pin, opts)
	return slices.Clone(segs), ok, err
}

// countTimelineDoor is the body of CountTimelineByLabel / ByType: the
// timeline at pin, or with opts.CurrentRows (pin must be 0) of the current
// rows. The segments may be the memo's own (read-only): the doors copy them,
// the hull reads two of them.
func (c *Core) countTimelineDoor(rel bool, name string, pin types.Instant, opts storepkg.CountTimelineOpts) ([]storepkg.CountSegment, bool, error) {
	if err := c.checkOpen(); err != nil {
		return nil, false, err
	}
	var err error
	if rel {
		err = c.validateRelTypeQueryName(name)
	} else {
		err = c.validateIndexLabel(name)
	}
	if err != nil {
		return nil, false, err
	}
	if opts.CurrentRows {
		if pin != 0 {
			return nil, false, fmt.Errorf("%w: CurrentRows reads the current rows, which no pin %d selects", ErrInvalidTimeRange, pin)
		}
		return c.currentRowsTimeline(rel, name, opts)
	}
	at, ok, err := c.countTimelinePin(pin)
	if err != nil || !ok {
		return nil, err == nil, err
	}
	scope := scopeNodes
	if rel {
		scope = c.relTypeRetentionScope(name)
	}
	if err := c.validateTemporalQueryOptsScan(storepkg.QueryOpts{ValidAt: 1, ValidAtSet: true, TxAt: at, TxAtSet: true}, scope); err != nil {
		return nil, false, err
	}
	tok, known := c.stateCutToken(rel, name)
	if !known {
		return nil, true, c.checkOpen()
	}
	return c.countTimeline(stateCutKey{rel: rel, tok: tok, pin: at}, opts)
}

// ValidHullByLabel is the hull of the valid instants at which label's
// members hold (see nodes.API.ValidHullByLabel).
func (n *NodeOps) ValidHullByLabel(label string, pin types.Instant, opts storepkg.CountTimelineOpts) (storepkg.ValidHull, bool, error) {
	return n.c.validHull(false, label, pin, opts)
}

// ValidHullByType is ValidHullByLabel for a relationship type.
func (r *RelOps) ValidHullByType(typeName string, pin types.Instant, opts storepkg.CountTimelineOpts) (storepkg.ValidHull, bool, error) {
	return r.c.validHull(true, typeName, pin, opts)
}

// validHull reads the hull off the count timeline's ends (one rule: the
// hull is where the count is positive), O(1) on a memoised timeline (no
// copy). MaxSegments does not apply.
func (c *Core) validHull(rel bool, name string, pin types.Instant, opts storepkg.CountTimelineOpts) (storepkg.ValidHull, bool, error) {
	opts.MaxSegments = 0
	segs, ok, err := c.countTimelineDoor(rel, name, pin, opts)
	if err != nil || !ok {
		return storepkg.ValidHull{}, false, err
	}
	return hullOf(segs), true, nil
}

// hullOf is the hull of a count timeline (ascending, positive counts).
func hullOf(segs []storepkg.CountSegment) storepkg.ValidHull {
	if len(segs) == 0 {
		return storepkg.ValidHull{Empty: true}
	}
	last := segs[len(segs)-1]
	// A segment's To == 0 is an open end: no stored row starts or ends at
	// the instant 0 (ValidFrom 0 is unset, ValidTo 0 open), so no member
	// span, hence no sweep boundary, lies at 0.
	return storepkg.ValidHull{From: segs[0].From, To: last.To, OpenEnd: last.To == 0}
}
