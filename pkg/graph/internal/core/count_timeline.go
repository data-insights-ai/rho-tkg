package core

import (
	"cmp"
	"math"
	"slices"

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

// countTimelineEntry is a cached timeline and the cut epoch it was built at.
type countTimelineEntry struct {
	epoch uint64
	segs  []storepkg.CountSegment
}

// countTimelineCap bounds the cached timelines per graph (each is at most
// two segments per member segment; typically a few).
const countTimelineCap = 256

// countTimelineAttempts is how many builds a call tries while writers move
// the cut epoch under it before it declines.
const countTimelineAttempts = 3

func (a *asOfColumnCache) getTimeline(key countTimelineKey, epoch uint64) ([]storepkg.CountSegment, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	e, ok := a.timelines[key]
	if !ok || e.epoch != epoch {
		return nil, false
	}
	return e.segs, true
}

func (a *asOfColumnCache) putTimeline(key countTimelineKey, epoch uint64, segs []storepkg.CountSegment) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.timelines == nil {
		a.timelines = make(map[countTimelineKey]countTimelineEntry)
	}
	if len(a.timelines) >= countTimelineCap {
		for k, e := range a.timelines {
			if e.epoch != epoch || len(a.timelines) >= countTimelineCap {
				delete(a.timelines, k)
			}
		}
	}
	a.timelines[key] = countTimelineEntry{epoch: epoch, segs: segs}
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

// countTimeline reads the timeline of key at its pin: the cached one, else
// built from the effective scan (and cached when the pin has landed and the
// epoch held). ok=false declines.
func (c *Core) countTimeline(key stateCutKey, opts storepkg.CountTimelineOpts) ([]storepkg.CountSegment, bool, error) {
	a := c.asOfColumns
	a.notePin(key.pin) // before the epoch is read (notePin)
	tkey := countTimelineKey{cut: key, untimed: opts.UntimedFromMinusInf}
	for range countTimelineAttempts {
		epoch := a.cutEpoch(key.rel)
		if segs, ok := a.getTimeline(tkey, epoch); ok {
			if opts.MaxSegments > 0 && len(segs) > opts.MaxSegments {
				return nil, false, nil
			}
			return slices.Clone(segs), true, nil
		}
		landed := c.pinLanded(key.pin)
		spans, err := c.timelineSpans(key, opts.UntimedFromMinusInf)
		if err != nil {
			return nil, false, err
		}
		if a.cutEpoch(key.rel) != epoch {
			continue // a history rewrite raced the read
		}
		segs, ok := sweepCountTimeline(spans, opts.MaxSegments)
		if !ok {
			return nil, false, nil
		}
		if landed {
			a.putTimeline(tkey, epoch, segs)
		}
		return slices.Clone(segs), true, nil
	}
	return nil, false, nil
}

// timelineSpans is every segment of key's effective scan at its pin, an
// untimed first segment from -inf under untimed.
func (c *Core) timelineSpans(key stateCutKey, untimed bool) ([]timelineSpan, error) {
	var spans []timelineSpan
	var genesis []int // spans whose start the genesis row decides
	var genesisIDs []int64
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
				spans = append(spans, timelineSpan{s.ValidFrom, s.ValidTo})
				if untimed && first(id) {
					vf, _, _ := s.Rel.ValidRange()
					tx, _, _ := s.Rel.TxStamps()
					yes, ask := untimedSegmentStart(s.ValidFrom, vf, tx, s.Rel.Version())
					if yes {
						spans[len(spans)-1].from = math.MinInt64
					} else if ask {
						genesis, genesisIDs = append(genesis, len(spans)-1), append(genesisIDs, id)
					}
				}
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
				spans = append(spans, timelineSpan{s.ValidFrom, s.ValidTo})
				if untimed && first(id) {
					vf, _, _ := s.Node.ValidRange()
					tx, _, _ := s.Node.TxStamps()
					yes, ask := untimedSegmentStart(s.ValidFrom, vf, tx, s.Node.Version())
					if yes {
						spans[len(spans)-1].from = math.MinInt64
					} else if ask {
						genesis, genesisIDs = append(genesis, len(spans)-1), append(genesisIDs, id)
					}
				}
				return true
			})
		if err != nil {
			return nil, err
		}
	}
	if len(genesis) == 0 {
		return spans, nil
	}
	err := c.readUnderRLock(func() error {
		for k, i := range genesis {
			var yes bool
			if key.rel {
				h, err := c.getRelHistory(types.RelID(genesisIDs[k]))
				if err != nil {
					return err
				}
				yes = genesisUntimed(h, spans[i].from)
			} else {
				h, err := c.getNodeHistory(types.NodeID(genesisIDs[k]))
				if err != nil {
					return err
				}
				yes = genesisUntimed(h, spans[i].from)
			}
			if yes {
				spans[i].from = math.MinInt64
			}
		}
		return nil
	})
	return spans, err
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
	c := n.c
	if err := c.checkOpen(); err != nil {
		return nil, false, err
	}
	if err := c.validateIndexLabel(label); err != nil {
		return nil, false, err
	}
	at, ok, err := c.countTimelinePin(pin)
	if err != nil || !ok {
		return nil, err == nil, err
	}
	if err := c.validateTemporalQueryOptsScan(storepkg.QueryOpts{ValidAt: 1, TxAt: at}, scopeNodes); err != nil {
		return nil, false, err
	}
	tok, known := c.stateCutToken(false, label)
	if !known {
		return nil, true, c.checkOpen()
	}
	return c.countTimeline(stateCutKey{tok: tok, pin: at}, opts)
}

// CountTimelineByType is CountTimelineByLabel for a relationship type.
func (r *RelOps) CountTimelineByType(typeName string, pin types.Instant, opts storepkg.CountTimelineOpts) ([]storepkg.CountSegment, bool, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return nil, false, err
	}
	if err := c.validateRelTypeQueryName(typeName); err != nil {
		return nil, false, err
	}
	at, ok, err := c.countTimelinePin(pin)
	if err != nil || !ok {
		return nil, err == nil, err
	}
	if err := c.validateTemporalQueryOptsScan(storepkg.QueryOpts{ValidAt: 1, TxAt: at}, c.relTypeRetentionScope(typeName)); err != nil {
		return nil, false, err
	}
	tok, known := c.stateCutToken(true, typeName)
	if !known {
		return nil, true, c.checkOpen()
	}
	return c.countTimeline(stateCutKey{rel: true, tok: tok, pin: at}, opts)
}
