package core

import (
	"errors"
	"math"
	"slices"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// =============================================================================
// Effective scans bounded by valid end (backlog 51, sigma-tkgd step 6)
//
// ForEachRelEffectiveByTypeFrom / ForEachNodeEffectiveByLabelsFrom are the
// type and label scans restricted to the entities still effective at or after
// a valid instant: an entity streams (its whole predicate-limited timeline, as
// the unbounded door streams it) iff one of those segments ends at or after
// validFrom or is open. Half-open segments [ValidFrom, ValidTo): a segment
// whose ValidTo equals validFrom (last valid instant validFrom-1) counts, so
// "ends before validFrom" and "streams" partition the entities.
//
// Candidates come from the store's valid-end index where it keeps one
// (store.RelValidEndCapability / NodeValidEndCapability: memory and badger),
// else from the unbounded door's gather (tiered, sharded, wrappers: correct,
// not accelerated). Either way each candidate's timeline is read by the
// per-entity body and the rule above is applied to it, so the index only
// decides which entities are read, never what streams.
// =============================================================================

// Valid-end sentinels of the index contract: an open end, and no segment.
const (
	validEndOpen = types.Instant(math.MaxInt64)
	validEndNone = types.Instant(math.MinInt64)
)

// validEndCand is one entity a bounded scan reads: id, and what the store's
// index knew when it listed it (gen, the stamp at or above which its bound
// holds, the bound). A fallback candidate carries no index state (stamp
// validEndOpen: never refined).
type validEndCand struct {
	id           int64
	gen          uint64
	stamp, bound types.Instant
}

func fallbackCands[T ~int64](ids []T) []validEndCand {
	out := make([]validEndCand, len(ids))
	for i, id := range ids {
		out[i] = validEndCand{id: int64(id), stamp: validEndOpen, bound: validEndOpen}
	}
	return out
}

// segmentsEnd is the effective valid end of a timeline: the last segment's
// ValidTo, validEndOpen when it is open, validEndNone without segments.
func segmentsEnd(n int, to func(int) types.Instant) types.Instant {
	if n == 0 {
		return validEndNone
	}
	end := validEndNone
	for i := 0; i < n; i++ {
		t := to(i)
		if t == 0 {
			return validEndOpen
		}
		end = max(end, t)
	}
	return end
}

// reachesValidFrom reports whether a segment end (0 = open) is at or after
// validFrom.
func reachesValidFrom(to, validFrom types.Instant) bool {
	return to == 0 || to >= validFrom
}

// ForEachRelEffectiveByTypeFrom is ForEachRelEffectiveByType restricted to the
// relationships whose effective timeline at pin has a segment ending at or
// after validFrom, or an open one (an untimed relationship never deleted is
// always included; a deleted one ends at its delete instant). Each streamed
// relationship streams its whole timeline, exactly as the unbounded door does;
// validFrom at or below every end streams what the unbounded door streams.
// Gates, isolation and errors are ForEachRelEffectiveByType's.
//
// Cost: on memory and badger, reads only the relationships of the type whose
// valid end (as last recorded) is at or after validFrom, plus those written
// after pin; the store's valid-end index is built on the first call (one pass
// over the rows' temporal fields) and maintained by every write after it.
// Tiered and sharded read every candidate of the unbounded door.
func (t *TempOps) ForEachRelEffectiveByTypeFrom(typeName string, pin, validFrom types.Instant, fn func(temporal.RelSegment) bool) error {
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
		cands []validEndCand
	)
	if err := c.readUnderRLock(func() error {
		tok, known = c.lookupRelTypeQueryToken(typeName)
		if !known {
			return nil
		}
		ids, err := c.relTypeCandidateIDsAt(tok, storepkg.QueryOpts{TxPin: pin})
		cands = fallbackCands(ids)
		return err
	}); err != nil {
		return err
	}
	keep := func(r *types.Relationship) bool { return r.HasTypeTokenRaw(tok) }
	for _, cd := range cands {
		if c.chainLoadTestHook != nil {
			c.chainLoadTestHook()
		}
		var segs []temporal.RelSegment
		err := c.readUnderRLock(func() error {
			var err error
			segs, err = c.relEffectiveTimelineLocked(types.RelID(cd.id), pin)
			return err
		})
		if err != nil && !errors.Is(err, storepkg.ErrRelNotFound) {
			return err
		}
		kept := segs[:0:0]
		reaches := false
		for _, s := range segs {
			if keep(s.Rel) {
				kept = append(kept, s)
				reaches = reaches || reachesValidFrom(s.ValidTo, validFrom)
			}
		}
		if !reaches {
			continue
		}
		for _, s := range kept {
			if !fn(s) {
				return nil
			}
		}
	}
	return nil
}

// ForEachNodeEffectiveByLabelsFrom is ForEachNodeEffectiveByLabels restricted
// to the nodes whose segments carrying one of the labels include one ending at
// or after validFrom, or an open one (ForEachRelEffectiveByTypeFrom's rule on
// the label-limited timeline). Gates, isolation, errors and cost are
// ForEachRelEffectiveByTypeFrom's; the index bounds a node by its whole
// timeline, so a node that carried a label once and lives on under other
// labels is read and then skipped.
func (t *TempOps) ForEachNodeEffectiveByLabelsFrom(labels []string, pin, validFrom types.Instant, fn func(temporal.NodeSegment) bool) error {
	c := t.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return grapherr.ErrNilCallback
	}
	for _, label := range labels {
		if err := c.validateIndexLabel(label); err != nil {
			return err
		}
	}
	if err := c.validateEffectiveScanPin(pin); err != nil {
		return err
	}
	var (
		toks  []uint16
		cands []validEndCand
	)
	if err := c.readUnderRLock(func() error {
		for _, label := range labels {
			if tok, known := c.labels.Lookup(label); known && !slices.Contains(toks, tok) {
				toks = append(toks, tok)
			}
		}
		if len(toks) == 0 {
			return nil
		}
		ids, err := c.nodeLabelsCandidatesAt(toks, pin)
		cands = fallbackCands(ids)
		return err
	}); err != nil {
		return err
	}
	keep := func(n *types.Node) bool { return slices.ContainsFunc(toks, n.HasLabelTokenRaw) }
	for _, cd := range cands {
		if c.chainLoadTestHook != nil {
			c.chainLoadTestHook()
		}
		var segs []temporal.NodeSegment
		err := c.readUnderRLock(func() error {
			var err error
			segs, err = c.nodeEffectiveTimelineLocked(types.NodeID(cd.id), pin)
			return err
		})
		if err != nil && !errors.Is(err, storepkg.ErrNodeNotFound) {
			return err
		}
		kept := segs[:0:0]
		reaches := false
		for _, s := range segs {
			if keep(s.Node) {
				kept = append(kept, s)
				reaches = reaches || reachesValidFrom(s.ValidTo, validFrom)
			}
		}
		if !reaches {
			continue
		}
		for _, s := range kept {
			if !fn(s) {
				return nil
			}
		}
	}
	return nil
}
