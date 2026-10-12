package core

import (
	"errors"
	"reflect"
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
// decides which entities are read, never what streams. A candidate read at a
// pin at or above its index stamp hands its exact end back (Refine), so an
// entity whose folded bound was loose is read once, not on every call.
// =============================================================================

// validEndCapabilities resolves the valid-end indexes with the K1 discipline:
// an exact native store, or a store declaring the methods itself; a wrapper
// that merely embeds a native store is refused (its writes may bypass the
// embedded store's index maintenance).
func validEndCapabilities(store storepkg.MandatoryStore) (storepkg.RelValidEndCapability, storepkg.NodeValidEndCapability) {
	trusted := func(capability reflect.Type, methods ...string) bool {
		return isExactNativeStore(store) || !embedsNativeCapability(store, capability, methods...)
	}
	rel, _ := store.(storepkg.RelValidEndCapability)
	if rel != nil && !trusted(reflect.TypeOf((*storepkg.RelValidEndCapability)(nil)).Elem(), "RelValidEndCandidates", "RefineRelValidEnd") {
		rel = nil
	}
	node, _ := store.(storepkg.NodeValidEndCapability)
	if node != nil && !trusted(reflect.TypeOf((*storepkg.NodeValidEndCapability)(nil)).Elem(), "NodeValidEndCandidates", "RefineNodeValidEnd") {
		node = nil
	}
	return rel, node
}

// validEndCand is one entity a bounded scan reads, with what the store's
// index knew when it listed it (store.ValidEndCandidate); a fallback
// candidate has stamp store.ValidEndOpen and is never refined.
type validEndCand = storepkg.ValidEndCandidate

func fallbackCands[T ~int64](ids []T) []validEndCand {
	out := make([]validEndCand, len(ids))
	for i, id := range ids {
		out[i] = validEndCand{ID: int64(id), Stamp: storepkg.ValidEndOpen, Bound: storepkg.ValidEndOpen}
	}
	return out
}

// streamValidFrom reads each candidate's timeline (read: nil when the entity
// is unknown), streams the segments keep accepts when one of them ends at or
// after validFrom or is open, and hands the timeline's exact end back to the
// index (refine; nil on the fallback) when the candidate was read at a pin at
// or above its stamp and its bound was looser.
func streamValidFrom[S any](c *Core, cands []validEndCand, pin, validFrom types.Instant,
	read func(id int64) ([]S, error), to func(S) types.Instant, keep func(S) bool,
	refine func(id int64, gen uint64, end types.Instant), fn func(S) bool) error {
	for _, cd := range cands {
		if c.chainLoadTestHook != nil {
			c.chainLoadTestHook()
		}
		segs, err := read(cd.ID)
		if err != nil {
			return err
		}
		if refine != nil && pin >= cd.Stamp {
			if end := segmentsEnd(segs, to); end < cd.Bound {
				refine(cd.ID, cd.Gen, end)
			}
		}
		reaches := false
		for _, s := range segs {
			if keep(s) && (to(s) == 0 || to(s) >= validFrom) {
				reaches = true
				break
			}
		}
		if !reaches {
			continue
		}
		for _, s := range segs {
			if keep(s) && !fn(s) {
				return nil
			}
		}
	}
	return nil
}

// segmentsEnd is the effective valid end of a timeline (segment ValidTo, 0 =
// open): store.ValidEndOpen when a segment is open, store.ValidEndNone without
// segments.
func segmentsEnd[S any](segs []S, to func(S) types.Instant) types.Instant {
	end := storepkg.ValidEndNone
	for _, s := range segs {
		t := to(s)
		if t == 0 {
			return storepkg.ValidEndOpen
		}
		end = max(end, t)
	}
	return end
}

// ForEachRelEffectiveByTypeFrom is ForEachRelEffectiveByType restricted to the
// relationships whose effective timeline at pin has a segment ending at or
// after validFrom, or an open one (an untimed relationship never deleted is
// always included; a deleted one ends at its delete instant; a retracted life
// has no segment). Each streamed relationship streams its whole timeline,
// exactly as the unbounded door does; validFrom at or below every end streams
// what the unbounded door streams. Gates, isolation and errors are
// ForEachRelEffectiveByType's.
//
// Cost: on memory and badger, reads the relationships of the type whose
// valid end (as folded from their rows, or as last read) is at or after
// validFrom, plus those written after pin; the store's valid-end index is
// built on the first call (one pass over every row's temporal fields) and
// maintained by every write after it. Tiered and sharded read every candidate
// of the unbounded door.
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
		cands []validEndCand
	)
	if err := c.readUnderRLock(func() error {
		var known bool
		if tok, known = c.lookupRelTypeQueryToken(typeName); !known {
			return nil
		}
		var err error
		if c.relValidEnds != nil {
			cands, err = c.relValidEnds.RelValidEndCandidates(tok, pin, validFrom)
			return err
		}
		ids, err := c.relTypeCandidateIDsAt(tok, storepkg.QueryOpts{TxPin: pin})
		cands = fallbackCands(ids)
		return err
	}); err != nil {
		return err
	}
	var refine func(int64, uint64, types.Instant)
	if c.relValidEnds != nil {
		refine = func(id int64, gen uint64, end types.Instant) {
			c.relValidEnds.RefineRelValidEnd(types.RelID(id), gen, end)
		}
	}
	return streamValidFrom(c, cands, pin, validFrom,
		func(id int64) ([]temporal.RelSegment, error) {
			var segs []temporal.RelSegment
			err := c.readUnderRLock(func() error {
				var err error
				segs, err = c.relEffectiveTimelineLocked(types.RelID(id), pin)
				return err
			})
			if errors.Is(err, storepkg.ErrRelNotFound) {
				return nil, nil
			}
			return segs, err
		},
		func(s temporal.RelSegment) types.Instant { return s.ValidTo },
		func(s temporal.RelSegment) bool { return s.Rel.HasTypeTokenRaw(tok) },
		refine, fn)
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
		var err error
		if c.nodeValidEnds != nil {
			cands, err = c.nodeValidEnds.NodeValidEndCandidates(toks, pin, validFrom)
			return err
		}
		ids, err := c.nodeLabelsCandidatesAt(toks, pin)
		cands = fallbackCands(ids)
		return err
	}); err != nil {
		return err
	}
	var refine func(int64, uint64, types.Instant)
	if c.nodeValidEnds != nil {
		refine = func(id int64, gen uint64, end types.Instant) {
			c.nodeValidEnds.RefineNodeValidEnd(types.NodeID(id), gen, end)
		}
	}
	return streamValidFrom(c, cands, pin, validFrom,
		func(id int64) ([]temporal.NodeSegment, error) {
			var segs []temporal.NodeSegment
			err := c.readUnderRLock(func() error {
				var err error
				segs, err = c.nodeEffectiveTimelineLocked(types.NodeID(id), pin)
				return err
			})
			if errors.Is(err, storepkg.ErrNodeNotFound) {
				return nil, nil
			}
			return segs, err
		},
		func(s temporal.NodeSegment) types.Instant { return s.ValidTo },
		func(s temporal.NodeSegment) bool { return slices.ContainsFunc(toks, s.Node.HasLabelTokenRaw) },
		refine, fn)
}
