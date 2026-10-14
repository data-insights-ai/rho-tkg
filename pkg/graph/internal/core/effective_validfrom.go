package core

import (
	"cmp"
	"reflect"
	"slices"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Effective scans bounded by valid end (backlog 51).
//
// ForEachRelEffectiveByTypeFrom / ForEachNodeEffectiveByLabelsFrom stream an
// entity (its whole predicate-limited timeline) iff one of its half-open segments
// [ValidFrom, ValidTo) ends at or after validFrom or is open; a segment with
// ValidTo == validFrom counts. Candidates come from the store's valid-end index
// (store.RelValidEndCapability / NodeValidEndCapability: memory, badger, tiered as
// the union of shards), else from the unbounded door's gather (sharded, wrappers:
// correct, not accelerated). The index only decides which entities are read, never
// what streams; a read at a pin at or above the index stamp hands the exact end
// back (Refine) so a loose bound is read once.

// validEndCapabilities resolves the valid-end indexes with the K1 discipline: an
// exact native store or one declaring the methods itself; a wrapper that merely
// embeds a native store is refused (its writes may bypass index maintenance).
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

// validEndCand is one entity a bounded scan reads, with what the store's index
// knew when it listed it; a fallback candidate has stamp store.ValidEndOpen and is never refined.
type validEndCand = storepkg.ValidEndCandidate

func fallbackCands[T ~int64](ids []T) []validEndCand {
	out := make([]validEndCand, len(ids))
	for i, id := range ids {
		out[i] = validEndCand{ID: int64(id), Stamp: storepkg.ValidEndOpen, Bound: storepkg.ValidEndOpen}
	}
	return out
}

// streamValidFrom reads each candidate's timeline (nil read = unknown entity;
// ascending ID, via streamEntityTimelines), streams the segments keep accepts when
// one ends at or after validFrom or is open, and refines the index with the exact
// end when the candidate was read at a pin at or above its stamp.
func streamValidFrom[S any](c *Core, cands []validEndCand, pin, validFrom types.Instant,
	read func(rd entityReads, id int64) ([]S, error), to func(S) types.Instant, keep func(S) bool,
	refine func(id int64, gen uint64, end types.Instant), fn func(S) bool) error {
	cands = slices.Clone(cands)
	slices.SortFunc(cands, func(a, b validEndCand) int { return cmp.Compare(a.ID, b.ID) })
	return streamEntityTimelines(c, cands, func(rd entityReads, cd validEndCand) ([]S, error) {
		if c.chainLoadTestHook != nil {
			c.chainLoadTestHook()
		}
		return read(rd, cd.ID)
	}, func(cd validEndCand, segs []S) bool {
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
			return true
		}
		for _, s := range segs {
			if keep(s) && !fn(s) {
				return false
			}
		}
		return true
	})
}

// segmentsEnd is the effective valid end of a timeline: store.ValidEndOpen when a
// segment is open, store.ValidEndNone without segments.
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

// ForEachRelEffectiveByTypeFrom is ForEachRelEffectiveByType restricted to
// relationships with a timeline segment ending at or after validFrom, or open (an
// untimed, never-deleted one is always included; a deleted one ends at its delete
// instant). Each streamed relationship streams its whole timeline. Gates, isolation
// and errors are ForEachRelEffectiveByType's.
//
// Cost: memory/badger read the type's relationships whose folded or last-read valid
// end is at or after validFrom, plus those written after pin; the index tracks a
// type from its first call. Tiered does the same per shard (opening closed cold
// shards) and never refines. Sharded reads every candidate of the unbounded door.
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
	if err := c.validateEffectiveScanPin(pin, c.relTypeRetentionScope(typeName)); err != nil {
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
		func(rd entityReads, id int64) ([]temporal.RelSegment, error) {
			segs, err := c.relEffectiveTimelineFrom(rd, types.RelID(id), pin)
			return skipUnknown(segs, err, storepkg.ErrRelNotFound)
		},
		func(s temporal.RelSegment) types.Instant { return s.ValidTo },
		func(s temporal.RelSegment) bool { return s.Rel.HasTypeTokenRaw(tok) },
		refine, fn)
}

// ForEachNodeEffectiveByLabelsFrom is ForEachNodeEffectiveByLabels restricted to
// nodes whose label-carrying segments include one ending at or after validFrom, or
// open. Gates, isolation, errors and cost are ForEachRelEffectiveByTypeFrom's; the
// index bounds a node by its whole timeline, so a node that once carried the label
// is read and then skipped.
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
	if err := c.validateEffectiveScanPin(pin, scopeNodes); err != nil {
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
		func(rd entityReads, id int64) ([]temporal.NodeSegment, error) {
			segs, err := c.nodeEffectiveTimelineFrom(rd, types.NodeID(id), pin)
			return skipUnknown(segs, err, storepkg.ErrNodeNotFound)
		},
		func(s temporal.NodeSegment) types.Instant { return s.ValidTo },
		func(s temporal.NodeSegment) bool { return slices.ContainsFunc(toks, s.Node.HasLabelTokenRaw) },
		refine, fn)
}
