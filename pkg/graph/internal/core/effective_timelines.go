package core

import (
	"errors"
	"slices"

	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// =============================================================================
// Effective timelines at many pins (row 98, backlog 79; ai-soc 2026-10-21)
//
// NodeEffectiveTimelines(id, pins) is NodeEffectiveTimeline(id, pin) for every
// pin of one call, read from ONE walk of the entity's chain: the current row,
// the history (or its temporal skeletons, badger) once, then the same fold
// (effectivePieces) per pin; a skeleton winner is hydrated once per row for
// all pins and normalized per pin (nodeRowAtTx). History rows read: about
// versions + distinct winners, not pins × versions (ai-soc: 511 versions at
// 511 pins read 524,288 rows through the singular door).
//
// The gates are the singular door's. The compaction and retention gates are
// monotone in the pin (a pin refused is refused at every lower pin), so the
// lowest pin answers for all of them: one gate read per call.
// =============================================================================

// NodeEffectiveTimelines returns NodeEffectiveTimeline(id, pins[i]) at index
// i for every pin: a slice aligned with pins (unsorted and repeated pins
// allowed; each answer stands on its own), read from one walk of the node's
// history chain. A pin at or below 0 answers nil (nothing recorded yet), as
// does a pin before the node's creation. Empty pins: an empty result, nothing
// read. Every pin is validated before any row is read: one pin above the
// commit clock (ErrTxPinTooNew), below compacted knowledge
// (ErrHistoryCompacted) or below a retention watermark (ErrRetentionExpired)
// refuses the whole call with no result. Other errors as the singular door:
// ErrGraphClosed, ErrInvalidStoreMutation (invalid ID), ErrNodeNotFound.
// The head-pin cache (backlog 68) is neither read nor filled.
func (t *TempOps) NodeEffectiveTimelines(id types.NodeID, pins []types.Instant) ([][]temporal.NodeSegment, error) {
	c := t.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if err := storepkg.ValidateNodeID(id); err != nil {
		return nil, err
	}
	mapped, err := c.validateEffectivePins(pins)
	if err != nil {
		return nil, err
	}
	if len(mapped) == 0 {
		return [][]temporal.NodeSegment{}, nil
	}
	var out [][]temporal.NodeSegment
	err = c.readUnderRLock(func() error {
		var err error
		out, err = c.nodeEffectiveTimelinesLocked(id, mapped)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// RelEffectiveTimelines is NodeEffectiveTimelines for relationships
// (RelEffectiveTimeline at every pin; ErrRelNotFound for an unknown ID).
func (t *TempOps) RelEffectiveTimelines(id types.RelID, pins []types.Instant) ([][]temporal.RelSegment, error) {
	c := t.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if err := storepkg.ValidateRelID(id); err != nil {
		return nil, err
	}
	mapped, err := c.validateEffectivePins(pins)
	if err != nil {
		return nil, err
	}
	if len(mapped) == 0 {
		return [][]temporal.RelSegment{}, nil
	}
	var out [][]temporal.RelSegment
	err = c.readUnderRLock(func() error {
		if err := c.checkRelTypePointRetention(id, slices.Min(mapped)); err != nil {
			return err
		}
		var err error
		out, err = c.relEffectiveTimelinesLocked(id, mapped)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// validateEffectivePins maps every pin as the singular door does
// (recordedPin) and validates it (validateEffectivePin); nil and the first
// pin's error on a refusal.
func (c *Core) validateEffectivePins(pins []types.Instant) ([]types.Instant, error) {
	mapped := make([]types.Instant, len(pins))
	for i, p := range pins {
		mapped[i] = recordedPin(p)
		if err := c.validateEffectivePin(mapped[i]); err != nil {
			return nil, err
		}
	}
	return mapped, nil
}

// nodeEffectiveTimelinesLocked is nodeEffectiveTimelineFrom (plain reads, no
// head cache) at every pin over one chain read (c.mu held).
func (c *Core) nodeEffectiveTimelinesLocked(id types.NodeID, pins []types.Instant) ([][]temporal.NodeSegment, error) {
	rd := entityReads{c: c}
	c.entityLocks.LockEntity(id.SnowflakeID())
	defer c.entityLocks.UnlockEntity(id.SnowflakeID())
	low := slices.Min(pins)
	if err := c.checkNodePointCompaction(id, low); err != nil {
		return nil, err
	}
	if err := c.checkNodePointRetention(id, low); err != nil {
		return nil, err
	}
	current, err := rd.lentCurrentNode(id) // handed out frozen: no copy needed
	if err != nil && !errors.Is(err, storepkg.ErrNodeNotFound) {
		return nil, err
	}
	if current != nil {
		if has, known, err := rd.hasNodeHistory(id); err != nil {
			return nil, err
		} else if known && !has {
			out, _ := effectiveTimelinesOver(c, nodeKernel, []*types.Node{current}, pins, nodeSegment, nil)
			return out, nil
		}
	}
	if c.temporalMetaHistory != nil {
		metas, err := rd.nodeMeta(id)
		if err != nil {
			return nil, err
		}
		if current == nil && len(metas) == 0 {
			return nil, storepkg.ErrNodeNotFound
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
		// Winners are told apart by origin, not by version (a re-imported
		// ID's current row may share version numbers with history); each
		// is read once for every pin.
		full := make(map[int]*types.Node)
		hydrate := func(src int, pin types.Instant, row *types.Node) (*types.Node, bool) {
			if src >= len(metas) {
				return row, true // the current row (or the resolver's normalized copy of it)
			}
			f, ok := full[src]
			if !ok {
				var err error
				if f, err = rd.nodeVersion(id, metas[src].Version); err != nil {
					return nil, false
				}
				full[src] = f
			}
			return nodeRowAtTx(c.axis(), f, pin)
		}
		if out, ok := effectiveTimelinesOver(c, nodeKernel, chain, pins, nodeSegment, hydrate); ok {
			return out, nil
		}
		// A winner vanished between the reads (e.g. a concurrent trim): fold
		// the full chain, as nodeEffectiveViaTemporalMeta's caller does.
	}
	history, err := rd.nodeHistory(id)
	if err != nil {
		return nil, err
	}
	if current == nil && len(history) == 0 {
		return nil, storepkg.ErrNodeNotFound
	}
	out, _ := effectiveTimelinesOver(c, nodeKernel, chainOf(history, current), pins, nodeSegment, nil)
	return out, nil
}

// relEffectiveTimelinesLocked mirrors nodeEffectiveTimelinesLocked (the
// relationship-type retention gate ran at the door).
func (c *Core) relEffectiveTimelinesLocked(id types.RelID, pins []types.Instant) ([][]temporal.RelSegment, error) {
	rd := entityReads{c: c}
	c.entityLocks.LockEntity(id.SnowflakeID())
	defer c.entityLocks.UnlockEntity(id.SnowflakeID())
	low := slices.Min(pins)
	if err := c.checkRelPointCompaction(id, low); err != nil {
		return nil, err
	}
	if err := c.checkRelPointRetention(low); err != nil {
		return nil, err
	}
	current, err := rd.lentCurrentRel(id) // handed out frozen: no copy needed
	if err != nil && !errors.Is(err, storepkg.ErrRelNotFound) {
		return nil, err
	}
	if current != nil {
		if has, known, err := rd.hasRelHistory(id); err != nil {
			return nil, err
		} else if known && !has {
			out, _ := effectiveTimelinesOver(c, relKernel, []*types.Relationship{current}, pins, relSegment, nil)
			return out, nil
		}
	}
	if c.temporalMetaHistory != nil {
		metas, err := rd.relMeta(id)
		if err != nil {
			return nil, err
		}
		if current == nil && len(metas) == 0 {
			return nil, storepkg.ErrRelNotFound
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
		// Winners are told apart by origin, not by version (a re-imported
		// ID's current row may share version numbers with history); each
		// is read once for every pin.
		full := make(map[int]*types.Relationship)
		hydrate := func(src int, pin types.Instant, row *types.Relationship) (*types.Relationship, bool) {
			if src >= len(metas) {
				return row, true // the current row (or the resolver's normalized copy of it)
			}
			f, ok := full[src]
			if !ok {
				var err error
				if f, err = rd.relVersion(id, metas[src].Version); err != nil {
					return nil, false
				}
				full[src] = f
			}
			return relRowAtTx(c.axis(), f, pin)
		}
		if out, ok := effectiveTimelinesOver(c, relKernel, chain, pins, relSegment, hydrate); ok {
			return out, nil
		}
	}
	history, err := rd.relHistory(id)
	if err != nil {
		return nil, err
	}
	if current == nil && len(history) == 0 {
		return nil, storepkg.ErrRelNotFound
	}
	out, _ := effectiveTimelinesOver(c, relKernel, chainOf(history, current), pins, relSegment, nil)
	return out, nil
}

// effectiveTimelinesOver folds chain at every pin (effectivePieces, the
// singular door's fold) into segments. hydrate, when not nil, maps a winner
// (its index in chain, the pin, the piece's row) to the row the segment
// holds: a skeleton winner's stored row normalized at the pin as the resolver
// normalizes it (rowAtTx, lesson 60); ok is false when one could not be read.
func effectiveTimelinesOver[T interface {
	comparable
	storeutil.TemporalRow
}, S any](c *Core, k *chainKernel[T], chain []T, pins []types.Instant, seg func(from, to types.Instant, row T) S, hydrate func(src int, pin types.Instant, row T) (T, bool)) ([][]S, bool) {
	out := make([][]S, len(pins))
	for i, pin := range pins {
		pieces := effectivePieces(c, k, chain, pin)
		if len(pieces) == 0 {
			continue // nothing recorded by the pin: nil, as the singular door
		}
		segs := make([]S, 0, len(pieces))
		for _, p := range pieces {
			row := p.row
			if hydrate != nil {
				var ok bool
				if row, ok = hydrate(p.src, pin, row); !ok {
					return nil, false
				}
			}
			segs = append(segs, seg(p.from, p.to, row))
		}
		out[i] = segs
	}
	return out, true
}
