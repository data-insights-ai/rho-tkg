package core

import (
	"context"
	"reflect"
	"slices"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The per-entity effective timelines under a read budget (storepkg.WithReadBudget,
// RH list R2, sigma's group B): the plain door's rule and answer, or
// *store.ReadBudgetError and no segments. ctx is checked first, then a budget
// the store cannot count is refused; the call's own checks (open, ID, pins) run
// before anything is counted. Counted: one candidate (the ID), before the read;
// through the metered read snapshot (memory: the versions of the chain it reads)
// the history versions looked at, the rows and skeletons decoded and, on
// tiered, each closed shard about to be opened; and each returned segment
// (Segments, and its struct size as BufferBytes) after the read and before the
// answer is returned. Without a budget each is its plain door.

// beginTimeline is the shared start of a timeline Ctx door: ctx, then the
// store's ability to count the budget. A nil meter is a context without a
// budget: the caller reads as the plain door.
func (c *Core) beginTimeline(ctx context.Context, door string) (*storepkg.ReadMeter, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}
	if err := c.budgetSupported(ctx, door); err != nil {
		return nil, err
	}
	return storepkg.ReadMeterFrom(ctx), nil
}

// chargeSegments counts n segments of size bytes each before they are
// returned: Segments, then BufferBytes, one segment at a time, so a refusal
// names the segment that passes the limit.
func chargeSegments(meter *storepkg.ReadMeter, door string, n, size int) error {
	for range n {
		if err := meter.AddSegments(door, 1); err != nil {
			return err
		}
		if err := meter.AddBufferBytes(door, size); err != nil {
			return err
		}
	}
	return nil
}

var (
	nodeSegmentSize = int(reflect.TypeFor[temporal.NodeSegment]().Size()) // #nosec G115 -- a struct size
	relSegmentSize  = int(reflect.TypeFor[temporal.RelSegment]().Size())  // #nosec G115 -- a struct size
)

// NodeEffectiveTimelineCtx is NodeEffectiveTimeline under ctx's read budget
// (temporal.API.NodeEffectiveTimelineCtx).
func (t *TempOps) NodeEffectiveTimelineCtx(ctx context.Context, id types.NodeID, pin types.Instant) ([]temporal.NodeSegment, error) {
	return t.nodeEffectiveTimelineCtx(ctx, "NodeEffectiveTimeline", id, types.UnboundedValidFrom, pin) // every segment reaches it
}

// NodeEffectiveTimelineFromCtx is NodeEffectiveTimelineFrom under ctx's read
// budget.
func (t *TempOps) NodeEffectiveTimelineFromCtx(ctx context.Context, id types.NodeID, validFrom, pin types.Instant) ([]temporal.NodeSegment, error) {
	return t.nodeEffectiveTimelineCtx(ctx, "NodeEffectiveTimelineFrom", id, validFrom, pin)
}

func (t *TempOps) nodeEffectiveTimelineCtx(ctx context.Context, door string, id types.NodeID, validFrom, pin types.Instant) ([]temporal.NodeSegment, error) {
	c := t.c
	meter, err := c.beginTimeline(ctx, door)
	if err != nil {
		return nil, err
	}
	if meter == nil {
		return t.nodeEffectiveTimeline(id, validFrom, pin)
	}
	pin = recordedPin(pin) // a pin <= 0: nothing recorded yet (round 11)
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if err := storepkg.ValidateNodeID(id); err != nil {
		return nil, err
	}
	if err := c.validateEffectivePin(pin); err != nil {
		return nil, err
	}
	if err := meter.AddCandidates(door, 1); err != nil {
		return nil, err
	}
	var out []temporal.NodeSegment
	err = c.readUnderRLock(func() error {
		rd, release := c.meteredEntityReads(meter.Tally(door))
		defer release()
		var err error
		out, err = c.nodeEffectiveTimelineTail(rd, id, validFrom, pin)
		return err
	})
	if err != nil {
		return nil, err
	}
	if err := chargeSegments(meter, door, len(out), nodeSegmentSize); err != nil {
		return nil, err
	}
	return out, nil
}

// RelEffectiveTimelineCtx is RelEffectiveTimeline under ctx's read budget.
func (t *TempOps) RelEffectiveTimelineCtx(ctx context.Context, id types.RelID, pin types.Instant) ([]temporal.RelSegment, error) {
	const door = "RelEffectiveTimeline"
	c := t.c
	meter, err := c.beginTimeline(ctx, door)
	if err != nil {
		return nil, err
	}
	if meter == nil {
		return t.RelEffectiveTimeline(id, pin)
	}
	pin = recordedPin(pin)
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if err := storepkg.ValidateRelID(id); err != nil {
		return nil, err
	}
	if err := c.validateEffectivePin(pin); err != nil {
		return nil, err
	}
	if err := meter.AddCandidates(door, 1); err != nil {
		return nil, err
	}
	var out []temporal.RelSegment
	err = c.readUnderRLock(func() error {
		rd, release := c.meteredEntityReads(meter.Tally(door))
		defer release()
		if err := c.checkRelTypePointRetentionRd(rd, id, pin); err != nil {
			return err
		}
		var err error
		out, err = c.relEffectiveTimelineFrom(rd, id, pin)
		return err
	})
	if err != nil {
		return nil, err
	}
	if err := chargeSegments(meter, door, len(out), relSegmentSize); err != nil {
		return nil, err
	}
	return out, nil
}

// NodeEffectiveTimelinesCtx is NodeEffectiveTimelines under ctx's read budget:
// one candidate, one walk of the node's chain, and every segment of every pin's
// answer counted before the result is returned.
func (t *TempOps) NodeEffectiveTimelinesCtx(ctx context.Context, id types.NodeID, pins []types.Instant) ([][]temporal.NodeSegment, error) {
	const door = "NodeEffectiveTimelines"
	c := t.c
	meter, err := c.beginTimeline(ctx, door)
	if err != nil {
		return nil, err
	}
	if meter == nil {
		return t.NodeEffectiveTimelines(id, pins)
	}
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
	if err := meter.AddCandidates(door, 1); err != nil {
		return nil, err
	}
	var out [][]temporal.NodeSegment
	err = c.readUnderRLock(func() error {
		rd, release := c.meteredEntityReads(meter.Tally(door))
		defer release()
		var err error
		out, _, err = c.nodeEffectiveTimelinesFrom(rd, id, mapped, nil)
		return err
	})
	if err != nil {
		return nil, err
	}
	for _, segs := range out {
		if err := chargeSegments(meter, door, len(segs), nodeSegmentSize); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// RelEffectiveTimelinesCtx is RelEffectiveTimelines under ctx's read budget, as
// NodeEffectiveTimelinesCtx.
func (t *TempOps) RelEffectiveTimelinesCtx(ctx context.Context, id types.RelID, pins []types.Instant) ([][]temporal.RelSegment, error) {
	const door = "RelEffectiveTimelines"
	c := t.c
	meter, err := c.beginTimeline(ctx, door)
	if err != nil {
		return nil, err
	}
	if meter == nil {
		return t.RelEffectiveTimelines(id, pins)
	}
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
	if err := meter.AddCandidates(door, 1); err != nil {
		return nil, err
	}
	var out [][]temporal.RelSegment
	err = c.readUnderRLock(func() error {
		rd, release := c.meteredEntityReads(meter.Tally(door))
		defer release()
		if err := c.checkRelTypePointRetentionRd(rd, id, slices.Min(mapped)); err != nil {
			return err
		}
		var err error
		out, err = c.relEffectiveTimelinesFrom(rd, id, mapped)
		return err
	})
	if err != nil {
		return nil, err
	}
	for _, segs := range out {
		if err := chargeSegments(meter, door, len(segs), relSegmentSize); err != nil {
			return nil, err
		}
	}
	return out, nil
}
