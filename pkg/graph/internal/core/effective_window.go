package core

import (
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Windowed effective reads by endpoint and their typed form (sigma-tkgd
// round 10 requests 1 and 2b). The label and type doors live beside their
// From siblings (effective_validfrom.go); these compose
// ForEachRelEffectiveAtNodes, so their segments are its segments, kept where
// they meet the window (temporal.Window.Meets).

// ForEachRelEffectiveAtNodesWindow is ForEachRelEffectiveAtNodes limited to
// the segments that meet window: for every valid instant t inside the window
// the streamed segments containing t are exactly ForEachRelEffectiveAtNodes'.
// A relationship's kept segments stream together, ascending, and need not be
// contiguous. Errors: ForEachRelEffectiveAtNodes', ErrInvalidTimeRange for a
// window that is not ascending and disjoint.
func (t *TempOps) ForEachRelEffectiveAtNodesWindow(nodeIDs []types.NodeID, dir temporal.Direction, relTypes []string,
	pin types.Instant, window temporal.Window, fn func(temporal.RelSegment) bool) error {
	return t.forEachRelEffectiveAtNodesWindowRun(nil, nodeIDs, dir, relTypes, recordedPin(pin), window, fn) // a pin <= 0: nothing recorded yet (round 11)
}

// forEachRelEffectiveAtNodesWindowRun is ForEachRelEffectiveAtNodesWindow's
// body under a read budget (run nil: none, the plain door; pin is already
// recordedPin's). Under a budget Segments counts the segments that meet the
// window, those handed to fn.
func (t *TempOps) forEachRelEffectiveAtNodesWindowRun(run *scanRun, nodeIDs []types.NodeID, dir temporal.Direction, relTypes []string,
	pin types.Instant, window temporal.Window, fn func(temporal.RelSegment) bool) error {
	if err := t.c.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return grapherr.ErrNilCallback
	}
	if err := window.Validate(); err != nil {
		return err
	}
	if len(window) == 0 {
		return t.c.relEffectiveAtNodesGates(nodeIDs, dir, relTypes, pin) // the empty window meets nothing
	}
	return t.c.forEachRelEffectiveAtNodesRun(run, nodeIDs, dir, relTypes, pin, func(s temporal.RelSegment) bool {
		return window.MeetsOn(t.c.axis(), s.ValidFrom, s.ValidTo)
	}, fn)
}

// ScanRelEffectiveColumnsAtNodes reads ForEachRelEffectiveAtNodesWindow's
// segments as typed relationship columns (store.RelColumnBatch through the
// shared row path, store.ScanColumnsFromRelSegments): one row per segment,
// its IDs, endpoints and the requested properties of the segment's row, and
// the segment's own valid span in ValidFrom / ValidTo (ValidTo 0: open), so
// a lookup by start or end node at a pin hands out typed values without a
// boxed row per value. Rows come in ForEachRelEffectiveAtNodesWindow's order,
// a relationship once per kept segment. Ordinals are 0 (a read coordinate
// numbers no row). RelType names the type when relTypes names exactly one,
// else it is empty and the rows mix the named types. A column whose rows
// hold integers and floats refuses with store.ErrMixedNumericColumn, as every
// column scan does. The segments are gathered before the first batch (the
// column kinds are decided over every row), so a call holds its rows in
// memory: read a high-degree seed set in parts. fn must not retain the batch.
func (t *TempOps) ScanRelEffectiveColumnsAtNodes(nodeIDs []types.NodeID, dir temporal.Direction, relTypes []string,
	pin types.Instant, window temporal.Window, props []string, fn func(*storepkg.RelColumnBatch) bool) error {
	return t.scanRelEffectiveColumnsAtNodesRun(nil, nodeIDs, dir, relTypes, recordedPin(pin), window, props, fn) // a pin <= 0: nothing recorded yet (round 11)
}

// segmentRowBytes is what a column scan holds per gathered segment (its
// relationship pointer and its valid span), the BufferBytes charged before
// each is held under a read budget.
const segmentRowBytes = bufSliceRef + 2*8

// scanRelEffectiveColumnsAtNodesRun is ScanRelEffectiveColumnsAtNodes' body
// under a read budget (run nil: none; pin is already recordedPin's): the
// window door's gather and counts, and each kept segment's segmentRowBytes
// counted before it is held, so a refusal comes before fn has seen a batch.
func (t *TempOps) scanRelEffectiveColumnsAtNodesRun(run *scanRun, nodeIDs []types.NodeID, dir temporal.Direction, relTypes []string,
	pin types.Instant, window temporal.Window, props []string, fn func(*storepkg.RelColumnBatch) bool) error {
	if err := t.c.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return grapherr.ErrNilCallback
	}
	var (
		rels     []*types.Relationship
		from, to []types.Instant
	)
	if err := t.forEachRelEffectiveAtNodesWindowRun(run, nodeIDs, dir, relTypes, pin, window, func(s temporal.RelSegment) bool {
		if err := run.holdBytes(segmentRowBytes); err != nil {
			run.refuse = err
			return false
		}
		rels, from, to = append(rels, s.Rel), append(from, s.ValidFrom), append(to, s.ValidTo)
		return true
	}); err != nil {
		return err
	}
	var name string
	if len(relTypes) == 1 {
		name = relTypes[0]
	}
	return storepkg.ScanColumnsFromRelSegments(rels, from, to, props, func(b *storepkg.RelColumnBatch) bool {
		b.RelType = name
		clear(b.Ordinals) // a read coordinate numbers no row
		clear(b.StartOrdinals)
		clear(b.EndOrdinals)
		return fn(b)
	})
}
