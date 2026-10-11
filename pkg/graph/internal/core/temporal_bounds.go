package core

import (
	"math"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func finite(v types.Instant) types.TemporalEndpoint {
	return types.TemporalEndpoint{Value: v, Finite: true}
}
func legacyEnd(v types.Instant) types.TemporalEndpoint {
	return types.TemporalEndpoint{Value: v, Finite: v != 0}
}
func capEndpoint(end, cap types.TemporalEndpoint) types.TemporalEndpoint {
	if cap.Finite && (!end.Finite || cap.Value < end.Value) {
		return cap
	}
	return end
}

// The minimum representable instant is the ordering key of an unbounded start;
// endpoint presence remains in the range, so it is never a date substitution.
func startKey(e types.TemporalEndpoint) types.Instant {
	if !e.Finite {
		return types.Instant(math.MinInt64)
	}
	return e.Value
}

func ownRange(start types.Instant, tm *types.TemporalMetadata) types.TemporalRange {
	r := types.TemporalRange{From: finite(start)}
	if tm != nil {
		if explicit, ok := tm.ValidBounds(); ok {
			return explicit
		}
		r.To = legacyEnd(tm.ValidTo)
	}
	return r
}
func (c *Core) nodeOwnRange(n *types.Node) types.TemporalRange {
	return ownRange(c.nodeSortValidFrom(n), n.Temporal())
}
func (c *Core) relOwnRange(r *types.Relationship) types.TemporalRange {
	return ownRange(c.relSortValidFrom(r), r.Temporal())
}

func versionRange(from, to types.Instant, tm, next *types.TemporalMetadata) types.TemporalRange {
	r := types.TemporalRange{From: finite(from), To: legacyEnd(to)}
	if next != nil && next.HasValidBounds() {
		n, _ := next.ValidBounds()
		if n.From.Finite && n.From.Value == 0 && to == 0 {
			r.To = finite(0)
		}
	}
	if tm != nil && tm.HasValidBounds() {
		explicit, _ := tm.ValidBounds()
		r.From = explicit.From
		if explicit.To.Finite {
			r.To = explicit.To
		}
	}
	return r
}
func (c *Core) nodeVersionRange(chain []*types.Node, i int) types.TemporalRange {
	f, t := c.nodeVersionBounds(chain, i)
	var next *types.TemporalMetadata
	if i+1 < len(chain) {
		next = chain[i+1].Temporal()
	}
	return versionRange(f, t, chain[i].Temporal(), next)
}
func (c *Core) relVersionRange(chain []*types.Relationship, i int) types.TemporalRange {
	f, t := c.relVersionBounds(chain, i)
	var next *types.TemporalMetadata
	if i+1 < len(chain) {
		next = chain[i+1].Temporal()
	}
	return versionRange(f, t, chain[i].Temporal(), next)
}

func (m lifeEnds[T]) bounds(row T, r types.TemporalRange) types.TemporalRange {
	if end, ok := m[row]; ok {
		r.To = capEndpoint(r.To, finite(end))
	}
	return r
}

func legacyWindow(start, end types.Instant) types.TemporalRange {
	return types.TemporalRange{From: finite(start), To: finite(end)}
}
