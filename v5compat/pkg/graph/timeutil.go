package graph

import (
	"math"
	"slices"

	v5 "github.com/data-insights-ai/rho-tkg/v5/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	v5t "github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

const (
	negInf = math.MinInt64 // -infinity (v4 UnboundedValidFrom)
	posInf = math.MaxInt64 // +infinity (v4 open end 0)
)

// span is a valid-time interval [lo,hi) in ms; negInf/posInf are the infinities.
type span struct{ lo, hi int64 }

func (s span) empty() bool           { return s.lo >= s.hi }
func (s span) contains(t int64) bool { return s.lo <= t && (s.hi == posInf || t < s.hi) }
func (s span) overlaps(o span) bool  { return max(s.lo, o.lo) < min(s.hi, o.hi) }
func (s span) intersect(o span) span { return span{max(s.lo, o.lo), min(s.hi, o.hi)} }
func fullSpan() span                 { return span{negInf, posInf} }

// spanSet is a normalised (sorted, disjoint, non-adjacent) list of spans.
type spanSet []span

func normSpans(in []span) spanSet {
	s := slices.DeleteFunc(slices.Clone(in), span.empty)
	slices.SortFunc(s, func(a, b span) int {
		switch {
		case a.lo < b.lo:
			return -1
		case a.lo > b.lo:
			return 1
		}
		return 0
	})
	var out spanSet
	for _, x := range s {
		if n := len(out); n > 0 && x.lo <= out[n-1].hi {
			out[n-1].hi = max(out[n-1].hi, x.hi)
		} else {
			out = append(out, x)
		}
	}
	return out
}

func (s spanSet) add(x span) spanSet { return normSpans(append(slices.Clone(s), x)) }

func (s spanSet) subtract(x span) spanSet {
	var out []span
	for _, y := range s {
		if !y.overlaps(x) {
			out = append(out, y)
			continue
		}
		out = append(out, span{y.lo, x.lo}, span{x.hi, y.hi})
	}
	return normSpans(out)
}

func (s spanSet) intersect(x span) spanSet {
	var out []span
	for _, y := range s {
		out = append(out, y.intersect(x))
	}
	return normSpans(out)
}

func (s spanSet) overlaps(x span) bool {
	for _, y := range s {
		if y.overlaps(x) {
			return true
		}
	}
	return false
}

// hull is the smallest span covering the set.
func (s spanSet) hull() span {
	if len(s) == 0 {
		return span{}
	}
	return span{s[0].lo, s[len(s)-1].hi}
}

// axes are the compat graph's fixed axes (valid time = default axis, knowledge
// axis unused until the evidence filter, WP-30).
type axes struct {
	cfg v5.Config
}

func (a axes) axis() temporal.Axis { return a.cfg.DefaultAxis.Axis() }

func (a axes) bound(ms int64, inclusive bool) temporal.Bound {
	p, err := v5t.InstantPosition(a.axis(), v5t.Instant(ms), a.cfg.Limits)
	if err != nil {
		panic(err) // every int64 ms fits the configured axis
	}
	b, err := temporal.FiniteBound(p, inclusive)
	if err != nil {
		panic(err)
	}
	return b
}

// scope maps a span to a v5 scope: [lo,hi) with the infinities open.
func (a axes) scope(s span) temporal.Scope {
	if s.lo == negInf && s.hi == posInf {
		sc, err := temporal.All(a.axis())
		if err != nil {
			panic(err)
		}
		return sc
	}
	lo, hi := temporal.NegativeInfinity(), temporal.PositiveInfinity()
	if s.lo != negInf {
		lo = a.bound(s.lo, true)
	}
	if s.hi != posInf {
		hi = a.bound(s.hi, false)
	}
	sc, err := temporal.Span(a.axis(), lo, hi, a.cfg.Limits)
	if err != nil {
		panic(err)
	}
	return sc
}

func (a axes) point(ms int64) temporal.Scope {
	p, err := v5t.InstantPosition(a.axis(), v5t.Instant(ms), a.cfg.Limits)
	if err != nil {
		panic(err)
	}
	sc, err := temporal.Point(p)
	if err != nil {
		panic(err)
	}
	return sc
}

func (a axes) known() temporal.Scope {
	sc, err := temporal.All(a.cfg.KnowledgeAxis.Axis())
	if err != nil {
		panic(err)
	}
	return sc
}

func (a axes) ms(b temporal.Bound, inf int64) int64 {
	p, ok := b.Position()
	if !ok {
		return inf
	}
	i, err := v5t.InstantFromPosition(p, a.axis(), a.cfg.Limits)
	if err != nil {
		return inf
	}
	return int64(i)
}

func (a axes) spanOf(sc temporal.Scope) span {
	lo, hi, _ := sc.Bounds()
	return span{a.ms(lo, negInf), a.ms(hi, posInf)}
}

// v4 instant of a span end: 0 for an open end (hi) or unset start (lo).
func loInstant(s span) int64 {
	if s.lo == negInf {
		return 0
	}
	return s.lo
}
func hiInstant(s span) int64 {
	if s.hi == posInf {
		return 0
	}
	return s.hi
}
