package importmap

import (
	"math"
	"sort"
)

// overlay is a naive valid-time model of one entity under a stream of ops:
// disjoint segments [lo, hi) with the properties and labels that hold there.
// The mapper uses it to diff the state a v4 row asserts against what the ops
// so far produced; the replay test uses it to check a whole stream against
// v4's effective timelines. It knows no knowledge time: callers apply the
// ops recorded up to a pin, in stream order.
type overlay struct{ segs []oseg }

type oseg struct {
	lo, hi int64
	props  map[string]PropValue
	labels map[string]bool
}

func (e Edge) coord() int64 {
	switch e.Inf {
	case -1:
		return math.MinInt64
	case 1:
		return math.MaxInt64
	}
	return e.MS
}

func (s *oseg) clone(lo, hi int64) oseg {
	c := oseg{lo: lo, hi: hi, props: map[string]PropValue{}, labels: map[string]bool{}}
	for k, v := range s.props {
		c.props[k] = v
	}
	for k := range s.labels {
		c.labels[k] = true
	}
	return c
}

// split makes lo and hi segment boundaries.
func (o *overlay) split(at int64) {
	for i, s := range o.segs {
		if s.lo < at && at < s.hi {
			l, r := s.clone(s.lo, at), s.clone(at, s.hi)
			o.segs = append(o.segs[:i], append([]oseg{l, r}, o.segs[i+1:]...)...)
			return
		}
	}
}

// each applies f to the segments inside [lo, hi); with fill it first creates
// empty segments in the gaps.
func (o *overlay) each(lo, hi int64, fill bool, f func(*oseg)) {
	if lo >= hi {
		return
	}
	o.split(lo)
	o.split(hi)
	if fill {
		cur := lo
		var gaps []oseg
		for _, s := range o.segs {
			if s.hi <= lo || s.lo >= hi {
				continue
			}
			if s.lo > cur {
				gaps = append(gaps, oseg{lo: cur, hi: s.lo, props: map[string]PropValue{}, labels: map[string]bool{}})
			}
			cur = s.hi
		}
		if cur < hi {
			gaps = append(gaps, oseg{lo: cur, hi: hi, props: map[string]PropValue{}, labels: map[string]bool{}})
		}
		o.segs = append(o.segs, gaps...)
		sort.Slice(o.segs, func(i, j int) bool { return o.segs[i].lo < o.segs[j].lo })
	}
	for i := range o.segs {
		if o.segs[i].lo >= lo && o.segs[i].hi <= hi {
			f(&o.segs[i])
		}
	}
}

func (o *overlay) cut(lo, hi int64) {
	if lo >= hi {
		return
	}
	o.split(lo)
	o.split(hi)
	kept := o.segs[:0:0]
	for _, s := range o.segs {
		if s.lo >= lo && s.hi <= hi {
			continue
		}
		kept = append(kept, s)
	}
	o.segs = kept
}

// apply changes the model as op does. End cuts everything from the scope's
// upper bound on; Retract removes the scope.
func (o *overlay) apply(op Op) {
	lo, hi := op.Scope.From.coord(), op.Scope.To.coord()
	switch op.Op {
	case "create_node", "create_rel":
		o.each(lo, hi, true, func(s *oseg) {
			s.props, s.labels = map[string]PropValue{}, map[string]bool{}
			for k, v := range op.Props {
				s.props[k] = v
			}
			for _, l := range op.Labels {
				s.labels[l] = true
			}
		})
	case "correct":
		o.each(lo, hi, true, func(s *oseg) {
			for k, v := range op.Props {
				s.props[k] = v
			}
		})
	case "unset":
		o.each(lo, hi, false, func(s *oseg) { delete(s.props, op.Key) })
	case "add_label":
		o.each(lo, hi, false, func(s *oseg) { s.labels[op.Label] = true })
	case "remove_label":
		o.each(lo, hi, false, func(s *oseg) { delete(s.labels, op.Label) })
	case "retract":
		o.cut(lo, hi)
	case "end":
		o.cut(hi, math.MaxInt64)
	}
}

// at returns the segment holding valid time v.
func (o *overlay) at(v int64) *oseg {
	for i := range o.segs {
		if o.segs[i].lo <= v && v < o.segs[i].hi {
			return &o.segs[i]
		}
	}
	return nil
}

// over lists the segments that intersect [lo, hi).
func (o *overlay) over(lo, hi int64) []*oseg {
	var out []*oseg
	for i := range o.segs {
		if o.segs[i].lo < hi && lo < o.segs[i].hi {
			out = append(out, &o.segs[i])
		}
	}
	return out
}
