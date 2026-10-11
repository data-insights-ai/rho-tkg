package types

import "cmp"

// RelateRanges classifies exact finite/unbounded intervals. Unlike scalar
// RelateOpen, zero is a finite coordinate when its endpoint says so.
func RelateRanges(a, b TemporalRange) (AllenRelation, error) {
	if a.Validate() != nil || b.Validate() != nil {
		return 0, ErrInvalidInterval
	}
	endStart := func(e, s TemporalEndpoint) int {
		if !e.Finite || !s.Finite {
			return 1
		}
		return cmp.Compare(e.Value, s.Value)
	}
	starts := func(x, y TemporalEndpoint) int {
		if !x.Finite && !y.Finite {
			return 0
		}
		if !x.Finite {
			return -1
		}
		if !y.Finite {
			return 1
		}
		return cmp.Compare(x.Value, y.Value)
	}
	ends := func(x, y TemporalEndpoint) int {
		if !x.Finite && !y.Finite {
			return 0
		}
		if !x.Finite {
			return 1
		}
		if !y.Finite {
			return -1
		}
		return cmp.Compare(x.Value, y.Value)
	}
	ae, be := endStart(a.To, b.From), endStart(b.To, a.From)
	s, e := starts(a.From, b.From), ends(a.To, b.To)
	switch {
	case ae < 0:
		return Before, nil
	case be < 0:
		return After, nil
	case ae == 0:
		return Meets, nil
	case be == 0:
		return MetBy, nil
	case s == 0 && e == 0:
		return Equals, nil
	case s == 0 && e < 0:
		return Starts, nil
	case s == 0:
		return StartedBy, nil
	case e == 0 && s > 0:
		return Finishes, nil
	case e == 0:
		return FinishedBy, nil
	case s < 0 && e < 0:
		return Overlaps, nil
	case s < 0:
		return Contains, nil
	case e > 0:
		return OverlappedBy, nil
	default:
		return During, nil
	}
}
