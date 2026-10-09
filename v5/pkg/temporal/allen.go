package temporal

// AllenRelation is one of the thirteen mutually exclusive endpoint-order
// relations between two finite proper intervals. It is not a membership test.
type AllenRelation uint8

// The Allen relations classify endpoint order for supplied proper intervals.
const (
	AllenBefore AllenRelation = iota + 1
	AllenMeets
	AllenOverlaps
	AllenStarts
	AllenDuring
	AllenFinishes
	AllenEquals
	AllenAfter
	AllenMetBy
	AllenOverlappedBy
	AllenStartedBy
	AllenContains
	AllenFinishedBy
)

// ClassifyAllen classifies supplied finite endpoints independently of boundary
// membership and region canonicalization. Each start must precede its end;
// points, infinities, unplaced values and relation networks are outside this API.
func ClassifyAllen(aStart, aEnd, bStart, bEnd Position, l Limits) (AllenRelation, error) {
	l, err := l.resolved()
	if err != nil {
		return 0, err
	}
	for _, p := range []Position{aStart, aEnd, bStart, bEnd} {
		if err := p.validate(l); err != nil {
			return 0, err
		}
		if err := sameAxis(aStart.axis, p.axis); err != nil {
			return 0, err
		}
		if positionWireBytes(p) > l.MaxValueBytes {
			return 0, ErrResourceLimit
		}
	}
	ae, _ := ComparePositions(aStart, aEnd, l)
	be, _ := ComparePositions(bStart, bEnd, l)
	if ae != Less || be != Less {
		return 0, ErrImproperInterval
	}
	endStart, _ := ComparePositions(aEnd, bStart, l)
	if endStart == Less {
		return AllenBefore, nil
	}
	if endStart == Equal {
		return AllenMeets, nil
	}
	startEnd, _ := ComparePositions(aStart, bEnd, l)
	if startEnd == Greater {
		return AllenAfter, nil
	}
	if startEnd == Equal {
		return AllenMetBy, nil
	}
	starts, _ := ComparePositions(aStart, bStart, l)
	ends, _ := ComparePositions(aEnd, bEnd, l)
	switch starts {
	case Less:
		switch ends {
		case Less:
			return AllenOverlaps, nil
		case Equal:
			return AllenFinishedBy, nil
		default:
			return AllenContains, nil
		}
	case Equal:
		switch ends {
		case Less:
			return AllenStarts, nil
		case Equal:
			return AllenEquals, nil
		default:
			return AllenStartedBy, nil
		}
	default:
		switch ends {
		case Less:
			return AllenDuring, nil
		case Equal:
			return AllenFinishes, nil
		default:
			return AllenOverlappedBy, nil
		}
	}
}
