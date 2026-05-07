package temporal

// TemporalConstraintKind identifies the type of temporal constraint enforced at write time.
type TemporalConstraintKind uint8

const (
	// ConstraintRelWithinEndpoints enforces that a relationship's validity window
	// is contained within the validity intervals of both endpoint nodes.
	//
	// Checks at write time (r = freshly-minted relationship):
	//   (1) relFrom >= startNode.effectiveValidFrom
	//   (2) relFrom >= endNode.effectiveValidFrom
	//   (3) if startNode.ValidTo != 0: relFrom < startNode.ValidTo
	//   (4) if endNode.ValidTo   != 0: relFrom < endNode.ValidTo
	//   (5) if r.Temporal().ValidTo != 0 AND startNode.ValidTo != 0:
	//           r.ValidTo <= startNode.ValidTo
	//   (6) same for endNode
	ConstraintRelWithinEndpoints TemporalConstraintKind = iota + 1
)

// TemporalConstraint is a single temporal invariant checked at write time.
type TemporalConstraint struct {
	Kind TemporalConstraintKind
}

// ConstraintSet is an immutable-by-convention ordered set of TemporalConstraints.
// The zero value is valid (empty — no constraints).
// Add returns a new copy; the original is never modified.
type ConstraintSet struct {
	items []TemporalConstraint
}

// NewConstraintSet creates a ConstraintSet from the given constraints.
func NewConstraintSet(cs ...TemporalConstraint) ConstraintSet {
	items := make([]TemporalConstraint, len(cs))
	copy(items, cs)
	return ConstraintSet{items: items}
}

// Add returns a new ConstraintSet with c appended. The receiver is not modified.
func (cs ConstraintSet) Add(c TemporalConstraint) ConstraintSet {
	items := make([]TemporalConstraint, len(cs.items)+1)
	copy(items, cs.items)
	items[len(cs.items)] = c
	return ConstraintSet{items: items}
}

// Len returns the number of constraints in the set.
func (cs ConstraintSet) Len() int {
	return len(cs.items)
}

// Items returns a defensive copy of the constraint slice.
// Returns nil if the set is empty.
func (cs ConstraintSet) Items() []TemporalConstraint {
	if len(cs.items) == 0 {
		return nil
	}
	out := make([]TemporalConstraint, len(cs.items))
	copy(out, cs.items)
	return out
}

// ForEach calls fn for each constraint without allocating a copy of the slice.
// Returns the first non-nil error returned by fn, stopping iteration.
func (cs ConstraintSet) ForEach(fn func(TemporalConstraint) error) error {
	for _, c := range cs.items {
		if err := fn(c); err != nil {
			return err
		}
	}
	return nil
}
