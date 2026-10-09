package state

import "github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"

// Slice restricts this component to window, preserving full cells, including
// null and explicit retraction revision/provenance. Gaps stay never-asserted
// absence. It neither creates revisions nor produces changes.
//
// The full source, window, working metadata and returned state must fit limits;
// a small result does not exempt a larger source from its policy. Change-output
// budgets are validated but not charged. MaxInputBytes applies to decoding,
// not to this operation on already materialized scopes.
//
// With the same/looser temporal policy, work is O(m log n + k) atomic operations
// plus exact coordinate/metadata bytes for m window atoms and k visited source
// atoms. Tighter temporal caps may first revalidate all n source atoms. Previous
// states are never modified. All may reuse immutable internal metadata, just
// like a copied State. Any refusal returns an uninitialized State, never partial
// output.
func (s State) Slice(window temporal.Scope, limits Limits) (State, error) {
	l, err := limits.resolved()
	if err != nil {
		return State{}, err
	}
	if err := s.check(l); err != nil {
		return State{}, err
	}
	if _, err := temporal.AppendScope(nil, window, l.Temporal); err != nil {
		return State{}, err
	}
	empty, err := temporal.Empty(s.axis)
	if err != nil {
		return State{}, err
	}
	// Pair validation precedes every fast path, including an empty source/window.
	if _, err := window.Overlaps(empty, l.Temporal); err != nil {
		return State{}, err
	}
	if window.Kind() == temporal.ScopeAll {
		s.policy = l
		return s, nil
	}
	out := stateBuilder{limits: l, usage: initialUsage(s.axis)}
	for _, part := range window.Parts() {
		// Seek by upper bound, skipping whole histories between disjoint windows.
		lo, hi := 0, len(s.pieces)
		for lo < hi {
			mid := lo + (hi-lo)/2
			before, err := scopeBefore(s.pieces[mid].scope, part, l.Temporal)
			if err != nil {
				return State{}, err
			}
			if before {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		for i := lo; i < len(s.pieces); i++ {
			piece := s.pieces[i]
			before, err := scopeBefore(part, piece.scope, l.Temporal)
			if err != nil {
				return State{}, err
			}
			if before {
				break
			}
			// Intersection normalizes a closed singleton before any successor
			// widening. No discarded tail is constructed just to clip an atom.
			common, err := piece.scope.Intersection(part, l.Temporal)
			if err != nil {
				return State{}, err
			}
			if common.Kind() != temporal.ScopeEmpty {
				if err := out.add(common, piece.cell); err != nil {
					return State{}, err
				}
			}
		}
	}
	if err := out.finish(); err != nil {
		return State{}, err
	}
	return State{axis: s.axis, pieces: out.parts, usage: out.usage, policy: l, valid: true}, nil
}
