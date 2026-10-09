package state

import (
	"slices"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// State is one immutable component's normalized atomic pieces. Equal adjacent
// cells merge only when payload, presence, revision and provenance all match.
// Copies share immutable metadata; no operation modifies a previous State.
type State struct {
	axis   temporal.Axis
	pieces []Piece
	usage  Usage
	policy Limits
	valid  bool
}

const ledgerHeaderBytes = 53        // Fixed component/CDC envelope, excluding its shared axis definition.
const axisDefinitionFixedBytes = 27 // AxisID(16),profile(1),version(2),two length prefixes(8).
const cellMetadataBytes = 34        // presence(1),value tag(1),ID/size/revision/provenance(4*8).

func initialUsage(axis temporal.Axis) Usage {
	descriptor := axis.Descriptor()
	return Usage{metadataBytes: ledgerHeaderBytes + axisDefinitionFixedBytes + len(descriptor.Reference) + len(descriptor.CanonicalUnit)}
}

// New constructs never-asserted empty state on a checked axis. The same reducer
// can serve a property, label or life component; graph constraints are external.
func New(axis temporal.Axis, l Limits) (State, error) {
	l, err := l.resolved()
	if err != nil {
		return State{}, err
	}
	empty, err := temporal.Empty(axis)
	if err != nil {
		return State{}, err
	}
	if _, err := temporal.AppendScope(nil, empty, l.Temporal); err != nil {
		return State{}, err
	}
	s := State{axis: axis, usage: initialUsage(axis), policy: l, valid: true}
	if err := s.usage.check(l, false); err != nil {
		return State{}, err
	}
	return s, nil
}

// Axis returns the component's immutable axis.
func (s State) Axis() temporal.Axis { return s.axis }

// Pieces returns a defensive copy of bounded atomic metadata (never payloads).
func (s State) Pieces() []Piece { return slices.Clone(s.pieces) }

// Usage returns its conservative metadata/reference ledger.
func (s State) Usage() Usage            { return s.usage }
func cloneChanges(in []Change) []Change { return slices.Clone(in) }

func (s State) check(l Limits) error {
	if !s.valid {
		return ErrInvalidState
	}
	if err := s.usage.check(l, false); err != nil {
		return err
	}
	if temporalPolicyCovers(l.Temporal, s.policy.Temporal) {
		return nil
	}
	// A tighter policy revalidates actual scopes instead of rejecting merely
	// because the prior admitted policy was wider. Ordinary lookups skip this.
	empty, err := temporal.Empty(s.axis)
	if err != nil {
		return err
	}
	if _, err := temporal.AppendScope(nil, empty, l.Temporal); err != nil {
		return err
	}
	for _, p := range s.pieces {
		if _, err := temporal.AppendScope(nil, p.scope, l.Temporal); err != nil {
			return err
		}
	}
	return nil
}

// At returns exact component state, including retraction provenance. With the
// same/looser temporal caps it checks cached budgets in O(1), then searches in
// O(log n) and checks one atomic scope. Tighter policies may revalidate O(n)
// metadata bytes; a fitting state is accepted even if its old policy was wider.
func (s State) At(p temporal.Position, l Limits) (Cell, error) {
	l, err := l.resolved()
	if err != nil {
		return Cell{}, err
	}
	if err := s.check(l); err != nil {
		return Cell{}, err
	}
	empty, err := temporal.Empty(s.axis)
	if err != nil {
		return Cell{}, err
	}
	if _, err := empty.Contains(p, l.Temporal); err != nil {
		return Cell{}, err
	}
	lo, hi := 0, len(s.pieces)
	for lo < hi {
		mid := lo + (hi-lo)/2
		lower, _, _ := s.pieces[mid].scope.Bounds()
		eligible, err := lowerAllows(lower, p, l.Temporal)
		if err != nil {
			return Cell{}, err
		}
		if eligible {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo == 0 {
		return Cell{}, nil
	}
	piece := s.pieces[lo-1]
	contains, err := piece.scope.Contains(p, l.Temporal)
	if err != nil {
		return Cell{}, err
	}
	if !contains {
		return Cell{}, nil
	}
	return piece.cell, nil
}
func lowerAllows(b temporal.Bound, p temporal.Position, l temporal.Limits) (bool, error) {
	if b.Kind() == temporal.BoundNegativeInfinity {
		return true, nil
	}
	v, _ := b.Position()
	order, err := temporal.ComparePositions(v, p, l)
	return order == temporal.Less || order == temporal.Equal && b.Inclusive(), err
}
