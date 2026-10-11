package graphstate

import (
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// No hidden provider capability: generic read-owned State metadata is admitted
// explicitly before defensive copies. Proven immutable scope/axis aliases do
// not clone their variable backing in Pieces; the 256-byte slots cover headers.
func (e *engine) pieces(s state.State) ([]state.Piece, error) {
	if e.budget != nil {
		if err := e.budget.Reserve(outputPieceSlotBytes * s.Usage().Pieces()); err != nil {
			return nil, err
		}
	}
	return s.Pieces(), nil
}
func (e *engine) parts(s temporal.Scope) ([]temporal.Scope, error) {
	if e.budget != nil {
		info, err := s.EncodingBounds(e.limits.Component.Temporal)
		if err != nil {
			return nil, err
		}
		// Parts owns both Scope headers and one interval array for every atom.
		if err := e.budget.Reserve(outputScopePartsBytes*info.Parts + info.NumericBytes); err != nil {
			return nil, err
		}
	}
	return s.Parts(), nil
}
func (e *engine) scopeWire(s temporal.Scope) ([]byte, error) {
	if e.budget != nil {
		return e.budget.ScopeBytes(s, e.limits.Component.Temporal)
	}
	return temporal.AppendScope(nil, s, e.limits.Component.Temporal)
}

// reserveReduction admits named reducer scratch/retained arrays before Set or
// Unset. It leaves the reducer unchanged and does not refund intermediate owners.
func (e *engine) reserveReduction(s state.State, window temporal.Scope) error {
	if e.budget == nil {
		return nil
	}
	pieces, err := e.pieces(s)
	if err != nil {
		return err
	}
	info, err := window.EncodingBounds(e.limits.Component.Temporal)
	if err != nil {
		return err
	}
	bounds, err := window.ScratchBounds(window, e.limits.Component.Temporal)
	if err != nil {
		return err
	}
	for _, p := range pieces {
		b, err := p.Scope().ScratchBounds(window, e.limits.Component.Temporal)
		if err != nil {
			return err
		}
		bounds, err = bounds.Merge(b)
		if err != nil {
			return err
		}
	}
	n, m := len(pieces), info.Parts
	if n == 0 {
		return e.reserveEmptyReduction(info, bounds)
	}
	iterations := n + m
	adds, changes := 3*iterations+n, 2*iterations
	// Each body iteration consumes an old or mutation atom. The tail emits <=n.
	// At most4 split Spans,1 intersection,1 SameSupport,3 state-builder adds and
	// 2 change-builder adds per body. Builder adds include merge/Span/measurement
	// and finalizeLast. The rounded call counts below also include State.check's
	// tightened-policy n encodes, the initial mutation encode/Overlaps(empty),
	// Parts classification, final builder finish and neighbor checks.
	terms := []struct{ count, bytes int }{
		{112*iterations + 16*n + 8*m + 8, bounds.ComparisonBytes},
		{32*iterations + 4*n + 2*m + 4, bounds.SuccessorBytes},
		{8*iterations + 3*n + 3*m + 4, bounds.AtomicEncodingBytes},
		{8*iterations + 3*n + 3*m + 4, 2 * bounds.AtomicWireBytes},
		// append growth+retired arrays: <=8 times appended slots (portable capacity
		// rounding allowance); Result.Changes' later defensive copy is separate.
		{8 * adds, 256}, {8 * changes, 384},
		{m, 512},
		// Temporary atomic Scope/interval arrays made by split, intersection and
		// builder merge; input coordinates remain shared immutable values.
		{5*iterations + adds, 512},
	}
	for _, term := range terms {
		if term.count < 0 || term.bytes < 0 || term.bytes != 0 && term.count > e.budget.Remaining()/term.bytes {
			return ErrResourceLimit
		}
		if err := e.budget.Reserve(term.count * term.bytes); err != nil {
			return err
		}
	}
	return nil
}
func (e *engine) reduce(s state.State, scope temporal.Scope, cell state.Cell) (state.Result, error) {
	if err := e.reserveReduction(s, scope); err != nil {
		return state.Result{}, err
	}
	return applyCell(s, scope, cell, e.limits.Component)
}

// appendOwned admits the fresh typed header array before append growth. Variable
// immutable backing belongs to the originating input/read/codec reservation.
func appendOwned[T any](e *engine, out []T, value T, slotBytes int) ([]T, error) {
	if e.budget == nil {
		return append(out, value), nil
	}
	capacity, err := e.budget.ReserveSlice(len(out), cap(out), slotBytes)
	if err != nil {
		return nil, err
	}
	if capacity > cap(out) {
		next := make([]T, len(out), capacity)
		copy(next, out)
		out = next
	}
	return append(out, value), nil
}
func (e *engine) reserveMap(entries int, capacity *int, slots int) error {
	if e.budget == nil {
		return nil
	}
	var err error
	*capacity, err = e.budget.ReserveMap(entries, *capacity, slots)
	return err
}
func (e *engine) reservePredicate(a, b temporal.Scope) error {
	if e.budget == nil {
		return nil
	}
	x, err := a.EncodingBounds(e.limits.Component.Temporal)
	if err != nil {
		return err
	}
	y, err := b.EncodingBounds(e.limits.Component.Temporal)
	if err != nil {
		return err
	}
	bound, err := a.ScratchBounds(b, e.limits.Component.Temporal)
	if err != nil {
		return err
	}
	visits := x.Parts + y.Parts
	// Both input validation walks/classifications plus at most visits intersections
	// (two selections, four normalization comparisons and three successors).
	for _, term := range []struct{ count, bytes int }{{16*visits + 4, bound.ComparisonBytes}, {3 * visits, bound.SuccessorBytes}, {visits, 512}} {
		if term.bytes != 0 && term.count > e.budget.Remaining()/term.bytes {
			return ErrResourceLimit
		}
		if err := e.budget.Reserve(term.count * term.bytes); err != nil {
			return err
		}
	}
	return nil
}

func (e *engine) scalarEqual(a, b Scalar) (bool, error) {
	if e.budget == nil {
		return a.Equal(b, e.limits)
	}
	left, err := e.budget.ScalarKey(a, e.limits)
	if err != nil {
		return false, err
	}
	right, err := e.budget.ScalarKey(b, e.limits)
	if err != nil {
		return false, err
	}
	return left == right, nil
}

// With no old pieces, cursor.ok stays false: each canonical mutation atom is
// emitted once, without split/intersection/tail branches. AppendScope has proved
// canonical support before those emissions, so stateBuilder cannot merge atoms.
// Neighbor mergeable checks still run and are admitted separately, including
// QN model comparisons and successor attempts; no reservation is refunded.
func (e *engine) reserveEmptyReduction(info temporal.ScopeEncodingBounds, bounds temporal.ScopeScratchBounds) error {
	m := info.Parts
	terms := []struct{ count, bytes int }{
		// One whole-scope encode; validation/Overlaps(empty)/Parts classification;
		// and three atomic measured encodes (out.add/finalizeLast/change.add).
		// A whole canonical encoding bounds the sum of its atomic encodings.
		{5, info.NumericBytes},
		{3 * max(0, m-1), bounds.ComparisonBytes},
		{max(0, m-1), bounds.SuccessorBytes},
		// Actual array owners: mutation Parts plus appended Piece/Change slots
		// including portable growth and retained old backing. No split arrays.
		{m, outputScopePartsBytes}, {8 * m, outputPieceSlotBytes}, {8 * m, 384},
		{6 * m, bounds.AtomicWireBytes}, {2, info.WireBytes},
	}
	for _, term := range terms {
		if term.count < 0 || term.bytes < 0 || term.bytes != 0 && term.count > e.budget.Remaining()/term.bytes {
			return ErrResourceLimit
		}
		if err := e.budget.Reserve(term.count * term.bytes); err != nil {
			return err
		}
	}
	return nil
}
