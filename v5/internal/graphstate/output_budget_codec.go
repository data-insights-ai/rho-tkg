package graphstate

import (
	"cmp"

	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// ReserveStateEncoding admits both codec traversals, neighbor numeric scratch,
// staging and owned final wire before AppendState. It changes no State/policy.
func (b *OutputBudget) ReserveStateEncoding(s state.State, l state.CodecLimits) (int, error) {
	if b == nil {
		return 0, ErrInvalidInput
	}
	if err := l.Validate(); err != nil {
		return 0, err
	}
	count := s.Usage().Pieces()
	if count > cmp.Or(l.State.MaxPieces, state.DefaultLimits().MaxPieces) || count > (cmp.Or(l.MaxEncodedBytes, state.DefaultCodecLimits().MaxEncodedBytes)-56)/91 {
		return 0, state.ErrResourceLimit
	}
	if count > b.Remaining()/384 {
		return 0, ErrResourceLimit
	}
	if err := b.Reserve(384 * count); err != nil {
		return 0, err
	}
	scopes := make([]temporal.Scope, 0, count)
	for _, piece := range s.Pieces() {
		scopes = append(scopes, piece.Scope())
	}
	return b.reserveComponentEncoding(scopes, false, l)
}

// ReserveChangeEncoding admits exact change-codec backing/traversals. Returned
// bytes describe preadmitted wire scratch, never authority or a Go heap bound.
func (b *OutputBudget) ReserveChangeEncoding(changes []state.Change, l state.CodecLimits) (int, error) {
	if b == nil {
		return 0, ErrInvalidInput
	}
	if err := l.Validate(); err != nil {
		return 0, err
	}
	if len(changes) > cmp.Or(l.State.MaxChangePieces, state.DefaultLimits().MaxChangePieces) || len(changes) > (cmp.Or(l.MaxEncodedBytes, state.DefaultCodecLimits().MaxEncodedBytes)-56)/125 {
		return 0, state.ErrResourceLimit
	}
	if len(changes) > b.Remaining()/128 {
		return 0, ErrResourceLimit
	}
	if err := b.Reserve(128 * len(changes)); err != nil {
		return 0, err
	}
	scopes := make([]temporal.Scope, len(changes))
	for i, change := range changes {
		scopes[i] = change.Scope()
	}
	return b.reserveComponentEncoding(scopes, true, l)
}

// AppendState/AppendChanges make two atomic scope encoding passes, inspect
// neighbors once, stage one exact full envelope and own the returned wire.
// Reserve six wire lengths+256 portable fixed slots before either codec; it
// covers temporary atom wires, exact staging, and destination growth rounding.
// The admitted amount can cover the existing final-wire source accounting once;
// decoded State/scope ownership is a distinct materialize charge before decode.
func (b *OutputBudget) reserveComponentEncoding(scopes []temporal.Scope, changes bool, l state.CodecLimits) (int, error) {
	if b == nil {
		return 0, ErrInvalidInput
	}
	cells := 34
	if changes {
		cells = 68
	}
	wireBytes := 56
	var aggregate temporal.ScopeScratchBounds
	for i, scope := range scopes {
		info, err := scope.EncodingBounds(l.State.Temporal)
		if err != nil {
			return 0, err
		}
		if info.WireBytes > b.Remaining()-wireBytes-4-cells {
			return 0, ErrResourceLimit
		}
		wireBytes += 4 + cells + info.WireBytes
		if wireBytes > cmp.Or(l.MaxEncodedBytes, state.DefaultCodecLimits().MaxEncodedBytes) {
			return 0, state.ErrResourceLimit
		}
		if info.NumericBytes > b.Remaining()/2 {
			return 0, ErrResourceLimit
		}
		if err := b.Reserve(2 * info.NumericBytes); err != nil {
			return 0, err
		}
		bounds, err := scope.ScratchBounds(scope, l.State.Temporal)
		if err != nil {
			return 0, err
		}
		if i == 0 {
			aggregate = bounds
		} else {
			aggregate, err = aggregate.Merge(bounds)
			if err != nil {
				return 0, err
			}
		}
	}
	if len(scopes) > 1 {
		perNeighbor := 3*aggregate.ComparisonBytes + aggregate.SuccessorBytes
		if perNeighbor != 0 && len(scopes)-1 > b.Remaining()/perNeighbor {
			return 0, ErrResourceLimit
		}
		if err := b.Reserve((len(scopes) - 1) * perNeighbor); err != nil {
			return 0, err
		}
	}
	if wireBytes > (b.Remaining()-256)/6 {
		return 0, ErrResourceLimit
	}
	admitted := 256 + 6*wireBytes
	if err := b.Reserve(admitted); err != nil {
		return 0, err
	}
	return admitted, nil
}
