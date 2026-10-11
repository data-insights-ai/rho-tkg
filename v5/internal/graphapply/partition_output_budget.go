package graphapply

import (
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func outputBudgetFailure(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, graphstate.ErrResourceLimit) {
		return errors.Join(errLimit, err)
	}
	return errors.Join(errInvalid, err)
}

// Decoders have already performed independent predecode commandOwnedBytes
// admission. Joining here charges retained decoded/backing ownership into the
// single staging/composition operation, without claiming predecode admission.
func joinGraphOutputBudget(q *reader, r graphRequest, decodedOwned int, l materializerLimits) error {
	if q.arena != nil {
		return errInvalid
	}
	parent, err := graphstate.NewOutputBudget(l.outputBytes)
	if err != nil {
		return outputBudgetFailure(err)
	}
	retained := 4096 + compositionMetadataBytes + decodedOwned + 4*cap(q.base.Image) + 64*cap(q.writes)
	for _, row := range q.writes {
		retained += cap(row.Key) + cap(row.Value)
	}
	for _, schema := range r.schemas {
		retained += len(schema.Name)
	}
	if err := parent.Reserve(retained); err != nil {
		return outputBudgetFailure(err)
	}
	q.arena = parent
	return nil
}
func reserveComposer(q *reader, n int) error {
	if q.arena == nil {
		return nil
	}
	return outputBudgetFailure(q.arena.Reserve(n))
}
func reserveCompositionWithBudget(q *reader, total *int, n, limit int) error {
	if n < 0 || n > limit-*total {
		return errLimit
	}
	if err := reserveComposer(q, n); err != nil {
		return err
	}
	*total += n
	return nil
}

// reserveLogicalEncoding admits the same codec/numeric owners used by storage,
// before either sizing/emission traversal. Fixed framing is a conservative
// actual-field bound; variable Scope/Scalar widths are inspected exactly.
func reserveLogicalEncoding(q *reader, changes graphChanges, l materializerLimits) error {
	if q.arena == nil {
		return nil
	}
	upper := 256
	add := func(n int) error {
		if n < 0 || n > q.arena.Remaining()-upper {
			return errLimit
		}
		upper += n
		return nil
	}
	scope := func(s temporal.Scope, calls int) error {
		info, err := s.EncodingBounds(l.catalog.Temporal)
		if err != nil {
			return encodingFailure(err)
		}
		if info.NumericBytes != 0 && calls > q.arena.Remaining()/info.NumericBytes {
			return errLimit
		}
		if err := reserveComposer(q, calls*(info.NumericBytes+2*info.WireBytes)); err != nil {
			return err
		}
		return add(info.WireBytes)
	}
	for _, schema := range changes.schemas {
		if err := add(64 + len(schema.Name)); err != nil {
			return err
		}
	}
	// Each actual axis can appear in the table once. Charging repeated known
	// declarations is conservative but does not clone their shared string backing.
	axis := func(a temporal.Axis) error {
		d := a.Descriptor()
		return add(128 + len(d.Reference) + len(d.CanonicalUnit))
	}
	for _, entity := range changes.entities {
		if err := axis(entity.Axis); err != nil {
			return err
		}
		if err := add(256 + len(entity.Type)); err != nil {
			return err
		}
	}
	if err := add(64 * len(changes.lives)); err != nil {
		return err
	}
	for _, value := range changes.values {
		key, err := q.arena.ScalarKey(value.Value, l.graph.Planner)
		if err != nil {
			return outputBudgetFailure(err)
		}
		// Both emitter traversals construct their own typed key; the inspection key
		// remains charged and is not transferred into an unowned codec buffer.
		if err := reserveComposer(q, 128+8*len(key)); err != nil {
			return err
		}
		if s, ok := value.Value.Scope(); ok {
			if err := scope(s, 2); err != nil {
				return err
			}
			if err := axis(s.Axis()); err != nil {
				return err
			}
		}
		if err := add(32 + len(key)); err != nil {
			return err
		}
	}
	for _, group := range changes.groups {
		if err := axis(group.Owned.Axis()); err != nil {
			return err
		}
		if err := add(128 + len(group.Key.Name)); err != nil {
			return err
		}
		if err := scope(group.Owned, 2); err != nil {
			return err
		}
		codec := state.CodecLimits{State: l.graph.Planner.Component, MaxEncodedBytes: l.changeBytes}
		for range 2 {
			if _, err := q.arena.ReserveChangeEncoding(group.Changes, codec); err != nil {
				return outputBudgetFailure(err)
			}
		}
		if err := add(56 + 72*len(group.Changes)); err != nil {
			return err
		}
		for _, change := range group.Changes {
			info, err := change.Scope().EncodingBounds(l.catalog.Temporal)
			if err != nil {
				return encodingFailure(err)
			}
			if err := add(info.WireBytes); err != nil {
				return err
			}
		}
	}
	if upper > q.arena.Remaining()/4 {
		return errLimit
	}
	if err := reserveComposer(q, 4*upper); err != nil {
		return err
	}
	return nil
}
