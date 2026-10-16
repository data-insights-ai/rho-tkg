package graphstate

import (
	"context"
	"fmt"

	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// UniqueConflict is the first pair of owners found holding an equal value of a
// unique property over overlapping effective time. It names no value.
type UniqueConflict struct {
	Name string
	A, B EntityID
}

func (c *UniqueConflict) Error() string {
	return fmt.Sprintf("%v: property %q held by entities %d and %d", ErrUniqueOverlap, c.Name, c.A, c.B)
}

// Is makes a conflict match ErrUniqueOverlap.
func (c *UniqueConflict) Is(target error) bool { return target == ErrUniqueOverlap }

// CheckUniqueClaims verifies that the effective claims of the scalar property d
// on the given component keys (every ScalarProperty key of that name, any
// owner) hold no equal value in two owners over overlapping time, with the same
// life-support masking Plan applies to a write. It reads only through v and so
// works before d is declared unique: a declaration validates its existing data
// with it. The result is a *UniqueConflict (matches ErrUniqueOverlap) or nil.
func CheckUniqueClaims(ctx context.Context, v ReadView, d PropertyDefinition, keys []ComponentKey, l Limits) error {
	l, err := l.resolve()
	if err != nil {
		return err
	}
	e, err := start(ctx, v, l, state.Revision{})
	if err != nil {
		return err
	}
	type claim struct {
		owner EntityID
		scope temporal.Scope
		value Scalar
	}
	byValue := map[string][]claim{}
	for _, key := range keys {
		if key.Kind != ScalarProperty || key.Name != d.Name {
			return ErrInvalidInput
		}
		owner, found, err := e.entity(key.Owner)
		if err != nil {
			return err
		}
		if !found {
			return ErrContradictoryRead
		}
		window, err := temporal.All(owner.Axis)
		if err != nil {
			return err
		}
		own, err := e.claims(key, window)
		if err != nil {
			return err
		}
		for _, c := range own {
			k, err := c.value.EqualityKey(e.limits)
			if err != nil {
				return err
			}
			for _, prior := range byValue[k] {
				if prior.owner == key.Owner {
					continue
				}
				overlap, err := c.scope.Overlaps(prior.scope, e.limits.Component.Temporal)
				if err != nil {
					return err
				}
				if overlap {
					return &UniqueConflict{Name: d.Name, A: prior.owner, B: key.Owner}
				}
			}
			byValue[k] = append(byValue[k], claim{key.Owner, c.scope, c.value})
		}
	}
	return nil
}
