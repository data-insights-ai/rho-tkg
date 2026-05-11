package temporal

import (
	"errors"
	"fmt"
	"testing"
)

// TestNewConstraintSet_Empty verifies that calling NewConstraintSet with no
// arguments produces an empty set: Len() == 0, and Items() returns nil per the
// documented API ("Returns nil if the set is empty").
func TestNewConstraintSet_Empty(t *testing.T) {
	cs := NewConstraintSet()
	if got := cs.Len(); got != 0 {
		t.Fatalf("Len() = %d, want 0", got)
	}
	if items := cs.Items(); items != nil {
		t.Fatalf("Items() on empty set = %v, want nil", items)
	}
}

// TestNewConstraintSet_WithArgs verifies that variadic constructor arguments
// are stored in insertion order and are accessible through Items.
func TestNewConstraintSet_WithArgs(t *testing.T) {
	a := TemporalConstraint{Kind: ConstraintRelWithinEndpoints}
	b := TemporalConstraint{Kind: ConstraintRelWithinEndpoints}
	cs := NewConstraintSet(a, b)
	if got := cs.Len(); got != 2 {
		t.Fatalf("Len() = %d, want 2", got)
	}
	items := cs.Items()
	if len(items) != 2 {
		t.Fatalf("len(Items()) = %d, want 2", len(items))
	}
	if items[0] != a || items[1] != b {
		t.Fatalf("Items() = %v, want [%v %v]", items, a, b)
	}
}

// TestConstraintSet_AddImmutable verifies the documented copy-on-write
// behaviour of Add: the receiver is never modified, and Add returns a new set
// with the constraint appended.
func TestConstraintSet_AddImmutable(t *testing.T) {
	original := NewConstraintSet()
	c := TemporalConstraint{Kind: ConstraintRelWithinEndpoints}

	updated := original.Add(c)

	if got := original.Len(); got != 0 {
		t.Fatalf("original.Len() after Add = %d, want 0 (Add must not mutate receiver)", got)
	}
	if got := updated.Len(); got != 1 {
		t.Fatalf("updated.Len() = %d, want 1", got)
	}
	items := updated.Items()
	if len(items) != 1 || items[0] != c {
		t.Fatalf("updated.Items() = %v, want [%v]", items, c)
	}
}

// TestConstraintSet_AddOnNonEmpty verifies that Add appends to the end.
func TestConstraintSet_AddOnNonEmpty(t *testing.T) {
	first := TemporalConstraint{Kind: ConstraintRelWithinEndpoints}
	second := TemporalConstraint{Kind: ConstraintRelWithinEndpoints}
	cs := NewConstraintSet(first).Add(second)

	if cs.Len() != 2 {
		t.Fatalf("Len() = %d, want 2", cs.Len())
	}
	items := cs.Items()
	if items[0] != first || items[1] != second {
		t.Fatalf("Items() = %v, want [%v %v]", items, first, second)
	}
}

// TestConstraintSet_Items_DefensiveCopy verifies that mutating the slice
// returned by Items() does not affect future Items() results — the returned
// slice is documented as a defensive copy.
func TestConstraintSet_Items_DefensiveCopy(t *testing.T) {
	c := TemporalConstraint{Kind: ConstraintRelWithinEndpoints}
	cs := NewConstraintSet(c)

	first := cs.Items()
	if len(first) != 1 {
		t.Fatalf("len(first) = %d, want 1", len(first))
	}
	// Mutate the returned slice. The internal storage must remain untouched.
	first[0] = TemporalConstraint{Kind: TemporalConstraintKind(99)}

	second := cs.Items()
	if len(second) != 1 {
		t.Fatalf("len(second) = %d, want 1", len(second))
	}
	if second[0] != c {
		t.Fatalf("second[0] = %v, want %v (mutating Items() result must not leak to set)", second[0], c)
	}
}

// TestConstraintSet_ForEach_IteratesInOrder verifies that ForEach visits each
// constraint exactly once and in insertion order.
func TestConstraintSet_ForEach_IteratesInOrder(t *testing.T) {
	a := TemporalConstraint{Kind: ConstraintRelWithinEndpoints}
	b := TemporalConstraint{Kind: ConstraintRelWithinEndpoints}
	c := TemporalConstraint{Kind: ConstraintRelWithinEndpoints}
	cs := NewConstraintSet(a, b, c)

	var visited []TemporalConstraint
	err := cs.ForEach(func(tc TemporalConstraint) error {
		visited = append(visited, tc)
		return nil
	})
	if err != nil {
		t.Fatalf("ForEach returned error %v, want nil", err)
	}
	if len(visited) != 3 {
		t.Fatalf("len(visited) = %d, want 3", len(visited))
	}
	if visited[0] != a || visited[1] != b || visited[2] != c {
		t.Fatalf("visited = %v, want [%v %v %v]", visited, a, b, c)
	}
}

// TestConstraintSet_ForEach_EmptySet verifies that ForEach on an empty set
// never invokes the callback and returns nil.
func TestConstraintSet_ForEach_EmptySet(t *testing.T) {
	cs := NewConstraintSet()
	calls := 0
	err := cs.ForEach(func(TemporalConstraint) error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("ForEach returned error %v, want nil", err)
	}
	if calls != 0 {
		t.Fatalf("callback invoked %d times on empty set, want 0", calls)
	}
}

func TestConstraintSet_ForEach_NilCallbackReturnsInvalidConstraint(t *testing.T) {
	t.Parallel()

	checks := []struct {
		name string
		set  ConstraintSet
	}{
		{name: "empty", set: NewConstraintSet()},
		{name: "non-empty", set: NewConstraintSet(TemporalConstraint{Kind: ConstraintRelWithinEndpoints})},
	}
	for _, check := range checks {
		err := check.set.ForEach(nil)
		if !errors.Is(err, ErrInvalidTemporalConstraint) {
			t.Fatalf("%s ForEach(nil) = %v, want ErrInvalidTemporalConstraint", check.name, err)
		}
		if !errors.Is(err, ErrTemporalConstraint) {
			t.Fatalf("%s ForEach(nil) = %v, want ErrTemporalConstraint", check.name, err)
		}
	}
}

// TestConstraintSet_ForEach_StopsOnError verifies the documented short-circuit
// behaviour: ForEach returns the first non-nil error and stops iteration.
func TestConstraintSet_ForEach_StopsOnError(t *testing.T) {
	a := TemporalConstraint{Kind: ConstraintRelWithinEndpoints}
	b := TemporalConstraint{Kind: ConstraintRelWithinEndpoints}
	c := TemporalConstraint{Kind: ConstraintRelWithinEndpoints}
	cs := NewConstraintSet(a, b, c)

	stopErr := errors.New("stop")
	calls := 0
	err := cs.ForEach(func(TemporalConstraint) error {
		calls++
		if calls == 2 {
			return stopErr
		}
		return nil
	})
	if !errors.Is(err, stopErr) {
		t.Fatalf("ForEach error = %v, want %v", err, stopErr)
	}
	if calls != 2 {
		t.Fatalf("callback invoked %d times, want 2 (must stop on first error)", calls)
	}
}

// TestConstraintSet_AddZeroValueConstraint documents that Add accepts the
// zero-value constraint without panicking. The set is a pure container and
// performs no validation of Kind.
func TestConstraintSet_AddZeroValueConstraint(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Add panicked on zero-value constraint: %v", r)
		}
	}()

	zero := TemporalConstraint{} // Kind is the zero value (0), which is not a defined constant.
	cs := NewConstraintSet().Add(zero)
	if cs.Len() != 1 {
		t.Fatalf("Len() = %d, want 1", cs.Len())
	}
	items := cs.Items()
	if items[0] != zero {
		t.Fatalf("Items()[0] = %v, want %v", items[0], zero)
	}
	if items[0].Kind != 0 {
		t.Fatalf("zero-value Kind = %d, want 0", items[0].Kind)
	}
}

// TestTemporalConstraintKind_ZeroValue documents the meaning of the zero value:
// it is NOT one of the defined constants. ConstraintRelWithinEndpoints is
// declared as iota+1, so 0 is reserved/unset.
func TestTemporalConstraintKind_ZeroValue(t *testing.T) {
	var zero TemporalConstraintKind
	if zero == ConstraintRelWithinEndpoints {
		t.Fatalf("zero value of TemporalConstraintKind must not equal ConstraintRelWithinEndpoints")
	}
	if zero != 0 {
		t.Fatalf("zero value = %d, want 0", zero)
	}
}

// TestConstraintRelWithinEndpoints_Value pins the numeric value of the
// constant. Changing it would be a breaking change for any persisted form or
// wire compatibility.
func TestConstraintRelWithinEndpoints_Value(t *testing.T) {
	if got := ConstraintRelWithinEndpoints; got != 1 {
		t.Fatalf("ConstraintRelWithinEndpoints = %d, want 1 (iota+1)", got)
	}
}

// TestSentinelErrors_Distinct verifies that all sentinel errors are distinct
// values (no accidental aliasing) and that errors.Is matches each one against
// itself.
func TestSentinelErrors_Distinct(t *testing.T) {
	sentinels := []struct {
		name string
		err  error
	}{
		{"ErrTemporalConstraint", ErrTemporalConstraint},
		{"ErrInvalidTemporalConstraint", ErrInvalidTemporalConstraint},
		{"ErrRelBeforeStartNode", ErrRelBeforeStartNode},
		{"ErrRelBeforeEndNode", ErrRelBeforeEndNode},
		{"ErrRelAfterStartNode", ErrRelAfterStartNode},
		{"ErrRelAfterEndNode", ErrRelAfterEndNode},
		{"ErrRelExceedsStartNodeValidity", ErrRelExceedsStartNodeValidity},
		{"ErrRelExceedsEndNodeValidity", ErrRelExceedsEndNodeValidity},
	}

	for _, s := range sentinels {
		if s.err == nil {
			t.Errorf("%s is nil", s.name)
			continue
		}
		if s.err.Error() == "" {
			t.Errorf("%s has empty message", s.name)
		}
		if !errors.Is(s.err, s.err) {
			t.Errorf("errors.Is(%s, %s) = false, want true", s.name, s.name)
		}
	}

	// Pairwise distinctness: no two sentinels are the same value.
	for i := 0; i < len(sentinels); i++ {
		for j := i + 1; j < len(sentinels); j++ {
			if sentinels[i].err == sentinels[j].err {
				t.Errorf("%s and %s are the same error value", sentinels[i].name, sentinels[j].name)
			}
		}
	}
}

// TestSentinelErrors_Wrapping verifies (per Testing Rule 4) that each sentinel
// error can be wrapped and recovered via errors.Is.
func TestSentinelErrors_Wrapping(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"ErrTemporalConstraint", ErrTemporalConstraint},
		{"ErrInvalidTemporalConstraint", ErrInvalidTemporalConstraint},
		{"ErrRelBeforeStartNode", ErrRelBeforeStartNode},
		{"ErrRelBeforeEndNode", ErrRelBeforeEndNode},
		{"ErrRelAfterStartNode", ErrRelAfterStartNode},
		{"ErrRelAfterEndNode", ErrRelAfterEndNode},
		{"ErrRelExceedsStartNodeValidity", ErrRelExceedsStartNodeValidity},
		{"ErrRelExceedsEndNodeValidity", ErrRelExceedsEndNodeValidity},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wrapped := fmt.Errorf("contextual prefix: %w", tc.err)
			if !errors.Is(wrapped, tc.err) {
				t.Fatalf("errors.Is(wrapped, %s) = false, want true", tc.name)
			}
			// Negative case: wrapped error must NOT match a different sentinel.
			var other error = ErrTemporalConstraint
			if tc.err == ErrTemporalConstraint {
				other = ErrRelBeforeStartNode
			}
			if errors.Is(wrapped, other) {
				t.Fatalf("errors.Is(wrapped(%s), other) = true, want false", tc.name)
			}
		})
	}
}

// TestSentinelErrors_DoubleWrapping verifies that errors.Is traverses an
// arbitrary depth of wrapping — protects against accidental %v vs %w typos in
// future call sites.
func TestSentinelErrors_DoubleWrapping(t *testing.T) {
	wrapped := fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", ErrRelBeforeStartNode))
	if !errors.Is(wrapped, ErrRelBeforeStartNode) {
		t.Fatalf("errors.Is(double-wrapped, ErrRelBeforeStartNode) = false, want true")
	}
}
