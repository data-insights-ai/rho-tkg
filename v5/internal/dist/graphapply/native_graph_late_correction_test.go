package graphapply

import (
	"errors"
	"testing"
)

// Authored only. The real six-process parent, not these pure arithmetic guards,
// must supply actual native originals/postings/cut/column proof later.
func TestNativeLateCorrectionHalfCohortAndHistoricalAdmission(t *testing.T) {
	s := nativeSelectorsSpec{1, nativeLateCorrectionFamily, 80, 50, 32, "columns"}
	p := nativeLateCorrectionCost(s)
	if nativeLateCorrectionValidate(p) != nil || nativeCreateUpdateValidate(p) == nil || nativeImmutableColumnsValidate(p) == nil {
		t.Fatal("distinct fresh authority")
	}
	for _, fact := range []uint64{0, 39, 40, 79} {
		want := int64(6)
		if fact >= 40 {
			want = 5
		}
		if nativeScalarCorrectedValue(p, 5, fact) != 5 || nativeScalarCorrectedValue(p, 6, fact) != want {
			t.Fatal("old cut or unchanged cohort substituted current", fact)
		}
	}
	p.Facts = 40
	if !errors.Is(nativeLateCorrectionValidate(p), errInvalid) {
		t.Fatal("insufficient genuine correction jobs for32 callers")
	}
	p = nativeLateCorrectionCost(s)
	a := nativeSelectorsAdmission{Family: s.Family, Spec: s, BudgetSeconds: 180, DeadlineUnixNanos: 123}
	policy, err := nativeLateCorrectionReadPolicy(s, a)
	if err != nil || policy.Queries != 5 {
		t.Fatal("five genuine originals with separately admitted index scan work", err)
	}
	old := p
	old.Version = 10
	if nativeScalarCorrectedValue(old, 6, 79) != 6 {
		t.Fatal("existing10 full updates changed")
	}
}
