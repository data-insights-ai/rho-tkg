package temporal

import (
	"errors"
	"testing"
)

func TestAxisValidateDoesNotTrustConstructionPolicy(t *testing.T) {
	a := testAxis(t, ProfileIntegerZ)
	before, hash := a.Descriptor(), a.DefinitionHash()
	for _, tc := range []struct {
		axis   Axis
		limits Limits
		want   error
	}{
		{a, Limits{}, nil},
		{a, Limits{MaxDescriptorBytes: axisDescriptorBytes(a)}, nil},
		{a, Limits{MaxDescriptorBytes: axisDescriptorBytes(a) - 1}, ErrResourceLimit},
		{Axis{}, Limits{}, ErrInvalidAxis},
		{Axis{}, Limits{MaxValueBytes: -1}, ErrInvalidLimits},
		{a, Limits{MaxMagnitudeBits: -1}, ErrInvalidLimits},
	} {
		if err := tc.axis.Validate(tc.limits); !errors.Is(err, tc.want) {
			t.Fatalf("Validate(%+v) = %v, want %v", tc.limits, err, tc.want)
		}
	}
	if a.Descriptor() != before || a.DefinitionHash() != hash {
		t.Fatal("validation changed immutable axis")
	}
	if allocs := testing.AllocsPerRun(100, func() {
		if err := a.Validate(Limits{}); err != nil {
			t.Fatal(err)
		}
	}); allocs != 0 {
		t.Fatalf("axis validation allocated %v times", allocs)
	}
}
