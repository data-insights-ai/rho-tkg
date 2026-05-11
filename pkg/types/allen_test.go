package types

import (
	"errors"
	"testing"
)

// --- Error tests (security/input validation first) ---

func TestRelate_ZeroEndpoints(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name                       string
		aStart, aEnd, bStart, bEnd Instant
	}{
		{"aStart=0", 0, 10, 20, 30},
		{"aEnd=0", 5, 0, 20, 30},
		{"bStart=0", 5, 10, 0, 30},
		{"bEnd=0", 5, 10, 20, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := Relate(tc.aStart, tc.aEnd, tc.bStart, tc.bEnd)
			if !errors.Is(err, ErrOpenInterval) {
				t.Errorf("got err=%v, want ErrOpenInterval", err)
			}
		})
	}
}

func TestRelate_InvalidInterval(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name                       string
		aStart, aEnd, bStart, bEnd Instant
	}{
		{"aStart==aEnd", 10, 10, 20, 30},
		{"aStart>aEnd", 15, 10, 20, 30},
		{"bStart==bEnd", 5, 10, 20, 20},
		{"bStart>bEnd", 5, 10, 30, 20},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := Relate(tc.aStart, tc.aEnd, tc.bStart, tc.bEnd)
			if !errors.Is(err, ErrInvalidInterval) {
				t.Errorf("got err=%v, want ErrInvalidInterval", err)
			}
		})
	}
}

// --- All 13 relations (table-driven) ---

func TestRelate_AllRelations(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name                       string
		aStart, aEnd, bStart, bEnd Instant
		want                       AllenRelation
	}{
		// Before: A=[1,3) B=[5,8)
		{"Before", 1, 3, 5, 8, Before},
		// After: A=[10,15) B=[1,5)
		{"After", 10, 15, 1, 5, After},
		// Meets: A=[1,5) B=[5,10)
		{"Meets", 1, 5, 5, 10, Meets},
		// MetBy: A=[5,10) B=[1,5)
		{"MetBy", 5, 10, 1, 5, MetBy},
		// Overlaps: A=[1,6) B=[4,10)
		{"Overlaps", 1, 6, 4, 10, Overlaps},
		// OverlappedBy: A=[4,10) B=[1,6)
		{"OverlappedBy", 4, 10, 1, 6, OverlappedBy},
		// Starts: A=[1,5) B=[1,10)
		{"Starts", 1, 5, 1, 10, Starts},
		// StartedBy: A=[1,10) B=[1,5)
		{"StartedBy", 1, 10, 1, 5, StartedBy},
		// During: A=[3,7) B=[1,10)
		{"During", 3, 7, 1, 10, During},
		// Contains: A=[1,10) B=[3,7)
		{"Contains", 1, 10, 3, 7, Contains},
		// Finishes: A=[5,10) B=[1,10)
		{"Finishes", 5, 10, 1, 10, Finishes},
		// FinishedBy: A=[1,10) B=[5,10)
		{"FinishedBy", 1, 10, 5, 10, FinishedBy},
		// Equals: A=[1,10) B=[1,10)
		{"Equals", 1, 10, 1, 10, Equals},

		// Boundary variations
		{"Before_adjacent", 1, 4, 5, 8, Before},     // gap of 1
		{"Before_wide_gap", 1, 3, 100, 200, Before}, // large gap
		{"Overlaps_minimal", 1, 3, 2, 5, Overlaps},  // 1 unit overlap
		{"During_minimal", 2, 3, 1, 4, During},      // barely inside
		{"Contains_minimal", 1, 4, 2, 3, Contains},  // barely containing
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := Relate(tc.aStart, tc.aEnd, tc.bStart, tc.bEnd)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("Relate(%d,%d,%d,%d) = %v, want %v",
					tc.aStart, tc.aEnd, tc.bStart, tc.bEnd, got, tc.want)
			}
		})
	}
}

// --- Inverse tests ---

func TestInverse_Roundtrip(t *testing.T) {
	t.Parallel()
	for r := AllenRelation(1); r <= allenRelationCount; r++ {
		if r.Inverse().Inverse() != r {
			t.Errorf("%v.Inverse().Inverse() = %v, want %v", r, r.Inverse().Inverse(), r)
		}
	}
}

func TestRelate_InverseConsistency(t *testing.T) {
	t.Parallel()
	cases := []struct {
		aStart, aEnd, bStart, bEnd Instant
	}{
		{1, 3, 5, 8},   // Before/After
		{1, 5, 5, 10},  // Meets/MetBy
		{1, 6, 4, 10},  // Overlaps/OverlappedBy
		{1, 5, 1, 10},  // Starts/StartedBy
		{3, 7, 1, 10},  // During/Contains
		{5, 10, 1, 10}, // Finishes/FinishedBy
		{1, 10, 1, 10}, // Equals/Equals
	}
	for _, tc := range cases {
		ab, err := Relate(tc.aStart, tc.aEnd, tc.bStart, tc.bEnd)
		if err != nil {
			t.Fatalf("Relate(%d,%d,%d,%d): %v", tc.aStart, tc.aEnd, tc.bStart, tc.bEnd, err)
		}
		ba, err := Relate(tc.bStart, tc.bEnd, tc.aStart, tc.aEnd)
		if err != nil {
			t.Fatalf("Relate(%d,%d,%d,%d): %v", tc.bStart, tc.bEnd, tc.aStart, tc.aEnd, err)
		}
		if ab.Inverse() != ba {
			t.Errorf("Relate(A,B)=%v, Inverse=%v, Relate(B,A)=%v — mismatch",
				ab, ab.Inverse(), ba)
		}
	}
}

// --- String/Symbol ---

func TestAllenRelation_StringAndSymbol(t *testing.T) {
	t.Parallel()
	names := make(map[string]bool)
	symbols := make(map[string]bool)
	for r := AllenRelation(1); r <= allenRelationCount; r++ {
		name := r.String()
		sym := r.Symbol()
		if name == "" || name == "Unknown" {
			t.Errorf("relation %d has empty/unknown String()", r)
		}
		if sym == "" || sym == "?" {
			t.Errorf("relation %d has empty/unknown Symbol()", r)
		}
		if names[name] {
			t.Errorf("duplicate String() %q", name)
		}
		if symbols[sym] {
			t.Errorf("duplicate Symbol() %q", sym)
		}
		names[name] = true
		symbols[sym] = true
	}
}

func TestAllenRelation_UnknownString(t *testing.T) {
	t.Parallel()
	r := AllenRelation(0)
	if r.String() != "Unknown" {
		t.Errorf("AllenRelation(0).String() = %q, want \"Unknown\"", r.String())
	}
	if r.Symbol() != "?" {
		t.Errorf("AllenRelation(0).Symbol() = %q, want \"?\"", r.Symbol())
	}
}

func TestAllenRelation_InverseZero(t *testing.T) {
	t.Parallel()
	r := AllenRelation(0)
	if r.Inverse() != 0 {
		t.Errorf("AllenRelation(0).Inverse() = %v, want 0", r.Inverse())
	}
}

func TestAllenRelationSet_InvalidRelationIgnored(t *testing.T) {
	t.Parallel()

	for _, r := range []AllenRelation{0, allenRelationCount + 1, 255} {
		if got := r.Set(); got != 0 {
			t.Fatalf("AllenRelation(%d).Set() = %v, want empty", r, got)
		}
	}

	hidden := AllenRelationSet(1 << 15)
	if !hidden.IsEmpty() {
		t.Fatal("set with only invalid relation bits should be empty")
	}
	if got := hidden.Len(); got != 0 {
		t.Fatalf("hidden-bit set Len() = %d, want 0", got)
	}
	if got := hidden.ToSlice(); len(got) != 0 {
		t.Fatalf("hidden-bit set ToSlice() = %v, want empty", got)
	}
	if got := hidden.String(); got != "{}" {
		t.Fatalf("hidden-bit set String() = %q, want {}", got)
	}
	if hidden.Contains(AllenRelation(16)) {
		t.Fatal("hidden-bit set must not report invalid relation membership")
	}

	withValid := hidden.Add(Before).Add(AllenRelation(14))
	if withValid != Before.Set() {
		t.Fatalf("hidden.Add(Before).Add(14) = %v, want %v", withValid, Before.Set())
	}
	if got := hidden.Union(Before.Set()); got != Before.Set() {
		t.Fatalf("hidden.Union(Before) = %v, want %v", got, Before.Set())
	}
	if got := (Before.Set() | hidden).Intersection(hidden); got != 0 {
		t.Fatalf("intersection with hidden bits = %v, want empty", got)
	}
}

// --- AllenRelationSet ---

func TestAllenRelationSet_Basic(t *testing.T) {
	t.Parallel()

	var s AllenRelationSet
	if !s.IsEmpty() {
		t.Error("zero set should be empty")
	}
	if s.Len() != 0 {
		t.Errorf("zero set Len() = %d, want 0", s.Len())
	}

	s = s.Add(Before)
	if s.IsEmpty() {
		t.Error("set with Before should not be empty")
	}
	if !s.Contains(Before) {
		t.Error("set should contain Before")
	}
	if s.Contains(After) {
		t.Error("set should not contain After")
	}
	if s.Len() != 1 {
		t.Errorf("Len() = %d, want 1", s.Len())
	}

	s = s.Add(Meets).Add(Equals)
	if s.Len() != 3 {
		t.Errorf("Len() = %d, want 3", s.Len())
	}

	// Union
	other := After.Set().Add(Meets) // {After, Meets}
	u := s.Union(other)
	if u.Len() != 4 { // {Before, After, Meets, Equals}
		t.Errorf("Union Len() = %d, want 4", u.Len())
	}

	// Intersection
	inter := s.Intersection(other) // {Meets}
	if inter.Len() != 1 {
		t.Errorf("Intersection Len() = %d, want 1", inter.Len())
	}
	if !inter.Contains(Meets) {
		t.Error("intersection should contain Meets")
	}

	// ToSlice
	sl := s.ToSlice()
	if len(sl) != 3 {
		t.Errorf("ToSlice len=%d, want 3", len(sl))
	}
	// Sorted by iota value: Before(1), Meets(3), Equals(13)
	if sl[0] != Before || sl[1] != Meets || sl[2] != Equals {
		t.Errorf("ToSlice = %v, want [Before, Meets, Equals]", sl)
	}
}

func TestAllenRelationSet_String(t *testing.T) {
	t.Parallel()

	var empty AllenRelationSet
	if empty.String() != "{}" {
		t.Errorf("empty set String() = %q, want \"{}\"", empty.String())
	}

	s := Before.Set().Add(After)
	str := s.String()
	if str != "{Before, After}" {
		t.Errorf("String() = %q, want \"{Before, After}\"", str)
	}
}

func TestAllenRelationSet_InverseSet(t *testing.T) {
	t.Parallel()
	s := Before.Set().Add(Meets).Add(Starts)
	inv := s.InverseSet()
	if !inv.Contains(After) {
		t.Error("InverseSet should contain After (inverse of Before)")
	}
	if !inv.Contains(MetBy) {
		t.Error("InverseSet should contain MetBy (inverse of Meets)")
	}
	if !inv.Contains(StartedBy) {
		t.Error("InverseSet should contain StartedBy (inverse of Starts)")
	}
	if inv.Len() != 3 {
		t.Errorf("InverseSet Len() = %d, want 3", inv.Len())
	}
}

func TestAllRelations_Complete(t *testing.T) {
	t.Parallel()
	all := AllRelations()
	if all.Len() != 13 {
		t.Errorf("AllRelations() Len() = %d, want 13", all.Len())
	}
	for r := AllenRelation(1); r <= allenRelationCount; r++ {
		if !all.Contains(r) {
			t.Errorf("AllRelations() missing %v", r)
		}
	}
}

// --- Composition ---

func TestCompose_EqualsIsIdentity(t *testing.T) {
	t.Parallel()
	for r := AllenRelation(1); r <= allenRelationCount; r++ {
		got := Compose(Equals, r)
		want := r.Set()
		if got != want {
			t.Errorf("Compose(Equals, %v) = %v, want %v", r, got, want)
		}
		got2 := Compose(r, Equals)
		if got2 != want {
			t.Errorf("Compose(%v, Equals) = %v, want %v", r, got2, want)
		}
	}
}

func TestCompose_KnownResults(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		r1, r2     AllenRelation
		wantSubset []AllenRelation
	}{
		{"Before+Before", Before, Before, []AllenRelation{Before}},
		{"Meets+Meets", Meets, Meets, []AllenRelation{Before}},
		{"Before+Meets", Before, Meets, []AllenRelation{Before}},
		{"Meets+During", Meets, During, []AllenRelation{Overlaps, During, Starts}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := Compose(tc.r1, tc.r2)
			for _, want := range tc.wantSubset {
				if !got.Contains(want) {
					t.Errorf("Compose(%v, %v) = %v, missing %v", tc.r1, tc.r2, got, want)
				}
			}
		})
	}
}

func TestCompose_NeverEmpty(t *testing.T) {
	t.Parallel()
	for r1 := AllenRelation(1); r1 <= allenRelationCount; r1++ {
		for r2 := AllenRelation(1); r2 <= allenRelationCount; r2++ {
			got := Compose(r1, r2)
			if got.IsEmpty() {
				t.Errorf("Compose(%v, %v) is empty", r1, r2)
			}
		}
	}
}

func TestCompose_InvalidRelation(t *testing.T) {
	t.Parallel()
	if got := Compose(0, Before); got != 0 {
		t.Errorf("Compose(0, Before) = %v, want empty", got)
	}
	if got := Compose(Before, 0); got != 0 {
		t.Errorf("Compose(Before, 0) = %v, want empty", got)
	}
	if got := Compose(AllenRelation(14), Before); got != 0 {
		t.Errorf("Compose(14, Before) = %v, want empty", got)
	}
}

func TestComposeSets_MatchesSingleton(t *testing.T) {
	t.Parallel()
	for r1 := AllenRelation(1); r1 <= allenRelationCount; r1++ {
		for r2 := AllenRelation(1); r2 <= allenRelationCount; r2++ {
			want := Compose(r1, r2)
			got := ComposeSets(r1.Set(), r2.Set())
			if got != want {
				t.Errorf("ComposeSets({%v}, {%v}) = %v, want %v", r1, r2, got, want)
			}
		}
	}
}

func TestComposeSets_MultiElement(t *testing.T) {
	t.Parallel()
	a := Before.Set().Add(Meets)
	b := After.Set()
	got := ComposeSets(a, b)
	// Should be union of Compose(Before,After) and Compose(Meets,After)
	want := Compose(Before, After).Union(Compose(Meets, After))
	if got != want {
		t.Errorf("ComposeSets(%v, %v) = %v, want %v", a, b, got, want)
	}
}
