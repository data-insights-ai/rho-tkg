package types

import (
	"errors"
	"strings"
)

// Errors for Allen's interval algebra.
var (
	// ErrOpenInterval is returned when an interval endpoint is zero.
	// Allen's algebra requires finite intervals; ValidTo == 0 means "still valid"
	// in v3 and cannot be classified.
	ErrOpenInterval = errors.New("types: Allen relation requires finite interval (no zero endpoints)")

	// ErrInvalidInterval is returned when start >= end.
	ErrInvalidInterval = errors.New("types: invalid interval: start >= end")
)

// AllenRelation represents one of Allen's 13 interval relations.
// The zero value is invalid (catches uninitialized usage).
type AllenRelation uint8

// Allen's 13 interval relations.
const (
	Before       AllenRelation = iota + 1 // A ends strictly before B starts
	After                                 // A starts strictly after B ends
	Meets                                 // A.end == B.start
	MetBy                                 // B.end == A.start
	Overlaps                              // A starts first, partial overlap
	OverlappedBy                          // B starts first, partial overlap
	Starts                                // same start, A shorter
	StartedBy                             // same start, B shorter
	During                                // A strictly inside B
	Contains                              // B strictly inside A
	Finishes                              // same end, A starts later
	FinishedBy                            // same end, A starts earlier
	Equals                                // identical intervals
)

// allenRelationCount is the total number of defined relations (13).
const allenRelationCount = 13

const allenRelationMask AllenRelationSet = (1 << allenRelationCount) - 1

func (r AllenRelation) valid() bool {
	return r >= 1 && r <= allenRelationCount
}

// String returns the human-readable name of the relation.
func (r AllenRelation) String() string {
	switch r {
	case Before:
		return "Before"
	case After:
		return "After"
	case Meets:
		return "Meets"
	case MetBy:
		return "MetBy"
	case Overlaps:
		return "Overlaps"
	case OverlappedBy:
		return "OverlappedBy"
	case Starts:
		return "Starts"
	case StartedBy:
		return "StartedBy"
	case During:
		return "During"
	case Contains:
		return "Contains"
	case Finishes:
		return "Finishes"
	case FinishedBy:
		return "FinishedBy"
	case Equals:
		return "Equals"
	default:
		return "Unknown"
	}
}

// Symbol returns a compact symbolic notation for the relation.
func (r AllenRelation) Symbol() string {
	switch r {
	case Before:
		return "<"
	case After:
		return ">"
	case Meets:
		return "m"
	case MetBy:
		return "mi"
	case Overlaps:
		return "o"
	case OverlappedBy:
		return "oi"
	case Starts:
		return "s"
	case StartedBy:
		return "si"
	case During:
		return "d"
	case Contains:
		return "di"
	case Finishes:
		return "f"
	case FinishedBy:
		return "fi"
	case Equals:
		return "="
	default:
		return "?"
	}
}

// Inverse returns the converse relation (swapping A and B).
func (r AllenRelation) Inverse() AllenRelation {
	switch r {
	case Before:
		return After
	case After:
		return Before
	case Meets:
		return MetBy
	case MetBy:
		return Meets
	case Overlaps:
		return OverlappedBy
	case OverlappedBy:
		return Overlaps
	case Starts:
		return StartedBy
	case StartedBy:
		return Starts
	case During:
		return Contains
	case Contains:
		return During
	case Finishes:
		return FinishedBy
	case FinishedBy:
		return Finishes
	case Equals:
		return Equals
	default:
		return 0
	}
}

// Set returns a singleton AllenRelationSet containing only this relation.
func (r AllenRelation) Set() AllenRelationSet {
	if !r.valid() {
		return 0
	}
	return AllenRelationSet(1) << (r - 1)
}

// Relate classifies the Allen relation between intervals [aStart, aEnd) and
// [bStart, bEnd). All four endpoints must be > 0 (ErrOpenInterval) and
// start < end (ErrInvalidInterval).
func Relate(aStart, aEnd, bStart, bEnd Instant) (AllenRelation, error) {
	if aStart == 0 || aEnd == 0 || bStart == 0 || bEnd == 0 {
		return 0, ErrOpenInterval
	}
	if aStart >= aEnd || bStart >= bEnd {
		return 0, ErrInvalidInterval
	}
	return relate(aStart, aEnd, bStart, bEnd), nil
}

// relate is the validation-free classifier used by both Relate() and the
// composition table builder. Precondition: all four values positive, start < end.
func relate(aStart, aEnd, bStart, bEnd Instant) AllenRelation {
	switch {
	case aEnd < bStart:
		return Before
	case bEnd < aStart:
		return After
	case aEnd == bStart:
		return Meets
	case bEnd == aStart:
		return MetBy
	case aStart == bStart && aEnd == bEnd:
		return Equals
	case aStart == bStart && aEnd < bEnd:
		return Starts
	case aStart == bStart && aEnd > bEnd:
		return StartedBy
	case aEnd == bEnd && aStart > bStart:
		return Finishes
	case aEnd == bEnd && aStart < bStart:
		return FinishedBy
	case aStart > bStart && aEnd < bEnd:
		return During
	case aStart < bStart && aEnd > bEnd:
		return Contains
	case aStart < bStart:
		return Overlaps
	default:
		return OverlappedBy
	}
}

// --- AllenRelationSet ---

// AllenRelationSet is a compact bitset representing a set of Allen relations.
// Bit i corresponds to AllenRelation(i+1). Zero value is the empty set.
type AllenRelationSet uint16

// Contains reports whether the set includes r.
func (s AllenRelationSet) Contains(r AllenRelation) bool {
	if !r.valid() {
		return false
	}
	return s&r.Set() != 0
}

// Add returns a new set with r added.
func (s AllenRelationSet) Add(r AllenRelation) AllenRelationSet {
	return (s | r.Set()) & allenRelationMask
}

// Union returns the set union of s and other.
func (s AllenRelationSet) Union(other AllenRelationSet) AllenRelationSet {
	return (s | other) & allenRelationMask
}

// Intersection returns the set intersection of s and other.
func (s AllenRelationSet) Intersection(other AllenRelationSet) AllenRelationSet {
	return (s & other) & allenRelationMask
}

// IsEmpty reports whether the set contains no relations.
func (s AllenRelationSet) IsEmpty() bool {
	return s&allenRelationMask == 0
}

// Len returns the number of relations in the set (popcount).
func (s AllenRelationSet) Len() int {
	n := 0
	bits := uint16(s & allenRelationMask)
	for bits != 0 {
		n++
		bits &= bits - 1
	}
	return n
}

// ToSlice returns the relations in the set as a sorted slice.
func (s AllenRelationSet) ToSlice() []AllenRelation {
	result := make([]AllenRelation, 0, s.Len())
	for i := AllenRelation(1); i <= allenRelationCount; i++ {
		if s.Contains(i) {
			result = append(result, i)
		}
	}
	return result
}

// String returns a human-readable representation like "{Before, Meets}".
func (s AllenRelationSet) String() string {
	if s.IsEmpty() {
		return "{}"
	}
	parts := make([]string, 0, s.Len())
	for i := AllenRelation(1); i <= allenRelationCount; i++ {
		if s.Contains(i) {
			parts = append(parts, i.String())
		}
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

// InverseSet returns a new set with each relation replaced by its inverse.
func (s AllenRelationSet) InverseSet() AllenRelationSet {
	var inv AllenRelationSet
	for i := AllenRelation(1); i <= allenRelationCount; i++ {
		if s.Contains(i) {
			inv = inv.Add(i.Inverse())
		}
	}
	return inv
}

// AllRelations returns the set containing all 13 Allen relations.
func AllRelations() AllenRelationSet {
	var s AllenRelationSet
	for i := AllenRelation(1); i <= allenRelationCount; i++ {
		s = s.Add(i)
	}
	return s
}

// --- Composition ---

// compositionTable[r1][r2] gives the set of possible relations for
// "if A r1 B and B r2 C, then A ? C". Indices 1..13 are used;
// index 0 is unused (AllenRelation zero value is invalid).
var compositionTable [14][14]AllenRelationSet

func init() {
	// Build a 7-point integer timeline. Every pair of distinct points
	// defines an interval, giving C(7,2) = 21 intervals. We enumerate
	// all ordered triples (A,B,C) of these intervals (9261 triples)
	// and record relate(A,C) into compositionTable[relate(A,B)][relate(B,C)].
	type interval struct{ s, e Instant }
	var intervals []interval
	for s := Instant(1); s <= 7; s++ {
		for e := s + 1; e <= 7; e++ {
			intervals = append(intervals, interval{s, e})
		}
	}

	for _, a := range intervals {
		for _, b := range intervals {
			for _, c := range intervals {
				ab := relate(a.s, a.e, b.s, b.e)
				bc := relate(b.s, b.e, c.s, c.e)
				ac := relate(a.s, a.e, c.s, c.e)
				compositionTable[ab][bc] = compositionTable[ab][bc].Add(ac)
			}
		}
	}
}

// Compose returns the set of Allen relations that can hold between A and C,
// given that A has relation r1 to B and B has relation r2 to C.
func Compose(r1, r2 AllenRelation) AllenRelationSet {
	if r1 < 1 || r1 > allenRelationCount || r2 < 1 || r2 > allenRelationCount {
		return 0
	}
	return compositionTable[r1][r2]
}

// ComposeSets returns the union of Compose(r1, r2) for all r1 in a and r2 in b.
func ComposeSets(a, b AllenRelationSet) AllenRelationSet {
	var result AllenRelationSet
	for r1 := AllenRelation(1); r1 <= allenRelationCount; r1++ {
		if !a.Contains(r1) {
			continue
		}
		for r2 := AllenRelation(1); r2 <= allenRelationCount; r2++ {
			if !b.Contains(r2) {
				continue
			}
			result = result.Union(Compose(r1, r2))
		}
	}
	return result
}
