package temporal

import (
	"errors"
	"fmt"
)

// ErrInvalidKnowledge identifies an absent or malformed point-knowledge value.
var ErrInvalidKnowledge = errors.New("temporal: invalid point knowledge")

// ErrInvalidConfidenceLevel identifies a confidence level outside exact [0,1].
var ErrInvalidConfidenceLevel = errors.New("temporal: invalid confidence level")

// PointKnowledgeKind separates placement evidence from occupied state support.
type PointKnowledgeKind uint8

// Point-knowledge kinds never promote confidence or nominal data to hard bounds.
const (
	PointKnowledgeInvalid PointKnowledgeKind = iota
	PointKnowledgeUnspecified
	PointKnowledgeNominalOnly
	PointKnowledgeHardSupport
	PointKnowledgeConfidenceRegion
	PointKnowledgeOpaqueConstraint
)

// KnowledgeConsistency applies only to the supplied hard point support. It is
// not a consistency claim about global evidence or opaque constraints.
type KnowledgeConsistency uint8

// Hard-support consistency results distinguish an empty possibility set.
const (
	KnowledgeConsistencyInvalid KnowledgeConsistency = iota
	KnowledgeConsistent
	KnowledgeInconsistent
)

// SupportResult is an exact predicate answer about a supplied hard support.
// Inconsistent support yields Holds=false for both predicates, never a vacuous
// definite occurrence. Error results have invalid consistency and make no claim.
type SupportResult struct {
	Holds       bool
	Consistency KnowledgeConsistency
}

// PointKnowledge is immutable evidence about one point's placement, not a state
// asserting continuous occupancy throughout the support. Hard support is a
// finite explicit union: it may have infinite cardinality and unbounded bounds.
// Optional nominal evidence is retained independently and never narrows support.
type PointKnowledge struct {
	axis            Axis
	kind            PointKnowledgeKind
	hasNominal      bool
	nominal         Position
	support         Scope
	confidenceLevel Rational
	constraint      OpaqueDescriptor
}

// UnspecifiedPointKnowledge makes no hard placement or quality claim.
func UnspecifiedPointKnowledge(axis Axis, l Limits) (PointKnowledge, error) {
	return checkedKnowledge(PointKnowledge{axis: axis, kind: PointKnowledgeUnspecified}, l)
}

// NominalPointKnowledge preserves a nominal position without a hard bound.
func NominalPointKnowledge(p Position, l Limits) (PointKnowledge, error) {
	l, err := l.resolved()
	if err != nil {
		return PointKnowledge{}, err
	}
	if err := p.validate(l); err != nil {
		return PointKnowledge{}, err
	}
	return checkedKnowledge(PointKnowledge{axis: p.axis, kind: PointKnowledgeNominalOnly, hasNominal: true, nominal: p}, l)
}

// HardPointKnowledge accepts an explicit native union of possible coordinates.
// All and unbounded spans are valid; Unplaced is not a supplied support set.
func HardPointKnowledge(s Scope, l Limits) (PointKnowledge, error) {
	l, err := l.resolved()
	if err != nil {
		return PointKnowledge{}, err
	}
	if err := s.validate(l); err != nil {
		return PointKnowledge{}, err
	}
	return checkedKnowledge(PointKnowledge{axis: s.axis, kind: PointKnowledgeHardSupport, support: s}, l)
}

// ConfidencePointKnowledge retains an exact confidence level and its
// stated region. The level does not infer a probability distribution or the
// probability that a fixed event lies in a frequentist confidence interval.
// It does not assert hard support, even at level one.
func ConfidencePointKnowledge(s Scope, confidenceLevel Rational, l Limits) (PointKnowledge, error) {
	l, err := l.resolved()
	if err != nil {
		return PointKnowledge{}, err
	}
	if err := s.validate(l); err != nil {
		return PointKnowledge{}, err
	}
	return checkedKnowledge(PointKnowledge{axis: s.axis, kind: PointKnowledgeConfidenceRegion, support: s, confidenceLevel: confidenceLevel}, l)
}

// OpaquePointKnowledge preserves a constraint without inferring feasible support
// or consistency. Sigma interprets the descriptor, including shared correlation.
func OpaquePointKnowledge(axis Axis, d OpaqueDescriptor, l Limits) (PointKnowledge, error) {
	return checkedKnowledge(PointKnowledge{axis: axis, kind: PointKnowledgeOpaqueConstraint, constraint: d}, l)
}

func checkedKnowledge(k PointKnowledge, l Limits) (PointKnowledge, error) {
	l, err := l.resolved()
	if err != nil {
		return PointKnowledge{}, err
	}
	if err := k.validate(l); err != nil {
		return PointKnowledge{}, err
	}
	return k, nil
}

// Kind returns the evidence tag; a zero PointKnowledge returns invalid.
func (k PointKnowledge) Kind() PointKnowledgeKind { return k.kind }

// Axis returns the immutable axis identity and definition.
func (k PointKnowledge) Axis() Axis { return k.axis }

// Nominal returns optional nominal evidence, independent of hard support.
func (k PointKnowledge) Nominal() (Position, bool) { return k.nominal, k.hasNominal }

// HardSupport returns possible-coordinate support only for an explicit hard tag.
func (k PointKnowledge) HardSupport() (Scope, bool) {
	return k.support, k.kind == PointKnowledgeHardSupport
}

// Confidence returns a stated region and exact confidence level only for that tag.
func (k PointKnowledge) Confidence() (Scope, Rational, bool) {
	return k.support, k.confidenceLevel, k.kind == PointKnowledgeConfidenceRegion
}

// Constraint returns an immutable descriptor only for an opaque constraint tag.
func (k PointKnowledge) Constraint() (OpaqueDescriptor, bool) {
	return k.constraint, k.kind == PointKnowledgeOpaqueConstraint
}

// WithNominal preserves independent nominal evidence without intersecting,
// narrowing or otherwise changing support. Unspecified becomes NominalOnly.
// Both old/new inputs and aggregate caps are checked before returning a value.
func (k PointKnowledge) WithNominal(p Position, l Limits) (PointKnowledge, error) {
	l, err := l.resolved()
	if err != nil {
		return PointKnowledge{}, err
	}
	if err := k.validate(l); err != nil {
		return PointKnowledge{}, err
	}
	if err := p.validate(l); err != nil {
		return PointKnowledge{}, err
	}
	if err := sameAxis(k.axis, p.axis); err != nil {
		return PointKnowledge{}, err
	}
	k.hasNominal = true
	k.nominal = p
	if k.kind == PointKnowledgeUnspecified {
		k.kind = PointKnowledgeNominalOnly
	}
	if err := k.validate(l); err != nil {
		return PointKnowledge{}, err
	}
	return k, nil
}

func (k PointKnowledge) validate(l Limits) error {
	if k.kind < PointKnowledgeUnspecified || k.kind > PointKnowledgeOpaqueConstraint {
		return ErrInvalidKnowledge
	}
	if err := k.axis.validate(); err != nil {
		return err
	}
	if err := k.axis.validateDescriptorBudget(l); err != nil {
		return err
	}
	if k.kind == PointKnowledgeUnspecified && k.hasNominal || k.kind == PointKnowledgeNominalOnly && !k.hasNominal {
		return ErrInvalidKnowledge
	}
	if k.hasNominal {
		if err := k.nominal.validate(l); err != nil {
			return err
		}
		if err := sameAxis(k.axis, k.nominal.axis); err != nil {
			return err
		}
	}
	switch k.kind {
	case PointKnowledgeHardSupport, PointKnowledgeConfidenceRegion:
		if err := k.support.validate(l); err != nil {
			return err
		}
		if err := sameAxis(k.axis, k.support.axis); err != nil {
			return err
		}
		if k.support.kind == ScopeUnplaced {
			return ErrUnplacedScope
		}
		if k.kind == PointKnowledgeConfidenceRegion {
			if err := k.confidenceLevel.validate(l); err != nil {
				return err
			}
			if k.confidenceLevel.num.Sign() < 0 || compareInteger(k.confidenceLevel.num, k.confidenceLevel.Denominator()) == Greater {
				return ErrInvalidConfidenceLevel
			}
		}
	case PointKnowledgeOpaqueConstraint:
		size, err := k.constraint.validate(l)
		if err != nil {
			return err
		}
		if size > l.MaxDescriptorBytes-axisDescriptorBytes(k.axis) {
			return fmt.Errorf("%w: knowledge axis and opaque descriptor bytes", ErrResourceLimit)
		}
	}
	size, err := knowledgeWireBytes(k, l)
	if err != nil {
		return err
	}
	// The expected/shared axis is referenced on wire but still occupies metadata
	// in the materialized value. Count its full definition once, not just its ID.
	if size > l.MaxValueBytes-axisDescriptorBytes(k.axis) {
		return fmt.Errorf("%w: aggregate point knowledge bytes", ErrResourceLimit)
	}
	return nil
}

func knowledgePredicateInputs(k PointKnowledge, window Scope, l Limits) (Limits, error) {
	l, err := l.resolved()
	if err != nil {
		return Limits{}, err
	}
	if err := k.validate(l); err != nil {
		return Limits{}, err
	}
	if err := window.validate(l); err != nil {
		return Limits{}, err
	}
	if err := sameAxis(k.axis, window.axis); err != nil {
		return Limits{}, err
	}
	if window.kind == ScopeUnplaced {
		return Limits{}, ErrUnplacedScope
	}
	if k.kind != PointKnowledgeHardSupport {
		return Limits{}, ErrUnsupportedPredicate
	}
	return l, nil
}

// PossibleIn holds exactly when nonempty hard support overlaps the window.
// An empty hard support is visibly inconsistent; it implies no occurrence.
func (k PointKnowledge) PossibleIn(window Scope, l Limits) (SupportResult, error) {
	l, err := knowledgePredicateInputs(k, window, l)
	if err != nil {
		return SupportResult{}, err
	}
	if k.support.kind == ScopeEmpty {
		return SupportResult{Consistency: KnowledgeInconsistent}, nil
	}
	holds, err := k.support.Overlaps(window, l)
	if err != nil {
		return SupportResult{}, err
	}
	return SupportResult{Holds: holds, Consistency: KnowledgeConsistent}, nil
}

// DefiniteIn holds exactly when nonempty hard support is contained in the
// window. A linear coverage walk uses no subtraction fragments or output rows.
func (k PointKnowledge) DefiniteIn(window Scope, l Limits) (SupportResult, error) {
	l, err := knowledgePredicateInputs(k, window, l)
	if err != nil {
		return SupportResult{}, err
	}
	if k.support.kind == ScopeEmpty {
		return SupportResult{Consistency: KnowledgeInconsistent}, nil
	}
	j := 0
	for _, part := range k.support.parts {
		for j < len(window.parts) && intervalBefore(window.parts[j], part, l) {
			j++
		}
		if j == len(window.parts) || !knowledgeIntervalCovered(part, window.parts[j], l) {
			return SupportResult{Consistency: KnowledgeConsistent}, nil
		}
	}
	return SupportResult{Holds: true, Consistency: KnowledgeConsistent}, nil
}
func knowledgeIntervalCovered(part, window interval, l Limits) bool {
	lo, hi := compareBound(part.lo, window.lo, l), compareBound(part.hi, window.hi, l)
	return (lo == Greater || lo == Equal && (!part.lo.inclusive || window.lo.inclusive)) && (hi == Less || hi == Equal && (!part.hi.inclusive || window.hi.inclusive))
}
