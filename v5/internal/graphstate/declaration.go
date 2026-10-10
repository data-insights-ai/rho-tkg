package graphstate

// Interpretation is a supplied immutable entity declaration. Support shape,
// kind, enumeration order and equal occupied support never infer this meaning.
// Zero means undeclared. Assertion associations have separate revisioned meaning.
type Interpretation uint8

// InterpretationOccurrence and the other closed declarations preserve supplied
// meaning without inferring it from support or graph shape.
const (
	InterpretationOccurrence Interpretation = iota + 1
	InterpretationState
	InterpretationObservation
	InterpretationConstraint
	InterpretationDerivedAssertion
	InterpretationAssertedRelation
)

// Valid admits zero/undeclared and the closed native declaration vocabulary.
func (i Interpretation) Valid() bool { return i <= InterpretationAssertedRelation }

// TemporalRole declares the meaning of the entity's native Axis association.
// It does not attach roles to properties or replace assertion's open named roles.
type TemporalRole uint8

// TemporalRoleValidity and the other closed roles describe only the native axis
// association; they do not supply property roles or evaluate a clock.
const (
	TemporalRoleValidity TemporalRole = iota + 1
	TemporalRoleOccurrenceTime
	TemporalRoleSourceOccurrence
	TemporalRoleObservationTime
	TemporalRoleSourceReceipt
	TemporalRoleCausalOrder
)

// Valid admits zero/undeclared and the closed native axis-role vocabulary.
func (r TemporalRole) Valid() bool { return r <= TemporalRoleCausalOrder }

func validDeclarations(r EntityRecord) bool {
	return r.Interpretation.Valid() && r.TemporalRole.Valid()
}
func declarationBytes(r EntityRecord) int {
	if r.Interpretation == 0 && r.TemporalRole == 0 {
		return 0
	}
	return 2
}
func entityBytes(r EntityRecord) int {
	return 64 + len(r.Type) + axisBytes(r.Axis) + declarationBytes(r)
}
