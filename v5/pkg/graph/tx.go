package graph

import (
	"fmt"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

// Tx is the write handle inside Graph.Update. Every method takes an op that
// carries its own valid-time temporal.Scope and an optional Recorded caller
// instant. Implemented by the engine; invalid ops fail with the same errors as
// CheckOp, ErrTxDone after Update returns.
//
// Label ops take a node ID, every other target is a Ref: relationships carry a
// type, not labels, so a relationship ref has no meaning on a label op and the
// type system says so.
type Tx interface {
	CreateNode(CreateNode) (types.NodeID, error)
	CreateRel(CreateRel) (types.RelID, error)
	Correct(Correct) error
	// End ends the validity of Target at the upper bound of the op's Scope.
	End(End) error
	AddLabel(AddLabel) error
	RemoveLabel(RemoveLabel) error
	Set(Set) error
	Unset(Unset) error
	Retract(Retract) error
	// GetOrCreate finds the node holding the unique claim (Label, Key, Value)
	// or creates it; created reports which (gap rows 6-7). Key must be a
	// Config seed marked Unique. The claim is unique per Key across all labels:
	// a holder of the value without Label is ErrUniqueViolation, so the same
	// value under two labels needs two keys.
	GetOrCreate(GetOrCreate) (id types.NodeID, created bool, err error)
}

// Op is a write operation. Validate checks it without an engine.
// The set of ops is closed.
//
// Every op has two time coordinates. Scope is the valid time the write is
// about, always explicit. Recorded is the caller's own knowledge instant
// (gap row 15, v4 *WithTx): raw evidence that is kept and reported, never a
// cut and never an ordering the engine trusts. The zero Recorded means "at
// commit" and is the normal case; a non-zero Recorded is a position on
// Config.KnowledgeAxis. A caller instant in the future of the commit is
// refused by the engine (TODO takeover plan: needs the commit clock).
type Op interface {
	Validate(l temporal.Limits) error
	writeScope() temporal.Scope
	recorded() temporal.Position
}

// CheckOp validates op under the graph's Limits and requires its Scope to be
// on Config.DefaultAxis and a non-zero Recorded to be on Config.KnowledgeAxis
// (the engine applies the same check to every Tx call).
func CheckOp(c Config, op Op) error {
	if op == nil {
		return fmt.Errorf("%w: nil op", ErrInvalidOp)
	}
	if err := op.Validate(c.Limits); err != nil {
		return err
	}
	if !sameAxis(op.writeScope().Axis(), c.DefaultAxis.Axis()) {
		return fmt.Errorf("%w: scope axis differs from the graph default axis", ErrInvalidScope)
	}
	if r := op.recorded(); !isZeroPosition(r) && !sameAxis(r.Axis(), c.KnowledgeAxis.Axis()) {
		return fmt.Errorf("%w: recorded instant is not on the knowledge axis", ErrInvalidScope)
	}
	return nil
}

func sameAxis(a, b temporal.Axis) bool { return a.DefinitionHash() == b.DefinitionHash() }

func isZeroPosition(p temporal.Position) bool { return p.Profile() == 0 }

func checkRecorded(p temporal.Position, l temporal.Limits) error {
	if isZeroPosition(p) {
		return nil
	}
	if _, err := temporal.ComparePositions(p, p, l); err != nil {
		return fmt.Errorf("%w: recorded instant: %v", ErrInvalidOp, err)
	}
	return nil
}

func checkWriteScope(s temporal.Scope, l temporal.Limits) error {
	if s.Kind() == temporal.ScopeInvalid {
		return fmt.Errorf("%w: write needs an explicit scope", ErrInvalidScope)
	}
	if s.Kind() == temporal.ScopeEmpty {
		return fmt.Errorf("%w: write scope is empty", ErrInvalidScope)
	}
	if err := s.Axis().Validate(l); err != nil {
		return fmt.Errorf("%w: axis: %v", ErrInvalidScope, err)
	}
	return nil
}

// checkWrite is the part every op shares: its scope and its recorded instant.
func checkWrite(s temporal.Scope, r temporal.Position, l temporal.Limits) error {
	if err := checkWriteScope(s, l); err != nil {
		return err
	}
	return checkRecorded(r, l)
}

func checkReadScope(s temporal.Scope, l temporal.Limits) error {
	if s.Kind() == temporal.ScopeInvalid {
		return fmt.Errorf("%w: scope is zero", ErrInvalidScope)
	}
	if err := s.Axis().Validate(l); err != nil {
		return fmt.Errorf("%w: axis: %v", ErrInvalidScope, err)
	}
	return nil
}

func nonEmpty(what, v string) error {
	if v == "" {
		return fmt.Errorf("%w: empty %s", ErrInvalidOp, what)
	}
	return nil
}

// CreateNode creates a node with labels and properties over Scope.
type CreateNode struct {
	Labels   []string
	Props    map[string]Value
	Scope    temporal.Scope
	Recorded temporal.Position
}

// Validate implements Op.
func (o CreateNode) Validate(l temporal.Limits) error {
	for _, s := range o.Labels {
		if err := nonEmpty("label", s); err != nil {
			return err
		}
	}
	if err := checkProps(o.Props); err != nil {
		return err
	}
	return checkWrite(o.Scope, o.Recorded, l)
}

// CreateRel creates a relationship From -> To over Scope.
type CreateRel struct {
	Type     string
	From, To types.NodeID
	Props    map[string]Value
	Scope    temporal.Scope
	Recorded temporal.Position
}

// Validate implements Op.
func (o CreateRel) Validate(l temporal.Limits) error {
	if err := nonEmpty("type", o.Type); err != nil {
		return err
	}
	if err := checkNode(o.From); err != nil {
		return err
	}
	if err := checkNode(o.To); err != nil {
		return err
	}
	if err := checkProps(o.Props); err != nil {
		return err
	}
	return checkWrite(o.Scope, o.Recorded, l)
}

// Correct rewrites properties over Scope as a correction of what was known
// (a new revision; the old one stays in the Timeline).
type Correct struct {
	Target   Ref
	Set      map[string]Value
	Scope    temporal.Scope
	Recorded temporal.Position
}

// Validate implements Op.
func (o Correct) Validate(l temporal.Limits) error {
	if err := o.Target.Validate(); err != nil {
		return err
	}
	if len(o.Set) == 0 {
		return fmt.Errorf("%w: correction sets nothing", ErrInvalidOp)
	}
	if err := checkProps(o.Set); err != nil {
		return err
	}
	return checkWrite(o.Scope, o.Recorded, l)
}

// End ends the validity of Target (gap row 14, v4 CloseVersion): the life stops
// at the upper bound of Scope, so there is no second end coordinate that could
// contradict it. The scope must have an upper bound: a Point, a Span or All
// (end at +infinity is a no-op the engine may refuse); Unplaced and Empty have
// none and are refused.
type End struct {
	Target   Ref
	Scope    temporal.Scope
	Recorded temporal.Position
}

// Validate implements Op.
func (o End) Validate(l temporal.Limits) error {
	if err := o.Target.Validate(); err != nil {
		return err
	}
	if err := checkWrite(o.Scope, o.Recorded, l); err != nil {
		return err
	}
	if o.Scope.Kind() == temporal.ScopeUnplaced {
		return fmt.Errorf("%w: an unplaced scope has no upper bound", ErrInvalidScope)
	}
	return nil
}

// AddLabel adds Label to a node over Scope.
type AddLabel struct {
	Target   types.NodeID
	Label    string
	Scope    temporal.Scope
	Recorded temporal.Position
}

// Validate implements Op.
func (o AddLabel) Validate(l temporal.Limits) error {
	return validateLabelOp(o.Target, o.Label, o.Scope, o.Recorded, l)
}

// RemoveLabel removes Label from a node over Scope.
type RemoveLabel struct {
	Target   types.NodeID
	Label    string
	Scope    temporal.Scope
	Recorded temporal.Position
}

// Validate implements Op.
func (o RemoveLabel) Validate(l temporal.Limits) error {
	return validateLabelOp(o.Target, o.Label, o.Scope, o.Recorded, l)
}

func validateLabelOp(id types.NodeID, label string, s temporal.Scope, r temporal.Position, l temporal.Limits) error {
	if err := checkNode(id); err != nil {
		return err
	}
	if err := nonEmpty("label", label); err != nil {
		return err
	}
	return checkWrite(s, r, l)
}

// Set sets one property over Scope. Value may be Int(0) or String(""): zero is
// a value. The zero Value is refused; Null() is a present null.
type Set struct {
	Target   Ref
	Key      string
	Value    Value
	Scope    temporal.Scope
	Recorded temporal.Position
}

// Validate implements Op.
func (o Set) Validate(l temporal.Limits) error {
	if err := o.Target.Validate(); err != nil {
		return err
	}
	if err := nonEmpty("property key", o.Key); err != nil {
		return err
	}
	if err := o.Value.Validate(); err != nil {
		return err
	}
	return checkWrite(o.Scope, o.Recorded, l)
}

// Unset removes one property over Scope.
type Unset struct {
	Target   Ref
	Key      string
	Scope    temporal.Scope
	Recorded temporal.Position
}

// Validate implements Op.
func (o Unset) Validate(l temporal.Limits) error {
	if err := o.Target.Validate(); err != nil {
		return err
	}
	if err := nonEmpty("property key", o.Key); err != nil {
		return err
	}
	return checkWrite(o.Scope, o.Recorded, l)
}

// Retract withdraws what was asserted about Target over Scope (gap row 49).
type Retract struct {
	Target   Ref
	Scope    temporal.Scope
	Recorded temporal.Position
}

// Validate implements Op.
func (o Retract) Validate(l temporal.Limits) error {
	if err := o.Target.Validate(); err != nil {
		return err
	}
	return checkWrite(o.Scope, o.Recorded, l)
}

// GetOrCreate is the unique-claim find-or-create (gap rows 6-7). The claim
// Value must be a non-null value.
type GetOrCreate struct {
	Label    string
	Key      string
	Value    Value
	Props    map[string]Value
	Scope    temporal.Scope
	Recorded temporal.Position
}

// Validate implements Op.
func (o GetOrCreate) Validate(l temporal.Limits) error {
	if err := nonEmpty("label", o.Label); err != nil {
		return err
	}
	if err := nonEmpty("claim key", o.Key); err != nil {
		return err
	}
	if err := o.Value.Validate(); err != nil {
		return err
	}
	if o.Value.IsNull() {
		return fmt.Errorf("%w: a unique claim cannot be null", ErrInvalidValue)
	}
	if err := checkProps(o.Props); err != nil {
		return err
	}
	return checkWrite(o.Scope, o.Recorded, l)
}

func (o CreateNode) writeScope() temporal.Scope  { return o.Scope }
func (o CreateRel) writeScope() temporal.Scope   { return o.Scope }
func (o Correct) writeScope() temporal.Scope     { return o.Scope }
func (o End) writeScope() temporal.Scope         { return o.Scope }
func (o AddLabel) writeScope() temporal.Scope    { return o.Scope }
func (o RemoveLabel) writeScope() temporal.Scope { return o.Scope }
func (o Set) writeScope() temporal.Scope         { return o.Scope }
func (o Unset) writeScope() temporal.Scope       { return o.Scope }
func (o Retract) writeScope() temporal.Scope     { return o.Scope }
func (o GetOrCreate) writeScope() temporal.Scope { return o.Scope }

func (o CreateNode) recorded() temporal.Position  { return o.Recorded }
func (o CreateRel) recorded() temporal.Position   { return o.Recorded }
func (o Correct) recorded() temporal.Position     { return o.Recorded }
func (o End) recorded() temporal.Position         { return o.Recorded }
func (o AddLabel) recorded() temporal.Position    { return o.Recorded }
func (o RemoveLabel) recorded() temporal.Position { return o.Recorded }
func (o Set) recorded() temporal.Position         { return o.Recorded }
func (o Unset) recorded() temporal.Position       { return o.Recorded }
func (o Retract) recorded() temporal.Position     { return o.Recorded }
func (o GetOrCreate) recorded() temporal.Position { return o.Recorded }
