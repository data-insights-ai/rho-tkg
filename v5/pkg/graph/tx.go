package graph

import (
	"fmt"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

// Value is a property value. Placeholder for the v5 value model (open question);
// the skeleton treats it as opaque and never inspects it.
type Value = any

// Tx is the write handle inside Graph.Update. Every method takes an op that
// carries its own temporal.Scope. Implemented by the engine; invalid ops fail
// with the same errors as Op.Validate, ErrTxDone after Update returns.
type Tx interface {
	CreateNode(CreateNode) (types.NodeID, error)
	CreateRel(CreateRel) (types.RelID, error)
	Correct(Correct) error
	Close(Close) error
	AddLabel(AddLabel) error
	RemoveLabel(RemoveLabel) error
	Set(Set) error
	Unset(Unset) error
	Retract(Retract) error
	// GetOrCreate finds the node holding the unique claim (Label, Key, Value)
	// or creates it; created reports which (gap rows 6-7).
	GetOrCreate(GetOrCreate) (id types.NodeID, created bool, err error)
}

// Op is a write operation. Validate checks it without an engine.
// The set of ops is closed.
type Op interface {
	Validate(l temporal.Limits) error
	writeScope() temporal.Scope
}

// CheckOp validates op and requires its Scope to be on the graph's default
// axis (the engine applies the same check to every Tx call).
func CheckOp(b types.DefaultAxisBinding, l temporal.Limits, op Op) error {
	if op == nil {
		return fmt.Errorf("%w: nil op", ErrInvalidOp)
	}
	if err := op.Validate(l); err != nil {
		return err
	}
	if op.writeScope().Axis().DefinitionHash() != b.Axis().DefinitionHash() {
		return fmt.Errorf("%w: scope axis differs from the graph default axis", ErrInvalidScope)
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
	Labels []string
	Props  map[string]Value
	Scope  temporal.Scope
}

// Validate implements Op.
func (o CreateNode) Validate(l temporal.Limits) error {
	for _, s := range o.Labels {
		if err := nonEmpty("label", s); err != nil {
			return err
		}
	}
	for k := range o.Props {
		if err := nonEmpty("property key", k); err != nil {
			return err
		}
	}
	return checkWriteScope(o.Scope, l)
}

// CreateRel creates a relationship From -> To over Scope.
type CreateRel struct {
	Type     string
	From, To types.NodeID
	Props    map[string]Value
	Scope    temporal.Scope
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
	for k := range o.Props {
		if err := nonEmpty("property key", k); err != nil {
			return err
		}
	}
	return checkWriteScope(o.Scope, l)
}

// Correct rewrites properties over Scope as a correction of what was known
// (a new revision; the old one stays in the Timeline).
type Correct struct {
	Target Ref
	Set    map[string]Value
	Scope  temporal.Scope
}

// Validate implements Op.
func (o Correct) Validate(l temporal.Limits) error {
	if err := o.Target.Validate(); err != nil {
		return err
	}
	if len(o.Set) == 0 {
		return fmt.Errorf("%w: correction sets nothing", ErrInvalidOp)
	}
	for k := range o.Set {
		if err := nonEmpty("property key", k); err != nil {
			return err
		}
	}
	return checkWriteScope(o.Scope, l)
}

// Close ends the validity of Target at End within Scope (gap row 14). End is a
// temporal.Bound: PositiveInfinity or finite; NegativeInfinity and the zero
// Bound are refused.
type Close struct {
	Target Ref
	End    temporal.Bound
	Scope  temporal.Scope
}

// Validate implements Op.
func (o Close) Validate(l temporal.Limits) error {
	if err := o.Target.Validate(); err != nil {
		return err
	}
	if k := o.End.Kind(); k == temporal.BoundInvalid || k == temporal.BoundNegativeInfinity {
		return fmt.Errorf("%w: close end", ErrInvalidBound)
	}
	return checkWriteScope(o.Scope, l)
}

// AddLabel adds Label to a node over Scope.
type AddLabel struct {
	Target types.NodeID
	Label  string
	Scope  temporal.Scope
}

// Validate implements Op.
func (o AddLabel) Validate(l temporal.Limits) error {
	return validateLabelOp(o.Target, o.Label, o.Scope, l)
}

// RemoveLabel removes Label from a node over Scope.
type RemoveLabel struct {
	Target types.NodeID
	Label  string
	Scope  temporal.Scope
}

// Validate implements Op.
func (o RemoveLabel) Validate(l temporal.Limits) error {
	return validateLabelOp(o.Target, o.Label, o.Scope, l)
}

func validateLabelOp(id types.NodeID, label string, s temporal.Scope, l temporal.Limits) error {
	if err := checkNode(id); err != nil {
		return err
	}
	if err := nonEmpty("label", label); err != nil {
		return err
	}
	return checkWriteScope(s, l)
}

// Set sets one property over Scope. Value may be 0 or "": zero is a value.
type Set struct {
	Target Ref
	Key    string
	Value  Value
	Scope  temporal.Scope
}

// Validate implements Op.
func (o Set) Validate(l temporal.Limits) error {
	if err := o.Target.Validate(); err != nil {
		return err
	}
	if err := nonEmpty("property key", o.Key); err != nil {
		return err
	}
	return checkWriteScope(o.Scope, l)
}

// Unset removes one property over Scope.
type Unset struct {
	Target Ref
	Key    string
	Scope  temporal.Scope
}

// Validate implements Op.
func (o Unset) Validate(l temporal.Limits) error {
	if err := o.Target.Validate(); err != nil {
		return err
	}
	if err := nonEmpty("property key", o.Key); err != nil {
		return err
	}
	return checkWriteScope(o.Scope, l)
}

// Retract withdraws what was asserted about Target over Scope (gap row 49).
type Retract struct {
	Target Ref
	Scope  temporal.Scope
}

// Validate implements Op.
func (o Retract) Validate(l temporal.Limits) error {
	if err := o.Target.Validate(); err != nil {
		return err
	}
	return checkWriteScope(o.Scope, l)
}

// GetOrCreate is the unique-claim find-or-create (gap rows 6-7).
type GetOrCreate struct {
	Label string
	Key   string
	Value Value
	Props map[string]Value
	Scope temporal.Scope
}

// Validate implements Op.
func (o GetOrCreate) Validate(l temporal.Limits) error {
	if err := nonEmpty("label", o.Label); err != nil {
		return err
	}
	if err := nonEmpty("claim key", o.Key); err != nil {
		return err
	}
	return checkWriteScope(o.Scope, l)
}

// Receipt is the opaque proof of a commit. The zero Receipt is invalid; there
// is no numeric LSN and no arithmetic on receipts.
type Receipt struct{ tok []byte }

// IsZero reports the invalid zero Receipt.
func (r Receipt) IsZero() bool { return len(r.tok) == 0 }

// Cut is an opaque certified knowledge boundary. The zero Cut is invalid.
type Cut struct{ tok []byte }

// IsZero reports the invalid zero Cut.
func (c Cut) IsZero() bool { return len(c.tok) == 0 }

func (o CreateNode) writeScope() temporal.Scope { return o.Scope }

func (o CreateRel) writeScope() temporal.Scope { return o.Scope }

func (o Correct) writeScope() temporal.Scope { return o.Scope }

func (o Close) writeScope() temporal.Scope { return o.Scope }

func (o AddLabel) writeScope() temporal.Scope { return o.Scope }

func (o RemoveLabel) writeScope() temporal.Scope { return o.Scope }

func (o Set) writeScope() temporal.Scope { return o.Scope }

func (o Unset) writeScope() temporal.Scope { return o.Scope }

func (o Retract) writeScope() temporal.Scope { return o.Scope }

func (o GetOrCreate) writeScope() temporal.Scope { return o.Scope }
