package graphstate

import (
	"context"

	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func mutationScope(s temporal.Scope, l Limits) error {
	switch s.Kind() {
	case temporal.ScopeInvalid:
		return ErrValidityRequired
	case temporal.ScopeEmpty:
		return ErrEmptyMutation
	case temporal.ScopeUnplaced:
		return ErrUnsupported
	}
	_, err := temporal.AppendScope(nil, s, l.Component.Temporal)
	return err
}

// Plan validates and assembles one atomic bounded delta against a supplied
// immutable graph view. It retains only touched windows, never graph/history
// snapshots. Storage must validate every dependency before atomic installation.
func Plan(ctx context.Context, v ReadView, ops []Operation, revision state.Revision, l Limits) (Delta, error) {
	l, err := l.resolve()
	if err != nil {
		return Delta{}, err
	}
	if len(ops) > l.MaxOperations {
		return Delta{}, ErrResourceLimit
	}
	for _, op := range ops {
		if err := mutationScope(op.Scope, l); err != nil {
			return Delta{}, err
		}
		if op.Owner == 0 || op.Life == 0 || op.Kind < CreateNode || op.Kind > Remove {
			return Delta{}, ErrInvalidInput
		}
		if op.Kind == AddLabel || op.Kind == RemoveLabel || op.Kind >= Set {
			if !validName(op.Name, l) {
				return Delta{}, ErrInvalidInput
			}
		}
		if op.Kind == CreateRelationship && !validName(op.Record.Type, l) {
			return Delta{}, ErrInvalidInput
		}
	}
	if revision.ID() == 0 {
		return Delta{}, state.ErrInvalidRevision
	}
	e, err := start(ctx, v, l, revision)
	if err != nil {
		return Delta{}, err
	}
	for _, op := range ops {
		if err := e.check(); err != nil {
			return Delta{}, err
		}
		if err := e.apply(op); err != nil {
			return Delta{}, err
		}
	}
	if err := e.validateUniqueness(); err != nil {
		return Delta{}, err
	}
	return e.delta, nil
}
func (e *engine) apply(op Operation) error {
	if op.Kind == CreateNode || op.Kind == CreateRelationship {
		return e.create(op)
	}
	owner, found, err := e.entity(op.Owner)
	if err != nil {
		return err
	}
	if !found {
		return ErrNotFound
	}
	if _, err := sameAxes(owner.Axis, op.Scope.Axis(), e.limits); err != nil {
		return err
	}
	if op.Kind == Reopen {
		return e.newLife(owner, op)
	}
	_, found, err = e.life(op.Owner, op.Life)
	if err != nil {
		return err
	}
	if !found {
		return ErrNotFound
	}
	if op.Kind == Close || op.Kind == Correct {
		if err := e.presence(op.Owner, op.Scope, op.Life, true); err != nil {
			return err
		}
		if op.Kind == Correct && op.Present {
			if err := e.endpointsCover(owner, op.Life, op.Scope); err != nil {
				return err
			}
			ref, _ := state.NewValueRef(uint64(op.Life), 0)
			return e.mutate(ComponentKey{Owner: op.Owner, Kind: Presence}, op.Scope, ref, true)
		}
		return e.mutate(ComponentKey{Owner: op.Owner, Kind: Presence}, op.Scope, state.ValueRef{}, false)
	}
	// Relationship properties intentionally use declared relation life, not
	// endpoint-effective visibility; SEM06 remains legal while endpoints are masked.
	if err := e.presence(op.Owner, op.Scope, op.Life, false); err != nil {
		return err
	}
	if !validName(op.Name, e.limits) {
		return ErrInvalidInput
	}
	if op.Kind == AddLabel || op.Kind == RemoveLabel {
		if owner.Kind != Node {
			return ErrTypeMismatch
		}
		return e.mutate(ComponentKey{Owner: op.Owner, Life: op.Life, Kind: Label, Name: op.Name}, op.Scope, state.Null(), op.Kind == AddLabel)
	}
	d, err := e.property(owner.Kind, op.Name)
	if err != nil {
		return err
	}
	if d.Owner != owner.Kind {
		return ErrTypeMismatch
	}
	key := ComponentKey{Owner: op.Owner, Life: op.Life, Name: op.Name}
	if op.Kind == Unset {
		if d.Cardinality == ScalarCardinality {
			key.Kind = ScalarProperty
			return e.mutate(key, op.Scope, state.ValueRef{}, false)
		}
		keys, err := e.keys(KeyPredicate{op.Owner, op.Life, SetMember, op.Name})
		if err != nil {
			return err
		}
		for _, key := range keys {
			if err := e.mutate(key, op.Scope, state.ValueRef{}, false); err != nil {
				return err
			}
		}
		return nil
	}
	if op.Value.Kind() != ScalarNull && op.Value.Kind() != d.Type {
		return ErrTypeMismatch
	}
	if op.Kind == Set {
		if d.Cardinality != ScalarCardinality {
			return ErrTypeMismatch
		}
		ref, err := e.intern(op.Value, op.ValueID)
		if err != nil {
			return err
		}
		key.Kind = ScalarProperty
		return e.mutate(key, op.Scope, ref, true)
	}
	if d.Cardinality != SetCardinality {
		return ErrTypeMismatch
	}
	if op.Value.Kind() == ScalarNull {
		return ErrUnsupported
	}
	ref, err := e.intern(op.Value, op.ValueID)
	if err != nil {
		return err
	}
	key.Kind = SetMember
	key.Member = ValueID(ref.ID())
	return e.mutate(key, op.Scope, state.Null(), op.Kind == Add)
}
func sameAxes(a, b temporal.Axis, l Limits) (bool, error) {
	x, err := temporal.Empty(a)
	if err != nil {
		return false, err
	}
	y, err := temporal.Empty(b)
	if err != nil {
		return false, err
	}
	return x.SameSupport(y, l.Component.Temporal)
}
func (e *engine) create(op Operation) error {
	_, found, err := e.entity(op.Owner)
	if err != nil {
		return err
	}
	if found {
		return ErrAlreadyExists
	}
	record := op.Record
	if record.ID != 0 && record.ID != op.Owner {
		return ErrInvalidInput
	}
	record.ID = op.Owner
	record.Axis = op.Scope.Axis()
	if op.Kind == CreateNode {
		record.Kind = Node
		record.Type = ""
		record.Source = 0
		record.Target = 0
		record.Mode = 0
	} else {
		record.Kind = Relationship
		if !validName(record.Type, e.limits) || record.Source == 0 || record.Target == 0 || (record.Mode != LifeBound && record.Mode != IdentityReference) {
			return ErrInvalidInput
		}
	}
	if err := e.output(64 + len(record.Type) + axisBytes(record.Axis)); err != nil {
		return err
	}
	e.entities[op.Owner] = record
	e.delta.Entities = append(e.delta.Entities, record)
	return e.newLife(record, op)
}
func (e *engine) newLife(owner EntityRecord, op Operation) error {
	_, exists, err := e.life(owner.ID, op.Life)
	if err != nil {
		return err
	}
	if exists {
		return ErrAlreadyExists
	}
	if err := e.presence(owner.ID, op.Scope, 0, true); err != nil {
		return err
	}
	binding := op.Binding
	binding.Owner = owner.ID
	binding.Life = op.Life
	if owner.Kind == Node {
		binding.SourceLife = 0
		binding.TargetLife = 0
	}
	if err := e.output(32); err != nil {
		return err
	}
	e.lives[lifeKey{owner.ID, op.Life}] = binding
	e.delta.Lives = append(e.delta.Lives, binding)
	if err := e.endpointsCover(owner, op.Life, op.Scope); err != nil {
		return err
	}
	ref, _ := state.NewValueRef(uint64(op.Life), 0)
	return e.mutate(ComponentKey{Owner: owner.ID, Kind: Presence}, op.Scope, ref, true)
}
func (e *engine) endpointsCover(owner EntityRecord, life LifeID, scope temporal.Scope) error {
	if owner.Kind != Relationship {
		return nil
	}
	binding, found, err := e.life(owner.ID, life)
	if err != nil {
		return err
	}
	if !found {
		return ErrNotFound
	}
	for _, endpoint := range []struct {
		id   EntityID
		life LifeID
	}{{owner.Source, binding.SourceLife}, {owner.Target, binding.TargetLife}} {
		node, found, err := e.entity(endpoint.id)
		if err != nil {
			return err
		}
		if !found {
			return ErrNotFound
		}
		if node.Kind != Node {
			return ErrTypeMismatch
		}
		if owner.Mode == IdentityReference {
			continue
		}
		if endpoint.life == 0 {
			return ErrInvalidInput
		}
		if _, found, err := e.life(endpoint.id, endpoint.life); err != nil {
			return err
		} else if !found {
			return ErrNotFound
		}
		if err := e.presence(endpoint.id, scope, endpoint.life, false); err != nil {
			return err
		}
	}
	return nil
}
