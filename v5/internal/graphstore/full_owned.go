package graphstore

import (
	"strings"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func (q *pageReader) availableBytes() int {
	limit := min(q.q.c.limits.MaxReadBytes, q.limits.MaxWorkBytes)
	if q.q.maxBytes > 0 {
		limit = min(limit, q.q.maxBytes)
	}
	return limit - q.q.bytes
}
func (q *pageReader) ownScope(s temporal.Scope) (temporal.Scope, error) {
	available := q.availableBytes()
	if available < 2 {
		return temporal.Scope{}, ErrResourceLimit
	}
	l := q.q.c.limits.Temporal
	l.MaxValueBytes = min(l.MaxValueBytes, available/2)
	var wire []byte
	var err error
	if q.q.arena != nil {
		wire, err = q.q.arena.ScopeBytes(s, l)
	} else {
		wire, err = temporal.AppendScope(nil, s, l)
	}
	if err != nil {
		return temporal.Scope{}, callerError(err)
	}
	backing, err := scopeOwnedBacking(wire)
	if err != nil {
		return temporal.Scope{}, err
	}
	if err := q.q.materialize(128 + backing + axisVariableBytes(s.Axis()) + 2*cap(wire)); err != nil {
		return temporal.Scope{}, err
	}
	return temporal.DecodeScope(wire, s.Axis(), l)
}
func (q *pageReader) ownState(s state.State) (state.State, error) {
	available := q.availableBytes()
	if available < 2 {
		return state.State{}, ErrResourceLimit
	}
	l := q.limits.codecLimits(q.q.c)
	l.MaxEncodedBytes = min(l.MaxEncodedBytes, available/2)
	admitted, err := q.reserveStateCodec(s, l)
	if err != nil {
		return state.State{}, err
	}
	wire, err := state.AppendState(nil, s, l)
	if err != nil {
		return state.State{}, callerError(err)
	}
	if err := q.q.materializeReserved(256+stateOwnedBacking(s)+2*cap(wire), min(admitted, 2*cap(wire))); err != nil {
		return state.State{}, err
	}
	return state.DecodeState(wire, s.Axis(), l)
}
func (q *pageReader) ownChanges(scope temporal.Scope, changes []state.Change) ([]state.Change, error) {
	available := q.availableBytes()
	if available < 2 {
		return nil, ErrResourceLimit
	}
	l := q.limits.codecLimits(q.q.c)
	l.MaxEncodedBytes = min(l.MaxEncodedBytes, available/2)
	admitted, err := q.reserveChangeCodec(changes, l)
	if err != nil {
		return nil, err
	}
	wire, err := state.AppendChanges(nil, scope.Axis(), changes, l)
	if err != nil {
		return nil, callerError(err)
	}
	cost := 2*cap(wire) + 256*len(changes)
	for _, change := range changes {
		scopeWire, err := q.scopeWire(change.Scope())
		if err != nil {
			return nil, callerError(err)
		}
		backing, err := scopeOwnedBacking(scopeWire)
		if err != nil {
			return nil, err
		}
		cost += backing + axisVariableBytes(change.Scope().Axis())
	}
	if err := q.q.materializeReserved(cost, min(admitted, 2*cap(wire))); err != nil {
		return nil, err
	}
	owned, _, err := state.DecodeChanges(wire, scope.Axis(), l)
	return owned, err
}
func (q *pageReader) ownScalar(value graphstate.Scalar) (graphstate.Scalar, error) {
	key, err := q.equalityKey(value)
	if err != nil {
		return graphstate.Scalar{}, err
	}
	var axis temporal.Axis
	hasAxis := false
	if scope, ok := value.Scope(); ok {
		hasAxis = true
		axis = scope.Axis()
		backing, err := scopeOwnedBacking([]byte(key)[1:])
		if err != nil {
			return graphstate.Scalar{}, err
		}
		if err := q.q.materialize(backing + axisVariableBytes(axis)); err != nil {
			return graphstate.Scalar{}, err
		}
	}
	if value.Kind() == graphstate.ScalarDescriptor {
		if err := q.q.materialize(descriptorOwnedBacking(len(key) - 1)); err != nil {
			return graphstate.Scalar{}, err
		}
	}
	return decodeScalar([]byte(key), axis, hasAxis, q.q.c.limits)
}
func (q *pageReader) ownDelta(delta graphstate.Delta, groups []ComponentChangeGroup, outputLimit int) (graphstate.Delta, []ComponentChangeGroup, error) {
	fixed := 512 + 256*len(delta.Entities) + 64*len(delta.Lives) + 256*len(delta.Values) + fullPatchOutputBytes*len(delta.Patches) + fullDependencyOutputBytes*len(delta.Dependencies) + fullGroupOutputBytes*len(groups)
	if fixed > outputLimit {
		return graphstate.Delta{}, nil, ErrResourceLimit
	}
	if err := q.q.materialize(fixed); err != nil {
		return graphstate.Delta{}, nil, err
	}
	out := graphstate.Delta{Graph: delta.Graph, View: delta.View, Entities: make([]graphstate.EntityRecord, len(delta.Entities)), Lives: make([]graphstate.LifeRecord, len(delta.Lives)), Values: make([]graphstate.ValueWrite, len(delta.Values)), Patches: make([]graphstate.ComponentPatch, len(delta.Patches)), Dependencies: make([]graphstate.Dependency, len(delta.Dependencies))}
	copy(out.Entities, delta.Entities)
	copy(out.Lives, delta.Lives)
	ownedGroups := make([]ComponentChangeGroup, len(groups))
	for i, group := range groups {
		var err error
		group.Owned, err = q.ownScope(group.Owned)
		if err != nil {
			return graphstate.Delta{}, nil, err
		}
		group.Changes, err = q.ownChanges(group.Owned, group.Changes)
		if err != nil {
			return graphstate.Delta{}, nil, err
		}
		if err := q.q.materialize(len(group.Key.Name)); err != nil {
			return graphstate.Delta{}, nil, err
		}
		group.Key.Name = strings.Clone(group.Key.Name)
		ownedGroups[i] = group
		patch := delta.Patches[i]
		patch.Key = group.Key
		patch.Owned = group.Owned
		patch.Changes = group.Changes
		patch.State, err = q.ownState(patch.State)
		if err != nil {
			return graphstate.Delta{}, nil, err
		}
		out.Patches[i] = patch
	}
	for i, value := range delta.Values {
		scalar, err := q.ownScalar(value.Value)
		if err != nil {
			return graphstate.Delta{}, nil, err
		}
		out.Values[i] = graphstate.ValueWrite{ID: value.ID, Value: scalar}
	}
	for i, dep := range delta.Dependencies {
		nameBytes := len(dep.Name) + len(dep.Key.Name) + len(dep.Prefix.Name) + len(dep.Unique.Definition.Name)
		if err := q.q.materialize(nameBytes); err != nil {
			return graphstate.Delta{}, nil, err
		}
		dep.Name = strings.Clone(dep.Name)
		dep.Key.Name = strings.Clone(dep.Key.Name)
		dep.Prefix.Name = strings.Clone(dep.Prefix.Name)
		dep.Unique.Definition.Name = strings.Clone(dep.Unique.Definition.Name)
		var err error
		if dep.Window.Kind() != temporal.ScopeInvalid {
			dep.Window, err = q.ownScope(dep.Window)
			if err != nil {
				return graphstate.Delta{}, nil, err
			}
		}
		if dep.Unique.Window.Kind() != temporal.ScopeInvalid {
			dep.Unique.Window, err = q.ownScope(dep.Unique.Window)
			if err != nil {
				return graphstate.Delta{}, nil, err
			}
		}
		if dep.Incident.Window.Kind() != temporal.ScopeInvalid {
			dep.Incident.Window, err = q.ownScope(dep.Incident.Window)
			if err != nil {
				return graphstate.Delta{}, nil, err
			}
		}
		if dep.Value.Kind() != graphstate.ScalarInvalid {
			dep.Value, err = q.ownScalar(dep.Value)
			if err != nil {
				return graphstate.Delta{}, nil, err
			}
		}
		if dep.Unique.Value.Kind() != graphstate.ScalarInvalid {
			dep.Unique.Value, err = q.ownScalar(dep.Unique.Value)
			if err != nil {
				return graphstate.Delta{}, nil, err
			}
		}
		out.Dependencies[i] = dep
	}
	return out, ownedGroups, nil
}
func graphVariableOutput(delta graphstate.Delta, groups []ComponentChangeGroup, l Limits) (int, error) {
	total := 0
	for _, entity := range delta.Entities {
		total += len(entity.Type) + axisVariableBytes(entity.Axis)
	}
	for _, value := range delta.Values {
		n, err := scalarVariableOwned(value.Value, 0, l)
		if err != nil {
			return 0, err
		}
		total += n
	}
	for _, group := range groups {
		wire, err := temporal.AppendScope(nil, group.Owned, l.Temporal)
		if err != nil {
			return 0, err
		}
		backing, err := scopeOwnedBacking(wire)
		if err != nil {
			return 0, err
		}
		total += len(group.Key.Name) + backing + axisVariableBytes(group.Owned.Axis()) + 256*cap(group.Changes)
		for _, change := range group.Changes {
			wire, err := temporal.AppendScope(nil, change.Scope(), l.Temporal)
			if err != nil {
				return 0, err
			}
			backing, err := scopeOwnedBacking(wire)
			if err != nil {
				return 0, err
			}
			total += backing + axisVariableBytes(change.Scope().Axis())
		}
	}
	for _, patch := range delta.Patches {
		total += stateOwnedBacking(patch.State)
	}
	for _, dep := range delta.Dependencies {
		total += len(dep.Name) + len(dep.Key.Name) + len(dep.Prefix.Name) + len(dep.Unique.Definition.Name)
		for _, scope := range []temporal.Scope{dep.Window, dep.Unique.Window, dep.Incident.Window} {
			if scope.Kind() != temporal.ScopeInvalid {
				wire, err := temporal.AppendScope(nil, scope, l.Temporal)
				if err != nil {
					return 0, err
				}
				backing, err := scopeOwnedBacking(wire)
				if err != nil {
					return 0, err
				}
				total += backing + axisVariableBytes(scope.Axis())
			}
		}
		for _, value := range []graphstate.Scalar{dep.Value, dep.Unique.Value} {
			if value.Kind() != graphstate.ScalarInvalid {
				n, err := scalarVariableOwned(value, 0, l)
				if err != nil {
					return 0, err
				}
				total += n
			}
		}
	}
	return total, nil
}
