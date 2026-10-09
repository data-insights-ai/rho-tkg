package graphstate

import (
	"cmp"
	"context"
	"errors"
	"slices"

	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// Project reads only requested entity/component windows at the supplied view.
// Stable identity remains present after life closure. Effective relationships
// use immutable endpoint LifeIDs; identity references remain visible after close.
func Project(ctx context.Context, v ReadView, id EntityID, p temporal.Position, mode Visibility, l Limits) (Projection, error) {
	l, err := l.resolve()
	if err != nil {
		return Projection{}, err
	}
	if mode != Declared && mode != Effective {
		return Projection{}, ErrInvalidInput
	}
	point, err := temporal.Point(p)
	if err != nil {
		return Projection{}, err
	}
	if _, err := temporal.AppendScope(nil, point, l.Component.Temporal); err != nil {
		return Projection{}, err
	}
	e, err := start(ctx, v, l, state.Revision{})
	if err != nil {
		return Projection{}, err
	}
	record, exists, err := e.entity(id)
	if err != nil {
		return Projection{}, err
	}
	out := Projection{Record: record, Exists: exists}
	if err := e.output(64 + len(record.Type) + axisBytes(record.Axis)); err != nil {
		return Projection{}, err
	}
	if !exists {
		out.Dependencies = e.delta.Dependencies
		return out, nil
	}
	if _, err := sameAxes(record.Axis, p.Axis(), l); err != nil {
		return Projection{}, err
	}
	cell, err := e.at(ComponentKey{Owner: id, Kind: Presence}, p)
	if err != nil {
		return Projection{}, err
	}
	out.Active = cell.Present()
	if !out.Active {
		out.Dependencies = e.delta.Dependencies
		return out, nil
	}
	out.Life = LifeID(cell.Value().ID())
	binding, found, err := e.life(id, out.Life)
	if err != nil {
		return Projection{}, err
	}
	if !found {
		return Projection{}, ErrContradictoryRead
	}
	if record.Kind == Relationship {
		for i, endpoint := range []struct {
			id    EntityID
			bound LifeID
		}{{record.Source, binding.SourceLife}, {record.Target, binding.TargetLife}} {
			node, exists, err := e.entity(endpoint.id)
			if err != nil {
				return Projection{}, err
			}
			if !exists || node.Kind != Node {
				return Projection{}, ErrContradictoryRead
			}
			status := EndpointStatus{ID: endpoint.id, BoundLife: endpoint.bound}
			compatible, err := sameAxes(node.Axis, p.Axis(), l)
			if err == nil && compatible {
				status.Known = true
				current, err := e.at(ComponentKey{Owner: endpoint.id, Kind: Presence}, p)
				if err != nil {
					return Projection{}, err
				}
				status.Active = current.Present()
				if status.Active {
					status.Life = LifeID(current.Value().ID())
				}
			} else if record.Mode == LifeBound || !errors.Is(err, temporal.ErrAxisMismatch) {
				return Projection{}, err
			}
			out.Endpoints[i] = status
			if mode == Effective && record.Mode == LifeBound && (!status.Active || status.Life != endpoint.bound) {
				out.Active = false
			}
		}
		if !out.Active {
			out.Dependencies = e.delta.Dependencies
			return out, nil
		}
	}
	keys, err := e.keys(KeyPredicate{Owner: id, Life: out.Life})
	if err != nil {
		return Projection{}, err
	}
	properties := make(map[string]PropertyValue)
	for _, key := range keys {
		var definition PropertyDefinition
		if key.Kind == ScalarProperty || key.Kind == SetMember {
			definition, err = e.property(record.Kind, key.Name)
			if err != nil {
				return Projection{}, err
			}
			if definition.Owner != record.Kind || key.Kind == ScalarProperty && definition.Cardinality != ScalarCardinality || key.Kind == SetMember && definition.Cardinality != SetCardinality {
				return Projection{}, ErrContradictoryRead
			}
		}
		value, err := e.at(key, p)
		if err != nil {
			return Projection{}, err
		}
		if !value.Present() {
			continue
		}
		switch key.Kind {
		case Label:
			if !value.Value().IsNull() {
				return Projection{}, ErrContradictoryRead
			}
			if err := e.output(8 + len(key.Name)); err != nil {
				return Projection{}, err
			}
			out.Labels = append(out.Labels, key.Name)
		case ScalarProperty:
			scalar := Null()
			if !value.Value().IsNull() {
				scalar, err = e.value(ValueID(value.Value().ID()))
				if err != nil {
					return Projection{}, err
				}
			}
			if scalar.Kind() != ScalarNull && scalar.Kind() != definition.Type {
				return Projection{}, ErrContradictoryRead
			}
			data, err := scalar.bytes(l)
			if err != nil {
				return Projection{}, err
			}
			n := scalar.retainedBytes(len(data))
			if err := e.output(16 + len(key.Name) + n); err != nil {
				return Projection{}, err
			}
			properties[key.Name] = PropertyValue{Name: key.Name, Cardinality: ScalarCardinality, Scalar: scalar}
		case SetMember:
			scalar, err := e.value(key.Member)
			if err != nil {
				return Projection{}, err
			}
			if scalar.Kind() != definition.Type {
				return Projection{}, ErrContradictoryRead
			}
			data, err := scalar.bytes(l)
			if err != nil {
				return Projection{}, err
			}
			n := scalar.retainedBytes(len(data))
			if err := e.output(16 + len(key.Name) + n); err != nil {
				return Projection{}, err
			}
			entry := properties[key.Name]
			entry.Name = key.Name
			entry.Cardinality = SetCardinality
			entry.Members = append(entry.Members, scalar)
			properties[key.Name] = entry
		}
	}
	for _, property := range properties {
		ordered := make([]struct {
			key   string
			value Scalar
		}, len(property.Members))
		for i, value := range property.Members {
			key, err := value.EqualityKey(l)
			if err != nil {
				return Projection{}, err
			}
			ordered[i] = struct {
				key   string
				value Scalar
			}{key, value}
		}
		slices.SortFunc(ordered, func(a, b struct {
			key   string
			value Scalar
		}) int {
			return cmp.Compare(a.key, b.key)
		})
		for i, item := range ordered {
			property.Members[i] = item.value
		}
		out.Properties = append(out.Properties, property)
	}
	slices.Sort(out.Labels)
	slices.SortFunc(out.Properties, func(a, b PropertyValue) int { return cmp.Compare(a.Name, b.Name) })
	out.Dependencies = e.delta.Dependencies
	return out, nil
}
