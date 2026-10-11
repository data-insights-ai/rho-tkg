package graphstore

import (
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
)

func canonicalIncidentKeys(r graphstate.EntityRecord) []postingKey {
	first := postingKey{family: canonicalIncidentRecord, endpoint: r.Source, mode: r.Mode, relationship: r.ID, roles: 1}
	if r.Source == r.Target {
		first.roles = 3
		return []postingKey{first}
	}
	second := first
	second.endpoint = r.Target
	second.roles = 2
	return []postingKey{first, second}
}
func declaredIncidentKeys(r graphstate.EntityRecord, life graphstate.LifeRecord) []postingKey {
	if r.Mode != graphstate.LifeBound {
		return nil
	}
	first := postingKey{family: declaredIncidentRecord, endpoint: r.Source, bound: life.SourceLife, relationship: r.ID, life: life.Life, roles: 1}
	if r.Source == r.Target && life.SourceLife == life.TargetLife {
		first.roles = 3
		return []postingKey{first}
	}
	second := first
	second.endpoint = r.Target
	second.bound = life.TargetLife
	second.roles = 2
	return []postingKey{first, second}
}

type postingMembershipLookup func(postingTreeRoot, postingKey, postingTreeLimits) (bool, error)

func (q *pageReader) checkCanonical(r graphstate.EntityRecord) error {
	return q.checkCanonicalWithLookup(r, q.hasPostingKey)
}
func (q *pageReader) checkCanonicalWithLookup(r graphstate.EntityRecord, lookup postingMembershipLookup) error {
	if q.q.route != nil {
		for _, id := range [...]graphstate.EntityID{r.ID, r.Source, r.Target} {
			if err := q.q.route.owner(q.q, uint64(id)); err != nil {
				return err
			}
		}
	}
	d, err := q.fullDescriptor()
	if err != nil {
		return err
	}
	for _, key := range canonicalIncidentKeys(r) {
		found, err := lookup(d.canonical, key, keyTreeLimits(q.limits))
		if err != nil {
			return err
		}
		if !found {
			return ErrCorrupt
		}
		endpoint, found, err := q.q.entity(EntityRef{q.q.c.root.namespace.Graph, key.endpoint})
		if err != nil {
			return err
		}
		if !found || endpoint.Kind != graphstate.Node {
			return ErrCorrupt
		}
	}
	return nil
}
func (q *pageReader) checkDeclared(r graphstate.EntityRecord, life graphstate.LifeRecord) error {
	return q.checkDeclaredWithLookup(r, life, q.hasPostingKey)
}
func (q *pageReader) checkDeclaredWithLookup(r graphstate.EntityRecord, life graphstate.LifeRecord, lookup postingMembershipLookup) error {
	if err := q.checkCanonicalWithLookup(r, lookup); err != nil {
		return err
	}
	if r.Mode != graphstate.LifeBound {
		return nil
	}
	d, err := q.fullDescriptor()
	if err != nil {
		return err
	}
	for _, key := range declaredIncidentKeys(r, life) {
		found, err := lookup(d.declared, key, keyTreeLimits(q.limits))
		if err != nil {
			return err
		}
		if !found {
			return ErrCorrupt
		}
		endpoint, found, err := q.q.entity(EntityRef{q.q.c.root.namespace.Graph, key.endpoint})
		if err != nil {
			return err
		}
		if !found || endpoint.Kind != graphstate.Node || endpoint.Axis.Descriptor() != r.Axis.Descriptor() || endpoint.Axis.DefinitionHash() != r.Axis.DefinitionHash() {
			return ErrCorrupt
		}
		if _, found, err := q.q.life(LifeRef{q.q.c.root.namespace.Graph, key.endpoint, key.bound}); err != nil {
			return err
		} else if !found {
			return ErrCorrupt
		}
	}
	return nil
}
func (q *pageReader) rawKey(k graphstate.ComponentKey, cell state.Cell) (postingKey, bool, error) {
	if !cell.Present() || k.Kind != graphstate.ScalarProperty && k.Kind != graphstate.SetMember || k.Kind == graphstate.ScalarProperty && cell.Value().IsNull() {
		return postingKey{}, false, nil
	}
	owner, found, err := q.q.entity(EntityRef{q.q.c.root.namespace.Graph, k.Owner})
	if err != nil {
		return postingKey{}, false, err
	}
	if !found {
		return postingKey{}, false, ErrCorrupt
	}
	definition, found, err := q.q.property(owner.Kind, k.Name)
	if err != nil {
		return postingKey{}, false, err
	}
	if !found {
		return postingKey{}, false, ErrCorrupt
	}
	id := k.Member
	if k.Kind == graphstate.ScalarProperty {
		id = graphstate.ValueID(cell.Value().ID())
	}
	value, wire, found, err := q.q.value(ValueRef{q.q.c.root.namespace.Graph, id})
	if err != nil {
		return postingKey{}, false, err
	}
	if !found || value.Value.Kind() != definition.Type {
		return postingKey{}, false, ErrCorrupt
	}
	if definition.Unique == graphstate.UniqueNone {
		return postingKey{}, false, nil
	}
	if q.q.route != nil {
		if err := q.q.route.unique(q.q, definition, string(wire)); err != nil {
			return postingKey{}, false, err
		}
	}
	if err := q.q.materialize(len(wire)); err != nil {
		return postingKey{}, false, err
	}
	canonical, found, err := q.q.canonicalIdentity(string(wire))
	if err != nil {
		return postingKey{}, false, err
	}
	if !found {
		return postingKey{}, false, ErrCorrupt
	}
	key := postingKey{family: uniquePostingRecord, owner: owner.Kind, scalar: definition.Type, name: k.Name, value: canonical.Ref.ID, component: k}
	return key, true, nil
}
func (q *pageReader) checkRawCell(k graphstate.ComponentKey, cell state.Cell) error {
	if !isFullTopology(q.q.c.root.topology) && q.q.full == nil && q.q.fullView == nil {
		return nil
	}
	key, present, err := q.rawKey(k, cell)
	if err != nil || !present {
		return err
	}
	d, err := q.fullDescriptor()
	if err != nil {
		return err
	}
	found, err := q.hasPostingKey(d.unique, key, keyTreeLimits(q.limits))
	if err != nil {
		return err
	}
	if !found {
		return ErrCorrupt
	}
	return nil
}
func (q *pageReader) validateRawCandidate(k postingKey, definition graphstate.PropertyDefinition, canonical graphstate.ValueID) error {
	if k.owner != definition.Owner || k.name != definition.Name || k.scalar != definition.Type || k.value != canonical {
		return ErrCorrupt
	}
	q.member = nil
	m, found, err := q.readMeta(k.component)
	if err != nil {
		return err
	}
	if !found {
		return ErrCorrupt
	}
	owner, found, err := q.q.entity(EntityRef{q.q.c.root.namespace.Graph, k.component.Owner})
	if err != nil {
		return err
	}
	if !found || owner.Kind != definition.Owner {
		return ErrCorrupt
	}
	cardinality := graphstate.ScalarCardinality
	if k.component.Kind == graphstate.SetMember {
		cardinality = graphstate.SetCardinality
	}
	if cardinality != definition.Cardinality {
		return ErrCorrupt
	}
	if _, err := q.directory(m.Root, k.component, m.Axis); err != nil {
		return err
	}
	if owner.Kind == graphstate.Relationship {
		if err := q.checkCanonical(owner); err != nil {
			return err
		}
	}
	if k.component.Kind == graphstate.SetMember {
		value, wire, found, err := q.q.value(ValueRef{q.q.c.root.namespace.Graph, k.component.Member})
		if err != nil {
			return err
		}
		if !found || value.Value.Kind() != definition.Type {
			return ErrCorrupt
		}
		if err := q.q.materialize(len(wire)); err != nil {
			return err
		}
		normalized, found, err := q.q.canonicalIdentity(string(wire))
		if err != nil {
			return err
		}
		if !found || normalized.Ref.ID != canonical {
			return ErrCorrupt
		}
	}
	return nil
}
func (q *pageReader) validateIncident(k postingKey) error {
	r, found, err := q.q.entity(EntityRef{q.q.c.root.namespace.Graph, k.relationship})
	if err != nil {
		return err
	}
	if !found || r.Kind != graphstate.Relationship {
		return ErrCorrupt
	}
	expected := canonicalIncidentKeys(r)
	if k.family == declaredIncidentRecord {
		if r.Mode != graphstate.LifeBound {
			return ErrCorrupt
		}
		life, found, err := q.q.life(LifeRef{q.q.c.root.namespace.Graph, r.ID, k.life})
		if err != nil {
			return err
		}
		if !found {
			return ErrCorrupt
		}
		expected = declaredIncidentKeys(r, life)
		if err := q.checkDeclared(r, life); err != nil {
			return err
		}
	} else if err := q.checkCanonical(r); err != nil {
		return err
	}
	for _, entry := range expected {
		if entry == k {
			return nil
		}
	}
	return ErrCorrupt
}
