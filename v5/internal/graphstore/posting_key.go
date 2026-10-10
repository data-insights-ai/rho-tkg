package graphstore

import (
	"cmp"
	"encoding/binary"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
)

type postingKey struct {
	family       recordKind
	owner        graphstate.EntityKind
	scalar       graphstate.ScalarKind
	name         string
	value        graphstate.ValueID
	component    graphstate.ComponentKey
	endpoint     graphstate.EntityID
	bound        graphstate.LifeID
	mode         graphstate.ReferenceMode
	relationship graphstate.EntityID
	life         graphstate.LifeID
	roles        byte
}

func validPostingKind(k recordKind) bool {
	return k == uniquePostingRecord || k == canonicalIncidentRecord || k == declaredIncidentRecord
}
func validPostingKey(k postingKey, l Limits) bool {
	switch k.family {
	case uniquePostingRecord:
		expected := postingKey{family: k.family, owner: k.owner, scalar: k.scalar, name: k.name, value: k.value, component: k.component}
		return k == expected && (k.owner == graphstate.Node || k.owner == graphstate.Relationship) && k.scalar >= graphstate.ScalarString && k.scalar <= graphstate.ScalarDescriptor && validName(k.name, l) && k.value != 0 && validComponent(k.component, l) && k.component.Name == k.name && (k.component.Kind == graphstate.ScalarProperty || k.component.Kind == graphstate.SetMember)
	case canonicalIncidentRecord:
		expected := postingKey{family: k.family, endpoint: k.endpoint, mode: k.mode, relationship: k.relationship, roles: k.roles}
		return k == expected && k.endpoint != 0 && k.relationship != 0 && (k.mode == graphstate.LifeBound || k.mode == graphstate.IdentityReference) && k.roles > 0 && k.roles <= 3
	case declaredIncidentRecord:
		expected := postingKey{family: k.family, endpoint: k.endpoint, bound: k.bound, relationship: k.relationship, life: k.life, roles: k.roles}
		return k == expected && k.endpoint != 0 && k.bound != 0 && k.relationship != 0 && k.life != 0 && k.roles > 0 && k.roles <= 3
	}
	return false
}
func comparePostingKeys(a, b postingKey) int {
	if order := cmp.Compare(a.family, b.family); order != 0 {
		return order
	}
	switch a.family {
	case uniquePostingRecord:
		return cmp.Or(cmp.Compare(a.owner, b.owner), cmp.Compare(a.name, b.name), cmp.Compare(a.scalar, b.scalar), cmp.Compare(a.value, b.value), compareComponentKeys(a.component, b.component))
	case canonicalIncidentRecord:
		return cmp.Or(cmp.Compare(a.endpoint, b.endpoint), cmp.Compare(a.mode, b.mode), cmp.Compare(a.relationship, b.relationship))
	default:
		return cmp.Or(cmp.Compare(a.endpoint, b.endpoint), cmp.Compare(a.bound, b.bound), cmp.Compare(a.relationship, b.relationship), cmp.Compare(a.life, b.life))
	}
}
func postingKeyMinimum(kind recordKind) int {
	switch kind {
	case uniquePostingRecord:
		return 43
	case canonicalIncidentRecord:
		return 18
	default:
		return 33
	}
}
func appendPostingKey(b []byte, k postingKey) []byte {
	switch k.family {
	case uniquePostingRecord:
		b = append(b, byte(k.owner))
		b = appendField(b, []byte(k.name))
		b = append(b, byte(k.scalar))
		b = binary.BigEndian.AppendUint64(b, uint64(k.value))
		return appendComponent(b, k.component)
	case canonicalIncidentRecord:
		b = binary.BigEndian.AppendUint64(b, uint64(k.endpoint))
		b = append(b, byte(k.mode))
		b = binary.BigEndian.AppendUint64(b, uint64(k.relationship))
	default:
		for _, v := range []uint64{uint64(k.endpoint), uint64(k.bound), uint64(k.relationship), uint64(k.life)} {
			b = binary.BigEndian.AppendUint64(b, v)
		}
	}
	return append(b, k.roles)
}
func decodePostingKey(c *cursor, kind recordKind, l Limits) (postingKey, error) {
	k := postingKey{family: kind}
	var err error
	if kind == uniquePostingRecord {
		owner, e := c.tag()
		if e != nil {
			return postingKey{}, e
		}
		k.owner = graphstate.EntityKind(owner)
		name, e := c.field(l.MaxNameBytes)
		if e != nil {
			return postingKey{}, e
		}
		k.name = string(name)
		scalar, e := c.tag()
		if e != nil {
			return postingKey{}, e
		}
		k.scalar = graphstate.ScalarKind(scalar)
		value, e := c.number()
		if e != nil {
			return postingKey{}, e
		}
		k.value = graphstate.ValueID(value)
		k.component, err = decodeComponent(c, l)
	} else {
		endpoint, e := c.number()
		if e != nil {
			return postingKey{}, e
		}
		k.endpoint = graphstate.EntityID(endpoint)
		if kind == canonicalIncidentRecord {
			mode, e := c.tag()
			if e != nil {
				return postingKey{}, e
			}
			k.mode = graphstate.ReferenceMode(mode)
			relation, e := c.number()
			if e != nil {
				return postingKey{}, e
			}
			k.relationship = graphstate.EntityID(relation)
		} else {
			bound, e := c.number()
			if e != nil {
				return postingKey{}, e
			}
			k.bound = graphstate.LifeID(bound)
			relation, e := c.number()
			if e != nil {
				return postingKey{}, e
			}
			k.relationship = graphstate.EntityID(relation)
			life, e := c.number()
			if e != nil {
				return postingKey{}, e
			}
			k.life = graphstate.LifeID(life)
		}
		k.roles, err = c.tag()
	}
	if err != nil {
		return postingKey{}, err
	}
	if !validPostingKey(k, l) {
		return postingKey{}, ErrCorrupt
	}
	return k, nil
}
