package graphstate

import (
	"cmp"
	"slices"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

type activeValue struct {
	value Scalar
	scope temporal.Scope
}

func (e *engine) lifeSupports(owner EntityID, life LifeID, window temporal.Scope) ([]temporal.Scope, error) {
	pages, err := e.component(ComponentKey{Owner: owner, Kind: Presence}, window)
	if err != nil {
		return nil, err
	}
	out := []temporal.Scope{}
	for _, page := range pages {
		for _, p := range page.Data.Pieces() {
			if p.Cell().Present() && LifeID(p.Cell().Value().ID()) == life {
				out = append(out, p.Scope())
				if len(out) > e.limits.MaxRows {
					return nil, ErrResourceLimit
				}
			}
		}
	}
	return out, nil
}
func (e *engine) mask(parts []temporal.Scope, owner EntityID, life LifeID) ([]temporal.Scope, error) {
	out := []temporal.Scope{}
	for _, part := range parts {
		live, err := e.lifeSupports(owner, life, part)
		if err != nil {
			return nil, err
		}
		out = append(out, live...)
		if len(out) > e.limits.MaxRows {
			return nil, ErrResourceLimit
		}
	}
	return out, nil
}
func (e *engine) claims(key ComponentKey, window temporal.Scope) ([]activeValue, error) {
	owner, found, err := e.entity(key.Owner)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ErrContradictoryRead
	}
	binding, found, err := e.life(key.Owner, key.Life)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ErrContradictoryRead
	}
	pages, err := e.component(key, window)
	if err != nil {
		return nil, err
	}
	out := []activeValue{}
	for _, page := range pages {
		for _, piece := range page.Data.Pieces() {
			cell := piece.Cell()
			if !cell.Present() {
				continue
			}
			var value Scalar
			if key.Kind == SetMember {
				value, err = e.value(key.Member)
			} else {
				if cell.Value().IsNull() {
					continue
				}
				value, err = e.value(ValueID(cell.Value().ID()))
			}
			if err != nil {
				return nil, err
			}
			parts, err := e.lifeSupports(key.Owner, key.Life, piece.Scope())
			if err != nil {
				return nil, err
			}
			if owner.Kind == Relationship && owner.Mode == LifeBound {
				parts, err = e.mask(parts, owner.Source, binding.SourceLife)
				if err != nil {
					return nil, err
				}
				parts, err = e.mask(parts, owner.Target, binding.TargetLife)
				if err != nil {
					return nil, err
				}
			}
			for _, scope := range parts {
				out = append(out, activeValue{value, scope})
				if len(out) > e.limits.MaxRows {
					return nil, ErrResourceLimit
				}
			}
		}
	}
	return out, nil
}
func (e *engine) incident(p IncidentPredicate) ([]EntityID, error) {
	t := newTracker()
	out := []EntityID{}
	seen := make(map[EntityID]struct{})
	for {
		if err := e.check(); err != nil {
			return nil, err
		}
		page, err := e.view.IncidentRelationships(e.ctx, p, t.cursor, ReadBudget{e.limits.MaxRows - e.rows, e.limits.MaxReadBytes - e.readBytes})
		if err != nil {
			return nil, err
		}
		if err := e.source(page.View); err != nil {
			return nil, err
		}
		if err := e.charge(len(page.Entities), len(page.Entities)*8); err != nil {
			return nil, err
		}
		if err := e.dep(Dependency{Kind: IncidentDependency, Version: page.Version, Incident: p}); err != nil {
			return nil, err
		}
		for _, id := range page.Entities {
			if id == 0 {
				return nil, ErrContradictoryRead
			}
			if _, ok := seen[id]; ok {
				return nil, ErrContradictoryRead
			}
			seen[id] = struct{}{}
			out = append(out, id)
		}
		if err := e.next(&t, page.Next, page.Complete); err != nil {
			return nil, err
		}
		if page.Complete {
			break
		}
	}
	for _, record := range e.delta.Entities {
		if record.Kind == Relationship && (record.Source == p.Endpoint || record.Target == p.Endpoint) {
			if _, ok := seen[record.ID]; !ok {
				seen[record.ID] = struct{}{}
				out = append(out, record.ID)
			}
		}
	}
	slices.Sort(out)
	return out, nil
}
func (e *engine) candidates(p UniquePredicate) ([]UniqueClaim, error) {
	t := newTracker()
	out := []UniqueClaim{}
	seen := make(map[UniqueClaim]struct{})
	for {
		if err := e.check(); err != nil {
			return nil, err
		}
		page, err := e.view.UniqueCandidates(e.ctx, p, t.cursor, ReadBudget{e.limits.MaxRows - e.rows, e.limits.MaxReadBytes - e.readBytes})
		if err != nil {
			return nil, err
		}
		if err := e.source(page.View); err != nil {
			return nil, err
		}
		bytes := 0
		for _, claim := range page.Claims {
			if !validKey(claim.Key, e.limits) || p.Definition.Cardinality == ScalarCardinality && claim.Key.Kind != ScalarProperty || p.Definition.Cardinality == SetCardinality && claim.Key.Kind != SetMember {
				return nil, ErrContradictoryRead
			}
			if len(claim.Key.Name) > e.limits.MaxReadBytes-bytes-64 {
				return nil, ErrResourceLimit
			}
			bytes += 64 + len(claim.Key.Name)
		}
		if err := e.charge(len(page.Claims), bytes); err != nil {
			return nil, err
		}
		if err := e.dep(Dependency{Kind: UniquenessDependency, Version: page.Version, Unique: p}); err != nil {
			return nil, err
		}
		for _, claim := range page.Claims {
			if claim.Owner == 0 || claim.Life == 0 || claim.Key.Owner != claim.Owner || claim.Key.Life != claim.Life || claim.Key.Name != p.Definition.Name {
				return nil, ErrContradictoryRead
			}
			if _, ok := seen[claim]; ok {
				return nil, ErrContradictoryRead
			}
			seen[claim] = struct{}{}
			out = append(out, claim)
		}
		if err := e.next(&t, page.Next, page.Complete); err != nil {
			return nil, err
		}
		if page.Complete {
			break
		}
	}
	for _, patch := range e.delta.Patches {
		k := patch.Key
		if k.Name == p.Definition.Name && (k.Kind == ScalarProperty || k.Kind == SetMember) {
			claim := UniqueClaim{k.Owner, k.Life, k}
			if _, ok := seen[claim]; !ok {
				seen[claim] = struct{}{}
				out = append(out, claim)
			}
		}
	}
	slices.SortFunc(out, func(a, b UniqueClaim) int { return compareKey(a.Key, b.Key) })
	return out, nil
}
func (e *engine) validateUniqueness() error {
	// Freeze mutations before adding read-only checks. All queries below replay
	// the FINAL overlay, so same-transaction swaps never see intermediate claims.
	patches := append([]ComponentPatch(nil), e.delta.Patches...)
	targets := make(map[lifeKey][]temporal.Scope)
	for _, patch := range patches {
		if patch.Key.Kind != Presence {
			targets[lifeKey{patch.Key.Owner, patch.Key.Life}] = append(targets[lifeKey{patch.Key.Owner, patch.Key.Life}], patch.Owned)
			continue
		}
		owner, found, err := e.entity(patch.Key.Owner)
		if err != nil {
			return err
		}
		if !found {
			return ErrContradictoryRead
		}
		for _, change := range patch.Changes {
			ids := []LifeID{}
			if change.Before().Present() {
				ids = append(ids, LifeID(change.Before().Value().ID()))
			}
			if change.After().Present() {
				ids = append(ids, LifeID(change.After().Value().ID()))
			}
			for _, life := range ids {
				targets[lifeKey{owner.ID, life}] = append(targets[lifeKey{owner.ID, life}], change.Scope())
			}
			if owner.Kind == Node {
				for _, life := range ids {
					incident, err := e.incident(IncidentPredicate{owner.ID, life, change.Scope()})
					if err != nil {
						return err
					}
					for _, id := range incident {
						pages, err := e.component(ComponentKey{Owner: id, Kind: Presence}, change.Scope())
						if err != nil {
							return err
						}
						for _, page := range pages {
							for _, p := range page.Data.Pieces() {
								if p.Cell().Present() {
									lk := lifeKey{id, LifeID(p.Cell().Value().ID())}
									targets[lk] = append(targets[lk], p.Scope())
								}
							}
						}
					}
				}
			}
		}
	}
	ordered := make([]lifeKey, 0, len(targets))
	for target := range targets {
		ordered = append(ordered, target)
	}
	slices.SortFunc(ordered, func(a, b lifeKey) int { return cmp.Or(cmp.Compare(a.owner, b.owner), cmp.Compare(a.life, b.life)) })
	for _, target := range ordered {
		windows := targets[target]
		keys, err := e.keys(KeyPredicate{Owner: target.owner, Life: target.life})
		if err != nil {
			return err
		}
		for _, key := range keys {
			if key.Kind != ScalarProperty && key.Kind != SetMember {
				continue
			}
			definition, err := e.property(key.Name)
			if err != nil {
				return err
			}
			if definition.Unique == UniqueNone {
				continue
			}
			for _, window := range windows {
				ownClaims, err := e.claims(key, window)
				if err != nil {
					return err
				}
				for _, own := range ownClaims {
					candidates, err := e.candidates(UniquePredicate{definition, own.value, own.scope})
					if err != nil {
						return err
					}
					for _, candidate := range candidates {
						if candidate.Owner == key.Owner {
							continue
						}
						other, found, err := e.entity(candidate.Owner)
						if err != nil {
							return err
						}
						if !found {
							return ErrContradictoryRead
						}
						if other.Kind != definition.Owner {
							continue
						}
						claims, err := e.claims(candidate.Key, own.scope)
						if err != nil {
							return err
						}
						for _, claim := range claims {
							equal, err := own.value.Equal(claim.value, e.limits)
							if err != nil {
								return err
							}
							if !equal {
								continue
							}
							overlap, err := own.scope.Overlaps(claim.scope, e.limits.Component.Temporal)
							if err != nil {
								return err
							}
							if overlap {
								return ErrUniqueOverlap
							}
						}
					}
				}
			}
		}
	}
	return nil
}
