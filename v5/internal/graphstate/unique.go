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
		pieces, err := e.pieces(page.Data)
		if err != nil {
			return nil, err
		}
		for _, p := range pieces {
			if p.Cell().Present() && LifeID(p.Cell().Value().ID()) == life {
				out, err = appendOwned(e, out, p.Scope(), 128)
				if err != nil {
					return nil, err
				}
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
		for _, scope := range live {
			out, err = appendOwned(e, out, scope, 128)
			if err != nil {
				return nil, err
			}
		}
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
		pieces, err := e.pieces(page.Data)
		if err != nil {
			return nil, err
		}
		for _, piece := range pieces {
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
				out, err = appendOwned(e, out, activeValue{value, scope}, 384)
				if err != nil {
					return nil, err
				}
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
	seenSlots := 0
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
			if err := e.reserveMap(len(seen), &seenSlots, 64); err != nil {
				return nil, err
			}
			seen[id] = struct{}{}
			out, err = appendOwned(e, out, id, 16)
			if err != nil {
				return nil, err
			}
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
				if err := e.reserveMap(len(seen), &seenSlots, 64); err != nil {
					return nil, err
				}
				seen[record.ID] = struct{}{}
				var err error
				out, err = appendOwned(e, out, record.ID, 16)
				if err != nil {
					return nil, err
				}
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
	seenSlots := 0
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
			if !validKey(claim.Key, e.limits) || (claim.Key.Kind != ScalarProperty && claim.Key.Kind != SetMember) {
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
			owner, found, err := e.entity(claim.Owner)
			if err != nil {
				return nil, err
			}
			if !found {
				return nil, ErrContradictoryRead
			}
			// Candidate access promises a complete superset. The same name may
			// have a different cardinality in a foreign owner's schema; filter
			// that irrelevant namespace before checking our schema's shape.
			if owner.Kind != p.Definition.Owner {
				continue
			}
			if p.Definition.Cardinality == ScalarCardinality && claim.Key.Kind != ScalarProperty || p.Definition.Cardinality == SetCardinality && claim.Key.Kind != SetMember {
				return nil, ErrContradictoryRead
			}
			if _, ok := seen[claim]; ok {
				return nil, ErrContradictoryRead
			}
			if err := e.reserveMap(len(seen), &seenSlots, 192); err != nil {
				return nil, err
			}
			seen[claim] = struct{}{}
			out, err = appendOwned(e, out, claim, 192)
			if err != nil {
				return nil, err
			}
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
				if err := e.reserveMap(len(seen), &seenSlots, 192); err != nil {
					return nil, err
				}
				seen[claim] = struct{}{}
				var err error
				out, err = appendOwned(e, out, claim, 192)
				if err != nil {
					return nil, err
				}
			}
		}
	}
	slices.SortFunc(out, func(a, b UniqueClaim) int { return compareKey(a.Key, b.Key) })
	return out, nil
}
func (e *engine) validateUniqueness() error {
	// Freeze mutations before adding read-only checks. All queries below replay
	// the FINAL overlay, so same-transaction swaps never see intermediate claims.
	if e.budget != nil {
		if err := e.budget.Reserve(512 * len(e.delta.Patches)); err != nil {
			return err
		}
	}
	patches := append([]ComponentPatch(nil), e.delta.Patches...)
	targets := make(map[lifeKey][]temporal.Scope)
	targetSlots := 0
	addTarget := func(key lifeKey, scope temporal.Scope) error {
		if _, found := targets[key]; !found {
			if err := e.reserveMap(len(targets), &targetSlots, 128); err != nil {
				return err
			}
		}
		value, err := appendOwned(e, targets[key], scope, 128)
		if err != nil {
			return err
		}
		targets[key] = value
		return nil
	}
	for _, patch := range patches {
		if patch.Key.Kind != Presence {
			if err := addTarget(lifeKey{patch.Key.Owner, patch.Key.Life}, patch.Owned); err != nil {
				return err
			}
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
			// Explicit absence only removes owner/endpoint life support, so it
			// cannot add an effective uniqueness claim. Other mutations still
			// validate their claims against the final overlay, including this
			// removal; restoration/addition keeps the incident checks below.
			if !change.After().Present() {
				continue
			}
			ids := []LifeID{}
			if change.Before().Present() {
				ids = append(ids, LifeID(change.Before().Value().ID()))
			}
			if change.After().Present() {
				ids = append(ids, LifeID(change.After().Value().ID()))
			}
			for _, life := range ids {
				if err := addTarget(lifeKey{owner.ID, life}, change.Scope()); err != nil {
					return err
				}
			}
			if owner.Kind == Node {
				for _, life := range ids {
					incident, err := e.incident(IncidentPredicate{owner.ID, life, change.Scope()})
					if err != nil {
						return err
					}
					for _, id := range incident {
						relation, found, err := e.entity(id)
						if err != nil {
							return err
						}
						if !found || relation.Kind != Relationship || relation.Source != owner.ID && relation.Target != owner.ID {
							return ErrContradictoryRead
						}
						// Stable-identity references do not depend on endpoint lives
						// and may use another axis. Do not read their presence using
						// this endpoint's coordinate window.
						if relation.Mode == IdentityReference {
							continue
						}
						pages, err := e.component(ComponentKey{Owner: id, Kind: Presence}, change.Scope())
						if err != nil {
							return err
						}
						for _, page := range pages {
							pieces, err := e.pieces(page.Data)
							if err != nil {
								return err
							}
							for _, p := range pieces {
								if p.Cell().Present() {
									lk := lifeKey{id, LifeID(p.Cell().Value().ID())}
									binding, found, err := e.life(id, lk.life)
									if err != nil {
										return err
									}
									if !found {
										return ErrContradictoryRead
									}
									sourceAffected := relation.Source == owner.ID && binding.SourceLife == life
									targetAffected := relation.Target == owner.ID && binding.TargetLife == life
									if !sourceAffected && !targetAffected {
										continue
									}
									if err := addTarget(lk, p.Scope()); err != nil {
										return err
									}
								}
							}
						}
					}
				}
			}
		}
	}
	if e.budget != nil {
		if err := e.budget.Reserve(16 * len(targets)); err != nil {
			return err
		}
	}
	ordered := make([]lifeKey, 0, len(targets))
	for target := range targets {
		ordered = append(ordered, target)
	}
	slices.SortFunc(ordered, func(a, b lifeKey) int { return cmp.Or(cmp.Compare(a.owner, b.owner), cmp.Compare(a.life, b.life)) })
	for _, target := range ordered {
		owner, found, err := e.entity(target.owner)
		if err != nil {
			return err
		}
		if !found {
			return ErrContradictoryRead
		}
		windows := targets[target]
		keys, err := e.keys(KeyPredicate{Owner: target.owner, Life: target.life})
		if err != nil {
			return err
		}
		for _, key := range keys {
			if key.Kind != ScalarProperty && key.Kind != SetMember {
				continue
			}
			definition, err := e.property(owner.Kind, key.Name)
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
							equal, err := e.scalarEqual(own.value, claim.value)
							if err != nil {
								return err
							}
							if !equal {
								continue
							}
							if err := e.reservePredicate(own.scope, claim.scope); err != nil {
								return err
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
