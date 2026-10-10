package graphstore

import (
	"slices"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func fullPresenceLimits(q *pageReader) currentPresenceTreeLimits {
	l := defaultCurrentPresenceTreeLimits()
	l.pages = q.limits
	l.codec.temporal = q.q.c.limits.Temporal
	l.codec.maxPageBytes = min(l.codec.maxPageBytes, q.q.c.limits.MaxRecordBytes)
	l.targetBytes = min(l.targetBytes, l.codec.maxPageBytes)
	l.codec.maxChildren = min(l.codec.maxChildren, q.limits.MaxChildren)
	l.codec.maxLevels = min(l.codec.maxLevels, q.limits.MaxLevels)
	return l
}

type fullPresenceStage struct {
	p    *pageStage
	wire []byte
}

func samePresenceRuns(a, b []presenceRun, l temporal.Limits) (bool, error) {
	if len(a) != len(b) {
		return false, nil
	}
	for i := range a {
		if a[i].life != b[i].life {
			return false, nil
		}
		same, err := a[i].scope.SameSupport(b[i].scope, l)
		if err != nil || !same {
			return same, err
		}
	}
	return true, nil
}
func (s *fullPresenceStage) coordinate(p temporal.Position) ([]byte, error) {
	q := s.p.pageReader
	capacity, scratch := cpPositionBudget(p, q.q.c.limits.Temporal)
	if err := q.q.materialize(scratch); err != nil {
		return nil, err
	}
	if cap(s.wire) < capacity {
		if err := q.q.materialize(capacity); err != nil {
			return nil, err
		}
		s.wire = make([]byte, 0, capacity)
	}
	encoded, err := temporal.AppendPosition(s.wire[:0], p, q.q.c.limits.Temporal)
	if err != nil {
		return nil, callerError(err)
	}
	if len(encoded) < 52 || cap(encoded) > cap(s.wire) {
		return nil, ErrCorrupt
	}
	body := encoded[52:]
	if err := q.q.materialize(len(body)); err != nil {
		return nil, err
	}
	return exactCopy(body), nil
}
func (s *fullPresenceStage) atoms(entity graphstate.EntityRecord, runs []presenceRun) ([]currentPresenceAtom, error) {
	q := s.p.pageReader
	count := 2 * len(runs)
	if count > defaultCurrentPresenceTreeLimits().maxEdits {
		return nil, ErrResourceLimit
	}
	if err := q.q.materialize(cpAtomOwned * count); err != nil {
		return nil, err
	}
	out := make([]currentPresenceAtom, 0, count)
	d := entity.Axis.Descriptor()
	axis := currentPresenceAxis{d.ID, entity.Axis.DefinitionHash(), d.Profile}
	for _, run := range runs {
		binding, found, err := q.q.life(LifeRef{q.q.c.root.namespace.Graph, entity.ID, run.life})
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, ErrCorrupt
		}
		a := currentPresenceAtom{axis: axis, endpoint: entity.Source, relationship: entity.ID, life: run.life, mode: entity.Mode, flags: cpSource}
		if entity.Mode == graphstate.LifeBound {
			a.bound = binding.SourceLife
		}
		lo, hi, _ := run.scope.Bounds()
		if run.scope.Kind() == temporal.ScopePoint {
			a.flags |= cpPoint
		} else {
			if lo.Kind() == temporal.BoundNegativeInfinity {
				a.flags |= cpLowerInfinite
			} else if lo.Inclusive() {
				a.flags |= cpLowerClosed
			}
			if hi.Kind() == temporal.BoundPositiveInfinity {
				a.flags |= cpUpperInfinite
			} else if hi.Inclusive() {
				a.flags |= cpUpperClosed
			}
		}
		if a.flags&cpLowerInfinite == 0 {
			pos, _ := lo.Position()
			a.lower, err = s.coordinate(pos)
			if err != nil {
				return nil, err
			}
		}
		if a.flags&(cpPoint|cpUpperInfinite) == 0 {
			pos, _ := hi.Position()
			a.upper, err = s.coordinate(pos)
			if err != nil {
				return nil, err
			}
		}
		b := a
		b.endpoint = entity.Target
		b.flags &^= cpSource
		b.flags |= cpTarget
		if entity.Mode == graphstate.LifeBound {
			b.bound = binding.TargetLife
		}
		if a.endpoint == b.endpoint && a.bound == b.bound {
			a.flags |= cpTarget
			out = append(out, a)
		} else {
			out = append(out, a, b)
		}
	}
	var compareErr error
	slices.SortFunc(out, func(a, b currentPresenceAtom) int {
		c, e := cpAtomCompare(a, b, fullPresenceLimits(q).codec)
		if e != nil {
			compareErr = e
		}
		return c
	})
	return out, compareErr
}
func (s *fullPresenceStage) repair(entity graphstate.EntityRecord, before, after []presenceRun) error {
	same, err := samePresenceRuns(before, after, s.p.q.c.limits.Temporal)
	if err != nil || same {
		return err
	}
	old, err := s.atoms(entity, before)
	if err != nil {
		return err
	}
	fresh, err := s.atoms(entity, after)
	if err != nil {
		return err
	}
	// Keep simultaneous old/new arrays charged. Compact only exact equal atoms;
	// equal keys with changed upper bounds are still checked replacements.
	if err := s.p.q.materialize(cpAtomOwned * (len(old) + len(fresh))); err != nil {
		return err
	}
	remove, insert := make([]currentPresenceAtom, 0, len(old)), make([]currentPresenceAtom, 0, len(fresh))
	for _, a := range old {
		unchanged := false
		for _, b := range fresh {
			if cpAtomEqual(a, b) {
				unchanged = true
				break
			}
		}
		if !unchanged {
			remove = append(remove, a)
		}
	}
	for _, b := range fresh {
		unchanged := false
		for _, a := range old {
			if cpAtomEqual(a, b) {
				unchanged = true
				break
			}
		}
		if !unchanged {
			insert = append(insert, b)
		}
	}
	l := fullPresenceLimits(s.p.pageReader)
	out, err := stageCurrentPresenceInOperation(s.p, s.p.q.full.descriptor.own, remove, insert, l)
	if err != nil {
		return err
	}
	s.p.q.full.descriptor.own = out.tree
	return nil
}
