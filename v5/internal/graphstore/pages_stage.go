package graphstore

import (
	"context"
	"encoding/binary"
	"errors"
	"strings"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

type pageStage struct {
	*pageReader
	root Root
}

func (q *pageStage) reserve() (uint64, error) {
	root, id, err := q.root.ReservePhysical(1)
	if err != nil {
		return 0, err
	}
	q.root = root
	return id, nil
}
func (q *pageStage) putDirectory(d directoryPage) error {
	b, err := encodeDirectory(q.root.namespace, d, q.q.c, q.limits)
	if err != nil {
		return err
	}
	return q.q.put(physicalKey(q.root.namespace, directoryRecord, d.ID), b)
}

func (q *pageStage) validatePatch(p graphstate.ComponentPatch) (ComponentChangeGroup, error) {
	if p.Owned.Kind() == temporal.ScopeEmpty || p.Owned.Kind() == temporal.ScopeUnplaced {
		return ComponentChangeGroup{}, ErrInvalid
	}
	if _, err := temporal.AppendScope(nil, p.Owned, q.q.c.limits.Temporal); err != nil {
		return ComponentChangeGroup{}, callerError(err)
	}
	definition, err := q.validateKey(p.Key, p.Owned.Axis())
	if err != nil {
		return ComponentChangeGroup{}, err
	}
	if _, err := state.AppendState(nil, p.State, q.limits.codecLimits(q.q.c)); err != nil {
		return ComponentChangeGroup{}, pageFailure(q.q.c, err, false)
	}
	if _, err := p.State.Slice(p.Owned, q.limits.stateLimits(q.q.c)); err != nil {
		return ComponentChangeGroup{}, pageFailure(q.q.c, err, false)
	}
	if err := q.q.materialize(p.State.Usage().MetadataBytes()); err != nil {
		return ComponentChangeGroup{}, err
	}
	if err := q.budget(); err != nil {
		return ComponentChangeGroup{}, err
	}
	for _, piece := range p.State.Pieces() {
		ok, err := within(piece.Scope(), p.Owned, q.q.c.limits.Temporal)
		if err != nil || !ok {
			return ComponentChangeGroup{}, ErrInvalid
		}
		if err := q.validateCell(p.Key, definition, piece.Cell()); err != nil {
			return ComponentChangeGroup{}, err
		}
	}
	wire, err := state.AppendChanges(nil, p.Owned.Axis(), p.Changes, q.limits.codecLimits(q.q.c))
	if err != nil {
		return ComponentChangeGroup{}, pageFailure(q.q.c, err, false)
	}
	if len(wire) > q.limits.MaxChangeBytes {
		return ComponentChangeGroup{}, ErrResourceLimit
	}
	for _, change := range p.Changes {
		ok, err := within(change.Scope(), p.Owned, q.q.c.limits.Temporal)
		if err != nil || !ok {
			return ComponentChangeGroup{}, ErrInvalid
		}
		if err := q.validateCell(p.Key, definition, change.After()); err != nil {
			return ComponentChangeGroup{}, err
		}
	}
	ownedWire, err := temporal.AppendScope(nil, p.Owned, q.q.c.limits.Temporal)
	if err != nil {
		return ComponentChangeGroup{}, callerError(err)
	}
	owned, err := temporal.DecodeScope(ownedWire, p.Owned.Axis(), q.q.c.limits.Temporal)
	if err != nil {
		return ComponentChangeGroup{}, err
	}
	changes, _, err := state.DecodeChanges(wire, p.Owned.Axis(), q.limits.codecLimits(q.q.c))
	if err != nil {
		return ComponentChangeGroup{}, err
	}
	if err := q.q.materialize(len(wire) + len(ownedWire) + len(p.Key.Name) + 64); err != nil {
		return ComponentChangeGroup{}, err
	}
	key := p.Key
	key.Name = strings.Clone(p.Key.Name)
	return ComponentChangeGroup{key, owned, changes}, q.budget()
}
func (q *pageStage) checkpointLeaf(d directoryPage, s state.State) ([]childPage, error) {
	wire, err := state.AppendState(nil, s, q.limits.codecLimits(q.q.c))
	if err != nil {
		return nil, err
	}
	if s.Usage().Pieces() <= q.limits.MaxCells && len(wire) <= q.limits.MaxCheckpointBytes {
		id, err := q.reserve()
		if err != nil {
			return nil, err
		}
		b, err := encodeCheckpoint(q.root.namespace, id, d.Key, s, q.q.c, q.limits)
		if err != nil {
			return nil, err
		}
		if err := q.q.put(physicalKey(q.root.namespace, checkpointRecord, id), b); err != nil {
			return nil, err
		}
		d.Base, d.Head = id, 0
		d.TailRecords, d.TailBytes, d.TailAtoms = 0, 0, 0
		d.Cells = s.Usage().Pieces()
		if err := q.putDirectory(d); err != nil {
			return nil, err
		}
		return []childPage{{d.ID, d.Owned}}, nil
	}
	pieces := s.Pieces()
	if len(pieces) < 2 {
		return nil, ErrResourceLimit
	}
	pivot, _, _ := pieces[len(pieces)/2].Scope().Bounds()
	position, finite := pivot.Position()
	if !finite {
		return nil, ErrResourceLimit
	}
	lo, hi, _ := d.Owned.Bounds()
	exclusive, err := temporal.FiniteBound(position, !pivot.Inclusive())
	if err != nil {
		return nil, err
	}
	inclusive, err := temporal.FiniteBound(position, pivot.Inclusive())
	if err != nil {
		return nil, err
	}
	left, err := temporal.Span(d.Owned.Axis(), lo, exclusive, q.q.c.limits.Temporal)
	if err != nil {
		return nil, err
	}
	right, err := temporal.Span(d.Owned.Axis(), inclusive, hi, q.q.c.limits.Temporal)
	if err != nil {
		return nil, err
	}
	if left.Kind() == temporal.ScopeEmpty || right.Kind() == temporal.ScopeEmpty {
		return nil, ErrResourceLimit
	}
	id, err := q.reserve()
	if err != nil {
		return nil, err
	}
	out := make([]childPage, 0, 2)
	for _, half := range []directoryPage{{ID: d.ID, Key: d.Key, Owned: left}, {ID: id, Key: d.Key, Owned: right}} {
		part, err := s.Slice(half.Owned, q.limits.stateLimits(q.q.c))
		if err != nil {
			return nil, err
		}
		children, err := q.checkpointLeaf(half, part)
		if err != nil {
			return nil, err
		}
		out = append(out, children...)
		if len(out) > q.limits.MaxChildren {
			return nil, ErrResourceLimit
		}
	}
	return out, nil
}
func (q *pageStage) updateLeaf(d directoryPage, p graphstate.ComponentPatch) ([]childPage, error) {
	current, err := q.materialize(d)
	if err != nil {
		return nil, err
	}
	physical := make([]state.Change, 0, len(p.Changes))
	for _, change := range p.Changes {
		common, err := change.Scope().Intersection(d.Owned, q.q.c.limits.Temporal)
		if err != nil {
			return nil, err
		}
		if common.Kind() == temporal.ScopeEmpty {
			continue
		}
		if err := verifyBefore(current, common, change.Before(), q.limits.stateLimits(q.q.c)); err != nil {
			return nil, errors.Join(ErrInvalid, err)
		}
		result, err := applyPageCell(current, common, change.After(), q.limits.stateLimits(q.q.c))
		if err != nil {
			return nil, pageFailure(q.q.c, err, false)
		}
		current = result.State()
		physical = append(physical, result.Changes()...)
	}
	common, err := p.Owned.Intersection(d.Owned, q.q.c.limits.Temporal)
	if err != nil {
		return nil, err
	}
	actual, err := current.Slice(common, q.limits.stateLimits(q.q.c))
	if err != nil {
		return nil, err
	}
	expected, err := p.State.Slice(d.Owned, q.limits.stateLimits(q.q.c))
	if err != nil {
		return nil, err
	}
	same, err := equalState(actual, expected, q.q.c, q.limits)
	if err != nil {
		return nil, err
	}
	if !same {
		return nil, errors.Join(ErrInvalid, ErrPatchConflict)
	}
	if len(physical) == 0 {
		return []childPage{{d.ID, d.Owned}}, nil
	}
	// Physical replay owns its atomic leaf; original write ownership and CDC
	// remain in the separately ordered returned change group.
	id, err := q.reserve()
	if err != nil {
		return nil, err
	}
	patch := patchPage{id, d.Head, d.Key, d.Owned, physical}
	wire, err := encodePatch(q.root.namespace, patch, q.q.c, q.limits)
	if err != nil {
		return nil, pageFailure(q.q.c, err, false)
	}
	if d.TailRecords == q.limits.MaxTailRecords || len(wire) > q.limits.MaxTailBytes-d.TailBytes || len(physical) > q.limits.MaxTailAtoms-d.TailAtoms || current.Usage().Pieces() > q.limits.MaxCells {
		return q.checkpointLeaf(d, current)
	}
	if err := q.q.put(physicalKey(q.root.namespace, patchRecord, id), wire); err != nil {
		return nil, err
	}
	d.Head = id
	d.TailRecords++
	d.TailBytes += len(wire)
	d.TailAtoms += len(physical)
	d.Cells = current.Usage().Pieces()
	if err := q.putDirectory(d); err != nil {
		return nil, err
	}
	return []childPage{{d.ID, d.Owned}}, nil
}
func (q *pageStage) updateDirectory(d directoryPage, p graphstate.ComponentPatch) ([]childPage, error) {
	if d.Level == 0 {
		return q.updateLeaf(d, p)
	}
	children := make([]childPage, 0, len(d.Children)+2)
	changed := false
	for _, child := range d.Children {
		overlap, err := child.Owned.Overlaps(p.Owned, q.q.c.limits.Temporal)
		if err != nil {
			return nil, err
		}
		if !overlap {
			children = append(children, child)
			continue
		}
		sub, err := q.directory(child.ID, d.Key, d.Owned.Axis())
		if err != nil {
			return nil, err
		}
		same, err := sameScope(sub.Owned, child.Owned, q.q.c.limits.Temporal)
		if err != nil || !same || sub.Level != d.Level-1 {
			return nil, ErrCorrupt
		}
		next, err := q.updateDirectory(sub, p)
		if err != nil {
			return nil, err
		}
		if len(next) != 1 || next[0].ID != child.ID {
			changed = true
		} else {
			same, err := sameScope(next[0].Owned, child.Owned, q.q.c.limits.Temporal)
			if err != nil {
				return nil, err
			}
			changed = changed || !same
		}
		children = append(children, next...)
		if len(children) > 2*q.limits.MaxChildren {
			return nil, ErrResourceLimit
		}
	}
	if !changed {
		return []childPage{{d.ID, d.Owned}}, nil
	}
	d.Children = children
	if len(children) <= q.limits.MaxChildren {
		if err := validateChildren(d, q.q.c.limits.Temporal); err != nil {
			return nil, err
		}
		if err := q.putDirectory(d); err != nil {
			return nil, err
		}
		return []childPage{{d.ID, d.Owned}}, nil
	}
	mid := len(children) / 2
	id, err := q.reserve()
	if err != nil {
		return nil, err
	}
	out := make([]childPage, 0, 2)
	for i, group := range [][]childPage{children[:mid], children[mid:]} {
		coverage, err := temporal.Empty(d.Owned.Axis())
		if err != nil {
			return nil, err
		}
		for _, child := range group {
			coverage, err = coverage.Union(child.Owned, q.q.c.limits.Temporal)
			if err != nil {
				return nil, err
			}
		}
		half := d
		half.Children = group
		half.Owned = coverage
		if i == 1 {
			half.ID = id
		}
		if err := validateChildren(half, q.q.c.limits.Temporal); err != nil {
			return nil, err
		}
		if err := q.putDirectory(half); err != nil {
			return nil, err
		}
		out = append(out, childPage{half.ID, coverage})
	}
	return out, nil
}
func (q *pageStage) installPatch(p graphstate.ComponentPatch) error {
	m, found, err := q.readMeta(p.Key)
	if err != nil {
		return err
	}
	fresh := !found
	if !found {
		if len(p.Changes) == 0 {
			empty, err := state.New(p.Owned.Axis(), q.limits.stateLimits(q.q.c))
			if err != nil {
				return err
			}
			same, err := equalState(empty, p.State, q.q.c, q.limits)
			if err != nil {
				return err
			}
			if !same {
				return errors.Join(ErrInvalid, ErrPatchConflict)
			}
			return nil
		}
		id, err := q.reserve()
		if err != nil {
			return err
		}
		m = componentMeta{p.Key, p.Owned.Axis(), id}
		all, err := temporal.All(m.Axis)
		if err != nil {
			return err
		}
		if err := q.putDirectory(directoryPage{ID: id, Key: m.Key, Owned: all}); err != nil {
			return err
		}
	}
	if m.Axis.Descriptor() != p.Owned.Axis().Descriptor() || m.Axis.DefinitionHash() != p.Owned.Axis().DefinitionHash() {
		return ErrInvalid
	}
	d, err := q.directory(m.Root, p.Key, m.Axis)
	if err != nil {
		return err
	}
	if d.Owned.Kind() != temporal.ScopeAll {
		return ErrCorrupt
	}
	next, err := q.updateDirectory(d, p)
	if err != nil {
		return err
	}
	if len(next) > 1 {
		if d.Level+1 >= q.limits.MaxLevels {
			return ErrResourceLimit
		}
		id, err := q.reserve()
		if err != nil {
			return err
		}
		parent := directoryPage{ID: id, Key: d.Key, Owned: d.Owned, Level: d.Level + 1, Children: next}
		if err := validateChildren(parent, q.q.c.limits.Temporal); err != nil {
			return err
		}
		if err := q.putDirectory(parent); err != nil {
			return err
		}
		m.Root = id
		fresh = true
	}
	if fresh {
		wire, err := encodeMeta(q.root.namespace, m, q.q.c.limits)
		if err != nil {
			return err
		}
		if err := q.q.put(componentKey(q.root.namespace, p.Key), wire); err != nil {
			return err
		}
		if q.q.indexes != nil {
			tree, err := q.insertComponentKey(q.q.indexes.descriptor.tree, p.Key, q.q.indexes.limits)
			if err != nil {
				return err
			}
			q.q.indexes.descriptor.tree = tree
		}
		if q.q.full != nil {
			tree, err := q.insertComponentKey(q.q.full.descriptor.keys, p.Key, keyTreeLimits(q.limits))
			if err != nil {
				return err
			}
			q.q.full.descriptor.keys = tree
		}
		return nil
	}
	return q.budget()
}
func validatePrivateRoot(s *Stage, r Root) error {
	baseline := s.c.root
	if s.full != nil {
		if r != s.full.root {
			return ErrInvalid
		}
		baseline = s.full.root
	}
	if s.indexed != nil {
		if r != s.indexed.root {
			return ErrInvalid
		}
		baseline = s.indexed.root
	}
	if r.namespace != baseline.namespace {
		return ErrNamespace
	}
	if r.owner != baseline.owner {
		return errors.Join(ErrInvalid, ErrStaleOwner)
	}
	if r.topology != baseline.topology || r.ownershipMode != baseline.ownershipMode || r.ownershipDigest != baseline.ownershipDigest || r.epoch != baseline.epoch || r.effect != baseline.effect || r.next < baseline.next {
		return ErrInvalid
	}
	for _, row := range s.writes {
		if len(row.Key) == 33 && row.Key[0] >= byte(directoryRecord) && row.Key[0] <= byte(patchRecord) {
			id := binary.BigEndian.Uint64(row.Key[25:])
			if id >= r.next {
				return ErrInvalid
			}
		}
	}
	return nil
}

// StageComponentPatches is a private materializer capability, not public Delta
// admission. The host must first plan constraints, admit allocator identities,
// and validate transaction dependencies against this view. This function checks
// references, exact before images and owned replacement state. It stages the
// ENTIRE supplied ordered sequence atomically, preserving preexisting writes on
// every refusal. It does not publish, acknowledge, certify or advance effects.
func StageComponentPatches(ctx context.Context, s *Stage, root Root, patches []graphstate.ComponentPatch, l PageLimits) (StagedComponents, error) {
	if s == nil {
		return StagedComponents{}, ErrInvalid
	}
	if s.full != nil {
		return StagedComponents{}, ErrTopologyUnsupported
	}
	l, err := l.resolve()
	if err != nil {
		return StagedComponents{}, err
	}
	if len(patches) > l.MaxPatches {
		return StagedComponents{}, ErrResourceLimit
	}
	var result StagedComponents
	err = s.operation(ctx, func(base *reader) error {
		if err := validatePrivateRoot(s, root); err != nil {
			return err
		}
		base.maxRows, base.maxBytes = l.MaxWorkRecords, l.MaxWorkBytes
		q := pageStage{&pageReader{q: base, limits: l}, root}
		q.allocation = &q.root
		if err := q.budget(); err != nil {
			return err
		}
		groups := make([]ComponentChangeGroup, 0, len(patches))
		bytes := 0
		for _, p := range patches {
			group, err := q.validatePatch(p)
			if err != nil {
				return err
			}
			wire, err := state.AppendChanges(nil, group.Owned.Axis(), group.Changes, l.codecLimits(s.c))
			if err != nil {
				return pageFailure(s.c, err, false)
			}
			scope, err := temporal.AppendScope(nil, group.Owned, s.c.limits.Temporal)
			if err != nil {
				return callerError(err)
			}
			cost := len(wire) + len(scope) + len(group.Key.Name) + 64
			if cost > l.MaxChangeBytes-bytes {
				return ErrResourceLimit
			}
			bytes += cost
			if err := q.installPatch(p); err != nil {
				return err
			}
			if err := q.budget(); err != nil {
				return err
			}
			groups = append(groups, group)
		}
		if err := q.budget(); err != nil {
			return err
		}
		if base.indexes != nil {
			base.indexes.root = q.root
			wire, err := encodeComponentIndexDescriptor(base.indexes.descriptor, q.root, s.c.limits)
			if err != nil {
				return err
			}
			if err := base.put(componentIndexDescriptorKey(q.root.namespace), wire); err != nil {
				return err
			}
		}
		result = StagedComponents{q.root, groups, q.work}
		return nil
	})
	if err != nil {
		return StagedComponents{}, err
	}
	return result, nil
}
