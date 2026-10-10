package graphstore

import (
	"cmp"
	"context"
	"errors"
	"strings"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func planFailure(err error) error {
	if err == nil || errors.Is(err, ErrCorrupt) || errors.Is(err, ErrPoisoned) || errors.Is(err, raftlog.ErrCorrupt) || errors.Is(err, raftlog.ErrPoisoned) || errors.Is(err, raftlog.ErrClosed) || errors.Is(err, ErrClosed) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return callerError(err)
}
func (v *ReadView) inputCost(n int) error {
	if n < 0 || n > v.limits.MaxSourceBytes-v.work.Bytes {
		return ErrResourceLimit
	}
	v.work.Bytes += n
	return nil
}
func cloneAxisForOperation(v *ReadView, a temporal.Axis) (temporal.Axis, error) {
	d := a.Descriptor()
	cost := 2 * (128 + len(d.Reference) + len(d.CanonicalUnit))
	if err := v.inputCost(cost); err != nil {
		return temporal.Axis{}, err
	}
	wire, err := encodeAxis(v.c.root.namespace, a, v.c.limits)
	if err != nil {
		return temporal.Axis{}, err
	}
	return readAxis(wire, v.c.root.namespace, v.c.limits)
}
func cloneOperations(ctx context.Context, v *ReadView, ops []graphstate.Operation) ([]graphstate.Operation, error) {
	maxOps := cmp.Or(v.limits.Planner.MaxOperations, graphstate.DefaultLimits().MaxOperations)
	if len(ops) > maxOps {
		return nil, ErrResourceLimit
	}
	if err := v.inputCost(640 * len(ops)); err != nil {
		return nil, err
	}
	out := make([]graphstate.Operation, len(ops))
	for i, op := range ops {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		switch op.Scope.Kind() {
		case temporal.ScopeInvalid:
			return nil, callerError(graphstate.ErrValidityRequired)
		case temporal.ScopeEmpty:
			return nil, callerError(graphstate.ErrEmptyMutation)
		case temporal.ScopeUnplaced:
			return nil, callerError(graphstate.ErrUnsupported)
		}
		if len(op.Name) > v.c.limits.MaxNameBytes || len(op.Record.Type) > v.c.limits.MaxNameBytes {
			return nil, callerError(graphstate.ErrInvalidInput)
		}
		if err := v.inputCost(len(op.Name) + len(op.Record.Type)); err != nil {
			return nil, err
		}
		op.Name = strings.Clone(op.Name)
		op.Record.Type = strings.Clone(op.Record.Type)
		axis, err := cloneAxisForOperation(v, op.Scope.Axis())
		if err != nil {
			return nil, callerError(err)
		}
		_, available := v.remaining()
		if available < 2 {
			return nil, ErrResourceLimit
		}
		tl := v.c.limits.Temporal
		tl.MaxValueBytes = min(tl.MaxValueBytes, available/2)
		wire, err := temporal.AppendScope(nil, op.Scope, tl)
		if err != nil {
			return nil, callerError(err)
		}
		backing, err := scopeOwnedBacking(wire)
		if err != nil {
			return nil, err
		}
		if err := v.inputCost(128 + backing + 2*cap(wire)); err != nil {
			return nil, err
		}
		op.Scope, err = temporal.DecodeScope(wire, axis, tl)
		if err != nil {
			return nil, callerError(err)
		}
		if op.Kind == graphstate.CreateNode || op.Kind == graphstate.CreateRelationship {
			if op.Record.Axis.Descriptor().ID != (temporal.AxisID{}) {
				supplied, err := cloneAxisForOperation(v, op.Record.Axis)
				if err != nil {
					return nil, callerError(err)
				}
				if supplied.Descriptor() != axis.Descriptor() || supplied.DefinitionHash() != axis.DefinitionHash() {
					return nil, callerError(temporal.ErrAxisMismatch)
				}
				op.Record.Axis = supplied
			}
		} else {
			op.Record.Axis = temporal.Axis{}
		}
		if op.Value.Kind() != graphstate.ScalarInvalid {
			_, available = v.remaining()
			if available < 4 {
				return nil, ErrResourceLimit
			}
			l := v.c.limits.valueLimits()
			l.MaxReadBytes = min(l.MaxReadBytes, available/4)
			if op.Value.Kind() == graphstate.ScalarScope {
				l.Component.Temporal.MaxValueBytes = min(l.Component.Temporal.MaxValueBytes, max(1, l.MaxReadBytes-1))
			}
			key, err := op.Value.EqualityKey(l)
			if err != nil {
				return nil, callerError(err)
			}
			if err := v.inputCost(4 * len(key)); err != nil {
				return nil, err
			}
			var valueAxis temporal.Axis
			hasAxis := false
			if scope, ok := op.Value.Scope(); ok {
				hasAxis = true
				backing, err := scopeOwnedBacking([]byte(key)[1:])
				if err != nil {
					return nil, err
				}
				if err := v.inputCost(backing); err != nil {
					return nil, err
				}
				valueAxis, err = cloneAxisForOperation(v, scope.Axis())
				if err != nil {
					return nil, callerError(err)
				}
			}
			op.Value, err = decodeScalar([]byte(key), valueAxis, hasAxis, v.c.limits)
			if err != nil {
				return nil, callerError(err)
			}
		}
		out[i] = op
	}
	return out, nil
}
func fullEffects(s *Stage, base raftlog.ApplicationRoot, delta graphstate.Delta, groups []ComponentChangeGroup, work PageWork, l GraphLimits) (GraphEffects, error) {
	// Preflight fixed slots and retained bytes before copying stage output.
	fixed := 512 + len(base.Image) + 64*len(s.writes) + fullGroupOutputBytes*cap(groups) + 256*cap(delta.Entities) + 64*cap(delta.Lives) + 256*cap(delta.Values) + fullDependencyOutputBytes*cap(delta.Dependencies) + fullPatchOutputBytes*cap(delta.Patches)
	for _, row := range s.writes {
		fixed += cap(row.Key) + cap(row.Value)
	}
	if fixed > l.MaxOutputBytes {
		return GraphEffects{}, ErrResourceLimit
	}
	variable, err := graphVariableOutput(delta, groups, s.c.limits)
	if err != nil {
		return GraphEffects{}, callerError(err)
	}
	if variable > l.MaxOutputBytes-fixed {
		return GraphEffects{}, ErrResourceLimit
	}
	writes, err := s.Writes()
	if err != nil {
		return GraphEffects{}, err
	}
	base.Image = exactCopy(base.Image)
	return GraphEffects{Base: base, Root: s.full.root, Writes: writes, Groups: groups, Dependencies: delta.Dependencies, Delta: delta, Work: work, OwnedBytes: fixed + variable}, nil
}

// InitializeGraphIndexes computes fresh-only Full index/schema effects. All four
// roots are actual empty pages; the exact returned base must also guard the
// outer allocator/request initialization and single final installation.
func InitializeGraphIndexes(ctx context.Context, c *Catalog, schemas []graphstate.PropertyDefinition, l GraphLimits) (GraphEffects, error) {
	if c == nil || ctx == nil {
		return GraphEffects{}, ErrInvalid
	}
	l, err := l.resolve()
	if err != nil {
		return GraphEffects{}, err
	}
	if l.MaxOutputBytes < 512+c.rootImageBytes {
		return GraphEffects{}, ErrResourceLimit
	}
	if err := c.check(ctx); err != nil {
		return GraphEffects{}, err
	}
	seed, err := NewRoot(c.root.namespace, c.root.owner)
	if err != nil {
		return GraphEffects{}, err
	}
	if c.root.topology != bootstrapTopology || c.root.epoch != 0 || c.root.next != 1 || c.root.effect != seed.effect {
		return GraphEffects{}, ErrTopologyUnsupported
	}
	if len(schemas) > c.limits.MaxStageRecords-5 {
		return GraphEffects{}, ErrResourceLimit
	}
	if l.MaxSourceBytes < 2*c.rootImageBytes+fullStageMetadataBytes {
		return GraphEffects{}, ErrResourceLimit
	}
	base, err := c.view.ProveNoApplicationData(ctx)
	if err != nil {
		return GraphEffects{}, err
	}
	initial, err := c.reader(ctx)
	if err != nil {
		return GraphEffects{}, err
	}
	initial.maxRows = min(l.MaxSourceRows, l.Pages.MaxWorkRecords)
	initial.maxBytes = min(l.MaxSourceBytes, l.Pages.MaxWorkBytes)
	if err := initial.materialize(len(base.Image)); err != nil {
		return GraphEffects{}, err
	}
	if _, found, err := initial.get(componentIndexDescriptorKey(c.root.namespace)); err != nil {
		return GraphEffects{}, c.failure(err)
	} else if found {
		return GraphEffects{}, c.failure(ErrCorrupt)
	}
	pages := l.Pages
	pages.MaxWorkRecords = min(pages.MaxWorkRecords, l.MaxSourceRows-initial.rows)
	pages.MaxWorkBytes = min(pages.MaxWorkBytes, l.MaxSourceBytes-initial.bytes)
	if pages.MaxWorkBytes < c.rootImageBytes+fullStageMetadataBytes {
		return GraphEffects{}, ErrResourceLimit
	}
	s, err := allocateFullStage(c, fullStageState{root: c.root, pages: pages})
	if err != nil {
		return GraphEffects{}, err
	}
	defer func() { _ = s.Close() }()
	var work PageWork
	err = s.operation(ctx, func(reader *reader) error {
		p := pageStage{&pageReader{q: reader, limits: pages}, c.root}
		p.allocation = &p.root
		if err := p.budget(); err != nil {
			return err
		}
		for _, definition := range schemas {
			if err := ctx.Err(); err != nil {
				return err
			}
			wire, err := encodeProperty(c.root.namespace, definition, c.limits)
			if err != nil {
				return err
			}
			key := schemaKey(c.root.namespace, definition.Owner, definition.Name)
			if _, found := reader.pending[string(key)]; found {
				return ErrInvalid
			}
			if err := reader.put(key, wire); err != nil {
				return err
			}
		}
		p.root.topology = fullTopology
		keys, err := p.newComponentKeyTree(keyTreeLimits(pages))
		if err != nil {
			return err
		}
		unique, err := p.newPostingKeyTree(uniquePostingRecord, keyTreeLimits(pages))
		if err != nil {
			return err
		}
		canonical, err := p.newPostingKeyTree(canonicalIncidentRecord, keyTreeLimits(pages))
		if err != nil {
			return err
		}
		declared, err := p.newPostingKeyTree(declaredIncidentRecord, keyTreeLimits(pages))
		if err != nil {
			return err
		}
		descriptor := fullIndexDescriptor{p.root.owner, p.root.topology.epoch, p.root.topology.schema, 2, keys, unique, canonical, declared}
		wire, err := encodeFullDescriptor(descriptor, p.root, c.limits)
		if err != nil {
			return err
		}
		if err := p.budget(); err != nil {
			return err
		}
		if err := reader.put(componentIndexDescriptorKey(c.root.namespace), wire); err != nil {
			return err
		}
		reader.full = &fullStageState{p.root, descriptor, pages}
		work = addWork(PageWork{Records: initial.rows, Bytes: initial.bytes}, p.work)
		return nil
	})
	if err != nil {
		return GraphEffects{}, err
	}
	return fullEffects(s, base, graphstate.Delta{}, nil, work, l)
}
func (q *pageStage) stageCanonical(entity graphstate.EntityRecord) error {
	for _, key := range canonicalIncidentKeys(entity) {
		tree, err := q.insertPostingKey(q.q.full.descriptor.canonical, key, keyTreeLimits(q.limits))
		if err != nil {
			return err
		}
		q.q.full.descriptor.canonical = tree
	}
	return nil
}
func (q *pageStage) stageDeclared(entity graphstate.EntityRecord, life graphstate.LifeRecord) error {
	if err := q.checkCanonical(entity); err != nil {
		return err
	}
	for _, key := range declaredIncidentKeys(entity, life) {
		tree, err := q.insertPostingKey(q.q.full.descriptor.declared, key, keyTreeLimits(q.limits))
		if err != nil {
			return err
		}
		q.q.full.descriptor.declared = tree
	}
	if err := q.checkDeclared(entity, life); err != nil {
		return err
	}
	return nil
}
func (q *pageStage) stageRaw(patch graphstate.ComponentPatch) error {
	for _, change := range patch.Changes {
		before, present, err := q.rawKey(patch.Key, change.Before())
		if err != nil {
			return err
		}
		if present {
			found, err := q.hasPostingKey(q.q.full.descriptor.unique, before, keyTreeLimits(q.limits))
			if err != nil {
				return err
			}
			if !found {
				return ErrCorrupt
			}
		}
		after, present, err := q.rawKey(patch.Key, change.After())
		if err != nil {
			return err
		}
		if present {
			root, err := q.insertPostingKey(q.q.full.descriptor.unique, after, keyTreeLimits(q.limits))
			if err != nil {
				return err
			}
			q.q.full.descriptor.unique = root
		}
	}
	return nil
}

// StageOperations owns input, runs Plan internally and atomically stages every
// catalog/page/index/ordered CDC effect in one operation. It accepts no Delta,
// supplies no grant/request admission and never installs or advances semantics.
func StageOperations(ctx context.Context, c *Catalog, ops []graphstate.Operation, revision state.Revision, l GraphLimits) (GraphEffects, error) {
	effects, _, err := stageOperations(ctx, c, ops, revision, l, nil)
	return effects, err
}

func stageOperations(ctx context.Context, c *Catalog, ops []graphstate.Operation, revision state.Revision, l GraphLimits, guard *SemanticGuard) (effects GraphEffects, work PageWork, err error) {
	if c == nil || ctx == nil {
		return GraphEffects{}, PageWork{}, ErrInvalid
	}
	l, err = l.resolve()
	if err != nil {
		return GraphEffects{}, PageWork{}, err
	}
	if l.MaxOutputBytes < 512+c.rootImageBytes {
		return GraphEffects{}, PageWork{}, ErrResourceLimit
	}
	if guard != nil {
		if err := guard.validate(); err != nil {
			return GraphEffects{}, PageWork{}, err
		}
		if semanticGuardWorkBytes > l.MaxOutputBytes-(512+c.rootImageBytes) {
			return GraphEffects{}, PageWork{}, ErrResourceLimit
		}
		l.MaxOutputBytes -= semanticGuardWorkBytes
	}
	v, err := OpenReadView(ctx, c, l)
	if err != nil {
		return GraphEffects{}, PageWork{}, err
	}
	var stagedWork PageWork
	defer func() {
		work = addWork(v.Work(), stagedWork)
		_ = v.Close()
		if err != nil {
			effects = GraphEffects{}
		}
	}()
	if guard != nil {
		if err := v.inputCost(semanticGuardMetadataBytes); err != nil {
			return GraphEffects{}, PageWork{}, err
		}
		if err := ctx.Err(); err != nil {
			return GraphEffects{}, PageWork{}, err
		}
		if err := guard.compare(c.root); err != nil {
			return GraphEffects{}, PageWork{}, err
		}
	}
	owned, err := cloneOperations(ctx, v, ops)
	if err != nil {
		return GraphEffects{}, PageWork{}, err
	}
	stagePages := l.Pages
	rows, bytes := v.remaining()
	stagePages.MaxWorkRecords = min(stagePages.MaxWorkRecords, rows)
	stagePages.MaxWorkBytes = min(stagePages.MaxWorkBytes, bytes)
	if stagePages.MaxWorkBytes < c.rootImageBytes+fullStageMetadataBytes {
		return GraphEffects{}, PageWork{}, ErrResourceLimit
	}
	s, err := newFullStage(ctx, c, v.descriptor, stagePages)
	if err != nil {
		return GraphEffects{}, PageWork{}, err
	}
	defer func() { _ = s.Close() }()
	var delta graphstate.Delta
	var groups []ComponentChangeGroup
	err = s.operation(ctx, func(base *reader) error {
		var pages *pageReader
		defer func() {
			if pages != nil {
				stagedWork = pages.work
			}
			stagedWork.Records, stagedWork.Bytes = base.rows, base.bytes
		}()
		// Reserve the already charged staging authority while Plan uses this view.
		reserved := base.bytes
		original := v.limits.MaxSourceBytes
		v.limits.MaxSourceBytes -= reserved
		planner := l.Planner
		planner.MaxDeltaBytes = min(cmp.Or(planner.MaxDeltaBytes, graphstate.DefaultLimits().MaxDeltaBytes), l.MaxOutputBytes)
		planned, err := graphstate.Plan(ctx, v, owned, revision, planner)
		v.limits.MaxSourceBytes = original
		if err != nil {
			return planFailure(err)
		}
		rows, bytes := v.remaining()
		base.maxRows = min(stagePages.MaxWorkRecords, rows)
		base.maxBytes = min(stagePages.MaxWorkBytes, bytes)
		base.denyReads = base.maxRows == 0
		p := pageStage{&pageReader{q: base, limits: stagePages}, c.root}
		p.allocation = &p.root
		pages = p.pageReader
		if err := p.budget(); err != nil {
			return err
		}
		if err := base.materialize(0); err != nil {
			return err
		}
		for _, entity := range planned.Entities {
			ref := EntityRef{c.root.namespace.Graph, entity.ID}
			if _, found, err := base.entity(ref); err != nil {
				return err
			} else if found {
				return callerError(graphstate.ErrAlreadyExists)
			}
			if err := base.stageEntity(ref, entity); err != nil {
				return err
			}
		}
		for _, entity := range planned.Entities {
			if entity.Kind == graphstate.Relationship {
				if err := p.stageCanonical(entity); err != nil {
					return err
				}
			}
		}
		for _, life := range planned.Lives {
			ref := LifeRef{c.root.namespace.Graph, life.Owner, life.Life}
			if _, found, err := base.life(ref); err != nil {
				return err
			} else if found {
				return callerError(graphstate.ErrAlreadyExists)
			}
			if err := base.stageLife(ref, life); err != nil {
				return err
			}
			entity, found, err := base.entity(EntityRef{c.root.namespace.Graph, life.Owner})
			if err != nil {
				return err
			}
			if !found {
				return ErrCorrupt
			}
			if entity.Kind == graphstate.Relationship {
				if err := p.stageDeclared(entity, life); err != nil {
					return err
				}
			}
		}
		for _, value := range planned.Values {
			ref := ValueRef{c.root.namespace.Graph, value.ID}
			if _, _, found, err := base.value(ref); err != nil {
				return err
			} else if found {
				return callerError(graphstate.ErrAlreadyExists)
			}
			key, err := p.equalityKey(value.Value)
			if err != nil {
				return err
			}
			if _, found, err := base.local(key); err != nil {
				return err
			} else if found {
				return ErrCorrupt
			}
			if err := base.stageValue(ref, value.Value, key); err != nil {
				return err
			}
		}
		if len(planned.Patches) > l.Pages.MaxPatches {
			return ErrResourceLimit
		}
		groups = make([]ComponentChangeGroup, 0, len(planned.Patches))
		changeBytes := 0
		for _, patch := range planned.Patches {
			group, err := p.validatePatch(patch)
			if err != nil {
				return err
			}
			wire, err := state.AppendChanges(nil, group.Owned.Axis(), group.Changes, l.Pages.codecLimits(c))
			if err != nil {
				return pageFailure(c, err, false)
			}
			scope, err := temporal.AppendScope(nil, group.Owned, c.limits.Temporal)
			if err != nil {
				return callerError(err)
			}
			cost := len(wire) + len(scope) + len(group.Key.Name) + 64
			if cost > l.Pages.MaxChangeBytes-changeBytes {
				return ErrResourceLimit
			}
			changeBytes += cost
			if err := p.stageRaw(patch); err != nil {
				return err
			}
			if err := p.installPatch(patch); err != nil {
				return err
			}
			if err := p.budget(); err != nil {
				return err
			}
			groups = append(groups, group)
		}
		normalized, ownedGroups, err := p.ownDelta(planned, groups, l.MaxOutputBytes)
		if err != nil {
			return err
		}
		planned, groups = normalized, ownedGroups
		if err := p.budget(); err != nil {
			return err
		}
		base.full.root = p.root
		encoded, err := encodeFullDescriptor(base.full.descriptor, p.root, c.limits)
		if err != nil {
			return err
		}
		if err := base.put(componentIndexDescriptorKey(c.root.namespace), encoded); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		delta = planned
		work = addWork(v.Work(), p.work)
		if work.Records > l.MaxSourceRows || work.Bytes > l.MaxSourceBytes {
			return ErrResourceLimit
		}
		return nil
	})
	if err != nil {
		return GraphEffects{}, PageWork{}, err
	}
	effects, err = fullEffects(s, v.base, delta, groups, work, l)
	if err == nil && guard != nil {
		effects.OwnedBytes += semanticGuardWorkBytes
	}
	return effects, work, err
}
