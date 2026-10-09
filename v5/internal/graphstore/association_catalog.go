package graphstore

import (
	"bytes"
	"context"
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5/internal/assertion"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

type associationReader struct {
	q      *reader
	limits AssociationLimits
	output int
}

func associationStart(ctx context.Context, c *Catalog, graph graphstate.GraphID, l AssociationLimits) (*associationReader, error) {
	if c == nil {
		return nil, ErrInvalid
	}
	// Namespace refusal precedes root access, allocation, lookup and text work.
	if graph != c.root.namespace.Graph {
		return nil, ErrNamespace
	}
	l, err := l.resolve()
	if err != nil {
		return nil, associationCallerError(err)
	}
	l = l.bounded(c.limits)
	if c.rootImageBytes > l.MaxReadBytes || 64 > l.MaxOutputBytes {
		return nil, ErrResourceLimit
	}
	q, err := c.reader(ctx)
	if err != nil {
		return nil, err
	}
	q.maxRows, q.maxBytes = l.MaxReadRows, l.MaxReadBytes
	return &associationReader{q: q, limits: l, output: 64}, nil
}

func (a *associationReader) out(n int) error {
	if n < 0 || n > a.limits.MaxOutputBytes-a.output {
		return ErrResourceLimit
	}
	a.output += n
	return nil
}
func (a *associationReader) materialize(n int) error { return a.q.materialize(n) }

func associationReadError(err error) error {
	if errors.Is(err, raftlog.ErrLimit) || errors.Is(err, temporal.ErrResourceLimit) || errors.Is(err, graphstate.ErrResourceLimit) || errors.Is(err, assertion.ErrResourceLimit) {
		return errors.Join(ErrResourceLimit, err)
	}
	return err
}

// Shared catalog admission caps every nested get and materialization before
// copying. The association wrapper only preserves resource-error classification.
func (a *associationReader) get(key []byte) ([]byte, bool, error) {
	b, found, err := a.q.get(key)
	return b, found, associationReadError(err)
}
func (a *associationReader) axis(id temporal.AxisID) (temporal.Axis, bool, error) {
	ax, found, err := a.q.axis(id)
	return ax, found, associationReadError(err)
}
func (a *associationReader) entity(id graphstate.EntityID) (graphstate.EntityRecord, bool, error) {
	r, found, err := a.q.entity(EntityRef{Graph: a.q.c.root.namespace.Graph, ID: id})
	return r, found, associationReadError(err)
}

func (a *associationReader) target(s assertion.Spec) (graphstate.EntityRecord, error) {
	t := s.Target
	owner := t.Entity
	if t.Kind == assertion.ComponentTarget {
		owner = t.Component.Owner
	}
	r, found, err := a.entity(owner)
	if err != nil {
		return graphstate.EntityRecord{}, err
	}
	if !found {
		return graphstate.EntityRecord{}, errors.Join(ErrInvalid, graphstate.ErrNotFound)
	}
	if s.Interpretation == assertion.AssertedRelation && (t.Kind != assertion.EntityTarget || r.Kind != graphstate.Relationship) {
		return graphstate.EntityRecord{}, errors.Join(ErrInvalid, graphstate.ErrTypeMismatch)
	}
	if t.Kind == assertion.EntityTarget || t.Component.Kind == graphstate.Presence {
		return r, nil
	}
	k := t.Component
	if len(k.Name) > a.q.c.limits.MaxNameBytes {
		return graphstate.EntityRecord{}, ErrResourceLimit
	}
	life, found, err := a.q.life(LifeRef{Graph: a.q.c.root.namespace.Graph, Owner: k.Owner, ID: k.Life})
	if err != nil {
		return graphstate.EntityRecord{}, associationReadError(err)
	}
	if !found {
		return graphstate.EntityRecord{}, errors.Join(ErrInvalid, graphstate.ErrNotFound)
	}
	if life.Owner != k.Owner || life.Life != k.Life {
		return graphstate.EntityRecord{}, ErrCorrupt
	}
	if k.Kind == graphstate.Label {
		if r.Kind != graphstate.Node {
			return graphstate.EntityRecord{}, errors.Join(ErrInvalid, graphstate.ErrTypeMismatch)
		}
		return r, nil
	}
	d, found, err := a.q.property(r.Kind, k.Name)
	if err != nil {
		return graphstate.EntityRecord{}, associationReadError(err)
	}
	if !found {
		return graphstate.EntityRecord{}, errors.Join(ErrInvalid, graphstate.ErrSchemaMismatch)
	}
	if k.Kind == graphstate.ScalarProperty && d.Cardinality != graphstate.ScalarCardinality || k.Kind == graphstate.SetMember && d.Cardinality != graphstate.SetCardinality {
		return graphstate.EntityRecord{}, errors.Join(ErrInvalid, graphstate.ErrTypeMismatch)
	}
	if k.Kind == graphstate.SetMember {
		if err := a.member(k.Member, d); err != nil {
			return graphstate.EntityRecord{}, err
		}
	}
	return r, nil
}

func (a *associationReader) member(id graphstate.ValueID, d graphstate.PropertyDefinition) error {
	v, _, found, err := a.q.value(ValueRef{Graph: a.q.c.root.namespace.Graph, ID: id})
	if err != nil {
		return associationReadError(err)
	}
	if !found {
		return errors.Join(ErrInvalid, graphstate.ErrNotFound)
	}
	if v.Value.Kind() != d.Type {
		return errors.Join(ErrInvalid, graphstate.ErrTypeMismatch)
	}
	return nil
}

func (a *associationReader) revision(ref assertion.Ref, revision uint64) (AssociationRead, error) {
	n := a.q.c.root.namespace
	b, found, err := a.get(associationRevisionKey(n, ref.ID, revision))
	if err != nil || !found {
		return AssociationRead{}, err
	}
	id, wire, err := inspectAssociation(b, n, a.q.c.limits)
	if err != nil {
		return AssociationRead{}, err
	}
	var axis temporal.Axis
	if id != (temporal.AxisID{}) {
		axis, found, err = a.axis(id)
		if err != nil {
			return AssociationRead{}, err
		}
		if !found {
			return AssociationRead{}, ErrCorrupt
		}
	}
	r, err := assertion.DecodeRecord(wire, axis, a.limits.Record)
	if err != nil {
		if errors.Is(err, assertion.ErrResourceLimit) || errors.Is(err, temporal.ErrResourceLimit) {
			return AssociationRead{}, associationCallerError(err)
		}
		return AssociationRead{}, errors.Join(ErrCorrupt, err)
	}
	s := r.Spec()
	if s.Ref != ref || s.Revision.ID() != revision {
		return AssociationRead{}, ErrCorrupt
	}
	if _, err := a.target(s); err != nil {
		// A decoded target that violates existence/type/schema is corrupt. Closed
		// views, cancellation, bounded-policy refusal and underlying I/O retain
		// their operational causes rather than becoming a malformed-record claim.
		if errors.Is(err, ErrInvalid) {
			return AssociationRead{}, ErrCorrupt
		}
		return AssociationRead{}, err
	}
	b, found, err = a.get(associationPostingKey(n, s.Target, ref.ID))
	if err != nil {
		return AssociationRead{}, err
	}
	if !found {
		return AssociationRead{}, ErrCorrupt
	}
	posting, err := readNumber(b, n, associationPostingRecord, a.q.c.limits)
	if err != nil || assertion.ID(posting) != ref.ID {
		return AssociationRead{}, ErrCorrupt
	}
	cost, err := associationRecordBytes(r, a.limits)
	if err != nil {
		return AssociationRead{}, err
	}
	if err := a.materialize(cost); err != nil {
		return AssociationRead{}, err
	}
	return AssociationRead{Found: true, Record: r}, nil
}

func (a *associationReader) current(ref assertion.Ref) (AssociationRead, error) {
	b, found, err := a.get(associationHeadKey(a.q.c.root.namespace, ref.ID))
	if err != nil || !found {
		return AssociationRead{}, err
	}
	revision, err := readNumber(b, a.q.c.root.namespace, associationHeadRecord, a.q.c.limits)
	if err != nil || revision == 0 {
		return AssociationRead{}, ErrCorrupt
	}
	r, err := a.revision(ref, revision)
	if err != nil {
		return AssociationRead{}, err
	}
	if !r.Found {
		return AssociationRead{}, ErrCorrupt
	}
	return r, nil
}

func (a *associationReader) binding(entity EntityRef) (assertion.Ref, bool, error) {
	b, found, err := a.get(associationPrimaryKey(a.q.c.root.namespace, entity.ID))
	if err != nil || !found {
		return assertion.Ref{}, found, err
	}
	id, err := readNumber(b, a.q.c.root.namespace, associationPrimaryRecord, a.q.c.limits)
	if err != nil || id == 0 {
		return assertion.Ref{}, false, ErrCorrupt
	}
	return assertion.Ref{Graph: entity.Graph, ID: assertion.ID(id)}, true, nil
}

func associationPrimaryValid(entity EntityRef, owner graphstate.EntityRecord, r assertion.Record) error {
	s := r.Spec()
	if s.Target != (assertion.Target{Kind: assertion.EntityTarget, Entity: entity.ID}) || s.Placement.Kind != assertion.NativePlacement {
		return errors.Join(ErrInvalid, assertion.ErrRebinding)
	}
	axis := s.Placement.Native.Axis()
	if axis.Descriptor() != owner.Axis.Descriptor() || axis.DefinitionHash() != owner.Axis.DefinitionHash() {
		return errors.Join(ErrInvalid, temporal.ErrAxisMismatch)
	}
	return nil
}

// Association reads current metadata, including retractions, at this actual
// retained application view. It does not consult or modify target life coverage.
func (c *Catalog) Association(ctx context.Context, ref assertion.Ref, l AssociationLimits) (AssociationRead, error) {
	a, err := associationStart(ctx, c, ref.Graph, l)
	if err != nil {
		return AssociationRead{}, err
	}
	if ref.ID == 0 {
		return AssociationRead{}, ErrInvalid
	}
	r, err := a.current(ref)
	if err == nil && r.Found {
		var cost int
		cost, err = associationRecordBytes(r.Record, a.limits)
		if err == nil {
			err = a.out(cost)
		}
	}
	if err != nil {
		return AssociationRead{}, c.failure(err)
	}
	return r, nil
}

// AssociationRevision reads a retained tuple (Graph,AssertionID,RevisionID),
// never falling back to the current revision or inferring commit order from IDs.
func (c *Catalog) AssociationRevision(ctx context.Context, ref assertion.Ref, revision uint64, l AssociationLimits) (AssociationRead, error) {
	a, err := associationStart(ctx, c, ref.Graph, l)
	if err != nil {
		return AssociationRead{}, err
	}
	if ref.ID == 0 || revision == 0 {
		return AssociationRead{}, ErrInvalid
	}
	r, err := a.revision(ref, revision)
	if err == nil && r.Found {
		var cost int
		cost, err = associationRecordBytes(r.Record, a.limits)
		if err == nil {
			err = a.out(cost)
		}
	}
	if err != nil {
		return AssociationRead{}, c.failure(err)
	}
	return r, nil
}

// Primary follows one explicitly bound stable ID. It preserves metadata after
// life closure/retraction and never selects the first posting or an older record.
func (c *Catalog) Primary(ctx context.Context, entity EntityRef, l AssociationLimits) (PrimaryRead, error) {
	a, err := associationStart(ctx, c, entity.Graph, l)
	if err != nil {
		return PrimaryRead{}, err
	}
	if entity.ID == 0 {
		return PrimaryRead{}, ErrInvalid
	}
	ref, found, err := a.binding(entity)
	if err != nil {
		return PrimaryRead{}, c.failure(err)
	}
	if !found {
		return PrimaryRead{}, nil
	}
	r, err := a.current(ref)
	if err == nil && !r.Found {
		err = ErrCorrupt
	}
	if err == nil {
		var owner graphstate.EntityRecord
		var exists bool
		owner, exists, err = a.entity(entity.ID)
		if err == nil && !exists {
			err = ErrCorrupt
		}
		if err == nil {
			if e := associationPrimaryValid(entity, owner, r.Record); e != nil {
				err = ErrCorrupt
			}
		}
	}
	if err == nil {
		var cost int
		cost, err = associationRecordBytes(r.Record, a.limits)
		if err == nil {
			err = a.out(24 + cost)
		}
	}
	if err != nil {
		return PrimaryRead{}, c.failure(err)
	}
	return PrimaryRead{Bound: true, Ref: ref, Association: r}, nil
}

func (a *associationReader) stageAxis(axis temporal.Axis) error {
	b, err := encodeAxis(a.q.c.root.namespace, axis, a.q.c.limits)
	if err != nil {
		return err
	}
	old, found, err := a.axis(axis.Descriptor().ID)
	if err != nil {
		return err
	}
	if found {
		if old.Descriptor() != axis.Descriptor() || old.DefinitionHash() != axis.DefinitionHash() {
			return ErrRebinding
		}
		return nil
	}
	return a.q.put(axisKey(a.q.c.root.namespace, axis.Descriptor().ID), b)
}

// Associations stages the entire operation atomically under shared Catalog caps.
// Association/lifecycle scopes remain independent. Primary bindings are explicit
// and immutable; changing both sides requires an explicit composed graph command.
// This is local staging, not target-global routing, allocator authority or commit.
func (s *Stage) Associations(ctx context.Context, writes []AssociationWrite, l AssociationLimits) (AssociationResult, error) {
	if s == nil {
		return AssociationResult{}, ErrInvalid
	}
	l, err := l.resolve()
	if err != nil {
		return AssociationResult{}, associationCallerError(err)
	}
	l = l.bounded(s.c.limits)
	if len(writes) > l.MaxOperations {
		return AssociationResult{}, ErrResourceLimit
	}
	for _, w := range writes {
		if w.Spec.Ref.Graph != s.c.root.namespace.Graph || w.PrimaryFor != (EntityRef{}) && w.PrimaryFor.Graph != s.c.root.namespace.Graph {
			return AssociationResult{}, ErrNamespace
		}
		if w.PrimaryFor != (EntityRef{}) && w.PrimaryFor.ID == 0 {
			return AssociationResult{}, ErrInvalid
		}
		if len(w.Spec.Target.Component.Name) > s.c.limits.MaxNameBytes {
			return AssociationResult{}, ErrResourceLimit
		}
	}
	var result AssociationResult
	err = s.operation(ctx, func(q *reader) error {
		q.maxRows, q.maxBytes = l.MaxReadRows, l.MaxReadBytes
		a := &associationReader{q: q, limits: l}
		if err := a.materialize(0); err != nil {
			return err
		}
		if err := a.out(64 + 64*len(writes)); err != nil {
			return err
		}
		result.Transitions = make([]assertion.Transition, 0, len(writes))
		primaryCount := 0
		for _, w := range writes {
			if w.PrimaryFor != (EntityRef{}) {
				primaryCount++
			}
		}
		if primaryCount > 0 {
			if err := a.out(96 * primaryCount); err != nil {
				return err
			}
			result.Bindings = make([]PrimaryBindingChange, 0, primaryCount)
		}
		for _, w := range writes {
			if err := a.stage(w, &result); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return AssociationResult{}, err
	}
	return result, nil
}

func (a *associationReader) stage(w AssociationWrite, result *AssociationResult) error {
	r, err := assertion.New(w.Spec, a.limits.Record)
	if err != nil {
		return associationCallerError(err)
	}
	owner, err := a.target(w.Spec)
	if err != nil {
		return err
	}
	previous, err := a.current(w.Spec.Ref)
	if err != nil {
		return err
	}
	n := a.q.c.root.namespace
	currentRevision := previous.Found && previous.Record.Spec().Revision.ID() == w.Spec.Revision.ID()
	if !currentRevision {
		_, used, err := a.get(associationRevisionKey(n, w.Spec.Ref.ID, w.Spec.Revision.ID()))
		if err != nil {
			return err
		}
		if used {
			return errors.Join(ErrInvalid, assertion.ErrRevisionReuse)
		}
	}
	x, err := assertion.Apply(previous.Record, w.Spec, a.limits.Record)
	if err != nil {
		return associationCallerError(err)
	}
	if w.Spec.Target.Kind == assertion.EntityTarget {
		entity := EntityRef{Graph: w.Spec.Ref.Graph, ID: w.Spec.Target.Entity}
		primary, bound, err := a.binding(entity)
		if err != nil {
			return err
		}
		if bound && primary == w.Spec.Ref || w.PrimaryFor != (EntityRef{}) {
			if err := associationPrimaryValid(entity, owner, r); err != nil {
				return err
			}
		}
	}
	if w.PrimaryFor != (EntityRef{}) {
		if w.PrimaryFor.ID != w.Spec.Target.Entity || w.Spec.Target.Kind != assertion.EntityTarget {
			return errors.Join(ErrInvalid, assertion.ErrRebinding)
		}
		prior, bound, err := a.binding(w.PrimaryFor)
		if err != nil {
			return err
		}
		if bound && prior != w.Spec.Ref {
			return ErrRebinding
		}
		if !bound {
			if err := a.q.put(associationPrimaryKey(n, w.PrimaryFor.ID), encodeNumber(n, associationPrimaryRecord, uint64(w.Spec.Ref.ID))); err != nil {
				return err
			}
			result.Bindings = append(result.Bindings, PrimaryBindingChange{Entity: w.PrimaryFor, After: w.Spec.Ref})
		}
	}
	cost, err := associationRecordBytes(r, a.limits)
	if err != nil {
		return err
	}
	if previous.Found {
		beforeCost, err := associationRecordBytes(previous.Record, a.limits)
		if err != nil {
			return err
		}
		cost += beforeCost
	}
	if err := a.out(cost); err != nil {
		return err
	}
	if !x.Replay() {
		if w.Spec.Placement.Kind == assertion.NativePlacement {
			if err := a.stageAxis(w.Spec.Placement.Native.Axis()); err != nil {
				return err
			}
		}
		encoded, err := encodeAssociation(n, r, a.limits, a.q.c.limits)
		if err != nil {
			return err
		}
		for _, row := range []raftlog.KV{
			{Key: associationRevisionKey(n, w.Spec.Ref.ID, w.Spec.Revision.ID()), Value: encoded},
			{Key: associationHeadKey(n, w.Spec.Ref.ID), Value: encodeNumber(n, associationHeadRecord, w.Spec.Revision.ID())},
		} {
			if err := a.q.put(row.Key, row.Value); err != nil {
				return err
			}
		}
		if !previous.Found {
			if err := a.q.put(associationPostingKey(n, w.Spec.Target, w.Spec.Ref.ID), encodeNumber(n, associationPostingRecord, uint64(w.Spec.Ref.ID))); err != nil {
				return err
			}
		}
	}
	result.Transitions = append(result.Transitions, x)
	return nil
}

// Associations returns at most one visited posting per page in this prototype.
// This bounds candidate/axis materialization without claiming a tuned scan path.
// Filtered/future-only rows still advance a bound continuation and consume work.
// Insufficient work/output for that candidate refuses with zero output, never
// labels a partial scan Complete. Native predicates decline Unplaced/incompatible
// axes instead of silently treating unknown or incomparable placement as false.
func (c *Catalog) Associations(ctx context.Context, query AssociationQuery, after []byte, budget graphstate.ReadBudget, l AssociationLimits) (AssociationPage, error) {
	a, err := associationStart(ctx, c, query.Graph, l)
	if err != nil {
		return AssociationPage{}, err
	}
	if budget.Rows < 1 || budget.Bytes < 1 {
		return AssociationPage{}, ErrInvalid
	}
	a.limits.MaxReadRows = min(a.limits.MaxReadRows, budget.Rows)
	a.limits.MaxReadBytes = min(a.limits.MaxReadBytes, budget.Bytes)
	a.limits.MaxOutputBytes = min(a.limits.MaxOutputBytes, budget.Bytes)
	a.q.maxRows, a.q.maxBytes = a.limits.MaxReadRows, a.limits.MaxReadBytes
	if a.q.bytes > a.limits.MaxReadBytes || a.output > a.limits.MaxOutputBytes {
		return AssociationPage{}, ErrResourceLimit
	}
	queryBytes, err := associationQueryBytes(query, a.limits, c.limits.MaxNameBytes)
	if err != nil {
		return AssociationPage{}, err
	}
	if err := a.materialize(len(queryBytes) + len(after) + c.rootImageBytes); err != nil {
		return AssociationPage{}, err
	}
	root, err := c.view.Root()
	if err != nil {
		return AssociationPage{}, err
	}
	hash := associationQueryHash(queryBytes)
	lower := associationTargetPrefix(c.root.namespace, query.Target)
	upper := associationPrefixEnd(lower)
	var start []byte
	if len(after) > 0 {
		start, err = parseAssociationContinuation(after, c.root.namespace, root.Index, root.ImageHash, hash, lower, upper, a.limits.MaxReadBytes)
		if err != nil {
			return AssociationPage{}, err
		}
	}
	if a.q.rows >= a.limits.MaxReadRows || a.q.bytes >= a.limits.MaxReadBytes {
		return AssociationPage{}, ErrResourceLimit
	}
	readLimits := c.view.ReadLimits()
	page, err := c.view.Scan(ctx, lower, upper, start, raftlog.ReadBudget{Rows: min(1, readLimits.Rows), Bytes: min(a.limits.MaxReadBytes-a.q.bytes, readLimits.Bytes)})
	if err != nil {
		return AssociationPage{}, c.failure(associationCallerError(err))
	}
	a.q.rows += page.Visited
	if err := a.materialize(page.Bytes); err != nil {
		return AssociationPage{}, err
	}
	out := AssociationPage{Complete: page.Complete}
	for _, row := range page.Rows {
		id, err := readNumber(row.Value, c.root.namespace, associationPostingRecord, c.limits)
		if err != nil || id == 0 || !bytes.Equal(row.Key, associationPostingKey(c.root.namespace, query.Target, assertion.ID(id))) {
			return AssociationPage{}, c.failure(ErrCorrupt)
		}
		r, err := a.current(assertion.Ref{Graph: query.Graph, ID: assertion.ID(id)})
		if err != nil {
			return AssociationPage{}, c.failure(err)
		}
		if !r.Found || r.Record.Spec().Target != query.Target {
			return AssociationPage{}, c.failure(ErrCorrupt)
		}
		s := r.Record.Spec()
		match := !s.Retracted && (query.Interpretation == 0 || query.Interpretation == s.Interpretation) && (query.Placement == 0 || query.Placement == s.Placement.Kind)
		if match && query.NativeWindow.Kind() != temporal.ScopeInvalid {
			match, err = s.Placement.Native.Overlaps(query.NativeWindow, a.limits.Record.Temporal)
			if err != nil {
				return AssociationPage{}, associationCallerError(err)
			}
		}
		if match {
			cost, err := associationRecordBytes(r.Record, a.limits)
			if err != nil {
				return AssociationPage{}, err
			}
			if err := a.out(64 + cost); err != nil {
				return AssociationPage{}, err
			}
			out.Records = []assertion.Record{r.Record}
		}
	}
	if !page.Complete {
		if len(page.Next) == 0 {
			return AssociationPage{}, c.failure(ErrCorrupt)
		}
		out.Next = associationContinuation(c.root.namespace, root.Index, root.ImageHash, hash, page.Next)
		if err := a.out(len(out.Next)); err != nil {
			return AssociationPage{}, err
		}
	}
	out.Visited = a.q.rows
	return out, nil
}
