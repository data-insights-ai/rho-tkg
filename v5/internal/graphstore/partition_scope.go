package graphstore

import (
	"context"
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

// partitionReadScope is an immutable-view routing proof. Fixed range witnesses
// can be reused only within that exact captured view; no per-fact cache grows.
type partitionReadScope struct {
	routing       PartitionRouting
	binding       types.DefaultAxisBinding
	configuration [32]byte
	view          *raftlog.ApplicationView
	namespace     Namespace
	ranges        [4]struct {
		publication RangePublication
		owner       RangeOwnership
	}
	next int
}

func (s *partitionReadScope) owner(q *reader, id uint64) error {
	if s == nil || id == 0 {
		return ErrInvalid
	}
	for _, w := range s.ranges {
		if w.publication.Contains(id) {
			if err := w.owner.Check(w.publication, s.namespace.Partition, w.owner.Epoch); err != nil {
				return err
			}
			return nil
		}
	}
	rows, bytes := q.c.limits.MaxReadRows-q.rows, q.c.limits.MaxReadBytes-q.bytes
	if q.maxRows > 0 {
		rows = min(rows, q.maxRows-q.rows)
	}
	if q.maxBytes > 0 {
		bytes = min(bytes, q.maxBytes-q.bytes)
	}
	if rows < 1 || bytes < 1 {
		return ErrResourceLimit
	}
	p, o, work, err := lookupPublishedRangeBudget(q.ctx, s.view, s.namespace, id, s.configuration, OwnershipBudget{SourceRows: rows, SourceBytes: bytes, OutputBytes: min(q.c.limits.MaxReadBytes, bytes)}, q.arena)
	q.rows += work.Records
	q.bytes += work.Bytes
	if err != nil {
		return err
	}
	if err := o.Check(p, s.namespace.Partition, o.Epoch); err != nil {
		if o.Partition != s.namespace.Partition {
			return errors.Join(ErrRemoteParticipant, err)
		}
		return err
	}
	s.ranges[s.next].publication, s.ranges[s.next].owner = p, o
	s.next = (s.next + 1) % len(s.ranges)
	return nil
}

// OpenPartitionGraphReadView opens a checked dependency view for a declared
// partition. Unknown or remote ranges are refusals, never complete graph absence.
// Routing/default/configuration and all physical roots bind the same borrowed
// ApplicationView. It does not provide a global cut, grant or installation door.
func OpenPartitionGraphReadView(ctx context.Context, view *raftlog.ApplicationView, d OwnershipDeclaration, binding types.DefaultAxisBinding, configuration [32]byte, l Limits, g GraphLimits, budget OwnershipBudget) (*ReadView, PageWork, error) {
	return openPartitionGraphReadViewBudget(ctx, view, d, binding, configuration, l, g, budget, nil)
}
func openPartitionGraphReadViewBudget(ctx context.Context, view *raftlog.ApplicationView, d OwnershipDeclaration, binding types.DefaultAxisBinding, configuration [32]byte, l Limits, g GraphLimits, budget OwnershipBudget, arena *graphstate.OutputBudget) (out *ReadView, work PageWork, err error) {
	if ctx == nil || view == nil || !d.valid() || configuration == ([32]byte{}) {
		return nil, work, ErrInvalid
	}
	g, err = g.resolve()
	if err != nil {
		return nil, work, err
	}
	if err := budget.validate(); err != nil {
		return nil, work, err
	}
	actual, err := view.ApplicationBinding(ctx)
	if err != nil {
		return nil, work, err
	}
	n := Namespace{Graph: d.Graph(), Partition: actual.Identity.Partition}
	entry, found := d.Partition(n.Partition)
	if !found || actual.Identity.Graph != [16]byte(n.Graph) || actual.Identity.Group != entry.Group {
		return nil, work, ErrNamespace
	}
	if arena != nil {
		cost := 2048 + 256*len(d.entries) + 4*len(binding.Axis().Descriptor().Reference) + 4*len(binding.Axis().Descriptor().CanonicalUnit)
		if err := arena.Reserve(cost); err != nil {
			return nil, work, callerError(err)
		}
	}
	c, opened, err := OpenPartitionCatalogWithDefaultAxis(ctx, view, n, entry.OwnershipEpoch, binding, l, g, budget)
	work = opened
	if err != nil {
		return nil, work, err
	}
	q, err := c.readerWithOutputBudget(ctx, arena)
	if err != nil {
		return nil, work, err
	}
	rows, bytes := min(g.MaxSourceRows, budget.SourceRows)-work.Records, min(g.MaxSourceBytes, budget.SourceBytes)-work.Bytes
	if rows < 1 || bytes < c.rootImageBytes+512 {
		return nil, work, ErrResourceLimit
	}
	q.maxRows, q.maxBytes = rows, bytes
	defer func() {
		work = addWork(opened, PageWork{Records: q.rows, Bytes: q.bytes})
		if out != nil {
			work = out.Work()
		}
		if err != nil {
			out = nil
		}
	}()
	wire, present, err := q.get(RoutingDefinitionKey(n))
	if err != nil {
		return nil, work, err
	}
	if !present {
		return nil, work, ErrCorrupt
	}
	if budget.OutputBytes < fullViewMetadataBytes+4*c.rootImageBytes+1536+8*len(wire) {
		return nil, work, ErrResourceLimit
	}
	if err := q.materialize(1536 + 8*len(wire)); err != nil {
		return nil, work, err
	}
	routing, err := DecodePartitionRoutingBinding(wire, d, binding, configuration, c.limits)
	if err != nil {
		return nil, work, err
	}
	prior := addWork(opened, PageWork{Records: q.rows, Bytes: q.bytes})
	remaining := g
	remaining.MaxSourceRows = min(g.MaxSourceRows, budget.SourceRows) - prior.Records
	remaining.MaxSourceBytes = min(g.MaxSourceBytes, budget.SourceBytes) - prior.Bytes
	if remaining.MaxSourceRows < 5 || remaining.MaxSourceBytes < fullViewMetadataBytes+c.rootImageBytes || budget.OutputBytes < fullViewMetadataBytes+c.rootImageBytes+512+8*len(wire) {
		return nil, work, ErrResourceLimit
	}
	if !physicalFullTopology(c.root) {
		return nil, work, ErrTopologyUnsupported
	}
	out, err = openFullStorageReadViewBudget(ctx, c, remaining, arena)
	if err != nil {
		return nil, work, err
	}
	scope := &partitionReadScope{routing: routing, binding: binding, configuration: configuration, view: view, namespace: n}
	out.route = scope
	out.pages.route = scope
	out.work = addWork(prior, out.work)
	out.limits.MaxSourceRows = min(g.MaxSourceRows, budget.SourceRows)
	out.limits.MaxSourceBytes = min(g.MaxSourceBytes, budget.SourceBytes)
	return out, out.work, nil
}

var _ graphstate.ReadView = (*ReadView)(nil)

// StagePartitionOperationsWithOutputBudget uses the checked declaration/default/
// routing proof and typed stager under one supplied caller allowance. Budget is
// neither coverage nor write authority; a child preserves the graph-local cap.
func StagePartitionOperationsWithOutputBudget(ctx context.Context, view *raftlog.ApplicationView, d OwnershipDeclaration, binding types.DefaultAxisBinding, configuration [32]byte, ops []graphstate.Operation, revision state.Revision, l Limits, g GraphLimits, budget OwnershipBudget, output *graphstate.OutputBudget) (GraphEffects, PageWork, error) {
	if output == nil {
		return GraphEffects{}, PageWork{}, ErrInvalid
	}
	return stagePartitionOperationsBudget(ctx, view, d, binding, configuration, ops, revision, l, g, budget, output)
}

// StagePartitionOperations admits typed operations through the checked reader,
// shared Plan and shared stager. It never installs or admits raw caller Delta.
func StagePartitionOperations(ctx context.Context, view *raftlog.ApplicationView, d OwnershipDeclaration, binding types.DefaultAxisBinding, configuration [32]byte, ops []graphstate.Operation, revision state.Revision, l Limits, g GraphLimits, budget OwnershipBudget) (GraphEffects, PageWork, error) {
	return stagePartitionOperationsBudget(ctx, view, d, binding, configuration, ops, revision, l, g, budget, nil)
}
func stagePartitionOperationsBudget(ctx context.Context, view *raftlog.ApplicationView, d OwnershipDeclaration, binding types.DefaultAxisBinding, configuration [32]byte, ops []graphstate.Operation, revision state.Revision, l Limits, g GraphLimits, budget OwnershipBudget, supplied *graphstate.OutputBudget) (GraphEffects, PageWork, error) {
	if err := budget.validate(); err != nil {
		return GraphEffects{}, PageWork{}, err
	}
	g, err := g.resolve()
	if err != nil {
		return GraphEffects{}, PageWork{}, err
	}
	g.MaxOutputBytes = min(g.MaxOutputBytes, budget.OutputBytes)
	arena, err := selectedGraphOutputBudget(g, supplied)
	if err != nil {
		return GraphEffects{}, PageWork{}, err
	}
	v, work, err := openPartitionGraphReadViewBudget(ctx, view, d, binding, configuration, l, g, budget, arena)
	if err != nil {
		return GraphEffects{}, work, err
	}
	resolved := v.limits
	resolved.MaxOutputBytes = min(resolved.MaxOutputBytes, budget.OutputBytes)
	return stageOpenedOperations(ctx, v.c, v, ops, revision, resolved, nil)
}
func (s *partitionReadScope) bucket(q *reader, kind RoutingBucketKind, key []byte) error {
	owner, err := s.routing.Bucket(kind, key)
	if err != nil {
		return err
	}
	if owner.Partition != s.namespace.Partition {
		return ErrRemoteParticipant
	}
	return q.materialize(128)
}
func (s *partitionReadScope) axis(q *reader, a temporal.Axis, encoded []byte, registration bool) error {
	if a.Descriptor().ID == s.binding.Axis().Descriptor().ID {
		if a.Descriptor() != s.binding.Axis().Descriptor() || a.DefinitionHash() != s.binding.Axis().DefinitionHash() {
			if !registration {
				return ErrCorrupt
			}
			return errors.Join(graphstate.ErrInvalidInput, ErrRebinding)
		}
		return nil
	}
	id := a.Descriptor().ID
	if err := s.bucket(q, AxisRoutingBucket, id[:]); err != nil {
		return err
	}
	if err := q.materialize(512 + 6*(27+axisVariableBytes(a))); err != nil {
		return err
	}
	key := AxisRegistrationKey(s.namespace, id)
	wire, found, err := q.get(key)
	if err != nil {
		return err
	}
	if found {
		stored, err := readAxis(wire, s.namespace, q.c.limits)
		if err != nil {
			return err
		}
		if stored.Descriptor() != a.Descriptor() || stored.DefinitionHash() != a.DefinitionHash() {
			if !registration {
				return ErrCorrupt
			}
			return errors.Join(graphstate.ErrInvalidInput, ErrRebinding)
		}
		return nil
	}
	if !registration {
		return ErrCorrupt
	}
	return q.put(key, encoded)
}
func (s *partitionReadScope) unique(q *reader, definition graphstate.PropertyDefinition, equality string) error {
	if err := q.materialize(128 + 2*len(definition.Name) + 2*len(equality)); err != nil {
		return err
	}
	// Lengths and type tags separate every exact logical uniqueness predicate.
	// The scope/time window does not route claims with the same logical value apart.
	key := []byte{byte(definition.Owner), byte(definition.Type), byte(definition.Cardinality), byte(definition.Unique)}
	key = appendField(key, []byte(definition.Name))
	key = appendField(key, []byte(equality))
	return s.bucket(q, UniqueRoutingBucket, key)
}

// previewAxis checks registration authority before operation cloning/planning.
// An absent authority can be registered only by this all-local typed stager;
// conflicting definitions sharing AxisID always reach the same authority.
func (s *partitionReadScope) previewAxis(q *reader, a temporal.Axis) error {
	if a.Descriptor().ID == s.binding.Axis().Descriptor().ID {
		if a.Descriptor() != s.binding.Axis().Descriptor() || a.DefinitionHash() != s.binding.Axis().DefinitionHash() {
			return ErrRebinding
		}
		return nil
	}
	id := a.Descriptor().ID
	if err := s.bucket(q, AxisRoutingBucket, id[:]); err != nil {
		return err
	}
	wire, found, err := q.get(AxisRegistrationKey(s.namespace, id))
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	if err := q.materialize(512 + 6*len(wire)); err != nil {
		return err
	}
	stored, err := readAxis(wire, s.namespace, q.c.limits)
	if err != nil {
		return err
	}
	if stored.Descriptor() != a.Descriptor() || stored.DefinitionHash() != a.DefinitionHash() {
		return ErrRebinding
	}
	return nil
}
