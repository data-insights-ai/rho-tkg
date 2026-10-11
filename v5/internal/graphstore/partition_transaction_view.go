package graphstore

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"slices"
	"sync"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

// PartitionTransactionView borrows the complete declared source set for one
// bounded planning request. Its identity binds actual source bases; it is not a
// certified graph cut, quorum proof, write grant or public distributed reader.
type PartitionTransactionView struct {
	mu               sync.Mutex
	declaration      OwnershipDeclaration
	binding          types.DefaultAxisBinding
	configuration    [32]byte
	views            []*ReadView
	id               graphstate.ViewID
	limits           GraphLimits
	arena            *graphstate.OutputBudget
	work             PageWork
	witnesses        []transactionRangeWitness
	closed           bool
	installation     *transactionInstallation
	planningRevision uint64
}
type transactionRangeWitness struct {
	publication RangePublication
	owner       RangeOwnership
}

// OpenPartitionTransactionView checks every declared source against the exact
// common genesis binding. Source and output consumption span all sources.
func OpenPartitionTransactionView(ctx context.Context, sources []*raftlog.ApplicationView, d OwnershipDeclaration, binding types.DefaultAxisBinding, configuration [32]byte, l Limits, g GraphLimits, budget OwnershipBudget, output *graphstate.OutputBudget) (*PartitionTransactionView, PageWork, error) {
	if len(sources) != d.Len() {
		return nil, PageWork{}, ErrInvalid
	}
	return openPartitionCompositeView(ctx, sources, d, binding, configuration, l, g, budget, output)
}

// OpenPartitionScopedReadView borrows exactly the selected sources. Missing
// authority refuses explicitly rather than expanding scope or proving absence.
// It supplies no certification or installation authority.
func OpenPartitionScopedReadView(ctx context.Context, sources []*raftlog.ApplicationView, scope []uint64, d OwnershipDeclaration, binding types.DefaultAxisBinding, configuration [32]byte, l Limits, g GraphLimits, budget OwnershipBudget, output *graphstate.OutputBudget) (*PartitionTransactionView, PageWork, error) {
	if ctx == nil || len(scope) == 0 || len(scope) != len(sources) || len(scope) > d.Len() {
		return nil, PageWork{}, ErrInvalid
	}
	for i, id := range scope {
		if i > 0 && scope[i-1] >= id || sources[i] == nil {
			return nil, PageWork{}, ErrInvalid
		}
		b, e := sources[i].ApplicationBinding(ctx)
		if e != nil {
			return nil, PageWork{}, e
		}
		if b.Identity.Partition != id {
			return nil, PageWork{}, ErrNamespace
		}
	}
	return openPartitionCompositeView(ctx, sources, d, binding, configuration, l, g, budget, output)
}
func openPartitionCompositeView(ctx context.Context, sources []*raftlog.ApplicationView, d OwnershipDeclaration, binding types.DefaultAxisBinding, configuration [32]byte, l Limits, g GraphLimits, budget OwnershipBudget, output *graphstate.OutputBudget) (out *PartitionTransactionView, work PageWork, err error) {
	if ctx == nil || !d.valid() || len(sources) == 0 || len(sources) > d.Len() || output == nil {
		return nil, work, ErrInvalid
	}
	if err := budget.validate(); err != nil {
		return nil, work, err
	}
	g, err = g.resolve()
	if err != nil {
		return nil, work, err
	}
	g.MaxSourceRows = min(g.MaxSourceRows, budget.SourceRows)
	g.MaxSourceBytes = min(g.MaxSourceBytes, budget.SourceBytes)
	g.MaxOutputBytes = min(g.MaxOutputBytes, budget.OutputBytes)
	arena, err := selectedGraphOutputBudget(g, output)
	if err != nil {
		return nil, work, err
	}
	if err := arena.Reserve(1024 + 64*d.Len()); err != nil {
		return nil, work, callerError(err)
	}
	out = &PartitionTransactionView{declaration: d, binding: binding, configuration: configuration, limits: g, arena: arena, views: make([]*ReadView, d.Len())}
	defer func() {
		if err != nil {
			for _, v := range out.views {
				if v != nil {
					_ = v.Close()
				}
			}
			out = nil
		}
	}()
	for _, source := range sources {
		if source == nil {
			return out, work, ErrInvalid
		}
		actual, e := source.ApplicationBinding(ctx)
		if e != nil {
			return out, work, e
		}
		index := -1
		for i, p := range d.entries {
			if p.Partition == actual.Identity.Partition {
				index = i
				break
			}
		}
		if index < 0 {
			return out, work, ErrNamespace
		}
		if out.views[index] != nil {
			return out, work, ErrInvalid
		}
		remaining := budget
		remaining.SourceRows = g.MaxSourceRows - work.Records
		remaining.SourceBytes = g.MaxSourceBytes - work.Bytes
		remaining.OutputBytes = min(g.MaxOutputBytes, arena.Remaining())
		if remaining.SourceRows < 1 || remaining.SourceBytes < 1 || remaining.OutputBytes < 1 {
			return out, work, ErrResourceLimit
		}
		v, w, e := openPartitionGraphReadViewBudget(ctx, source, d, binding, configuration, l, g, remaining, arena)
		work = addWork(work, w)
		if e != nil {
			return out, work, e
		}
		out.views[index] = v
		if index != 0 && out.views[0] != nil && v.route.routing.Digest() != out.views[0].route.routing.Digest() {
			return out, work, ErrRebinding
		}
	}
	out.views = slices.DeleteFunc(out.views, func(v *ReadView) bool { return v == nil })
	for _, v := range out.views {
		if v.route.routing.Digest() != out.views[0].route.routing.Digest() {
			return out, work, ErrRebinding
		}
	}
	if err := arena.Reserve(256 + 64*d.Len()); err != nil {
		return out, work, callerError(err)
	}
	h := sha256.New()
	_, _ = h.Write([]byte("rho-tkg:partition-transaction-view:v1\x00"))
	digest := d.Digest()
	_, _ = h.Write(digest[:])
	_, _ = h.Write(configuration[:])
	for _, v := range out.views {
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], v.c.root.namespace.Partition)
		_, _ = h.Write(b[:])
		_, _ = h.Write(v.id[:])
		v.route.transaction = out
	}
	copy(out.id[:], h.Sum(nil)[:16])
	out.work = work
	return out, work, nil
}
func (v *PartitionTransactionView) source(partition uint64) (*ReadView, error) {
	for _, s := range v.views {
		if s.c.root.namespace.Partition == partition {
			return s, nil
		}
	}
	return nil, ErrScopeUnavailable
}
func (v *PartitionTransactionView) check(ctx context.Context) error {
	if v == nil || ctx == nil {
		return ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if v.closed {
		return ErrClosed
	}
	return nil
}

// Graph returns the declared graph; nil/closed returns zero.
func (v *PartitionTransactionView) Graph() graphstate.GraphID {
	if v == nil {
		return graphstate.GraphID{}
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return graphstate.GraphID{}
	}
	return v.declaration.Graph()
}

// Identity returns the composite captured-source identity, never a cut.
func (v *PartitionTransactionView) Identity() graphstate.ViewID {
	if v == nil {
		return graphstate.ViewID{}
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return graphstate.ViewID{}
	}
	return v.id
}

// Work reports total consumed source work, including failed attempts.
func (v *PartitionTransactionView) Work() PageWork {
	if v == nil {
		return PageWork{}
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.work
}

// Close releases graph reader handles while preserving all borrowed sources.
func (v *PartitionTransactionView) Close() error {
	if v == nil {
		return ErrInvalid
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return nil
	}
	v.closed = true
	for _, s := range v.views {
		_ = s.Close()
	}
	v.witnesses = nil
	return nil
}

// owner requires at least one exact published witness, and rejects disagreement
// between retained copies. Missing local routing is never graph-wide absence.
func (v *PartitionTransactionView) owner(q *reader, id uint64) (uint64, error) {
	if id == 0 {
		return 0, ErrInvalid
	}
	for _, w := range v.witnesses {
		if w.publication.Contains(id) {
			return w.owner.Partition, w.owner.Check(w.publication, w.owner.Partition, w.owner.Epoch)
		}
	}
	var selected transactionRangeWitness
	found := false
	for _, s := range v.views {
		rows := min(q.c.limits.MaxReadRows, q.maxRows) - q.rows
		bytes := min(q.c.limits.MaxReadBytes, q.maxBytes) - q.bytes
		if rows < 1 || bytes < 1 {
			return 0, ErrResourceLimit
		}
		p, o, w, err := lookupPublishedRangeBudget(q.ctx, s.c.view, s.c.root.namespace, id, v.configuration, OwnershipBudget{rows, bytes, min(bytes, v.arena.Remaining())}, q.arena)
		q.rows += w.Records
		q.bytes += w.Bytes
		if errors.Is(err, ErrRoutingUnknown) {
			continue
		}
		if err != nil {
			return 0, err
		}
		if err := o.Check(p, o.Partition, o.Epoch); err != nil {
			return 0, err
		}
		if _, err := v.source(o.Partition); err != nil {
			return 0, err
		}
		if found && (selected.publication != p || selected.owner != o) {
			return 0, ErrRebinding
		}
		selected = transactionRangeWitness{p, o}
		found = true
	}
	if !found {
		return 0, ErrRoutingUnknown
	}
	if len(v.witnesses) >= v.limits.Planner.MaxDependencies && v.limits.Planner.MaxDependencies != 0 {
		return 0, ErrResourceLimit
	}
	capacity, err := v.arena.ReserveSlice(len(v.witnesses), cap(v.witnesses), 256)
	if err != nil {
		return 0, callerError(err)
	}
	if capacity > cap(v.witnesses) {
		next := make([]transactionRangeWitness, len(v.witnesses), capacity)
		copy(next, v.witnesses)
		v.witnesses = next
	}
	v.witnesses = append(v.witnesses, selected)
	return selected.owner.Partition, nil
}
func (q *reader) transaction() *PartitionTransactionView {
	if q.route == nil {
		return nil
	}
	return q.route.transaction
}
func (q *reader) onPartition(partition uint64, fn func() error) error {
	t := q.transaction()
	if t == nil || partition == q.c.root.namespace.Partition {
		return fn()
	}
	v, err := t.source(partition)
	if err != nil {
		return err
	}
	oldC, oldRoute, oldView, oldFull := q.c, q.route, q.fullView, q.full
	q.c, q.route, q.fullView, q.full = v.c, v.route, &v.descriptor, nil
	if t.installation != nil {
		for n, s := range t.views {
			if s == v {
				q.full = &t.installation.states[n]
				break
			}
		}
	}
	defer func() { q.c, q.route, q.fullView, q.full = oldC, oldRoute, oldView, oldFull }()
	return fn()
}
func (q *pageReader) routedPostingMembership(root postingTreeRoot, key postingKey, lookup postingMembershipLookup) (bool, error) {
	t := q.q.transaction()
	if t == nil {
		return lookup(root, key, keyTreeLimits(q.limits))
	}
	partition, err := t.owner(q.q, uint64(key.endpoint))
	if err != nil {
		return false, err
	}
	if partition == q.q.c.root.namespace.Partition {
		return lookup(root, key, keyTreeLimits(q.limits))
	}
	var found bool
	err = q.q.onPartition(partition, func() error {
		p := pageReader{q: q.q, limits: q.limits}
		d, e := p.fullDescriptor()
		if e != nil {
			return e
		}
		tree := d.canonical
		if key.family == declaredIncidentRecord {
			tree = d.declared
		}
		found, e = p.hasPostingKey(tree, key, keyTreeLimits(p.limits))
		return e
	})
	return found, err
}
func (v *PartitionTransactionView) selectOwner(ctx context.Context, id uint64) (*ReadView, error) {
	s := v.views[0]
	before := s.Work()
	s.limits.MaxSourceRows = before.Records + v.limits.MaxSourceRows - v.work.Records
	s.limits.MaxSourceBytes = before.Bytes + v.limits.MaxSourceBytes - v.work.Bytes
	q, err := s.begin(ctx, graphstate.ReadBudget{})
	if err != nil {
		return nil, err
	}
	partition, e := v.owner(q.q, id)
	err = s.finish(q, e)
	v.work = addWork(v.work, subtractWork(s.Work(), before))
	if err != nil {
		return nil, err
	}
	return v.source(partition)
}
func subtractWork(a, b PageWork) PageWork {
	return PageWork{a.Records - b.Records, a.Bytes - b.Bytes, a.DirectoryPages - b.DirectoryPages, a.CheckpointPages - b.CheckpointPages, a.PatchPages - b.PatchPages, a.DecodedCells - b.DecodedCells}
}
func (v *PartitionTransactionView) prepareSource(s *ReadView) PageWork {
	before := s.Work()
	s.limits.MaxSourceRows = before.Records + v.limits.MaxSourceRows - v.work.Records
	s.limits.MaxSourceBytes = before.Bytes + v.limits.MaxSourceBytes - v.work.Bytes
	// The distinct output and continuation policies belong to the transaction,
	// rather than to each local reader. Existing local rollback remains intact.
	count, cursorBytes, outputBytes := 0, 0, 0
	for _, other := range v.views {
		if other != s {
			count += len(other.cursors) + len(other.pages.cursors)
			cursorBytes += other.cursorBytes + other.pages.cursorBytes
			outputBytes += other.outputBytes
		}
	}
	s.limits.Pages.MaxCursors = v.limits.Pages.MaxCursors - count
	s.limits.Pages.MaxCursorBytes = v.limits.Pages.MaxCursorBytes - cursorBytes
	s.pages.limits.MaxCursors = s.limits.Pages.MaxCursors
	s.pages.limits.MaxCursorBytes = s.limits.Pages.MaxCursorBytes
	s.limits.MaxOutputBytes = v.limits.MaxOutputBytes - outputBytes
	return before
}
func (v *PartitionTransactionView) finishSource(s *ReadView, before PageWork) {
	v.work = addWork(v.work, subtractWork(s.Work(), before))
}

// Entity reads the published primary authority, including negative reads.
func (v *PartitionTransactionView) Entity(ctx context.Context, id graphstate.EntityID) (graphstate.EntityRead, error) {
	if v == nil {
		return graphstate.EntityRead{}, ErrInvalid
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if err := v.check(ctx); err != nil {
		return graphstate.EntityRead{}, err
	}
	s, err := v.selectOwner(ctx, uint64(id))
	if err != nil {
		return graphstate.EntityRead{}, err
	}
	before := v.prepareSource(s)
	r, err := s.Entity(ctx, id)
	v.finishSource(s, before)
	if err == nil {
		r.View = v.id
	}
	return r, err
}

// Life follows its owner's range, never its life number.
func (v *PartitionTransactionView) Life(ctx context.Context, owner graphstate.EntityID, id graphstate.LifeID) (graphstate.LifeRead, error) {
	if v == nil {
		return graphstate.LifeRead{}, ErrInvalid
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if err := v.check(ctx); err != nil {
		return graphstate.LifeRead{}, err
	}
	s, err := v.selectOwner(ctx, uint64(owner))
	if err != nil {
		return graphstate.LifeRead{}, err
	}
	before := v.prepareSource(s)
	r, err := s.Life(ctx, owner, id)
	v.finishSource(s, before)
	if err == nil {
		r.View = v.id
	}
	return r, err
}

// Property uses immutable common-genesis schema authority.
func (v *PartitionTransactionView) Property(ctx context.Context, owner graphstate.EntityKind, name string) (graphstate.PropertyRead, error) {
	if v == nil {
		return graphstate.PropertyRead{}, ErrInvalid
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if err := v.check(ctx); err != nil {
		return graphstate.PropertyRead{}, err
	}
	s := v.views[0]
	before := v.prepareSource(s)
	r, err := s.Property(ctx, owner, name)
	v.finishSource(s, before)
	if err == nil {
		r.View = v.id
	}
	return r, err
}

// Value resolves the published ValueID authority independently of identity buckets.
func (v *PartitionTransactionView) Value(ctx context.Context, id graphstate.ValueID) (graphstate.ValueRead, error) {
	if v == nil {
		return graphstate.ValueRead{}, ErrInvalid
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if err := v.check(ctx); err != nil {
		return graphstate.ValueRead{}, err
	}
	s, err := v.selectOwner(ctx, uint64(id))
	if err != nil {
		return graphstate.ValueRead{}, err
	}
	before := v.prepareSource(s)
	r, err := s.Value(ctx, id)
	v.finishSource(s, before)
	if err == nil {
		r.View = v.id
	}
	return r, err
}
func (v *PartitionTransactionView) bucket(kind RoutingBucketKind, key []byte) (*ReadView, error) {
	o, err := v.views[0].route.routing.Bucket(kind, key)
	if err != nil {
		return nil, err
	}
	return v.source(o.Partition)
}

// ValueIdentity reads exact typed identity at its declared global authority.
func (v *PartitionTransactionView) ValueIdentity(ctx context.Context, value graphstate.Scalar) (graphstate.ValueRead, error) {
	if v == nil {
		return graphstate.ValueRead{}, ErrInvalid
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if err := v.check(ctx); err != nil {
		return graphstate.ValueRead{}, err
	}
	key, err := v.arena.ScalarKey(value, v.views[0].c.limits.valueLimits())
	if err != nil {
		return graphstate.ValueRead{}, callerError(err)
	}
	s, err := v.bucket(IdentityRoutingBucket, []byte(key))
	if err != nil {
		return graphstate.ValueRead{}, err
	}
	before := v.prepareSource(s)
	r, err := s.ValueIdentity(ctx, value)
	v.finishSource(s, before)
	if err == nil {
		r.View = v.id
	}
	return r, err
}

// ComponentPage retains actual primary page ownership and continuation semantics.
func (v *PartitionTransactionView) ComponentPage(ctx context.Context, p graphstate.ComponentQuery, c graphstate.Cursor, b graphstate.ReadBudget) (graphstate.ComponentPage, error) {
	if v == nil {
		return graphstate.ComponentPage{}, ErrInvalid
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if err := v.check(ctx); err != nil {
		return graphstate.ComponentPage{}, err
	}
	s, err := v.selectOwner(ctx, uint64(p.Key.Owner))
	if err != nil {
		return graphstate.ComponentPage{}, err
	}
	before := v.prepareSource(s)
	r, err := s.ComponentPage(ctx, p, c, b)
	v.finishSource(s, before)
	if err == nil && v.planningRevision != 0 {
		if e := v.arena.Reserve(256 * r.Data.Usage().Pieces()); e != nil {
			return graphstate.ComponentPage{}, callerError(e)
		}
		for _, piece := range r.Data.Pieces() {
			if piece.Cell().Revision().ID() >= v.planningRevision {
				return graphstate.ComponentPage{}, callerError(state.ErrInvalidRevision)
			}
		}
	}
	if err == nil {
		r.View = v.id
	}
	return r, err
}

// ComponentKeys includes checked empty prefixes at the owner's authority.
func (v *PartitionTransactionView) ComponentKeys(ctx context.Context, p graphstate.KeyPredicate, c graphstate.Cursor, b graphstate.ReadBudget) (graphstate.KeyPage, error) {
	if v == nil {
		return graphstate.KeyPage{}, ErrInvalid
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if err := v.check(ctx); err != nil {
		return graphstate.KeyPage{}, err
	}
	s, err := v.selectOwner(ctx, uint64(p.Owner))
	if err != nil {
		return graphstate.KeyPage{}, err
	}
	before := v.prepareSource(s)
	r, err := s.ComponentKeys(ctx, p, c, b)
	v.finishSource(s, before)
	if err == nil {
		r.View = v.id
	}
	return r, err
}
func uniqueRoutingKey(d graphstate.PropertyDefinition, equality string) []byte {
	key := []byte{byte(d.Owner), byte(d.Type), byte(d.Cardinality), byte(d.Unique)}
	key = appendField(key, []byte(d.Name))
	return appendField(key, []byte(equality))
}

// UniqueCandidates reads the global claim bucket and validates remote primaries.
func (v *PartitionTransactionView) UniqueCandidates(ctx context.Context, p graphstate.UniquePredicate, c graphstate.Cursor, b graphstate.ReadBudget) (graphstate.ClaimPage, error) {
	if v == nil {
		return graphstate.ClaimPage{}, ErrInvalid
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if err := v.check(ctx); err != nil {
		return graphstate.ClaimPage{}, err
	}
	key, err := v.arena.ScalarKey(p.Value, v.views[0].c.limits.valueLimits())
	if err != nil {
		return graphstate.ClaimPage{}, callerError(err)
	}
	if err := v.arena.Reserve(128 + 2*len(key) + 2*len(p.Definition.Name)); err != nil {
		return graphstate.ClaimPage{}, callerError(err)
	}
	s, err := v.bucket(UniqueRoutingBucket, uniqueRoutingKey(p.Definition, key))
	if err != nil {
		return graphstate.ClaimPage{}, err
	}
	before := v.prepareSource(s)
	r, err := s.UniqueCandidates(ctx, p, c, b)
	v.finishSource(s, before)
	if err == nil {
		r.View = v.id
	}
	return r, err
}

// IncidentRelationships reads the endpoint's complete declared posting authority.
func (v *PartitionTransactionView) IncidentRelationships(ctx context.Context, p graphstate.IncidentPredicate, c graphstate.Cursor, b graphstate.ReadBudget) (graphstate.EntityPage, error) {
	if v == nil {
		return graphstate.EntityPage{}, ErrInvalid
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if err := v.check(ctx); err != nil {
		return graphstate.EntityPage{}, err
	}
	s, err := v.selectOwner(ctx, uint64(p.Endpoint))
	if err != nil {
		return graphstate.EntityPage{}, err
	}
	before := v.prepareSource(s)
	r, err := s.IncidentRelationships(ctx, p, c, b)
	v.finishSource(s, before)
	if err == nil {
		r.View = v.id
	}
	return r, err
}

var _ graphstate.ReadView = (*PartitionTransactionView)(nil)
