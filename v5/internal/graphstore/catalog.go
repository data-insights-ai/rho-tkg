package graphstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"math"
	"slices"
	"sync"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// Catalog borrows one immutable ApplicationView; its caller owns that view's
// lifetime. It retains only root metadata and bounded active staging, never a
// graph/history/value map. Semantic corruption/I/O stops all catalog methods.
type Catalog struct {
	view                        *raftlog.ApplicationView
	root                        Root
	limits                      Limits
	mu                          sync.Mutex
	poison                      error
	stages, records, stageBytes int
	hash                        func(string) [32]byte // fixed production hash; private collision-test seam
}

func equalityDigest(key string) [32]byte {
	return sha256.Sum256(append([]byte("rho-tkg:local-value-key:v1\x00"), key...))
}

// OpenCatalog validates routing and ownership without creating a cut, taking
// ownership of view or reading every catalog record. Single-voter support is a
// property of the underlying application integration, not identity placement.
func OpenCatalog(view *raftlog.ApplicationView, n Namespace, ownershipEpoch uint64, l Limits) (*Catalog, error) {
	l, err := l.resolve()
	if err != nil {
		return nil, err
	}
	if err := n.validate(); err != nil {
		return nil, err
	}
	if view == nil || ownershipEpoch == 0 {
		return nil, ErrInvalid
	}
	image, err := view.Root()
	if err != nil {
		return nil, err
	}
	root, err := DecodeRoot(image.Image)
	if err != nil {
		return nil, err
	}
	if root.namespace != n {
		return nil, ErrNamespace
	}
	if root.owner != ownershipEpoch {
		return nil, ErrStaleOwner
	}
	return &Catalog{view: view, root: root, limits: l, hash: equalityDigest}, nil
}
func (c *Catalog) check(ctx context.Context) error {
	if c == nil || ctx == nil {
		return ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	poison := c.poison
	c.mu.Unlock()
	if poison != nil {
		return errors.Join(ErrPoisoned, poison)
	}
	_, err := c.view.Root()
	return err
}
func (c *Catalog) failure(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, raftlog.ErrLimit) || errors.Is(err, temporal.ErrResourceLimit) || errors.Is(err, graphstate.ErrResourceLimit) {
		return errors.Join(ErrResourceLimit, err)
	}
	if errors.Is(err, ErrInvalid) || errors.Is(err, ErrNamespace) || errors.Is(err, ErrRebinding) || errors.Is(err, ErrResourceLimit) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, raftlog.ErrClosed) || errors.Is(err, ErrClosed) {
		return err
	}
	c.mu.Lock()
	if c.poison == nil {
		c.poison = err
	}
	c.mu.Unlock()
	return err
}

// Root returns checked metadata by value; it is not a commit receipt.
func (c *Catalog) Root() (Root, error) {
	if err := c.check(context.Background()); err != nil {
		return Root{}, err
	}
	return c.root, nil
}

type reader struct {
	c                 *Catalog
	ctx               context.Context
	stage             *Stage
	pending           map[string]raftlog.KV
	rows, bytes       int
	maxRows, maxBytes int // optional operation caps; zero preserves catalog policy
}

func (c *Catalog) reader(ctx context.Context) (*reader, error) {
	if err := c.check(ctx); err != nil {
		return nil, err
	}
	return &reader{c: c, ctx: ctx, bytes: rootBytes}, nil
}
func (q *reader) get(key []byte) ([]byte, bool, error) {
	l := q.c.limits
	if q.maxRows > 0 {
		l.MaxReadRows = min(l.MaxReadRows, q.maxRows)
	}
	if q.maxBytes > 0 {
		l.MaxReadBytes = min(l.MaxReadBytes, q.maxBytes)
	}
	if err := q.ctx.Err(); err != nil {
		return nil, false, err
	}
	if q.rows >= l.MaxReadRows || len(key)+64 > l.MaxReadBytes-q.bytes {
		return nil, false, ErrResourceLimit
	}
	q.rows++
	var row raftlog.KV
	found := false
	if q.pending != nil {
		row, found = q.pending[string(key)]
	}
	if !found && q.stage != nil {
		row, found = q.stage.writes[string(key)]
	}
	if !found {
		maxBytes := min(l.MaxRecordBytes+len(key), l.MaxReadBytes-q.bytes-64, q.c.view.ReadLimits().Bytes)
		var err error
		row, found, err = q.c.view.Get(q.ctx, key, maxBytes)
		if err != nil {
			return nil, false, err
		}
	}
	size := max(len(key), cap(row.Key)) + cap(row.Value) + 64
	if size > l.MaxReadBytes-q.bytes {
		return nil, false, ErrResourceLimit
	}
	q.bytes += size
	if found && row.Deleted {
		return nil, false, ErrCorrupt
	}
	if !found {
		return nil, false, nil
	}
	return row.Value, true, nil
}
func (q *reader) graph(id graphstate.GraphID) error {
	if id != q.c.root.namespace.Graph {
		return ErrNamespace
	}
	return nil
}
func (q *reader) axis(id temporal.AxisID) (temporal.Axis, bool, error) {
	if id == (temporal.AxisID{}) {
		return temporal.Axis{}, false, ErrInvalid
	}
	b, found, err := q.get(axisKey(q.c.root.namespace, id))
	if err != nil || !found {
		return temporal.Axis{}, found, err
	}
	a, err := readAxis(b, q.c.root.namespace, q.c.limits)
	if err == nil && a.Descriptor().ID != id {
		err = ErrCorrupt
	}
	if err == nil {
		d := a.Descriptor()
		err = q.materialize(27 + len(d.Reference) + len(d.CanonicalUnit))
	}
	return a, err == nil, err
}
func (q *reader) checkAxis(a temporal.Axis) error {
	stored, found, err := q.axis(a.Descriptor().ID)
	if err != nil {
		return err
	}
	if !found || stored.Descriptor() != a.Descriptor() || stored.DefinitionHash() != a.DefinitionHash() {
		return ErrCorrupt
	}
	return nil
}
func (q *reader) property(owner graphstate.EntityKind, name string) (graphstate.PropertyDefinition, bool, error) {
	if !validName(name, q.c.limits) || owner != graphstate.Node && owner != graphstate.Relationship {
		return graphstate.PropertyDefinition{}, false, ErrInvalid
	}
	b, found, err := q.get(schemaKey(q.c.root.namespace, owner, name))
	if err != nil || !found {
		return graphstate.PropertyDefinition{}, found, err
	}
	d, err := readProperty(b, q.c.root.namespace, q.c.limits)
	if err == nil && (d.Owner != owner || d.Name != name) {
		err = ErrCorrupt
	}
	if err == nil {
		err = q.materialize(len(d.Name))
	}
	return d, err == nil, err
}
func (q *reader) entity(ref EntityRef) (graphstate.EntityRecord, bool, error) {
	if err := q.graph(ref.Graph); err != nil {
		return graphstate.EntityRecord{}, false, err
	}
	if ref.ID == 0 {
		return graphstate.EntityRecord{}, false, ErrInvalid
	}
	b, found, err := q.get(entityKey(q.c.root.namespace, ref.ID))
	if err != nil || !found {
		return graphstate.EntityRecord{}, found, err
	}
	r, err := readEntity(b, q.c.root.namespace, q.c.limits)
	if err == nil && r.ID != ref.ID {
		err = ErrCorrupt
	}
	if err == nil {
		d := r.Axis.Descriptor()
		err = q.materialize(len(r.Type) + 27 + len(d.Reference) + len(d.CanonicalUnit))
	}
	if err == nil {
		err = q.checkAxis(r.Axis)
	}
	return r, err == nil, err
}
func lifeShape(owner graphstate.EntityRecord, r graphstate.LifeRecord) bool {
	if owner.Kind == graphstate.Node {
		return r.SourceLife == 0 && r.TargetLife == 0
	}
	return owner.Mode != graphstate.LifeBound || r.SourceLife != 0 && r.TargetLife != 0
}
func (q *reader) life(ref LifeRef) (graphstate.LifeRecord, bool, error) {
	if err := q.graph(ref.Graph); err != nil {
		return graphstate.LifeRecord{}, false, err
	}
	if ref.Owner == 0 || ref.ID == 0 {
		return graphstate.LifeRecord{}, false, ErrInvalid
	}
	b, found, err := q.get(lifeKey(q.c.root.namespace, ref.Owner, ref.ID))
	if err != nil || !found {
		return graphstate.LifeRecord{}, found, err
	}
	r, err := readLife(b, q.c.root.namespace, q.c.limits)
	if err == nil && (r.Owner != ref.Owner || r.Life != ref.ID) {
		err = ErrCorrupt
	}
	if err == nil {
		owner, found, e := q.entity(EntityRef{ref.Graph, ref.Owner})
		if e != nil {
			err = e
		} else if !found || !lifeShape(owner, r) {
			err = ErrCorrupt
		}
	}
	return r, err == nil, err
}
func (q *reader) bucket(hash [32]byte) (uint64, bool, error) {
	b, found, err := q.get(bucketKey(q.c.root.namespace, hash))
	if err != nil || !found {
		return 0, found, err
	}
	count, err := readNumber(b, q.c.root.namespace, bucketRecord, q.c.limits)
	if err == nil && (count == 0 || count == math.MaxUint64) {
		err = ErrCorrupt
	}
	if err == nil && count > uint64(q.c.limits.MaxBucketValues) {
		err = ErrResourceLimit
	}
	return count, err == nil, err
} // #nosec G115 -- resolved positive MaxBucketValues <= 65536.
func (q *reader) member(hash [32]byte, ordinal uint64) (graphstate.ValueID, error) {
	b, found, err := q.get(memberKey(q.c.root.namespace, hash, ordinal))
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, ErrCorrupt
	}
	id, err := readNumber(b, q.c.root.namespace, bucketMemberRecord, q.c.limits)
	if err != nil || id == 0 {
		return 0, ErrCorrupt
	}
	return graphstate.ValueID(id), nil
}
func (q *reader) value(ref ValueRef) (ValueEntry, []byte, bool, error) {
	if err := q.graph(ref.Graph); err != nil {
		return ValueEntry{}, nil, false, err
	}
	if ref.ID == 0 {
		return ValueEntry{}, nil, false, ErrInvalid
	}
	b, found, err := q.get(valueKey(q.c.root.namespace, ref.ID))
	if err != nil || !found {
		return ValueEntry{}, nil, found, err
	}
	entry, key, ordinal, err := readValue(b, q.c.root.namespace, q.c.limits)
	if err == nil && entry.Ref != ref {
		err = ErrCorrupt
	}
	if err == nil {
		if s, ok := entry.Value.Scope(); ok {
			err = q.checkAxis(s.Axis())
		}
	}
	if err == nil {
		hash := q.c.hash(string(key))
		count, found, e := q.bucket(hash)
		if e != nil {
			err = e
		} else if !found || ordinal >= count {
			err = ErrCorrupt
		} else {
			id, e := q.member(hash, ordinal)
			if e != nil {
				err = e
			} else if id != ref.ID {
				err = ErrCorrupt
			}
		}
	}
	if err == nil {
		extra := len(key)
		if s, ok := entry.Value.Scope(); ok {
			d := s.Axis().Descriptor()
			extra += 27 + len(d.Reference) + len(d.CanonicalUnit)
		}
		err = q.materialize(extra)
	}
	return entry, key, err == nil, err
}
func (q *reader) local(key string) (ValueEntry, bool, error) {
	hash := q.c.hash(key)
	count, found, err := q.bucket(hash)
	if err != nil || !found {
		return ValueEntry{}, found, err
	}
	for ordinal := uint64(0); ordinal < count; ordinal++ {
		id, err := q.member(hash, ordinal)
		if err != nil {
			return ValueEntry{}, false, err
		}
		entry, stored, found, err := q.value(ValueRef{q.c.root.namespace.Graph, id})
		if err != nil {
			return ValueEntry{}, false, err
		}
		if !found {
			return ValueEntry{}, false, ErrCorrupt
		}
		if q.c.hash(string(stored)) != hash || entry.ordinal != ordinal {
			return ValueEntry{}, false, ErrCorrupt
		}
		if bytes.Equal(stored, []byte(key)) {
			return entry, true, nil
		}
	}
	return ValueEntry{}, false, nil
}

// Axis reads a full exact descriptor at this view.
func (c *Catalog) Axis(ctx context.Context, id temporal.AxisID) (temporal.Axis, bool, error) {
	q, err := c.reader(ctx)
	if err != nil {
		return temporal.Axis{}, false, err
	}
	r, found, err := q.axis(id)
	if err != nil {
		return temporal.Axis{}, false, c.failure(err)
	}
	return r, found, nil
}

// Property reads one owner-kind-qualified immutable definition.
func (c *Catalog) Property(ctx context.Context, owner graphstate.EntityKind, name string) (graphstate.PropertyDefinition, bool, error) {
	q, err := c.reader(ctx)
	if err != nil {
		return graphstate.PropertyDefinition{}, false, err
	}
	r, found, err := q.property(owner, name)
	if err != nil {
		return graphstate.PropertyDefinition{}, false, c.failure(err)
	}
	return r, found, nil
}

// Entity reads stable identity, including its complete native axis definition.
func (c *Catalog) Entity(ctx context.Context, ref EntityRef) (graphstate.EntityRecord, bool, error) {
	q, err := c.reader(ctx)
	if err != nil {
		return graphstate.EntityRecord{}, false, err
	}
	r, found, err := q.entity(ref)
	if err != nil {
		return graphstate.EntityRecord{}, false, c.failure(err)
	}
	return r, found, nil
}

// Life reads immutable owner/binding metadata, not temporal life coverage.
func (c *Catalog) Life(ctx context.Context, ref LifeRef) (graphstate.LifeRecord, bool, error) {
	q, err := c.reader(ctx)
	if err != nil {
		return graphstate.LifeRecord{}, false, err
	}
	r, found, err := q.life(ref)
	if err != nil {
		return graphstate.LifeRecord{}, false, c.failure(err)
	}
	return r, found, nil
}

// Value reads an exact supplied ID and verifies descriptor/bucket references.
func (c *Catalog) Value(ctx context.Context, ref ValueRef) (ValueEntry, bool, error) {
	q, err := c.reader(ctx)
	if err != nil {
		return ValueEntry{}, false, err
	}
	r, _, found, err := q.value(ref)
	if err != nil {
		return ValueEntry{}, false, c.failure(err)
	}
	return r, found, nil
}

// LookupLocalValueIdentity compares the exact EqualityKey, never just its hash.
// First-seen canonical lookup is stable when later equal-content aliases arrive.
// This is partition-local lookup, NOT graph-wide uniqueness or routing.
func (c *Catalog) LookupLocalValueIdentity(ctx context.Context, value graphstate.Scalar) (ValueEntry, bool, error) {
	q, err := c.reader(ctx)
	if err != nil {
		return ValueEntry{}, false, err
	}
	key, err := value.EqualityKey(c.limits.valueLimits())
	if err != nil {
		return ValueEntry{}, false, c.failure(callerError(err))
	}
	if len(key) > c.limits.MaxReadBytes-q.bytes {
		return ValueEntry{}, false, ErrResourceLimit
	}
	q.bytes += len(key)
	r, found, err := q.local(key)
	if err != nil {
		return ValueEntry{}, false, c.failure(err)
	}
	return r, found, nil
}

// Stage accumulates bounded touched catalog records only. It performs shape,
// identity and reference checks, not graph temporal constraints/transactions.
// Its caller must use the validated host/materializer path for graph mutations.
// It neither commits nor acknowledges and cannot install an arbitrary Delta.
type Stage struct {
	mu     sync.Mutex
	c      *Catalog
	writes map[string]raftlog.KV
	bytes  int
	closed bool
}

// NewStage creates a bounded private staging handle. Close releases shared caps.
func (c *Catalog) NewStage(ctx context.Context) (*Stage, error) {
	if err := c.check(ctx); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.poison != nil {
		return nil, errors.Join(ErrPoisoned, c.poison)
	}
	if c.stages >= c.limits.MaxStages || 128 > c.limits.MaxStageBytes-c.stageBytes {
		return nil, ErrResourceLimit
	}
	c.stages++
	c.stageBytes += 128
	return &Stage{c: c, writes: make(map[string]raftlog.KV), bytes: 128}, nil
}
func (s *Stage) operation(ctx context.Context, fn func(*reader) error) error {
	if s == nil {
		return ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	q, err := s.c.reader(ctx)
	if err != nil {
		return err
	}
	q.stage = s
	q.pending = make(map[string]raftlog.KV)
	if err := fn(q); err != nil {
		return s.c.failure(err)
	}
	extraRecords, extraBytes := 0, 0
	for key, row := range q.pending {
		cost := 2*len(key) + len(row.Value) + 64
		if old, ok := s.writes[key]; ok {
			extraBytes += cost - (2*len(key) + len(old.Value) + 64)
		} else {
			extraRecords++
			extraBytes += cost
		}
	}
	s.c.mu.Lock()
	defer s.c.mu.Unlock()
	if s.c.poison != nil {
		return errors.Join(ErrPoisoned, s.c.poison)
	}
	if extraRecords > s.c.limits.MaxStageRecords-s.c.records || extraBytes > s.c.limits.MaxStageBytes-s.c.stageBytes {
		return ErrResourceLimit
	}
	for k, row := range q.pending {
		s.writes[k] = row
	}
	s.c.records += extraRecords
	s.c.stageBytes += extraBytes
	s.bytes += extraBytes
	return nil
}
func (q *reader) put(key, value []byte) error {
	if len(q.pending) >= q.c.limits.MaxStageRecords {
		return ErrResourceLimit
	}
	total := 128
	for k, row := range q.pending {
		total += 2*len(k) + len(row.Value) + 64
	}
	cost := 2*len(key) + len(value) + 64
	if cost > q.c.limits.MaxStageBytes-total {
		return ErrResourceLimit
	}
	q.pending[string(key)] = raftlog.KV{Key: exactCopy(key), Value: exactCopy(value)}
	return nil
}
func (q *reader) stageAxis(axis temporal.Axis) error {
	encoded, err := encodeAxis(q.c.root.namespace, axis, q.c.limits)
	if err != nil {
		return err
	}
	old, found, err := q.axis(axis.Descriptor().ID)
	if err != nil {
		return err
	}
	if found {
		if old.Descriptor() != axis.Descriptor() || old.DefinitionHash() != axis.DefinitionHash() {
			return ErrRebinding
		}
		return nil
	}
	return q.put(axisKey(q.c.root.namespace, axis.Descriptor().ID), encoded)
}

// Axis stages exact descriptor identity; errors leave the stage unchanged.
func (s *Stage) Axis(ctx context.Context, axis temporal.Axis) error {
	return s.operation(ctx, func(q *reader) error { return q.stageAxis(axis) })
}

// Property stages an immutable owner-qualified schema definition.
func (s *Stage) Property(ctx context.Context, d graphstate.PropertyDefinition) error {
	return s.operation(ctx, func(q *reader) error {
		encoded, err := encodeProperty(q.c.root.namespace, d, q.c.limits)
		if err != nil {
			return err
		}
		old, found, err := q.property(d.Owner, d.Name)
		if err != nil {
			return err
		}
		if found {
			if old != d {
				return ErrRebinding
			}
			return nil
		}
		return q.put(schemaKey(q.c.root.namespace, d.Owner, d.Name), encoded)
	})
}

// Entity stages shape/identity and descriptor metadata. Endpoint existence/life
// coverage belongs to validated graph planning; remote references are not forced
// into this partition's entity catalog.
func (s *Stage) Entity(ctx context.Context, ref EntityRef, r graphstate.EntityRecord) error {
	return s.operation(ctx, func(q *reader) error {
		if err := q.graph(ref.Graph); err != nil {
			return err
		}
		if ref.ID == 0 || r.ID != ref.ID {
			return ErrInvalid
		}
		encoded, err := encodeEntity(q.c.root.namespace, r, q.c.limits)
		if err != nil {
			return err
		}
		old, found, err := q.entity(ref)
		if err != nil {
			return err
		}
		if found {
			prior, err := encodeEntity(q.c.root.namespace, old, q.c.limits)
			if err != nil {
				return err
			}
			if !bytes.Equal(prior, encoded) {
				return ErrRebinding
			}
			return nil
		}
		if err := q.stageAxis(r.Axis); err != nil {
			return err
		}
		return q.put(entityKey(q.c.root.namespace, ref.ID), encoded)
	})
}

// Life stages owner/binding identity; temporal coverage and remote endpoint
// references are validated by the graph planner, not this catalog layer.
func (s *Stage) Life(ctx context.Context, ref LifeRef, r graphstate.LifeRecord) error {
	return s.operation(ctx, func(q *reader) error {
		if err := q.graph(ref.Graph); err != nil {
			return err
		}
		if ref.Owner != r.Owner || ref.ID != r.Life || ref.Owner == 0 || ref.ID == 0 {
			return ErrInvalid
		}
		owner, found, err := q.entity(EntityRef{ref.Graph, ref.Owner})
		if err != nil {
			return err
		}
		if !found || !lifeShape(owner, r) {
			return ErrInvalid
		}
		old, found, err := q.life(ref)
		if err != nil {
			return err
		}
		if found {
			if old != r {
				return ErrRebinding
			}
			return nil
		}
		encoded, err := encodeLife(q.c.root.namespace, r, q.c.limits)
		if err != nil {
			return err
		}
		return q.put(lifeKey(q.c.root.namespace, ref.Owner, ref.ID), encoded)
	})
}
func (q *reader) stageValue(ref ValueRef, value graphstate.Scalar, key string) error {
	if err := q.graph(ref.Graph); err != nil {
		return err
	}
	if ref.ID == 0 {
		return ErrInvalid
	}
	_, old, found, err := q.value(ref)
	if err != nil {
		return err
	}
	if found {
		if !bytes.Equal(old, []byte(key)) {
			return ErrRebinding
		}
		return nil
	}
	hash := q.c.hash(key)
	count, _, err := q.bucket(hash)
	if err != nil {
		return err
	}
	if count >= uint64(q.c.limits.MaxBucketValues) || count == math.MaxUint64 {
		return ErrResourceLimit
	}
	if scope, ok := value.Scope(); ok {
		if err := q.stageAxis(scope.Axis()); err != nil {
			return err
		}
	}
	encoded, err := encodeValue(q.c.root.namespace, ref, value, key, count, q.c.limits)
	if err != nil {
		return err
	}
	for _, row := range []raftlog.KV{{Key: valueKey(q.c.root.namespace, ref.ID), Value: encoded}, {Key: memberKey(q.c.root.namespace, hash, count), Value: encodeNumber(q.c.root.namespace, bucketMemberRecord, uint64(ref.ID))}, {Key: bucketKey(q.c.root.namespace, hash), Value: encodeNumber(q.c.root.namespace, bucketRecord, count+1)}} {
		if err := q.put(row.Key, row.Value); err != nil {
			return err
		}
	}
	return nil
} // #nosec G115 -- resolved positive MaxBucketValues <= 65536.
// Value preserves an admitted supplied ID, including equal-content aliases.
// It does not certify allocator claims or graph-wide canonical uniqueness.
func (s *Stage) Value(ctx context.Context, ref ValueRef, value graphstate.Scalar) error {
	return s.operation(ctx, func(q *reader) error {
		key, err := value.EqualityKey(q.c.limits.valueLimits())
		if err != nil {
			return callerError(err)
		}
		return q.stageValue(ref, value, key)
	})
}

// InternLocal uses exact local lookup; a supplied unused fresh ID remains burned
// under the allocator contract. It is never reclaimed here.
func (s *Stage) InternLocal(ctx context.Context, fresh ValueRef, value graphstate.Scalar) (ValueRef, error) {
	var result ValueRef
	err := s.operation(ctx, func(q *reader) error {
		if err := q.graph(fresh.Graph); err != nil {
			return err
		}
		if fresh.ID == 0 {
			return ErrInvalid
		}
		key, err := value.EqualityKey(q.c.limits.valueLimits())
		if err != nil {
			return callerError(err)
		}
		q.bytes += len(key)
		if q.bytes > q.c.limits.MaxReadBytes {
			return ErrResourceLimit
		}
		old, found, err := q.local(key)
		if err != nil {
			return err
		}
		if found {
			result = old.Ref
			return nil
		}
		if err := q.stageValue(fresh, value, key); err != nil {
			return err
		}
		result = fresh
		return nil
	})
	if err != nil {
		return ValueRef{}, err
	}
	return result, nil
}

// Writes returns independently owned, sorted, exact-capacity bytes suitable for
// ApplicationBatch.Writes. It is NOT admission, freshness or constraint proof.
func (s *Stage) Writes() ([]raftlog.KV, error) {
	if s == nil {
		return nil, ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}
	if err := s.c.check(context.Background()); err != nil {
		return nil, err
	}
	out := make([]raftlog.KV, 0, len(s.writes))
	for _, row := range s.writes {
		out = append(out, raftlog.KV{Key: exactCopy(row.Key), Value: exactCopy(row.Value)})
	}
	slices.SortFunc(out, func(a, b raftlog.KV) int { return bytes.Compare(a.Key, b.Key) })
	return out, nil
}

// Close releases only staging. The caller still owns the borrowed read view.
func (s *Stage) Close() error {
	if s == nil {
		return ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.c.mu.Lock()
	defer s.c.mu.Unlock()
	s.c.stages--
	s.c.records -= len(s.writes)
	s.c.stageBytes -= s.bytes
	s.closed = true
	s.writes = nil
	return nil
}

func exactCopy(src []byte) []byte {
	if src == nil {
		return nil
	}
	out := make([]byte, len(src))
	copy(out, src)
	return out
}

func callerError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, temporal.ErrResourceLimit) || errors.Is(err, graphstate.ErrResourceLimit) {
		return errors.Join(ErrResourceLimit, err)
	}
	return errors.Join(ErrInvalid, err)
}

func (q *reader) materialize(bytes int) error {
	limit := q.c.limits.MaxReadBytes
	if q.maxBytes > 0 {
		limit = min(limit, q.maxBytes)
	}
	if bytes < 0 || bytes > limit-q.bytes {
		return ErrResourceLimit
	}
	q.bytes += bytes
	return nil
}
