package graphstore

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"strings"
	"sync"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// ReadView is a complete local single-partition graphstate view. It borrows
// Catalog/ApplicationView, owns bounded cursors, and supplies no cut/lease or
// installation authority. Source work is cumulative over this handle's lifetime,
// including misses, collision candidates, filtered rows and failed operations.
// Project/StageOperations create and close their own handle for one request.
type ReadView struct {
	mu                       sync.Mutex
	c                        *Catalog
	limits                   GraphLimits
	descriptor               fullIndexDescriptor
	base                     raftlog.ApplicationRoot
	id                       graphstate.ViewID
	pages                    *PageReader
	cursors                  map[graphstate.Cursor]fullContinuation
	cursorBytes, outputBytes int
	handleBytes              int
	work                     PageWork
	closed                   bool
}
type fullContinuation struct {
	kind         recordKind
	query        [32]byte
	key          graphstate.ComponentKey
	posting      postingKey
	relationship graphstate.EntityID
	bytes        int
}

var _ graphstate.ReadView = (*ReadView)(nil)

// OpenReadView refuses primitive/KeysOnly roots. Full coverage binds actual
// validated tree roots and immutable schema/canonical dictionary namespaces.
// Initialization and every read consume the same aggregate source ledger.
func OpenReadView(ctx context.Context, c *Catalog, l GraphLimits) (view *ReadView, err error) {
	if c == nil || ctx == nil {
		return nil, ErrInvalid
	}
	l, err = l.resolve()
	if err != nil {
		return nil, err
	}
	if err := c.check(ctx); err != nil {
		return nil, err
	}
	if c.root.topology != fullTopology {
		return nil, ErrTopologyUnsupported
	}
	// Charge the retained descriptor/limits/base image/page-reader/map headers.
	c.mu.Lock()
	if c.fullViews >= c.limits.MaxStages || fullViewMetadataBytes+c.rootImageBytes > c.limits.MaxReadBytes-c.fullViewBytes {
		c.mu.Unlock()
		return nil, ErrResourceLimit
	}
	c.fullViews++
	c.fullViewBytes += fullViewMetadataBytes + c.rootImageBytes
	c.mu.Unlock()
	success := false
	defer func() {
		if !success {
			c.mu.Lock()
			c.fullViews--
			c.fullViewBytes -= fullViewMetadataBytes + c.rootImageBytes
			c.mu.Unlock()
		}
	}()
	if l.MaxSourceBytes < c.rootImageBytes+fullViewMetadataBytes || l.MaxSourceRows < 5 {
		return nil, ErrResourceLimit
	}
	q, err := c.reader(ctx)
	if err != nil {
		return nil, err
	}
	q.maxRows = min(l.MaxSourceRows, l.Pages.MaxWorkRecords)
	q.maxBytes = min(l.MaxSourceBytes, l.Pages.MaxWorkBytes)
	if err := q.materialize(fullViewMetadataBytes); err != nil {
		return nil, err
	}
	d, found, err := q.fullDescriptor(c.root)
	if err == nil && !found {
		err = ErrCorrupt
	}
	if err != nil {
		return nil, c.failure(err)
	}
	p := pageReader{q: q, limits: l.Pages}
	if err := p.validateFullRoots(d); err != nil {
		return nil, c.failure(err)
	}
	// The fixed root image and fingerprint scratch are bounded before copying.
	if err := q.materialize(c.rootImageBytes + 512); err != nil {
		return nil, err
	}
	base, err := c.view.Root()
	if err != nil {
		return nil, err
	}
	wire, err := encodeFullDescriptor(d, c.root, c.limits)
	if err != nil {
		return nil, err
	}
	id := fullViewIdentity(c.root.namespace, wire, base)
	pages := &PageReader{c: c, limits: l.Pages, id: id, index: base.Index, cursors: make(map[graphstate.Cursor]continuation), complete: &d}
	_ = p.budget()
	view = &ReadView{c: c, limits: l, descriptor: d, base: base, id: id, pages: pages, cursors: make(map[graphstate.Cursor]fullContinuation), work: p.work, handleBytes: fullViewMetadataBytes + cap(base.Image)}
	success = true
	return view, nil
}
func fullViewIdentity(n Namespace, descriptor []byte, base raftlog.ApplicationRoot) graphstate.ViewID {
	b := append([]byte("rho-tkg:full-view:v1\x00"), keyPrefix(n, componentIndexDescriptorRecord)...)
	b = append(b, descriptor...)
	b = binary.BigEndian.AppendUint64(b, base.Generation)
	b = binary.BigEndian.AppendUint64(b, base.Index)
	b = append(b, base.ImageHash[:]...)
	hash := sha256.Sum256(b)
	var id graphstate.ViewID
	copy(id[:], hash[:16])
	return id
}

// Graph returns the qualified namespace; nil/closed handles return zero.
func (v *ReadView) Graph() graphstate.GraphID {
	if v == nil {
		return graphstate.GraphID{}
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return graphstate.GraphID{}
	}
	return v.c.root.namespace.Graph
}

// Identity is shared by every point/page door, including its PageReader.
func (v *ReadView) Identity() graphstate.ViewID {
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

// Work returns cumulative actual source/decode work, including failed reads.
func (v *ReadView) Work() PageWork {
	if v == nil {
		return PageWork{}
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.work
}

// Close serializes with reads, releases owned cursor/base handles and shared
// catalog accounting, and leaves the borrowed ApplicationView/Catalog open.
func (v *ReadView) Close() error {
	if v == nil {
		return ErrInvalid
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return nil
	}
	v.closed = true
	_ = v.pages.Close()
	v.cursors = nil
	v.cursorBytes = 0
	v.base.Image = nil
	v.c.mu.Lock()
	v.c.fullViews--
	v.c.fullViewBytes -= v.handleBytes
	v.c.mu.Unlock()
	return nil
}
func (v *ReadView) remaining() (int, int) {
	return v.limits.MaxSourceRows - v.work.Records, v.limits.MaxSourceBytes - v.work.Bytes
}
func (v *ReadView) check(ctx context.Context) error {
	if ctx == nil {
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
func (v *ReadView) begin(ctx context.Context, budget graphstate.ReadBudget) (*pageReader, error) {
	if err := v.check(ctx); err != nil {
		return nil, err
	}
	rows, bytes := v.remaining()
	rows = min(rows, v.limits.Pages.MaxWorkRecords)
	bytes = min(bytes, v.limits.Pages.MaxWorkBytes)
	if budget.Rows > 0 {
		rows = min(rows, budget.Rows)
	}
	if budget.Bytes > 0 {
		bytes = min(bytes, budget.Bytes)
	}
	if rows < 1 || bytes < v.c.rootImageBytes {
		return nil, ErrResourceLimit
	}
	q, err := v.c.reader(ctx)
	if err != nil {
		return nil, err
	}
	q.maxRows, q.maxBytes = rows, bytes
	q.fullView = &v.descriptor
	if err := q.materialize(0); err != nil {
		return nil, err
	}
	return &pageReader{q: q, limits: v.limits.Pages}, nil
}
func addWork(a, b PageWork) PageWork {
	return PageWork{a.Records + b.Records, a.Bytes + b.Bytes, a.DirectoryPages + b.DirectoryPages, a.CheckpointPages + b.CheckpointPages, a.PatchPages + b.PatchPages, a.DecodedCells + b.DecodedCells}
}
func (v *ReadView) finish(q *pageReader, err error) error {
	_ = q.budget()
	if err == nil {
		err = q.q.ctx.Err()
	}
	v.work = addWork(v.work, q.work)
	return v.c.failure(err)
}
func (v *ReadView) output(bytes int) error {
	if bytes < 0 || bytes > v.limits.MaxOutputBytes-v.outputBytes {
		return ErrResourceLimit
	}
	v.outputBytes += bytes
	return nil
}
func (v *ReadView) version() graphstate.ReadVersion { return graphstate.ReadVersion(v.base.Index) }

// Entity returns checked immutable metadata or explicit absence at this view.
func (v *ReadView) Entity(ctx context.Context, id graphstate.EntityID) (graphstate.EntityRead, error) {
	if v == nil || id == 0 {
		return graphstate.EntityRead{}, ErrInvalid
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	q, err := v.begin(ctx, graphstate.ReadBudget{})
	if err != nil {
		return graphstate.EntityRead{}, err
	}
	r, found, err := q.q.entity(EntityRef{v.c.root.namespace.Graph, id})
	if err == nil && found && r.Kind == graphstate.Relationship {
		err = q.checkCanonical(r)
	}
	cost := fullEntityOutputBytes + len(r.Type)
	if found {
		cost += axisVariableBytes(r.Axis)
	}
	if err = v.finish(q, err); err != nil {
		return graphstate.EntityRead{}, err
	}
	if err := v.output(cost); err != nil {
		return graphstate.EntityRead{}, err
	}
	return graphstate.EntityRead{View: v.id, Version: v.version(), Found: found, Record: r}, nil
}

// Life preserves its owner-qualified immutable endpoint binding after closure.
func (v *ReadView) Life(ctx context.Context, owner graphstate.EntityID, id graphstate.LifeID) (graphstate.LifeRead, error) {
	if v == nil || owner == 0 || id == 0 {
		return graphstate.LifeRead{}, ErrInvalid
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	q, err := v.begin(ctx, graphstate.ReadBudget{})
	if err != nil {
		return graphstate.LifeRead{}, err
	}
	r, found, err := q.q.life(LifeRef{v.c.root.namespace.Graph, owner, id})
	if err == nil && found {
		entity, exists, e := q.q.entity(EntityRef{v.c.root.namespace.Graph, owner})
		err = e
		if err == nil && !exists {
			err = ErrCorrupt
		}
		if err == nil && entity.Kind == graphstate.Relationship {
			err = q.checkDeclared(entity, r)
		}
	}
	if err = v.finish(q, err); err != nil {
		return graphstate.LifeRead{}, err
	}
	if err := v.output(fullLifeOutputBytes); err != nil {
		return graphstate.LifeRead{}, err
	}
	return graphstate.LifeRead{View: v.id, Version: v.version(), Found: found, Record: r}, nil
}

// Property reads one immutable owner-kind-qualified definition, including absence.
func (v *ReadView) Property(ctx context.Context, owner graphstate.EntityKind, name string) (graphstate.PropertyRead, error) {
	if v == nil {
		return graphstate.PropertyRead{}, ErrInvalid
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if !validName(name, v.c.limits) || owner != graphstate.Node && owner != graphstate.Relationship {
		return graphstate.PropertyRead{}, ErrInvalid
	}
	q, err := v.begin(ctx, graphstate.ReadBudget{})
	if err != nil {
		return graphstate.PropertyRead{}, err
	}
	r, found, err := q.q.property(owner, name)
	if err = v.finish(q, err); err != nil {
		return graphstate.PropertyRead{}, err
	}
	if err := v.output(fullPropertyOutputBytes + len(r.Name)); err != nil {
		return graphstate.PropertyRead{}, err
	}
	return graphstate.PropertyRead{View: v.id, Version: v.version(), Found: found, Record: r}, nil
}

// Value resolves an exact dictionary ID, including immutable content aliases.
func (v *ReadView) Value(ctx context.Context, id graphstate.ValueID) (graphstate.ValueRead, error) {
	if v == nil || id == 0 {
		return graphstate.ValueRead{}, ErrInvalid
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	q, err := v.begin(ctx, graphstate.ReadBudget{})
	if err != nil {
		return graphstate.ValueRead{}, err
	}
	r, _, found, err := q.q.value(ValueRef{v.c.root.namespace.Graph, id})
	cost := fullValueOutputBytes
	if err == nil && found {
		variable, e := scalarVariableOwned(r.Value, int(r.PayloadBytes), v.c.limits)
		err = e
		cost += variable
	}
	if err = v.finish(q, err); err != nil {
		return graphstate.ValueRead{}, err
	}
	if err := v.output(cost); err != nil {
		return graphstate.ValueRead{}, err
	}
	result := graphstate.ValueRead{View: v.id, Version: v.version(), Found: found}
	if found {
		result.ID = id
		result.Value = r.Value
	}
	return result, nil
} // #nosec G115 -- payload bytes derive from a <=1MiB catalog record.
// ValueIdentity uses exact typed equality, never the hash or an alias ID alone.
func (v *ReadView) ValueIdentity(ctx context.Context, value graphstate.Scalar) (graphstate.ValueRead, error) {
	if v == nil {
		return graphstate.ValueRead{}, ErrInvalid
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	q, err := v.begin(ctx, graphstate.ReadBudget{})
	if err != nil {
		return graphstate.ValueRead{}, err
	}
	key, err := q.equalityKey(value)
	var entry ValueEntry
	found := false
	if err == nil {
		entry, found, err = q.q.local(key)
	}
	cost := fullValueOutputBytes
	if err == nil && found {
		variable, e := scalarVariableOwned(entry.Value, int(entry.PayloadBytes), v.c.limits)
		err = e
		cost += variable
	}
	if err = v.finish(q, err); err != nil {
		return graphstate.ValueRead{}, err
	}
	if err := v.output(cost); err != nil {
		return graphstate.ValueRead{}, err
	}
	result := graphstate.ValueRead{View: v.id, Version: v.version(), Found: found}
	if found {
		result.ID = entry.Ref.ID
		result.Value = entry.Value
	}
	return result, nil
} // #nosec G115 -- payload bytes derive from a <=1MiB catalog record.
func (q *pageReader) equalityKey(value graphstate.Scalar) (string, error) {
	available := min(q.q.c.limits.MaxReadBytes, q.limits.MaxWorkBytes)
	if q.q.maxBytes > 0 {
		available = min(available, q.q.maxBytes)
	}
	available -= q.q.bytes
	if available < 2 {
		return "", ErrResourceLimit
	}
	l := q.q.c.limits.valueLimits()
	l.MaxReadBytes = min(l.MaxReadBytes, available/2)
	// Scope encoding is bounded before allocating; exact string/numeric sizes are
	// checked by Scalar.EqualityKey before append and the retained key is charged.
	if value.Kind() == graphstate.ScalarScope {
		l.Component.Temporal.MaxValueBytes = min(l.Component.Temporal.MaxValueBytes, l.MaxReadBytes-1)
		if l.Component.Temporal.MaxValueBytes < 1 {
			return "", ErrResourceLimit
		}
	}
	key, err := value.EqualityKey(l)
	if err != nil {
		return "", callerError(err)
	}
	if err := q.q.materialize(2 * len(key)); err != nil {
		return "", err
	}
	return key, nil
}

// ComponentPage uses the same identity and shared source/cursor/output caps.
func (v *ReadView) ComponentPage(ctx context.Context, query graphstate.ComponentQuery, token graphstate.Cursor, budget graphstate.ReadBudget) (graphstate.ComponentPage, error) {
	if v == nil {
		return graphstate.ComponentPage{}, ErrInvalid
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if err := v.check(ctx); err != nil {
		return graphstate.ComponentPage{}, err
	}
	if !validComponent(query.Key, v.c.limits) || budget.Rows < 1 || budget.Bytes < 1 || query.Window.Kind() == temporal.ScopeEmpty || query.Window.Kind() == temporal.ScopeUnplaced {
		return graphstate.ComponentPage{}, ErrInvalid
	}
	rows, available := v.remaining()
	available = min(available, budget.Bytes, v.limits.Pages.MaxWorkBytes, v.c.limits.MaxReadBytes)
	if rows < 1 || available < v.c.rootImageBytes {
		return graphstate.ComponentPage{}, ErrResourceLimit
	}
	l := v.c.limits.Temporal
	l.MaxValueBytes = min(l.MaxValueBytes, max(1, (available-v.c.rootImageBytes-128-2*len(query.Key.Name))/2))
	fingerprint, err := temporal.AppendScope(nil, query.Window, l)
	if err != nil {
		return graphstate.ComponentPage{}, callerError(err)
	}
	scratch := 2*cap(fingerprint) + 128 + 2*len(query.Key.Name)
	if scratch > available-v.c.rootImageBytes {
		return graphstate.ComponentPage{}, ErrResourceLimit
	}
	v.work.Bytes += scratch
	rows, bytes := v.remaining()
	rows = min(rows, budget.Rows)
	bytes = min(bytes, budget.Bytes)
	original := v.pages.limits
	defer func() { v.pages.limits = original }()
	v.pages.limits.MaxWorkRecords = min(original.MaxWorkRecords, rows)
	v.pages.limits.MaxWorkBytes = min(original.MaxWorkBytes, bytes)
	v.pages.limits.MaxCursors = original.MaxCursors - len(v.cursors)
	v.pages.limits.MaxCursorBytes = original.MaxCursorBytes - v.cursorBytes
	d := query.Window.Axis().Descriptor()
	header := fullComponentOutputBytes + len(d.Reference) + len(d.CanonicalUnit)
	outputLimit := min(budget.Bytes, v.limits.MaxOutputBytes-v.outputBytes)
	if outputLimit <= header {
		return graphstate.ComponentPage{}, ErrResourceLimit
	}
	budget.Bytes = outputLimit
	old, hadOld := v.pages.cursors[token]
	result, err := v.pages.ComponentPage(ctx, query, token, budget)
	v.work = addWork(v.work, v.pages.LastWork())
	if err != nil {
		return graphstate.ComponentPage{}, err
	}
	rollback := func() {
		if result.Next != 0 {
			current := v.pages.cursors[result.Next]
			delete(v.pages.cursors, result.Next)
			v.pages.cursorBytes -= current.bytes
		}
		if hadOld {
			v.pages.cursors[token] = old
			v.pages.cursorBytes += old.bytes
		}
	}
	wire, e := temporal.AppendScope(nil, result.Owned, v.c.limits.Temporal)
	if e != nil {
		rollback()
		return graphstate.ComponentPage{}, v.c.failure(e)
	}
	cost, e := componentOwnedBytes(result.Data, result.Owned, wire)
	if e != nil {
		rollback()
		return graphstate.ComponentPage{}, v.c.failure(e)
	}
	if err := ctx.Err(); err != nil {
		rollback()
		return graphstate.ComponentPage{}, err
	}
	if err := v.output(cost); err != nil {
		rollback()
		return graphstate.ComponentPage{}, err
	}
	return result, nil
}
func (v *ReadView) storeCursor(token graphstate.Cursor, old fullContinuation, next fullContinuation) (graphstate.Cursor, error) {
	count := len(v.cursors) + len(v.pages.cursors)
	if token != 0 {
		count--
	}
	total := v.cursorBytes + v.pages.cursorBytes - old.bytes
	if count >= v.limits.Pages.MaxCursors || next.bytes > v.limits.Pages.MaxCursorBytes-total {
		return 0, ErrResourceLimit
	}
	return allocatePageCursor()
}
func (v *ReadView) advanceCursor(token, next graphstate.Cursor, old, value fullContinuation) {
	if token != 0 {
		delete(v.cursors, token)
		v.cursorBytes -= old.bytes
	}
	if next != 0 {
		value.key.Name = strings.Clone(value.key.Name)
		value.posting.name = strings.Clone(value.posting.name)
		value.posting.component.Name = strings.Clone(value.posting.component.Name)
		v.cursors[next] = value
		v.cursorBytes += value.bytes
	}
}
func (v *ReadView) oldCursor(token graphstate.Cursor, kind recordKind, hash [32]byte) (fullContinuation, error) {
	if token == 0 {
		return fullContinuation{}, nil
	}
	old, found := v.cursors[token]
	if !found || old.kind != kind || old.query != hash {
		return fullContinuation{}, ErrInvalid
	}
	return old, nil
}

// ComponentKeys seeks the exact longest contiguous prefix in its retained tree.
func (v *ReadView) ComponentKeys(ctx context.Context, predicate graphstate.KeyPredicate, token graphstate.Cursor, budget graphstate.ReadBudget) (graphstate.KeyPage, error) {
	if v == nil {
		return graphstate.KeyPage{}, ErrInvalid
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if predicate.Owner == 0 || predicate.Kind > graphstate.SetMember || predicate.Name != "" && !validName(predicate.Name, v.c.limits) || budget.Rows < 1 || budget.Bytes < 1 {
		return graphstate.KeyPage{}, ErrInvalid
	}
	rows, bytes := v.remaining()
	_ = rows
	bytes = min(bytes, budget.Bytes, v.limits.Pages.MaxWorkBytes, v.c.limits.MaxReadBytes)
	if err := v.check(ctx); err != nil {
		return graphstate.KeyPage{}, err
	}
	scratch := 45 + len(predicate.Name)
	if scratch+256+v.c.limits.MaxNameBytes > bytes-v.c.rootImageBytes {
		return graphstate.KeyPage{}, ErrResourceLimit
	}
	data := make([]byte, 0, scratch)
	data = appendComponent(append(data, v.id[:]...), graphstate.ComponentKey{Owner: predicate.Owner, Life: predicate.Life, Kind: predicate.Kind, Name: predicate.Name})
	hash := sha256.Sum256(append([]byte("full:keys:v1\x00"), data...))
	old, err := v.oldCursor(token, componentKeyTreeRecord, hash)
	if err != nil {
		return graphstate.KeyPage{}, err
	}
	q, err := v.begin(ctx, budget)
	if err != nil {
		return graphstate.KeyPage{}, err
	}
	if err := q.q.materialize(scratch + 256 + v.c.limits.MaxNameBytes); err != nil {
		return graphstate.KeyPage{}, v.finish(q, err)
	}
	root, err := q.componentKeyTreeRoot(v.descriptor.keys, keyTreeLimits(q.limits))
	if err != nil {
		return graphstate.KeyPage{}, v.finish(q, err)
	}
	after := keyPrefixStart(predicate)
	inclusive := true
	if token != 0 {
		after = old.key
		inclusive = false
	}
	out := graphstate.KeyPage{View: v.id, Version: v.version()}
	last := graphstate.ComponentKey{}
	visited := 0
	output := 128
	out.Complete, err = q.walkComponentKeys(root, predicate, after, inclusive, keyTreeLimits(q.limits), func(key graphstate.ComponentKey) (bool, error) {
		q.member = &key
		m, found, err := q.readMeta(key)
		if err != nil {
			return false, err
		}
		if !found {
			return false, ErrCorrupt
		}
		d, err := q.directory(m.Root, key, m.Axis)
		if err != nil {
			return false, err
		}
		if d.Owned.Kind() != temporal.ScopeAll {
			return false, ErrCorrupt
		}
		if keyPredicateMatches(key, predicate) {
			limit := min(budget.Bytes, v.limits.MaxOutputBytes-v.outputBytes)
			capacity := cap(out.Keys)
			if len(out.Keys) == capacity {
				capacity = min(max(1, 2*capacity), budget.Rows, max(0, (limit-output-len(key.Name))/64))
			}
			cost := 64*(capacity-cap(out.Keys)) + len(key.Name)
			if capacity < len(out.Keys)+1 || cost > limit-output {
				return false, ErrResourceLimit
			}
			if err := q.q.materialize(cost); err != nil {
				return false, err
			}
			if capacity > cap(out.Keys) {
				keys := make([]graphstate.ComponentKey, len(out.Keys), capacity)
				copy(keys, out.Keys)
				out.Keys = keys
			}
			key.Name = strings.Clone(key.Name)
			out.Keys = append(out.Keys, key)
			output += cost
		}
		last = key
		visited++
		return true, q.budget()
	})
	if err != nil {
		if !errors.Is(err, ErrResourceLimit) || visited == 0 {
			return graphstate.KeyPage{}, v.finish(q, err)
		}
		out.Complete = false
	}
	next := fullContinuation{}
	if !out.Complete {
		next = fullContinuation{kind: componentKeyTreeRecord, query: hash, key: last, bytes: 256 + len(last.Name)}
		out.Next, err = v.storeCursor(token, old, next)
		if err != nil {
			return graphstate.KeyPage{}, v.finish(q, err)
		}
	}
	if err := v.finish(q, nil); err != nil {
		return graphstate.KeyPage{}, err
	}
	if err := v.output(output); err != nil {
		return graphstate.KeyPage{}, err
	}
	v.advanceCursor(token, out.Next, old, next)
	return out, nil
}

// Project runs the semantic projector against one owned complete reader and
// closes its cursor/handle accounting on every path, leaving the borrow open.
func Project(ctx context.Context, c *Catalog, id graphstate.EntityID, at temporal.Position, visibility graphstate.Visibility, l GraphLimits) (graphstate.Projection, PageWork, error) {
	v, err := OpenReadView(ctx, c, l)
	if err != nil {
		return graphstate.Projection{}, PageWork{}, err
	}
	defer func() { _ = v.Close() }()
	result, err := graphstate.Project(ctx, v, id, at, visibility, l.Planner)
	return result, v.Work(), err
}
