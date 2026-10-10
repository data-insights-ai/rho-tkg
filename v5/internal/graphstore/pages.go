package graphstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync/atomic"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

var pageCursorSequence atomic.Uint64

func allocatePageCursor() (graphstate.Cursor, error) {
	for {
		old := pageCursorSequence.Load()
		if old == math.MaxUint64 {
			return 0, ErrResourceLimit
		}
		if pageCursorSequence.CompareAndSwap(old, old+1) {
			return graphstate.Cursor(old + 1), nil
		}
	}
}

type pageReader struct {
	q          *reader
	limits     PageLimits
	work       PageWork
	allocation *Root
	binding    *storedComponentBinding
	member     *graphstate.ComponentKey // operation-local proof from a checked key-tree path
}

// Binding/reference reuse is operation-local and charged before retention.
// Catalog identities are immutable, including in the private staging overlay.
type storedComponentBinding struct {
	key        graphstate.ComponentKey
	axis       temporal.Axis
	definition graphstate.PropertyDefinition
	references map[state.ValueRef]bool
}

func storedBindingError(err error) error {
	if errors.Is(err, ErrInvalid) {
		// Do not retain the caller-input sentinel: Catalog.failure deliberately
		// leaves ErrInvalid unpoisoned, whereas stored semantic violations stop it.
		return fmt.Errorf("%w: persisted component binding: %v", ErrCorrupt, err)
	}
	return err
}

func (q *pageReader) storedBinding(k graphstate.ComponentKey, axis temporal.Axis) (graphstate.PropertyDefinition, error) {
	if q.binding != nil && q.binding.key == k && q.binding.axis == axis {
		return q.binding.definition, nil
	}
	definition, err := q.validateKey(k, axis)
	if err != nil {
		return graphstate.PropertyDefinition{}, storedBindingError(err)
	}
	if err := q.q.materialize(128 + len(k.Name)); err != nil {
		return graphstate.PropertyDefinition{}, err
	}
	if err := q.budget(); err != nil {
		return graphstate.PropertyDefinition{}, err
	}
	q.binding = &storedComponentBinding{key: k, axis: axis, definition: definition}
	return definition, nil
}

func (q *pageReader) storedCell(k graphstate.ComponentKey, axis temporal.Axis, cell state.Cell) error {
	definition, err := q.storedBinding(k, axis)
	if err != nil {
		return err
	}
	if cell.Present() && q.binding.references[cell.Value()] {
		return nil
	}
	if err := q.validateCell(k, definition, cell); err != nil {
		return storedBindingError(err)
	}
	if err := q.checkRawCell(k, cell); err != nil {
		return err
	}
	if cell.Present() && !cell.Value().IsNull() {
		if len(q.binding.references) >= q.limits.MaxWorkRecords {
			return ErrResourceLimit
		}
		if err := q.q.materialize(64); err != nil {
			return err
		}
		if err := q.budget(); err != nil {
			return err
		}
		if q.binding.references == nil {
			q.binding.references = make(map[state.ValueRef]bool)
		}
		q.binding.references[cell.Value()] = true
	}
	return nil
}

// Addressability is independent of whether a patch asserts a value. Immutable
// node/relationship targets, schema and set members also govern no-op/unset.
func (q *pageReader) validateKey(k graphstate.ComponentKey, axis temporal.Axis) (graphstate.PropertyDefinition, error) {
	if !validComponent(k, q.q.c.limits) {
		return graphstate.PropertyDefinition{}, ErrInvalid
	}
	entity, found, err := q.q.entity(EntityRef{q.q.c.root.namespace.Graph, k.Owner})
	if err != nil {
		return graphstate.PropertyDefinition{}, err
	}
	if !found || entity.Axis.Descriptor() != axis.Descriptor() || entity.Axis.DefinitionHash() != axis.DefinitionHash() {
		return graphstate.PropertyDefinition{}, ErrInvalid
	}
	if k.Kind == graphstate.Label && entity.Kind != graphstate.Node {
		return graphstate.PropertyDefinition{}, errors.Join(ErrInvalid, graphstate.ErrTypeMismatch)
	}
	if k.Kind != graphstate.Presence {
		_, found, err := q.q.life(LifeRef{q.q.c.root.namespace.Graph, k.Owner, k.Life})
		if err != nil {
			return graphstate.PropertyDefinition{}, err
		}
		if !found {
			return graphstate.PropertyDefinition{}, ErrInvalid
		}
	}
	var definition graphstate.PropertyDefinition
	if k.Kind == graphstate.ScalarProperty || k.Kind == graphstate.SetMember {
		definition, found, err = q.q.property(entity.Kind, k.Name)
		if err != nil {
			return graphstate.PropertyDefinition{}, err
		}
		cardinality := graphstate.ScalarCardinality
		if k.Kind == graphstate.SetMember {
			cardinality = graphstate.SetCardinality
		}
		if !found || definition.Cardinality != cardinality {
			return graphstate.PropertyDefinition{}, errors.Join(ErrInvalid, graphstate.ErrTypeMismatch)
		}
		if k.Kind == graphstate.SetMember {
			entry, _, found, err := q.q.value(ValueRef{q.q.c.root.namespace.Graph, k.Member})
			if err != nil {
				return graphstate.PropertyDefinition{}, err
			}
			if !found || entry.Value.Kind() != definition.Type {
				return graphstate.PropertyDefinition{}, errors.Join(ErrInvalid, graphstate.ErrTypeMismatch)
			}
		}
	}
	return definition, q.budget()
}
func (q *pageReader) validateCell(k graphstate.ComponentKey, definition graphstate.PropertyDefinition, cell state.Cell) error {
	if !cell.Present() {
		return nil
	}
	value := cell.Value()
	switch k.Kind {
	case graphstate.Presence:
		if value.IsNull() || value.PayloadBytes() != 0 {
			return ErrInvalid
		}
		_, found, err := q.q.life(LifeRef{q.q.c.root.namespace.Graph, k.Owner, graphstate.LifeID(value.ID())})
		if err != nil {
			return err
		}
		if !found {
			return ErrInvalid
		}
	case graphstate.Label, graphstate.SetMember:
		if !value.IsNull() {
			return ErrInvalid
		}
	case graphstate.ScalarProperty:
		if value.IsNull() {
			return nil
		}
		entry, _, found, err := q.q.value(ValueRef{q.q.c.root.namespace.Graph, graphstate.ValueID(value.ID())})
		if err != nil {
			return err
		}
		if !found || entry.Value.Kind() != definition.Type || entry.PayloadBytes != value.PayloadBytes() {
			return ErrInvalid
		}
	}
	return q.budget()
}
func (q *pageReader) validPhysical(id uint64) bool {
	next := q.q.c.root.next
	if q.allocation != nil {
		next = q.allocation.next
	}
	return id > 0 && id < next
}
func (q *pageReader) budget() error {
	q.work.Records, q.work.Bytes = q.q.rows, q.q.bytes
	if q.work.Records > q.limits.MaxWorkRecords || q.work.Bytes > q.limits.MaxWorkBytes {
		return ErrResourceLimit
	}
	return nil
}
func (q *pageReader) get(key []byte) ([]byte, bool, error) {
	if q.q.rows >= q.limits.MaxWorkRecords {
		return nil, false, ErrResourceLimit
	}
	b, found, err := q.q.get(key)
	if err != nil {
		return nil, false, err
	}
	if err := q.budget(); err != nil {
		return nil, false, err
	}
	return b, found, nil
}
func sameScope(a, b temporal.Scope, l temporal.Limits) (bool, error) {
	x, err := a.Difference(b, l)
	if err != nil {
		return false, err
	}
	if x.Kind() != temporal.ScopeEmpty {
		return false, nil
	}
	x, err = b.Difference(a, l)
	return x.Kind() == temporal.ScopeEmpty, err
}
func within(a, b temporal.Scope, l temporal.Limits) (bool, error) {
	x, err := a.Difference(b, l)
	return x.Kind() == temporal.ScopeEmpty, err
}
func boundOrder(a, b temporal.Bound, l temporal.Limits) (temporal.Ordering, error) {
	if a.Kind() != b.Kind() {
		if a.Kind() < b.Kind() {
			return temporal.Less, nil
		}
		return temporal.Greater, nil
	}
	if a.Kind() != temporal.BoundFinite {
		return temporal.Equal, nil
	}
	x, _ := a.Position()
	y, _ := b.Position()
	return temporal.ComparePositions(x, y, l)
}
func validateChildren(d directoryPage, l temporal.Limits) error {
	coverage, err := temporal.Empty(d.Owned.Axis())
	if err != nil {
		return ErrCorrupt
	}
	ids := make(map[uint64]bool, len(d.Children))
	for i, child := range d.Children {
		if child.ID == 0 || child.ID == d.ID || ids[child.ID] || !atomOwned(child.Owned) {
			return ErrCorrupt
		}
		ids[child.ID] = true
		if i > 0 {
			a, _, _ := d.Children[i-1].Owned.Bounds()
			b, _, _ := child.Owned.Bounds()
			order, err := boundOrder(a, b, l)
			if err != nil || order == temporal.Greater || order == temporal.Equal && (!a.Inclusive() || b.Inclusive()) {
				return ErrCorrupt
			}
		}
		overlaps, err := coverage.Overlaps(child.Owned, l)
		if err != nil || overlaps {
			return ErrCorrupt
		}
		coverage, err = coverage.Union(child.Owned, l)
		if err != nil {
			return errors.Join(ErrCorrupt, err)
		}
	}
	same, err := sameScope(coverage, d.Owned, l)
	if err != nil || !same {
		return ErrCorrupt
	}
	return nil
}
func applyPageCell(s state.State, scope temporal.Scope, cell state.Cell, l state.Limits) (state.Result, error) {
	if cell.Present() {
		return s.Set(scope, cell.Value(), cell.Revision(), l)
	}
	return s.Unset(scope, cell.Revision(), l)
}
func verifyBefore(s state.State, scope temporal.Scope, want state.Cell, l state.Limits) error {
	old, err := s.Slice(scope, l)
	if err != nil {
		return err
	}
	pieces := old.Pieces()
	if want == (state.Cell{}) {
		if len(pieces) == 0 {
			return nil
		}
		return ErrPatchConflict
	}
	if len(pieces) != 1 || pieces[0].Cell() != want {
		return ErrPatchConflict
	}
	same, err := sameScope(pieces[0].Scope(), scope, l.Temporal)
	if err != nil {
		return err
	}
	if !same {
		return ErrPatchConflict
	}
	return nil
}
func (q *pageReader) materialize(d directoryPage) (state.State, error) {
	s, err := q.checkpoint(d.Base, d.Key, d.Owned.Axis())
	if err != nil {
		return state.State{}, err
	}
	tail := make([]patchPage, 0, d.TailRecords)
	id, tailBytes, atoms := d.Head, 0, 0
	for range d.TailRecords {
		if id == 0 {
			return state.State{}, ErrCorrupt
		}
		p, n, err := q.patch(id, d.Key, d.Owned.Axis())
		if err != nil {
			return state.State{}, err
		}
		same, err := sameScope(p.Owned, d.Owned, q.q.c.limits.Temporal)
		if err != nil || !same {
			return state.State{}, ErrCorrupt
		}
		tailBytes += n
		atoms += len(p.Changes)
		if tailBytes > d.TailBytes || atoms > d.TailAtoms {
			return state.State{}, ErrCorrupt
		}
		tail = append(tail, p)
		id = p.Previous
	}
	if id != 0 || tailBytes != d.TailBytes || atoms != d.TailAtoms {
		return state.State{}, ErrCorrupt
	}
	for i := len(tail) - 1; i >= 0; i-- {
		for _, change := range tail[i].Changes {
			ok, err := within(change.Scope(), d.Owned, q.q.c.limits.Temporal)
			if err != nil || !ok {
				return state.State{}, ErrCorrupt
			}
			if err := verifyBefore(s, change.Scope(), change.Before(), q.limits.stateLimits(q.q.c)); err != nil {
				return state.State{}, pageFailure(q.q.c, err, true)
			}
			result, err := applyPageCell(s, change.Scope(), change.After(), q.limits.stateLimits(q.q.c))
			if err != nil {
				return state.State{}, pageFailure(q.q.c, err, true)
			}
			s = result.State()
			if err := q.q.materialize(s.Usage().MetadataBytes()); err != nil {
				return state.State{}, err
			}
			if err := q.budget(); err != nil {
				return state.State{}, err
			}
			q.work.DecodedCells++
		}
	}
	if s.Usage().Pieces() != d.Cells {
		return state.State{}, ErrCorrupt
	}
	for _, p := range s.Pieces() {
		ok, err := within(p.Scope(), d.Owned, q.q.c.limits.Temporal)
		if err != nil || !ok {
			return state.State{}, ErrCorrupt
		}
	}
	if err := q.q.materialize(s.Usage().MetadataBytes() + tailBytes); err != nil {
		return state.State{}, err
	}
	if err := q.budget(); err != nil {
		return state.State{}, err
	}
	return s, nil
}
func (q *pageReader) findLeaf(m componentMeta, w temporal.Scope) (directoryPage, error) {
	d, err := q.directory(m.Root, m.Key, m.Axis)
	if err != nil {
		return directoryPage{}, err
	}
	if d.Owned.Kind() != temporal.ScopeAll {
		return directoryPage{}, ErrCorrupt
	}
	for depth := 0; d.Level > 0; depth++ {
		if depth >= q.limits.MaxLevels {
			return directoryPage{}, ErrCorrupt
		}
		found := false
		for _, child := range d.Children {
			overlap, err := child.Owned.Overlaps(w, q.q.c.limits.Temporal)
			if err != nil {
				return directoryPage{}, err
			}
			if !overlap {
				continue
			}
			next, err := q.directory(child.ID, m.Key, m.Axis)
			if err != nil {
				return directoryPage{}, err
			}
			same, err := sameScope(next.Owned, child.Owned, q.q.c.limits.Temporal)
			if err != nil || !same || next.Level != d.Level-1 {
				return directoryPage{}, ErrCorrupt
			}
			d = next
			found = true
			break
		}
		if !found {
			return directoryPage{}, ErrCorrupt
		}
	}
	return d, nil
}

// NewPageReader creates a bounded cursor owner over a borrowed immutable view.
// It is a local paging capability, not a distributed certified cut or lease.
// Initialization copies only the fixed supported root image (at most 172 bytes) and
// bounded fixed identity-hash scratch, separately from operation LastWork.
func NewPageReader(c *Catalog, l PageLimits) (*PageReader, error) {
	if c == nil {
		return nil, ErrInvalid
	}
	l, err := l.resolve()
	if err != nil {
		return nil, err
	}
	if err := c.check(context.Background()); err != nil {
		return nil, err
	}
	if c.root.hasOwnershipDeclaration() {
		return nil, ErrTopologyUnsupported
	}
	root, err := c.view.Root()
	if err != nil {
		return nil, err
	}
	id := pageViewIdentity(c.root.namespace, root)
	return &PageReader{c: c, limits: l, id: id, index: root.Index, cursors: make(map[graphstate.Cursor]continuation)}, nil
}

func pageViewIdentity(n Namespace, root raftlog.ApplicationRoot) graphstate.ViewID {
	b := keyPrefix(n, componentRecord)
	b = binary.BigEndian.AppendUint64(b, root.Generation)
	b = binary.BigEndian.AppendUint64(b, root.Index)
	b = append(b, root.ImageHash[:]...)
	hash := sha256.Sum256(b)
	var id graphstate.ViewID
	copy(id[:], hash[:16])
	return id
}

// Identity returns this local immutable-view identity; it does not certify a cut.
func (p *PageReader) Identity() graphstate.ViewID {
	if p == nil {
		return graphstate.ViewID{}
	}
	return p.id
}

// LastWork returns the preceding operation's conservative read/decode ledger.
func (p *PageReader) LastWork() PageWork {
	if p == nil {
		return PageWork{}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.last
}

// Close releases continuations and waits for an active read. Borrowed Catalog
// and ApplicationView remain owned by the caller.
func (p *PageReader) Close() error {
	if p == nil {
		return ErrInvalid
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	p.cursors = nil
	p.cursorBytes = 0
	return nil
}
func (p *PageReader) queryHash(query graphstate.ComponentQuery) ([32]byte, error) {
	b := appendComponent(append([]byte("rho-component-query:v1\x00"), p.id[:]...), query.Key)
	var err error
	b, err = appendScopeField(b, query.Window, p.c.limits.Temporal)
	if err != nil {
		return [32]byte{}, callerError(err)
	}
	if query.MergeContext {
		b = append(b, 1)
	} else {
		b = append(b, 0)
	}
	return sha256.Sum256(b), nil
}
func (q *pageReader) fitPage(s state.State, w temporal.Scope, budget graphstate.ReadBudget) (state.State, temporal.Scope, error) {
	fit := func(w temporal.Scope) (state.State, bool, error) {
		out, err := s.Slice(w, q.limits.stateLimits(q.q.c))
		if err != nil {
			return state.State{}, false, err
		}
		wire, err := temporal.AppendScope(nil, w, q.q.c.limits.Temporal)
		if err != nil {
			return state.State{}, false, err
		}
		bytes := out.Usage().MetadataBytes() + len(wire)
		if q.q.fullView != nil {
			bytes, err = componentOwnedBytes(out, w, wire)
			if err != nil {
				return state.State{}, false, err
			}
		}
		return out, out.Usage().Pieces()+1 <= budget.Rows && bytes <= budget.Bytes, nil
	}
	out, ok, err := fit(w)
	if err != nil {
		return state.State{}, temporal.Scope{}, err
	}
	if ok {
		return out, w, nil
	}
	clipped, err := s.Slice(w, q.limits.stateLimits(q.q.c))
	if err != nil {
		return state.State{}, temporal.Scope{}, err
	}
	pieces := clipped.Pieces()
	lo, _, _ := w.Bounds()
	for i := min(len(pieces)-1, budget.Rows-1); i >= 0; i-- {
		lower, _, _ := pieces[i].Scope().Bounds()
		pos, finite := lower.Position()
		if !finite {
			continue
		}
		hi, err := temporal.FiniteBound(pos, !lower.Inclusive())
		if err != nil {
			return state.State{}, temporal.Scope{}, err
		}
		prefix, err := temporal.Span(w.Axis(), lo, hi, q.q.c.limits.Temporal)
		if err != nil {
			return state.State{}, temporal.Scope{}, err
		}
		if prefix.Kind() == temporal.ScopeEmpty {
			continue
		}
		out, ok, err = fit(prefix)
		if err != nil {
			return state.State{}, temporal.Scope{}, err
		}
		if ok {
			return out, prefix, nil
		}
	}
	return state.State{}, temporal.Scope{}, ErrResourceLimit
}

// ComponentPage returns complete owned coverage, including never-asserted gaps.
// A page is one bounded leaf/window atom. Optional merge context is empty;
// context is never copied into writes. Errors return no partial output.
func (p *PageReader) ComponentPage(ctx context.Context, query graphstate.ComponentQuery, token graphstate.Cursor, budget graphstate.ReadBudget) (graphstate.ComponentPage, error) {
	if p == nil {
		return graphstate.ComponentPage{}, ErrInvalid
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.last = PageWork{}
	if p.closed {
		return graphstate.ComponentPage{}, ErrClosed
	}
	if !validComponent(query.Key, p.c.limits) || budget.Rows < 1 || budget.Bytes < 1 || query.Window.Kind() == temporal.ScopeEmpty || query.Window.Kind() == temporal.ScopeUnplaced {
		return graphstate.ComponentPage{}, ErrInvalid
	}
	hash, err := p.queryHash(query)
	if err != nil {
		return graphstate.ComponentPage{}, err
	}
	remaining := query.Window
	old := continuation{}
	if token != 0 {
		var found bool
		old, found = p.cursors[token]
		if !found || old.query != hash {
			return graphstate.ComponentPage{}, ErrInvalid
		}
		remaining = old.remaining
	}
	base, err := p.c.reader(ctx)
	if err != nil {
		return graphstate.ComponentPage{}, err
	}
	base.maxRows, base.maxBytes = p.limits.MaxWorkRecords, p.limits.MaxWorkBytes
	base.fullView = p.complete
	q := pageReader{q: base, limits: p.limits}
	defer func() { _ = q.budget(); p.last = q.work }()
	m, found, err := q.readMeta(query.Key)
	if err != nil {
		return graphstate.ComponentPage{}, p.c.failure(err)
	}
	var s state.State
	wanted := remaining.Parts()[0]
	if found {
		if m.Axis.Descriptor() != query.Window.Axis().Descriptor() || m.Axis.DefinitionHash() != query.Window.Axis().DefinitionHash() {
			return graphstate.ComponentPage{}, errors.Join(ErrInvalid, temporal.ErrAxisMismatch)
		}
		leaf, err := q.findLeaf(m, wanted)
		if err != nil {
			return graphstate.ComponentPage{}, p.c.failure(err)
		}
		wanted, err = wanted.Intersection(leaf.Owned, p.c.limits.Temporal)
		if err != nil {
			return graphstate.ComponentPage{}, p.c.failure(err)
		}
		s, err = q.materialize(leaf)
		if err != nil {
			return graphstate.ComponentPage{}, p.c.failure(err)
		}
	} else {
		owner, exists, e := q.q.entity(EntityRef{p.c.root.namespace.Graph, query.Key.Owner})
		if e != nil {
			return graphstate.ComponentPage{}, p.c.failure(e)
		}
		if e := q.budget(); e != nil {
			return graphstate.ComponentPage{}, e
		}
		if exists && (owner.Axis.Descriptor() != query.Window.Axis().Descriptor() || owner.Axis.DefinitionHash() != query.Window.Axis().DefinitionHash()) {
			return graphstate.ComponentPage{}, errors.Join(ErrInvalid, temporal.ErrAxisMismatch)
		}
		s, err = state.New(query.Window.Axis(), p.limits.stateLimits(p.c))
		if err != nil {
			return graphstate.ComponentPage{}, callerError(err)
		}
	}
	data, owned, err := q.fitPage(s, wanted, budget)
	if err != nil {
		return graphstate.ComponentPage{}, pageFailure(p.c, err, false)
	}
	if p.complete != nil {
		data, owned, err = q.exactComponentOutput(data, owned, budget)
		if err != nil {
			return graphstate.ComponentPage{}, pageFailure(p.c, err, false)
		}
	}
	rest, err := remaining.Difference(owned, p.c.limits.Temporal)
	if err != nil {
		return graphstate.ComponentPage{}, pageFailure(p.c, err, false)
	}
	if err := q.budget(); err != nil {
		return graphstate.ComponentPage{}, err
	}
	complete := rest.Kind() == temporal.ScopeEmpty
	next := graphstate.Cursor(0)
	cost := 0
	if !complete {
		wire, err := temporal.AppendScope(nil, rest, p.c.limits.Temporal)
		if err != nil {
			return graphstate.ComponentPage{}, callerError(err)
		}
		d := rest.Axis().Descriptor()
		cost = len(wire) + 27 + len(d.Reference) + len(d.CanonicalUnit) + 64
		if p.complete != nil {
			backing, e := scopeOwnedBacking(wire)
			if e != nil {
				return graphstate.ComponentPage{}, p.c.failure(e)
			}
			cost = 256 + backing + len(d.Reference) + len(d.CanonicalUnit)
			if err := q.q.materialize(cost + 2*cap(wire)); err != nil {
				return graphstate.ComponentPage{}, err
			}
			rest, e = temporal.DecodeScope(wire, rest.Axis(), p.c.limits.Temporal)
			if e != nil {
				return graphstate.ComponentPage{}, callerError(e)
			}
		}
		count := len(p.cursors)
		if token != 0 {
			count--
		}
		if count >= p.limits.MaxCursors || cost > p.limits.MaxCursorBytes-(p.cursorBytes-old.bytes) {
			return graphstate.ComponentPage{}, ErrResourceLimit
		}
		next, err = allocatePageCursor()
		if err != nil {
			return graphstate.ComponentPage{}, err
		}
	}
	if token != 0 {
		delete(p.cursors, token)
		p.cursorBytes -= old.bytes
	}
	if next != 0 {
		p.cursors[next] = continuation{hash, rest, cost}
		p.cursorBytes += cost
	}
	return graphstate.ComponentPage{View: p.id, Version: graphstate.ReadVersion(p.index), Owned: owned, Data: data, Next: next, Complete: complete}, nil
}
func pageFailure(c *Catalog, err error, corrupt bool) error {
	if errors.Is(err, state.ErrResourceLimit) || errors.Is(err, temporal.ErrResourceLimit) || errors.Is(err, ErrResourceLimit) {
		return errors.Join(ErrResourceLimit, err)
	}
	if corrupt {
		return c.failure(errors.Join(ErrCorrupt, fmt.Errorf("persisted page replay: %v", err)))
	}
	if errors.Is(err, ErrPatchConflict) {
		return errors.Join(ErrInvalid, err)
	}
	return c.failure(callerError(err))
}
func equalState(a, b state.State, c *Catalog, l PageLimits) (bool, error) {
	x, err := state.AppendState(nil, a, l.codecLimits(c))
	if err != nil {
		return false, err
	}
	y, err := state.AppendState(nil, b, l.codecLimits(c))
	return bytes.Equal(x, y), err
}
