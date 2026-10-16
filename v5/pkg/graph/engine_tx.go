package graph

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

// engineTx collects typed engine operations; the commit plans and installs them
// as one batch. Tx methods validate the op like CheckOp and the schema, and the
// first failure is returned at the call (and again by Update if fn swallows it).
type engineTx struct {
	e       *pebbleEngine
	ctx     context.Context
	base    uint64
	next    uint64
	ops     []graphstate.Operation
	alloc   allocState
	vals    uint64
	decls   []graphstate.PropertyDefinition
	done    bool
	err     error
	rec     temporal.Position
	ceiling temporal.Position
	claims  map[string]types.NodeID
	created map[types.NodeID]bool
	ends    map[endKey]temporal.Position       // earliest End per life, set in this Update
	fresh   map[graphstate.EntityID]bool       // created in this Update: no committed state
	spans   map[graphstate.EntityID][]lifeSpan // lives created or reopened in this Update
	lives   map[graphstate.EntityID]committedLives
	meta    map[string][]byte
	// reopened: this Update starts a second life of some entity.
	reopened bool
}

type endKey struct {
	id   graphstate.EntityID
	life graphstate.LifeID
}

type committedLives struct {
	spans []lifeSpan
	max   graphstate.LifeID
}

var _ Tx = (*engineTx)(nil)

func (t *engineTx) fail(err error) error {
	if t.err == nil {
		t.err = err
	}
	return err
}

func (t *engineTx) pre(op Op) error {
	if t.done {
		return ErrTxDone
	}
	if err := CheckOp(t.e.cfg, op); err != nil {
		return t.fail(err)
	}
	t.rec = op.recorded()
	if !isZeroPosition(t.rec) {
		// The engine has no clock: the commit's own knowledge instant is the ceiling.
		if o, err := temporal.ComparePositions(t.rec, t.ceil(), t.e.cfg.Limits); err != nil || o == temporal.Greater {
			return t.fail(fmt.Errorf("%w: Recorded is after the commit", ErrInvalidScope))
		}
	}
	return nil
}

// claimID is the entity ID of a create: an explicit one must lie in the reserved
// range 1..Seed (the minted range starts above it, so the two never meet); zero
// takes the next minted ID.
func (t *engineTx) claimID(explicit int64) (graphstate.EntityID, error) {
	if explicit == 0 {
		return t.newID()
	}
	if explicit < 0 || uint64(explicit) > t.alloc.offset {
		return 0, t.fail(fmt.Errorf("%w: an explicit ID must lie in 1..Allocation.Seed", ErrInvalidID))
	}
	return graphstate.EntityID(explicit), nil
}

func (t *engineTx) newID() (graphstate.EntityID, error) {
	id, err := t.alloc.next()
	if err != nil {
		return 0, t.fail(err)
	}
	return graphstate.EntityID(id), nil
}

// newValueID is a per-Update sequence; the commit adds the commit index.
func (t *engineTx) newValueID() graphstate.ValueID {
	t.vals++
	if t.vals >= 1<<valSeqBits {
		t.fail(fmt.Errorf("%w: too many property writes in one Update", ErrInvalidRequest))
	}
	return graphstate.ValueID(t.vals)
}

func (t *engineTx) add(op graphstate.Operation) {
	op.Recorded = t.rec
	t.ops = append(t.ops, op)
}

// ceil is the commit instant on the knowledge axis, fixed when the Update starts.
func (t *engineTx) ceil() temporal.Position { return t.ceiling }

// schema resolves the declared definition of key (seed, earlier declaration,
// committed catalog) or declares it from the first value's kind, and returns the
// value coerced to the key's kind: an exact numeric converts, anything else of
// another kind is ErrInvalidValue. A null cannot declare a key.
func (t *engineTx) schema(node bool, key string, v Value) (graphstate.Scalar, error) {
	s, err := valueToScalar(v)
	if err != nil {
		return graphstate.Scalar{}, t.fail(err)
	}
	owner := graphstate.Relationship
	if node {
		owner = graphstate.Node
	}
	d, ok := t.e.defs(owner)[key]
	if !ok {
		for _, p := range t.decls {
			if p.Owner == owner && p.Name == key {
				d, ok = p, true
			}
		}
	}
	if !ok {
		var err error
		if d, ok, err = t.e.definition(t.ctx, t.base, owner, key); err != nil {
			return graphstate.Scalar{}, t.fail(err)
		}
	}
	if !ok {
		if s.Kind() == graphstate.ScalarNull {
			return graphstate.Scalar{}, t.fail(fmt.Errorf("%w: a null cannot declare a property", ErrInvalidValue))
		}
		d = graphstate.PropertyDefinition{Name: key, Owner: owner, Type: s.Kind(), Cardinality: graphstate.ScalarCardinality}
		t.decls = append(t.decls, d)
	}
	out, err := coerce(d.Type, s)
	if err != nil {
		return graphstate.Scalar{}, t.fail(err)
	}
	if d.Unique != graphstate.UniqueNone {
		out = out.UniqueForm() // a unique claim follows Equal: [3.0,4] is stored as [3,4]
	}
	return out, nil
}

func coerce(to graphstate.ScalarKind, s graphstate.Scalar) (graphstate.Scalar, error) {
	switch {
	case s.Kind() == graphstate.ScalarNull || s.Kind() == to:
		return s, nil
	case to == graphstate.ScalarI64 && s.Kind() == graphstate.ScalarF64:
		f, _ := s.Float64Value()
		if f == math.Trunc(f) && f >= -(1<<63) && f < 1<<63 {
			return graphstate.I64(int64(f)), nil
		}
	case to == graphstate.ScalarF64 && s.Kind() == graphstate.ScalarI64:
		i, _ := s.Int64Value()
		if f := float64(i); f >= -(1<<63) && f < 1<<63 && int64(f) == i {
			if out, err := graphstate.F64(f); err == nil {
				return out, nil
			}
		}
	}
	return graphstate.Scalar{}, fmt.Errorf("%w: value does not fit the declared property kind", ErrInvalidValue)
}

func sortedKeys(m map[string]Value) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	slices.Sort(ks)
	return ks
}

func (t *engineTx) props(id graphstate.EntityID, life graphstate.LifeID, node bool, m map[string]Value, scope temporal.Scope) error {
	for _, k := range sortedKeys(m) {
		s, err := t.schema(node, k, m[k])
		if err != nil {
			return err
		}
		t.add(graphstate.Operation{Kind: graphstate.Set, Owner: id, Life: life, Scope: scope, Name: k, Value: s, ValueID: t.newValueID()})
	}
	return nil
}

func (t *engineTx) CreateNode(o CreateNode) (types.NodeID, error) {
	if err := t.pre(o); err != nil {
		return 0, err
	}
	return t.createNode(int64(o.ID), o.Labels, o.Props, o.Scope)
}

func (t *engineTx) born(id graphstate.EntityID, scope temporal.Scope) {
	if t.fresh == nil {
		t.fresh = map[graphstate.EntityID]bool{}
		t.spans = map[graphstate.EntityID][]lifeSpan{}
	}
	t.fresh[id] = true
	t.spans[id] = append(t.spans[id], lifeSpan{lifeID, scope})
}

func (t *engineTx) createNode(explicit int64, labels []string, props map[string]Value, scope temporal.Scope) (types.NodeID, error) {
	id, err := t.claimID(explicit)
	if err != nil {
		return 0, err
	}
	t.born(id, scope)
	t.add(graphstate.Operation{Kind: graphstate.CreateNode, Owner: id, Life: lifeID, Scope: scope})
	for _, l := range labels {
		t.add(graphstate.Operation{Kind: graphstate.AddLabel, Owner: id, Life: lifeID, Scope: scope, Name: l})
	}
	if err := t.props(id, lifeID, true, props, scope); err != nil {
		return 0, err
	}
	return types.NodeID(id), nil
}

func (t *engineTx) CreateRel(o CreateRel) (types.RelID, error) {
	if err := t.pre(o); err != nil {
		return 0, err
	}
	id, err := t.claimID(int64(o.ID))
	if err != nil {
		return 0, err
	}
	t.born(id, o.Scope)
	// By identity, not bound to the endpoints' lives: Scope may extend beyond them.
	t.add(graphstate.Operation{Kind: graphstate.CreateRelationship, Owner: id, Life: lifeID, Scope: o.Scope,
		Record: graphstate.EntityRecord{Type: o.Type, Source: graphstate.EntityID(o.From), Target: graphstate.EntityID(o.To), Mode: graphstate.IdentityReference}})
	if err := t.props(id, lifeID, false, o.Props, o.Scope); err != nil {
		return 0, err
	}
	return types.RelID(id), nil
}

func target(r Ref) (graphstate.EntityID, bool) {
	if r.IsRel() {
		return graphstate.EntityID(r.Rel()), false
	}
	return graphstate.EntityID(r.Node()), true
}

// Correct re-asserts the target over Scope and sets Set there. A retracted
// region is filled (a later assertion is new knowledge); nothing at or after
// the entity's End (committed or earlier in this Update) is: the scope is
// clipped there, and a scope wholly after the End is ErrInvalidScope.
func (t *engineTx) Correct(o Correct) error {
	if err := t.pre(o); err != nil {
		return err
	}
	id, node := target(o.Target)
	life, err := t.lifeFor(id, o.Scope)
	if err != nil {
		return t.fail(err)
	}
	scope, err := t.beforeEnd(id, life, o.Scope)
	if err != nil {
		return t.fail(err)
	}
	t.add(graphstate.Operation{Kind: graphstate.Correct, Owner: id, Life: life, Scope: scope, Present: true})
	return t.props(id, life, node, o.Set, scope)
}

// beforeEnd clips scope at the entity's End.
func (t *engineTx) beforeEnd(id graphstate.EntityID, life graphstate.LifeID, scope temporal.Scope) (temporal.Scope, error) {
	p, ok, err := t.endOf(id, life)
	if err != nil || !ok {
		return scope, err
	}
	l := t.e.cfg.Limits
	lo, err := temporal.FiniteBound(p, true)
	if err != nil {
		return scope, err
	}
	gone, err := temporal.Span(scope.Axis(), lo, temporal.PositiveInfinity(), l)
	if err != nil {
		return scope, err
	}
	allowed, err := scope.Difference(gone, l)
	if err != nil {
		return scope, err
	}
	if allowed.Kind() == temporal.ScopeEmpty {
		return scope, fmt.Errorf("%w: the whole scope lies at or after the entity's End", ErrInvalidScope)
	}
	return allowed, nil
}

// endOf is the End position of one life: this Update's, else the committed one.
func (t *engineTx) endOf(id graphstate.EntityID, life graphstate.LifeID) (temporal.Position, bool, error) {
	if p, ok := t.ends[endKey{id, life}]; ok {
		return p, true, nil
	}
	if t.fresh[id] {
		return temporal.Position{}, false, nil
	}
	return t.e.endAt(t.ctx, t.base, id, life)
}

// lifeFor is the life of id that scope falls in. Until some entity has had a
// second life every entity has life 1 and nothing is read. Otherwise lives made
// in this Update win over committed ones; a scope inside two lives is
// ErrInvalidScope, and so is one inside none when the entity has several lives
// (a refill of an old life's hole is not told from the new life: Reopen it).
func (t *engineTx) lifeFor(id graphstate.EntityID, scope temporal.Scope) (graphstate.LifeID, error) {
	if !t.e.multi && !t.reopened {
		return lifeID, nil
	}
	l := t.e.cfg.Limits
	hit := func(spans []lifeSpan) (graphstate.LifeID, int, error) {
		var life graphstate.LifeID
		n := 0
		for _, sp := range spans {
			ov, err := sp.scope.Overlaps(scope, l)
			if err != nil {
				return 0, 0, err
			}
			if ov && sp.life != life {
				life = sp.life
				n++
			}
		}
		return life, n, nil
	}
	maxLife := graphstate.LifeID(0)
	for _, sp := range t.spans[id] {
		maxLife = max(maxLife, sp.life)
	}
	if life, n, err := hit(t.spans[id]); err != nil {
		return 0, err
	} else if n > 1 {
		return 0, fmt.Errorf("%w: the scope spans two lives of the entity", ErrInvalidScope)
	} else if n == 1 {
		return life, nil
	}
	if !t.fresh[id] {
		c, ok := t.lives[id]
		if !ok {
			var err error
			c.spans, c.max, err = t.e.livesOf(t.ctx, t.base, id)
			if err != nil {
				return 0, err
			}
			if t.lives == nil {
				t.lives = map[graphstate.EntityID]committedLives{}
			}
			t.lives[id] = c
		}
		maxLife = max(maxLife, c.max)
		if life, n, err := hit(c.spans); err != nil {
			return 0, err
		} else if n > 1 {
			return 0, fmt.Errorf("%w: the scope spans two lives of the entity", ErrInvalidScope)
		} else if n == 1 {
			return life, nil
		}
	}
	if maxLife > 1 {
		return 0, fmt.Errorf("%w: the scope lies in no life of an entity with several lives", ErrInvalidScope)
	}
	return lifeID, nil
}

// Reopen starts the next life of the target over Scope.
func (t *engineTx) Reopen(o Reopen) error {
	if err := t.pre(o); err != nil {
		return err
	}
	id, _ := target(o.Target)
	next := graphstate.LifeID(0)
	for _, sp := range t.spans[id] {
		next = max(next, sp.life)
	}
	if !t.fresh[id] {
		_, committed, err := t.e.livesOf(t.ctx, t.base, id)
		if err != nil {
			return t.fail(err)
		}
		next = max(next, committed)
	}
	next++
	if t.spans == nil {
		t.spans = map[graphstate.EntityID][]lifeSpan{}
	}
	t.spans[id] = append(t.spans[id], lifeSpan{next, o.Scope})
	t.reopened = true
	t.add(graphstate.Operation{Kind: graphstate.Reopen, Owner: id, Life: next, Scope: o.Scope})
	return nil
}

// SetMeta stages an engine KV row written with the Update's batch.
func (t *engineTx) SetMeta(key string, value []byte) error {
	if t.done {
		return ErrTxDone
	}
	if err := checkMetaKey(key); err != nil {
		return t.fail(err)
	}
	if len(value) > MaxMetaValueBytes {
		return t.fail(fmt.Errorf("%w: meta value over %d bytes", ErrInvalidRequest, MaxMetaValueBytes))
	}
	if t.meta == nil {
		t.meta = map[string][]byte{}
	}
	t.meta[key] = bytes.Clone(value)
	return nil
}

// End closes the target from the upper bound of Scope to +infinity.
func (t *engineTx) End(o End) error {
	if err := t.pre(o); err != nil {
		return err
	}
	_, hi, ok := o.Scope.Bounds()
	p, finite := hi.Position()
	if !ok || !finite {
		return t.fail(fmt.Errorf("%w: End needs a scope with a finite upper bound", ErrInvalidScope))
	}
	lo, err := temporal.FiniteBound(p, true)
	if err != nil {
		return t.fail(fmt.Errorf("%w: %v", ErrInvalidScope, err))
	}
	tail, err := temporal.Span(o.Scope.Axis(), lo, temporal.PositiveInfinity(), t.e.cfg.Limits)
	if err != nil {
		return t.fail(fmt.Errorf("%w: %v", ErrInvalidScope, err))
	}
	id, _ := target(o.Target)
	life, err := t.lifeFor(id, tail)
	if err != nil {
		return t.fail(err)
	}
	prior, ok, err := t.endOf(id, life)
	if err != nil {
		return t.fail(err)
	}
	if ok { // the standing End stays the earliest; a later one is still recorded (evidence)
		c, err := temporal.ComparePositions(p, prior, t.e.cfg.Limits)
		if err != nil {
			return t.fail(fmt.Errorf("%w: %v", ErrInvalidScope, err))
		}
		if c != temporal.Less {
			p = prior
		}
	}
	if t.ends == nil {
		t.ends = map[endKey]temporal.Position{}
	}
	t.ends[endKey{id, life}] = p
	t.add(graphstate.Operation{Kind: graphstate.End, Owner: id, Life: life, Scope: tail})
	return nil
}

func (t *engineTx) AddLabel(o AddLabel) error {
	if err := t.pre(o); err != nil {
		return err
	}
	life, err := t.lifeFor(graphstate.EntityID(o.Target), o.Scope)
	if err != nil {
		return t.fail(err)
	}
	t.add(graphstate.Operation{Kind: graphstate.AddLabel, Owner: graphstate.EntityID(o.Target), Life: life, Scope: o.Scope, Name: o.Label})
	return nil
}

func (t *engineTx) RemoveLabel(o RemoveLabel) error {
	if err := t.pre(o); err != nil {
		return err
	}
	life, err := t.lifeFor(graphstate.EntityID(o.Target), o.Scope)
	if err != nil {
		return t.fail(err)
	}
	t.add(graphstate.Operation{Kind: graphstate.RemoveLabel, Owner: graphstate.EntityID(o.Target), Life: life, Scope: o.Scope, Name: o.Label})
	return nil
}

func (t *engineTx) Set(o Set) error {
	if err := t.pre(o); err != nil {
		return err
	}
	id, node := target(o.Target)
	s, err := t.schema(node, o.Key, o.Value)
	if err != nil {
		return err
	}
	life, err := t.lifeFor(id, o.Scope)
	if err != nil {
		return t.fail(err)
	}
	t.add(graphstate.Operation{Kind: graphstate.Set, Owner: id, Life: life, Scope: o.Scope, Name: o.Key, Value: s, ValueID: t.newValueID()})
	return nil
}

func (t *engineTx) Unset(o Unset) error {
	if err := t.pre(o); err != nil {
		return err
	}
	id, node := target(o.Target)
	owner := graphstate.Relationship
	if node {
		owner = graphstate.Node
	}
	declared := false
	for _, p := range t.decls {
		declared = declared || p.Owner == owner && p.Name == o.Key
	}
	if _, ok, err := t.e.definition(t.ctx, t.base, owner, o.Key); err != nil {
		return t.fail(err)
	} else if !ok && !declared {
		return t.fail(fmt.Errorf("%w: property was never declared", ErrInvalidOp))
	}
	life, err := t.lifeFor(id, o.Scope)
	if err != nil {
		return t.fail(err)
	}
	t.add(graphstate.Operation{Kind: graphstate.Unset, Owner: id, Life: life, Scope: o.Scope, Name: o.Key})
	return nil
}

// Retract withdraws the assertion over Scope (a retraction cell, not a validity end).
func (t *engineTx) Retract(o Retract) error {
	if err := t.pre(o); err != nil {
		return err
	}
	id, _ := target(o.Target)
	life, err := t.lifeFor(id, o.Scope)
	if err != nil {
		return t.fail(err)
	}
	scope, err := t.beforeEnd(id, life, o.Scope)
	if err != nil {
		if errors.Is(err, ErrInvalidScope) { // already closed there by the End
			return nil
		}
		return t.fail(err)
	}
	t.add(graphstate.Operation{Kind: graphstate.Close, Owner: id, Life: life, Scope: scope})
	return nil
}

// GetOrCreate finds the live node holding the unique claim (Label, Key, Value)
// or creates it. The claim is the declared unique property of that Key (over all
// nodes); a holder without Label is a unique violation.
func (t *engineTx) GetOrCreate(o GetOrCreate) (types.NodeID, bool, error) {
	if err := t.pre(o); err != nil {
		return 0, false, err
	}
	d, ok := t.e.node[o.Key]
	if !ok || d.Unique != graphstate.UniqueScalar { // only a Config seed can be unique
		return 0, false, t.fail(fmt.Errorf("%w: key is not a declared unique node property", ErrInvalidOp))
	}
	sc, err := t.schema(true, o.Key, o.Value)
	if err != nil {
		return 0, false, err
	}
	id, found, err := t.e.findClaim(t.ctx, t.base, d, sc, o.Label, o.Scope)
	if err != nil {
		return 0, false, t.fail(err)
	}
	if found {
		return id, false, nil
	}
	ck, err := sc.EqualityKey(graphstate.Limits{})
	if err != nil {
		return 0, false, t.fail(err)
	}
	if t.claims == nil {
		t.claims = map[string]types.NodeID{}
	}
	if prior, ok := t.claims[o.Key+"\x00"+ck]; ok {
		return prior, false, nil
	}
	props := map[string]Value{o.Key: o.Value}
	for k, v := range o.Props {
		if k != o.Key {
			props[k] = v
		}
	}
	nid, err := t.createNode(0, []string{o.Label}, props, o.Scope)
	if err != nil {
		return 0, false, err
	}
	t.claims[o.Key+"\x00"+ck] = nid
	return nid, true, nil
}

var _ = graphstore.ErrInvalid
