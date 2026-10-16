package graph

import (
	"context"
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
	ended   map[graphstate.EntityID]temporal.Scope // union of this Update's Close scopes
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
		if isZeroPosition(t.e.ceiling()) {
			return t.fail(fmt.Errorf("%w: Recorded needs a KnowledgeAxis", ErrInvalidScope))
		}
		// The engine has no clock: the commit's own knowledge instant is the ceiling.
		if o, err := temporal.ComparePositions(t.rec, t.ceil(), t.e.cfg.Limits); err != nil || o == temporal.Greater {
			return t.fail(fmt.Errorf("%w: Recorded is after the commit", ErrInvalidScope))
		}
	}
	return nil
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

// ceil is the commit instant on the knowledge axis, fixed for the Update.
func (t *engineTx) ceil() temporal.Position {
	if isZeroPosition(t.ceiling) {
		t.ceiling = t.e.ceiling()
	}
	return t.ceiling
}

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

func (t *engineTx) props(id graphstate.EntityID, node bool, m map[string]Value, scope temporal.Scope) error {
	for _, k := range sortedKeys(m) {
		s, err := t.schema(node, k, m[k])
		if err != nil {
			return err
		}
		t.add(graphstate.Operation{Kind: graphstate.Set, Owner: id, Life: lifeID, Scope: scope, Name: k, Value: s, ValueID: t.newValueID()})
	}
	return nil
}

func (t *engineTx) CreateNode(o CreateNode) (types.NodeID, error) {
	if err := t.pre(o); err != nil {
		return 0, err
	}
	return t.createNode(o.Labels, o.Props, o.Scope)
}

func (t *engineTx) createNode(labels []string, props map[string]Value, scope temporal.Scope) (types.NodeID, error) {
	id, err := t.newID()
	if err != nil {
		return 0, err
	}
	t.add(graphstate.Operation{Kind: graphstate.CreateNode, Owner: id, Life: lifeID, Scope: scope})
	for _, l := range labels {
		t.add(graphstate.Operation{Kind: graphstate.AddLabel, Owner: id, Life: lifeID, Scope: scope, Name: l})
	}
	if err := t.props(id, true, props, scope); err != nil {
		return 0, err
	}
	return types.NodeID(id), nil
}

func (t *engineTx) CreateRel(o CreateRel) (types.RelID, error) {
	if err := t.pre(o); err != nil {
		return 0, err
	}
	id, err := t.newID()
	if err != nil {
		return 0, err
	}
	t.add(graphstate.Operation{Kind: graphstate.CreateRelationship, Owner: id, Life: lifeID, Scope: o.Scope,
		Record:  graphstate.EntityRecord{Type: o.Type, Source: graphstate.EntityID(o.From), Target: graphstate.EntityID(o.To), Mode: graphstate.LifeBound},
		Binding: graphstate.LifeRecord{SourceLife: lifeID, TargetLife: lifeID}})
	if err := t.props(id, false, o.Props, o.Scope); err != nil {
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

// Correct re-asserts the target over Scope and sets Set there, but never where
// the entity was ended or retracted (an explicit retraction in the committed
// presence, or an End/Retract earlier in this Update); a scope wholly inside
// such a region is ErrInvalidScope.
func (t *engineTx) Correct(o Correct) error {
	if err := t.pre(o); err != nil {
		return err
	}
	id, node := target(o.Target)
	scope, err := t.unretracted(id, o.Scope)
	if err != nil {
		return t.fail(err)
	}
	t.add(graphstate.Operation{Kind: graphstate.Correct, Owner: id, Life: lifeID, Scope: scope, Present: true})
	return t.props(id, node, o.Set, scope)
}

func (t *engineTx) unretracted(id graphstate.EntityID, scope temporal.Scope) (temporal.Scope, error) {
	r, err := t.e.readerAt(t.ctx, t.base)
	if err != nil {
		return scope, err
	}
	defer r.close()
	l := t.e.cfg.Limits
	gone, err := temporal.Empty(scope.Axis())
	if err != nil {
		return scope, err
	}
	if closed, ok := t.ended[id]; ok {
		if gone, err = gone.Union(closed, l); err != nil {
			return scope, err
		}
	}
	pages, err := r.pieces(graphstate.ComponentKey{Owner: id, Kind: graphstate.Presence}, scope)
	if err != nil {
		return scope, err
	}
	for _, pg := range pages {
		for _, pc := range pg.Data.Pieces() {
			if !pc.Cell().Present() && pc.Cell().Revision().ID() != 0 {
				if gone, err = gone.Union(pc.Scope(), l); err != nil {
					return scope, err
				}
			}
		}
	}
	allowed, err := scope.Difference(gone, l)
	if err != nil {
		return scope, err
	}
	if allowed.Kind() == temporal.ScopeEmpty {
		return scope, fmt.Errorf("%w: the whole scope lies in an ended or retracted region", ErrInvalidScope)
	}
	return allowed, nil
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
	return t.close(id, tail)
}

// close stages a Close and remembers its scope, so a later Correct in the same
// Update cannot revive what this one ended or retracted.
func (t *engineTx) close(id graphstate.EntityID, scope temporal.Scope) error {
	prior, ok := t.ended[id]
	if !ok {
		prior = scope
	}
	union, err := prior.Union(scope, t.e.cfg.Limits)
	if err != nil {
		return t.fail(fmt.Errorf("%w: %v", ErrInvalidScope, err))
	}
	if t.ended == nil {
		t.ended = map[graphstate.EntityID]temporal.Scope{}
	}
	t.ended[id] = union
	t.add(graphstate.Operation{Kind: graphstate.Close, Owner: id, Life: lifeID, Scope: scope})
	return nil
}

func (t *engineTx) AddLabel(o AddLabel) error {
	if err := t.pre(o); err != nil {
		return err
	}
	t.add(graphstate.Operation{Kind: graphstate.AddLabel, Owner: graphstate.EntityID(o.Target), Life: lifeID, Scope: o.Scope, Name: o.Label})
	return nil
}

func (t *engineTx) RemoveLabel(o RemoveLabel) error {
	if err := t.pre(o); err != nil {
		return err
	}
	t.add(graphstate.Operation{Kind: graphstate.RemoveLabel, Owner: graphstate.EntityID(o.Target), Life: lifeID, Scope: o.Scope, Name: o.Label})
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
	t.add(graphstate.Operation{Kind: graphstate.Set, Owner: id, Life: lifeID, Scope: o.Scope, Name: o.Key, Value: s, ValueID: t.newValueID()})
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
	t.add(graphstate.Operation{Kind: graphstate.Unset, Owner: id, Life: lifeID, Scope: o.Scope, Name: o.Key})
	return nil
}

// Retract withdraws the assertion over Scope (a retraction cell, not a validity end).
func (t *engineTx) Retract(o Retract) error {
	if err := t.pre(o); err != nil {
		return err
	}
	id, _ := target(o.Target)
	return t.close(id, o.Scope)
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
	nid, err := t.createNode([]string{o.Label}, props, o.Scope)
	if err != nil {
		return 0, false, err
	}
	t.claims[o.Key+"\x00"+ck] = nid
	return nid, true, nil
}

var _ = graphstore.ErrInvalid
