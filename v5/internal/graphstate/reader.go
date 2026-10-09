package graphstate

import (
	"cmp"
	"context"
	"reflect"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

type lifeKey struct {
	owner EntityID
	life  LifeID
}
type engine struct {
	ctx                                context.Context
	view                               ReadView
	id                                 ViewID
	graph                              GraphID
	limits                             Limits
	revision                           state.Revision
	delta                              Delta
	entities                           map[EntityID]EntityRecord
	lives                              map[lifeKey]LifeRecord
	values                             map[ValueID]Scalar
	identity                           map[string]ValueID
	pages, rows, readBytes, deltaBytes int
}

func start(ctx context.Context, v ReadView, l Limits, r state.Revision) (*engine, error) {
	if ctx == nil {
		return nil, ErrInvalidInput
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Reflection occurs once at the boundary for typed-nil providers, never per row.
	if nilProvider(v) {
		return nil, ErrNilView
	}
	if v.Graph() == (GraphID{}) || v.Identity() == (ViewID{}) {
		return nil, ErrInvalidView
	}
	e := &engine{ctx: ctx, view: v, id: v.Identity(), graph: v.Graph(), limits: l, revision: r, entities: make(map[EntityID]EntityRecord), lives: make(map[lifeKey]LifeRecord), values: make(map[ValueID]Scalar), identity: make(map[string]ValueID)}
	e.delta = Delta{Graph: e.graph, View: e.id}
	if err := e.output(32); err != nil {
		return nil, err
	}
	return e, nil
}
func nilProvider(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return rv.IsNil()
	default:
		return false
	}
}
func (e *engine) check() error {
	if err := e.ctx.Err(); err != nil {
		return err
	}
	if e.view.Identity() != e.id || e.view.Graph() != e.graph {
		return ErrInvalidView
	}
	return nil
}
func (e *engine) source(v ViewID) error {
	if err := e.check(); err != nil {
		return err
	}
	if v != e.id {
		return ErrInvalidView
	}
	return nil
}
func (e *engine) charge(rows, bytes int) error {
	if rows < 0 || bytes < 0 || rows > e.limits.MaxRows-e.rows || bytes > e.limits.MaxReadBytes-e.readBytes {
		return ErrResourceLimit
	}
	e.rows += rows
	e.readBytes += bytes
	return e.check()
}
func (e *engine) dep(d Dependency) error {
	if len(e.delta.Dependencies) >= e.limits.MaxDependencies {
		return ErrResourceLimit
	}
	bytes := 128 + len(d.Name) + len(d.Key.Name) + len(d.Prefix.Name) + len(d.Unique.Definition.Name)
	for _, s := range []temporal.Scope{d.Window, d.Unique.Window, d.Incident.Window} {
		if s.Kind() != temporal.ScopeInvalid {
			wire, err := temporal.AppendScope(nil, s, e.limits.Component.Temporal)
			if err != nil {
				return err
			}
			bytes += len(wire) + axisBytes(s.Axis())
		}
	}
	for _, v := range []Scalar{d.Value, d.Unique.Value} {
		if v.Kind() != ScalarInvalid {
			data, err := v.bytes(e.limits)
			if err != nil {
				return err
			}
			n := v.retainedBytes(len(data))
			bytes += n
		}
	}
	if err := e.output(bytes); err != nil {
		return err
	}
	d.View = e.id
	e.delta.Dependencies = append(e.delta.Dependencies, d)
	return nil
}
func (e *engine) output(n int) error {
	if n < 0 || n > e.limits.MaxDeltaBytes-e.deltaBytes {
		return ErrResourceLimit
	}
	e.deltaBytes += n
	return nil
}
func validName(s string, l Limits) bool {
	return len(s) > 0 && len(s) <= l.MaxNameBytes && utf8.ValidString(s) && strings.TrimSpace(s) != ""
}
func axisBytes(a temporal.Axis) int {
	d := a.Descriptor()
	return 27 + len(d.Reference) + len(d.CanonicalUnit)
}
func validKey(k ComponentKey, l Limits) bool {
	if k.Owner == 0 {
		return false
	}
	switch k.Kind {
	case Presence:
		return k.Life == 0 && k.Name == "" && k.Member == 0
	case Label, ScalarProperty:
		return k.Life != 0 && validName(k.Name, l) && k.Member == 0
	case SetMember:
		return k.Life != 0 && validName(k.Name, l) && k.Member != 0
	default:
		return false
	}
}
func (e *engine) entity(id EntityID) (EntityRecord, bool, error) {
	if id == 0 {
		return EntityRecord{}, false, ErrInvalidInput
	}
	if value, ok := e.entities[id]; ok {
		return value, true, nil
	}
	if err := e.check(); err != nil {
		return EntityRecord{}, false, err
	}
	read, err := e.view.Entity(e.ctx, id)
	if err != nil {
		return EntityRecord{}, false, err
	}
	if err := e.source(read.View); err != nil {
		return EntityRecord{}, false, err
	}
	if err := e.charge(1, 64+len(read.Record.Type)+axisBytes(read.Record.Axis)); err != nil {
		return EntityRecord{}, false, err
	}
	if err := e.dep(Dependency{Kind: EntityDependency, Owner: id, Version: read.Version, Absent: !read.Found}); err != nil {
		return EntityRecord{}, false, err
	}
	if read.Found {
		if read.Record.ID != id || (read.Record.Kind != Node && read.Record.Kind != Relationship) {
			return EntityRecord{}, false, ErrContradictoryRead
		}
		if read.Record.Kind == Relationship && (!validName(read.Record.Type, e.limits) || read.Record.Source == 0 || read.Record.Target == 0 || (read.Record.Mode != LifeBound && read.Record.Mode != IdentityReference)) {
			return EntityRecord{}, false, ErrContradictoryRead
		}
		empty, err := temporal.Empty(read.Record.Axis)
		if err != nil {
			return EntityRecord{}, false, err
		}
		if _, err := temporal.AppendScope(nil, empty, e.limits.Component.Temporal); err != nil {
			return EntityRecord{}, false, err
		}
	}
	return read.Record, read.Found, nil
}
func (e *engine) life(owner EntityID, id LifeID) (LifeRecord, bool, error) {
	if owner == 0 || id == 0 {
		return LifeRecord{}, false, ErrInvalidInput
	}
	if value, ok := e.lives[lifeKey{owner, id}]; ok {
		return value, true, nil
	}
	read, err := e.view.Life(e.ctx, owner, id)
	if err != nil {
		return LifeRecord{}, false, err
	}
	if err := e.source(read.View); err != nil {
		return LifeRecord{}, false, err
	}
	if err := e.charge(1, 32); err != nil {
		return LifeRecord{}, false, err
	}
	if err := e.dep(Dependency{Kind: LifeDependency, Owner: owner, Life: id, Version: read.Version, Absent: !read.Found}); err != nil {
		return LifeRecord{}, false, err
	}
	if read.Found && (read.Record.Owner != owner || read.Record.Life != id) {
		return LifeRecord{}, false, ErrContradictoryRead
	}
	if read.Found {
		ownerRecord, found, err := e.entity(owner)
		if err != nil {
			return LifeRecord{}, false, err
		}
		if !found {
			return LifeRecord{}, false, ErrContradictoryRead
		}
		if ownerRecord.Kind == Node && (read.Record.SourceLife != 0 || read.Record.TargetLife != 0) || ownerRecord.Kind == Relationship && ownerRecord.Mode == LifeBound && (read.Record.SourceLife == 0 || read.Record.TargetLife == 0) {
			return LifeRecord{}, false, ErrContradictoryRead
		}
	}
	return read.Record, read.Found, nil
}
func (e *engine) property(name string) (PropertyDefinition, error) {
	if !validName(name, e.limits) {
		return PropertyDefinition{}, ErrInvalidInput
	}
	read, err := e.view.Property(e.ctx, name)
	if err != nil {
		return PropertyDefinition{}, err
	}
	if err := e.source(read.View); err != nil {
		return PropertyDefinition{}, err
	}
	if err := e.charge(1, 16+len(name)); err != nil {
		return PropertyDefinition{}, err
	}
	if err := e.dep(Dependency{Kind: SchemaDependency, Name: name, Version: read.Version, Absent: !read.Found}); err != nil {
		return PropertyDefinition{}, err
	}
	if !read.Found {
		return PropertyDefinition{}, ErrSchemaMismatch
	}
	d := read.Record
	if d.Name != name || (d.Owner != Node && d.Owner != Relationship) || d.Type < ScalarString || d.Type > ScalarScope || (d.Cardinality != ScalarCardinality && d.Cardinality != SetCardinality) {
		return PropertyDefinition{}, ErrSchemaMismatch
	}
	if d.Unique > UniqueMembers || d.Unique == UniqueScalar && d.Cardinality != ScalarCardinality || d.Unique == UniqueMembers && d.Cardinality != SetCardinality {
		return PropertyDefinition{}, ErrUnsupported
	}
	return d, nil
}
func (e *engine) value(id ValueID) (Scalar, error) {
	if value, ok := e.values[id]; ok {
		return value, nil
	}
	read, err := e.view.Value(e.ctx, id)
	if err != nil {
		return Scalar{}, err
	}
	if err := e.source(read.View); err != nil {
		return Scalar{}, err
	}
	if err := e.dep(Dependency{Kind: ValueDependency, ValueID: id, Version: read.Version, Absent: !read.Found}); err != nil {
		return Scalar{}, err
	}
	if !read.Found || read.ID != id {
		return Scalar{}, ErrContradictoryRead
	}
	b, err := read.Value.bytes(e.limits)
	if err != nil {
		return Scalar{}, err
	}
	n := read.Value.retainedBytes(len(b))
	if err := e.charge(1, n); err != nil {
		return Scalar{}, err
	}
	return read.Value, nil
}
func (e *engine) intern(v Scalar, fresh ValueID) (state.ValueRef, error) {
	if v.Kind() == ScalarNull {
		return state.Null(), nil
	}
	key, err := v.EqualityKey(e.limits)
	if err != nil {
		return state.ValueRef{}, err
	}
	n := v.retainedBytes(len(key))
	if id, ok := e.identity[key]; ok {
		return state.NewValueRef(uint64(id), uint64(n))
	} // #nosec G115 -- retained-value size is nonnegative and budget bounded.
	read, err := e.view.ValueIdentity(e.ctx, v)
	if err != nil {
		return state.ValueRef{}, err
	}
	if err := e.source(read.View); err != nil {
		return state.ValueRef{}, err
	}
	if err := e.dep(Dependency{Kind: ValueIdentityDependency, Value: v, Version: read.Version, Absent: !read.Found}); err != nil {
		return state.ValueRef{}, err
	}
	if err := e.charge(1, n); err != nil {
		return state.ValueRef{}, err
	}
	id := read.ID
	if read.Found {
		same, err := read.Value.Equal(v, e.limits)
		if err != nil {
			return state.ValueRef{}, err
		}
		if !same || id == 0 {
			return state.ValueRef{}, ErrContradictoryRead
		}
	} else {
		if fresh == 0 {
			return state.ValueRef{}, ErrInvalidInput
		}
		prior, err := e.view.Value(e.ctx, fresh)
		if err != nil {
			return state.ValueRef{}, err
		}
		if err := e.source(prior.View); err != nil {
			return state.ValueRef{}, err
		}
		if err := e.dep(Dependency{Kind: ValueDependency, ValueID: fresh, Version: prior.Version, Absent: !prior.Found}); err != nil {
			return state.ValueRef{}, err
		}
		if prior.Found {
			return state.ValueRef{}, ErrAlreadyExists
		}
		if _, ok := e.values[fresh]; ok {
			return state.ValueRef{}, ErrAlreadyExists
		}
		id = fresh
		if err := e.output(8 + n); err != nil {
			return state.ValueRef{}, err
		}
		e.delta.Values = append(e.delta.Values, ValueWrite{id, v})
		e.values[id] = v
	}
	e.identity[key] = id
	return state.NewValueRef(uint64(id), uint64(n)) // #nosec G115 -- retained-value size is nonnegative and budget bounded.
}

type pageTracker struct {
	seen   map[Cursor]struct{}
	cursor Cursor
}

func (e *engine) next(t *pageTracker, next Cursor, complete bool) error {
	if e.pages >= e.limits.MaxPages {
		return ErrResourceLimit
	}
	e.pages++
	if complete {
		if next != 0 {
			return ErrContradictoryRead
		}
		return nil
	}
	if next == 0 || next == t.cursor {
		return ErrIncompleteRead
	}
	if _, ok := t.seen[next]; ok {
		return ErrIncompleteRead
	}
	t.seen[next] = struct{}{}
	t.cursor = next
	return nil
}
func newTracker() pageTracker { return pageTracker{seen: make(map[Cursor]struct{})} }

func (e *engine) keys(p KeyPredicate) ([]ComponentKey, error) {
	t := newTracker()
	out := []ComponentKey{}
	seen := make(map[ComponentKey]struct{})
	for {
		if err := e.check(); err != nil {
			return nil, err
		}
		page, err := e.view.ComponentKeys(e.ctx, p, t.cursor, ReadBudget{e.limits.MaxRows - e.rows, e.limits.MaxReadBytes - e.readBytes})
		if err != nil {
			return nil, err
		}
		if err := e.source(page.View); err != nil {
			return nil, err
		}
		bytes := 0
		for _, key := range page.Keys {
			if !validKey(key, e.limits) {
				return nil, ErrContradictoryRead
			}
			if len(key.Name) > e.limits.MaxReadBytes-bytes-48 {
				return nil, ErrResourceLimit
			}
			bytes += 48 + len(key.Name)
		}
		if err := e.charge(len(page.Keys), bytes); err != nil {
			return nil, err
		}
		if err := e.dep(Dependency{Kind: PrefixDependency, Version: page.Version, Prefix: p}); err != nil {
			return nil, err
		}
		for _, key := range page.Keys {
			if key.Owner != p.Owner || (p.Life != 0 && key.Life != p.Life) || (p.Kind != 0 && key.Kind != p.Kind) || (p.Name != "" && key.Name != p.Name) {
				return nil, ErrContradictoryRead
			}
			if _, ok := seen[key]; ok {
				return nil, ErrContradictoryRead
			}
			seen[key] = struct{}{}
			out = append(out, key)
		}
		if err := e.next(&t, page.Next, page.Complete); err != nil {
			return nil, err
		}
		if page.Complete {
			break
		}
	}
	for _, patch := range e.delta.Patches {
		k := patch.Key
		if k.Owner == p.Owner && (p.Life == 0 || k.Life == p.Life) && (p.Kind == 0 || k.Kind == p.Kind) && (p.Name == "" || k.Name == p.Name) {
			if _, ok := seen[k]; !ok {
				seen[k] = struct{}{}
				out = append(out, k)
			}
		}
	}
	slices.SortFunc(out, compareKey)
	return out, nil
}
func compareKey(a, b ComponentKey) int {
	return cmp.Or(cmp.Compare(a.Owner, b.Owner), cmp.Compare(a.Life, b.Life), cmp.Compare(a.Kind, b.Kind), cmp.Compare(a.Name, b.Name), cmp.Compare(a.Member, b.Member))
}
