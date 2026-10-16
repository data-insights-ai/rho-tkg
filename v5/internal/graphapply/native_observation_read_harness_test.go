package graphapply

import (
	"context"
	"errors"
	"reflect"
	"strings"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// nativeObservationRead owns only results actually fetched during ONE bounded
// fixture observation. It borrows the unchanged certified view and output
// ledger. It never lives on a source/session or survives an observation return.
// Entity axes and scalar values are immutable value types. This wrapper retains
// only complete immutable component states, never cursors, mutable result slices
// or failed reads. State and Scope APIs guarantee immutable copies.
type nativeObservationRead struct {
	graphstate.ReadView
	owners   []context.Context
	arena    *graphstate.OutputBudget
	id       graphstate.ViewID
	temporal temporal.Limits
	entities []struct {
		id    graphstate.EntityID
		value graphstate.EntityRead
	}
	lives []struct {
		owner graphstate.EntityID
		life  graphstate.LifeID
		value graphstate.LifeRead
	}
	properties []struct {
		owner graphstate.EntityKind
		name  string
		value graphstate.PropertyRead
	}
	values []struct {
		id    graphstate.ValueID
		value graphstate.ValueRead
	}
	components []struct {
		query  graphstate.ComponentQuery
		budget graphstate.ReadBudget
		value  graphstate.ComponentPage
	}
}

// These finite capacities cover the existing32-ID observations, not arbitrary
// graph queries. Slot constants cover fixed headers and retired slice backing;
// variable immutable axis/name backing is charged separately below.
const nativeObservationSlots = 512 + 32*256 + 16*128 + 8*128 + 16*512 + 64*2048

func newNativeObservationRead(v graphstate.ReadView, arena *graphstate.OutputBudget, l state.Limits, owners []context.Context) (graphstate.ReadView, error) {
	if v == nil || arena == nil || len(owners) == 0 || len(owners) > 16 {
		return nil, errInvalid
	}
	for _, ctx := range owners {
		if ctx == nil {
			return nil, errInvalid
		}
		if err := ctx.Err(); err != nil {
			return nil, errors.Join(graphstore.ErrSourceUnavailable, err)
		}
	}
	if v.Identity() == (graphstate.ViewID{}) || v.Graph() == (graphstate.GraphID{}) {
		return nil, graphstore.ErrClosed
	}
	if err := arena.Reserve(nativeObservationSlots); err != nil {
		return nil, outputBudgetFailure(err)
	}
	out := &nativeObservationRead{ReadView: v, arena: arena, id: v.Identity(), temporal: l.Temporal, owners: append([]context.Context(nil), owners...)}
	return out, nil
}
func (v *nativeObservationRead) check(ctx context.Context) error {
	if ctx == nil {
		return errInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, owner := range v.owners {
		if err := owner.Err(); err != nil {
			return errors.Join(graphstore.ErrSourceUnavailable, err)
		}
	}
	if v.ReadView.Identity() == (graphstate.ViewID{}) {
		return graphstore.ErrClosed
	}
	if v.ReadView.Identity() != v.id {
		return graphstate.ErrInvalidView
	}
	return nil
}
func (v *nativeObservationRead) Entity(ctx context.Context, id graphstate.EntityID) (graphstate.EntityRead, error) {
	if err := v.check(ctx); err != nil {
		return graphstate.EntityRead{}, err
	}
	if id == 0 {
		return v.ReadView.Entity(ctx, id)
	}
	for _, e := range v.entities {
		if e.id == id {
			return e.value, nil
		}
	}
	if len(v.entities) == 32 {
		return graphstate.EntityRead{}, errLimit
	}
	value, err := v.ReadView.Entity(ctx, id)
	if err != nil {
		return value, err
	}
	d := value.Record.Axis.Descriptor()
	if err := v.arena.Reserve(256 + 4*(len(value.Record.Type)+len(d.Reference)+len(d.CanonicalUnit))); err != nil {
		return graphstate.EntityRead{}, outputBudgetFailure(err)
	}
	if v.entities == nil {
		v.entities = make([]struct {
			id    graphstate.EntityID
			value graphstate.EntityRead
		}, 0, 32)
	}
	value.Record.Type = strings.Clone(value.Record.Type)
	v.entities = append(v.entities, struct {
		id    graphstate.EntityID
		value graphstate.EntityRead
	}{id, value})
	return value, nil
}
func (v *nativeObservationRead) Life(ctx context.Context, owner graphstate.EntityID, life graphstate.LifeID) (graphstate.LifeRead, error) {
	if err := v.check(ctx); err != nil {
		return graphstate.LifeRead{}, err
	}
	if owner == 0 || life == 0 {
		return v.ReadView.Life(ctx, owner, life)
	}
	for _, e := range v.lives {
		if e.owner == owner && e.life == life {
			return e.value, nil
		}
	}
	if len(v.lives) == 16 {
		return graphstate.LifeRead{}, errLimit
	}
	value, err := v.ReadView.Life(ctx, owner, life)
	if err != nil {
		return value, err
	}
	if v.lives == nil {
		v.lives = make([]struct {
			owner graphstate.EntityID
			life  graphstate.LifeID
			value graphstate.LifeRead
		}, 0, 16)
	}
	v.lives = append(v.lives, struct {
		owner graphstate.EntityID
		life  graphstate.LifeID
		value graphstate.LifeRead
	}{owner, life, value})
	return value, nil
}
func (v *nativeObservationRead) Property(ctx context.Context, owner graphstate.EntityKind, name string) (graphstate.PropertyRead, error) {
	if err := v.check(ctx); err != nil {
		return graphstate.PropertyRead{}, err
	}
	for _, e := range v.properties {
		if e.owner == owner && e.name == name {
			return e.value, nil
		}
	}
	if len(v.properties) == 8 {
		return graphstate.PropertyRead{}, errLimit
	}
	value, err := v.ReadView.Property(ctx, owner, name)
	if err != nil {
		return value, err
	}
	if err := v.arena.Reserve(128 + 4*(len(name)+len(value.Record.Name))); err != nil {
		return graphstate.PropertyRead{}, outputBudgetFailure(err)
	}
	if v.properties == nil {
		v.properties = make([]struct {
			owner graphstate.EntityKind
			name  string
			value graphstate.PropertyRead
		}, 0, 8)
	}
	value.Record.Name = strings.Clone(value.Record.Name)
	v.properties = append(v.properties, struct {
		owner graphstate.EntityKind
		name  string
		value graphstate.PropertyRead
	}{owner, strings.Clone(name), value})
	return value, nil
}
func (v *nativeObservationRead) Value(ctx context.Context, id graphstate.ValueID) (graphstate.ValueRead, error) {
	if err := v.check(ctx); err != nil {
		return graphstate.ValueRead{}, err
	}
	if id == 0 {
		return v.ReadView.Value(ctx, id)
	}
	for _, e := range v.values {
		if e.id == id {
			return e.value, nil
		}
	}
	if len(v.values) == 16 {
		return graphstate.ValueRead{}, errLimit
	}
	value, err := v.ReadView.Value(ctx, id)
	if err != nil {
		return value, err
	}
	// These observers consume only native scalar-i64/null values. Leave wider
	// immutable payloads on the original owning reader rather than undercharge.
	if value.Found && value.Value.Kind() != graphstate.ScalarI64 && value.Value.Kind() != graphstate.ScalarNull {
		return value, nil
	}
	if v.values == nil {
		v.values = make([]struct {
			id    graphstate.ValueID
			value graphstate.ValueRead
		}, 0, 16)
	}
	v.values = append(v.values, struct {
		id    graphstate.ValueID
		value graphstate.ValueRead
	}{id, value})
	return value, nil
}

func (v *nativeObservationRead) ComponentPage(ctx context.Context, q graphstate.ComponentQuery, next graphstate.Cursor, b graphstate.ReadBudget) (graphstate.ComponentPage, error) {
	if err := v.check(ctx); err != nil {
		return graphstate.ComponentPage{}, err
	}
	if next != 0 || q.MergeContext {
		return v.ReadView.ComponentPage(ctx, q, next, b)
	}
	for _, e := range v.components {
		if e.budget == b && reflect.DeepEqual(e.query, q) {
			return e.value, nil
		}
	}
	if len(v.components) == 64 {
		return graphstate.ComponentPage{}, errLimit
	}
	value, err := v.ReadView.ComponentPage(ctx, q, next, b)
	if err != nil {
		return value, err
	}
	if !value.Complete || value.Next != 0 || cap(value.MergeContext) != 0 {
		return value, nil
	}
	if err := v.arena.ReserveState(value.Data); err != nil {
		return graphstate.ComponentPage{}, outputBudgetFailure(err)
	}
	if err := v.arena.ReserveScope(q.Window, v.temporal); err != nil {
		return graphstate.ComponentPage{}, outputBudgetFailure(err)
	}
	if err := v.arena.ReserveScope(value.Owned, v.temporal); err != nil {
		return graphstate.ComponentPage{}, outputBudgetFailure(err)
	}
	if err := v.arena.Reserve(128 + 4*len(q.Key.Name)); err != nil {
		return graphstate.ComponentPage{}, outputBudgetFailure(err)
	}
	if v.components == nil {
		v.components = make([]struct {
			query  graphstate.ComponentQuery
			budget graphstate.ReadBudget
			value  graphstate.ComponentPage
		}, 0, 64)
	}
	q.Key.Name = strings.Clone(q.Key.Name)
	v.components = append(v.components, struct {
		query  graphstate.ComponentQuery
		budget graphstate.ReadBudget
		value  graphstate.ComponentPage
	}{q, b, value})
	return value, nil
}
