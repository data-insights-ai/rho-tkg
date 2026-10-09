package graphstate

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"maps"
	"os"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// Full-copy maps/named snapshots are test-only, not production storage or cuts.
type fixtureView struct {
	axis       temporal.Axis
	id         ViewID
	entities   map[EntityID]EntityRecord
	lives      map[lifeKey]LifeRecord
	values     map[ValueID]Scalar
	components map[ComponentKey]state.State
	defs       map[string]PropertyDefinition
	pageHook   func(ComponentQuery, Cursor, ComponentPage) ComponentPage
}

func newFixtureView(t testing.TB) *fixtureView {
	t.Helper()
	a, err := temporal.NewAxis(temporal.AxisDescriptor{ID: temporal.AxisID{1}, Profile: temporal.ProfileIntegerZ, Version: 1, Reference: "oracle", CanonicalUnit: "microsecond"}, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return &fixtureView{axis: a, id: ViewID{1}, entities: map[EntityID]EntityRecord{}, lives: map[lifeKey]LifeRecord{}, values: map[ValueID]Scalar{}, components: map[ComponentKey]state.State{}, defs: map[string]PropertyDefinition{}}
}
func (v *fixtureView) Graph() GraphID   { return GraphID{1} }
func (v *fixtureView) Identity() ViewID { return v.id }
func (v *fixtureView) Entity(_ context.Context, id EntityID) (EntityRead, error) {
	x, ok := v.entities[id]
	return EntityRead{v.id, 1, ok, x}, nil
}
func (v *fixtureView) Life(_ context.Context, id EntityID, life LifeID) (LifeRead, error) {
	x, ok := v.lives[lifeKey{id, life}]
	return LifeRead{v.id, 1, ok, x}, nil
}
func (v *fixtureView) Property(_ context.Context, name string) (PropertyRead, error) {
	x, ok := v.defs[name]
	return PropertyRead{v.id, 1, ok, x}, nil
}
func (v *fixtureView) Value(_ context.Context, id ValueID) (ValueRead, error) {
	x, ok := v.values[id]
	return ValueRead{v.id, 1, ok, id, x}, nil
}
func (v *fixtureView) ValueIdentity(_ context.Context, value Scalar) (ValueRead, error) {
	for id, x := range v.values {
		same, err := x.Equal(value, Limits{})
		if err != nil {
			return ValueRead{}, err
		}
		if same {
			return ValueRead{v.id, 1, true, id, x}, nil
		}
	}
	return ValueRead{View: v.id, Version: 1}, nil
}
func (v *fixtureView) ComponentPage(_ context.Context, q ComponentQuery, cursor Cursor, _ ReadBudget) (ComponentPage, error) {
	s, err := state.New(q.Window.Axis(), state.Limits{})
	if err != nil {
		return ComponentPage{}, err
	}
	if old, ok := v.components[q.Key]; ok {
		for _, piece := range old.Pieces() {
			scope, err := piece.Scope().Intersection(q.Window, temporal.Limits{})
			if err != nil {
				return ComponentPage{}, err
			}
			if scope.Kind() == temporal.ScopeEmpty {
				continue
			}
			r, err := applyCell(s, scope, piece.Cell(), state.Limits{})
			if err != nil {
				return ComponentPage{}, err
			}
			s = r.State()
		}
	}
	p := ComponentPage{View: v.id, Version: 1, Owned: q.Window, Data: s, Complete: true}
	if v.pageHook != nil {
		p = v.pageHook(q, cursor, p)
	}
	return p, nil
}
func (v *fixtureView) ComponentKeys(_ context.Context, p KeyPredicate, _ Cursor, _ ReadBudget) (KeyPage, error) {
	out := KeyPage{View: v.id, Version: 1, Complete: true}
	for key := range v.components {
		if key.Owner == p.Owner && (p.Life == 0 || key.Life == p.Life) && (p.Kind == 0 || key.Kind == p.Kind) && (p.Name == "" || key.Name == p.Name) {
			out.Keys = append(out.Keys, key)
		}
	}
	return out, nil
}
func (v *fixtureView) UniqueCandidates(_ context.Context, p UniquePredicate, _ Cursor, _ ReadBudget) (ClaimPage, error) {
	out := ClaimPage{View: v.id, Version: 1, Complete: true}
	for key, s := range v.components {
		if key.Name != p.Definition.Name || (key.Kind != ScalarProperty && key.Kind != SetMember) {
			continue
		}
		found := false
		for _, piece := range s.Pieces() {
			if !piece.Cell().Present() {
				continue
			}
			id := key.Member
			if key.Kind == ScalarProperty {
				if piece.Cell().Value().IsNull() {
					continue
				}
				id = ValueID(piece.Cell().Value().ID())
			}
			same, err := v.values[id].Equal(p.Value, Limits{})
			if err != nil {
				return ClaimPage{}, err
			}
			found = found || same
		}
		if found {
			out.Claims = append(out.Claims, UniqueClaim{key.Owner, key.Life, key})
		}
	}
	return out, nil
}
func (v *fixtureView) IncidentRelationships(_ context.Context, p IncidentPredicate, _ Cursor, _ ReadBudget) (EntityPage, error) {
	out := EntityPage{View: v.id, Version: 1, Complete: true}
	for id, r := range v.entities {
		if r.Kind == Relationship && (r.Source == p.Endpoint || r.Target == p.Endpoint) {
			out.Entities = append(out.Entities, id)
		}
	}
	return out, nil
}
func (v *fixtureView) clone() *fixtureView {
	x := *v
	x.entities = maps.Clone(v.entities)
	x.lives = maps.Clone(v.lives)
	x.values = maps.Clone(v.values)
	x.components = maps.Clone(v.components)
	x.defs = maps.Clone(v.defs)
	return &x
}
func (v *fixtureView) install(t testing.TB, d Delta) {
	t.Helper()
	for _, r := range d.Entities {
		v.entities[r.ID] = r
	}
	for _, r := range d.Lives {
		v.lives[lifeKey{r.Owner, r.Life}] = r
	}
	for _, r := range d.Values {
		v.values[r.ID] = r.Value
	}
	for _, patch := range d.Patches {
		s, ok := v.components[patch.Key]
		if !ok {
			var err error
			s, err = state.New(patch.Owned.Axis(), state.Limits{})
			if err != nil {
				t.Fatal(err)
			}
		}
		before := s
		replayed := s
		for _, change := range patch.Changes {
			inside, err := subset(change.Scope(), patch.Owned, temporal.Limits{})
			if err != nil || !inside {
				t.Fatal("CDC escaped owned coverage", err)
			}
			coverage := []temporal.Scope{}
			for _, old := range before.Pieces() {
				common, err := old.Scope().Intersection(change.Scope(), temporal.Limits{})
				if err != nil {
					t.Fatal(err)
				}
				if common.Kind() == temporal.ScopeEmpty {
					continue
				}
				if old.Cell() != change.Before() {
					t.Fatal("CDC before image mismatch")
				}
				coverage = append(coverage, common)
			}
			if change.Before() != (state.Cell{}) {
				ok, err := coveredByAtoms(change.Scope(), coverage, temporal.Limits{})
				if err != nil || !ok {
					t.Fatal("missing CDC before coverage", err)
				}
			} else if len(coverage) != 0 {
				t.Fatal("phantom never-asserted before")
			}
			r, err := applyCell(replayed, change.Scope(), change.After(), state.Limits{})
			if err != nil {
				t.Fatal(err)
			}
			replayed = r.State()
		}
		coverage := []temporal.Scope{}
		for _, piece := range patch.State.Pieces() {
			inside, err := subset(piece.Scope(), patch.Owned, temporal.Limits{})
			if err != nil || !inside {
				t.Fatal("after-state escaped owned coverage", err)
			}
			coverage = append(coverage, piece.Scope())
			r, err := applyCell(s, piece.Scope(), piece.Cell(), state.Limits{})
			if err != nil {
				t.Fatal(err)
			}
			s = r.State()
		}
		covered, err := coveredByAtoms(patch.Owned, coverage, temporal.Limits{})
		if err != nil || !covered {
			t.Fatal("after-state missing owned support", err)
		}
		left, right := s.Pieces(), replayed.Pieces()
		if len(left) != len(right) {
			t.Fatal("state/CDC piece count mismatch")
		}
		for i := range left {
			same, err := left[i].Scope().SameSupport(right[i].Scope(), temporal.Limits{})
			if err != nil || !same || left[i].Cell() != right[i].Cell() {
				t.Fatal("state/CDC parity", err)
			}
		}
		v.components[patch.Key] = s
	}
}
func testPosition(t testing.TB, a temporal.Axis, n int64) temporal.Position {
	t.Helper()
	p, err := temporal.IntegerPosition(a, temporal.Int64(n))
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func testSpan(t testing.TB, a temporal.Axis, lo, hi int64) temporal.Scope {
	t.Helper()
	lb, _ := temporal.FiniteBound(testPosition(t, a, lo), true)
	hb, _ := temporal.FiniteBound(testPosition(t, a, hi), false)
	s, err := temporal.Span(a, lb, hb, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func fixtureScope(t testing.TB, a temporal.Axis, raw []json.RawMessage) temporal.Scope {
	t.Helper()
	if raw == nil {
		return temporal.Scope{}
	}
	if len(raw) != 2 {
		t.Fatal(raw)
	}
	bounds := make([]temporal.Bound, 2)
	for i, b := range raw {
		var symbol string
		if json.Unmarshal(b, &symbol) == nil {
			switch symbol {
			case "-inf":
				bounds[i] = temporal.NegativeInfinity()
			case "+inf":
				bounds[i] = temporal.PositiveInfinity()
			default:
				t.Fatal(symbol)
			}
		} else {
			var n int64
			if err := json.Unmarshal(b, &n); err != nil {
				t.Fatal(err)
			}
			bounds[i], _ = temporal.FiniteBound(testPosition(t, a, n), i == 0)
		}
	}
	s, err := temporal.Span(a, bounds[0], bounds[1], temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

type fixtureOperation struct {
	Op, ID, Owner, Source, Target, Type, Key, Label string
	Life, SourceLife, TargetLife                    uint64
	Valid                                           []json.RawMessage
	Value                                           json.RawMessage
	Present                                         bool
	Properties                                      map[string]json.RawMessage
	Labels                                          []string
}
type fixtureStep struct {
	System      string
	Operations  []fixtureOperation
	Valid       int64
	ExpectError string `json:"expect_error"`
	Expect      json.RawMessage
}
type fixtureCase struct {
	ID     string
	Schema struct {
		Properties map[string]struct {
			Owner, Type, Cardinality string
			Unique                   bool
		}
	}
	Steps []fixtureStep
}

func fixtureScalar(t testing.TB, raw json.RawMessage) Scalar {
	t.Helper()
	if bytes.Equal(raw, []byte("null")) {
		return Null()
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return String(s)
	}
	var n int64
	if json.Unmarshal(raw, &n) == nil {
		return I64(n)
	}
	var b bool
	if json.Unmarshal(raw, &b) == nil {
		return Bool(b)
	}
	var f float64
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	v, err := F64(f)
	if err != nil {
		t.Fatal(err)
	}
	return v
}
func fixtureError(code string) error {
	switch code {
	case "NOT_FOUND":
		return ErrNotFound
	case "ALREADY_EXISTS":
		return ErrAlreadyExists
	case "OWNER_VALIDITY":
		return ErrOwnerValidity
	case "LIFECYCLE_OVERLAP":
		return ErrLifecycleOverlap
	case "TYPE_MISMATCH":
		return ErrTypeMismatch
	case "SCHEMA_MISMATCH":
		return ErrSchemaMismatch
	case "UNIQUE_OVERLAP":
		return ErrUniqueOverlap
	case "VALIDITY_REQUIRED":
		return ErrValidityRequired
	case "INVALID_INTERVAL":
		return ErrEmptyMutation
	default:
		return nil
	}
}
func TestOriginalSixteenFixturesThroughProductionPlanner(t *testing.T) {
	data, err := os.ReadFile("testdata/temporal-fixtures.json")
	if err != nil {
		t.Fatal(err)
	}
	// Byte-identical copy of the historical handoff; module tests cannot depend
	// on parent-repository docs that are absent from a published /v5 module zip.
	digest := sha256.Sum256(data)
	if hex.EncodeToString(digest[:]) != "0598d593bd39ccf95ea2737bb55b0358631cfc6009a03f35b8f39101a7a4f6f2" {
		t.Fatal("historical fixture corpus drift")
	}
	var corpus struct{ Cases []fixtureCase }
	if err := json.Unmarshal(data, &corpus); err != nil {
		t.Fatal(err)
	}
	if len(corpus.Cases) != 16 {
		t.Fatal(len(corpus.Cases))
	}
	assertions := 0
	for _, c := range corpus.Cases {
		t.Run(c.ID, func(t *testing.T) {
			v := newFixtureView(t)
			for name, d := range c.Schema.Properties {
				kind := ScalarString
				switch d.Type {
				case "Bool":
					kind = ScalarBool
				case "I64":
					kind = ScalarI64
				case "F64":
					kind = ScalarF64
				}
				owner := Node
				if d.Owner == "edge" {
					owner = Relationship
				}
				card := ScalarCardinality
				if d.Cardinality == "set" {
					card = SetCardinality
				}
				unique := UniqueNone
				if d.Unique {
					unique = UniqueScalar
				}
				v.defs[name] = PropertyDefinition{name, owner, kind, card, unique}
			}
			snapshots := map[string]*fixtureView{"EMPTY": v.clone()}
			ids := map[string]EntityID{}
			names := map[EntityID]string{}
			nextEntity := EntityID(1)
			nextValue := ValueID(1)
			sequence := uint64(1)
			identify := func(name string) EntityID {
				id, ok := ids[name]
				if !ok {
					id = nextEntity
					nextEntity++
					ids[name] = id
					names[id] = name
				}
				return id
			}
			for _, step := range c.Steps {
				if step.Operations != nil {
					ops := []Operation{}
					for _, raw := range step.Operations {
						ownerName := raw.Owner
						if ownerName == "" {
							ownerName = raw.ID
						}
						owner := identify(ownerName)
						life := LifeID(raw.Life)
						if life == 0 {
							life = 1
						}
						scope := fixtureScope(t, v.axis, raw.Valid)
						op := Operation{Owner: owner, Life: life, Scope: scope, Name: raw.Key, Present: raw.Present}
						switch raw.Op {
						case "create_vertex":
							op.Kind = CreateNode
						case "create_edge":
							op.Kind = CreateRelationship
							op.Record = EntityRecord{Type: raw.Type, Source: identify(raw.Source), Target: identify(raw.Target), Mode: LifeBound}
							if op.Record.Type == "" {
								op.Record.Type = "LINK"
							}
							sl, tl := LifeID(raw.SourceLife), LifeID(raw.TargetLife)
							if sl == 0 {
								sl = 1
							}
							if tl == 0 {
								tl = 1
							}
							op.Binding = LifeRecord{SourceLife: sl, TargetLife: tl}
						case "reopen":
							op.Kind = Reopen
							op.Life = 1
							for key := range v.lives {
								if key.owner == owner && key.life >= op.Life {
									op.Life = key.life + 1
								}
							}
							op.Binding = LifeRecord{SourceLife: LifeID(raw.SourceLife), TargetLife: LifeID(raw.TargetLife)}
						case "close":
							op.Kind = Close
						case "correct":
							op.Kind = Correct
						case "set":
							op.Kind = Set
						case "unset":
							op.Kind = Unset
						case "add":
							op.Kind = Add
						case "remove":
							op.Kind = Remove
						case "add_label":
							op.Kind = AddLabel
							op.Name = raw.Label
						case "remove_label":
							op.Kind = RemoveLabel
							op.Name = raw.Label
						default:
							t.Fatal(raw.Op)
						}
						if raw.Value != nil {
							op.Value = fixtureScalar(t, raw.Value)
							op.ValueID = nextValue
							nextValue++
						}
						ops = append(ops, op)
						for name, value := range raw.Properties {
							ops = append(ops, Operation{Kind: Set, Owner: owner, Life: op.Life, Scope: scope, Name: name, Value: fixtureScalar(t, value), ValueID: nextValue})
							nextValue++
						}
						for _, label := range raw.Labels {
							ops = append(ops, Operation{Kind: AddLabel, Owner: owner, Life: op.Life, Scope: scope, Name: label})
						}
					}
					rev, _ := state.NewRevision(sequence, 0)
					sequence++
					delta, err := Plan(t.Context(), v, ops, rev, Limits{})
					if step.ExpectError != "" {
						assertions++
						if !errors.Is(err, fixtureError(step.ExpectError)) {
							t.Fatalf("%s expected%s got%v", step.System, step.ExpectError, err)
						}
						continue
					}
					if err != nil {
						t.Fatalf("%s %v", step.System, err)
					}
					v.install(t, delta)
					v.id[0]++
					snapshots[step.System] = v.clone()
				} else {
					assertions++
					snapshot := snapshots[step.System]
					actual := map[string]any{"nodes": map[string]any{}, "edges": map[string]any{}}
					for id := range snapshot.entities {
						projection, err := Project(t.Context(), snapshot, id, testPosition(t, snapshot.axis, step.Valid), Effective, Limits{})
						if err != nil {
							t.Fatal(err)
						}
						if !projection.Active {
							continue
						}
						props := map[string]any{}
						for _, property := range projection.Properties {
							if property.Cardinality == SetCardinality {
								members := []any{}
								for _, value := range property.Members {
									members = append(members, scalarJSON(value))
								}
								props[property.Name] = members
							} else {
								props[property.Name] = scalarJSON(property.Scalar)
							}
						}
						if projection.Record.Kind == Node {
							labels := slices.Clone(projection.Labels)
							if labels == nil {
								labels = []string{}
							}
							actual["nodes"].(map[string]any)[names[id]] = map[string]any{"life": uint64(projection.Life), "properties": props, "labels": labels}
						} else {
							actual["edges"].(map[string]any)[names[id]] = map[string]any{"life": uint64(projection.Life), "properties": props, "source": names[projection.Record.Source], "target": names[projection.Record.Target], "type": projection.Record.Type}
						}
					}
					encoded, _ := json.Marshal(actual)
					var want, got any
					if err := json.Unmarshal(step.Expect, &want); err != nil {
						t.Fatal(err)
					}
					if err := json.Unmarshal(encoded, &got); err != nil {
						t.Fatal(err)
					}
					wantBytes, _ := json.Marshal(want)
					gotBytes, _ := json.Marshal(got)
					if !bytes.Equal(wantBytes, gotBytes) {
						t.Fatalf("%s@%d\nwant%s\ngot%s", step.System, step.Valid, wantBytes, gotBytes)
					}
				}
			}
		})
	}
	if assertions != 52 {
		t.Fatal("fixture assertions", assertions)
	}
}
func scalarJSON(s Scalar) any {
	switch s.Kind() {
	case ScalarNull:
		return nil
	case ScalarString:
		v, _ := s.StringValue()
		return v
	case ScalarBool:
		v, _ := s.BoolValue()
		return v
	case ScalarI64:
		v, _ := s.Int64Value()
		return v
	case ScalarF64:
		v, _ := s.Float64Value()
		return v
	default:
		return nil
	}
}
