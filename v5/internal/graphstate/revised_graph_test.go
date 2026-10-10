package graphstate

import (
	"cmp"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// Named map copies are supplied immutable fixture views, never durable stores,
// certified cuts, transaction validation or public snapshot/feed implementations.
type revisedGraphAdapter struct {
	record    revisedRecord
	graph     revisedGraph
	view      *fixtureView
	ids       map[string]EntityID
	names     map[EntityID]string
	nextValue ValueID
	refs      *revisedReferences
}

func revisedTranslateGraph(record revisedRecord, graph revisedGraph) (*revisedGraphAdapter, error) {
	for _, operations := range revisedGraphOperationGroups(graph) {
		for _, operation := range operations {
			if _, _, err := revisedDeclarations(operation); err != nil {
				return nil, err
			}
			for _, value := range operation.Properties {
				if _, err := revisedAttachedScalar(record, value); err != nil {
					return nil, err
				}
			}
			if operation.Value != nil {
				if _, err := revisedAttachedScalar(record, operation.Value); err != nil {
					return nil, err
				}
			}
		}
	}
	axis, err := revisedAxisValue(graph.Axis)
	if err != nil {
		return nil, err
	}
	// The mathematical graph record does not declare a schema. These explicit
	// fixture schemas constrain the native lane to its literal scalar subset;
	// present-null is valid in the declared nullable name/string component.
	definitions := map[ownerSchemaKey]PropertyDefinition{}
	switch record.ID {
	case "E19-close-reopen-identity-reference":
		definitions[ownerSchemaKey{Node, "name"}] = PropertyDefinition{"name", Node, ScalarString, ScalarCardinality, UniqueNone}
	case "E19-strict-life-bound-presence-correction":
		definitions[ownerSchemaKey{Relationship, "v"}] = PropertyDefinition{"v", Relationship, ScalarI64, ScalarCardinality, UniqueNone}
	case "SEM03-06-node-rel-property-history":
		definitions[ownerSchemaKey{Node, "name"}] = PropertyDefinition{"name", Node, ScalarString, ScalarCardinality, UniqueNone}
		definitions[ownerSchemaKey{Relationship, "weight"}] = PropertyDefinition{"weight", Relationship, ScalarI64, ScalarCardinality, UniqueNone}
	case "E02-disjoint-native-region":
		for _, kind := range []EntityKind{Node, Relationship} {
			definitions[ownerSchemaKey{kind, "v"}] = PropertyDefinition{"v", kind, ScalarString, ScalarCardinality, UniqueNone}
		}
	case "E01-event-multiplicity", "E01-E03-interpretation-independent-of-support-Z", "E07-causal-records":
	case "SEM14-16-endpoint-boundary":
		definitions[ownerSchemaKey{Node, "x"}] = PropertyDefinition{"x", Node, ScalarI64, ScalarCardinality, UniqueNone}
	case "E06-preservation", "E10-preservation", "E13-preservation", "E14-preservation", "E20-preservation":
		definitions[ownerSchemaKey{Node, "descriptor"}] = PropertyDefinition{"descriptor", Node, ScalarDescriptor, ScalarCardinality, UniqueNone}
	default:
		return nil, fmt.Errorf("%w: no reviewed complete graph binding for %s", errRevisedPending, record.ID)
	}
	view := &fixtureView{axis: axis, id: ViewID{1}, entities: map[EntityID]EntityRecord{}, lives: map[lifeKey]LifeRecord{}, values: map[ValueID]Scalar{}, components: map[ComponentKey]state.State{}, defs: definitions}
	return &revisedGraphAdapter{record: record, graph: graph, view: view, ids: map[string]EntityID{}, names: map[EntityID]string{}, nextValue: 1, refs: newRevisedReferences()}, nil
}

func revisedGraphOperationGroups(graph revisedGraph) [][]revisedOperation {
	groups := make([][]revisedOperation, 0, len(graph.Steps)+len(graph.Failures))
	for _, step := range graph.Steps {
		groups = append(groups, step.Operations)
	}
	for _, failure := range graph.Failures {
		groups = append(groups, failure.Operations)
	}
	return groups
}

func (adapter *revisedGraphAdapter) identify(name string) (EntityID, error) {
	if name == "" {
		return 0, errors.New("revised adapter: missing entity name")
	}
	if id := adapter.ids[name]; id != 0 {
		return id, nil
	}
	id := EntityID(len(adapter.ids) + 1)
	adapter.ids[name], adapter.names[id] = id, name
	return id, nil
}

func revisedMutationScope(operation revisedOperation, axis temporal.Axis, definition revisedAxis) (temporal.Scope, error) {
	lowerClosed, upperClosed := true, false
	if operation.LowerClosed != nil {
		lowerClosed = *operation.LowerClosed
	}
	if operation.UpperClosed != nil {
		upperClosed = *operation.UpperClosed
	}
	var pair []json.RawMessage
	if err := revisedStrictJSON(operation.Valid, &pair); err == nil {
		if len(pair) != 2 {
			return temporal.Scope{}, errors.New("revised adapter: interval needs two endpoints")
		}
		return revisedSpan(axis, revisedPiece{Lower: pair[0], Upper: pair[1], LowerClosed: lowerClosed, UpperClosed: upperClosed}, temporal.Limits{})
	}
	var region struct {
		Kind   string `json:"kind"`
		Pieces []struct {
			Lower       json.RawMessage `json:"lower"`
			Upper       json.RawMessage `json:"upper"`
			LowerClosed *bool           `json:"lower_closed"`
			UpperClosed *bool           `json:"upper_closed"`
			Axis        string          `json:"axis"`
		} `json:"pieces"`
	}
	if err := revisedStrictJSON(operation.Valid, &region); err != nil {
		return temporal.Scope{}, err
	}
	if region.Kind != "region" {
		return temporal.Scope{}, fmt.Errorf("%w: symbolic/native-inexpressible placement %s", ErrUnsupported, region.Kind)
	}
	parts := make([]temporal.Scope, len(region.Pieces))
	for i, piece := range region.Pieces {
		if piece.Axis != "" && piece.Axis != definition.Identity {
			return temporal.Scope{}, temporal.ErrAxisMismatch
		}
		lc, hc := true, false
		if piece.LowerClosed != nil {
			lc = *piece.LowerClosed
		}
		if piece.UpperClosed != nil {
			hc = *piece.UpperClosed
		}
		value, err := revisedSpan(axis, revisedPiece{Lower: piece.Lower, Upper: piece.Upper, LowerClosed: lc, UpperClosed: hc}, temporal.Limits{})
		if err != nil {
			return temporal.Scope{}, err
		}
		parts[i] = value
	}
	return temporal.Region(axis, parts, temporal.Limits{})
}

func revisedSingleFixtureLife(view *fixtureView, owner EntityID) (LifeID, error) {
	life := LifeID(0)
	for key := range view.lives {
		if key.owner == owner {
			if life != 0 && life != key.life {
				return 0, fmt.Errorf("%w: ambiguous implicit fixture LifeID", errRevisedPending)
			}
			life = key.life
		}
	}
	// Missing references are passed as nonzero unresolved references so the
	// production planner, rather than the adapter, returns ErrNotFound.
	if life == 0 {
		life = 1
	}
	return life, nil
}

func (adapter *revisedGraphAdapter) operations(view *fixtureView, raw []revisedOperation) ([]Operation, error) {
	operations := []Operation{}
	createdLives := map[EntityID]LifeID{}
	for _, item := range raw {
		if err := revisedValidateOperation(item); err != nil {
			return nil, err
		}
		interpretation, role, err := revisedDeclarations(item)
		if err != nil {
			return nil, err
		}
		name := item.Owner
		if name == "" {
			name = item.ID
		}
		owner, err := adapter.identify(name)
		if err != nil {
			return nil, err
		}
		life := LifeID(item.Life)
		if life == 0 {
			life = createdLives[owner]
			if life == 0 {
				life, err = revisedSingleFixtureLife(view, owner)
				if err != nil {
					return nil, err
				}
			}
		}
		operationAxis, operationDefinition := view.axis, adapter.graph.Axis
		if item.Axis != "" {
			operationDefinition.Identity = item.Axis
			operationAxis, err = revisedAxisValue(operationDefinition)
			if err != nil {
				return nil, err
			}
		}
		scope, err := revisedMutationScope(item, operationAxis, operationDefinition)
		if err != nil {
			return nil, err
		}
		operation := Operation{Owner: owner, Life: life, Scope: scope, Name: item.Key, Record: EntityRecord{Interpretation: interpretation, TemporalRole: role}}
		switch item.Op {
		case "create_vertex":
			operation.Kind = CreateNode
			createdLives[owner] = life
		case "create_edge":
			operation.Kind = CreateRelationship
			source, err := adapter.identify(item.Source)
			if err != nil {
				return nil, err
			}
			target, err := adapter.identify(item.Target)
			if err != nil {
				return nil, err
			}
			mode := LifeBound
			switch item.ReferenceMode {
			case "", "life_bound":
			case "identity_reference":
				mode = IdentityReference
			default:
				return nil, errors.New("revised adapter: unknown reference mode")
			}
			operation.Record = EntityRecord{Type: item.Type, Source: source, Target: target, Mode: mode, Interpretation: interpretation, TemporalRole: role}
			if mode == LifeBound {
				sourceLife, targetLife := createdLives[source], createdLives[target]
				if sourceLife == 0 {
					sourceLife, err = revisedSingleFixtureLife(view, source)
					if err != nil {
						return nil, err
					}
				}
				if targetLife == 0 {
					targetLife, err = revisedSingleFixtureLife(view, target)
					if err != nil {
						return nil, err
					}
				}
				operation.Binding = LifeRecord{SourceLife: sourceLife, TargetLife: targetLife}
			}
			createdLives[owner] = life
		case "reopen":
			operation.Kind = Reopen
		case "close":
			operation.Kind = Close
		case "correct":
			operation.Kind = Correct
			operation.Present = true
			if item.Present != nil {
				operation.Present = *item.Present
			}
		case "set":
			operation.Kind = Set
		case "unset":
			operation.Kind = Unset
		case "add":
			operation.Kind = Add
		case "remove":
			operation.Kind = Remove
		case "add_label":
			operation.Kind, operation.Name = AddLabel, item.Label
		case "remove_label":
			operation.Kind, operation.Name = RemoveLabel, item.Label
		}
		if item.Value != nil {
			operation.Value, err = revisedAttachedScalar(adapter.record, item.Value)
			if err != nil {
				return nil, err
			}
			operation.ValueID = adapter.nextValue
			adapter.nextValue++
		}
		operations = append(operations, operation)
		for _, key := range slices.Sorted(maps.Keys(item.Properties)) {
			value, err := revisedAttachedScalar(adapter.record, item.Properties[key])
			if err != nil {
				return nil, err
			}
			operations = append(operations, Operation{Kind: Set, Owner: owner, Life: life, Scope: scope, Name: key, Value: value, ValueID: adapter.nextValue})
			adapter.nextValue++
		}
		for _, label := range item.Labels {
			operations = append(operations, Operation{Kind: AddLabel, Owner: owner, Life: life, Scope: scope, Name: label})
		}
	}
	return operations, nil
}

func revisedProjectedScalar(value Scalar) (any, error) {
	switch value.Kind() {
	case ScalarNull:
		return nil, nil
	case ScalarString:
		result, _ := value.StringValue()
		return result, nil
	case ScalarBool:
		result, _ := value.BoolValue()
		return result, nil
	case ScalarDescriptor:
		d, ok := value.Descriptor()
		if !ok {
			return nil, ErrInvalidInput
		}
		var result any
		if err := revisedStrictJSON(d.Spec().Payload, &result); err != nil {
			return nil, err
		}
		return result, nil
	case ScalarI64:
		result, _ := value.Int64Value()
		return result, nil
	default:
		return nil, fmt.Errorf("revised adapter: unsupported output scalar kind %d", value.Kind())
	}
}

func (adapter *revisedGraphAdapter) project(t testing.TB, view *fixtureView, read revisedRead) any {
	t.Helper()
	position, err := revisedPosition(view.axis, read.Coordinate, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	mode := Visibility(0)
	switch read.Mode {
	case "effective":
		mode = Effective
	case "declared":
		mode = Declared
	default:
		t.Fatal("unknown view", read.Mode)
	}
	result := map[string]map[string]any{"nodes": {}, "edges": {}}
	for _, id := range slices.Sorted(maps.Keys(view.entities)) {
		projection, err := Project(t.Context(), view, id, position, mode, Limits{})
		if err != nil {
			t.Fatal(err)
		}
		if !projection.Active {
			continue
		}
		properties := map[string]any{}
		for _, property := range projection.Properties {
			switch property.Cardinality {
			case ScalarCardinality:
				value, err := revisedProjectedScalar(property.Scalar)
				if err != nil {
					t.Fatal(err)
				}
				properties[property.Name] = value
			case SetCardinality:
				members := []any{}
				for _, member := range property.Members {
					value, err := revisedProjectedScalar(member)
					if err != nil {
						t.Fatal(err)
					}
					members = append(members, value)
				}
				properties[property.Name] = members
			default:
				t.Fatal("unknown output cardinality")
			}
		}
		row := map[string]any{"life": uint64(projection.Life), "properties": properties}
		if projection.Record.Interpretation != 0 {
			row["interpretation"] = revisedInterpretationName(projection.Record.Interpretation)
		}
		if projection.Record.TemporalRole != 0 {
			row["temporal_role"] = revisedRoleName(projection.Record.TemporalRole)
		}
		if projection.Record.Kind == Node {
			labels := slices.Clone(projection.Labels)
			if labels == nil {
				labels = []string{}
			}
			row["labels"] = labels
			result["nodes"][adapter.names[id]] = row
		} else {
			row["source"], row["target"], row["type"] = adapter.names[projection.Record.Source], adapter.names[projection.Record.Target], projection.Record.Type
			if read.LifecycleStatus {
				statuses := []any{}
				for _, status := range projection.Endpoints {
					var activeLife, boundLife any
					if status.Active {
						activeLife = uint64(status.Life)
					}
					if status.BoundLife != 0 {
						boundLife = uint64(status.BoundLife)
					}
					statuses = append(statuses, map[string]any{"identity": adapter.names[status.ID], "exists": status.Known, "active_life": activeLife, "bound_life": boundLife, "bound_life_active": status.Active && status.BoundLife != 0 && status.Life == status.BoundLife})
				}
				row["endpoint_status"] = statuses
				switch projection.Record.Mode {
				case LifeBound:
					row["reference_mode"] = "life_bound"
				case IdentityReference:
					row["reference_mode"] = "identity_reference"
				default:
					t.Fatal("unknown output reference mode")
				}
			}
			result["edges"][adapter.names[id]] = row
		}
	}
	return result
}

func revisedViewDigest(t testing.TB, view *fixtureView) [32]byte {
	t.Helper()
	type componentRecord struct {
		Key    ComponentKey
		Pieces []any
	}
	records := []componentRecord{}
	keys := slices.Collect(maps.Keys(view.components))
	slices.SortFunc(keys, compareKey)
	for _, key := range keys {
		pieces := []any{}
		for _, piece := range view.components[key].Pieces() {
			wire, err := temporal.AppendScope(nil, piece.Scope(), temporal.Limits{})
			if err != nil {
				t.Fatal(err)
			}
			cell := piece.Cell()
			pieces = append(pieces, map[string]any{"scope": wire, "present": cell.Present(), "null": cell.Value().IsNull(), "payload": cell.Value().ID(), "bytes": cell.Value().PayloadBytes(), "revision": cell.Revision().ID(), "provenance": cell.Revision().Provenance()})
		}
		records = append(records, componentRecord{key, pieces})
	}
	lives := []LifeRecord{}
	for _, life := range view.lives {
		lives = append(lives, life)
	}
	slices.SortFunc(lives, func(a, b LifeRecord) int {
		return cmp.Or(cmp.Compare(a.Owner, b.Owner), cmp.Compare(a.Life, b.Life))
	})
	entities := []any{}
	for _, id := range slices.Sorted(maps.Keys(view.entities)) {
		record := view.entities[id]
		entities = append(entities, map[string]any{"id": record.ID, "kind": record.Kind, "axis": record.Axis.Descriptor(), "type": record.Type, "source": record.Source, "target": record.Target, "mode": record.Mode, "interpretation": record.Interpretation, "temporal_role": record.TemporalRole})
	}
	values := []any{}
	for _, id := range slices.Sorted(maps.Keys(view.values)) {
		key, err := view.values[id].EqualityKey(Limits{})
		if err != nil {
			t.Fatal(err)
		}
		values = append(values, map[string]any{"id": id, "kind": view.values[id].Kind(), "value": view.values[id].Render(), "canonical": key})
	}
	raw, err := json.Marshal(struct {
		Entities   []any
		Lives      []LifeRecord
		Values     []any
		Components []componentRecord
	}{entities, lives, values, records})
	if err != nil {
		t.Fatal(err)
	}
	return sha256.Sum256(raw)
}

func revisedGraphError(code string) error {
	switch code {
	case "GRAPH_MISMATCH":
		return ErrNamespace
	case "AXIS_MISMATCH":
		return temporal.ErrAxisMismatch
	case "NOT_FOUND":
		return ErrNotFound
	case "OWNER_VALIDITY":
		return ErrOwnerValidity
	case "LIFECYCLE_OVERLAP":
		return ErrLifecycleOverlap
	case "LIFE_EXISTS":
		return ErrAlreadyExists
	default:
		return nil
	}
}

func revisedSemanticChangeGroups(delta Delta) int {
	// The independent model groups one possibly disjoint region by complete
	// before/after Cell. Go atomic fragments are normalized into those semantic
	// groups for the fixture count; this is not a CDC envelope acceptance test.
	type key struct {
		Component     ComponentKey
		Before, After state.Cell
	}
	groups := map[key]bool{}
	for _, patch := range delta.Patches {
		for _, change := range patch.Changes {
			groups[key{patch.Key, change.Before(), change.After()}] = true
		}
	}
	return len(groups)
}

func revisedRunGraph(t *testing.T, record revisedRecord) {
	graph, err := revisedGraphRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := revisedTranslateGraph(record, graph)
	if err != nil {
		t.Fatal("native record cannot be mapped faithfully", err)
	}
	snapshots := map[string]*fixtureView{}
	digests := map[string][32]byte{}
	for _, step := range graph.Steps {
		operations, err := adapter.operations(adapter.view, step.Operations)
		if err != nil {
			t.Fatal(err)
		}
		revision, err := adapter.refs.revision(new(step.Revision), new(step.Provenance))
		if err != nil {
			t.Fatal(err)
		}
		before := revisedViewDigest(t, adapter.view)
		delta, err := PlanQualified(t.Context(), adapter.view, revisedOperationGraph(adapter.view.Graph(), step.Operations), operations, revision, Limits{})
		if err != nil {
			t.Fatal(step.Snapshot, err)
		}
		if before != revisedViewDigest(t, adapter.view) {
			t.Fatal("Plan mutated its immutable input")
		}
		if revisedSemanticChangeGroups(delta) != step.ChangeCount {
			t.Fatalf("%s complete-cell change group count: got %d want %d", step.Snapshot, revisedSemanticChangeGroups(delta), step.ChangeCount)
		}
		adapter.view.install(t, delta)
		adapter.view.id[0]++
		snapshots[step.Snapshot] = adapter.view.clone()
		digests[step.Snapshot] = revisedViewDigest(t, snapshots[step.Snapshot])
	}
	// All historical/current reads happen after all mutations. A current-state
	// substitution would change the exact property/label/entity/lifecycle sets.
	for _, read := range graph.Reads {
		view, exists := snapshots[read.Snapshot]
		if !exists {
			t.Fatal("unknown named fixture view", read.Snapshot)
		}
		revisedJSONEqual(t, read.Expected, adapter.project(t, view, read))
	}
	for _, failure := range graph.Failures {
		view := snapshots[failure.BaseSnapshot]
		if view == nil {
			t.Fatal("unknown failure view")
		}
		operations, err := adapter.operations(view, failure.Operations)
		if err != nil {
			t.Fatal(err)
		}
		revision, err := adapter.refs.revision(new("failed:revision"), new("failed:provenance"))
		if err != nil {
			t.Fatal(err)
		}
		delta, err := PlanQualified(t.Context(), view, revisedOperationGraph(view.Graph(), failure.Operations), operations, revision, Limits{})
		want := revisedGraphError(failure.ExpectedError)
		if want == nil || !errors.Is(err, want) {
			t.Fatal("exact native failure", failure.ExpectedError, err)
		}
		if !failure.ExpectedAtomic || len(delta.Entities)+len(delta.Lives)+len(delta.Values)+len(delta.Patches)+len(delta.Dependencies) != 0 {
			t.Fatal("failure exposed a partial delta", delta)
		}
		for name, snapshot := range snapshots {
			if revisedViewDigest(t, snapshot) != digests[name] {
				t.Fatal("failure mutated current/earlier view", name)
			}
		}
	}
	// Failed reference creation must not introduce a phantom entity.
	for _, read := range graph.Reads {
		revisedJSONEqual(t, read.Expected, adapter.project(t, snapshots[read.Snapshot], read))
	}
}

func TestRevisedPendingGraphBindingsAreRejectedRatherThanWeaklyTranslated(t *testing.T) {
	for _, record := range revisedLoad(t) {
		rule := revisedRules[record.ID]
		if record.Kind != "graph" || rule.lane != revisedPending {
			continue
		}
		graph, err := revisedGraphRecord(record)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := revisedTranslateGraph(record, graph); !errors.Is(err, errRevisedPending) {
			t.Fatalf("missing graph binding must remain explicit: %s: %v", record.ID, err)
		}
	}
}

func TestRevisedNativeSetWideUnsetPreservesOldNodeAndRelationshipMembership(t *testing.T) {
	// This typed native counterpart is not one of the 42 reference records:
	// Python's set-wide Unset mapping is a separate limitation of that model.
	view := newFixtureView(t)
	for _, kind := range []EntityKind{Node, Relationship} {
		view.defs[ownerSchemaKey{kind, "tags"}] = PropertyDefinition{"tags", kind, ScalarString, SetCardinality, UniqueNone}
	}
	whole, middle := testSpan(t, view.axis, 0, 10), testSpan(t, view.axis, 3, 7)
	commitOps(t, view, 1,
		Operation{Kind: CreateNode, Owner: 1, Life: 1, Scope: whole},
		Operation{Kind: CreateNode, Owner: 2, Life: 1, Scope: whole},
		Operation{Kind: CreateRelationship, Owner: 3, Life: 1, Scope: whole, Record: EntityRecord{Type: "R", Source: 1, Target: 2, Mode: LifeBound}, Binding: LifeRecord{SourceLife: 1, TargetLife: 1}},
		Operation{Kind: Add, Owner: 1, Life: 1, Scope: whole, Name: "tags", Value: String("A"), ValueID: 1},
		Operation{Kind: Add, Owner: 1, Life: 1, Scope: whole, Name: "tags", Value: String("B"), ValueID: 2},
		Operation{Kind: Add, Owner: 3, Life: 1, Scope: whole, Name: "tags", Value: String("A"), ValueID: 3},
		Operation{Kind: Add, Owner: 3, Life: 1, Scope: whole, Name: "tags", Value: String("B"), ValueID: 4},
	)
	old := view.clone()
	commitOps(t, view, 2, Operation{Kind: Unset, Owner: 1, Life: 1, Scope: middle, Name: "tags"}, Operation{Kind: Unset, Owner: 3, Life: 1, Scope: middle, Name: "tags"})
	for _, owner := range []EntityID{1, 3} {
		for _, check := range []struct {
			snapshot *fixtureView
			at       int64
			want     []string
		}{{old, 5, []string{"A", "B"}}, {view, 5, []string{}}, {view, 8, []string{"A", "B"}}} {
			projection, err := Project(t.Context(), check.snapshot, owner, testPosition(t, view.axis, check.at), Effective, Limits{})
			if err != nil || !projection.Active {
				t.Fatal(err)
			}
			members := []string{}
			for _, property := range projection.Properties {
				if property.Name != "tags" || property.Cardinality != SetCardinality {
					t.Fatal("unexpected property", property)
				}
				for _, member := range property.Members {
					value, ok := member.StringValue()
					if !ok {
						t.Fatal("type lost")
					}
					members = append(members, value)
				}
			}
			if !slices.Equal(members, check.want) {
				t.Fatal("set-wide Unset leaked current/old membership", owner, members, check.want)
			}
		}
	}
}
