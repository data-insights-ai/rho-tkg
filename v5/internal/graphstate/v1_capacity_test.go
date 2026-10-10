package graphstate

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func TestV1RealMetadataAndCDCPolicyCapacityIsAtomic(t *testing.T) {
	var record revisedRecord
	for _, r := range revisedLoad(t) {
		if r.ID == "budget-variable-name-axis-change-ledger" {
			record = r
		}
	}
	definition := revisedMustField[revisedAxis](t, record, "axis")
	raw := revisedMustField[[]revisedOperation](t, record, "operations")
	axis, err := revisedAxisValue(definition)
	if err != nil {
		t.Fatal(err)
	}
	scope, err := revisedMutationScope(raw[0], axis, definition)
	if err != nil {
		t.Fatal(err)
	}
	var name string
	var value Scalar
	for key, payload := range raw[0].Properties {
		name = key
		value, err = revisedScalar(payload)
		if err != nil {
			t.Fatal(err)
		}
	}
	view := newFixtureView(t)
	view.axis = axis
	view.defs[ownerSchemaKey{Node, name}] = PropertyDefinition{name, Node, ScalarString, ScalarCardinality, UniqueNone}
	ops := []Operation{{Kind: CreateNode, Owner: 1, Life: 1, Scope: scope}, {Kind: Set, Owner: 1, Life: 1, Scope: scope, Name: name, Value: value, ValueID: 1}, {Kind: CreateNode, Owner: 2, Life: 1, Scope: scope}, {Kind: CreateRelationship, Owner: 3, Life: 1, Scope: scope, Record: EntityRecord{Type: "LINK", Source: 1, Target: 2, Mode: LifeBound}, Binding: LifeRecord{SourceLife: 1, TargetLife: 1}}}
	rev, _ := state.NewRevision(1, 8)
	before := revisedViewDigest(t, view)
	delta, err := Plan(t.Context(), view, ops, rev, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	// Measure actual Go representation ledgers and codec bytes, independently of
	// the fixture's JSON1354/2794. A complete state's and CDC's limits are separate.
	metadata, cdcMetadata, wireBytes, cdcWireBytes := 0, 0, 0, 0
	for _, patch := range delta.Patches {
		metadata += patch.State.Usage().MetadataBytes()
		w, err := state.AppendState(nil, patch.State, state.CodecLimits{})
		if err != nil {
			t.Fatal(err)
		}
		wireBytes += len(w)
		w, err = state.AppendChanges(nil, patch.Owned.Axis(), patch.Changes, state.CodecLimits{})
		if err != nil {
			t.Fatal(err)
		}
		cdcWireBytes += len(w)
		for _, change := range patch.Changes {
			if change.After().Revision() != rev {
				t.Fatal("CDC lost actual revision/provenance", change)
			}
		}
		cdcMetadata += len(patch.Changes) * 68
	}
	if len(name) < 128 || metadata == 0 || cdcMetadata == 0 || wireBytes == 0 || cdcWireBytes == 0 {
		t.Fatal("fixture lacks variable metadata and complete changes")
	}
	t.Logf("actual Go state metadata=%d state wire=%d CDC fixed-cell lower bound=%d CDC wire=%d", metadata, wireBytes, cdcMetadata, cdcWireBytes)
	fit := func(candidate []Operation, cap int) (Delta, error) {
		return Plan(t.Context(), view, candidate, rev, Limits{MaxDeltaBytes: cap})
	}
	minimum := func(candidate []Operation) int {
		t.Helper()
		lo, hi := 1, 1<<20
		for lo < hi {
			mid := (lo + hi) / 2
			_, err := fit(candidate, mid)
			if err == nil {
				hi = mid
			} else {
				if !errors.Is(err, ErrResourceLimit) {
					t.Fatal(err)
				}
				lo = mid + 1
			}
		}
		return lo
	}
	first := minimum(ops[:2])
	second := minimum(ops[2:3])
	aggregate := minimum(ops)
	if aggregate <= max(first, second) {
		t.Fatal("complete batch charged no aggregate metadata", first, second, aggregate)
	}
	for _, variation := range []int{-1, 0, 1} {
		got, err := fit(ops, aggregate+variation)
		if variation < 0 {
			if !errors.Is(err, ErrResourceLimit) || !reflect.DeepEqual(got, Delta{}) {
				t.Fatal("insufficient aggregate produced partial result", got, err)
			}
		} else if err != nil || !reflect.DeepEqual(got, delta) {
			t.Fatal("exact representation capacity changed result", err)
		}
		if revisedViewDigest(t, view) != before {
			t.Fatal("planning mutated input")
		}
	}
	// Actual encoded before/after changes have their OWN byte ceiling, and errors
	// preserve caller prefix bytes. The state fitting does not imply CDC fitting.
	for _, patch := range delta.Patches {
		w, err := state.AppendChanges(nil, axis, patch.Changes, state.CodecLimits{})
		if err != nil {
			t.Fatal(err)
		}
		for _, variation := range []int{-1, 0, 1} {
			prefix := []byte("prefix")
			saved := bytes.Clone(prefix)
			got, err := state.AppendChanges(prefix, axis, patch.Changes, state.CodecLimits{MaxEncodedBytes: len(w) + variation})
			if variation < 0 {
				if !errors.Is(err, state.ErrResourceLimit) || !bytes.Equal(got, saved) {
					t.Fatal(got, err)
				}
			} else if err != nil || !bytes.Equal(got[len(prefix):], w) {
				t.Fatal(err)
			}
		}
	}
	view.install(t, delta)
	old := view.clone()
	middle, err := revisedMutationScope(revisedOperation{Valid: []byte("[3,7]")}, axis, definition)
	if err != nil {
		t.Fatal(err)
	}
	commitOps(t, view, 2, Operation{Kind: Set, Owner: 1, Life: 1, Scope: middle, Name: name, Value: String("corrected"), ValueID: 2})
	position, err := temporal.RationalPosition(axis, temporal.RationalInt64(5))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		view *fixtureView
		want Scalar
	}{{old, value}, {view, String("corrected")}} {
		p, err := Project(t.Context(), tc.view, 1, position, Effective, Limits{})
		if err != nil || !p.Active || len(p.Properties) != 1 {
			t.Fatal(p, err)
		}
		same, err := p.Properties[0].Scalar.Equal(tc.want, Limits{})
		if err != nil || !same {
			t.Fatal(p, err)
		}
	}
}

func TestV1CapacityIndependentlyRequiresStateAndCompleteCDCContributions(t *testing.T) {
	v := newFixtureView(t)
	name := "complete-before-after-and-axis"
	whole := testSpan(t, v.axis, 0, 10)
	v.defs[ownerSchemaKey{Node, name}] = PropertyDefinition{name, Node, ScalarString, ScalarCardinality, UniqueNone}
	commitOps(t, v, 1, Operation{Kind: CreateNode, Owner: 1, Life: 1, Scope: whole}, Operation{Kind: Set, Owner: 1, Life: 1, Scope: whole, Name: name, Value: String("old"), ValueID: 1})
	revision, _ := state.NewRevision(2, 23)
	ops := []Operation{{Kind: Unset, Owner: 1, Life: 1, Scope: whole, Name: name}}
	delta, err := Plan(t.Context(), v, ops, revision, Limits{})
	if err != nil || len(delta.Patches) != 1 || len(delta.Entities)+len(delta.Lives)+len(delta.Values) != 0 {
		t.Fatal(delta, err)
	}
	// Isolate one replacement: no entity/life/dictionary writes can hide missing
	// state or CDC cost. Literal documented wire fields are independently counted:
	// each ledger header53, axis ID/profile/version/string frames27, each Cell34
	// (presence/tag +valueID/size/revision/provenance), and two Cells per change.
	descriptor := v.axis.Descriptor()
	axis := 27 + len(descriptor.Reference) + len(descriptor.CanonicalUnit)
	stateFloor, cdcFloor := 53+axis, 53+axis
	for _, piece := range delta.Patches[0].State.Pieces() {
		wire, err := temporal.AppendScope(nil, piece.Scope(), temporal.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		stateFloor += len(wire) + 34
	}
	for _, change := range delta.Patches[0].Changes {
		wire, err := temporal.AppendScope(nil, change.Scope(), temporal.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		cdcFloor += len(wire) + 68
		if change.Before().Revision().ID() != 1 || change.After().Revision() != revision {
			t.Fatal("incomplete actual before/after revisions", change)
		}
	}
	if delta.Patches[0].State.Usage().MetadataBytes() != stateFloor {
		t.Fatal("state metadata omitted documented fields", delta.Patches[0].State.Usage().MetadataBytes(), stateFloor)
	}
	fixed := 32
	for _, dependency := range delta.Dependencies {
		fixed += 128 + len(dependency.Name) + len(dependency.Key.Name) + len(dependency.Prefix.Name) + len(dependency.Unique.Definition.Name)
		for _, window := range []temporal.Scope{dependency.Window, dependency.Unique.Window, dependency.Incident.Window} {
			if window.Kind() != temporal.ScopeInvalid {
				wire, err := temporal.AppendScope(nil, window, temporal.Limits{})
				if err != nil {
					t.Fatal(err)
				}
				d := window.Axis().Descriptor()
				fixed += len(wire) + 27 + len(d.Reference) + len(d.CanonicalUnit)
			}
		}
		for _, value := range []Scalar{dependency.Value, dependency.Unique.Value} {
			if value.Kind() != ScalarInvalid {
				key, err := value.EqualityKey(Limits{})
				if err != nil {
					t.Fatal(err)
				}
				fixed += len(key)
				if scope, ok := value.Scope(); ok {
					d := scope.Axis().Descriptor()
					fixed += 27 + len(d.Reference) + len(d.CanonicalUnit)
				}
			}
		}
	}
	required := fixed + stateFloor + cdcFloor
	if got, err := Plan(t.Context(), v, ops, revision, Limits{MaxDeltaBytes: required}); err != nil || !reflect.DeepEqual(got, delta) {
		t.Fatal("independent exact total not admitted", required, got, err)
	}
	// Deliberately do not derive this required floor from Plan's own successful
	// limit. Omitting EITHER complete state or CDC would admit this insufficient
	// budget and fail. Dependency windows/scalar fields are counted explicitly so
	// hidden dependency slack cannot mask omission of either output contribution.
	for _, cap := range []int{required - 1, fixed + stateFloor, fixed + cdcFloor} {
		got, err := Plan(t.Context(), v, ops, revision, Limits{MaxDeltaBytes: cap})
		if !errors.Is(err, ErrResourceLimit) || !reflect.DeepEqual(got, Delta{}) {
			t.Fatal("independent state/CDC floor bypassed", cap, required, got, err)
		}
	}
	t.Logf("independent contribution floors fixed/names=%d state=%d complete CDC=%d full axis=%d", fixed, stateFloor, cdcFloor, axis)
}
