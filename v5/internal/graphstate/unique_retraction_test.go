package graphstate

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

type retractionReads struct {
	ReadView
	incident, keys, candidates int
	refuseIncident             bool
}

func (v *retractionReads) IncidentRelationships(ctx context.Context, p IncidentPredicate, c Cursor, b ReadBudget) (EntityPage, error) {
	v.incident++
	if v.refuseIncident {
		return EntityPage{}, ErrResourceLimit
	}
	return v.ReadView.IncidentRelationships(ctx, p, c, b)
}
func (v *retractionReads) ComponentKeys(ctx context.Context, p KeyPredicate, c Cursor, b ReadBudget) (KeyPage, error) {
	v.keys++
	return v.ReadView.ComponentKeys(ctx, p, c, b)
}
func (v *retractionReads) UniqueCandidates(ctx context.Context, p UniquePredicate, c Cursor, b ReadBudget) (ClaimPage, error) {
	v.candidates++
	return v.ReadView.UniqueCandidates(ctx, p, c, b)
}

func retractionGraph(t *testing.T, n int, shape string) *fixtureView {
	t.Helper()
	v := newFixtureView(t)
	whole := testSpan(t, v.axis, 0, 20)
	commitIncident(t, v, 1, Operation{Kind: CreateNode, Owner: 1, Life: 1, Scope: whole}, Operation{Kind: CreateNode, Owner: 2, Life: 1, Scope: whole}, Operation{Kind: CreateNode, Owner: 3, Life: 1, Scope: whole})
	source, target := EntityID(1), EntityID(2)
	if shape == "target" {
		source, target = 2, 1
	}
	if shape == "self-loop" {
		target = 1
	}
	for j := range n {
		commitIncident(t, v, uint64(j+2), Operation{Kind: CreateRelationship, Owner: EntityID(100 + j), Life: 1, Scope: whole, Record: EntityRecord{Type: "BOUND", Source: source, Target: target, Mode: LifeBound}, Binding: LifeRecord{SourceLife: 1, TargetLife: 1}})
	}
	return v
}
func TestPurePresenceRetractionDoesNotExpandUniqueness(t *testing.T) {
	for _, shape := range []string{"source", "target", "self-loop", "relationship"} {
		for _, kind := range []OperationKind{Close, Correct} {
			t.Run(fmt.Sprintf("%s/%d", shape, kind), func(t *testing.T) {
				v := retractionGraph(t, 64, shape)
				old := v.clone()
				scope := testSpan(t, v.axis, 10, 20)
				owner := EntityID(1)
				if shape == "relationship" {
					owner = 100
				}
				reads := &retractionReads{ReadView: v, refuseIncident: true}
				r, _ := state.NewRevision(1000, 0)
				d, err := Plan(t.Context(), reads, []Operation{{Kind: kind, Owner: owner, Life: 1, Scope: scope, Present: false}}, r, Limits{})
				if err != nil {
					t.Fatal("retraction unnecessarily expands incident claims", err)
				}
				if reads.incident != 0 || reads.keys != 0 || reads.candidates != 0 || len(d.Entities) != 0 || len(d.Lives) != 0 || len(d.Values) != 0 || len(d.Patches) != 1 || len(d.Patches[0].Changes) != 1 {
					t.Fatal("nonlocal retraction output/work", reads, d)
				}
				p := d.Patches[0]
				if p.Key != (ComponentKey{Owner: owner, Kind: Presence}) || !p.Changes[0].Before().Present() || p.Changes[0].After().Present() {
					t.Fatal("wrong exact presence CDC", p)
				}
				same, err := p.Owned.SameSupport(scope, temporal.Limits{})
				if err != nil || !same {
					t.Fatal("wrong owned window", err)
				}
				entity, life, component := false, false, false
				for _, dep := range d.Dependencies {
					entity = entity || dep.Kind == EntityDependency && dep.Owner == owner
					life = life || dep.Kind == LifeDependency && dep.Owner == owner
					component = component || dep.Kind == ComponentDependency && dep.Key.Owner == owner
					if dep.Kind == IncidentDependency || dep.Kind == PrefixDependency || dep.Kind == UniquenessDependency {
						t.Fatal("unnecessary retraction predicate", dep)
					}
				}
				if !entity || !life || !component {
					t.Fatal("lost mutation dependencies")
				}
				v.install(t, d)
				for id := EntityID(100); id < 164; id++ {
					before, err := Project(t.Context(), old, id, testPosition(t, v.axis, 15), Effective, Limits{})
					if err != nil || !before.Active {
						t.Fatal("old cut changed", id, err)
					}
					after, err := Project(t.Context(), v, id, testPosition(t, v.axis, 15), Effective, Limits{})
					if err != nil || after.Active != (shape == "relationship" && id != owner) {
						t.Fatal("wrong effective set", id, after, err)
					}
					declared, err := Project(t.Context(), v, id, testPosition(t, v.axis, 15), Declared, Limits{})
					if err != nil || declared.Active != (shape != "relationship" || id != owner) {
						t.Fatal("wrong declared set", id, declared, err)
					}
				}
				phantom, err := Project(t.Context(), v, 9999, testPosition(t, v.axis, 15), Effective, Limits{})
				if err != nil || phantom.Exists || phantom.Active {
					t.Fatal("phantom projection", phantom, err)
				}
			})
		}
	}
}

func mixedRetractionGraph(t *testing.T) *fixtureView {
	t.Helper()
	v := newFixtureView(t)
	whole := testSpan(t, v.axis, 0, 20)
	v.defs[ownerSchemaKey{Relationship, "code"}] = PropertyDefinition{"code", Relationship, ScalarString, ScalarCardinality, UniqueScalar}
	commitIncident(t, v, 1, Operation{Kind: CreateNode, Owner: 1, Life: 1, Scope: whole}, Operation{Kind: CreateNode, Owner: 2, Life: 1, Scope: whole}, Operation{Kind: CreateNode, Owner: 3, Life: 1, Scope: whole})
	for j, code := range []string{"x", "y", "z"} {
		source := EntityID(2)
		if j == 0 {
			source = 1
		}
		commitIncident(t, v, uint64(j+2), Operation{Kind: CreateRelationship, Owner: EntityID(4 + j), Life: 1, Scope: whole, Record: EntityRecord{Type: "BOUND", Source: source, Target: 3, Mode: LifeBound}, Binding: LifeRecord{SourceLife: 1, TargetLife: 1}}, Operation{Kind: Set, Owner: EntityID(4 + j), Life: 1, Scope: whole, Name: "code", Value: String(code), ValueID: ValueID(j + 1)})
	}
	return v
}
func TestRetractionMixedUniquenessUsesFinalOverlay(t *testing.T) {
	for _, code := range []string{"x", "z"} {
		t.Run(code, func(t *testing.T) {
			v := mixedRetractionGraph(t)
			old := v.clone()
			future := testSpan(t, v.axis, 10, 20)
			reads := &retractionReads{ReadView: v, refuseIncident: true}
			r, _ := state.NewRevision(10, 0)
			d, err := Plan(t.Context(), reads, []Operation{{Kind: Close, Owner: 1, Life: 1, Scope: future}, {Kind: Set, Owner: 5, Life: 1, Scope: future, Name: "code", Value: String(code), ValueID: 99}}, r, Limits{})
			if code == "z" {
				if !errors.Is(err, ErrUniqueOverlap) || !reflect.DeepEqual(d, Delta{}) {
					t.Fatal("surviving conflict bypassed", d, err)
				}
				return
			}
			if err != nil || reads.incident != 0 || reads.candidates == 0 {
				t.Fatal("removed claim not checked by final overlay", d, err, reads)
			}
			v.install(t, d)
			projectIncident(t, old, 4, v.axis, 15, Effective, true, 1, "x")
			projectIncident(t, v, 4, v.axis, 15, Effective, false, 1, "")
			projectIncident(t, v, 4, v.axis, 15, Declared, true, 1, "x")
			projectIncident(t, v, 5, v.axis, 15, Effective, true, 1, "x")
			projectIncident(t, old, 5, v.axis, 15, Effective, true, 1, "y")
			reads = &retractionReads{ReadView: v}
			r, _ = state.NewRevision(11, 0)
			d, err = Plan(t.Context(), reads, []Operation{{Kind: Correct, Owner: 1, Life: 1, Scope: future, Present: true}}, r, Limits{})
			if !errors.Is(err, ErrUniqueOverlap) || !reflect.DeepEqual(d, Delta{}) || reads.incident == 0 {
				t.Fatal("restoration bypassed incident uniqueness", d, err, reads)
			}
		})
	}
}

// This canonical test representation contains logical output only. Dependency
// footprints are deliberately NOT asserted byte-identical. The baseline and
// fixed runs report this same digest for parity evidence.
func retractionLogicalBytes(t *testing.T, d Delta) []byte {
	t.Helper()
	if len(d.Entities)+len(d.Lives)+len(d.Values) != 0 {
		t.Fatal("unexpected logical catalog writes")
	}
	var out []byte
	for _, p := range d.Patches {
		out = binary.BigEndian.AppendUint64(out, uint64(p.Key.Owner))
		out = binary.BigEndian.AppendUint64(out, uint64(p.Key.Life))
		out = append(out, byte(p.Key.Kind))
		out = binary.BigEndian.AppendUint64(out, uint64(len(p.Key.Name)))
		out = append(out, p.Key.Name...)
		out = binary.BigEndian.AppendUint64(out, uint64(p.Key.Member))
		scope, err := temporal.AppendScope(nil, p.Owned, temporal.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		s, err := state.AppendState(nil, p.State, state.CodecLimits{})
		if err != nil {
			t.Fatal(err)
		}
		changes, err := state.AppendChanges(nil, p.Owned.Axis(), p.Changes, state.CodecLimits{})
		if err != nil {
			t.Fatal(err)
		}
		for _, b := range [][]byte{scope, s, changes} {
			out = binary.BigEndian.AppendUint64(out, uint64(len(b)))
			out = append(out, b...)
		}
	}
	return out
}
func TestRetractionLogicalOutputParity(t *testing.T) {
	v := mixedRetractionGraph(t)
	r, _ := state.NewRevision(10, 0)
	d, err := Plan(t.Context(), v, []Operation{{Kind: Close, Owner: 1, Life: 1, Scope: testSpan(t, v.axis, 10, 20)}}, r, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(retractionLogicalBytes(t, d))
	const baseline = "6d208b4cbc7a37f65b3de866c273d94ac73db8aef61aa7e85b8a6069c13210d3"
	if got := fmt.Sprintf("%x", hash); got != baseline {
		t.Fatalf("logical output changed: got %s baseline %s", got, baseline)
	}
	t.Logf("logical-output-sha256=%x dependencies=%d", hash, len(d.Dependencies))
}
