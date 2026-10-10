package assertion

import (
	"bytes"
	"encoding/hex"
	"errors"
	"reflect"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
)

func TestV1SharedInterpretationPreservesAllExistingAssertionTags(t *testing.T) {
	rev, err := state.NewRevision(1, 0)
	if err != nil {
		t.Fatal(err)
	}
	spec := Spec{Ref: Ref{Graph: graphstate.GraphID{1}, ID: 1}, Target: Target{Kind: EntityTarget, Entity: 1}, Revision: rev, Placement: Placement{Kind: NoAssociation}, Knowledge: Knowledge{Kind: NoKnowledge}}
	const occurrenceGolden = "41520101000000000000000000000000000000000000000000000101000000000000000100000000000000000000000000000000000000000000000000000000000000000000000001000000000000000000000000000000000100000000010001"
	baseline, err := hex.DecodeString(occurrenceGolden)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		value Interpretation
		tag   byte
	}{{Occurrence, 1}, {State, 2}, {Observation, 3}, {Constraint, 4}, {Derived, 5}, {AssertedRelation, 6}} {
		if byte(tc.value) != tc.tag || !tc.value.Valid() {
			t.Fatal(tc)
		}
		spec.Interpretation = tc.value
		r, err := New(spec, Limits{})
		if err != nil {
			t.Fatal(err)
		}
		wire, err := AppendRecord(nil, r, Limits{})
		if err != nil {
			t.Fatal(err)
		}
		golden := bytes.Clone(baseline)
		golden[89] = tc.tag
		if !bytes.Equal(wire, golden) {
			t.Fatal("AR1 golden changed", wire, golden)
		}
		decoded, err := DecodeRecord(wire, temporal.Axis{}, Limits{})
		if err != nil || !reflect.DeepEqual(decoded.Spec(), spec) {
			t.Fatal(decoded, err)
		}
		again, err := AppendRecord(nil, decoded, Limits{})
		if err != nil || !bytes.Equal(wire, again) {
			t.Fatal("AR1 bytes changed", err)
		}
	}
	for _, i := range []Interpretation{0, 255} {
		spec.Interpretation = i
		if _, err := New(spec, Limits{}); !errors.Is(err, ErrInvalid) {
			t.Fatal(i, err)
		}
	}
}
