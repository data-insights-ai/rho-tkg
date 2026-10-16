package graphapply

import (
	"crypto/sha256"
	"errors"
	"reflect"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func TestPartitionGraphTypedCodecPreservesNodeRelationshipDeclarationsAndDescriptor(t *testing.T) {
	l := defaultMaterializerLimits()
	binding := graphGenesisBinding(t)
	scope, err := temporal.All(binding.Axis())
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := temporal.PreserveOpaqueDescriptor(temporal.OpaqueDescriptorSpec{ID: temporal.DescriptorID{19}, Type: "calendar", SchemaVersion: 999, Correlation: temporal.CorrelationID{20}, References: []temporal.DescriptorReference{{Role: "rules", ID: temporal.DescriptorID{21}, Type: "tzdb", SchemaVersion: 2026}}, Payload: []byte("opaque P1D is not24h")}, l.catalog.Temporal)
	if err != nil {
		t.Fatal(err)
	}
	value, err := graphstate.DescriptorValue(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	revision, err := state.NewRevision(1, 23)
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []graphstate.EntityKind{graphstate.Node, graphstate.Relationship} {
		record := graphstate.EntityRecord{ID: 1, Kind: kind, Axis: binding.Axis(), Interpretation: graphstate.InterpretationOccurrence, TemporalRole: graphstate.TemporalRoleSourceReceipt}
		opKind := graphstate.CreateNode
		if kind == graphstate.Relationship {
			opKind = graphstate.CreateRelationship
			record.Source = 3
			record.Target = 5
			record.Mode = graphstate.IdentityReference
			record.Type = "edge"
		}
		r := partitionGraphCommand{configuration: [32]byte{1}, declaration: [32]byte{2}, routing: [32]byte{3}, request: graphRequest{ns: namespace{graph: [16]byte(binding.Graph()), partition: 3}, id: requestID{7}, kind: graphOperations, revision: revision, operations: []graphstate.Operation{{Kind: opKind, Record: record, Life: 2, Scope: scope}, {Kind: graphstate.Set, Owner: 1, Life: 2, Name: "descriptor", Scope: scope, Value: value, ValueID: 7}}}}
		if _, err := encodeGraphRequest(r.request, l); !errors.Is(err, graphstate.ErrUnsupported) {
			t.Fatalf("legacy metadata door changed: %v", err)
		}
		wire, err := encodePartitionGraphCommand(r, l)
		if err != nil {
			t.Fatalf("new typed codec still inherits legacy refusal: %v", err)
		}
		got, err := decodePartitionGraphCommand(wire, l)
		if err != nil || got.configuration != r.configuration || got.declaration != r.declaration || got.routing != r.routing || len(got.request.operations) != 2 || got.request.operations[0].Record != record {
			t.Fatalf("typed declaration roundtrip: %+v %v", got, err)
		}
		equal, err := got.request.operations[1].Value.Equal(value, l.graph.Planner)
		if err != nil || !equal {
			t.Fatalf("descriptor envelope changed: %v", err)
		}
		if _, err := decodeGraphRequest(wire, l); !errors.Is(err, errCorrupt) {
			t.Fatalf("legacy decoder admitted new wire: %v", err)
		}
	}
}

func TestPartitionGraphCodecDirectFramingFencesAndOwnershipRefusals(t *testing.T) {
	l := defaultMaterializerLimits()
	r := partitionGraphCommand{configuration: [32]byte{1}, declaration: [32]byte{2}, routing: [32]byte{3}, request: graphCodecRequest(t)}
	wire, err := encodePartitionGraphCommand(r, l)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"config", "declaration", "routing", "kind"} {
		bad := r
		switch field {
		case "config":
			bad.configuration = [32]byte{}
		case "declaration":
			bad.declaration = [32]byte{}
		case "routing":
			bad.routing = [32]byte{}
		case "kind":
			bad.request.kind = initGraph
		}
		if _, err := encodePartitionGraphCommand(bad, l); !errors.Is(err, errInvalid) {
			t.Fatal(field, err)
		}
	}
	badPolicy := l
	badPolicy.commandBytes = -1
	if _, err := encodePartitionGraphCommand(r, badPolicy); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if _, err := decodePartitionGraphCommand(wire, badPolicy); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	for _, offset := range []int{0, 4, 5, 21, 29, 45, 77, 109} {
		bad := owned(wire)
		if offset >= 45 {
			clear(bad[offset : offset+32])
		} else {
			bad[offset] ^= 1
		}
		bad = seal(bad[:len(bad)-32])
		admitted := 99
		got, err := decodePartitionGraphCommandOwned(bad, l, &admitted)
		if !errors.Is(err, errCorrupt) || !reflect.DeepEqual(got, partitionGraphCommand{}) || admitted != 0 {
			t.Fatal(offset, got, admitted, err)
		}
	}
	for _, bad := range [][]byte{nil, wire[:80], seal(append(owned(wire[:len(wire)-32]), 0))} {
		if _, err := decodePartitionGraphCommand(bad, l); !errors.Is(err, errCorrupt) {
			t.Fatal("malformed framing", err)
		}
	}
	for _, cap := range []int{1, 128 + 8*(requestHeaderBytes+3*32+4+32)} {
		tight := l
		tight.commandOwnedBytes = cap
		if _, err := encodePartitionGraphCommand(r, tight); !errors.Is(err, errLimit) {
			t.Fatal("encoder ownership", cap, err)
		}
		if _, err := decodePartitionGraphCommand(wire, tight); !errors.Is(err, errLimit) {
			t.Fatal("decoder ownership", cap, err)
		}
	}
	for _, kind := range []commandKind{graphOperations, guardedGraphOperations} {
		for _, why := range []reason{reasonNone, reasonInvalid, reasonMismatch, reasonStale, reasonRemoteParticipant, reasonRoutingUnknown} {
			o := outcome{ns: r.request.ns, kind: kind, identity: [16]byte{1}, hash: sha256.Sum256(wire), index: 1, disposition: applied, reason: why}
			encoded, err := encodeTypedGraphOutcome(o, true)
			if err != nil {
				t.Fatal(kind, why, err)
			}
			got, err := decodePartitionMappedOutcome(encoded, o.ns)
			if err != nil || got != o {
				t.Fatal(got, err)
			}
			if _, err := decodeGraphOutcome(encoded, o.ns); !errors.Is(err, errCorrupt) {
				t.Fatal("legacy outcome accepted new variant", err)
			}
		}
	}
	control := outcome{ns: r.request.ns, kind: initDeclaredPartition, identity: [16]byte{1}, hash: sha256.Sum256(wire), index: 1, disposition: applied}
	encoded, err := encodeDeclaredOutcome(control)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := decodePartitionMappedOutcome(encoded, control.ns); err != nil || got != control {
		t.Fatal(got, err)
	}
	if _, err := decodePartitionMappedOutcome(nil, control.ns); !errors.Is(err, errCorrupt) {
		t.Fatal(err)
	}
}
