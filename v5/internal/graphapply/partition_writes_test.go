package graphapply

import (
	"bytes"
	"crypto/sha256"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func partitionWriteSchemas() []graphstate.PropertyDefinition {
	return []graphstate.PropertyDefinition{{Name: "descriptor", Owner: graphstate.Node, Type: graphstate.ScalarDescriptor, Cardinality: graphstate.ScalarCardinality}, {Name: "p", Owner: graphstate.Node, Type: graphstate.ScalarI64, Cardinality: graphstate.ScalarCardinality, Unique: graphstate.UniqueScalar}, {Name: "descriptor", Owner: graphstate.Relationship, Type: graphstate.ScalarDescriptor, Cardinality: graphstate.ScalarCardinality}}
}
func partitionNineOperations(t *testing.T, first uint64) ([]graphstate.Operation, []freshBinding) {
	t.Helper()
	axis := graphGenesisBinding(t).Axis()
	scope, err := temporal.All(axis)
	if err != nil {
		t.Fatal(err)
	}
	n1, n2 := graphstate.EntityID(first), graphstate.EntityID(first+2)
	life1, life2 := graphstate.LifeID(first+1), graphstate.LifeID(first+3)
	ops := []graphstate.Operation{{Kind: graphstate.CreateNode, Owner: n1, Record: graphstate.EntityRecord{ID: n1, Kind: graphstate.Node, Axis: axis, Interpretation: graphstate.InterpretationOccurrence, TemporalRole: graphstate.TemporalRoleSourceReceipt}, Life: life1, Scope: scope}, {Kind: graphstate.CreateNode, Owner: n2, Record: graphstate.EntityRecord{ID: n2, Kind: graphstate.Node, Axis: axis, Interpretation: graphstate.InterpretationState, TemporalRole: graphstate.TemporalRoleValidity}, Life: life2, Scope: scope}}
	for i, mode := range []graphstate.ReferenceMode{graphstate.LifeBound, graphstate.IdentityReference, graphstate.LifeBound} {
		id, life := graphstate.EntityID(first+4+uint64(i*2)), graphstate.LifeID(first+5+uint64(i*2))
		target := n2
		if i == 2 {
			target = n1
		}
		op := graphstate.Operation{Kind: graphstate.CreateRelationship, Owner: id, Record: graphstate.EntityRecord{ID: id, Kind: graphstate.Relationship, Axis: axis, Type: "edge", Source: n1, Target: target, Mode: mode, Interpretation: graphstate.InterpretationAssertedRelation, TemporalRole: graphstate.TemporalRoleOccurrenceTime}, Life: life, Scope: scope}
		if mode == graphstate.LifeBound {
			op.Binding.SourceLife = life1
			op.Binding.TargetLife = life2
			if i == 2 {
				op.Binding.TargetLife = life1
			}
		}
		ops = append(ops, op)
	}
	d, err := temporal.PreserveOpaqueDescriptor(temporal.OpaqueDescriptorSpec{ID: temporal.DescriptorID{7}, Type: "calendar", SchemaVersion: 999, Payload: []byte("opaque P1D, not24h")}, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	value, err := graphstate.DescriptorValue(d)
	if err != nil {
		t.Fatal(err)
	}
	ops = append(ops, graphstate.Operation{Kind: graphstate.AddLabel, Owner: n1, Life: life1, Name: "old", Scope: scope}, graphstate.Operation{Kind: graphstate.Set, Owner: n1, Life: life1, Name: "descriptor", Value: value, ValueID: graphstate.ValueID(first + 10), Scope: scope}, graphstate.Operation{Kind: graphstate.Set, Owner: n2, Life: life2, Name: "p", Value: graphstate.I64(9), ValueID: graphstate.ValueID(first + 11), Scope: scope}, graphstate.Operation{Kind: graphstate.Set, Owner: graphstate.EntityID(first + 4), Life: graphstate.LifeID(first + 5), Name: "descriptor", Value: value, ValueID: graphstate.ValueID(first + 12), Scope: scope})
	claims := make([]freshBinding, 0, 12)
	for i := range 5 {
		owner := graphstate.EntityID(first + uint64(i*2))
		claims = append(claims, freshBinding{role: entityBinding, id: uint64(owner)}, freshBinding{role: lifeBinding, owner: owner, id: first + uint64(i*2) + 1})
	}
	claims = append(claims, freshBinding{role: valueBinding, id: first + 10}, freshBinding{role: valueBinding, id: first + 11})
	return ops, claims
}

func TestPartitionWritesNineNativeOperationsAreAdmittedWithActualReservedRanges(t *testing.T) {
	r := partitionCodecRequest(t, 0, defaultMaterializerLimits())
	n := newPartitionWriteNetworkWithRouting(t, r, partitionWriteSchemas())
	handle := n.activate(0, [16]byte{1})
	n.acquire(0, handle, 1, 16)
	h := n.hosts[0][0]
	proof := n.read(0, allocationQuery{kind: observeConfiguration})
	ops, claims := partitionNineOperations(t, 1)
	for i := range claims {
		claims[i].grant = grantReference{session: handle.session, sequence: 1}
	}
	revision, err := state.NewRevision(1, 23)
	if err != nil {
		t.Fatal(err)
	}
	command := partitionGraphCommand{configuration: proof.observation.configuration, declaration: n.d.Digest(), routing: r.routing.Digest(), request: graphRequest{ns: h.machine.ns, kind: graphOperations, id: n.requestID(), revision: revision, operations: ops, claims: claims}}
	wire, err := encodePartitionGraphCommand(command, h.machine.limits)
	if err != nil {
		t.Fatal(err)
	}
	entry := replica.Entry{Index: h.driver.Applied() + 1, Generation: h.machine.store.ApplicationGeneration(), Term: 2, Data: wire}
	batch, err := h.machine.Stage(entry, h.machine.store.ApplicationBudget())
	if err != nil || len(batch.Writes) < 20 || len(batch.Changes) == 0 {
		decoded, decodeErr := decodePartitionGraphOutcome(batch.Outcome, h.machine.ns)
		t.Logf("materializer outcome=%+v decode=%v", decoded, decodeErr)
		view, e := h.machine.store.ApplicationView(h.driver.Applied())
		if e != nil {
			t.Fatal(e)
		}
		defer view.Close()
		staged, work, e := graphstore.StagePartitionOperations(t.Context(), view, n.d, graphGenesisBinding(t), proof.observation.configuration, ops, revision, h.machine.limits.catalog, h.machine.limits.graph, graphstore.OwnershipBudget{SourceRows: 1024, SourceBytes: 4 << 20, OutputBytes: 16 << 20})
		t.Logf("direct checked stage work=%+v entities/lives/values/groups=%d/%d/%d/%d err=%v", work, len(staged.Delta.Entities), len(staged.Delta.Lives), len(staged.Delta.Values), len(staged.Groups), e)
		t.Fatalf("nine typed native operations still unavailable despite actual local grant/routing: writes%d CDC%d %v", len(batch.Writes), len(batch.Changes), err)
	}
	// This direct materializer regression does not install, allocate fresh IDs or
	// replace the forthcoming actual Host/Driver six-replica acceptance scenario.
}

func (n *allocationTestNetwork) submitPartitionGraph(group int, p allocationProposal, want reason) outcome {
	n.t.Helper()
	event, err := n.hosts[group][0].Submit(p)
	if err != nil {
		n.t.Fatal(err)
	}
	n.pump(group, event)
	var original outcome
	for j, h := range n.hosts[group] {
		wire, err := h.machine.store.ApplicationRecord(n.t.Context(), h.driver.Applied(), true, graphOutcomeBytes)
		if err != nil {
			n.t.Fatal(err)
		}
		got, err := decodePartitionGraphOutcome(wire, h.machine.ns)
		if err != nil {
			n.t.Fatal(err)
		}
		if j == 0 {
			original = got
		} else if got != original {
			n.t.Fatal("replica graph outcome mismatch", got, original)
		}
	}
	if original.reason != want {
		n.t.Fatal("graph outcome", original, want)
	}
	return original
}

func TestPartitionCommittedRemoteDependencyRejectsThenAllowsLocalWrite(t *testing.T) {
	r := partitionCodecRequest(t, 0, defaultMaterializerLimits())
	n := newPartitionWriteNetworkWithRouting(t, r, partitionWriteSchemas())
	local := n.activate(0, [16]byte{1})
	n.acquire(0, local, 1, 16)
	remote := n.activate(1, [16]byte{2})
	n.acquire(1, remote, 1, 16)
	h := n.hosts[0][0]
	scope, err := temporal.All(graphGenesisBinding(t).Axis())
	if err != nil {
		t.Fatal(err)
	}
	// 17 belongs to the actual separately published partition8 range. No caller
	// participant omission can turn this into a complete local entity absence.
	p, err := h.PreparePartitionGraphOperations(n.requestID(), []graphstate.Operation{{Kind: graphstate.CreateNode, Owner: 17, Life: 18, Scope: scope}}, 7, nil)
	if err != nil {
		t.Fatal(err)
	}
	var beforeImages, beforeFloors [3][]byte
	for j, host := range n.hosts[0] {
		_, beforeImages[j], err = host.machine.store.Checkpoint()
		if err != nil {
			t.Fatal(err)
		}
		view, e := host.machine.store.ApplicationView(host.driver.Applied())
		if e != nil {
			t.Fatal(e)
		}
		row, found, e := view.Get(t.Context(), partitionRoundFloorKey(host.machine.ns), 1024)
		if e != nil || !found {
			t.Fatal(found, e)
		}
		beforeFloors[j] = row.Value
		if e := view.Close(); e != nil {
			t.Fatal(e)
		}
	}
	event, err := h.Submit(p)
	if err != nil {
		t.Fatal(err)
	}
	n.pump(0, event)
	// The committed request must advance with a typed rejection, without changing
	// graph/round state, rather than stop the driver at this committed entry.
	for j, host := range n.hosts[0] {
		if host.driver.Applied() != h.driver.Applied() {
			t.Fatal("rejection did not advance all voters")
		}
		wire, e := host.machine.store.ApplicationRecord(t.Context(), host.driver.Applied(), true, graphOutcomeBytes)
		if e != nil {
			t.Fatal(e)
		}
		got, e := decodePartitionGraphOutcome(wire, host.machine.ns)
		if e != nil || got.reason != reasonRemoteParticipant || got.disposition != applied || got.hash != sha256.Sum256(p.wire) {
			t.Fatalf("exact remote rejection on voter%d: %+v %v", j, got, e)
		}
		_, image, e := host.machine.store.Checkpoint()
		if e != nil || !bytes.Equal(image, beforeImages[j]) {
			t.Fatal("rejection changed graph root", j, e)
		}
		cdc, e := host.machine.store.ApplicationRecord(t.Context(), host.driver.Applied(), false, host.machine.store.ApplicationLimits().MaxChangeBytes)
		if e != nil || len(cdc) != 0 {
			t.Fatal("rejection emitted graph CDC", j, e)
		}
		view, e := host.machine.store.ApplicationView(host.driver.Applied())
		if e != nil {
			t.Fatal(e)
		}
		row, found, e := view.Get(t.Context(), partitionRoundFloorKey(host.machine.ns), 1024)
		if e != nil || !found || !bytes.Equal(row.Value, beforeFloors[j]) {
			t.Fatal("rejection changed graph round", j, e)
		}
		if e := view.Close(); e != nil {
			t.Fatal(e)
		}
	}
	ops, claims := partitionNineOperations(t, 1)
	for i := range claims {
		claims[i].grant = grantReference{session: local.session, sequence: 1}
	}
	valid, err := h.PreparePartitionGraphOperations(n.requestID(), ops, 8, claims)
	if err != nil {
		t.Fatal("remote rejection poisoned actual Host", err)
	}
	n.submitPartitionGraph(0, valid, reasonNone)
}
