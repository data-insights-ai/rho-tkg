package graphapply

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func partitionValueFixture(t *testing.T, r graphstore.PartitionRouting, partition uint64) (graphstate.Scalar, graphstate.Scalar, temporal.Axis) {
	t.Helper()
	l := defaultMaterializerLimits()
	var integer, descriptor graphstate.Scalar
	var axis temporal.Axis
	for i := int64(1); i <= 1024 && integer.Kind() == graphstate.ScalarInvalid; i++ {
		v := graphstate.I64(i)
		key, err := v.EqualityKey(l.graph.Planner)
		if err != nil {
			t.Fatal(err)
		}
		identity, err := r.Bucket(graphstore.IdentityRoutingBucket, []byte(key))
		if err != nil {
			t.Fatal(err)
		}
		logical := []byte{byte(graphstate.Node), byte(graphstate.ScalarI64), byte(graphstate.ScalarCardinality), byte(graphstate.UniqueScalar)}
		logical = binary.BigEndian.AppendUint32(logical, 1)
		logical = append(logical, 'p')
		logical = binary.BigEndian.AppendUint32(logical, uint32(len(key)))
		logical = append(logical, key...)
		unique, err := r.Bucket(graphstore.UniqueRoutingBucket, logical)
		if err != nil {
			t.Fatal(err)
		}
		if identity.Partition == partition && unique.Partition == partition {
			integer = v
		}
	}
	for i := 1; i <= 1024 && descriptor.Kind() == graphstate.ScalarInvalid; i++ {
		d, err := temporal.PreserveOpaqueDescriptor(temporal.OpaqueDescriptorSpec{ID: temporal.DescriptorID{100}, Type: "calendar", SchemaVersion: 999, Payload: []byte(fmt.Sprintf("opaque-partition%d-choice%d", partition, i))}, l.catalog.Temporal)
		if err != nil {
			t.Fatal(err)
		}
		v, err := graphstate.DescriptorValue(d)
		if err != nil {
			t.Fatal(err)
		}
		key, err := v.EqualityKey(l.graph.Planner)
		if err != nil {
			t.Fatal(err)
		}
		owner, err := r.Bucket(graphstore.IdentityRoutingBucket, []byte(key))
		if err != nil {
			t.Fatal(err)
		}
		if owner.Partition == partition {
			descriptor = v
		}
	}
	for i := 1; i <= 1024 && axis.Descriptor().ID == (temporal.AxisID{}); i++ {
		var id temporal.AxisID
		id[0] = 90
		binary.BigEndian.PutUint64(id[8:], uint64(i))
		a, err := temporal.NewAxis(temporal.AxisDescriptor{ID: id, Profile: temporal.ProfileRationalQ, Version: 1, Reference: "partition-native-axis:v1", CanonicalUnit: "second"}, l.catalog.Temporal)
		if err != nil {
			t.Fatal(err)
		}
		owner, err := r.Bucket(graphstore.AxisRoutingBucket, id[:])
		if err != nil {
			t.Fatal(err)
		}
		if owner.Partition != partition {
			continue
		}
		scope, err := temporal.All(a)
		if err != nil {
			t.Fatal(err)
		}
		key, err := graphstate.ScopeValue(scope).EqualityKey(l.graph.Planner)
		if err != nil {
			t.Fatal(err)
		}
		identity, err := r.Bucket(graphstore.IdentityRoutingBucket, []byte(key))
		if err != nil {
			t.Fatal(err)
		}
		if identity.Partition == partition {
			axis = a
		}
	}
	if integer.Kind() == graphstate.ScalarInvalid || descriptor.Kind() == graphstate.ScalarInvalid || axis.Descriptor().ID == (temporal.AxisID{}) {
		t.Fatal("bounded bucket-owner fixture selection exhausted")
	}
	return integer, descriptor, axis
}
func partitionIntegrationNetwork(t *testing.T) (*allocationTestNetwork, graphstore.PartitionRouting) {
	t.Helper()
	r := partitionCodecRequest(t, 1, defaultMaterializerLimits())
	owners := make([]graphstore.BucketOwnership, 6)
	for family := range 3 {
		for bucket, part := range []uint64{3, 8} {
			owners[family*2+bucket] = graphstore.BucketOwnership{Kind: graphstore.RoutingBucketKind(family + 1), Bucket: uint32(bucket), Partition: part, Epoch: 1}
		}
	}
	routing, err := graphstore.NewPartitionRouting(r.declaration, 1, 1, owners, defaultMaterializerLimits().catalog)
	if err != nil {
		t.Fatal(err)
	}
	r.routing = new(routing)
	schemas := partitionWriteSchemas()
	schemas = append(schemas[:2], append([]graphstate.PropertyDefinition{{Name: "window", Owner: graphstate.Node, Type: graphstate.ScalarScope, Cardinality: graphstate.ScalarCardinality}}, schemas[2:]...)...)
	return newPartitionWriteNetworkWithRouting(t, r, schemas), routing
}
func partitionProjection(t *testing.T, h *allocationHost, index uint64, id graphstate.EntityID, axis temporal.Axis) (graphstate.Projection, graphstore.PageWork) {
	t.Helper()
	view, err := h.machine.store.ApplicationView(index)
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	return partitionProjectionFromView(t, h, view, id, axis)
}
func partitionProjectionFromView(t *testing.T, h *allocationHost, view *raftlog.ApplicationView, id graphstate.EntityID, axis temporal.Axis) (graphstate.Projection, graphstore.PageWork) {
	t.Helper()
	row, found, err := view.Get(t.Context(), genesisConfigurationKey(h.machine.ns), 4096)
	if err != nil || !found {
		t.Fatal(found, err)
	}
	cfg, err := decodeGenesisAllocationConfig(row.Value, h.machine.ns.graph)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := cfg.digest()
	if err != nil {
		t.Fatal(err)
	}
	v, _, err := graphstore.OpenPartitionGraphReadView(t.Context(), view, h.machine.declaration, cfg.defaultAxis, digest, h.machine.limits.catalog, h.machine.limits.graph, graphstore.OwnershipBudget{SourceRows: 1024, SourceBytes: 4 << 20, OutputBytes: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	point, err := temporal.RationalPosition(axis, temporal.RationalInt64(5))
	if err != nil {
		t.Fatal(err)
	}
	p, err := graphstate.Project(t.Context(), v, id, point, graphstate.Effective, h.machine.limits.graph.Planner)
	if err != nil {
		t.Fatal(err)
	}
	return p, v.Work()
}
func TestPartitionWritesBothGroupsPreserveTypedAuthorityHistoryAndReopen(t *testing.T) {
	n, routing := partitionIntegrationNetwork(t)
	var installed [2]uint64
	var descriptors [2]graphstate.Scalar
	var integers [2]graphstate.Scalar
	var axes [2]temporal.Axis
	for group, part := range []uint64{3, 8} {
		handle := n.activate(group, [16]byte{byte(group + 1)})
		cursor := n.acquire(group, handle, 1, 16)
		first, err := cursor.Next()
		if err != nil {
			t.Fatal(err)
		}
		integer, descriptor, axis := partitionValueFixture(t, routing, part)
		integers[group], descriptors[group], axes[group] = integer, descriptor, axis
		ops, claims := partitionNineOperations(t, first)
		ops[6].Value, ops[7].Value, ops[8].Value = descriptor, integer, descriptor
		for i := range claims {
			claims[i].grant = grantReference{session: handle.session, sequence: 1}
		}
		h := n.hosts[group][0]
		p, err := h.PreparePartitionGraphOperations(n.requestID(), ops, 23, claims)
		if err != nil {
			t.Fatal(err)
		}
		original := n.submitPartitionGraph(group, p, reasonNone)
		installed[group] = original.index
		for _, replica := range n.hosts[group] {
			assertPartitionNineCDC(t, replica, original.index, ops, first)
		}
		for _, replica := range n.hosts[group] {
			for _, offset := range []uint64{0, 4} {
				projected, _ := partitionProjection(t, replica, original.index, graphstate.EntityID(first+offset), graphGenesisBinding(t).Axis())
				if !projected.Exists || !projected.Active || projected.Record.Interpretation == 0 || projected.Record.TemporalRole == 0 {
					t.Fatal("typed node/relationship declaration erased", projected)
				}
				found := false
				for _, property := range projected.Properties {
					if property.Name == "descriptor" {
						found = true
						equal, err := property.Scalar.Equal(descriptor, h.machine.limits.graph.Planner)
						if err != nil || !equal {
							t.Fatal("opaque envelope erased", err)
						}
					}
				}
				if !found {
					t.Fatal("descriptor property missing")
				}
			}
		}
		custom, err := temporal.All(axis)
		if err != nil {
			t.Fatal(err)
		}
		native := graphstate.ScopeValue(custom)
		defaultScope, err := temporal.All(graphGenesisBinding(t).Axis())
		if err != nil {
			t.Fatal(err)
		}
		added := []graphstate.Operation{{Kind: graphstate.CreateNode, Owner: graphstate.EntityID(first + 13), Life: graphstate.LifeID(first + 14), Scope: custom, Record: graphstate.EntityRecord{Interpretation: graphstate.InterpretationObservation, TemporalRole: graphstate.TemporalRoleCausalOrder}}, {Kind: graphstate.Set, Owner: graphstate.EntityID(first), Life: graphstate.LifeID(first + 1), Scope: defaultScope, Name: "window", Value: native, ValueID: graphstate.ValueID(first + 15)}}
		extra := []freshBinding{{role: entityBinding, id: first + 13, grant: grantReference{session: handle.session, sequence: 1}}, {role: lifeBinding, owner: graphstate.EntityID(first + 13), id: first + 14, grant: grantReference{session: handle.session, sequence: 1}}, {role: valueBinding, id: first + 15, grant: grantReference{session: handle.session, sequence: 1}}}
		next, err := h.PreparePartitionGraphOperations(n.requestID(), added, 24, extra)
		if err != nil {
			t.Fatal(err)
		}
		n.submitPartitionGraph(group, next, reasonNone)
		customNode, _ := partitionProjection(t, h, h.driver.Applied(), graphstate.EntityID(first+13), axis)
		if !customNode.Active || customNode.Record.Axis.Descriptor() != axis.Descriptor() {
			t.Fatal("non-default native axis not registered under its local authority", customNode)
		}
		close, err := h.PreparePartitionGraphOperations(n.requestID(), []graphstate.Operation{{Kind: graphstate.Close, Owner: graphstate.EntityID(first), Life: graphstate.LifeID(first + 1), Scope: defaultScope}}, 25, nil)
		if err != nil {
			t.Fatal(err)
		}
		n.submitPartitionGraph(group, close, reasonNone)
		for _, replica := range n.hosts[group] {
			old, _ := partitionProjection(t, replica, original.index, graphstate.EntityID(first), graphGenesisBinding(t).Axis())
			current, _ := partitionProjection(t, replica, replica.driver.Applied(), graphstate.EntityID(first), graphGenesisBinding(t).Axis())
			assertPartitionGraphSet(t, replica, original.index, first, true, descriptor, integer)
			assertPartitionGraphSet(t, replica, replica.driver.Applied(), first, false, descriptor, integer)
			if !old.Active || current.Active || len(old.Labels) != 1 || old.Labels[0] != "old" {
				t.Fatal("retained original read aliased post-close state", old, current)
			}
			lb, _ := partitionProjection(t, replica, replica.driver.Applied(), graphstate.EntityID(first+4), graphGenesisBinding(t).Axis())
			ir, _ := partitionProjection(t, replica, replica.driver.Applied(), graphstate.EntityID(first+6), graphGenesisBinding(t).Axis())
			if lb.Active || !ir.Active {
				t.Fatal("LB/IR endpoint masking changed", lb, ir)
			}
		}
	}
	for group, hosts := range n.hosts {
		for member, h := range hosts {
			if err := h.driver.Close(); err != nil {
				t.Fatal(err)
			}
			cfg := n.configs[group][member]
			cfg.Create = false
			s, err := raftlog.Open(cfg)
			if err != nil {
				t.Fatal(err)
			}
			m, err := newPartitionWriteMaterializer(s, h.machine.ns, n.d, h.machine.limits)
			if err != nil {
				t.Fatal("reopen", err)
			}
			reopened := &allocationHost{machine: m}
			first := uint64(1 + 16*group)
			old, _ := partitionProjection(t, reopened, installed[group], graphstate.EntityID(first), graphGenesisBinding(t).Axis())
			index, _, err := s.Checkpoint()
			if err != nil {
				t.Fatal(err)
			}
			current, _ := partitionProjection(t, reopened, index, graphstate.EntityID(first), graphGenesisBinding(t).Axis())
			assertPartitionGraphSet(t, reopened, installed[group], first, true, descriptors[group], integers[group])
			assertPartitionGraphSet(t, reopened, index, first, false, descriptors[group], integers[group])
			if !old.Active || current.Active {
				t.Fatal("reopen lost exact retained history")
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
	if equal, err := integers[0].Equal(integers[1], defaultMaterializerLimits().graph.Planner); err != nil || equal {
		t.Fatal("fixture did not select distinct typed authorities", err)
	}
	if equal, err := descriptors[0].Equal(descriptors[1], defaultMaterializerLimits().graph.Planner); err != nil || equal {
		t.Fatal("descriptor fixture did not select distinct owners", err)
	}
	if axes[0].Descriptor().ID == axes[1].Descriptor().ID {
		t.Fatal("native axis fixture did not select both owners")
	}
}

func assertPartitionNineCDC(t *testing.T, h *allocationHost, index uint64, ops []graphstate.Operation, first uint64) {
	t.Helper()
	wire, err := h.machine.store.ApplicationRecord(t.Context(), index, true, graphOutcomeBytes)
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := decodePartitionGraphOutcome(wire, h.machine.ns)
	if err != nil || outcome.reason != reasonNone {
		t.Fatal(outcome, err)
	}
	retained, err := h.machine.store.ApplicationRecord(t.Context(), index, false, h.machine.store.ApplicationLimits().MaxChangeBytes)
	if err != nil {
		t.Fatal(err)
	}
	changes, err := decodeTypedChangeEnvelope(retained, outcome, h.machine.limits, true)
	if err != nil {
		t.Fatal("typed retained CDC", err)
	}
	if len(changes.entities) != 5 || len(changes.lives) != 5 || len(changes.values) != 2 || len(changes.groups) != 9 || changes.initialized {
		t.Fatal("complete nine-operation CDC counts", len(changes.entities), len(changes.lives), len(changes.values), len(changes.groups))
	}
	for i := range 5 {
		expected := ops[i].Record
		expected.ID = ops[i].Owner
		expected.Axis = ops[i].Scope.Axis()
		if !reflect.DeepEqual(changes.entities[i], expected) {
			t.Fatal("entity declaration/axis CDC mismatch", i, changes.entities[i], expected)
		}
		life := ops[i].Binding
		life.Owner = ops[i].Owner
		life.Life = ops[i].Life
		if changes.lives[i] != life {
			t.Fatal("life/endpoint binding CDC mismatch", i, changes.lives[i], life)
		}
	}
	for i, opIndex := range []int{6, 7} {
		value := changes.values[i]
		equal, err := value.Value.Equal(ops[opIndex].Value, h.machine.limits.graph.Planner)
		if err != nil || !equal || value.ID != graphstate.ValueID(first+10+uint64(i)) {
			t.Fatal("canonical value CDC mismatch", i, value.ID, err)
		}
	}
	for i, group := range changes.groups {
		operation := ops[i]
		expected := graphstate.ComponentKey{Owner: operation.Owner, Kind: graphstate.Presence}
		switch i {
		case 5:
			expected.Life = operation.Life
			expected.Kind = graphstate.Label
			expected.Name = "old"
		case 6, 7, 8:
			expected.Life = operation.Life
			expected.Kind = graphstate.ScalarProperty
			expected.Name = operation.Name
		}
		if group.Key != expected || len(group.Changes) != 1 {
			t.Fatal("ordered component group missing/extra", i, group.Key, len(group.Changes))
		}
		same, err := group.Owned.SameSupport(operation.Scope, h.machine.limits.catalog.Temporal)
		if err != nil || !same {
			t.Fatal("CDC owned support", i, err)
		}
		change := group.Changes[0]
		same, err = change.Scope().SameSupport(operation.Scope, h.machine.limits.catalog.Temporal)
		if err != nil || !same || change.Before() != (state.Cell{}) || !change.After().Present() || change.After().Revision().ID() != 1 || change.After().Revision().Provenance() != 23 {
			t.Fatal("exact changed support/before/revision missing", i, change, err)
		}
		value := change.After().Value()
		switch i {
		case 0, 1, 2, 3, 4:
			if value.ID() != uint64(operation.Life) || value.PayloadBytes() != 0 {
				t.Fatal("presence life CDC", i, value)
			}
		case 5:
			if !value.IsNull() {
				t.Fatal("label null CDC", value)
			}
		case 6, 8:
			if value.ID() != first+10 {
				t.Fatal("descriptor alias CDC", i, value)
			}
		case 7:
			if value.ID() != first+11 {
				t.Fatal("typed value CDC", value)
			}
		}
	}
	logical, err := encodeTypedGraphChanges(changes, h.machine.limits, true)
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := encodeChangeEnvelope(outcome, logical, h.machine.limits)
	if err != nil || !bytes.Equal(canonical, retained) {
		t.Fatal("retained CDC changed canonical bytes", err)
	}
	if _, err = decodeChangeEnvelope(retained, outcome, h.machine.limits); !errors.Is(err, errCorrupt) {
		t.Fatal("legacy CDC decoder admitted new contract", err)
	}
}
func assertPartitionGraphSet(t *testing.T, h *allocationHost, index, first uint64, beforeClose bool, descriptor, integer graphstate.Scalar) {
	t.Helper()
	want := []bool{beforeClose, true, beforeClose, true, beforeClose}
	for i := range 5 {
		projection, _ := partitionProjection(t, h, index, graphstate.EntityID(first+uint64(2*i)), graphGenesisBinding(t).Axis())
		if !projection.Exists || projection.Active != want[i] {
			t.Fatal("exact node/parallel/self-loop active set", i, index, projection.Active, want[i])
		}
		if projection.Record.ID != graphstate.EntityID(first+uint64(2*i)) || projection.Record.Axis.Descriptor() != graphGenesisBinding(t).Axis().Descriptor() {
			t.Fatal("record/default binding changed", i, projection.Record)
		}
		if !projection.Active {
			continue
		}
		if projection.Life != graphstate.LifeID(first+uint64(2*i)+1) {
			t.Fatal("wrong lifecycle", i, projection.Life)
		}
		expectedProperty := ""
		expectedValue := graphstate.Scalar{}
		switch i {
		case 0, 2:
			expectedProperty = "descriptor"
			expectedValue = descriptor
		case 1:
			expectedProperty = "p"
			expectedValue = integer
		}
		if expectedProperty == "" {
			if len(projection.Properties) != 0 {
				t.Fatal("phantom properties", i, projection.Properties)
			}
		} else {
			found := false
			for _, property := range projection.Properties {
				if property.Name == "window" && i == 0 && !beforeClose {
					continue
				}
				if property.Name != expectedProperty || found || property.Cardinality != graphstate.ScalarCardinality {
					t.Fatal("extra property", i, property)
				}
				equal, err := property.Scalar.Equal(expectedValue, h.machine.limits.graph.Planner)
				if err != nil || !equal {
					t.Fatal("typed exact property", i, err)
				}
				found = true
			}
			if !found {
				t.Fatal("missing property", i, expectedProperty)
			}
		}
		if i == 0 {
			if len(projection.Labels) != 1 || projection.Labels[0] != "old" {
				t.Fatal("old label missing", projection.Labels)
			}
		} else if len(projection.Labels) != 0 {
			t.Fatal("phantom labels", i, projection.Labels)
		}
	}
	phantom, _ := partitionProjection(t, h, index, graphstate.EntityID(first+12), graphGenesisBinding(t).Axis())
	if phantom.Exists || phantom.Active || len(phantom.Properties) != 0 || len(phantom.Labels) != 0 {
		t.Fatal("unused fresh ID returned phantom graph entity", phantom)
	}
}
