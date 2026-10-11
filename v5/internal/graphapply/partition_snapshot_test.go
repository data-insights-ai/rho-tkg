package graphapply

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"reflect"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func partitionSnapshotGroup(n *allocationTestNetwork, group int) *declaredTestGroup {
	g := &declaredTestGroup{t: n.t, declaration: n.d, ns: n.hosts[group][0].machine.ns}
	for i, h := range n.hosts[group] {
		g.stores[i] = h.machine.store
		g.machines[i] = h.machine
		g.drivers[i] = h.driver
		g.configs[i] = n.configs[group][i]
	}
	return g
}
func partitionSnapshotSubmit(t *testing.T, transport *declaredSnapshotTransport, h *allocationHost, proposal allocationProposal) outcome {
	t.Helper()
	event, err := h.Submit(proposal)
	if err != nil {
		t.Fatal(err)
	}
	transport.enqueue(replica.Output{Packets: event.packets, SnapshotSends: event.snapshotSends})
	transport.drain()
	var original outcome
	for i, store := range transport.g.stores {
		if transport.dropFollower && i == 2 {
			continue
		}
		index := transport.g.drivers[i].Applied()
		wire, err := store.ApplicationRecord(t.Context(), index, true, graphOutcomeBytes)
		if err != nil {
			t.Fatal(err)
		}
		got, err := decodePartitionGraphOutcome(wire, transport.g.ns)
		if err != nil || got.reason != reasonNone || got.hash != sha256.Sum256(proposal.wire) {
			t.Fatal(got, err)
		}
		if i == 0 {
			original = got
		} else if got != original {
			t.Fatal("snapshot schedule voter outcome differs", got, original)
		}
	}
	return original
}

func TestPartitionNonemptyGraphSnapshotRetainedHistoryFailoverAndReopen(t *testing.T) {
	network, routing := partitionIntegrationNetwork(t)
	var firsts, originalIndexes [2]uint64
	var descriptors, integers [2]graphstate.Scalar
	for group, partition := range []uint64{3, 8} {
		handle := network.activate(group, [16]byte{byte(group + 1)})
		cursor := network.acquire(group, handle, 1, 16)
		first, err := cursor.Next()
		if err != nil {
			t.Fatal(err)
		}
		integer, descriptor, _ := partitionValueFixture(t, routing, partition)
		firsts[group], integers[group], descriptors[group] = first, integer, descriptor
		operations, claims := partitionNineOperations(t, first)
		operations[6].Value = descriptor
		operations[7].Value = integer
		operations[8].Value = descriptor
		for i := range claims {
			claims[i].grant = grantReference{session: handle.session, sequence: 1}
		}
		proposal, err := network.hosts[group][0].PreparePartitionGraphOperations(network.requestID(), operations, 23, claims)
		if err != nil {
			t.Fatal(err)
		}
		original := network.submitPartitionGraph(group, proposal, reasonNone)
		originalIndexes[group] = original.index
		for _, host := range network.hosts[group] {
			assertPartitionNineCDC(t, host, original.index, operations, first)
			assertPartitionGraphSet(t, host, original.index, first, true, descriptor, integer)
		}
	}
	// The second declared group remains a real, independently installed graph;
	// group3's actual snapshot must preserve its own complete local history.
	group := partitionSnapshotGroup(network, 0)
	transport := &declaredSnapshotTransport{t: t, g: group, queue: make([]replica.Packet, 0, 128)}
	oldIndex, first := originalIndexes[0], firsts[0]
	old, err := group.stores[2].ApplicationView(oldIndex)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	oldRoot, err := old.RootBounded(t.Context(), 172)
	if err != nil || oldRoot.Generation != 1 {
		t.Fatal(oldRoot, err)
	}
	oldCDC, err := group.stores[0].ApplicationRecord(t.Context(), oldIndex, false, group.stores[0].ApplicationLimits().MaxChangeBytes)
	if err != nil {
		t.Fatal(err)
	}
	baseline := make(map[string][]byte)
	namespace := graphstore.Namespace{Graph: network.d.Graph(), Partition: 3}
	for _, key := range [][]byte{genesisConfigurationKey(group.ns), graphstore.RoutingDefinitionKey(namespace), graphstore.RangePublicationKey(namespace, first), graphstore.RangeOwnershipKey(namespace, first)} {
		row, found, err := old.Get(t.Context(), key, 4096)
		if err != nil || !found {
			t.Fatal(found, err)
		}
		baseline[string(key)] = row.Value
	}
	assertRetained := func(view *raftlog.ApplicationView) {
		t.Helper()
		for key, expected := range baseline {
			row, found, err := view.Get(t.Context(), []byte(key), 4096)
			if err != nil || !found || !bytes.Equal(row.Value, expected) {
				t.Fatal("snapshot changed exact config/routing/publication/fence", key, found, err)
			}
		}
	}
	scope, err := temporal.All(graphGenesisBinding(t).Axis())
	if err != nil {
		t.Fatal(err)
	}
	droppedAt := group.drivers[2].Applied()
	assertPartitionGraphSet(t, network.hosts[0][2], droppedAt, first, true, descriptors[0], integers[0])
	transport.dropFollower = true
	for _, operation := range []graphstate.Operation{{Kind: graphstate.RemoveLabel, Owner: graphstate.EntityID(first), Life: graphstate.LifeID(first + 1), Name: "old", Scope: scope}, {Kind: graphstate.Close, Owner: graphstate.EntityID(first), Life: graphstate.LifeID(first + 1), Scope: scope}} {
		proposal, err := network.hosts[0][0].PreparePartitionGraphOperations(network.requestID(), []graphstate.Operation{operation}, 31, nil)
		if err != nil {
			t.Fatal(err)
		}
		partitionSnapshotSubmit(t, transport, network.hosts[0][0], proposal)
	}
	cutIndex := group.drivers[0].Applied()
	if group.drivers[2].Applied() != droppedAt {
		t.Fatal("dropped follower advanced beyond captured control/graph frontier", group.drivers[2].Applied(), droppedAt, oldIndex)
	}
	if err := group.stores[0].PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	beginning, err := group.stores[0].FirstIndex()
	if err != nil || beginning <= oldIndex {
		t.Fatal("retained log replay could satisfy snapshot test", beginning, oldIndex, err)
	}
	transport.dropFollower = false
	pages := 0
	for range 20 {
		output, err := group.drivers[0].Tick()
		if err != nil {
			t.Fatal(err)
		}
		transport.enqueue(output)
		transport.drain()
		if transport.send != nil {
			break
		}
		offers, err := group.drivers[0].SnapshotSends()
		if err != nil {
			t.Fatal(err)
		}
		for _, send := range offers {
			complete := false
			for !complete && pages < 128 {
				complete, err = send.Build(t.Context(), raftlog.ReadBudget{Rows: 8, Bytes: 64 << 10})
				pages++
				if err != nil {
					t.Fatal(err)
				}
			}
			if !complete {
				t.Fatal("snapshot manifest page bound")
			}
			if err := group.drivers[0].SealSnapshotSend(send); err != nil {
				t.Fatal(err)
			}
		}
	}
	if transport.send == nil {
		t.Fatal("real AS3 MsgSnap sender required")
	}
	manifest, err := transport.send.Manifest()
	if err != nil || manifest.Version != 3 || manifest.Index != cutIndex || manifest.SemanticContractID != partitionWriteSemanticContractID() {
		t.Fatal(manifest, err)
	}
	receiver, err := group.stores[2].BeginApplicationImport(t.Context(), manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Abort()
	complete := false
	for !complete && pages < 128 {
		chunk, err := transport.send.Next(t.Context(), raftlog.ReadBudget{Rows: 8, Bytes: 64 << 10})
		pages++
		if err != nil {
			t.Fatal(err)
		}
		if err := receiver.Append(t.Context(), chunk); err != nil {
			t.Fatal(err)
		}
		complete = chunk.Final
	}
	if !complete {
		t.Fatal("snapshot stream page bound")
	}
	if err := receiver.Verify(t.Context()); err != nil {
		t.Fatal(err)
	}
	prepared, err := receiver.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	if group.stores[2].ApplicationGeneration() != 1 || group.drivers[2].Applied() != droppedAt {
		t.Fatal("dormant nonempty import published early")
	}
	activated, err := group.drivers[2].StepApplicationSnapshot(transport.packet, prepared)
	if err != nil {
		t.Fatal(err)
	}
	if group.stores[2].ApplicationGeneration() != 2 || group.drivers[2].Applied() != manifest.Index {
		t.Fatal("actual bank generation transition required")
	}
	audit, err := group.stores[2].ApplicationActivationAudit()
	if err != nil || audit.CutID != manifest.CutID || audit.Index != cutIndex {
		t.Fatal(audit, err)
	}
	feedback, err := group.drivers[0].ReportSnapshotSend(transport.send, true)
	if err != nil {
		t.Fatal(err)
	}
	transport.send = nil
	transport.packet = replica.Packet{}
	transport.enqueue(feedback)
	transport.enqueue(activated)
	transport.drain()
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
	if err := receiver.Abort(); err != nil {
		t.Fatal(err)
	}
	// Existing contract: pinned retired-bank views remain exact across activation.
	held, err := old.RootBounded(t.Context(), 172)
	if err != nil || !reflect.DeepEqual(held, oldRoot) {
		t.Fatal("pinned retired bank changed", held, err)
	}
	assertRetained(old)
	originalProjection, _ := partitionProjectionFromView(t, network.hosts[0][2], old, graphstate.EntityID(first), graphGenesisBinding(t).Axis())
	if !originalProjection.Active || len(originalProjection.Labels) != 1 || originalProjection.Labels[0] != "old" {
		t.Fatal("pinned graph history aliased activated state", originalProjection)
	}
	assertPartitionGraphSet(t, network.hosts[0][2], cutIndex, first, false, descriptors[0], integers[0])
	// Elect the intended second actual voter; stale leader-only reads must refuse.
	for range 20 {
		for _, host := range network.hosts[0] {
			event, err := host.Tick()
			if err != nil {
				t.Fatal(err)
			}
			if len(event.snapshotSends) != 0 || len(event.replies) != 0 {
				t.Fatal("unexpected isolated owner")
			}
		}
	}
	event, err := network.hosts[0][1].Campaign()
	if err != nil {
		t.Fatal(err)
	}
	transport.enqueue(replica.Output{Packets: event.packets, SnapshotSends: event.snapshotSends})
	transport.drain()
	readEvent, err := network.hosts[0][1].Read(allocationQuery{kind: observeConfiguration, nonce: [16]byte{98}})
	if err != nil {
		t.Fatal("intended member2 is not actual current-term leader", err)
	}
	replies := network.pump(0, readEvent)
	if len(replies) != 1 || replies[0].err != nil || replies[0].proof.owner != network.hosts[0][1] {
		t.Fatal("member2 completed-source proof required", replies)
	}
	if _, err := network.hosts[0][0].Read(allocationQuery{kind: observeConfiguration, nonce: [16]byte{99}}); !errors.Is(err, replica.ErrUnavailable) {
		t.Fatal("old leader served completed-source read", err)
	}
	tail, err := network.hosts[0][1].PreparePartitionGraphOperations(network.requestID(), []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: graphstate.EntityID(first + 2), Life: graphstate.LifeID(first + 3), Name: "tail", Scope: scope}}, 32, nil)
	if err != nil {
		t.Fatal(err)
	}
	outcome := partitionSnapshotSubmit(t, transport, network.hosts[0][1], tail)
	tailIndex := group.drivers[1].Applied()
	if outcome.index != tailIndex || tailIndex <= cutIndex {
		t.Fatal("failover graph tail missing", outcome, tailIndex)
	}
	tailProjection, _ := partitionProjection(t, network.hosts[0][2], tailIndex, graphstate.EntityID(first+2), graphGenesisBinding(t).Axis())
	if !reflect.DeepEqual(tailProjection.Labels, []string{"tail"}) {
		t.Fatal("failover mutation missing", tailProjection)
	}
	if err := group.drivers[2].Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := old.RootBounded(t.Context(), 172); !errors.Is(err, raftlog.ErrClosed) {
		t.Fatal("stale borrow survived store close", err)
	}
	config := group.configs[2]
	config.Create = false
	reopened, err := raftlog.Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	machine, err := newPartitionWriteMaterializer(reopened, group.ns, network.d, defaultMaterializerLimits())
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := replica.Open(replica.Config{ID: 3, Store: reopened, ApplicationMachine: machine, ApplicationSnapshotSends: replica.ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 4 << 20}})
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	host := &allocationHost{machine: machine, driver: restarted}
	if restarted.Applied() != tailIndex || reopened.ApplicationGeneration() != 2 {
		t.Fatal("reopen lost activation/failover tail")
	}
	if recovered, err := reopened.ApplicationActivationAudit(); err != nil || recovered != audit {
		t.Fatal("activation audit changed", recovered, err)
	}
	for _, index := range []uint64{oldIndex, cutIndex, tailIndex} {
		view, err := reopened.ApplicationView(index)
		if err != nil {
			t.Fatal(err)
		}
		assertRetained(view)
		if err := view.Close(); err != nil {
			t.Fatal(err)
		}
	}
	assertPartitionGraphSet(t, host, oldIndex, first, true, descriptors[0], integers[0])
	assertPartitionGraphSet(t, host, cutIndex, first, false, descriptors[0], integers[0])
	after, _ := partitionProjection(t, host, tailIndex, graphstate.EntityID(first+2), graphGenesisBinding(t).Axis())
	if !reflect.DeepEqual(after.Labels, []string{"tail"}) {
		t.Fatal("reopen lost graph tail", after)
	}
	recoveredCDC, err := reopened.ApplicationRecord(t.Context(), oldIndex, false, reopened.ApplicationLimits().MaxChangeBytes)
	if err != nil || !bytes.Equal(recoveredCDC, oldCDC) {
		t.Fatal("retained typed graph CDC changed through AS3/tail/reopen", err)
	}
	for _, other := range network.hosts[1] {
		assertPartitionGraphSet(t, other, originalIndexes[1], firsts[1], true, descriptors[1], integers[1])
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	if usage, err := reopened.ApplicationTransferUsage(); err != nil || usage.Exports != 0 || usage.Import || usage.Prepared != 0 || usage.Claims != 0 || usage.PinnedLogicalBytes != 0 {
		t.Fatal("snapshot resources survived reopen", usage, err)
	}
}
