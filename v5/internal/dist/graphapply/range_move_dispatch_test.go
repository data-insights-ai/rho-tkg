package graphapply

import (
	"bytes"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/dist/replica"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

func rangeMoveDispatchNativeHome(t *testing.T, graph graphstate.GraphID) *allocationTestNetwork {
	t.Helper()
	l := defaultMaterializerLimits()
	d, err := graphstore.NewOwnershipDeclaration(graph, 9, []graphstore.PartitionOwnership{{Partition: 3, OwnershipEpoch: 2, Group: [16]byte{4}}, {Partition: 8, OwnershipEpoch: 3, Group: [16]byte{8}}}, l.catalog)
	if err != nil {
		t.Fatal(err)
	}
	routing, err := graphstore.NewPartitionRouting(d, 1, 0, []graphstore.BucketOwnership{{Kind: 1, Partition: 3, Epoch: 1}, {Kind: 2, Partition: 3, Epoch: 1}, {Kind: 3, Partition: 3, Epoch: 1}}, l.catalog)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := types.NewDefaultAxisBinding(types.GraphID(graph), temporal.AxisID{31}, l.catalog.Temporal)
	if err != nil {
		t.Fatal(err)
	}
	n := &allocationTestNetwork{t: t, d: d, pending: make(map[[16]byte]allocationProof)}
	entry, _ := d.Partition(3)
	for member := range 3 {
		id := uint64(member + 1)
		policy := raftlog.DefaultApplicationPolicy(id)
		policy.MaxImageBytes, policy.MaxInstallWrites = 172, 256
		policy.RetainedApplicationRecords, policy.RetainedApplicationBytes = 8192, 16<<20
		controls, recovery := raftlog.ApplicationControlConfig{Version: 1}, raftlog.ApplicationRecoveryConfig{Version: 1}
		contract, err := raftlog.ApplicationContractForPolicyWithRecovery(policy, controls, recovery)
		if err != nil {
			t.Fatal(err)
		}
		cfg := raftlog.Config{Dir: filepath.Join(t.TempDir(), strconv.Itoa(member)), Create: true, Application: policy, Controls: controls, Recovery: recovery, Transfer: raftlog.ApplicationTransferConfig{Identity: raftlog.ApplicationIdentity{Graph: [16]byte(graph), Partition: 3, Group: entry.Group}, Contract: contract, Limits: raftlog.DefaultApplicationTransferLimits()}, Generations: raftlog.ApplicationGenerationLimits{MaxBytes: 32 << 20, MaxRecords: 16384}, PublishedCuts: raftlog.ApplicationPublishedCutLimits{MaxTransferChunks: 4096}, Replication: raftlog.ApplicationReplicationConfig{Voters: [3]uint64{1, 2, 3}}, SemanticContractID: rangeMoveSemanticContractID()}
		n.configs[0][member] = cfg
		store, err := raftlog.Open(cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := store.Close(); err != nil {
				t.Error(err)
			}
		})
		if err := graphstore.BootstrapBoundOwnership(store, store.ApplicationBinding(), d, [3]uint64{1, 2, 3}); err != nil {
			t.Fatal(err)
		}
		machine, err := newRangeMoveMaterializer(store, namespace{graph: idalloc.GraphID(graph), partition: 3}, d, l)
		if err != nil {
			t.Fatal(err)
		}
		host, err := openAllocationHost(machine, id, replica.Config{Store: store, ApplicationSnapshotSends: replica.ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 4 << 20}})
		if err != nil {
			t.Fatal(err)
		}
		n.hosts[0][member] = host
		t.Cleanup(func() {
			if err := host.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	event, err := n.hosts[0][0].Campaign()
	if err != nil {
		t.Fatal(err)
	}
	n.pump(0, event)
	proposal, err := n.hosts[0][0].PreparePartitionInitialization(3, bootstrapAttemptID{1}, partitionWriteSchemas(), 16, binding, routing, allocationProof{})
	if err != nil {
		t.Fatal(err)
	}
	n.submit(0, proposal)
	handle := n.activate(0, [16]byte{1})
	n.acquire(0, handle, 1, 16)
	return n
}

func TestRangeMoveNativeStageDoesNotTreatGraphIDAsInitializationOpcode(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for _, first := range []byte{7, 8} {
		t.Run(strconv.Itoa(int(first)), func(t *testing.T) {
			n := rangeMoveDispatchNativeHome(t, graphstate.GraphID{first})
			h := n.hosts[0][0]
			configuration := n.read(0, allocationQuery{kind: observeConfiguration}).observation.configuration
			view, err := h.machine.store.ApplicationView(h.driver.Applied())
			if err != nil {
				t.Fatal(err)
			}
			base, err := view.RootBounded(t.Context(), 172)
			if err != nil {
				t.Fatal(err)
			}
			publication, owner, _, err := graphstore.LookupPublishedRange(t.Context(), view, graphstore.Namespace{Graph: graphstate.GraphID(h.machine.ns.graph), Partition: 3}, 1, configuration, graphstore.OwnershipBudget{SourceRows: 64, SourceBytes: 1 << 20, OutputBytes: 1 << 20})
			if closeErr := view.Close(); closeErr != nil {
				t.Fatal(closeErr)
			}
			if err != nil {
				t.Fatal("actual native published range", err)
			}
			target, err := h.target(8)
			if err != nil {
				t.Fatal(err)
			}
			intent := rangeMoveIntent{id: n.requestID(), source: h.machine.scope(), target: target, publication: publication, epoch: owner.Epoch, maxPages: 1, pageRows: 1, pageBytes: 8 << 10}
			intentWire, err := encodeRangeMoveIntent(intent)
			if err != nil {
				t.Fatal(err)
			}
			output, err := graphstate.NewOutputBudget(8 << 20)
			if err != nil {
				t.Fatal(err)
			}
			proposal, err := h.prepareRangeMoveCommand(intentWire, rangeMoveFence, 3, 0, nil, nil, output)
			if err != nil || len(proposal.wire) < 5 || proposal.wire[4] != first {
				t.Fatal("actual graph-qualified source proposal", err)
			}
			// Exercise the full Stage route against genuine initialized store and
			// published allocation provenance. No private batch is installed here.
			batch, err := h.machine.Stage(replica.Entry{Index: base.Index + 1, Generation: base.Generation, Term: 2, Data: proposal.wire}, h.machine.store.ApplicationBudget())
			if err != nil {
				t.Fatalf("GMQ1 GraphID first byte%d was misclassified as initialization opcode: %v", first, err)
			}
			status, err := decodeRangeMoveStatus(batch.Outcome)
			if err != nil || status.phase != rangeMoveFence || status.result != rangeMoveApplied || status.ns != h.machine.ns || len(batch.Writes) == 0 || len(batch.ControlPuts) != 1 || !bytes.Equal(batch.ControlPuts[0].Value, intentWire) {
				t.Fatal("correct dispatch omitted exact native Fence effects", status, err)
			}
			index, image, err := h.machine.store.Checkpoint()
			if err != nil || index != base.Index || !bytes.Equal(image, base.Image) {
				t.Fatal("private Stage installed its effects", err)
			}
		})
	}
}
