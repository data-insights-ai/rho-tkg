package graphapply

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/data-insights-ai/rho-tkg/v5/internal/dist/replica"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

type lifecycleProvisionFunc func(context.Context, initialGraphPeer, []byte) error

func (f lifecycleProvisionFunc) ProvisionInitialGraph(ctx context.Context, peer initialGraphPeer, wire []byte) error {
	return f(ctx, peer, wire)
}

func lifecycleSpec() logicalGraphCreationSpec {
	return logicalGraphCreationSpec{
		partitions: []graphstore.PartitionOwnership{{Partition: 3, OwnershipEpoch: 2}, {Partition: 8, OwnershipEpoch: 3}},
		topology:   1, voters: [3]uint64{1, 2, 3}, routingVersion: 1,
		buckets:  []graphstore.BucketOwnership{{Kind: graphstore.IdentityRoutingBucket, Partition: 3, Epoch: 1}, {Kind: graphstore.UniqueRoutingBucket, Partition: 3, Epoch: 1}, {Kind: graphstore.AxisRoutingBucket, Partition: 3, Epoch: 1}},
		schemas:  []graphstate.PropertyDefinition{{Name: "p", Owner: graphstate.Node, Type: graphstate.ScalarI64, Cardinality: graphstate.ScalarCardinality}, {Name: "p", Owner: graphstate.Relationship, Type: graphstate.ScalarI64, Cardinality: graphstate.ScalarCardinality}},
		maxBlock: 16, agreement: partitionWriteSemanticContractID(), limits: defaultMaterializerLimits(),
	}
}

func lifecycleConfig(dir string, voter uint64) raftlog.Config {
	p := raftlog.DefaultApplicationPolicy(voter)
	p.MaxImageBytes, p.MaxInstallWrites = 172, 256
	p.RetainedApplicationRecords, p.RetainedApplicationBytes = 8192, 16<<20
	return raftlog.Config{Dir: dir, Application: p, Transfer: raftlog.ApplicationTransferConfig{Contract: raftlog.ApplicationContractForPolicy(p), Limits: raftlog.DefaultApplicationTransferLimits()}, Generations: raftlog.ApplicationGenerationLimits{MaxBytes: 32 << 20, MaxRecords: 16384}, PublishedCuts: raftlog.ApplicationPublishedCutLimits{MaxTransferChunks: 4096}}
}

func lifecycleSeedNetwork(t *testing.T) (*allocationTestNetwork, provisionedLogicalGraph, declaredInitCommand) {
	t.Helper()
	n := &allocationTestNetwork{t: t, pending: make(map[[16]byte]allocationProof)}
	spec := lifecycleSpec()
	base := t.TempDir()
	var original []byte
	provision := lifecycleProvisionFunc(func(ctx context.Context, peer initialGraphPeer, wire []byte) error {
		if original == nil {
			original = bytes.Clone(wire)
		} else if !bytes.Equal(original, wire) {
			t.Fatal("initial replicas did not receive identical genesis")
		}
		group := 0
		if peer.partition == 8 {
			group = 1
		}
		cfg := lifecycleConfig(filepath.Join(base, fmt.Sprintf("p%d-v%d", peer.partition, peer.voter)), peer.voter)
		g := provisionedLogicalGraph{wire, spec.voters, spec.agreement}
		h, err := provisionInitialGraphReplica(ctx, cfg, g, peer, spec.limits)
		if err != nil {
			return err
		}
		n.configs[group][peer.voter-1], n.hosts[group][peer.voter-1] = cfg, h
		t.Cleanup(func() {
			if err := h.Close(); err != nil {
				t.Error(err)
			}
		})
		index, image, err := h.machine.store.Checkpoint()
		if err != nil {
			return err
		}
		root, err := graphstore.DecodeRoot(image)
		if err != nil || index != 1 || root.SemanticEpoch() != 0 {
			t.Fatal("provisioned Host must remain a durable pending seed", index, root, err)
		}
		// A caller cannot turn provisioning success into ready graph data.
		if _, err := h.Read(allocationQuery{kind: observeConfiguration, nonce: [16]byte{1}}); !errors.Is(err, replica.ErrUnavailable) {
			t.Fatal("seed admitted authoritative read", err)
		}
		return nil
	})
	g, err := createLogicalGraph(t.Context(), spec, provision)
	if err != nil {
		t.Fatal(err)
	}
	r, err := decodeDeclaredInit(g.genesis, spec.limits)
	if err != nil {
		t.Fatal(err)
	}
	n.d = r.declaration
	return n, g, r
}

func lifecycleInitialize(t *testing.T, n *allocationTestNetwork, r declaredInitCommand) {
	t.Helper()
	for group := range 2 {
		out, err := n.hosts[group][0].Campaign()
		if err != nil {
			t.Fatal(err)
		}
		n.pump(group, out)
	}
	// Constructor/campaign success still does not install graph configuration.
	event, err := n.hosts[0][0].Read(allocationQuery{kind: observeConfiguration, nonce: [16]byte{2}})
	if err != nil {
		t.Fatal(err)
	}
	replies := n.pump(0, event)
	if len(replies) != 1 || !errors.Is(replies[0].err, errCorrupt) || replies[0].proof != (allocationProof{}) {
		t.Fatal("pre-Init quorum read granted graph readiness", replies)
	}
	first, err := n.hosts[0][0].PreparePartitionInitialization(3, bootstrapAttemptID{1}, r.schemas, r.maxBlock, r.defaultAxis, *r.routing, allocationProof{})
	if err != nil {
		t.Fatal(err)
	}
	n.submit(0, first)
	proof := n.read(0, allocationQuery{kind: observeConfiguration})
	second, err := n.hosts[0][0].PreparePartitionInitialization(8, bootstrapAttemptID{2}, r.schemas, r.maxBlock, r.defaultAxis, *r.routing, proof)
	if err != nil {
		t.Fatal(err)
	}
	n.submit(1, second)
}

func lifecycleNetwork(t *testing.T) (*allocationTestNetwork, provisionedLogicalGraph, declaredInitCommand) {
	t.Helper()
	n, g, r := lifecycleSeedNetwork(t)
	lifecycleInitialize(t, n, r)
	return n, g, r
}

func TestLogicalGraphLifecyclePendingChoicesBindOnlyAtCommittedInit(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n, original, candidate := lifecycleSeedNetwork(t)
	candidate.maxBlock++
	candidate.schemas = []graphstate.PropertyDefinition{{Name: "chosen-at-init", Owner: graphstate.Node, Type: graphstate.ScalarI64, Cardinality: graphstate.ScalarCardinality}}
	var err error
	candidate.defaultAxis, err = types.NewDefaultAxisBinding(types.GraphID(candidate.ns.graph), temporal.AxisID{99}, defaultMaterializerLimits().catalog.Temporal)
	if err != nil {
		t.Fatal(err)
	}
	chosen := original
	chosen.genesis, err = encodeDeclaredInit(candidate, defaultMaterializerLimits())
	if err != nil {
		t.Fatal(err)
	}
	for group, hosts := range n.hosts {
		for member, h := range hosts {
			if err := h.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := openLogicalGraphReplica(t.Context(), n.configs[group][member], chosen, initialGraphPeer{h.machine.ns.partition, uint64(member + 1)}, h.machine.limits)
			if err != nil {
				t.Fatal("pending seed falsely bound uncommitted choices", err)
			}
			n.hosts[group][member] = reopened
			t.Cleanup(func() {
				if err := reopened.Close(); err != nil {
					t.Error(err)
				}
			})
		}
	}
	lifecycleInitialize(t, n, candidate)
	for group, hosts := range n.hosts {
		for member, h := range hosts {
			if err := h.Close(); err != nil {
				t.Fatal(err)
			}
			peer := initialGraphPeer{h.machine.ns.partition, uint64(member + 1)}
			bad, err := openLogicalGraphReplica(t.Context(), n.configs[group][member], original, peer, h.machine.limits)
			if bad != nil || !errors.Is(err, errInvalid) || !errors.Is(err, graphstore.ErrRebinding) {
				t.Fatal("committed Init accepted old candidate", err)
			}
			restored, err := openLogicalGraphReplica(t.Context(), n.configs[group][member], chosen, peer, h.machine.limits)
			if err != nil {
				t.Fatal("committed chosen configuration could not restore", err)
			}
			if err := restored.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestLogicalGraphLifecycleCommittedHistoryAndAllocatorReopen(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n, g, r := lifecycleNetwork(t)
	handle := n.activate(0, [16]byte{1})
	n.acquire(0, handle, 1, 16)
	axis := r.defaultAxis.Axis()
	scope, err := temporal.All(axis)
	if err != nil {
		t.Fatal(err)
	}
	ops := []graphstate.Operation{
		{Kind: graphstate.CreateNode, Owner: 1, Life: 2, Scope: scope, Record: graphstate.EntityRecord{ID: 1, Kind: graphstate.Node, Axis: axis, Interpretation: graphstate.InterpretationState, TemporalRole: graphstate.TemporalRoleValidity}},
		{Kind: graphstate.CreateNode, Owner: 3, Life: 4, Scope: scope, Record: graphstate.EntityRecord{ID: 3, Kind: graphstate.Node, Axis: axis, Interpretation: graphstate.InterpretationState, TemporalRole: graphstate.TemporalRoleValidity}},
		{Kind: graphstate.CreateRelationship, Owner: 5, Life: 6, Scope: scope, Binding: graphstate.LifeRecord{SourceLife: 2, TargetLife: 4}, Record: graphstate.EntityRecord{ID: 5, Kind: graphstate.Relationship, Axis: axis, Type: "edge", Source: 1, Target: 3, Mode: graphstate.LifeBound, Interpretation: graphstate.InterpretationAssertedRelation, TemporalRole: graphstate.TemporalRoleOccurrenceTime}},
		{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Name: "old", Scope: scope},
		{Kind: graphstate.Set, Owner: 1, Life: 2, Name: "p", Value: graphstate.I64(9), ValueID: 7, Scope: scope},
		{Kind: graphstate.Set, Owner: 5, Life: 6, Name: "p", Value: graphstate.I64(11), ValueID: 8, Scope: scope},
	}
	grant := grantReference{session: handle.session, sequence: 1}
	claims := []freshBinding{{role: entityBinding, id: 1, grant: grant}, {role: lifeBinding, owner: 1, id: 2, grant: grant}, {role: entityBinding, id: 3, grant: grant}, {role: lifeBinding, owner: 3, id: 4, grant: grant}, {role: entityBinding, id: 5, grant: grant}, {role: lifeBinding, owner: 5, id: 6, grant: grant}, {role: valueBinding, id: 7, grant: grant}, {role: valueBinding, id: 8, grant: grant}}
	h := n.hosts[0][0]
	p, err := h.PreparePartitionGraphOperations(n.requestID(), ops, 23, claims)
	if err != nil {
		t.Fatal(err)
	}
	original := n.submitPartitionGraph(0, p, reasonNone)
	p, err = h.PreparePartitionGraphOperations(n.requestID(), []graphstate.Operation{{Kind: graphstate.Close, Owner: 1, Life: 2, Scope: scope}, {Kind: graphstate.Close, Owner: 5, Life: 6, Scope: scope}}, 24, nil)
	if err != nil {
		t.Fatal(err)
	}
	n.submitPartitionGraph(0, p, reasonNone)
	assertHistory := func(h *allocationHost) {
		t.Helper()
		for _, id := range []graphstate.EntityID{1, 5} {
			old, _ := partitionProjection(t, h, original.index, id, axis)
			current, _ := partitionProjection(t, h, h.driver.Applied(), id, axis)
			if !old.Exists || !old.Active || !current.Exists || current.Active || len(old.Properties) != 1 || old.Properties[0].Name != "p" {
				t.Fatal("node/relationship old/current state lost", id, old, current)
			}
			value := int64(9)
			if id == 5 {
				value = 11
			}
			equal, err := old.Properties[0].Scalar.Equal(graphstate.I64(value), h.machine.limits.graph.Planner)
			if err != nil || !equal {
				t.Fatal("exact historical scalar", id, err)
			}
			if id == 1 && !reflect.DeepEqual(old.Labels, []string{"old"}) {
				t.Fatal("old label missing", old.Labels)
			}
		}
		unchanged, _ := partitionProjection(t, h, h.driver.Applied(), 3, axis)
		phantom, _ := partitionProjection(t, h, h.driver.Applied(), 9, axis)
		if !unchanged.Active || phantom.Exists || phantom.Active {
			t.Fatal("exact divergent lifecycle census", unchanged, phantom)
		}
	}
	for _, h := range n.hosts[0] {
		assertHistory(h)
	}
	for group, hosts := range n.hosts {
		for member, h := range hosts {
			if err := h.Close(); err != nil {
				t.Fatal(err)
			}
			if group == 0 && member == 0 {
				changed := g
				wrong := r
				wrong.maxBlock++
				changed.genesis, err = encodeDeclaredInit(wrong, h.machine.limits)
				if err != nil {
					t.Fatal(err)
				}
				bad, err := openLogicalGraphReplica(t.Context(), n.configs[group][member], changed, initialGraphPeer{h.machine.ns.partition, uint64(member + 1)}, h.machine.limits)
				if bad != nil || !errors.Is(err, graphstore.ErrRebinding) || !errors.Is(err, errInvalid) {
					t.Fatal("reopen accepted changed full genesis", err)
				}
			}
			reopened, err := openLogicalGraphReplica(t.Context(), n.configs[group][member], g, initialGraphPeer{h.machine.ns.partition, uint64(member + 1)}, h.machine.limits)
			if err != nil {
				t.Fatal(err)
			}
			n.hosts[group][member] = reopened
			t.Cleanup(func() {
				if err := reopened.Close(); err != nil {
					t.Error(err)
				}
			})
			if group == 0 {
				assertHistory(reopened)
			}
		}
	}
	out, err := n.hosts[0][0].Campaign()
	if err != nil {
		t.Fatal(err)
	}
	n.pump(0, out)
	service := n.read(0, allocationQuery{kind: observeAllocator})
	before, err := idalloc.Inspect(&service.observation.allocator)
	if err != nil || before.HighWater != 16 {
		t.Fatal("reopen reset high-water", before, err)
	}
	transfer, err := n.hosts[0][0].TransferAllocator(n.requestID(), service)
	if err != nil {
		t.Fatal(err)
	}
	n.submit(0, transfer)
	fresh := n.activate(0, [16]byte{2})
	cursor := n.acquire(0, fresh, 1, 4)
	for want := uint64(17); want <= 20; want++ {
		got, err := cursor.Next()
		if err != nil || got != want {
			t.Fatal("fresh issuance reused reserved IDs", got, want, err)
		}
	}
}

func TestLogicalGraphLifecyclePartialProvisionAndOldIdentityRefusals(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for _, failAt := range []int{1, 2} {
		t.Run(fmt.Sprintf("after%dseeds", failAt), func(t *testing.T) {
			spec := lifecycleSpec()
			base := t.TempDir()
			var hosts []*allocationHost
			var configs []raftlog.Config
			var captured provisionedLogicalGraph
			failure := errors.New("authenticated provisioning interrupted")
			p := lifecycleProvisionFunc(func(ctx context.Context, peer initialGraphPeer, wire []byte) error {
				captured = provisionedLogicalGraph{bytes.Clone(wire), spec.voters, spec.agreement}
				if len(hosts) == failAt {
					return failure
				}
				cfg := lifecycleConfig(filepath.Join(base, fmt.Sprintf("v%d", peer.voter)), peer.voter)
				h, err := provisionInitialGraphReplica(ctx, cfg, captured, peer, spec.limits)
				if err != nil {
					return err
				}
				hosts, configs = append(hosts, h), append(configs, cfg)
				return nil
			})
			g, err := createLogicalGraph(t.Context(), spec, p)
			if !errors.Is(err, failure) || len(g.genesis) != 0 || len(hosts) != failAt {
				t.Fatal("partial provisioning claimed completion", err)
			}
			for member, h := range hosts {
				if err := h.Close(); err != nil {
					t.Fatal(err)
				}
				other, err := createLogicalGraph(t.Context(), spec, lifecycleProvisionFunc(func(context.Context, initialGraphPeer, []byte) error { return nil }))
				if err != nil {
					t.Fatal(err)
				}
				wrong, err := openLogicalGraphReplica(t.Context(), configs[member], other, initialGraphPeer{3, uint64(member + 1)}, spec.limits)
				if wrong != nil || !errors.Is(err, raftlog.ErrInvalid) {
					t.Fatal("wrong retained graph binding", err)
				}
				reopened, err := openLogicalGraphReplica(t.Context(), configs[member], captured, initialGraphPeer{3, uint64(member + 1)}, spec.limits)
				if err != nil {
					t.Fatal("partial failure deleted synced disk", err)
				}
				index, image, err := reopened.machine.store.Checkpoint()
				root, decodeErr := graphstore.DecodeRoot(image)
				if err != nil || decodeErr != nil || index != 1 || root.SemanticEpoch() != 0 {
					t.Fatal("partial seed changed", index, err, decodeErr)
				}
				if err := reopened.Close(); err != nil {
					t.Fatal(err)
				}
				duplicate, err := provisionInitialGraphReplica(t.Context(), configs[member], captured, initialGraphPeer{3, uint64(member + 1)}, spec.limits)
				if duplicate != nil || !errors.Is(err, pebble.ErrDBAlreadyExists) {
					t.Fatal("same-directory fresh creation collision", err)
				}
			}
			missing := lifecycleConfig(filepath.Join(base, "lost-old-voter"), 3)
			bad, err := openLogicalGraphReplica(t.Context(), missing, captured, initialGraphPeer{3, 3}, spec.limits)
			if bad != nil || !errors.Is(err, pebble.ErrDBDoesNotExist) {
				t.Fatal("old identity bootstrapped on missing storage", err)
			}
			entries, err := os.ReadDir(missing.Dir)
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				t.Fatal(err)
			}
			// Pebble opens LOCK before checking ErrorIfNotExists. This bounded
			// housekeeping file is not a database, graph identity or seed.
			if len(entries) > 1 || len(entries) == 1 && (entries[0].Name() != "LOCK" || entries[0].IsDir()) {
				t.Fatal("missing old identity refusal created database material", entries)
			}
			retried, err := openLogicalGraphReplica(t.Context(), missing, captured, initialGraphPeer{3, 3}, spec.limits)
			if retried != nil || !errors.Is(err, pebble.ErrDBDoesNotExist) {
				t.Fatal("failed Open left a reopenable identity or seed", err)
			}
		})
	}
}

func TestLogicalGraphLifecycleActualMaterializerAgreements(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for _, agreement := range []raftlog.ApplicationSemanticContractID{partitionWriteSemanticContractID(), crossPartitionSemanticContractID(), certifiedGraphSemanticContractID(), rangeMoveSemanticContractID()} {
		t.Run(fmt.Sprintf("%x", agreement[:4]), func(t *testing.T) {
			spec := lifecycleSpec()
			spec.agreement = agreement
			spec.partitions = spec.partitions[:1]
			base := t.TempDir()
			calls := 0
			p := lifecycleProvisionFunc(func(ctx context.Context, peer initialGraphPeer, wire []byte) error {
				cfg := lifecycleConfig(filepath.Join(base, fmt.Sprintf("v%d", peer.voter)), peer.voter)
				if crossGraphAgreement(agreement) {
					cfg.Controls = raftlog.ApplicationControlConfig{Version: 1}
					cfg.Recovery = raftlog.ApplicationRecoveryConfig{Version: 1}
					var err error
					cfg.Transfer.Contract, err = raftlog.ApplicationContractForPolicyWithRecovery(cfg.Application, cfg.Controls, cfg.Recovery)
					if err != nil {
						return err
					}
				}
				g := provisionedLogicalGraph{wire, spec.voters, agreement}
				h, err := provisionInitialGraphReplica(ctx, cfg, g, peer, spec.limits)
				if err != nil {
					return err
				}
				if h.machine.SemanticContractID() != agreement {
					t.Fatal("wrong concrete materializer agreement")
				}
				calls++
				return h.Close()
			})
			if _, err := createLogicalGraph(t.Context(), spec, p); err != nil || calls != 3 {
				t.Fatal("actual agreement provisioning", calls, err)
			}
		})
	}
}

func TestLogicalGraphLifecycleIdentityAndAdmissionBoundaries(t *testing.T) {
	spec := lifecycleSpec()
	calls := 0
	p := lifecycleProvisionFunc(func(context.Context, initialGraphPeer, []byte) error { calls++; return nil })
	for _, entropy := range []struct {
		input []byte
		want  error
	}{
		{nil, io.EOF}, {make([]byte, 16), errInvalid}, {bytes.Repeat([]byte{1}, 32), errGraphIdentityCollision},
	} {
		g, err := createLogicalGraphWithEntropy(t.Context(), spec, p, bytes.NewReader(entropy.input))
		if !errors.Is(err, entropy.want) || len(g.genesis) != 0 || calls != 0 {
			t.Fatal("entropy/collision refusal dispatched", err)
		}
	}
	var nilProvision lifecycleProvisionFunc
	for _, provider := range []initialGraphProvisioner{nil, nilProvision} {
		if _, err := createLogicalGraph(t.Context(), spec, provider); !errors.Is(err, errInvalid) {
			t.Fatal("nil provider", err)
		}
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := createLogicalGraph(cancelled, spec, p); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	var missingContext context.Context
	if _, err := createLogicalGraph(missingContext, spec, p); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	for _, edit := range []func(*logicalGraphCreationSpec){
		func(s *logicalGraphCreationSpec) {
			s.partitions = []graphstore.PartitionOwnership{{Partition: 3, OwnershipEpoch: 1, Group: [16]byte{1}}}
		},
		func(s *logicalGraphCreationSpec) { s.voters = [3]uint64{1, 1, 3} },
		func(s *logicalGraphCreationSpec) { s.agreement = registeredInventorySemanticContractID() },
		func(s *logicalGraphCreationSpec) { s.partitions = nil },
	} {
		changed := spec
		edit(&changed)
		if _, err := createLogicalGraph(t.Context(), changed, p); !errors.Is(err, errInvalid) {
			t.Fatal("invalid source spec", err)
		}
	}
	g, err := createLogicalGraph(t.Context(), spec, p)
	if err != nil || calls != 6 {
		t.Fatal("bounded per-voter provisioning", calls, err)
	}
	decoded, err := decodeDeclaredInit(g.genesis, spec.limits)
	if err != nil || decoded.ns.graph == (idalloc.GraphID{}) {
		t.Fatal(err)
	}
	cfg := lifecycleConfig(filepath.Join(t.TempDir(), "must-not-open"), 1)
	for _, edit := range []func(*raftlog.Config){
		func(c *raftlog.Config) { c.Create = true },
		func(c *raftlog.Config) { c.Transfer.Identity.Graph = [16]byte{1} },
		func(c *raftlog.Config) { c.SemanticContractID = spec.agreement },
		func(c *raftlog.Config) { c.Replication.Voters = spec.voters },
	} {
		changed := cfg
		edit(&changed)
		h, err := openLogicalGraphReplica(t.Context(), changed, g, initialGraphPeer{3, 1}, spec.limits)
		if h != nil || !errors.Is(err, errInvalid) {
			t.Fatal("caller overrode lifecycle binding", err)
		}
	}
	if _, err := openLogicalGraphReplica(missingContext, cfg, g, initialGraphPeer{3, 1}, spec.limits); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if _, err := openLogicalGraphReplica(cancelled, cfg, g, initialGraphPeer{3, 1}, spec.limits); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := openLogicalGraphReplica(t.Context(), cfg, g, initialGraphPeer{99, 1}, spec.limits); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
}
