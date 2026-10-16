package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"testing"
	"unsafe"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/dist/replica"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	pb "go.etcd.io/raft/v3/raftpb"
)

// The donor is an existing real graph fixture. Export/import/Verify/Prepare are
// genuine transport owners; the receiver stays at empty index0 without a live
// materializer, Driver, Host or bootstrap. No snapshot wire/row is invented.
func preparedDeclaredAuditFixture(t *testing.T, source *raftlog.Store, cfg raftlog.Config) (*raftlog.Store, *raftlog.PreparedApplicationSnapshot, raftlog.ApplicationSnapshotManifest, replica.ApplicationSnapshotContext, *pb.Snapshot) {
	t.Helper()
	if err := source.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	cut, err := source.PublishedApplicationCut()
	if err != nil {
		t.Fatal(err)
	}
	export, err := source.BeginPublishedApplicationExport(t.Context(), cut.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := export.Close(); err != nil {
			t.Error(err)
		}
	}()
	ready := false
	for range 256 {
		ready, err = export.BuildManifest(t.Context(), raftlog.ReadBudget{Rows: 64, Bytes: 256 << 10})
		if err != nil {
			t.Fatal(err)
		}
		if ready {
			break
		}
	}
	if !ready {
		t.Fatal("bounded genuine manifest build")
	}
	manifest, err := export.Manifest()
	if err != nil {
		t.Fatal(err)
	}
	cfg.Dir, cfg.FS, cfg.Create = "empty-audit-receiver", vfs.NewMem(), true
	receiver, err := raftlog.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := receiver.Close(); err != nil {
			t.Error(err)
		}
	})
	incoming, err := receiver.BeginApplicationImport(t.Context(), manifest)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := incoming.Abort(); err != nil {
			t.Error(err)
		}
	})
	complete := false
	var manifestID [32]byte
	for range 256 {
		chunk, err := export.Next(t.Context(), raftlog.ReadBudget{Rows: 64, Bytes: 256 << 10})
		if err != nil {
			t.Fatal(err)
		}
		if manifestID != [32]byte{} && manifestID != chunk.ManifestID {
			t.Fatal("genuine transfer manifest changed")
		}
		manifestID = chunk.ManifestID
		if err := incoming.Append(t.Context(), chunk); err != nil {
			t.Fatal(err)
		}
		if chunk.Final {
			complete = true
			break
		}
	}
	if !complete {
		t.Fatal("bounded genuine import stream")
	}
	if err := incoming.Verify(t.Context()); err != nil {
		t.Fatal("transport did not verify", err)
	}
	prepared, err := incoming.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := prepared.Close(); err != nil {
			t.Error(err)
		}
	})
	wire, err := raftlog.EncodeApplicationSnapshotDescriptor(manifest)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := &pb.Snapshot{Data: wire, Metadata: &pb.SnapshotMetadata{Index: new(manifest.Index), Term: new(manifest.Term), ConfState: manifest.ConfState}}
	if err := receiver.CheckPreparedApplicationSnapshot(prepared, snapshot); err != nil {
		t.Fatal("actual receiver prepared capability absent", err)
	}
	voters := manifest.ConfState.GetVoters()
	if len(voters) != 3 {
		t.Fatal("genuine fixed membership absent")
	}
	c := replica.ApplicationSnapshotContext{Index: manifest.Index, Term: manifest.Term, Voters: [3]uint64{voters[0], voters[1], voters[2]}, DescriptorHash: sha256.Sum256(wire), CutID: manifest.CutID, ManifestID: manifestID}
	return receiver, prepared, manifest, c, snapshot
}

func beginDeclaredTestAudit(t *testing.T, a *declaredSnapshotAuditor, p *raftlog.PreparedApplicationSnapshot, c replica.ApplicationSnapshotContext) *declaredSnapshotAudit {
	t.Helper()
	owner, err := a.BeginApplicationSnapshotAudit(p, c, replica.ApplicationAuditBudget{WorkBytes: 64 << 20, OwnedBytes: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	audit, ok := owner.(*declaredSnapshotAudit)
	if !ok {
		t.Fatal("wrong semantic owner")
	}
	t.Cleanup(func() {
		if err := audit.Close(); err != nil {
			t.Error(err)
		}
	})
	return audit
}

func finishDeclaredTestAudit(t *testing.T, a *declaredSnapshotAudit) {
	t.Helper()
	budget := replica.ApplicationAuditBudget{WorkBytes: 64 << 20, OwnedBytes: 64 << 20}
	if done, err := a.Advance(t.Context(), budget); !done || err != nil {
		t.Fatal("shared restored-state validation", done, err)
	}
	if err := a.Finish(t.Context(), budget); err != nil {
		t.Fatal(err)
	}
	if a.view != nil || a.prepared != nil || a.auditor != nil || !a.sealed || uint64(unsafe.Sizeof(*a)) > declaredAuditFixedBytes {
		t.Fatal("Finish retained variable owners or unsealed state")
	}
	if err := a.Finish(t.Context(), budget); err != nil {
		t.Fatal("sealed Finish not idempotent", err)
	}
}

func TestDeclaredSnapshotAuditorInitializedNodeRelationshipAndRetainedRoots(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCertifiedGraphNetwork(t)
	ops, claims := crossEdgeFixture(t, n)
	tx, registration := n.crossPlan(ops, claims, 24)
	decision := n.crossCommit(tx, registration)
	n.crossResolve(decision)
	h := n.hosts[0][0]
	oldIndex := h.driver.Applied()
	oldImageIndex, oldImage, err := h.machine.store.Checkpoint()
	if err != nil || oldImageIndex != oldIndex {
		t.Fatal(err)
	}
	proposal, err := h.PreparePartitionGraphOperations(n.requestID(), []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Scope: ops[0].Scope, Name: "later"}}, 25, nil)
	if err != nil {
		t.Fatal(err)
	}
	n.submitPartitionGraph(0, proposal, reasonNone)
	receiver, prepared, manifest, c, snapshot := preparedDeclaredAuditFixture(t, h.machine.store, n.configs[0][2])
	if manifest.Index <= oldIndex || bytes.Equal(manifest.Image, oldImage) {
		t.Fatal("fixture did not mutate after retained root")
	}
	auditor, err := newDeclaredSnapshotAuditor(receiver, h.machine.ns, n.d, h.machine.limits, h.machine.SemanticContractID())
	if err != nil {
		t.Fatal(err)
	}
	if _, live := any(auditor).(replica.ApplicationMachine); live {
		t.Fatal("read-only auditor accidentally became live materializer")
	}
	v, err := prepared.OpenReadView(t.Context(), oldIndex, 172)
	if err != nil {
		t.Fatal(err)
	}
	old, err := v.RootBounded(t.Context(), 172)
	if err != nil || !bytes.Equal(old.Image, oldImage) || old.Generation == receiver.ApplicationGeneration() {
		t.Fatal("inactive retained root substituted active/current root", old, err)
	}
	cfg, err := h.currentConfiguration()
	if err != nil {
		t.Fatal(err)
	}
	catalog, _, err := graphstore.OpenPartitionCatalogWithDefaultAxis(t.Context(), v, graphstore.Namespace{Graph: n.d.Graph(), Partition: h.machine.ns.partition}, h.machine.owner, cfg.defaultAxis, h.machine.limits.catalog, h.machine.limits.graph, graphstore.OwnershipBudget{SourceRows: 512, SourceBytes: 4 << 20, OutputBytes: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []struct {
		id   graphstate.EntityID
		kind graphstate.EntityKind
	}{{1, graphstate.Node}, {3, graphstate.Relationship}} {
		ref := graphstore.EntityRef{Graph: n.d.Graph(), ID: expected.id}
		record, found, err := catalog.Entity(t.Context(), ref)
		if err != nil || !found || record.ID != ref.ID || record.Kind != expected.kind || record.Axis.Descriptor() != cfg.defaultAxis.Axis().Descriptor() {
			t.Fatal("real node/relationship metadata missing", ref, record, found, err)
		}
		if expected.kind == graphstate.Relationship && (record.Source != 1 || record.Target != 17 || record.Type != "cross-edge") {
			t.Fatal("cross endpoint/type metadata lost", record)
		}
		if _, found, err := catalog.Entity(t.Context(), graphstore.EntityRef{Graph: n.d.Graph(), ID: 999}); err != nil || found {
			t.Fatal("phantom entity returned", found, err)
		}
	}
	for _, def := range partitionWriteSchemas() {
		actual, found, err := catalog.Property(t.Context(), def.Owner, def.Name)
		if err != nil || !found || actual != def {
			t.Fatal("co-initialized schema lost", actual, found, err)
		}
	}
	row, found, err := v.Get(t.Context(), allocatorKey(h.machine.ns), 4096)
	if err != nil || !found {
		t.Fatal(err)
	}
	state, err := idalloc.DecodeCheckpoint(row.Value)
	if err != nil {
		t.Fatal(err)
	}
	info, err := idalloc.Inspect(&state)
	if err != nil || info.HighWater != 32 || info.MaxBlock != cfg.maxBlock {
		t.Fatal("real HOME allocation high-water missing", info, err)
	}
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
	audit := beginDeclaredTestAudit(t, auditor, prepared, c)
	if err := audit.Finish(t.Context(), auditor.advance); !errors.Is(err, errInvalid) {
		t.Fatal("unaudited Finish granted seal", err)
	}
	if claim, err := prepared.Claim(snapshot); claim != nil || !errors.Is(err, raftlog.ErrLimit) {
		t.Fatal("live audit allowed Claim", claim, err)
	}
	finishDeclaredTestAudit(t, audit)
	claim, err := prepared.Claim(snapshot)
	if err != nil {
		t.Fatal("Finish did not close prepared readers", err)
	}
	if err := claim.Close(); err != nil {
		t.Fatal(err)
	}
	index, image, err := receiver.Checkpoint()
	if err != nil || index != 0 || len(image) != 0 {
		t.Fatal("semantic audit activated/bootstrap receiving state", index, image, err)
	}
	if err := audit.Close(); err != nil {
		t.Fatal(err)
	}
	if err := audit.Close(); err != nil {
		t.Fatal("Close not idempotent", err)
	}
}

func TestDeclaredSnapshotAuditorRefusesSeedWhileRestorePreservesSeed(t *testing.T) {
	d := declaredTestDeclaration(t, []graphstore.PartitionOwnership{{Partition: 3, OwnershipEpoch: 2, Group: [16]byte{4}}, {Partition: 8, OwnershipEpoch: 3, Group: [16]byte{5}}})
	g := newDeclaredTestGroup(t, d, 3)
	index, image, err := g.stores[0].Checkpoint()
	if err != nil || g.machines[0].Restore(index, image) != nil {
		t.Fatal("ordinary pending-seed Restore changed", err)
	}
	receiver, prepared, _, c, _ := preparedDeclaredAuditFixture(t, g.stores[0], g.configs[2])
	auditor, err := newDeclaredSnapshotAuditor(receiver, g.ns, d, defaultMaterializerLimits(), declaredSemanticContractID())
	if err != nil {
		t.Fatal(err)
	}
	audit := beginDeclaredTestAudit(t, auditor, prepared, c)
	if done, err := audit.Advance(t.Context(), auditor.advance); done || !errors.Is(err, errNotInitialized) {
		t.Fatal("pending seed admitted for replacement", done, err)
	}
	if err := audit.Finish(t.Context(), auditor.advance); !errors.Is(err, errInvalid) {
		t.Fatal("failed audit granted seal", err)
	}
}

func TestDeclaredSnapshotAuditorTransportValidSemanticPrerequisitesRefuse(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for _, kind := range []string{"missing-genesis", "missing-HOME-allocator", "wrong-max-block", "missing-routing", "missing-round-floor", "missing-certificate-fence", "orphan-YES-lock"} {
		t.Run(kind, func(t *testing.T) {
			n := newCertifiedGraphNetwork(t)
			h := n.hosts[0][0]
			row := raftlog.KV{Key: genesisConfigurationKey(h.machine.ns), Deleted: true}
			switch kind {
			case "missing-HOME-allocator":
				row.Key = allocatorKey(h.machine.ns)
			case "wrong-max-block":
				authority, err := idalloc.NewAuthority([16]byte{7}, 1)
				if err != nil {
					t.Fatal(err)
				}
				state, err := idalloc.NewState(h.machine.ns.graph, authority, 32)
				if err != nil {
					t.Fatal(err)
				}
				row.Key, row.Deleted = allocatorKey(h.machine.ns), false
				row.Value, err = idalloc.MarshalCheckpoint(&state)
				if err != nil {
					t.Fatal(err)
				}
			case "missing-routing":
				row.Key = graphstore.RoutingDefinitionKey(graphstore.Namespace{Graph: n.d.Graph(), Partition: h.machine.ns.partition})
			case "missing-round-floor":
				row.Key = partitionRoundFloorKey(h.machine.ns)
			case "missing-certificate-fence":
				row.Key = graphFenceKey(h.machine.ns)
			case "orphan-YES-lock":
				row.Key, row.Deleted, row.Value = crossLockKey(h.machine.ns), false, []byte("0123456789abcdef")
			}
			// Existing privileged corruption builder creates honest frames and
			// retained accounting. Transport verifies; semantic Restore must fail.
			index := corruptDeclaredKV(t, h.machine.store, row)
			_, image, err := h.machine.store.Checkpoint()
			if err != nil {
				t.Fatal(err)
			}
			if err := h.machine.Restore(index, image); !errors.Is(err, errCorrupt) {
				t.Fatal("ordinary Restore did not identify prerequisite corruption", err)
			}
			receiver, prepared, _, c, _ := preparedDeclaredAuditFixture(t, h.machine.store, n.configs[0][2])
			auditor, err := newDeclaredSnapshotAuditor(receiver, h.machine.ns, n.d, h.machine.limits, h.machine.SemanticContractID())
			if err != nil {
				t.Fatal(err)
			}
			audit := beginDeclaredTestAudit(t, auditor, prepared, c)
			if done, err := audit.Advance(t.Context(), auditor.advance); done || !errors.Is(err, errCorrupt) {
				t.Fatal("transport verification substituted semantic Restore", done, err)
			}
		})
	}
}

func TestDeclaredSnapshotAuditorPreparedCompletionOwnerAndYESLock(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCertifiedGraphNetwork(t)
	ops, claims := crossEdgeFixture(t, n)
	tx, proposal := n.crossPlan(ops, claims, 24)
	registered := n.crossSubmit(0, proposal)
	registration := n.crossReadPhase(0, registered.tx, crossRegister)
	proposal, err := n.hosts[0][0].PrepareCrossParticipant(tx, 3, registration, nil)
	if err != nil {
		t.Fatal(err)
	}
	status := n.crossSubmit(0, proposal)
	if !status.yes || status.phase != crossPrepare {
		t.Fatal("actual durable YES absent", status)
	}
	h := n.hosts[0][0]
	receiver, prepared, _, c, _ := preparedDeclaredAuditFixture(t, h.machine.store, n.configs[0][2])
	auditor, err := newDeclaredSnapshotAuditor(receiver, h.machine.ns, n.d, h.machine.limits, h.machine.SemanticContractID())
	if err != nil {
		t.Fatal(err)
	}
	audit := beginDeclaredTestAudit(t, auditor, prepared, c)
	finishDeclaredTestAudit(t, audit)
	if index, _, err := receiver.Checkpoint(); err != nil || index != 0 {
		t.Fatal("YES audit activated or supplied a decision", index, err)
	}
}

func TestDeclaredSnapshotAuditorPreparedRangeMoveUsesRetainedInactiveBaseline(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n, _, intent, _, transports := rangeMoveCoordinatorFixture(t, 64)
	gate := &rangeMoveAdmissionImportGate{graphPeerWireTransport: transports[8], sequence: 3}
	transports[8] = gate
	coordinator, err := newRangeMoveCoordinator(n.hosts[0][0], transports)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := runRangeMoveToCompletion(t, coordinator, intent)
	if !errors.Is(err, errRangeMovePending) || !errors.Is(err, io.EOF) || pending.committed || !gate.stopped {
		t.Fatal("genuine target import did not pause", pending, err)
	}
	h := n.hosts[1][0]
	receiver, prepared, manifest, c, _ := preparedDeclaredAuditFixture(t, h.machine.store, n.configs[1][2])
	auditor, err := newDeclaredSnapshotAuditor(receiver, h.machine.ns, n.d, h.machine.limits, h.machine.SemanticContractID())
	if err != nil {
		t.Fatal(err)
	}
	audit := beginDeclaredTestAudit(t, auditor, prepared, c)
	finishDeclaredTestAudit(t, audit)
	if usage, err := receiver.ApplicationUsage(); err != nil || usage.Views != 0 {
		t.Fatal("historical baseline reader leaked", usage, err)
	}
	if index, _, err := receiver.Checkpoint(); err != nil || index >= manifest.Index {
		t.Fatal("inactive audit read or replaced current target", index, err)
	}
}

func TestDeclaredSnapshotAuditorConstructorContextBudgetsAndLifetime(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCertifiedGraphNetwork(t)
	h := n.hosts[0][0]
	receiver, prepared, _, c, _ := preparedDeclaredAuditFixture(t, h.machine.store, n.configs[0][2])
	l, agreement := h.machine.limits, h.machine.SemanticContractID()
	for _, kind := range []string{"nil-store", "namespace", "agreement", "limits", "image-budget", "wrong-group"} {
		t.Run(kind, func(t *testing.T) {
			store, ns, bounds, semantic := receiver, h.machine.ns, l, agreement
			want := errInvalid
			switch kind {
			case "nil-store":
				store = nil
			case "namespace":
				ns.graph = idalloc.GraphID{99}
			case "agreement":
				semantic = raftlog.ApplicationSemanticContractID{99}
			case "limits":
				bounds.sourceRows = 0
			case "image-budget":
				bounds.sourceBytes = 171
				want = errLimit
			case "wrong-group":
				ns.partition = 8
				want = graphstore.ErrNamespace
			}
			if a, err := newDeclaredSnapshotAuditor(store, ns, n.d, bounds, semantic); a != nil || !errors.Is(err, want) {
				t.Fatal("invalid constructor performed semantic work", a, err)
			}
		})
	}
	auditor, err := newDeclaredSnapshotAuditor(receiver, h.machine.ns, n.d, l, agreement)
	if err != nil {
		t.Fatal(err)
	}
	big := replica.ApplicationAuditBudget{WorkBytes: 64 << 20, OwnedBytes: 64 << 20}
	if a, err := (*declaredSnapshotAuditor)(nil).BeginApplicationSnapshotAudit(prepared, c, big); a != nil || !errors.Is(err, errInvalid) {
		t.Fatal(a, err)
	}
	if a, err := auditor.BeginApplicationSnapshotAudit(nil, c, big); a != nil || !errors.Is(err, errInvalid) {
		t.Fatal(a, err)
	}
	if a, err := auditor.BeginApplicationSnapshotAudit(new(raftlog.PreparedApplicationSnapshot), c, big); a != nil || !errors.Is(err, raftlog.ErrInvalid) {
		t.Fatal(a, err)
	}
	for _, kind := range []string{"index", "term", "descriptor", "cut", "manifest", "voters"} {
		bad := c
		switch kind {
		case "index":
			bad.Index = 0
		case "term":
			bad.Term = 0
		case "descriptor":
			bad.DescriptorHash = [32]byte{}
		case "cut":
			bad.CutID = [32]byte{}
		case "manifest":
			bad.ManifestID = [32]byte{}
		case "voters":
			bad.Voters[1] = bad.Voters[0]
		}
		if a, err := auditor.BeginApplicationSnapshotAudit(prepared, bad, big); a != nil || !errors.Is(err, errInvalid) {
			t.Fatal(kind, a, err)
		}
	}
	before, err := receiver.ApplicationTransferUsage()
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range []replica.ApplicationAuditBudget{{}, {WorkBytes: 1, OwnedBytes: 1}} {
		want := replica.ErrLimit
		if b == (replica.ApplicationAuditBudget{}) {
			want = replica.ErrInvalid
		}
		if a, err := auditor.BeginApplicationSnapshotAudit(prepared, c, b); a != nil || !errors.Is(err, want) {
			t.Fatal("Begin ignored operation budget", a, err)
		}
	}
	foreign := *auditor
	foreign.validation.binding.Identity.Graph[0] ^= 1
	if a, err := foreign.BeginApplicationSnapshotAudit(prepared, c, big); a != nil || !errors.Is(err, errInvalid) {
		t.Fatal("binding mismatch accepted", a, err)
	}
	mismatch := *auditor
	mismatch.contract.MaxImageBytes++
	if a, err := mismatch.BeginApplicationSnapshotAudit(prepared, c, big); a != nil || !errors.Is(err, errInvalid) {
		t.Fatal("actual contract mismatch accepted", a, err)
	}
	if after, err := receiver.ApplicationTransferUsage(); err != nil || after != before {
		t.Fatal("Begin refusal retained a view/pin", before, after, err)
	}
	audit := beginDeclaredTestAudit(t, auditor, prepared, c)
	if done, err := audit.Advance(nil, big); done || !errors.Is(err, errInvalid) { //nolint:staticcheck // Deliberate nil-context refusal contract.
		t.Fatal(done, err)
	}
	if err := audit.Finish(nil, big); !errors.Is(err, errInvalid) { //nolint:staticcheck // Deliberate nil-context refusal contract.
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if done, err := audit.Advance(ctx, big); done || !errors.Is(err, context.Canceled) {
		t.Fatal(done, err)
	}
	if err := audit.Finish(ctx, big); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if done, err := audit.Advance(t.Context(), replica.ApplicationAuditBudget{WorkBytes: 1, OwnedBytes: 1}); done || !errors.Is(err, replica.ErrLimit) {
		t.Fatal(done, err)
	}
	if err := prepared.Close(); !errors.Is(err, raftlog.ErrLimit) {
		t.Fatal("budget/cancellation released live prepared borrow", err)
	}
	if done, err := audit.Advance(t.Context(), big); !done || err != nil {
		t.Fatal("bounded resource relief did not audit same import", done, err)
	}
	if err := audit.Finish(t.Context(), replica.ApplicationAuditBudget{WorkBytes: 1, OwnedBytes: 1}); !errors.Is(err, replica.ErrLimit) {
		t.Fatal(err)
	}
	if err := audit.Finish(t.Context(), big); err != nil {
		t.Fatal(err)
	}
	if done, err := audit.Advance(t.Context(), big); !done || err != nil {
		t.Fatal("sealed owner could not report its completion", done, err)
	}
	if done, err := audit.Advance(t.Context(), replica.ApplicationAuditBudget{}); done || !errors.Is(err, replica.ErrInvalid) {
		t.Fatal("sealed state bypassed invalid budget", done, err)
	}
	if err := audit.Close(); err != nil {
		t.Fatal(err)
	}
	if done, err := audit.Advance(t.Context(), big); done || !errors.Is(err, raftlog.ErrClosed) {
		t.Fatal(done, err)
	}
	if err := audit.Finish(t.Context(), big); !errors.Is(err, raftlog.ErrClosed) {
		t.Fatal(err)
	}
	if err := audit.Close(); err != nil {
		t.Fatal("repeated Close changed original cleanup result", err)
	}
	var nilAudit *declaredSnapshotAudit
	if done, err := nilAudit.Advance(t.Context(), big); done || !errors.Is(err, errInvalid) {
		t.Fatal(done, err)
	}
	if err := nilAudit.Finish(t.Context(), big); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if err := nilAudit.Close(); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	invalid := &declaredSnapshotAudit{self: audit}
	if done, err := invalid.Advance(t.Context(), big); done || !errors.Is(err, errInvalid) {
		t.Fatal(done, err)
	}
	if err := invalid.Close(); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
}

func TestDeclaredSnapshotAuditorClosedStoreRetainsOriginalReadErrorAndReleasesBorrow(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCertifiedGraphNetwork(t)
	h := n.hosts[0][0]
	receiver, prepared, _, c, _ := preparedDeclaredAuditFixture(t, h.machine.store, n.configs[0][2])
	auditor, err := newDeclaredSnapshotAuditor(receiver, h.machine.ns, n.d, h.machine.limits, h.machine.SemanticContractID())
	if err != nil {
		t.Fatal(err)
	}
	audit := beginDeclaredTestAudit(t, auditor, prepared, c)
	if err := receiver.Close(); err != nil {
		t.Fatal(err)
	}
	if done, err := audit.Advance(t.Context(), auditor.advance); done || !errors.Is(err, raftlog.ErrClosed) {
		t.Fatal("closed receiving bank error replaced", done, err)
	}
	for range 2 {
		if err := audit.Close(); err != nil {
			t.Fatal("closed-store owner release changed result", err)
		}
	}
	if audit.view != nil || audit.prepared != nil || audit.auditor != nil {
		t.Fatal("Close retained invalidated prepared/configuration owners")
	}
}
