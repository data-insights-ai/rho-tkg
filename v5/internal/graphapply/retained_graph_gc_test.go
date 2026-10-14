package graphapply

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

// Explicit test-only fresh binding, not an exported/profile constructor. Reuse
// the existing six-voter packet pump without a second oracle or child processes.
func collectedGraphNetwork(t *testing.T) *allocationTestNetwork {
	t.Helper()
	r := partitionCodecRequest(t, 1, defaultMaterializerLimits())
	n := &allocationTestNetwork{t: t, d: r.declaration, pending: make(map[[16]byte]allocationProof)}
	for g, partition := range []uint64{3, 8} {
		for member := range 3 {
			id := uint64(member + 1)
			p := raftlog.DefaultApplicationPolicy(id)
			p.MaxImageBytes = 172
			p.MaxInstallWrites = 256
			p.RetainedApplicationRecords = 8192
			p.RetainedApplicationBytes = 16 << 20
			control, recovery, retention := raftlog.ApplicationControlConfig{Version: 1}, raftlog.ApplicationRecoveryConfig{Version: 2}, raftlog.ApplicationRetentionConfig{Version: 1}
			contract, err := raftlog.ApplicationContractForPolicyWithRetention(p, control, recovery, retention)
			if err != nil {
				t.Fatal(err)
			}
			part, _ := n.d.Partition(partition)
			cfg := raftlog.Config{Dir: filepath.Join(t.TempDir(), fmt.Sprintf("gc-%d-%d", partition, id)), Create: true, Application: p, Controls: control, Recovery: recovery, Retention: retention, SemanticContractID: collectedGraphSemanticContractID(), Replication: raftlog.ApplicationReplicationConfig{Voters: [3]uint64{1, 2, 3}}, Transfer: raftlog.ApplicationTransferConfig{Identity: raftlog.ApplicationIdentity{Graph: [16]byte(n.d.Graph()), Partition: partition, Group: part.Group}, Contract: contract, Limits: raftlog.DefaultApplicationTransferLimits()}, Generations: raftlog.ApplicationGenerationLimits{MaxBytes: 32 << 20, MaxRecords: 16384}, PublishedCuts: raftlog.ApplicationPublishedCutLimits{MaxTransferChunks: 4096}}
			n.configs[g][member] = cfg
			s, err := raftlog.Open(cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := s.Close(); err != nil {
					t.Error(err)
				}
			})
			if err := graphstore.BootstrapBoundOwnership(s, s.ApplicationBinding(), n.d, [3]uint64{1, 2, 3}); err != nil {
				t.Fatal(err)
			}
			m, err := newDeclaredMaterializerWithAgreement(s, namespace{idalloc.GraphID(n.d.Graph()), partition}, n.d, defaultMaterializerLimits(), collectedGraphSemanticContractID())
			if err != nil {
				t.Fatal(err)
			}
			h, err := openAllocationHost(m, id, replica.Config{Store: s, ApplicationSnapshotSends: replica.ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 4 << 20}})
			if err != nil {
				t.Fatal(err)
			}
			n.hosts[g][member] = h
			t.Cleanup(func() {
				if err := h.Close(); err != nil {
					t.Error(err)
				}
			})
		}
		e, err := n.hosts[g][0].Campaign()
		if err != nil {
			t.Fatal(err)
		}
		n.pump(g, e)
	}
	h := n.hosts[0][0]
	p, err := h.PreparePartitionInitialization(3, bootstrapAttemptID{1}, partitionWriteSchemas(), r.maxBlock, graphGenesisBinding(t), *r.routing, allocationProof{})
	if err != nil {
		t.Fatal(err)
	}
	n.submit(0, p)
	p, err = h.PreparePartitionInitialization(8, bootstrapAttemptID{2}, partitionWriteSchemas(), r.maxBlock, graphGenesisBinding(t), *r.routing, n.read(0, allocationQuery{kind: observeConfiguration}))
	if err != nil {
		t.Fatal(err)
	}
	n.submit(1, p)
	return n
}
func TestCollectedGraphRetentionNativeStageOriginalAndCurrentSource(t *testing.T) {
	n := collectedGraphNetwork(t)
	h := n.hosts[0][0]
	for _, id := range []raftlog.ApplicationSemanticContractID{collectedGraphSemanticContractID(), retainedGraphSemanticContractID(), conditionalGraphSemanticContractID()} {
		if !conditionalGraphAgreement(id) || !rangeMoveGraphAgreement(id) || !certifiedGraphAgreement(id) || !crossGraphAgreement(id) || !partitionGraphAgreement(id) {
			t.Fatal("semantic capability chain stranded", id)
		}
	}
	engine, _ := certifiedCoordinator(n)
	read := certifiedOpen(t, engine, graphReadRequest{mode: graphReadFresh}, nil)
	cut, err := read.Cut()
	if err != nil {
		t.Fatal(err)
	}
	if err := read.Close(); err != nil {
		t.Fatal(err)
	}
	var old graphCertificate
	for _, cert := range cut.certificates {
		if cert.scope.partition == h.machine.ns.partition {
			old = cert
		}
	}
	if old.index == 0 {
		t.Fatal("missing actual home certificate")
	}
	current, err := h.machine.store.ApplicationView(h.driver.Applied())
	if err != nil {
		t.Fatal(err)
	}
	plm, found, err := current.Get(t.Context(), partitionRoundMapKey(h.machine.ns, 0), 256)
	if err != nil || !found {
		t.Fatal("actual original PLM", err)
	}
	if err := current.Close(); err != nil {
		t.Fatal(err)
	}
	// A partial participant publication already protects its actual data/feed;
	// neither local expiry time nor lack of a full portable token frees it.
	manifest := graphPinManifest{publisher: h.machine.scope(), issuer: h.session, request: [16]byte(n.requestID()), expires: 2}
	for _, cert := range cut.certificates {
		part := graphPinPart{certificate: cert, feedStart: cert.sourceIndex + 1}
		part.lineage, err = pinLineage(part)
		if err != nil {
			t.Fatal(err)
		}
		manifest.parts = append(manifest.parts, part)
	}
	pinWire, err := encodeGraphPinManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	// Follower preflight observes no pin. Hold its actual forwarded proposal;
	// the leader installs the pin first, so the retained GC body reaches Stage
	// after its admission observation and must install an immutable Blocked.
	raceBound := h.driver.Applied()
	delayed, err := n.hosts[0][1].PrepareGraphRetention(t.Context(), n.requestID(), graphRetentionFloors{raceBound, raceBound, raceBound + 1, raceBound, raceBound, raceBound})
	if err != nil {
		t.Fatal(err)
	}
	held, err := n.hosts[0][1].Submit(delayed)
	if err != nil || len(held.packets) != 1 || held.packets[0].From != 2 || held.packets[0].To != 1 {
		t.Fatal("actual delayed forwarded proposal", held, err)
	}
	pin := pinCommandFor(manifest, pinWire, h.machine.scope(), pinPublish, manifest.request, 0)
	if got := retainedPinSubmit(t, n, pin); got.status != pinApplied {
		t.Fatal(got)
	}
	raced, err := h.Step(held.packets[0])
	if err != nil {
		t.Fatal(err)
	}
	n.pump(0, raced)
	blocked, found, err := h.GraphRetentionOriginal(t.Context(), delayed)
	if err != nil || !found || blocked.status != graphRetentionBlocked {
		t.Fatal("Stage race did not retain first refusal", blocked, found, err)
	}
	bound := h.driver.Applied()
	floors := graphRetentionFloors{bound, bound, bound + 1, bound, bound, bound}
	before := h.driver.Applied()
	if _, err := h.PrepareGraphRetention(t.Context(), n.requestID(), floors); !errors.Is(err, errGraphRetentionPending) || h.driver.Applied() != before {
		t.Fatal("actual pin did not protect footprint", err)
	}
	terminal := pinCommandFor(manifest, pinWire, h.machine.scope(), pinRelease, [16]byte(n.requestID()), 0)
	if got := retainedPinSubmit(t, n, terminal); got.status != pinApplied {
		t.Fatal("funded terminal", got)
	}
	raced, err = h.Submit(delayed)
	if err != nil {
		t.Fatal("refused original replay", err)
	}
	n.pump(0, raced)
	if got, found, err := h.GraphRetentionOriginal(t.Context(), delayed); err != nil || !found || got != blocked {
		t.Fatal("cleared blocker changed original decision", got, found, err)
	}
	unchanged, err := h.machine.store.ApplicationView(h.driver.Applied())
	if err != nil {
		t.Fatal(err)
	}
	plan, enabled, _, err := unchanged.RetentionPlan(t.Context(), raftlog.ReadBudget{Rows: 1, Bytes: 4096})
	if closeErr := unchanged.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if err != nil || !enabled || plan.ID != ([32]byte{}) {
		t.Fatal("refused replay installed a plan", plan, err)
	}
	bound = h.driver.Applied()
	floors = graphRetentionFloors{bound, bound, bound + 1, bound, bound, bound}
	proposal, err := h.PrepareGraphRetention(t.Context(), n.requestID(), floors)
	if err != nil {
		t.Fatal(err)
	}
	command, err := decodeGraphRetentionCommand(proposal.wire)
	if err != nil {
		t.Fatal(err)
	}
	var nilHost *allocationHost
	if _, err := nilHost.PrepareGraphRetention(t.Context(), n.requestID(), floors); !errors.Is(err, errInvalid) {
		t.Fatal("nil prepare", err)
	}
	if _, _, err := nilHost.GraphRetentionOriginal(t.Context(), proposal); !errors.Is(err, errInvalid) {
		t.Fatal("nil original", err)
	}
	if _, found, err := h.GraphRetentionOriginal(t.Context(), proposal); err != nil || found {
		t.Fatal("unapplied bytes invented original", found, err)
	}
	// This exercises Host Submit -> ordinary recovery classification -> actual
	// Raft persistence -> Stage -> opaque native installation on all three voters.
	event, err := h.Submit(proposal)
	if err != nil {
		t.Fatal(err)
	}
	// Open a genuine reader AFTER Host preflight/Propose but before the
	// forwarded quorum ACKs can commit/apply this entry. Its nondurable lifetime
	// may block physical deletion, never replicated floor installation.
	lateView, err := h.machine.store.ApplicationView(h.driver.Applied())
	if err != nil {
		t.Fatal(err)
	}
	lateRoot, err := lateView.RootAt(t.Context(), 1, 172)
	if err != nil {
		t.Fatal(err)
	}
	n.pump(0, event)
	stillRoot, err := lateView.RootAt(t.Context(), 1, 172)
	if err != nil || !bytes.Equal(stillRoot.Image, lateRoot.Image) || stillRoot.ImageHash != lateRoot.ImageHash {
		t.Fatal("committed floor revoked concurrent reader", err)
	}
	latePlan, enabled, _, err := lateView.RetentionPlan(t.Context(), raftlog.ReadBudget{Rows: 1, Bytes: 4096})
	if err != nil || !enabled || latePlan.ID != ([32]byte{}) {
		t.Fatal("reader borrowed later plan", latePlan, err)
	}
	if _, _, err := h.machine.store.CollectApplicationRetention(t.Context(), sha256.Sum256(proposal.wire), raftlog.ReadBudget{Rows: 16, Bytes: 1 << 20}); !errors.Is(err, raftlog.ErrLimit) {
		t.Fatal("collector ignored concurrent reader", err)
	}
	if err := lateView.Close(); err != nil {
		t.Fatal(err)
	}
	var original graphRetentionResult
	for i, voter := range n.hosts[0] {
		wire, err := voter.machine.store.ApplicationRecord(t.Context(), voter.driver.Applied(), true, 256)
		if err != nil {
			t.Fatal(err)
		}
		result, err := decodeGraphRetentionResult(wire)
		if err != nil || result.status != graphRetentionApplied || result.id != sha256.Sum256(proposal.wire) || result.round != 0 {
			t.Fatal("actual native result", result, err)
		}
		if i == 0 {
			original = result
		} else if result != original {
			t.Fatal("voter result mismatch", result, original)
		}
		var progress raftlog.ApplicationRetentionProgress
		for page := 0; !progress.Complete && page < 256; page++ {
			progress, _, err = voter.machine.store.CollectApplicationRetention(t.Context(), result.id, raftlog.ReadBudget{Rows: 16, Bytes: 1 << 20})
			if err != nil {
				t.Fatal("finite physical collector", err)
			}
		}
		if !progress.Complete {
			t.Fatal("finite cursor incomplete")
		}
		if err := voter.machine.store.ScrubApplication(t.Context()); err != nil {
			t.Fatal("sparse native state", err)
		}
		if err := voter.machine.store.PublishSnapshot(); err != nil {
			t.Fatal(err)
		}
		index, image, err := voter.machine.store.Checkpoint()
		if err != nil {
			t.Fatal(err)
		}
		if err := voter.machine.Restore(index, image); err != nil {
			t.Fatal("semantic anchor/ledger restore", err)
		}
	}
	// Reopen an actual follower directory under the SAME fresh agreement. Local
	// immutable-original recovery needs no old issuer handle or new proposal.
	prior := n.hosts[0][2]
	if err := prior.Close(); err != nil {
		t.Fatal(err)
	}
	cfg := n.configs[0][2]
	cfg.Create = false
	reopened, err := raftlog.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	machine, err := newDeclaredMaterializerWithAgreement(reopened, prior.machine.ns, n.d, defaultMaterializerLimits(), collectedGraphSemanticContractID())
	if err != nil {
		t.Fatal(err)
	}
	replacement, err := openAllocationHost(machine, 3, replica.Config{Store: reopened, ApplicationSnapshotSends: replica.ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 4 << 20}})
	if err != nil {
		t.Fatal(err)
	}
	n.hosts[0][2] = replacement
	t.Cleanup(func() {
		if err := replacement.Close(); err != nil {
			t.Error(err)
		}
	})
	if got, found, err := replacement.GraphRetentionOriginal(t.Context(), proposal); err != nil || !found || got != original {
		t.Fatal("reopened immutable original", got, found, err)
	}
	if got, found, err := h.GraphRetentionOriginal(t.Context(), proposal); err != nil || !found || got != original {
		t.Fatal("immutable original after collection", got, found, err)
	}
	if source, err := h.AtGraphCertificate(t.Context(), old); source != nil || !errors.Is(err, errGraphHistoryExpired) || !errors.Is(err, raftlog.ErrApplicationExpired) {
		t.Fatal("authenticated old cut was not expired", source, err)
	}
	fake := old
	fake.sourceIndex = 1
	if source, err := h.AtGraphCertificate(t.Context(), fake); source != nil || !errors.Is(err, errInvalid) || errors.Is(err, errGraphHistoryExpired) {
		t.Fatal("caller source index fabricated expiry", source, err)
	}
	fresh := certifiedOpen(t, engine, graphReadRequest{mode: graphReadFresh}, nil)
	freshCut, err := fresh.Cut()
	if err != nil {
		t.Fatal(err)
	}
	for _, cert := range freshCut.certificates {
		if cert.scope.partition == h.machine.ns.partition && (cert.sourceIndex != original.index || cert.sourceRound != 0) {
			t.Fatal("Fresh reused collected original PLM location", cert)
		}
	}
	if err := fresh.Close(); err != nil {
		t.Fatal(err)
	}
	current, err = h.machine.store.ApplicationView(h.driver.Applied())
	if err != nil {
		t.Fatal(err)
	}
	retained, found, err := current.Get(t.Context(), partitionRoundMapKey(h.machine.ns, 0), 256)
	if err != nil || !found || !bytes.Equal(retained.Value, plm.Value) {
		t.Fatal("GC rewrote immutable PLM", err)
	}
	if err := current.Close(); err != nil {
		t.Fatal(err)
	}
	event, err = h.Submit(proposal)
	if err != nil {
		t.Fatal("original replay", err)
	}
	n.pump(0, event)
	if got, found, err := h.GraphRetentionOriginal(t.Context(), proposal); err != nil || !found || got != original {
		t.Fatal("duplicate original changed", got, found, err)
	}
	// Genuine later business installs have their own immutable PLM. They must
	// stop selecting the earlier GC anchor even though its control remains live.
	crossEdgeFixture(t, n)
	later := certifiedOpen(t, engine, graphReadRequest{mode: graphReadFresh}, nil)
	laterCut, err := later.Cut()
	if err != nil {
		t.Fatal(err)
	}
	for _, cert := range laterCut.certificates {
		if cert.scope.partition != h.machine.ns.partition {
			continue
		}
		v, err := h.machine.store.ApplicationView(h.driver.Applied())
		if err != nil {
			t.Fatal(err)
		}
		mapRow, found, err := v.Get(t.Context(), partitionRoundMapKey(h.machine.ns, cert.sourceRound), 256)
		if closeErr := v.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
		if err != nil || !found {
			t.Fatal("later actual PLM", err)
		}
		body, err := graphBody(mapRow.Value, "PLM\x01", 256)
		if err != nil || len(body) != 220 || cert.sourceRound == 0 || cert.sourceIndex <= original.index || cert.sourceIndex != binary.BigEndian.Uint64(body[40:48]) || !bytes.Equal(cert.image[:], body[48:]) {
			t.Fatal("later business source replaced by old GC anchor", cert, err)
		}
	}
	if err := later.Close(); err != nil {
		t.Fatal(err)
	}
	altered := command
	altered.floors.data--
	changed, err := encodeGraphRetentionCommand(altered)
	if err != nil {
		t.Fatal(err)
	}
	before = h.driver.Applied()
	if _, err := h.Submit(allocationProposal{target: proposal.target, wire: changed}); !errors.Is(err, errInvalid) || h.driver.Applied() != before {
		t.Fatal("changed original meaning was admitted", err)
	}
	legacy := *h.machine
	legacy.agreement, legacy.binding.SemanticContractID = retainedGraphSemanticContractID(), retainedGraphSemanticContractID()
	oldHost := &allocationHost{machine: &legacy}
	if _, err := oldHost.Submit(allocationProposal{target: legacy.scope(), wire: proposal.wire}); !errors.Is(err, errInvalid) {
		t.Fatal("old retained agreement accepted GC", err)
	}
}
