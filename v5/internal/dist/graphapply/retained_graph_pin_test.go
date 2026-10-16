package graphapply

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/dist/replica"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// This bounded fixture selects a genuinely fresh recovery2/contract4 binding.
// It reuses the existing native six-voter packet pump; it creates no processes.
func retainedPinNetwork(t *testing.T) *allocationTestNetwork {
	t.Helper()
	return retainedPinNetworkWithPolicy(t, nil)
}
func retainedPinNetworkWithPolicy(t *testing.T, adjust func(*raftlog.ApplicationPolicy)) *allocationTestNetwork {
	agreement := retainedGraphSemanticContractID()
	t.Helper()
	r := partitionCodecRequest(t, 1, defaultMaterializerLimits())
	n := &allocationTestNetwork{t: t, d: r.declaration, pending: make(map[[16]byte]allocationProof)}
	for group, partition := range []uint64{3, 8} {
		for member := range 3 {
			id := uint64(member + 1)
			p := raftlog.DefaultApplicationPolicy(id)
			p.MaxImageBytes = 172
			p.MaxInstallWrites = 256
			p.RetainedApplicationRecords = 8192
			p.RetainedApplicationBytes = 16 << 20
			if adjust != nil {
				adjust(&p)
			}
			controls := raftlog.ApplicationControlConfig{Version: 1}
			recovery := raftlog.ApplicationRecoveryConfig{Version: 2}
			contract, err := raftlog.ApplicationContractForPolicyWithRecovery(p, controls, recovery)
			if err != nil {
				t.Fatal(err)
			}
			part, _ := n.d.Partition(partition)
			cfg := raftlog.Config{Dir: filepath.Join(t.TempDir(), fmt.Sprintf("partition%d-member%d", partition, id)), Create: true, Application: p, Controls: controls, Recovery: recovery, Transfer: raftlog.ApplicationTransferConfig{Identity: raftlog.ApplicationIdentity{Graph: [16]byte(n.d.Graph()), Partition: partition, Group: part.Group}, Contract: contract, Limits: raftlog.DefaultApplicationTransferLimits()}, Generations: raftlog.ApplicationGenerationLimits{MaxBytes: max(32<<20, p.RetainedApplicationBytes), MaxRecords: max(16384, p.RetainedApplicationRecords)}, PublishedCuts: raftlog.ApplicationPublishedCutLimits{MaxTransferChunks: 4096}, Replication: raftlog.ApplicationReplicationConfig{Voters: [3]uint64{1, 2, 3}}, SemanticContractID: agreement}
			n.configs[group][member] = cfg
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
			m, err := newRetainedGraphMaterializer(s, namespace{idalloc.GraphID(n.d.Graph()), partition}, n.d, defaultMaterializerLimits())
			if err != nil {
				t.Fatal(err)
			}
			h, err := openAllocationHost(m, id, replica.Config{Store: s, ApplicationSnapshotSends: replica.ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 4 << 20}})
			if err != nil {
				t.Fatal(err)
			}
			n.hosts[group][member] = h
			t.Cleanup(func() {
				if err := h.Close(); err != nil {
					t.Error(err)
				}
			})
		}
		event, err := n.hosts[group][0].Campaign()
		if err != nil {
			t.Fatal(err)
		}
		n.pump(group, event)
	}
	h := n.hosts[0][0]
	p, err := h.PreparePartitionInitialization(3, bootstrapAttemptID{1}, partitionWriteSchemas(), r.maxBlock, graphGenesisBinding(t), *r.routing, allocationProof{})
	if err != nil {
		t.Fatal(err)
	}
	n.submit(0, p)
	proof := n.read(0, allocationQuery{kind: observeConfiguration})
	p, err = h.PreparePartitionInitialization(8, bootstrapAttemptID{2}, partitionWriteSchemas(), r.maxBlock, graphGenesisBinding(t), *r.routing, proof)
	if err != nil {
		t.Fatal(err)
	}
	n.submit(1, p)
	return n
}

func retainedPinSubmit(t *testing.T, n *allocationTestNetwork, c graphPinCommand) graphPinResult {
	t.Helper()
	h := n.hosts[0][0]
	p, err := h.retainedPinProposal(c)
	if err != nil {
		t.Fatal(err)
	}
	event, err := h.Submit(p)
	if err != nil {
		t.Fatal(err)
	}
	n.pump(0, event)
	var result graphPinResult
	for i, voter := range n.hosts[0] {
		wire, err := voter.machine.store.ApplicationRecord(t.Context(), voter.driver.Applied(), true, 256)
		if err != nil {
			t.Fatal(err)
		}
		actual, err := decodeGraphPinResult(wire)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			result = actual
		} else if actual != result {
			t.Fatal("pin outcome differs across actual voters", actual, result)
		}
	}
	return result
}
func retainedPinBank(t *testing.T, n *allocationTestNetwork, expected []raftlog.ApplicationRecoveryReservation) {
	t.Helper()
	for _, voter := range n.hosts[0] {
		v, err := voter.machine.store.ApplicationView(voter.driver.Applied())
		if err != nil {
			t.Fatal(err)
		}
		equal, _, err := v.CheckTerminalBank(t.Context(), expected, v.ReadLimits())
		err = errors.Join(err, v.Close())
		if err != nil || !equal {
			t.Fatal("graph owner and actual native terminal bank differ", equal, err)
		}
		index, image, err := voter.machine.store.Checkpoint()
		if err != nil {
			t.Fatal(err)
		}
		if err := voter.machine.Restore(index, image); err != nil {
			t.Fatal("operative pin ledger restore", err)
		}
	}
}

func TestRetainedGraphPinParticipantLedgerOriginalsAndReplicatedExpiry(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := retainedPinNetwork(t)
	ops, _ := crossEdgeFixture(t, n)
	coordinator, _ := certifiedCoordinator(n)
	old := certifiedOpen(t, coordinator, graphReadRequest{mode: graphReadFresh}, nil)
	cut, err := old.Cut()
	if err != nil {
		t.Fatal(err)
	}
	h := n.hosts[0][0]
	manifest := graphPinManifest{publisher: h.machine.scope(), issuer: h.session, request: [16]byte(n.requestID()), expires: 2}
	for _, cert := range cut.certificates {
		part := graphPinPart{certificate: cert, feedStart: cert.sourceIndex + 1}
		part.lineage, err = pinLineage(part)
		if err != nil {
			t.Fatal(err)
		}
		manifest.parts = append(manifest.parts, part)
	}
	publication := func(value graphPinManifest) graphPinCommand {
		t.Helper()
		wire, err := encodeGraphPinManifest(value)
		if err != nil {
			t.Fatal(err)
		}
		got, err := decodeGraphPinManifest(wire)
		if err != nil || got.id() != value.id() || !bytes.Equal(got.parts[0].certificate.image[:], value.parts[0].certificate.image[:]) {
			t.Fatal("exact original manifest codec", got, err)
		}
		return graphPinCommand{scope: h.machine.scope(), issuer: value.issuer, action: value.request, id: value.id(), digest: sha256.Sum256(wire), phase: pinPublish, epoch: value.epoch, manifest: wire}
	}
	first := publication(manifest)
	published := retainedPinSubmit(t, n, first)
	if published.status != pinApplied || published.publication != published.index {
		t.Fatal("unchanged authentic cut was not pinned", published)
	}
	bank := pinReservation(h.machine.scope(), first.id, first.digest)
	retainedPinBank(t, n, []raftlog.ApplicationRecoveryReservation{bank})
	// A later graph mutation must not rewrite the held cut or its exact pin.
	mutation, err := h.PreparePartitionGraphOperations(n.requestID(), []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Name: "after-pin", Scope: ops[0].Scope}}, 30, nil)
	if err != nil {
		t.Fatal(err)
	}
	n.submitPartitionGraph(0, mutation, reasonNone)
	current := certifiedOpen(t, coordinator, graphReadRequest{mode: graphReadFresh}, nil)
	assertCertifiedProjection(t, certifiedView(t, old), 1, true, nil)
	assertCertifiedProjection(t, certifiedView(t, current), 1, true, []string{"after-pin"})
	advance := first
	advance.phase, advance.action, advance.manifest = pinAdvanceFeed, [16]byte(n.requestID()), nil
	advance.feedStart = h.driver.Applied() + 1
	advanced := retainedPinSubmit(t, n, advance)
	if advanced.status != pinApplied || advanced.feedStart != advance.feedStart {
		t.Fatal("actual feed did not advance", advanced)
	}
	release := first
	release.phase, release.action, release.manifest = pinRelease, [16]byte(n.requestID()), nil
	released := retainedPinSubmit(t, n, release)
	if released.status != pinApplied || released.publication != published.publication {
		t.Fatal("funded release", released)
	}
	retainedPinBank(t, n, nil)
	if got := retainedPinSubmit(t, n, first); got != published {
		t.Fatal("terminated original publication lost its exact outcome", got, published)
	}
	if got := retainedPinSubmit(t, n, release); got != released {
		t.Fatal("terminal replay changed original outcome", got, released)
	}
	retainedPinBank(t, n, nil)
	// Replicated policy epochs alone authorize expiry; graph valid time and local
	// wall-clock/session lifetime do not enter these commands or their decisions.
	manifest.request = [16]byte(n.requestID())
	second := publication(manifest)
	secondPublished := retainedPinSubmit(t, n, second)
	if secondPublished.status != pinApplied {
		t.Fatal(secondPublished)
	}
	expire := second
	expire.phase, expire.action, expire.manifest, expire.epoch = pinExpire, [16]byte(n.requestID()), nil, manifest.expires
	p, err := h.retainedPinProposal(expire)
	if err != nil {
		t.Fatal(err)
	}
	before := h.driver.Applied()
	if _, err := h.Submit(p); !errors.Is(err, replica.ErrUnavailable) || h.driver.Applied() != before {
		t.Fatal("local time or premature epoch authorized expiry", err)
	}
	for epoch := uint64(0); epoch < manifest.expires; epoch++ {
		epochResult := retainedPinSubmit(t, n, graphPinCommand{scope: h.machine.scope(), issuer: h.session, action: [16]byte(n.requestID()), phase: pinAdvanceEpoch, expectedEpoch: epoch, epoch: epoch + 1})
		if epochResult.status != pinApplied || epochResult.epoch != epoch+1 {
			t.Fatal("replicated lease epoch", epochResult)
		}
	}
	expired := retainedPinSubmit(t, n, expire)
	if expired.status != pinApplied || expired.publication != secondPublished.publication {
		t.Fatal("funded genuine expiry", expired)
	}
	retainedPinBank(t, n, nil)
	if got := retainedPinSubmit(t, n, second); got != secondPublished {
		t.Fatal("expiry rewrote original publication", got)
	}
	manifest.epoch, manifest.expires, manifest.request = 2, 4, [16]byte(n.requestID())
	third := publication(manifest)
	if got := retainedPinSubmit(t, n, third); got.status != pinApplied {
		t.Fatal(got)
	}
	closeCommand := third
	closeCommand.phase, closeCommand.action, closeCommand.manifest, closeCommand.epoch = pinClose, [16]byte(n.requestID()), nil, 0
	if got := retainedPinSubmit(t, n, closeCommand); got.status != pinApplied {
		t.Fatal("owner Close did not spend its own reserved alternative", got)
	}
	retainedPinBank(t, n, nil)
	// Compact inert/private guards: these encodings confer no retained authority.
	var nilHost *allocationHost
	if _, err := nilHost.retainedPinProposal(first); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	partial := manifest
	partial.request, partial.parts = [16]byte(n.requestID()), manifest.parts[:1]
	partialProposal, err := h.retainedPinProposal(publication(partial))
	if err != nil {
		t.Fatal(err)
	}
	before = h.driver.Applied()
	if _, err := h.Submit(partialProposal); !errors.Is(err, errInvalid) || h.driver.Applied() != before {
		t.Fatal("partial roster admitted", err)
	}
	missing := manifest
	missing.request, missing.parts = [16]byte(n.requestID()), append([]graphPinPart(nil), manifest.parts...)
	missing.parts[0].certificate.round += 100
	missing.parts[1].certificate.round += 100
	for i := range missing.parts {
		missing.parts[i].lineage, err = pinLineage(missing.parts[i])
		if err != nil {
			t.Fatal(err)
		}
	}
	missingProposal, err := h.retainedPinProposal(publication(missing))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Submit(missingProposal); !errors.Is(err, replica.ErrUnavailable) || h.driver.Applied() != before {
		t.Fatal("missing source authority was committed or guessed", err)
	}
	oldMachine := *h.machine
	oldMachine.agreement, oldMachine.binding.SemanticContractID = certifiedGraphSemanticContractID(), certifiedGraphSemanticContractID()
	oldHost := &allocationHost{machine: &oldMachine}
	inert := allocationProposal{target: oldMachine.scope(), wire: first.manifest}
	inert.wire, err = encodeGraphPinCommand(first)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := oldHost.Submit(inert); !errors.Is(err, errInvalid) {
		t.Fatal("ordinary agreement accepted GPL1", err)
	}
}
