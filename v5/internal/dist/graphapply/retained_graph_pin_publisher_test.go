package graphapply

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/dist/replica"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// This is a finite authored native transport guard, not process/GC acceptance.
type retainedPublisherWire struct {
	*nativeGraphTestTransport
	beforePublish     func(graphPinCommand)
	dropPublishReply  bool
	publications      int
	readsAfterInstall int
}

func (w *retainedPublisherWire) RoundTrip(ctx context.Context, wire []byte) ([]byte, error) {
	request, err := decodeGraphSourceRequest(wire)
	if err != nil {
		return nil, err
	}
	publish := false
	if request.kind == sourceSubmit {
		proposal, err := decodeGraphSourceProposal(request.command)
		if err != nil {
			return nil, err
		}
		if isGraphPinWire(proposal.wire) {
			command, err := decodeGraphPinCommand(proposal.wire)
			if err != nil {
				return nil, err
			}
			publish = command.phase == pinPublish
			if publish {
				if w.beforePublish != nil {
					fn := w.beforePublish
					w.beforePublish = nil
					fn(command)
				}
				w.publications++
			}
		}
	}
	if request.kind == sourceCrossRead && bytes.HasPrefix(request.command, []byte{'G', 'P', 'Q', 1}) {
		question, err := decodeRetainedPinQuestion(request.command)
		if err != nil {
			return nil, err
		}
		if len(question.manifest) > 0 && w.publications > 0 {
			w.readsAfterInstall++
		}
	}
	response, err := w.nativeGraphTestTransport.RoundTrip(ctx, wire)
	if err == nil && publish && w.dropPublishReply {
		w.dropPublishReply = false
		return nil, graphstore.ErrSourceUnavailable
	}
	return response, err
}
func retainedPublisherFixture(t *testing.T) (*allocationTestNetwork, *retainedPinPublisher, []graphstate.Operation, []*retainedPublisherWire) {
	t.Helper()
	return retainedPublisherFromNetwork(t, retainedPinNetwork(t))
}
func retainedPublisherFromNetwork(t *testing.T, n *allocationTestNetwork) (*allocationTestNetwork, *retainedPinPublisher, []graphstate.Operation, []*retainedPublisherWire) {
	t.Helper()
	ops, _ := crossEdgeFixture(t, n)
	transports := make(map[uint64]graphPeerWireTransport)
	wires := make([]*retainedPublisherWire, 2)
	for group, partition := range []uint64{3, 8} {
		h := n.hosts[group][0]
		server, err := newGraphSourceServer(t.Context(), h, defaultGraphSourceLimits(h.machine.limits))
		if err != nil {
			t.Fatal(err)
		}
		service, err := newGraphPeerService(h, server, &nativeGraphTestPump{n: n, group: group, host: h})
		if err != nil {
			t.Fatal(err)
		}
		wires[group] = &retainedPublisherWire{nativeGraphTestTransport: &nativeGraphTestTransport{service: service}}
		transports[partition] = wires[group]
	}
	coordinator, err := newCrossProcessCoordinator(n.hosts[0][0], transports)
	if err != nil {
		t.Fatal(err)
	}
	publisher, err := newRetainedPinPublisher(coordinator)
	if err != nil {
		t.Fatal(err)
	}
	return n, publisher, ops, wires
}

func TestRetainedPinPortablePublisherNeedsPostInstallLiveOwners(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	t.Run("actual originals and held source ownership", func(t *testing.T) {
		n, p, ops, wires := retainedPublisherFixture(t)
		request, err := p.Begin(t.Context(), n.requestID(), graphReadRequest{mode: graphReadFresh}, 3)
		if err != nil {
			t.Fatal(err)
		}
		original, err := request.Original()
		if err != nil {
			t.Fatal(err)
		}
		wires[1].dropPublishReply = true
		token, err := request.Publish(t.Context())
		if !errors.Is(err, errRetainedPinPending) || len(token.manifest) != 0 {
			t.Fatal("uncertain publication became portable token", token, err)
		}
		resumed, err := p.Resume(t.Context(), original)
		if err != nil {
			t.Fatal(err)
		}
		token, err = resumed.Publish(t.Context())
		if err != nil || len(token.publications) != 2 {
			t.Fatal("actual retained-original recovery", token, err)
		}
		if wires[0].readsAfterInstall == 0 || wires[1].readsAfterInstall == 0 {
			t.Fatal("token omitted final post-install observation pass")
		}
		encoded, err := encodeRetainedPinToken(token)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := decodeRetainedPinToken(encoded)
		if err != nil || !bytes.Equal(decoded.manifest, original) {
			t.Fatal("portable original codec", err)
		}
		held, err := p.Open(t.Context(), decoded)
		if err != nil {
			t.Fatal(err)
		}
		defer held.Close()
		mutation, err := n.hosts[0][0].PreparePartitionGraphOperations(n.requestID(), []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Name: "after-portable-pin", Scope: ops[0].Scope}}, 31, nil)
		if err != nil {
			t.Fatal(err)
		}
		n.submitPartitionGraph(0, mutation, reasonNone)
		current, err := p.coordinator.OpenCertified(t.Context(), graphReadRequest{mode: graphReadFresh}, nil)
		if err != nil {
			t.Fatal(err)
		}
		assertCertifiedProjection(t, certifiedView(t, held), 1, true, nil)
		assertCertifiedProjection(t, certifiedView(t, current), 1, true, []string{"after-portable-pin"})
		if err := current.Close(); err != nil {
			t.Fatal(err)
		}
		if err := resumed.Release(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := resumed.Release(t.Context()); err != nil {
			t.Fatal(err)
		}
		// Durable release refuses NEW admission while the already owned historical
		// view continues answering exactly until its own explicit Close.
		if next, err := p.Open(t.Context(), decoded); !errors.Is(err, errGraphPinReleased) || next != nil {
			t.Fatal("new open after release", next, err)
		}
		assertCertifiedProjection(t, certifiedView(t, held), 1, true, nil)
		if err := held.Close(); err != nil {
			t.Fatal(err)
		}
		// Policy advancement is real replicated input, independent of valid time.
		expiring, err := p.Begin(t.Context(), n.requestID(), graphReadRequest{mode: graphReadFresh}, 1)
		if err != nil {
			t.Fatal(err)
		}
		expiredToken, err := expiring.Publish(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if err := expiring.AdvanceFeed(t.Context(), []uint64{n.hosts[0][0].driver.Applied() + 1, n.hosts[1][0].driver.Applied() + 1}, [16]byte(n.requestID())); err != nil {
			t.Fatal(err)
		}
		if err := p.AdvanceEpoch(t.Context(), p.coordinator.host.session, [16]byte(n.requestID()), 0); err != nil {
			t.Fatal(err)
		}
		if next, err := p.Open(t.Context(), expiredToken); !errors.Is(err, errGraphPinExpired) || next != nil {
			t.Fatal("known epoch expiry aliased unavailable or empty", next, err)
		}
		if err := expiring.Expire(t.Context()); err != nil {
			t.Fatal(err)
		}
		third, err := p.Begin(t.Context(), n.requestID(), graphReadRequest{mode: graphReadFresh}, 3)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := third.Publish(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := third.Close(t.Context()); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("A closes before B install", func(t *testing.T) {
		n, p, _, wires := retainedPublisherFixture(t)
		request, err := p.Begin(t.Context(), n.requestID(), graphReadRequest{mode: graphReadFresh}, 2)
		if err != nil {
			t.Fatal(err)
		}
		wires[1].beforePublish = func(command graphPinCommand) {
			manifest, err := decodeGraphPinManifest(command.manifest)
			if err != nil {
				t.Fatal(err)
			}
			closeCommand := pinCommandFor(manifest, command.manifest, n.hosts[0][0].machine.scope(), pinClose, pinTerminalAction(manifest, command.manifest, pinClose), 0)
			if actual := retainedPinSubmit(t, n, closeCommand); actual.status != pinApplied {
				t.Fatal(actual)
			}
		}
		token, err := request.Publish(t.Context())
		if !errors.Is(err, errGraphPinReleased) || len(token.manifest) != 0 {
			t.Fatal("early proof combined owners never simultaneously live", token, err)
		}
		if err := request.Close(t.Context()); err != nil {
			t.Fatal("pending remaining owner cleanup", err)
		}
	})
	t.Run("actual ordinary quota cliff keeps exact originals recoverable", func(t *testing.T) {
		n := retainedPinNetworkWithPolicy(t, func(p *raftlog.ApplicationPolicy) { p.RetainedApplicationRecords = 520 })
		_, publisher, _, _ := retainedPublisherFromNetwork(t, n)
		request, err := publisher.Begin(t.Context(), n.requestID(), graphReadRequest{mode: graphReadFresh}, ^uint64(0)-1)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := request.Publish(t.Context()); err != nil {
			t.Fatal(err)
		}
		manifest, err := decodeGraphPinManifest(request.manifest)
		if err != nil {
			t.Fatal(err)
		}
		h := n.hosts[0][0]
		pump := &nativeGraphTestPump{n: n, group: 0, host: h}
		var epoch uint64
		fill := func() {
			t.Helper()
			for range 128 {
				command := graphPinCommand{scope: h.machine.scope(), issuer: h.session, action: [16]byte(n.requestID()), phase: pinAdvanceEpoch, expectedEpoch: epoch, epoch: epoch + 1}
				proposal, err := h.retainedPinProposal(command)
				if err != nil {
					t.Fatal(err)
				}
				body, err := pump.Submit(t.Context(), proposal)
				if errors.Is(err, raftlog.ErrLimit) {
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				actual, err := decodeGraphPinResult(body)
				if err != nil || actual.status != pinApplied || actual.epoch != epoch+1 {
					t.Fatal(actual, err)
				}
				epoch = actual.epoch
			}
			t.Fatal("small real record quota did not saturate in finite bound")
		}
		checkOriginal := func(command graphPinCommand) {
			t.Helper()
			proposal, err := h.retainedPinProposal(command)
			if err != nil {
				t.Fatal(err)
			}
			before := h.driver.Applied()
			if _, err := h.Submit(proposal); !errors.Is(err, raftlog.ErrLimit) {
				t.Fatal("ordinary append was not saturated", err)
			}
			invocation, err := publisher.invocation()
			if err != nil {
				t.Fatal(err)
			}
			actual, err := invocation.endpoints[h.machine.ns.partition].submitRetainedPin(t.Context(), command, invocation.budget)
			if err != nil || actual.status != pinApplied || actual.publication == 0 || h.driver.Applied() != before {
				t.Fatal("immutable replay required ordinary credit", actual, err, before, h.driver.Applied())
			}
		}
		fill()
		checkOriginal(pinCommandFor(manifest, request.manifest, h.machine.scope(), pinPublish, manifest.request, 0))
		// All-participant recovery still authenticates current live owners AFTER
		// installation/replay; the local GPX result alone is not a portable token.
		if _, err := request.Publish(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := request.Close(t.Context()); err != nil {
			t.Fatal("funded terminal at quota cliff", err)
		}
		fill() // releasing unused alternatives may restore a little ordinary credit
		checkOriginal(pinCommandFor(manifest, request.manifest, h.machine.scope(), pinClose, pinTerminalAction(manifest, request.manifest, pinClose), 0))
	})
	t.Run("inert and nil boundary guards", func(t *testing.T) {
		if _, err := newRetainedPinPublisher(nil); !errors.Is(err, errInvalid) {
			t.Fatal(err)
		}
		var p *retainedPinPublisher
		var r *retainedPinRequest
		if _, err := p.Begin(t.Context(), requestID{1}, graphReadRequest{}, 1); !errors.Is(err, errInvalid) {
			t.Fatal(err)
		}
		if _, err := p.Resume(t.Context(), nil); !errors.Is(err, errInvalid) {
			t.Fatal(err)
		}
		if _, err := p.Open(t.Context(), retainedPinToken{}); !errors.Is(err, errInvalid) {
			t.Fatal(err)
		}
		if err := p.AdvanceEpoch(t.Context(), [16]byte{1}, [16]byte{2}, 0); !errors.Is(err, errInvalid) {
			t.Fatal(err)
		}
		if _, err := r.Original(); !errors.Is(err, errInvalid) {
			t.Fatal(err)
		}
		if _, err := r.Publish(t.Context()); !errors.Is(err, errInvalid) {
			t.Fatal(err)
		}
		for _, err := range []error{r.Close(t.Context()), r.Release(t.Context()), r.Expire(t.Context()), r.AdvanceFeed(t.Context(), nil, [16]byte{1})} {
			if !errors.Is(err, errInvalid) {
				t.Fatal(err)
			}
		}
		scope := graphCutWireFixture(1).scope
		scope.semantic = retainedGraphSemanticContractID()
		base := graphWireResponse{peer: graphPeerIdentity{scope: scope, voter: 1}, issuer: [16]byte{1}, kind: sourceCertifiedOpen, sequence: 1, nonce: [16]byte{2}}
		ordinary, err := encodeGraphWireResponse(base)
		if err != nil || !bytes.Equal(ordinary[:4], []byte{'G', 'S', 'R', 1}) {
			t.Fatal("ordinary fresh response changed", err)
		}
		for _, cause := range []error{errGraphPinExpired, errGraphPinReleased} {
			base.err = cause
			wire, err := encodeGraphWireResponse(base)
			if err != nil || !bytes.Equal(wire[:4], []byte{'G', 'S', 'R', 2}) {
				t.Fatal(err)
			}
			decoded, err := decodeGraphWireResponse(wire)
			if err != nil || !errors.Is(decoded.err, cause) || errors.Is(decoded.err, replica.ErrUnavailable) {
				t.Fatal("fresh explicit status", decoded.err, err)
			}
		}
		if _, err := decodeRetainedPinToken(nil); !errors.Is(err, errCorrupt) {
			t.Fatal(err)
		}
	})
}
