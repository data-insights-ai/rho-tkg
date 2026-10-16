package graphapply

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

type replacementAssociationTest struct {
	peer        graphPeerIdentity
	incarnation [16]byte
	ctx         context.Context
	cancel      context.CancelFunc
	closes      int
	closeErr    error
}

func (a *replacementAssociationTest) Peer() graphPeerIdentity  { return a.peer }
func (a *replacementAssociationTest) Incarnation() [16]byte    { return a.incarnation }
func (a *replacementAssociationTest) Context() context.Context { return a.ctx }
func (a *replacementAssociationTest) Close() error {
	a.closes++
	if a.cancel != nil {
		a.cancel()
	}
	return a.closeErr
}

type replacementAssociatorFunc func(context.Context, graphPeerIdentity) (graphReplacementAssociation, error)

func (f replacementAssociatorFunc) RetireAndAcquire(ctx context.Context, p graphPeerIdentity) (graphReplacementAssociation, error) {
	return f(ctx, p)
}

// Uses actual factory-created graph stores, committed Init and native Raft.
// Retired C is closed before the replacement read, and its traffic is dropped;
// only the surviving A/B voters can complete the read. These are local unit
// controls, not credential-service or distributed-process acceptance evidence.
type replacementNativePump struct {
	*nativeGraphTestPump
	drop uint8
}

func (p *replacementNativePump) Allocation(ctx context.Context, q allocationQuery) (allocationReply, error) {
	e, err := p.host.Read(q)
	if err != nil {
		return allocationReply{}, err
	}
	id := e.readID
	replies, queue := e.replies, e.packets
	for count := 0; len(queue) > 0; count++ {
		if err := ctx.Err(); err != nil {
			return allocationReply{}, err
		}
		if count >= 4096 {
			return allocationReply{}, errLimit
		}
		packet := queue[0]
		queue = queue[1:]
		if packet.Snapshot || packet.To < 1 || packet.To > 3 {
			return allocationReply{}, errInvalid
		}
		if p.drop&(1<<uint(packet.To-1)) != 0 {
			continue
		}
		next, err := p.n.hosts[0][packet.To-1].Step(packet)
		if err != nil {
			return allocationReply{}, err
		}
		if len(queue)+len(next.packets) > 128 {
			return allocationReply{}, errLimit
		}
		queue = append(queue, next.packets...)
		replies = append(replies, next.replies...)
	}
	for _, r := range replies {
		if r.id == id && r.question == q {
			return r, nil
		}
	}
	if err := p.host.cancelSourceRead(id); err != nil {
		return allocationReply{}, err
	}
	return allocationReply{}, graphstore.ErrSourceUnavailable
}
func replacementNativeNetwork(t *testing.T, agreement raftlog.ApplicationSemanticContractID) (*allocationTestNetwork, provisionedLogicalGraph) {
	t.Helper()
	spec := lifecycleSpec()
	spec.agreement = agreement
	spec.partitions = spec.partitions[:1]
	n := &allocationTestNetwork{t: t, pending: make(map[[16]byte]allocationProof)}
	base := t.TempDir()
	provision := lifecycleProvisionFunc(func(ctx context.Context, peer initialGraphPeer, wire []byte) error {
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
		h, err := provisionInitialGraphReplica(ctx, cfg, provisionedLogicalGraph{wire, spec.voters, agreement}, peer, spec.limits)
		if err != nil {
			return err
		}
		n.hosts[0][peer.voter-1], n.configs[0][peer.voter-1] = h, cfg
		t.Cleanup(func() {
			if err := h.Close(); err != nil {
				t.Error(err)
			}
		})
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
	e, err := n.hosts[0][0].Campaign()
	if err != nil {
		t.Fatal(err)
	}
	n.pump(0, e)
	proposal, err := n.hosts[0][0].PreparePartitionInitialization(3, bootstrapAttemptID{1}, r.schemas, r.maxBlock, r.defaultAxis, *r.routing, allocationProof{})
	if err != nil {
		t.Fatal(err)
	}
	n.submit(0, proposal)
	return n, g
}
func replacementEndpointFixture(t *testing.T, n *allocationTestNetwork, source int) (*graphAuthenticatedEndpoint, *nativeGraphTestTransport, *replacementNativePump) {
	t.Helper()
	pump := &replacementNativePump{nativeGraphTestPump: &nativeGraphTestPump{n: n, group: 0, host: n.hosts[0][source]}, drop: 1 << 2}
	service, err := newGraphReplacementService(pump.host, pump)
	if err != nil {
		t.Fatal(err)
	}
	wire := &nativeGraphTestTransport{service: service}
	output, err := graphstate.NewOutputBudget(32 << 20)
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := newGraphReplacementEndpoint(n.d, wire, newGraphRemoteBudget(pump.host.machine.limits, output))
	if err != nil {
		t.Fatal(err)
	}
	return endpoint, wire, pump
}
func replacementRetirer(t *testing.T, n *allocationTestNetwork, a *replacementAssociationTest) replacementAssociatorFunc {
	t.Helper()
	return func(ctx context.Context, p graphPeerIdentity) (graphReplacementAssociation, error) {
		if p.voter != 3 {
			t.Fatal("unexpected retirement target", p)
		}
		if err := n.hosts[0][2].Close(); err != nil {
			return nil, err
		}
		a.peer = p
		a.incarnation = [16]byte{91}
		a.ctx, a.cancel = context.WithCancel(ctx)
		return a, nil
	}
}
func TestGraphReplacementAdmissionNativeSurvivingQuorumAllAgreements(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for _, agreement := range []raftlog.ApplicationSemanticContractID{partitionWriteSemanticContractID(), crossPartitionSemanticContractID(), certifiedGraphSemanticContractID(), rangeMoveSemanticContractID()} {
		t.Run(fmt.Sprintf("%x", agreement[:4]), func(t *testing.T) {
			n, _ := replacementNativeNetwork(t, agreement)
			endpoint, wire, _ := replacementEndpointFixture(t, n, 0)
			association := &replacementAssociationTest{}
			a, err := beginGraphReplacement(t.Context(), endpoint, 3, replacementRetirer(t, n, association))
			if err != nil || a == nil {
				t.Fatal("genuine surviving-quorum admission", err)
			}
			defer a.close()
			wantIndex := n.hosts[0][0].driver.Applied()
			wantTerm, err := n.hosts[0][0].machine.store.Term(wantIndex)
			if err != nil || a.body.index != wantIndex || a.body.term != wantTerm || a.body.voters != ([3]uint64{1, 2, 3}) || a.body.scope != wire.Peer().scope || a.body.source != 1 || a.body.target.voter != 3 || a.body.target.incarnation != association.incarnation || a.body.id.session != wire.service.host.session || a.body.configuration == ([32]byte{}) {
				t.Fatal("native read/membership/term binding", a.body, err)
			}
			if err := a.check(); err != nil {
				t.Fatal(err)
			}
			// Re-encode and decode the exact genuine body: it remains inert data and
			// does not reconstruct the owned admission/retirement association.
			encoded, err := encodeGraphReplacementRead(a.body)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := decodeGraphReplacementRead(encoded)
			if err != nil || decoded != a.body {
				t.Fatal(decoded, err)
			}
			copied := *a
			if !errors.Is(copied.check(), errInvalid) || !errors.Is(copied.close(), errInvalid) {
				t.Fatal("copied owner became admission")
			}
			old := a.body
			a.body.target.voter = 2
			if !errors.Is(a.check(), errInvalid) {
				t.Fatal("C grant consumed by B")
			}
			a.body = old
			if _, err := endpoint.call(t.Context(), graphSourceRequest{kind: sourceOpen}, 8192); !errors.Is(err, errInvalid) {
				t.Fatal("replacement-only service admitted cross source", err)
			}
			if err := a.close(); err != nil {
				t.Fatal(err)
			}
			if err := a.close(); err != nil || association.closes != 1 {
				t.Fatal("idempotent owned retirement release", association.closes, err)
			}
			if !errors.Is(a.check(), graphstore.ErrSourceUnavailable) || !errors.Is(a.check(), context.Canceled) {
				t.Fatal("closed admission remained live", a.check())
			}
		})
	}
}
func TestGraphReplacementAdmissionQuorumLossFollowerAndUnknownVoterRefuse(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for _, kind := range []string{"quorum-loss", "follower", "unknown-voter"} {
		t.Run(kind, func(t *testing.T) {
			n, _ := replacementNativeNetwork(t, partitionWriteSemanticContractID())
			source := 0
			if kind == "follower" {
				source = 1
			}
			endpoint, _, pump := replacementEndpointFixture(t, n, source)
			if kind == "quorum-loss" {
				pump.drop |= 1 << 1
			}
			target := uint64(3)
			if kind == "unknown-voter" {
				target = 4
			}
			association := &replacementAssociationTest{}
			acquire := replacementRetirer(t, n, association)
			if target == 4 {
				acquire = func(ctx context.Context, p graphPeerIdentity) (graphReplacementAssociation, error) {
					association.peer = p
					association.incarnation = [16]byte{1}
					association.ctx, association.cancel = context.WithCancel(ctx)
					return association, nil
				}
			}
			a, err := beginGraphReplacement(t.Context(), endpoint, target, acquire)
			want := graphstore.ErrSourceUnavailable
			if kind == "follower" {
				want = replica.ErrUnavailable
			}
			if kind == "unknown-voter" {
				want = errInvalid
			}
			if a != nil || !errors.Is(err, want) || association.closes != 1 {
				t.Fatal("refusal must release association without ready state", a, err, association.closes)
			}
		})
	}
}
func TestGraphReplacementAdmissionReopenSourceSessionInvalidates(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n, g := replacementNativeNetwork(t, partitionWriteSemanticContractID())
	endpoint, wire, pump := replacementEndpointFixture(t, n, 0)
	association := &replacementAssociationTest{}
	a, err := beginGraphReplacement(t.Context(), endpoint, 3, replacementRetirer(t, n, association))
	if err != nil {
		t.Fatal(err)
	}
	defer a.close()
	previous := n.hosts[0][0]
	if err := previous.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := openLogicalGraphReplica(t.Context(), n.configs[0][0], g, initialGraphPeer{3, 1}, previous.machine.limits)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	})
	n.hosts[0][0] = reopened
	pump.host = reopened
	service, err := newGraphReplacementService(reopened, pump)
	if err != nil {
		t.Fatal(err)
	}
	wire.service = service
	// Reopened source is a follower. Its authenticated refusal still delivers
	// the changed real Host session, revoking the prior endpoint owner.
	command, err := encodeGraphReplacementTarget(a.body.target)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := endpoint.call(t.Context(), graphSourceRequest{kind: sourceReplacementRead, command: command}, 1024); !errors.Is(err, graphstore.ErrSourceUnavailable) {
		t.Fatal("changed source session", err)
	}
	if !errors.Is(a.check(), graphstore.ErrSourceUnavailable) {
		t.Fatal("delivered source change did not invalidate admission")
	}
}

type changedReplacementWire struct {
	graphPeerWireTransport
	change func(*graphWireResponse)
}

func (w changedReplacementWire) RoundTrip(ctx context.Context, wire []byte) ([]byte, error) {
	reply, err := w.graphPeerWireTransport.RoundTrip(ctx, wire)
	if err != nil {
		return nil, err
	}
	parsed, err := decodeGraphWireResponse(reply)
	if err != nil {
		return nil, err
	}
	w.change(&parsed)
	return encodeGraphWireResponse(parsed)
}
func TestGraphReplacementAdmissionRejectsUnmatchedActualCompletions(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for _, kind := range []string{"nonce", "target", "incarnation", "scope", "source", "body-session", "corrupt-body"} {
		t.Run(kind, func(t *testing.T) {
			n, _ := replacementNativeNetwork(t, partitionWriteSemanticContractID())
			endpoint, wire, _ := replacementEndpointFixture(t, n, 0)
			endpoint.transport = changedReplacementWire{graphPeerWireTransport: wire, change: func(r *graphWireResponse) {
				if kind == "nonce" {
					r.nonce[0] ^= 1
					return
				}
				if kind == "corrupt-body" {
					r.body = []byte("broken")
					return
				}
				p, err := decodeGraphReplacementRead(r.body)
				if err != nil {
					t.Fatal(err)
				}
				switch kind {
				case "target":
					p.target.voter = 2
				case "incarnation":
					p.target.incarnation[0] ^= 1
				case "scope":
					p.scope.ownership++
				case "source":
					p.source = 2
				case "body-session":
					p.id.session[0] ^= 1
				}
				r.body, err = encodeGraphReplacementRead(p)
				if err != nil {
					t.Fatal(err)
				}
			}}
			association := &replacementAssociationTest{}
			a, err := beginGraphReplacement(t.Context(), endpoint, 3, replacementRetirer(t, n, association))
			want := errInvalid
			if kind == "corrupt-body" {
				want = errCorrupt
			}
			if a != nil || !errors.Is(err, want) || association.closes != 1 {
				t.Fatal("unmatched completion constructed admission", a, err, association.closes)
			}
		})
	}
}
func TestGraphReplacementAdmissionAssociationBindingAndLifetime(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for _, kind := range []string{"wrong-peer", "nil-context", "zero-incarnation", "already-revoked", "nil-association", "typed-nil-association", "acquire-error", "live-revocation"} {
		t.Run(kind, func(t *testing.T) {
			n, _ := replacementNativeNetwork(t, partitionWriteSemanticContractID())
			endpoint, _, _ := replacementEndpointFixture(t, n, 0)
			association := &replacementAssociationTest{}
			realAcquire := replacementRetirer(t, n, association)
			acquire := replacementAssociatorFunc(func(ctx context.Context, p graphPeerIdentity) (graphReplacementAssociation, error) {
				switch kind {
				case "nil-association":
					return nil, nil
				case "typed-nil-association":
					return (*replacementAssociationTest)(nil), nil
				case "acquire-error":
					return nil, graphstore.ErrSourceUnavailable
				}
				a, err := realAcquire(ctx, p)
				if err != nil {
					return nil, err
				}
				switch kind {
				case "wrong-peer":
					association.peer.scope.group[0] ^= 1
				case "nil-context":
					association.cancel()
					association.ctx = nil
				case "zero-incarnation":
					association.incarnation = [16]byte{}
				case "already-revoked":
					association.cancel()
				}
				return a, nil
			})
			a, err := beginGraphReplacement(t.Context(), endpoint, 3, acquire)
			if kind == "live-revocation" {
				if err != nil {
					t.Fatal(err)
				}
				association.cancel()
				if !errors.Is(a.check(), graphstore.ErrSourceUnavailable) {
					t.Fatal("revoked owner remained admissible")
				}
				if err := a.close(); err != nil {
					t.Fatal(err)
				}
				return
			}
			want := errInvalid
			if kind == "already-revoked" || kind == "acquire-error" {
				want = graphstore.ErrSourceUnavailable
			}
			closes := 1
			if kind == "nil-association" || kind == "typed-nil-association" || kind == "acquire-error" {
				closes = 0
			}
			if a != nil || !errors.Is(err, want) || association.closes != closes {
				t.Fatal("association refusal", a, err, association.closes)
			}
		})
	}
}
func TestGraphReplacementAdmissionCodecAndNilBoundaries(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	var admission *graphReplacementAdmission
	if !errors.Is(admission.check(), errInvalid) || !errors.Is(admission.close(), errInvalid) {
		t.Fatal("nil owner")
	}
	var host *allocationHost
	if _, err := host.exportReplacementRead(allocationProof{}, graphReplacementTarget{}); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if _, err := newGraphReplacementService(nil, nil); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if _, err := newGraphReplacementEndpoint(graphstore.OwnershipDeclaration{}, nil, nil); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	var nilAssociator replacementAssociatorFunc
	if a, err := beginGraphReplacement(t.Context(), nil, 3, nilAssociator); a != nil || !errors.Is(err, errInvalid) {
		t.Fatal(a, err)
	}
	//nolint:staticcheck // Nil context is a required refusal boundary.
	if a, err := beginGraphReplacement(nil, nil, 3, nil); a != nil || !errors.Is(err, errInvalid) {
		t.Fatal(a, err)
	}
	target := graphReplacementTarget{voter: 3, incarnation: [16]byte{1}}
	encoded, err := encodeGraphReplacementTarget(target)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeGraphReplacementTarget(encoded)
	if err != nil || decoded != target {
		t.Fatal(decoded, err)
	}
	for i := range len(encoded) {
		if _, err := decodeGraphReplacementTarget(encoded[:i]); !errors.Is(err, errCorrupt) {
			t.Fatal("target truncation", i, err)
		}
	}
	if _, err := decodeGraphReplacementTarget(append(bytes.Clone(encoded), 0)); !errors.Is(err, errLimit) {
		t.Fatal("trailing target", err)
	}
	if _, err := encodeGraphReplacementTarget(graphReplacementTarget{}); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if _, err := encodeGraphReplacementRead(graphReplacementReadBody{}); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	n, _ := replacementNativeNetwork(t, partitionWriteSemanticContractID())
	endpoint, wire, _ := replacementEndpointFixture(t, n, 0)
	association := &replacementAssociationTest{}
	a, err := beginGraphReplacement(t.Context(), endpoint, 3, replacementRetirer(t, n, association))
	if err != nil {
		t.Fatal(err)
	}
	defer a.close()
	encoded, err = encodeGraphReplacementRead(a.body)
	if err != nil {
		t.Fatal(err)
	}
	for i := range len(encoded) {
		if _, err := decodeGraphReplacementRead(encoded[:i]); !errors.Is(err, errCorrupt) {
			t.Fatal("read truncation", i, err)
		}
	}
	if _, err := decodeGraphReplacementRead(append(bytes.Clone(encoded), 0)); !errors.Is(err, errCorrupt) {
		t.Fatal("trailing read", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if got, err := beginGraphReplacement(ctx, endpoint, 3, nilAssociator); got != nil || !errors.Is(err, errInvalid) {
		t.Fatal("typed nil before canceled context", got, err)
	}
	if got, err := beginGraphReplacement(ctx, endpoint, 3, replacementRetirer(t, n, &replacementAssociationTest{})); got != nil || !errors.Is(err, context.Canceled) {
		t.Fatal("current cancellation", got, err)
	}
	if got, err := beginGraphReplacement(t.Context(), endpoint, 1, replacementRetirer(t, n, &replacementAssociationTest{})); got != nil || !errors.Is(err, errInvalid) {
		t.Fatal("source cannot replace itself", got, err)
	}
	wrong := wire.Peer()
	wrong.scope.topology++
	if _, err := newGraphReplacementEndpoint(n.d, &fixedWrongGraphPeer{graphPeerWireTransport: wire, peer: wrong}, endpoint.budget); !errors.Is(err, errInvalid) {
		t.Fatal("stale expected declaration", err)
	}
	if _, err := wire.service.host.exportReplacementRead(allocationProof{}, target); !errors.Is(err, errInvalid) {
		t.Fatal("non-native proof", err)
	}
}

type pausedReplacementWire struct {
	graphPeerWireTransport
	entered chan struct{}
}

func (w pausedReplacementWire) RoundTrip(ctx context.Context, wire []byte) ([]byte, error) {
	if _, err := w.graphPeerWireTransport.RoundTrip(ctx, wire); err != nil {
		return nil, err
	}
	close(w.entered) // Pause AFTER the genuine surviving-quorum completion.
	<-ctx.Done()
	return nil, ctx.Err()
}
func TestGraphReplacementAdmissionRevocationCancelsActualPausedCompletion(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n, _ := replacementNativeNetwork(t, partitionWriteSemanticContractID())
	endpoint, wire, _ := replacementEndpointFixture(t, n, 0)
	entered := make(chan struct{})
	endpoint.transport = pausedReplacementWire{graphPeerWireTransport: wire, entered: entered}
	association := &replacementAssociationTest{}
	type result struct {
		admission *graphReplacementAdmission
		err       error
	}
	done := make(chan result, 1)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	go func() {
		a, err := beginGraphReplacement(ctx, endpoint, 3, replacementRetirer(t, n, association))
		done <- result{a, err}
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("genuine replacement reply never paused", ctx.Err())
	}
	association.cancel()
	select {
	case got := <-done:
		if got.admission != nil || !errors.Is(got.err, context.Canceled) || !errors.Is(got.err, graphstore.ErrSourceUnavailable) || association.closes != 1 {
			t.Fatal("revoked in-flight admission", got.err, association.closes)
		}
	case <-ctx.Done():
		t.Fatal("revocation did not cancel transport", ctx.Err())
	}
}
