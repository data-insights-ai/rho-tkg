package graphapply

import (
	"context"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/dist/replica"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"go.etcd.io/raft/v3"
)

// The controlled scheduling point is after a REAL surviving-quorum read and
// before the purpose-specific service exports it. No proof coordinates or
// metadata are rewritten: a native committed fence and ordinary publication
// advance the retained log Base, while application history remains intact.
type replacementPublicationPump struct {
	*replacementNativePump
	association    *replacementAssociationTest
	published      bool
	proof          allocationProof
	publishedIndex uint64
	exportErr      error
}

func (p *replacementPublicationPump) driveSurvivors(ctx context.Context, event allocationEvent) error {
	queue := event.packets
	if len(event.snapshotSends) != 0 {
		return errInvalid
	}
	for count := 0; len(queue) > 0; count++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if count >= 4096 {
			return errLimit
		}
		packet := queue[0]
		queue = queue[1:]
		if packet.Snapshot || packet.To < 1 || packet.To > 3 {
			return errInvalid
		}
		if p.drop&(1<<uint(packet.To-1)) != 0 {
			continue
		}
		next, err := p.n.hosts[0][packet.To-1].Step(packet)
		if err != nil {
			return err
		}
		if len(next.snapshotSends) != 0 {
			return errInvalid
		}
		if len(queue)+len(next.packets) > 128 {
			return errLimit
		}
		queue = append(queue, next.packets...)
	}
	return nil
}

func (p *replacementPublicationPump) Allocation(ctx context.Context, q allocationQuery) (allocationReply, error) {
	reply, err := p.replacementNativePump.Allocation(ctx, q)
	if err != nil || reply.err != nil || p.published {
		return reply, err
	}
	p.proof = reply.proof
	proposal, err := p.host.PrepareGraphFence(1)
	if err != nil {
		return allocationReply{}, err
	}
	event, err := p.host.Submit(proposal)
	if err != nil {
		return allocationReply{}, err
	}
	if err := p.driveSurvivors(ctx, event); err != nil {
		return allocationReply{}, err
	}
	p.publishedIndex = p.host.driver.Applied()
	wire, err := p.host.machine.store.ApplicationRecord(ctx, p.publishedIndex, true, 256)
	if err != nil {
		return allocationReply{}, err
	}
	outcome, err := decodeGraphCutResult(wire)
	if err != nil {
		return allocationReply{}, err
	}
	if outcome.index != p.publishedIndex || outcome.phase != graphCutFence || outcome.status != graphCutApplied || p.publishedIndex <= reply.proof.observation.source.index || p.n.hosts[0][1].driver.Applied() != p.publishedIndex {
		return allocationReply{}, errInvalid
	}
	if err := p.host.machine.store.PublishSnapshot(); err != nil {
		return allocationReply{}, err
	}
	p.published = true
	// Exercise the exact local joined causes before the wire codec deliberately
	// reduces them to its existing SourceUnavailable classification. This is
	// the same unchanged native proof the real service exports after return.
	target := graphReplacementTarget{voter: p.association.Peer().voter, incarnation: p.association.Incarnation()}
	body, exportErr := p.host.exportReplacementRead(reply.proof, target)
	p.exportErr = exportErr
	if body != (graphReplacementReadBody{}) {
		return allocationReply{}, errInvalid
	}
	return reply, nil
}

type recordedReplacementPublicationWire struct {
	graphPeerWireTransport
	response graphWireResponse
}

func (w *recordedReplacementPublicationWire) RoundTrip(ctx context.Context, wire []byte) ([]byte, error) {
	response, err := w.graphPeerWireTransport.RoundTrip(ctx, wire)
	if err != nil {
		return nil, err
	}
	w.response, err = decodeGraphWireResponse(response)
	return response, err
}

func TestGraphReplacementAdmissionPublicationPastNativeReadRefusesAndFreshReadRetries(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n, _ := replacementNativeNetwork(t, certifiedGraphSemanticContractID())
	endpoint, transport, nativePump := replacementEndpointFixture(t, n, 0)
	association := &replacementAssociationTest{}
	pump := &replacementPublicationPump{replacementNativePump: nativePump, association: association}
	service, err := newGraphReplacementService(nativePump.host, pump)
	if err != nil {
		t.Fatal(err)
	}
	transport.service = service
	recorded := &recordedReplacementPublicationWire{graphPeerWireTransport: transport}
	endpoint.transport = recorded
	admission, err := beginGraphReplacement(t.Context(), endpoint, 3, replacementRetirer(t, n, association))
	if admission != nil || !errors.Is(err, graphstore.ErrSourceUnavailable) {
		t.Fatal("compacted read produced pending admission", admission, err)
	}
	if !errors.Is(pump.exportErr, graphstore.ErrSourceUnavailable) || !errors.Is(pump.exportErr, raft.ErrCompacted) {
		t.Fatal("native export lost compacted/source-unavailable causes", pump.exportErr)
	}
	cut, err := nativePump.host.machine.store.PublishedApplicationCut()
	if err != nil || cut.Index != pump.publishedIndex || cut.Index <= pump.proof.observation.source.index {
		t.Fatal("publication did not advance actual Base past native read", cut.Index, pump.publishedIndex, err)
	}
	if association.closes != 1 || !errors.Is(association.Context().Err(), context.Canceled) {
		t.Fatal("failed read retained unique-live association", association.closes, association.Context().Err())
	}
	if recorded.response.kind != sourceReplacementRead || !errors.Is(recorded.response.err, graphstore.ErrSourceUnavailable) || len(recorded.response.body) != 0 {
		t.Fatal("failed service exported a successful replacement reply", recorded.response)
	}
	if len(nativePump.host.pending) != 0 {
		t.Fatal("failed export leaked native read metadata")
	}
	if _, err := n.hosts[0][2].Tick(); !errors.Is(err, replica.ErrStopped) {
		t.Fatal("retired voter resumed after failed replacement", err)
	}
	// A emits no receiving Host, import/activation or replacement snapshot ACK.
	// The actual publication must not poison the source: a newly acquired
	// association and fresh quorum read can use the retained Base term normally.
	freshAssociation := &replacementAssociationTest{}
	retire := replacementRetirer(t, n, freshAssociation)
	freshAcquire := replacementAssociatorFunc(func(ctx context.Context, peer graphPeerIdentity) (graphReplacementAssociation, error) {
		owner, err := retire(ctx, peer)
		if err == nil {
			freshAssociation.incarnation = [16]byte{92}
		}
		return owner, err
	})
	fresh, err := beginGraphReplacement(t.Context(), endpoint, 3, freshAcquire)
	if err != nil || fresh == nil {
		t.Fatal("fresh read could not recover after normal compaction", err)
	}
	defer fresh.close()
	if fresh.body.index != cut.Index || fresh.body.term != cut.Term || fresh.body.target.incarnation != freshAssociation.Incarnation() || fresh.body.nonce == pump.proof.question.nonce {
		t.Fatal("retry reused compacted evidence", fresh.body, cut)
	}
	if err := fresh.check(); err != nil {
		t.Fatal(err)
	}
	if err := fresh.close(); err != nil || freshAssociation.closes != 1 {
		t.Fatal("fresh pending owner release", err, freshAssociation.closes)
	}
}
