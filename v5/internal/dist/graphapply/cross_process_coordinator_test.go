package graphapply

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/dist/replica"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// This fixture exercises genuine Host quorum/native capture and the production
// authentication boundary. Six OS-child acceptance is separate.
type coordinatorTestWire struct {
	*nativeGraphTestTransport
	opens             int
	prepares          int
	registrations     [][]byte
	dropRegister      bool
	dropDecisionReply bool
	dropGet           bool
	requiredGets      int
}

func (w *coordinatorTestWire) RoundTrip(ctx context.Context, wire []byte) ([]byte, error) {
	r, err := decodeGraphSourceRequest(wire)
	if err != nil {
		return nil, err
	}
	if r.kind == sourceGet && w.dropGet {
		w.requiredGets++
		return nil, io.EOF
	}
	if r.kind == sourceOpen {
		w.opens++
	}
	if r.kind == sourceSubmit {
		p, e := decodeGraphSourceProposal(r.command)
		if e != nil {
			return nil, e
		}
		// Inject only into this wrapper's actual cross-command families. Other
		// checked proposals (including certified cut controls) retain their own
		// native decoder and submission path.
		if !isCrossPartitionWire(p.wire) {
			return w.nativeGraphTestTransport.RoundTrip(ctx, wire)
		}
		command, _, e := decodeCrossCommand(p.wire, w.service.host.machine.limits)
		if e != nil {
			return nil, e
		}
		if command.phase == crossRegister {
			w.registrations = append(w.registrations, bytes.Clone(r.command))
			if w.dropRegister {
				w.dropRegister = false
				return nil, graphstore.ErrSourceUnavailable
			}
		}
		if command.phase == crossPrepare {
			w.prepares++
		}
		if command.phase == crossDecide && w.dropDecisionReply {
			w.dropDecisionReply = false
			_, err := w.nativeGraphTestTransport.RoundTrip(ctx, wire)
			if err != nil {
				return nil, err
			}
			return nil, graphstore.ErrSourceUnavailable
		}
	}
	return w.nativeGraphTestTransport.RoundTrip(ctx, wire)
}
func coordinatorNativeFixture(t *testing.T) (*allocationTestNetwork, *crossProcessCoordinator, []graphstate.Operation, []freshBinding, []*coordinatorTestWire) {
	t.Helper()
	n := newCertifiedGraphNetwork(t)
	ops, claims := crossEdgeFixture(t, n)
	transports := make(map[uint64]graphPeerWireTransport)
	wires := make([]*coordinatorTestWire, 2)
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
		w := &coordinatorTestWire{nativeGraphTestTransport: &nativeGraphTestTransport{service: service}}
		wires[group] = w
		transports[partition] = w
	}
	c, err := newCrossProcessCoordinator(n.hosts[0][0], transports)
	if err != nil {
		t.Fatal(err)
	}
	return n, c, ops, claims, wires
}

func TestCoordinatorFaultWireKeepsCutSubmitsSeparateFromCrossFaults(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n, c, _, _, wires := coordinatorNativeFixture(t)
	for _, wire := range wires {
		wire.dropRegister, wire.dropDecisionReply = true, true
	}
	read, err := c.OpenCertified(t.Context(), graphReadRequest{mode: graphReadFresh}, nil)
	if err != nil {
		t.Fatal("genuine cut commands must reach the native transport", err)
	}
	if err := read.Close(); err != nil {
		t.Fatal(err)
	}
	arena, err := graphstate.NewOutputBudget(defaultMaterializerLimits().outputBytes)
	if err != nil {
		t.Fatal(err)
	}
	endpoints, err := c.endpoints(newGraphRemoteBudget(defaultMaterializerLimits(), arena))
	if err != nil {
		t.Fatal(err)
	}
	for group, partition := range []uint64{3, 8} {
		wire := wires[group]
		before := n.hosts[group][0].driver.Applied()
		for _, version := range []byte{1, 2} {
			bad := allocationProposal{target: wire.Peer().scope, wire: []byte{'G', 'C', 'T', version}}
			if _, err := endpoints[partition].submit(t.Context(), bad); !errors.Is(err, errCorrupt) {
				t.Fatal("corrupt cross command escaped its original decoder", err)
			}
		}
		if !wire.dropRegister || !wire.dropDecisionReply || len(wire.registrations) != 0 || wire.prepares != 0 || n.hosts[group][0].driver.Applied() != before {
			t.Fatal("cut or corrupt cross command consumed a fault or installed native state")
		}
	}
}

func TestCrossProcessCoordinatorSuccessiveRunReadAndExactResume(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n, c, ops, claims, wires := coordinatorNativeFixture(t)
	id := n.requestID()
	first, err := c.Run(t.Context(), id, ops, 37, claims)
	if err != nil || !first.decision.commit || first.decision.phase != crossDecide {
		t.Fatal("genuine remote native decision", first, err)
	}
	read, err := c.OpenQuiescentSources(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	assertCertifiedProjection(t, read.ReadView(), 3, true, nil)
	assertCertifiedIncident(t, read.ReadView(), ops[0].Scope, 1, 2, []graphstate.EntityID{3})
	assertCertifiedIncident(t, read.ReadView(), ops[0].Scope, 17, 18, []graphstate.EntityID{3})
	if err := read.Close(); err != nil {
		t.Fatal(err)
	}
	opens := wires[0].opens + wires[1].opens
	again, err := c.Run(t.Context(), id, ops, 37, claims)
	if err != nil || again.decision != first.decision || again.resume != first.resume {
		t.Fatal("immutable original request retry", again, err)
	}
	if wires[0].opens+wires[1].opens != opens {
		t.Fatal("registered retry replanned from new sources")
	}
	_, err = c.Run(t.Context(), id, ops, 38, claims)
	if !errors.Is(err, errInvalid) {
		t.Fatal("changed provenance borrowed prior success", err)
	}
	second, err := c.Run(t.Context(), n.requestID(), []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Scope: ops[0].Scope, Name: "second"}}, 38, nil)
	if err != nil || !second.decision.commit || second.decision.round <= first.decision.round {
		t.Fatal("second distinct operation on persistent stream", second, err)
	}
	later, err := c.OpenQuiescentSources(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	assertCertifiedProjection(t, later.ReadView(), 1, true, []string{"second"})
	if err := later.Close(); err != nil {
		t.Fatal(err)
	}
	for _, group := range n.hosts {
		for _, h := range group {
			if _, err := h.Tick(); err != nil {
				t.Fatal("native voter unhealthy", err)
			}
		}
	}
}
func TestCrossProcessCoordinatorDroppedRegistrationResendsOriginalBytes(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	_, c, ops, claims, wires := coordinatorNativeFixture(t)
	wires[0].dropRegister = true
	id := requestID{211}
	pending, err := c.Run(t.Context(), id, ops, 37, claims)
	if !errors.Is(err, errCrossProcessPending) || !errors.Is(err, graphstore.ErrSourceUnavailable) || pending.decision != (crossStatus{}) || pending.resume.tx == ([16]byte{}) {
		t.Fatal("unknown submission not resumable pending", pending, err)
	}
	opens := wires[0].opens + wires[1].opens
	done, err := c.Run(t.Context(), id, ops, 37, claims)
	if err != nil || !done.decision.commit || done.resume != pending.resume {
		t.Fatal("frozen original resume failed", done, err)
	}
	if len(wires[0].registrations) != 2 || !bytes.Equal(wires[0].registrations[0], wires[0].registrations[1]) || wires[0].opens+wires[1].opens != opens {
		t.Fatal("absence replanned or changed queued original bytes")
	}
}
func TestCrossProcessCoordinatorEarlyNORequiresAbsentProofAndResolvesPresent(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n, c, ops, claims, wires := coordinatorNativeFixture(t)
	before := [2]raftlog.ApplicationRoot{}
	indexes := [2]uint64{}
	for group := range n.hosts {
		view, e := n.hosts[group][0].machine.store.ApplicationView(n.hosts[group][0].driver.Applied())
		if e != nil {
			t.Fatal(e)
		}
		before[group], e = view.RootBounded(t.Context(), 172)
		if e != nil {
			t.Fatal(e)
		}
		if e = view.Close(); e != nil {
			t.Fatal(e)
		}
		indexes[group] = n.hosts[group][0].driver.Applied()
	}
	claims = append([]freshBinding(nil), claims...)
	claims[0].grant.sequence = 999 // Genuine invalid grant reaches native prepare NO.
	result, err := c.Run(t.Context(), n.requestID(), ops, 37, claims)
	if err != nil || result.decision.commit || result.decision.phase != crossDecide {
		t.Fatal("native NO decision", result, err)
	}
	if wires[0].prepares != 1 || wires[1].prepares != 0 {
		t.Fatal("early NO violated ordered prepare prefix", wires[0].prepares, wires[1].prepares)
	}
	proof := n.crossReadPhase(0, result.resume.tx, crossResolve)
	if proof.status.phase != crossResolve || proof.status.commit || proof.status.digest != result.decision.digest {
		t.Fatal("NO participant not resolved", proof.status)
	}
	for group := range n.hosts {
		for _, h := range n.hosts[group] {
			view, e := h.machine.store.ApplicationView(h.driver.Applied())
			if e != nil {
				t.Fatal(e)
			}
			root, e := view.RootBounded(t.Context(), 172)
			if e != nil {
				t.Fatal(e)
			}
			if e = view.Close(); e != nil {
				t.Fatal(e)
			}
			if !bytes.Equal(root.Image, before[group].Image) {
				t.Fatal("ABORT changed graph root", group)
			}
			if group == 1 && h.driver.Applied() != indexes[group] {
				t.Fatal("absent participant acquired application state")
			}
			if group == 0 {
				cdc, e := h.machine.store.ApplicationRecord(t.Context(), h.driver.Applied(), false, 64<<10)
				if e != nil || len(cdc) != 0 {
					t.Fatal("ABORT emitted graph CDC", e)
				}
			}
		}
	}
	_, _, absentErr := n.hosts[1][0].machine.observeCrossTransaction(crossQuery{tx: result.resume.tx}, n.hosts[1][0].driver.Applied())
	if !errors.Is(absentErr, replica.ErrUnavailable) {
		t.Fatal("native later participant was not genuinely absent", absentErr)
	}
	read, err := c.OpenQuiescentSources(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	assertCertifiedProjection(t, read.ReadView(), 3, false, nil)
	assertCertifiedIncident(t, read.ReadView(), ops[0].Scope, 1, 2, nil)
	assertCertifiedIncident(t, read.ReadView(), ops[0].Scope, 17, 18, nil)
	local, err := n.hosts[0][0].PreparePartitionGraphOperations(n.requestID(), []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Scope: ops[0].Scope, Name: "healthy"}}, 38, nil)
	if err != nil {
		t.Fatal(err)
	}
	n.submitPartitionGraph(0, local, reasonNone)
	for _, h := range n.hosts[0] {
		projection, _ := partitionProjection(t, h, h.driver.Applied(), 1, graphGenesisBinding(t).Axis())
		if len(projection.Labels) != 1 || projection.Labels[0] != "healthy" {
			t.Fatal("ABORT stranded source-owned participant lock", projection)
		}
	}
}

func TestCrossProcessCoordinatorLostDecisionReplyRecoversDurableDecision(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n, c, ops, claims, wires := coordinatorNativeFixture(t)
	wires[0].dropDecisionReply = true
	id := n.requestID()
	pending, err := c.Run(t.Context(), id, ops, 37, claims)
	if !errors.Is(err, errCrossProcessPending) || !errors.Is(err, graphstore.ErrSourceUnavailable) || pending.decision != (crossStatus{}) {
		t.Fatal("lost decision reply invented completion", pending, err)
	}
	decision := n.crossReadPhase(0, pending.resume.tx, crossDecide).status
	if !decision.commit {
		t.Fatal("source decision was not durably COMMIT", decision)
	}
	opens := wires[0].opens + wires[1].opens
	done, err := c.Run(t.Context(), id, ops, 37, claims)
	if err != nil || done.decision != decision || done.resume != pending.resume {
		t.Fatal("source-owned immutable decision recovery", done, err)
	}
	if wires[0].opens+wires[1].opens != opens {
		t.Fatal("durable decision recovery replanned")
	}
	for group := range n.hosts {
		proof := n.crossReadPhase(group, done.resume.tx, crossResolve)
		if !proof.status.commit || proof.status.digest != decision.digest || proof.status.round != decision.round {
			t.Fatal("recovery missed participant", proof.status)
		}
	}
}

func TestCrossProcessCoordinatorNilAndCancelledAdmission(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for _, c := range []*crossProcessCoordinator{nil, {}} {
		result, err := c.Run(t.Context(), requestID{1}, nil, 1, nil)
		if !errors.Is(err, errInvalid) || result != (crossProcessResult{}) {
			t.Fatal("nil coordinator Run", result, err)
		}
		read, err := c.OpenQuiescentSources(t.Context())
		if !errors.Is(err, errInvalid) || read != nil {
			t.Fatal("nil coordinator read", err)
		}
	}
	if c, err := newCrossProcessCoordinator(nil, nil); !errors.Is(err, errInvalid) || c != nil {
		t.Fatal("nil host factory", err)
	}
	var read *crossProcessRead
	if read.ReadView() != nil || !errors.Is(read.Close(), errInvalid) {
		t.Fatal("nil composite reader contract")
	}
	_, c, ops, claims, wires := coordinatorNativeFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	result, err := c.Run(ctx, requestID{1}, ops, 1, claims)
	if !errors.Is(err, context.Canceled) || result != (crossProcessResult{}) || wires[0].opens+wires[1].opens != 0 || len(c.runs) != 0 {
		t.Fatal("cancelled request captured/registered work", result, err)
	}
}

func TestCrossProcessCapturedMetadataDoesNotReplaceUnavailableGraphData(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n, c, ops, claims, wires := coordinatorNativeFixture(t)
	if _, err := c.Run(t.Context(), n.requestID(), ops, 37, claims); err != nil {
		t.Fatal(err)
	}
	read, err := c.OpenQuiescentSources(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	wires[0].dropGet = true
	metadata, err := read.sources[0].RootBounded(t.Context(), 172)
	if err != nil || len(metadata.Image) != 172 {
		t.Fatal("owned metadata is not live-peer proof", err)
	}
	entity, err := read.ReadView().Entity(t.Context(), 1)
	if !errors.Is(err, graphstore.ErrSourceUnavailable) || entity.Found || wires[0].requiredGets == 0 {
		t.Fatal("metadata fabricated graph data/absence on missing required RPC", entity, err, wires[0].requiredGets)
	}
}

func TestCrossProcessIncidentPhysicalWorkLimitRefusesWithoutPartialSet(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n, c, ops, claims, _ := coordinatorNativeFixture(t)
	if _, err := c.Run(t.Context(), n.requestID(), ops, 37, claims); err != nil {
		t.Fatal(err)
	}
	read, err := c.OpenQuiescentSources(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	predicate := graphstate.IncidentPredicate{Endpoint: 1, Life: 2, Window: ops[0].Scope}
	page, err := read.ReadView().IncidentRelationships(t.Context(), predicate, 0, graphstate.ReadBudget{Rows: 16, Bytes: 64 << 10})
	if !errors.Is(err, graphstore.ErrResourceLimit) || len(page.Entities) != 0 || page.Complete {
		t.Fatal("small physical-work query published partial/false empty", page, err)
	}
	assertCertifiedIncident(t, read.ReadView(), ops[0].Scope, 1, 2, []graphstate.EntityID{3})
}

func TestCrossProcessCoordinatorGenuineFactoryAndPendingIntentRefusals(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	_, c, ops, claims, wires := coordinatorNativeFixture(t)
	if _, err := newCrossProcessCoordinator(c.host, map[uint64]graphPeerWireTransport{3: nil, 8: nil}); !errors.Is(err, errInvalid) {
		t.Fatal("unwired genuine scope", err)
	}
	if _, err := newCrossProcessCoordinator(c.host, map[uint64]graphPeerWireTransport{3: wires[1], 8: wires[0]}); !errors.Is(err, errInvalid) {
		t.Fatal("genuine wrong partition association", err)
	}
	wires[0].dropRegister = true
	id := requestID{232}
	pending, err := c.Run(t.Context(), id, ops, 37, claims)
	if !errors.Is(err, errCrossProcessPending) {
		t.Fatal(err)
	}
	if _, err := c.Run(t.Context(), id, ops, 38, claims); !errors.Is(err, errInvalid) {
		t.Fatal("changed pending business intent", err)
	}
	if len(wires[0].registrations) != 1 || len(c.runs) != 1 || c.runs[pending.resume.tx].resume != pending.resume {
		t.Fatal("changed intent borrowed/retired frozen request")
	}
	if err := c.host.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Run(t.Context(), id, ops, 37, claims); !errors.Is(err, errInvalid) {
		t.Fatal("closed genuine coordinator", err)
	}
}

type coordinatorPausedLookupWire struct {
	graphPeerWireTransport
	entered chan struct{}
	release chan struct{}
}

func (w *coordinatorPausedLookupWire) RoundTrip(ctx context.Context, wire []byte) ([]byte, error) {
	reply, err := w.graphPeerWireTransport.RoundTrip(ctx, wire)
	if err != nil {
		return nil, err
	}
	request, err := decodeGraphSourceRequest(wire)
	if err != nil {
		return nil, err
	}
	if request.kind == sourceRequestLookup {
		select {
		case w.entered <- struct{}{}:
		default:
		}
		// Pause only after an actual completed native quorum lookup. No data,
		// proof, peer identity or native outcome is manufactured by this guard.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-w.release:
		}
	}
	return reply, nil
}

func TestCrossProcessCoordinatorBusyAndFrozenPendingCapacity(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	t.Run("busy genuine lookup", func(t *testing.T) {
		n, c, ops, claims, wires := coordinatorNativeFixture(t)
		paused := &coordinatorPausedLookupWire{graphPeerWireTransport: wires[0], entered: make(chan struct{}, 1), release: make(chan struct{})}
		c.transports[3] = paused
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		id := n.requestID()
		done := make(chan error, 1)
		go func() { _, err := c.Run(ctx, id, ops, 37, claims); done <- err }()
		select {
		case <-paused.entered:
		case <-t.Context().Done():
			t.Fatal(t.Context().Err())
		}
		pending, err := c.Run(t.Context(), id, ops, 37, claims)
		if !errors.Is(err, errCrossProcessPending) || pending.decision != (crossStatus{}) || pending.resume.tx == ([16]byte{}) {
			t.Fatal("busy request invented completion", pending, err)
		}
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) || !errors.Is(err, errCrossProcessPending) {
			t.Fatal("cancelled native lookup result", err)
		}
		close(paused.release)
		result, err := c.Run(t.Context(), id, ops, 37, claims)
		if err != nil || !result.decision.commit || result.resume != pending.resume {
			t.Fatal("busy cancellation stranded original request", result, err)
		}
	})
	t.Run("two immutable unknown registrations", func(t *testing.T) {
		_, c, ops, claims, wires := coordinatorNativeFixture(t)
		for _, id := range []requestID{{241}, {242}} {
			wires[0].dropRegister = true
			pending, err := c.Run(t.Context(), id, ops, 37, claims)
			if !errors.Is(err, errCrossProcessPending) || pending.decision != (crossStatus{}) || pending.resume.tx == ([16]byte{}) {
				t.Fatal("unknown registration lost frozen identity", pending, err)
			}
		}
		opens := wires[0].opens + wires[1].opens
		registrations := len(wires[0].registrations)
		refused, err := c.Run(t.Context(), requestID{243}, ops, 37, claims)
		if !errors.Is(err, errLimit) || refused.decision != (crossStatus{}) || len(c.runs) != 2 || wires[0].opens+wires[1].opens != opens || len(wires[0].registrations) != registrations {
			t.Fatal("pending capacity admitted extra capture/registration", refused, err)
		}
		result, err := c.Run(t.Context(), requestID{241}, ops, 37, claims)
		if err != nil || !result.decision.commit || len(c.runs) != 1 {
			t.Fatal("capacity refusal retired or stranded frozen original", result, err)
		}
	})
}
