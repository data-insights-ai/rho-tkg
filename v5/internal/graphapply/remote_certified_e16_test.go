package graphapply

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

// This guard supplies the missing physical-process E16 proof. It changes no
// engine behavior: a real later read fence is not resolution of an older YES.
func TestRemoteE16PreparedCommit90AfterObserved100RequiresResolvedCertificate(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCertifiedProcessNetwork(t)
	oldOracle := newCertifiedProcessOracle()
	freshRequest, err := encodeGraphE15ReadRequest(graphE15ReadRequest{mode: graphReadFresh})
	if err != nil {
		t.Fatal(err)
	}
	oldReply := n.all[2].call(t, processRequest{op: processSourceCertifiedOpen, data: freshRequest})
	oldHandle, oldCut, err := decodeGraphE15ReadToken(oldReply.aux)
	if err != nil {
		t.Fatal(err)
	}
	closeOld := graphE16CloseOnCleanup(t, n.all[2], oldHandle)
	graphE16CompareCut(t, n, oldHandle, oldOracle)
	for _, leader := range []uint64{1, 4} {
		graphE16CutCommand(t, n, leader, certifiedPrepareFence, 89, graphCutApplied)
	}
	arrived := make(chan crossCommand, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	hub := n.all[1].mux.hub
	hub.mu.Lock()
	hub.beforeRelay = func(ctx context.Context, origin, partition uint64, wire []byte) error {
		if origin != 1 || partition != 3 {
			return nil
		}
		request, err := decodeGraphSourceRequest(wire)
		if err != nil || request.kind != sourceSubmit {
			return err
		}
		proposal, err := decodeGraphSourceProposal(request.command)
		if err != nil || !isCrossPartitionWire(proposal.wire) {
			return err
		}
		command, _, err := decodeCrossCommand(proposal.wire, defaultMaterializerLimits())
		if err != nil || command.phase != crossDecide {
			return err
		}
		select {
		case arrived <- command:
		default:
		}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	hub.mu.Unlock()
	request := n.all[1].call(t, processRequest{op: processCertifiedAction, entity: certifiedCreateCrossEdge, index: 3})
	proposal, err := decodeCertifiedProcessProposal(request.aux)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan graphProcessAnswer, 1)
	var workers sync.WaitGroup
	workers.Go(func() {
		r, e := n.all[1].exchange(ctx, processRequest{op: processSourceCoordinate, data: proposal.wire})
		result <- graphProcessAnswer{r, e}
	})
	t.Cleanup(func() {
		unblock()
		cancel()
		workers.Wait()
		hub.mu.Lock()
		hub.beforeRelay = nil
		hub.mu.Unlock()
	})
	var undecided crossCommand
	select {
	case undecided = <-arrived:
	case got := <-result:
		t.Fatal("transaction completed before genuine Decide transport hold", got.err)
	case <-time.After(10 * time.Second):
		t.Fatal("transaction never reached genuine Decide after both YES")
	}
	if len(undecided.proofs) != 2 {
		t.Fatal("held decision must contain both real participant votes", undecided.proofs)
	}
	tx := undecided.proofs[0].tx
	digest := undecided.proofs[0].digest
	for i, vote := range undecided.proofs {
		partition := []uint64{3, 8}[i]
		if vote.ns.partition != partition || vote.phase != crossPrepare || !vote.yes || vote.floor != 89 || vote.tx != tx || vote.digest != digest {
			t.Fatal("held decision does not preserve native YES89 provenance", vote)
		}
	}
	for _, ids := range [][]uint64{{1, 2, 3}, {4, 5, 6}} {
		var first []byte
		for _, id := range ids {
			state := n.all[id].call(t, processRequest{op: processStateOp})
			record := n.all[id].call(t, processRequest{op: processEntryRecord, index: state.applied})
			vote, e := decodeCrossStatus(record.outcome)
			if e != nil || vote.phase != crossPrepare || !vote.yes || vote.floor != 89 || vote.tx != tx || vote.digest != digest || len(record.changes) != 0 {
				t.Fatal("actual voter lacks undecided durable YES89", id, vote, e)
			}
			if first == nil {
				first = record.outcome
			} else if !bytes.Equal(first, record.outcome) {
				t.Fatal("durable YES differs within voter group", id)
			}
		}
	}
	for _, leader := range []uint64{1, 4} {
		graphE16CutCommand(t, n, leader, certifiedPrepareFence, 100, graphCutApplied)
	}
	for i, leader := range []uint64{1, 4} {
		observation := graphE16Collect(t, n, leader, oldCut.certificates[i].scope)
		if observation.floor != 100 || observation.pending != tx || observation.coordinator != 3 {
			t.Fatal("completed source read must observe100 and unresolved older TxID", leader, observation)
		}
	}
	readCtx, stop := context.WithTimeout(t.Context(), 10*time.Second)
	refused, readErr := n.all[2].exchange(readCtx, processRequest{op: processSourceCertifiedOpen, data: freshRequest})
	stop()
	// Before a decision exists, Fresh returns a pending refusal. The outer IPC
	// cause mask preserves its joined unavailable cause, not a fabricated token.
	if !errors.Is(readErr, replica.ErrUnavailable) || errors.Is(readErr, context.DeadlineExceeded) || len(refused.aux) != 0 {
		t.Fatal("max observed100 became certification of unresolved older YES", readErr, len(refused.aux))
	}
	for _, leader := range []uint64{1, 4} {
		graphE16CutCommand(t, n, leader, certifiedPrepareCertificate, 100, graphCutPending)
	}
	for id, p := range n.all {
		r := p.call(t, processRequest{op: processSourceCutEvidence, entity: 100})
		evidence, e := decodeGraphE15CutEvidence(r.aux)
		if e != nil || evidence.fence != 100 || len(evidence.certificate) != 0 {
			t.Fatal("pending voter published a resolved100 certificate", id, evidence, e)
		}
	}
	graphE16CompareCut(t, n, oldHandle, oldOracle)
	unblock()
	var completed crossProcessResult
	select {
	case got := <-result:
		if got.err != nil {
			t.Fatal("older prepared operation failed after release", got.err)
		}
		completed, err = decodeGraphProcessCoordinatorResult(got.response)
		if err != nil || !completed.decision.commit || completed.decision.round != 90 || completed.decision.tx != tx || completed.decision.digest != digest {
			t.Fatal("observed100 changed immutable old prepared COMMIT90", completed.decision, err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("older prepared operation did not complete after Decide release")
	}
	drainCtx, stopDrain := context.WithTimeout(t.Context(), 10*time.Second)
	defer stopDrain()
	if err := drainGraphProcessMuxes(drainCtx, n.all); err != nil {
		t.Fatal(err)
	}
	for _, ids := range [][]uint64{{1, 2, 3}, {4, 5, 6}} {
		var outcome, changes []byte
		for _, id := range ids {
			state := n.all[id].call(t, processRequest{op: processStateOp})
			record := n.all[id].call(t, processRequest{op: processEntryRecord, index: state.applied})
			resolved, e := decodeCrossStatus(record.outcome)
			if e != nil || resolved.phase != crossResolve || !resolved.commit || resolved.round != 90 || resolved.tx != tx || resolved.digest != digest || resolved.index != state.applied || len(record.changes) == 0 {
				t.Fatal("actual voter lacks original COMMIT90 resolve/CDC", id, resolved, e)
			}
			if outcome == nil {
				outcome, changes = record.outcome, record.changes
			} else if !bytes.Equal(outcome, record.outcome) || !bytes.Equal(changes, record.changes) {
				t.Fatal("original resolve outcome/CDC differs within voter group", id)
			}
		}
	}
	freshReply := n.all[2].call(t, processRequest{op: processSourceCertifiedOpen, data: freshRequest})
	freshHandle, freshCut, err := decodeGraphE15ReadToken(freshReply.aux)
	if err != nil || len(freshCut.certificates) != 2 {
		t.Fatal("resolved Fresh must return complete coverage", err)
	}
	closeFresh := graphE16CloseOnCleanup(t, n.all[2], freshHandle)
	for _, cert := range freshCut.certificates {
		if cert.round != 100 || cert.sourceRound != 90 {
			t.Fatal("Fresh100 omitted actual resolved90 prefix", cert)
		}
	}
	newOracle := newCertifiedProcessOracle()
	newOracle.crossEdge()
	graphE16CompareCut(t, n, freshHandle, newOracle)
	graphE16CompareCut(t, n, oldHandle, oldOracle)
	closeOld()
	atRequest, err := encodeGraphE15ReadRequest(graphE15ReadRequest{mode: graphReadAt, cut: oldCut})
	if err != nil {
		t.Fatal(err)
	}
	atReply := n.all[2].call(t, processRequest{op: processSourceCertifiedOpen, data: atRequest})
	atHandle, atCut, err := decodeGraphE15ReadToken(atReply.aux)
	if err != nil || !slices.Equal(atCut.certificates, oldCut.certificates) {
		t.Fatal("reopened At changed original cut", err)
	}
	closeAt := graphE16CloseOnCleanup(t, n.all[2], atHandle)
	graphE16CompareCut(t, n, atHandle, oldOracle)
	closeAt()
	closeFresh()
	if err := drainGraphProcessMuxes(drainCtx, n.all); err != nil {
		t.Fatal(err)
	}
	for id, p := range n.all {
		r := p.call(t, processRequest{op: processSourceCutEvidence, entity: 100})
		evidence, e := decodeGraphE15CutEvidence(r.aux)
		cert, ce := decodeGraphCertificate(evidence.certificate)
		selected := freshCut.certificates[0]
		if id >= 4 {
			selected = freshCut.certificates[1]
		}
		if e != nil || ce != nil || evidence.fence != 100 || cert != selected {
			t.Fatal("returned100 certificate differs from actual voter", id, e, ce)
		}
		state := p.call(t, processRequest{op: processStateOp})
		if state.stats[statViews] != 0 || state.stats[statPinnedBytes] != 0 || !slices.Equal(state.handles, []uint64{0, 0}) {
			t.Fatal("E16 retained reader/worker/relay residue", id, state.stats, state.handles)
		}
	}
	t.Log("E16: both YES floors89; observed fences100; certificate100 refused before decision; actual COMMIT90; resolved Fresh100 and exact old At")
}

func graphE16CutCommand(t *testing.T, n *certifiedProcessNetwork, leader, action, round uint64, status byte) {
	t.Helper()
	r := n.all[leader].call(t, processRequest{op: processCertifiedAction, entity: action, index: round})
	proposal, err := decodeCertifiedProcessProposal(r.aux)
	if err != nil {
		t.Fatal(err)
	}
	command, err := decodeGraphCutCommand(proposal.wire)
	phase := graphCutFence
	if action == certifiedPrepareCertificate {
		phase = graphCutCertify
	}
	if err != nil || command.scope != proposal.target || command.round != round || command.phase != phase {
		t.Fatal("child did not prepare the actual requested cut command", command, err)
	}
	n.group(leader).event(t, leader, processRequest{op: processPropose, data: r.aux})
	state := n.all[leader].call(t, processRequest{op: processStateOp})
	var outcome []byte
	for id, p := range n.group(leader).nodes {
		record := p.call(t, processRequest{op: processEntryRecord, index: state.applied})
		result, e := decodeGraphCutResult(record.outcome)
		if e != nil || result.index != state.applied || result.phase != phase || result.round != round || result.status != status || len(record.changes) != 0 {
			t.Fatal("cut command lacks actual typed outcome or emitted graph CDC", id, result, e)
		}
		if outcome == nil {
			outcome = record.outcome
		} else if !bytes.Equal(outcome, record.outcome) {
			t.Fatal("typed cut outcome differs within voter group", id)
		}
	}
}

func graphE16Collect(t *testing.T, n *certifiedProcessNetwork, leader uint64, scope allocationScope) graphCutObservation {
	t.Helper()
	limits := defaultMaterializerLimits()
	nonce, err := graphReadNonce()
	if err != nil {
		t.Fatal(err)
	}
	q := graphCutQuery{kind: 1, nonce: nonce, sourceRows: limits.sourceRows, sourceBytes: limits.sourceBytes, outputBytes: limits.outputBytes}
	query, err := encodeGraphCutQuery(q)
	if err != nil {
		t.Fatal(err)
	}
	request := graphSourceRequest{kind: sourceCutRead, sequence: 1, nonce: nonce, command: query}
	wire, err := encodeGraphSourceRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	// Service origin3 is a distinct configured child stream. Origin1's Decide
	// and origin2's certified reader cannot occupy this stream's native gate.
	r := n.all[leader].call(t, processRequest{op: processSourceServe, entity: 3, data: wire})
	response, err := decodeGraphWireResponse(r.aux)
	if err != nil || response.err != nil || response.peer.scope != scope || response.peer.voter != leader || response.kind != request.kind || response.sequence != request.sequence || response.nonce != nonce {
		t.Fatal("completed configured source association/correlation", err, response.err)
	}
	body, err := decodeGraphCutReplyBody(response.body)
	if err != nil || body.scope != scope || body.question != q || body.id.session != response.issuer || body.observation.index > r.applied {
		t.Fatal("actual request-bound completed source100 observation", err)
	}
	return body.observation
}

func graphE16CompareCut(t *testing.T, n *certifiedProcessNetwork, handle uint64, oracle *certifiedProcessOracle) {
	t.Helper()
	for _, ids := range [][]uint64{{1, 3, 17}, {7}} {
		input, err := encodeGraphProcessObserveIDs(ids)
		if err != nil {
			t.Fatal(err)
		}
		r := n.all[2].call(t, processRequest{op: processSourceCertifiedObserve, index: handle, data: input})
		facts, err := decodeGraphProcessObservationBatch(r.aux)
		if err != nil {
			t.Fatal(err)
		}
		assertGraphE15IDs(t, facts, ids)
		for _, fact := range facts {
			if err := oracle.compare(fact.id, fact.fact); err != nil {
				t.Fatal("exact E16 graph/lifecycle/incidence/phantom oracle", handle, err)
			}
		}
	}
}

func graphE16CloseOnCleanup(t *testing.T, p *graphProcess, handle uint64) func() {
	t.Helper()
	var once sync.Once
	close := func() {
		once.Do(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if _, err := p.exchange(ctx, processRequest{op: processSourceCertifiedClose, index: handle}); err != nil {
				t.Error("E16 certified owner cleanup", handle, err)
			}
		})
	}
	t.Cleanup(close)
	return close
}
