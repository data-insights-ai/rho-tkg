package graphapply

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// This records actual authenticated request/reply bytes without substituting a
// source, observation, error or proof. The delegated transport remains native.
type recordedCertifiedControlTransport struct {
	graphPeerWireTransport
	request, reply []byte
}

func (r *recordedCertifiedControlTransport) RoundTrip(ctx context.Context, request []byte) ([]byte, error) {
	r.request = bytes.Clone(request)
	reply, err := r.graphPeerWireTransport.RoundTrip(ctx, request)
	r.reply = bytes.Clone(reply)
	return reply, err
}

// RED: zero allowances remain valid for native compatibility and inert codecs,
// but an authenticated remote request must not receive native default budgets.
func TestCertifiedGraphControlBudgetServiceRejectsZeroRemoteGrantBeforeNativeRead(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCertifiedGraphNetwork(t)
	c := remoteCertifiedFixture(t, n)
	backend := remoteCertifiedReceiptBackend(t, c)
	nonce, err := graphReadNonce()
	if err != nil {
		t.Fatal(err)
	}
	query, err := encodeGraphCutQuery(graphCutQuery{kind: 1, nonce: nonce})
	if err != nil {
		t.Fatal("native/inert zero-allowance compatibility prerequisite", err)
	}
	h := n.hosts[1][0]
	issued := h.issuedReads.Load()
	response, err := backend.endpoints[8].call(t.Context(), graphSourceRequest{kind: sourceCutRead, command: query}, 8192)
	if !errors.Is(err, errInvalid) || response.body != nil || h.issuedReads.Load() != issued {
		t.Fatal("remote zero grant entered default-budget native read instead of pre-dispatch refusal", err, len(response.body), issued, h.issuedReads.Load())
	}
}

// GUARD: the E15 whole-operation RED can refuse on framing alone. This direct
// completed ReadIndex exercises the source grant at the native reader itself.
func TestCertifiedGraphControlBudgetNativePendingManifestHonorsReadIndexGrant(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCertifiedGraphNetwork(t)
	ops, claims := crossEdgeFixture(t, n)
	tx, register := n.crossPlan(ops, claims, 24)
	decision := n.crossCommit(tx, register)
	h := n.hosts[1][0]
	pump := &nativeGraphTestPump{n: n, group: 1, host: h}
	l := h.machine.limits
	if len(tx.manifest) <= 172 || !decision.status.commit || decision.status.phase != crossDecide {
		t.Fatal("fixture must retain a genuine pending committed manifest")
	}
	nonce, err := graphReadNonce()
	if err != nil {
		t.Fatal(err)
	}
	query := graphCutQuery{kind: 1, nonce: nonce, sourceRows: l.sourceRows, sourceBytes: len(tx.manifest) - 1, outputBytes: l.outputBytes}
	refused, err := pump.Cut(t.Context(), query)
	if err != nil || !errors.Is(refused.err, graphstore.ErrResourceLimit) || !errors.Is(refused.err, raftlog.ErrLimit) || refused.proof != (graphCutProof{}) || refused.question != query || refused.id.session == ([16]byte{}) || refused.id.sequence == 0 {
		t.Fatalf("native grant refusal: transport=%v operation=%v localLimit=%v proofZero=%v questionMatch=%v issuer=%v sequence=%d", err, refused.err, errors.Is(refused.err, errLimit), refused.proof == (graphCutProof{}), refused.question == query, refused.id.session != ([16]byte{}), refused.id.sequence)
	}
	nonce, err = graphReadNonce()
	if err != nil {
		t.Fatal(err)
	}
	query.nonce, query.sourceBytes = nonce, l.sourceBytes
	completed, err := pump.Cut(t.Context(), query)
	if err != nil || completed.err != nil {
		t.Fatal("funded genuine native completed read counterpart", err, completed.err)
	}
	if err := checkGraphCutProof(completed.proof, query); err != nil {
		t.Fatal("funded counterpart lacks actual completed native provenance", err)
	}
	work := completed.proof.observation
	if completed.id == refused.id || completed.question != query || work.pending != decision.status.tx || work.coordinator != decision.status.ns.partition || work.sourceBytes < len(tx.manifest) || work.sourceRows <= 0 || work.outputBytes <= 0 || work.sourceRows > query.sourceRows || work.sourceBytes > query.sourceBytes || work.outputBytes > query.outputBytes {
		t.Fatal("completed cut hid physical manifest work or changed request grants", len(tx.manifest), query, work)
	}
	if work.sourceBytes > 172 {
		// This one-short counterpart funds the exact same pending state, with
		// generous row/output allowances. It cannot be a framing refusal.
		query.sourceBytes = work.sourceBytes - 1
		query.nonce, err = graphReadNonce()
		if err != nil {
			t.Fatal(err)
		}
		short, err := pump.Cut(t.Context(), query)
		if err != nil || !errors.Is(short.err, errLimit) || short.proof != (graphCutProof{}) {
			t.Fatal("one-short actual native source work acquired authority", short, err)
		}
	}
	actual := n.crossReadPhase(0, decision.status.tx, crossDecide)
	if actual.status != decision.status {
		t.Fatal("budget refusal changed immutable committed decision", actual.status, decision.status)
	}
}

func TestCertifiedGraphControlBudgetAuthenticatedLedgerIncludesActualNativeWork(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCertifiedGraphNetwork(t)
	ops, claims := crossEdgeFixture(t, n)
	tx, register := n.crossPlan(ops, claims, 24)
	decision := n.crossCommit(tx, register)
	c := remoteCertifiedFixture(t, n)
	recording := &recordedCertifiedControlTransport{graphPeerWireTransport: c.transports[8]}
	c.transports[8] = recording
	backend := remoteCertifiedReceiptBackend(t, c)
	bytesBefore, rowsBefore, outputBefore := backend.budget.bytes, backend.budget.rows, backend.arena.Used()
	observation, err := backend.Read(t.Context(), 8, graphCutQuery{kind: 1})
	if err != nil || observation.pending != decision.status.tx {
		t.Fatal("funded authenticated pending cut prerequisite", observation, err)
	}
	request, err := decodeGraphSourceRequest(recording.request)
	if err != nil || request.kind != sourceCutRead {
		t.Fatal("guard must record actual dispatched cut request", err)
	}
	grant, err := decodeGraphCutQuery(request.command)
	if err != nil || grant.sourceRows <= 0 || grant.sourceBytes <= 0 || grant.outputBytes <= 0 {
		t.Fatal("authenticated request lacked positive native work grant", grant, err)
	}
	reply, err := decodeGraphWireResponse(recording.reply)
	if err != nil || reply.err != nil {
		t.Fatal("guard must record successful genuine authenticated reply", err, reply.err)
	}
	body, err := decodeGraphCutReplyBody(reply.body)
	grant.nonce = request.nonce // The authenticated envelope owns correlation.
	if err != nil || body.question != grant || body.observation != observation || body.id.session != reply.issuer {
		t.Fatal("native completed-work body lost actual request correlation", err)
	}
	if observation.sourceBytes < len(tx.manifest) || observation.sourceRows <= 0 || observation.outputBytes <= 0 || observation.sourceBytes > grant.sourceBytes || observation.sourceRows > grant.sourceRows || observation.outputBytes > grant.outputBytes {
		t.Fatal("authenticated completed work omitted native manifest or exceeded grant", len(tx.manifest), grant, observation)
	}
	wireBytes := len(recording.request) + len(recording.reply)
	if backend.budget.calls != 1 || backend.budget.bytes-bytesBefore != wireBytes+observation.sourceBytes || backend.budget.rows-rowsBefore != observation.sourceRows {
		t.Fatal("total ledger charged only wire or reset native physical work", backend.budget.bytes-bytesBefore, wireBytes, observation.sourceBytes, backend.budget.rows-rowsBefore, observation.sourceRows)
	}
	wireOutput := 4*wireBytes + 512 + 128
	if backend.arena.Used()-outputBefore != wireOutput+observation.outputBytes || backend.budget.bytes > backend.budget.maxBytes || backend.budget.rows > backend.budget.maxRows {
		t.Fatal("completed native ownership was not charged to the original shared arena", backend.arena.Used()-outputBefore, wireOutput, observation.outputBytes)
	}
}
