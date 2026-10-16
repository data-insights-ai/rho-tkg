package graphapply

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
)

// Faults occur only after the real source service completes native quorum work.
// Mutated reply bytes are negative correlation inputs, never sealed proofs.
type failedCertifiedReplyTransport struct {
	graphPeerWireTransport
	t              *testing.T
	fault          string
	cancel         context.CancelFunc
	request, reply []byte
	calls          int
}

func (f *failedCertifiedReplyTransport) RoundTrip(ctx context.Context, request []byte) ([]byte, error) {
	f.calls++
	f.request = bytes.Clone(request)
	reply, err := f.graphPeerWireTransport.RoundTrip(ctx, request)
	if err != nil {
		return nil, err
	}
	f.reply = bytes.Clone(reply)
	response, err := decodeGraphWireResponse(reply)
	if err != nil || response.err != nil {
		f.t.Fatal("fault must follow genuine successful native response", err, response.err)
	}
	body, err := decodeGraphCutReplyBody(response.body)
	if err != nil || body.observation.sourceRows <= 0 || body.observation.sourceBytes <= 0 || body.observation.outputBytes <= 0 {
		f.t.Fatal("fault must follow completed native work, not fabricated authority", err)
	}
	switch f.fault {
	case "drop":
		return nil, graphstore.ErrSourceUnavailable
	case "cancel":
		f.cancel()
		return reply, nil
	case "corrupt-cut-body":
		response.body = bytes.Clone(response.body)
		response.body[len(response.body)-1] ^= 1
	case "mismatched-cut-nonce":
		response.body = bytes.Clone(response.body)
		// GCP2's body starts9, GCB2's GCQ2 starts192, nonce starts13.
		// Re-seal inert frames only, preserving real outer authentication fields.
		response.body[9+192+13] ^= 1
		wireGuardReseal(response.body[9+192 : 9+192+121])
		wireGuardReseal(response.body[9 : len(response.body)-32])
		wireGuardReseal(response.body)
	default:
		f.t.Fatal("unknown test reply fault", f.fault)
	}
	changed, err := encodeGraphWireResponse(response)
	if err != nil {
		f.t.Fatal(err)
	}
	return changed, nil
}

func TestRemoteCertifiedReadFailureRetiresOriginalGrantWithoutBudgetReset(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for _, tc := range []struct {
		fault    string
		want     error
		received bool
	}{
		{"drop", graphstore.ErrSourceUnavailable, false},
		{"cancel", context.Canceled, false},
		{"corrupt-cut-body", errCorrupt, true},
		{"mismatched-cut-nonce", errInvalid, true},
	} {
		t.Run(tc.fault, func(t *testing.T) {
			n := newCertifiedGraphNetwork(t)
			c := remoteCertifiedFixture(t, n)
			genuine := c.transports[8]
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			fault := &failedCertifiedReplyTransport{graphPeerWireTransport: genuine, t: t, fault: tc.fault, cancel: cancel}
			c.transports[8] = fault
			backend := remoteCertifiedReceiptBackend(t, c)
			bytesBefore, rowsBefore, outputBefore := backend.budget.bytes, backend.budget.rows, backend.arena.Used()
			remainingOutput := backend.arena.Remaining()
			issuedBefore := n.hosts[1][0].issuedReads.Load()
			observation, err := backend.Read(ctx, 8, graphCutQuery{kind: 1})
			if !errors.Is(err, tc.want) || observation != (graphCutObservation{}) || fault.calls != 1 || n.hosts[1][0].issuedReads.Load() <= issuedBefore {
				t.Fatal("post-native reply failure returned partial authority or skipped genuine work", observation, err, fault.calls)
			}
			request, err := decodeGraphSourceRequest(fault.request)
			if err != nil {
				t.Fatal(err)
			}
			grant, err := decodeGraphCutQuery(request.command)
			if err != nil {
				t.Fatal(err)
			}
			// Independently specified framing reserves:8192 reply +1024 wrapper
			// +121 query bytes; output reserves4*each wire plus1024 fixed bytes.
			if grant.sourceRows != min(1<<20, backend.budget.maxRows-rowsBefore) || grant.sourceBytes != min(64<<20, backend.budget.maxBytes-bytesBefore-9337) || grant.outputBytes != min(64<<20, remainingOutput-38372) {
				t.Fatal("original per-request grant changed across callback", grant)
			}
			wireBytes := len(fault.request)
			wireOutput := 4*wireBytes + 512
			if tc.received {
				wireBytes += len(fault.reply)
				wireOutput += 4*len(fault.reply) + 128
			}
			if backend.budget.rows-rowsBefore != grant.sourceRows || backend.budget.bytes-bytesBefore != grant.sourceBytes+wireBytes || backend.arena.Used()-outputBefore != grant.outputBytes+wireOutput || backend.budget.calls != 1 {
				t.Fatal("failed native work refunded or spent only wire on original ledger", backend.budget.rows-rowsBefore, backend.budget.bytes-bytesBefore, backend.arena.Used()-outputBefore, grant, wireBytes, wireOutput)
			}
			spentBytes, spentRows, spentOutput := backend.budget.bytes, backend.budget.rows, backend.arena.Used()
			observation, err = backend.Read(t.Context(), 8, graphCutQuery{kind: 1})
			if !errors.Is(err, errLimit) || observation != (graphCutObservation{}) || fault.calls != 1 || backend.budget.bytes != spentBytes || backend.budget.rows != spentRows || backend.arena.Used() != spentOutput {
				t.Fatal("retry reset failed operation allowance or dispatched another native read", observation, err)
			}
			c.transports[8] = genuine
			independent := remoteCertifiedReceiptBackend(t, c)
			observation, err = independent.Read(t.Context(), 8, graphCutQuery{kind: 1})
			if err != nil || observation.scope != n.hosts[1][0].machine.scope() || observation.sourceRows <= 0 || observation.sourceBytes <= 0 {
				t.Fatal("failed caller operation poisoned independent native source access", observation, err)
			}
			if backend.budget.bytes != spentBytes || backend.budget.rows != spentRows || backend.arena.Used() != spentOutput {
				t.Fatal("independent operation refreshed previous operation allowance")
			}
		})
	}
}

func TestRemoteCertifiedReadAdmissionRefusesMissingScopeAndMalformedQueryWithoutWork(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCertifiedGraphNetwork(t)
	c := remoteCertifiedFixture(t, n)
	backend := remoteCertifiedReceiptBackend(t, c)
	issued := n.hosts[1][0].issuedReads.Load()
	for _, tc := range []struct {
		partition uint64
		query     graphCutQuery
		want      error
	}{
		{99, graphCutQuery{kind: 1}, errGraphCutScope},
		{8, graphCutQuery{kind: 5}, errInvalid},
	} {
		observation, err := backend.Read(t.Context(), tc.partition, tc.query)
		if !errors.Is(err, tc.want) || observation != (graphCutObservation{}) || backend.budget.calls != 0 || backend.budget.bytes != 0 || backend.budget.rows != 0 || backend.arena.Used() != 0 || n.hosts[1][0].issuedReads.Load() != issued {
			t.Fatal("unadmitted scope/query dispatched source work or returned authority", observation, err)
		}
	}
}
