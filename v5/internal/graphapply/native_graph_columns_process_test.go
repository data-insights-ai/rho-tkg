package graphapply

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

// Source-authored, unexecuted OS-child guard: actual native keepers and physical
// derived directories survive process replacement. No parent Host is invented.
func TestNativeColumnsDurableOSChildCreateReopenAndRetire(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	if _, err := nativeColumnProcessPolicy(nil); !errors.Is(err, errInvalid) {
		t.Fatal("nil fresh policy", err)
	}
	for _, failure := range []error{errGraphPinExpired, errGraphPinReleased} {
		code := nativeColumnProcessErrorCode(failure)
		if code == 0 || !errors.Is(nativeColumnProcessRemoteError{processRemoteError{code: code}}, failure) {
			t.Fatal("fresh classification lost sentinel", failure, code)
		}
	}
	code := nativeColumnProcessErrorCode(errors.Join(errGraphHistoryExpired, raftlog.ErrApplicationExpired))
	classified := nativeColumnProcessRemoteError{processRemoteError{code: code}}
	if code != 9 || !errors.Is(classified, errGraphHistoryExpired) || !errors.Is(classified, raftlog.ErrApplicationExpired) {
		t.Fatal("fresh history classification lost either sentinel")
	}
	wire, err := encodeProcessResponse(processResponse{sequence: 1, code: code})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeProcessResponse(wire); !errors.Is(err, errCorrupt) {
		t.Fatal("legacy codec accepted a fresh classification", err)
	}
	if got, err := decodeProcessResponseForProfile(wire, true); err != nil || !errors.Is(processResponseError(got), raftlog.ErrApplicationExpired) {
		t.Fatal("fresh codec lost exact expiry", got, err)
	}
	if nativeColumnProcessErrorCode(raftlog.ErrApplicationExpired) != 0 || nativeColumnProcessErrorCode(replica.ErrUnavailable) != 0 {
		t.Fatal("raw expiry/unavailable promoted to authenticated history expiry")
	}
	spec := nativeGraphCostSpec{Version: 1, Facts: 2, CrossPercent: 100, Writers: 1}
	n := newNativeColumnProcessNetwork(t, spec)
	_, stream := nativeGraphCostArtifact(t, n, "durable-column-child.jsonl")
	record := func(stage string, member uint64, input []byte, out processResponse) {
		t.Helper()
		wire, err := encodeProcessResponse(out)
		if err != nil {
			t.Fatal(err)
		}
		if err := stream.Encode(struct {
			Stage         string
			Member        uint64
			Input, Output []byte
		}{stage, member, input, wire}); err != nil {
			t.Fatal(err)
		}
	}
	fresh := func() graphCut {
		t.Helper()
		handle, cut := nativeColumnProcessOpen(t, n, graphReadFresh, graphCut{})
		n.all[2].call(t, processRequest{op: processSourceCertifiedClose, index: handle})
		return cut
	}
	initial := fresh()
	for phase := uint64(1); phase <= 3; phase++ {
		nativeColumnProcessSubmit(t, n, spec, phase, 0)
	}
	for fact := uint64(0); fact < spec.Facts; fact++ {
		nativeColumnProcessSubmit(t, n, spec, 4, fact)
	}
	pair := nativeGraphCostColumnPair{Initial: initial, Final: fresh(), ID: [16]byte{231}}
	open := func(create bool) [2]*nativeGraphCostColumnClient {
		t.Helper()
		var clients [2]*nativeGraphCostColumnClient
		for g := range 2 {
			c := &nativeGraphCostColumnClient{t: t, n: n, stream: stream, stage: "durable-process", group: g}
			r := c.call(nativeGraphCostColumnRequest{Action: nativeColumnsOpen, Initial: nativeGraphCostColumnCutInput(t, pair.Initial), Final: nativeGraphCostColumnCutInput(t, pair.Final), Generation: pair.ID, Create: create, RetainedExpires: 20})
			if r.Binding.Version != 2 || r.Binding.TerminalAttempts != 4 || r.Retention == nil || r.Retention.State != nativeColumnPinLive {
				t.Fatal("fresh native pin was not installed before source exposure", r)
			}
			if pair.Bindings[g].Coverage != 0 && !nativeColumnSame(pair.Bindings[g], r.Binding) {
				t.Fatal("physical process reopen changed original binding")
			}
			pair.Bindings[g], clients[g] = r.Binding, c
		}
		return clients
	}
	clients := open(true)
	var original [2]nativeColumnProcessRetentionReceipt
	for g, c := range clients {
		for page := uint64(0); ; page++ {
			if page > c.binding.Policy.MaxChunks {
				t.Fatal("finite actual publication did not finish")
			}
			r := c.call(nativeGraphCostColumnRequest{Action: nativeColumnsPublish})
			if r.EOF {
				pair.Manifests[g], original[g] = r.Manifest, *r.Retention
				if r.Manifest.Generation.Pin != original[g].Pin {
					t.Fatal("derived generation lost its exact native owner")
				}
				break
			}
		}
	}
	if err := nativeColumnJoinBoundaries(pair.Bindings, pair.Manifests); err != nil {
		t.Fatal(err)
	}
	nativeGraphCostColumnReadPair(t, clients, &pair, spec, pair.Final, false, false)
	nativeGraphCostColumnCloseClients(clients) // transient Close must not release pins
	for fact := uint64(0); fact < spec.Facts; fact++ {
		nativeColumnProcessSubmit(t, n, spec, 5, fact)
	}
	_ = fresh()
	restartNativeColumnProcesses(t, n, spec, stream)
	clients = open(false)
	for g, c := range clients {
		r := c.call(nativeGraphCostColumnRequest{Action: nativeColumnsRead, At: nativeGraphCostColumnCutInput(t, pair.Final)})
		if !bytes.Equal(r.Retention.Original, original[g].Original) || !bytes.Equal(r.Retention.Token, original[g].Token) || r.Retention.Pin != original[g].Pin {
			t.Fatal("reopen substituted native original/token")
		}
	}
	// Full exact old row/column census requires p5, excludes later p6 and joins
	// both authentic local sealed prefixes; a current-state substitution fails.
	nativeGraphCostColumnReadPair(t, clients, &pair, spec, pair.Final, false, false)
	// The normal cookie-bound terminal path uses Sequence0, unlike continuation.
	closed := clients[1].call(nativeGraphCostColumnRequest{Action: nativeColumnsRetire})
	if closed.Retention.State != nativeColumnRetired || closed.Retention.Attempts != 1 {
		t.Fatal("explicit live-session retirement was not durable")
	}
	clients[0].call(nativeGraphCostColumnRequest{Action: nativeColumnsClose})
	input, err := nativeGraphCostColumnInput(nativeGraphCostColumnRequest{Action: nativeColumnsRecoverRetire, Generation: pair.ID})
	if err != nil {
		t.Fatal(err)
	}
	// Refuse the actual B cancellation transport after A's completed native
	// cancellation. This is a genuine partial terminal operation, no fake state.
	hub := n.all[1].mux.hub
	blockedCancellation := false // protected by hub.mu
	hub.mu.Lock()
	hub.beforeRelay = func(ctx context.Context, origin, target uint64, wire []byte) error {
		request, err := decodeGraphSourceRequest(wire)
		if err != nil || request.kind != sourceSubmit || origin != 1 || target != 8 {
			return err
		}
		proposal, err := decodeGraphSourceProposal(request.command)
		if err != nil || !isGraphPinWire(proposal.wire) {
			return err
		}
		command, err := decodeGraphPinCommand(proposal.wire)
		if err == nil && command.phase == pinCancelIssue {
			hub.mu.Lock()
			blockedCancellation = true
			hub.mu.Unlock()
			return replica.ErrUnavailable
		}
		return err
	}
	hub.mu.Unlock()
	response, err := n.all[1].exchange(t.Context(), processRequest{op: processCostObserve, data: input})
	record("partial-retirement", 1, input, response)
	if !errors.Is(errors.Join(err, processResponseError(response)), replica.ErrUnavailable) || len(response.aux) != 0 {
		t.Fatal("partial retiring operation invented terminal success", response, err)
	}
	hub.mu.Lock()
	hub.beforeRelay = nil
	blocked := blockedCancellation
	hub.mu.Unlock()
	if !blocked {
		t.Fatal("actual missing-participant cancellation was not intercepted")
	}
	restartNativeColumnProcesses(t, n, spec, stream)
	for g, leader := range n.leaders {
		response := n.all[leader].call(t, processRequest{op: processCostObserve, data: input})
		record("retirement-recovered", leader, input, response)
		r, err := decodeNativeColumnProcessRetirement(response.aux)
		wantAttempts := byte(2)
		if g == 1 { // already terminal originals consume no second attempt
			wantAttempts = 1
		}
		if err != nil || r.Generation != pair.ID || r.Pin != original[g].Pin || !bytes.Equal(r.Original, original[g].Original) || !bytes.Equal(r.Token, original[g].Token) || r.Attempts != wantAttempts {
			t.Fatal("same-directory retiring recovery changed original or reset attempts", r, err)
		}
		// A fully retired directory never hydrates a new source at its old token.
		wire, err := nativeGraphCostColumnInput(nativeGraphCostColumnRequest{Action: nativeColumnsOpen, Initial: nativeGraphCostColumnCutInput(t, pair.Initial), Final: nativeGraphCostColumnCutInput(t, pair.Final), Generation: pair.ID, RetainedExpires: 20})
		if err != nil {
			t.Fatal(err)
		}
		response, err = n.all[leader].exchange(t.Context(), processRequest{op: processCostObserve, data: wire})
		record("closed-generation-refusal", leader, wire, response)
		if !errors.Is(errors.Join(err, processResponseError(response)), errGraphPinReleased) || len(response.aux) != 0 {
			t.Fatal("retired generation was rehydrated", response, err)
		}
		// The local closed-journal refusal above grants no native authority. Probe
		// the original GPT separately through the actual logical keepers.
		wire, err = nativeGraphCostColumnInput(nativeGraphCostColumnRequest{Action: nativeColumnsPinProbe, Generation: pair.ID, RetainedToken: original[g].Token})
		if err != nil {
			t.Fatal(err)
		}
		response, err = n.all[leader].exchange(t.Context(), processRequest{op: processCostObserve, data: wire})
		record("actual-keeper-token-refusal", leader, wire, response)
		if !errors.Is(errors.Join(err, processResponseError(response)), errGraphPinReleased) || len(response.aux) != 0 {
			t.Fatal("actual native keepers did not reject original retired token", response, err)
		}
	}
}
