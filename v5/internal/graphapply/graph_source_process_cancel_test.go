package graphapply

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

// These duplex pipes exercise the actual embedding mux/framing, not graph
// authority. Withholding a terminal response is an explicit transport schedule.
func processCancelWireFixture(t *testing.T) (*graphProcessMux, func() processRequest, func(processRequest, error, ...[]byte)) {
	t.Helper()
	requestReader, requestWriter := io.Pipe()
	responseReader, responseWriter := io.Pipe()
	ctx, cancel := context.WithCancel(t.Context())
	p := &graphProcess{stdin: requestWriter, stdout: responseReader, incarnation: [16]byte{1}}
	hub := &graphProcessHub{ctx: ctx, cancel: cancel, idle: make(chan struct{}, 1)}
	m := &graphProcessMux{p: p, hub: hub, pending: make(map[uint64]chan graphProcessAnswer), relays: make(map[uint64]context.CancelFunc)}
	var wg sync.WaitGroup
	wg.Go(m.readLoop)
	t.Cleanup(func() {
		cancel()
		_ = requestReader.Close()
		_ = requestWriter.Close()
		_ = responseReader.Close()
		_ = responseWriter.Close()
		wg.Wait()
	})
	request := func() processRequest {
		t.Helper()
		wire, err := readProcessFrame(requestReader)
		if err != nil {
			t.Fatal(err)
		}
		r, err := decodeProcessRequest(wire)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	response := func(r processRequest, err error, aux ...[]byte) {
		t.Helper()
		out := processResponse{incarnation: p.incarnation, sequence: r.sequence}
		if len(aux) == 1 {
			out.aux = aux[0]
		}
		body, encodeErr := encodeProcessResponse(out)
		if encodeErr != nil {
			t.Fatal(encodeErr)
		}
		mask := graphErrorMask(err)
		if err := writeProcessFrame(responseWriter, append([]byte{'G', 'P', 'R', '1', byte(mask >> 8), byte(mask)}, body...)); err != nil {
			t.Fatal(err)
		}
	}
	return m, request, response
}

func TestGraphSourceProcessRelayLossDeliversErrorWithoutCapturedReply(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for _, loss := range []bool{true, false} {
		t.Run(map[bool]string{true: "lost-reply", false: "native-refusal"}[loss], func(t *testing.T) {
			origin, receive, _ := processCancelWireFixture(t)
			target, request, reply := processCancelWireFixture(t)
			origin.id = 2
			origin.hub.peers = map[uint64]*graphProcessMux{4: target}
			origin.relayQueue = make(chan graphProcessRelayJob, 1)
			// Inert canonical GSR bytes test the two transport layers. A native
			// refusal remains a received frame, not a lost transport reply.
			n := newCertifiedGraphNetwork(t)
			peer := graphPeerIdentity{scope: n.hosts[1][0].machine.scope(), voter: 4}
			response := graphWireResponse{peer: peer, issuer: [16]byte{1}, kind: sourceCrossRead, sequence: 7, nonce: [16]byte{2}, body: []byte("captured native reply")}
			if !loss {
				response.body, response.err = nil, errInvalid
			}
			wire, err := encodeGraphWireResponse(response)
			if err != nil {
				t.Fatal(err)
			}
			var captured []byte
			origin.hub.afterRelay = func(ctx context.Context, actor, partition uint64, sent, received []byte) error {
				if actor != 2 || partition != 8 || !bytes.Equal(sent, []byte("original question")) {
					return errCorrupt
				}
				captured = bytes.Clone(received)
				if loss {
					return graphstore.ErrSourceUnavailable
				}
				return nil
			}
			done := make(chan struct{})
			go func() { defer close(done); origin.relayLoop() }()
			t.Cleanup(func() { origin.hub.cancel(); <-done })
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			origin.relayQueue <- graphProcessRelayJob{envelope: graphProcessEnvelope{sequence: 19, target: 8, body: []byte("original question")}, ctx: ctx, cancel: cancel}
			requested := request()
			if requested.op != processSourceServe || requested.entity != 2 || !bytes.Equal(requested.data, []byte("original question")) {
				t.Fatal("actual mux changed source question", requested)
			}
			reply(requested, nil, wire)
			forwarded := receive()
			if forwarded.op != processSourceReply || forwarded.index != 19 || !bytes.Equal(captured, wire) {
				t.Fatal("actual mux lost reply correlation/captured evidence", forwarded)
			}
			pending := make(chan graphProcessPumpReply, 1)
			child := &graphProcessChildRelay{pending: map[uint64]chan graphProcessPumpReply{19: pending}}
			if err := child.deliver(forwarded); err != nil {
				t.Fatal("actual relay produced undeliverable child grammar", err)
			}
			got := <-pending
			if loss {
				if !errors.Is(got.err, graphstore.ErrSourceUnavailable) || graphErrorMask(got.err) != graphErrorMask(graphstore.ErrSourceUnavailable) || len(got.body) != 0 {
					t.Fatal("lost reply smuggled native authority or changed refusal", got.err)
				}
			} else {
				decoded, err := decodeGraphWireResponse(got.body)
				if got.err != nil || forwarded.entity != 0 || !bytes.Equal(got.body, wire) || err != nil || !errors.Is(decoded.err, errInvalid) {
					t.Fatal("received native refusal became transport loss", got.err, err)
				}
			}
		})
	}
}

func TestGraphSourceProcessRelayMalformedErrorCannotConsumePending(t *testing.T) {
	for _, request := range []processRequest{
		{index: 7, entity: uint64(graphErrorMask(graphstore.ErrSourceUnavailable)), data: []byte("forged success")},
		{index: 7, entity: uint64(1) << len(graphWireCauses)},
	} {
		pending := make(chan graphProcessPumpReply, 1)
		child := &graphProcessChildRelay{pending: map[uint64]chan graphProcessPumpReply{7: pending}}
		if err := child.deliver(request); !errors.Is(err, errCorrupt) || len(pending) != 0 {
			t.Fatal("malformed error consumed pending source request", err)
		}
		if err := child.deliver(processRequest{index: 8}); err != nil || len(pending) != 0 {
			t.Fatal("foreign reply substituted for original request", err)
		}
	}
}

func TestGraphSourceProcessCanceledExchangeRetainsOriginalUntilTerminal(t *testing.T) {
	m, receive, reply := processCancelWireFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := m.exchange(ctx, processRequest{op: processSourceObserve, entity: 1}); done <- err }()
	original := receive()
	cancel()
	cancellation := receive()
	if cancellation.op != processSourceCancel || cancellation.index != original.sequence {
		t.Fatal("cancel did not name original operation", cancellation)
	}
	// The original frame was sent before cancellation. Once its cancel frame is
	// observed, the root implementation has already chosen whether to retain it.
	m.mu.Lock()
	_, held := m.pending[original.sequence]
	m.mu.Unlock()
	if !held {
		t.Fatal("cancel-only return retired correlation before child worker terminal; next Observe can occupy cleanup capacity")
	}
	reply(original, context.Canceled)
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal("terminal cancellation lost original cause", err)
	}
	m.mu.Lock()
	left := len(m.pending)
	m.mu.Unlock()
	if left != 0 {
		t.Fatal("terminal cancellation retained correlation", left)
	}
	// A duplicate late terminal is inert; it must not poison the next genuine
	// operation or be used as completion of that operation.
	reply(original, context.Canceled)
	next := make(chan error, 1)
	go func() { _, err := m.exchange(t.Context(), processRequest{op: processStateOp}); next <- err }()
	current := receive()
	reply(current, nil)
	if err := <-next; err != nil {
		t.Fatal("late terminal poisoned next request", err)
	}
}

func TestGraphSourceProcessCanceledExchangePeerDeathCannotBeTerminalSuccess(t *testing.T) {
	m, receive, _ := processCancelWireFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := m.exchange(ctx, processRequest{op: processSourceObserve, entity: 1}); done <- err }()
	original := receive()
	cancel()
	cancellation := receive()
	if cancellation.index != original.sequence {
		t.Fatal("wrong cancellation identity")
	}
	m.fail(replica.ErrStopped)
	if err := <-done; !errors.Is(err, context.Canceled) || !errors.Is(err, replica.ErrStopped) {
		t.Fatal("peer death was mistaken for completed cancellation", err)
	}
}

func TestGraphSourceProcessCanceledExchangeDeadlineRetiresUnconfirmedOwner(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	m, receive, _ := processCancelWireFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := m.exchange(ctx, processRequest{op: processSourceObserve, entity: 1}); done <- err }()
	original := receive()
	cancel()
	cancellation := receive()
	if cancellation.index != original.sequence {
		t.Fatal("wrong cancellation identity")
	}
	// No terminal response arrives: the real three-second cleanup deadline must
	// refuse rather than silently treating the operation as completed/idle.
	if err := <-done; !errors.Is(err, context.Canceled) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("cleanup deadline was terminal success", err)
	}
	m.mu.Lock()
	pending, failed := len(m.pending), m.err
	m.mu.Unlock()
	if pending != 0 || !errors.Is(failed, context.DeadlineExceeded) {
		t.Fatal("cleanup deadline left a live owner", pending, failed)
	}
	if _, err := m.exchange(t.Context(), processRequest{op: processStateOp}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("unconfirmed connection was reused as healthy", err)
	}
}

func TestGraphSourceProcessQueuedCancellationRepliesOnceToOriginalPausedRequest(t *testing.T) {
	for _, tc := range []struct {
		name string
		op   byte
	}{{"data", processSourceServe}, {"barrier", processSourcePausedBarrier}} {
		t.Run(tc.name, func(t *testing.T) {
			original := processRequest{op: tc.op, incarnation: [16]byte{1}, sequence: 7, entity: 1}
			other := processRequest{op: processSourceServe, sequence: 8}
			paused := []processRequest{other}
			var barrier *processRequest
			if tc.op == processSourceServe {
				paused = append(paused, original)
			} else {
				barrier = new(original)
			}
			count := 0
			respond := func(r processRequest, out processResponse, err error) error {
				count++
				if r.sequence != original.sequence || r.incarnation != original.incarnation || r.op != original.op || !errors.Is(err, context.Canceled) || len(out.aux)+len(out.packets) != 0 {
					t.Fatal("queued cancel changed original terminal identity/output", r, out, err)
				}
				return nil
			}
			for range 2 {
				if err := cancelGraphProcessQueued(original.sequence, &paused, &barrier, respond); err != nil {
					t.Fatal(err)
				}
			}
			if count != 1 || barrier != nil || len(paused) != 1 || paused[0].sequence != other.sequence {
				t.Fatal("queued cancel did not release exactly its owner", count, paused, barrier)
			}
		})
	}
}
