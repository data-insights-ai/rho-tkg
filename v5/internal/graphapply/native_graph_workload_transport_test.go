package graphapply

import (
	"bytes"
	"context"
	"errors"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

// These compact direct guards are authored source, not a measured RTT result.
func TestNativeWorkloadTransportOwnsFiniteQueueAndCancelsHeldFrames(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	spec := nativeWorkloadTransportSpec{Kind: "injected-rtt", TargetRTTMillis: 1, Seed: 20261009}
	p, err := newNativeWorkloadTransport(ctx, spec, 32)
	if err != nil || p.oneWay() != 500*time.Microsecond || reflect.TypeFor[nativeWorkloadTransport]().Size() > nativeWorkloadTransportMetadata {
		t.Fatal("finite scheduler prepayment/half RTT", err)
	}
	owner, stop := context.WithCancel(t.Context())
	defer stop()
	h := &graphProcessHub{ctx: owner, cancel: stop, packets: make(chan graphProcessPacketJob, processPacketCount), idle: make(chan struct{}, 1)}
	if err := p.install(h); err != nil {
		t.Fatal(err)
	}
	if err := p.install(h); !errors.Is(err, errInvalid) {
		t.Fatal("mutable/repeated installation", err)
	}
	payload := make([]byte, 3)
	h.mu.Lock()
	err = p.enqueueLocked(h, 1, replica.Packet{From: 1, To: 2, Payload: payload})
	h.mu.Unlock()
	if err != nil || p.held != 1 || h.packetBytes != cap(payload)+64 || h.workloadBytes != nativeWorkloadTransportMetadata {
		t.Fatal("held payload/metadata not prepaid", err)
	}
	cancel()
	if _, err := p.next(h); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled held transport", err)
	}
	p.close(h)
	p.close(h)
	if h.packetBytes != 0 || h.workloadBytes != 0 || p.held != 0 || p.n != 0 || len(h.packets) != 0 {
		t.Fatal("canceled packet/ready queue leaked or repeated close double-refunded")
	}
	r := p.receipt(h)
	if r.Raft.Admitted != 1 || r.Raft.Canceled != 1 || r.Raft.Completed != 0 || r.Raft.Bytes != 3 || r.Raft.ConfiguredNanos != 500000 || r.SourceRequest.Admitted+r.SourceReply.Admitted != 0 {
		t.Fatal("retired canceled grant or fabricated response", r)
	}
	h.mu.Lock()
	err = p.enqueueLocked(h, 1, replica.Packet{From: 1, To: 2, Payload: payload})
	h.mu.Unlock()
	if !errors.Is(err, errInvalid) || p.receipt(h).Raft != r.Raft {
		t.Fatal("closed queue revived", err)
	}
}

func TestNativeWorkloadTransportRejectsUnfundedProfilesAndOrdersReadyFrames(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	base := nativeWorkloadTransportSpec{Kind: "local", Seed: 20261009}
	for _, spec := range []nativeWorkloadTransportSpec{
		{}, {Kind: "injected-rtt", TargetRTTMillis: 2, Seed: 1},
		{Kind: "local", TargetRTTMillis: 1, Seed: 1},
		{Kind: "injected-rtt", TargetRTTMillis: 100, JitterMillis: 1, Seed: 1},
	} {
		if _, err := newNativeWorkloadTransport(ctx, spec, 32); !errors.Is(err, errInvalid) {
			t.Fatal("unimplemented distribution admitted", spec, err)
		}
	}
	if _, err := newNativeWorkloadTransport(context.WithoutCancel(t.Context()), base, 32); !errors.Is(err, errInvalid) {
		t.Fatal("unbounded owner", err)
	}
	p, err := newNativeWorkloadTransport(ctx, base, 3)
	if err != nil {
		t.Fatal(err)
	}
	for _, sequence := range []int64{3, 1, 2} {
		if err := p.push(graphProcessPacketJob{enqueuedNanos: sequence}); err != nil {
			t.Fatal(err)
		}
	}
	for want := int64(1); want <= 3; want++ {
		if p.pop().enqueuedNanos != want {
			t.Fatal("ready queue serialized by arrival rather than due time")
		}
	}
	for range 3 {
		if err := p.admit(1, 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.admit(1, 0); !errors.Is(err, errLimit) {
		t.Fatal("invocation quota reset", err)
	}
	p.ledger[2].ObservedNanos = ^uint64(0)
	if err := p.finish(2, time.Now().Add(-time.Second), nil); !errors.Is(err, errLimit) {
		t.Fatal("observed aggregate overflow", err)
	}
	if err := p.admit(2, 0); !errors.Is(err, errLimit) {
		t.Fatal("overflow owner revived", err)
	}
	framed, err := newNativeWorkloadTransport(ctx, base, 3)
	if err != nil {
		t.Fatal(err)
	}
	writer := &nativeWorkloadShortWriter{}
	if err := framed.writeFrame(writer, []byte("123456789")); !errors.Is(err, io.ErrShortWrite) {
		t.Fatal("partial actual frame write", err)
	}
	if _, err := framed.readFrame(bytes.NewReader([]byte{0, 0})); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal("partial actual frame read", err)
	}
	if framed.pipes[0] != (nativeWorkloadPipeLedger{AttemptedFrames: 1, FailedFrames: 1, PrepaidBytes: 13, ActualBytes: 2}) || writer.actual.Len() != 2 || framed.pipes[1] != (nativeWorkloadPipeLedger{AttemptedFrames: 1, FailedFrames: 1, PrepaidBytes: processFrameBytes + 4, ActualBytes: 2}) {
		t.Fatal("failed IO/frame bytes omitted or guessed", framed.pipes)
	}
}

type nativeWorkloadShortWriter struct{ actual bytes.Buffer }

func (w *nativeWorkloadShortWriter) Write(b []byte) (int, error) {
	n, err := w.actual.Write(b[:min(2, len(b))])
	return n, errors.Join(io.ErrShortWrite, err)
}
