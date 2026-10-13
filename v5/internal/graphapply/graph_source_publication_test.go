package graphapply

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

// All capture authority comes from native committed state and actual completed
// ReadIndex. The latch changes scheduling only, never proof or captured bytes.
func p09PublicationFixture(t *testing.T, kind string) (*graphSourceServer, func(context.Context) (bool, error)) {
	t.Helper()
	if kind == "certified" {
		_, server, initial := certifiedSourceInfoFixture(t)
		if err := server.closeSource(initial.cookie); err != nil {
			t.Fatal(err)
		}
		return server, func(ctx context.Context) (bool, error) {
			info, err := server.openCertified(ctx, initial.certificate, [16]byte{72})
			return reflect.ValueOf(info).IsZero(), err
		}
	}
	if kind == "move" {
		n, _, intent, wires, _ := rangeMoveCoordinatorFixture(t, 32)
		wire, err := encodeRangeMoveIntent(intent)
		if err != nil {
			t.Fatal(err)
		}
		status := rangeMoveAdmissionSubmit(t, n, 0, wire, rangeMoveFence, 0, nil, nil)
		if status.phase != rangeMoveFence || status.result != rangeMoveApplied {
			t.Fatal("actual source fence prerequisite", status)
		}
		status = rangeMoveAdmissionSubmit(t, n, 0, wire, rangeMoveCapture, 0, nil, nil)
		if status.phase != rangeMoveCapture || status.result != rangeMoveApplied {
			t.Fatal("actual source capture prerequisite", status)
		}
		h := n.hosts[0][0]
		pump := nativeGraphTestPump{n: n, group: 0, host: h}
		question := rangeMoveQuery{move: status.move, nonce: [16]byte{73}, sourceRows: 512, sourceBytes: 4 << 20, outputBytes: 8 << 20}
		reply, err := pump.Move(t.Context(), question)
		if err != nil || reply.err != nil || reply.proof.owner != h || reply.proof.observation.status.phase != rangeMoveCapture {
			t.Fatal("actual completed source move ReadIndex", err, reply.err)
		}
		server := wires[0].service.sources
		t.Cleanup(func() {
			if err := server.close(); err != nil {
				t.Error(err)
			}
		})
		return server, func(ctx context.Context) (bool, error) {
			info, err := server.openMove(ctx, reply.proof)
			return reflect.ValueOf(info).IsZero(), err
		}
	}
	n := newCertifiedGraphNetwork(t)
	crossEdgeFixture(t, n)
	h := n.hosts[0][0]
	server, err := newGraphSourceServer(t.Context(), h, defaultGraphSourceLimits(h.machine.limits))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := server.close(); err != nil {
			t.Error(err)
		}
	})
	proof := n.read(0, allocationQuery{kind: observeConfiguration})
	return server, func(ctx context.Context) (bool, error) {
		info, err := server.open(ctx, proof)
		return reflect.ValueOf(info).IsZero(), err
	}
}

func TestGraphSourcePublicationCancelledAfterNativeCaptureReturnsNoCookie(t *testing.T) {
	for _, kind := range []string{"configuration", "certified", "move"} {
		cancellations := []string{"operation", "owner"}
		if kind == "configuration" {
			cancellations = append(cancellations, "closed-owner")
		}
		for _, cancellation := range cancellations {
			t.Run(kind+"/"+cancellation, func(t *testing.T) {
				server, open := p09PublicationFixture(t, kind)
				store := server.host.machine.store
				before, err := store.ApplicationUsage()
				if err != nil {
					t.Fatal(err)
				}
				entered := make(chan *graphSourceLease, 1)
				release := make(chan struct{})
				unblock := sync.OnceFunc(func() { close(release) })
				defer unblock()
				server.beforePublication = func(lease *graphSourceLease) {
					entered <- lease
					select {
					case <-release:
					case <-t.Context().Done():
					}
				}
				ctx, cancel := context.WithCancelCause(t.Context())
				defer cancel(nil)
				operationCause := errors.New("captured P09 operation cancelled")
				type result struct {
					zero bool
					err  error
				}
				done := make(chan result, 1)
				go func() {
					zero, err := open(ctx)
					done <- result{zero, err}
				}()
				deadline, stop := context.WithTimeout(t.Context(), 3*time.Second)
				defer stop()
				var captured *graphSourceLease
				select {
				case captured = <-entered:
				case outcome := <-done:
					t.Fatal("creation failed before actual native capture latch", outcome.err)
				case <-deadline.Done():
					t.Fatal("native capture never reached final publication boundary")
				}
				root, err := captured.view.RootBounded(t.Context(), 172)
				if err != nil || root.Generation == 0 || root.Index == 0 || len(root.Image) != 172 {
					t.Fatal("latch did not retain actual native captured root", root, err)
				}
				var closed chan error
				switch cancellation {
				case "operation":
					cancel(operationCause)
				case "owner":
					server.cancel()
				default:
					closed = make(chan error, 1)
					go func() { closed <- server.close() }()
					select {
					case <-server.ctx.Done():
					case <-deadline.Done():
						t.Fatal("native source close did not fence captured creation")
					}
				}
				unblock()
				var outcome result
				select {
				case outcome = <-done:
				case <-deadline.Done():
					t.Fatal("cancelled captured creation did not terminate")
				}
				if !outcome.zero || !errors.Is(outcome.err, context.Canceled) {
					t.Fatalf("cancelled final publication returned metadata/cookie or lost cause: zero=%v err=%v", outcome.zero, outcome.err)
				}
				if cancellation == "operation" && !errors.Is(outcome.err, operationCause) {
					t.Fatal("final publication lost original operation cancellation cause", outcome.err)
				}
				if closed != nil {
					if !errors.Is(outcome.err, replica.ErrStopped) {
						t.Fatal("final publication lost closed-owner refusal", outcome.err)
					}
					select {
					case err := <-closed:
						if err != nil {
							t.Fatal("native close failed after capture cleanup", err)
						}
					case <-deadline.Done():
						t.Fatal("native close waited indefinitely for failed captured creation")
					}
				}
				if root, err := captured.view.RootBounded(t.Context(), 172); root.Image != nil || !errors.Is(err, raftlog.ErrClosed) {
					t.Fatal("cancelled creation retained its native root owner", err)
				}
				if captured.moveMeta != nil {
					if _, err := captured.moveMeta.Root(); !errors.Is(err, raftlog.ErrClosed) {
						t.Fatal("cancelled creation retained manifest metadata owner", err)
					}
				}
				after, err := store.ApplicationUsage()
				if err != nil || after.Views != before.Views || after.ViewBytes != before.ViewBytes {
					t.Fatal("cancelled final publication leaked captured view accounting", before, after, err)
				}
				server.mu.Lock()
				pending := server.pending
				var live int
				for _, lease := range server.slots {
					if lease != nil && !lease.released.Load() {
						live++
					}
				}
				server.mu.Unlock()
				if pending != 0 || live != 0 {
					t.Fatal("cancelled creation published a live slot or retained pending admission", pending, live)
				}
			})
		}
	}
}
