package graphapply

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// Pause only a real cookie-bound frozen page AFTER the actual native service
// has produced its response. The release channel is solely a finite cleanup
// escape; ordinary success requires the RPC context to be canceled by ownership.
type rangeMoveFrozenLifetimeWire struct {
	graphPeerWireTransport
	entered chan struct{}
	release chan struct{}
}

func (w *rangeMoveFrozenLifetimeWire) RoundTrip(ctx context.Context, wire []byte) ([]byte, error) {
	r, err := decodeGraphSourceRequest(wire)
	if err != nil {
		return nil, err
	}
	response, err := w.graphPeerWireTransport.RoundTrip(ctx, wire)
	if err != nil {
		return nil, err
	}
	if r.kind == sourceMoveRead && r.cookie != (graphSourceCookie{}) {
		select {
		case w.entered <- struct{}{}:
		default:
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-w.release:
			return nil, context.Canceled
		}
	}
	return response, nil
}

func rangeMoveFrozenLifetimeSource(t *testing.T, owner context.Context) (*remoteGraphSource, *graphSourceLease, *rangeMoveFrozenLifetimeWire) {
	t.Helper()
	n, _, intent, wires, transports := rangeMoveCoordinatorFixture(t, 64)
	gate := &rangeMoveAdmissionImportGate{graphPeerWireTransport: transports[8], sequence: 0}
	transports[8] = gate
	coordinator, err := newRangeMoveCoordinator(n.hosts[0][0], transports)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := runRangeMoveToCompletion(t, coordinator, intent)
	if !gate.stopped || !errors.Is(err, errRangeMovePending) || !errors.Is(err, io.EOF) || pending.committed {
		t.Fatal("source manifest did not actually complete before target Import0", pending, err)
	}
	output, err := graphstate.NewOutputBudget(coordinator.limits.outputBytes)
	if err != nil {
		t.Fatal(err)
	}
	endpoints, err := coordinator.base.endpoints(newGraphRemoteBudget(coordinator.limits, output))
	if err != nil {
		t.Fatal(err)
	}
	endpoint := endpoints[intent.source.partition]
	source, err := endpoint.openMoveSource(owner, pending.resume.move)
	if err != nil {
		t.Fatal("actual authenticated frozen source", err)
	}
	t.Cleanup(func() {
		if err := source.Close(); err != nil {
			t.Error("final native source cleanup", err)
		}
	})
	lease, err := wires[0].service.sources.lease(source.info.cookie)
	if err != nil {
		t.Fatal(err)
	}
	pause := &rangeMoveFrozenLifetimeWire{graphPeerWireTransport: endpoint.transport, entered: make(chan struct{}, 1), release: make(chan struct{})}
	endpoint.transport = pause
	return source, lease, pause
}

func TestRangeMoveFrozenPageOwnershipInterruptsBackgroundCallerAndClosesBothViews(t *testing.T) {
	for _, mode := range []string{"Close", "owner-cancel"} {
		t.Run(mode, func(t *testing.T) {
			owner, cancelOwner := context.WithCancel(t.Context())
			defer cancelOwner()
			source, lease, pause := rangeMoveFrozenLifetimeSource(t, owner)
			lease.mu.Lock()
			captured, metadata := lease.view, lease.moveMeta
			lease.mu.Unlock()
			if captured == nil || metadata == nil || captured == metadata {
				t.Fatal("not distinct genuine C and manifest/P owners")
			}
			type pageResult struct {
				page []byte
				err  error
			}
			pageDone := make(chan pageResult, 1)
			closeDone := make(chan error, 1)
			pageReceived, closeStarted, closeReceived := false, false, false
			// Always release the transport and drain both goroutines before native
			// Host cleanup, including the intended3-second Close-deadline RED.
			defer func() {
				close(pause.release)
				cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if !pageReceived {
					select {
					case <-pageDone:
					case <-cleanup.Done():
						t.Error("cleanup did not drain frozen page goroutine")
					}
				}
				if closeStarted && !closeReceived {
					select {
					case <-closeDone:
					case <-cleanup.Done():
						t.Error("cleanup did not drain Close goroutine")
					}
				}
				if err := source.Close(); err != nil {
					t.Error("native frozen source cleanup", err)
				}
			}()
			outer, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			// An independently live caller cannot be what cancels this RPC.
			go func() { page, err := source.moveFrozenPage(context.Background(), 1); pageDone <- pageResult{page, err} }()
			select {
			case <-pause.entered:
			case result := <-pageDone:
				pageReceived = true
				t.Fatal("fixture failed before genuine native frozen response pause", result.err)
			case <-outer.Done():
				t.Fatal("actual native frozen response did not reach transport pause")
			}
			if mode == "owner-cancel" {
				cancelOwner()
				// Observe cancellation BEFORE Close, which would independently
				// cancel s.ctx and otherwise hide missing owner propagation.
				select {
				case result := <-pageDone:
					pageReceived = true
					if result.page != nil || !errors.Is(result.err, context.Canceled) && !errors.Is(result.err, graphstore.ErrSourceUnavailable) {
						t.Fatal("owner cancellation alone returned page or lost cancellation sentinel", result.err)
					}
				case <-outer.Done():
					t.Fatal("owner cancellation alone did not interrupt background frozen page before Close")
				}
			}
			closeStarted = true
			go func() { closeDone <- source.Close() }()
			select {
			case err := <-closeDone:
				closeReceived = true
				if err != nil {
					t.Fatal("Close failed to interrupt pending background frozen page", err)
				}
			case <-outer.Done():
				t.Fatal("Close did not interrupt pending background frozen page within outer bound")
			}
			if !pageReceived {
				select {
				case result := <-pageDone:
					pageReceived = true
					if result.page != nil || !errors.Is(result.err, context.Canceled) && !errors.Is(result.err, graphstore.ErrSourceUnavailable) {
						t.Fatal("canceled frozen source returned page or lost ownership cancellation sentinel", result.err)
					}
				case <-outer.Done():
					t.Fatal("background frozen page survived Close/owner cancellation")
				}
			}
			if !lease.done() {
				t.Fatal("native source lease retained either owner after Close")
			}
			for _, view := range []*raftlog.ApplicationView{captured, metadata} {
				if root, err := view.RootBounded(t.Context(), 172); root.Image != nil || !errors.Is(err, raftlog.ErrClosed) {
					t.Fatal("Close left genuine C or manifest/P owner live", err)
				}
			}
		})
	}
}
