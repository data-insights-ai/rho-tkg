package graphapply

import (
	"bytes"
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

// The pause controls data access only. Owner, capture, proof, native view and
// returned data are genuine; this is not physical-I/O cancellation evidence.
type pausedGraphSourceData struct {
	graphstore.ApplicationReadSource
	entered chan struct{}
	release chan struct{}
}

func (p *pausedGraphSourceData) Get(ctx context.Context, key []byte, maximum int) (raftlog.KV, bool, error) {
	select {
	case p.entered <- struct{}{}:
	default:
	}
	select {
	case <-ctx.Done():
		return raftlog.KV{}, false, ctx.Err()
	case <-p.release:
		return p.ApplicationReadSource.Get(ctx, key, maximum)
	}
}

func sourceSessionFixture(t *testing.T) (*allocationTestNetwork, *graphSourceServer, graphSourceInfo, *graphSourceLease) {
	t.Helper()
	n := newCertifiedGraphNetwork(t)
	crossEdgeFixture(t, n)
	h := n.hosts[0][0]
	s, err := newGraphSourceServer(t.Context(), h, defaultGraphSourceLimits(h.machine.limits))
	if err != nil {
		t.Fatal(err)
	}
	info, err := s.open(t.Context(), n.read(0, allocationQuery{kind: observeConfiguration}))
	if err != nil {
		t.Fatal(err)
	}
	l, err := s.lease(info.cookie)
	if err != nil {
		t.Fatal(err)
	}
	return n, s, info, l
}

func TestGraphSourceExpiryCancelsPausedNativeData(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n, s, info, l := sourceSessionFixture(t)
	pause := &pausedGraphSourceData{ApplicationReadSource: l.view, entered: make(chan struct{}, 1), release: make(chan struct{})}
	l.mu.Lock()
	l.read = pause
	l.mu.Unlock()
	deadline, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	type result struct {
		row   raftlog.KV
		found bool
		err   error
	}
	done := make(chan result, 1)
	key := graphstore.RangePublicationKey(graphstore.Namespace{Graph: n.d.Graph(), Partition: 3}, 1)
	go func() {
		row, found, err := s.get(t.Context(), info.cookie, key, 64<<10)
		done <- result{row, found, err}
	}()
	defer func() {
		close(pause.release)
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("paused native source did not stop after release")
		}
	}()
	select {
	case <-pause.entered:
	case <-deadline.Done():
		t.Fatal("source never entered controlled data pause")
	}
	l.cancel() // The same owner context signal used by finite lease expiry.
	select {
	case got := <-done:
		// Put the terminal result back for independent deferred worker cleanup.
		done <- got
		if !errors.Is(got.err, graphstore.ErrSourceUnavailable) || got.found || len(got.row.Key)+len(got.row.Value) != 0 {
			t.Fatal("expired source returned output with live caller", got)
		}
	case <-deadline.Done():
		t.Fatal("owner expiry did not cancel in-flight source data")
	}
}

func TestGraphSourceHostCloseDetachesBeforeEventRefusal(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n, _, _, lease := sourceSessionFixture(t)
	h := n.hosts[0][0]
	lease.mu.Lock() // Controlled pending cleanup, independent of native disk IO.
	closeDone := make(chan error, 1)
	go func() { closeDone <- h.Close() }()
	defer func() {
		lease.mu.Unlock()
		select {
		case err := <-closeDone:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("Host close did not finish after cleanup release")
		}
	}()
	deadline, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	for !h.ended.Load() {
		select {
		case <-deadline.Done():
			t.Fatal("Host never entered close")
		default:
			runtime.Gosched()
		}
	}
	done := make(chan error, 1)
	go func() { _, err := h.Tick(); done <- err }()
	select {
	case err := <-done:
		if !errors.Is(err, replica.ErrStopped) {
			t.Fatal("closed Host event refusal", err)
		}
	case <-deadline.Done():
		t.Fatal("Host event mutex waited for native session cleanup")
	}
	if _, err := h.Read(allocationQuery{kind: observeConfiguration, nonce: [16]byte{99}}); !errors.Is(err, replica.ErrStopped) {
		t.Fatal("closed Host read admission", err)
	}
}

func TestGraphSourceDirectCloseCancelsPausedNativeData(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n, server, info, lease := sourceSessionFixture(t)
	pause := &pausedGraphSourceData{ApplicationReadSource: lease.view, entered: make(chan struct{}, 1), release: make(chan struct{})}
	lease.mu.Lock()
	lease.read = pause
	lease.mu.Unlock()
	defer close(pause.release)
	deadline, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	key := graphstore.RangePublicationKey(graphstore.Namespace{Graph: n.d.Graph(), Partition: 3}, 1)
	go func() {
		row, found, err := server.get(t.Context(), info.cookie, key, 64<<10)
		if found || len(row.Key)+len(row.Value) != 0 {
			done <- errors.New("closed source returned data")
			return
		}
		done <- err
	}()
	select {
	case <-pause.entered:
	case <-deadline.Done():
		t.Fatal("genuine native read not paused")
	}
	closed := make(chan error, 1)
	go func() { closed <- server.closeSource(info.cookie) }()
	select {
	case err := <-done:
		if !errors.Is(err, graphstore.ErrSourceUnavailable) {
			t.Fatal(err)
		}
	case <-deadline.Done():
		t.Fatal("direct source close waited for lease mutex before cancellation")
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-deadline.Done():
		t.Fatal("direct close did not release native owner")
	}
}

func TestGraphSourceSessionDirectAdmissionReplayAndLifetime(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCertifiedGraphNetwork(t)
	crossEdgeFixture(t, n)
	h := n.hosts[0][0]
	limits := defaultGraphSourceLimits(h.machine.limits)
	//nolint:staticcheck // Nil context refusal is the required invalid-input contract.
	if _, err := newGraphSourceServer(nil, h, limits); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	malformed := limits
	malformed.sessions = 0
	if _, err := newGraphSourceServer(t.Context(), h, malformed); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	server, err := newGraphSourceServer(t.Context(), h, limits)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := newGraphSourceServer(t.Context(), h, limits); !errors.Is(err, errInvalid) {
		t.Fatal("duplicate source owner", err)
	}
	proof := n.read(0, allocationQuery{kind: observeConfiguration})
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := server.open(cancelled, proof); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	info, err := server.open(t.Context(), proof)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := server.open(t.Context(), proof)
	if err != nil || replay.cookie != info.cookie || replay.root.ImageHash != info.root.ImageHash {
		t.Fatal("exact source replay", err)
	}
	clear(replay.root.Image)
	if !bytes.Equal(info.root.Image, server.slots[0].info.root.Image) {
		t.Fatal("replay image alias")
	}
	second, err := server.open(t.Context(), n.read(0, allocationQuery{kind: observeConfiguration}))
	if err != nil {
		t.Fatal(err)
	}
	third := n.read(0, allocationQuery{kind: observeConfiguration})
	if rejected, err := server.open(t.Context(), third); !errors.Is(err, errLimit) || rejected.cookie != (graphSourceCookie{}) {
		t.Fatal("third simultaneous owner admitted", err)
	}
	wrong := info.cookie
	wrong.secret[0] ^= 1
	if _, err := server.lease(wrong); !errors.Is(err, graphstore.ErrSourceUnavailable) {
		t.Fatal("unowned cookie", err)
	}
	if _, err := server.root(t.Context(), info.cookie, 171); !errors.Is(err, raftlog.ErrLimit) {
		t.Fatal("native metadata ceiling", err)
	}
	if _, _, err := server.get(t.Context(), info.cookie, nil, -1); !errors.Is(err, errLimit) {
		t.Fatal("invalid bounded get", err)
	}
	if _, err := server.scan(t.Context(), info.cookie, nil, nil, nil, raftlog.ReadBudget{}); !errors.Is(err, errLimit) {
		t.Fatal("zero scan allowance", err)
	}
	//nolint:staticcheck // Nil context refusal is the required invalid-input contract.
	if _, _, err := server.get(nil, info.cookie, nil, 0); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	//nolint:staticcheck // Nil context refusal is the required invalid-input contract.
	if _, err := server.scan(nil, info.cookie, nil, nil, nil, raftlog.ReadBudget{Rows: 1, Bytes: 1}); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if err := server.closeSource(info.cookie); err != nil {
		t.Fatal(err)
	}
	if rejected, err := server.open(t.Context(), proof); !errors.Is(err, graphstore.ErrSourceUnavailable) || rejected.cookie != (graphSourceCookie{}) {
		t.Fatal("expired read identity retargeted", err)
	}
	if err := server.closeSource(second.cookie); err != nil {
		t.Fatal(err)
	}
	if err := server.close(); err != nil {
		t.Fatal(err)
	}
	if err := server.close(); err != nil {
		t.Fatal("repeat native owner close", err)
	}
	if _, err := server.open(t.Context(), third); !errors.Is(err, replica.ErrStopped) {
		t.Fatal(err)
	}
	if _, err := server.lease(info.cookie); !errors.Is(err, replica.ErrStopped) {
		t.Fatal(err)
	}
	var nilServer *graphSourceServer
	if !errors.Is(nilServer.close(), errInvalid) {
		t.Fatal("nil source server close")
	}
	if _, err := nilServer.lease(info.cookie); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
}
