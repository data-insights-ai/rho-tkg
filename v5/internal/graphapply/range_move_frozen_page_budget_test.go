package graphapply

import (
	"errors"
	"io"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

func TestRangeMoveFrozenPageRefusesExhaustedRowsBeforeCapturedSource(t *testing.T) {
	n, _, intent, wires, transports := rangeMoveCoordinatorFixture(t, 64)
	// The exact target Import0 request is dropped before the real Host receives
	// it, after the source has durably completed its graph+replay manifest.
	gate := &rangeMoveAdmissionImportGate{graphPeerWireTransport: transports[8], sequence: 0}
	transports[8] = gate
	coordinator, err := newRangeMoveCoordinator(n.hosts[0][0], transports)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := runRangeMoveToCompletion(t, coordinator, intent)
	if !gate.stopped || !errors.Is(err, errRangeMovePending) || !errors.Is(err, io.EOF) || pending.committed {
		t.Fatal("actual move did not pause before target Import0", pending, err)
	}
	output, err := graphstate.NewOutputBudget(coordinator.limits.outputBytes)
	if err != nil {
		t.Fatal(err)
	}
	endpoints, err := coordinator.base.endpoints(newGraphRemoteBudget(coordinator.limits, output))
	if err != nil {
		t.Fatal(err)
	}
	source, err := endpoints[intent.source.partition].openMoveSource(t.Context(), pending.resume.move)
	if err != nil {
		t.Fatal("actual authenticated completed manifest source session", err)
	}
	defer func() {
		if err := source.Close(); err != nil {
			t.Error("native source cleanup", err)
		}
	}()
	server := wires[0].service.sources
	lease, err := server.lease(source.info.cookie)
	if err != nil {
		t.Fatal(err)
	}
	lease.mu.Lock()
	captured, metadata := lease.view, lease.moveMeta
	lease.mu.Unlock()
	if captured == nil || metadata == nil || captured == metadata {
		t.Fatal("not distinct actual C capture and completed manifest owners")
	}
	if err := captured.Close(); err != nil {
		t.Fatal(err)
	}
	// The manifest owner stays live. Its real progress/I/P reads consume3 rows;
	// the closed C view discriminates attempted export from pre-work refusal.
	if root, err := metadata.RootBounded(t.Context(), 172); err != nil || root.Index != source.move.manifestIndex {
		t.Fatal("fixture closed manifest owner instead of pinned C", err)
	}
	request := func(rows int) graphSourceRequest {
		q := rangeMoveQuery{move: pending.resume.move, nonce: [16]byte{131}, page: 1, sourceRows: rows, sourceBytes: 16 << 20, outputBytes: 16 << 20}
		command, err := encodeRangeMoveQuery(q)
		if err != nil {
			t.Fatal(err)
		}
		return graphSourceRequest{kind: sourceMoveRead, nonce: q.nonce, cookie: source.info.cookie, command: command}
	}
	// Positive remainder reaches the actual closed C owner, establishing the
	// discriminator without a fake provider, fabricated page or probe counter.
	if body, err := server.moveFrozenPage(t.Context(), request(64)); body != nil || !errors.Is(err, raftlog.ErrClosed) {
		t.Fatal("funded native exporter did not reach genuinely closed C", err)
	}
	body, err := server.moveFrozenPage(t.Context(), request(3))
	if body != nil || !errors.Is(err, errLimit) || errors.Is(err, raftlog.ErrClosed) {
		t.Fatal("exhausted3-row grant touched C after metadata instead of refusing before export", err)
	}
}

func TestRangeMoveDurableGraphExportRefusesZeroRemainderBeforeOpeningCapture(t *testing.T) {
	source, lease, pause := rangeMoveFrozenLifetimeSource(t, t.Context())
	defer close(pause.release)
	defer source.Close()
	native := pause.graphPeerWireTransport.(*rangeMoveCoordinatorWire).service.sources.host
	root, err := lease.moveMeta.RootBounded(t.Context(), 172)
	if err != nil {
		t.Fatal(err)
	}
	output, err := graphstate.NewOutputBudget(32 << 20)
	if err != nil {
		t.Fatal(err)
	}
	row, found, err := lease.moveMeta.Get(t.Context(), rangeMoveProgressKey(native.machine.ns, source.move.moveID), 1<<20)
	if err != nil || !found {
		t.Fatal(found, err)
	}
	progress, err := decodeRangeMoveProgress(row.Value, output)
	if err != nil {
		t.Fatal(err)
	}
	intentRecord, found, _, err := lease.moveMeta.GetControl(t.Context(), rangeMoveControlKey(native.machine.ns, source.move.moveID, 'I'), raftlog.ReadBudget{Rows: 2, Bytes: 4096})
	if err != nil || !found {
		t.Fatal(found, err)
	}
	intent, err := decodeRangeMoveIntent(intentRecord.Value)
	if err != nil {
		t.Fatal(err)
	}
	// Saturate actual native view ownership. A funded helper must expose the
	// native limit; a zero work remainder must refuse before trying to acquire it.
	var held []*raftlog.ApplicationView
	defer func() {
		for _, view := range held {
			if err := view.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	for range native.machine.store.ApplicationLimits().MaxViews + 1 {
		view, e := native.machine.store.ApplicationView(root.Index)
		if errors.Is(e, raftlog.ErrLimit) {
			break
		}
		if e != nil {
			t.Fatal(e)
		}
		held = append(held, view)
	}
	limits := native.machine.limits.allocation
	q := reader{ctx: t.Context(), view: lease.moveMeta, base: root, ns: native.machine.ns, limits: limits, arena: output}
	if _, err := native.machine.exportMoveGraphPage(&q, progress, graphstore.RangeMoveCursor{}, intent); !errors.Is(err, raftlog.ErrLimit) {
		t.Fatal("funded native helper did not reach genuine saturated view quota", err)
	}
	q.rows = q.limits.readRows
	_, err = native.machine.exportMoveGraphPage(&q, progress, graphstore.RangeMoveCursor{}, intent)
	if !errors.Is(err, errLimit) || errors.Is(err, raftlog.ErrLimit) {
		t.Fatal("zero durable-export work remainder acquired native capture before refusal", err)
	}
}
