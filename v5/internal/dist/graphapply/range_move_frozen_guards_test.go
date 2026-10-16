package graphapply

import (
	"bytes"
	"context"
	"errors"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// This decodes only an actual native frozen-page reply for fault injection.
// No observation, completed ReadIndex, manifest or source authority is forged.
type rangeMoveFrozenGuardFrame struct {
	query                    rangeMoveQuery
	intent, image            [32]byte
	index                    uint64
	rows, nativeBytes, owned int
	kind                     byte
	data                     []byte
}

func rangeMoveFrozenGuardDecode(wire []byte) (rangeMoveFrozenGuardFrame, error) {
	body, err := graphBody(wire, "GMV\x01", graphSourceFrameBytes-512)
	if err != nil {
		return rangeMoveFrozenGuardFrame{}, err
	}
	c := graphCursor{b: body}
	query := c.field(160)
	f := rangeMoveFrozenGuardFrame{}
	copy(f.intent[:], c.take(32))
	f.index = c.u64()
	copy(f.image[:], c.take(32))
	f.rows, f.nativeBytes, f.owned = c.count(1<<20, 0), c.count(64<<20, 0), c.count(64<<20, 0)
	f.kind, f.data = c.tag(), c.field(1<<20)
	if c.err != nil || len(c.b) != 0 {
		return rangeMoveFrozenGuardFrame{}, errors.Join(errCorrupt, c.err)
	}
	f.query, err = decodeRangeMoveQuery(query)
	return f, err
}

func (f rangeMoveFrozenGuardFrame) encode() ([]byte, error) {
	query, err := encodeRangeMoveQuery(f.query)
	if err != nil {
		return nil, err
	}
	return boundedEncoding(graphSourceFrameBytes-512, func(w *boundedWriter) {
		w.add([]byte{'G', 'M', 'V', 1})
		w.field(query)
		w.add(f.intent[:])
		w.u64(f.index)
		w.add(f.image[:])
		w.u32(f.rows)
		w.u32(f.nativeBytes)
		w.u32(f.owned)
		w.tag(f.kind)
		w.field(f.data)
	})
}

// Corruption modifies only the bytes of a real successfully produced frozen
// response. Omission drops that same response after production. All other
// calls, including ownership cleanup, use the original native service.
type rangeMoveFrozenGuardWire struct {
	graphPeerWireTransport
	mutate     func(*rangeMoveFrozenGuardFrame)
	drop       bool
	changed    int
	beforeDrop func()
}

func (w *rangeMoveFrozenGuardWire) RoundTrip(ctx context.Context, wire []byte) ([]byte, error) {
	request, err := decodeGraphSourceRequest(wire)
	if err != nil {
		return nil, err
	}
	response, err := w.graphPeerWireTransport.RoundTrip(ctx, wire)
	if err != nil || request.kind != sourceMoveRead || request.cookie == (graphSourceCookie{}) {
		return response, err
	}
	parsed, err := decodeGraphWireResponse(response)
	if err != nil || parsed.err != nil {
		return response, err
	}
	if w.drop {
		w.drop = false
		w.changed++
		if w.beforeDrop != nil {
			w.beforeDrop()
		}
		return nil, io.EOF
	}
	if w.mutate == nil {
		return response, nil
	}
	frame, err := rangeMoveFrozenGuardDecode(parsed.body)
	if err != nil {
		return nil, err
	}
	w.mutate(&frame)
	w.changed++
	parsed.body, err = frame.encode()
	if err != nil {
		return nil, err
	}
	return encodeGraphWireResponse(parsed)
}

type rangeMoveFrozenGuardState struct {
	root                      raftlog.ApplicationRoot
	status, progress, capture []byte
	decision                  []byte
	decisionFound             bool
}

func rangeMoveFrozenGuardStateAt(t *testing.T, h *allocationHost, move [16]byte) rangeMoveFrozenGuardState {
	t.Helper()
	view, err := h.machine.store.ApplicationView(h.driver.Applied())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := view.Close(); err != nil {
			t.Error(err)
		}
	}()
	state := rangeMoveFrozenGuardState{}
	state.root, err = view.RootBounded(t.Context(), 172)
	if err != nil {
		t.Fatal(err)
	}
	for i, key := range [][]byte{rangeMoveStatusKey(h.machine.ns, move), rangeMoveProgressKey(h.machine.ns, move)} {
		row, found, err := view.Get(t.Context(), key, min(1<<20, view.ReadLimits().Bytes))
		if err != nil || !found || row.Deleted {
			t.Fatal("actual frozen transition state missing", i, found, err)
		}
		if i == 0 {
			state.status = row.Value
		} else {
			state.progress = row.Value
		}
	}
	capture, found, _, err := view.GetControl(t.Context(), rangeMoveControlKey(h.machine.ns, move, 'C'), raftlog.ReadBudget{Rows: 2, Bytes: 4096})
	if err != nil || !found {
		t.Fatal("actual C capture missing", found, err)
	}
	state.capture = capture.Value
	decision, found, _, err := view.GetControl(t.Context(), rangeMoveControlKey(h.machine.ns, move, 'D'), raftlog.ReadBudget{Rows: 2, Bytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	state.decision, state.decisionFound = decision.Value, found
	return state
}

func rangeMoveFrozenGuardOpen(t *testing.T, c *rangeMoveCoordinator, move [16]byte) *remoteGraphSource {
	t.Helper()
	// Opening another session is a separately admitted bounded operation. The
	// native source server still enforces its original ownership/work quotas.
	output, err := graphstate.NewOutputBudget(c.limits.outputBytes)
	if err != nil {
		t.Fatal(err)
	}
	endpoints, err := c.base.endpoints(newGraphRemoteBudget(c.limits, output))
	if err != nil {
		t.Fatal(err)
	}
	source, err := endpoints[c.base.host.machine.ns.partition].openMoveSource(t.Context(), move)
	if err != nil {
		t.Fatal("actual completed manifest source open", err)
	}
	return source
}

func TestRangeMoveFrozenPageRejectsChangedCaptureManifestAndExpectedPageBindings(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n, _, intent, wires, transports := rangeMoveCoordinatorFixture(t, 64)
	gate := &rangeMoveAdmissionImportGate{graphPeerWireTransport: transports[8], sequence: 0}
	transports[8] = gate
	coordinator, err := newRangeMoveCoordinator(n.hosts[0][0], transports)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := runRangeMoveToCompletion(t, coordinator, intent)
	if !gate.stopped || !errors.Is(err, errRangeMovePending) || !errors.Is(err, io.EOF) || pending.committed {
		t.Fatal("native manifest did not finish before target Import0", pending, err)
	}
	var before [2][3]rangeMoveFrozenGuardState
	for group := range 2 {
		for member, h := range n.hosts[group] {
			before[group][member] = rangeMoveFrozenGuardStateAt(t, h, pending.resume.move)
			if before[group][member].decisionFound {
				t.Fatal("binding guard started after handoff COMMIT")
			}
		}
	}
	captures := wires[0].captures + wires[1].captures
	for _, tc := range []struct {
		name string
		edit func(*rangeMoveFrozenGuardFrame)
	}{
		{"wrong-move", func(f *rangeMoveFrozenGuardFrame) { f.query.move[0] ^= 1 }},
		{"changed-intent", func(f *rangeMoveFrozenGuardFrame) { f.intent[0] ^= 1 }},
		{"changed-manifest-index", func(f *rangeMoveFrozenGuardFrame) { f.index++ }},
		{"changed-manifest-image", func(f *rangeMoveFrozenGuardFrame) { f.image[0] ^= 1 }},
		{"different-expected-page", func(f *rangeMoveFrozenGuardFrame) { f.query.page++ }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := rangeMoveFrozenGuardOpen(t, coordinator, pending.resume.move)
			originalWire := source.endpoint.transport
			defer func() {
				source.endpoint.transport = originalWire
				if err := source.Close(); err != nil {
					t.Error(err)
				}
			}()
			lease, err := wires[0].service.sources.lease(source.info.cookie)
			if err != nil {
				t.Fatal(err)
			}
			lease.mu.Lock()
			captured, metadata := lease.view, lease.moveMeta
			lease.mu.Unlock()
			if captured == nil || metadata == nil || captured == metadata || source.move.root.Index >= source.move.manifestIndex {
				t.Fatal("fixture did not pin distinct actual C and later manifest/P coordinates")
			}
			// The successful counterpart comes from this exact cookie and actual P
			// cursor. It proves refusal is a binding guard, not an absent fixture.
			page, err := source.moveFrozenPage(t.Context(), 1)
			if err != nil || len(page) < 2 || page[0] != 1 {
				t.Fatal("genuine frozen graph page counterpart", err)
			}
			output, err := graphstate.NewOutputBudget(1 << 20)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := graphstore.DecodeRangeMovePage(page[1:], output)
			if err != nil {
				t.Fatal(err)
			}
			receipt, found, _, err := metadata.GetControl(t.Context(), rangeMovePageControlKey(n.hosts[0][0].machine.ns, pending.resume.move, 1), raftlog.ReadBudget{Rows: 2, Bytes: 256 << 10})
			if err != nil || !found || len(receipt.Value) < 2 || receipt.Value[0] != 1 || receipt.Index <= source.move.root.Index {
				t.Fatal("page did not have an actual post-C P receipt", found, err)
			}
			cursor, err := graphstore.DecodeRangeMoveCursor(receipt.Value[1:], output)
			if err != nil || !reflect.DeepEqual(cursor, decoded.Before) {
				t.Fatal("frozen page did not use the exact later P cursor", err)
			}
			fault := &rangeMoveFrozenGuardWire{graphPeerWireTransport: originalWire, mutate: tc.edit}
			source.endpoint.transport = fault
			if page, err := source.moveFrozenPage(t.Context(), 1); page != nil || !errors.Is(err, errInvalid) || fault.changed != 1 {
				t.Fatal("altered real frozen response borrowed C/manifest/page binding", tc.name, fault.changed, err)
			}
		})
	}
	for group := range 2 {
		for member, h := range n.hosts[group] {
			if after := rangeMoveFrozenGuardStateAt(t, h, pending.resume.move); !reflect.DeepEqual(after, before[group][member]) {
				t.Fatal("refused frozen reply changed native cursor, root, capture or COMMIT", group, member)
			}
		}
	}
	transports[8] = gate.graphPeerWireTransport
	// The coordinator's existing endpoint observes the same gate object.
	gate.sequence = 1024
	done := rangeMoveKeeperResume(t, coordinator, pending.resume)
	if !done.committed || wires[0].captures+wires[1].captures != captures {
		t.Fatal("healthy original Resume recaptured or failed after binding refusals", done)
	}
}

func TestRangeMoveOmittedActualFrozenReplyCannotAdvanceOrActivateAndResumeKeepsCapture(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n, _, intent, wires, transports := rangeMoveCoordinatorFixture(t, 64)
	var atDrop [2][3]rangeMoveFrozenGuardState
	fault := &rangeMoveFrozenGuardWire{graphPeerWireTransport: transports[3], drop: true}
	fault.beforeDrop = func() {
		move := rangeMoveID(n.hosts[0][0].machine.ns, intent.id)
		for group := range 2 {
			for member, h := range n.hosts[group] {
				atDrop[group][member] = rangeMoveFrozenGuardStateAt(t, h, move)
			}
		}
	}
	transports[3] = fault
	coordinator, err := newRangeMoveCoordinator(n.hosts[0][0], transports)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := runRangeMoveToCompletion(t, coordinator, intent)
	if !errors.Is(err, errRangeMovePending) || !errors.Is(err, io.EOF) || pending.committed || fault.changed != 1 {
		t.Fatal("omitted actual frozen reply completed instead of retaining pending", pending, fault.changed, err)
	}
	for group := range 2 {
		for member, h := range n.hosts[group] {
			after := rangeMoveFrozenGuardStateAt(t, h, pending.resume.move)
			if !reflect.DeepEqual(after, atDrop[group][member]) || after.decisionFound {
				t.Fatal("omitted page advanced native cursor/root or handoff COMMIT", group, member)
			}
		}
	}
	output, err := graphstate.NewOutputBudget(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	progress, err := decodeRangeMoveProgress(atDrop[1][0].progress, output)
	if err != nil || progress.imported == nil || !progress.imported.PreparationComplete || progress.imported.Sequence != 0 || progress.imported.Complete || progress.imported.Sealed {
		t.Fatal("omission did not follow genuine preparation before any logical page", progress.imported, err)
	}
	captures := wires[0].captures + wires[1].captures
	done := rangeMoveKeeperResume(t, coordinator, pending.resume)
	if !done.committed || fault.changed != 1 || wires[0].captures+wires[1].captures != captures {
		t.Fatal("restored actual source reply failed to resume original capture", done)
	}
	for group := range 2 {
		for member, h := range n.hosts[group] {
			if after := rangeMoveFrozenGuardStateAt(t, h, done.resume.move); !bytes.Equal(after.capture, atDrop[group][member].capture) {
				t.Fatal("network-loss Resume replaced immutable actual C capture", group, member)
			}
		}
	}
}

func rangeMoveFrozenGuardTTLFixture(t *testing.T) (*allocationTestNetwork, *rangeMoveCoordinator, rangeMoveIntent, []*rangeMoveCoordinatorWire, map[uint64]graphPeerWireTransport) {
	t.Helper()
	// The same bounded native six-voter profile as rangeMoveCoordinatorFixture;
	// only the source server's constructor-admitted session lifetime differs.
	n := newCrossPartitionNetworkAgreement(t, func(p *raftlog.ApplicationPolicy) {
		p.RetainedApplicationBytes = 64 << 20
		p.RetainedApplicationRecords = 32768
	}, rangeMoveSemanticContractID())
	_, _ = crossEdgeFixture(t, n)
	transports := make(map[uint64]graphPeerWireTransport)
	wires := make([]*rangeMoveCoordinatorWire, 2)
	for group, partition := range []uint64{3, 8} {
		h := n.hosts[group][0]
		limits := defaultGraphSourceLimits(h.machine.limits)
		if group == 0 {
			limits.lifetime = 2 * time.Second
		}
		server, err := newGraphSourceServer(t.Context(), h, limits)
		if err != nil {
			t.Fatal(err)
		}
		service, err := newGraphPeerService(h, server, &nativeGraphTestPump{n: n, group: group, host: h})
		if err != nil {
			t.Fatal(err)
		}
		wires[group] = &rangeMoveCoordinatorWire{nativeGraphTestTransport: &nativeGraphTestTransport{service: service}}
		transports[partition] = wires[group]
	}
	h := n.hosts[0][0]
	coordinator, err := newRangeMoveCoordinator(h, transports)
	if err != nil {
		t.Fatal(err)
	}
	intent, err := h.PrepareRangeMove(t.Context(), n.requestID(), 1, 8, 64, 64, 8<<10)
	if err != nil {
		t.Fatal(err)
	}
	return n, coordinator, intent, wires, transports
}

func TestRangeMoveFrozenNativeTTLClosesCaptureAndManifestWithoutRecapture(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n, _, intent, wires, transports := rangeMoveFrozenGuardTTLFixture(t)
	gate := &rangeMoveAdmissionImportGate{graphPeerWireTransport: transports[8], sequence: 0}
	transports[8] = gate
	coordinator, err := newRangeMoveCoordinator(n.hosts[0][0], transports)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := runRangeMoveToCompletion(t, coordinator, intent)
	if !gate.stopped || !errors.Is(err, errRangeMovePending) || !errors.Is(err, io.EOF) {
		t.Fatal("TTL guard requires actual completed manifest", pending, err)
	}
	source := rangeMoveFrozenGuardOpen(t, coordinator, pending.resume.move)
	defer func() {
		if err := source.Close(); err != nil {
			t.Error(err)
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
	if source.move.lifetime != 2*time.Second || captured == nil || metadata == nil || captured == metadata {
		t.Fatal("TTL fixture did not own the two actual native views")
	}
	// Prove this genuine session was usable before its real constructor deadline.
	if page, err := source.moveFrozenPage(t.Context(), 1); len(page) < 2 || err != nil {
		t.Fatal("actual pre-expiry frozen page", err)
	}
	server.mu.Lock()
	next := server.next
	server.mu.Unlock()
	calls, captures := wires[0].calls, wires[0].captures+wires[1].captures
	outer, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	select {
	case <-lease.ctx.Done():
		if !errors.Is(lease.ctx.Err(), context.DeadlineExceeded) {
			t.Fatal("native TTL ended by cancellation instead of its real deadline", lease.ctx.Err())
		}
	case <-outer.Done():
		t.Fatal("native configured session deadline did not fire")
	}
	tick := time.Tick(10 * time.Millisecond)
	for !lease.done() {
		select {
		case <-tick:
		case <-outer.Done():
			t.Fatal("expiry did not automatically release both native view owners")
		}
	}
	for _, view := range []*raftlog.ApplicationView{captured, metadata} {
		if root, err := view.RootBounded(t.Context(), 172); root.Image != nil || !errors.Is(err, raftlog.ErrClosed) {
			t.Fatal("expired source retained C or manifest/P view", err)
		}
	}
	if page, err := source.moveFrozenPage(t.Context(), 1); page != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("expired cookie reopened or returned frozen page", err)
	}
	server.mu.Lock()
	after := server.next
	server.mu.Unlock()
	if after != next || wires[0].calls != calls || wires[0].captures+wires[1].captures != captures {
		t.Fatal("expired session performed another RPC, open or native capture")
	}
}
