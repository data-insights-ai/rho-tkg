package graphapply

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// These envelopes are embedding events, not graph authority. The parent binds
// each origin to the actual configured child pipe; graph bodies remain opaque.
const (
	processSourcePackets byte = iota + 1
	processSourceRelay
	processSourceRelayCancel
	processSourcePending = 16
	processSourceWorkers = 2
)

type graphProcessEnvelope struct {
	kind             byte
	incarnation      [16]byte
	sequence, target uint64
	body             []byte
}

func encodeGraphProcessEnvelope(e graphProcessEnvelope) ([]byte, error) {
	if e.kind < processSourcePackets || e.kind > processSourceRelayCancel || e.sequence == 0 || e.incarnation == ([16]byte{}) {
		return nil, errInvalid
	}
	return processEncoding(func(w *boundedWriter) {
		w.add([]byte("GPE1"))
		w.tag(e.kind)
		w.add(e.incarnation[:])
		w.u64(e.sequence)
		w.u64(e.target)
		w.field(e.body)
	})
}
func decodeGraphProcessEnvelope(wire []byte) (graphProcessEnvelope, error) {
	c := graphCursor{b: wire}
	if !bytes.Equal(c.take(4), []byte("GPE1")) {
		return graphProcessEnvelope{}, errCorrupt
	}
	e := graphProcessEnvelope{kind: c.tag(), incarnation: c.array(), sequence: c.u64(), target: c.u64(), body: c.field(processFrameBytes - 64)}
	if c.err != nil || len(c.b) != 0 || e.kind < processSourcePackets || e.kind > processSourceRelayCancel || e.sequence == 0 || e.incarnation == ([16]byte{}) {
		return graphProcessEnvelope{}, errors.Join(errCorrupt, c.err)
	}
	return e, nil
}

type graphProcessAnswer struct {
	response processResponse
	err      error
}
type graphProcessPacketJob struct {
	origin uint64
	packet replica.Packet
}
type graphProcessRelayJob struct {
	envelope graphProcessEnvelope
	ctx      context.Context
	cancel   context.CancelFunc
}
type graphProcessHub struct {
	ctx                              context.Context
	cancel                           context.CancelFunc
	peers                            map[uint64]*graphProcessMux
	packets                          chan graphProcessPacketJob
	wg                               sync.WaitGroup
	mu                               sync.Mutex
	packetBytes, delivered, inFlight int
	deliveryLimit                    int
	packetCompleted                  chan struct{}
	idle                             chan struct{}
	// Optional test transport schedule; protected by mu and never graph authority.
	beforeRelay  func(context.Context, uint64, uint64, []byte) error
	afterRelay   func(context.Context, uint64, uint64, []byte, []byte) error
	relayTargets map[uint64]map[uint64]uint64
	packetFilter func(replica.Packet) (bool, error)
	// Paired-only actual MsgSnap transport owners share the original queue byte
	// allowance; neither these IDs nor the copied packets grant activation.
	pairedSnapshots bool
	snapshots       []processSnapshotPacket
	snapshotBytes   int
}
type graphProcessMux struct {
	p             *graphProcess
	id            uint64
	hub           *graphProcessHub
	mu, writeMu   sync.Mutex
	pending       map[uint64]chan graphProcessAnswer
	relays        map[uint64]context.CancelFunc
	relayQueue    chan graphProcessRelayJob
	err           error
	terminalClose uint64
}

// attachGraphProcessMuxes runs only after all configured pipes exist, before the
// first campaign. Legacy child modes keep their original synchronous exchange.
func attachGraphProcessMuxes(t *testing.T, all map[uint64]*graphProcess) {
	attachGraphProcessMuxesWithLimit(t, all, processDeliveries)
}
func attachGraphProcessMuxesWithLimit(t *testing.T, all map[uint64]*graphProcess, limit int) {
	t.Helper()
	if limit < processDeliveries || limit > 9<<30 {
		t.Fatal("invalid finite fixture delivery allowance", limit)
	}
	// The owner outlives t.Context cancellation so graceful child cleanup can
	// finish. Its explicit cleanup runs after the network has reaped its children.
	ctx, cancel := context.WithCancel(context.Background())
	hub := &graphProcessHub{ctx: ctx, cancel: cancel, peers: make(map[uint64]*graphProcessMux), packets: make(chan graphProcessPacketJob, processPacketCount), idle: make(chan struct{}, 1), deliveryLimit: limit}
	if limit != processDeliveries {
		hub.packetCompleted = make(chan struct{}, 1)
	}
	for id, p := range all {
		m := &graphProcessMux{p: p, id: id, hub: hub, pending: make(map[uint64]chan graphProcessAnswer), relays: make(map[uint64]context.CancelFunc), relayQueue: make(chan graphProcessRelayJob, processSourceWorkers)}
		hub.peers[id] = m
		p.mux = m
	}
	for _, m := range hub.peers {
		hub.wg.Go(m.readLoop)
		for range processSourceWorkers {
			hub.wg.Go(m.relayLoop)
		}
	}
	hub.wg.Go(hub.packetLoop)
	// Network cleanup is registered later and closes/reaps children first.
	t.Cleanup(func() { cancel(); hub.wg.Wait() })
}
func (h *graphProcessHub) changed() {
	select {
	case h.idle <- struct{}{}:
	default:
	}
}

// This waits for embedding traffic/worker RPCs to settle, not for a certified
// database cut. It follows actual queue and in-flight counters without sleeps.
func drainGraphProcessMuxes(ctx context.Context, all map[uint64]*graphProcess) error {
	if len(all) == 0 {
		return errInvalid
	}
	var hub *graphProcessHub
	for _, p := range all {
		if p.mux == nil {
			return errInvalid
		}
		hub = p.mux.hub
		break
	}
	for {
		hub.mu.Lock()
		idle := hub.packetBytes == 0 && hub.inFlight == 0 && len(hub.packets) == 0
		hub.mu.Unlock()
		for _, p := range all {
			m := p.mux
			m.mu.Lock()
			e := m.err
			idle = idle && len(m.pending) == 0 && len(m.relays) == 0
			m.mu.Unlock()
			if e != nil {
				return e
			}
		}
		if idle {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-hub.ctx.Done():
			return hub.ctx.Err()
		case <-hub.idle:
		}
	}
}
func (m *graphProcessMux) fail(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return
	}
	m.err = err
	m.hub.changed()
	if m.hub.packetCompleted != nil {
		select {
		case m.hub.packetCompleted <- struct{}{}:
		default:
		}
	}
	_ = m.p.stdin.Close()
	_ = m.p.stdout.Close()
	for _, ch := range m.pending {
		ch <- graphProcessAnswer{err: err}
	}
	clear(m.pending)
	for _, cancel := range m.relays {
		cancel()
	}
	clear(m.relays)
}
func (m *graphProcessMux) send(r processRequest, ch chan graphProcessAnswer) (uint64, error) {
	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	m.mu.Lock()
	if m.err != nil {
		e := m.err
		m.mu.Unlock()
		return 0, e
	}
	if ch != nil && len(m.pending) >= processSourcePending {
		m.mu.Unlock()
		return 0, errLimit
	}
	m.p.sequence++
	r.sequence = m.p.sequence
	r.incarnation = m.p.incarnation
	if ch != nil {
		m.pending[r.sequence] = ch
		if r.op == processClose {
			m.terminalClose = r.sequence
		}
	}
	wire, err := encodeProcessRequest(r)
	m.p.maxFrame = max(m.p.maxFrame, len(wire)+4)
	m.mu.Unlock()
	if err == nil {
		err = writeProcessFrame(m.p.stdin, wire)
	}
	if err != nil {
		m.fail(err)
	}
	return r.sequence, err
}
func (m *graphProcessMux) exchange(ctx context.Context, r processRequest) (processResponse, error) {
	ch := make(chan graphProcessAnswer, 1)
	seq, err := m.send(r, ch)
	if err != nil {
		return processResponse{}, err
	}
	select {
	case got := <-ch:
		return got.response, got.err
	case <-ctx.Done():
		// Cancel delivery is not worker termination. Keep the original response
		// correlation until the child has released its operation/cleanup owner.
		// The same three-second cleanup bound as retained-source Close prevents
		// unavailable child/pipe traffic from turning cancellation into a wait.
		cleanup, stopCleanup := context.WithTimeout(m.hub.ctx, 3*time.Second)
		stopFailure := context.AfterFunc(cleanup, func() { m.fail(cleanup.Err()) })
		defer func() { stopFailure(); stopCleanup() }()
		_, sendErr := m.send(processRequest{op: processSourceCancel, index: seq}, nil)
		if sendErr != nil {
			return processResponse{}, errors.Join(ctx.Err(), sendErr, cleanup.Err())
		}
		select {
		case got := <-ch:
			return got.response, errors.Join(ctx.Err(), got.err)
		case <-cleanup.Done():
			m.fail(cleanup.Err())
			return processResponse{}, errors.Join(ctx.Err(), cleanup.Err())
		case <-m.hub.ctx.Done():
			return processResponse{}, errors.Join(ctx.Err(), m.hub.ctx.Err())
		}
	case <-m.hub.ctx.Done():
		return processResponse{}, m.hub.ctx.Err()
	}
}
func (m *graphProcessMux) readLoop() {
	for {
		wire, err := readProcessFrame(m.p.stdout)
		if err != nil {
			m.fail(err)
			return
		}
		if bytes.HasPrefix(wire, []byte("GPE1")) {
			e, err := decodeGraphProcessEnvelope(wire)
			m.mu.Lock()
			valid := m.p.incarnation != ([16]byte{}) && e.incarnation == m.p.incarnation
			m.mu.Unlock()
			if err != nil || !valid {
				m.fail(errors.Join(errCorrupt, err))
				return
			}
			switch e.kind {
			case processSourcePackets:
				out, err := decodeProcessResponse(e.body)
				if err != nil || out.incarnation != e.incarnation {
					m.fail(errors.Join(errCorrupt, err))
					return
				}
				if err := m.hub.enqueue(m.id, out.packets, out.handles...); err != nil {
					m.fail(err)
					return
				}

			case processSourceRelay:
				m.mu.Lock()
				full := len(m.relays) >= processSourcePending
				_, duplicate := m.relays[e.sequence]
				ctx, cancel := context.WithCancel(m.hub.ctx)
				if !full && !duplicate {
					m.relays[e.sequence] = cancel
				}
				m.mu.Unlock()
				if full || duplicate {
					cancel()
					m.fail(errLimit)
					return
				}
				select {
				case m.relayQueue <- graphProcessRelayJob{e, ctx, cancel}:
				default:
					m.fail(errLimit)
					return
				}
			case processSourceRelayCancel:
				m.mu.Lock()
				cancel := m.relays[e.sequence]
				m.mu.Unlock()
				if cancel != nil {
					cancel()
				}
			}
			continue
		}
		if !bytes.HasPrefix(wire, []byte("GPR1")) {
			m.fail(errCorrupt)
			return
		}
		if len(wire) < 6 {
			m.fail(errCorrupt)
			return
		}
		mask := uint16(wire[4])<<8 | uint16(wire[5])
		if mask>>len(graphWireCauses) != 0 {
			m.fail(errCorrupt)
			return
		}
		r, err := decodeProcessResponse(wire[6:])
		m.mu.Lock()
		if err == nil && m.p.incarnation == ([16]byte{}) {
			m.p.incarnation = r.incarnation
		}
		if err == nil && (r.incarnation != m.p.incarnation || r.sequence > m.p.sequence) {
			err = errCorrupt
		}
		ch := m.pending[r.sequence]
		terminal := err == nil && mask == 0 && r.code == 0 && r.sequence == m.terminalClose && m.terminalClose != 0
		delete(m.pending, r.sequence)
		m.hub.changed()
		m.p.maxFrame = max(m.p.maxFrame, len(wire)+4)
		m.p.maxHeld = max(m.p.maxHeld, r.stats[statViews])
		m.p.maxPinned = max(m.p.maxPinned, r.stats[statPinnedBytes])
		m.mu.Unlock()
		if err != nil {
			m.fail(err)
			return
		}
		if ch != nil {
			var nativeErr error
			if mask != 0 {
				nativeErr = graphWireError{mask}
			}
			ch <- graphProcessAnswer{response: r, err: nativeErr}
		}
		if terminal {
			// Only the exact authenticated successful Close switches out of framing.
			// Go's test harness may print PASS/coverage after the child handler returns;
			// retain bounded drainage until process Wait/EOF rather than SIGPIPE it.
			n, tailErr := io.CopyN(io.Discard, m.p.stdout, processStderrBytes+1)
			if n > processStderrBytes {
				m.fail(errLimit)
			} else if tailErr != nil && !errors.Is(tailErr, io.EOF) {
				m.fail(tailErr)
			}
			return
		}
	}
}
func (h *graphProcessHub) enqueue(origin uint64, packets []replica.Packet, handles ...uint64) error {
	snapshotAt := 0
	for _, packet := range packets {
		if packet.From != origin || h.peer(packet.To) == nil || (packet.To <= 3) != (origin <= 3) {
			return errInvalid
		}
		cost := cap(packet.Payload) + 64
		h.mu.Lock()
		full := cost > processQueueBytes-h.packetBytes-h.snapshotBytes
		if packet.Snapshot {
			if !h.pairedSnapshots || snapshotAt >= len(handles) || handles[snapshotAt] == 0 {
				h.mu.Unlock()
				return errInvalid
			}
			if full || len(h.snapshots) >= 2 {
				h.mu.Unlock()
				return errLimit
			}
			h.snapshots = append(h.snapshots, processSnapshotPacket{packet, handles[snapshotAt]})
			h.snapshotBytes += cost
			snapshotAt++
			h.mu.Unlock()
			h.changed()
			continue
		}
		if !full {
			h.packetBytes += cost
		}
		h.mu.Unlock()
		if full {
			return errLimit
		}
		select {
		case h.packets <- graphProcessPacketJob{origin, packet}:
			h.changed()
		default:
			h.mu.Lock()
			h.packetBytes -= cost
			h.mu.Unlock()
			return errLimit
		}
	}
	if snapshotAt != len(handles) {
		return errInvalid
	}
	return nil
}
func (h *graphProcessHub) packetLoop() {
	for {
		select {
		case <-h.ctx.Done():
			return
		case job := <-h.packets:
			h.mu.Lock()
			h.packetBytes -= cap(job.packet.Payload) + 64
			h.delivered++
			h.inFlight++
			full := h.delivered > h.deliveryLimit
			filter := h.packetFilter
			h.mu.Unlock()

			if full {
				h.mu.Lock()
				h.inFlight--
				h.mu.Unlock()
				h.changed()
				h.peer(job.origin).fail(errLimit)
				continue
			}
			if filter != nil {
				drop, err := pairedGraphFaultPacketDisposition(h.delivered, filter, job.packet)
				if err != nil {
					h.peer(job.origin).fail(err)
				}
				if drop || err != nil {
					h.mu.Lock()
					h.inFlight--
					h.mu.Unlock()
					h.changed()
					continue
				}
			}
			ctx, cancel := context.WithTimeout(h.ctx, 10*time.Second)
			r, err := h.peer(job.packet.To).exchange(ctx, processRequest{op: processStep, packet: job.packet})
			cancel()
			if err == nil {
				err = processResponseError(r)
			}
			if err == nil {
				err = h.enqueue(job.packet.To, r.packets, r.handles...)
			}
			if err != nil {
				h.peer(job.origin).fail(err)
			}
			h.mu.Lock()
			h.inFlight--
			h.mu.Unlock()
			h.changed()
			if h.packetCompleted != nil {
				select {
				case h.packetCompleted <- struct{}{}:
				default:
				}
			}
		}
	}
}
func (m *graphProcessMux) relayLoop() {
	for {
		select {
		case <-m.hub.ctx.Done():
			return
		case job := <-m.relayQueue:
			e := job.envelope
			target := uint64(1)
			if e.target == 8 {
				target = 4
			} else if e.target != 3 {
				job.cancel()
				m.fail(errInvalid)
				continue
			}
			ctx, cancel := context.WithTimeout(job.ctx, 10*time.Second)
			m.hub.mu.Lock()
			before := m.hub.beforeRelay
			after := m.hub.afterRelay
			if route := m.hub.relayTargets[m.id]; route != nil {
				if actual := route[e.target]; actual != 0 {
					target = actual
				}
			}
			m.hub.mu.Unlock()
			var r processResponse
			var err error
			if before != nil {
				err = before(ctx, m.id, e.target, e.body)
			}
			if err == nil {
				r, err = m.hub.peer(target).exchange(ctx, processRequest{op: processSourceServe, entity: m.id, data: e.body})
			}
			if err == nil {
				err = processResponseError(r)
			}
			if err == nil && after != nil {
				err = after(ctx, m.id, e.target, e.body, r.aux)
			}
			cancel()
			mask := graphErrorMask(err)
			_, sendErr := m.send(processRequest{op: processSourceReply, index: e.sequence, entity: uint64(mask), data: r.aux}, nil)
			m.mu.Lock()
			delete(m.relays, e.sequence)
			m.hub.changed()
			m.mu.Unlock()
			job.cancel()
			if sendErr != nil {
				m.fail(sendErr)
			}
		}
	}
}

// The child pump has a single bounded native read/submission slot. This preserves
// precise error correlation even for native cross replies whose error has no ID.
// Host Step/Tick/cancel and IPC remain on the independent event lane.
type graphProcessPumpCall struct {
	ctx        context.Context
	kind       byte
	allocation allocationQuery
	cross      crossQuery
	request    crossRequestQuestion
	cut        graphCutQuery
	move       rangeMoveQuery
	baseline   uint64
	scanned    int
	proposal   allocationProposal
	id         allocationReadID
	reply      chan graphProcessPumpReply
}

// advanceGraphProcessPumpQueue isolates the dispatcher queue transition; the
// callback starts real native work in the child, never a fabricated proof.
func advanceGraphProcessPumpQueue(queue *[]*graphProcessPumpCall, idle func() bool, start func(*graphProcessPumpCall) error) error {
	for idle() && len(*queue) > 0 {
		c := (*queue)[0]
		(*queue)[0] = nil
		*queue = (*queue)[1:]
		if err := start(c); err != nil {
			return err
		}
	}
	return nil
}

// pollGraphProcessCutSubmission correlates a real persisted Raft command and
// its committed GSO. Operation refusal is separate from native event failure.
func pollGraphProcessCutSubmission(ctx context.Context, h *allocationHost, proposal allocationProposal, baseline uint64, scanned int) (uint64, int, []byte, error, error) {
	command, err := decodeGraphCutCommand(proposal.wire)
	if err != nil {
		return baseline, scanned, nil, err, nil
	}
	applied := h.driver.Applied()
	for index := baseline + 1; index <= applied; index++ {
		if scanned >= 1024 {
			return baseline, scanned, nil, errLimit, nil
		}
		scanned++
		baseline = index
		entries, e := h.machine.store.Entries(index, index+1, 512)
		if e != nil {
			return baseline, scanned, nil, nil, e
		}
		if len(entries) != 1 || entries[0].GetIndex() != index {
			return baseline, scanned, nil, nil, errCorrupt
		}
		if !bytes.Equal(entries[0].GetData(), proposal.wire) {
			continue
		}
		body, e := h.machine.store.ApplicationRecord(ctx, index, true, 512)
		if e != nil {
			if errors.Is(e, context.Canceled) || errors.Is(e, context.DeadlineExceeded) {
				return baseline, scanned, nil, e, nil
			}
			return baseline, scanned, nil, nil, e
		}
		if e := ctx.Err(); e != nil {
			return baseline, scanned, nil, e, nil
		}
		result, e := decodeGraphCutResult(body)
		if e != nil || result.index != index || result.phase != command.phase || result.round != command.round {
			return baseline, scanned, nil, nil, errors.Join(errInvalid, e)
		}
		return baseline, scanned, body, nil, nil
	}
	return baseline, scanned, nil, nil, nil
}

type graphProcessPumpReply struct {
	allocation allocationReply
	cross      crossReply
	request    crossRequestReply
	cut        graphCutReply
	move       rangeMoveReply
	body       []byte
	err        error
}
type graphProcessChildPump struct {
	ctx      context.Context
	calls    chan *graphProcessPumpCall
	canceled chan *graphProcessPumpCall
}

func (p *graphProcessChildPump) wait(c *graphProcessPumpCall) (graphProcessPumpReply, error) {
	c.reply = make(chan graphProcessPumpReply, 1)
	select {
	case p.calls <- c:
	case <-c.ctx.Done():
		return graphProcessPumpReply{}, c.ctx.Err()
	case <-p.ctx.Done():
		return graphProcessPumpReply{}, p.ctx.Err()
	}
	select {
	case r := <-c.reply:
		return r, r.err
	case <-c.ctx.Done():
		select {
		case p.canceled <- c:
		case <-p.ctx.Done():
		}
		return graphProcessPumpReply{}, c.ctx.Err()
	case <-p.ctx.Done():
		return graphProcessPumpReply{}, p.ctx.Err()
	}
}
func (p *graphProcessChildPump) Allocation(ctx context.Context, q allocationQuery) (allocationReply, error) {
	r, e := p.wait(&graphProcessPumpCall{ctx: ctx, kind: sourceOpen, allocation: q})
	return r.allocation, e
}
func (p *graphProcessChildPump) Cut(ctx context.Context, q graphCutQuery) (graphCutReply, error) {
	r, e := p.wait(&graphProcessPumpCall{ctx: ctx, kind: sourceCutRead, cut: q})
	return r.cut, e
}
func (p *graphProcessChildPump) Move(ctx context.Context, q rangeMoveQuery) (rangeMoveReply, error) {
	r, e := p.wait(&graphProcessPumpCall{ctx: ctx, kind: sourceMoveRead, move: q})
	return r.move, e
}
func (p *graphProcessChildPump) Cross(ctx context.Context, q crossQuery) (crossReply, error) {
	r, e := p.wait(&graphProcessPumpCall{ctx: ctx, kind: sourceCrossRead, cross: q})
	return r.cross, e
}
func (p *graphProcessChildPump) Request(ctx context.Context, q crossRequestQuestion) (crossRequestReply, error) {
	r, e := p.wait(&graphProcessPumpCall{ctx: ctx, kind: sourceRequestLookup, request: q})
	return r.request, e
}
func (p *graphProcessChildPump) Submit(ctx context.Context, q allocationProposal) ([]byte, error) {
	r, e := p.wait(&graphProcessPumpCall{ctx: ctx, kind: sourceSubmit, proposal: q})
	return r.body, e
}

type graphProcessChildWriter struct {
	mu          sync.Mutex
	incarnation [16]byte
	sequence    uint64
}

func (w *graphProcessChildWriter) frame(wire []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return writeProcessFrame(os.Stdout, wire)
}
func (w *graphProcessChildWriter) event(kind byte, sequence, target uint64, body []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if sequence == 0 {
		w.sequence++
		sequence = w.sequence
	}
	wire, err := encodeGraphProcessEnvelope(graphProcessEnvelope{kind, w.incarnation, sequence, target, body})
	if err != nil {
		return err
	}
	return writeProcessFrame(os.Stdout, wire)
}

type graphProcessChildTransport struct {
	peer  graphPeerIdentity
	owner *graphProcessChildRelay
}
type graphProcessChildRelay struct {
	mu      sync.Mutex
	writer  *graphProcessChildWriter
	next    uint64
	pending map[uint64]chan graphProcessPumpReply
	ctx     context.Context
}

func (p *graphProcessChildTransport) Peer() graphPeerIdentity { return p.peer }
func (p *graphProcessChildTransport) RoundTrip(ctx context.Context, wire []byte) ([]byte, error) {
	o := p.owner
	o.mu.Lock()
	if len(o.pending) >= processSourcePending {
		o.mu.Unlock()
		return nil, errLimit
	}
	o.next++
	sequence := o.next
	ch := make(chan graphProcessPumpReply, 1)
	o.pending[sequence] = ch
	o.mu.Unlock()
	defer func() { o.mu.Lock(); delete(o.pending, sequence); o.mu.Unlock() }()
	if err := o.writer.event(processSourceRelay, sequence, p.peer.scope.partition, wire); err != nil {
		return nil, err
	}
	select {
	case r := <-ch:
		return r.body, r.err
	case <-ctx.Done():
		return nil, errors.Join(ctx.Err(), o.writer.event(processSourceRelayCancel, sequence, p.peer.scope.partition, nil))
	case <-o.ctx.Done():
		return nil, o.ctx.Err()
	}
}

func graphProcessSourceFact(ctx context.Context, h *allocationHost, v graphstate.ReadView, id uint64) (certifiedProcessFact, error) {
	f := certifiedProcessFact{}
	entity, err := v.Entity(ctx, graphstate.EntityID(id))
	if err != nil || !entity.Found {
		return f, err
	}
	f.Exists = true
	h.mu.Lock()
	cfg, err := h.currentConfiguration()
	h.mu.Unlock()
	if err != nil {
		return f, err
	}
	point, err := temporal.RationalPosition(cfg.defaultAxis.Axis(), temporal.RationalInt64(0))
	if err != nil {
		return f, err
	}
	projection, err := graphstate.Project(ctx, v, graphstate.EntityID(id), point, graphstate.Effective, h.machine.limits.graph.Planner)
	if err != nil {
		return f, err
	}
	f.Active, f.Life, f.Labels = projection.Active, uint64(projection.Life), projection.Labels
	f.Source, f.Target = uint64(projection.Record.Source), uint64(projection.Record.Target)
	if entity.Record.Kind == graphstate.Node {
		scope, err := temporal.All(cfg.defaultAxis.Axis())
		if err != nil {
			return f, err
		}
		page, err := v.IncidentRelationships(ctx, graphstate.IncidentPredicate{Endpoint: graphstate.EntityID(id), Life: projection.Life, Window: scope}, 0, graphstate.ReadBudget{Rows: 100, Bytes: 1 << 20})
		if err != nil {
			return f, err
		}
		if !page.Complete || len(page.Entities) > 16 {
			return f, errLimit
		}
		for _, edge := range page.Entities {
			f.Incident = append(f.Incident, uint64(edge))
		}
	}
	return f, nil
}

// Resume identifiers survive an uncertain operation. Decoding this inert
// envelope confers no phase/decision authority; only the native worker Run did.
func encodeGraphProcessCoordinatorResult(r crossProcessResult, runErr error) ([]byte, error) {
	scope, err := encodeAllocationScope(r.resume.scope)
	if err != nil {
		return nil, err
	}
	var decision []byte
	if r.decision != (crossStatus{}) {
		decision, err = encodeCrossStatus(r.decision)
		if err != nil {
			return nil, err
		}
	}
	return processEncoding(func(w *boundedWriter) {
		w.add([]byte("GCR1"))
		w.field(scope)
		w.add(r.resume.request[:])
		w.add(r.resume.tx[:])
		w.add(r.resume.business[:])
		if errors.Is(runErr, errCrossProcessPending) {
			w.tag(1)
		} else {
			w.tag(0)
		}
		w.field(decision)
	})
}
func decodeGraphProcessCoordinatorResult(response processResponse) (crossProcessResult, error) {
	c := graphCursor{b: response.aux}
	if !bytes.Equal(c.take(4), []byte("GCR1")) {
		return crossProcessResult{}, errors.Join(errCorrupt, processResponseError(response))
	}
	scope := c.field(allocationScopeBytes)
	r := crossProcessResult{}
	copy(r.resume.request[:], c.take(16))
	r.resume.tx = c.array()
	copy(r.resume.business[:], c.take(32))
	pending := c.tag()
	decision := c.field(256)
	if c.err != nil || len(c.b) != 0 || pending > 1 || r.resume.request == (requestID{}) || r.resume.tx == ([16]byte{}) || r.resume.business == ([32]byte{}) {
		return crossProcessResult{}, errors.Join(errCorrupt, c.err)
	}
	var err error
	r.resume.scope, err = decodeAllocationScope(scope)
	if err != nil {
		return crossProcessResult{}, err
	}
	if len(decision) > 0 {
		r.decision, err = decodeCrossStatus(decision)
		if err != nil || r.decision.tx != r.resume.tx {
			return crossProcessResult{}, errors.Join(errCorrupt, err)
		}
	}
	err = processResponseError(response)
	if pending == 1 {
		err = errors.Join(err, errCrossProcessPending)
	}
	return r, err
}

const graphProcessObserveMaxIDs = 3

type graphProcessFactTuple struct {
	id   uint64
	fact certifiedProcessFact
}

func encodeGraphProcessObserveIDs(ids []uint64) ([]byte, error) {
	if len(ids) == 0 {
		return nil, errInvalid
	}
	if len(ids) > graphProcessObserveMaxIDs {
		return nil, errLimit
	}
	for i, id := range ids {
		if id == 0 {
			return nil, errInvalid
		}
		for _, earlier := range ids[:i] {
			if earlier == id {
				return nil, errInvalid
			}
		}
	}
	return processEncoding(func(w *boundedWriter) {
		w.add([]byte("GSO1"))
		w.u32(len(ids))
		for _, id := range ids {
			w.u64(id)
		}
	})
}
func decodeGraphProcessObserveIDs(wire []byte) ([]uint64, error) {
	c := graphCursor{b: wire}
	if !bytes.Equal(c.take(4), []byte("GSO1")) {
		return nil, errCorrupt
	}
	n := c.count(graphProcessObserveMaxIDs, 8)
	if c.err != nil || n == 0 {
		return nil, errors.Join(errCorrupt, c.err)
	}
	ids := make([]uint64, n)
	for i := range ids {
		ids[i] = c.u64()
		if ids[i] == 0 {
			return nil, errCorrupt
		}
		for _, earlier := range ids[:i] {
			if earlier == ids[i] {
				return nil, errCorrupt
			}
		}
	}
	if c.err != nil || len(c.b) != 0 {
		return nil, errors.Join(errCorrupt, c.err)
	}
	return ids, nil
}
func encodeGraphProcessObservationBatch(tuples []graphProcessFactTuple) ([]byte, error) {
	if len(tuples) == 0 {
		return nil, errInvalid
	}
	if len(tuples) > graphProcessObserveMaxIDs {
		return nil, errLimit
	}
	bodies := make([][]byte, len(tuples))
	for i, tuple := range tuples {
		if tuple.id == 0 {
			return nil, errInvalid
		}
		for _, earlier := range tuples[:i] {
			if tuple.id == earlier.id {
				return nil, errInvalid
			}
		}
		var err error
		bodies[i], err = encodeCertifiedProcessFact(tuple.fact)
		if err != nil {
			return nil, err
		}
		if len(bodies[i]) > 8<<10 {
			return nil, errLimit
		}
	}
	return processEncoding(func(w *boundedWriter) {
		w.add([]byte("GSF1"))
		w.u32(len(tuples))
		for i, tuple := range tuples {
			w.u64(tuple.id)
			w.field(bodies[i])
		}
	})
}
func decodeGraphProcessObservationBatch(wire []byte) ([]graphProcessFactTuple, error) {
	c := graphCursor{b: wire}
	if !bytes.Equal(c.take(4), []byte("GSF1")) {
		return nil, errCorrupt
	}
	n := c.count(graphProcessObserveMaxIDs, 12)
	if c.err != nil || n == 0 {
		return nil, errors.Join(errCorrupt, c.err)
	}
	tuples := make([]graphProcessFactTuple, n)
	for i := range tuples {
		tuples[i].id = c.u64()
		body := c.field(8 << 10)
		if c.err != nil || tuples[i].id == 0 {
			return nil, errors.Join(errCorrupt, c.err)
		}
		for _, earlier := range tuples[:i] {
			if tuples[i].id == earlier.id {
				return nil, errCorrupt
			}
		}
		var err error
		tuples[i].fact, err = decodeCertifiedProcessFact(body)
		if err != nil {
			return nil, err
		}
	}
	if c.err != nil || len(c.b) != 0 {
		return nil, errors.Join(errCorrupt, c.err)
	}
	return tuples, nil
}

type graphProcessWorkerResult struct {
	request  processRequest
	response processResponse
	err      error
}

// Queued requests have not acquired native owners. Cancellation still closes
// their exact original response correlation, once, on the event lane.
func cancelGraphProcessQueued(sequence uint64, paused *[]processRequest, barrier **processRequest, respond func(processRequest, processResponse, error) error) error {
	if *barrier != nil && (*barrier).sequence == sequence {
		original := **barrier
		*barrier = nil
		if err := respond(original, processResponse{}, context.Canceled); err != nil {
			return err
		}
	}
	for i, original := range *paused {
		if original.sequence == sequence {
			*paused = append((*paused)[:i], (*paused)[i+1:]...)
			return respond(original, processResponse{}, context.Canceled)
		}
	}
	return nil
}

func graphSourceProcessChild(t *testing.T, h *allocationHost, o *certifiedProcessOwners, init declaredInitCommand) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	p08 := newAllocationP08Owners(ctx, h, o)
	defer p08.close()
	writer := &graphProcessChildWriter{incarnation: o.incarnation}
	pump := &graphProcessChildPump{ctx: ctx, calls: make(chan *graphProcessPumpCall, processSourcePending), canceled: make(chan *graphProcessPumpCall, processSourcePending)}
	server, err := newGraphSourceServer(ctx, h, defaultGraphSourceLimits(h.machine.limits))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := server.close(); err != nil {
			t.Error(err)
		}
	}()
	services := make(map[uint64]*graphPeerService)
	relay := &graphProcessChildRelay{writer: writer, pending: make(map[uint64]chan graphProcessPumpReply), ctx: ctx}
	transports := make(map[uint64]graphPeerWireTransport)
	for _, part := range []uint64{3, 8} {
		voter := uint64(1)
		if part == 8 {
			voter = 4
		}
		transports[part] = &graphProcessChildTransport{graphPeerIdentity{mustProcessScope(h, part), voter}, relay}
	}
	coordinator, err := newCrossProcessCoordinator(h, transports)
	if err != nil {
		t.Fatal(err)
	}
	var moveCoordinator *rangeMoveCoordinator
	if h.machine.rangeMoves() {
		moveCoordinator, err = newRangeMoveCoordinatorOn(coordinator)
		if err != nil {
			t.Fatal(err)
		}
		// Both coordinators use the same authenticated child pipe connections.
		// A fresh operation ledger must not reset their correlation sequences.
	}
	certifiedSessions := &graphProcessCertifiedSessions{ctx: ctx, coordinator: coordinator, owners: make(map[uint64]graphProcessCertifiedOwner)}
	defer func() {
		if e := certifiedSessions.close(); e != nil {
			t.Error(e)
		}
	}()
	requests := make(chan processRequest, processSourceWorkers)
	inputErr := make(chan error, 1)
	go func() {
		for {
			wire, e := readProcessFrame(os.Stdin)
			if e != nil {
				inputErr <- e
				return
			}
			r, e := decodeProcessRequest(wire)
			if e != nil {
				inputErr <- e
				return
			}
			select {
			case requests <- r:
			case <-ctx.Done():
				return
			}
		}
	}()
	results := make(chan graphProcessWorkerResult, processSourceWorkers)
	workers := make(map[uint64]context.CancelFunc)
	controlWorkers := make(map[uint64]bool)
	var wg sync.WaitGroup
	var snapshots *processOwners
	if os.Getenv("RHO_GRAPH_PAIRED_PROCESS") == "1" {
		snapshots = &processOwners{incarnation: o.incarnation}
	}
	var active *graphProcessPumpCall
	queue := make([]*graphProcessPumpCall, 0, processSourcePending)
	var pause bool
	paused := make([]processRequest, 0, processSourceWorkers)
	var pauseBarrier *processRequest
	finish := func(c *graphProcessPumpCall, r graphProcessPumpReply) {
		if c.ctx.Err() == nil {
			c.reply <- r
		}
		active = nil
	}
	emit := func(e allocationEvent) error {
		if err := p08.event(&e); err != nil {
			return err
		}
		if len(e.capacityReplies) != 0 || len(e.snapshotSends) != 0 && snapshots == nil {
			return errInvalid
		}
		if active != nil {
			switch active.kind {
			case sourceMoveRead:
				if len(e.moveReplies) > 0 {
					if len(e.moveReplies) != 1 || e.moveReplies[0].id != active.id || e.moveReplies[0].question != active.move {
						return errInvalid
					}
					finish(active, graphProcessPumpReply{move: e.moveReplies[0]})
					e.moveReplies = nil
				}
			case sourceOpen:
				if len(e.replies) > 0 {
					if len(e.replies) != 1 || e.replies[0].id != active.id {
						return errInvalid
					}
					finish(active, graphProcessPumpReply{allocation: e.replies[0]})
					e.replies = nil
				}
			case sourceCrossRead:
				if len(e.crossReplies) > 0 {
					if len(e.crossReplies) != 1 || e.crossReplies[0].err == nil && e.crossReplies[0].proof.id != active.id {
						return errInvalid
					}
					finish(active, graphProcessPumpReply{cross: e.crossReplies[0]})
					e.crossReplies = nil
				}
			case sourceCutRead:
				if len(e.cutReplies) > 0 {
					if len(e.cutReplies) != 1 || e.cutReplies[0].id != active.id || e.cutReplies[0].question != active.cut {
						return errInvalid
					}
					finish(active, graphProcessPumpReply{cut: e.cutReplies[0]})
					e.cutReplies = nil
				}
			case sourceRequestLookup:
				if len(e.requestReplies) > 0 {
					if len(e.requestReplies) != 1 || e.requestReplies[0].err == nil && e.requestReplies[0].proof.id != active.id {
						return errInvalid
					}
					finish(active, graphProcessPumpReply{request: e.requestReplies[0]})
					e.requestReplies = nil
				}
			}
		}
		if err := o.event(e); err != nil {
			return err
		}
		if len(e.packets) > 0 {
			handles, err := pairedSnapshotHandles(snapshots, e)
			if err != nil {
				return err
			}
			wire, err := encodeProcessResponse(processResponse{incarnation: o.incarnation, sequence: 1, packets: e.packets, handles: handles})
			if err != nil {
				return err
			}
			return writer.event(processSourcePackets, 0, 0, wire)
		}
		return nil
	}
	startPump := func(c *graphProcessPumpCall) error {
		if c.ctx.Err() != nil {
			return nil
		}
		active = c
		var event allocationEvent
		var err error
		switch c.kind {
		case sourceMoveRead:
			event, err = h.ReadRangeMove(c.ctx, c.move)
		case sourceOpen:
			event, err = h.Read(c.allocation)
		case sourceCrossRead:
			event, err = h.ReadCrossPhase(c.cross.tx, c.cross.phase, c.cross.nonce)
		case sourceRequestLookup:
			event, err = h.readCrossRequest(c.request)
		case sourceCutRead:
			event, err = h.ReadGraphCut(c.cut)
		case sourceSubmit:
			c.baseline = h.driver.Applied()
			event, err = h.Submit(c.proposal)
		}
		if err != nil {
			finish(c, graphProcessPumpReply{err: err})
			return nil
		}
		c.id = event.readID
		return emit(event)
	}
	pollSubmit := func() error {
		if active == nil || active.kind != sourceSubmit {
			return nil
		}
		if isGraphCutWire(active.proposal.wire) {
			next, scanned, body, operationErr, eventErr := pollGraphProcessCutSubmission(active.ctx, h, active.proposal, active.baseline, active.scanned)
			active.baseline, active.scanned = next, scanned
			if eventErr != nil {
				return eventErr
			}
			if body != nil || operationErr != nil {
				finish(active, graphProcessPumpReply{body: body, err: operationErr})
			}
			return nil
		}
		if isRangeMoveWire(active.proposal.wire) {
			next, scanned, body, operationErr, eventErr := pollGraphProcessMoveSubmission(active.ctx, h, active.proposal, active.baseline, active.scanned)
			active.baseline, active.scanned = next, scanned
			if eventErr != nil {
				return eventErr
			}
			if body != nil || operationErr != nil {
				finish(active, graphProcessPumpReply{body: body, err: operationErr})
			}
			return nil
		}

		c, _, err := decodeCrossCommand(active.proposal.wire, h.machine.limits)
		if err != nil {
			finish(active, graphProcessPumpReply{err: err})
			return nil
		}
		arena, err := graphstate.NewOutputBudget(h.machine.limits.outputBytes)
		if err != nil {
			return err
		}
		manifest, err := decodeCrossManifest(c.manifest, h.machine.limits, arena)
		if err != nil {
			return err
		}
		status, _, err := h.machine.observeCrossTransaction(crossQuery{tx: manifest.tx, nonce: [16]byte{1}, phase: c.phase}, h.driver.Applied())
		if errors.Is(err, replica.ErrUnavailable) {
			return nil
		}
		if err != nil {
			return err
		}
		body, err := encodeCrossStatus(status)
		finish(active, graphProcessPumpReply{body: body, err: err})
		return nil
	}
	isSourceClose := func(r processRequest) bool {
		if r.op != processSourceServe {
			return false
		}
		request, err := decodeGraphSourceRequest(r.data)
		return err == nil && request.kind == sourceClose
	}
	launch := func(r processRequest) error {
		control := isSourceClose(r)
		if control {
			if len(controlWorkers) >= 1 {
				return errLimit
			}
		} else if len(workers)-len(controlWorkers) >= processSourceWorkers {
			return errLimit
		}
		if control {
			controlWorkers[r.sequence] = true
		}
		workerCtx, stop := context.WithCancel(ctx)
		workers[r.sequence] = stop
		var service *graphPeerService
		if r.op == processSourceServe {
			if r.entity < 1 || r.entity > 6 {
				delete(workers, r.sequence)
				delete(controlWorkers, r.sequence)
				stop()
				return errInvalid
			}
			service = services[r.entity]
			if service == nil {
				var e error
				service, e = newGraphPeerService(h, server, pump)
				if e != nil {
					delete(workers, r.sequence)
					delete(controlWorkers, r.sequence)
					stop()
					return e
				}
				services[r.entity] = service
			}
		}
		wg.Go(func() {
			out := processResponse{}
			var e error
			switch r.op {
			case processSourceServe:
				out.aux, e = service.serve(workerCtx, r.data)
			case processSourceCoordinate:
				var original partitionGraphCommand
				original, e = decodePartitionGraphCommand(r.data, h.machine.limits)
				if e == nil {
					var result crossProcessResult
					result, e = coordinator.Run(workerCtx, original.request.id, original.request.operations, original.request.revision.Provenance(), original.request.claims)
					if result.resume.request != (requestID{}) {
						var encodeErr error
						out.aux, encodeErr = encodeGraphProcessCoordinatorResult(result, e)
						e = errors.Join(e, encodeErr)
					}
				}
			case processRangeRun, processRangeResume:
				out.aux, e = graphE17RunMove(workerCtx, h, o.incarnation, moveCoordinator, r)
			case processPairedPendingOpen:
				out.aux, e = pairedGraphFaultPendingOpen(workerCtx, certifiedSessions, r)
			case processCostObserve:
				if o.cost == nil {
					e = errInvalid
				} else {
					out.aux, e = nativeGraphCostObserve(workerCtx, certifiedSessions, r)
				}
			case processLifecycleObserve:
				out.aux, e = nativeLifecycleObserve(workerCtx, certifiedSessions, r)
			case processCertifiedIndexObserve:
				out.aux, e = graphE18IndexObserve(workerCtx, certifiedSessions, r)
			case processRangeCertifiedDetail:
				out.aux, e = graphE17SessionObserve(workerCtx, certifiedSessions, r)
			case processSourceCertifiedOpen, processSourceCertifiedObserve, processSourceCertifiedClose:
				out.aux, e = certifiedSessions.serve(workerCtx, r)
			case processSourceObserve:
				ids := []uint64{r.entity}
				if r.entity == 0 {
					ids, e = decodeGraphProcessObserveIDs(r.data)
				} else if len(r.data) != 0 {
					e = errInvalid
				}
				if e != nil {
					break
				}
				var read *crossProcessRead
				read, e = coordinator.OpenQuiescentSources(workerCtx)
				if e == nil {
					// One retained composite view and its original shared output
					// arena serve every ID. Per-entity source work never resets.
					arena := read.sources[0].endpoint.budget.output
					e = arena.Reserve(32 + 128*len(ids))
					tuples := make([]graphProcessFactTuple, len(ids))
					for i, id := range ids {
						if e != nil {
							break
						}
						tuples[i].id = id
						tuples[i].fact, e = graphProcessSourceFact(workerCtx, h, read.ReadView(), id)
						if e != nil {
							break
						}
						f := tuples[i].fact
						cost := 72 + 16*cap(f.Labels) + 8*cap(f.Incident)
						for _, label := range f.Labels {
							cost += len(label)
						}
						e = arena.Reserve(cost)
					}
					if e == nil {
						if r.entity != 0 {
							out.aux, e = encodeCertifiedProcessFact(tuples[0].fact)
						} else {
							out.aux, e = encodeGraphProcessObservationBatch(tuples)
						}
					}
					if e == nil {
						e = arena.Reserve(2 * cap(out.aux))
					}
					closeErr := read.Close()
					e = errors.Join(e, closeErr)
					if e != nil {
						out.aux = nil
					}
				}
			}
			select {
			case results <- graphProcessWorkerResult{r, out, e}:
			case <-ctx.Done():
			}
		})
		return nil
	}
	respond := func(r processRequest, response processResponse, err error) error {
		if len(response.aux) > 1<<20 {
			response.aux = nil
			err = errors.Join(err, errLimit)
		}
		response.incarnation, response.sequence = o.incarnation, r.sequence
		if r.op != processClose {
			var e error
			response.stats, response.cutID, response.manifestID, e = processStats(h.machine.store)
			err = errors.Join(err, e)
		}
		response.applied = h.driver.Applied()
		if snapshots == nil || r.op != processBuild && r.op != processNext {
			response.ready = o.ready
		}
		response.code, response.detail = processError(err)
		response.causeMask = processCauseMask(err)
		wire, e := encodeProcessResponse(response)
		if e != nil {
			return e
		}
		mask := graphErrorMask(err)
		frame := append([]byte("GPR1"), byte(mask>>8), byte(mask))
		return writer.frame(append(frame, wire...))
	}
	defer func() {
		cancel()
		for _, stop := range workers {
			stop()
		}
		if active != nil && active.id != (allocationReadID{}) {
			_ = h.cancelSourceRead(active.id)
		}
		wg.Wait()
		if snapshots != nil {
			h.mu.Lock()
			err := snapshots.close(h.driver)
			h.mu.Unlock()
			if err != nil {
				t.Error("paired snapshot owner cleanup", err)
			}
		}
	}()
	for {
		if err := advanceGraphProcessPumpQueue(&queue, func() bool { return active == nil }, func(c *graphProcessPumpCall) error {
			if err := startPump(c); err != nil {
				return err
			}
			return pollSubmit()
		}); err != nil {
			t.Fatal(err)
		}
		select {
		case e := <-inputErr:
			if !errors.Is(e, io.EOF) {
				t.Error(e)
			}
			return
		case <-ctx.Done():
			return
		case c := <-pump.calls:
			if len(queue) >= processSourcePending {
				c.reply <- graphProcessPumpReply{err: errLimit}
			} else {
				queue = append(queue, c)
			}
		case c := <-pump.canceled:
			if active == c {
				if c.id != (allocationReadID{}) {
					_ = h.cancelSourceRead(c.id)
				}
				active = nil
			}
			for i, q := range queue {
				if q == c {
					queue = append(queue[:i], queue[i+1:]...)
					break
				}
			}
		case result := <-results:
			stop := workers[result.request.sequence]
			delete(workers, result.request.sequence)
			delete(controlWorkers, result.request.sequence)
			if stop != nil {
				stop()
			}
			if err := respond(result.request, result.response, result.err); err != nil {
				t.Fatal(err)
			}
		case r := <-requests:
			if r.incarnation != o.incarnation && (r.sequence != 1 || r.incarnation != ([16]byte{})) {
				t.Fatal("source IPC incarnation")
			}
			var event allocationEvent
			response := processResponse{}
			var e error
			async := false
			switch r.op {
			case processCampaign:
				event, e = h.Campaign()
			case processTick:
				event, e = h.Tick()
			case processStep:
				event, e = h.Step(r.packet)
			case processOffers, processBuild, processSeal, processManifest, processNext, processFeedback, processBeginImport, processAppend, processVerify, processPrepare, processActivate, processImportClose:
				response, event, e = pairedSnapshotProcessEvent(ctx, h, snapshots, r)
			case processPairedConnections:
				response.aux, e = pairedGraphFaultConnections(r.entity, services, server)
			case processPairedReconnect:
				coordinator, e = pairedGraphFaultReconnect(ctx, h, coordinator, certifiedSessions, moveCoordinator, server, r, len(workers)+len(paused)+len(controlWorkers), len(queue), active != nil)
				if e == nil {
					certifiedSessions.coordinator = coordinator
				}
			case processCostRecord:
				if o.cost == nil {
					e = errInvalid
				} else {
					response.aux, e = nativeGraphCostRecordWire(ctx, h, r.index)
				}
			case processLifecycleRecord:
				if os.Getenv("RHO_GRAPH_PAIRED_PROCESS") == "1" {
					response.aux, e = pairedGraphFaultRecordWire(ctx, h, r.index)
				} else {
					response.aux, e = nativeLifecycleRecordWire(ctx, h, r.index)
				}
			case processP08ChangedGraph:
				var original allocationProposal
				original, e = decodeCertifiedProcessProposal(r.data)
				if e == nil {
					var command partitionGraphCommand
					command, e = decodePartitionGraphCommand(original.wire, h.machine.limits)
					if e == nil {
						var changed allocationProposal
						changed, e = h.PreparePartitionGraphOperations(command.request.id, command.request.operations, 42, command.request.claims)
						if e == nil {
							response.aux, e = encodeCertifiedProcessProposal(changed)
						}
					}
				}
			case processP08Metadata:
				response.aux, e = allocationP08Metadata(ctx, h, r.data)
			case processP08BeginAcquire:
				response.aux, e = p08.begin(r.index, r.entity)
			case processP08ReadAcquire:
				event, e = p08.read()
			case processP08FinishAcquire:
				response.aux, e = p08.finish(r.entity != 0)
			case processP08Mint:
				response.aux, e = p08.mint(r.index)
			case processP08PrepareGraph:
				var p allocationProposal
				p, e = p08.proposal(r)
				if e == nil {
					response.aux, e = encodeCertifiedProcessProposal(p)
				}
			case processP08ReadGrant:
				event, e = allocationP08ReadRequest(o, r)
			case processCertifiedRead:
				event, e = o.read(r)
			case processCertifiedAction:
				var p allocationProposal
				p, e = o.action(t, r, init)
				if e == nil && len(p.wire) > 0 {
					response.aux, e = encodeCertifiedProcessProposal(p)
				}
			case processPropose:
				var p allocationProposal
				p, e = decodeCertifiedProcessProposal(r.data)
				if e == nil {
					event, e = h.Submit(p)
				}
			case processStateOp:
				_, response.image, e = h.machine.store.Checkpoint()
				if e == nil && r.entity == 1 {
					first, firstErr := h.machine.store.FirstIndex()
					last, lastErr := h.machine.store.LastIndex()
					offers, offersErr := h.driver.SnapshotSends()
					e = errors.Join(firstErr, lastErr, offersErr)
					if e == nil {
						response.aux, e = json.Marshal(struct {
							FirstIndex, LastIndex uint64
							SnapshotOffers        int
						}{first, last, len(offers)})
					}
				}
				relay.mu.Lock()
				response.handles = []uint64{uint64(len(workers) + len(paused)), uint64(len(relay.pending))}
				relay.mu.Unlock()
			case processEntryRecord:
				response.outcome, e = h.machine.store.ApplicationRecord(ctx, r.index, true, 64<<10)
				if e == nil {
					response.changes, e = h.machine.store.ApplicationRecord(ctx, r.index, false, 1<<20)
				}
			case processSourceCutEvidence:
				var evidence graphE15CutEvidence
				evidence, e = captureGraphE15CutEvidence(ctx, h, r.entity)
				if e == nil {
					response.aux, e = encodeGraphE15CutEvidence(evidence)
				}
			case processRangeNativeVoterProbe:
				response.aux, e = graphE17NativeVoterPage(ctx, h, r.data)
			case processRangeSnapshotProbe, processRangeImportProbe, processRangeEvidenceProbe, processRangeProgressProbe:
				response, e = graphRangeMoveNativeProbe(ctx, h, r)
			case processCertifiedObserve:
				var fact certifiedProcessFact
				fact, e = certifiedProcessObservation(ctx, h, r.entity)
				if e == nil {
					response.aux, e = encodeCertifiedProcessFact(fact)
				}
			case processSourceReply:
				if r.entity>>len(graphWireCauses) != 0 || r.entity != 0 && len(r.data) != 0 {
					e = errCorrupt
					break
				}
				relay.mu.Lock()
				ch := relay.pending[r.index]
				relay.mu.Unlock()
				if ch != nil {
					var replyErr error
					if r.entity != 0 {
						replyErr = graphWireError{uint16(r.entity)}
					}
					ch <- graphProcessPumpReply{body: bytes.Clone(r.data), err: replyErr}
				}
			case processSourceCancel:
				if stop := workers[r.index]; stop != nil {
					stop()
				}
				if err := cancelGraphProcessQueued(r.index, &paused, &pauseBarrier, respond); err != nil {
					t.Fatal(err)
				}

			case processSourcePausedBarrier:
				if len(paused) > 0 {
					response.handles = []uint64{uint64(len(paused)), uint64(len(workers))}
				} else if pauseBarrier != nil {
					e = errLimit
				} else {
					copyRequest := r
					pauseBarrier = &copyRequest
					async = true
				}
			case processSourcePause:
				pause = r.entity != 0
				if !pause {
					for _, q := range paused {
						if e = launch(q); e != nil {
							break
						}
					}
					paused = nil
				}
			case processSourceServe, processSourceCoordinate, processSourceObserve, processSourceCertifiedOpen, processSourceCertifiedObserve, processSourceCertifiedClose, processRangeRun, processRangeResume, processRangeCertifiedDetail, processCertifiedIndexObserve, processLifecycleObserve, processPairedPendingOpen, processCostObserve:
				if pause && r.op == processSourceServe && !isSourceClose(r) {
					if len(paused) >= processSourceWorkers {
						e = errLimit
					} else {
						paused = append(paused, r)
						async = true
						if pauseBarrier != nil {
							if err := respond(*pauseBarrier, processResponse{handles: []uint64{uint64(len(paused)), uint64(len(workers))}}, nil); err != nil {
								t.Fatal(err)
							}
							pauseBarrier = nil
						}
					}
				} else {
					e = launch(r)
					async = e == nil
				}
			case processClose:
				cancel()
				for _, stop := range workers {
					stop()
				}
				wg.Wait()
				if snapshots != nil {
					h.mu.Lock()
					e = snapshots.close(h.driver)
					h.mu.Unlock()
				}
				e = errors.Join(e, h.Close())
				if err := respond(r, response, e); err != nil {
					t.Error(err)
				}
				return
			default:
				e = errInvalid
			}
			if async {
				continue
			}
			// Ordinary bootstrap traffic stays in its synchronous response, so existing
			// tinyGraphTransport still owns that traffic. Worker-pump packets are events.
			if e == nil {
				var handles []uint64
				handles, e = pairedSnapshotHandles(snapshots, event)
				if e == nil && len(handles) > 0 {
					response.handles = handles
				}
				if e == nil {
					e = emit(allocationEvent{readID: event.readID, replies: event.replies, crossReplies: event.crossReplies, requestReplies: event.requestReplies, capacityReplies: event.capacityReplies, cutReplies: event.cutReplies, moveReplies: event.moveReplies})
				}
			}
			response.packets = event.packets
			if e == nil {
				e = pollSubmit()
			}
			if err := respond(r, response, e); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestGraphSourceProcessEnvelopeRejectsUnboundAndExtraInput(t *testing.T) {
	valid := graphProcessEnvelope{processSourceRelay, [16]byte{1}, 1, 8, []byte{1, 2}}
	wire, err := encodeGraphProcessEnvelope(valid)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeGraphProcessEnvelope(wire)
	if err != nil || decoded.incarnation != valid.incarnation || decoded.target != 8 || !bytes.Equal(decoded.body, valid.body) {
		t.Fatal(decoded, err)
	}
	for _, b := range [][]byte{nil, wire[:len(wire)-1], append(bytes.Clone(wire), 0)} {
		if _, err := decodeGraphProcessEnvelope(b); !errors.Is(err, errCorrupt) {
			t.Fatal("malformed embedding frame", err)
		}
	}
	valid.incarnation = [16]byte{}
	if _, err := encodeGraphProcessEnvelope(valid); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	// This frame identifies only an embedding request; it never imports a proof.
}

// This guard waits for an actually queued peer call, then exercises real Host
// event progress and cancellation. It makes no certified-cut claim.
func TestGraphSourceSixProcessesPausedPeerKeepsNativeLaneAndCancels(t *testing.T) {
	n := newCertifiedProcessNetwork(t)
	n.all[4].call(t, processRequest{op: processSourcePause, entity: 1})
	source := n.all[1].call(t, processRequest{op: processCertifiedAction, entity: certifiedCreateCrossEdge, index: 3})
	proposal, err := decodeCertifiedProcessProposal(source.aux)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := n.all[1].exchange(ctx, processRequest{op: processSourceCoordinate, data: proposal.wire})
		done <- err
	}()
	paused := n.all[4].call(t, processRequest{op: processSourcePausedBarrier})
	if len(paused.handles) != 2 || paused.handles[0] != 1 || paused.handles[1] != 0 {
		t.Fatal("no exact paused service association", paused.handles)
	}
	for _, op := range []byte{processTick, processStateOp} {
		n.all[4].call(t, processRequest{op: op})
	}
	// An independent native quorum read also progresses on the coordinator while
	// its peer worker awaits the paused destination. The parent's event mux must
	// deliver worker packets without inventing completed read evidence.
	n.read(t, 1, observeConfiguration, 0)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("coordinator cancellation", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("canceled coordinator did not complete")
	}
	n.all[4].call(t, processRequest{op: processSourcePause})
	// Synchronize on worker terminal responses before inspecting native owners.
	// Quiescent coordinator reads exercise the same persistent endpoint streams.
	n.all[1].call(t, processRequest{op: processSourceObserve, entity: 1})
	drainCtx, stopDrain := context.WithTimeout(t.Context(), 10*time.Second)
	defer stopDrain()
	if err := drainGraphProcessMuxes(drainCtx, n.all); err != nil {
		t.Fatal("embedding traffic did not drain", err)
	}
	for id, p := range n.all {
		r := p.call(t, processRequest{op: processStateOp})
		if r.stats[statViews] != 0 || r.stats[statPinnedBytes] != 0 || len(r.handles) != 2 || r.handles[0] != 0 || r.handles[1] != 0 {
			t.Fatal("canceled source owner residue", id, r.stats, r.handles)
		}
	}
	oracle := newCertifiedProcessOracle()
	n.compare(t, oracle)
}

// This deterministic queue guard names the dropped-progress input. The callback
// models only local queue admission; native Host evidence is tested separately.
func TestGraphSourceProcessPumpQueueSkipsCanceledWithoutStallingNext(t *testing.T) {
	canceled, stop := context.WithCancel(t.Context())
	stop()
	first := &graphProcessPumpCall{ctx: canceled}
	second := &graphProcessPumpCall{ctx: t.Context()}
	queue := []*graphProcessPumpCall{first, second}
	var active *graphProcessPumpCall
	starts := 0
	err := advanceGraphProcessPumpQueue(&queue, func() bool { return active == nil }, func(c *graphProcessPumpCall) error {
		if c.ctx.Err() != nil {
			return nil
		}
		starts++
		active = c
		return nil
	})
	if err != nil || active != second || starts != 1 || len(queue) != 0 {
		t.Fatal("single-step queue skipped canceled first but stranded live second", starts, len(queue), err)
	}
}

// These copied tuples are inert observations. This guard covers the bounded
// batch seam without constructing any Host/read/phase authority.
func TestGraphSourceProcessObservationBatchBoundsOrderAndOwnedFacts(t *testing.T) {
	ids := []uint64{1, 3, 7}
	request, err := encodeGraphProcessObserveIDs(ids)
	if err != nil {
		t.Fatal(err)
	}
	decodedIDs, err := decodeGraphProcessObserveIDs(request)
	if err != nil || len(decodedIDs) != 3 || decodedIDs[0] != 1 || decodedIDs[1] != 3 || decodedIDs[2] != 7 {
		t.Fatal(decodedIDs, err)
	}
	tuples := []graphProcessFactTuple{{1, certifiedProcessFact{Exists: true, Active: true, Life: 2, Labels: []string{"healthy"}, Incident: []uint64{3}}}, {3, certifiedProcessFact{Exists: true, Active: true, Life: 4, Source: 1, Target: 17}}, {7, certifiedProcessFact{}}}
	wire, err := encodeGraphProcessObservationBatch(tuples)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeGraphProcessObservationBatch(wire)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded) != 3 || decoded[0].id != 1 || decoded[1].id != 3 || decoded[2].id != 7 || decoded[0].fact.Labels[0] != "healthy" || decoded[0].fact.Incident[0] != 3 || decoded[1].fact.Target != 17 || decoded[2].fact.Exists {
		t.Fatal("batch lost exact ordered tuples", decoded)
	}
	clear(wire)
	clear(request)
	tuples[0].fact.Labels[0] = "changed"
	tuples[0].fact.Incident[0] = 99
	if decoded[0].fact.Labels[0] != "healthy" || decoded[0].fact.Incident[0] != 3 || decodedIDs[2] != 7 {
		t.Fatal("decoded batch aliases input")
	}
	for _, invalid := range [][]uint64{nil, {1, 1}, {0}, {1, 3, 7, 17}} {
		_, err := encodeGraphProcessObserveIDs(invalid)
		want := errInvalid
		if len(invalid) > 3 {
			want = errLimit
		}
		if !errors.Is(err, want) {
			t.Fatal("batch request admission", invalid, err)
		}
	}
	good, err := encodeGraphProcessObserveIDs(ids)
	if err != nil {
		t.Fatal(err)
	}
	for _, invalid := range [][]byte{nil, good[:len(good)-1], append(bytes.Clone(good), 0), {'G', 'S', 'O', '1', 0, 0, 0, 4}} {
		if _, err := decodeGraphProcessObserveIDs(invalid); !errors.Is(err, errCorrupt) {
			t.Fatal("malformed batch request", err)
		}
	}
}
