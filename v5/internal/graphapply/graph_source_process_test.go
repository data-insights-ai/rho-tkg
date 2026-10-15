package graphapply

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strconv"
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
	// Cost fixtures only. The timestamp keeps the queued job within the existing
	// 64-byte admission allowance; no per-packet timing history is retained.
	enqueuedNanos int64
}

// Aggregate embedding timings, not durability attribution. Queue age overlaps
// client waits; Step exchange includes IPC and native driver/application work.
type graphProcessCostTiming struct {
	QueueCount, StepCount                              uint64
	QueueNanos, QueueMaxNanos, StepNanos, StepMaxNanos int64
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
	costProgress                     chan struct{}           // cost-only broadcast generation, protected by mu
	costTiming                       *graphProcessCostTiming // optional, protected by mu
	costStarted                      time.Time
	idle                             chan struct{}
	// Optional test transport schedule; protected by mu and never graph authority.
	beforeRelay         func(context.Context, uint64, uint64, []byte) error
	afterRelay          func(context.Context, uint64, uint64, []byte, []byte) error
	relayTargets        map[uint64]map[uint64]uint64
	packetFilter        func(replica.Packet) (bool, error)
	nativeFaultSchedule *nativeFaultPacketScheduler
	workload            *nativeWorkloadTransport // immutable fresh workload owner
	workloadBytes       int                      // fixed scheduler ownership within existing queue bytes
	scheduledPackets    int                      // charged held/duplicate frames within existing caps
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
	attachGraphProcessMuxesWithWorkload(t, all, limit, nil)
}
func attachGraphProcessMuxesWithWorkload(t *testing.T, all map[uint64]*graphProcess, limit int, workload *nativeWorkloadTransport) {
	t.Helper()
	if limit < processDeliveries || limit > 9<<30 && (workload == nil || uint64(limit) > workload.maximum) {
		t.Fatal("invalid finite fixture delivery allowance", limit)
	}
	// The owner outlives t.Context cancellation so graceful child cleanup can
	// finish. Its explicit cleanup runs after the network has reaped its children.
	ctx, cancel := context.WithCancel(context.Background())
	hub := &graphProcessHub{ctx: ctx, cancel: cancel, peers: make(map[uint64]*graphProcessMux), packets: make(chan graphProcessPacketJob, processPacketCount), idle: make(chan struct{}, 1), deliveryLimit: limit}
	if limit != processDeliveries {
		hub.packetCompleted = make(chan struct{}, 1)
		hub.costProgress = make(chan struct{})
	}
	for id, p := range all {
		m := &graphProcessMux{p: p, id: id, hub: hub, pending: make(map[uint64]chan graphProcessAnswer), relays: make(map[uint64]context.CancelFunc), relayQueue: make(chan graphProcessRelayJob, processSourceWorkers)}
		hub.peers[id] = m
		p.mux = m
	}
	if workload != nil {
		if err := workload.install(hub); err != nil {
			cancel()
			t.Fatal(err)
		}
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

func (h *graphProcessHub) costTimingSnapshot() graphProcessCostTiming {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.costTiming == nil {
		return graphProcessCostTiming{}
	}
	return *h.costTiming
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
	m.hub.mu.Lock()
	m.hub.costProgressLocked()
	m.hub.mu.Unlock()
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
		if m.hub.workload != nil {
			err = m.hub.workload.writeFrame(m.p.stdin, wire)
		} else {
			err = writeProcessFrame(m.p.stdin, wire)
		}
	}
	if err != nil {
		m.fail(err)
	}
	return r.sequence, err
}
func (m *graphProcessMux) exchange(ctx context.Context, r processRequest) (processResponse, error) {
	if err := ctx.Err(); err != nil {
		return processResponse{}, err
	}
	ch := make(chan graphProcessAnswer, 1)
	// The initial send can block in a pipe write before the response select.
	// Expiry closes that failed transport; once sent, the existing per-request
	// cancellation and terminal-owner cleanup below retain their own lifetime.
	sendStopped := make(chan struct{})
	stopSend := func() bool { return true }
	stopWatch := func() {}
	if deadline, bounded := ctx.Deadline(); bounded {
		// Manual cancellation after a complete frame must still cancel the
		// original worker, rather than racing send completion and failing the
		// whole stream. A blocked send retains the operation's original deadline.
		watch, stop := context.WithDeadline(context.WithoutCancel(ctx), deadline)
		stopWatch = stop
		stopSend = context.AfterFunc(watch, func() { m.fail(errors.Join(ctx.Err(), watch.Err())); close(sendStopped) })
	}
	seq, err := m.send(r, ch)
	if !stopSend() {
		<-sendStopped
	}
	stopWatch()
	if err != nil {
		return processResponse{}, errors.Join(err, ctx.Err())
	}
	select {
	case got := <-ch:
		return got.response, errors.Join(got.err, ctx.Err())
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
		var wire []byte
		var err error
		if m.hub.workload != nil {
			wire, err = m.hub.workload.readFrame(m.p.stdout)
		} else {
			wire, err = readProcessFrame(m.p.stdout)
		}
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
		r, err := decodeProcessResponseForProfile(wire[6:], m.p.durableColumns)
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
		if h.workload != nil {
			err := h.workload.enqueueLocked(h, origin, packet)
			h.mu.Unlock()
			if err != nil {
				return err
			}
			h.changed()
			continue
		}
		full := cost > processQueueBytes-h.packetBytes-h.snapshotBytes || !packet.Snapshot && len(h.packets)+h.scheduledPackets >= processPacketCount
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
		var enqueuedNanos int64
		if h.costTiming != nil {
			enqueuedNanos = time.Since(h.costStarted).Nanoseconds()
		}
		h.mu.Unlock()
		if full {
			return errLimit
		}
		select {
		case h.packets <- graphProcessPacketJob{origin, packet, enqueuedNanos}:
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
	defer func() {
		if h.workload != nil {
			h.cancel()
			h.workload.close(h)
		}
		h.mu.Lock()
		schedule := h.nativeFaultSchedule
		h.mu.Unlock()
		if schedule != nil {
			_, _ = schedule.close(h)
		}
	}()
	for {
		var job graphProcessPacketJob
		if h.workload != nil {
			var err error
			job, err = h.workload.next(h)
			if err != nil {
				h.cancel()
				return
			}
		} else {
			select {
			case <-h.ctx.Done():
				return
			case job = <-h.packets:
			}
		}
		h.mu.Lock()
		h.packetBytes -= cap(job.packet.Payload) + 64
		if h.workload != nil {
			h.workload.held--
		}
		h.delivered++
		h.inFlight++
		full := h.delivered > h.deliveryLimit
		filter := h.packetFilter
		schedule := h.nativeFaultSchedule
		timed := h.costTiming != nil
		if timed {
			age := time.Since(h.costStarted).Nanoseconds() - job.enqueuedNanos
			h.costTiming.QueueCount++
			h.costTiming.QueueNanos += age
			h.costTiming.QueueMaxNanos = max(h.costTiming.QueueMaxNanos, age)
		}
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
		err := h.deliverFaultScheduled(job, schedule, timed)
		if err != nil {
			h.peer(job.origin).fail(err)
		}
		h.mu.Lock()
		h.inFlight--
		h.costProgressLocked()
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

func (h *graphProcessHub) deliverFaultScheduled(job graphProcessPacketJob, schedule *nativeFaultPacketScheduler, timed bool) (err error) {
	jobs, count := [2]graphProcessPacketJob{job}, 1
	base := h.ctx
	if schedule != nil {
		jobs, count, err = schedule.route(h, job)
		defer func() {
			if err != nil {
				schedule.failed(err)
			}
			schedule.done(h)
		}()
		if err != nil {
			return err
		}
		base = schedule.ctx
	}
	for i := range count {
		if i > 0 {
			h.mu.Lock()
			h.delivered++
			full := h.delivered > h.deliveryLimit
			h.mu.Unlock()
			if full {
				return errLimit
			}
		}
		actual := jobs[i]
		ctx, cancel := context.WithTimeout(base, 10*time.Second)
		var start time.Time
		if timed {
			start = time.Now()
		}
		r, err := h.peer(actual.packet.To).exchange(ctx, processRequest{op: processStep, packet: actual.packet})
		var nanos int64
		if timed {
			nanos = time.Since(start).Nanoseconds()
		}
		cancel()
		if timed {
			h.mu.Lock()
			h.costTiming.StepCount++
			h.costTiming.StepNanos += nanos
			h.costTiming.StepMaxNanos = max(h.costTiming.StepMaxNanos, nanos)
			h.mu.Unlock()
		}
		if err == nil {
			err = processResponseError(r)
		}
		if err == nil {
			err = h.enqueue(actual.packet.To, r.packets, r.handles...)
		}
		if err != nil {
			return err
		}
	}
	return nil
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
			if m.hub.workload != nil && r.sequence != 0 {
				// A correlated native error reply is still a received frame and
				// traverses the same reply leg; Completed means delay, never ACK.
				err = errors.Join(err, m.hub.workload.after(ctx, m.id, e.target, e.body, r.aux))
			} else if err == nil && after != nil {
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
	ctx            context.Context
	kind           byte
	allocation     allocationQuery
	cross          crossQuery
	capacity       bool
	capacityLimits graphCapacityLimits
	request        crossRequestQuestion
	cut            graphCutQuery
	move           rangeMoveQuery
	baseline       uint64
	scanned        int
	proposal       allocationProposal
	id             allocationReadID
	reply          chan graphProcessPumpReply
	transition     *nativeLogicalTransitionTicket
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
	return pollGraphProcessSubmission(ctx, h, proposal, baseline, scanned, 512, func(body []byte, index uint64) error {
		result, err := decodeGraphCutResult(body)
		if err != nil || result.index != index || result.phase != command.phase || result.round != command.round {
			return errors.Join(errInvalid, err)
		}
		return nil
	})
}
func pollGraphProcessPinSubmission(ctx context.Context, h *allocationHost, proposal allocationProposal, baseline uint64, scanned int) (uint64, int, []byte, error, error) {
	command, err := decodeGraphPinCommand(proposal.wire)
	if err != nil {
		return baseline, scanned, nil, err, nil
	}
	return pollGraphProcessSubmission(ctx, h, proposal, baseline, scanned, uint64(h.machine.limits.commandBytes), func(body []byte, index uint64) error {
		result, err := decodeGraphPinResult(body)
		if err != nil || result.index > index || result.phase != command.phase || result.id != command.id || result.digest != command.digest || result.action != command.action {
			return errors.Join(errInvalid, err)
		}
		return nil
	})
} // #nosec G115 -- immutable materializer command limit validated positive <=4MiB.
func pollGraphProcessSubmission(ctx context.Context, h *allocationHost, proposal allocationProposal, baseline uint64, scanned int, commandBytes uint64, match func([]byte, uint64) error) (uint64, int, []byte, error, error) {
	applied := h.driver.Applied()
	for index := baseline + 1; index <= applied; index++ {
		if scanned >= 1024 {
			return baseline, scanned, nil, errLimit, nil
		}
		scanned++
		baseline = index
		entries, e := h.machine.store.Entries(index, index+1, commandBytes)
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
		if e := match(body, index); e != nil {
			return baseline, scanned, nil, nil, e
		}
		return baseline, scanned, body, nil, nil
	}
	return baseline, scanned, nil, nil, nil
}

type graphProcessPumpReply struct {
	allocation allocationReply
	cross      crossReply
	capacity   crossCapacityReply
	request    crossRequestReply
	cut        graphCutReply
	move       rangeMoveReply
	body       []byte
	err        error
}
type graphProcessChildPump struct {
	ctx        context.Context
	calls      chan *graphProcessPumpCall
	canceled   chan *graphProcessPumpCall
	transition *nativeLogicalTransitionOwner
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
func (p *graphProcessChildPump) Capacity(ctx context.Context, q allocationProposal, nonce [16]byte, limits graphCapacityLimits) (crossCapacityReply, error) {
	r, e := p.wait(&graphProcessPumpCall{ctx: ctx, kind: sourceCrossRead, proposal: q, cross: crossQuery{nonce: nonce}, capacity: true, capacityLimits: limits})
	return r.capacity, e
}
func (p *graphProcessChildPump) Request(ctx context.Context, q crossRequestQuestion) (crossRequestReply, error) {
	r, e := p.wait(&graphProcessPumpCall{ctx: ctx, kind: sourceRequestLookup, request: q})
	return r.request, e
}
func (p *graphProcessChildPump) Submit(ctx context.Context, q allocationProposal) (wire []byte, err error) {
	if p.transition != nil {
		ticket, actual := p.transition.begin(ctx, q)
		if actual != nil {
			return nil, actual
		}
		if ticket != nil {
			defer func() { ticket.finish(err) }()
			if ticket.receipt.Control.Side == "before-submit" {
				if err = ticket.hold(); err != nil {
					return nil, err
				}
			}
			var reply graphProcessPumpReply
			reply, err = p.wait(&graphProcessPumpCall{ctx: ticket.ctx, kind: sourceSubmit, proposal: q, transition: ticket})
			if err == nil && ticket.receipt.Control.Side == "after-applied" {
				err = ticket.hold()
			}
			return reply.body, err
		}
	}
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
	var logicalTransitions *nativeLogicalTransitionOwner
	switch os.Getenv("RHO_GRAPH_TRANSITION_PROCESS") {
	case "", "off":
	case "1":
		member, parseErr := strconv.ParseUint(os.Getenv("RHO_GRAPH_PROCESS_ID"), 10, 64)
		if parseErr != nil {
			t.Fatal(parseErr)
		}
		var openErr error
		logicalTransitions, openErr = newNativeLogicalTransitionOwner(ctx, h, member)
		if openErr != nil {
			t.Fatal(openErr)
		}
		pump.transition = logicalTransitions
		defer logicalTransitions.close()
	default:
		t.Fatal("unknown native live transition profile")
	}
	server, err := newGraphSourceServer(ctx, h, defaultGraphSourceLimits(h.machine.limits))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := server.close(); err != nil {
			t.Error(err)
		}
	}()
	if o.family != nil {
		server.lifetimeNow = o.family.now // Before any service, worker or lease publication.
	}
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
	var decisionChanged <-chan struct{}
	switch os.Getenv("RHO_GRAPH_DECISION_FAULT_PROCESS") {
	case "", "off":
	case "1":
		member, parseErr := strconv.ParseUint(os.Getenv("RHO_GRAPH_PROCESS_ID"), 10, 64)
		if parseErr != nil {
			t.Fatal(parseErr)
		}
		o.decisions, err = newNativeDecisionOwner(ctx, coordinator, member)
		if err != nil {
			t.Fatal(err)
		}
		coordinator.beforeDecision = o.decisions.hold
		decisionChanged = o.decisions.changed
		defer o.decisions.close()
	default:
		t.Fatal("unknown native decision fault profile")
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
	incidents := &nativeGraphCostIncidentSessions{ctx: ctx, coordinator: coordinator, spec: o.cost}
	defer func() {
		if err := incidents.close(); err != nil {
			t.Error(err)
		}
	}()
	columns := &nativeGraphCostColumnSessions{ctx: ctx, coordinator: coordinator, spec: o.cost, dir: os.Getenv("RHO_GRAPH_PROCESS_DIR")}
	workloadCtx := ctx
	if o.workload != nil {
		if o.workloadDeadline.IsZero() {
			t.Fatal("missing immutable whole workload deadline")
		}
		var stopWorkload context.CancelFunc
		workloadCtx, stopWorkload = context.WithDeadline(ctx, o.workloadDeadline)
		defer stopWorkload()
	}
	if o.workload != nil && o.workload.Layout == "columns" {
		profile := *o.workload
		columns.ctx = workloadCtx
		admission := o.workloadAdmission
		columns.policyProvider = func() (nativeColumnPolicy, error) { return nativeContentionColumnsPolicy(h, profile, admission) }
	}
	contention, err := newNativeContentionSessions(workloadCtx, coordinator, o, columns, o.workload, o.workloadRun)
	if err != nil {
		t.Fatal(err)
	}
	if o.cost != nil && o.cost.Version == 4 {
		deadline, deadlineErr := nativeGraphEdgeUniqueDeadlineFromEnv()
		if deadlineErr != nil {
			t.Fatal(deadlineErr)
		}
		var stopColumns context.CancelFunc
		columns.ctx, stopColumns = context.WithDeadline(ctx, deadline)
		defer stopColumns()
		columns.policyProvider = func() (nativeColumnPolicy, error) {
			if enabled, e := nativeGraphEdgeUniqueProcessProfile(o.cost); e != nil || !enabled {
				return nativeColumnPolicy{}, errors.Join(errInvalid, e)
			}
			_, admission, e := nativeGraphEdgeUniqueAdmission(*o.cost)
			if e != nil {
				return nativeColumnPolicy{}, e
			}
			policy, e := nativeGraphEdgeUniqueColumnPolicy(h)
			policy.Admission = admission
			return policy, e
		}
	}
	var feed *nativeFeedSessions
	if o.cost != nil && o.cost.Version == 5 {
		spec, e := nativeFeedSpecFromEnv()
		if e != nil {
			t.Fatal(e)
		}
		admission, digest, e := nativeFeedAdmissionFromEnv(spec)
		if e != nil {
			t.Fatal(e)
		}
		var stopFeed context.CancelFunc
		columns.ctx, stopFeed = context.WithDeadline(ctx, time.Unix(0, admission.DeadlineUnixNanos))
		defer stopFeed()
		columns.policyProvider = func() (nativeColumnPolicy, error) { return nativeFeedColumnPolicy(h, spec, admission, digest) }
		feed, e = newNativeFeedSessions(columns.ctx, coordinator, o.cost)
		if e != nil {
			t.Fatal(e)
		}
		defer func() {
			if e := feed.close(); e != nil {
				t.Error(e)
			}
		}()
	}
	defer func() {
		if err := columns.close(); err != nil {
			t.Error(err)
		}
	}()
	p05 := newP05ProcessSessions(ctx, certifiedSessions, o)
	defer func() {
		if err := p05.close(); err != nil {
			t.Error(err)
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
	var decisionBarrier *processRequest
	var decisionMask uint8
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
		if len(e.snapshotSends) != 0 && snapshots == nil {
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
				if active.capacity {
					if len(e.capacityReplies) > 0 {
						if len(e.capacityReplies) != 1 || e.capacityReplies[0].id != active.id || e.capacityReplies[0].err == nil && (e.capacityReplies[0].proof.owner != h || e.capacityReplies[0].proof.observation.nonce != active.cross.nonce) {
							return errInvalid
						}
						finish(active, graphProcessPumpReply{capacity: e.capacityReplies[0]})
						e.capacityReplies = nil
					}
					break
				}
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
		if len(e.capacityReplies) != 0 {
			return errInvalid
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
			if c.capacity {
				event, err = h.readCrossCapacityConflict(c.proposal, c.cross.nonce, c.capacityLimits)
			} else {
				event, err = h.ReadCrossPhase(c.cross.tx, c.cross.phase, c.cross.nonce)
			}
		case sourceRequestLookup:
			event, err = h.readCrossRequest(c.request)
		case sourceCutRead:
			event, err = h.ReadGraphCut(c.cut)
		case sourceSubmit:
			c.baseline = h.driver.Applied()
			if c.transition != nil {
				c.transition.proofBaseline = c.baseline
			}
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
		if active.transition != nil {
			observed, err := logicalTransitions.poll(h, active)
			if err != nil {
				finish(active, graphProcessPumpReply{err: err})
				return nil
			}
			if !observed {
				return nil
			}
		}
		if isRetainedFeedWire(active.proposal.wire) {
			next, scanned, body, operationErr, eventErr := pollGraphProcessFeedSubmission(active.ctx, h, active.proposal, active.baseline, active.scanned)
			active.baseline, active.scanned = next, scanned
			if eventErr != nil {
				return eventErr
			}
			if body != nil || operationErr != nil {
				finish(active, graphProcessPumpReply{body: body, err: operationErr})
			}
			return nil
		}
		if isGraphPinWire(active.proposal.wire) {
			next, scanned, body, operationErr, eventErr := pollGraphProcessPinSubmission(active.ctx, h, active.proposal, active.baseline, active.scanned)
			active.baseline, active.scanned = next, scanned
			if eventErr != nil {
				return eventErr
			}
			if body != nil || operationErr != nil {
				finish(active, graphProcessPumpReply{body: body, err: operationErr})
			}
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
		if feed != nil && r.op == processCostObserve && bytes.HasPrefix(r.data, []byte(nativeFeedMarker)) {
			q, err := nativeFeedInputDecode(r)
			if err == nil && (q.Action == "close" || q.Action == "snapshot-close") {
				return true
			}
		}
		if o.cost != nil && (o.cost.Version == 4 || o.cost.Version == 5) && nativeGraphEdgeUniqueColumnClose(r) {
			return true
		}
		if logicalTransitions != nil && r.op == processNativeTransitionControl {
			if c, err := decodeNativeTransitionPinControl(r.data); err == nil && (c.Action == "close" || c.Action == "cancel") {
				return true
			}
			if c, err := decodeNativeLogicalTransitionControl(r.data); err == nil && (c.Action == "release" || c.Action == "close") {
				return true
			}
		}
		if contention != nil && r.op == processCostObserve && bytes.HasPrefix(r.data, []byte(nativeContentionMarker)) {
			if q, err := decodeNativeContentionInput(r.data); err == nil {
				if q.Action == "close" || q.Action == "records-close" || q.Action == "columns" && nativeGraphCostColumnCloseControl(processRequest{op: processCostObserve, data: q.Column}) {
					return true
				}
			}
		}
		if o.cost != nil && (nativeGraphCostIncidentCloseControl(r) || nativeGraphCostColumnCloseControl(r)) {
			return true
		}
		if r.op != processSourceServe {
			return false
		}
		request, err := decodeGraphSourceRequest(r.data)
		if err != nil {
			return false
		}
		if request.kind == sourceClose {
			return true
		}
		if request.kind != sourceSubmit || !h.machine.retainedPins() {
			return false
		}
		proposal, err := decodeGraphSourceProposal(request.command)
		if err != nil || !isGraphPinWire(proposal.wire) {
			return false
		}
		command, err := decodeGraphPinCommand(proposal.wire)
		return err == nil && (command.phase == pinRelease || command.phase == pinExpire || command.phase == pinClose || command.phase == pinCancelIssue && durableColumnGraphAgreement(h.machine.SemanticContractID()))
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
			case processNativeTransitionControl:
				if r.index != 0 || r.entity != 0 || r.packet.From != 0 || r.packet.To != 0 || r.packet.Snapshot || len(r.packet.Payload) != 0 {
					e = errInvalid
				} else if bytes.HasPrefix(r.data, []byte(nativeTransitionMovePrepareMarker)) {
					out.aux, e = logicalTransitions.prepareMove(workerCtx, o.family, o.incarnation, r)
				} else if bytes.HasPrefix(r.data, []byte(nativeTransitionPinMarker)) {
					out.aux, e = logicalTransitions.servePin(workerCtx, coordinator, o.incarnation, r)
				} else {
					out.aux, e = logicalTransitions.serve(workerCtx, r.data)
				}
			case processP05Open, processP05Observe, processP05Coordinate, processP05Resume, processP05Close:
				out.aux, e = p05.serve(workerCtx, r)
			case processSourceServe:
				out.aux, e = service.serve(workerCtx, r.data)
			case processSourceCoordinate:
				if o.cost != nil && o.cost.Version == 4 && bytes.HasPrefix(r.data, []byte(nativeEdgeFreeze)) {
					out.aux, e = nativeGraphEdgeUniqueFreezePlan(workerCtx, coordinator, r.data[len(nativeEdgeFreeze):])
					break
				}
				if o.cost != nil && o.cost.Version == 4 && bytes.HasPrefix(r.data, []byte(nativeEdgeRegister)) {
					out.aux, e = nativeGraphEdgeUniqueRegisterPlan(workerCtx, coordinator, r.data[len(nativeEdgeRegister):])
					break
				}
				var original partitionGraphCommand
				original, e = decodePartitionGraphCommand(r.data, h.machine.limits)
				if e == nil {
					var result crossProcessResult
					result, e = coordinator.Run(workerCtx, original.request.id, original.request.operations, original.request.revision.Provenance(), original.request.claims)
					if e != nil && o.cost != nil && o.cost.Version == 4 {
						refusal, refusalErr := nativeGraphEdgeUniqueRefusalWire(coordinator, r.data, result, e)
						if refusalErr != nil {
							e = errors.Join(e, refusalErr)
						} else if len(refusal) != 0 {
							out.changes = refusal
						}
					}
					if e == nil && o.cost != nil {
						out.changes, e = nativeGraphCostCoordinatorProofWire(workerCtx, h, result)
					}
					if result.resume.request != (requestID{}) {
						var encodeErr error
						out.aux, encodeErr = encodeGraphProcessCoordinatorResult(result, e)
						e = errors.Join(e, encodeErr)
					}
				}
			case processRangeRun, processRangeResume:
				out.aux, e = o.family.moveCall(workerCtx, h, o.incarnation, moveCoordinator, r)
			case processPairedPendingOpen:
				out.aux, e = pairedGraphFaultPendingOpen(workerCtx, certifiedSessions, r)
			case processCostObserve:
				if o.cost == nil {
					e = errInvalid
				} else if feed != nil && bytes.HasPrefix(r.data, []byte(nativeFeedMarker)) {
					out.aux, e = feed.serve(workerCtx, r)
				} else if contention != nil && bytes.HasPrefix(r.data, []byte(nativeContentionMarker)) {
					out.aux, e = contention.serve(workerCtx, r)
				} else if (o.cost.Version == 4 || o.cost.Version == 5) && bytes.HasPrefix(r.data, []byte(nativeEdgeColumns)) {
					out.aux, e = nativeGraphEdgeUniqueColumnServe(workerCtx, columns, r)
				} else if bytes.HasPrefix(r.data, []byte(nativeColumnsMarker)) {
					out.aux, e = columns.serve(workerCtx, r)
				} else if bytes.HasPrefix(r.data, []byte(nativeIncidentMarker)) {
					out.aux, e = incidents.serve(workerCtx, r)
				} else if o.cost.Version == 4 && bytes.HasPrefix(r.data, []byte(nativeEdgeObserve)) {
					out.aux, e = nativeGraphEdgeUniqueObserve(workerCtx, certifiedSessions, *o.cost, r)
				} else if o.cost.Version == 4 && bytes.HasPrefix(r.data, []byte(nativeEdgeRecords)) {
					out.aux, e = nativeGraphEdgeUniqueRecords(workerCtx, h, r)
				} else if len(r.data) != 0 {
					out.aux, e = nativeGraphCostIndexObserve(workerCtx, certifiedSessions, r)
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
		ackPoll, markerErr := nativeGraphCostAckPoll(r, o.cost != nil)
		err = errors.Join(err, markerErr)
		if r.op != processClose && !ackPoll {
			var e error
			if o.cost != nil && r.op != processStateOp {
				// The cost embedding requests diagnostics explicitly with State.
				// Native operations and retained Outcome/CDC remain authoritative;
				// repeated diagnostic checkpoint copies are not part of their API.
				_, e = h.machine.store.LastIndex()
			} else {
				response.stats, response.cutID, response.manifestID, e = processStats(h.machine.store)
			}
			err = errors.Join(err, e)
		}
		response.applied = h.driver.Applied()
		if snapshots == nil || r.op != processBuild && r.op != processNext {
			response.ready = o.ready
		}
		response.code, response.detail = processError(err)
		if code := nativeFeedProfileResponseCode(r, feed != nil, o.cost != nil && (o.cost.Version == 4 || o.cost.Version == 5), err); code != 0 {
			response.code = code
		}
		if durableColumnGraphAgreement(h.machine.SemanticContractID()) && r.op == processCostObserve {
			columnRequest := r
			if contention != nil && bytes.HasPrefix(r.data, []byte(nativeContentionMarker)) {
				if q, inputErr := decodeNativeContentionInput(r.data); inputErr == nil && q.Action == "columns" {
					columnRequest.data = q.Column
				}
			}
			if _, inputErr := nativeGraphCostColumnDecode(columnRequest); inputErr == nil {
				if code := nativeColumnProcessErrorCode(err); code != 0 {
					response.code = code
				}
			}
		}
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
		if err := contention.close(); err != nil {
			t.Error("contention owner cleanup", err)
		}
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
		case <-decisionChanged:
			if decisionBarrier != nil {
				body, err := o.decisions.ready(decisionMask)
				if body != nil || err != nil {
					request := *decisionBarrier
					decisionBarrier = nil
					if err := respond(request, processResponse{aux: body}, err); err != nil {
						t.Fatal(err)
					}
				}
			}
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
			case processPublish:
				if o.family == nil || r.index != 0 || r.entity != 0 || len(r.data) != 0 || r.packet.From != 0 || r.packet.To != 0 || r.packet.Snapshot || len(r.packet.Payload) != 0 {
					e = errInvalid
				} else {
					h.mu.Lock()
					if h.unavailableLocked() {
						e = replica.ErrUnavailable
					} else {
						e = h.machine.store.PublishSnapshot()
					}
					h.mu.Unlock()
				}
			case processOffers, processBuild, processSeal, processManifest, processNext, processFeedback, processBeginImport, processAppend, processVerify, processPrepare, processActivate, processImportClose:
				response, event, e = pairedSnapshotProcessEvent(ctx, h, snapshots, r)
			case processPairedConnections:
				response.aux, e = pairedGraphFaultConnections(r.entity, services, server)
			case processPairedReconnect:
				if logicalTransitions != nil && o.family != nil && moveCoordinator != nil {
					coordinator, e = nativeTransitionMoveReconnect(ctx, h, coordinator, certifiedSessions, moveCoordinator, server, r, len(workers)+len(paused)+len(controlWorkers), len(queue), active != nil)
				} else {
					coordinator, e = pairedGraphFaultReconnect(ctx, h, coordinator, certifiedSessions, moveCoordinator, server, r, len(workers)+len(paused)+len(controlWorkers), len(queue), active != nil)
				}
				if e == nil {
					certifiedSessions.coordinator = coordinator
					incidents.coordinator = coordinator
					columns.coordinator = coordinator
					if contention != nil {
						contention.coordinator = coordinator
					}
					if feed != nil {
						feed.coordinator = coordinator
					}
				}
			case processCostRecords:
				if o.cost == nil {
					e = errInvalid
				} else {
					response.aux, e = nativeGraphCostRecordsWire(ctx, h, r)
				}
			case processCostRecord:
				if o.cost == nil {
					e = errInvalid
				} else {
					if len(r.data) != 0 {
						response.aux, e = nativeGraphCostRequestRecordWire(ctx, h, r)
					} else {
						response.aux, e = nativeGraphCostRecordWire(ctx, h, r.index)
					}
				}
			case processLifecycleRecord:
				if os.Getenv("RHO_GRAPH_PAIRED_PROCESS") == "1" {
					response.aux, e = pairedGraphFaultRecordWire(ctx, h, r.index)
				} else {
					response.aux, e = nativeLifecycleRecordWire(ctx, h, r.index)
				}
			case processNativeFaultControl:
				if r.index != 0 || r.entity != 0 {
					e = errInvalid
				} else {
					response.aux, e = o.fault.control(r.data)
				}
			case processNativeDecisionControl:
				if r.index != 0 || r.entity != 0 || r.packet.From != 0 || r.packet.To != 0 || r.packet.Snapshot || len(r.packet.Payload) != 0 {
					e = errInvalid
				} else {
					var mask uint8
					response.aux, mask, event, e = o.decisions.control(r.data)
					if e == nil && mask != 0 {
						if decisionBarrier != nil {
							e = errLimit
						} else {
							request := r
							decisionBarrier = &request
							decisionMask = mask
							async = true
						}
					}
				}
			case processNativeFaultFamilyControl:
				if r.index != 0 || r.entity != 0 || r.packet.From != 0 || r.packet.To != 0 || r.packet.Snapshot || len(r.packet.Payload) != 0 {
					e = errInvalid
				} else {
					response.aux, e = o.family.control(r.data)
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
			case processP05Proposal:
				var p allocationProposal
				p, e = p05.proposal(ctx, r)
				if e == nil {
					response.aux, e = encodeCertifiedProcessProposal(p)
				}
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
				ackPoll, markerErr := nativeGraphCostAckPoll(r, o.cost != nil)
				if markerErr != nil {
					e = markerErr
					break
				}
				if ackPoll {
					// Keep Store closed/poisoned failures, but do not load/hash/copy
					// diagnostic images. respond returns real volatile Applied only;
					// the caller still requires the original retained Outcome/CDC.
					_, e = h.machine.store.LastIndex()
					break
				}
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
				if decisionBarrier != nil && decisionBarrier.sequence == r.index {
					request := *decisionBarrier
					decisionBarrier = nil
					if err := respond(request, processResponse{}, context.Canceled); err != nil {
						t.Fatal(err)
					}
				}
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
			case processNativeTransitionControl, processP05Open, processP05Observe, processP05Coordinate, processP05Resume, processP05Close, processSourceServe, processSourceCoordinate, processSourceObserve, processSourceCertifiedOpen, processSourceCertifiedObserve, processSourceCertifiedClose, processRangeRun, processRangeResume, processRangeCertifiedDetail, processCertifiedIndexObserve, processLifecycleObserve, processPairedPendingOpen, processCostObserve:
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
				e = errors.Join(feed.close(), columns.close())
				if snapshots != nil {
					h.mu.Lock()
					e = errors.Join(e, snapshots.close(h.driver))
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
