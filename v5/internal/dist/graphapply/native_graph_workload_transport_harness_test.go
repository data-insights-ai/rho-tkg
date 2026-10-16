package graphapply

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/dist/replica"
)

// The fresh workload owns its transport. These are injected delays on the six
// actual child pipes, not measurements of a physical LAN/WAN or host-wide tc.
type nativeWorkloadTransportSpec struct {
	Kind            string `json:"kind"`
	TargetRTTMillis uint64 `json:"targetRTTMillis"`
	JitterMillis    uint64 `json:"jitterMillis"`
	Seed            uint64 `json:"seed"`
}

func (s nativeWorkloadTransportSpec) validate() error {
	if s.Seed == 0 || s.JitterMillis != 0 {
		return errInvalid
	}
	if s.Kind == "local" && s.TargetRTTMillis == 0 {
		return nil
	}
	if s.Kind != "injected-rtt" || s.TargetRTTMillis != 1 && s.TargetRTTMillis != 20 && s.TargetRTTMillis != 100 {
		return errInvalid
	}
	return nil
}

// Fixed ownership, admitted before allocating the owner. Payloads remain in the
// existing queue byte ledger while delayed; there is no second payload copy or
// goroutine per packet. The ready heap never exceeds the existing128 frame cap.
const nativeWorkloadTransportMetadata = 32 << 10

type nativeWorkloadDelayLedger struct {
	Admitted, Completed, Canceled, Bytes           uint64
	ConfiguredNanos, ObservedNanos, DeviationNanos uint64
	MinimumObservedNanos, MaximumObservedNanos     uint64
}

type nativeWorkloadTransportReceipt struct {
	Version                          uint8
	Spec                             nativeWorkloadTransportSpec
	Raft, SourceRequest, SourceReply nativeWorkloadDelayLedger
	MaximumHeldFrames                uint64
	MaximumHeldBytes                 uint64
	MetadataBytes                    uint64
	PipeWrites, PipeReads            nativeWorkloadPipeLedger
	Scope                            string
}

type nativeWorkloadPipeLedger struct {
	AttemptedFrames, CompleteFrames, FailedFrames uint64
	PrepaidBytes, ActualBytes                     uint64
}

type nativeWorkloadTransport struct {
	ctx                             context.Context
	spec                            nativeWorkloadTransportSpec
	started                         time.Time
	maximum                         uint64
	mu                              sync.Mutex
	ledger                          [3]nativeWorkloadDelayLedger
	pipes                           [2]nativeWorkloadPipeLedger
	err                             error
	held, maximumHeld, maximumBytes uint64 // guarded by the hub mutex
	closed                          bool
	heap                            [processPacketCount]graphProcessPacketJob // packet loop owns this array
	n                               int
}

// Count all actual parent/child frame bytes, including framing, asynchronous
// Raft/source traffic, error replies, cancellation and Close. This layer overlaps
// the native payload counters; adding the two would double count traffic.
func (p *nativeWorkloadTransport) pipeGrant(kind int, bytes uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if kind < 0 || kind > 1 || bytes == 0 || bytes > processFrameBytes+4 || p.pipes[kind].AttemptedFrames >= p.maximum || !nativeColumnAdd(p.pipes[kind].PrepaidBytes, bytes) {
		return errLimit
	}
	if p.err != nil {
		return p.err
	}
	l := &p.pipes[kind]
	l.AttemptedFrames++
	l.PrepaidBytes += bytes
	return nil
}

func (p *nativeWorkloadTransport) pipeBytes(kind int, n int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	l := &p.pipes[kind]
	if n < 0 || l.ActualBytes > l.PrepaidBytes || uint64(n) > l.PrepaidBytes-l.ActualBytes { // #nosec G115 -- nonnegative delivered count, checked here.
		p.err = errCorrupt
		return p.err
	}
	l.ActualBytes += uint64(n) // #nosec G115 -- admitted delivered count.
	return nil
}

func (p *nativeWorkloadTransport) pipeFinish(kind int, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err != nil {
		p.pipes[kind].FailedFrames++
	} else {
		p.pipes[kind].CompleteFrames++
	}
}

type nativeWorkloadPipeWriter struct {
	writer io.Writer
	owner  *nativeWorkloadTransport
}

func (w nativeWorkloadPipeWriter) Write(b []byte) (int, error) {
	n, err := w.writer.Write(b)
	return n, errors.Join(err, w.owner.pipeBytes(0, n))
}

type nativeWorkloadPipeReader struct {
	reader io.Reader
	owner  *nativeWorkloadTransport
}

func (r nativeWorkloadPipeReader) Read(b []byte) (int, error) {
	n, err := r.reader.Read(b)
	return n, errors.Join(err, r.owner.pipeBytes(1, n))
}

func (p *nativeWorkloadTransport) writeFrame(writer io.Writer, wire []byte) error {
	if err := p.pipeGrant(0, uint64(len(wire)+4)); err != nil { // #nosec G115 -- owned bounded process frame.
		return err
	}
	err := writeProcessFrame(nativeWorkloadPipeWriter{writer, p}, wire)
	p.pipeFinish(0, err)
	return err
}

func (p *nativeWorkloadTransport) readFrame(reader io.Reader) ([]byte, error) {
	if err := p.pipeGrant(1, processFrameBytes+4); err != nil {
		return nil, err
	}
	wire, err := readProcessFrame(nativeWorkloadPipeReader{reader, p})
	p.pipeFinish(1, err)
	return wire, err
}

func newNativeWorkloadTransport(ctx context.Context, spec nativeWorkloadTransportSpec, maximum uint64) (*nativeWorkloadTransport, error) {
	// The finite count bounds frame ownership/work, not wall-clock latency.
	// Each aggregate addition is checked before admission. Quotas allocate no
	// storage proportional to their value or to the dataset size.
	if ctx == nil || spec.validate() != nil || maximum == 0 || maximum > 1<<41 || nativeWorkloadTransportMetadata >= processQueueBytes {
		return nil, errInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	deadline, bounded := ctx.Deadline()
	if !bounded || time.Until(deadline) > nativeContentionWholeSeconds*time.Second {
		return nil, errInvalid
	}
	return &nativeWorkloadTransport{ctx: ctx, spec: spec, started: time.Now(), maximum: maximum}, nil
}

func (p *nativeWorkloadTransport) oneWay() time.Duration {
	return time.Duration(p.spec.TargetRTTMillis) * time.Millisecond / 2 // #nosec G115 -- admitted0/1/20/100ms.
}

func (p *nativeWorkloadTransport) admit(kind int, bytes int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if kind < 0 || kind >= len(p.ledger) || bytes < 0 || bytes > processFrameBytes || p.ledger[kind].Admitted >= p.maximum {
		return errLimit
	}
	if err := p.ctx.Err(); err != nil {
		return err
	}
	if p.err != nil {
		return p.err
	}
	l := &p.ledger[kind]
	configured := uint64(p.oneWay())                                                     // #nosec G115 -- admitted nonnegative half RTT.
	if uint64(bytes) > ^uint64(0)-l.Bytes || configured > ^uint64(0)-l.ConfiguredNanos { // #nosec G115 -- nonnegative bounded delivered frame.
		p.err = errLimit
		return p.err
	}
	l.Admitted++
	l.Bytes += uint64(bytes) // #nosec G115 -- nonnegative bounded frame; checked before admission.
	l.ConfiguredNanos += configured
	return nil
}

func (p *nativeWorkloadTransport) finish(kind int, started time.Time, err error) error {
	observed := uint64(max(time.Duration(0), time.Since(started))) // #nosec G115 -- actual monotone nonnegative duration.
	p.mu.Lock()
	defer p.mu.Unlock()
	l := &p.ledger[kind]
	if err != nil {
		l.Canceled++
		return nil
	}
	configured := uint64(p.oneWay()) // #nosec G115 -- admitted nonnegative half RTT.
	var deviation uint64
	if observed >= configured {
		deviation = observed - configured
	} else {
		deviation = configured - observed
	}
	if observed > ^uint64(0)-l.ObservedNanos || deviation > ^uint64(0)-l.DeviationNanos {
		p.err = errLimit
		return p.err
	}
	l.Completed++
	l.ObservedNanos += observed
	l.DeviationNanos += deviation
	if l.Completed == 1 {
		l.MinimumObservedNanos = observed
	} else {
		l.MinimumObservedNanos = min(l.MinimumObservedNanos, observed)
	}
	l.MaximumObservedNanos = max(l.MaximumObservedNanos, observed)
	return nil
}

func (p *nativeWorkloadTransport) wait(ctx context.Context, kind int, bytes int) (err error) {
	if ctx == nil {
		return errInvalid
	}
	if err := p.admit(kind, bytes); err != nil {
		return err
	}
	started := time.Now()
	defer func() { err = errors.Join(err, p.finish(kind, started, err)) }()
	if err = errors.Join(ctx.Err(), p.ctx.Err()); err != nil {
		return err
	}
	if p.oneWay() == 0 {
		return nil
	}
	timer := time.NewTimer(p.oneWay())
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.ctx.Done():
		return p.ctx.Err()
	case <-timer.C:
		return errors.Join(ctx.Err(), p.ctx.Err())
	}
}

// Exactly one half RTT before request delivery and one before response delivery.
// Raft frames instead each incur one half RTT, so an actual request/ACK pair
// has one target RTT. Source processing and queueing remain separately visible.
func (p *nativeWorkloadTransport) before(ctx context.Context, _, _ uint64, request []byte) error {
	return p.wait(ctx, 1, len(request))
}

func (p *nativeWorkloadTransport) after(ctx context.Context, _, _ uint64, _ []byte, reply []byte) error {
	return p.wait(ctx, 2, len(reply))
}

func (p *nativeWorkloadTransport) install(h *graphProcessHub) error {
	if p == nil || h == nil {
		return errInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.workload != nil || h.beforeRelay != nil || h.afterRelay != nil || h.packetFilter != nil || h.nativeFaultSchedule != nil || h.delivered != 0 || h.inFlight != 0 || h.packetBytes != 0 || h.snapshotBytes != 0 || len(h.packets) != 0 {
		return errInvalid
	}
	if err := errors.Join(h.ctx.Err(), p.ctx.Err()); err != nil {
		return err
	}
	h.workload, h.workloadBytes, h.costStarted = p, nativeWorkloadTransportMetadata, p.started
	h.beforeRelay, h.afterRelay = p.before, p.after
	return nil
}

// The hub mutex spans admission AND the nonblocking queue handoff, so teardown
// cannot observe an empty channel while an already charged sender still owns a
// packet outside it. No source callback or native child operation runs here.
func (p *nativeWorkloadTransport) enqueueLocked(h *graphProcessHub, origin uint64, packet replica.Packet) error {
	if p.closed || packet.Snapshot {
		return errInvalid
	}
	if err := errors.Join(h.ctx.Err(), p.ctx.Err()); err != nil {
		return err
	}
	cost := cap(packet.Payload) + 64
	if p.held >= processPacketCount || cost > processQueueBytes-h.packetBytes-h.snapshotBytes-h.workloadBytes {
		return errLimit
	}
	if err := p.admit(0, len(packet.Payload)); err != nil {
		return err
	}
	job := graphProcessPacketJob{origin: origin, packet: packet, enqueuedNanos: time.Since(p.started).Nanoseconds()}
	h.packetBytes += cost
	p.held++
	select {
	case h.packets <- job:
		p.maximumHeld = max(p.maximumHeld, p.held)
		p.maximumBytes = max(p.maximumBytes, uint64(h.packetBytes+h.workloadBytes)) // #nosec G115 -- checked nonnegative bounded queue ownership.
		return nil
	default:
		h.packetBytes -= cost
		p.held--
		return errors.Join(errLimit, p.finish(0, p.started, context.Canceled))
	}
}

func (p *nativeWorkloadTransport) due(job graphProcessPacketJob) int64 {
	return job.enqueuedNanos + int64(p.oneWay())
}

func (p *nativeWorkloadTransport) push(job graphProcessPacketJob) error {
	if p.n >= len(p.heap) {
		return errLimit
	}
	i := p.n
	p.n++
	for i > 0 {
		parent := (i - 1) / 2
		if p.due(p.heap[parent]) <= p.due(job) {
			break
		}
		p.heap[i] = p.heap[parent]
		i = parent
	}
	p.heap[i] = job
	return nil
}

func (p *nativeWorkloadTransport) pop() graphProcessPacketJob {
	first := p.heap[0]
	p.n--
	last := p.heap[p.n]
	p.heap[p.n] = graphProcessPacketJob{}
	if p.n == 0 {
		return first
	}
	i := 0
	for child := 1; child < p.n; child = 2*i + 1 {
		if child+1 < p.n && p.due(p.heap[child+1]) < p.due(p.heap[child]) {
			child++
		}
		if p.due(last) <= p.due(p.heap[child]) {
			break
		}
		p.heap[i] = p.heap[child]
		i = child
	}
	p.heap[i] = last
	return first
}

// Only the original packet-loop owner calls next. Enqueue has already reserved
// every frame and byte before channel/heap ownership, including delayed frames.
func (p *nativeWorkloadTransport) next(h *graphProcessHub) (graphProcessPacketJob, error) {
	for {
		if err := errors.Join(h.ctx.Err(), p.ctx.Err()); err != nil {
			return graphProcessPacketJob{}, err
		}
		var ready <-chan time.Time
		var timer *time.Timer
		if p.n > 0 {
			remaining := time.Duration(p.due(p.heap[0]) - time.Since(p.started).Nanoseconds())
			if remaining <= 0 {
				job := p.heap[0]
				if err := p.finish(0, p.started.Add(time.Duration(job.enqueuedNanos)), nil); err != nil {
					return graphProcessPacketJob{}, err
				}
				p.pop()
				return job, nil
			}
			timer = time.NewTimer(remaining)
			ready = timer.C
		}
		select {
		case <-h.ctx.Done():
			if timer != nil {
				timer.Stop()
			}
			return graphProcessPacketJob{}, h.ctx.Err()
		case <-p.ctx.Done():
			if timer != nil {
				timer.Stop()
			}
			return graphProcessPacketJob{}, p.ctx.Err()
		case <-ready:
		case job := <-h.packets:
			if err := p.push(job); err != nil {
				if timer != nil {
					timer.Stop()
				}
				return graphProcessPacketJob{}, err
			}
		}
		if timer != nil {
			timer.Stop()
		}
	}
}

// Called by the packet-loop owner after cancellation/failure, after preventing
// further queue admission. Every retained payload is retired exactly once.
func (p *nativeWorkloadTransport) close(h *graphProcessHub) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if p.closed {
		return
	}
	p.closed = true
	retire := func(job graphProcessPacketJob) {
		h.packetBytes -= cap(job.packet.Payload) + 64
		p.held--
		_ = p.finish(0, p.started, context.Canceled)
	}
	for p.n > 0 {
		retire(p.pop())
	}
	for {
		select {
		case job := <-h.packets:
			retire(job)
		default:
			h.workloadBytes = 0
			return
		}
	}
}

func (p *nativeWorkloadTransport) receipt(h *graphProcessHub) nativeWorkloadTransportReceipt {
	h.mu.Lock()
	held, bytes := p.maximumHeld, p.maximumBytes
	h.mu.Unlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	return nativeWorkloadTransportReceipt{Version: 1, Spec: p.spec, Raft: p.ledger[0], SourceRequest: p.ledger[1], SourceReply: p.ledger[2], MaximumHeldFrames: held, MaximumHeldBytes: bytes, MetadataBytes: nativeWorkloadTransportMetadata, PipeWrites: p.pipes[0], PipeReads: p.pipes[1], Scope: "owned six-child pipes; separate one-way Raft and half request/reply delays; Completed means delay elapsed, not native delivery/ACK; observed delay includes queueing; zero configured jitter does not imply zero observed jitter; Pipe ActualBytes counts actual framing/payload IO including partial failures/Close and overlaps native payload counters; no physical network claim"}
}
