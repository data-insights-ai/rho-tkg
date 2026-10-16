package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

// A conjunction selector names one actual source submission. Member is the
// observed native process, not a presumed partition leader or source voter.
// Original/authority stays on the real source caller; this owner only arms the
// existing native batch diagnostic before forwarding those exact source bytes.
type nativeConjunctionSelector struct {
	Actor, Partition, Member uint64
	Phase                    crossPhase
	Occurrence               uint8 // Zero is the legacy first occurrence; fresh rows use1..16.
	Kind                     raftlog.StableStorageKind
	Side                     string
	Fence                    bool            // Fresh P02 local fence; never a cross-phase alias.
	FenceScope               allocationScope // Genuine earlier configured scope, by value.
}

func (s nativeConjunctionSelector) valid() bool {
	phase := s.Phase >= crossRegister && s.Phase <= crossResolve && s.FenceScope == (allocationScope{})
	if s.Fence {
		phase = s.Phase == 0 && s.Partition == 3 && s.Actor == s.Member && s.Member <= 3 && s.Occurrence <= 1 && s.FenceScope.valid() && s.FenceScope.partition == 3
	}
	return s.Actor >= 1 && s.Actor <= 6 && s.Member >= 1 && s.Member <= 6 && (s.Partition == 3 && s.Member <= 3 || s.Partition == 8 && s.Member >= 4) && phase && s.Occurrence <= 16 && (s.Kind == raftlog.StableStorageReady || s.Kind == raftlog.StableStorageApplication) && (s.Side == "before" || s.Side == "after")
}

// One bounded exact native target is retained. Installation and teardown are
// scoped LIFO with the semantic parent's own relay owner: an existing before
// callback runs first; the after callback is never replaced. This permits the
// parent to retain its genuine successful phase capture independently of failed
// or unknown native attempts. No callback calls Store or manufactures an ACK.
type nativeConjunctionBatchGate struct {
	mu                                 sync.Mutex
	ctx                                context.Context
	cancel                             context.CancelFunc
	f                                  *pairedGraphFaultNetwork
	hub                                *graphProcessHub
	target                             *graphProcess
	selector                           nativeConjunctionSelector
	dir                                string
	owner                              nativeBatchReceipt
	arm                                nativeBatchReceipt
	selected                           []byte
	reads                              uint64
	closed                             bool
	evidenceTaken                      bool
	refused                            bool
	candidates, encounters, decodeWork uint64
	lost                               chan struct{}
	lostOnce                           sync.Once
	closeOnce                          sync.Once
	priorBefore                        func(context.Context, uint64, uint64, []byte) error
	priorFailed                        func(graphProcessPacketJob, error) bool
	captureStep                        bool // Immutable before callback publication; terminal only.
	stepLoss                           *nativeP06TerminalStepLoss
}

func newNativeConjunctionBatchGate(ctx context.Context, f *pairedGraphFaultNetwork, selector nativeConjunctionSelector, run [16]byte, deadline int64, captureStep ...bool) (*nativeConjunctionBatchGate, error) {
	if ctx == nil || f == nil || f.n == nil || !selector.valid() || run == ([16]byte{}) || len(captureStep) > 1 {
		return nil, errInvalid
	}
	actual, bounded := ctx.Deadline()
	if !bounded || actual.UnixNano() > deadline || ctx.Err() != nil {
		return nil, errors.Join(errInvalid, ctx.Err())
	}
	target := f.n.all[selector.Member]
	if target == nil || target.mux == nil || target.cmd == nil || target.cmd.Process == nil || target.closed {
		return nil, errInvalid
	}
	owned, cancel := context.WithCancel(ctx)
	g := &nativeConjunctionBatchGate{ctx: owned, cancel: cancel, f: f, hub: target.mux.hub, target: target, selector: selector, dir: filepath.Join(f.n.dir, fmt.Sprint(selector.Member)), lost: make(chan struct{})}
	g.captureStep = len(captureStep) == 1 && captureStep[0]
	if err := reserveNativeConjunctionGate(f, g); err != nil {
		cancel()
		return nil, err
	}
	owner, err := nativeBatchCall(ctx, target, nativeBatchControl{Version: 1, Action: "inspect"})
	if err != nil || owner.State != "idle" || owner.Run != run || owner.Member != selector.Member || owner.PID != uint64(target.cmd.Process.Pid) || owner.DeadlineUnixNanos > deadline {
		cancel()
		f.mu.Lock()
		f.conjunctionGate = nil
		f.mu.Unlock()
		return nil, errors.Join(errCorrupt, err)
	}
	g.owner = owner
	g.hub.mu.Lock()
	g.priorBefore, g.priorFailed = g.hub.beforeRelay, g.hub.failedStep
	g.hub.beforeRelay, g.hub.failedStep = g.before, g.failed
	g.hub.mu.Unlock()
	context.AfterFunc(owned, g.close) // Publication precedes expiry teardown.
	return g, nil
}

func reserveNativeConjunctionGate(f *pairedGraphFaultNetwork, g *nativeConjunctionBatchGate) error {
	if f == nil || g == nil {
		return errInvalid
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.conjunctionGate != nil {
		return errLimit
	}
	f.conjunctionGate = g // Sole fixture slot, before native effects.
	return nil
}

// Fresh terminal/P02 parent only: pre-admit/hash an actual source-owned command
// as a diagnostic selector before its ordinary Submit. Neither this method nor its
// copied bytes import/reconstruct any private proposal authority or stamps.
func (g *nativeConjunctionBatchGate) armObserved(ctx context.Context, proposal allocationProposal) error {
	if g == nil || ctx == nil {
		return errInvalid
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed || g.ctx.Err() != nil || ctx.Err() != nil {
		return errors.Join(errCrossProcessPending, g.ctx.Err(), ctx.Err())
	}
	if g.arm.Sequence != 0 || g.refused || g.candidates >= 128 || len(proposal.wire) == 0 || len(proposal.wire) > nativeLogicalTransitionBodyBytes {
		return errLimit
	}
	g.candidates++
	g.decodeWork += 4096 + 32*processFrameBytes
	if !proposal.target.valid() {
		return errInvalid
	}
	if g.selector.Fence {
		command, err := decodeGraphCutCommand(proposal.wire)
		if err != nil || command.phase != graphCutFence || command.scope != proposal.target || command.scope != g.selector.FenceScope {
			return errors.Join(errInvalid, err)
		}
	} else {
		command, _, err := decodeCrossCommand(proposal.wire, defaultMaterializerLimits())
		if err != nil || command.ns.graph != proposal.target.graph || command.ns.partition != proposal.target.partition || command.phase != g.selector.Phase || command.ns.partition != g.selector.Partition {
			return errors.Join(errInvalid, err)
		}
	}
	control := nativeBatchControl{Version: 1, Action: "arm", Cookie: g.owner.Cookie, Sequence: 1, Kind: g.selector.Kind, Side: g.selector.Side, CommandSHA256: sha256.Sum256(proposal.wire)}
	deadline, _ := g.ctx.Deadline()
	actualCtx, cancel := context.WithDeadline(ctx, deadline)
	stop := context.AfterFunc(g.ctx, cancel)
	defer func() { stop(); cancel() }()
	actual, err := nativeBatchCall(actualCtx, g.target, control)
	if err != nil || actual.State != "armed" || actual.Control != control || actual.Run != g.owner.Run || actual.PID != g.owner.PID || actual.DeadlineUnixNanos != g.owner.DeadlineUnixNanos {
		return errors.Join(errCorrupt, err)
	}
	g.selected = make([]byte, len(proposal.wire))
	copy(g.selected, proposal.wire)
	g.arm = actual
	if err := g.armProgram(proposal.wire); err != nil {
		return err
	}
	return nil
}

func (g *nativeConjunctionBatchGate) armProgram(command []byte) error {
	g.f.mu.Lock()
	program := g.f.protocolProgram
	g.f.mu.Unlock()
	if program != nil {
		return program.arm(g.selector, command)
	}
	return nil
}

func (g *nativeConjunctionBatchGate) before(ctx context.Context, origin, partition uint64, request []byte) error {
	if g.priorBefore != nil {
		if err := g.priorBefore(ctx, origin, partition, request); err != nil {
			return err
		}
	}
	if g.selector.Fence {
		return nil // Local genuine proposal is explicitly armed before Submit.
	}
	if origin != g.selector.Actor || partition != g.selector.Partition {
		return nil
	}
	// Only the fixed service header is inspected here. Other operations still
	// reach their ordinary full decoder and original endpoint/transport grants;
	// they cannot spend the separate full-Submit diagnostic candidate budget.
	if len(request) >= 5 && len(request) <= processFrameBytes && bytes.Equal(request[:4], []byte("GSQ\x01")) && request[4] >= sourceOpen && request[4] <= sourceReplacementRead && request[4] != sourceSubmit {
		return nil
	}
	g.mu.Lock()
	if g.arm.Sequence != 0 || g.refused {
		g.mu.Unlock()
		return nil
	}
	if g.candidates >= 128 || len(request) > processFrameBytes {
		g.refused = true
		g.mu.Unlock()
		return nil // Diagnostic exhaustion cannot invent a native refusal.
	}
	g.candidates++
	g.decodeWork += 4096 + 32*processFrameBytes // Full bounded envelope traversal, before decode.
	g.mu.Unlock()
	command, cross, err := nativeFaultPhase(request)
	if err != nil || !cross || command.phase != g.selector.Phase {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed || g.ctx.Err() != nil {
		return errors.Join(errCrossProcessPending, g.ctx.Err())
	}
	if g.arm.Sequence != 0 || g.refused {
		return nil
	}
	g.encounters++
	occurrence := max(uint64(1), uint64(g.selector.Occurrence))
	if g.encounters != occurrence {
		return nil
	}
	q, err := decodeGraphSourceRequest(request)
	if err != nil {
		return err
	}
	proposal, err := decodeGraphSourceProposal(q.command)
	if err != nil || len(proposal.wire) == 0 || len(proposal.wire) > nativeLogicalTransitionBodyBytes || command.ns.partition != partition {
		return errors.Join(errInvalid, err)
	}
	control := nativeBatchControl{Version: 1, Action: "arm", Cookie: g.owner.Cookie, Sequence: 1, Kind: g.selector.Kind, Side: g.selector.Side, CommandSHA256: sha256.Sum256(proposal.wire)}
	deadline, _ := g.ctx.Deadline()
	callCtx, cancel := context.WithDeadline(ctx, deadline)
	stop := context.AfterFunc(g.ctx, cancel)
	defer func() { stop(); cancel() }()
	actual, err := nativeBatchCall(callCtx, g.target, control)
	if err != nil || actual.State != "armed" || actual.Control != control || actual.Run != g.owner.Run || actual.PID != g.owner.PID || actual.DeadlineUnixNanos != g.owner.DeadlineUnixNanos {
		return errors.Join(errCorrupt, err)
	}
	// One64KiB source command and the existing8KiB diagnostic reply are the
	// fixed owned observation. Native owner pre-admits complete Ready hashing.
	g.selected = make([]byte, len(proposal.wire))
	copy(g.selected, proposal.wire)
	g.arm = actual
	if err := g.armProgram(proposal.wire); err != nil {
		return err
	}
	return nil
}

func (g *nativeConjunctionBatchGate) read(expected nativeBatchReceipt) (nativeBatchReceipt, error) {
	g.mu.Lock()
	if g.reads >= 129 {
		g.mu.Unlock()
		return nativeBatchReceipt{}, errLimit
	}
	g.reads++ // Prepay64KiB per8KiB actual file/read/decode/hash/canonical check.
	g.mu.Unlock()
	if err := g.ctx.Err(); err != nil {
		return nativeBatchReceipt{}, err
	}
	receipt, err := nativeBatchRead(g.dir, expected)
	return receipt, errors.Join(err, g.ctx.Err())
}

func (g *nativeConjunctionBatchGate) failed(job graphProcessPacketJob, actual error) bool {
	// The hub additionally checks its actual all-zero response before calling
	// this hook. Recheck the exact supported transport cause for direct callers.
	matched := g.ctx.Err() == nil && nativeBatchFailedStepAllowed(processResponse{}, actual) && job.packet.To == g.selector.Member && g.hub.peer(g.selector.Member).p == g.target
	g.mu.Lock()
	expected, closed := g.arm, g.closed
	g.mu.Unlock()
	if matched && !closed && expected.Sequence != 0 {
		if _, err := g.read(expected); err == nil {
			g.mu.Lock()
			if g.closed || g.ctx.Err() != nil || g.hub.peer(g.selector.Member).p != g.target {
				g.mu.Unlock()
				return false
			}
			if g.captureStep && g.stepLoss == nil {
				if len(job.packet.Payload) > processFrameBytes {
					g.refused = true
					g.mu.Unlock()
					return false
				}
				// One actual failed Step payload, separately prepaid BEFORE its
				// owned copy/hash. Ordinary frame/queue limits stay unchanged.
				observation := &nativeP06TerminalStepLoss{WorkGrant: 4*processFrameBytes + 8192}
				observation.Transport = actual.Error()
				if len(observation.Transport) > 4096 {
					g.refused = true
					g.mu.Unlock()
					return false
				}
				observation.Packet = job.packet
				g.stepLoss = observation
				g.stepLoss.Packet.Payload = make([]byte, len(job.packet.Payload))
				copy(g.stepLoss.Packet.Payload, job.packet.Payload)
				g.stepLoss.PayloadSHA256 = sha256.Sum256(job.packet.Payload)
			}
			g.f.mu.Lock()
			g.f.down[g.selector.Member] = true
			g.f.mu.Unlock()
			g.lostOnce.Do(func() { close(g.lost) })
			g.mu.Unlock()
			return true
		}
	}
	return g.priorFailed != nil && g.priorFailed(job, actual)
}

func (g *nativeConjunctionBatchGate) readCount() uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.reads
}

func (g *nativeConjunctionBatchGate) evidence() (nativeBatchReceipt, nativeBatchReceipt, []byte, uint64, error) {
	if g == nil {
		return nativeBatchReceipt{}, nativeBatchReceipt{}, nil, 0, errInvalid
	}
	g.mu.Lock()
	arm := g.arm
	if arm.Sequence == 0 || len(g.selected) == 0 || g.evidenceTaken || g.closed || g.refused {
		g.mu.Unlock()
		return nativeBatchReceipt{}, nativeBatchReceipt{}, nil, 0, errCrossProcessPending
	}
	// Transfer the one already-funded owned observation, not another hidden
	// clone on each diagnostic read. No subsequent candidate can re-arm it.
	selected := g.selected
	g.selected, g.evidenceTaken = nil, true
	g.mu.Unlock()
	receipt, err := g.read(arm)
	g.mu.Lock()
	reads := g.reads
	g.mu.Unlock()
	return arm, receipt, selected, reads, err
}

func (g *nativeConjunctionBatchGate) close() {
	if g == nil {
		return
	}
	g.closeOnce.Do(func() {
		g.cancel() // Active operation cancellation precedes state/relay locks.
		g.mu.Lock()
		g.closed = true
		g.mu.Unlock()
		g.hub.mu.Lock()
		g.hub.beforeRelay, g.hub.failedStep = g.priorBefore, g.priorFailed
		g.hub.mu.Unlock()
		g.f.mu.Lock()
		if g.f.conjunctionGate == g {
			g.f.conjunctionGate = nil
		}
		g.f.mu.Unlock()
	})
}

// Fresh parent calls retain the ordinary per-invocation caps, additionally
// bounded by the SAME original absolute deadline. Nil uses literal legacy APIs.
func (f *pairedGraphFaultNetwork) operationContext(limit time.Duration) (context.Context, context.CancelFunc) {
	if f.parent != nil {
		return context.WithTimeout(f.parent, limit)
	}
	return context.WithTimeout(f.t.Context(), limit)
}

func (f *pairedGraphFaultNetwork) requireActive() {
	if f.parent != nil && f.parent.Err() != nil {
		f.t.Fatal("original fixture operation deadline", f.parent.Err())
	}
}

func (f *pairedGraphFaultNetwork) call(p *graphProcess, request processRequest) processResponse {
	if f.parent == nil {
		return p.call(f.t, request)
	}
	f.requireActive()
	ctx, cancel := f.operationContext(10 * time.Second)
	defer cancel()
	response, transport := p.exchange(ctx, request)
	if err := errors.Join(transport, processResponseError(response), ctx.Err()); err != nil {
		f.t.Fatal("fresh fixture actual native RPC", request.op, err)
	}
	return response
}

func (f *pairedGraphFaultNetwork) groupEvent(group *tinyGraphTransport, member uint64, request processRequest) {
	if f.parent == nil {
		group.event(f.t, member, request)
		return
	}
	group.enqueue(f.t, f.call(group.nodes[member], request))
	f.drainGroup(group)
}

// This adapter owns no second transport or queue: it consumes the existing
// tiny transport's exact queue, counters, filters and real Step responses. Only
// an authenticated selected target loss may retire a delivery without ACKs.
func (f *pairedGraphFaultNetwork) drainGroup(group *tinyGraphTransport) {
	if f.parent == nil {
		group.drain(f.t)
		return
	}
	for len(group.queue) > 0 {
		f.requireActive()
		if group.delivered == processDeliveries {
			f.t.Fatal("bounded packet schedule exhausted")
		}
		packet := group.queue[0]
		group.queue[0] = replica.Packet{}
		group.queue = group.queue[1:]
		group.queuedBytes -= cap(packet.Payload) + 64
		group.delivered++
		if group.blocked[packet.From] || group.blocked[packet.To] || group.dropPacket != nil && group.dropPacket(packet) {
			continue
		}
		f.mu.Lock()
		dead := f.down[packet.From] || f.down[packet.To]
		gate := f.conjunctionGate
		f.mu.Unlock()
		if dead {
			continue
		}
		ctx, cancel := f.operationContext(10 * time.Second)
		response, transport := group.nodes[packet.To].exchange(ctx, processRequest{op: processStep, packet: packet})
		err := errors.Join(transport, processResponseError(response), ctx.Err())
		cancel()
		if err != nil {
			if gate != nil && nativeBatchFailedStepAllowed(response, err) && gate.failed(graphProcessPacketJob{origin: packet.From, packet: packet}, err) {
				continue
			}
			target := group.nodes[packet.To]
			stderr, overflow := target.stderr.state()
			f.t.Fatalf("fresh bounded native Step failed: op=%d packet=%d->%d snapshot=%t bytes=%d targetPID=%d code=%d causes=%x detail=%q stderr=%q stderrOverflow=%t: %v", processStep, packet.From, packet.To, packet.Snapshot, len(packet.Payload), target.cmd.Process.Pid, response.code, response.causeMask, response.detail, stderr, overflow, err)
		}
		group.enqueue(f.t, response)
	}
	f.requireActive() // Expiry is never a successful empty drain.
}

func (f *pairedGraphFaultNetwork) bootstrapRead(actor uint64, kind allocationObservationKind, recipient uint64) {
	if f.parent == nil {
		f.n.read(f.t, actor, kind, recipient)
		return
	}
	f.groupEvent(f.n.group(actor), actor, processRequest{op: processCertifiedRead, entity: uint64(kind), index: recipient})
	if !f.call(f.n.all[actor], processRequest{op: processStateOp}).ready {
		f.t.Fatal("actual request-bound bootstrap quorum read incomplete")
	}
}

func (f *pairedGraphFaultNetwork) bootstrapAction(actor, kind, index uint64, data []byte) {
	if f.parent == nil {
		f.n.action(f.t, actor, kind, index, data)
		return
	}
	response := f.call(f.n.all[actor], processRequest{op: processCertifiedAction, entity: kind, index: index, data: data})
	if kind == certifiedOpenRecipient {
		if len(response.aux) != 0 {
			f.t.Fatal("recipient open returned proposal")
		}
		return
	}
	proposal, err := decodeCertifiedProcessProposal(response.aux)
	if err != nil {
		f.t.Fatal("actual source-produced bootstrap proposal", err)
	}
	group, leader := f.n.groups[0], f.n.leaders[0]
	if proposal.target.partition == 8 {
		group, leader = f.n.groups[1], f.n.leaders[1]
	}
	ns, _ := certifiedProcessNamespace(leader)
	agreement := certifiedGraphSemanticContractID()
	if f.n.rangeMoves {
		agreement = rangeMoveSemanticContractID()
	}
	if pairedFaultConditionalFixture() {
		agreement = conditionalGraphSemanticContractID()
	}
	if [16]byte(proposal.target.graph) != ([16]byte{1}) || proposal.target.partition != ns.partition || proposal.target.semantic != agreement {
		f.t.Fatal("source bootstrap proposal escaped actual topology")
	}
	f.groupEvent(group, leader, processRequest{op: processPropose, data: response.aux})
	state := f.call(f.n.all[leader], processRequest{op: processStateOp})
	result := f.call(f.n.all[leader], processRequest{op: processEntryRecord, index: state.applied})
	original, err := decodeDeclaredOutcome(result.outcome, ns)
	valid := original.disposition == applied || kind == certifiedInstallGrant && original.disposition == controlRecovery
	if err != nil || original.hash != sha256.Sum256(proposal.wire) || original.index != state.applied || !valid || original.reason != reasonNone {
		f.t.Fatal("actual native bootstrap original", original, err)
	}
	for id, voter := range group.nodes {
		actual := f.call(voter, processRequest{op: processStateOp})
		stored := f.call(voter, processRequest{op: processEntryRecord, index: state.applied})
		if actual.applied != state.applied || !bytes.Equal(actual.image, state.image) || !bytes.Equal(stored.outcome, result.outcome) || !bytes.Equal(stored.changes, result.changes) || actual.stats[statViews] != 0 || actual.stats[statPinnedBytes] != 0 {
			f.t.Fatal("actual bootstrap voter/original or retained owner differs", id)
		}
	}
	if f.bootstrapAgreement != ([32]byte{}) && f.bootstrapAgreement != proposal.target.semantic {
		f.t.Fatal("bootstrap native agreement changed between actual originals")
	}
	f.bootstrapAgreement = proposal.target.semantic
}

func (f *pairedGraphFaultNetwork) lifecycleRead(actor, handle, selector uint64) nativeLifecycleObservation {
	if f.parent == nil {
		return nativeLifecycleRead(f.t, f.n, actor, handle, selector)
	}
	response := f.call(f.n.all[actor], processRequest{op: processLifecycleObserve, index: handle, entity: selector})
	result, err := decodeNativeLifecycleObservation(response.aux)
	if err != nil {
		f.t.Fatal("actual certified lifecycle observation", err)
	}
	return result
}

// Cleanup is independently bounded after operation expiry. It returns actual
// replies/errors and is never used to certify a successful expired operation.
func (f *pairedGraphFaultNetwork) cleanupExchange(p *graphProcess, request processRequest) (processResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	response, transport := p.exchange(ctx, request)
	return response, errors.Join(transport, processResponseError(response))
}

func (f *pairedGraphFaultNetwork) cleanupCertifiedHandle(p *graphProcess, handle *uint64) {
	if *handle == 0 || p.closed && p.cmd.ProcessState != nil {
		return
	}
	if _, err := f.cleanupExchange(p, processRequest{op: processSourceCertifiedClose, index: *handle}); err != nil {
		f.t.Error("actual certified owner cleanup after interrupted observation", err)
	}
	*handle = 0
}

// Same actual reconnect door as P03, with the fresh immutable parent deadline.
// Closed source replay slots are retained; none are reset to fund this retry.
const nativeConjunctionReconnectPrefix = "NCR1"

func nativeConjunctionReconnectRequest(r processRequest, enabled bool) (processRequest, bool, error) {
	if len(r.data) == 16 {
		return r, false, nil // Literal legacy voter payload/admission below.
	}
	if !enabled || r.op != processPairedReconnect || len(r.data) != len(nativeConjunctionReconnectPrefix)+16 || !bytes.HasPrefix(r.data, []byte(nativeConjunctionReconnectPrefix)) || r.index != 0 || r.entity != 0 || r.packet.From != 0 || r.packet.To != 0 || r.packet.Snapshot || len(r.packet.Payload) != 0 {
		return r, false, errInvalid
	}
	r.data = r.data[len(nativeConjunctionReconnectPrefix):]
	return r, true, nil
}

func nativeConjunctionReconnect(f *pairedGraphFaultNetwork, actor uint64) {
	f.drain()
	f.packetDrain()
	hub := f.n.all[actor].mux.hub
	for i, partition := range []uint64{3, 8} {
		previous := []uint64{1, 4}[i]
		hub.mu.Lock()
		if routes := hub.relayTargets[actor]; routes != nil && routes[partition] != 0 {
			previous = routes[partition]
		}
		hub.mu.Unlock()
		target := f.n.leaders[i]
		if previous == target {
			continue
		}
		r := f.call(f.n.all[target], processRequest{op: processPairedConnections, entity: actor})
		c := graphCursor{b: r.aux}
		live := c.tag()
		c.u64() // Closed source replay slots remain spent.
		s := f.call(f.n.all[target], processRequest{op: processStateOp})
		if c.err != nil || len(c.b) != 0 || live != 0 || s.stats[statViews] != 0 || s.stats[statPinnedBytes] != 0 || len(s.handles) != 2 || s.handles[0] != 0 || s.handles[1] != 0 {
			f.t.Fatal("fresh reconnect target owns live work", actor, target, c.err)
		}
	}
	data := allocationP08Words(f.n.leaders[0], f.n.leaders[1])
	if f.parent != nil {
		data = append([]byte(nativeConjunctionReconnectPrefix), data...)
	}
	f.call(f.n.all[actor], processRequest{op: processPairedReconnect, data: data})
	hub.mu.Lock()
	if hub.relayTargets == nil {
		hub.relayTargets = make(map[uint64]map[uint64]uint64)
	}
	hub.relayTargets[actor] = map[uint64]uint64{3: f.n.leaders[0], 8: f.n.leaders[1]}
	hub.mu.Unlock()
}

// One after-relay owner records at most32 genuine source submissions. Failed
// replies are NOT successful captures. All copies/full envelope decodes are
// prepaid before traversal, and a diagnostic refusal never changes the reply.
type nativeConjunctionCapture struct {
	mu                                                     sync.Mutex
	hub                                                    *graphProcessHub
	actor                                                  uint64
	tx                                                     [16]byte
	prior                                                  func(context.Context, uint64, uint64, []byte, []byte) error
	priorBefore                                            func(context.Context, uint64, uint64, []byte) error
	finished                                               bool
	refused                                                bool
	candidates, attempts, work, output, retained, reserved uint64
	requests                                               []p06Phase
	// Fresh P02 only: two actual planning-source opens share this SAME callback
	// owner. They are diagnostics, never imported source/certificate authority.
	planningSources              bool
	sourceOutput, sourceRetained uint64
	sources                      []p06Phase
}

func newNativeConjunctionCapture(f *pairedGraphFaultNetwork, actor uint64, tx [16]byte, planningSources ...bool) *nativeConjunctionCapture {
	if len(planningSources) > 1 {
		f.t.Fatal("one immutable optional planning-source capture mode")
	}
	c := &nativeConjunctionCapture{hub: f.n.all[actor].mux.hub, actor: actor, tx: tx}
	c.planningSources = len(planningSources) == 1 && planningSources[0]
	c.hub.mu.Lock()
	c.prior, c.priorBefore = c.hub.afterRelay, c.hub.beforeRelay
	c.hub.afterRelay, c.hub.beforeRelay = c.after, c.before
	c.hub.mu.Unlock()
	return c
}

func (c *nativeConjunctionCapture) before(ctx context.Context, origin, partition uint64, request []byte) error {
	if c.priorBefore != nil {
		if err := c.priorBefore(ctx, origin, partition, request); err != nil {
			return err
		}
	}
	if origin != c.actor {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.finished || c.candidates >= 256 || len(request) > processFrameBytes {
		c.refused = true
		return nil
	}
	c.candidates++
	c.work += 4096 + 32*processFrameBytes
	command, cross, err := nativeFaultPhase(request)
	if err != nil {
		c.refused = true
		return nil
	}
	if !cross {
		if c.planningSources {
			q, err := decodeGraphSourceRequest(request)
			if err != nil {
				c.refused = true
			} else if q.kind == sourceOpen {
				if len(c.sources) == 2 || len(request) > 4096 {
					c.refused = true
					return nil
				}
				// Separate from the32-submit/256KiB owner. These two finite
				// source replies and their JSON output are prepaid before copy.
				c.sourceOutput += 8*(4096+nativeBatchReceiptBytes) + 8192
				p := p06Phase{Origin: origin, Partition: partition, Request: make([]byte, len(request))}
				copy(p.Request, request)
				c.sourceRetained += uint64(len(request))
				c.sources = append(c.sources, p)
			}
		}
		return nil
	}
	if c.attempts >= 32 || len(request) > 2*nativeLogicalTransitionBodyBytes || uint64(len(request)+nativeBatchReceiptBytes) > (256<<10)-c.reserved {
		c.refused = true
		return nil
	}
	c.attempts++
	c.output += 2*nativeLogicalTransitionBodyBytes + nativeBatchReceiptBytes
	c.reserved += uint64(len(request) + nativeBatchReceiptBytes)
	manifest, err := nativeConjunctionManifest(command)
	if err != nil || manifest.tx != c.tx || command.ns.partition != partition {
		c.refused = true
		return nil
	}
	p := p06Phase{Origin: origin, Partition: partition, Request: make([]byte, len(request))}
	copy(p.Request, request)
	c.retained += uint64(len(request))
	c.requests = append(c.requests, p)
	return nil
}

func (c *nativeConjunctionCapture) after(ctx context.Context, origin, partition uint64, request, reply []byte) error {
	if c.prior != nil {
		if err := c.prior(ctx, origin, partition, request, reply); err != nil {
			return err
		}
	}
	if origin != c.actor {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	var selected *p06Phase
	for i := range c.requests {
		if c.requests[i].Partition == partition && bytes.Equal(c.requests[i].Request, request) {
			selected = &c.requests[i]
			break
		}
	}
	if selected == nil && c.planningSources {
		for i := range c.sources {
			if c.sources[i].Partition != partition || !bytes.Equal(c.sources[i].Request, request) {
				continue
			}
			row := &c.sources[i]
			if c.finished || len(row.Reply) != 0 || len(reply) > nativeBatchReceiptBytes {
				c.refused = true
				return nil
			}
			q, qerr := decodeGraphSourceRequest(request)
			r, rerr := decodeGraphWireResponse(reply)
			info, ierr := decodeGraphSourceInfo(r.body)
			if qerr != nil || rerr != nil || ierr != nil || r.err != nil || q.kind != sourceOpen || r.kind != q.kind || r.sequence != q.sequence || r.nonce != q.nonce || info.nonce != q.nonce || info.cookie.issuer != r.issuer || info.readID.session != r.issuer || info.observation.source.scope != r.peer.scope || r.peer.scope.partition != partition {
				c.refused = true
				return nil
			}
			row.Reply = make([]byte, len(reply))
			copy(row.Reply, reply)
			c.sourceRetained += uint64(len(reply))
			return nil
		}
	}
	if selected == nil {
		return nil // Non-Submit traffic was classified/prepaid in before.
	}
	if c.finished || len(selected.Reply) != 0 || len(reply) > nativeBatchReceiptBytes {
		c.refused = true
		return nil
	}
	q, qerr := decodeGraphSourceRequest(request)
	p, perr := decodeGraphSourceProposal(q.command)
	command, _, commandErr := decodeCrossCommand(p.wire, defaultMaterializerLimits())
	manifest, manifestErr := nativeConjunctionManifest(command)
	r, rerr := decodeGraphWireResponse(reply)
	status, err := decodeCrossStatus(r.body)
	if qerr != nil || perr != nil || commandErr != nil || manifestErr != nil || rerr != nil || r.err != nil || r.sequence != q.sequence || r.nonce != q.nonce || r.peer.scope != p.target || err != nil || status.tx != c.tx || status.digest != manifest.digest || status.ns.partition != partition || status.phase < command.phase {
		c.refused = true
		return nil
	}
	selected.Reply = make([]byte, len(reply))
	copy(selected.Reply, reply)
	c.retained += uint64(len(reply))
	return nil
}

func nativeConjunctionManifest(command crossCommand) (crossManifest, error) {
	arena, err := graphstate.NewOutputBudget(defaultMaterializerLimits().outputBytes)
	if err != nil {
		return crossManifest{}, err
	}
	return decodeCrossManifest(command.manifest, defaultMaterializerLimits(), arena)
}

func (c *nativeConjunctionCapture) close() {
	// LIFO: the batch gate restores its before/failed hooks first; this sole
	// after owner then restores its predecessor. No new callback is installed
	// until all genuine pending replies have retired and the hub is drained.
	c.hub.mu.Lock()
	c.mu.Lock()
	if c.finished {
		c.mu.Unlock()
		c.hub.mu.Unlock()
		return
	}
	c.hub.afterRelay, c.hub.beforeRelay = c.prior, c.priorBefore
	c.finished = true
	c.mu.Unlock()
	c.hub.mu.Unlock()
}

// Borrow only actual captured source command bytes after the parent has
// authenticated the complete retained six-original census. This grants no
// private proof/stamp: p06ReplayPhase still enters the ordinary native Submit
// path, which must revalidate the real terminal original. In particular an
// unanswered Resolve is not converted into a successful source reply.
func nativeConjunctionTerminalResolveCommands(phases, unknown []p06Phase, manifest []byte) ([2]allocationProposal, error) {
	var commands [2]allocationProposal
	if len(phases) > 10 || len(unknown) > 1 {
		return commands, errLimit
	}
	if len(manifest) == 0 || len(manifest) > nativeLogicalTransitionBodyBytes || len(phases) == 0 {
		return commands, errInvalid
	}
	// The capture pre-admitted full envelope traversal/temporary decode at
	// 32*processFrameBytes per actual candidate, including this bounded second
	// classification. Eleven borrowed references require no wire clone/map.
	var rows [11]p06Phase
	n := copy(rows[:], phases)
	n += copy(rows[n:], unknown)
	for _, row := range rows[:n] {
		if len(row.Request) == 0 || len(row.Request) > 2*nativeLogicalTransitionBodyBytes {
			return commands, errLimit
		}
		request, err := decodeGraphSourceRequest(row.Request)
		if err != nil || request.kind != sourceSubmit {
			return commands, errors.Join(errInvalid, err)
		}
		proposal, err := decodeGraphSourceProposal(request.command)
		if err != nil {
			return commands, err
		}
		command, _, err := decodeCrossCommand(proposal.wire, defaultMaterializerLimits())
		if err != nil || !bytes.Equal(command.manifest, manifest) || command.ns.partition != row.Partition || proposal.target.partition != row.Partition {
			return commands, errors.Join(errInvalid, err)
		}
		if command.phase != crossResolve {
			continue
		}
		position := 0
		if row.Partition == 8 {
			position = 1
		} else if row.Partition != 3 {
			return commands, errInvalid
		}
		commands[position] = proposal
	}
	if len(commands[0].wire) == 0 || len(commands[1].wire) == 0 {
		return commands, errInvalid
	}
	return commands, nil
}
