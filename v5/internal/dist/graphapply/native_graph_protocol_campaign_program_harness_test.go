package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/dist/replica"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

const nativeProtocolProgramProfile = "native-protocol-causal-program-v1"

// Driver.drain persists a Ready before exposing rd.Messages. SIGKILL at either
// Ready Sync side on the submitting local leader therefore precedes emission
// of that command's Raft frames. This narrow pre-emission ordering is not a
// waiver of still-applicable authenticated service-RPC/predecessor schedules.
func nativeProtocolProgramFenceReachable(recipe nativeProtocolFenceRecipe, program nativeProtocolProgramSpec) bool {
	return recipe.valid() && program.valid() && !(recipe.Schedule == "persisted-fence-before-application" && recipe.Phase == 0 && recipe.Kind == raftlog.StableStorageReady && program.Message != "literal")
}

func nativeProtocolProgramFrozenSelector(recipe nativeProtocolFrozenRecipe, program nativeProtocolProgramSpec) nativeConjunctionSelector {
	if !recipe.valid() || !program.valid() {
		return nativeConjunctionSelector{}
	}
	selector := recipe.selector()
	if program.Layout == "rotated" {
		if recipe.Original == "B" {
			selector.Actor = 6 // Actual current B leader retains both native candidates.
		}
		selector.Member = 1
		if recipe.Partition == 8 {
			selector.Member = 4
		}
	}
	return selector
}

// Fresh fixtures bind this field only after validating genuine bootstrap
// originals. Historical nil-context fixtures retain their exact constructor
// selection; the explicit conditional fixture is a distinct real child too.
func (f *pairedGraphFaultNetwork) nativeAgreement() [32]byte {
	if f.bootstrapAgreement != ([32]byte{}) {
		return f.bootstrapAgreement
	}
	if pairedFaultConditionalFixture() {
		return conditionalGraphSemanticContractID()
	}
	if f.n.rangeMoves {
		return rangeMoveSemanticContractID()
	}
	return certifiedGraphSemanticContractID()
}

// This selector is shared with the actual child BEFORE Open. Neither the
// program name nor an artifact's agreement field can change a native store's
// materializer. Blank recovery preserves the existing state fixture literally.
func nativeProtocolStateAgreement() ([32]byte, error) {
	recovery := os.Getenv("RHO_GRAPH_PROTOCOL_STATE_RECOVERY")
	if recovery == "" {
		return rangeMoveSpanSemanticContractID(), nil
	}
	if recovery != "4" || os.Getenv("RHO_GRAPH_PROTOCOL_STATE_PROCESS") != "1" || os.Getenv("RHO_GRAPH_TRANSFER_PROCESS") != "" {
		return [32]byte{}, errInvalid
	}
	selected, err := nativeProtocolStateProfileFromEnv()
	if err != nil || !selected {
		return [32]byte{}, errors.Join(errInvalid, err)
	}
	return rangeMovePooledSemanticContractID(), nil
}

func nativeProtocolStateExpectedAgreement(t *testing.T) [32]byte {
	t.Helper()
	agreement, err := nativeProtocolStateAgreement()
	if err != nil {
		t.Fatal("actual explicitly selected state agreement", err)
	}
	return agreement
}

// Layouts select REAL elections and source endpoints. They are not a mapping
// of diagnostic voter numbers: all requests, receipts and wire identities keep
// their original configured member and incarnation.
type nativeProtocolProgramSpec struct {
	Version uint8
	Layout  string
	Message string
}

func (p nativeProtocolProgramSpec) valid() bool {
	if p.Version != 1 || p.Layout != "original" && p.Layout != "rotated" {
		return false
	}
	switch p.Message {
	case "literal", "delay", "reorder", "duplicate":
		return true
	}
	return false
}

type nativeProtocolProgramElection struct {
	Prior, Leader uint64
	Configuration []byte
	State         []byte
	Owner         nativeBatchReceipt
}

type nativeProtocolProgramEvidence struct {
	Profile           string
	Spec              nativeProtocolProgramSpec
	DeadlineUnixNanos int64
	Elections         []nativeProtocolProgramElection
	Successors        []nativeProtocolProgramElection
	CommandSHA256     [32]byte
	MessageFrom       uint64
	MessageTo         uint64
	Packets           []nativeFaultPacketEvent
	Frames            []replica.Packet
	FrameCandidates   uint64
	TickResponse      []byte
	WorkGrant         uint64
	OutputGrant       uint64
}

// A single owner is published before the semantic adapter opens any sources.
// It never owns/recreates a transaction. Its only mutation is an actual native
// election or installation of the existing bounded packet scheduler.
type nativeProtocolProgram struct {
	mu                  sync.Mutex
	ctx                 context.Context
	f                   *pairedGraphFaultNetwork
	spec                nativeProtocolProgramSpec
	evidence            nativeProtocolProgramEvidence
	finish              func() ([]nativeFaultPacketEvent, error)
	armed               bool
	attaching           bool
	attachCancel        context.CancelFunc
	attachDone          chan struct{}
	closed              bool
	closing             bool
	terminal            error
	output              *graphstate.OutputBudget
	hub                 *graphProcessHub
	priorFilter         func(replica.Packet) (bool, error)
	scheduler           *nativeFaultPacketScheduler
	nativeTarget        uint64
	matched             chan struct{}
	pumpDone            chan struct{}
	pumpCancel          context.CancelFunc
	selectionSet        bool
	selectionPhase      crossPhase
	selectionPartition  uint64
	selectionFence      bool
	successorAttempts   uint64
	predecessor         *nativeProtocolPredecessorOwner
	preReadyPredecessor bool
	afterPredecessor    []byte
	p06                 bool
	afterLoss           *nativeProtocolProgramElection
}

func newNativeProtocolProgram(ctx context.Context, f *pairedGraphFaultNetwork, spec nativeProtocolProgramSpec) (*nativeProtocolProgram, error) {
	if ctx == nil || f == nil || f.n == nil || f.parent == nil || !spec.valid() {
		return nil, errInvalid
	}
	deadline, bounded := ctx.Deadline()
	parent, original := f.parent.Deadline()
	if !bounded || !original || !deadline.Equal(parent) || ctx.Err() != nil || f.parent.Err() != nil {
		return nil, errors.Join(errInvalid, ctx.Err(), f.parent.Err())
	}
	// Full fixed grants precede native election, response encoding and packet
	// hashing/copying. These counters fund diagnostics, not native authority, and
	// do not allocate memory in proportion to their quota.
	output, err := graphstate.NewOutputBudget(16 << 20)
	if err != nil {
		return nil, err
	}
	if err := output.Reserve(16 << 20); err != nil {
		return nil, err
	}
	p := &nativeProtocolProgram{ctx: ctx, f: f, spec: spec, output: output, evidence: nativeProtocolProgramEvidence{Profile: nativeProtocolProgramProfile, Spec: spec, DeadlineUnixNanos: deadline.UnixNano(), WorkGrant: uint64((128 + 13) * 32 * processFrameBytes), OutputGrant: 16 << 20}}
	p.predecessor, err = nativeProtocolPredecessorFromEnv(p)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	if f.protocolProgram != nil || f.conjunctionGate != nil || f.protocolReplyHold != nil {
		f.mu.Unlock()
		return nil, errLimit
	}
	f.protocolProgram = p
	f.mu.Unlock()
	stop := context.AfterFunc(ctx, p.cleanup)
	f.t.Cleanup(func() { stop(); p.cleanup() })
	return p, nil
}

// Called once, before any live source/Run/queue owner. f.elect only records its
// new leader after a real native ReadIndex succeeds. The two copied responses
// below preserve that configured identity for the independent program checker.
func (p *nativeProtocolProgram) elect() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.closing || len(p.evidence.Elections) != 0 || p.ctx.Err() != nil {
		return errors.Join(errInvalid, p.ctx.Err())
	}
	for group, next := range [2]uint64{2, 6} {
		prior := p.f.n.leaders[group]
		if p.spec.Layout == "original" {
			next = prior
		} else {
			p.f.elect(prior, next)
		}
		configuration := p.f.call(p.f.n.all[next], processRequest{op: processCertifiedRead, entity: uint64(observeConfiguration)})
		p.f.n.group(next).enqueue(p.f.t, configuration)
		p.f.drainGroup(p.f.n.group(next))
		state := p.f.call(p.f.n.all[next], processRequest{op: processStateOp})
		if processResponseError(configuration) != nil || !state.ready || configuration.incarnation != state.incarnation || configuration.incarnation == ([16]byte{}) || p.f.n.leaders[group] != next {
			return errCorrupt
		}
		owner, err := nativeBatchCall(p.ctx, p.f.n.all[next], nativeBatchControl{Version: 1, Action: "inspect"})
		if err != nil || owner.Member != next || owner.PID != uint64(p.f.n.all[next].cmd.Process.Pid) || owner.DeadlineUnixNanos != p.evidence.DeadlineUnixNanos || owner.State != "idle" {
			return errors.Join(errCorrupt, err)
		}
		p.evidence.Elections = append(p.evidence.Elections, nativeProtocolProgramElection{Prior: prior, Leader: next, Configuration: p06ProcessWire(p.f.t, configuration), State: p06ProcessWire(p.f.t, state), Owner: owner})
	}
	return nil
}

func (p *nativeProtocolProgram) target(partition uint64, protected ...uint64) (uint64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.closing || p.ctx.Err() != nil || len(p.evidence.Elections) != 2 || partition != 3 && partition != 8 || len(protected) == 0 || len(protected) > 6 {
		return 0, errors.Join(errInvalid, p.ctx.Err())
	}
	for _, member := range protected {
		if member < 1 || member > 6 {
			return 0, errInvalid
		}
	}
	group := 0
	if partition == 8 {
		group = 1
	}
	leader := p.currentLeader(group)
	for member := uint64(1 + group*3); member <= uint64(3+group*3); member++ {
		liveOwner := false
		for _, owner := range protected {
			liveOwner = liveOwner || member == owner
		}
		if member != leader && !liveOwner {
			return member, nil
		}
	}
	return 0, errInvalid
}

// The selector comes from the ACTUAL decoded native gate, not a requested
// phase label. Attaching occurs before ordinary Submit and requires an idle
// packet queue; a missing matching packet makes close refuse qualification.
func (p *nativeProtocolProgram) arm(selector nativeConjunctionSelector, command []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.selectionSet && (selector.Phase != p.selectionPhase || selector.Partition != p.selectionPartition || selector.Fence != p.selectionFence) {
		return nil // Actual earlier causal gate remains native, never counted as this occurrence.
	}
	if !selector.valid() {
		return errors.Join(errInvalid, p.ctx.Err())
	}
	return p.armNativeLocked(selector.Partition, selector.Member, command, &selector)
}

// The state gate authenticates its complete actual GMQ/GPL command and native
// entry before this private attachment. No cross-phase enum or decoded source
// authority is manufactured to make a state command resemble a transaction.
func (p *nativeProtocolProgram) armNative(partition, member uint64, command []byte) error {
	if p == nil {
		return errInvalid
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.selectionSet || p.predecessor != nil {
		return errInvalid
	}
	return p.armNativeLocked(partition, member, command, nil)
}

func (p *nativeProtocolProgram) armNativeLocked(partition, member uint64, command []byte, selector *nativeConjunctionSelector) error {
	if p.closed || p.closing || p.armed || p.attaching || p.ctx.Err() != nil || len(p.evidence.Elections) != 2 || partition != 3 && partition != 8 || member < 1 || member > 6 || (member <= 3) != (partition == 3) || len(command) == 0 || len(command) > nativeLogicalTransitionBodyBytes {
		return errors.Join(errInvalid, p.ctx.Err())
	}
	group := 0
	if partition == 8 {
		group = 1
	}
	leader := p.f.n.leaders[group]
	if leader != p.currentLeader(group) {
		return errInvalid // Later causal elections require a new actual arm, not relabelling.
	}
	// Sending toward a surviving different voter is deliberate: the native
	// target can die at Sync while the message program still retires honestly.
	to := uint64(0)
	for peer := uint64(1 + group*3); peer <= uint64(3+group*3); peer++ {
		if peer != leader && peer != member {
			to = peer
			break
		}
	}
	if to == 0 {
		return errInvalid
	}
	p.evidence.CommandSHA256 = sha256.Sum256(command)
	p.evidence.MessageFrom, p.evidence.MessageTo = leader, to
	p.nativeTarget = member
	if selector != nil {
		if err := p.predecessor.arm(*selector, command); err != nil {
			return err
		}
	}
	if p.preReadyPredecessor {
		if member != leader || p.spec.Message == "literal" {
			return errInvalid
		}
		actual, err := nativeProtocolPredecessorTick(p.ctx, p.f, member, p.spec.Message, nil)
		if err != nil {
			return err
		}
		p.evidence.TickResponse, p.afterPredecessor = actual.tickWire, actual.stateWire
		p.evidence.Packets, p.evidence.Frames = actual.events, actual.tick.packets
		p.evidence.FrameCandidates = uint64(len(actual.tick.packets))
		p.evidence.MessageFrom, p.evidence.MessageTo = actual.tick.packets[0].From, actual.tick.packets[0].To
		p.armed = true
		return nil
	}
	if p.spec.Message != "literal" {
		delay := 0
		if p.spec.Message == "delay" {
			delay = 20
		}
		schedule, err := newNativeFaultPacketScheduler(p.ctx, p.spec.Message, leader, to, 1, delay)
		if err != nil {
			return err
		}
		// The selected source Submit has not been forwarded. Earlier native
		// packets may still be retiring; let their existing event lane finish
		// without holding a program/gate callback lock. This one reservation
		// prevents a concurrent arm from replacing the original attachment.
		attaching, cancel := context.WithCancel(p.ctx)
		done := make(chan struct{})
		p.attaching, p.attachCancel, p.attachDone = true, cancel, done
		p.mu.Unlock()
		finish, err := attachNativeFaultPacketSchedulerAfterDrain(attaching, p.f.n.all[leader].mux.hub, schedule)
		cancel()
		p.mu.Lock()
		p.attaching, p.attachCancel, p.attachDone = false, nil, nil
		if err == nil && (p.closed || p.closing || p.ctx.Err() != nil) {
			p.mu.Unlock()
			_, closeErr := finish()
			p.mu.Lock()
			err = errors.Join(errInvalid, closeErr, p.ctx.Err())
		}
		close(done)
		if err != nil {
			schedule.cancel()
			return err
		}
		p.finish = finish
		p.scheduler = schedule
		p.hub = p.f.n.all[leader].mux.hub
		p.hub.mu.Lock()
		p.priorFilter = p.hub.packetFilter
		p.hub.packetFilter = p.observePacket
		p.hub.mu.Unlock()
		if p.spec.Message == "reorder" {
			p.matched, p.pumpDone = make(chan struct{}), make(chan struct{})
			pump, cancel := context.WithTimeout(p.ctx, 10*time.Second)
			p.pumpCancel = cancel
			go p.reorderTick(pump, leader)
		}
	}
	p.armed = true
	return nil
}

// Must be called before the first native arm. This is diagnostic occurrence
// selection only; every matched input still passes the ordinary native gate's
// actual full decoder, scope and exact-entry commitment checks.
func (p *nativeProtocolProgram) selectCommand(phase crossPhase, partition uint64, fence bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.closing || p.armed || p.attaching || p.selectionSet || p.ctx.Err() != nil || partition != 3 && partition != 8 ||
		(fence && (phase != 0 || partition != 3) || !fence && (phase < crossRegister || phase > crossResolve)) {
		return errors.Join(errInvalid, p.ctx.Err())
	}
	p.selectionSet, p.selectionPhase, p.selectionPartition, p.selectionFence = true, phase, partition, fence
	return nil
}

// Called with p.mu held. A new source peer is admitted only after a real
// successor election and its actual config/read/native-owner observation.
func (p *nativeProtocolProgram) currentLeader(group int) uint64 {
	leader := p.evidence.Elections[group].Leader
	for _, row := range p.evidence.Successors {
		if (row.Leader <= 3) == (group == 0) {
			leader = row.Leader
		}
	}
	return leader
}

func (p *nativeProtocolProgram) successor(prior, next uint64) (nativeProtocolProgramElection, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	closedFence := p.closed && p.selectionFence && p.armed && p.successorAttempts == 0
	closedP06 := p.closed && p.p06 && p.armed && p.afterLoss == nil
	if p.attaching || (p.closed || p.closing || p.armed) && !closedFence && !closedP06 || p.ctx.Err() != nil || len(p.evidence.Elections) != 2 || p.successorAttempts >= 2 || prior == next || prior < 1 || next < 1 || prior > 6 || next > 6 || (prior <= 3) != (next <= 3) {
		return nativeProtocolProgramElection{}, errors.Join(errInvalid, p.ctx.Err())
	}
	group := 0
	if next > 3 {
		group = 1
	}
	if p.currentLeader(group) != prior || p.f.n.leaders[group] != next {
		return nativeProtocolProgramElection{}, errInvalid
	}
	p.successorAttempts++ // Failed/unknown captures retire this pre-admitted grant too.
	if closedFence {
		// The message window has ended, but this one prepaid native successor
		// observation still owns the fixture slot under its ORIGINAL deadline.
		defer func() {
			p.f.mu.Lock()
			if p.f.protocolProgram == p {
				p.f.protocolProgram = nil
			}
			p.f.mu.Unlock()
		}()
	}
	configuration := p.f.call(p.f.n.all[next], processRequest{op: processCertifiedRead, entity: uint64(observeConfiguration)})
	p.f.n.group(next).enqueue(p.f.t, configuration)
	p.f.drainGroup(p.f.n.group(next))
	state := p.f.call(p.f.n.all[next], processRequest{op: processStateOp})
	owner, err := nativeBatchCall(p.ctx, p.f.n.all[next], nativeBatchControl{Version: 1, Action: "inspect"})
	if err != nil || !state.ready || configuration.incarnation != state.incarnation || owner.Member != next || owner.PID != uint64(p.f.n.all[next].cmd.Process.Pid) || owner.DeadlineUnixNanos != p.evidence.DeadlineUnixNanos || owner.State != "idle" {
		return nativeProtocolProgramElection{}, errors.Join(errCorrupt, err)
	}
	row := nativeProtocolProgramElection{Prior: prior, Leader: next, Configuration: p06ProcessWire(p.f.t, configuration), State: p06ProcessWire(p.f.t, state), Owner: owner}
	if !p.closed {
		p.evidence.Successors = append(p.evidence.Successors, row)
		if err := p.predecessor.successor(); err != nil {
			return nativeProtocolProgramElection{}, err
		}
	} else if closedP06 {
		p.afterLoss = &row
	}
	return row, nil
}

func (p *nativeProtocolProgram) observePacket(packet replica.Packet) (bool, error) {
	if p.priorFilter != nil {
		dropped, err := p.priorFilter(packet)
		if dropped || err != nil {
			return dropped, err
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.terminal != nil || len(p.evidence.Frames) >= 2 || p.ctx.Err() != nil {
		return false, nil
	}
	if p.evidence.FrameCandidates >= 128 {
		p.terminal = errLimit
		return false, nil // Diagnostic refusal never drops native traffic.
	}
	p.evidence.FrameCandidates++
	if packet.Snapshot || len(packet.Payload) == 0 || len(packet.Payload) > processFrameBytes {
		p.terminal = errCorrupt
		return false, nil
	}
	if len(p.evidence.Frames) == 0 {
		if packet.From != p.evidence.MessageFrom || packet.To == p.nativeTarget && p.nativeTarget != packet.From {
			return false, nil
		}
		// The immutable rule is "first emitted surviving peer". Resolve its
		// actual recipient BEFORE route() sees its first matching frame. This
		// preserves the real member on both wire and receipt, including when
		// the selected native target is the submitting local leader itself.
		p.scheduler.mu.Lock()
		if p.scheduler.used || p.scheduler.seen != 0 || p.scheduler.held != nil {
			p.scheduler.mu.Unlock()
			p.terminal = errCorrupt
			return false, nil
		}
		p.scheduler.to = packet.To
		p.scheduler.mu.Unlock()
		p.evidence.MessageTo = packet.To
	}
	// One matched native frame and at most its next actual queue successor.
	// Both complete copies are funded before the selected Submit; no diagnostic
	// packet is ever fed back into the native Driver or used as source authority.
	observed := packet
	observed.Payload = make([]byte, len(packet.Payload))
	copy(observed.Payload, packet.Payload)
	p.evidence.Frames = append(p.evidence.Frames, observed)
	if len(p.evidence.Frames) == 1 && p.matched != nil {
		close(p.matched)
		// No selected frame is delivered until this real Tick has returned.
		// Therefore a local application-boundary target cannot die from a
		// quorum reply while the auxiliary Tick is still awaiting its result.
		p.mu.Unlock()
		select {
		case <-p.pumpDone:
		case <-p.ctx.Done():
		}
		p.mu.Lock()
	}
	return false, nil
}

// Reorder needs a genuine next native message even when the selected command
// is the only caller. One declared native Tick produces that successor through
// the SAME hub/scheduler. It does not deliver a fabricated packet or ACK and
// cannot acquire a new caller/source deadline.
func (p *nativeProtocolProgram) reorderTick(ctx context.Context, leader uint64) {
	defer close(p.pumpDone)
	select {
	case <-p.matched:
	case <-ctx.Done():
		return
	}
	r, transport := p.f.n.all[leader].exchange(ctx, processRequest{op: processTick})
	err := errors.Join(transport, processResponseError(r), ctx.Err())
	if err == nil && len(r.packets) == 0 {
		err = errInvalid // A no-output Tick is not a matched reorder successor.
	}
	if err == nil {
		err = p.hub.enqueue(leader, r.packets, r.handles...)
	}
	var wire []byte
	if err == nil {
		wire, err = encodeProcessResponse(r)
	}
	p.mu.Lock()
	if err != nil {
		p.terminal = errors.Join(p.terminal, err)
	} else {
		p.evidence.TickResponse = wire
	}
	p.mu.Unlock()
}

func (p *nativeProtocolProgram) close() (nativeProtocolProgramEvidence, error) {
	if p == nil {
		return nativeProtocolProgramEvidence{}, errInvalid
	}
	p.mu.Lock()
	p.closing = true
	pumpCancel, pumpDone := p.pumpCancel, p.pumpDone
	attachCancel, attachDone := p.attachCancel, p.attachDone
	p.mu.Unlock()
	if attachDone != nil {
		attachCancel()
		<-attachDone
	}
	if pumpDone != nil {
		pumpCancel()
		// Cancellation owns no replacement authority. Join the actual bounded
		// pipe call before releasing diagnostic callbacks/retained queue bytes.
		<-pumpDone
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.closed {
		p.closed = true
		if !p.armed || p.ctx.Err() != nil {
			p.terminal = errors.Join(errInvalid, p.ctx.Err())
		}
		p.terminal = errors.Join(p.terminal, p.predecessor.seal())
		if p.finish != nil {
			p.hub.mu.Lock()
			p.hub.packetFilter = p.priorFilter
			p.hub.mu.Unlock()
			packets, err := p.finish()
			p.evidence.Packets = packets
			p.terminal = errors.Join(p.terminal, err)
			if len(p.evidence.Packets) != 1 {
				p.terminal = errors.Join(p.terminal, errCorrupt)
			}
			if len(p.evidence.Frames) == 0 || p.spec.Message == "reorder" && len(p.evidence.Frames) != 2 {
				p.terminal = errors.Join(p.terminal, errCorrupt)
			}
		}
	}
	p.f.mu.Lock()
	if p.f.protocolProgram == p && (!p.selectionFence || !p.armed || p.successorAttempts != 0 || p.ctx.Err() != nil) {
		p.f.protocolProgram = nil
	}
	p.f.mu.Unlock()
	return p.evidence, p.terminal
}

// The diagnostic owner has the SAME absolute deadline, including cleanup of an
// abandoned selected schedule. Process reaping remains the fixture's separately
// bounded cleanup responsibility and never manufactures a semantic success.
func (p *nativeProtocolProgram) cleanup() { _, _ = p.close() }

func nativeProtocolProgramFinish(t *testing.T, f *pairedGraphFaultNetwork) *nativeProtocolProgramEvidence {
	t.Helper()
	f.mu.Lock()
	p := f.protocolProgram
	f.mu.Unlock()
	if p == nil {
		return nil // No extra packet drain/default artifact fields in old parents.
	}
	f.packetDrain()
	evidence, err := p.close()
	if err != nil {
		t.Fatal("unmatched/expired/incomplete actual message program", err)
	}
	return &evidence
}

func nativeProtocolProgramStart(t *testing.T, ctx context.Context, f *pairedGraphFaultNetwork, programs ...nativeProtocolProgramSpec) *nativeProtocolProgram {
	t.Helper()
	if len(programs) == 0 {
		return nil
	}
	if len(programs) != 1 {
		t.Fatal("one immutable causal program per actual semantic fixture")
	}
	p, err := newNativeProtocolProgram(ctx, f, programs[0])
	if err != nil {
		t.Fatal("actual program admission", err)
	}
	if err := p.elect(); err != nil {
		t.Fatal("actual program elections", err)
	}
	for member := uint64(1); member <= 6; member++ {
		nativeConjunctionReconnect(f, member)
	}
	if err := p.predecessor.start(); err != nil {
		t.Fatal("actual predecessor companion admission", err)
	}
	return p
}

// The legacy P02 census uses its historical per-call contexts. The fresh
// program keeps the same six genuine phase laws and complete voter census,
// with every actual native read bounded by the original fixture context.
func nativeProtocolProgramP02Votes(t *testing.T, f *pairedGraphFaultNetwork, phases []p06Phase, proposal allocationProposal, decision crossStatus) (crossManifest, [2]crossStatus) {
	t.Helper()
	if len(phases) != 6 {
		t.Fatal("one actual six-phase P02 caller completion")
	}
	var manifest crossManifest
	var original []byte
	var votes [2]crossStatus
	for index, phase := range phases {
		f.requireActive()
		request, err := decodeGraphSourceRequest(phase.Request)
		observed := p06SourceProposal(t, phase.Request)
		command, _, commandErr := decodeCrossCommand(observed.wire, defaultMaterializerLimits())
		arena, arenaErr := graphstate.NewOutputBudget(defaultMaterializerLimits().outputBytes)
		if err != nil || commandErr != nil || arenaErr != nil {
			t.Fatal("actual original P02 command", err, commandErr, arenaErr)
		}
		actual, err := decodeCrossManifest(command.manifest, defaultMaterializerLimits(), arena)
		if err != nil {
			t.Fatal(err)
		}
		if index == 0 {
			manifest = actual
			original = command.manifest
			p06BindBusiness(t, proposal, manifest)
		}
		reply, replyErr := decodeGraphWireResponse(phase.Reply)
		status, statusErr := decodeCrossStatus(reply.body)
		if replyErr != nil || statusErr != nil || reply.err != nil || reply.kind != request.kind || reply.sequence != request.sequence || reply.nonce != request.nonce || reply.peer.scope != observed.target || actual.tx != manifest.tx || actual.digest != manifest.digest || !bytes.Equal(command.manifest, original) || status.tx != manifest.tx || status.digest != manifest.digest || status.phase != command.phase || status.ns != command.ns || command.ns.partition != phase.Partition || command.phase != []crossPhase{crossRegister, crossPrepare, crossPrepare, crossDecide, crossResolve, crossResolve}[index] {
			t.Fatal("actual SAME P02 caller/manifest/phase correlation", replyErr, statusErr)
		}
		if index == 1 || index == 2 {
			votes[index-1] = status
			if !status.yes {
				t.Fatal("genuine P02 YES refused")
			}
		}
		if index >= 3 && (!status.commit || status.round != decision.round) {
			t.Fatal("actual durable P02 decision changed")
		}
		want := status
		if command.phase == crossRegister {
			part, found := manifest.part(status.ns.partition)
			if !found {
				t.Fatal("actual coordinator absent from frozen P02 participants")
			}
			want.floor = part.floor
		}
		first := uint64(1)
		if status.ns.partition == 8 {
			first = 4
		}
		for member := first; member < first+3; member++ {
			record := f.call(f.n.all[member], processRequest{op: processEntryRecord, index: status.index})
			stored, err := decodeCrossStatus(record.outcome)
			if err != nil || stored != want || record.stats[statCommit] < status.index || record.stats[statCheckpoint] < status.index {
				t.Fatal("complete actual native P02 original census", member, err)
			}
		}
	}
	return manifest, votes
}
