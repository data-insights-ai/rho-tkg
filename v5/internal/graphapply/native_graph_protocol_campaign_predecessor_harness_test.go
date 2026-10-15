package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

const nativeProtocolPredecessorProfile = "native-protocol-predecessor-composition-v1"
const nativeProtocolPredecessorEnv = "RHO_GRAPH_PROTOCOL_PREDECESSOR"
const nativeProtocolPredecessorPairBytes = 128 << 10
const nativeProtocolPredecessorOutput = 1 << 20

// These are ONLY the twenty actual local-leader Ready exclusions. The existing
// semantic recipe and actual configured leaders remain the authority for the
// selected command; this description owns no transaction/source/native proof.
type nativeProtocolPredecessorRecipe struct {
	Family      string
	Fence       *nativeProtocolFenceRecipe       `json:",omitzero"`
	Live        *nativeProtocolLiveP06Recipe     `json:",omitzero"`
	Coordinator *nativeProtocolCoordinatorRecipe `json:",omitzero"`
	Partial     *nativeProtocolPartialRecipe     `json:",omitzero"`
}

func (r nativeProtocolPredecessorRecipe) valid() bool {
	count := 0
	for _, present := range []bool{r.Fence != nil, r.Live != nil, r.Coordinator != nil, r.Partial != nil} {
		if present {
			count++
		}
	}
	if count != 1 {
		return false
	}
	switch r.Family {
	case "p02-local-fence":
		return r.Fence != nil && r.Fence.valid() && r.Fence.Schedule == "persisted-fence-before-application" && r.Fence.Phase == 0 && r.Fence.Kind == raftlog.StableStorageReady
	case "p06-first-live":
		return r.Live != nil && r.Live.valid() && r.Live.Kind == raftlog.StableStorageReady
	case "p03-recovery":
		return r.Coordinator != nil && r.Coordinator.valid() && r.Coordinator.Phase == crossResolve && r.Coordinator.Partition == r.Coordinator.Coordinator && r.Coordinator.Kind == raftlog.StableStorageReady
	case "partial-recovery":
		return r.Partial != nil && r.Partial.valid() && r.Partial.Phase == crossResolve && r.Partial.Partition == 8 && r.Partial.Kind == raftlog.StableStorageReady
	}
	return false
}

func (r nativeProtocolPredecessorRecipe) name() string {
	if !r.valid() {
		return ""
	}
	if r.Fence != nil {
		return r.Fence.name()
	}
	if r.Live != nil {
		return r.Live.name()
	}
	if r.Coordinator != nil {
		return r.Coordinator.name()
	}
	return r.Partial.name()
}

func nativeProtocolPredecessorInventory() []nativeProtocolPredecessorRecipe {
	var out []nativeProtocolPredecessorRecipe
	for _, r := range nativeProtocolFenceInventory() {
		if r.Schedule == "persisted-fence-before-application" && r.Phase == 0 && r.Kind == raftlog.StableStorageReady {
			out = append(out, nativeProtocolPredecessorRecipe{Family: "p02-local-fence", Fence: &r})
		}
	}
	for _, r := range nativeProtocolLiveP06Inventory() {
		if r.Kind == raftlog.StableStorageReady {
			out = append(out, nativeProtocolPredecessorRecipe{Family: "p06-first-live", Live: &r})
		}
	}
	for _, r := range nativeProtocolCoordinatorInventory() {
		if r.Phase == crossResolve && r.Partition == r.Coordinator && r.Kind == raftlog.StableStorageReady {
			out = append(out, nativeProtocolPredecessorRecipe{Family: "p03-recovery", Coordinator: &r})
		}
	}
	for _, r := range nativeProtocolPartialInventory() {
		if r.Phase == crossResolve && r.Partition == 8 && r.Kind == raftlog.StableStorageReady {
			out = append(out, nativeProtocolPredecessorRecipe{Family: "partial-recovery", Partial: &r})
		}
	}
	return out
}

type nativeProtocolPredecessorSpec struct {
	Version              uint8
	Recipe               nativeProtocolPredecessorRecipe
	Layout, Mode, Output string
}

func (s nativeProtocolPredecessorSpec) valid() bool {
	return s.Version == 1 && s.Recipe.valid() && (s.Layout == "original" || s.Layout == "rotated") && (s.Mode == "delay" || s.Mode == "duplicate" || s.Mode == "reorder") && filepath.IsAbs(s.Output) && filepath.Base(s.Output) == "predecessor.json"
}

func (s nativeProtocolPredecessorSpec) mechanism() string {
	if s.Recipe.Fence != nil || s.Mode == "reorder" {
		return "emitted-predecessor-tick-packets"
	}
	if s.Mode == "delay" {
		return "selected-service-request-delay"
	}
	return "completed-predecessor-service-cache-duplicate"
}

type nativeProtocolPredecessorEvent struct {
	Ordinal uint64
	Action  string
}

type nativeProtocolPredecessorEvidence struct {
	Version                                                                  uint8
	Profile                                                                  string
	Spec                                                                     nativeProtocolPredecessorSpec
	BaseDirectory, BasePath, Mechanism                                       string
	DeadlineUnixNanos                                                        int64
	Run                                                                      [16]byte
	Candidates, WorkGrant, OutputGrant, RetainedBytes                        uint64
	Actor, Partition, Voter                                                  uint64
	CommandSHA256                                                            [32]byte
	SelectedRequest, PredecessorRequest, PredecessorReply, DuplicateResponse []byte
	TickResponse, AfterState                                                 []byte
	Packets                                                                  []nativeFaultPacketEvent
	ConfiguredDelayMillis                                                    int
	ObservedDelayNanos                                                       int64
	Events                                                                   []nativeProtocolPredecessorEvent
}

// One fixed owner/one pair/two Tick packets. Its environment is a private test
// admission, not a production selector or authority. Native23/program22 provide
// the existing types; the mechanical program attachment is a separate ordered
// dependency. Historical programs with a blank environment take no new path.
type nativeProtocolPredecessorOwner struct {
	mu                                             sync.Mutex
	p                                              *nativeProtocolProgram
	hub                                            *graphProcessHub
	ctx                                            context.Context
	file                                           *os.File
	output                                         *graphstate.OutputBudget
	e                                              nativeProtocolPredecessorEvidence
	priorBefore                                    func(context.Context, uint64, uint64, []byte) error
	priorAfter                                     func(context.Context, uint64, uint64, []byte, []byte) error
	installed, selected, performed, sealed, closed bool
	terminal                                       error
}

func nativeProtocolPredecessorFromEnv(p *nativeProtocolProgram) (*nativeProtocolPredecessorOwner, error) {
	raw := []byte(os.Getenv(nativeProtocolPredecessorEnv))
	if len(raw) == 0 {
		return nil, nil
	}
	if p == nil || p.f == nil || p.ctx == nil || len(raw) > 8192 || p.spec.Message != "literal" {
		return nil, errInvalid
	}
	var spec nativeProtocolPredecessorSpec
	if err := nativeFeedDecodeJSON(raw, &spec); err != nil {
		return nil, err
	}
	canonical, err := json.Marshal(spec)
	if err != nil || !bytes.Equal(canonical, raw) || !spec.valid() || spec.Layout != p.spec.Layout {
		return nil, errors.Join(errInvalid, err)
	}
	parent := filepath.Dir(spec.Output)
	actual, err := filepath.EvalSymlinks(parent)
	if err != nil || actual != filepath.Clean(parent) {
		return nil, errors.Join(errInvalid, err)
	}
	output, err := graphstate.NewOutputBudget(nativeProtocolPredecessorOutput)
	if err != nil {
		return nil, err
	}
	if err := output.Reserve(nativeProtocolPredecessorOutput); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(spec.Output, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, err
	}
	deadline, bounded := p.ctx.Deadline()
	if !bounded || p.ctx.Err() != nil {
		return nil, errors.Join(errInvalid, p.ctx.Err(), file.Close())
	}
	o := &nativeProtocolPredecessorOwner{p: p, hub: p.f.n.all[1].mux.hub, ctx: p.ctx, file: file, output: output, e: nativeProtocolPredecessorEvidence{Version: 1, Profile: nativeProtocolPredecessorProfile, Spec: spec, BaseDirectory: p.f.n.dir, BasePath: filepath.Join(p.f.n.dir, spec.Recipe.name()+".jsonl"), Mechanism: spec.mechanism(), DeadlineUnixNanos: deadline.UnixNano(), WorkGrant: 128*(4096+32*processFrameBytes) + 12*processFrameBytes + (16 << 20), OutputGrant: nativeProtocolPredecessorOutput, Events: []nativeProtocolPredecessorEvent{}}}
	p.f.t.Cleanup(func() {
		if err := o.close(); err != nil {
			p.f.t.Error("predecessor owner cleanup", err)
		}
	})
	return o, nil
}

func (o *nativeProtocolPredecessorOwner) event(action string) {
	o.e.Events = append(o.e.Events, nativeProtocolPredecessorEvent{uint64(len(o.e.Events) + 1), action})
}

func (o *nativeProtocolPredecessorOwner) start() error {
	if o == nil {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.ctx.Err() != nil || len(o.p.evidence.Elections) != 2 || o.closed {
		return errors.Join(errInvalid, o.ctx.Err())
	}
	o.event("actual-elections-and-reconnect-complete")
	o.e.Run = o.p.evidence.Elections[0].Owner.Run
	if o.e.Mechanism == "emitted-predecessor-tick-packets" || o.e.Spec.Recipe.Partial != nil {
		return nil
	}
	return o.installLocked()
}

func (o *nativeProtocolPredecessorOwner) installLocked() error {
	if o.installed {
		return errInvalid
	}
	h := o.hub
	h.mu.Lock()
	o.priorBefore, o.priorAfter = h.beforeRelay, h.afterRelay
	h.beforeRelay, h.afterRelay = o.before, o.after
	h.mu.Unlock()
	o.installed = true
	o.event("service-observer-attached")
	return nil
}

// Partial-install's existing gate requires an empty callback slot and releases
// it before its actual successor. No competing callback is installed earlier.
func (o *nativeProtocolPredecessorOwner) successor() error {
	if o == nil {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.e.Spec.Recipe.Partial == nil || o.e.Mechanism == "emitted-predecessor-tick-packets" {
		return nil
	}
	if o.closed || o.ctx.Err() != nil || o.installed {
		return errors.Join(errInvalid, o.ctx.Err())
	}
	o.event("actual-recovery-successor-observed")
	return o.installLocked()
}

func (o *nativeProtocolPredecessorOwner) gate() *nativeConjunctionBatchGate {
	o.p.f.mu.Lock()
	g := o.p.f.conjunctionGate
	o.p.f.mu.Unlock()
	return g
}

func (o *nativeProtocolPredecessorOwner) before(ctx context.Context, actor, partition uint64, request []byte) error {
	if o.priorBefore != nil {
		if err := o.priorBefore(ctx, actor, partition, request); err != nil {
			return err
		}
	}
	g := o.gate()
	if g == nil || g.selector.Actor != actor || g.selector.Partition != partition {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.sealed || o.closed || o.selected {
		return nil
	}
	if o.e.Candidates >= 128 || len(request) > nativeLogicalTransitionBodyBytes {
		o.terminal = errLimit
		return nil
	}
	o.e.Candidates++
	c, cross, err := nativeFaultPhase(request)
	if err != nil || !cross || c.phase != g.selector.Phase || c.ns.partition != partition {
		return err
	}
	q, err := decodeGraphSourceRequest(request)
	if err != nil {
		return err
	}
	p, err := decodeGraphSourceProposal(q.command)
	if err != nil {
		return err
	}
	h := o.p.f.n.all[actor].mux.hub
	h.mu.Lock()
	voter := uint64(1)
	if partition == 8 {
		voter = 4
	}
	if route := h.relayTargets[actor]; route != nil && route[partition] != 0 {
		voter = route[partition]
	}
	h.mu.Unlock()
	if p.target.partition != partition || p.target != mustProcessScopeFromProgram(o.p, partition) || voter != o.p.f.n.leaders[(partition-3)/5] {
		o.terminal = errCorrupt
		return nil
	}
	if o.e.RetainedBytes+uint64(len(request)) > nativeProtocolPredecessorPairBytes {
		o.terminal = errLimit
		return nil
	}
	o.selected = true
	o.e.Actor, o.e.Partition, o.e.Voter = actor, partition, voter
	o.e.SelectedRequest = bytes.Clone(request)
	o.e.RetainedBytes += uint64(len(request))
	o.event("selected-service-request-observed")
	if o.e.Mechanism != "selected-service-request-delay" {
		return nil
	}
	o.e.ConfiguredDelayMillis = 20
	o.event("selected-service-delay-begin")
	began := time.Now()
	timer := time.NewTimer(20 * time.Millisecond)
	select {
	case <-timer.C:
	case <-ctx.Done():
		timer.Stop()
		return ctx.Err()
	case <-o.ctx.Done():
		timer.Stop()
		return o.ctx.Err()
	}
	o.e.ObservedDelayNanos = time.Since(began).Nanoseconds()
	o.performed = true
	o.event("selected-service-delay-end")
	return nil
}

func mustProcessScopeFromProgram(p *nativeProtocolProgram, partition uint64) allocationScope {
	// The six-process fixture's actual bootstrap agreement is explicit. Every
	// complete decoded scope is additionally joined to native election evidence.
	n, _ := certifiedProcessNamespace(p.f.n.leaders[(partition-3)/5])
	d := partitionCodecRequest(p.f.t, 1, defaultMaterializerLimits()).declaration
	part, found := d.Partition(partition)
	if !found {
		return allocationScope{}
	}
	return allocationScope{graph: n.graph, partition: n.partition, group: part.Group, ownership: part.OwnershipEpoch, topology: d.TopologyEpoch(), declaration: d.Digest(), semantic: p.f.nativeAgreement()}
}

func (o *nativeProtocolPredecessorOwner) predecessor(partition uint64, phase crossPhase) bool {
	r := o.e.Spec.Recipe
	if r.Live != nil {
		if r.Live.Phase == crossPrepare {
			return partition == 3 && phase == crossPrepare
		}
		return partition == 8 && phase == crossPrepare
	}
	if r.Coordinator != nil {
		return partition == r.Coordinator.Coordinator && phase == crossDecide
	}
	return r.Partial != nil && partition == 8 && phase == crossDecide
}

func (o *nativeProtocolPredecessorOwner) after(ctx context.Context, actor, partition uint64, request, reply []byte) error {
	if o.priorAfter != nil {
		if err := o.priorAfter(ctx, actor, partition, request, reply); err != nil {
			return err
		}
	}
	if o.e.Mechanism != "completed-predecessor-service-cache-duplicate" {
		return nil
	}
	g := o.gate()
	if g == nil || g.selector.Actor != actor {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.sealed || o.closed || o.performed {
		return nil
	}
	if o.e.Candidates >= 128 || len(request)+2*len(reply)+4096 > nativeProtocolPredecessorPairBytes {
		o.terminal = errLimit
		return nil
	}
	o.e.Candidates++
	q, err := decodeGraphSourceRequest(request)
	if err != nil || q.kind != sourceCrossRead || len(q.command) != 0 || !o.predecessor(partition, q.phase) {
		return nil
	}
	r, err := decodeGraphWireResponse(reply)
	if err != nil || r.err != nil {
		return nil
	}
	a, err := decodeCrossAuthorityBody(r.body)
	wantYes := true
	if live := o.e.Spec.Recipe.Live; live != nil && live.Phase == crossDecide {
		wantYes = live.Commit
	}
	if err != nil || r.kind != q.kind || r.sequence != q.sequence || r.nonce != q.nonce || a.scope != r.peer.scope || a.id.session != r.issuer || a.question != (crossQuery{tx: q.tx, nonce: q.nonce, phase: q.phase}) || a.status.tx != q.tx || a.status.phase != q.phase || a.status.digest != sha256.Sum256(a.manifest) || a.status.phase == crossPrepare && a.status.yes != wantYes {
		o.terminal = errCorrupt
		return nil
	}
	h := o.p.f.n.all[actor].mux.hub
	h.mu.Lock()
	voter := uint64(1)
	if partition == 8 {
		voter = 4
	}
	if route := h.relayTargets[actor]; route != nil && route[partition] != 0 {
		voter = route[partition]
	}
	h.mu.Unlock()
	if r.peer.voter != voter || r.peer.scope != mustProcessScopeFromProgram(o.p, partition) {
		o.terminal = errCorrupt
		return nil
	}
	// The original endpoint is still waiting for this callback, so it cannot
	// advance this exact origin/peer stream between first reply and duplicate.
	o.e.PredecessorRequest, o.e.PredecessorReply = bytes.Clone(request), bytes.Clone(reply)
	o.e.RetainedBytes += uint64(len(request) + len(reply))
	o.event("causal-predecessor-service-reply-complete")
	o.event("same-service-cache-retransmit")
	call, cancel := context.WithTimeout(ctx, 10*time.Second)
	duplicate, transport := h.peer(voter).exchange(call, processRequest{op: processSourceServe, entity: actor, data: request})
	cause := errors.Join(transport, processResponseError(duplicate), call.Err())
	cancel()
	if cause != nil || duplicate.incarnation != o.p.f.n.all[voter].incarnation || !bytes.Equal(duplicate.aux, reply) || len(duplicate.packets) != 0 || len(duplicate.handles) != 0 {
		o.terminal = errors.Join(errCorrupt, cause)
		return nil
	}
	wire, err := encodeProcessResponse(duplicate)
	if err != nil || o.e.RetainedBytes+uint64(len(wire)) > nativeProtocolPredecessorPairBytes {
		o.terminal = errors.Join(errLimit, err)
		return nil
	}
	o.e.DuplicateResponse = wire
	o.e.RetainedBytes += uint64(len(wire))
	o.performed = true
	o.event("byte-identical-cache-reply-received")
	return nil
}

func (o *nativeProtocolPredecessorOwner) arm(selector nativeConjunctionSelector, command []byte) error {
	if o == nil {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.sealed || o.closed || o.ctx.Err() != nil || o.e.CommandSHA256 != ([32]byte{}) || !o.e.Spec.Recipe.matches(selector) || selector.Member != o.p.f.n.leaders[(selector.Partition-3)/5] {
		return errors.Join(errInvalid, o.ctx.Err())
	}
	o.e.CommandSHA256 = sha256.Sum256(command)
	if o.e.Mechanism != "emitted-predecessor-tick-packets" {
		q, err := decodeGraphSourceRequest(o.e.SelectedRequest)
		if err != nil {
			return err
		}
		p, err := decodeGraphSourceProposal(q.command)
		if err != nil || !bytes.Equal(p.wire, command) {
			return errors.Join(errCorrupt, err)
		}
		if o.e.Mechanism == "completed-predecessor-service-cache-duplicate" {
			prior, err := decodeGraphSourceRequest(o.e.PredecessorRequest)
			c, cross, decode := nativeFaultPhase(o.e.SelectedRequest)
			m, manifestErr := nativeConjunctionManifest(c)
			if err != nil || decode != nil || manifestErr != nil || !cross || prior.tx != m.tx {
				return errors.Join(errCorrupt, err, decode, manifestErr)
			}
			reply, err := decodeGraphWireResponse(o.e.PredecessorReply)
			if err != nil || reply.peer.scope.partition == p.target.partition && (q.sequence <= prior.sequence || q.sequence-prior.sequence != 1 || q.nonce == prior.nonce) {
				return errors.Join(errCorrupt, err)
			}
		}
		if !o.performed {
			return errors.Join(errInvalid, o.terminal)
		}
		o.event("selected-command-native-arm-complete")
		return o.terminal
	}
	o.e.Actor, o.e.Partition, o.e.Voter = selector.Actor, selector.Partition, selector.Member
	o.p.f.packetDrain()
	before, err := o.p.f.n.all[selector.Member].exchange(o.ctx, processRequest{op: processTick})
	if err = errors.Join(err, processResponseError(before)); err != nil {
		return err
	}
	if len(before.packets) < 1 || len(before.packets) > 2 || len(before.handles) != 0 {
		return errInvalid
	}
	for _, packet := range before.packets {
		if packet.Snapshot || packet.From != selector.Member || len(packet.Payload) == 0 || len(packet.Payload) > 64<<10 {
			return errLimit
		}
	}
	if o.e.Spec.Mode == "reorder" && len(before.packets) != 2 {
		return errInvalid
	}
	// These frames already exist before the selected command enters native
	// Propose/Step. The existing transport alone owns their actual deliveries.
	wire, err := encodeProcessResponse(before)
	if err != nil {
		return err
	}
	if len(wire) > 256<<10 {
		return errLimit
	}
	o.e.TickResponse = wire
	o.e.RetainedBytes = uint64(len(wire))
	o.event("predecessor-native-tick-returned-emitted-packets")
	delay := 0
	if o.e.Spec.Mode == "delay" {
		delay = 20
	}
	schedule, err := newNativeFaultPacketScheduler(o.ctx, o.e.Spec.Mode, before.packets[0].From, before.packets[0].To, 1, delay)
	if err != nil {
		return err
	}
	h := o.p.f.n.all[selector.Member].mux.hub
	finish, err := attachNativeFaultPacketScheduler(h, schedule)
	if err != nil {
		schedule.cancel()
		return err
	}
	if err := h.enqueue(selector.Member, before.packets, before.handles...); err != nil {
		_, closed := finish()
		return errors.Join(err, closed)
	}
	o.p.f.packetDrain()
	o.e.Packets, err = finish()
	if err != nil {
		return err
	}
	if len(o.e.Packets) != 1 {
		return errCorrupt
	}
	state, err := o.p.f.n.all[selector.Member].exchange(o.ctx, processRequest{op: processStateOp})
	if err = errors.Join(err, processResponseError(state)); err != nil {
		return err
	}
	o.e.AfterState, err = encodeProcessResponse(state)
	if err != nil {
		return err
	}
	o.e.RetainedBytes += uint64(len(o.e.AfterState))
	o.performed = true
	o.event("predecessor-packets-delivered-by-native-scheduler")
	o.event("selected-command-native-arm-complete")
	return nil
}

func (r nativeProtocolPredecessorRecipe) matches(s nativeConjunctionSelector) bool {
	if !r.valid() || !s.valid() || s.Kind != raftlog.StableStorageReady {
		return false
	}
	if r.Fence != nil {
		return s.Fence && s.Phase == 0 && s.Partition == 3 && s.Side == r.Fence.Side
	}
	if s.Fence {
		return false
	}
	if r.Live != nil {
		return s.Phase == r.Live.Phase && s.Partition == 8 && s.Side == r.Live.Side
	}
	if r.Coordinator != nil {
		return s.Phase == crossResolve && s.Partition == r.Coordinator.Partition && s.Side == r.Coordinator.Side
	}
	return s.Phase == crossResolve && s.Partition == 8 && s.Side == r.Partial.Side
}

// Seal does not tear down another callback owner's live stack. Semantic
// adapters finish their original gate/capture defers first; t.Cleanup restores
// this earliest observer only after the complete semantic case has returned.
func (o *nativeProtocolPredecessorOwner) seal() error {
	if o == nil {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.sealed {
		return o.terminal
	}
	o.sealed = true
	if !o.performed || o.e.CommandSHA256 == ([32]byte{}) || o.ctx.Err() != nil {
		o.terminal = errors.Join(errInvalid, o.terminal, o.ctx.Err())
	}
	if o.terminal != nil {
		return o.terminal
	}
	o.event("actual-selected-ready-loss-observed")
	body, err := json.Marshal(o.e)
	if err != nil || len(body)+4096 > nativeProtocolPredecessorOutput || o.output.Used() != nativeProtocolPredecessorOutput {
		return errors.Join(errLimit, err)
	}
	written, err := o.file.Write(append(body, '\n'))
	if written != len(body)+1 {
		return errors.Join(io.ErrShortWrite, err)
	}
	return err
}

func (o *nativeProtocolPredecessorOwner) close() error {
	if o == nil {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return nil
	}
	o.closed = true
	if o.installed {
		h := o.hub
		h.mu.Lock()
		h.beforeRelay, h.afterRelay = o.priorBefore, o.priorAfter
		h.mu.Unlock()
	}
	if !o.sealed {
		return errors.Join(errInvalid, o.file.Close())
	}
	return o.file.Close()
}

func nativeProtocolPredecessorRead(path string, maximum int) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	b, read := io.ReadAll(io.LimitReader(f, int64(maximum)+1))
	closed := f.Close()
	if len(b) > maximum {
		return nil, errors.Join(errLimit, read, closed)
	}
	return b, errors.Join(read, closed)
}
