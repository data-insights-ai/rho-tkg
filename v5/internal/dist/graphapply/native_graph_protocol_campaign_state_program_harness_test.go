package graphapply

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
)

const nativeProtocolStateProgramProfile = "native-protocol-owned-pooled-state-program-v1"

func nativeProtocolStateCheckerContext(t *testing.T, original int64) (context.Context, context.CancelFunc) {
	t.Helper()
	deadline := time.Unix(0, original)
	if cap := time.Now().Add(30 * time.Second); cap.Before(deadline) {
		deadline = cap
	}
	return context.WithDeadline(t.Context(), deadline)
}

// The legacy hold clears both callbacks at release. Restore only at that
// exact empty LIFO boundary; an intervening owner is never overwritten.
func nativeProtocolStateSavedHooks(h *graphProcessHub) func() error {
	h.mu.Lock()
	before, after := h.beforeRelay, h.afterRelay
	h.mu.Unlock()
	var mu sync.Mutex
	done := false
	var terminal error
	return func() error {
		mu.Lock()
		defer mu.Unlock()
		if done {
			return terminal
		}
		done = true
		h.mu.Lock()
		defer h.mu.Unlock()
		if h.beforeRelay != nil || h.afterRelay != nil {
			terminal = errInvalid
			return terminal
		}
		h.beforeRelay, h.afterRelay = before, after
		return nil
	}
}

func nativeProtocolStateCommittedGate(f *pairedGraphFaultNetwork) (<-chan pairedFaultBoundary, func(), func() error) {
	hub := f.n.all[4].mux.hub
	restore := nativeProtocolStateSavedHooks(hub)
	hub.mu.Lock()
	prior := hub.afterRelay
	hub.mu.Unlock()
	entered, release := f.gate(5, true)
	hub.mu.Lock()
	pause := hub.afterRelay
	hub.afterRelay = func(ctx context.Context, origin, part uint64, request, reply []byte) error {
		if prior != nil {
			if err := prior(ctx, origin, part, request, reply); err != nil {
				return err
			}
		}
		return pause(ctx, origin, part, request, reply)
	}
	hub.mu.Unlock()
	return entered, release, restore
}

type nativeProtocolStateProgramEvidence struct {
	Program                                                        nativeProtocolProgramEvidence
	Target                                                         uint64
	Window, Transport                                              string
	MessageCommand, MessageRequest, MessageReply, BusinessOriginal []byte
	BlockedCollectorResponse                                       []byte
	LocalRecord                                                    []byte
	Cohort                                                         []pairedFaultProcess
}

// This adapter owns no source or transaction. It reuses the original program
// owner's elections/packet scheduler and the existing semantic state's owners.
type nativeProtocolStateProgram struct {
	mu          sync.Mutex
	t           *testing.T
	ctx         context.Context
	f           *pairedGraphFaultNetwork
	base        *nativeProtocolProgram
	recipe      nativeProtocolStateRecipe
	evidence    nativeProtocolStateProgramEvidence
	hub         *graphProcessHub
	priorBefore func(context.Context, uint64, uint64, []byte) error
	priorAfter  func(context.Context, uint64, uint64, []byte, []byte) error
	business    bool
	armed       bool
	candidates  uint64
	terminal    error
}

func newNativeProtocolStateProgram(t *testing.T, ctx context.Context, f *pairedGraphFaultNetwork, recipe nativeProtocolStateRecipe, spec nativeProtocolProgramSpec) *nativeProtocolStateProgram {
	t.Helper()
	if !recipe.valid() || recipe.Family == "allocation" || nativeProtocolStateExpectedAgreement(t) != rangeMovePooledSemanticContractID() {
		t.Fatal("exact owned state program; accepted issuance proof is separate")
	}
	p := &nativeProtocolStateProgram{t: t, ctx: ctx, f: f, recipe: recipe}
	p.base = nativeProtocolProgramStart(t, ctx, f, spec)
	target, err := p.base.target(recipe.Partition, 2, 5)
	if err != nil {
		t.Fatal(err)
	}
	p.evidence.Target = target
	p.evidence.Window = "selected-command"
	if recipe.Family == "gc" {
		p.evidence.Window = "committed-K-predecessor"
	}
	if recipe.Family == "publication-race" {
		p.evidence.Window = "new-root-predecessor"
	}
	p.hub = f.n.all[2].mux.hub
	p.hub.mu.Lock()
	p.priorBefore, p.priorAfter = p.hub.beforeRelay, p.hub.afterRelay
	p.hub.beforeRelay, p.hub.afterRelay = p.before, p.after
	p.hub.mu.Unlock()
	t.Cleanup(p.restore)
	return p
}

func (p *nativeProtocolStateProgram) restore() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.hub == nil {
		return
	}
	p.hub.mu.Lock()
	p.hub.beforeRelay, p.hub.afterRelay = p.priorBefore, p.priorAfter
	p.hub.mu.Unlock()
	p.hub = nil
}

func (p *nativeProtocolStateProgram) arm(member, partition uint64, command []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.armed || p.ctx.Err() != nil || member != p.evidence.Target || partition != p.recipe.Partition {
		return errors.Join(errInvalid, p.ctx.Err())
	}
	if err := p.base.armNative(partition, member, command); err != nil {
		return err
	}
	p.armed = true
	p.evidence.MessageCommand = bytes.Clone(command)
	if p.evidence.Window == "selected-command" {
		p.evidence.Transport = "native-batch"
	}
	return nil
}

func (p *nativeProtocolStateProgram) beginBusiness(original allocationProposal) {
	if p == nil || p.recipe.Family != "gc" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.business || p.armed || p.ctx.Err() != nil {
		p.t.Fatal("one actual K predecessor under original deadline")
	}
	p.business = true
	p.evidence.BusinessOriginal = bytes.Clone(original.wire)
}

func (p *nativeProtocolStateProgram) localSubmit(proposal allocationProposal, encoded []byte) error {
	if p.recipe.Family != "publication-race" || proposal.target.partition != 3 {
		return errInvalid
	}
	if err := p.arm(p.evidence.Target, 3, proposal.wire); err != nil {
		return err
	}
	p.mu.Lock()
	p.evidence.Transport = "local-propose"
	p.evidence.MessageRequest = bytes.Clone(encoded)
	p.mu.Unlock()
	leader := p.f.n.leaders[0]
	response, err := p.f.n.all[leader].exchange(p.ctx, processRequest{op: processPropose, data: encoded})
	if err = errors.Join(err, processResponseError(response), p.ctx.Err()); err != nil {
		return err
	}
	if err = p.hub.enqueue(leader, response.packets, response.handles...); err != nil {
		return err
	}
	p.f.packetDrain()
	wire, err := encodeProcessResponse(response)
	if err != nil {
		return err
	}
	p.mu.Lock()
	p.evidence.MessageReply = wire
	p.mu.Unlock()
	return nil
}

func (p *nativeProtocolStateProgram) before(ctx context.Context, origin, partition uint64, wire []byte) error {
	if p.priorBefore != nil {
		if err := p.priorBefore(ctx, origin, partition, wire); err != nil {
			return err
		}
	}
	p.mu.Lock()
	active := p.business && !p.armed
	if active {
		if p.candidates >= 128 || len(wire) > nativeLogicalTransitionBodyBytes {
			p.mu.Unlock()
			return errLimit
		}
		p.candidates++
	}
	p.mu.Unlock()
	if !active || origin != 5 || partition != 3 {
		return nil
	}
	request, err := decodeGraphSourceRequest(wire)
	if err != nil {
		return err
	}
	if request.kind != sourceSubmit {
		return nil
	}
	proposal, err := decodeGraphSourceProposal(request.command)
	if err != nil {
		return err
	}
	if !isCrossPartitionWire(proposal.wire) {
		return nil
	}
	command, _, err := decodeCrossCommand(proposal.wire, defaultMaterializerLimits())
	if err != nil {
		return err
	}
	if command.phase != crossPrepare || command.ns.partition != 3 {
		return nil
	}
	arena, err := graphstate.NewOutputBudget(defaultMaterializerLimits().outputBytes)
	if err != nil {
		return err
	}
	manifest, err := decodeCrossManifest(command.manifest, defaultMaterializerLimits(), arena)
	if err != nil {
		return err
	}
	p06BindBusiness(p.t, allocationProposal{wire: p.evidence.BusinessOriginal}, manifest)
	if err := p.arm(p.evidence.Target, 3, proposal.wire); err != nil {
		return err
	}
	p.mu.Lock()
	p.evidence.Transport = "source-submit"
	p.evidence.MessageRequest = bytes.Clone(wire)
	p.mu.Unlock()
	return nil
}

func (p *nativeProtocolStateProgram) after(ctx context.Context, origin, partition uint64, wire, reply []byte) error {
	p.mu.Lock()
	if p.evidence.Transport == "source-submit" && bytes.Equal(wire, p.evidence.MessageRequest) {
		if origin != 5 || partition != 3 || len(p.evidence.MessageReply) != 0 {
			p.mu.Unlock()
			return errInvalid
		}
		p.evidence.MessageReply = bytes.Clone(reply)
	}
	p.mu.Unlock()
	if p.priorAfter != nil {
		return p.priorAfter(ctx, origin, partition, wire, reply)
	}
	return nil
}

func (p *nativeProtocolStateProgram) finish() *nativeProtocolStateProgramEvidence {
	p.t.Helper()
	actual := nativeProtocolProgramFinish(p.t, p.f)
	if actual == nil || !p.armed || p.terminal != nil {
		p.t.Fatal("unmatched actual state message program", p.terminal)
	}
	p.evidence.Program = *actual
	p.restore()
	p.evidence.Cohort = p.f.stopAll()
	return &p.evidence
}
