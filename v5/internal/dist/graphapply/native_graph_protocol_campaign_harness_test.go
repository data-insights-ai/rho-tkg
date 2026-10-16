package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/dist/replica"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
)

const nativeProtocolCampaignProfile = "native-protocol-storage-campaign-v1"

// One finite artifact allowance is admitted before fresh child construction.
// It funds diagnostic serialization, separately from source/native event work;
// it is an admission counter, never an allocation/RSS or performance claim.
func nativeProtocolArtifactAllowance(t *testing.T) *graphstate.OutputBudget {
	t.Helper()
	budget, err := graphstate.NewOutputBudget(16 << 20)
	if err != nil {
		t.Fatal(err)
	}
	if err := budget.Reserve(16 << 20); err != nil {
		t.Fatal(err)
	}
	return budget
}

// This classification is produced from an ACTUAL child API error, before the
// diagnostic JSON would otherwise lose joined native causes. It is never
// sufficient for recovery: the selected native receipt, exact unknown request,
// original identity, actual process loss and live outer context must also join.
func nativeProtocolPendingLoss(err error) bool {
	leaves, loss := 0, false
	var visit func(error, int) bool
	visit = func(actual error, depth int) bool {
		if actual == nil || depth > 8 || leaves >= 32 {
			return false
		}
		if many, ok := actual.(interface{ Unwrap() []error }); ok {
			children := many.Unwrap()
			if len(children) == 0 || len(children) > 32 {
				return false
			}
			for _, child := range children {
				if !visit(child, depth+1) {
					return false
				}
			}
			return true
		}
		if one, ok := actual.(interface{ Unwrap() error }); ok {
			return visit(one.Unwrap(), depth+1)
		}
		leaves++
		if actual == errCrossProcessPending {
			return true
		}
		if wire, ok := actual.(graphWireError); ok {
			const allowed = uint16(1<<3 | 1<<10)
			if wire.mask&allowed == 0 || wire.mask&^allowed != 0 {
				return false
			}
			loss = true
			return true
		}
		if remote, ok := actual.(processRemoteError); ok {
			// The child process envelope uses processCauseMask, whose native
			// unavailable bit is seven; source GSR masks use bit three instead.
			if remote.mask != 1<<7 || remote.code != 3 && remote.code != 6 {
				return false
			}
			loss = true
			return true
		}
		if actual == replica.ErrUnavailable || actual == graphstore.ErrSourceUnavailable {
			loss = true
			return true
		}
		return false // Includes unknown, limit, corruption, cancellation/expiry.
	}
	return visit(err, 0) && loss
}

// Only the fresh first-live-run adapter can already have a durable D after its
// selected native interruption. It consumes the ACTUAL capture (two Resolve
// replies or five Prepare/Decide/Resolve replies); it never fills missing rows.
// The caller additionally joins all six immutable originals and complete CDC.
func nativeProtocolRecoveryCapture(t *testing.T, capture *nativeConjunctionCapture, prepare allocationProposal, manifest crossManifest, decision crossStatus, commit bool) ([]p06Phase, nativeP06InitialCharge) {
	t.Helper()
	capture.close()
	capture.mu.Lock()
	count := len(capture.requests)
	capture.mu.Unlock()
	if count != 2 && count != 5 {
		t.Fatal("actual first-live-run recovery lacks complete closure replies", count)
	}
	rows, charge := nativeP06InitialTake(t, capture, count)
	if count == 5 {
		p06CheckRecoveryPhases(t, rows, prepare, manifest, decision, commit)
		return rows, charge
	}
	for i, row := range rows {
		partition := []uint64{3, 8}[i]
		proposal := p06SourceProposal(t, row.Request)
		command, _, err := decodeCrossCommand(proposal.wire, defaultMaterializerLimits())
		status, statusErr := decodeCrossStatus(p06ReplyBody(t, row.Reply))
		if err != nil || statusErr != nil || row.Partition != partition || command.phase != crossResolve || command.ns.partition != partition || !bytes.Equal(command.manifest, prepareManifest(t, prepare)) || status.phase != crossResolve || status.ns.partition != partition || status.tx != manifest.tx || status.digest != manifest.digest || status.round != decision.round || status.commit != commit {
			t.Fatal("known durable decision did not produce its actual two resolves", err, statusErr)
		}
	}
	return rows, charge
}

// This is the existing real proposal/packet path while a service reply is
// intentionally held. It drains only native packets; waiting for all relays
// would deadlock the still-live coordinator invocation on its own held reply.
func nativeProtocolSubmitHeld(f *pairedGraphFaultNetwork, actor uint64, proposal allocationProposal, want reason) p06LocalRecord {
	before := f.call(f.n.all[actor], processRequest{op: processStateOp})
	r := p02SubmitWhilePaused(f, actor, proposal)
	ns, _ := certifiedProcessNamespace(actor)
	o, err := decodePartitionGraphOutcome(r.Outcome, ns)
	if err != nil || o.reason != want || o.disposition != applied || o.hash != sha256.Sum256(proposal.wire) || o.index != r.Index {
		f.t.Fatal("held original local outcome", o, err)
	}
	after := f.call(f.n.all[actor], processRequest{op: processStateOp})
	if want != reasonNone && (len(r.CDC) != 0 || !bytes.Equal(before.image, after.image)) || want == reasonNone && len(r.CDC) == 0 {
		f.t.Fatal("held conflict/mutation differs from actual graph and CDC")
	}
	return r
}

// A live reply hold selects an already completed, authenticated service call.
// It neither cancels/restarts the caller nor converts its diagnostic bytes into
// a proposal. In particular, releasing an A YES can let the SAME coordinator
// invocation attempt B using its original endpoint set and remaining deadline.
type nativeProtocolReplySelector struct {
	Actor, Partition uint64
	Phase            crossPhase
	Transaction      [16]byte
	Peer             graphPeerIdentity // Configured actual pipe/scope, never reply self-report.
}

func (s nativeProtocolReplySelector) valid() bool {
	return s.Actor >= 1 && s.Actor <= 6 && (s.Partition == 3 || s.Partition == 8) && s.Phase >= crossRegister && s.Phase <= crossResolve && s.Transaction != ([16]byte{}) && s.Peer.scope.valid() && s.Peer.scope.partition == s.Partition && (s.Partition == 3 && s.Peer.voter >= 1 && s.Peer.voter <= 3 || s.Partition == 8 && s.Peer.voter >= 4 && s.Peer.voter <= 6)
}

type nativeProtocolReplyHold struct {
	mu                     sync.Mutex
	ctx                    context.Context
	cancel                 context.CancelFunc
	f                      *pairedGraphFaultNetwork
	hub                    *graphProcessHub
	selector               nativeProtocolReplySelector
	prior                  func(context.Context, uint64, uint64, []byte, []byte) error
	priorBefore            func(context.Context, uint64, uint64, []byte) error
	beforeMode             bool // Immutable diagnostic mode, not native authority.
	entered                chan pairedFaultBoundary
	released               chan struct{}
	releaseOnce, closeOnce sync.Once
	closed, selected       bool
	candidates, work       uint64
	output, retained       uint64
}

const (
	nativeProtocolReplyCandidates = 128
	nativeProtocolReplyBytes      = 128 << 10
	nativeProtocolReplyDecode     = 4096 + 32*processFrameBytes
)

func newNativeProtocolReplyHold(ctx context.Context, f *pairedGraphFaultNetwork, selector nativeProtocolReplySelector) (*nativeProtocolReplyHold, error) {
	return newNativeProtocolHold(ctx, f, selector, false)
}

func newNativeProtocolBeforeHold(ctx context.Context, f *pairedGraphFaultNetwork, selector nativeProtocolReplySelector) (*nativeProtocolReplyHold, error) {
	return newNativeProtocolHold(ctx, f, selector, true)
}

func newNativeProtocolHold(ctx context.Context, f *pairedGraphFaultNetwork, selector nativeProtocolReplySelector, before bool) (*nativeProtocolReplyHold, error) {
	if ctx == nil || f == nil || f.n == nil || f.parent == nil || !selector.valid() {
		return nil, errInvalid
	}
	deadline, bounded := ctx.Deadline()
	original, originalBounded := f.parent.Deadline()
	if !bounded || !originalBounded || deadline.After(original) || ctx.Err() != nil || f.parent.Err() != nil {
		return nil, errors.Join(errInvalid, ctx.Err(), f.parent.Err())
	}
	child := f.n.all[selector.Actor]
	if child == nil || child.closed || child.mux == nil {
		return nil, replica.ErrUnavailable
	}
	owned, cancel := context.WithCancel(ctx)
	h := &nativeProtocolReplyHold{ctx: owned, cancel: cancel, f: f, hub: child.mux.hub, selector: selector, beforeMode: before, entered: make(chan pairedFaultBoundary, 1), released: make(chan struct{})}
	f.mu.Lock()
	if f.protocolReplyHold != nil {
		f.mu.Unlock()
		cancel()
		return nil, errLimit
	}
	f.protocolReplyHold = h
	f.mu.Unlock()
	// Publication and teardown follow the same LIFO ownership as the existing
	// native gate/capture. One live hold belongs to the typed causal adapter;
	// its separately installed native gate never replaces afterRelay.
	h.hub.mu.Lock()
	target := uint64(1)
	if selector.Partition == 8 {
		target = 4
	}
	if route := h.hub.relayTargets[selector.Actor]; route != nil && route[selector.Partition] != 0 {
		target = route[selector.Partition]
	}
	if target != selector.Peer.voter || h.hub.peers[target] == nil || h.hub.peers[target].p != f.n.all[target] {
		h.hub.mu.Unlock()
		f.mu.Lock()
		f.protocolReplyHold = nil
		f.mu.Unlock()
		cancel()
		return nil, errInvalid
	}
	if before {
		h.priorBefore, h.hub.beforeRelay = h.hub.beforeRelay, h.before
	} else {
		h.prior, h.hub.afterRelay = h.hub.afterRelay, h.after
	}
	h.hub.mu.Unlock()
	context.AfterFunc(owned, h.close)
	return h, nil
}

func (h *nativeProtocolReplyHold) before(ctx context.Context, origin, partition uint64, request []byte) error {
	if h.priorBefore != nil {
		if err := h.priorBefore(ctx, origin, partition, request); err != nil {
			return err
		}
	}
	if origin != h.selector.Actor || partition != h.selector.Partition {
		return nil
	}
	h.mu.Lock()
	if h.closed || h.selected {
		h.mu.Unlock()
		return nil
	}
	if h.candidates == nativeProtocolReplyCandidates || len(request) > processFrameBytes {
		h.closed = true
		h.mu.Unlock()
		return nil
	}
	h.candidates++
	h.work += nativeProtocolReplyDecode
	h.mu.Unlock()
	command, cross, err := nativeFaultPhase(request)
	if err != nil || !cross || command.phase != h.selector.Phase {
		return nil
	}
	manifest, err := nativeConjunctionManifest(command)
	q, requestErr := decodeGraphSourceRequest(request)
	proposal, proposalErr := decodeGraphSourceProposal(q.command)
	if err != nil || requestErr != nil || proposalErr != nil || manifest.tx != h.selector.Transaction || proposal.target != h.selector.Peer.scope {
		return nil
	}
	h.mu.Lock()
	if h.closed || h.selected || h.ctx.Err() != nil || ctx.Err() != nil || len(request) > nativeProtocolReplyBytes {
		h.mu.Unlock()
		return nil
	}
	h.output = 8*nativeProtocolReplyBytes + 8192
	boundary := pairedFaultBoundary{Request: make([]byte, len(request))}
	copy(boundary.Request, request)
	h.retained, h.selected = uint64(len(request)), true
	h.entered <- boundary
	h.mu.Unlock()
	select {
	case <-h.released:
		return ctx.Err()
	case <-h.ctx.Done():
		return h.ctx.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (h *nativeProtocolReplyHold) after(ctx context.Context, origin, partition uint64, request, reply []byte) error {
	if h.prior != nil {
		if err := h.prior(ctx, origin, partition, request, reply); err != nil {
			return err
		}
	}
	if origin != h.selector.Actor || partition != h.selector.Partition {
		return nil
	}
	h.mu.Lock()
	if h.closed || h.selected {
		h.mu.Unlock()
		return nil
	}
	if h.candidates == nativeProtocolReplyCandidates || len(request) > processFrameBytes || len(reply) > processFrameBytes {
		h.closed = true
		h.mu.Unlock()
		return nil // Diagnostic exhaustion cannot create a service error/ACK.
	}
	h.candidates++
	h.work += nativeProtocolReplyDecode // Full bounded decode before traversal.
	h.mu.Unlock()
	q, err := decodeGraphSourceRequest(request)
	if err != nil || q.kind != sourceCrossRead || len(q.command) != 0 || q.phase != h.selector.Phase || q.tx != h.selector.Transaction {
		return nil // Unrelated native traffic keeps its actual result unchanged.
	}
	actual, err := decodeGraphWireResponse(reply)
	if err != nil || actual.err != nil {
		return nil // A refusal or malformed observation is never a selected YES.
	}
	// This decode is inert diagnostic equality. Only the original live endpoint
	// completion in production may consume this reply and seal its authority.
	observation, err := decodeCrossAuthorityBody(actual.body)
	status := observation.status
	if err != nil || observation.scope != h.selector.Peer.scope || actual.peer != h.selector.Peer || observation.id.session != actual.issuer || observation.question != (crossQuery{tx: q.tx, nonce: q.nonce, phase: q.phase}) || status.phase != h.selector.Phase || status.tx != h.selector.Transaction || status.ns.partition != partition || status.digest != sha256.Sum256(observation.manifest) || status.phase == crossPrepare && !status.yes || actual.kind != q.kind || actual.sequence != q.sequence || actual.nonce != q.nonce {
		return nil
	}
	h.mu.Lock()
	if h.closed || h.selected || h.ctx.Err() != nil || ctx.Err() != nil || len(request)+len(reply) > nativeProtocolReplyBytes {
		h.mu.Unlock()
		return nil
	}
	h.output = 8*nativeProtocolReplyBytes + 8192 // Binary + later JSON diagnostic ownership, before copying.
	boundary := pairedFaultBoundary{Request: make([]byte, len(request)), Reply: make([]byte, len(reply))}
	copy(boundary.Request, request)
	copy(boundary.Reply, reply)
	h.retained, h.selected = uint64(len(request)+len(reply)), true
	h.entered <- boundary
	h.mu.Unlock()
	select {
	case <-h.released:
	case <-h.ctx.Done():
	case <-ctx.Done():
	}
	// This callback observed a real completed reply. Cancellation retires its
	// original caller; it must not invent a nonzero transport mask with that body.
	return nil
}

func (h *nativeProtocolReplyHold) release() {
	h.releaseOnce.Do(func() { close(h.released) })
}

func (h *nativeProtocolReplyHold) close() {
	if h == nil {
		return
	}
	h.closeOnce.Do(func() {
		h.release()
		h.cancel()
		h.mu.Lock()
		h.closed = true
		h.mu.Unlock()
		h.hub.mu.Lock()
		if h.beforeMode {
			h.hub.beforeRelay = h.priorBefore
		} else {
			h.hub.afterRelay = h.prior
		}
		h.hub.mu.Unlock()
		h.f.mu.Lock()
		if h.f.protocolReplyHold == h {
			h.f.protocolReplyHold = nil
		}
		h.f.mu.Unlock()
	})
}
