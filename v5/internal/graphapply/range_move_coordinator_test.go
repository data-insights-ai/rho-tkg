package graphapply

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// This wire delivers actual native Host events. Faults drop a request or a
// completed reply; they never replace native statuses, proofs, or page data.
type rangeMoveCoordinatorWire struct {
	*nativeGraphTestTransport
	intents                                [][]byte
	dropFence, dropDecision                bool
	calls, captures, manifestSteps, aborts int
	lastKind                               byte
	lastMask                               uint16
}

func (w *rangeMoveCoordinatorWire) RoundTrip(ctx context.Context, wire []byte) ([]byte, error) {
	w.calls++
	r, err := decodeGraphSourceRequest(wire)
	if err != nil {
		return nil, err
	}
	if r.kind == sourceSubmit {
		proposal, err := decodeGraphSourceProposal(r.command)
		if err != nil {
			return nil, err
		}
		command, err := decodeRangeMoveCommand(proposal.wire)
		if err != nil {
			return nil, err
		}
		if command.phase == rangeMoveFence {
			w.intents = append(w.intents, bytes.Clone(command.intent))
			if w.dropFence {
				w.dropFence = false
				return nil, io.EOF
			}
		}
		if command.phase == rangeMoveCapture {
			w.captures++
		}
		if command.phase == rangeMoveDescribe && command.sequence > 0 {
			w.manifestSteps++
		}
		if command.phase == rangeMoveAbort {
			w.aborts++
		}
		if command.phase == rangeMoveDecide && w.dropDecision {
			w.dropDecision = false
			if _, err := w.nativeGraphTestTransport.RoundTrip(ctx, wire); err != nil {
				return nil, err
			}
			return nil, io.EOF
		}
	}
	response, err := w.nativeGraphTestTransport.RoundTrip(ctx, wire)
	w.lastKind = r.kind
	if err == nil {
		parsed, e := decodeGraphWireResponse(response)
		if e == nil {
			w.lastMask = graphErrorMask(parsed.err)
		}
	}
	return response, err
}

func reconnectRangeMoveCoordinatorWires(t *testing.T, wires []*rangeMoveCoordinatorWire) {
	t.Helper()
	// A replacement coordinator owns new authenticated connection streams.
	// Reconnect through the production service constructor; retained native
	// Host state, source captures, controls, and issued proof sessions survive.
	for _, wire := range wires {
		old := wire.service
		service, err := newGraphPeerService(old.host, old.sources, old.pump)
		if err != nil {
			t.Fatal(err)
		}
		wire.service = service
	}
}

func rangeMoveCoordinatorFixture(t *testing.T, maxPages int) (*allocationTestNetwork, *rangeMoveCoordinator, rangeMoveIntent, []*rangeMoveCoordinatorWire, map[uint64]graphPeerWireTransport) {
	t.Helper()
	n := newCrossPartitionNetworkAgreement(t, func(p *raftlog.ApplicationPolicy) {
		p.RetainedApplicationBytes = 64 << 20
		p.RetainedApplicationRecords = 32768
	}, rangeMoveSemanticContractID())
	// Nonempty divergent owners: node1 and its life start on3; node17 and its
	// life start on8. The uncommitted cross-edge fixture result is unused.
	_, _ = crossEdgeFixture(t, n)
	transports := make(map[uint64]graphPeerWireTransport)
	wires := make([]*rangeMoveCoordinatorWire, 2)
	for group, partition := range []uint64{3, 8} {
		h := n.hosts[group][0]
		server, err := newGraphSourceServer(t.Context(), h, defaultGraphSourceLimits(h.machine.limits))
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
	c, err := newRangeMoveCoordinator(h, transports)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := h.currentConfiguration()
	if err != nil {
		t.Fatal(err)
	}
	configuration, err := cfg.digest()
	if err != nil {
		t.Fatal(err)
	}
	view, err := h.machine.store.ApplicationView(h.driver.Applied())
	if err != nil {
		t.Fatal(err)
	}
	publication, owner, _, err := graphstore.LookupPublishedRange(t.Context(), view, graphstore.Namespace{Graph: graphstate.GraphID(h.machine.ns.graph), Partition: h.machine.ns.partition}, 1, configuration, graphstore.OwnershipBudget{SourceRows: 64, SourceBytes: 1 << 20, OutputBytes: 1 << 20})
	err = errors.Join(err, view.Close())
	if err != nil {
		t.Fatal(err)
	}
	target, err := h.target(8)
	if err != nil {
		t.Fatal(err)
	}
	intent := rangeMoveIntent{id: n.requestID(), source: h.machine.scope(), target: target, publication: publication, epoch: owner.Epoch, maxPages: maxPages, pageRows: 64, pageBytes: 8 << 10}
	return n, c, intent, wires, transports
}

func TestRangeMoveCoordinatorRunResumePreservesNonemptyOldView(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n, c, intent, wires, _ := rangeMoveCoordinatorFixture(t, 32)
	old, closeOld := crossView(t, n, 0, [2]uint64{})
	defer closeOld()
	assertCertifiedProjection(t, old, 1, true, nil)
	assertCertifiedProjection(t, old, 17, true, nil)
	done, err := runRangeMoveToCompletion(t, c, intent)
	if err != nil || !done.committed || done.source.phase != rangeMoveFinish || done.target.phase != rangeMoveFinish || done.decision.phase != rangeMoveDecide || done.decision.round == 0 {
		t.Fatalf("actual native range did not complete source=%+v target=%+v wire1=%d/%x wire2=%d/%x err=%v", done.source, done.target, wires[0].lastKind, wires[0].lastMask, wires[1].lastKind, wires[1].lastMask, err)
	}
	window, err := temporal.All(graphGenesisBinding(t).Axis())
	if err != nil {
		t.Fatal(err)
	}
	proposal, err := n.hosts[1][0].PreparePartitionGraphOperations(n.requestID(), []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Scope: window, Name: "after-move"}}, 39, nil)
	if err != nil {
		t.Fatal("actual target refused mutation of moved stable ID", err)
	}
	n.submitPartitionGraph(1, proposal, reasonNone)
	for member := range 3 {
		current, close := crossView(t, n, member, [2]uint64{})
		assertCertifiedProjection(t, current, 1, true, []string{"after-move"})
		assertCertifiedProjection(t, current, 17, true, nil)
		close()
	}
	// Mutation-then-old-query guards against replacing the retained source view
	// with current target state, even though both must preserve stable IDs.
	assertCertifiedProjection(t, old, 1, true, nil)
	assertCertifiedProjection(t, old, 17, true, nil)
	again, err := c.Resume(t.Context(), done.resume)
	if err != nil || again != done {
		t.Fatal("resume changed immutable native outcome", again, done, err)
	}
	changed := intent
	changed.pageBytes++
	if _, err := c.Run(t.Context(), changed); !errors.Is(err, errInvalid) {
		t.Fatal("changed cap reused original source identity", err)
	}
}

func TestRangeMoveCoordinatorUnknownFenceRetainsExactOriginal(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	_, c, intent, wires, _ := rangeMoveCoordinatorFixture(t, 32)
	wires[0].dropFence = true
	pending, err := c.Run(t.Context(), intent)
	if !errors.Is(err, errRangeMovePending) || !errors.Is(err, io.EOF) || pending.committed || len(wires[0].intents) != 1 {
		t.Fatal("uncertain fence lost exact retry identity", pending, err)
	}
	changed := intent
	changed.pageRows++
	if _, err := c.Run(t.Context(), changed); !errors.Is(err, errInvalid) {
		t.Fatal("unknown fence was replanned with changed bounds", err)
	}
	done := rangeMoveKeeperResume(t, c, pending.resume)
	if !done.committed || len(wires[0].intents) != 2 || !bytes.Equal(wires[0].intents[0], wires[0].intents[1]) {
		t.Fatal("resume did not retransmit exact original intent", done, err)
	}
}

func TestRangeMoveCoordinatorDurableDecisionSurvivesCoordinatorReplacement(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n, c, intent, wires, transports := rangeMoveCoordinatorFixture(t, 32)
	wires[0].dropDecision = true
	pending, err := runRangeMoveToCompletion(t, c, intent)
	if !errors.Is(err, errRangeMovePending) || !errors.Is(err, io.EOF) || pending.committed {
		t.Fatal("lost COMMIT reply became completed or aborted", pending, err)
	}
	captures := wires[0].captures + wires[1].captures
	reconnectRangeMoveCoordinatorWires(t, wires)
	restarted, err := newRangeMoveCoordinator(n.hosts[0][0], transports)
	if err != nil {
		t.Fatal(err)
	}
	done := rangeMoveKeeperResume(t, restarted, pending.resume)
	if !done.committed || done.decision.phase != rangeMoveDecide || done.source.phase != rangeMoveFinish || done.target.phase != rangeMoveFinish {
		t.Fatal("new coordinator did not recover actual durable COMMIT", done, err)
	}
	if wires[0].captures+wires[1].captures != captures {
		t.Fatal("restart recaptured source instead of resuming exact stored progress")
	}
}

func TestRangeMoveCoordinatorImmutablePageCapDurablyAborts(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n, c, intent, wires, transports := rangeMoveCoordinatorFixture(t, 1)
	before, closeBefore := crossView(t, n, 0, [2]uint64{})
	defer closeBefore()
	done, err := c.Run(t.Context(), intent)
	if !errors.Is(err, errLimit) || errors.Is(err, errRangeMovePending) || done.committed || done.source.phase != rangeMoveAbort || done.source.sequence != 0 || done.target.phase != rangeMoveAbort || done.target.sequence != 0 {
		t.Fatal("immutable page cap left a pending fence", done, err)
	}
	assertCertifiedProjection(t, before, 1, true, nil)
	current, close := crossView(t, n, 0, [2]uint64{})
	assertCertifiedProjection(t, current, 1, true, nil)
	assertCertifiedProjection(t, current, 17, true, nil)
	close()
	reconnectRangeMoveCoordinatorWires(t, wires)
	restarted, err := newRangeMoveCoordinator(n.hosts[0][0], transports)
	if err != nil {
		t.Fatal(err)
	}
	again, err := restarted.Resume(t.Context(), done.resume)
	if !errors.Is(err, errLimit) || errors.Is(err, errRangeMovePending) || again != done {
		t.Fatal("restart lost durable pre-COMMIT abort outcome", again, done, err)
	}
}

func TestRangeMoveCoordinatorNilCancelledAndForeignResumeRefuse(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	if got, err := newRangeMoveCoordinator(nil, nil); got != nil || !errors.Is(err, errInvalid) {
		t.Fatal("nil Host admitted coordinator", err)
	}
	if _, err := (*rangeMoveCoordinator)(nil).Run(t.Context(), rangeMoveIntent{}); !errors.Is(err, errInvalid) {
		t.Fatal("nil Run", err)
	}
	if _, err := (*rangeMoveCoordinator)(nil).Resume(t.Context(), rangeMoveResume{}); !errors.Is(err, errInvalid) {
		t.Fatal("nil Resume", err)
	}
	_, c, intent, wires, _ := rangeMoveCoordinatorFixture(t, 32)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := c.Run(ctx, intent); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled Run admitted source work", err)
	}
	if wires[0].calls+wires[1].calls != 0 {
		t.Fatal("cancelled invocation reached transport")
	}
	if _, err := c.Resume(t.Context(), rangeMoveResume{scope: intent.target, request: intent.id}); !errors.Is(err, errInvalid) {
		t.Fatal("foreign source resume admitted")
	}
}

func TestRangeMoveCoordinatorOperationLedgerExhaustionResumesWithoutAbort(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	_, c, intent, wires, _ := rangeMoveCoordinatorFixture(t, 32)
	// Tighten client invocation work only. Native Host/page policies and the
	// original immutable request caps retain their genuine configured values.
	c.limits.sourceRows = 512
	pending, err := c.Run(t.Context(), intent)
	if !errors.Is(err, errRangeMovePending) || !errors.Is(err, errLimit) || pending.committed || wires[0].manifestSteps == 0 {
		t.Fatal("operation ledger did not exhaust after actual durable manifest work", pending, wires[0].manifestSteps, err)
	}
	captures := wires[0].captures + wires[1].captures
	var done rangeMoveResult
	for range 8 {
		done, err = c.Resume(t.Context(), pending.resume)
		if !errors.Is(err, errRangeMovePending) {
			break
		}
	}
	if err != nil || !done.committed || done.source.phase != rangeMoveFinish || done.target.phase != rangeMoveFinish {
		t.Fatal("fresh bounded invocation did not resume exact durable cursor", done, err)
	}
	if wires[0].aborts+wires[1].aborts != 0 || wires[0].captures+wires[1].captures != captures {
		t.Fatal("consumed operation work became ABORT or source recapture")
	}
}

func TestRangeMoveCoordinatorProtectedAbortUsesParentBudget(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	output, err := graphstate.NewOutputBudget(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	l := defaultMaterializerLimits()
	budget := newGraphRemoteBudget(l, output)
	protected, arena, err := rangeMoveProtectAbort(budget)
	if err != nil || output.Used() != 64<<10 || budget.calls != 1 || budget.bytes != 8192 || protected.maxCalls != 1 || arena.Remaining() != 64<<10 {
		t.Fatal("abort protection refreshed or omitted parent admission", err)
	}
	if err := protected.admit(4096, 1024); err != nil {
		t.Fatal("prepaid bounded exchange refused", err)
	}
	if err := protected.admit(1, 1); !errors.Is(err, errLimit) {
		t.Fatal("prepaid single exchange allowance reset", err)
	}
	if output.Used() != 64<<10 || budget.bytes != 8192 {
		t.Fatal("detached prepaid spending changed original reservation")
	}
	if _, _, err := rangeMoveProtectAbort(nil); !errors.Is(err, errInvalid) {
		t.Fatal("nil protection budget", err)
	}
}

// Each explicit Resume is a new admitted operation, carrying only the original
// immutable identity and recovering the exact durable cursor. No operation
// budget is reset inside Run, and unrelated errors remain failures.
func runRangeMoveToCompletion(t *testing.T, c *rangeMoveCoordinator, intent rangeMoveIntent) (rangeMoveResult, error) {
	t.Helper()
	result, err := c.Run(t.Context(), intent)
	for range 128 {
		if !errors.Is(err, errRangeMovePending) {
			return result, err
		}
		if !errors.Is(err, errLimit) && !errors.Is(err, graphstate.ErrResourceLimit) && !errors.Is(err, graphstore.ErrResourceLimit) {
			return result, err
		}
		t.Logf("explicit bounded Resume source=%d/%d target=%d/%d", result.source.phase, result.source.sequence, result.target.phase, result.target.sequence)
		result, err = c.Resume(t.Context(), result.resume)
	}
	return result, err
}
