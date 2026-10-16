package graphapply

import (
	"bytes"
	"context"
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

// Gates retain only exact proposals produced by the actual coordinator. They
// drop a request before delivery; no successful proof or phase is synthesized.
type rangeMoveImportGuardGate struct {
	graphPeerWireTransport
	mode                                       byte
	stopped                                    bool
	cancelOnStop                               context.CancelFunc
	describe, importZero, preparation, pending allocationProposal
}

func (g *rangeMoveImportGuardGate) RoundTrip(ctx context.Context, wire []byte) ([]byte, error) {
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
		if command.phase == rangeMoveDescribe && command.sequence == 0 {
			g.describe = proposal
		}
		if command.phase == rangeMoveImport && command.sequence == 0 {
			g.importZero = proposal
		}
		if command.phase == rangeMoveImport && command.sequence == 1 && bytes.Equal(command.data, []byte{0}) {
			g.preparation = proposal
		}
		stop := g.mode == 1 && command.phase == rangeMoveDescribe && command.sequence == 0 ||
			g.mode == 2 && command.phase == rangeMoveImport && command.sequence == 0 ||
			g.mode == 3 && command.phase == rangeMoveImport && command.sequence == 2 ||
			g.mode == 4 && command.phase == rangeMoveImport && len(command.data) > 0 && command.data[0] == 2
		if stop {
			g.stopped, g.pending = true, proposal
			if g.cancelOnStop != nil {
				g.cancelOnStop()
				return nil, ctx.Err()
			}
			return nil, io.EOF
		}
	}
	return g.graphPeerWireTransport.RoundTrip(ctx, wire)
}

type rangeMoveImportGuardState struct {
	transition rangeMoveFrozenGuardState
	owner      raftlog.ApplicationRecoveryOwner
	usage      raftlog.ApplicationUsage
	cdc        []byte
}

func rangeMoveImportGuardStates(t *testing.T, n *allocationTestNetwork, move [16]byte) [6]rangeMoveImportGuardState {
	t.Helper()
	var states [6]rangeMoveImportGuardState
	for group := range 2 {
		for member, h := range n.hosts[group] {
			state := &states[3*group+member]
			state.transition = rangeMoveFrozenGuardStateAt(t, h, move)
			state.owner = rangeMoveAdmissionOwner(t, h)
			var err error
			state.cdc, err = h.machine.store.ApplicationRecord(t.Context(), h.driver.Applied(), false, h.machine.store.ApplicationLimits().MaxChangeBytes)
			if err != nil {
				t.Fatal(err)
			}
			state.usage, err = h.machine.store.ApplicationUsage()
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	return states
}

func rangeMoveImportGuardReader(t *testing.T, h *allocationHost) *reader {
	t.Helper()
	view, err := h.machine.store.ApplicationView(h.driver.Applied())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := view.Close(); err != nil {
			t.Error(err)
		}
	})
	base, err := view.RootBounded(t.Context(), 172)
	if err != nil {
		t.Fatal(err)
	}
	output, err := graphstate.NewOutputBudget(h.machine.limits.outputBytes)
	if err != nil {
		t.Fatal(err)
	}
	return &reader{ctx: t.Context(), view: view, ns: h.machine.ns, base: base, limits: h.machine.limits.allocation, arena: output, bytes: cap(base.Image)}
}

func rangeMoveImportGuardResumeToPause(t *testing.T, c *rangeMoveCoordinator, resume rangeMoveResume, gate *rangeMoveImportGuardGate) rangeMoveResult {
	t.Helper()
	var result rangeMoveResult
	var err error
	for range 128 {
		result, err = c.Resume(t.Context(), resume)
		if !errors.Is(err, errRangeMovePending) || !errors.Is(err, errLimit) && !errors.Is(err, graphstate.ErrResourceLimit) && !errors.Is(err, graphstore.ErrResourceLimit) {
			break
		}
	}
	if !gate.stopped || !errors.Is(err, errRangeMovePending) || !errors.Is(err, io.EOF) || result.committed || result.resume != resume {
		t.Fatal("original bounded Resume did not reach native gate", result, err)
	}
	return result
}

// This is the full pure materializer Stage boundary over an actual native
// retained base. Its private candidate entry/batch is never installed or
// described as a consensus result; malformed commands remain adversarial data.
func rangeMoveImportGuardStage(t *testing.T, h *allocationHost, command rangeMoveCommand) (raftlog.ApplicationBatch, error) {
	t.Helper()
	wire, err := encodeRangeMoveCommand(command)
	if err != nil {
		t.Fatal("adversarial command is not canonical admitted framing", err)
	}
	view, err := h.machine.store.ApplicationView(h.driver.Applied())
	if err != nil {
		t.Fatal(err)
	}
	base, err := view.RootBounded(t.Context(), 172)
	err = errors.Join(err, view.Close())
	if err != nil {
		t.Fatal(err)
	}
	term, err := h.machine.store.Term(base.Index)
	if err != nil {
		t.Fatal(err)
	}
	return h.machine.Stage(replica.Entry{Index: base.Index + 1, Generation: base.Generation, Term: term, Data: wire}, h.machine.store.ApplicationBudget())
}

func rangeMoveImportGuardCounterpart(t *testing.T, h *allocationHost, proposal allocationProposal) rangeMoveCommand {
	t.Helper()
	command, err := decodeRangeMoveCommand(proposal.wire)
	if err != nil {
		t.Fatal("not the exact native-proposed counterpart", err)
	}
	batch, err := rangeMoveImportGuardStage(t, h, command)
	if err != nil || len(batch.Writes) == 0 || len(batch.Changes) != 0 {
		t.Fatal("actual native-proposed Stage counterpart failed or emitted business CDC", err)
	}
	return command
}

func rangeMoveImportGuardRefusal(t *testing.T, n *allocationTestNetwork, group int, command rangeMoveCommand, want error, move [16]byte) {
	t.Helper()
	before := rangeMoveImportGuardStates(t, n, move)
	batch, err := rangeMoveImportGuardStage(t, n.hosts[group][0], command)
	if !errors.Is(err, want) || !reflect.ValueOf(batch).IsZero() {
		t.Fatal("invalid continuation/import returned a partial batch or lost sentinel", command.phase, command.sequence, err, want)
	}
	if after := rangeMoveImportGuardStates(t, n, move); !reflect.DeepEqual(after, before) {
		t.Fatal("refused Stage changed native root/cursor/capture/owner/CDC or leaked views")
	}
}

func TestRangeMoveNativeDescribeAndImportRefusalsPreserveOriginalContinuationAndCredits(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n, _, intent, wires, transports := rangeMoveCoordinatorFixture(t, 64)
	sourceGate := &rangeMoveImportGuardGate{graphPeerWireTransport: transports[3], mode: 1}
	targetGate := &rangeMoveImportGuardGate{graphPeerWireTransport: transports[8]}
	transports[3], transports[8] = sourceGate, targetGate
	coordinator, err := newRangeMoveCoordinator(n.hosts[0][0], transports)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := runRangeMoveToCompletion(t, coordinator, intent)
	if !sourceGate.stopped || !errors.Is(err, errRangeMovePending) || !errors.Is(err, io.EOF) || pending.committed {
		t.Fatal("native move did not pause at original Describe0", pending, err)
	}
	move := pending.resume.move
	captures := wires[0].captures + wires[1].captures
	describe := rangeMoveImportGuardCounterpart(t, n.hosts[0][0], sourceGate.describe)
	output, err := graphstate.NewOutputBudget(defaultMaterializerLimits().outputBytes)
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := graphstore.DecodeRangeMoveDescriptor(describe.data, output)
	if err != nil {
		t.Fatal(err)
	}
	actualSourceCapture, err := decodeRangeMoveStatus(rangeMoveFrozenGuardStateAt(t, n.hosts[0][0], move).status)
	if err != nil || actualSourceCapture.phase != rangeMoveCapture {
		t.Fatal("genuine source capture status", err)
	}
	for _, tc := range []struct {
		name string
		edit func(*testing.T, *rangeMoveCommand)
	}{
		{"missing-description", func(_ *testing.T, c *rangeMoveCommand) { c.data = nil }},
		{"missing-target-capture-authority", func(_ *testing.T, c *rangeMoveCommand) { c.proofs = nil }},
		{"genuine-source-proof-cannot-replace-target-capture", func(_ *testing.T, c *rangeMoveCommand) {
			c.proofs = []rangeMoveStatus{actualSourceCapture}
		}},
		{"source-capture-index-substitution", func(t *testing.T, c *rangeMoveCommand) {
			d := descriptor
			d.Source.Index++
			c.data, err = graphstore.EncodeRangeMoveDescriptor(d, output)
			if err != nil {
				t.Fatal(err)
			}
		}},
		{"target-capture-index-substitution", func(t *testing.T, c *rangeMoveCommand) {
			d := descriptor
			d.Target.Index++
			c.data, err = graphstore.EncodeRangeMoveDescriptor(d, output)
			if err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			command := describe
			tc.edit(t, &command)
			rangeMoveImportGuardRefusal(t, n, 0, command, errInvalid, move)
		})
	}
	sourceGate.mode = 0
	targetGate.mode, targetGate.stopped = 2, false
	pending = rangeMoveImportGuardResumeToPause(t, coordinator, pending.resume, targetGate)
	importZero := rangeMoveImportGuardCounterpart(t, n.hosts[1][0], targetGate.importZero)
	t.Run("inert-source-manifest-corruption", func(t *testing.T) {
		command := importZero
		command.data = bytes.Clone(command.data)
		command.data[len(command.data)-1] ^= 1
		rangeMoveImportGuardRefusal(t, n, 1, command, errCorrupt, move)
	})
	t.Run("import-needs-actual-source-manifest-authority", func(t *testing.T) {
		command := importZero
		command.proofs = nil
		rangeMoveImportGuardRefusal(t, n, 1, command, errInvalid, move)
	})
	t.Run("source-cannot-stage-target-import", func(t *testing.T) {
		command := importZero
		command.scope = intent.source
		rangeMoveImportGuardRefusal(t, n, 0, command, errInvalid, move)
	})
	t.Run("target-cannot-describe-source-capture", func(t *testing.T) {
		command := importZero
		command.phase = rangeMoveDescribe
		rangeMoveImportGuardRefusal(t, n, 1, command, errInvalid, move)
	})
	t.Run("same-move-foreign-partition-recovery-owner", func(t *testing.T) {
		before := rangeMoveImportGuardStates(t, n, move)
		h := n.hosts[1][0]
		q := rangeMoveImportGuardReader(t, h)
		owner := before[0].owner // Genuine source owner, never a fabricated ID.
		if owner.ID == before[3].owner.ID || owner.Digest != before[3].owner.Digest {
			t.Fatal("native same-move owners did not retain partition qualification")
		}
		owned, err := h.machine.moveOwnsRecovery(q, owner)
		if owned || !errors.Is(err, errCorrupt) {
			t.Fatal("target borrowed genuine source partition's recovery owner", owned, err)
		}
		if err := q.view.Close(); err != nil {
			t.Fatal(err)
		}
		if after := rangeMoveImportGuardStates(t, n, move); !reflect.DeepEqual(after, before) {
			t.Fatal("refused foreign owner changed native state or leaked view")
		}
	})
	t.Run("cancelled-target-capture-access", func(t *testing.T) {
		before := rangeMoveImportGuardStates(t, n, move)
		h := n.hosts[1][0]
		q := rangeMoveImportGuardReader(t, h)
		current, err := decodeRangeMoveStatus(before[3].transition.status)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		q.ctx = ctx
		next := rangeMoveStatus{ns: h.machine.ns, move: move, intent: current.intent, phase: rangeMoveImport, index: q.base.Index + 1, result: rangeMoveApplied}
		batch, err := h.machine.stageRangeMoveTarget(q, importZero, intent, current, next, h.machine.store.ApplicationBudget())
		if !errors.Is(err, context.Canceled) || !reflect.ValueOf(batch).IsZero() || len(q.writes) != 0 {
			t.Fatal("cancelled native target C access returned effects or lost cancellation", err)
		}
		if err := q.view.Close(); err != nil {
			t.Fatal(err)
		}
		if after := rangeMoveImportGuardStates(t, n, move); !reflect.DeepEqual(after, before) {
			t.Fatal("cancelled import changed native state or leaked view")
		}
	})
	targetGate.mode, targetGate.stopped = 3, false
	pending = rangeMoveImportGuardResumeToPause(t, coordinator, pending.resume, targetGate)
	pageCommand := rangeMoveImportGuardCounterpart(t, n.hosts[1][0], targetGate.pending)
	if len(pageCommand.data) < 2 || pageCommand.data[0] != 1 || len(targetGate.preparation.wire) == 0 {
		t.Fatal("native Import2 is not an actual graph page after preparation")
	}
	// A genuine earlier Import0 retry at current Import1 is a native commit,
	// unlike the private malformed-data probes below. It must keep the original
	// status/cursor/owner and consume no remaining completion credit.
	beforeReplay := rangeMoveImportGuardStates(t, n, move)
	reservation := rangeMoveAdmissionReservation(t, n.hosts[1][0])
	pump := nativeGraphTestPump{n: n, group: 1, host: n.hosts[1][0]}
	response, err := pump.Submit(t.Context(), targetGate.importZero)
	replayed, decodeErr := decodeRangeMoveStatus(response)
	original, originalErr := decodeRangeMoveStatus(beforeReplay[3].transition.status)
	if err != nil || decodeErr != nil || originalErr != nil || replayed != original {
		t.Fatal("actual earlier Import0 retry reinterpreted current native outcome", replayed, original, err, decodeErr, originalErr)
	}
	afterReplay := rangeMoveImportGuardStates(t, n, move)
	for i := range beforeReplay {
		a, b := beforeReplay[i], afterReplay[i]
		if !bytes.Equal(a.transition.root.Image, b.transition.root.Image) || a.transition.root.ImageHash != b.transition.root.ImageHash || !bytes.Equal(a.transition.status, b.transition.status) || !bytes.Equal(a.transition.progress, b.transition.progress) || !bytes.Equal(a.transition.capture, b.transition.capture) || a.owner != b.owner || b.transition.decisionFound || len(b.cdc) != 0 {
			t.Fatal("native earlier-page retry changed graph/cursor/capture/owner/COMMIT or emitted CDC", i)
		}
	}
	if after := rangeMoveAdmissionReservation(t, n.hosts[1][0]); !reflect.DeepEqual(after, reservation) {
		t.Fatal("native earlier-page retry consumed or replaced original completion credits")
	}
	t.Run("cancelled-native-next-page-delivery", func(t *testing.T) {
		before := rangeMoveImportGuardStates(t, n, move)
		reservation := rangeMoveAdmissionReservation(t, n.hosts[1][0])
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		targetGate.stopped, targetGate.cancelOnStop = false, cancel
		defer func() { targetGate.cancelOnStop = nil }()
		var cancelled rangeMoveResult
		var err error
		for range 128 {
			cancelled, err = coordinator.Resume(ctx, pending.resume)
			if !errors.Is(err, errRangeMovePending) || !errors.Is(err, errLimit) && !errors.Is(err, graphstate.ErrResourceLimit) && !errors.Is(err, graphstore.ErrResourceLimit) {
				break
			}
		}
		if !targetGate.stopped || !errors.Is(err, context.Canceled) || !errors.Is(err, errRangeMovePending) || cancelled.committed || cancelled.resume != pending.resume {
			t.Fatal("mid-import caller cancellation lost native pending identity", cancelled, err)
		}
		if after := rangeMoveImportGuardStates(t, n, move); !reflect.DeepEqual(after, before) {
			t.Fatal("cancelled next-page delivery changed native cursor/root/owners/CDC or leaked frozen owners")
		}
		if after := rangeMoveAdmissionReservation(t, n.hosts[1][0]); !reflect.DeepEqual(after, reservation) {
			t.Fatal("cancelled native delivery consumed future completion credits")
		}
	})
	// Fetch a genuine native replay page through the frozen cookie path. It is
	// valid data, but target graph transfer has not completed and cannot admit it.
	sourceState := rangeMoveFrozenGuardStateAt(t, n.hosts[0][0], move)
	output, err = graphstate.NewOutputBudget(defaultMaterializerLimits().outputBytes)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := decodeRangeMoveProgress(sourceState.progress, output)
	if err != nil || !manifest.transferComplete() {
		t.Fatal("actual source manifest", err)
	}
	frozen := rangeMoveFrozenGuardOpen(t, coordinator, move)
	replayPage, err := frozen.moveFrozenPage(t.Context(), manifest.graphPages+1)
	err = errors.Join(err, frozen.Close())
	if err != nil || len(replayPage) < 2 || replayPage[0] != 2 {
		t.Fatal("genuine frozen replay page", err)
	}
	for _, tc := range []struct {
		name string
		want error
		edit func(*testing.T, *rangeMoveCommand)
	}{
		{"empty-import-page", errInvalid, func(_ *testing.T, c *rangeMoveCommand) { c.data = nil }},
		{"unknown-import-page-kind", errInvalid, func(_ *testing.T, c *rangeMoveCommand) { c.data = []byte{255} }},
		{"corrupt-graph-page-codec", graphstore.ErrCorrupt, func(_ *testing.T, c *rangeMoveCommand) {
			c.data = bytes.Clone(c.data)
			c.data[len(c.data)-1] ^= 1
		}},
		{"valid-page-with-foreign-before-cursor", graphstore.ErrInvalid, func(t *testing.T, c *rangeMoveCommand) {
			page, err := graphstore.DecodeRangeMovePage(c.data[1:], output)
			if err != nil {
				t.Fatal(err)
			}
			page.Before.Binding[0] ^= 1
			page.Next.Binding = page.Before.Binding
			wire, err := graphstore.EncodeRangeMovePage(page, output)
			if err != nil {
				t.Fatal(err)
			}
			c.data = append([]byte{1}, wire...)
		}},
		{"genuine-replay-before-graph-completion", errInvalid, func(_ *testing.T, c *rangeMoveCommand) { c.data = replayPage }},
		{"premature-seal-before-complete-stream", errInvalid, func(_ *testing.T, c *rangeMoveCommand) {
			c.phase, c.sequence, c.data = rangeMoveSeal, 0, nil
		}},
		{"changed-original-immutable-intent", errInvalid, func(t *testing.T, c *rangeMoveCommand) {
			changed := intent
			changed.pageRows++
			c.intent, err = encodeRangeMoveIntent(changed)
			if err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			command := pageCommand
			tc.edit(t, &command)
			rangeMoveImportGuardRefusal(t, n, 1, command, tc.want, move)
		})
	}
	targetGate.mode, targetGate.stopped = 4, false
	pending = rangeMoveImportGuardResumeToPause(t, coordinator, pending.resume, targetGate)
	replayCommand := rangeMoveImportGuardCounterpart(t, n.hosts[1][0], targetGate.pending)
	if len(replayCommand.data) < 2 || replayCommand.data[0] != 2 {
		t.Fatal("native pause did not follow complete graph transfer before first replay page")
	}
	t.Run("corrupt-replay-page-codec", func(t *testing.T) {
		command := replayCommand
		command.data = bytes.Clone(command.data)
		command.data[len(command.data)-1] ^= 1
		rangeMoveImportGuardRefusal(t, n, 1, command, errCorrupt, move)
	})
	t.Run("valid-replay-with-skipped-native-cursor", func(t *testing.T) {
		command := replayCommand
		output, err := graphstate.NewOutputBudget(defaultMaterializerLimits().outputBytes)
		if err != nil {
			t.Fatal(err)
		}
		page, err := decodeRangeMoveReplayPage(command.data[1:], output)
		if err != nil {
			t.Fatal(err)
		}
		page.before.sequence++
		page.next.sequence++
		wire, err := encodeRangeMoveReplayPage(page, output)
		if err != nil {
			t.Fatal("canonical but wrong genuine cursor", err)
		}
		command.data = append([]byte{2}, wire...)
		rangeMoveImportGuardRefusal(t, n, 1, command, errInvalid, move)
	})
	targetGate.mode = 0
	done := rangeMoveKeeperResume(t, coordinator, pending.resume)
	if !done.committed || done.resume != pending.resume || wires[0].captures+wires[1].captures != captures {
		t.Fatal("healthy original Resume failed or replaced immutable captures after refusals/retry", done)
	}
}
