package graphapply

import (
	"bytes"
	"context"
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

// Keep actual pre-COMMIT source EOF authority, then allow the real Decide to
// commit. The target Activate is disconnected before delivery. No phase/proof
// bytes are rewritten and native positive-page reads are never used.
type rangeMoveTransitionGate struct {
	graphPeerWireTransport
	n              *allocationTestNetwork
	source         bool
	move           [16]byte
	beforeDecision rangeMoveAuthority
	decideProposal allocationProposal
	stopped        bool
}

func (g *rangeMoveTransitionGate) RoundTrip(ctx context.Context, wire []byte) ([]byte, error) {
	r, err := decodeGraphSourceRequest(wire)
	if err != nil {
		return nil, err
	}
	if r.kind == sourceSubmit {
		p, err := decodeGraphSourceProposal(r.command)
		if err != nil {
			return nil, err
		}
		c, err := decodeRangeMoveCommand(p.wire)
		if err != nil {
			return nil, err
		}
		if g.source && c.phase == rangeMoveDecide && !g.beforeDecision.validSeal() {
			g.decideProposal = p
			g.beforeDecision = rangeMoveAdmissionRead(g.n.t, g.n, 0, g.move)
			if g.beforeDecision.observation.status.phase != rangeMoveDescribe {
				return nil, errCorrupt
			}
		}
		if !g.source && c.phase == rangeMoveActivate && !g.stopped {
			g.stopped = true
			return nil, io.EOF
		}
	}
	return g.graphPeerWireTransport.RoundTrip(ctx, wire)
}

type rangeMoveTransitionSnapshot struct {
	index                uint64
	image, progress, cdc []byte
	status, decision     rangeMoveStatus
	active               [16]byte
	owner                raftlog.ApplicationRecoveryOwner
	reservation          raftlog.ApplicationRecoveryReservation
}

func rangeMoveTransitionStates(t *testing.T, n *allocationTestNetwork, move [16]byte) [6]rangeMoveTransitionSnapshot {
	t.Helper()
	var result [6]rangeMoveTransitionSnapshot
	for group := range 2 {
		for member, h := range n.hosts[group] {
			view, err := h.machine.store.ApplicationView(h.driver.Applied())
			if err != nil {
				t.Fatal(err)
			}
			root, err := view.RootBounded(t.Context(), 172)
			if err != nil {
				t.Fatal(err)
			}
			q := reader{ctx: t.Context(), view: view, ns: h.machine.ns, base: root, limits: h.machine.limits.allocation}
			active, err := readRangeMoveActive(&q)
			if err != nil {
				t.Fatal(err)
			}
			status, present, err := readRangeMoveStatus(&q, move)
			if err != nil || !present {
				t.Fatal("actual transition status", present, err)
			}
			progress, _, err := q.getBounded(rangeMoveProgressKey(q.ns, move), 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			decisionRecord, found, err := crossControl(&q, rangeMoveControlKey(q.ns, move, 'D'), 256)
			if err != nil {
				t.Fatal(err)
			}
			var decision rangeMoveStatus
			if found {
				decision, err = decodeRangeMoveStatus(decisionRecord.Value)
				if err != nil {
					t.Fatal(err)
				}
			}
			if err := view.Close(); err != nil {
				t.Fatal(err)
			}
			cdc, err := h.machine.store.ApplicationRecord(t.Context(), h.driver.Applied(), false, h.machine.store.ApplicationLimits().MaxChangeBytes)
			if err != nil {
				t.Fatal(err)
			}
			result[3*group+member] = rangeMoveTransitionSnapshot{root.Index, root.Image, progress, cdc, status, decision, active, rangeMoveAdmissionOwner(t, h), rangeMoveAdmissionReservation(t, h)}
		}
	}
	return result
}

func rangeMoveTransitionProposal(t *testing.T, n *allocationTestNetwork, intent []byte, phase rangeMovePhase, partition uint64, evidence []rangeMoveAuthority) allocationProposal {
	t.Helper()
	output, err := graphstate.NewOutputBudget(8 << 20)
	if err != nil {
		t.Fatal(err)
	}
	p, err := n.hosts[0][0].prepareRangeMoveCommand(intent, phase, partition, 0, nil, evidence, output)
	if err != nil {
		t.Fatal("actual source-owned negative transition proposal", err)
	}
	return p
}

func rangeMoveTransitionAssertPrivateRefusal(t *testing.T, n *allocationTestNetwork, group int, p allocationProposal, move [16]byte) {
	t.Helper()
	before := rangeMoveTransitionStates(t, n, move)
	h := n.hosts[group][0]
	view, err := h.machine.store.ApplicationView(h.driver.Applied())
	if err != nil {
		t.Fatal(err)
	}
	root, err := view.RootBounded(t.Context(), 172)
	err = errors.Join(err, view.Close())
	if err != nil {
		t.Fatal(err)
	}
	hard, _, err := h.machine.store.InitialState()
	if err != nil {
		t.Fatal(err)
	}
	// This is the real native Stage door BEFORE Submit/Install. An invalid
	// phase must never be committed merely to test whether it poisons Driver.
	// Entry metadata comes from the actual current native root/HardState; it
	// supplies no synthetic completed read, owner, decision or phase authority.
	batch, err := h.machine.Stage(replica.Entry{Index: root.Index + 1, Generation: root.Generation, Term: hard.GetTerm(), Data: p.wire}, h.machine.store.ApplicationBudget())
	if !errors.Is(err, errInvalid) || !reflect.ValueOf(batch).IsZero() {
		t.Fatal("invalid transition returned installable effects instead of zero refusal", err)
	}
	if !reflect.DeepEqual(rangeMoveTransitionStates(t, n, move), before) {
		t.Fatal("private refusal changed original status/progress/root/CDC/admission/recovery credits")
	}
}

// Re-evaluate the exact actual Decide against its genuine retained pre-Decide
// source. The successful result is private and compared to the real installed D;
// neither an invented status nor an uninstalled batch becomes phase authority.
func rangeMoveTransitionDecisionGuards(t *testing.T, n *allocationTestNetwork, intent rangeMoveIntent, gate *rangeMoveTransitionGate, original rangeMoveStatus) {
	t.Helper()
	command, err := decodeRangeMoveCommand(gate.decideProposal.wire)
	if err != nil || command.phase != rangeMoveDecide || command.scope != intent.source || !gate.beforeDecision.validSeal() {
		t.Fatal("missing exact genuine original Decide proposal", err)
	}
	h := n.hosts[0][0]
	view, err := h.machine.store.ApplicationView(gate.beforeDecision.observation.index)
	if err != nil {
		t.Fatal("retained native pre-Decide source", err)
	}
	defer func() {
		if err := view.Close(); err != nil {
			t.Error(err)
		}
	}()
	root, err := view.RootBounded(t.Context(), 172)
	if err != nil || root.Index != gate.beforeDecision.observation.root.Index || root.ImageHash != gate.beforeDecision.observation.root.ImageHash || !bytes.Equal(root.Image, gate.beforeDecision.observation.root.Image) {
		t.Fatal("retained source changed original native pre-Decide coordinate", err)
	}
	current := gate.beforeDecision.observation.status
	if current.phase != rangeMoveDescribe || current.intent != original.intent || current.move != original.move {
		t.Fatal("pre-Decide native status lost original identity")
	}
	readerFor := func(rows, outputBytes int) *reader {
		output, err := graphstate.NewOutputBudget(outputBytes)
		if err != nil {
			t.Fatal(err)
		}
		limits := h.machine.limits.allocation
		limits.readRows = rows
		return &reader{ctx: t.Context(), view: view, ns: h.machine.ns, base: root, limits: limits, arena: output, bytes: cap(root.Image)}
	}
	before := rangeMoveTransitionStates(t, n, original.move)
	funded := readerFor(512, 8<<20)
	batch, err := h.machine.stageRangeMoveDecision(funded, command, intent, current, original, h.machine.store.ApplicationBudget())
	if err != nil || len(batch.Changes) != 0 || len(batch.ControlPuts) != 1 || !bytes.Equal(batch.ControlPuts[0].Key, rangeMoveControlKey(h.machine.ns, original.move, 'D')) {
		t.Fatal("funded native pre-Decide counterpart did not reproduce original private decision", err)
	}
	decoded, err := decodeRangeMoveStatus(batch.Outcome)
	if err != nil || decoded != original || !bytes.Equal(batch.ControlPuts[0].Value, batch.Outcome) {
		t.Fatal("funded private decision differed from actual immutable D", err)
	}
	if !reflect.DeepEqual(rangeMoveTransitionStates(t, n, original.move), before) {
		t.Fatal("funded private counterpart installed decision/effects")
	}
	assertRefusal := func(name string, q *reader, c rangeMoveCommand, want error, wantRows int) {
		t.Helper()
		batch, err := h.machine.stageRangeMoveDecision(q, c, intent, current, original, h.machine.store.ApplicationBudget())
		if !errors.Is(err, want) || !reflect.ValueOf(batch).IsZero() || len(q.writes) != 0 || wantRows >= 0 && q.rows != wantRows {
			t.Fatal("invalid/unfunded Decide lost cause, returned partial decision or exceeded source grant", name, q.rows, err)
		}
		if !reflect.DeepEqual(rangeMoveTransitionStates(t, n, original.move), before) {
			t.Fatal("Decide refusal changed native owner/credits/root/progress/D/CDC", name)
		}
	}
	badData := command
	badData.data = []byte{1}
	assertRefusal("unexpected-Decide-data", readerFor(512, 8<<20), badData, errInvalid, 0)
	// Only genuine source EOF is supplied. It proves finished export, not a
	// target seal; the source must not decide without that target coverage.
	missingTargetProposal := rangeMoveTransitionProposal(t, n, command.intent, rangeMoveDecide, intent.source.partition, []rangeMoveAuthority{gate.beforeDecision})
	missingTarget, err := decodeRangeMoveCommand(missingTargetProposal.wire)
	if err != nil {
		t.Fatal(err)
	}
	assertRefusal("no-target-seal", readerFor(512, 8<<20), missingTarget, errInvalid, 1)
	// One actual GMP read consumes row1; row2 is installed-floor lookup and
	// row3 is allocation-fence lookup. Budgets remain cumulative through both.
	assertRefusal("GMP-consumed-only-row", readerFor(1, 8<<20), command, errLimit, 1)
	assertRefusal("GMP-and-floor-consumed-rows", readerFor(2, 8<<20), command, errLimit, 2)
	// Admit exactly the real GMP point-copy cost on the original output arena,
	// leaving zero ownership for progress decoding. This tests decoder refusal
	// after a genuine stored read, not a fake corrupt/missing progress value.
	q := readerFor(512, 8<<20)
	key := rangeMoveProgressKey(h.machine.ns, original.move)
	row, found, err := view.Get(t.Context(), key, 1<<20)
	if err != nil || !found || row.Deleted {
		t.Fatal("actual pre-Decide GMP missing", err)
	}
	pointCost := 128 + 4*len(key) + 64 + cap(row.Key) + cap(row.Value)
	if err := q.arena.Reserve(q.arena.Remaining() - pointCost); err != nil {
		t.Fatal(err)
	}
	assertRefusal("GMP-copy-consumed-owned-output", q, command, graphstate.ErrResourceLimit, 1)
}

func TestRangeMoveTransitionsRefuseStaleClosureAndAbortAfterCommitThenOriginalResume(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n, _, intent, wires, transports := rangeMoveCoordinatorFixture(t, 64)
	intentWire, err := encodeRangeMoveIntent(intent)
	if err != nil {
		t.Fatal(err)
	}
	move := rangeMoveID(n.hosts[0][0].machine.ns, intent.id)
	sourceGate := &rangeMoveTransitionGate{graphPeerWireTransport: transports[3], n: n, source: true, move: move}
	targetGate := &rangeMoveTransitionGate{graphPeerWireTransport: transports[8], n: n, move: move}
	transports[3], transports[8] = sourceGate, targetGate
	coordinator, err := newRangeMoveCoordinator(n.hosts[0][0], transports)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := runRangeMoveToCompletion(t, coordinator, intent)
	if !targetGate.stopped || !errors.Is(err, errRangeMovePending) || !errors.Is(err, io.EOF) || pending.committed || !sourceGate.beforeDecision.validSeal() {
		t.Fatal("actual COMMIT/target Seal seam not reached", pending, err)
	}
	source := rangeMoveAdmissionRead(t, n, 0, move)
	target := rangeMoveAdmissionRead(t, n, 1, move)
	if source.observation.status.phase != rangeMoveDecide || source.observation.decision.phase != rangeMoveDecide || source.observation.decision.result != rangeMoveApplied || target.observation.status.phase != rangeMoveSeal {
		t.Fatal("fixture lacks genuine immutable COMMIT and sealed target")
	}
	originalDecision := source.observation.decision
	rangeMoveTransitionDecisionGuards(t, n, intent, sourceGate, originalDecision)
	captures := wires[0].captures + wires[1].captures
	// A real source EOF proof remains data about completed export, never COMMIT
	// authority. Treating phase>=Describe as decision would activate too early.
	staleActivate := rangeMoveTransitionProposal(t, n, intentWire, rangeMoveActivate, 8, []rangeMoveAuthority{sourceGate.beforeDecision})
	rangeMoveTransitionAssertPrivateRefusal(t, n, 1, staleActivate, move)
	// Durable COMMIT is irrevocable; refusal must preserve the original D and
	// protected completion owner, rather than switching this identity to ABORT.
	abort := rangeMoveTransitionProposal(t, n, intentWire, rangeMoveAbort, 3, nil)
	rangeMoveTransitionAssertPrivateRefusal(t, n, 0, abort, move)
	// Cancellation of an inverse-read lookup uses an actual present progress key
	// and a live native view. It cannot return copied original bytes or do work.
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	q := rangeMoveReplayNativeReader(t, n.hosts[1][0], 8<<20)
	q.ctx = cancelled
	rows, workBytes, owned := q.rows, q.bytes, q.arena.Used()
	if value, found, err := rangeMoveUndoOriginal(q, rangeMoveProgressKey(q.ns, move)); value != nil || found || !errors.Is(err, context.Canceled) || q.rows != rows || q.bytes != workBytes || q.arena.Used() != owned {
		t.Fatal("canceled genuine inverse lookup returned bytes or consumed native work", err)
	}
	if err := q.view.Close(); err != nil {
		t.Fatal(err)
	}
	// A valid COMMIT-backed activation also preserves the caller's exhausted
	// owned-output cause. It must not turn an operational refusal into corrupt
	// progress, private writes, release of completion ownership, or ABORT.
	validActivate := rangeMoveTransitionProposal(t, n, intentWire, rangeMoveActivate, 8, []rangeMoveAuthority{source})
	command, err := decodeRangeMoveCommand(validActivate.wire)
	if err != nil {
		t.Fatal(err)
	}
	beforeBudget := rangeMoveTransitionStates(t, n, move)
	q = rangeMoveReplayNativeReader(t, n.hosts[1][0], 8<<20)
	if err := q.arena.Reserve(q.arena.Remaining()); err != nil {
		t.Fatal(err)
	}
	batch, err := n.hosts[1][0].machine.stageRangeMoveActivation(q, command, intent, target.observation.status, target.observation.status, n.hosts[1][0].machine.store.ApplicationBudget())
	if !errors.Is(err, graphstate.ErrResourceLimit) || !reflect.ValueOf(batch).IsZero() || len(q.writes) != 0 {
		t.Fatal("unfunded valid activation lost resource cause or emitted effects", err)
	}
	if err := q.view.Close(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(rangeMoveTransitionStates(t, n, move), beforeBudget) {
		t.Fatal("unfunded activation changed original state/owners/credits")
	}
	// Advance only the valid target Activate through actual native quorum APPLY.
	activated := rangeMoveAdmissionSubmit(t, n, 1, intentWire, rangeMoveActivate, 0, nil, []rangeMoveAuthority{source})
	if activated.phase != rangeMoveActivate || activated.round != originalDecision.round {
		t.Fatal("genuine target activation did not apply", activated)
	}
	target = rangeMoveAdmissionRead(t, n, 1, move)
	source = rangeMoveAdmissionRead(t, n, 0, move)
	if source.observation.status.phase != rangeMoveDecide || target.observation.status.phase != rangeMoveActivate {
		t.Fatal("premature Finish fixture did not retain source activation gap")
	}
	// Both statuses are real and carry the same COMMIT round/digest. Source has
	// not activated: accepting its earlier Decide as Finish coverage releases
	// target completion/admission too early. The precise phase check matters.
	finish := rangeMoveTransitionProposal(t, n, intentWire, rangeMoveFinish, 8, []rangeMoveAuthority{source, target})
	rangeMoveTransitionAssertPrivateRefusal(t, n, 1, finish, move)
	done := rangeMoveKeeperResume(t, coordinator, pending.resume)
	if done.resume != pending.resume || done.decision != originalDecision || wires[0].captures+wires[1].captures != captures {
		t.Fatal("refusal changed immutable outcome or forced recapture", done)
	}
	for group := range 2 {
		actual := rangeMoveAdmissionRead(t, n, group, move)
		if actual.observation.status.phase != rangeMoveFinish || actual.observation.decision != originalDecision || !bytes.Equal(actual.observation.intent, intentWire) {
			t.Fatal("original Resume did not complete exact COMMIT", group)
		}
		for _, h := range n.hosts[group] {
			if owner := rangeMoveAdmissionOwner(t, h); owner != (raftlog.ApplicationRecoveryOwner{}) {
				t.Fatal("original completion retained recovery owner", owner)
			}
		}
	}
}
