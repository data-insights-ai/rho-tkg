package graphapply

import (
	"bytes"
	"context"
	"errors"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

const nativeP06TerminalStage = "terminal-replay"
const nativeP06TerminalProfile = "native-p06-terminal-sync-conjunction-v3"

// Every value comes from the already-executed common semantic parent. This is
// not a serialized factory/authority recipe and supplies no reconstructed proof.
type nativeP06PreparedFixture struct {
	f                          *pairedGraphFaultNetwork
	ctx                        context.Context
	recipe                     nativeP06ConjunctionRecipe
	run                        [16]byte
	deadline                   int64
	before                     pairedFaultObservation
	old                        graphCut
	from                       [2]uint64
	original                   allocationProposal
	actor                      uint64
	boundary                   pairedFaultBoundary
	prepare                    allocationProposal
	manifest                   crossManifest
	interrupted                graphProcessAnswer
	firstKilled, firstRejoined pairedFaultProcess
	refused                    p06LocalRecord
	changed                    *p06LocalRecord
	firstReopened              p06Observation
	program                    *nativeProtocolProgram
}

type nativeP06TerminalStepLoss struct {
	Packet        replica.Packet
	PayloadSHA256 [32]byte
	Transport     string
	WorkGrant     uint64
}

type nativeP06TerminalSubmission struct {
	Input, Reply                      []byte
	Transport                         string
	Accepted                          bool // Propose returned, not an applied-phase or durability ACK.
	InputGrantBytes, OutputGrantBytes uint64
	BeforeImage, RecoveredImage       []byte
	StepLoss                          *nativeP06TerminalStepLoss
}

func nativeP06TerminalRecipes() []nativeP06ConjunctionRecipe {
	var rows []nativeP06ConjunctionRecipe
	for _, base := range nativeP06ConjunctionRecipes() {
		for _, slot := range [][2]uint64{{uint64(crossPrepare), 3}, {uint64(crossResolve), 3}, {uint64(crossResolve), 8}} {
			row := base
			row.Phase, row.Partition, row.Stage = crossPhase(slot[0]), slot[1], nativeP06TerminalStage
			rows = append(rows, row)
		}
	}
	return rows
}

func nativeP06TerminalCase(t *testing.T, s nativeP06PreparedFixture) {
	t.Helper()
	f, r := s.f, s.recipe
	if !r.valid() || r.Stage != nativeP06TerminalStage {
		t.Fatal("unsupported terminal occurrence")
	}
	capture := newNativeConjunctionCapture(f, s.actor, s.manifest.tx)
	defer capture.close()
	decisionReply := f.call(f.n.all[s.actor], processRequest{op: processSourceCoordinate, data: s.original.wire})
	result, err := decodeGraphProcessCoordinatorResult(decisionReply)
	if err != nil || result.resume.tx != s.manifest.tx || result.resume.request != s.manifest.request.id || result.decision.phase != crossDecide || result.decision.commit != r.Commit || result.decision.digest != s.manifest.digest {
		t.Fatal("actual first recovery did not settle SAME original", err)
	}
	f.drain()
	capture.close()
	capture.mu.Lock()
	if capture.refused || len(capture.requests) != 5 {
		t.Fatal("terminal case needs five genuine recovery replies")
	}
	phases := capture.requests
	candidates, attempts, work, output, retainedBytes := capture.candidates, capture.attempts, capture.work, capture.output, capture.retained
	capture.requests = nil
	capture.mu.Unlock()
	p06CheckRecoveryPhases(t, phases, s.prepare, s.manifest, result.decision, r.Commit)
	f.trace.OperationMapping = append(f.trace.OperationMapping, lifecycleOperationMap{Logical: "edge-create", Coordinator: r.Coordinator, Request: pairedFaultResultOf(result).Request, Transaction: pairedFaultResultOf(result).Transaction, Round: result.decision.round})
	recovered := p06Observe(f, s.actor, graphReadFresh, graphCut{})
	p06AssertView(t, recovered.View, r.Commit, !r.Commit, false)
	// Both real immutable Resolve originals must be present BEFORE an observed
	// source command is retried through ordinary native Submit. The private
	// source proof/stamp is never deserialized/recreated from its wire bytes.
	nativeP06CheckCensus(t, phases, p06Records(f, s.from, f.indexes()), s.manifest, prepareManifest(t, s.prepare), result.decision, r.Commit)
	resolves, err := nativeConjunctionTerminalResolveCommands(phases, nil, prepareManifest(t, s.prepare))
	if err != nil {
		t.Fatal(err)
	}
	proposal := s.prepare
	if r.Phase == crossResolve {
		proposal = resolves[0]
		if r.Partition == 8 {
			proposal = resolves[1]
		}
	}
	target, successor := uint64(2), uint64(1)
	if r.Partition == 8 {
		target, successor = 4, 6
	}
	if s.program != nil {
		target = f.n.leaders[0]
		if r.Partition == 8 {
			target = f.n.leaders[1]
		}
		successor = nativeP06ProgramAlternate(target)
	}
	beforeImage := f.call(f.n.all[target], processRequest{op: processStateOp}).image
	f.drain()
	f.packetDrain()
	gate, err := newNativeConjunctionBatchGate(s.ctx, f, nativeConjunctionSelector{Actor: s.actor, Partition: r.Partition, Member: target, Phase: r.Phase, Occurrence: 1, Kind: r.Kind, Side: r.Side}, s.run, s.deadline, true)
	if err != nil {
		t.Fatal("actual terminal batch owner", err)
	}
	defer gate.close()
	if err := gate.armObserved(s.ctx, proposal); err != nil {
		t.Fatal("unobserved terminal selector", err)
	}
	// One actual Propose frame plus response decode/binary/JSON ownership are
	// prepaid before encoding or native effects, never allocated by this quota.
	submission := &nativeP06TerminalSubmission{InputGrantBytes: nativeLogicalTransitionBodyBytes + 512, OutputGrantBytes: 8 * processFrameBytes, BeforeImage: beforeImage}
	submission.Input, err = encodeCertifiedProcessProposal(proposal)
	if err != nil || uint64(len(submission.Input)) > submission.InputGrantBytes {
		t.Fatal("bounded actual terminal Propose input", err)
	}
	ctx, cancel := f.operationContext(10 * time.Second)
	reply, transport := f.n.all[target].exchange(ctx, processRequest{op: processPropose, data: submission.Input})
	contextErr := ctx.Err()
	cancel()
	if contextErr != nil {
		t.Fatal("terminal native call exceeded original deadline", contextErr)
	}
	if transport != nil {
		if !nativeBatchFailedStepAllowed(reply, transport) {
			t.Fatal("native refusal/expiry/corruption is not terminal target loss", transport)
		}
		submission.Transport = transport.Error()
	} else {
		if err := processResponseError(reply); err != nil {
			t.Fatal("native terminal Propose returned a refusal", err)
		}
		submission.Accepted = true
		if s.program != nil {
			if err := f.n.all[target].mux.hub.enqueue(target, reply.packets, reply.handles...); err != nil {
				t.Fatal("SAME owned terminal program transport", err)
			}
			f.packetDrain()
		} else {
			group := f.n.group(target)
			group.enqueue(t, reply) // Actual native packets, never fabricated ACKs.
			f.drainGroup(group)     // Install death can occur in a real subsequent Step.
		}
	}
	submission.Reply = p06ProcessWire(t, reply)
	arm, receipt, selected, _, err := gate.evidence()
	if err != nil || !bytes.Equal(selected, proposal.wire) {
		t.Fatal("unmatched/ambiguous/foreign terminal transition", err)
	}
	gate.target.mux.mu.Lock()
	targetLoss := gate.target.mux.err
	gate.target.mux.mu.Unlock()
	if !nativeBatchFailedStepAllowed(processResponse{}, targetLoss) {
		t.Fatal("terminal target lacks actual typed pipe death", targetLoss)
	}
	gate.mu.Lock()
	submission.StepLoss = gate.stepLoss
	gate.stepLoss = nil // Transfer this one funded actual failed-Step observation.
	gate.mu.Unlock()
	if submission.Accepted && submission.StepLoss == nil || !submission.Accepted && submission.StepLoss != nil || len(submission.Transport) > 4096 {
		t.Fatal("Propose loss and subsequent Step loss were conflated")
	}
	nativeP06ProgramLoss(t, s.program)
	failed := nativeBatchReap(t, f, target, receipt)
	gate.close()
	f.elect(target, successor)
	nativeP06ProgramSuccessor(t, s.program, target, successor)
	nativeConjunctionReconnect(f, s.actor)
	replayed := f.call(f.n.all[s.actor], processRequest{op: processSourceCoordinate, data: s.original.wire})
	replayResult, err := decodeGraphProcessCoordinatorResult(replayed)
	if err != nil || replayResult != result {
		t.Fatal("actual original replay changed first decision", err)
	}
	f.drain()
	submission.RecoveredImage = f.call(f.n.all[successor], processRequest{op: processStateOp}).image
	if !bytes.Equal(submission.BeforeImage, submission.RecoveredImage) {
		t.Fatal("terminal retry duplicated a graph effect")
	}
	duplicates := []p06LocalRecord{p06ReplayPhase(f, s.prepare, s.manifest.tx), p06ReplayPhase(f, resolves[0], s.manifest.tx), p06ReplayPhase(f, resolves[1], s.manifest.tx)}
	endpoint := p06LocalProposal(f, f.n.leaders[0], certifiedP08Close, 1, allocationP08Words(2))
	closed := p06SubmitLocal(f, f.n.leaders[0], endpoint, reasonNone)
	f.trace.OperationMapping = append(f.trace.OperationMapping, lifecycleOperationMap{Logical: "node-close", Round: p06LocalRound(t, closed)})
	current := p06Observe(f, s.actor, graphReadFresh, graphCut{})
	p06AssertView(t, current.View, r.Commit, !r.Commit, true)
	retained := p06Observe(f, s.actor, graphReadAt, s.old)
	wantOld := p06Observation{s.before.Cut, s.before.View, s.before.RawObservation}
	if !reflect.DeepEqual(retained, wantOld) {
		t.Fatal("terminal retry substituted current for original old cut")
	}
	var localOld p06Observation
	rejoined := f.replaceBeforeHeal(target, func(_ *graphProcess, _ processResponse) {
		if s.program != nil {
			localOld = nativeP06InitialLocalOld(f, target, s.old)
		} else {
			localOld = p06Observe(f, target, graphReadAt, s.old)
		}
	})
	if !reflect.DeepEqual(localOld, wantOld) {
		t.Fatal("actual terminal replacement lost local old history/index")
	}
	nativeConjunctionReconnect(f, s.actor)
	reopenedReply := f.call(f.n.all[s.actor], processRequest{op: processSourceCoordinate, data: s.original.wire})
	reopenedResult, err := decodeGraphProcessCoordinatorResult(reopenedReply)
	if err != nil || reopenedResult != result {
		t.Fatal("same-directory terminal recovery lost immutable decision", err)
	}
	f.drain()
	through := f.indexes()
	records := p06Records(f, s.from, through)
	nativeP06CheckCensus(t, phases, records, s.manifest, prepareManifest(t, s.prepare), result.decision, r.Commit)
	p06CheckCDC(t, records, s.manifest, r.Commit, s.changed, s.refused, closed)
	reads := gate.readCount()
	gate.mu.Lock()
	gateCandidates, gateWork := gate.candidates, gate.decodeWork
	gate.mu.Unlock()
	a := nativeP06ConjunctionArtifact{Version: 3, Profile: nativeP06TerminalProfile, Case: r.name(), Recipe: r, Run: s.run, DeadlineUnixNanos: s.deadline, Actor: s.actor, FirstVoter: s.firstKilled.Member, Target: target, Successor: successor,
		P06:      p06Artifact{Version: 1, Coordinator: r.Coordinator, Commit: r.Commit, Original: s.original.wire, Manifest: s.prepare.wire, Pending: p06ProcessWire(t, s.interrupted.response), Decision: p06ProcessWire(t, decisionReply), FirstYES: s.boundary, Processes: []pairedFaultProcess{s.firstKilled, s.firstRejoined}, Refused: s.refused, Changed: s.changed, Closed: closed, Duplicates: duplicates, Phases: phases, Before: wantOld, Recovered: recovered, Current: current, Retained: retained, From: s.from, Through: through},
		Attempts: []nativeP06ConjunctionAttempt{{p06ProcessWire(t, decisionReply), "", false}, {p06ProcessWire(t, replayed), "", false}, {p06ProcessWire(t, reopenedReply), "", false}}, Arm: arm, Batch: receipt, SelectedCommand: selected, TargetTransportLoss: targetLoss.Error(), Processes: []pairedFaultProcess{failed, rejoined}, FirstReopened: s.firstReopened, FaultReopened: localOld, ReceiptReads: reads, ReceiptGrantBytes: reads * 65536, GateCandidates: gateCandidates, GateDecodeWork: gateWork, CaptureCandidates: candidates, CaptureAttempts: attempts, CaptureWork: work, CaptureOutput: output, CaptureRetained: retainedBytes, Duplicate: submission}
	a.Program = nativeP06ProgramComplete(t, s.program)
	nativeP06ConjunctionWrite(t, s.ctx, f, a, records)
}

func TestNativePreparedLockP06TerminalReplaysKeepCurrentOriginalAcrossSyncDeath(t *testing.T) {
	for i, recipe := range nativeP06TerminalRecipes() {
		if testing.Short() && i != 0 && i != 22 && i != 29 && i != 47 {
			continue
		}
		t.Run(recipe.name(), func(t *testing.T) { nativeP06ConjunctionCase(t, recipe) })
	}
}

func TestNativeP06TerminalSelectorsCannotImportAuthorityOrInventStepReply(t *testing.T) {
	seen := make(map[string]bool)
	for _, recipe := range nativeP06TerminalRecipes() {
		if !recipe.valid() || seen[recipe.name()] {
			t.Fatal("non-finite terminal replay occurrence")
		}
		seen[recipe.name()] = true
	}
	if len(seen) != 48 || (nativeP06ConjunctionRecipe{Coordinator: 3, Commit: true, Kind: 1, Side: "after", Phase: crossPrepare, Partition: 8, Stage: nativeP06TerminalStage}).valid() {
		t.Fatal("terminal roster invented a fourth original duplicate")
	}
	var gate *nativeConjunctionBatchGate
	if err := gate.armObserved(t.Context(), allocationProposal{}); !errors.Is(err, errInvalid) {
		t.Fatal("nil diagnostic owner", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	gate = &nativeConjunctionBatchGate{ctx: ctx}
	if err := gate.armObserved(t.Context(), allocationProposal{}); !errors.Is(err, context.Canceled) || gate.candidates != 0 {
		t.Fatal("expired owner performed diagnostic/native work", err)
	}
	if nativeBatchFailedStepAllowed(processResponse{code: 1}, io.EOF) || nativeBatchFailedStepAllowed(processResponse{}, errors.Join(io.EOF, errLimit)) || nativeBatchFailedStepAllowed(processResponse{}, context.DeadlineExceeded) {
		t.Fatal("native refusal/expiry was treated as an absent terminal ACK")
	}
}
