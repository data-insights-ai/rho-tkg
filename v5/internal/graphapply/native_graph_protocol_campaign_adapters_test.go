package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// P01's authority source and coordinator remain leaders1/4 and actor5. The
// independently selected native voter3/6 can die without replacing that live
// authority. Leader-loss variants are separate causal programs, not implied by
// this first native-storage conjunction.
type nativeProtocolScopeRecipe struct {
	Mode      graphReadMode
	Phase     crossPhase
	Partition uint64
	Kind      raftlog.StableStorageKind
	Side      string
}

func (r nativeProtocolScopeRecipe) valid() bool {
	return (r.Mode == graphReadFresh || r.Mode == graphReadAfter) &&
		(r.Phase == crossRegister && r.Partition == 8 || r.Phase == crossPrepare && (r.Partition == 3 || r.Partition == 8) || r.Phase == crossDecide && r.Partition == 8 || r.Phase == crossResolve && (r.Partition == 3 || r.Partition == 8)) &&
		(r.Kind == raftlog.StableStorageReady || r.Kind == raftlog.StableStorageApplication) && (r.Side == "before" || r.Side == "after")
}

func (r nativeProtocolScopeRecipe) name() string {
	return fmt.Sprintf("p01-mode%d-phase%d-p%d-k%d-%s", r.Mode, r.Phase, r.Partition, r.Kind, r.Side)
}

func nativeProtocolScopeInventory() []nativeProtocolScopeRecipe {
	var rows []nativeProtocolScopeRecipe
	for _, mode := range []graphReadMode{graphReadFresh, graphReadAfter} {
		for _, phase := range []struct {
			phase crossPhase
			part  uint64
		}{{crossRegister, 8}, {crossPrepare, 3}, {crossPrepare, 8}, {crossDecide, 8}, {crossResolve, 3}, {crossResolve, 8}} {
			for _, kind := range []raftlog.StableStorageKind{raftlog.StableStorageReady, raftlog.StableStorageApplication} {
				for _, side := range []string{"before", "after"} {
					rows = append(rows, nativeProtocolScopeRecipe{mode, phase.phase, phase.part, kind, side})
				}
			}
		}
	}
	return rows
}

type nativeProtocolScopeEvidence struct {
	Recipe            nativeProtocolScopeRecipe
	Run               [16]byte
	DeadlineUnixNanos int64
	nativeProtocolBatchEvidence
	P01 nativeTransitionScopeArtifact
}

type nativeProtocolBatchEvidence struct {
	Actor, Target                   uint64
	Arm, Batch                      nativeBatchReceipt
	SelectedCommand                 []byte
	TargetTransportLoss             string
	ReceiptReads, ReceiptGrantBytes uint64
	GateCandidates, GateDecodeWork  uint64
	TargetProcesses                 []pairedFaultProcess
	TargetReopened                  p06Observation
	Program                         *nativeProtocolProgramEvidence `json:",omitempty"`
}

func nativeProtocolFinishBatch(t *testing.T, f *pairedGraphFaultNetwork, gate *nativeConjunctionBatchGate, old graphCut, packetOnly ...bool) nativeProtocolBatchEvidence {
	t.Helper()
	arm, receipt, selected, _, err := gate.evidence()
	if err != nil {
		t.Fatal("native protocol occurrence unmatched/ambiguous/exhausted", err)
	}
	return nativeProtocolFinishObservedBatch(t, f, gate, old, arm, receipt, selected, packetOnly...)
}

func nativeProtocolFinishObservedBatch(t *testing.T, f *pairedGraphFaultNetwork, gate *nativeConjunctionBatchGate, old graphCut, arm, receipt nativeBatchReceipt, selected []byte, packetOnly ...bool) nativeProtocolBatchEvidence {
	t.Helper()
	f.requireActive()
	gate.mu.Lock()
	owned := gate.evidenceTaken && !gate.closed && !gate.refused && gate.ctx.Err() == nil && arm == gate.arm && len(selected) > 0 && len(selected) <= nativeLogicalTransitionBodyBytes && sha256.Sum256(selected) == arm.Control.CommandSHA256
	gate.mu.Unlock()
	if !owned || !nativeBatchReceiptValid(receipt, arm) {
		t.Fatal("foreign/unobserved native evidence cannot own process cleanup")
	}
	gate.target.mux.mu.Lock()
	loss := gate.target.mux.err
	gate.target.mux.mu.Unlock()
	if !nativeBatchFailedStepAllowed(processResponse{}, loss) {
		t.Fatal("selected protocol voter lacks actual typed transport loss", loss)
	}
	program := nativeProtocolProgramFinish(t, f)
	killed := nativeBatchReap(t, f, gate.selector.Member, receipt)
	reads := gate.readCount()
	gate.mu.Lock()
	candidates, work := gate.candidates, gate.decodeWork
	gate.mu.Unlock()
	gate.close()
	var reopened p06Observation
	joined := f.replaceBeforeHeal(gate.selector.Member, func(_ *graphProcess, _ processResponse) {
		reopened = nativeP06InitialLocalOld(f, gate.selector.Member, old)
	}, packetOnly...)
	return nativeProtocolBatchEvidence{Actor: gate.selector.Actor, Target: gate.selector.Member, Arm: arm, Batch: receipt, SelectedCommand: selected, TargetTransportLoss: loss.Error(), ReceiptReads: reads, ReceiptGrantBytes: reads * 65536, GateCandidates: candidates, GateDecodeWork: work, TargetProcesses: []pairedFaultProcess{killed, joined}, TargetReopened: reopened, Program: program}
}

// The selected serving voter can be the second loss in its partition. This
// helper does not pretend that its private caller can make progress without
// quorum. It restores the earlier actual volume and elects it using the one
// surviving voter, then restores the selected volume before any new Run.
func nativeProtocolFinishRecoveryBatch(t *testing.T, f *pairedGraphFaultNetwork, gate *nativeConjunctionBatchGate, old graphCut, restoreLeader uint64) (nativeProtocolBatchEvidence, pairedFaultProcess) {
	t.Helper()
	f.requireActive()
	arm, receipt, selected, _, err := gate.evidence()
	gate.mu.Lock()
	owned := gate.evidenceTaken && !gate.closed && !gate.refused && gate.ctx.Err() == nil && arm == gate.arm
	gate.mu.Unlock()
	if err != nil || !owned || len(selected) == 0 || len(selected) > nativeLogicalTransitionBodyBytes || !nativeBatchReceiptValid(receipt, arm) || sha256.Sum256(selected) != arm.Control.CommandSHA256 {
		t.Fatal("P03 selected recovery did not reach its exact native boundary", err)
	}
	gate.target.mux.mu.Lock()
	loss := gate.target.mux.err
	gate.target.mux.mu.Unlock()
	if !nativeBatchFailedStepAllowed(processResponse{}, loss) {
		t.Fatal("P03 selected native death lacks typed pipe loss", loss)
	}
	if restoreLeader != 0 {
		f.mu.Lock()
		dead := f.down[restoreLeader]
		callerDead := f.down[gate.selector.Actor]
		losses := 0
		for member := uint64(1); member <= 6; member++ {
			if (member <= 3) == (restoreLeader <= 3) && f.down[member] {
				losses++
			}
		}
		f.mu.Unlock()
		if !dead || callerDead || losses != 2 || restoreLeader == gate.selector.Member || (restoreLeader <= 3) != (gate.selector.Member <= 3) || gate.selector.Actor == gate.selector.Member || gate.selector.Actor == restoreLeader {
			t.Fatal("quorum reconstruction requires two distinct losses and a live original caller")
		}
	}
	program := nativeProtocolProgramFinish(t, f)
	failed := nativeBatchReap(t, f, gate.selector.Member, receipt)
	reads := gate.readCount()
	gate.mu.Lock()
	candidates, work := gate.candidates, gate.decodeWork
	gate.mu.Unlock()
	gate.close()
	var firstRejoined pairedFaultProcess
	if restoreLeader != 0 {
		firstRejoined = f.replaceBeforeHeal(restoreLeader, func(_ *graphProcess, _ processResponse) {
			nativeP06InitialLocalOld(f, restoreLeader, old)
			f.elect(gate.selector.Member, restoreLeader)
		})
	}
	var restoredOld p06Observation
	joined := f.replaceBeforeHeal(gate.selector.Member, func(_ *graphProcess, _ processResponse) {
		restoredOld = nativeP06InitialLocalOld(f, gate.selector.Member, old)
	})
	evidence := nativeProtocolBatchEvidence{Actor: gate.selector.Actor, Target: gate.selector.Member, Arm: arm, Batch: receipt, SelectedCommand: selected, TargetTransportLoss: loss.Error(), ReceiptReads: reads, ReceiptGrantBytes: reads * 65536, GateCandidates: candidates, GateDecodeWork: work, TargetProcesses: []pairedFaultProcess{failed, joined}, TargetReopened: restoredOld, Program: program}
	return evidence, firstRejoined
}

func nativeProtocolScopeWrite(t *testing.T, ctx context.Context, f *pairedGraphFaultNetwork, evidence nativeProtocolScopeEvidence, records []lifecycleRawRecord) string {
	t.Helper()
	artifact := struct {
		Version       uint8
		Profile, Case string
		Evidence      nativeProtocolScopeEvidence
	}{1, nativeProtocolCampaignProfile, evidence.Recipe.name(), evidence}
	if evidence.Program != nil {
		artifact.Version, artifact.Profile = 2, nativeProtocolProgramProfile
	}
	path := nativeTransitionWriteRows(t, filepath.Join(f.n.dir, artifact.Case+".jsonl"), artifact, records, evidence.P01.Through)
	if evidence.Program != nil {
		nativeProtocolCheck(t, ctx, path, true)
	} else {
		nativeProtocolCheck(t, ctx, path)
	}
	return path
}

func nativeProtocolCheck(t *testing.T, ctx context.Context, path string, program ...bool) {
	t.Helper()
	if len(program) > 1 || len(program) == 1 && !program[0] {
		t.Fatal("explicit fresh program checker selection")
	}
	script := "native_protocol_campaign_check.py"
	if len(program) == 1 {
		script = "native_protocol_campaign_program_check.py"
	}
	check := exec.CommandContext(ctx, "python3", "-B", filepath.Join("..", "..", "..", "tools", "v5-substrate", script), path)
	var diagnostics processStderr
	check.Stdout, check.Stderr = &diagnostics, &diagnostics
	if err := check.Run(); err != nil {
		body, _ := diagnostics.state()
		t.Fatal("independent protocol/storage/original/serial checker", err, body)
	}
	if _, overflow := diagnostics.state(); overflow {
		t.Fatal("independent protocol diagnostic output exceeded allowance")
	}
}

func TestNativeProtocolP01ScopeAUsesActualParticipantStorageBatches(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for i, recipe := range nativeProtocolScopeInventory() {
		if testing.Short() && i != 0 && i != 11 && i != 36 && i != 47 {
			continue
		}
		t.Run(recipe.name(), func(t *testing.T) { nativeTransitionScopeCase(t, recipe.Mode, recipe) })
	}
}

type nativeProtocolFenceRecipe struct {
	Schedule  string
	Phase     crossPhase
	Partition uint64
	Kind      raftlog.StableStorageKind
	Side      string
}

func (r nativeProtocolFenceRecipe) valid() bool {
	if r.Schedule == "persisted-fence-before-application" {
		phase := r.Phase == 0 && r.Partition == 3 || r.Phase == crossRegister && r.Partition == 3 || r.Phase == crossPrepare && (r.Partition == 3 || r.Partition == 8) || r.Phase == crossDecide && r.Partition == 3 || r.Phase == crossResolve && (r.Partition == 3 || r.Partition == 8)
		return phase && (r.Kind == raftlog.StableStorageReady || r.Kind == raftlog.StableStorageApplication) && (r.Side == "before" || r.Side == "after")
	}
	return (r.Schedule == "delayed-A-prepare" || r.Schedule == "A-before-B-after") &&
		(nativeProtocolScopeRecipe{Mode: graphReadFresh, Phase: r.Phase, Partition: r.Partition, Kind: r.Kind, Side: r.Side}).valid()
}

func (r nativeProtocolFenceRecipe) name() string {
	return fmt.Sprintf("p02-%s-phase%d-p%d-k%d-%s", r.Schedule, r.Phase, r.Partition, r.Kind, r.Side)
}

func nativeProtocolFenceLiveInventory() []nativeProtocolFenceRecipe {
	var rows []nativeProtocolFenceRecipe
	for _, schedule := range []string{"delayed-A-prepare", "A-before-B-after"} {
		for _, phase := range nativeProtocolScopeInventory() {
			if phase.Mode == graphReadFresh {
				rows = append(rows, nativeProtocolFenceRecipe{schedule, phase.Phase, phase.Partition, phase.Kind, phase.Side})
			}
		}
	}
	return rows
}

func nativeProtocolFenceInventory() []nativeProtocolFenceRecipe {
	rows := nativeProtocolFenceLiveInventory()
	for _, phase := range []struct {
		phase crossPhase
		part  uint64
	}{{0, 3}, {crossRegister, 3}, {crossPrepare, 3}, {crossPrepare, 8}, {crossDecide, 3}, {crossResolve, 3}, {crossResolve, 8}} {
		for _, kind := range []raftlog.StableStorageKind{raftlog.StableStorageReady, raftlog.StableStorageApplication} {
			for _, side := range []string{"before", "after"} {
				rows = append(rows, nativeProtocolFenceRecipe{"persisted-fence-before-application", phase.phase, phase.part, kind, side})
			}
		}
	}
	return rows
}

type nativeProtocolPlanningCharge struct{ Output, Retained uint64 }

type nativeProtocolPersistedFence struct {
	Proposal          []byte
	Submission        nativeP06TerminalSubmission
	Run               [16]byte
	DeadlineUnixNanos int64
	Kind              raftlog.StableStorageKind
	Side              string
	nativeProtocolBatchEvidence
	Successor          uint64
	Original           *p06LocalRecord // Only an actual committed survivor read.
	Applied, Duplicate p06LocalRecord
	SuccessorEvidence  *nativeProtocolProgramElection `json:",omitempty"`
}

// This is P02's genuine fence persistence/application leader-loss predecessor,
// not a source reply or an imported Stage marker. The original factory proposal
// stays on this parent until ordinary Submit after election. A Ready receipt is
// not quorum commitment: only the later actual successor application establishes
// the floor; an install receipt additionally joins its actual committed record.
func nativeProtocolPersistFence(t *testing.T, f *pairedGraphFaultNetwork, ctx context.Context, run [16]byte, deadline int64, old graphCut, from [2]uint64, proposal allocationProposal, kind raftlog.StableStorageKind, side string) nativeProtocolPersistedFence {
	t.Helper()
	if len(old.certificates) != 2 || old.certificates[0].scope.partition != 3 || old.certificates[1].scope.partition != 8 {
		t.Fatal("P02 native fence predecessor needs actual complete old cut")
	}
	command, err := decodeGraphCutCommand(proposal.wire)
	leader, successor := uint64(1), uint64(2)
	f.mu.Lock()
	program := f.protocolProgram
	f.mu.Unlock()
	if program != nil {
		leader = f.n.leaders[0]
		if leader == 2 {
			successor = 1
		}
	}
	if err != nil || command.phase != graphCutFence || command.scope != old.certificates[0].scope || proposal.target != command.scope || f.n.leaders[0] != leader || leader != 1 && leader != 2 {
		t.Fatal("actual P02 earlier factory/configured fence", err)
	}
	beforeImage := f.call(f.n.all[leader], processRequest{op: processStateOp}).image
	gate, err := newNativeConjunctionBatchGate(ctx, f, nativeConjunctionSelector{Actor: leader, Partition: 3, Member: leader, Kind: kind, Side: side, Fence: true, FenceScope: command.scope}, run, deadline, true)
	if err != nil {
		t.Fatal(err)
	}
	defer gate.close()
	if err := gate.armObserved(ctx, proposal); err != nil {
		t.Fatal("actual local native fence was not armed", err)
	}
	submission := nativeP06TerminalSubmission{InputGrantBytes: nativeLogicalTransitionBodyBytes + 512, OutputGrantBytes: 8 * processFrameBytes, BeforeImage: beforeImage}
	submission.Input, err = encodeCertifiedProcessProposal(proposal)
	if err != nil || uint64(len(submission.Input)) > submission.InputGrantBytes {
		t.Fatal("finite genuine fence Propose input", err)
	}
	callCtx, cancel := f.operationContext(10 * time.Second)
	response, transport := f.n.all[leader].exchange(callCtx, processRequest{op: processPropose, data: submission.Input})
	contextErr := callCtx.Err()
	cancel()
	if contextErr != nil {
		t.Fatal("P02 fence native call exceeded original deadline", contextErr)
	}
	if transport != nil {
		if !nativeBatchFailedStepAllowed(response, transport) {
			t.Fatal("P02 fence refused/expired/corrupt native response", transport)
		}
		submission.Transport = transport.Error()
	} else {
		if err := processResponseError(response); err != nil {
			t.Fatal("P02 fence native Propose did not accept", err)
		}
		submission.Accepted = true // Ordinary admission; never an apply/Sync ACK.
		if program == nil {
			f.n.group(leader).enqueue(t, response)
			f.drainGroup(f.n.group(leader))
		} else {
			if err := f.n.all[leader].mux.hub.enqueue(leader, response.packets, response.handles...); err != nil {
				t.Fatal("actual native fence messages through program hub", err)
			}
			f.packetDrain()
		}
	}
	submission.Reply = p06ProcessWire(t, response)
	arm, receipt, selected, _, err := gate.evidence()
	if err != nil || !bytes.Equal(selected, proposal.wire) {
		t.Fatal("P02 actual native fence identity/batch missing", err)
	}
	gate.target.mux.mu.Lock()
	loss := gate.target.mux.err
	gate.target.mux.mu.Unlock()
	if !nativeBatchFailedStepAllowed(processResponse{}, loss) {
		t.Fatal("P02 actual fence leader did not die at selected native transition", loss)
	}
	gate.mu.Lock()
	submission.StepLoss, gate.stepLoss = gate.stepLoss, nil
	candidates, work := gate.candidates, gate.decodeWork
	gate.mu.Unlock()
	if submission.Accepted && submission.StepLoss == nil || !submission.Accepted && submission.StepLoss != nil || len(submission.Transport) > 4096 {
		t.Fatal("P02 fence Propose loss and later Step death were conflated")
	}
	var messageProgram *nativeProtocolProgramEvidence
	if program != nil {
		program.mu.Lock()
		selectedProgram := program.armed
		program.mu.Unlock()
		if selectedProgram {
			messageProgram = nativeProtocolProgramFinish(t, f)
		}
	}
	killed := nativeBatchReap(t, f, leader, receipt)
	reads := gate.readCount()
	gate.close()
	f.elect(leader, successor)
	var successorEvidence *nativeProtocolProgramElection
	if program != nil {
		observed, err := program.successor(leader, successor)
		if err != nil {
			t.Fatal("actual causal native successor before source reassociation", err)
		}
		successorEvidence = &observed
	}
	// Application-install selection is reached only after committed-entry
	// validation. Read its actual survivor output; never infer it from Ready or
	// manufacture the missing leader reply. Before/after install use SAME entry.
	var original *p06LocalRecord
	if kind == raftlog.StableStorageApplication {
		index := receipt.Events[0].Index
		stored := f.call(f.n.all[successor], processRequest{op: processEntryRecord, index: index})
		outcome, err := decodeGraphCutResult(stored.outcome)
		if err != nil || outcome.phase != graphCutFence || outcome.status != graphCutApplied || outcome.round != command.round || outcome.index != index || len(stored.changes) != 0 {
			t.Fatal("P02 successor lacks SAME committed native fence", err)
		}
		actual := p06LocalRecord{Partition: 3, Index: index, Proposal: proposal.wire, Outcome: stored.outcome, CDC: stored.changes}
		original = &actual
	}
	// The exact genuine proposal is revalidated by the ordinary successor Host.
	// Control replies retain actual local indexes; no artificial original GSO is
	// substituted when the earlier Ready entry had never committed.
	applied := p02SubmitWhilePaused(f, successor, proposal)
	installed, err := decodeGraphCutResult(applied.Outcome)
	if err != nil || installed.phase != graphCutFence || installed.status != graphCutApplied || installed.round != command.round || installed.index != applied.Index || len(applied.CDC) != 0 {
		t.Fatal("P02 successor did not genuinely establish retained fence", err)
	}
	f.records(from)
	var reopened p06Observation
	rejoined := f.replaceBeforeHeal(leader, func(_ *graphProcess, _ processResponse) {
		reopened = nativeP06InitialLocalOld(f, leader, old)
	})
	duplicate := p02SubmitWhilePaused(f, successor, proposal)
	replayed, err := decodeGraphCutResult(duplicate.Outcome)
	if err != nil || replayed.phase != graphCutFence || replayed.status != graphCutApplied || replayed.round != command.round || replayed.index != duplicate.Index || duplicate.Index <= applied.Index || len(duplicate.CDC) != 0 {
		t.Fatal("P02 same fence replay changed genuine floor/output", err)
	}
	submission.RecoveredImage = f.call(f.n.all[successor], processRequest{op: processStateOp}).image
	if !bytes.Equal(beforeImage, submission.RecoveredImage) {
		t.Fatal("native fence/election/replay invented a graph effect")
	}
	return nativeProtocolPersistedFence{Proposal: proposal.wire, Submission: submission, Run: run, DeadlineUnixNanos: deadline, Kind: kind, Side: side,
		nativeProtocolBatchEvidence: nativeProtocolBatchEvidence{Actor: leader, Target: leader, Arm: arm, Batch: receipt, SelectedCommand: selected, TargetTransportLoss: loss.Error(), ReceiptReads: reads, ReceiptGrantBytes: reads * 65536, GateCandidates: candidates, GateDecodeWork: work, TargetProcesses: []pairedFaultProcess{killed, rejoined}, TargetReopened: reopened, Program: messageProgram},
		Successor:                   successor, Original: original, Applied: applied, Duplicate: duplicate, SuccessorEvidence: successorEvidence}
}

type nativeProtocolFenceEvidence struct {
	Recipe            nativeProtocolFenceRecipe
	Run               [16]byte
	DeadlineUnixNanos int64
	nativeProtocolBatchEvidence
	P02                 p02FenceArtifact
	From, Through       [2]uint64
	Capture             nativeP06InitialCharge
	Planning            nativeProtocolPlanningCharge
	Hold                nativeProtocolHoldCharge
	Boundary            pairedFaultBoundary
	Calls               [][]byte
	Cohort              []pairedFaultProcess
	Reopened, PostRetry p06Observation
	FenceTransition     *nativeProtocolPersistedFence `json:",omitzero"`
}

// These first two P02 causal orderings keep the SAME initial coordinator call:
// either its actual A Submit is held before effects, or its completed A vote
// read is held after YES. The native target is a separate real voter. The third
// persistence/application leader-loss ordering has its own native fence input;
// it must not be represented by this earlier/later source-reply distinction.
func nativeProtocolFenceCase(t *testing.T, recipe nativeProtocolFenceRecipe, programs ...nativeProtocolProgramSpec) {
	t.Helper()
	if !recipe.valid() {
		t.Fatal("unsupported P02 live causal ordering")
	}
	if recipe.Schedule == "persisted-fence-before-application" {
		nativeProtocolFenceStageCase(t, recipe, programs...)
		return
	}
	artifactBudget := nativeProtocolArtifactAllowance(t)
	run, deadline := nativeP06ConjunctionIdentity(t)
	t.Setenv("RHO_GRAPH_STABLE_STORAGE_PROCESS", "1")
	ctx, cancel := context.WithDeadline(t.Context(), time.Unix(0, deadline))
	defer cancel()
	f := newPairedGraphFaultNetworkWithParent(t, ctx)
	program := nativeProtocolProgramStart(t, ctx, f, programs...)
	base := f.indexes()
	before := nativeFaultSeed(t, f)
	old := p06Observe(f, 2, graphReadFresh, graphCut{})
	oldCut, err := pairedFaultCut(old.Cut)
	if err != nil || len(oldCut.certificates) != 2 || oldCut.certificates[0].round != oldCut.certificates[1].round || oldCut.certificates[0].round > ^uint64(0)-100 {
		t.Fatal("actual P02 complete old cut/fence arithmetic", err)
	}
	bound := oldCut.certificates[0].round + 100
	from, original := f.indexes(), f.proposal(3)
	business, err := decodePartitionGraphCommand(original.wire, defaultMaterializerLimits())
	if err != nil {
		t.Fatal(err)
	}
	ns := business.request.ns
	ns.partition = 8
	tx := crossTransactionID(ns, business.request.id)
	capture := newNativeConjunctionCapture(f, 5, tx, true)
	defer capture.close()
	selector := nativeProtocolReplySelector{Actor: 5, Partition: 3, Phase: crossPrepare, Transaction: tx, Peer: graphPeerIdentity{scope: oldCut.certificates[0].scope, voter: f.n.leaders[0]}}
	var hold *nativeProtocolReplyHold
	if recipe.Schedule == "delayed-A-prepare" {
		hold, err = newNativeProtocolBeforeHold(ctx, f, selector)
	} else {
		hold, err = newNativeProtocolReplyHold(ctx, f, selector)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer hold.close()
	target := uint64(3)
	if recipe.Partition == 8 {
		target = 6
	}
	if program != nil {
		target, err = program.target(recipe.Partition, 5, 2)
		if err != nil {
			t.Fatal("actual P02 target preserves original caller and old reader", err)
		}
	}
	gate, err := newNativeConjunctionBatchGate(ctx, f, nativeConjunctionSelector{Actor: 5, Partition: recipe.Partition, Member: target, Phase: recipe.Phase, Occurrence: 1, Kind: recipe.Kind, Side: recipe.Side}, run, deadline)
	if err != nil {
		t.Fatal(err)
	}
	defer gate.close()
	call, stop := f.beginRun(5, original)
	defer stop()
	boundary := f.awaitBoundary(hold.entered)
	fences := []p06LocalRecord{p02Fence(f, f.n.leaders[0], bound)}
	if recipe.Schedule == "A-before-B-after" {
		fences = append(fences, p02Fence(f, f.n.leaders[1], bound))
	}
	hold.mu.Lock()
	if !hold.selected || hold.closed || hold.ctx.Err() != nil {
		hold.mu.Unlock()
		t.Fatal("P02 original caller/source reply did not remain live")
	}
	holdCharge := nativeProtocolHoldCharge{hold.candidates, hold.work, hold.output, hold.retained}
	hold.mu.Unlock()
	hold.release()
	answer := nativeP06AwaitAnswer(f, call)
	result, resultErr := decodeGraphProcessCoordinatorResult(answer.response)
	if answer.err != nil || resultErr != nil || !result.decision.commit || result.decision.tx != tx || result.decision.ns.partition != 8 || result.decision.round <= bound {
		t.Fatal("P02 actual original did not settle above durable fence", answer.err, resultErr)
	}
	f.drain()
	// LIFO teardown transfers funded captures only after every actual callback
	// has returned. No failed request is padded into the six successful replies.
	arm, receipt, selected, _, err := gate.evidence()
	if err != nil {
		t.Fatal("P02 actual selected native batch absent", err)
	}
	// FinishBatch owns this transfer, so release its one-shot evidence back only
	// through the existing typed values, without replaying or re-reading it.
	native := nativeProtocolFinishObservedBatch(t, f, gate, oldCut, arm, receipt, selected)
	hold.close()
	phases, charge := nativeP06InitialTake(t, capture, 6)
	capture.mu.Lock()
	if capture.refused || len(capture.sources) != 2 || len(capture.sources[0].Reply) == 0 || len(capture.sources[1].Reply) == 0 {
		capture.mu.Unlock()
		t.Fatal("P02 actual two planning sources incomplete")
	}
	sources := capture.sources
	capture.sources = nil
	planning := nativeProtocolPlanningCharge{capture.sourceOutput, capture.sourceRetained}
	capture.mu.Unlock()
	var manifest crossManifest
	var votes [2]crossStatus
	if program == nil {
		manifest, votes = p02Votes(f, phases, original, result.decision)
	} else {
		manifest, votes = nativeProtocolProgramP02Votes(t, f, phases, original, result.decision)
	}
	p02BindSources(t, sources, manifest, nil)
	if manifest.parts[0].floor >= bound || recipe.Schedule == "A-before-B-after" && (votes[0].floor >= bound || votes[1].floor < bound) || recipe.Schedule == "delayed-A-prepare" && votes[0].floor < bound {
		t.Fatal("P02 genuine captured/apply-time floor ordering", votes, bound)
	}
	if recipe.Schedule == "delayed-A-prepare" {
		if !bytes.Equal(boundary.Request, phases[1].Request) || len(boundary.Reply) != 0 {
			t.Fatal("before-Prepare hold was not the exact later actual submission")
		}
	} else {
		read, err := decodeGraphWireResponse(boundary.Reply)
		observed, observationErr := decodeCrossAuthorityBody(read.body)
		if err != nil || observationErr != nil || read.err != nil || observed.status != votes[0] {
			t.Fatal("after-A hold did not retain the actual completed vote read", err, observationErr)
		}
	}
	step := p03Complete(f, 5, "edge-create", original, result, from, before)
	f.assertView(step.After, 3)
	pairedFaultAssertCDC(t, step, 3)
	assertLifecycleCandidates(t, step.RawObservation, 3)
	current, retained := p06Observe(f, 5, graphReadFresh, graphCut{}), p06Observe(f, 5, graphReadAt, oldCut)
	if !reflect.DeepEqual(retained, old) || !reflect.DeepEqual(native.TargetReopened, old) {
		t.Fatal("P02 held/native-replacement old data/index substituted current")
	}
	retries := []pairedFaultRetry{f.replay(5, "edge-create", original, result, base)}
	cohortBase := f.indexes()
	f.records(base)
	cohort := f.stopAll()
	cohort = append(cohort, f.reopenAll()...)
	f.records(cohortBase)
	reopened := p06Observe(f, 5, graphReadAt, oldCut)
	retries = append(retries, f.replay(5, "edge-create", original, result, base))
	postRetry := p06Observe(f, 5, graphReadFresh, graphCut{})
	if !reflect.DeepEqual(reopened, old) || !reflect.DeepEqual(postRetry.View, current.View) {
		t.Fatal("P02 same-directory original replay/old/current changed")
	}
	through := f.indexes()
	records := p06Records(f, base, through)
	evidence := nativeProtocolFenceEvidence{Recipe: recipe, Run: run, DeadlineUnixNanos: deadline, nativeProtocolBatchEvidence: native,
		P02: p02FenceArtifact{Schedule: recipe.Schedule, Trace: f.trace, Phases: phases, SourceCaptures: sources, Fences: fences, Old: old, Current: current, Retained: retained, Retries: retries}, From: base, Through: through,
		Capture: charge, Planning: planning, Hold: holdCharge, Boundary: boundary, Calls: [][]byte{p06ProcessWire(t, answer.response)}, Cohort: cohort, Reopened: reopened, PostRetry: postRetry}
	if artifactBudget.Used() != 16<<20 || artifactBudget.Remaining() != 0 {
		t.Fatal("P02 diagnostic artifact was not prepaid")
	}
	artifact := struct {
		Version       uint8
		Profile, Case string
		Evidence      nativeProtocolFenceEvidence
	}{1, nativeProtocolCampaignProfile, recipe.name(), evidence}
	if native.Program != nil {
		artifact.Version, artifact.Profile = 2, nativeProtocolProgramProfile
	}
	path := nativeTransitionWriteRows(t, filepath.Join(f.n.dir, recipe.name()+".jsonl"), artifact, records, through)
	if native.Program != nil {
		nativeProtocolCheck(t, ctx, path, true)
	} else {
		nativeProtocolCheck(t, ctx, path)
	}
}

func nativeProtocolFenceStageCase(t *testing.T, recipe nativeProtocolFenceRecipe, programs ...nativeProtocolProgramSpec) {
	t.Helper()
	if !recipe.valid() || recipe.Schedule != "persisted-fence-before-application" {
		t.Fatal("actual persistence/application ordering required")
	}
	artifactBudget := nativeProtocolArtifactAllowance(t)
	run, deadline := nativeP06ConjunctionIdentity(t)
	t.Setenv("RHO_GRAPH_STABLE_STORAGE_PROCESS", "1")
	ctx, cancel := context.WithDeadline(t.Context(), time.Unix(0, deadline))
	defer cancel()
	f := newPairedGraphFaultNetworkWithParent(t, ctx)
	program := nativeProtocolProgramStart(t, ctx, f, programs...)
	if program != nil {
		if !nativeProtocolProgramFenceReachable(recipe, program.spec) {
			t.Fatal("local leader Ready stops before native messages are emitted; not a matched message occurrence")
		}
		if err := program.selectCommand(recipe.Phase, recipe.Partition, recipe.Phase == 0); err != nil {
			t.Fatal(err)
		}
	}
	base := f.indexes()
	before := nativeFaultSeed(t, f)
	old := p06Observe(f, 3, graphReadFresh, graphCut{})
	oldCut, err := pairedFaultCut(old.Cut)
	if err != nil || len(oldCut.certificates) != 2 || oldCut.certificates[0].round > ^uint64(0)-100 {
		t.Fatal("actual complete predecessor/fence bound", err)
	}
	bound := oldCut.certificates[0].round + 100
	// Both genuine source factories run before source1 can die. No restarted
	// recipient mints replacement IDs or reseals the retained business request.
	original := f.proposal(3)
	delayed := p06LocalProposal(f, f.n.leaders[0], certifiedP08Close, 1, allocationP08Words(2))
	fence := p06LocalProposal(f, f.n.leaders[0], certifiedPrepareFence, bound, nil)
	kind, side := raftlog.StableStorageApplication, "before"
	if recipe.Phase == 0 {
		kind, side = recipe.Kind, recipe.Side
	}
	persisted := nativeProtocolPersistFence(t, f, ctx, run, deadline, oldCut, base, fence, kind, side)
	if !reflect.DeepEqual(persisted.TargetReopened, old) {
		t.Fatal("actual fence leader replacement lost old graph/index")
	}
	nativeConjunctionReconnect(f, 3)
	business, err := decodePartitionGraphCommand(original.wire, defaultMaterializerLimits())
	if err != nil {
		t.Fatal(err)
	}
	tx := crossTransactionID(business.request.ns, business.request.id)
	from := f.indexes()
	capture := newNativeConjunctionCapture(f, 3, tx, true)
	defer capture.close()
	var gate *nativeConjunctionBatchGate
	if recipe.Phase != 0 {
		target := uint64(1) // Actual restored follower; source2 and actor3 survive.
		if recipe.Partition == 8 {
			target = 6
		}
		if program != nil {
			target, err = program.target(recipe.Partition, 3)
			if err != nil {
				t.Fatal("actual post-fence native target preserves SAME original caller/source", err)
			}
		}
		gate, err = newNativeConjunctionBatchGate(ctx, f, nativeConjunctionSelector{Actor: 3, Partition: recipe.Partition, Member: target, Phase: recipe.Phase, Occurrence: 1, Kind: recipe.Kind, Side: recipe.Side}, run, deadline)
		if err != nil {
			t.Fatal(err)
		}
		defer gate.close()
	}
	answer := f.call(f.n.all[3], processRequest{op: processSourceCoordinate, data: original.wire})
	result, err := decodeGraphProcessCoordinatorResult(answer)
	if err != nil || !result.decision.commit || result.decision.ns.partition != 3 || result.decision.tx != tx || result.decision.round <= bound {
		t.Fatal("actual post-fence source plan/COMMIT failed", err)
	}
	f.drain()
	native := persisted.nativeProtocolBatchEvidence
	if gate != nil {
		native = nativeProtocolFinishBatch(t, f, gate, oldCut)
	}
	phases, charge := nativeP06InitialTake(t, capture, 6)
	capture.mu.Lock()
	if capture.refused || len(capture.sources) != 2 || len(capture.sources[0].Reply) == 0 || len(capture.sources[1].Reply) == 0 {
		capture.mu.Unlock()
		t.Fatal("actual post-fence planning source census incomplete")
	}
	sources, planning := capture.sources, nativeProtocolPlanningCharge{capture.sourceOutput, capture.sourceRetained}
	capture.sources = nil
	capture.mu.Unlock()
	var manifest crossManifest
	var votes [2]crossStatus
	if program == nil {
		manifest, votes = p02Votes(f, phases, original, result.decision)
	} else {
		manifest, votes = nativeProtocolProgramP02Votes(t, f, phases, original, result.decision)
	}
	p02BindSources(t, sources, manifest, nil)
	if votes[0].floor < bound {
		t.Fatal("post-fence actual Prepare sampled stale floor")
	}
	for _, source := range sources {
		if source.Partition == 3 {
			reply, err := decodeGraphWireResponse(source.Reply)
			info, infoErr := decodeGraphSourceInfo(reply.body)
			if err != nil || infoErr != nil || info.root.Index < persisted.Applied.Index || info.observation.source.index < persisted.Applied.Index {
				t.Fatal("actual source capture preceded successor fence application", err, infoErr)
			}
		}
	}
	step := p03Complete(f, 3, "edge-create", original, result, from, before)
	f.assertView(step.After, 3)
	pairedFaultAssertCDC(t, step, 3)
	assertLifecycleCandidates(t, step.RawObservation, 3)
	closed := p06SubmitLocal(f, f.n.leaders[0], delayed, reasonNone)
	round := p06LocalRound(t, closed)
	if round <= bound || round <= result.decision.round {
		t.Fatal("genuine delayed pre-fence local command committed below floor")
	}
	p02CheckLocalCloseCDC(t, closed, round)
	f.trace.OperationMapping = append(f.trace.OperationMapping, lifecycleOperationMap{Logical: "node-close", Round: round})
	current, retained := p06Observe(f, 3, graphReadFresh, graphCut{}), p06Observe(f, 3, graphReadAt, oldCut)
	p06AssertView(t, current.View, true, false, true)
	if !reflect.DeepEqual(retained, old) || !reflect.DeepEqual(native.TargetReopened, old) {
		t.Fatal("actual source/selected replacement substituted current for old")
	}
	retries := []pairedFaultRetry{f.replay(3, "edge-create", original, result, base)}
	cohortBase := f.indexes()
	f.records(base)
	cohort := f.stopAll()
	cohort = append(cohort, f.reopenAll()...)
	f.records(cohortBase)
	reopened := p06Observe(f, 3, graphReadAt, oldCut)
	retries = append(retries, f.replay(3, "edge-create", original, result, base))
	duplicate := p06Submit(f, 1, delayed)
	ns, _ := certifiedProcessNamespace(1)
	firstLocal, localErr := decodePartitionGraphOutcome(closed.Outcome, ns)
	replayed, replayErr := decodePartitionGraphOutcome(duplicate.Outcome, ns)
	want := firstLocal
	want.disposition = requestReplay
	if localErr != nil || replayErr != nil || firstLocal.disposition != applied || replayed != want || duplicate.Index <= closed.Index || len(duplicate.CDC) != 0 {
		t.Fatal("delayed local same-directory replay changed original/CDC", localErr, replayErr)
	}
	postRetry := p06Observe(f, 3, graphReadFresh, graphCut{})
	if !reflect.DeepEqual(reopened, old) || !reflect.DeepEqual(postRetry.View, current.View) {
		t.Fatal("P02 same-directory old/current/original replay changed")
	}
	through := f.indexes()
	records := p06Records(f, base, through)
	evidence := nativeProtocolFenceEvidence{Recipe: recipe, Run: run, DeadlineUnixNanos: deadline, nativeProtocolBatchEvidence: native,
		P02:  p02FenceArtifact{Schedule: recipe.Schedule, Trace: f.trace, Phases: phases, SourceCaptures: sources, Fences: []p06LocalRecord{persisted.Applied, persisted.Duplicate}, Old: old, Current: current, Retained: retained, Retries: retries, DelayedLocal: &closed, DelayedLocalReplay: &duplicate},
		From: base, Through: through, Capture: charge, Planning: planning, Calls: [][]byte{p06ProcessWire(t, answer)}, Cohort: cohort, Reopened: reopened, PostRetry: postRetry, FenceTransition: &persisted}
	if artifactBudget.Used() != 16<<20 || artifactBudget.Remaining() != 0 {
		t.Fatal("P02 diagnostic artifact was not prepaid")
	}
	artifact := struct {
		Version       uint8
		Profile, Case string
		Evidence      nativeProtocolFenceEvidence
	}{1, nativeProtocolCampaignProfile, recipe.name(), evidence}
	if native.Program != nil {
		artifact.Version, artifact.Profile = 2, nativeProtocolProgramProfile
	}
	path := nativeTransitionWriteRows(t, filepath.Join(f.n.dir, recipe.name()+".jsonl"), artifact, records, through)
	if native.Program != nil {
		nativeProtocolCheck(t, ctx, path, true)
	} else {
		nativeProtocolCheck(t, ctx, path)
	}
}

func TestNativeProtocolP02ActualApplyFloorsStraddleHeldPrepareAcrossNativeBatches(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for i, recipe := range nativeProtocolFenceInventory() {
		if testing.Short() && i != 0 && i != 11 && i != 36 && i != 47 && i != 48 && i != 75 {
			continue
		}
		t.Run(recipe.name(), func(t *testing.T) { nativeProtocolFenceCase(t, recipe) })
	}
}

// These are the two reachable later submissions in the FIRST live coRun after
// its A YES leader dies. A-coordinated Decide and both Resolve calls cannot use
// a replacement A endpoint inside that invocation; they belong to authentic
// post-return recovery, already covered by the separately versioned profiles.
type nativeProtocolLiveP06Recipe struct {
	Coordinator uint64
	Commit      bool
	Phase       crossPhase
	Kind        raftlog.StableStorageKind
	Side        string
}

func (r nativeProtocolLiveP06Recipe) valid() bool {
	base := nativeP06ConjunctionRecipe{Coordinator: r.Coordinator, Commit: r.Commit, Kind: r.Kind, Side: r.Side}
	return base.valid() && (r.Phase == crossPrepare || r.Phase == crossDecide && r.Coordinator == 8)
}

func (r nativeProtocolLiveP06Recipe) name() string {
	return fmt.Sprintf("p06-co%d-commit%t-first-live-B%d-k%d-%s", r.Coordinator, r.Commit, r.Phase, r.Kind, r.Side)
}

func nativeProtocolLiveP06Inventory() []nativeProtocolLiveP06Recipe {
	var rows []nativeProtocolLiveP06Recipe
	for _, base := range nativeP06ConjunctionRecipes() {
		rows = append(rows, nativeProtocolLiveP06Recipe{base.Coordinator, base.Commit, crossPrepare, base.Kind, base.Side})
		if base.Coordinator == 8 {
			rows = append(rows, nativeProtocolLiveP06Recipe{base.Coordinator, base.Commit, crossDecide, base.Kind, base.Side})
		}
	}
	return rows
}

type nativeProtocolHoldCharge struct {
	Candidates, Work, Output, Retained uint64
}

type nativeProtocolLiveP06Evidence struct {
	Recipe                          nativeProtocolLiveP06Recipe
	Run                             [16]byte
	DeadlineUnixNanos               int64
	Actor, Target, Successor        uint64
	OriginalCall                    nativeP06ConjunctionAttempt
	InitialPhases                   []p06Phase
	InitialCapture                  nativeP06InitialCharge
	Hold                            nativeProtocolHoldCharge
	VoteRead                        pairedFaultBoundary
	Arm, Batch                      nativeBatchReceipt
	SelectedCommand                 []byte
	TargetTransportLoss             string
	ReceiptReads, ReceiptGrantBytes uint64
	GateCandidates, GateDecodeWork  uint64
	TargetProcesses                 []pairedFaultProcess
	TargetReopened                  p06Observation
	Completion                      nativeP06PreparedCompletion
	Program                         *nativeProtocolProgramEvidence `json:",omitempty"`
	ProgramCohort                   []pairedFaultProcess           `json:",omitempty"`
}

// This adapter does not recover/restart the coordinator before the selected
// first-B call. Its first A source reply remains held on the original service
// callback, then is released into that SAME caller context and endpoint set.
func nativeProtocolLiveP06Case(t *testing.T, recipe nativeProtocolLiveP06Recipe, programs ...nativeProtocolProgramSpec) nativeProtocolLiveP06Evidence {
	t.Helper()
	if !recipe.valid() {
		t.Fatal("infeasible first-live-run P06 transition")
	}
	artifactBudget := nativeProtocolArtifactAllowance(t)
	run, deadline := nativeP06ConjunctionIdentity(t)
	t.Setenv("RHO_GRAPH_STABLE_STORAGE_PROCESS", "1")
	ctx, cancel := context.WithDeadline(t.Context(), time.Unix(0, deadline))
	defer cancel()
	f := newPairedGraphFaultNetworkWithParent(t, ctx)
	program := nativeProtocolProgramStart(t, ctx, f, programs...)
	firstVoter, recoveryVoter, target, successor := uint64(1), uint64(2), uint64(4), uint64(6)
	if program != nil {
		if recipe.Kind == raftlog.StableStorageReady && program.spec.Message != "literal" {
			t.Fatal("selected submitting B leader dies before emitting this Ready's native messages")
		}
		firstVoter, target = f.n.leaders[0], f.n.leaders[1]
		if firstVoter == 2 {
			recoveryVoter = 1
		}
		if target == 6 {
			successor = 4
		}
	}
	before := nativeFaultSeed(t, f)
	old, err := pairedFaultCut(before.Cut)
	if err != nil {
		t.Fatal(err)
	}
	wantOld := p06Observation{before.Cut, before.View, before.RawObservation}
	from, original := f.indexes(), f.proposal(3)
	actor := uint64(3)
	if recipe.Coordinator == 8 {
		actor = 5
	}
	business, err := decodePartitionGraphCommand(original.wire, defaultMaterializerLimits())
	if err != nil {
		t.Fatal(err)
	}
	ns := business.request.ns
	ns.partition = recipe.Coordinator
	tx := crossTransactionID(ns, business.request.id)
	capture := newNativeConjunctionCapture(f, actor, tx)
	defer capture.close()
	hold, err := newNativeProtocolReplyHold(ctx, f, nativeProtocolReplySelector{Actor: actor, Partition: 3, Phase: crossPrepare, Transaction: tx, Peer: graphPeerIdentity{scope: old.certificates[0].scope, voter: firstVoter}})
	if err != nil {
		t.Fatal(err)
	}
	defer hold.close()
	gate, err := newNativeConjunctionBatchGate(ctx, f, nativeConjunctionSelector{Actor: actor, Partition: 8, Member: target, Phase: recipe.Phase, Occurrence: 1, Kind: recipe.Kind, Side: recipe.Side}, run, deadline)
	if err != nil {
		t.Fatal(err)
	}
	defer gate.close()
	firstCall, stopFirst := f.beginRun(actor, original)
	defer stopFirst()
	voteRead := f.awaitBoundary(hold.entered)
	capture.mu.Lock()
	if len(capture.requests) != 2 || len(capture.requests[1].Reply) == 0 {
		capture.mu.Unlock()
		t.Fatal("held vote-authority read lacks earlier actual Submit YES reply")
	}
	boundary := pairedFaultBoundary{Request: capture.requests[1].Request, Reply: capture.requests[1].Reply}
	capture.mu.Unlock()
	prepare, vote, manifest := p06PrepareBoundary(t, boundary, recipe.Coordinator, firstVoter)
	submitRequest, requestErr := decodeGraphSourceRequest(boundary.Request)
	submitReply, submitErr := decodeGraphWireResponse(boundary.Reply)
	voteRequest, voteRequestErr := decodeGraphSourceRequest(voteRead.Request)
	voteReply, replyErr := decodeGraphWireResponse(voteRead.Reply)
	observedVote, observedErr := decodeCrossAuthorityBody(voteReply.body)
	if requestErr != nil || submitErr != nil || voteRequestErr != nil || replyErr != nil || observedErr != nil || voteReply.err != nil || voteRequest.kind != sourceCrossRead || voteRequest.tx != tx || voteRequest.phase != crossPrepare || voteRequest.sequence <= submitRequest.sequence || voteReply.peer != submitReply.peer || voteReply.issuer != submitReply.issuer || observedVote.status != vote || !bytes.Equal(observedVote.manifest, prepareManifest(t, prepare)) {
		t.Fatal("actual later vote-authority read differs from the same peer/stream Submit YES", requestErr, submitErr, voteRequestErr, replyErr, observedErr)
	}
	p06BindBusiness(t, original, manifest)
	f.packetDrain()
	for member := uint64(1); member <= 3; member++ {
		stored := f.call(f.n.all[member], processRequest{op: processEntryRecord, index: vote.index})
		if !bytes.Equal(stored.outcome, p06ReplyBody(t, boundary.Reply)) || len(stored.changes) != 0 {
			t.Fatal("first live YES lacks all actual prepared voter originals", member)
		}
	}
	firstKilled := f.kill(firstVoter, "prepared-participant-leader")
	f.elect(firstVoter, recoveryVoter)
	if program != nil {
		if _, err := program.successor(firstVoter, recoveryVoter); err != nil {
			t.Fatal("actual A successor while SAME completed service reply remains held", err)
		}
	}
	refused := nativeProtocolSubmitHeld(f, recoveryVoter, p06LocalProposal(f, recoveryVoter, certifiedP08Close, 1, allocationP08Words(2)), reasonPreparedConflict)
	var changed *p06LocalRecord
	if !recipe.Commit {
		actual := nativeProtocolSubmitHeld(f, target, p06LocalProposal(f, target, certifiedP08Close, 17, allocationP08Words(18)), reasonNone)
		changed = &actual
	}
	hold.mu.Lock()
	holdCharge := nativeProtocolHoldCharge{hold.candidates, hold.work, hold.output, hold.retained}
	if !hold.selected || hold.closed || hold.ctx.Err() != nil {
		hold.mu.Unlock()
		t.Fatal("first coordinator context did not survive its real held reply")
	}
	hold.mu.Unlock()
	hold.release() // No stopFirst, new Run, reconnect or source capture here.
	unknown := nativeP06AwaitAnswer(f, firstCall)
	pending, pendingErr := decodeGraphProcessCoordinatorResult(unknown.response)
	if !nativeP06UnknownTransport(unknown.err) || !errors.Is(pendingErr, errCrossProcessPending) || pending.resume.tx != tx || pending.resume.request != business.request.id || pending.decision != (crossStatus{}) {
		t.Fatal("first live invocation lost actual pending original", unknown.err, pendingErr)
	}
	arm, receipt, selected, _, err := gate.evidence()
	if err != nil {
		t.Fatal("first live B call did not reach selected native transition", err)
	}
	command, _, err := decodeCrossCommand(selected, defaultMaterializerLimits())
	if err != nil || command.phase != recipe.Phase || command.ns.partition != 8 || !bytes.Equal(command.manifest, prepareManifest(t, prepare)) {
		t.Fatal("selected first live B command changed original", err)
	}
	gate.target.mux.mu.Lock()
	targetLoss := gate.target.mux.err
	gate.target.mux.mu.Unlock()
	if !nativeBatchFailedStepAllowed(processResponse{}, targetLoss) {
		t.Fatal("selected B native child lacks exact transport death", targetLoss)
	}
	messageProgram := nativeProtocolProgramFinish(t, f)
	failed := nativeBatchReap(t, f, target, receipt)
	reads := gate.readCount()
	gate.mu.Lock()
	gateCandidates, gateWork := gate.candidates, gate.decodeWork
	gate.mu.Unlock()
	gate.close()
	hold.close() // LIFO before the sole actual source capture is retired.
	count := 3
	if recipe.Phase == crossDecide {
		count = 4
	}
	initialRows, initialCharge := nativeP06InitialTake(t, capture, count)
	if !bytes.Equal(initialRows[1].Request, boundary.Request) || !bytes.Equal(initialRows[1].Reply, boundary.Reply) || !bytes.Equal(p06SourceProposal(t, initialRows[count-1].Request).wire, selected) || len(initialRows[count-1].Reply) != 0 {
		t.Fatal("first live source census borrowed a reply or another invocation")
	}
	f.drain()
	f.elect(target, successor) // Actor5 remains the SAME private coordinator owner.
	var targetOld p06Observation
	rejoined := f.replaceBeforeHeal(target, func(_ *graphProcess, _ processResponse) { targetOld = nativeP06InitialLocalOld(f, target, old) })
	if !reflect.DeepEqual(targetOld, wantOld) {
		t.Fatal("actual selected B replacement substituted current history")
	}
	nativeConjunctionReconnect(f, actor)
	base := nativeP06ConjunctionRecipe{Coordinator: recipe.Coordinator, Commit: recipe.Commit, Kind: recipe.Kind, Side: recipe.Side}
	completion := nativeP06CompletePrepared(t, nativeP06PreparedContinuation{f: f, Recipe: base, Actor: actor, FirstVoter: firstVoter, RecoveryVoter: recoveryVoter, Original: original, Before: wantOld, Old: old, From: from, Pending: unknown.response, FirstKilled: firstKilled,
		Evidence: nativeP06PreparedProvenance{Boundary: boundary, Prepare: prepare, Manifest: manifest, Vote: vote}, BeforeRecoveryRefused: &refused, BeforeRecoveryChanged: changed, AllowKnownDecision: true})
	var programCohort []pairedFaultProcess
	if messageProgram != nil {
		// Actual terminal process receipts authenticate surviving native role
		// observers too. No new Run/source/authority is acquired for this proof.
		programCohort = f.stopAll()
	}
	evidence := nativeProtocolLiveP06Evidence{Recipe: recipe, Run: run, DeadlineUnixNanos: deadline, Actor: actor, Target: target, Successor: successor, OriginalCall: nativeP06ConjunctionAttempt{Reply: p06ProcessWire(t, unknown.response), Transport: unknown.err.Error(), Pending: true}, InitialPhases: initialRows, InitialCapture: initialCharge, Hold: holdCharge, VoteRead: voteRead, Arm: arm, Batch: receipt, SelectedCommand: selected, TargetTransportLoss: targetLoss.Error(), ReceiptReads: reads, ReceiptGrantBytes: reads * 65536, GateCandidates: gateCandidates, GateDecodeWork: gateWork, TargetProcesses: []pairedFaultProcess{failed, rejoined}, TargetReopened: targetOld, Completion: completion, Program: messageProgram, ProgramCohort: programCohort}
	nativeProtocolLiveP06Write(t, ctx, f, evidence, artifactBudget)
	return evidence
}

// The independent checker receives the complete actual prefix exactly once.
// The held source reply is diagnostic bytes; it never becomes a native input.
func nativeProtocolLiveP06Write(t *testing.T, ctx context.Context, f *pairedGraphFaultNetwork, evidence nativeProtocolLiveP06Evidence, budget *graphstate.OutputBudget) {
	t.Helper()
	if budget == nil || budget.Used() != 16<<20 || budget.Remaining() != 0 {
		t.Fatal("fresh artifact was not funded before native effects")
	}
	records := evidence.Completion.P06.Records
	evidence.Completion.P06.Records = nil
	artifact := struct {
		Version       uint8
		Profile, Case string
		Evidence      nativeProtocolLiveP06Evidence
	}{1, nativeProtocolCampaignProfile, evidence.Recipe.name(), evidence}
	if evidence.Program != nil {
		artifact.Version, artifact.Profile = 2, nativeProtocolProgramProfile
	}
	path := nativeTransitionWriteRows(t, filepath.Join(f.n.dir, artifact.Case+".jsonl"), artifact, records, evidence.Completion.P06.Through)
	if evidence.Program != nil {
		nativeProtocolCheck(t, ctx, path, true)
	} else {
		nativeProtocolCheck(t, ctx, path)
	}
}

func TestNativeProtocolFirstLiveP06UsesActualAuthorityReplyAndOriginalContext(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for i, recipe := range nativeProtocolLiveP06Inventory() {
		if testing.Short() && i != 0 && i != 7 && i != 16 && i != 23 {
			continue
		}
		t.Run(recipe.name(), func(t *testing.T) { nativeProtocolLiveP06Case(t, recipe) })
	}
}

// P03 has its own durable-D predecessor. Early faults are restored before the
// three-voter D census; recovery Resolve faults occur after the leader loss.
// In the latter same-partition case the second loss removes quorum: no recovery
// success is claimed until an actual old volume has been reopened and elected.
type nativeProtocolCoordinatorRecipe struct {
	Coordinator uint64
	Phase       crossPhase
	Partition   uint64
	Kind        raftlog.StableStorageKind
	Side        string
}

func (r nativeProtocolCoordinatorRecipe) valid() bool {
	return (r.Coordinator == 3 || r.Coordinator == 8) &&
		(r.Phase == crossRegister && r.Partition == r.Coordinator || r.Phase == crossPrepare && (r.Partition == 3 || r.Partition == 8) || r.Phase == crossDecide && r.Partition == r.Coordinator || r.Phase == crossResolve && (r.Partition == 3 || r.Partition == 8)) &&
		(r.Kind == raftlog.StableStorageReady || r.Kind == raftlog.StableStorageApplication) && (r.Side == "before" || r.Side == "after")
}

func (r nativeProtocolCoordinatorRecipe) name() string {
	return fmt.Sprintf("p03-co%d-phase%d-p%d-k%d-%s", r.Coordinator, r.Phase, r.Partition, r.Kind, r.Side)
}

func (r nativeProtocolCoordinatorRecipe) recoveryActor(program bool, caller, recoverer, target uint64) uint64 {
	if program && r.Phase != crossResolve && target == recoverer {
		return caller
	}
	return recoverer
}

func nativeProtocolCoordinatorInventory() []nativeProtocolCoordinatorRecipe {
	var rows []nativeProtocolCoordinatorRecipe
	for _, co := range []uint64{3, 8} {
		for _, p := range []struct {
			phase crossPhase
			part  uint64
		}{{crossRegister, co}, {crossPrepare, 3}, {crossPrepare, 8}, {crossDecide, co}, {crossResolve, 3}, {crossResolve, 8}} {
			for _, kind := range []raftlog.StableStorageKind{raftlog.StableStorageReady, raftlog.StableStorageApplication} {
				for _, side := range []string{"before", "after"} {
					rows = append(rows, nativeProtocolCoordinatorRecipe{co, p.phase, p.part, kind, side})
				}
			}
		}
	}
	return rows
}

type nativeProtocolCoordinatorEvidence struct {
	Recipe            nativeProtocolCoordinatorRecipe
	Run               [16]byte
	DeadlineUnixNanos int64
	nativeProtocolBatchEvidence
	P03             *pairedFaultTrace // Hash-bound legacy-sized sidecar, attached only by the offline reader.
	TraceName       string
	TraceSHA256     [32]byte
	TraceBytes      uint64
	Records         []lifecycleRawRecord // Streamed actual prefix; never an inline summary.
	From, Through   [2]uint64
	Boundary        pairedFaultBoundary
	OriginalPending []byte
	OutagePending   []byte // Nil unless the selected native death removed quorum.
	InitialPhases   []p06Phase
	InitialCapture  nativeP06InitialCharge
	RecoveryPhases  []p06Phase
	RecoveryCapture nativeP06InitialCharge
	RecoveryCalls   []nativeP06ConjunctionAttempt
	RecoveryActor   uint64
	BeforeRecovery  pairedFaultProcess
}

func nativeProtocolRetireCoordinatorCall(t *testing.T, f *pairedGraphFaultNetwork, call <-chan graphProcessAnswer, cancel context.CancelFunc, original allocationProposal, decision crossStatus, scope allocationScope) []byte {
	t.Helper()
	cancel()
	answer := nativeP06AwaitAnswer(f, call)
	pending, err := decodeGraphProcessCoordinatorResult(answer.response)
	command, commandErr := decodePartitionGraphCommand(original.wire, defaultMaterializerLimits())
	if commandErr != nil || !errors.Is(answer.err, context.Canceled) || !errors.Is(err, errCrossProcessPending) || answer.response.code == 0 || answer.response.sequence == 0 || pending.decision != (crossStatus{}) || pending.resume.scope != scope || pending.resume.request != command.request.id || pending.resume.tx != decision.tx || pending.resume.business != p03BusinessDigest(original, decision.ns, t) {
		t.Fatal("P03 actual original cancellation lost its durable identity", commandErr, answer.err, err)
	}
	return p06ProcessWire(t, answer.response)
}

func nativeProtocolTakeCapture(t *testing.T, c *nativeConjunctionCapture) ([]p06Phase, nativeP06InitialCharge) {
	t.Helper()
	c.mu.Lock()
	count := len(c.requests)
	c.mu.Unlock()
	if count == 0 || count > 16 {
		t.Fatal("bounded actual protocol submission census", count)
	}
	return nativeP06InitialTake(t, c, count)
}

func nativeProtocolCoordinatorCase(t *testing.T, recipe nativeProtocolCoordinatorRecipe, programs ...nativeProtocolProgramSpec) {
	t.Helper()
	if !recipe.valid() {
		t.Fatal("inapplicable P03 native occurrence")
	}
	budget := nativeProtocolArtifactAllowance(t)
	traceBudget, err := graphstate.NewOutputBudget(8 << 20)
	if err != nil {
		t.Fatal(err)
	}
	if err := traceBudget.Reserve(8 << 20); err != nil {
		t.Fatal(err)
	}
	run, deadline := nativeP06ConjunctionIdentity(t)
	t.Setenv("RHO_GRAPH_STABLE_STORAGE_PROCESS", "1")
	ctx, cancel := context.WithDeadline(t.Context(), time.Unix(0, deadline))
	defer cancel()
	f := newPairedGraphFaultNetworkWithParent(t, ctx)
	f.trace.Scope = "P03 actual durable-D leader loss plus selected native Ready/install; finite fresh causal program, not all-D acceptance"
	program := nativeProtocolProgramStart(t, ctx, f, programs...)
	if program != nil && recipe.Phase == crossResolve && recipe.Partition == recipe.Coordinator && recipe.Kind == raftlog.StableStorageReady && program.spec.Message != "literal" {
		t.Fatal("selected serving recovery leader dies before this Ready emits native packets")
	}
	before := f.observe(4, graphReadFresh, graphCut{}, false)
	f.assertView(before.View, 0)
	f.trace.InitialCut = before.Cut
	base := f.indexes()
	names := []string{"node-a", "node-b", "edge-create", "precommit-update", "postcommit-update", "zone-loss-update"}
	proposals := make([]allocationProposal, 6)
	results := make([]crossProcessResult, 6)
	for stage := 1; stage <= 4; stage++ {
		proposals[stage-1] = f.proposal(stage)
		from := f.indexes()
		actor := uint64(4)
		if stage == 1 {
			actor = 1
		}
		results[stage-1] = f.run(actor, proposals[stage-1])
		step := f.complete(names[stage-1], proposals[stage-1], results[stage-1], from, before)
		f.assertView(step.After, stage)
		pairedFaultAssertCDC(t, step, stage)
		assertLifecycleCandidates(t, step.RawObservation, min(stage, 3))
		before = pairedFaultObserver(step)
	}
	old, err := pairedFaultCut(before.Cut)
	if err != nil {
		t.Fatal(err)
	}
	proposals[4], proposals[5] = f.proposal(5), f.proposal(6)
	fifthFrom := f.indexes()
	leader, caller, recoverer := uint64(1), uint64(2), uint64(3)
	if recipe.Coordinator == 8 {
		leader, caller, recoverer = 4, 5, 6
	}
	if program != nil && program.spec.Layout == "rotated" {
		leader, caller, recoverer = 2, 3, 1
		if recipe.Coordinator == 8 {
			leader, caller, recoverer = 6, 5, 4
		}
	}
	command, err := decodePartitionGraphCommand(proposals[4].wire, defaultMaterializerLimits())
	if err != nil {
		t.Fatal(err)
	}
	ns := command.request.ns
	ns.partition = recipe.Coordinator
	tx := crossTransactionID(ns, command.request.id)
	capture := newNativeConjunctionCapture(f, caller, tx)
	defer capture.close()
	entered, release := p03GateCommittedReply(f, caller, recipe.Coordinator, true)
	defer release()
	var gate *nativeConjunctionBatchGate
	target := uint64(3)
	if recipe.Partition == 8 {
		target = 6
	}
	if program != nil {
		target, err = program.target(recipe.Partition, caller)
		if err != nil {
			t.Fatal("actual P03 selected follower preserves held SAME caller", err)
		}
	}
	if recipe.Phase != crossResolve {
		gate, err = newNativeConjunctionBatchGate(ctx, f, nativeConjunctionSelector{Actor: caller, Partition: recipe.Partition, Member: target, Phase: recipe.Phase, Occurrence: 1, Kind: recipe.Kind, Side: recipe.Side}, run, deadline)
		if err != nil {
			t.Fatal(err)
		}
		defer gate.close()
	}
	call, stop := f.beginRun(caller, proposals[4])
	defer stop()
	boundary := f.awaitBoundary(entered)
	// Decode only to authenticate the actual pending response. Complete D
	// authority is independently inspected after any early follower rejoins.
	q, err := decodeGraphSourceRequest(boundary.Request)
	if err != nil {
		t.Fatal(err)
	}
	proposal, err := decodeGraphSourceProposal(q.command)
	reply, replyErr := decodeGraphWireResponse(boundary.Reply)
	decision, decisionErr := decodeCrossStatus(reply.body)
	if err != nil || replyErr != nil || decisionErr != nil || reply.err != nil || decision.phase != crossDecide || !decision.commit || decision.ns.partition != recipe.Coordinator || reply.peer.voter != leader || decision.tx != tx {
		t.Fatal("P03 missing genuine held durable Decide", err, replyErr, decisionErr)
	}
	var native nativeProtocolBatchEvidence
	if gate != nil {
		if err := gate.awaitLoss(); err != nil {
			t.Fatal("P03 selected follower lacks authenticated native receipt/loss", err)
		}
		// Keep the genuine Decide reply held while native packets finish. A
		// full relay drain would wait for that same deliberately held reply.
		f.packetDrain()
		native = nativeProtocolFinishBatch(t, f, gate, old, true)
	}
	actualD, scope, committed := p03CommittedDecision(f, boundary, proposals[4], leader, recipe.Coordinator)
	if program != nil {
		// The original helper's diagnostic path admits only leaders1/4. This
		// is the actual same child's configured durable directory, not a wire
		// member/instance or authority replacement.
		committed.Volume = filepath.Join(f.n.dir, fmt.Sprint(leader))
	}
	if actualD != decision || scope != proposal.target {
		t.Fatal("P03 changed captured durable D")
	}
	killed := f.kill(leader, "replica")
	pending := nativeProtocolRetireCoordinatorCall(t, f, call, stop, proposals[4], decision, proposal.target)
	release()
	f.drain()
	initial, initialCharge := nativeProtocolTakeCapture(t, capture)
	capture.close() // No live original Run remains before endpoint reassociation.
	f.elect(leader, caller)
	if program != nil && recipe.Phase == crossResolve {
		if _, err := program.successor(leader, caller); err != nil {
			t.Fatal("actual durable-D successor before selected recovery occurrence", err)
		}
	}
	// The replaced target proved its own old cut before heal. Its new client
	// cannot reuse the remote service's spent pre-crash origin stream. Keep
	// recovery on the surviving original caller/Host when that target was the
	// planned recoverer; unchanged peer streams and the idle original remain.
	recoverer = recipe.recoveryActor(program != nil, caller, recoverer, target)
	nativeConjunctionReconnect(f, recoverer)
	// These reads really occur while the first leader is down, before any
	// second-voter failure. They do not claim unavailable-quorum Fresh success.
	p03OldRead(f, recoverer, 3, "while-coordinator-down")
	p03OldRead(f, recoverer, 4, "while-coordinator-down")
	recovery := newNativeConjunctionCapture(f, recoverer, tx)
	defer recovery.close()
	var calls []nativeP06ConjunctionAttempt
	var outage []byte
	var rejoined pairedFaultProcess
	if recipe.Phase == crossResolve {
		if recipe.Partition == recipe.Coordinator {
			target = caller // Actual serving leader; recoverer survives both losses.
		}
		gate, err = newNativeConjunctionBatchGate(ctx, f, nativeConjunctionSelector{Actor: recoverer, Partition: recipe.Partition, Member: target, Phase: crossResolve, Occurrence: 1, Kind: recipe.Kind, Side: recipe.Side}, run, deadline)
		if err != nil {
			t.Fatal(err)
		}
		defer gate.close()
		attempt, attemptCancel := f.beginRun(recoverer, proposals[4])
		defer attemptCancel()
		answer := nativeP06AwaitAnswer(f, attempt)
		result, pendingErr := decodeGraphProcessCoordinatorResult(answer.response)
		pending := nativeP06UnknownTransport(answer.err) && errors.Is(pendingErr, errCrossProcessPending)
		success := answer.err == nil && pendingErr == nil && result.decision == decision
		if !pending && !success || recipe.Partition == recipe.Coordinator && !pending || result.resume.tx != tx || result.resume.request != command.request.id {
			t.Fatal("P03 selected recovery loss has neither actual completion nor allowed pending", answer.err, pendingErr)
		}
		transport := ""
		if answer.err != nil {
			transport = answer.err.Error()
		}
		calls = append(calls, nativeP06ConjunctionAttempt{Reply: p06ProcessWire(t, answer.response), Transport: transport, Pending: pending})
		restore := uint64(0)
		if recipe.Partition == recipe.Coordinator {
			outage = p06ProcessWire(t, answer.response)
			restore = leader
		}
		native, rejoined = nativeProtocolFinishRecoveryBatch(t, f, gate, old, restore)
		nativeConjunctionReconnect(f, recoverer)
	}
	completed := f.call(f.n.all[recoverer], processRequest{op: processSourceCoordinate, data: proposals[4].wire})
	results[4], err = decodeGraphProcessCoordinatorResult(completed)
	originalPending, pendingErr := decodeGraphProcessCoordinatorResult(mustNativeProtocolResponse(t, pending))
	calls = append(calls, nativeP06ConjunctionAttempt{Reply: p06ProcessWire(t, completed)})
	f.drain()
	if err != nil || !errors.Is(pendingErr, errCrossProcessPending) || results[4].decision != decision || results[4].resume != originalPending.resume {
		t.Fatal("P03 recovered a different first D")
	}
	recoveryRows, recoveryCharge := nativeProtocolTakeCapture(t, recovery)
	recovery.close()
	p03AssertDecisionRecords(f, decision, boundary, rejoined.Member != 0)
	step := p03Complete(f, recoverer, names[4], proposals[4], results[4], fifthFrom, before)
	f.assertView(step.After, 5)
	pairedFaultAssertCDC(t, step, 5)
	assertLifecycleCandidates(t, step.RawObservation, 3)
	retry := f.replay(recoverer, names[4], proposals[4], results[4], base)
	if rejoined.Member == 0 {
		rejoined = f.replace(leader)
	}
	p03AssertDecisionRecords(f, decision, boundary, true)
	fault := pairedFaultCase{Case: "coordinator-leader-loss-after-commit", Operation: names[4], Marker: "actual D plus native batch conjunction", MarkerRaw: boundary.Reply, CallerClassification: "pending", CallerRaw: pending, OriginalRequest: step.OriginalRequest, Observer: pairedFaultObserver(step), Retry: &retry, ProcessEvidence: []pairedFaultProcess{committed, killed, rejoined}}
	fault.ReopenRetries = append(fault.ReopenRetries, f.replay(recoverer, names[4], proposals[4], results[4], base))
	f.trace.Faults = append(f.trace.Faults, fault)
	before = pairedFaultObserver(step)
	nativeConjunctionReconnect(f, 6)
	from := f.indexes()
	results[5] = f.run(6, proposals[5])
	last := p03Complete(f, 6, names[5], proposals[5], results[5], from, before)
	f.assertView(last.After, 6)
	pairedFaultAssertCDC(t, last, 6)
	assertLifecycleCandidates(t, last.RawObservation, 3)
	stops, starts := f.stopAll(), f.reopenAll()
	final := f.observe(4, graphReadFresh, graphCut{}, false)
	f.assertView(final.View, 6)
	cohort := pairedFaultCase{Case: "same-volume-reopen", Operation: names[5], Marker: "all six actual voters reaped before reopen", MarkerRaw: stops[5].Raw, CallerClassification: "observer-only", CallerRaw: bytes.Clone(f.lastObservationRaw), OriginalRequest: last.OriginalRequest, Observer: final, ProcessEvidence: append(stops, starts...)}
	for stage := 1; stage <= 5; stage++ {
		p03OldRead(f, 4, stage, "after-cohort-reopen")
	}
	for index, p := range proposals {
		actor := uint64(4)
		if index == 0 || index == 4 && recipe.Coordinator == 3 {
			actor = 1
		}
		cohort.ReopenRetries = append(cohort.ReopenRetries, f.replay(actor, names[index], p, results[index], base))
	}
	f.trace.Faults = append(f.trace.Faults, cohort)
	inventory := f.inventory(base)
	records := make([]lifecycleRawRecord, len(inventory))
	for i := range inventory {
		records[i] = inventory[i].lifecycleRawRecord
	}
	f.trace.RawCounts["coordinatorPartition"] = recipe.Coordinator
	f.trace.RawCounts["completeRawRecords"] = inventory
	for member, child := range f.n.all {
		s := f.call(child, processRequest{op: processStateOp})
		if s.stats[statViews] != 0 || s.stats[statPinnedBytes] != 0 || len(s.handles) != 2 || s.handles[0] != 0 || s.handles[1] != 0 {
			t.Fatal("P03 native reader/worker ownership leaked", member)
		}
	}
	if !reflect.DeepEqual(final.View, last.After) || budget.Used() != 16<<20 || budget.Remaining() != 0 || traceBudget.Used() != 8<<20 || traceBudget.Remaining() != 0 {
		t.Fatal("P03 current replay or prepaid artifact changed")
	}
	traceWire, err := json.MarshalIndent(f.trace, "", "  ")
	if err != nil || len(traceWire)+1 > 8<<20 {
		t.Fatal("P03 actual complete trace exceeds its prepaid legacy bound", err)
	}
	traceWire = append(traceWire, '\n')
	traceName := recipe.name() + "-history.json"
	traceFile, err := os.OpenFile(filepath.Join(f.n.dir, traceName), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	n, writeErr := traceFile.Write(traceWire)
	closeErr := traceFile.Close()
	if n != len(traceWire) || writeErr != nil || closeErr != nil {
		t.Fatal("actual bounded sidecar write/close", n, writeErr, closeErr)
	}
	evidence := nativeProtocolCoordinatorEvidence{Recipe: recipe, Run: run, DeadlineUnixNanos: deadline, nativeProtocolBatchEvidence: native, TraceName: traceName, TraceSHA256: sha256.Sum256(traceWire), TraceBytes: uint64(len(traceWire)), From: base, Through: f.indexes(), Boundary: boundary, OriginalPending: pending, OutagePending: outage, InitialPhases: initial, InitialCapture: initialCharge, RecoveryPhases: recoveryRows, RecoveryCapture: recoveryCharge, RecoveryCalls: calls, RecoveryActor: recoverer, BeforeRecovery: killed}
	artifact := struct {
		Version       uint8
		Profile, Case string
		Evidence      nativeProtocolCoordinatorEvidence
	}{1, nativeProtocolCampaignProfile, recipe.name(), evidence}
	if native.Program != nil {
		artifact.Version, artifact.Profile = 2, nativeProtocolProgramProfile
	}
	path := nativeTransitionWriteRows(t, filepath.Join(f.n.dir, recipe.name()+".jsonl"), artifact, records, evidence.Through)
	if native.Program != nil {
		nativeProtocolCheck(t, ctx, path, true)
	} else {
		nativeProtocolCheck(t, ctx, path)
	}
}

func mustNativeProtocolResponse(t *testing.T, wire []byte) processResponse {
	t.Helper()
	r, err := decodeProcessResponse(wire)
	if err != nil {
		t.Fatal("actual recorded native reply", err)
	}
	return r
}

func TestNativeProtocolP03KeepsDurableDecisionThroughApplicableNativeTransitions(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for i, recipe := range nativeProtocolCoordinatorInventory() {
		if testing.Short() && i != 0 && i != 19 && i != 24 && i != 47 {
			continue
		}
		t.Run(recipe.name(), func(t *testing.T) { nativeProtocolCoordinatorCase(t, recipe) })
	}
}

type nativeProtocolPartialRecipe struct {
	Phase     crossPhase
	Partition uint64
	Kind      raftlog.StableStorageKind
	Side      string
}

func (r nativeProtocolPartialRecipe) valid() bool {
	return (nativeProtocolScopeRecipe{Mode: graphReadFresh, Phase: r.Phase, Partition: r.Partition, Kind: r.Kind, Side: r.Side}).valid()
}

func (r nativeProtocolPartialRecipe) name() string {
	return fmt.Sprintf("p03-partial-phase%d-p%d-k%d-%s", r.Phase, r.Partition, r.Kind, r.Side)
}

func (r nativeProtocolPartialRecipe) selector(actor uint64) nativeConjunctionSelector {
	member := uint64(3)
	if r.Partition == 8 {
		member = 5
	}
	return nativeConjunctionSelector{Actor: actor, Partition: r.Partition, Member: member, Phase: r.Phase, Occurrence: 1, Kind: r.Kind, Side: r.Side}
}

func nativeProtocolPartialInventory() []nativeProtocolPartialRecipe {
	var rows []nativeProtocolPartialRecipe
	for _, r := range nativeProtocolScopeInventory() {
		if r.Mode == graphReadFresh {
			rows = append(rows, nativeProtocolPartialRecipe{r.Phase, r.Partition, r.Kind, r.Side})
		}
	}
	return rows
}

type nativeProtocolPartialEvidence struct {
	Recipe            nativeProtocolPartialRecipe
	Run               [16]byte
	DeadlineUnixNanos int64
	nativeProtocolBatchEvidence
	OriginalActor, Reader, RecoveryActor uint64
	Partial                              nativeFaultArtifact
	Completed                            []byte
	Outage                               *nativeP06ConjunctionAttempt
	ProgramCohort                        []pairedFaultProcess `json:",omitempty"`
}

func nativeProtocolPartialRead(t *testing.T, f *pairedGraphFaultNetwork, actor uint64, mode graphReadMode, tx [16]byte, coordinator uint64) nativeFaultPartialRead {
	t.Helper()
	input, err := encodeGraphE15ReadRequest(graphE15ReadRequest{mode: mode, transaction: tx, coordinator: coordinator})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := f.operationContext(5 * time.Second)
	defer cancel()
	response, transport := f.n.all[actor].exchange(ctx, processRequest{op: processSourceCertifiedOpen, data: input})
	actual := errors.Join(transport, processResponseError(response), ctx.Err())
	if !nativeProtocolPendingLoss(actual) || len(response.aux) != 0 || response.sequence == 0 || response.incarnation != f.n.all[actor].incarnation {
		t.Fatal("partial Fresh/After must be actual known unavailable, not invalid/limit/corrupt/expiry", actual)
	}
	f.requireActive()
	name := "Fresh"
	if mode == graphReadAfter {
		name = "After"
	}
	return nativeFaultPartialRead{Mode: name, Classification: "unavailable", Request: input, Response: p06ProcessWire(t, response)}
}

func nativeProtocolPartialWrite(t *testing.T, ctx context.Context, f *pairedGraphFaultNetwork, recipe nativeProtocolPartialRecipe, run [16]byte, deadline int64, native nativeProtocolBatchEvidence, partial nativeFaultArtifact, completed []byte, outage *nativeP06ConjunctionAttempt, budget *graphstate.OutputBudget) {
	t.Helper()
	actor := uint64(6)
	if native.Program != nil && native.Program.Spec.Layout == "rotated" {
		actor = 4
	}
	if !recipe.valid() || native.Actor != actor || native.Target != recipe.selector(actor).Member || budget == nil || budget.Used() != 16<<20 || budget.Remaining() != 0 {
		t.Fatal("partial adapter lacks actual member/caller or prepaid output identity")
	}
	records := partial.Records
	partial.Records = nil
	evidence := nativeProtocolPartialEvidence{Recipe: recipe, Run: run, DeadlineUnixNanos: deadline, nativeProtocolBatchEvidence: native, OriginalActor: actor, Reader: 2, RecoveryActor: actor, Partial: partial, Completed: completed, Outage: outage}
	if native.Program != nil {
		evidence.ProgramCohort = f.stopAll()
	}
	artifact := struct {
		Version       uint8
		Profile, Case string
		Evidence      nativeProtocolPartialEvidence
	}{1, nativeProtocolCampaignProfile, recipe.name(), evidence}
	if native.Program != nil {
		artifact.Version, artifact.Profile = 2, nativeProtocolProgramProfile
	}
	path := nativeTransitionWriteRows(t, filepath.Join(f.n.dir, recipe.name()+".jsonl"), artifact, records, partial.Through)
	if native.Program != nil {
		nativeProtocolCheck(t, ctx, path, true)
	} else {
		nativeProtocolCheck(t, ctx, path)
	}
}

func TestNativeProtocolP03PartialInstallKeepsUnavailableAndOriginalRecoveryDistinct(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for i, r := range nativeProtocolPartialInventory() {
		if testing.Short() && i != 0 && i != 7 && i != 16 && i != 23 {
			continue
		}
		t.Run(r.name(), func(t *testing.T) { nativeFaultPartialCase(t, r) })
	}
}

type nativeProtocolConditionalRecipe struct {
	Schedule  string
	Operation uint8 // One actual post-read business invocation; not a replay alias.
	Phase     crossPhase
	Partition uint64
	Kind      raftlog.StableStorageKind
	Side      string
}

func (r nativeProtocolConditionalRecipe) valid() bool {
	operations := uint8(1)
	switch r.Schedule {
	case "write-skew-A-first", "write-skew-B-first", "authority-fence-before-prepare":
	case "empty-range-and-absent-key-insertion":
		operations = 2
	case "temporal-life-value-ABA":
		operations = 4
	default:
		return false
	}
	co := uint64(3)
	if r.Schedule == "write-skew-B-first" {
		co = 8
	}
	return r.Operation > 0 && r.Operation <= operations && (nativeProtocolCoordinatorRecipe{Coordinator: co, Phase: r.Phase, Partition: r.Partition, Kind: r.Kind, Side: r.Side}).valid()
}

func (r nativeProtocolConditionalRecipe) name() string {
	return fmt.Sprintf("p05-%s-op%d-phase%d-p%d-k%d-%s", r.Schedule, r.Operation, r.Phase, r.Partition, r.Kind, r.Side)
}

func nativeProtocolConditionalInventory() []nativeProtocolConditionalRecipe {
	var rows []nativeProtocolConditionalRecipe
	for _, schedule := range []string{"write-skew-A-first", "write-skew-B-first", "empty-range-and-absent-key-insertion", "temporal-life-value-ABA", "authority-fence-before-prepare"} {
		co, count := uint64(3), uint8(1)
		if schedule == "write-skew-B-first" {
			co = 8
		}
		if schedule == "empty-range-and-absent-key-insertion" {
			count = 2
		}
		if schedule == "temporal-life-value-ABA" {
			count = 4
		}
		for operation := uint8(1); operation <= count; operation++ {
			for _, r := range nativeProtocolCoordinatorInventory() {
				if r.Coordinator == co {
					rows = append(rows, nativeProtocolConditionalRecipe{schedule, operation, r.Phase, r.Partition, r.Kind, r.Side})
				}
			}
		}
	}
	return rows
}

type nativeProtocolConditionalEvidence struct {
	Recipe            nativeProtocolConditionalRecipe
	Run               [16]byte
	DeadlineUnixNanos int64
	nativeProtocolBatchEvidence
	// No third certified view is opened while the two genuine P05 handles
	// occupy their ordinary source slots. Native reopen is proved by the actual
	// process receipt; historical answers come from the unchanged P05 owner and
	// its final genuine At, not a made-up local replacement observation.
	TargetReopened     *p06Observation
	P05                *p05Artifact // Complete trace lives in its separately prepaid sibling.
	From, Through      [2]uint64
	Records            []lifecycleRawRecord
	TraceName          string
	TraceSHA256        [32]byte
	TraceBytes         uint64
	Capture            p05CampaignCaptureCharge
	ProgramCohort      []pairedFaultProcess `json:",omitempty"`
	ProgramReopenAfter uint64               `json:",omitempty"`
}

func nativeProtocolConditionalCase(t *testing.T, recipe nativeProtocolConditionalRecipe, programs ...nativeProtocolProgramSpec) {
	t.Helper()
	if !recipe.valid() {
		t.Fatal("invalid P05 actual invocation/native occurrence")
	}
	budget := nativeProtocolArtifactAllowance(t)
	traceBudget, err := graphstate.NewOutputBudget(8 << 20)
	if err != nil {
		t.Fatal(err)
	}
	if err := traceBudget.Reserve(8 << 20); err != nil {
		t.Fatal(err)
	}
	run, deadline := nativeP06ConjunctionIdentity(t)
	t.Setenv("RHO_GRAPH_STABLE_STORAGE_PROCESS", "1")
	ctx, cancel := context.WithDeadline(t.Context(), time.Unix(0, deadline))
	defer cancel()
	var gate *nativeConjunctionBatchGate
	var native nativeProtocolBatchEvidence
	finished := false
	actor := uint64(1)
	if recipe.Schedule == "write-skew-B-first" {
		actor = 4
	}
	target := uint64(3)
	if recipe.Partition == 8 {
		target = 6
	}
	hooks := &p05CampaignHooks{Parent: ctx}
	var program *nativeProtocolProgram
	var reopenAfter uint64
	if len(programs) != 0 {
		hooks.Start = func(f *pairedGraphFaultNetwork) {
			program = nativeProtocolProgramStart(t, ctx, f, programs...)
			if program.spec.Layout == "rotated" && recipe.Partition == 8 {
				target = 5 // Actual follower; held conditions stay on the real leader6.
			}
		}
		hooks.BeforeReopen = func(f *pairedGraphFaultNetwork, ordinal uint64) {
			f.requireActive()
			if recipe.Schedule != "write-skew-A-first" || !finished || ordinal == 0 || ordinal > 128 || reopenAfter != 0 {
				t.Fatal("actual single conditional cohort/cumulative capture boundary")
			}
			reopenAfter = ordinal
		}
	}
	hooks.Begin = func(f *pairedGraphFaultNetwork, _ uint64, _ graphCut) {
		var err error
		gate, err = newNativeConjunctionBatchGate(ctx, f, nativeConjunctionSelector{Actor: actor, Partition: recipe.Partition, Member: target, Phase: recipe.Phase, Occurrence: recipe.Operation, Kind: recipe.Kind, Side: recipe.Side}, run, deadline)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(gate.close)
	}
	hooks.BeforeRecords = func(f *pairedGraphFaultNetwork) {
		if finished || gate == nil {
			return
		}
		gate.mu.Lock()
		armed := gate.arm.Sequence != 0
		refused := gate.refused || gate.closed
		gate.mu.Unlock()
		if refused {
			t.Fatal("P05 diagnostic selector exhausted before genuine occurrence")
		}
		if !armed {
			return
		} // Later ABA/insertion operation has not run yet.
		f.drain()
		arm, receipt, selected, _, err := gate.evidence()
		if err != nil || !nativeBatchReceiptValid(receipt, arm) || sha256.Sum256(selected) != arm.Control.CommandSHA256 {
			t.Fatal("P05 selected native occurrence missing/foreign", err)
		}
		gate.target.mux.mu.Lock()
		loss := gate.target.mux.err
		gate.target.mux.mu.Unlock()
		if !nativeBatchFailedStepAllowed(processResponse{}, loss) {
			t.Fatal("P05 native boundary was not actual typed pipe loss", loss)
		}
		messageProgram := nativeProtocolProgramFinish(t, f)
		killed := nativeBatchReap(t, f, target, receipt)
		reads := gate.readCount()
		gate.mu.Lock()
		candidates, work := gate.candidates, gate.decodeWork
		gate.mu.Unlock()
		gate.close()
		joined := f.replace(target) // No new certified source/condition authority.
		native = nativeProtocolBatchEvidence{Actor: actor, Target: target, Arm: arm, Batch: receipt, SelectedCommand: selected, TargetTransportLoss: loss.Error(), ReceiptReads: reads, ReceiptGrantBytes: reads * 65536, GateCandidates: candidates, GateDecodeWork: work, TargetProcesses: []pairedFaultProcess{killed, joined}, Program: messageProgram}
		finished = true
	}
	hooks.Finish = func(f *pairedGraphFaultNetwork, p05 p05Artifact, base [2]uint64, charge p05CampaignCaptureCharge) {
		if !finished || budget.Used() != 16<<20 || budget.Remaining() != 0 || traceBudget.Used() != 8<<20 || traceBudget.Remaining() != 0 {
			t.Fatal("P05 selected occurrence never genuinely completed")
		}
		through := f.indexes()
		records := p05.Records
		trace, err := json.MarshalIndent(p05, "", "  ")
		if err != nil || len(trace)+1 > 8<<20 {
			t.Fatal("P05 complete trace exceeds separately prepaid bound", err)
		}
		trace = append(trace, '\n')
		name := recipe.name() + "-history.json"
		file, err := os.OpenFile(filepath.Join(f.n.dir, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		n, writeErr := file.Write(trace)
		closeErr := file.Close()
		if n != len(trace) || writeErr != nil || closeErr != nil {
			t.Fatal("P05 actual bounded trace write/close", n, writeErr, closeErr)
		}
		evidence := nativeProtocolConditionalEvidence{Recipe: recipe, Run: run, DeadlineUnixNanos: deadline, nativeProtocolBatchEvidence: native, From: base, Through: through, Capture: charge, TraceName: name, TraceSHA256: sha256.Sum256(trace), TraceBytes: uint64(len(trace))}
		if program != nil {
			evidence.ProgramCohort = f.stopAll()
			evidence.ProgramReopenAfter = reopenAfter
		}
		artifact := struct {
			Version       uint8
			Profile, Case string
			Evidence      nativeProtocolConditionalEvidence
		}{1, nativeProtocolCampaignProfile, recipe.name(), evidence}
		if program != nil {
			artifact.Version, artifact.Profile = 2, nativeProtocolProgramProfile
		}
		path := nativeTransitionWriteRows(t, filepath.Join(f.n.dir, recipe.name()+".jsonl"), artifact, records, through)
		if program != nil {
			nativeProtocolCheck(t, ctx, path, true)
		} else {
			nativeProtocolCheck(t, ctx, path)
		}
	}
	p05ConditionalCase(t, recipe.Schedule, hooks)
}

func TestNativeProtocolP05DependencyScenariosKeepActualNativeOccurrences(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for i, recipe := range nativeProtocolConditionalInventory() {
		if testing.Short() && i != 0 && i != 24 && i != 48 && i != 96 && i != 191 && i != 215 {
			continue
		}
		t.Run(recipe.name(), func(t *testing.T) { nativeProtocolConditionalCase(t, recipe) })
	}
}

type nativeProtocolFrozenRecipe struct {
	Order, Original string
	Phase           crossPhase
	Partition       uint64
	Candidate       string // Only the actual two private B Decide slots bypass sourceSubmit.
	Kind            raftlog.StableStorageKind
	Side            string
}

func (r nativeProtocolFrozenRecipe) valid() bool {
	if r.Order != "commit-first" && r.Order != "abort-first" || r.Kind != raftlog.StableStorageReady && r.Kind != raftlog.StableStorageApplication || r.Side != "before" && r.Side != "after" {
		return false
	}
	if r.Original == "A" {
		return r.Partition == 3 && r.Phase >= crossRegister && r.Phase <= crossResolve && r.Candidate == ""
	}
	if r.Original != "B" {
		return false
	}
	if r.Phase == crossDecide {
		return r.Partition == 8 && (r.Candidate == "capacity-ABORT" || r.Candidate == "all-YES")
	}
	return r.Candidate == "" && (r.Phase == crossRegister && r.Partition == 8 || (r.Phase == crossPrepare || r.Phase == crossResolve) && (r.Partition == 3 || r.Partition == 8))
}

func (r nativeProtocolFrozenRecipe) name() string {
	candidate := r.Candidate
	if candidate == "" {
		candidate = "ordinary"
	}
	return fmt.Sprintf("p04-%s-%s-phase%d-p%d-%s-k%d-%s", r.Order, r.Original, r.Phase, r.Partition, candidate, r.Kind, r.Side)
}

func (r nativeProtocolFrozenRecipe) selector() nativeConjunctionSelector {
	actor, member := uint64(3), uint64(2)
	if r.Original == "B" {
		actor = 4
	}
	if r.Partition == 8 {
		member = 6
	}
	return nativeConjunctionSelector{Actor: actor, Partition: r.Partition, Member: member, Phase: r.Phase, Occurrence: 1, Kind: r.Kind, Side: r.Side}
}

func nativeProtocolFrozenInventory() []nativeProtocolFrozenRecipe {
	var rows []nativeProtocolFrozenRecipe
	for _, order := range []string{"commit-first", "abort-first"} {
		for _, original := range []string{"A", "B"} {
			for _, phase := range []struct {
				phase     crossPhase
				partition uint64
				candidate string
			}{
				{crossRegister, 3, ""}, {crossPrepare, 3, ""}, {crossDecide, 3, ""}, {crossResolve, 3, ""},
				{crossRegister, 8, ""}, {crossPrepare, 8, ""}, {crossResolve, 8, ""},
				{crossDecide, 8, "capacity-ABORT"}, {crossDecide, 8, "all-YES"},
			} {
				for _, kind := range []raftlog.StableStorageKind{raftlog.StableStorageReady, raftlog.StableStorageApplication} {
					for _, side := range []string{"before", "after"} {
						r := nativeProtocolFrozenRecipe{order, original, phase.phase, phase.partition, phase.candidate, kind, side}
						if r.valid() {
							rows = append(rows, r)
						}
					}
				}
			}
		}
	}
	return rows
}

func nativeProtocolDecisionCall(f *pairedGraphFaultNetwork, p *graphProcess, request nativeDecisionControl) (nativeDecisionReceipt, processResponse) {
	body, err := json.Marshal(request)
	if err != nil {
		f.t.Fatal(err)
	}
	response := f.call(p, processRequest{op: processNativeDecisionControl, data: body})
	var receipt nativeDecisionReceipt
	if err := json.Unmarshal(response.aux, &receipt); err != nil || receipt.Version != 1 || receipt.Profile != nativeDecisionProfile || receipt.Cookie == ([16]byte{}) || receipt.Member < 1 || receipt.Member > 6 || receipt.PID != uint64(p.cmd.Process.Pid) || receipt.Controls < 1 || receipt.Controls > nativeDecisionControls || receipt.PrepaidBytes < 1 {
		f.t.Fatal("actual fresh queued owner receipt", err)
	}
	return receipt, response
}

func nativeProtocolDecisionRetire(f *pairedGraphFaultNetwork, call <-chan graphProcessAnswer, cancel context.CancelFunc, tx [16]byte) []byte {
	cancel()
	answer := nativeP06AwaitAnswer(f, call)
	result, err := decodeGraphProcessCoordinatorResult(answer.response)
	if !errors.Is(answer.err, context.Canceled) || !errors.Is(err, errCrossProcessPending) || result.resume.tx != tx || result.decision != (crossStatus{}) {
		f.t.Fatal("actual retained candidate did not retire pending", answer.err, err)
	}
	return p06ProcessWire(f.t, answer.response)
}

type nativeProtocolFrozenEvidence struct {
	Recipe            nativeProtocolFrozenRecipe
	Run               [16]byte
	DeadlineUnixNanos int64
	nativeProtocolBatchEvidence
	P04             nativeDecisionArtifact
	Frozen          nativeDecisionFrozenReceipt
	CapacityCapture nativeProtocolHoldCharge
	ProgramCohort   []pairedFaultProcess `json:",omitempty"`
}

func nativeProtocolFrozenWrite(t *testing.T, ctx context.Context, f *pairedGraphFaultNetwork, recipe nativeProtocolFrozenRecipe, run [16]byte, deadline int64, native nativeProtocolBatchEvidence, frozen nativeDecisionFrozenReceipt, p04 nativeDecisionArtifact, charge nativeProtocolHoldCharge, budget *graphstate.OutputBudget) {
	selector, queueMember := recipe.selector(), uint64(4)
	if native.Program != nil {
		selector = nativeProtocolProgramFrozenSelector(recipe, native.Program.Spec)
		if native.Program.Spec.Layout == "rotated" {
			queueMember = 6
		}
	}
	if !recipe.valid() || budget == nil || budget.Used() != 16<<20 || budget.Remaining() != 0 || native.Actor != selector.Actor || native.Target != selector.Member || frozen.Version != 1 || frozen.Member != queueMember || frozen.Cookie != p04.Queue.Cookie || !bytes.Equal(frozen.Manifest, p04.Queue.Manifest) {
		t.Fatal("P04 actual native/frozen owner evidence missing")
	}
	records := p04.Records
	p04.Records = nil
	evidence := nativeProtocolFrozenEvidence{Recipe: recipe, Run: run, DeadlineUnixNanos: deadline, nativeProtocolBatchEvidence: native, P04: p04, Frozen: frozen, CapacityCapture: charge}
	if native.Program != nil {
		evidence.ProgramCohort = f.stopAll()
	}
	artifact := struct {
		Version       uint8
		Profile, Case string
		Evidence      nativeProtocolFrozenEvidence
	}{1, nativeProtocolCampaignProfile, recipe.name(), evidence}
	if native.Program != nil {
		artifact.Version, artifact.Profile = 2, nativeProtocolProgramProfile
	}
	path := nativeTransitionWriteRows(t, filepath.Join(f.n.dir, recipe.name()+".jsonl"), artifact, records, p04.Through)
	if native.Program != nil {
		nativeProtocolCheck(t, ctx, path, true)
	} else {
		nativeProtocolCheck(t, ctx, path)
	}
}

func TestNativeProtocolP04QueuedOriginalsKeepFrozenIdentityAtActualNativeBoundaries(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for i, recipe := range nativeProtocolFrozenInventory() {
		if testing.Short() && i != 0 && i != 15 && i != 43 && i != 44 && i != 87 {
			continue
		}
		t.Run(recipe.name(), func(t *testing.T) { nativeDecisionCase(t, recipe.Order, recipe) })
	}
}
