package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

const nativeP06FirstPrepareProfile = "native-p06-first-prepare-sync-conjunction-v1"

// This diagnostic is an actual surviving native IPC read, never a fabricated
// GSR reply or a proof factory. Its exact original coordinate joins the selected
// install-after Sync receipt and the later complete immutable-original census.
type nativeP06SurvivingVote struct {
	Member                            uint64
	Reply                             []byte
	Native                            nativeBatchReceipt
	InputGrantBytes, OutputGrantBytes uint64
}

type nativeP06PreparedProvenance struct {
	Boundary pairedFaultBoundary
	Prepare  allocationProposal
	Manifest crossManifest
	Vote     crossStatus
	Survivor *nativeP06SurvivingVote
}

type nativeP06PreparedContinuation struct {
	f                                *pairedGraphFaultNetwork
	Recipe                           nativeP06ConjunctionRecipe
	Actor, FirstVoter, RecoveryVoter uint64
	Original                         allocationProposal
	Before                           p06Observation
	Old                              graphCut
	From                             [2]uint64
	Pending                          processResponse
	FirstKilled                      pairedFaultProcess
	Evidence                         nativeP06PreparedProvenance
	// Fresh shared campaign only: these are actual records produced while the
	// SAME first coordinator invocation is held, not commands to reconstruct.
	BeforeRecoveryRefused *p06LocalRecord
	BeforeRecoveryChanged *p06LocalRecord
	AllowKnownDecision    bool
}

type nativeP06PreparedCompletion struct {
	P06            p06Artifact
	Calls          [][]byte
	FirstReopened  p06Observation
	RecoveryBVoter uint64
	Capture        nativeP06InitialCharge
}

// One causal completion, shared by Register and first-Prepare profiles. Inputs
// carry their actual source/native observations and original typed handles;
// none is deserialized into private authority. All native calls use f.parent.
func nativeP06CompletePrepared(t *testing.T, s nativeP06PreparedContinuation) nativeP06PreparedCompletion {
	t.Helper()
	f, recipe, evidence := s.f, s.Recipe, s.Evidence
	prepare, manifest := evidence.Prepare, evidence.Manifest
	if evidence.Vote.phase != crossPrepare || !evidence.Vote.yes || evidence.Vote.tx != manifest.tx || evidence.Vote.digest != manifest.digest || evidence.Vote.ns.partition != 3 {
		t.Fatal("shared completion lacks actual first prepared vote")
	}
	if evidence.Survivor == nil {
		_, vote, _ := p06PrepareBoundary(t, evidence.Boundary, recipe.Coordinator, s.FirstVoter)
		if vote != evidence.Vote {
			t.Fatal("source first YES changed before shared completion")
		}
	} else {
		actual, err := decodeProcessResponse(evidence.Survivor.Reply)
		vote, voteErr := decodeCrossStatus(actual.outcome)
		native := evidence.Survivor.Native
		if err != nil || processResponseError(actual) != nil || voteErr != nil || vote != evidence.Vote || len(actual.changes) != 0 || len(evidence.Boundary.Reply) != 0 || evidence.Survivor.Member != s.RecoveryVoter || evidence.Survivor.InputGrantBytes != 512 || evidence.Survivor.OutputGrantBytes != 8*processFrameBytes || native.Count != 2 || native.Control.Kind != raftlog.StableStorageApplication || native.Control.Side != "after" || !native.Events[1].Synced || native.Events[1].Index != vote.index || native.Events[1].CommandSHA256 != sha256.Sum256(prepare.wire) {
			t.Fatal("missing actual stored-vote/selected Sync join", err, voteErr)
		}
	}
	var refused p06LocalRecord
	if s.BeforeRecoveryRefused == nil {
		refused = p06SubmitLocal(f, s.RecoveryVoter, p06LocalProposal(f, s.RecoveryVoter, certifiedP08Close, 1, allocationP08Words(2)), reasonPreparedConflict)
	} else {
		refused = *s.BeforeRecoveryRefused
	}
	changed := s.BeforeRecoveryChanged
	if recipe.Commit && changed != nil {
		t.Fatal("COMMIT continuation was supplied an intervening endpoint Close")
	}
	if !recipe.Commit && changed == nil {
		actual := p06SubmitLocal(f, f.n.leaders[1], p06LocalProposal(f, f.n.leaders[1], certifiedP08Close, 17, allocationP08Words(18)), reasonNone)
		changed = &actual
	}
	if changed != nil {
		f.trace.OperationMapping = append(f.trace.OperationMapping, lifecycleOperationMap{Logical: "node-b-close", Round: p06LocalRound(t, *changed)})
	}
	bVoter := f.n.leaders[1]
	capture := newNativeConjunctionCapture(f, s.Actor, manifest.tx)
	defer capture.close()
	decisionReply := f.call(f.n.all[s.Actor], processRequest{op: processSourceCoordinate, data: s.Original.wire})
	result, err := decodeGraphProcessCoordinatorResult(decisionReply)
	pending, pendingErr := decodeGraphProcessCoordinatorResult(s.Pending)
	if err != nil || !errors.Is(pendingErr, errCrossProcessPending) || result.resume != pending.resume || result.resume.tx != manifest.tx || result.decision.phase != crossDecide || result.decision.commit != recipe.Commit || result.decision.digest != manifest.digest {
		t.Fatal("prepared continuation changed immutable original", err, pendingErr)
	}
	f.drain()
	var phases []p06Phase
	var charge nativeP06InitialCharge
	if s.AllowKnownDecision {
		phases, charge = nativeProtocolRecoveryCapture(t, capture, prepare, manifest, result.decision, recipe.Commit)
	} else {
		phases, charge = nativeP06InitialTake(t, capture, 5)
		p06CheckRecoveryPhases(t, phases, prepare, manifest, result.decision, recipe.Commit)
	}
	nativeP06CheckCensus(t, phases, p06Records(f, s.From, f.indexes()), manifest, prepareManifest(t, prepare), result.decision, recipe.Commit)
	f.trace.OperationMapping = append(f.trace.OperationMapping, lifecycleOperationMap{Logical: "edge-create", Coordinator: recipe.Coordinator, Request: pairedFaultResultOf(result).Request, Transaction: pairedFaultResultOf(result).Transaction, Round: result.decision.round})
	recovered := p06Observe(f, s.Actor, graphReadFresh, graphCut{})
	p06AssertView(t, recovered.View, recipe.Commit, !recipe.Commit, false)
	resolves, err := nativeConjunctionTerminalResolveCommands(phases, nil, prepareManifest(t, prepare))
	if err != nil {
		t.Fatal(err)
	}
	duplicates := []p06LocalRecord{p06ReplayPhase(f, prepare, manifest.tx), p06ReplayPhase(f, resolves[0], manifest.tx), p06ReplayPhase(f, resolves[1], manifest.tx)}
	closed := p06SubmitLocal(f, s.RecoveryVoter, p06LocalProposal(f, s.RecoveryVoter, certifiedP08Close, 1, allocationP08Words(2)), reasonNone)
	f.trace.OperationMapping = append(f.trace.OperationMapping, lifecycleOperationMap{Logical: "node-close", Round: p06LocalRound(t, closed)})
	current := p06Observe(f, s.Actor, graphReadFresh, graphCut{})
	p06AssertView(t, current.View, recipe.Commit, !recipe.Commit, true)
	retained := p06Observe(f, s.Actor, graphReadAt, s.Old)
	if !reflect.DeepEqual(retained, s.Before) {
		t.Fatal("prepared continuation replaced old graph/index state")
	}
	var firstOld p06Observation
	firstRejoined := f.replaceBeforeHeal(s.FirstVoter, func(_ *graphProcess, _ processResponse) { firstOld = nativeP06InitialLocalOld(f, s.FirstVoter, s.Old) })
	if !reflect.DeepEqual(firstOld, s.Before) {
		t.Fatal("actual first YES replacement lost local historical source")
	}
	nativeConjunctionReconnect(f, s.Actor)
	replayed := f.call(f.n.all[s.Actor], processRequest{op: processSourceCoordinate, data: s.Original.wire})
	replayResult, err := decodeGraphProcessCoordinatorResult(replayed)
	if err != nil || replayResult != result {
		t.Fatal("same-directory retry changed actual original", err)
	}
	f.drain()
	through := f.indexes()
	records := p06Records(f, s.From, through)
	nativeP06CheckCensus(t, phases, records, manifest, prepareManifest(t, prepare), result.decision, recipe.Commit)
	p06CheckCDC(t, records, manifest, recipe.Commit, changed, refused, closed)
	return nativeP06PreparedCompletion{P06: p06Artifact{Version: 1, Coordinator: recipe.Coordinator, Commit: recipe.Commit, Original: s.Original.wire, Manifest: prepare.wire, Pending: p06ProcessWire(t, s.Pending), Decision: p06ProcessWire(t, decisionReply), FirstYES: evidence.Boundary, Processes: []pairedFaultProcess{s.FirstKilled, firstRejoined}, Refused: refused, Closed: closed, Changed: changed, Duplicates: duplicates, Phases: phases, Before: s.Before, Recovered: recovered, Current: current, Retained: retained, From: s.From, Through: through, Records: records}, Calls: [][]byte{p06ProcessWire(t, decisionReply), p06ProcessWire(t, replayed)}, FirstReopened: firstOld, RecoveryBVoter: bVoter, Capture: charge}
}

type nativeP06FirstPrepareArtifact struct {
	Version                                                             int
	Profile, Case                                                       string
	Recipe                                                              nativeP06ConjunctionRecipe
	Run                                                                 [16]byte
	DeadlineUnixNanos                                                   int64
	Actor, Target, Successor, FirstVoter, RecoveryVoter, RecoveryBVoter uint64
	P06                                                                 p06Artifact
	InitialCall                                                         nativeP06ConjunctionAttempt
	InitialPhases                                                       []p06Phase
	Calls                                                               [][]byte
	Arm, Batch                                                          nativeBatchReceipt
	SelectedCommand                                                     []byte
	TargetTransportLoss                                                 string
	Processes                                                           []pairedFaultProcess
	EarlyReopened                                                       *p06Observation
	FirstReopened                                                       p06Observation
	Survivor                                                            *nativeP06SurvivingVote
	ReceiptReads, ReceiptGrantBytes, GateCandidates, GateDecodeWork     uint64
	InitialCapture, RecoveryCapture                                     nativeP06InitialCharge
	Program                                                             *nativeP06ProgramEvidence `json:",omitempty"`
}

func nativeP06FirstPrepareName(r nativeP06ConjunctionRecipe) string {
	return fmt.Sprintf("p06-co%d-commit%t-first-prepare-k%d-%s", r.Coordinator, r.Commit, r.Kind, r.Side)
}

func nativeP06FirstPrepareCase(t *testing.T, recipe nativeP06ConjunctionRecipe, programs ...nativeProtocolProgramSpec) {
	t.Helper()
	if !recipe.valid() || recipe.Phase != 0 || recipe.Partition != 0 || recipe.Stage != "" {
		t.Fatal("first Prepare is a separate causal profile")
	}
	run, deadline := nativeP06ConjunctionIdentity(t)
	t.Setenv("RHO_GRAPH_STABLE_STORAGE_PROCESS", "1")
	ctx, cancel := context.WithDeadline(t.Context(), time.Unix(0, deadline))
	defer cancel()
	f := newPairedGraphFaultNetworkWithParent(t, ctx)
	program := nativeP06ProgramStart(t, ctx, f, recipe.Kind, programs...)
	firstVoter, recoveryVoter := uint64(1), uint64(2)
	if program != nil {
		firstVoter = f.n.leaders[0]
		recoveryVoter = nativeP06ProgramAlternate(firstVoter)
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
	gate, err := newNativeConjunctionBatchGate(ctx, f, nativeConjunctionSelector{Actor: actor, Partition: 3, Member: firstVoter, Phase: crossPrepare, Occurrence: 1, Kind: recipe.Kind, Side: recipe.Side}, run, deadline)
	if err != nil {
		t.Fatal(err)
	}
	defer gate.close()
	call, stop := f.beginRun(actor, original)
	defer stop()
	unknown := nativeP06AwaitAnswer(f, call)
	initial, initialErr := decodeGraphProcessCoordinatorResult(unknown.response)
	if !errors.Is(initialErr, errCrossProcessPending) || !nativeP06UnknownTransport(unknown.err) || initial.resume.tx != tx || initial.resume.request != business.request.id || initial.decision != (crossStatus{}) {
		t.Fatal("actual lost first Prepare lacks SAME pending original", initialErr, unknown.err)
	}
	arm, receipt, selected, _, err := gate.evidence()
	if err != nil {
		t.Fatal("unmatched first Prepare transition", err)
	}
	command, _, err := decodeCrossCommand(selected, defaultMaterializerLimits())
	if err != nil || command.phase != crossPrepare || command.ns.partition != 3 {
		t.Fatal("selected first Prepare command", err)
	}
	manifest, err := nativeConjunctionManifest(command)
	if err != nil || manifest.tx != tx {
		t.Fatal(err)
	}
	p06BindBusiness(t, original, manifest)
	gate.target.mux.mu.Lock()
	targetLoss := gate.target.mux.err
	gate.target.mux.mu.Unlock()
	if !nativeBatchFailedStepAllowed(processResponse{}, targetLoss) {
		t.Fatal("first Prepare target lacks typed pipe death", targetLoss)
	}
	nativeP06ProgramLoss(t, program)
	failed := nativeBatchReap(t, f, firstVoter, receipt)
	reads := gate.readCount()
	gate.mu.Lock()
	gateCandidates, gateWork := gate.candidates, gate.decodeWork
	gate.mu.Unlock()
	gate.close()
	initialRows, initialCharge := nativeP06InitialTake(t, capture, 2)
	first := p06SourceProposal(t, initialRows[0].Request)
	registered, _, err := decodeCrossCommand(first.wire, defaultMaterializerLimits())
	registration, statusErr := decodeCrossStatus(p06ReplyBody(t, initialRows[0].Reply))
	if err != nil || statusErr != nil || registered.phase != crossRegister || !bytes.Equal(registered.manifest, command.manifest) || registration.phase != crossRegister || registration.tx != tx || registration.digest != manifest.digest || len(initialRows[1].Reply) != 0 || !bytes.Equal(p06SourceProposal(t, initialRows[1].Request).wire, selected) {
		t.Fatal("first Prepare was not preceded by genuine Register success", err, statusErr)
	}
	f.drain()
	f.elect(firstVoter, recoveryVoter)
	nativeP06ProgramSuccessor(t, program, firstVoter, recoveryVoter)
	nativeConjunctionReconnect(f, actor)
	var earlyOld *p06Observation
	var processes []pairedFaultProcess
	s := nativeP06PreparedContinuation{f: f, Recipe: recipe, Actor: actor, Original: original, Before: wantOld, Old: old, From: from}
	if recipe.Kind == raftlog.StableStorageApplication && recipe.Side == "after" {
		// The first YES was installed before native self-kill, but its GSR was
		// lost. Read the SAME original from the actual surviving applied leader
		// before any mutation or retry; never manufacture a successful reply.
		index := receipt.Events[1].Index
		// Fund one bounded native IPC/read/decode/binary/JSON observation before
		// dispatch; these are finite grants, not measured output or allocations.
		survivor := &nativeP06SurvivingVote{Member: recoveryVoter, Native: receipt, InputGrantBytes: 512, OutputGrantBytes: 8 * processFrameBytes}
		stored := f.call(f.n.all[recoveryVoter], processRequest{op: processEntryRecord, index: index})
		vote, err := decodeCrossStatus(stored.outcome)
		if err != nil || vote.index != index || vote.phase != crossPrepare || !vote.yes || vote.tx != tx || vote.digest != manifest.digest || len(stored.changes) != 0 {
			t.Fatal("survivor lacks SAME selected durable YES original", err)
		}
		s.FirstVoter, s.RecoveryVoter, s.Pending, s.FirstKilled = firstVoter, recoveryVoter, unknown.response, failed
		survivor.Reply = p06ProcessWire(t, stored)
		s.Evidence = nativeP06PreparedProvenance{Boundary: pairedFaultBoundary{initialRows[1].Request, nil}, Prepare: p06SourceProposal(t, initialRows[1].Request), Manifest: manifest, Vote: vote, Survivor: survivor}
	} else {
		var local p06Observation
		rejoined := f.replaceBeforeHeal(firstVoter, func(_ *graphProcess, _ processResponse) { local = nativeP06InitialLocalOld(f, firstVoter, old) })
		if !reflect.DeepEqual(local, wantOld) {
			t.Fatal("initial first-Prepare replacement lost local old cut")
		}
		earlyOld, processes = &local, []pairedFaultProcess{failed, rejoined}
		nativeConjunctionReconnect(f, actor)
		firstGate, clear := p06FirstYESGate(f, actor)
		defer clear()
		call, stop := f.beginRun(actor, original)
		defer stop()
		boundary := f.awaitBoundary(firstGate)
		prepare, vote, actual := p06PrepareBoundary(t, boundary, recipe.Coordinator, recoveryVoter)
		if !bytes.Equal(prepareManifest(t, prepare), command.manifest) || actual.digest != manifest.digest {
			t.Fatal("observed YES replanned original")
		}
		f.packetDrain()
		for member := uint64(1); member <= 3; member++ {
			stored := f.call(f.n.all[member], processRequest{op: processEntryRecord, index: vote.index})
			if !bytes.Equal(stored.outcome, p06ReplyBody(t, boundary.Reply)) || len(stored.changes) != 0 {
				t.Fatal("observed first YES lacks complete voter originals", member)
			}
		}
		killed := f.kill(recoveryVoter, "prepared-participant-leader")
		stop()
		pending := nativeP06AwaitAnswer(f, call)
		resume, err := decodeGraphProcessCoordinatorResult(pending.response)
		if !errors.Is(pending.err, context.Canceled) || !errors.Is(err, errCrossProcessPending) || resume.resume != initial.resume || resume.decision != (crossStatus{}) {
			t.Fatal("observed first YES lost original pending identity", err, pending.err)
		}
		clear()
		f.drain()
		f.elect(recoveryVoter, firstVoter)
		nativeConjunctionReconnect(f, actor)
		s.FirstVoter, s.RecoveryVoter, s.Pending, s.FirstKilled = recoveryVoter, firstVoter, pending.response, killed
		// A repeated GSR may name an already installed original, especially
		// install-before. Keep that actual original index; no new YES is claimed.
		s.Evidence = nativeP06PreparedProvenance{Boundary: boundary, Prepare: prepare, Manifest: actual, Vote: vote}
	}
	completed := nativeP06CompletePrepared(t, s)
	if s.Evidence.Survivor != nil {
		processes, completed.P06.Processes = completed.P06.Processes, nil // ONE actual crash/reopen, not two invented events.
	}
	records := completed.P06.Records
	completed.P06.Records = nil
	a := nativeP06FirstPrepareArtifact{Version: 1, Profile: nativeP06FirstPrepareProfile, Case: nativeP06FirstPrepareName(recipe), Recipe: recipe, Run: run, DeadlineUnixNanos: deadline, Actor: actor, Target: firstVoter, Successor: recoveryVoter, FirstVoter: s.FirstVoter, RecoveryVoter: s.RecoveryVoter, RecoveryBVoter: completed.RecoveryBVoter, P06: completed.P06, InitialCall: nativeP06ConjunctionAttempt{p06ProcessWire(t, unknown.response), unknown.err.Error(), true}, InitialPhases: initialRows, Calls: completed.Calls, Arm: arm, Batch: receipt, SelectedCommand: selected, TargetTransportLoss: targetLoss.Error(), Processes: processes, EarlyReopened: earlyOld, FirstReopened: completed.FirstReopened, Survivor: s.Evidence.Survivor, ReceiptReads: reads, ReceiptGrantBytes: reads * 65536, GateCandidates: gateCandidates, GateDecodeWork: gateWork, InitialCapture: initialCharge, RecoveryCapture: completed.Capture}
	a.Program = nativeP06ProgramComplete(t, program)
	path := nativeTransitionWriteRows(t, filepath.Join(f.n.dir, a.Case+".jsonl"), a, records, a.P06.Through)
	checker := "native_transition_first_prepare_check.py"
	if a.Program != nil {
		checker = "native_protocol_campaign_p06_check.py"
	}
	check := exec.CommandContext(ctx, "python3", "-B", filepath.Join("..", "..", "..", "tools", "v5-substrate", checker), path)
	var diagnostics processStderr
	check.Stdout, check.Stderr = &diagnostics, &diagnostics
	if err := check.Run(); err != nil {
		body, _ := diagnostics.state()
		t.Fatal("first Prepare independent raw/serial bridge", err, body)
	}
	if _, overflow := diagnostics.state(); overflow {
		t.Fatal("bounded first Prepare checker diagnostics")
	}
}

func TestNativePreparedLockP06FirstPrepareKeepsActualUnknownAndStoredYESAcrossSyncDeath(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for i, recipe := range nativeP06ConjunctionRecipes() {
		if testing.Short() && i != 0 && i != 3 && i != 8 && i != 15 {
			continue
		}
		t.Run(nativeP06FirstPrepareName(recipe), func(t *testing.T) { nativeP06FirstPrepareCase(t, recipe) })
	}
}

func TestNativeP06FirstPrepareRegistryNeverInventsMissingYESReply(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	seen := make(map[string]bool)
	for _, recipe := range nativeP06ConjunctionRecipes() {
		name := nativeP06FirstPrepareName(recipe)
		if seen[name] || !recipe.valid() {
			t.Fatal("finite first-Prepare inventory")
		}
		seen[name] = true
	}
	if len(seen) != 16 {
		t.Fatal("first-Prepare inventory")
	}
}
