package graphapply

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

const nativeP06ConjunctionProfile = "native-p06-decide-sync-conjunction-v1"
const nativeP06RecoveryConjunctionProfile = "native-p06-recovery-sync-conjunction-v2"

// Decide16 keeps its original wire/recipe. Recovery64 adds Prepare/Resolve on
// both actual participants; terminal48 adds the three actual late duplicates.
// Missing recipes remain: Register/first Prepare, rotated initial leaders, message variants,
// other P01–P09 rows and applicable publication/import/GC compositions.
type nativeP06ConjunctionRecipe struct {
	Coordinator uint64
	Commit      bool
	Kind        raftlog.StableStorageKind
	Side        string
	Phase       crossPhase `json:",omitzero"`
	Partition   uint64     `json:",omitzero"`
	Stage       string     `json:",omitzero"`
}

func (r nativeP06ConjunctionRecipe) valid() bool {
	if (r.Coordinator != 3 && r.Coordinator != 8) || (r.Kind != raftlog.StableStorageReady && r.Kind != raftlog.StableStorageApplication) || (r.Side != "before" && r.Side != "after") {
		return false
	}
	if r.Stage == nativeP06TerminalStage {
		return r.Phase == crossPrepare && r.Partition == 3 || r.Phase == crossResolve && (r.Partition == 3 || r.Partition == 8)
	}
	return r.Stage == "" && (r.Phase == 0 && r.Partition == 0 || (r.Phase == crossPrepare || r.Phase == crossResolve) && (r.Partition == 3 || r.Partition == 8))
}

func (r nativeP06ConjunctionRecipe) selected() (crossPhase, uint64) {
	if r.Phase == 0 {
		return crossDecide, r.Coordinator
	}
	return r.Phase, r.Partition
}

func (r nativeP06ConjunctionRecipe) name() string {
	if r.Stage == nativeP06TerminalStage {
		return fmt.Sprintf("p06-co%d-commit%t-terminal%d-p%d-k%d-%s", r.Coordinator, r.Commit, r.Phase, r.Partition, r.Kind, r.Side)
	}
	if r.Phase != 0 {
		return fmt.Sprintf("p06-co%d-commit%t-recovery%d-p%d-k%d-%s", r.Coordinator, r.Commit, r.Phase, r.Partition, r.Kind, r.Side)
	}
	return fmt.Sprintf("p06-co%d-commit%t-decide-k%d-%s", r.Coordinator, r.Commit, r.Kind, r.Side)
}

func nativeP06ConjunctionRecipes() []nativeP06ConjunctionRecipe {
	var rows []nativeP06ConjunctionRecipe
	for _, coordinator := range []uint64{3, 8} {
		for _, commit := range []bool{true, false} {
			for _, kind := range []raftlog.StableStorageKind{raftlog.StableStorageReady, raftlog.StableStorageApplication} {
				for _, side := range []string{"before", "after"} {
					rows = append(rows, nativeP06ConjunctionRecipe{Coordinator: coordinator, Commit: commit, Kind: kind, Side: side})
				}
			}
		}
	}
	return rows
}

func nativeP06RecoveryConjunctionRecipes() []nativeP06ConjunctionRecipe {
	var rows []nativeP06ConjunctionRecipe
	for _, base := range nativeP06ConjunctionRecipes() {
		for _, phase := range []crossPhase{crossPrepare, crossResolve} {
			for _, partition := range []uint64{3, 8} {
				row := base
				row.Phase, row.Partition = phase, partition
				rows = append(rows, row)
			}
		}
	}
	return rows
}

type nativeP06ConjunctionAttempt struct {
	Reply     []byte
	Transport string
	Pending   bool
}

type nativeP06ConjunctionArtifact struct {
	Version                                                                         int
	Profile, Case                                                                   string
	Recipe                                                                          nativeP06ConjunctionRecipe
	Run                                                                             [16]byte
	DeadlineUnixNanos                                                               int64
	Actor, FirstVoter, Target, Successor                                            uint64
	P06                                                                             p06Artifact
	Attempts                                                                        []nativeP06ConjunctionAttempt
	UnknownPhases                                                                   []p06Phase
	Arm, Batch                                                                      nativeBatchReceipt
	SelectedCommand                                                                 []byte
	TargetTransportLoss                                                             string
	Processes                                                                       []pairedFaultProcess
	FirstReopened, FaultReopened                                                    p06Observation
	ReceiptReads, ReceiptGrantBytes                                                 uint64
	GateCandidates, GateDecodeWork                                                  uint64
	CaptureCandidates, CaptureAttempts, CaptureWork, CaptureOutput, CaptureRetained uint64
	Duplicate                                                                       *nativeP06TerminalSubmission `json:",omitempty"`
	Program                                                                         *nativeP06ProgramEvidence    `json:",omitempty"`
}

func nativeP06ConjunctionIdentity(t *testing.T) ([16]byte, int64) {
	t.Helper()
	var run [16]byte
	if text := os.Getenv("RHO_GRAPH_TRANSITION_RUN"); text == "" {
		if _, err := io.ReadFull(rand.Reader, run[:]); err != nil {
			t.Fatal(err)
		}
		t.Setenv("RHO_GRAPH_TRANSITION_RUN", hex.EncodeToString(run[:]))
	} else {
		wire, err := hex.DecodeString(text)
		if err != nil || len(wire) != 16 || hex.EncodeToString(wire) != text {
			t.Fatal("original conjunction run identity", err)
		}
		copy(run[:], wire)
	}
	if run == ([16]byte{}) {
		t.Fatal("zero conjunction run")
	}
	if os.Getenv("RHO_GRAPH_TRANSITION_DEADLINE") == "" {
		t.Setenv("RHO_GRAPH_TRANSITION_DEADLINE", strconv.FormatInt(time.Now().Add(3*time.Minute).UnixNano(), 10))
	}
	text := os.Getenv("RHO_GRAPH_TRANSITION_DEADLINE")
	deadline, err := strconv.ParseInt(text, 10, 64)
	now := time.Now().UnixNano()
	if err != nil || strconv.FormatInt(deadline, 10) != text || deadline <= now || deadline-now > int64(1800*time.Second) {
		t.Fatal("fixed original conjunction deadline", err)
	}
	return run, deadline
}

func nativeP06AwaitAnswer(f *pairedGraphFaultNetwork, call <-chan graphProcessAnswer) graphProcessAnswer {
	ctx, cancel := f.operationContext(10 * time.Second)
	defer cancel()
	select {
	case answer := <-call:
		f.requireActive()
		return answer
	case <-ctx.Done():
		f.t.Fatal("actual caller did not retire within original deadline", ctx.Err())
		return graphProcessAnswer{}
	}
}

// Genuine source reply capture and complete stored phase census are separate.
// An unknown Decide may be durable, so the second Run can skip its submission;
// no synthetic successful Decide reply is inserted to make a five-row table.
func nativeP06CheckCensus(t *testing.T, phases []p06Phase, records []lifecycleRawRecord, manifest crossManifest, manifestWire []byte, decision crossStatus, commit bool) {
	t.Helper()
	if len(phases) < 4 || len(phases) > 10 {
		t.Fatal("finite actual successful recovery replies", len(phases))
	}
	originals := make(map[[2]uint64]crossStatus)
	for _, record := range records {
		status, err := decodeCrossStatus(record.Outcome)
		if err != nil || status.index != record.Index {
			continue
		}
		key := [2]uint64{record.Partition, uint64(status.phase)}
		if _, exists := originals[key]; exists || status.tx != manifest.tx || status.digest != manifest.digest {
			t.Fatal("missing/duplicate/foreign actual original phase")
		}
		originals[key] = status
	}
	want := [][2]uint64{{manifest.coordinator, uint64(crossRegister)}, {3, uint64(crossPrepare)}, {8, uint64(crossPrepare)}, {manifest.coordinator, uint64(crossDecide)}, {3, uint64(crossResolve)}, {8, uint64(crossResolve)}}
	if len(originals) != len(want) {
		t.Fatal("incomplete genuine stored original inventory", len(originals))
	}
	for _, key := range want {
		status, found := originals[key]
		why := reasonNone
		if key == ([2]uint64{8, uint64(crossPrepare)}) && !commit {
			why = reasonReadConflict
		}
		if !found || status.why != why || status.phase == crossPrepare && status.yes != (key[0] == 3 || commit) || status.phase >= crossDecide && (status.commit != commit || status.round != decision.round) || status.phase == crossDecide && status != decision {
			t.Fatal("actual stored vote/decision differs", key, status)
		}
	}
	for _, phase := range phases {
		proposal := p06SourceProposal(t, phase.Request)
		command, _, err := decodeCrossCommand(proposal.wire, defaultMaterializerLimits())
		status, statusErr := decodeCrossStatus(p06ReplyBody(t, phase.Reply))
		original, found := originals[[2]uint64{phase.Partition, uint64(status.phase)}]
		if err != nil || statusErr != nil || !found || !bytes.Equal(command.manifest, manifestWire) || status != original || status.phase < command.phase {
			t.Fatal("successful reply is not an actual stored original", err, statusErr)
		}
	}
}

func nativeP06UnknownTransport(err error) bool {
	if err == nil {
		return false
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		children := joined.Unwrap()
		if len(children) == 0 {
			return false
		}
		for _, child := range children {
			if !nativeP06UnknownTransport(child) {
				return false
			}
		}
		return true
	}
	if actual, ok := err.(graphWireError); ok {
		return actual.mask&(1<<3|1<<13) != 0 && actual.mask&^uint16(1<<3|1<<10|1<<13) == 0
	}
	return false
}

func nativeP06ConjunctionCase(t *testing.T, recipe nativeP06ConjunctionRecipe, programs ...nativeProtocolProgramSpec) {
	t.Helper()
	if !recipe.valid() {
		t.Fatal("unsupported P06 conjunction recipe")
	}
	run, deadline := nativeP06ConjunctionIdentity(t)
	t.Setenv("RHO_GRAPH_STABLE_STORAGE_PROCESS", "1")
	ctx, cancel := context.WithDeadline(t.Context(), time.Unix(0, deadline))
	defer cancel()
	f := newPairedGraphFaultNetworkWithParent(t, ctx)
	program := nativeP06ProgramStart(t, ctx, f, recipe.Kind, programs...)
	firstVoter, recoveryVoter, bVoter := uint64(1), uint64(2), uint64(4)
	if program != nil {
		firstVoter, bVoter = f.n.leaders[0], f.n.leaders[1]
		recoveryVoter = nativeP06ProgramAlternate(firstVoter)
	}
	before := nativeFaultSeed(t, f)
	old, err := pairedFaultCut(before.Cut)
	if err != nil {
		t.Fatal(err)
	}
	from, original := f.indexes(), f.proposal(3)
	actor := uint64(3) // Coordinator is outside its current leader's OS process.
	if recipe.Coordinator == 8 {
		actor = 5
	}
	firstGate, clearFirst := p06FirstYESGate(f, actor)
	defer clearFirst()
	firstCall, stopFirst := f.beginRun(actor, original)
	defer stopFirst()
	boundary := f.awaitBoundary(firstGate)
	prepare, vote, manifest := p06PrepareBoundary(t, boundary, recipe.Coordinator, firstVoter)
	p06BindBusiness(t, original, manifest)
	f.packetDrain()
	for member := uint64(1); member <= 3; member++ {
		stored := f.call(f.n.all[member], processRequest{op: processEntryRecord, index: vote.index})
		if !bytes.Equal(stored.outcome, p06ReplyBody(t, boundary.Reply)) || len(stored.changes) != 0 {
			t.Fatal("first actual YES lacks complete voter originals", member)
		}
	}
	firstKilled := f.kill(firstVoter, "prepared-participant-leader")
	stopFirst()
	interrupted := nativeP06AwaitAnswer(f, firstCall)
	resume, pending := decodeGraphProcessCoordinatorResult(interrupted.response)
	if !errors.Is(interrupted.err, context.Canceled) || !errors.Is(pending, errCrossProcessPending) || resume.resume.tx != manifest.tx || resume.resume.request != manifest.request.id || resume.decision != (crossStatus{}) {
		t.Fatal("first YES cancellation lost genuine pending original", interrupted.err, pending)
	}
	clearFirst()
	f.drain()
	f.elect(firstVoter, recoveryVoter)
	nativeP06ProgramSuccessor(t, program, firstVoter, recoveryVoter)
	nativeConjunctionReconnect(f, actor)
	blocked := p06LocalProposal(f, recoveryVoter, certifiedP08Close, 1, allocationP08Words(2))
	refused := p06SubmitLocal(f, recoveryVoter, blocked, reasonPreparedConflict)
	var changed *p06LocalRecord
	if !recipe.Commit {
		p := p06LocalProposal(f, bVoter, certifiedP08Close, 17, allocationP08Words(18))
		actual := p06SubmitLocal(f, bVoter, p, reasonNone)
		changed = &actual
		f.trace.OperationMapping = append(f.trace.OperationMapping, lifecycleOperationMap{Logical: "node-b-close", Round: p06LocalRound(t, actual)})
	}
	// Restore the first dead voter before a second actual leader death, keeping
	// a genuine quorum. At executes through its replacement BEFORE fair heal.
	var firstReopened p06Observation
	firstRejoined := f.replaceBeforeHeal(firstVoter, func(_ *graphProcess, _ processResponse) {
		if program != nil {
			firstReopened = nativeP06InitialLocalOld(f, firstVoter, old)
		} else {
			firstReopened = p06Observe(f, firstVoter, graphReadAt, old)
		}
	})
	if !reflect.DeepEqual(firstReopened.Native, before.RawObservation) || !reflect.DeepEqual(firstReopened.View, before.View) || !reflect.DeepEqual(firstReopened.Cut, before.Cut) {
		t.Fatal("actual first replacement lost old graph/index cut")
	}
	if recipe.Stage == nativeP06TerminalStage {
		nativeP06TerminalCase(t, nativeP06PreparedFixture{f: f, ctx: ctx, recipe: recipe, run: run, deadline: deadline, before: before, old: old, from: from, original: original, actor: actor, boundary: boundary, prepare: prepare, manifest: manifest, interrupted: interrupted, firstKilled: firstKilled, firstRejoined: firstRejoined, refused: refused, changed: changed, firstReopened: firstReopened, program: program})
		return
	}
	target, successor := uint64(2), uint64(1)
	phase, partition := recipe.selected()
	if partition == 8 {
		target, successor = 4, 6
	}
	if program != nil {
		target = f.n.leaders[0]
		if partition == 8 {
			target = f.n.leaders[1]
		}
		successor = nativeP06ProgramAlternate(target)
	}
	capture := newNativeConjunctionCapture(f, actor, manifest.tx)
	defer capture.close()
	gate, err := newNativeConjunctionBatchGate(ctx, f, nativeConjunctionSelector{Actor: actor, Partition: partition, Member: target, Phase: phase, Occurrence: 1, Kind: recipe.Kind, Side: recipe.Side}, run, deadline)
	if err != nil {
		t.Fatal("one actual bounded recovery submission gate", err)
	}
	defer gate.close() // LIFO: restore before/failed before the capture's after.
	call, stop := f.beginRun(actor, original)
	defer stop()
	unknown := nativeP06AwaitAnswer(f, call)
	stop()
	unknownResult, unknownErr := decodeGraphProcessCoordinatorResult(unknown.response)
	nativeLossCode := unknown.response.code == 3 && unknown.response.causeMask == 1<<7 && errors.Is(processResponseError(unknown.response), replica.ErrUnavailable) || unknown.response.code == 6 && unknown.response.causeMask == 0
	if !nativeP06UnknownTransport(unknown.err) || !nativeLossCode || !errors.Is(unknownErr, errCrossProcessPending) || unknownResult.resume.tx != manifest.tx || unknownResult.resume.request != manifest.request.id || unknownResult.decision != (crossStatus{}) {
		t.Fatal("native selected death was not genuine unknown original", unknown.err, unknownErr)
	}
	arm, receipt, selected, _, err := gate.evidence()
	if err != nil {
		t.Fatal("unmatched/ambiguous/exhausted recovery transition cannot qualify", err)
	}
	gate.target.mux.mu.Lock()
	targetLoss := gate.target.mux.err
	gate.target.mux.mu.Unlock()
	if !nativeBatchFailedStepAllowed(processResponse{}, targetLoss) {
		t.Fatal("selected target did not retain genuine transport death", targetLoss)
	}
	selectedCommand, _, err := decodeCrossCommand(selected, defaultMaterializerLimits())
	if err != nil || selectedCommand.phase != phase || selectedCommand.ns.partition != partition || !bytes.Equal(selectedCommand.manifest, prepareManifest(t, prepare)) {
		t.Fatal("selected actual recovery submission changed first original", err)
	}
	nativeP06ProgramLoss(t, program)
	batchKilled := nativeBatchReap(t, f, target, receipt)
	gate.close()
	f.elect(target, successor)
	nativeP06ProgramSuccessor(t, program, target, successor)
	nativeConjunctionReconnect(f, actor)
	finalResponse := f.call(f.n.all[actor], processRequest{op: processSourceCoordinate, data: original.wire})
	result, err := decodeGraphProcessCoordinatorResult(finalResponse)
	if err != nil || result.resume.tx != manifest.tx || result.resume.request != manifest.request.id || result.decision.phase != crossDecide || result.decision.commit != recipe.Commit || result.decision.digest != manifest.digest {
		t.Fatal("actual successor did not settle SAME original", err)
	}
	f.drain()
	capture.close()
	capture.mu.Lock()
	var phases, failed []p06Phase
	for _, actual := range capture.requests {
		if len(actual.Reply) == 0 {
			failed = append(failed, actual)
		} else {
			phases = append(phases, actual)
		}
	}
	if capture.refused || len(failed) != 1 || capture.attempts != uint64(len(phases)+len(failed)) {
		t.Fatal("actual recovery capture refused or hid unknown attempts")
	}
	unknownProposal := p06SourceProposal(t, failed[0].Request)
	if !bytes.Equal(unknownProposal.wire, selected) || failed[0].Partition != partition || failed[0].Origin != actor {
		t.Fatal("unanswered source attempt was not selected native submission")
	}
	candidates, attempts, work, output, retainedBytes := capture.candidates, capture.attempts, capture.work, capture.output, capture.retained
	capture.requests = nil // Ownership transferred once after callbacks retire.
	capture.mu.Unlock()
	f.trace.OperationMapping = append(f.trace.OperationMapping, lifecycleOperationMap{Logical: "edge-create", Coordinator: recipe.Coordinator, Request: hex.EncodeToString(manifest.request.id[:]), Transaction: hex.EncodeToString(manifest.tx[:]), Round: result.decision.round})
	recovered := p06Observe(f, actor, graphReadFresh, graphCut{})
	p06AssertView(t, recovered.View, recipe.Commit, !recipe.Commit, false)
	var terminal [2]allocationProposal
	if recipe.Phase != 0 {
		// ABORT can skip an already resolved participant after a lost ACK. The
		// unanswered source command is not a successful reply or private proof:
		// authenticate the complete actual originals before ordinary native
		// terminal replay revalidates those same observed command bytes.
		nativeP06CheckCensus(t, phases, p06Records(f, from, f.indexes()), manifest, prepareManifest(t, prepare), result.decision, recipe.Commit)
		terminal, err = nativeConjunctionTerminalResolveCommands(phases, failed, prepareManifest(t, prepare))
		if err != nil {
			t.Fatal("missing genuine source command after actual terminal census", err)
		}
	}
	duplicates := []p06LocalRecord{p06ReplayPhase(f, prepare, manifest.tx)}
	for ordinal, partition := range []uint64{3, 8} {
		if recipe.Phase != 0 {
			duplicates = append(duplicates, p06ReplayPhase(f, terminal[ordinal], manifest.tx))
			continue
		}
		var actual *allocationProposal
		for _, phase := range phases {
			p := p06SourceProposal(t, phase.Request)
			command, _, decodeErr := decodeCrossCommand(p.wire, defaultMaterializerLimits())
			if decodeErr != nil {
				t.Fatal(decodeErr)
			}
			if command.phase == crossResolve && command.ns.partition == partition {
				actual = &p
			}
		}
		if actual == nil {
			t.Fatal("missing genuine successful Resolve source command", partition)
		}
		duplicates = append(duplicates, p06ReplayPhase(f, *actual, manifest.tx))
	}
	endpoint := p06LocalProposal(f, f.n.leaders[0], certifiedP08Close, 1, allocationP08Words(2))
	closed := p06SubmitLocal(f, f.n.leaders[0], endpoint, reasonNone)
	f.trace.OperationMapping = append(f.trace.OperationMapping, lifecycleOperationMap{Logical: "node-close", Round: p06LocalRound(t, closed)})
	current := p06Observe(f, actor, graphReadFresh, graphCut{})
	p06AssertView(t, current.View, recipe.Commit, !recipe.Commit, true)
	retained := p06Observe(f, actor, graphReadAt, old)
	if !reflect.DeepEqual(retained.Native, before.RawObservation) || !reflect.DeepEqual(retained.View, before.View) || !reflect.DeepEqual(retained.Cut, before.Cut) {
		t.Fatal("old certified state substituted current")
	}
	var faultReopened p06Observation
	batchRejoined := f.replaceBeforeHeal(target, func(_ *graphProcess, _ processResponse) {
		if program != nil {
			faultReopened = nativeP06InitialLocalOld(f, target, old)
		} else {
			faultReopened = p06Observe(f, target, graphReadAt, old)
		}
	})
	if !reflect.DeepEqual(faultReopened, retained) {
		t.Fatal("actual selected replacement lost local old data")
	}
	// Full useful-original replay after both real directory replacements.
	nativeConjunctionReconnect(f, actor)
	replayed := f.call(f.n.all[actor], processRequest{op: processSourceCoordinate, data: original.wire})
	replayResult, err := decodeGraphProcessCoordinatorResult(replayed)
	if err != nil || replayResult.decision != result.decision || replayResult.resume != result.resume {
		t.Fatal("same original replay changed actual decision", err)
	}
	f.drain()
	through := f.indexes()
	records := p06Records(f, from, through)
	nativeP06CheckCensus(t, phases, records, manifest, prepareManifest(t, prepare), result.decision, recipe.Commit)
	p06CheckCDC(t, records, manifest, recipe.Commit, changed, refused, closed)
	reads := gate.readCount()
	gate.mu.Lock()
	gateCandidates, gateWork := gate.candidates, gate.decodeWork
	gate.mu.Unlock()
	transport := ""
	if unknown.err != nil {
		transport = unknown.err.Error()
	}
	version, profile := 1, nativeP06ConjunctionProfile
	if recipe.Phase != 0 {
		version, profile = 2, nativeP06RecoveryConjunctionProfile
	}
	a := nativeP06ConjunctionArtifact{Version: version, Profile: profile, Case: recipe.name(), Recipe: recipe, Run: run, DeadlineUnixNanos: deadline, Actor: actor, FirstVoter: firstVoter, Target: target, Successor: successor,
		P06:      p06Artifact{Version: 1, Coordinator: recipe.Coordinator, Commit: recipe.Commit, Original: original.wire, Manifest: prepare.wire, Pending: p06ProcessWire(t, interrupted.response), Decision: p06ProcessWire(t, finalResponse), FirstYES: boundary, Processes: []pairedFaultProcess{firstKilled, firstRejoined}, Refused: refused, Changed: changed, Closed: closed, Duplicates: duplicates, Phases: phases, Before: p06Observation{before.Cut, before.View, before.RawObservation}, Recovered: recovered, Current: current, Retained: retained, From: from, Through: through, Records: nil},
		Attempts: []nativeP06ConjunctionAttempt{{p06ProcessWire(t, unknown.response), transport, true}, {p06ProcessWire(t, finalResponse), "", false}, {p06ProcessWire(t, replayed), "", false}}, UnknownPhases: failed, Arm: arm, Batch: receipt, SelectedCommand: selected, TargetTransportLoss: targetLoss.Error(), Processes: []pairedFaultProcess{batchKilled, batchRejoined}, FirstReopened: firstReopened, FaultReopened: faultReopened, ReceiptReads: reads, ReceiptGrantBytes: reads * 65536, GateCandidates: gateCandidates, GateDecodeWork: gateWork, CaptureCandidates: candidates, CaptureAttempts: attempts, CaptureWork: work, CaptureOutput: output, CaptureRetained: retainedBytes}
	a.Program = nativeP06ProgramComplete(t, program)
	nativeP06ConjunctionWrite(t, ctx, f, a, records)
}

func nativeP06ConjunctionWrite(t *testing.T, ctx context.Context, f *pairedGraphFaultNetwork, a nativeP06ConjunctionArtifact, records []lifecycleRawRecord) {
	t.Helper()
	path := nativeTransitionWriteRows(t, filepath.Join(f.n.dir, a.Case+".jsonl"), a, records, a.P06.Through)
	checker := "native_transition_conjunction_check.py"
	if a.Program != nil {
		checker = "native_protocol_campaign_p06_check.py"
	}
	check := exec.CommandContext(ctx, "python3", "-B", filepath.Join("..", "..", "..", "tools", "v5-substrate", checker), path)
	var diagnostics processStderr
	check.Stdout, check.Stderr = &diagnostics, &diagnostics
	if err := check.Run(); err != nil {
		body, _ := diagnostics.state()
		t.Fatal("independent P06 conjunction original/serial checker", err, body)
	}
	if _, overflow := diagnostics.state(); overflow {
		t.Fatal("independent P06 conjunction diagnostic budget")
	}
}

func prepareManifest(t *testing.T, proposal allocationProposal) []byte {
	t.Helper()
	command, _, err := decodeCrossCommand(proposal.wire, defaultMaterializerLimits())
	if err != nil {
		t.Fatal(err)
	}
	return command.manifest
}

func TestNativePreparedLockP06ExactDecideBatchesPreserveOriginalAndOldCut(t *testing.T) {
	for i, recipe := range nativeP06ConjunctionRecipes() {
		// Four short cells retain both coordinators/outcomes/kinds/sides.
		if testing.Short() && i != 0 && i != 7 && i != 9 && i != 14 {
			continue
		}
		t.Run(recipe.name(), func(t *testing.T) { nativeP06ConjunctionCase(t, recipe) })
	}
}

func TestNativePreparedLockP06RecoveryPrepareAndResolveBatchesPreserveOriginal(t *testing.T) {
	for i, recipe := range nativeP06RecoveryConjunctionRecipes() {
		// Four additional short cells retain both phases/participants, both
		// coordinator/outcome/kind/side choices. Full keeps all64 exact rows.
		if testing.Short() && i != 0 && i != 29 && i != 38 && i != 63 {
			continue
		}
		t.Run(recipe.name(), func(t *testing.T) { nativeP06ConjunctionCase(t, recipe) })
	}
}

func TestNativeConjunctionRegistryDeadlineAndGateOwnership(t *testing.T) {
	old, err := json.Marshal(nativeP06ConjunctionRecipes()[0])
	if err != nil || string(old) != `{"Coordinator":3,"Commit":true,"Kind":1,"Side":"before"}` {
		t.Fatal("Decide16 recipe bytes changed", err)
	}
	seen := make(map[string]bool)
	for _, row := range nativeP06ConjunctionRecipes() {
		if !row.valid() || seen[row.name()] {
			t.Fatal("non-finite/duplicate conjunction recipe")
		}
		seen[row.name()] = true
	}
	if len(seen) != 16 || (nativeP06ConjunctionRecipe{Coordinator: 1, Kind: 1, Side: "after"}).valid() || (nativeConjunctionSelector{Actor: 3, Member: 4, Partition: 3, Phase: crossDecide, Kind: 1, Side: "after"}).valid() {
		t.Fatal("invented coordinator/target/recipe")
	}
	for _, row := range nativeP06RecoveryConjunctionRecipes() {
		if !row.valid() || seen[row.name()] {
			t.Fatal("non-finite/duplicate actual recovery occurrence")
		}
		seen[row.name()] = true
	}
	if len(seen) != 80 || (nativeP06ConjunctionRecipe{Coordinator: 3, Commit: true, Kind: 1, Side: "before", Phase: crossPrepare}).valid() || (nativeP06ConjunctionRecipe{Coordinator: 3, Commit: true, Kind: 1, Side: "before", Phase: crossRegister, Partition: 3}).valid() {
		t.Fatal("recovery registry silently claims Register/initial occurrences")
	}
	if _, err := nativeConjunctionTerminalResolveCommands(nil, nil, nil); !errors.Is(err, errInvalid) {
		t.Fatal("missing observed commands became terminal replay authority", err)
	}
	if _, err := nativeConjunctionTerminalResolveCommands(make([]p06Phase, 11), nil, nil); !errors.Is(err, errLimit) {
		t.Fatal("unbounded terminal observation traversal", err)
	}
	f := &pairedGraphFaultNetwork{t: t}
	legacy, cancelLegacy := f.operationContext(time.Minute)
	if legacy.Err() != nil {
		t.Fatal("nil parent changed legacy context")
	}
	cancelLegacy()
	parent, cancelParent := context.WithCancel(t.Context())
	f.parent = parent
	cancelParent()
	actual, cancelActual := f.operationContext(time.Minute)
	defer cancelActual()
	if !errors.Is(actual.Err(), context.Canceled) {
		t.Fatal("fresh operation reset an expired original deadline")
	}
	parent, cancelParent = context.WithTimeout(t.Context(), time.Minute)
	defer cancelParent()
	f.parent = parent
	bounded, stop := f.operationContext(10 * time.Second)
	defer stop()
	deadline, _ := bounded.Deadline()
	outer, _ := parent.Deadline()
	if deadline.After(outer) || deadline.After(time.Now().Add(10*time.Second)) {
		t.Fatal("fresh per-call context exceeds native cap or original deadline")
	}
	hub := &graphProcessHub{}
	priorCalled := false
	hub.beforeRelay = func(context.Context, uint64, uint64, []byte) error { priorCalled = true; return nil }
	ownerCtx, ownerCancel := context.WithCancel(parent)
	gate := &nativeConjunctionBatchGate{ctx: ownerCtx, cancel: ownerCancel, f: f, hub: hub, priorBefore: hub.beforeRelay}
	if err := reserveNativeConjunctionGate(f, gate); err != nil {
		t.Fatal(err)
	}
	if err := reserveNativeConjunctionGate(f, &nativeConjunctionBatchGate{}); !errors.Is(err, errLimit) {
		t.Fatal("two gates may overwrite fixture callback ownership", err)
	}
	gate.close()
	gate.close()
	if f.conjunctionGate != nil || !errors.Is(ownerCtx.Err(), context.Canceled) {
		t.Fatal("gate close did not retire sole owner")
	}
	if err := hub.beforeRelay(parent, 3, 3, nil); err != nil || !priorCalled {
		t.Fatal("LIFO close lost prior semantic owner", err)
	}
	// Capture/prepaid ownership is inert here: malformed observations cannot
	// become successful replies or alter any actual source response.
	capture := &nativeConjunctionCapture{hub: hub, actor: 3}
	if err := capture.before(parent, 3, 3, []byte("inert malformed observation")); err != nil || !capture.refused || len(capture.requests) != 0 || capture.work == 0 {
		t.Fatal("unfunded/malformed capture became successful", err)
	}
	if !nativeP06UnknownTransport(errors.Join(graphWireError{mask: 1 << 3})) || nativeP06UnknownTransport(errors.Join(graphWireError{mask: 1 << 3}, errLimit)) || nativeP06UnknownTransport(graphWireError{mask: 1<<3 | 1<<12}) || nativeP06UnknownTransport(io.EOF) {
		t.Fatal("native invalid/limit/expiry or raw caller death qualified as unknown Decide")
	}
}
