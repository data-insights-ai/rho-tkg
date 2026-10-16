package graphapply

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
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
	"testing/synctest"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/dist/replica"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
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
	check := exec.CommandContext(ctx, "python3", "-B", filepath.Join("..", "..", "..", "..", "tools", "v5-substrate", checker), path)
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
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for i, recipe := range nativeP06ConjunctionRecipes() {
		// Four short cells retain both coordinators/outcomes/kinds/sides.
		if testing.Short() && i != 0 && i != 7 && i != 9 && i != 14 {
			continue
		}
		t.Run(recipe.name(), func(t *testing.T) { nativeP06ConjunctionCase(t, recipe) })
	}
}

func TestNativePreparedLockP06RecoveryPrepareAndResolveBatchesPreserveOriginal(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
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
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
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

func TestNativeConjunctionGateReadTrafficPreservesFourthPrepareCandidate(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCrossPartitionNetwork(t)
	ops, claims := crossEdgeFixture(t, n)
	tx, registration := n.crossPlan(ops, claims, 24)
	n.crossSubmit(0, registration)
	var id [16]byte
	copy(id[:], tx.manifest[4:20])
	registered := n.crossReadPhase(0, id, crossRegister)
	first, err := n.hosts[0][0].PrepareCrossParticipant(tx, 3, registered, nil)
	if err != nil {
		t.Fatal(err)
	}
	n.crossSubmit(0, first)
	vote := n.crossReadPhase(0, id, crossPrepare)
	prepare, err := n.hosts[0][0].PrepareCrossParticipant(tx, 8, registered, []crossProof{vote})
	if err != nil {
		t.Fatal(err)
	}
	proposal, err := encodeGraphSourceProposal(prepare)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	// The actual native Prepare below is diagnostic input only. Its fourth
	// occurrence must reach the native-owner door, whose absent transport is
	// deliberately refused; no synthetic arm/receipt becomes authority.
	absentOwner := errors.New("guard: native owner transport unavailable")
	newGate := func() *nativeConjunctionBatchGate {
		return &nativeConjunctionBatchGate{ctx: ctx, selector: nativeConjunctionSelector{Actor: 1, Partition: 8, Member: 5, Phase: crossPrepare, Occurrence: 4, Kind: raftlog.StableStorageApplication, Side: "after"}, target: &graphProcess{mux: &graphProcessMux{err: absentOwner}}}
	}
	request := func(kind byte, sequence uint64, command []byte) []byte {
		wire, e := encodeGraphSourceRequest(graphSourceRequest{kind: kind, sequence: sequence, nonce: [16]byte{1}, maximum: 128, command: command})
		if e != nil {
			t.Fatal(e)
		}
		return wire
	}
	gate := newGate()
	prior := 0
	gate.priorBefore = func(context.Context, uint64, uint64, []byte) error { prior++; return nil }
	for i := range 129 {
		if err := gate.before(ctx, 1, 8, request(sourceGet, uint64(i+1), nil)); err != nil {
			t.Fatal("native-encoded read traffic changed the service path", err)
		}
	}
	if gate.refused || gate.candidates != 0 || gate.decodeWork != 0 || gate.encounters != 0 {
		t.Fatal("unrelated native read traffic exhausted Submit selector", gate.refused, gate.candidates, gate.decodeWork, gate.encounters)
	}
	for i := range 4 {
		err := gate.before(ctx, 1, 8, request(sourceSubmit, uint64(i+130), proposal))
		if i < 3 && err != nil || i == 3 && (!errors.Is(err, absentOwner) || !errors.Is(err, errCorrupt)) {
			t.Fatal("fourth actual Prepare did not reach its real native-owner door", i, err)
		}
	}
	if prior != 133 || gate.candidates != 4 || gate.encounters != 4 || gate.decodeWork != 4*(4096+32*processFrameBytes) || gate.refused || gate.arm.Sequence != 0 || len(gate.selected) != 0 {
		t.Fatal("read classification changed exact full Submit funding/occurrence or fabricated native authority")
	}
	for _, malformed := range [][]byte{nil, []byte("GSQ\x01"), {'G', 'S', 'Q', 1, sourceSubmit}, request(sourceSubmit, 1, []byte("not-a-proposal"))} {
		g := newGate()
		if err := g.before(ctx, 1, 8, malformed); err == nil || g.candidates != 1 || g.decodeWork != 4096+32*processFrameBytes || g.encounters != 0 || g.arm.Sequence != 0 {
			t.Fatal("malformed relevant candidate escaped its full decode grant", err)
		}
	}
	// The fixed-header bypass confers no service authentication: malformed read
	// bodies remain invalid at the unchanged full native service decoder.
	badRead := request(sourceGet, 1, nil)
	badRead[len(badRead)-1] ^= 1
	g := newGate()
	if err := g.before(ctx, 1, 8, badRead); err != nil || g.candidates != 0 {
		t.Fatal("read classification performed a hidden full diagnostic decode", err)
	}
	if _, err := decodeGraphSourceRequest(badRead); !errors.Is(err, errCorrupt) {
		t.Fatal("header classification authenticated a corrupt service request", err)
	}
	for _, oversized := range [][]byte{make([]byte, processFrameBytes+1), append(request(sourceGet, 1, nil), make([]byte, processFrameBytes)...)} {
		g := newGate()
		if err := g.before(ctx, 1, 8, oversized); err != nil || !g.refused || g.candidates != 0 || g.decodeWork != 0 {
			t.Fatal("oversized header gained decode or diagnostic qualification", err)
		}
	}
	g = newGate()
	g.candidates, g.decodeWork = 128, 128*(4096+32*processFrameBytes)
	if err := g.before(ctx, 1, 8, request(sourceSubmit, 1, proposal)); err != nil || !g.refused || g.candidates != 128 || g.decodeWork != 128*(4096+32*processFrameBytes) {
		t.Fatal("Submit candidate limit was enlarged or refunded", err)
	}
	if _, _, _, _, err := g.evidence(); !errors.Is(err, errCrossProcessPending) {
		t.Fatal("exhausted selector gained native evidence", err)
	}
	g = newGate()
	dead, stop := context.WithCancel(ctx)
	stop()
	g.ctx = dead
	if err := g.before(ctx, 1, 8, request(sourceSubmit, 1, proposal)); !errors.Is(err, context.Canceled) || !errors.Is(err, errCrossProcessPending) || g.candidates != 1 || g.encounters != 0 || g.arm.Sequence != 0 {
		t.Fatal("Submit renewed an expired original context", err)
	}
	foreign, err := encodeGraphSourceProposal(first)
	if err != nil {
		t.Fatal(err)
	}
	g = newGate()
	g.selector.Occurrence = 1
	if err := g.before(ctx, 1, 8, request(sourceSubmit, 1, foreign)); !errors.Is(err, errInvalid) || g.candidates != 1 || g.decodeWork != 4096+32*processFrameBytes || g.arm.Sequence != 0 {
		t.Fatal("foreign native namespace bypassed paid Submit validation", err)
	}
}

func TestNativeConjunctionFollowerReceiptWaitsForAuthenticatedLoss(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		command := []byte("inert native receipt routing selection; not proposal authority")
		control := nativeBatchControl{Version: 1, Action: "arm", Cookie: [16]byte{1}, Sequence: 1, Kind: raftlog.StableStorageApplication, Side: "before", CommandSHA256: sha256.Sum256(command)}
		deadline, _ := ctx.Deadline()
		arm := nativeBatchReceipt{Version: 1, Profile: nativeBatchProfile, Cookie: control.Cookie, Run: [16]byte{2}, Member: 1, PID: uint64(os.Getpid()), Sequence: 1, Control: control, State: "armed", DeadlineUnixNanos: deadline.UnixNano()}
		hub := &graphProcessHub{peers: make(map[uint64]*graphProcessMux), idle: make(chan struct{}, 1)}
		target := &graphProcess{mux: &graphProcessMux{hub: hub, err: io.EOF}}
		hub.peers[1] = target.mux
		target.mux.p = target
		f := &pairedGraphFaultNetwork{t: t, parent: ctx, down: make(map[uint64]bool)}
		gate := &nativeConjunctionBatchGate{ctx: ctx, f: f, hub: hub, target: target, selector: nativeConjunctionSelector{Actor: 3, Partition: 3, Member: 1, Phase: crossDecide, Kind: control.Kind, Side: control.Side}, dir: t.TempDir(), owner: arm, arm: arm, selected: command, lost: make(chan struct{})}
		f.conjunctionGate = gate
		answer := make(chan error, 1)
		go func() { answer <- gate.awaitLoss() }()
		synctest.Wait()
		select {
		case err := <-answer:
			t.Fatal("selected follower became ready before its native receipt/loss", err)
		default:
		}
		if gate.readCount() != 0 || gate.evidenceTaken {
			t.Fatal("waiting consumed missing receipt or transferred selected command")
		}
		job := graphProcessPacketJob{origin: 2, packet: replica.Packet{From: 2, To: 1}}
		if gate.failed(job, io.EOF) {
			t.Fatal("missing native receipt authenticated transport loss")
		}
		// This canonical inert receipt exercises the real file decoder and gate
		// binding. Only the OS semantic parent proves actual Sync/self-SIGKILL.
		receipt := arm
		receipt.State, receipt.Attempts, receipt.Work, receipt.Entries, receipt.Output, receipt.Count = "observing", 1, 4096, 2, 2*nativeBatchReceiptBytes+1024, 1
		receipt.Events[0] = raftlog.StableStorageObservation{Selection: raftlog.StableStorageSelection{Cookie: control.Cookie, Sequence: control.Sequence, CommandSHA256: control.CommandSHA256}, Kind: control.Kind, Stage: 1, Matches: 1, EntryCount: 1, Position: 1, FirstIndex: 43, LastIndex: 43, Index: 43, Term: 2, CommandBytes: uint64(len(command)), CommandSHA256: control.CommandSHA256, EntryManifestSHA256: [32]byte{3}, BatchBytes: 128, BatchPayloadSHA256: [32]byte{4}, Work: raftlog.StableStorageWork{Entries: 2, Bytes: 4096, OutputBytes: 1024}}
		if err := nativeBatchPersist(gate.dir, receipt); err != nil {
			t.Fatal(err)
		}
		foreign := job
		foreign.packet.To = 3
		if gate.failed(foreign, io.EOF) || gate.failed(job, errLimit) {
			t.Fatal("foreign target/native refusal authenticated selected loss")
		}
		gate.mu.Lock()
		gate.arm.PID++
		gate.mu.Unlock()
		if gate.failed(job, io.EOF) {
			t.Fatal("foreign process receipt authenticated selected loss")
		}
		gate.mu.Lock()
		gate.arm = arm
		gate.mu.Unlock()
		synctest.Wait()
		select {
		case err := <-answer:
			t.Fatal("unmatched loss released native evidence waiter", err)
		default:
		}
		if !gate.failed(job, io.EOF) {
			t.Fatal("exact native receipt/target loss was refused")
		}
		if err := <-answer; err != nil {
			t.Fatal("authenticated loss did not release original waiter", err)
		}
		actualArm, actual, selected, reads, err := gate.evidence()
		if err != nil || actualArm != arm || actual != receipt || !bytes.Equal(selected, command) || reads != 4 || !f.down[1] {
			t.Fatal("receipt/command/owner changed across causal wait", err, reads)
		}
	})
}

func TestNativeConjunctionFollowerReceiptWaitKeepsOriginalContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		gate := &nativeConjunctionBatchGate{ctx: ctx, arm: nativeBatchReceipt{Sequence: 1}, selected: []byte{1}, lost: make(chan struct{})}
		answer := make(chan error, 1)
		go func() { answer <- gate.awaitLoss() }()
		synctest.Wait()
		cancel()
		if err := <-answer; !errors.Is(err, context.Canceled) || !errors.Is(err, errCrossProcessPending) {
			t.Fatal("native receipt wait escaped its original cancellation", err)
		}
		close(gate.lost)
		if err := gate.awaitLoss(); !errors.Is(err, context.Canceled) || gate.readCount() != 0 || gate.evidenceTaken {
			t.Fatal("late native loss revived expired evidence ownership", err)
		}
		for _, mutation := range []func(*nativeConjunctionBatchGate){
			func(g *nativeConjunctionBatchGate) { g.arm.Sequence = 0 },
			func(g *nativeConjunctionBatchGate) { g.selected = nil },
			func(g *nativeConjunctionBatchGate) { g.closed = true },
			func(g *nativeConjunctionBatchGate) { g.refused = true },
			func(g *nativeConjunctionBatchGate) { g.evidenceTaken = true },
		} {
			g := &nativeConjunctionBatchGate{ctx: t.Context(), arm: nativeBatchReceipt{Sequence: 1}, selected: []byte{1}, lost: make(chan struct{})}
			mutation(g)
			close(g.lost)
			if err := g.awaitLoss(); !errors.Is(err, errCrossProcessPending) || g.readCount() != 0 {
				t.Fatal("unowned native loss gained evidence admission", err)
			}
		}
		if err := (*nativeConjunctionBatchGate)(nil).awaitLoss(); !errors.Is(err, errInvalid) {
			t.Fatal("nil native gate became evidence owner", err)
		}
	})
}
