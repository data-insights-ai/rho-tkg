package graphapply

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

const nativeP06InitialProfile = "native-p06-initial-register-sync-conjunction-v1"

type nativeP06InitialCharge struct {
	Candidates, Attempts, Work, Output, Retained uint64
}

type nativeP06InitialArtifact struct {
	Version                                                             int
	Profile, Case                                                       string
	Recipe                                                              nativeP06ConjunctionRecipe
	Run                                                                 [16]byte
	DeadlineUnixNanos                                                   int64
	Actor, Target, Successor, FirstVoter, RecoveryVoter, RecoveryBVoter uint64
	P06                                                                 p06Artifact
	InitialCall                                                         nativeP06ConjunctionAttempt
	InitialPhase                                                        p06Phase
	Calls                                                               [][]byte
	Arm, Batch                                                          nativeBatchReceipt
	SelectedCommand                                                     []byte
	TargetTransportLoss                                                 string
	Processes                                                           []pairedFaultProcess
	InitialReopened, FirstReopened                                      p06Observation
	ReceiptReads, ReceiptGrantBytes, GateCandidates, GateDecodeWork     uint64
	InitialCapture, RecoveryCapture                                     nativeP06InitialCharge
}

func nativeP06InitialName(r nativeP06ConjunctionRecipe) string {
	return fmt.Sprintf("p06-co%d-commit%t-initial-register-k%d-%s", r.Coordinator, r.Commit, r.Kind, r.Side)
}

// Transfer only actual prepaid observations after the native owner has retired.
// No new source allowance, private authority or successful reply is constructed.
func nativeP06InitialTake(t *testing.T, c *nativeConjunctionCapture, count int) ([]p06Phase, nativeP06InitialCharge) {
	t.Helper()
	c.close()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.refused || len(c.requests) != count {
		t.Fatal("incomplete genuine initial/recovery source census")
	}
	rows := c.requests
	c.requests = nil
	return rows, nativeP06InitialCharge{c.candidates, c.attempts, c.work, c.output, c.retained}
}

func nativeP06InitialLocalOld(f *pairedGraphFaultNetwork, member uint64, old graphCut) p06Observation {
	// At needs no leader ReadIndex. Select this genuine replacement as its own
	// partition's source before fair heal; otherwise follower2's default source1
	// would only establish a routed historical answer, not local recovery.
	a, b := f.n.leaders[0], f.n.leaders[1]
	if member <= 3 {
		a = member
	} else {
		b = member
	}
	f.call(f.n.all[member], processRequest{op: processPairedReconnect, data: allocationP08Words(a, b)})
	// The parent multiplexer routes independently of the child's authenticated
	// transport. Publish its matching route only AFTER the actual child ACK.
	if err := nativeP06InitialLocalRoute(f.n.all[member].mux.hub, member, a, b); err != nil {
		f.t.Fatal("actual replacement local source route", err)
	}
	return p06Observe(f, member, graphReadAt, old)
}

func nativeP06InitialLocalRoute(hub *graphProcessHub, member, a, b uint64) error {
	if hub == nil || member < 1 || member > 6 || a < 1 || a > 3 || b < 4 || b > 6 || member <= 3 && a != member || member >= 4 && b != member {
		return errInvalid
	}
	hub.mu.Lock()
	defer hub.mu.Unlock()
	if hub.relayTargets == nil {
		hub.relayTargets = make(map[uint64]map[uint64]uint64)
	}
	hub.relayTargets[member] = map[uint64]uint64{3: a, 8: b}
	return nil
}

func nativeP06InitialRegisterCase(t *testing.T, recipe nativeP06ConjunctionRecipe) {
	t.Helper()
	if !recipe.valid() || recipe.Phase != 0 || recipe.Partition != 0 || recipe.Stage != "" {
		t.Fatal("initial Register is a separate pre-YES causal recipe")
	}
	run, deadline := nativeP06ConjunctionIdentity(t)
	t.Setenv("RHO_GRAPH_STABLE_STORAGE_PROCESS", "1")
	ctx, cancel := context.WithDeadline(t.Context(), time.Unix(0, deadline))
	defer cancel()
	f := newPairedGraphFaultNetworkWithParent(t, ctx)
	before := nativeFaultSeed(t, f)
	old, err := pairedFaultCut(before.Cut)
	if err != nil {
		t.Fatal(err)
	}
	from, original := f.indexes(), f.proposal(3)
	actor, target, successor := uint64(3), uint64(1), uint64(2)
	if recipe.Coordinator == 8 {
		actor, target, successor = 5, 4, 6
	}
	business, err := decodePartitionGraphCommand(original.wire, defaultMaterializerLimits())
	if err != nil {
		t.Fatal(err)
	}
	ns := business.request.ns
	ns.partition = recipe.Coordinator
	tx := crossTransactionID(ns, business.request.id)
	initialCapture := newNativeConjunctionCapture(f, actor, tx)
	defer initialCapture.close()
	gate, err := newNativeConjunctionBatchGate(ctx, f, nativeConjunctionSelector{Actor: actor, Partition: recipe.Coordinator, Member: target, Phase: crossRegister, Occurrence: 1, Kind: recipe.Kind, Side: recipe.Side}, run, deadline)
	if err != nil {
		t.Fatal(err)
	}
	defer gate.close() // One gate, restored before its sole after-relay owner.
	initialCall, stopInitial := f.beginRun(actor, original)
	defer stopInitial()
	unknown := nativeP06AwaitAnswer(f, initialCall)
	initialResult, initialErr := decodeGraphProcessCoordinatorResult(unknown.response)
	if !errors.Is(initialErr, errCrossProcessPending) || !nativeP06UnknownTransport(unknown.err) || initialResult.resume.tx != tx || initialResult.resume.request != business.request.id || initialResult.decision != (crossStatus{}) {
		t.Fatal("actual lost Register call lacks SAME pending original", initialErr, unknown.err)
	}
	arm, receipt, selected, _, err := gate.evidence()
	if err != nil {
		t.Fatal("unobserved initial Register transition", err)
	}
	command, _, err := decodeCrossCommand(selected, defaultMaterializerLimits())
	if err != nil || command.phase != crossRegister || command.ns.partition != recipe.Coordinator {
		t.Fatal("actual initial Register command", err)
	}
	initialManifest, err := nativeConjunctionManifest(command)
	if err != nil || initialManifest.tx != tx {
		t.Fatal("actual initial retained manifest", err)
	}
	p06BindBusiness(t, original, initialManifest)
	gate.target.mux.mu.Lock()
	targetLoss := gate.target.mux.err
	gate.target.mux.mu.Unlock()
	if !nativeBatchFailedStepAllowed(processResponse{}, targetLoss) {
		t.Fatal("initial native target lacks typed pipe death", targetLoss)
	}
	failed := nativeBatchReap(t, f, target, receipt)
	reads := gate.readCount()
	gate.mu.Lock()
	gateCandidates, gateWork := gate.candidates, gate.decodeWork
	gate.mu.Unlock()
	gate.close()
	initialRows, initialCharge := nativeP06InitialTake(t, initialCapture, 1)
	if len(initialRows[0].Reply) != 0 || !bytes.Equal(p06SourceProposal(t, initialRows[0].Request).wire, selected) {
		t.Fatal("lost Register acquired an invented successful source reply")
	}
	f.drain()
	f.elect(target, successor)
	var initialOld p06Observation
	initialRejoined := f.replaceBeforeHeal(target, func(_ *graphProcess, _ processResponse) { initialOld = nativeP06InitialLocalOld(f, target, old) })
	wantOld := p06Observation{before.Cut, before.View, before.RawObservation}
	if !reflect.DeepEqual(initialOld, wantOld) {
		t.Fatal("initial replacement lost its local original cut")
	}
	nativeConjunctionReconnect(f, actor)
	// The SAME surviving coordinator owns the genuine retained run.tx. Even
	// when Register was not committed, retry uses that private manifest; this
	// parent never recovers an authority object from SelectedCommand.
	firstVoter := f.n.leaders[0]
	firstGate, clearFirst := p06FirstYESGate(f, actor)
	defer clearFirst()
	firstCall, stopFirst := f.beginRun(actor, original)
	defer stopFirst()
	boundary := f.awaitBoundary(firstGate)
	prepare, vote, manifest := p06PrepareBoundary(t, boundary, recipe.Coordinator, firstVoter)
	if manifest.tx != tx || !bytes.Equal(prepareManifest(t, prepare), command.manifest) {
		t.Fatal("retained private registration manifest changed before first YES")
	}
	f.packetDrain()
	for member := uint64(1); member <= 3; member++ {
		stored := f.call(f.n.all[member], processRequest{op: processEntryRecord, index: vote.index})
		if !bytes.Equal(stored.outcome, p06ReplyBody(t, boundary.Reply)) || len(stored.changes) != 0 {
			t.Fatal("actual first YES lacks complete voter originals", member)
		}
	}
	firstKilled := f.kill(firstVoter, "prepared-participant-leader")
	stopFirst()
	interrupted := nativeP06AwaitAnswer(f, firstCall)
	resume, pending := decodeGraphProcessCoordinatorResult(interrupted.response)
	if !errors.Is(interrupted.err, context.Canceled) || !errors.Is(pending, errCrossProcessPending) || resume.resume != initialResult.resume || resume.decision != (crossStatus{}) {
		t.Fatal("first YES loss changed genuine pending original", interrupted.err, pending)
	}
	clearFirst()
	f.drain()
	recoveryVoter := uint64(2)
	if firstVoter == 2 {
		recoveryVoter = 1
	}
	f.elect(firstVoter, recoveryVoter)
	nativeConjunctionReconnect(f, actor)
	refused := p06SubmitLocal(f, recoveryVoter, p06LocalProposal(f, recoveryVoter, certifiedP08Close, 1, allocationP08Words(2)), reasonPreparedConflict)
	var changed *p06LocalRecord
	if !recipe.Commit {
		actual := p06SubmitLocal(f, f.n.leaders[1], p06LocalProposal(f, f.n.leaders[1], certifiedP08Close, 17, allocationP08Words(18)), reasonNone)
		changed = &actual
		f.trace.OperationMapping = append(f.trace.OperationMapping, lifecycleOperationMap{Logical: "node-b-close", Round: p06LocalRound(t, actual)})
	}
	recoveryBVoter := f.n.leaders[1]
	capture := newNativeConjunctionCapture(f, actor, tx)
	defer capture.close()
	decisionReply := f.call(f.n.all[actor], processRequest{op: processSourceCoordinate, data: original.wire})
	result, err := decodeGraphProcessCoordinatorResult(decisionReply)
	if err != nil || result.resume != initialResult.resume || result.decision.phase != crossDecide || result.decision.commit != recipe.Commit || result.decision.digest != manifest.digest {
		t.Fatal("initial interruption changed first immutable decision", err)
	}
	f.drain()
	phases, recoveryCharge := nativeP06InitialTake(t, capture, 5)
	p06CheckRecoveryPhases(t, phases, prepare, manifest, result.decision, recipe.Commit)
	nativeP06CheckCensus(t, phases, p06Records(f, from, f.indexes()), manifest, prepareManifest(t, prepare), result.decision, recipe.Commit)
	f.trace.OperationMapping = append(f.trace.OperationMapping, lifecycleOperationMap{Logical: "edge-create", Coordinator: recipe.Coordinator, Request: pairedFaultResultOf(result).Request, Transaction: pairedFaultResultOf(result).Transaction, Round: result.decision.round})
	recovered := p06Observe(f, actor, graphReadFresh, graphCut{})
	p06AssertView(t, recovered.View, recipe.Commit, !recipe.Commit, false)
	resolves, err := nativeConjunctionTerminalResolveCommands(phases, nil, prepareManifest(t, prepare))
	if err != nil {
		t.Fatal(err)
	}
	duplicates := []p06LocalRecord{p06ReplayPhase(f, prepare, tx), p06ReplayPhase(f, resolves[0], tx), p06ReplayPhase(f, resolves[1], tx)}
	closed := p06SubmitLocal(f, recoveryVoter, p06LocalProposal(f, recoveryVoter, certifiedP08Close, 1, allocationP08Words(2)), reasonNone)
	f.trace.OperationMapping = append(f.trace.OperationMapping, lifecycleOperationMap{Logical: "node-close", Round: p06LocalRound(t, closed)})
	current := p06Observe(f, actor, graphReadFresh, graphCut{})
	p06AssertView(t, current.View, recipe.Commit, !recipe.Commit, true)
	retained := p06Observe(f, actor, graphReadAt, old)
	if !reflect.DeepEqual(retained, wantOld) {
		t.Fatal("initial Register recovery replaced old graph/index state")
	}
	var firstOld p06Observation
	firstRejoined := f.replaceBeforeHeal(firstVoter, func(_ *graphProcess, _ processResponse) { firstOld = nativeP06InitialLocalOld(f, firstVoter, old) })
	if !reflect.DeepEqual(firstOld, wantOld) {
		t.Fatal("first YES replacement lost actual local historical source")
	}
	nativeConjunctionReconnect(f, actor)
	replayed := f.call(f.n.all[actor], processRequest{op: processSourceCoordinate, data: original.wire})
	replayResult, err := decodeGraphProcessCoordinatorResult(replayed)
	if err != nil || replayResult != result {
		t.Fatal("same-directory retry changed actual original", err)
	}
	f.drain()
	through := f.indexes()
	records := p06Records(f, from, through)
	nativeP06CheckCensus(t, phases, records, manifest, prepareManifest(t, prepare), result.decision, recipe.Commit)
	p06CheckCDC(t, records, manifest, recipe.Commit, changed, refused, closed)
	a := nativeP06InitialArtifact{Version: 1, Profile: nativeP06InitialProfile, Case: nativeP06InitialName(recipe), Recipe: recipe, Run: run, DeadlineUnixNanos: deadline, Actor: actor, Target: target, Successor: successor, FirstVoter: firstVoter, RecoveryVoter: recoveryVoter, RecoveryBVoter: recoveryBVoter,
		P06:         p06Artifact{Version: 1, Coordinator: recipe.Coordinator, Commit: recipe.Commit, Original: original.wire, Manifest: prepare.wire, Pending: p06ProcessWire(t, interrupted.response), Decision: p06ProcessWire(t, decisionReply), FirstYES: boundary, Processes: []pairedFaultProcess{firstKilled, firstRejoined}, Refused: refused, Closed: closed, Changed: changed, Duplicates: duplicates, Phases: phases, Before: wantOld, Recovered: recovered, Current: current, Retained: retained, From: from, Through: through},
		InitialCall: nativeP06ConjunctionAttempt{p06ProcessWire(t, unknown.response), unknown.err.Error(), true}, InitialPhase: initialRows[0], Calls: [][]byte{p06ProcessWire(t, decisionReply), p06ProcessWire(t, replayed)}, Arm: arm, Batch: receipt, SelectedCommand: selected, TargetTransportLoss: targetLoss.Error(), Processes: []pairedFaultProcess{failed, initialRejoined}, InitialReopened: initialOld, FirstReopened: firstOld, ReceiptReads: reads, ReceiptGrantBytes: reads * 65536, GateCandidates: gateCandidates, GateDecodeWork: gateWork, InitialCapture: initialCharge, RecoveryCapture: recoveryCharge}
	path := nativeTransitionWriteRows(t, filepath.Join(f.n.dir, a.Case+".jsonl"), a, records, through)
	check := exec.CommandContext(ctx, "python3", "-B", filepath.Join("..", "..", "..", "tools", "v5-substrate", "native_transition_initial_check.py"), path)
	var diagnostics processStderr
	check.Stdout, check.Stderr = &diagnostics, &diagnostics
	if err := check.Run(); err != nil {
		t.Fatal("initial Register independent raw/serial bridge", err, diagnostics.String())
	}
}

func TestNativePreparedLockP06InitialRegisterPrecedesFirstYESAcrossSyncDeath(t *testing.T) {
	for i, recipe := range nativeP06ConjunctionRecipes() {
		if testing.Short() && i != 0 && i != 7 && i != 10 && i != 15 {
			continue
		}
		t.Run(nativeP06InitialName(recipe), func(t *testing.T) { nativeP06InitialRegisterCase(t, recipe) })
	}
}

func TestNativeP06InitialRegisterRegistryKeepsPreYESOrderAndOriginalIdentity(t *testing.T) {
	seen := make(map[string]bool)
	for _, recipe := range nativeP06ConjunctionRecipes() {
		name := nativeP06InitialName(recipe)
		if !recipe.valid() || seen[name] {
			t.Fatal("non-finite initial Register registry")
		}
		seen[name] = true
	}
	if len(seen) != 16 {
		t.Fatal("initial Register inventory")
	}
	// Inert routing only: no source/certificate or native authority is created.
	hub := &graphProcessHub{}
	if err := nativeP06InitialLocalRoute(hub, 2, 2, 4); err != nil || hub.relayTargets[2][3] != 2 || hub.relayTargets[2][8] != 4 {
		t.Fatal("replacement2 historical read would route through1", err)
	}
	if err := nativeP06InitialLocalRoute(hub, 2, 1, 4); !errors.Is(err, errInvalid) || hub.relayTargets[2][3] != 2 {
		t.Fatal("nonlocal source replaced an admitted local route", err)
	}
	// Content/rounds cannot replace a coordinator-scoped original retry key.
	ns := namespace{partition: 3}
	ns.graph[0] = 1
	id := requestID{2}
	original := crossTransactionID(ns, id)
	ns.partition = 8
	if original == crossTransactionID(ns, id) || original == crossTransactionID(namespace{graph: ns.graph, partition: 3}, requestID{3}) {
		t.Fatal("coordinator/request retry identity collapsed")
	}
}
