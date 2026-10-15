package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
)

const nativeTransitionScopeProfile = "native-outside-data-scope-v1"

type nativeTransitionScopeObservation struct {
	Request, Token, Facts []byte
}

type nativeTransitionScopePrepared struct {
	Member        uint64
	State, Record []byte
}

type nativeTransitionScopeArtifact struct {
	Version                           uint8
	Profile, Mode                     string
	Original, Manifest, PendingCaller []byte
	Decision                          pairedFaultBoundary
	Lookup                            pairedFaultBoundary
	Failure                           []byte
	FailureMask                       uint16
	Before, Current                   p06Observation
	Old, Held, Recovered              nativeTransitionScopeObservation
	Retained, Reopened                nativeTransitionScopeObservation
	Retry                             []byte
	Prepared                          []nativeTransitionScopePrepared
	Processes                         []pairedFaultProcess
	From, Through                     [2]uint64
	Records                           []lifecycleRawRecord
}

func nativeTransitionScopeOpen(t *testing.T, f *pairedGraphFaultNetwork, ctx context.Context, actor uint64, request graphE15ReadRequest) (uint64, graphCut, nativeTransitionScopeObservation) {
	t.Helper()
	wire, err := encodeGraphE15ReadRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	r, err := f.n.all[actor].exchange(ctx, processRequest{op: processSourceCertifiedOpen, data: wire})
	if err != nil {
		t.Fatal("actual scoped open", err)
	}
	handle, cut, err := decodeGraphE15ReadToken(r.aux)
	if err != nil || len(cut.certificates) != 1 || cut.certificates[0].scope.partition != 3 {
		t.Fatal("A-only actual returned certificate", cut, err)
	}
	observation := nativeTransitionScopeObservation{Request: wire, Token: r.aux}
	observation.Facts = nativeTransitionScopeFacts(t, f, ctx, actor, handle)
	return handle, cut, observation
}

func nativeTransitionScopeFacts(t *testing.T, f *pairedGraphFaultNetwork, ctx context.Context, actor, handle uint64) []byte {
	t.Helper()
	ids, err := encodeGraphProcessObserveIDs([]uint64{1, 3, 7})
	if err != nil {
		t.Fatal(err)
	}
	r, err := f.n.all[actor].exchange(ctx, processRequest{op: processSourceCertifiedObserve, index: handle, data: ids})
	if err != nil {
		t.Fatal("actual A-only read", err)
	}
	return r.aux
}

func nativeTransitionScopeAssert(t *testing.T, wire []byte, current bool) {
	t.Helper()
	facts, err := decodeGraphProcessObservationBatch(wire)
	if err != nil || len(facts) != 3 {
		t.Fatal("complete scoped native answer", err)
	}
	for i, id := range []uint64{1, 3, 7} {
		actual := facts[i]
		if actual.id != id {
			t.Fatal("missing/substituted scoped entity")
		}
		expected := certifiedProcessFact{}
		if id == 1 && current {
			expected = certifiedProcessFact{Exists: true, Active: true, Life: 2}
		}
		got := actual.fact
		if got.Exists != expected.Exists || got.Active != expected.Active || got.Life != expected.Life || got.Source != 0 || got.Target != 0 || !slices.Equal(got.Labels, expected.Labels) || len(got.Incident) != 0 {
			t.Fatal("literal scoped graph/labels/negative entities", actual.fact, expected)
		}
	}
}

// This bounded source case uses six genuine OS children. The existing P01
// native-host parent remains a separate, more detailed scalar/claim witness.
func nativeTransitionScopeCase(t *testing.T, mode graphReadMode, campaign ...nativeProtocolScopeRecipe) string {
	t.Helper()
	if mode != graphReadFresh && mode != graphReadAfter || len(campaign) > 1 || len(campaign) == 1 && (!campaign[0].valid() || campaign[0].Mode != mode) {
		t.Fatal("finite P01 mode")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	var run [16]byte
	var deadline int64
	if len(campaign) == 1 {
		cancel()
		run, deadline = nativeP06ConjunctionIdentity(t)
		t.Setenv("RHO_GRAPH_STABLE_STORAGE_PROCESS", "1")
		ctx, cancel = context.WithDeadline(t.Context(), time.Unix(0, deadline))
		defer cancel()
	}
	arena, err := graphstate.NewOutputBudget(16 << 20)
	if err != nil {
		t.Fatal(err)
	}
	// Retained source/diagnostic ownership is admitted before child creation.
	// This allowance is not measured RSS or a raised production read limit.
	if err := arena.Reserve(2*processFrameBytes + (4 << 20) + (2 << 20)); err != nil {
		t.Fatal(err)
	}
	if len(campaign) == 1 {
		if err := arena.Reserve(4 << 20); err != nil {
			t.Fatal("fresh native receipt/process/local-old serialized allowance", err)
		}
	}
	var f *pairedGraphFaultNetwork
	if len(campaign) == 0 {
		f = newPairedGraphFaultNetwork(t)
	} else {
		f = newPairedGraphFaultNetworkWithParent(t, ctx)
	}
	reader := uint64(3)
	if len(campaign) == 1 {
		reader = 2 // Survives both actual selected native voters3/6.
	}
	// Only B exists at C. A's genuine creation is later coordinated by B;
	// the old scoped exact-empty answer must remain empty after that commit.
	nativeTransitionCertificateBusiness(t, f, 2)
	before := p06Observe(f, 6, graphReadFresh, graphCut{})
	var nativeOld graphCut
	if len(campaign) == 1 {
		nativeOld, err = pairedFaultCut(before.Cut)
		if err != nil {
			t.Fatal("actual complete old graph cut for selected native replacement", err)
		}
	}
	oldHandle, oldCut, old := nativeTransitionScopeOpen(t, f, ctx, 2, graphE15ReadRequest{mode: graphReadFresh, partitions: []uint64{3}})
	oldClosed := false
	defer func() {
		if !oldClosed {
			if f.parent == nil {
				f.n.all[2].call(t, processRequest{op: processSourceCertifiedClose, index: oldHandle})
			} else {
				f.cleanupCertifiedHandle(f.n.all[2], &oldHandle)
			}
		}
	}()
	nativeTransitionScopeAssert(t, old.Facts, false)
	from := f.indexes()
	original := f.proposal(1)
	command, err := decodePartitionGraphCommand(original.wire, defaultMaterializerLimits())
	if err != nil {
		t.Fatal(err)
	}
	tx := crossTransactionID(namespace{command.request.ns.graph, 8}, command.request.id)
	manifest := nativeTransitionCutManifest(t, f.n.all[1].mux.hub, tx)
	entered, releaseDecision := p03GateCommittedReply(f, 5, 8)
	defer releaseDecision()
	var gate *nativeConjunctionBatchGate
	var native nativeProtocolScopeEvidence
	if len(campaign) == 1 {
		r := campaign[0]
		target := uint64(3)
		if r.Partition == 8 {
			target = 6
		}
		gate, err = newNativeConjunctionBatchGate(ctx, f, nativeConjunctionSelector{Actor: 5, Partition: r.Partition, Member: target, Phase: r.Phase, Occurrence: 1, Kind: r.Kind, Side: r.Side}, run, deadline)
		if err != nil {
			t.Fatal(err)
		}
		defer gate.close()
	}
	call, cancelCall := f.beginRun(5, original)
	defer cancelCall()
	decisionBoundary := f.awaitBoundary(entered)
	peer, err := decodeGraphWireResponse(decisionBoundary.Reply)
	if err != nil || peer.err != nil {
		t.Fatal("genuine B Decide reply", err, peer.err)
	}
	decision, err := decodeCrossStatus(peer.body)
	if err != nil || !decision.commit || decision.phase != crossDecide || decision.ns.partition != 8 || decision.tx != tx {
		t.Fatal("original coordinator B durable COMMIT", decision, err)
	}
	if gate == nil {
		p03AssertDecisionRecords(f, decision, decisionBoundary, true)
	}
	actualManifest := manifest()
	if len(actualManifest) > 64<<10 || sha256.Sum256(actualManifest) != decision.digest {
		t.Fatal("bounded original full manifest")
	}
	cancelCall()
	var pending graphProcessAnswer
	select {
	case pending = <-call:
	case <-ctx.Done():
		t.Fatal("same original caller not retired", ctx.Err())
	}
	releaseDecision()
	f.drain()
	if gate != nil && campaign[0].Phase != crossResolve {
		native.nativeProtocolBatchEvidence = nativeProtocolFinishBatch(t, f, gate, nativeOld)
		p03AssertDecisionRecords(f, decision, decisionBoundary, true)
	}
	resume, decodeErr := decodeGraphProcessCoordinatorResult(pending.response)
	if !errors.Is(decodeErr, errCrossProcessPending) || resume.resume.tx != tx || resume.decision != (crossStatus{}) || !errors.Is(pending.err, context.Canceled) {
		t.Fatal("unknown original changed to success/ABORT", pending.err, decodeErr)
	}
	hub := f.n.all[1].mux.hub
	hub.mu.Lock()
	if gate == nil || campaign[0].Phase != crossResolve {
		hub.beforeRelay = nil
	}
	// A selected Resolve still lies ahead. Keep its actual outer native gate;
	// the bounded original-manifest observer remains its immutable predecessor.
	hub.mu.Unlock()
	prepared := make([]nativeTransitionScopePrepared, 0, 3)
	for _, member := range []uint64{1, 2, 3} {
		state := f.call(f.n.all[member], processRequest{op: processStateOp})
		record := f.call(f.n.all[member], processRequest{op: processEntryRecord, index: state.applied})
		vote, err := decodeCrossStatus(record.outcome)
		if err != nil || vote.phase != crossPrepare || !vote.yes || vote.tx != tx || vote.digest != decision.digest || vote.index != state.applied || len(record.changes) != 0 {
			t.Fatal("actual A voters must retain only original YES", member, vote, err)
		}
		prepared = append(prepared, nativeTransitionScopePrepared{Member: member, State: p06ProcessWire(t, state), Record: p06ProcessWire(t, record)})
	}
	lookup := make(chan pairedFaultBoundary, 1)
	releaseLookup := make(chan struct{})
	var once sync.Once
	unblock := sync.OnceFunc(func() { close(releaseLookup) })
	defer unblock()
	hub.mu.Lock()
	hub.afterRelay = func(caller context.Context, origin, partition uint64, wire, reply []byte) error {
		if origin != reader || partition != 8 {
			return nil
		}
		request, err := decodeGraphSourceRequest(wire)
		if err != nil || request.kind != sourceCrossRead || request.phase != crossDecide {
			return err
		}
		if len(wire)+len(reply) > 64<<10 {
			return errLimit
		}
		once.Do(func() { lookup <- pairedFaultBoundary{bytes.Clone(wire), bytes.Clone(reply)} })
		select {
		case <-releaseLookup:
			return graphstore.ErrSourceUnavailable // Lose this actual native reply.
		case <-caller.Done():
			return caller.Err()
		}
	}
	hub.mu.Unlock()
	defer func() { hub.mu.Lock(); hub.afterRelay = nil; hub.mu.Unlock() }()
	spec := graphE15ReadRequest{mode: mode, partitions: []uint64{3}}
	if mode == graphReadAfter {
		spec.transaction, spec.coordinator = tx, 8
	}
	request, err := encodeGraphE15ReadRequest(spec)
	if err != nil {
		t.Fatal(err)
	}
	answers := make(chan graphProcessAnswer, 1)
	readCtx, stopRead := context.WithCancel(ctx)
	defer stopRead()
	go func() {
		r, err := f.n.all[reader].exchange(readCtx, processRequest{op: processSourceCertifiedOpen, data: request})
		answers <- graphProcessAnswer{response: r, err: err}
	}()
	drained := false
	defer func() {
		stopRead()
		unblock()
		if !drained {
			<-answers
		}
	}()
	var actualLookup pairedFaultBoundary
	select {
	case actualLookup = <-lookup:
	case answer := <-answers:
		drained = true
		t.Fatal("A-only read bypassed B authority", answer.err)
	case <-ctx.Done():
		t.Fatal("actual B decision lookup absent", ctx.Err())
	}
	select {
	case answer := <-answers:
		drained = true
		t.Fatal("A-only read published while B authority reply held", answer.err)
	default:
	}
	held := old
	held.Facts = nativeTransitionScopeFacts(t, f, ctx, 2, oldHandle)
	if !bytes.Equal(held.Facts, old.Facts) {
		t.Fatal("held A cut changed before B authority is available")
	}
	unblock()
	failed := <-answers
	drained = true
	mask := graphErrorMask(failed.err)
	if !errors.Is(failed.err, graphstore.ErrSourceUnavailable) || failed.response.code != 3 && failed.response.code != 6 || len(failed.response.aux) != 0 || mask == 0 || mask&^uint16((1<<3)|(1<<10)) != 0 {
		t.Fatal("unavailable B did not refuse without a cut", failed.err, failed.response.code, mask)
	}
	hub.mu.Lock()
	hub.afterRelay = nil
	hub.mu.Unlock()
	result := f.run(5, original)
	if result.resume != resume.resume || result.decision != decision {
		t.Fatal("recovery changed immutable original")
	}
	f.trace.OperationMapping = append(f.trace.OperationMapping, lifecycleOperationMap{Logical: "node-a", Coordinator: 8,
		Request: hex.EncodeToString(command.request.id[:]), Transaction: hex.EncodeToString(tx[:]), Round: decision.round})
	if gate != nil && campaign[0].Phase == crossResolve {
		f.drain()
		native.nativeProtocolBatchEvidence = nativeProtocolFinishBatch(t, f, gate, nativeOld)
	}
	currentHandle, currentCut, recovered := nativeTransitionScopeOpen(t, f, ctx, reader, spec)
	if currentCut.certificates[0].round < decision.round {
		t.Fatal("scoped Fresh/After omitted acknowledged B original")
	}
	nativeTransitionScopeAssert(t, recovered.Facts, true)
	f.call(f.n.all[reader], processRequest{op: processSourceCertifiedClose, index: currentHandle})
	retained := old
	retained.Facts = nativeTransitionScopeFacts(t, f, ctx, 2, oldHandle)
	nativeTransitionScopeAssert(t, retained.Facts, false)
	f.call(f.n.all[2], processRequest{op: processSourceCertifiedClose, index: oldHandle})
	oldClosed = true
	current := p06Observe(f, 6, graphReadFresh, graphCut{})
	retryReply := f.call(f.n.all[5], processRequest{op: processSourceCoordinate, data: original.wire})
	retry, err := decodeGraphProcessCoordinatorResult(retryReply)
	if err != nil || retry != result {
		t.Fatal("same original replay", err)
	}
	through := f.indexes()
	records := p06Records(f, from, through)
	nativeTransitionScopeCDC(t, records, decision)
	processes := f.stopAll()
	processes = append(processes, f.reopenAll()...)
	reopenedHandle, reopenedCut, reopened := nativeTransitionScopeOpen(t, f, ctx, 2, graphE15ReadRequest{mode: graphReadAt, cut: oldCut, partitions: []uint64{3}})
	if !slices.Equal(reopenedCut.certificates, oldCut.certificates) || !bytes.Equal(reopened.Facts, old.Facts) {
		t.Fatal("native same-directory reopen substituted current A")
	}
	f.call(f.n.all[2], processRequest{op: processSourceCertifiedClose, index: reopenedHandle})
	name := "Fresh"
	if mode == graphReadAfter {
		name = "After"
	}
	failure, err := encodeProcessResponse(failed.response)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(f.n.dir, "native-outside-scope-"+name+".jsonl")
	artifact := nativeTransitionScopeArtifact{Version: 1, Profile: nativeTransitionScopeProfile, Mode: name, Original: original.wire,
		Manifest: actualManifest, PendingCaller: p06ProcessWire(t, pending.response), Decision: decisionBoundary, Lookup: actualLookup,
		Failure: failure, FailureMask: mask, Before: before, Current: current, Old: old, Held: held, Recovered: recovered,
		Retained: retained, Reopened: reopened, Retry: p06ProcessWire(t, retryReply), Prepared: prepared, Processes: processes,
		From: from, Through: through}
	if gate != nil {
		native.Recipe, native.Run, native.DeadlineUnixNanos, native.P01 = campaign[0], run, deadline, artifact
		return nativeProtocolScopeWrite(t, ctx, f, native, records)
	}
	nativeTransitionWriteRows(t, path, artifact, records, through)
	return path
}

func nativeTransitionScopeCDC(t *testing.T, records []lifecycleRawRecord, decision crossStatus) {
	t.Helper()
	seen := make(map[uint64]bool)
	for _, record := range records {
		status, err := decodeCrossStatus(record.Outcome)
		if err != nil || status.tx != decision.tx || status.phase != crossResolve || status.index != record.Index {
			continue
		}
		if seen[record.Partition] || !status.commit || status.round != decision.round || status.digest != decision.digest {
			t.Fatal("same original participant/decision CDC")
		}
		seen[record.Partition] = true
		out := outcome{ns: status.ns, kind: graphOperations, identity: status.tx, hash: status.digest, index: status.index, disposition: applied}
		changes, err := decodeTypedChangeEnvelope(record.CDC, out, defaultMaterializerLimits(), true)
		if err != nil {
			t.Fatal("complete original participant CDC", err)
		}
		if record.Partition == 8 {
			if changes.nonempty() {
				t.Fatal("B validation participant invented business effects")
			}
			continue
		}
		if record.Partition != 3 || len(changes.entities) != 1 || len(changes.lives) != 1 || len(changes.values) != 0 || len(changes.groups) != 1 {
			t.Fatal("exact A node creation CDC census")
		}
		entity, life, group := changes.entities[0], changes.lives[0], changes.groups[0]
		if entity.ID != 1 || entity.Kind != graphstate.Node || entity.Source != 0 || entity.Target != 0 || life.Owner != 1 || life.Life != 2 || life.SourceLife != 0 || life.TargetLife != 0 || group.Key != (graphstate.ComponentKey{Owner: 1, Kind: graphstate.Presence}) || len(group.Changes) != 1 {
			t.Fatal("exact A node/life/presence original")
		}
		change := group.Changes[0]
		if change.Before().Present() || change.Before().Revision().ID() != 0 || !change.After().Present() || change.After().Value().ID() != 2 || change.After().Revision().ID() != decision.round || change.After().Revision().Provenance() != 1 {
			t.Fatal("exact original absent/new before-after revision")
		}
		p06CheckScope(t, change.Scope())
	}
	if len(seen) != 2 || !seen[3] || !seen[8] {
		t.Fatal("missing complete genuine participant group")
	}
}
