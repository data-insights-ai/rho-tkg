package graphapply

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

const nativeTransitionMovePrepareMarker = "GTM1"
const nativeTransitionMoveProfile = "native-owned-transfer-transition-v1"

// This input selects one actual child-local factory invocation. It contains no
// intent/proof/manifest bytes and cannot turn a decoded GMR into authority.
type nativeTransitionMovePrepare struct {
	Version              uint8
	Cookie, FamilyCookie [16]byte
	RangeID, Target      uint64
	Pages, Rows, Bytes   int
}

type nativeTransitionPreparedMove struct {
	request nativeTransitionMovePrepare
	wire    []byte
	err     error
	done    bool
}

func (o *nativeLogicalTransitionOwner) prepareMove(caller context.Context, family *nativeFaultFamilyOwner, incarnation [16]byte, r processRequest) ([]byte, error) {
	if o == nil || caller == nil || family == nil || incarnation == ([16]byte{}) || !o.host.machine.rangeMoves() || len(r.data) > 4096 || !bytes.HasPrefix(r.data, []byte(nativeTransitionMovePrepareMarker)) {
		return nil, errInvalid
	}
	if err := caller.Err(); err != nil {
		return nil, err
	}
	var input nativeTransitionMovePrepare
	decoder := json.NewDecoder(bytes.NewReader(r.data[len(nativeTransitionMovePrepareMarker):]))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		return nil, errors.Join(errInvalid, err)
	}
	canonical, err := json.Marshal(input)
	if err != nil || !bytes.Equal(canonical, r.data[len(nativeTransitionMovePrepareMarker):]) || input.Version != 1 || input.Cookie != o.cookie || input.RangeID != 1 || input.Target != 8 || input.Pages < 1 || input.Pages > graphE17AdmittedMovePages || input.Rows != 8 || input.Bytes != 16<<10 || o.host.machine.ns.partition != 3 {
		return nil, errors.Join(errInvalid, err)
	}
	o.mu.Lock()
	if o.controls >= 64 || o.closed || o.ctx.Err() != nil {
		o.mu.Unlock()
		return nil, replica.ErrUnavailable
	}
	o.controls++
	// Only the first actual factory read spends its immutable native read/work
	// grant; every diagnostic reply additionally prepays its bounded owned bytes.
	grant := uint64(8192)
	if o.preparedMove == nil {
		grant += o.prepareGrant
	}
	if err := o.reserveLocked(grant); err != nil {
		o.mu.Unlock()
		return nil, err
	}
	if previous := o.preparedMove; previous != nil {
		if previous.request != input {
			o.mu.Unlock()
			return nil, errInvalid
		}
		if !previous.done {
			o.mu.Unlock()
			return nil, errRangeMovePending
		}
		wire, err := owned(previous.wire), errors.Join(previous.err, caller.Err())
		o.mu.Unlock()
		return wire, err
	}
	family.mu.Lock()
	if family.closed || family.ctx.Err() != nil || family.cookie != input.FamilyCookie || family.move != nil || family.moveBusy || family.moveCalls >= nativeFaultFamilyMoveCalls {
		family.mu.Unlock()
		o.mu.Unlock()
		return nil, errInvalid
	}
	if err := family.arena.Reserve(8192); err != nil {
		family.mu.Unlock()
		o.mu.Unlock()
		return nil, outputBudgetFailure(err)
	}
	family.moveCalls++
	family.moveBusy = true
	stage := &nativeTransitionPreparedMove{request: input}
	o.preparedMove = stage
	family.mu.Unlock()
	o.mu.Unlock()
	ctx, cancel := context.WithCancel(caller)
	stopOwner, stopFamily := context.AfterFunc(o.ctx, cancel), context.AfterFunc(family.ctx, cancel)
	defer func() { stopOwner(); stopFamily(); cancel() }()
	id := requestID(incarnation)
	binary.BigEndian.PutUint64(id[8:], r.sequence)
	var intent rangeMoveIntent
	var actual error
	if rangeMovePoolsEnabled(o.host.machine.SemanticContractID()) {
		// The pooled profile requires explicit finite grants in its ORIGINAL
		// factory value. Legacy zero-pool intents are not valid pooled authority.
		intent, actual = o.host.PreparePooledRangeMove(ctx, id, input.RangeID, input.Target, input.Pages, input.Rows, input.Bytes, rangeMovePools{2 << 20, 8 << 20, 8 << 20})
	} else {
		intent, actual = o.host.PrepareRangeMove(ctx, id, input.RangeID, input.Target, input.Pages, input.Rows, input.Bytes)
	}
	var wire []byte
	if actual == nil {
		var encoded []byte
		encoded, actual = encodeRangeMoveIntent(intent)
		if actual == nil {
			resume := rangeMoveResume{scope: intent.source, request: intent.id, move: rangeMoveID(namespace{intent.source.graph, intent.source.partition}, intent.id), intent: sha256.Sum256(encoded)}
			family.mu.Lock()
			actual = errors.Join(ctx.Err(), family.ctx.Err())
			if actual == nil && !family.closed {
				family.move, family.moveResume = &intent, resume
			} else if actual == nil {
				actual = replica.ErrUnavailable
			}
			family.mu.Unlock()
			if actual == nil {
				wire, actual = encodeGraphE17MoveResult(rangeMoveResult{resume: resume}, errRangeMovePending)
			}
		}
	}
	family.mu.Lock()
	family.moveBusy = false
	family.mu.Unlock()
	o.mu.Lock()
	stage.wire, stage.err, stage.done = wire, actual, true
	o.mu.Unlock()
	return owned(wire), errors.Join(actual, ctx.Err())
}

// Reassociation is permitted only by this opted-in child after its actual
// operation/worker/source lanes drained. Preserve every retained move run and
// original factory value; no decoded recipe or result supplies a new intent.
func nativeTransitionMoveReconnect(ctx context.Context, h *allocationHost, old *crossProcessCoordinator, sessions *graphProcessCertifiedSessions, move *rangeMoveCoordinator, server *graphSourceServer, r processRequest, workers, queued int, active bool, retainRuns ...bool) (*crossProcessCoordinator, error) {
	if move == nil || move.base != old || workers != 0 || queued != 0 || active || len(retainRuns) > 1 {
		return old, errInvalid
	}
	move.mu.Lock()
	for _, run := range move.runs {
		if run.busy {
			move.mu.Unlock()
			return old, errRangeMovePending
		}
	}
	if len(retainRuns) == 1 && retainRuns[0] {
		// NCR1 retains the SAME base, including its certified engine, publisher
		// references and private runs. Hold move admission until all ordinary
		// source/session/RPC gates and route replacements have been validated.
		defer move.mu.Unlock()
		return pairedGraphFaultReconnect(ctx, h, old, sessions, nil, server, r, workers, queued, active, true)
	}
	move.mu.Unlock()
	next, err := pairedGraphFaultReconnect(ctx, h, old, sessions, nil, server, r, workers, queued, active)
	if err != nil {
		return old, err
	}
	move.mu.Lock()
	move.base = next
	move.mu.Unlock()
	return next, nil
}

type nativeTransitionMoveRecipe struct {
	Version         uint8
	Phase           byte
	Partition, Page uint64
	Side            string
}

func (r nativeTransitionMoveRecipe) valid() bool {
	if r.Version != 1 || r.Side != "before-submit" && r.Side != "after-applied" || r.Partition != 3 && r.Partition != 8 {
		return false
	}
	switch rangeMovePhase(r.Phase) {
	case rangeMoveFence, rangeMoveCapture, rangeMoveActivate, rangeMoveFinish, rangeMoveAbort:
		return r.Page == 0
	case rangeMoveDescribe:
		return r.Partition == 3 && r.Page <= 1
	case rangeMoveImport:
		return r.Partition == 8 && (r.Page == 0 || r.Page == 1 || r.Page == 3)
	case rangeMoveSeal:
		return r.Partition == 8 && r.Page == 0
	case rangeMoveDecide:
		return r.Partition == 3 && r.Page == 0
	default:
		return false
	}
}

type nativeMoveTransitionArtifact struct {
	Version                               uint8
	Profile                               string
	Recipe                                nativeTransitionMoveRecipe
	Gate                                  nativeLogicalTransitionReceipt
	Run                                   [16]byte
	DeadlineUnixNanos                     int64
	FactoryCookie                         [16]byte
	PrepareRequest, PrepareResponse       []byte
	Intent                                []byte
	Pending, Complete, Replay             []byte
	Before, Retained, Recovered, Reopened nativeMoveTransitionRead
	PlacementBefore                       []byte
	PlacementAfter                        [][]byte
	Processes                             []pairedFaultProcess
	From, Through                         [2]uint64
	Records                               []lifecycleRawRecord
}

type nativeMoveTransitionRead struct {
	nativeFaultFamilyRangeRead
	Census [][]byte
}

func nativeTransitionMoveRead(t *testing.T, ctx context.Context, n *certifiedProcessNetwork, actor uint64, mode graphReadMode, cut graphCut) nativeMoveTransitionRead {
	return nativeTransitionMoveReadFor(t, ctx, n, actor, mode, cut, rangeMoveSemanticContractID())
}

func nativeTransitionMoveReadFor(t *testing.T, ctx context.Context, n *certifiedProcessNetwork, actor uint64, mode graphReadMode, cut graphCut, semantic raftlog.ApplicationSemanticContractID) nativeMoveTransitionRead {
	t.Helper()
	n.e17PublicUsed++
	if n.e17PublicUsed > n.e17PublicPhases {
		t.Fatal("admitted native read phase")
	}
	call := func(r processRequest) processResponse {
		response, actual := n.all[actor].exchange(ctx, r)
		if err := errors.Join(actual, processResponseError(response)); err != nil {
			t.Fatal("same-deadline native transfer read", err)
		}
		return response
	}
	request, err := encodeGraphE15ReadRequest(graphE15ReadRequest{mode: mode, cut: cut})
	if err != nil {
		t.Fatal(err)
	}
	opened := call(processRequest{op: processSourceCertifiedOpen, data: request})
	handle, actual, err := decodeGraphE15ReadToken(opened.aux)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		response, actual := n.all[actor].exchange(cleanup, processRequest{op: processSourceCertifiedClose, index: handle})
		if err := errors.Join(actual, processResponseError(response)); err != nil {
			t.Error("same owned read cleanup", err)
		}
	}()
	for _, certificate := range actual.certificates {
		if certificate.scope.semantic != semantic {
			t.Fatal("foreign range read profile")
		}
	}
	read := nativeMoveTransitionRead{}
	read.Cut, err = lifecycleRawCertificate(actual)
	if err != nil {
		t.Fatal(err)
	}
	for _, ids := range [][]uint64{{1, 3, 17}, {7}} {
		wire, err := encodeGraphProcessObserveIDs(ids)
		if err != nil {
			t.Fatal(err)
		}
		response := call(processRequest{op: processRangeCertifiedDetail, index: handle, data: wire})
		facts, err := decodeGraphE17Facts(response.aux)
		if err != nil || len(facts) != len(ids) {
			t.Fatal("complete native typed read", err)
		}
		for i, fact := range facts {
			if fact.id != ids[i] {
				t.Fatal("actual typed read ID order")
			}
		}
		if len(response.aux) > 64<<10 {
			t.Fatal("bounded owned typed detail")
		}
		owned := make([]byte, len(response.aux))
		copy(owned, response.aux)
		read.Details = append(read.Details, owned)
	}
	// Same retained view, ordinary three-ID batch cap, complete declared ID
	// domain. A copied range must not manufacture an unobserved phantom entity.
	bytesOwned := 0
	for first := uint64(1); first <= 32; first += graphProcessObserveMaxIDs {
		var ids []uint64
		for id := first; id <= 32 && len(ids) < graphProcessObserveMaxIDs; id++ {
			ids = append(ids, id)
		}
		wire, err := encodeGraphProcessObserveIDs(ids)
		if err != nil {
			t.Fatal(err)
		}
		response := call(processRequest{op: processSourceCertifiedObserve, index: handle, data: wire})
		facts, err := decodeGraphProcessObservationBatch(response.aux)
		if err != nil || len(facts) != len(ids) || len(response.aux) > (64<<10)-bytesOwned {
			t.Fatal("bounded complete native entity census", err)
		}
		bytesOwned += len(response.aux)
		for i, tuple := range facts {
			if tuple.id != ids[i] || tuple.fact.Exists != (tuple.id == 1 || tuple.id == 3 || tuple.id == 17) {
				t.Fatal("transfer entity census omitted or invented an ID")
			}
		}
		owned := make([]byte, len(response.aux))
		copy(owned, response.aux)
		read.Census = append(read.Census, owned)
	}
	return read
}

func nativeTransitionMoveCase(t *testing.T, recipe nativeTransitionMoveRecipe) string {
	t.Helper()
	if !recipe.valid() {
		t.Fatal("finite actual move phase recipe")
	}
	var run [16]byte
	if raw := os.Getenv("RHO_GRAPH_TRANSITION_RUN"); raw != "" {
		wire, err := hex.DecodeString(raw)
		if err != nil || len(wire) != 16 || hex.EncodeToString(wire) != raw {
			t.Fatal("original run identity", err)
		}
		copy(run[:], wire)
	} else if _, err := rand.Read(run[:]); err != nil {
		t.Fatal(err)
	}
	if run == ([16]byte{}) {
		t.Fatal("zero transition run")
	}
	deadline := time.Now().Add(3 * time.Minute).UnixNano()
	if raw := os.Getenv("RHO_GRAPH_TRANSITION_DEADLINE"); raw != "" {
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || strconv.FormatInt(value, 10) != raw || value <= time.Now().UnixNano() || value-time.Now().UnixNano() > int64(1800*time.Second) {
			t.Fatal("original whole-run deadline", err)
		}
		deadline = value
	}
	runCtx, stopRun := context.WithDeadline(t.Context(), time.Unix(0, deadline))
	defer stopRun()
	t.Setenv("RHO_GRAPH_TRANSITION_RUN", hex.EncodeToString(run[:]))
	t.Setenv("RHO_GRAPH_TRANSITION_DEADLINE", strconv.FormatInt(deadline, 10))
	t.Setenv("RHO_GRAPH_TRANSITION_PROCESS", "1")
	t.Setenv("RHO_GRAPH_FAULT_FAMILY_PROCESS", "1")
	n := newCertifiedProcessNetworkProfile(t, true)
	f := &pairedGraphFaultNetwork{t: t, n: n, down: make(map[uint64]bool), intentionalKills: make(map[*graphProcess]bool)}
	f.installFilter()
	graphE17SubmitLocal(t, n, 1, graphE17Prepare(t, n, 1, certifiedRangeSet, 1, graphE17GrantInput(5, 7)), reasonNone)
	graphE17Cross(t, n, 4, graphE17Prepare(t, n, 4, certifiedRangeSet, 17, graphE17GrantInput(19, 13)))
	graphE17Cross(t, n, 1, graphE17Prepare(t, n, 1, certifiedCreateCrossEdge, 0, nil))
	before := nativeTransitionMoveRead(t, runCtx, n, 3, graphReadFresh, graphCut{})
	cut, err := pairedFaultCut(before.Cut)
	if err != nil {
		t.Fatal(err)
	}
	placement := n.all[1].call(t, processRequest{op: processRangeEvidenceProbe, entity: 1}).aux
	factory := nativeFaultFamilyCall(t, n.all[2], nativeFaultFamilyControl{Version: 1, Action: "inspect"})
	owner := nativeLogicalTransitionCall(t, n.all[2], nativeLogicalTransitionControl{Version: 1, Action: "inspect"})
	pages := graphE17AdmittedMovePages
	if rangeMovePhase(recipe.Phase) == rangeMoveAbort {
		// A real caller-declared one-page intent cannot complete this nonempty
		// staged transfer after preparation. Require native ABORT/undo; ordinary
		// storage, Run, queue and invocation capacities are unchanged.
		pages = 1
	}
	input := nativeTransitionMovePrepare{Version: 1, Cookie: owner.Cookie, FamilyCookie: factory.Cookie, RangeID: 1, Target: 8, Pages: pages, Rows: 8, Bytes: 16 << 10}
	request, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	request = append([]byte(nativeTransitionMovePrepareMarker), request...)
	prepared := n.all[2].call(t, processRequest{op: processNativeTransitionControl, data: request})
	original, err := decodeGraphE17MoveResult(prepared)
	if !errors.Is(err, errRangeMovePending) || original.resume.request == (requestID{}) || original.source != (rangeMoveStatus{}) || original.target != (rangeMoveStatus{}) || original.decision != (rangeMoveStatus{}) {
		t.Fatal("actual typed factory acquired decision authority", err)
	}
	duplicate := n.all[2].call(t, processRequest{op: processNativeTransitionControl, data: request})
	if !bytes.Equal(duplicate.aux, prepared.aux) {
		t.Fatal("lost factory reply replaced original")
	}
	if recipe.Phase == 1 && recipe.Partition == 3 && recipe.Side == "before-submit" {
		beforeRefusal := n.all[3].call(t, processRequest{op: processStateOp}).applied
		refused, actual := n.all[3].exchange(t.Context(), processRequest{op: processRangeResume, data: prepared.aux})
		if !errors.Is(actual, errInvalid) || len(refused.aux) != 0 || n.all[3].call(t, processRequest{op: processStateOp}).applied != beforeRefusal {
			t.Fatal("decoded pre-fence GMR recreated absent original factory", actual)
		}
		foreign := input
		foreign.FamilyCookie[0] ^= 1
		body, err := json.Marshal(foreign)
		if err != nil {
			t.Fatal(err)
		}
		refused, actual = n.all[2].exchange(t.Context(), processRequest{op: processNativeTransitionControl, data: append([]byte(nativeTransitionMovePrepareMarker), body...)})
		if !errors.Is(actual, errInvalid) || len(refused.aux) != 0 {
			t.Fatal("foreign factory owner replaced live original", actual)
		}
	}
	member, successor := uint64(1), uint64(3)
	if recipe.Partition == 8 {
		member, successor = 4, 5
	}
	selected := nativeLogicalTransitionCall(t, n.all[member], nativeLogicalTransitionControl{Version: 1, Action: "inspect"})
	arm := nativeLogicalTransitionControl{Version: 1, Action: "arm", Cookie: selected.Cookie, Sequence: 1, Family: "move", Phase: recipe.Phase, Side: recipe.Side, Identity: original.resume.move, Partition: recipe.Partition, Page: recipe.Page}
	nativeLogicalTransitionCall(t, n.all[member], arm)
	from := f.indexes()
	ctx, cancel := context.WithTimeout(runCtx, 20*time.Second)
	defer cancel()
	done := make(chan graphProcessAnswer, 1)
	go func() {
		response, actual := n.all[2].exchange(ctx, processRequest{op: processRangeResume, data: prepared.aux})
		done <- graphProcessAnswer{response: response, err: actual}
	}()
	gate := nativeLogicalTransitionCall(t, n.all[member], nativeLogicalTransitionControl{Version: 1, Action: "await", Cookie: selected.Cookie, Sequence: 1})
	command, err := decodeRangeMoveCommand(gate.Proposal)
	if err != nil || gate.State != "reached" || gate.Control != arm || command.phase != rangeMovePhase(recipe.Phase) || command.sequence != recipe.Page || sha256.Sum256(command.intent) != original.resume.intent || gate.ProposalSHA256 != sha256.Sum256(gate.Proposal) {
		t.Fatal("missing actual original-bound move gate", err)
	}
	intent, err := decodeRangeMoveIntent(command.intent)
	if err != nil || intent.id != original.resume.request || intent.maxPages != pages {
		t.Fatal("factory intent changed", err)
	}
	killed := f.kill(member, "range-owner")
	cancel()
	var pending processResponse
	select {
	case answer := <-done:
		pending = answer.response
		result, actual := decodeGraphE17MoveResult(pending)
		if !errors.Is(actual, errRangeMovePending) || result.resume != original.resume || result.committed {
			t.Fatal("interrupted original became terminal/replaced", actual)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("move worker not retired after actual owner loss")
	}
	f.elect(member, successor)
	p03Reconnect(f, 2)
	attempt := graphE17MoveExchangeContext(t, runCtx, n.all[2], processRequest{op: processRangeResume, data: pending.aux})
	completed, actual := decodeGraphE17MoveResult(attempt.response)
	for calls := 1; calls < graphE17AdmittedMovePages && errors.Is(actual, errRangeMovePending); calls++ {
		if !graphE17PermittedResume(actual, attempt.interruption) {
			t.Fatal("unclassified move continuation", actual)
		}
		attempt = graphE17MoveExchangeContext(t, runCtx, n.all[2], processRequest{op: processRangeResume, data: attempt.response.aux})
		completed, actual = decodeGraphE17MoveResult(attempt.response)
		if completed.resume != original.resume {
			t.Fatal("continuation replaced original")
		}
	}
	aborted := rangeMovePhase(recipe.Phase) == rangeMoveAbort
	if completed.resume != original.resume || aborted && (!errors.Is(actual, errLimit) || completed.committed || completed.source.phase != rangeMoveAbort || completed.target.phase != rangeMoveAbort) || !aborted && (actual != nil || !completed.committed || completed.source.phase != rangeMoveFinish || completed.target.phase != rangeMoveFinish) {
		t.Fatal("actual original handoff terminal", actual)
	}
	replayResponse, transport := n.all[2].exchange(runCtx, processRequest{op: processRangeResume, data: attempt.response.aux})
	replayed, replayErr := decodeGraphE17MoveResult(replayResponse)
	if replayed != completed || aborted && !errors.Is(replayErr, errLimit) || !aborted && replayErr != nil || transport != nil && (!aborted || !errors.Is(transport, errLimit)) {
		t.Fatal("terminal original replay changed", transport, replayErr)
	}
	recovered := nativeTransitionMoveRead(t, runCtx, n, 2, graphReadFresh, graphCut{})
	retained := nativeTransitionMoveRead(t, runCtx, n, 2, graphReadAt, cut)
	prior := n.all[member]
	var reopened nativeMoveTransitionRead
	rejoined := f.replaceBeforeHeal(member, func(_ *graphProcess, _ processResponse) {
		reopened = nativeTransitionMoveRead(t, runCtx, n, member, graphReadAt, cut)
	})
	n.retired = append(n.retired, prior)
	if !reflect.DeepEqual(before, retained) || !reflect.DeepEqual(before, reopened) {
		t.Fatal("transfer/restart substituted current for original cut")
	}
	var placements [][]byte
	for actor := uint64(1); actor <= 6; actor++ {
		placements = append(placements, n.all[actor].call(t, processRequest{op: processRangeEvidenceProbe, entity: 1}).aux)
	}
	through := f.indexes()
	if err := runCtx.Err(); err != nil {
		t.Fatal("whole-run transfer deadline", err)
	}
	artifact := nativeMoveTransitionArtifact{Version: 1, Profile: nativeTransitionMoveProfile, Recipe: recipe, Gate: gate, Run: run, DeadlineUnixNanos: deadline, FactoryCookie: factory.Cookie, PrepareRequest: request, PrepareResponse: p06ProcessWire(t, prepared), Intent: command.intent, Pending: p06ProcessWire(t, pending), Complete: p06ProcessWire(t, attempt.response), Replay: p06ProcessWire(t, replayResponse), Before: before, Retained: retained, Recovered: recovered, Reopened: reopened, PlacementBefore: placement, PlacementAfter: placements, Processes: []pairedFaultProcess{killed, rejoined}, From: from, Through: through, Records: p06Records(f, from, through)}
	header := artifact
	header.Records = nil
	path := filepath.Join(n.dir, fmt.Sprintf("native-transition-move-%d-p%d-page%d-%s.jsonl", recipe.Phase, recipe.Partition, recipe.Page, recipe.Side))
	return nativeTransitionWriteRows(t, path, header, artifact.Records, through)
}
