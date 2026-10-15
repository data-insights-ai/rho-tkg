package graphapply

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

func TestNativeComposedFaultPartialInstallSurvivesDecisionLeaderLossAndDelayedResolve(t *testing.T) {
	nativeFaultPartialCase(t)
}

func nativeFaultPartialCase(t *testing.T, campaign ...nativeProtocolPartialRecipe) {
	nativeFaultPartialCaseOwned(t, nil, campaign...)
}

func nativeFaultPartialCaseOwned(t *testing.T, programSpec *nativeProtocolProgramSpec, campaign ...nativeProtocolPartialRecipe) {
	if len(campaign) > 1 || len(campaign) == 1 && !campaign[0].valid() {
		t.Fatal("invalid fresh P03 partial-install occurrence")
	}
	t.Setenv("RHO_GRAPH_NATIVE_FAULT_PROCESS", "1")
	var f *pairedGraphFaultNetwork
	ctx := t.Context()
	var run [16]byte
	var deadline int64
	var artifactBudget *graphstate.OutputBudget
	if len(campaign) == 0 {
		f = newPairedGraphFaultNetwork(t)
	} else {
		artifactBudget = nativeProtocolArtifactAllowance(t)
		run, deadline = nativeP06ConjunctionIdentity(t)
		t.Setenv("RHO_GRAPH_STABLE_STORAGE_PROCESS", "1")
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, time.Unix(0, deadline))
		defer cancel()
		f = newPairedGraphFaultNetworkWithParent(t, ctx)
	}
	var program *nativeProtocolProgram
	leader, successor, recoveryActor := uint64(4), uint64(5), uint64(6)
	if programSpec != nil {
		if len(campaign) != 1 {
			t.Fatal("one genuine partial-install recipe owns its program")
		}
		program = nativeProtocolProgramStart(t, ctx, f, *programSpec)
		if campaign[0].Phase == crossResolve && campaign[0].Partition == 8 && campaign[0].Kind == raftlog.StableStorageReady && program.spec.Message != "literal" {
			t.Fatal("selected serving B leader dies before its Ready emits native frames")
		}
		leader = f.n.leaders[1]
		if leader == 6 {
			recoveryActor = 4
		}
	}
	before := f.observe(4, graphReadFresh, graphCut{}, false)
	for stage, name := range []string{"node-a", "node-b"} {
		proposal := f.proposal(stage + 1)
		from := f.indexes()
		actor := uint64(1)
		if stage == 1 {
			actor = 4
		}
		result := f.run(actor, proposal)
		step := f.complete(name, proposal, result, from, before)
		f.assertView(step.After, stage+1)
		before = pairedFaultObserver(step)
	}
	old := p06Observe(f, 1, graphReadFresh, graphCut{})
	oldCut, err := pairedFaultCut(old.Cut)
	if err != nil {
		t.Fatal(err)
	}
	held, _ := f.open(1, graphReadAt, oldCut)
	heldClosed := false
	defer func() {
		if !heldClosed {
			if len(campaign) == 0 {
				f.n.all[1].call(t, processRequest{op: processSourceCertifiedClose, index: held})
			} else {
				f.cleanupCertifiedHandle(f.n.all[1], &held)
			}
		}
	}()
	readHeld := func() p06Observation {
		raw := f.lifecycleRead(1, held, 0)
		raw.Census = f.lifecycleRead(1, held, 2).Census
		view, err := lifecycleNormalizeView(raw, f.trace.IdentityMapping, f.trace.OperationMapping)
		if err != nil {
			t.Fatal(err)
		}
		return p06Observation{Cut: old.Cut, View: view, Native: raw}
	}
	first := readHeld()
	proposal := f.proposal(3)
	from := f.indexes()
	actor, reader := uint64(5), uint64(6)
	if len(campaign) == 1 {
		actor, reader = recoveryActor, 2
	}
	gate, err := newNativeFaultPartialGate(ctx, f.n.all[actor].mux.hub, actor)
	if err != nil {
		t.Fatal(err)
	}
	defer gate.release()
	var nativeGate *nativeConjunctionBatchGate
	var native nativeProtocolBatchEvidence
	var outage *nativeP06ConjunctionAttempt
	var firstRejoined pairedFaultProcess
	if len(campaign) == 1 && !(campaign[0].Phase == crossResolve && campaign[0].Partition == 8) {
		nativeGate, err = newNativeConjunctionBatchGate(ctx, f, campaign[0].selector(actor), run, deadline)
		if err != nil {
			t.Fatal(err)
		}
		defer nativeGate.close()
	}
	call, cancel := f.beginRun(actor, proposal)
	defer cancel()
	_ = f.awaitBoundary(gate.entered)
	gate.mu.Lock()
	installed, withheld := gate.first, gate.second
	gate.mu.Unlock()
	resolved, manifest := nativeFaultInstalledResolve(t, proposal, installed)
	f.packetDrain()
	if nativeGate != nil {
		native = nativeProtocolFinishBatch(t, f, nativeGate, oldCut, true)
	}
	// Inspect every real A voter before certification or coordinator loss.
	for id := uint64(1); id <= 3; id++ {
		record := f.call(f.n.all[id], processRequest{op: processEntryRecord, index: resolved.index})
		status, err := decodeCrossStatus(record.outcome)
		if err != nil || status != resolved || len(record.changes) == 0 {
			t.Fatal("A was not actually installed", id, err)
		}
	}
	read := nativeFaultPartialReadCall
	if len(campaign) == 1 {
		read = nativeProtocolPartialRead
	}
	reads := []nativeFaultPartialRead{read(t, f, reader, graphReadFresh, [16]byte{}, 0), read(t, f, reader, graphReadAfter, manifest.tx, manifest.coordinator)}
	retained := readHeld()
	if !reflect.DeepEqual(first, retained) || !reflect.DeepEqual(old, retained) {
		t.Fatal("partial installation substituted current state at held old cut")
	}
	f.call(f.n.all[1], processRequest{op: processSourceCertifiedClose, index: held})
	heldClosed = true
	killed := f.kill(leader, "replica")
	cancel()
	var answer graphProcessAnswer
	if len(campaign) == 1 {
		answer = nativeP06AwaitAnswer(f, call)
	} else {
		select {
		case answer = <-call:
		case <-time.After(10 * time.Second):
			t.Fatal("partial original worker not retired")
		}
	}
	pending, pendingErr := decodeGraphProcessCoordinatorResult(answer.response)
	if !errors.Is(answer.err, context.Canceled) || !errors.Is(pendingErr, errCrossProcessPending) || pending.resume.tx != manifest.tx || pending.decision != (crossStatus{}) {
		t.Fatal("original partial-install caller classified as completed", answer.err, pendingErr)
	}
	pendingWire := p06ProcessWire(t, answer.response)
	gate.release()
	f.elect(leader, successor)
	if program != nil && campaign[0].Phase == crossResolve && campaign[0].Partition == 8 {
		if _, err := program.successor(leader, successor); err != nil {
			t.Fatal("actual B successor before selected recovery Resolve", err)
		}
	}
	var result crossProcessResult
	var completed []byte
	if len(campaign) == 0 {
		p03Reconnect(f, 6)
		result = f.run(6, proposal)
	} else {
		nativeConjunctionReconnect(f, recoveryActor)
		if campaign[0].Phase == crossResolve && campaign[0].Partition == 8 {
			nativeGate, err = newNativeConjunctionBatchGate(ctx, f, campaign[0].selector(recoveryActor), run, deadline)
			if err != nil {
				t.Fatal(err)
			}
			defer nativeGate.close()
			unknownCall, stopUnknown := f.beginRun(recoveryActor, proposal)
			defer stopUnknown()
			unknown := nativeP06AwaitAnswer(f, unknownCall)
			actual, actualErr := decodeGraphProcessCoordinatorResult(unknown.response)
			if !nativeP06UnknownTransport(unknown.err) || !errors.Is(actualErr, errCrossProcessPending) || actual.resume != pending.resume || actual.decision != (crossStatus{}) {
				t.Fatal("partial B second-voter loss must be actual SAME-original pending", unknown.err, actualErr)
			}
			outage = &nativeP06ConjunctionAttempt{Reply: p06ProcessWire(t, unknown.response), Transport: unknown.err.Error(), Pending: true}
			native, firstRejoined = nativeProtocolFinishRecoveryBatch(t, f, nativeGate, oldCut, leader)
			nativeConjunctionReconnect(f, recoveryActor)
		}
		response := f.call(f.n.all[recoveryActor], processRequest{op: processSourceCoordinate, data: proposal.wire})
		result, err = decodeGraphProcessCoordinatorResult(response)
		if err != nil || result.resume != pending.resume {
			t.Fatal("partial SAME-original recovery changed identity", err)
		}
		completed = p06ProcessWire(t, response)
		f.drain()
	}
	if !result.decision.commit || result.decision.tx != resolved.tx || result.decision.digest != resolved.digest || result.decision.round != resolved.round {
		t.Fatal("recovery changed actual durable decision", result.decision, resolved)
	}
	step := p03Complete(f, recoveryActor, "edge-create", proposal, result, from, before)
	f.assertView(step.After, 3)
	pairedFaultAssertCDC(t, step, 3)
	assertLifecycleCandidates(t, step.RawObservation, 3)
	recovered := p06Observe(f, recoveryActor, graphReadFresh, graphCut{})
	rejoined := firstRejoined
	if rejoined.Member == 0 {
		rejoined = f.replace(leader)
	}
	// The delayed genuine original A phase is replayed after recovery/rejoin.
	// Its earlier original index survives; the new retained entry has empty CDC.
	delayed := p06ReplayPhase(f, p06SourceProposal(t, installed.Request), manifest.tx)
	if delayed.Partition != 3 || len(delayed.CDC) != 0 {
		t.Fatal("delayed Resolve duplicated business")
	}
	if len(campaign) == 0 {
		p03Reconnect(f, 1)
	} else {
		nativeConjunctionReconnect(f, 1)
	}
	reopened := p06Observe(f, 1, graphReadAt, oldCut)
	if !reflect.DeepEqual(reopened, old) {
		t.Fatal("new genuine old-cut read changed after process recovery")
	}
	through := f.indexes()
	actualCommand, _, err := nativeFaultPhase(installed.Request)
	if err != nil {
		t.Fatal(err)
	}
	artifact := nativeFaultArtifact{Version: 1, Profile: nativeFaultProfile, Case: "partial-install-decision-loss", Original: proposal.wire, Manifest: actualCommand.manifest,
		PendingCaller: pendingWire, InstalledA: installed, WithheldB: withheld, PartialReads: reads, Before: old, Retained: retained, Recovered: recovered, Reopened: reopened,
		Processes: []pairedFaultProcess{killed, rejoined}, From: from, Through: through, Records: p06Records(f, from, through)}
	if len(campaign) == 0 {
		writeNativeFaultArtifact(t, f.n.dir, artifact)
	} else {
		nativeProtocolPartialWrite(t, ctx, f, campaign[0], run, deadline, native, artifact, completed, outage, artifactBudget)
	}
}

// Compact direct FS/owner guards only; these do not provide graph/process,
// power-loss or capacity qualification. Execution follows full implementation.
func TestNativeFaultChildFilesystemRecordsOnlyActualMatchedOperations(t *testing.T) {
	for _, kind := range []string{"short-write", "write-eio", "write-enospc", "file-sync", "directory-sync"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			owner, err := newNativeFaultFS(dir, 1)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "000001.log")
			file, err := owner.Create(path, vfs.WriteCategoryUnspecified)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := file.Close(); err != nil {
					t.Error(err)
				}
			})
			inspect := func() nativeFaultReceipt {
				wire, err := json.Marshal(nativeFaultControl{Version: 1, Action: "inspect"})
				if err != nil {
					t.Fatal(err)
				}
				raw, err := owner.control(wire)
				if err != nil {
					t.Fatal(err)
				}
				var receipt nativeFaultReceipt
				if err := json.Unmarshal(raw, &receipt); err != nil {
					t.Fatal(err)
				}
				return receipt
			}
			before := inspect()
			arm := nativeFaultControl{Version: 1, Action: "arm", Cookie: before.Cookie, Sequence: 1, Kind: kind, Nth: 1}
			wire, err := json.Marshal(arm)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := owner.control(wire); err != nil {
				t.Fatal(err)
			}
			// An unmatched genuine file operation cannot qualify the injection.
			other, err := owner.Create(filepath.Join(dir, "other.bin"), vfs.WriteCategoryUnspecified)
			if err != nil {
				t.Fatal(err)
			}
			_, writeErr := other.Write([]byte("unmatched"))
			if err := errors.Join(writeErr, other.Close()); err != nil {
				t.Fatal(err)
			}
			if receipt := inspect(); receipt.Matched || !receipt.Armed || receipt.Seen != 0 {
				t.Fatal("unmatched qualified", receipt)
			}
			var actual error
			if kind == "directory-sync" {
				directory, err := owner.OpenDir(dir)
				if err != nil {
					t.Fatal(err)
				}
				actual = directory.Sync()
				if err := directory.Close(); err != nil {
					t.Fatal(err)
				}
			} else if kind == "file-sync" {
				actual = file.SyncData()
			} else {
				_, actual = file.Write([]byte("abc"))
			}
			want := error(syscall.EIO)
			if kind == "short-write" {
				want = io.ErrShortWrite
			}
			if kind == "write-enospc" {
				want = syscall.ENOSPC
			}
			if !errors.Is(actual, want) {
				t.Fatal(actual, want)
			}
			after := inspect()
			if !after.Matched || after.Armed || after.Sequence != 1 || after.Seen != 1 || after.IO.Error != want.Error() {
				t.Fatal("not actual matched injection", after)
			}
			persisted, err := os.ReadFile(nativeFaultIOPath(dir, after.Cookie, 1))
			if err != nil {
				t.Fatal(err)
			}
			body, err := json.Marshal(after)
			if err != nil || !bytes.Equal(persisted, append(body, '\n')) {
				t.Fatal("actual receipt changed", err)
			}
			if kind == "short-write" {
				actual, err := os.ReadFile(path)
				if err != nil || !bytes.Equal(actual, []byte("ab")) || after.IO.Requested != 3 || after.IO.Written != 2 {
					t.Fatal("not actual short prefix", actual, err, after)
				}
			}
			if _, err := owner.control(wire); err != nil || inspect().Armed {
				t.Fatal("last arm replay reinjected", err)
			}
			arm.Cookie[0] ^= 1
			bad, err := json.Marshal(arm)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := owner.control(bad); !errors.Is(err, errInvalid) {
				t.Fatal("foreign owner accepted", err)
			}
			if kind == "short-write" {
				if owner.Unwrap() != vfs.Default {
					t.Fatal("lost real filesystem")
				}
				opened, err := owner.Open(filepath.Join(dir, "other.bin"))
				if err != nil {
					t.Fatal(err)
				}
				if err := opened.Close(); err != nil {
					t.Fatal(err)
				}
				rw, err := owner.OpenReadWrite(filepath.Join(dir, "other.bin"), vfs.WriteCategoryUnspecified)
				if err != nil {
					t.Fatal(err)
				}
				_, writeErr := rw.WriteAt([]byte("x"), 0)
				_, syncErr := rw.SyncTo(1)
				if err := errors.Join(writeErr, syncErr, rw.Sync(), rw.SyncData(), rw.Close()); err != nil {
					t.Fatal(err)
				}
				reused, err := owner.ReuseForWrite(filepath.Join(dir, "other.bin"), filepath.Join(dir, "reused.log"), vfs.WriteCategoryUnspecified)
				if err != nil {
					t.Fatal(err)
				}
				if err := reused.Close(); err != nil {
					t.Fatal(err)
				}
				disarm, err := json.Marshal(nativeFaultControl{Version: 1, Action: "disarm", Cookie: before.Cookie, Sequence: 1})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := owner.control(disarm); err != nil {
					t.Fatal(err)
				}
				if _, err := owner.control(nil); !errors.Is(err, errInvalid) {
					t.Fatal("nil control accepted", err)
				}
			}
		})
	}
}

func TestNativeFaultHeldAndDuplicatePacketsStayWithinOriginalOwnerBudget(t *testing.T) {
	for _, action := range []string{"duplicate", "reorder"} {
		t.Run(action, func(t *testing.T) {
			h := &graphProcessHub{packets: make(chan graphProcessPacketJob, processPacketCount), idle: make(chan struct{}, 1)}
			s, err := newNativeFaultPacketScheduler(t.Context(), action, 1, 2, 1, 0)
			if err != nil {
				t.Fatal(err)
			}
			close, err := attachNativeFaultPacketScheduler(h, s)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if _, err := close(); err != nil {
					t.Error(err)
				}
			}()
			original := []byte("actual-private-packet")
			payload := make([]byte, len(original), 64)
			copy(payload, original)
			job := graphProcessPacketJob{origin: 1, packet: replica.Packet{From: 1, To: 2, Payload: payload}}
			jobs, count, err := s.route(h, job)
			charge := cap(payload) + 64
			if action == "duplicate" {
				charge = len(payload) + 64
			}
			if err != nil || h.scheduledPackets != 1 || h.packetBytes != charge {
				t.Fatal("uncharged ownership", count, err, h.packetBytes)
			}
			if action == "reorder" {
				if count != 0 {
					t.Fatal("not held", count)
				}
				second := job
				second.packet.Payload = []byte("later-packet")
				jobs, count, err = s.route(h, second)
				if err != nil || count != 2 || !bytes.Equal(jobs[0].packet.Payload, second.packet.Payload) || !bytes.Equal(jobs[1].packet.Payload, payload) {
					t.Fatal("not actual reorder", count, err)
				}
			} else if count != 2 || !bytes.Equal(jobs[0].packet.Payload, jobs[1].packet.Payload) || cap(jobs[1].packet.Payload) != len(payload) {
				t.Fatal("not exact duplicate", count)
			}
			s.done(h)
			if h.scheduledPackets != 0 || h.packetBytes != 0 {
				t.Fatal("packet charge leak")
			}
		})
	}
	ctx, cancel := context.WithCancel(t.Context())
	h := &graphProcessHub{packets: make(chan graphProcessPacketJob, processPacketCount), idle: make(chan struct{}, 1)}
	s, err := newNativeFaultPacketScheduler(ctx, "reorder", 1, 2, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	close, err := attachNativeFaultPacketScheduler(h, s)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = s.route(h, graphProcessPacketJob{origin: 1, packet: replica.Packet{From: 1, To: 2, Payload: []byte("held")}})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if _, err := close(); !errors.Is(err, replica.ErrUnavailable) || h.packetBytes != 0 || h.scheduledPackets != 0 {
		t.Fatal("canceled held packet certified or leaked", err)
	}
	if _, err := close(); !errors.Is(err, replica.ErrUnavailable) {
		t.Fatal("repeated close forgot canceled held traffic", err)
	}
}

func nativeFaultSeed(t *testing.T, f *pairedGraphFaultNetwork) pairedFaultObservation {
	t.Helper()
	before := f.observe(4, graphReadFresh, graphCut{}, false)
	for stage, name := range []string{"node-a", "node-b"} {
		proposal, from := f.proposal(stage+1), f.indexes()
		actor := uint64(1)
		if stage == 1 {
			actor = 4
		}
		step := f.complete(name, proposal, f.run(actor, proposal), from, before)
		f.assertView(step.After, stage+1)
		before = pairedFaultObserver(step)
	}
	return before
}

func TestNativeComposedFaultActualWALFailureRecoversOriginalDecision(t *testing.T) {
	for _, kind := range []string{"short-write", "write-eio", "write-enospc", "file-sync"} {
		t.Run(kind, func(t *testing.T) {
			t.Setenv("RHO_GRAPH_NATIVE_FAULT_PROCESS", "1")
			f := newPairedGraphFaultNetwork(t)
			before := nativeFaultSeed(t, f)
			old := p06Observe(f, 6, graphReadFresh, graphCut{})
			oldCut, err := pairedFaultCut(old.Cut)
			if err != nil {
				t.Fatal(err)
			}
			proposal, from := f.proposal(3), f.indexes()
			entered, release, err := nativeFaultBeforePhase(t.Context(), f.n.all[5].mux.hub, 5, 3, crossResolve)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			call, cancel := f.beginRun(5, proposal)
			defer cancel()
			boundary := f.awaitBoundary(entered)
			command, _, err := nativeFaultPhase(boundary.Request)
			if err != nil || command.phase != crossResolve {
				t.Fatal("actual installation boundary", err)
			}
			// The decision precedes the arm. A storage failure cannot fabricate
			// an ABORT or erase the already quorum-retained original COMMIT.
			arena, err := graphstate.NewOutputBudget(defaultMaterializerLimits().outputBytes)
			if err != nil {
				t.Fatal(err)
			}
			manifest, err := decodeCrossManifest(command.manifest, defaultMaterializerLimits(), arena)
			if err != nil {
				t.Fatal(err)
			}
			p06BindBusiness(t, proposal, manifest)
			if len(command.proofs) != 1 {
				t.Fatal("Resolve lacks exact original decision proof")
			}
			decision := f.n.all[4].call(t, processRequest{op: processEntryRecord, index: command.proofs[0].index})
			stored, err := decodeCrossStatus(decision.outcome)
			if err != nil || !stored.commit || stored.phase != crossDecide || stored.tx != manifest.tx || len(decision.changes) != 0 {
				t.Fatal("missing actual decision before IO arm", err)
			}
			f.packetDrain()
			owner := nativeFaultControlCall(t, f.n.all[1], nativeFaultControl{Version: 1, Action: "inspect"})
			armed := nativeFaultControlCall(t, f.n.all[1], nativeFaultControl{Version: 1, Action: "arm", Cookie: owner.Cookie, Sequence: 1, Kind: kind, Nth: 1})
			// Stop new peer deliveries to this scheduled failing voter; the
			// genuine source Resolve still reaches its ordinary local WAL path.
			f.mu.Lock()
			f.down[1] = true
			f.mu.Unlock()
			release()
			var answer graphProcessAnswer
			select {
			case answer = <-call:
			case <-time.After(10 * time.Second):
				t.Fatal("native IO caller not retired")
			}
			if result, err := decodeGraphProcessCoordinatorResult(answer.response); err == nil && result.decision != (crossStatus{}) {
				t.Fatal("failed participant installation acknowledged completion")
			}
			cancel()
			receipt := readNativeFaultIO(t, filepath.Join(f.n.dir, "1"), armed)
			failed := nativeFaultReap(t, f.n.all[1], 1, filepath.Join(f.n.dir, "1"), receipt)
			f.mu.Lock()
			f.intentionalKills[f.n.all[1]] = true
			f.mu.Unlock()
			f.elect(1, 2)
			p03Reconnect(f, 6)
			result := f.run(6, proposal)
			if result.decision.tx != stored.tx || result.decision.digest != stored.digest || result.decision.round != stored.round || !result.decision.commit {
				t.Fatal("IO recovery changed frozen decision")
			}
			step := p03Complete(f, 6, "edge-create", proposal, result, from, before)
			f.assertView(step.After, 3)
			pairedFaultAssertCDC(t, step, 3)
			assertLifecycleCandidates(t, step.RawObservation, 3)
			recovered := p06Observe(f, 6, graphReadFresh, graphCut{})
			rejoined := f.replace(1)
			oldArm, err := json.Marshal(nativeFaultControl{Version: 1, Action: "arm", Cookie: receipt.Cookie, Sequence: 2, Kind: kind, Nth: 1})
			if err != nil {
				t.Fatal(err)
			}
			ctx, stop := context.WithTimeout(t.Context(), 5*time.Second)
			refusal, transport := f.n.all[1].exchange(ctx, processRequest{op: processNativeFaultControl, data: oldArm})
			stop()
			if !errors.Is(errors.Join(transport, processResponseError(refusal)), errInvalid) || refusal.incarnation != f.n.all[1].incarnation || refusal.sequence == 0 {
				t.Fatal("old IO cookie entered replacement owner", transport, processResponseError(refusal))
			}
			retained := p06Observe(f, 6, graphReadAt, oldCut)
			reopened := p06Observe(f, 1, graphReadAt, oldCut)
			if !reflect.DeepEqual(old, retained) || !reflect.DeepEqual(old, reopened) {
				t.Fatal("IO recovery substituted current at old cut")
			}
			through := f.indexes()
			writeNativeFaultArtifact(t, f.n.dir, nativeFaultArtifact{Version: 1, Profile: nativeFaultProfile, Case: "wal-" + kind, Original: proposal.wire, Manifest: command.manifest, Before: old, Retained: retained, Recovered: recovered, Reopened: reopened, Processes: []pairedFaultProcess{failed, rejoined}, From: from, Through: through, Records: p06Records(f, from, through), IO: &receipt})
		})
	}
}

func TestNativeComposedFaultDirectorySyncFailsGenuineSameDirectoryReopen(t *testing.T) {
	t.Setenv("RHO_GRAPH_NATIVE_FAULT_PROCESS", "1")
	f := newPairedGraphFaultNetwork(t)
	nativeFaultSeed(t, f)
	old := p06Observe(f, 6, graphReadFresh, graphCut{})
	cut, err := pairedFaultCut(old.Cut)
	if err != nil {
		t.Fatal(err)
	}
	from := f.indexes()
	killed := f.kill(1, "replica")
	f.elect(1, 2)
	dir := filepath.Join(f.n.dir, fmt.Sprint(1))
	candidate := startGraphProcess(t, dir, 1, "RHO_GRAPH_CERTIFIED_PROCESS=1", "RHO_GRAPH_SOURCE_PROCESS=1", "RHO_GRAPH_LIFECYCLE_PROCESS=1", "RHO_GRAPH_PROCESS_REOPEN=1", "RHO_GRAPH_PAIRED_PROCESS=1", "RHO_GRAPH_NATIVE_FAULT_START=directory-sync")
	// No mux or Ready publication occurs for this pre-Open owner. The actual
	// failed process is reaped before its private cookie is read from disk.
	failed := nativeFaultReap(t, candidate, 1, dir, nativeFaultReceipt{})
	receipt := readNativeFaultStartupIO(t, dir, 1, uint64(candidate.cmd.Process.Pid))
	rejoined := f.replace(1)
	p03Reconnect(f, 6)
	recovered := p06Observe(f, 6, graphReadFresh, graphCut{})
	retained := p06Observe(f, 6, graphReadAt, cut)
	reopened := p06Observe(f, 1, graphReadAt, cut)
	if !reflect.DeepEqual(old, retained) || !reflect.DeepEqual(old, reopened) || !reflect.DeepEqual(old.Native, recovered.Native) {
		t.Fatal("directory reopen failure changed graph/old cut")
	}
	through := f.indexes()
	writeNativeFaultArtifact(t, f.n.dir, nativeFaultArtifact{Version: 1, Profile: nativeFaultProfile, Case: "directory-sync-during-reopen", Before: old, Retained: retained, Recovered: recovered, Reopened: reopened, Processes: []pairedFaultProcess{killed, failed, rejoined}, From: from, Through: through, Records: p06Records(f, from, through), IO: &receipt})
}

func TestNativeComposedFaultActualPacketsPreserveWholeTransactionAndOldCut(t *testing.T) {
	for _, schedule := range []struct {
		action string
		delay  int
	}{{"drop", 0}, {"duplicate", 0}, {"reorder", 0}, {"partition", 0}, {"delay", 1}, {"delay", 20}, {"delay", 100}} {
		t.Run(fmt.Sprintf("%s-%dms", schedule.action, schedule.delay), func(t *testing.T) {
			f := newPairedGraphFaultNetwork(t)
			before := nativeFaultSeed(t, f)
			old := p06Observe(f, 6, graphReadFresh, graphCut{})
			cut, err := pairedFaultCut(old.Cut)
			if err != nil {
				t.Fatal(err)
			}
			proposal, from := f.proposal(3), f.indexes()
			f.packetDrain()
			capture, err := nativeFaultCaptureManifest(f.n.all[1].mux.hub, 5)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _, _ = capture() }()
			scheduler, err := newNativeFaultPacketScheduler(t.Context(), schedule.action, 1, 2, 1, schedule.delay)
			if err != nil {
				t.Fatal(err)
			}
			detach, err := attachNativeFaultPacketScheduler(f.n.all[1].mux.hub, scheduler)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if _, err := detach(); err != nil {
					t.Error("unfinished actual packet schedule", err)
				}
			}()
			result := f.run(5, proposal)
			f.packetDrain()
			events, err := detach()
			if err != nil || len(events) == 0 {
				t.Fatal("no actual matched completed packet schedule", err)
			}
			// A healed directed partition must actually catch the omitted voter
			// up before the complete three-voter retained-record census.
			f.heal(1)
			step := p03Complete(f, 6, "edge-create", proposal, result, from, before)
			f.assertView(step.After, 3)
			pairedFaultAssertCDC(t, step, 3)
			assertLifecycleCandidates(t, step.RawObservation, 3)
			recovered := p06Observe(f, 6, graphReadFresh, graphCut{})
			retained := p06Observe(f, 6, graphReadAt, cut)
			if !reflect.DeepEqual(old, retained) {
				t.Fatal("packet schedule changed old cut")
			}
			through := f.indexes()
			// Retain the actual source-submitted immutable original, never a
			// manifest reconstructed from the observed graph.
			manifest, err := capture()
			if err != nil {
				t.Fatal(err)
			}
			writeNativeFaultArtifact(t, f.n.dir, nativeFaultArtifact{Version: 1, Profile: nativeFaultProfile, Case: fmt.Sprintf("packet-%s-%dms", schedule.action, schedule.delay), Original: proposal.wire, Manifest: manifest, Before: old, Retained: retained, Recovered: recovered, Reopened: retained, From: from, Through: through, Records: p06Records(f, from, through), PacketEvents: events})
		})
	}
}

func TestNativeComposedFaultFrozenConflictKeepsOriginalRefusalThroughRecovery(t *testing.T) {
	f := newPairedGraphFaultNetwork(t)
	before := nativeFaultSeed(t, f)
	old := p06Observe(f, 6, graphReadFresh, graphCut{})
	cut, err := pairedFaultCut(old.Cut)
	if err != nil {
		t.Fatal(err)
	}
	proposal, from := f.proposal(3), f.indexes()
	entered, release := p06FirstYESGate(f, 5)
	defer release()
	call, cancel := f.beginRun(5, proposal)
	defer cancel()
	boundary := f.awaitBoundary(entered)
	_, _, manifest := p06PrepareBoundary(t, boundary, 8)
	p06BindBusiness(t, proposal, manifest)
	// This second genuine frozen original conflicts with the prepared endpoint
	// read. Its durable local refusal precedes the first original's decision.
	cancel()
	select {
	case answer := <-call:
		result, err := decodeGraphProcessCoordinatorResult(answer.response)
		if !errors.Is(answer.err, context.Canceled) || !errors.Is(err, errCrossProcessPending) || result.decision != (crossStatus{}) {
			t.Fatal("prepared original misclassified", answer.err, err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("prepared original not retired")
	}
	release()
	conflicting := p06LocalProposal(f, 2, certifiedP08Close, 1, allocationP08Words(2))
	refused := p06SubmitLocal(f, 1, conflicting, reasonPreparedConflict)
	result := f.run(6, proposal)
	step := p03Complete(f, 6, "edge-create", proposal, result, from, before)
	f.assertView(step.After, 3)
	pairedFaultAssertCDC(t, step, 3)
	recovered := p06Observe(f, 6, graphReadFresh, graphCut{})
	replay := p06Submit(f, 1, conflicting)
	if replay.Index <= refused.Index || !bytes.Equal(replay.Outcome, refused.Outcome) || len(replay.CDC) != 0 {
		t.Fatal("frozen conflict replay changed first durable outcome")
	}
	current := p06Observe(f, 6, graphReadFresh, graphCut{})
	if !reflect.DeepEqual(recovered.Native, current.Native) {
		t.Fatal("old refused original became new post-recovery Close")
	}
	retained := p06Observe(f, 6, graphReadAt, cut)
	if !reflect.DeepEqual(old, retained) {
		t.Fatal("frozen conflict changed old cut")
	}
	through := f.indexes()
	actual, _, err := nativeFaultPhase(boundary.Request)
	if err != nil {
		t.Fatal(err)
	}
	writeNativeFaultArtifact(t, f.n.dir, nativeFaultArtifact{Version: 1, Profile: nativeFaultProfile, Case: "frozen-conflict-original-recovery", Original: proposal.wire, Manifest: actual.manifest, Before: old, Retained: retained, Recovered: recovered, Reopened: retained, From: from, Through: through, Records: p06Records(f, from, through), Refused: &refused, RefusedReplay: &replay})
}
