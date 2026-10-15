package graphapply

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"syscall"
	"testing"
	"time"
)

const nativeSnapshotSenderProfile = "native-snapshot-inflight-sender-loss-v1"

type nativeSnapshotSenderEvidence struct {
	Version                                                           uint8
	Run                                                               [16]byte
	DeadlineUnixNanos                                                 int64
	Side                                                              string
	SelectedSnapshot                                                  int
	SenderOwner                                                       uint64
	SenderBefore, ReceiverBefore, ReceiverPartial, ReceiverAfterClose []byte
	SenderReadIndex, RestartedReadIndex, SenderAfter                  []byte
	SenderReadRequest, RestartedReadRequest                           []byte
	PartialOld                                                        p06Observation
	StaleRequest, StaleReply, OriginalReply, ReplayReply              []byte
}

func nativeSnapshotSenderSide(mode string) string {
	switch mode {
	case "sender-before-append":
		return "before"
	case "sender-after-append":
		return "after"
	default:
		return ""
	}
}

// A logical down bit or replacement at the same member is insufficient.
// Only the captured, intentionally SIGKILLed and actually reaped OS object
// loses its feedback obligation; its replacement acquires no old send handle.
func nativeSnapshotSenderReaped(f *pairedGraphFaultNetwork, donor *graphProcess) bool {
	if f == nil || donor == nil || donor.cmd == nil || donor.cmd.Process == nil || !donor.closed || donor.cmd.ProcessState == nil {
		return false
	}
	f.mu.Lock()
	intentional := f.intentionalKills[donor]
	f.mu.Unlock()
	status, ok := donor.cmd.ProcessState.Sys().(syscall.WaitStatus)
	return intentional && ok && status.Signaled() && status.Signal() == syscall.SIGKILL && donor.cmd.ProcessState.Pid() == donor.cmd.Process.Pid
}

func TestNativeSnapshotSenderLossRequiresExactReapedProcessAndApplicableBoundary(t *testing.T) {
	if nativeSnapshotSenderReaped(nil, nil) || nativeSnapshotSenderReaped(&pairedGraphFaultNetwork{}, new(graphProcess)) {
		t.Fatal("logical member/down state substituted for actual SIGKILL/reap")
	}
	for _, mode := range []string{"", "before-append", "sender-before-begin", "sender-after-activate"} {
		if nativeSnapshotSenderSide(mode) != "" {
			t.Fatal("unimplemented sender boundary", mode)
		}
	}
	if nativeSnapshotSenderSide("sender-before-append") != "before" || nativeSnapshotSenderSide("sender-after-append") != "after" {
		t.Fatal("exact two in-flight Append boundaries")
	}
}

func nativeSnapshotSenderCase(t *testing.T, side string) {
	t.Helper()
	if side != "before" && side != "after" {
		t.Fatal("sender boundary")
	}
	var run [16]byte
	if value := os.Getenv("RHO_GRAPH_TRANSITION_RUN"); value == "" {
		if _, err := rand.Read(run[:]); err != nil {
			t.Fatal(err)
		}
		t.Setenv("RHO_GRAPH_TRANSITION_RUN", hex.EncodeToString(run[:]))
	} else {
		wire, err := hex.DecodeString(value)
		if err != nil || len(wire) != 16 || hex.EncodeToString(wire) != value {
			t.Fatal("original sender run", err)
		}
		copy(run[:], wire)
	}
	if os.Getenv("RHO_GRAPH_TRANSITION_DEADLINE") == "" {
		t.Setenv("RHO_GRAPH_TRANSITION_DEADLINE", strconv.FormatInt(time.Now().Add(3*time.Minute).UnixNano(), 10))
	}
	deadline, err := strconv.ParseInt(os.Getenv("RHO_GRAPH_TRANSITION_DEADLINE"), 10, 64)
	if err != nil || strconv.FormatInt(deadline, 10) != os.Getenv("RHO_GRAPH_TRANSITION_DEADLINE") || deadline <= time.Now().UnixNano() || deadline-time.Now().UnixNano() > int64(1800*time.Second) || run == ([16]byte{}) {
		t.Fatal("original finite sender deadline/run", err)
	}
	ctx, cancel := context.WithDeadline(t.Context(), time.Unix(0, deadline))
	defer cancel()
	t.Setenv("RHO_GRAPH_FAULT_FAMILY_PROCESS", "1")
	f := newPairedGraphFaultNetworkWithParent(t, ctx)
	before := nativeFaultSeed(t, f)
	old := p06Observe(f, 6, graphReadFresh, graphCut{})
	cut, err := pairedFaultCut(old.Cut)
	if err != nil {
		t.Fatal(err)
	}
	prior := f.call(f.n.all[3], processRequest{op: processStateOp})
	processes := []pairedFaultProcess{f.kill(3, "replica")}
	f.n.groups[0].blocked = map[uint64]bool{3: true}
	defer func() { f.n.groups[0].blocked = nil }()
	proposal, from := f.proposal(3), f.indexes()
	capture, err := nativeFaultCaptureManifest(f.n.all[1].mux.hub, 5)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = capture() }()
	originalReply := f.call(f.n.all[5], processRequest{op: processSourceCoordinate, data: proposal.wire})
	result, err := decodeGraphProcessCoordinatorResult(originalReply)
	if err != nil || !result.decision.commit {
		t.Fatal("genuine committed original", err)
	}
	f.drain()
	manifest, err := capture()
	if err != nil {
		t.Fatal(err)
	}
	published := f.call(f.n.all[1], processRequest{op: processPublish})
	if published.stats[statCheckpoint] <= prior.applied {
		t.Fatal("source publication must exceed lagged receiver")
	}
	leaderRead := f.call(f.n.all[1], processRequest{op: processCertifiedRead, entity: uint64(observeConfiguration)})
	f.n.groups[0].enqueue(t, leaderRead)
	f.drainGroup(f.n.groups[0])
	senderBefore := f.call(f.n.all[1], processRequest{op: processStateOp})
	if !senderBefore.ready || senderBefore.stats[statTerm] == 0 || senderBefore.stats[statVote] != 1 || leaderRead.incarnation != senderBefore.incarnation {
		t.Fatal("sender lacks actual successful leader ReadIndex and supporting hard state")
	}
	e := &nativeSnapshotSenderEvidence{Version: 1, Run: run, DeadlineUnixNanos: deadline, Side: side, SelectedSnapshot: -1, OriginalReply: p06ProcessWire(t, originalReply), SenderBefore: p06ProcessWire(t, senderBefore), SenderReadIndex: p06ProcessWire(t, leaderRead)}
	e.SenderReadRequest, err = encodeProcessRequest(processRequest{incarnation: leaderRead.incarnation, sequence: leaderRead.sequence, op: processCertifiedRead, entity: uint64(observeConfiguration)})
	if err != nil {
		t.Fatal(err)
	}
	var notices []nativeFaultSnapshotBoundary
	var lostSender *graphProcess
	injected, closed := false, false
	f.nativeSnapshotState = "sender-" + side + "-append"
	f.nativeSnapshotHook = func(b nativeFaultSnapshotBoundary) bool {
		if b.Donor != 1 || b.Receiver != 3 || b.SenderOwner == 0 {
			t.Fatal("sender escaped actual pending snapshot owner")
		}
		notices = append(notices, b)
		if b.Phase == "after-sender-close" {
			if !injected || closed {
				t.Fatal("orphan/repeated sender cleanup")
			}
			closed = true
			e.ReceiverAfterClose = p06ProcessWire(t, f.call(f.n.all[3], processRequest{op: processStateOp}))
			return true
		}
		if injected || b.Phase != side+"-append" {
			return true
		}
		chunk, err := decodePairedProcessChunk(b.Chunk)
		if err != nil || chunk.Final || chunk.Sequence != 0 || b.Owner == 0 {
			t.Fatal("sender loss requires actual first nonterminal page", err)
		}
		e.SelectedSnapshot, e.SenderOwner = len(notices)-1, b.SenderOwner
		lostSender = f.n.all[1]
		processes = append(processes, f.kill(1, "snapshot-sender"))
		if !nativeSnapshotSenderReaped(f, f.n.all[1]) {
			t.Fatal("sender was not actually reaped")
		}
		partial := f.call(f.n.all[3], processRequest{op: processStateOp})
		e.ReceiverPartial = p06ProcessWire(t, partial)
		startup, err := decodeProcessResponse(e.ReceiverBefore)
		if err != nil || partial.applied != startup.applied || !bytes.Equal(partial.image, startup.image) || partial.stats[statCheckpoint] != startup.stats[statCheckpoint] || partial.stats[statImport] != 1 || partial.stats[statVerified] != 0 {
			t.Fatal("partial import changed active receiver state", err)
		}
		f.reconnect(2, 3)
		e.PartialOld = p06Observe(f, 2, graphReadAt, cut)
		if !reflect.DeepEqual(old, e.PartialOld) {
			t.Fatal("partial import replaced the held old graph")
		}
		injected = true
		// Retry needs only actual returned boundaries; avoid duplicating every
		// pre-Append diagnostic after the selected source process is gone.
		f.nativeSnapshotState = "sender-after-append"
		return false
	}
	defer func() { f.nativeSnapshotHook = nil; f.nativeSnapshotState = "" }()
	receiver := f.replaceBeforeHeal(3, func(_ *graphProcess, state processResponse) {
		e.ReceiverBefore = p06ProcessWire(t, state)
		f.n.groups[0].blocked = nil
		sealed := make(map[uint64]bool)
		for round := 0; round < 32 && !injected; round++ {
			f.groupEvent(f.n.groups[0], 1, processRequest{op: processTick})
			f.drain()
			f.snapshotCatchup(1, sealed)
		}
		if !injected || !closed {
			t.Fatal("sender selector lacked real import/cleanup")
		}
		afterClose, err := decodeProcessResponse(e.ReceiverAfterClose)
		if err != nil || afterClose.applied != state.applied || !bytes.Equal(afterClose.image, state.image) || afterClose.stats[statImport] != 0 {
			t.Fatal("abandoned partial import became active", err)
		}
		processes = append(processes, f.replaceBeforeHeal(1, func(sender *graphProcess, restored processResponse) {
			if !nativeSnapshotSenderReaped(f, lostSender) || nativeSnapshotSenderReaped(f, sender) {
				t.Fatal("logical member replacement changed captured sender reap identity")
			}
			if restored.applied < published.applied {
				t.Fatal("sender restart lost committed prefix")
			}
			request := processRequest{op: processNext, index: e.SenderOwner}
			reply, transport := sender.exchange(ctx, request)
			if transport != nil || !errors.Is(processResponseError(reply), errInvalid) || len(reply.aux) != 0 || len(reply.handles) != 0 {
				t.Fatal("restarted process acquired old sender authority", transport, processResponseError(reply))
			}
			request.incarnation, request.sequence = reply.incarnation, reply.sequence
			e.StaleRequest, err = encodeProcessRequest(request)
			if err != nil {
				t.Fatal(err)
			}
			e.StaleReply = p06ProcessWire(t, reply)
			// Restore opens a follower. A real Campaign and its actual drained
			// vote/append packets precede heal's leader-only ReadIndex; a copied
			// member ID or prior vote is not a new leader claim.
			f.groupEvent(f.n.groups[0], 1, processRequest{op: processCampaign})
		}))
	})
	processes = append(processes, receiver)
	if !receiver.Ready || len(notices) < 5 || notices[len(notices)-1].Phase != "after-activate" {
		t.Fatal("retry lacks complete genuine activation")
	}
	f.nativeSnapshotHook, f.nativeSnapshotState = nil, ""
	restartedRead := f.call(f.n.all[1], processRequest{op: processCertifiedRead, entity: uint64(observeConfiguration)})
	f.n.groups[0].enqueue(t, restartedRead)
	f.drainGroup(f.n.groups[0])
	senderAfter := f.call(f.n.all[1], processRequest{op: processStateOp})
	if !senderAfter.ready || senderAfter.stats[statTerm] <= senderBefore.stats[statTerm] || senderAfter.stats[statVote] != 1 || restartedRead.incarnation != senderAfter.incarnation {
		t.Fatal("same-directory sender did not genuinely regain leadership")
	}
	e.RestartedReadIndex, e.SenderAfter = p06ProcessWire(t, restartedRead), p06ProcessWire(t, senderAfter)
	e.RestartedReadRequest, err = encodeProcessRequest(processRequest{incarnation: restartedRead.incarnation, sequence: restartedRead.sequence, op: processCertifiedRead, entity: uint64(observeConfiguration)})
	if err != nil {
		t.Fatal(err)
	}
	// These completed/closed callers used the dead issuer before the loss.
	// Rebind only through the existing authenticated, quiescent peer door;
	// the durable original request/body below remains exactly unchanged.
	f.reconnect(5, 1)
	f.reconnect(6, 1)
	replayed := f.call(f.n.all[5], processRequest{op: processSourceCoordinate, data: proposal.wire})
	again, err := decodeGraphProcessCoordinatorResult(replayed)
	if err != nil || again.decision != result.decision || again.resume != result.resume {
		t.Fatal("sender recovery changed immutable original", err)
	}
	e.ReplayReply = p06ProcessWire(t, replayed)
	f.drain()
	step := p03Complete(f, 6, "edge-create", proposal, result, from, before)
	f.assertView(step.After, 3)
	pairedFaultAssertCDC(t, step, 3)
	assertLifecycleCandidates(t, step.RawObservation, 3)
	recovered := p06Observe(f, 6, graphReadFresh, graphCut{})
	retained := p06Observe(f, 6, graphReadAt, cut)
	if !reflect.DeepEqual(old, retained) {
		t.Fatal("sender recovery substituted current for retained old cut")
	}
	through := f.indexes()
	name := "snapshot-sender-" + side + "-append"
	writeNativeFaultFamilyArtifact(t, f, nativeFaultFamilyArtifact{nativeFaultArtifact: nativeFaultArtifact{Case: name, Original: proposal.wire, Manifest: manifest, Before: old, Retained: retained, Recovered: recovered, Reopened: e.PartialOld, From: from, Through: through, Records: p06Records(f, from, through), Processes: processes}, Snapshot: notices, SnapshotSender: e})
	path := filepath.Join(f.n.dir, "native-fault-family-"+name+".jsonl")
	check := exec.CommandContext(ctx, "python3", "-B", filepath.Join("..", "..", "..", "tools", "v5-substrate", "native_fault_family_check.py"), path)
	var diagnostic processStderr
	check.Stdout, check.Stderr = &diagnostic, &diagnostic
	if err := check.Run(); err != nil {
		body, _ := diagnostic.state()
		t.Fatal("sender full raw/oracle checker", err, body)
	}
	if _, overflow := diagnostic.state(); overflow {
		t.Fatal("sender checker diagnostic bound")
	}
}

func TestNativeSnapshotSenderLossDuringImportKeepsWholeOriginalAndOldCut(t *testing.T) {
	for _, side := range []string{"before", "after"} {
		t.Run(side, func(t *testing.T) { nativeSnapshotSenderCase(t, side) })
	}
}
