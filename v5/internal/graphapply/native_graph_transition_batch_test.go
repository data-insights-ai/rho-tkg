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
	"syscall"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

func TestNativeExactBatchOwnerKeepsFiniteArmsAndRefusesAmbiguousOrExpiredReceipt(t *testing.T) {
	run := [16]byte{1}
	o, err := newNativeBatchOwner(t.Context(), t.TempDir(), 1, run, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	defer o.close()
	c := nativeBatchControl{Version: 1, Action: "arm", Cookie: o.state.Cookie, Sequence: 1, Kind: raftlog.StableStorageReady, Side: "after", CommandSHA256: sha256.Sum256([]byte("inert selection, never a proposal"))}
	call := func(c nativeBatchControl) error {
		wire, err := nativeBatchControlWire(c)
		if err != nil {
			return err
		}
		_, err = o.control(wire)
		return err
	}
	if err := call(c); err != nil {
		t.Fatal(err)
	}
	s, selected := o.StableStorageSelection(raftlog.StableStorageReady)
	if !selected || s.CommandSHA256 != c.CommandSHA256 {
		t.Fatal("exact inert selection")
	}
	if _, selected := o.StableStorageSelection(raftlog.StableStorageApplication); selected {
		t.Fatal("Ready/application batches conflated")
	}
	w := raftlog.StableStorageWork{Entries: 3, Bytes: 4096, OutputBytes: 1024}
	if !o.ReserveStableStorageObservation(s, w) || o.state.Work != w.Bytes || o.state.Attempts != 1 {
		t.Fatal("hashing not prepaid")
	}
	if err := call(c); err != nil || o.state.Work != w.Bytes || o.state.Attempts != 1 {
		t.Fatal("exact arm replay reset spent ledger", err)
	}
	foreign := c
	foreign.Cookie[0] ^= 1
	if err := call(foreign); !errors.Is(err, replica.ErrUnavailable) {
		t.Fatal("foreign owner", err)
	}
	// A full real-batch manifest can contain two equal commands. Neither its
	// first entry nor a copied receipt can select which native transition won.
	o.ObserveStableStorage(raftlog.StableStorageObservation{Selection: s, Kind: c.Kind, Matches: 2, Work: w})
	if _, selected := o.StableStorageSelection(c.Kind); selected || o.state.State != "unavailable" || o.state.Work != w.Bytes {
		t.Fatal("ambiguous/no-refund diagnostic")
	}
	c.Sequence++
	if err := call(c); err != nil {
		t.Fatal(err)
	}
	s, _ = o.StableStorageSelection(c.Kind)
	if o.ReserveStableStorageObservation(s, raftlog.StableStorageWork{Entries: 1, Bytes: 1<<40 + 1, OutputBytes: 1024}) || o.state.State != "unavailable" {
		t.Fatal("exhausted diagnostic altered native admission")
	}
	o.close()
	o.close()
	c.Sequence++
	if err := call(c); !errors.Is(err, replica.ErrUnavailable) {
		t.Fatal("expired owner rearmed", err)
	}
	if _, selected := o.StableStorageSelection(c.Kind); selected {
		t.Fatal("closed diagnostic revived")
	}
	// These are inert routing inputs only. The process callback additionally
	// authenticates its expected target/PID/cookie/batch from the synced file.
	if !nativeBatchFailedStepAllowed(processResponse{}, io.EOF) || nativeBatchFailedStepAllowed(processResponse{sequence: 1, code: 2}, io.EOF) || nativeBatchFailedStepAllowed(processResponse{}, errLimit) || nativeBatchFailedStepAllowed(processResponse{}, errCorrupt) || nativeBatchFailedStepAllowed(processResponse{}, context.DeadlineExceeded) || nativeBatchFailedStepAllowed(processResponse{}, errors.Join(io.EOF, errLimit)) {
		t.Fatal("native refusal/expiry/corruption was routed as target death")
	}
	if nativeBatchFailedStepAllowed(processResponse{code: 2}, io.EOF) || nativeBatchFailedStepAllowed(processResponse{causeMask: 1}, io.EOF) || nativeBatchFailedStepAllowed(processResponse{aux: []byte{1}}, io.EOF) || nativeBatchFailedStepAllowed(processResponse{aux: []byte{}}, io.EOF) {
		t.Fatal("uncorrelated native fields were treated as an all-zero transport response")
	}
}

type nativeBatchArtifact struct {
	nativeFaultArtifact
	Phase             byte
	Partition         uint64
	Kind              raftlog.StableStorageKind
	Side              string
	SelectedCommand   []byte
	Arm               nativeBatchReceipt
	Batch             nativeBatchReceipt
	Run               [16]byte
	DeadlineUnixNanos int64
	ReceiptReads      uint64
	ReceiptGrantBytes uint64
	Program           *nativeP06ProgramEvidence `json:",omitempty"`
}

func nativeBatchCall(ctx context.Context, p *graphProcess, c nativeBatchControl) (nativeBatchReceipt, error) {
	var r nativeBatchReceipt
	wire, err := nativeBatchControlWire(c)
	if err != nil {
		return r, err
	}
	out, transport := p.exchange(ctx, processRequest{op: processNativeFaultControl, data: wire})
	if err := errors.Join(transport, processResponseError(out)); err != nil {
		return r, err
	}
	if len(out.aux) > nativeBatchReceiptBytes {
		return r, errLimit
	}
	d := json.NewDecoder(bytes.NewReader(out.aux))
	d.DisallowUnknownFields()
	if err := d.Decode(&r); err != nil || r.Version != 1 || r.Profile != nativeBatchProfile || r.Cookie == ([16]byte{}) || r.PID != uint64(p.cmd.Process.Pid) {
		return r, errors.Join(errCorrupt, err)
	}
	canonical, err := json.Marshal(r)
	if err != nil || !bytes.Equal(canonical, out.aux) {
		return r, errors.Join(errCorrupt, err)
	}
	return r, nil
}

func nativeBatchReap(t *testing.T, f *pairedGraphFaultNetwork, member uint64, receipt nativeBatchReceipt) pairedFaultProcess {
	t.Helper()
	return nativeTransitionReap(t, f, member, receipt.Member, receipt.PID, receipt, "native-batch-transition-kill")
}

// Typed callers authenticate the actual native receipt before entering this
// shared OS wait/pipe cleanup path. The legacy wrapper keeps its JSON order and
// value unchanged; this function creates no converted or synthetic receipt.
func nativeTransitionReap(t *testing.T, f *pairedGraphFaultNetwork, member, receiptMember, receiptPID uint64, receipt any, action string) pairedFaultProcess {
	t.Helper()
	p := f.n.all[member]
	if p.closed || p.mux == nil || receiptMember != member || receiptPID != uint64(p.cmd.Process.Pid) {
		t.Fatal("foreign native batch child")
	}
	p.closed = true
	f.mu.Lock()
	f.down[member], f.intentionalKills[p] = true, true
	f.mu.Unlock()
	waited := make(chan error, 1)
	go func() { waited <- p.cmd.Wait() }()
	var waitErr error
	select {
	case waitErr = <-waited:
	case <-time.After(10 * time.Second):
		killErr := p.cmd.Process.Kill() // Cleanup only; this cell cannot qualify.
		select {
		case waitErr = <-waited:
		case <-time.After(10 * time.Second):
			waitErr = context.DeadlineExceeded
		}
		p.closeErr = errors.Join(waitErr, killErr, p.stdin.Close(), p.stdout.Close())
		t.Fatal("matched native batch did not self-kill/reap", p.closeErr)
	}
	p.closeErr = errors.Join(waitErr, p.stdin.Close(), p.stdout.Close())
	if p.cmd.ProcessState == nil {
		t.Fatal("missing actual native process state")
	}
	status, ok := p.cmd.ProcessState.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL || p.cmd.ProcessState.ExitCode() != -1 {
		t.Fatal("batch receipt did not cause actual self-SIGKILL", p.closeErr)
	}
	stderr, overflow := p.stderr.state()
	if overflow {
		t.Fatal("batch child diagnostic overflow")
	}
	volume := filepath.Join(f.n.dir, fmt.Sprint(member))
	raw, err := json.Marshal(struct {
		Member, PID            uint64
		ExitCode, SignalNumber int
		Signaled, Reaped       bool
		Volume, Stderr         string
		Receipt                any
	}{member, uint64(p.cmd.Process.Pid), -1, 9, true, true, volume, stderr, receipt})
	if err != nil {
		t.Fatal(err)
	}
	return pairedFaultProcess{EventID: f.event(), Action: action, Role: "replica", Member: member, Instance: pairedGraphFaultInstance(member, p), Volume: volume, Reaped: true, Signal: "SIGKILL", Raw: raw}
}

func nativeBatchCrossCase(t *testing.T, phase byte, partition uint64, kind raftlog.StableStorageKind, side string, programs ...nativeProtocolProgramSpec) {
	t.Helper()
	var run [16]byte
	if raw := os.Getenv("RHO_GRAPH_TRANSITION_RUN"); raw == "" {
		if _, err := io.ReadFull(rand.Reader, run[:]); err != nil {
			t.Fatal(err)
		}
		t.Setenv("RHO_GRAPH_TRANSITION_RUN", hex.EncodeToString(run[:]))
	} else {
		wire, err := hex.DecodeString(raw)
		if err != nil || len(wire) != 16 || hex.EncodeToString(wire) != raw {
			t.Fatal("original batch run identity", err)
		}
		copy(run[:], wire)
	}
	if run == ([16]byte{}) {
		t.Fatal("zero batch run identity")
	}
	if os.Getenv("RHO_GRAPH_TRANSITION_DEADLINE") == "" {
		t.Setenv("RHO_GRAPH_TRANSITION_DEADLINE", strconv.FormatInt(time.Now().Add(3*time.Minute).UnixNano(), 10))
	}
	deadline, err := strconv.ParseInt(os.Getenv("RHO_GRAPH_TRANSITION_DEADLINE"), 10, 64)
	if err != nil || strconv.FormatInt(deadline, 10) != os.Getenv("RHO_GRAPH_TRANSITION_DEADLINE") || deadline <= time.Now().UnixNano() || deadline-time.Now().UnixNano() > int64(1800*time.Second) {
		t.Fatal("fixed original batch deadline", err)
	}
	t.Setenv("RHO_GRAPH_STABLE_STORAGE_PROCESS", "1")
	ctx, cancel := context.WithDeadline(t.Context(), time.Unix(0, deadline))
	defer cancel()
	var f *pairedGraphFaultNetwork
	if len(programs) == 0 {
		f = newPairedGraphFaultNetwork(t)
	} else {
		f = newPairedGraphFaultNetworkWithParent(t, ctx)
	}
	program := nativeP06ProgramStart(t, ctx, f, kind, programs...)
	before := nativeFaultSeed(t, f)
	old := p06Observe(f, 6, graphReadFresh, graphCut{})
	cut, err := pairedFaultCut(old.Cut)
	if err != nil {
		t.Fatal(err)
	}
	proposal, from := f.proposal(3), f.indexes()
	member, successor := uint64(1), uint64(2)
	if partition == 8 {
		member, successor = 4, 5
	}
	if program != nil {
		member = f.n.leaders[0]
		if partition == 8 {
			member = f.n.leaders[1]
		}
		successor = nativeP06ProgramAlternate(member)
	}
	gate, err := newNativeConjunctionBatchGate(ctx, f, nativeConjunctionSelector{Actor: 5, Partition: partition, Member: member, Phase: crossPhase(phase), Kind: kind, Side: side}, run, deadline)
	if err != nil {
		t.Fatal("genuine bounded batch owner", err)
	}
	clear := gate.close
	defer clear()
	call, stopCall := f.beginRun(5, proposal)
	defer stopCall()
	var pending []byte
	select {
	case answer := <-call:
		result, actual := decodeGraphProcessCoordinatorResult(answer.response)
		if !errors.Is(actual, errCrossProcessPending) || result.decision != (crossStatus{}) {
			t.Fatal("interrupted exact batch invented completion/ABORT", actual)
		}
		pending = p06ProcessWire(t, answer.response)
	case <-time.After(10 * time.Second):
		t.Fatal("unmatched native batch selector or retained caller")
	}
	stopCall()
	actualArm, receipt, selectedCommand, _, err := gate.evidence()
	if err != nil {
		t.Fatal("unmatched/exhausted/failed native batch cannot qualify", err)
	}
	nativeP06ProgramLoss(t, program)
	failed := nativeBatchReap(t, f, member, receipt)
	clear()
	f.elect(member, successor)
	nativeP06ProgramSuccessor(t, program, member, successor)
	recoverer := uint64(6)
	if program != nil && member == 6 {
		recoverer = 5
	}
	p03Reconnect(f, recoverer)
	result := f.run(recoverer, proposal)
	_ = p03Complete(f, recoverer, "edge-create", proposal, result, from, before)
	recovered := p06Observe(f, recoverer, graphReadFresh, graphCut{})
	var reopened p06Observation
	rejoined := f.replaceBeforeHeal(member, func(_ *graphProcess, _ processResponse) {
		if program != nil {
			reopened = nativeP06InitialLocalOld(f, member, cut)
		} else {
			reopened = p06Observe(f, member, graphReadAt, cut)
		}
	})
	retained := p06Observe(f, recoverer, graphReadAt, cut)
	if !reflect.DeepEqual(old, retained) || !reflect.DeepEqual(old, reopened) {
		t.Fatal("native batch recovery substituted current for held old cut")
	}
	command, _, err := decodeCrossCommand(selectedCommand, defaultMaterializerLimits())
	if err != nil || byte(command.phase) != phase || command.ns.partition != partition {
		t.Fatal("selected command changed phase/scope", err)
	}
	through := f.indexes()
	name := fmt.Sprintf("batch-cross-%d-p%d-k%d-%s", phase, partition, kind, side)
	reads := gate.readCount()
	artifact := nativeBatchArtifact{nativeFaultArtifact: nativeFaultArtifact{Version: 3, Profile: nativeBatchProfile, Case: name, Original: proposal.wire, Manifest: command.manifest, PendingCaller: pending, Before: old, Retained: retained, Recovered: recovered, Reopened: reopened, Processes: []pairedFaultProcess{failed, rejoined}, From: from, Through: through, Records: p06Records(f, from, through)}, Phase: phase, Partition: partition, Kind: kind, Side: side, SelectedCommand: selectedCommand, Arm: actualArm, Batch: receipt, Run: run, DeadlineUnixNanos: deadline, ReceiptReads: reads, ReceiptGrantBytes: reads * 65536}
	artifact.Program = nativeP06ProgramComplete(t, program)
	header := artifact
	header.Records = nil
	path := nativeTransitionWriteRows(t, filepath.Join(f.n.dir, "native-"+name+".jsonl"), header, artifact.Records, through)
	checkCtx, stopCheck := context.WithDeadline(t.Context(), time.Unix(0, deadline))
	defer stopCheck()
	checker := "native_transition_batch_check.py"
	if artifact.Program != nil {
		checker = "native_protocol_campaign_p06_check.py"
	}
	check := exec.CommandContext(checkCtx, "python3", "-B", filepath.Join("..", "..", "..", "tools", "v5-substrate", checker), path)
	var diagnostics processStderr
	check.Stdout, check.Stderr = &diagnostics, &diagnostics
	if err := check.Run(); err != nil {
		body, _ := diagnostics.state()
		t.Fatal("independent exact native-batch source/serial checker", err, body)
	}
	if _, overflow := diagnostics.state(); overflow {
		t.Fatal("independent batch checker diagnostic bound")
	}
}

// 24 finite cells compose the six applicable cross-protocol commands with their
// actual native Ready/install batches. They do not claim checkpoint/activation,
// every P01–P09 recipe, physical power-loss or runtime qualification. Short keeps
// both participants/operations/sides via PrepareA and DecideB; full retains all.
func TestNativeTransactionExactReadyAndInstallBatchesRecoverOriginalAndOldCut(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for _, phase := range []byte{1, 2, 3, 4} {
		for _, partition := range []uint64{3, 8} {
			if (phase == 1 || phase == 3) && partition != 8 || testing.Short() && !(phase == 2 && partition == 3 || phase == 3 && partition == 8) {
				continue
			}
			for _, kind := range []raftlog.StableStorageKind{raftlog.StableStorageReady, raftlog.StableStorageApplication} {
				for _, side := range []string{"before", "after"} {
					t.Run(fmt.Sprintf("phase%d-p%d-k%d-%s", phase, partition, kind, side), func(t *testing.T) { nativeBatchCrossCase(t, phase, partition, kind, side) })
				}
			}
		}
	}
}
