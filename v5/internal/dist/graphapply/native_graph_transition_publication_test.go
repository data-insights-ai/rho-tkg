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
	"sync"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

const nativePublicationProfile = "native-actual-publication-sync-process-v1"

type nativePublicationStorage struct {
	FirstIndex, LastIndex uint64
	SnapshotOffers        int
}

func nativePublicationStorageOf(r processResponse) (nativePublicationStorage, error) {
	var v nativePublicationStorage
	if len(r.aux) == 0 || len(r.aux) > 4096 {
		return v, errCorrupt
	}
	d := json.NewDecoder(bytes.NewReader(r.aux))
	d.DisallowUnknownFields()
	if err := d.Decode(&v); err != nil {
		return v, errors.Join(errCorrupt, err)
	}
	b, err := json.Marshal(v)
	if err != nil || !bytes.Equal(b, r.aux) || v.FirstIndex == 0 || v.LastIndex == ^uint64(0) || v.FirstIndex > v.LastIndex+1 || v.SnapshotOffers < 0 || v.SnapshotOffers > 2 {
		return v, errors.Join(errCorrupt, err)
	}
	return v, nil
}

// Artifact storage rows are actual native diagnostics, not a supplied cut,
// checkpoint image or synthetic snapshot/transfer capability.
type nativePublicationArtifact struct {
	nativeFaultArtifact
	Side                            string
	Run                             [16]byte
	DeadlineUnixNanos               int64
	Arm, State                      nativeStateReceipt
	BeforeStorage, RestoredStorage  []byte
	ReceiptReads, ReceiptGrantBytes uint64
}

func nativePublicationCase(t *testing.T, side string) {
	t.Helper()
	var run [16]byte
	if raw := os.Getenv("RHO_GRAPH_TRANSITION_RUN"); raw == "" {
		if _, err := io.ReadFull(rand.Reader, run[:]); err != nil {
			t.Fatal(err)
		}
		t.Setenv("RHO_GRAPH_TRANSITION_RUN", hex.EncodeToString(run[:]))
	} else {
		b, err := hex.DecodeString(raw)
		if err != nil || len(b) != 16 || hex.EncodeToString(b) != raw {
			t.Fatal("fixed publication run", err)
		}
		copy(run[:], b)
	}
	if os.Getenv("RHO_GRAPH_TRANSITION_DEADLINE") == "" {
		t.Setenv("RHO_GRAPH_TRANSITION_DEADLINE", strconv.FormatInt(time.Now().Add(3*time.Minute).UnixNano(), 10))
	}
	deadline, err := strconv.ParseInt(os.Getenv("RHO_GRAPH_TRANSITION_DEADLINE"), 10, 64)
	if err != nil || strconv.FormatInt(deadline, 10) != os.Getenv("RHO_GRAPH_TRANSITION_DEADLINE") || deadline <= time.Now().UnixNano() || deadline-time.Now().UnixNano() > int64(1800*time.Second) || run == ([16]byte{}) {
		t.Fatal("original absolute publication budget", err)
	}
	t.Setenv("RHO_GRAPH_FAULT_FAMILY_PROCESS", "1")
	t.Setenv("RHO_GRAPH_STABLE_STORAGE_PROCESS", "1")
	t.Setenv("RHO_GRAPH_STABLE_STATE_PROCESS", "1")
	ctx, cancel := context.WithDeadline(t.Context(), time.Unix(0, deadline))
	defer cancel()
	f := newPairedGraphFaultNetwork(t)
	before := nativeFaultSeed(t, f)
	old := p06Observe(f, 6, graphReadFresh, graphCut{})
	cut, err := pairedFaultCut(old.Cut)
	if err != nil {
		t.Fatal(err)
	}
	proposal, from := f.proposal(3), f.indexes()
	capture, err := nativeFaultCaptureManifest(f.n.all[1].mux.hub, 5)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = capture() }()
	result := f.run(5, proposal)
	manifest, err := capture()
	if err != nil {
		t.Fatal(err)
	}
	target := f.n.all[1]
	beforeState := target.call(t, processRequest{op: processStateOp, entity: 1})
	storage, err := nativePublicationStorageOf(beforeState)
	if err != nil || beforeState.stats[statCheckpoint] == 0 || beforeState.stats[statCheckpoint] < storage.FirstIndex || beforeState.stats[statCheckpoint] == ^uint64(0) {
		t.Fatal("publication requires a genuine uncompacted applied prefix", err)
	}
	beforeWire := p06ProcessWire(t, beforeState)
	owner, err := nativeStateCall(ctx, target, nativeStateControl{Version: 1, Action: "inspect"})
	if err != nil || owner.Member != 1 || owner.Run != run || owner.State != "idle" || owner.DeadlineUnixNanos > deadline {
		t.Fatal("actual publication owner", err)
	}
	control := nativeStateControl{Version: 1, Action: "arm", Cookie: owner.Cookie, Sequence: 1, Kind: raftlog.StableStatePublication, Side: side, Filter: nativeStateFilter{Applied: beforeState.stats[statCheckpoint], SourceIndex: beforeState.stats[statCheckpoint]}}
	arm, err := nativeStateCall(ctx, target, control)
	if err != nil || arm.Control != control || arm.State != "armed" {
		t.Fatal("native publication arm", err)
	}
	var mu sync.Mutex
	var reads uint64
	readExpected := func() (nativeStateReceipt, error) {
		mu.Lock()
		if reads >= 129 {
			mu.Unlock()
			return nativeStateReceipt{}, errLimit
		}
		reads++
		mu.Unlock()
		if err := ctx.Err(); err != nil {
			return nativeStateReceipt{}, err
		}
		return nativeStateRead(filepath.Join(f.n.dir, "1"), arm)
	}
	hub := target.mux.hub
	hub.mu.Lock()
	if hub.failedStep != nil {
		hub.mu.Unlock()
		t.Fatal("publication recipe already owns failure routing")
	}
	hub.failedStep = func(job graphProcessPacketJob, actual error) bool {
		if actual == nil || job.packet.To != 1 || hub.peer(1).p != target {
			return false
		}
		if _, err := readExpected(); err != nil {
			return false
		}
		f.mu.Lock()
		f.down[1] = true
		f.mu.Unlock()
		return true
	}
	hub.mu.Unlock()
	clear := sync.OnceFunc(func() { hub.mu.Lock(); hub.failedStep = nil; hub.mu.Unlock() })
	defer clear()
	response, transport := target.exchange(ctx, processRequest{op: processPublish})
	if !nativeBatchFailedStepAllowed(response, transport) {
		t.Fatal("publication did not self-kill at actual native batch", transport, processResponseError(response))
	}
	receipt, err := readExpected()
	if err != nil {
		t.Fatal("unmatched publication cannot qualify", err)
	}
	id := receipt.Events[0].Selection.Identity
	if id.SourceCutID == ([32]byte{}) || id.CurrentPublicationID != id.SourceCutID || id.SourceImageSHA256 != id.ImageSHA256 || id.Applied != beforeState.stats[statCheckpoint] || id.SourceIndex != id.Applied {
		t.Fatal("native publication identity changed actual captured checkpoint")
	}
	failed := nativeTransitionReap(t, f, 1, receipt.Member, receipt.PID, receipt, "native-state-transition-kill")
	clear()
	f.elect(1, 2)
	p03Reconnect(f, 6)
	// Unknown publication is never a business ABORT. The actual immutable
	// original remains the same; recovery may return its earlier first result.
	result = f.run(6, proposal)
	step := p03Complete(f, 6, "edge-create", proposal, result, from, before)
	f.assertView(step.After, 3)
	pairedFaultAssertCDC(t, step, 3)
	assertLifecycleCandidates(t, step.RawObservation, 3)
	recovered := p06Observe(f, 6, graphReadFresh, graphCut{})
	var reopened p06Observation
	var restoredWire []byte
	rejoined := f.replaceBeforeHeal(1, func(p *graphProcess, _ processResponse) {
		r := p.call(t, processRequest{op: processStateOp, entity: 1})
		actual, err := nativePublicationStorageOf(r)
		if err != nil || r.stats[statCheckpoint] != id.Applied || r.applied < id.Applied || actual.LastIndex < storage.LastIndex {
			t.Fatal("native publication lost acknowledged checkpoint/prefix", err)
		}
		expected := storage.FirstIndex
		if side == "after" {
			expected = id.SourceIndex + 1
		}
		if actual.FirstIndex != expected {
			t.Fatal("before/after Sync compaction boundary changed", actual.FirstIndex, expected)
		}
		restoredWire = p06ProcessWire(t, r)
		// Source2 has not previously used replacement1. Read its OWN native
		// old prefix before heal; no process-local source handle is transferred.
		f.reconnect(2, 1)
		reopened = p06Observe(f, 2, graphReadAt, cut)
		if !reflect.DeepEqual(reopened, old) {
			t.Fatal("publication replaced local old cut with current state")
		}
	})
	retained := p06Observe(f, 6, graphReadAt, cut)
	if !reflect.DeepEqual(old, retained) {
		t.Fatal("publication changed held old cut")
	}
	through := f.indexes()
	mu.Lock()
	actualReads := reads
	mu.Unlock()
	a := nativePublicationArtifact{nativeFaultArtifact: nativeFaultArtifact{Version: 5, Profile: nativePublicationProfile, Case: "state-publication-" + side, Original: proposal.wire, Manifest: manifest, Before: old, Retained: retained, Recovered: recovered, Reopened: reopened, Processes: []pairedFaultProcess{failed, rejoined}, From: from, Through: through, Records: p06Records(f, from, through)}, Side: side, Run: run, DeadlineUnixNanos: deadline, Arm: arm, State: receipt, BeforeStorage: beforeWire, RestoredStorage: restoredWire, ReceiptReads: actualReads, ReceiptGrantBytes: actualReads * 131072}
	header := a
	header.Records = nil
	path := nativeTransitionWriteRows(t, filepath.Join(f.n.dir, "native-"+a.Case+".jsonl"), header, a.Records, through)
	command := exec.CommandContext(ctx, "python3", "-B", filepath.Join("..", "..", "..", "..", "tools", "v5-substrate", "native_transition_publication_check.py"), path)
	var diagnostic processStderr
	command.Stdout, command.Stderr = &diagnostic, &diagnostic
	if err := command.Run(); err != nil {
		body, _ := diagnostic.state()
		t.Fatal("independent publication/prefix/serial checker", err, body)
	}
	if _, overflow := diagnostic.state(); overflow {
		t.Fatal("publication checker diagnostic bound")
	}
}

func TestNativePublicationSyncBoundariesKeepActualCompactionOriginalAndLocalOldCut(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for _, side := range []string{"before", "after"} {
		t.Run(side, func(t *testing.T) { nativePublicationCase(t, side) })
	}
}

func TestNativePublicationStorageDiagnosticRefusesInventedOrOverflowedBoundaries(t *testing.T) {
	valid := processResponse{aux: []byte(`{"FirstIndex":2,"LastIndex":7,"SnapshotOffers":0}`)}
	v, err := nativePublicationStorageOf(valid)
	if err != nil || v.FirstIndex != 2 || v.LastIndex != 7 {
		t.Fatal(v, err)
	}
	for _, raw := range []string{`{"FirstIndex":0,"LastIndex":7,"SnapshotOffers":0}`, `{"FirstIndex":9,"LastIndex":7,"SnapshotOffers":0}`, `{"FirstIndex":1,"LastIndex":18446744073709551615,"SnapshotOffers":0}`, `{"FirstIndex":2,"LastIndex":7,"SnapshotOffers":3}`, `{"FirstIndex":2,"LastIndex":7,"SnapshotOffers":0,"invented":1}`} {
		if _, err := nativePublicationStorageOf(processResponse{aux: []byte(raw)}); !errors.Is(err, errCorrupt) {
			t.Fatal(fmt.Sprint("invented native boundary ", raw), err)
		}
	}
}
