package graphapply

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"testing"
	"time"
)

const nativeTransitionNamespaceProfile = "native-namespace-publication-transition-v1"

type nativeTransitionNamespaceArtifact struct {
	nativeFaultArtifact
	DeadlineUnixNanos int64
	CurrentBeforeKill p06Observation
	ReopenedCurrent   p06Observation
	Decision          []byte
	OriginalRetry     []byte
}

func nativeTransitionNamespaceInventory() []string {
	var kinds []string
	for _, operation := range []string{"options-rename", "manifest-create", "manifest-close", "manifest-directory-sync"} {
		for _, side := range []string{"before", "after"} {
			kinds = append(kinds, operation+"-"+side)
		}
	}
	return kinds
}

func nativeTransitionNamespaceCase(t *testing.T, kind string) string {
	t.Helper()
	if !slices.Contains(nativeTransitionNamespaceInventory(), kind) {
		t.Fatal("finite actual namespace selector")
	}
	deadline := time.Now().Add(3 * time.Minute).UnixNano()
	if raw := os.Getenv("RHO_GRAPH_TRANSITION_DEADLINE"); raw != "" {
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || strconv.FormatInt(value, 10) != raw || value <= time.Now().UnixNano() || value-time.Now().UnixNano() > int64(1800*time.Second) {
			t.Fatal("original whole-run deadline", err)
		}
		deadline = value
	}
	ctx, cancel := context.WithDeadline(t.Context(), time.Unix(0, deadline))
	defer cancel()
	t.Setenv("RHO_GRAPH_TRANSITION_DEADLINE", strconv.FormatInt(deadline, 10))
	t.Setenv("RHO_GRAPH_NATIVE_FAULT_PROCESS", "1")
	f := newPairedGraphFaultNetwork(t)
	before := nativeFaultSeed(t, f)
	old := p06Observe(f, 6, graphReadFresh, graphCut{})
	oldCut, err := pairedFaultCut(old.Cut)
	if err != nil {
		t.Fatal(err)
	}
	proposal, from := f.proposal(3), f.indexes()
	command, err := decodePartitionGraphCommand(proposal.wire, defaultMaterializerLimits())
	if err != nil {
		t.Fatal(err)
	}
	tx := crossTransactionID(namespace{command.request.ns.graph, 8}, command.request.id)
	manifest := nativeTransitionCutManifest(t, f.n.all[5].mux.hub, tx)
	result := f.run(5, proposal)
	actualManifest := manifest()
	f.n.all[5].mux.hub.mu.Lock()
	f.n.all[5].mux.hub.beforeRelay = nil
	f.n.all[5].mux.hub.mu.Unlock()
	step := p03Complete(f, 6, "edge-create", proposal, result, from, before)
	f.assertView(step.After, 3)
	pairedFaultAssertCDC(t, step, 3)
	assertLifecycleCandidates(t, step.RawObservation, 3)
	current := p06Observe(f, 6, graphReadFresh, graphCut{})
	newCut, err := pairedFaultCut(current.Cut)
	if err != nil {
		t.Fatal(err)
	}
	killed := f.kill(1, "namespace-original-replica")
	f.elect(1, 2)
	dir := filepath.Join(f.n.dir, "1")
	// This is the real same-directory raftlog.Open. The pre-Open arm must
	// self-kill at the matched VFS method; a parent cleanup kill cannot qualify.
	candidate := startGraphProcess(t, dir, 1, "RHO_GRAPH_CERTIFIED_PROCESS=1", "RHO_GRAPH_SOURCE_PROCESS=1", "RHO_GRAPH_LIFECYCLE_PROCESS=1", "RHO_GRAPH_PROCESS_REOPEN=1", "RHO_GRAPH_PAIRED_PROCESS=1", "RHO_GRAPH_NATIVE_FAULT_START="+kind)
	failed := nativeFaultReap(t, candidate, 1, dir, nativeFaultReceipt{})
	failed.EventID = f.event()
	receipt := readNativeFaultStartupIO(t, dir, 1, uint64(candidate.cmd.Process.Pid))
	if receipt.Kind != kind || !nativeTransitionReceiptValid(receipt) {
		t.Fatal("unmatched/nonguaranteed namespace arm cannot qualify", receipt)
	}
	var reopened, reopenedCurrent p06Observation
	rejoined := f.replaceBeforeHeal(1, func(_ *graphProcess, _ processResponse) {
		// The actual third process reads its own native historical sources
		// before leader catch-up or any peer-routing substitution.
		reopened = p06Observe(f, 1, graphReadAt, oldCut)
		reopenedCurrent = p06Observe(f, 1, graphReadAt, newCut)
	})
	p03Reconnect(f, 6)
	retry, transport := f.n.all[6].exchange(ctx, processRequest{op: processSourceCoordinate, data: proposal.wire})
	replayed, replayErr := decodeGraphProcessCoordinatorResult(retry)
	if err := errors.Join(transport, replayErr); err != nil || replayed.decision != result.decision {
		t.Fatal("namespace restart changed actual original decision", err)
	}
	recovered := p06Observe(f, 6, graphReadFresh, graphCut{})
	retained := p06Observe(f, 6, graphReadAt, oldCut)
	if !reflect.DeepEqual(old, reopened) || !reflect.DeepEqual(old, retained) || !reflect.DeepEqual(current, reopenedCurrent) || !reflect.DeepEqual(current, recovered) {
		t.Fatal("namespace publication restart changed retained/current graph or original certificate")
	}
	decision, err := encodeCrossStatus(result.decision)
	if err != nil {
		t.Fatal(err)
	}
	through := f.indexes()
	records := p06Records(f, from, through)
	for _, record := range records {
		if bytes.Equal(record.Outcome, decision) && len(record.CDC) != 0 {
			t.Fatal("immutable decision replay emitted business CDC")
		}
	}
	if err := ctx.Err(); err != nil {
		t.Fatal("original namespace deadline", err)
	}
	artifact := nativeTransitionNamespaceArtifact{nativeFaultArtifact: nativeFaultArtifact{Version: 1, Profile: nativeTransitionNamespaceProfile, Case: "namespace-" + kind, Original: proposal.wire, Manifest: actualManifest,
		Before: old, Retained: retained, Recovered: recovered, Reopened: reopened, Processes: []pairedFaultProcess{killed, failed, rejoined}, From: from, Through: through, IO: &receipt},
		DeadlineUnixNanos: deadline, CurrentBeforeKill: current, ReopenedCurrent: reopenedCurrent, Decision: decision, OriginalRetry: p06ProcessWire(t, retry)}
	return nativeTransitionWriteRows(t, filepath.Join(f.n.dir, fmt.Sprintf("native-%s.jsonl", kind)), artifact, records, through)
}
