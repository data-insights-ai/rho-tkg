package graphapply

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestNativeStorageTransitionSelectorsKeepGuaranteesAndFiniteOwnership(t *testing.T) {
	owner, err := newNativeFaultFS(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, operation := range []string{"wal-write", "wal-sync", "wal-sync-data", "wal-sync-to", "directory-sync", "options-rename", "manifest-create", "manifest-close"} {
		for _, side := range []string{"before", "after"} {
			kind := operation + "-" + side
			if got, at, ok := nativeTransitionSelector(kind); !ok || got != operation || at != side || !nativeFaultKind(kind) {
				t.Fatal("selector", kind)
			}
		}
	}
	if _, _, ok := nativeTransitionSelector("wal-sync-after-extra"); ok {
		t.Fatal("foreign selector accepted")
	}
	if nativeTransitionMatches("wal-sync-after", "000001.log", false, "sync-data") || nativeTransitionMatches("manifest-close-after", "MANIFEST-000001", false, "close") {
		t.Fatal("distinct native operations conflated")
	}
	arm := nativeFaultControl{Version: 1, Action: "arm", Cookie: owner.state.Cookie, Sequence: 1, Kind: "wal-sync-to-after", Nth: 1}
	wire, err := json.Marshal(arm)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := owner.control(wire); err != nil {
		t.Fatal(err)
	}
	if _, matched, err := owner.match(filepath.Join(owner.dir, "other.bin"), false, "sync-to", nil); err != nil || matched || owner.state.Seen != 0 {
		t.Fatal("unmatched IO qualified", err)
	}
	if _, matched, err := owner.match(filepath.Join(owner.dir, "000001.log"), false, "sync-to", nil); err != nil || !matched {
		t.Fatal("actual selector", err)
	}
	// A queued SyncTo is not a completed guaranteed synchronization. This is
	// a compact receipt-shape guard; the process parent below uses real IO.
	owner.state.IO.Transition = &nativeTransitionIO{Operation: "wal-sync-to", Side: "after", Returned: true}
	owner.state.IO.Error = "declared native storage transition SIGKILL"
	owner.state.IO.WrittenSHA256 = owner.state.IO.InputSHA256
	if nativeTransitionReceiptValid(owner.state) {
		t.Fatal("queued SyncTo granted a durability guarantee")
	}
	owner.state.IO.Transition.Guaranteed = true
	if !nativeTransitionReceiptValid(owner.state) {
		t.Fatal("successful full SyncTo receipt rejected")
	}
	owner.state.IO.Transition.ReturnError = "actual IO failure"
	if nativeTransitionReceiptValid(owner.state) {
		t.Fatal("failed IO qualified successful after barrier")
	}
	// Pending ownership cannot be replaced by a later arm or exact replay.
	if _, err := owner.control(wire); !errors.Is(err, errInvalid) {
		t.Fatal("pending arm owner replaced", err)
	}
}

// These are separate actual VFS cases during a genuine pending cross write.
// A background/election operation may be the selected IO; this test does NOT
// attribute that operation as Prepare/Decide's own storage transition. The
// separately authenticated first decision and complete prefix establish graph
// recovery, while the receipt establishes only the named actual VFS boundary.
func TestNativeStorageTransitionWALBeforeAfterRecoversSameOriginalAndOldCut(t *testing.T) {
	for _, kind := range []string{"wal-write-before", "wal-write-after", "wal-sync-data-before", "wal-sync-data-after"} {
		t.Run(kind, func(t *testing.T) {
			t.Setenv("RHO_GRAPH_NATIVE_FAULT_PROCESS", "1")
			f := newPairedGraphFaultNetwork(t)
			before := nativeFaultSeed(t, f)
			old := p06Observe(f, 6, graphReadFresh, graphCut{})
			cut, err := pairedFaultCut(old.Cut)
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
			command, cross, err := nativeFaultPhase(boundary.Request)
			if err != nil || !cross || command.phase != crossResolve || len(command.proofs) != 1 {
				t.Fatal("genuine pending original", err)
			}
			originalDecision := f.n.all[4].call(t, processRequest{op: processEntryRecord, index: command.proofs[0].index})
			decision, err := decodeCrossStatus(originalDecision.outcome)
			if err != nil || !decision.commit || decision.phase != crossDecide || len(originalDecision.changes) != 0 {
				t.Fatal("actual immutable first decision", err)
			}
			f.packetDrain()
			owner := nativeFaultControlCall(t, f.n.all[1], nativeFaultControl{Version: 1, Action: "inspect"})
			armed := nativeFaultControlCall(t, f.n.all[1], nativeFaultControl{Version: 1, Action: "arm", Cookie: owner.Cookie, Sequence: 1, Kind: kind, Nth: 1})
			f.mu.Lock()
			f.down[1] = true
			f.mu.Unlock()
			release()
			select {
			case answer := <-call:
				result, actual := decodeGraphProcessCoordinatorResult(answer.response)
				if actual == nil && result.decision != (crossStatus{}) {
					t.Fatal("crashed source acknowledged whole completion")
				}
			case <-time.After(10 * time.Second):
				t.Fatal("unmatched storage selector or unretired original caller")
			}
			cancel()
			dir := filepath.Join(f.n.dir, "1")
			receipt := readNativeFaultIO(t, dir, armed)
			if !nativeTransitionReceiptValid(receipt) {
				t.Fatal("no genuine completed storage boundary", receipt)
			}
			failed := nativeFaultReap(t, f.n.all[1], 1, dir, receipt)
			f.mu.Lock()
			f.intentionalKills[f.n.all[1]] = true
			f.mu.Unlock()
			f.elect(1, 2)
			p03Reconnect(f, 6)
			result := f.run(6, proposal)
			if result.decision != decision {
				t.Fatal("same-original recovery changed first decision")
			}
			step := p03Complete(f, 6, "edge-create", proposal, result, from, before)
			f.assertView(step.After, 3)
			pairedFaultAssertCDC(t, step, 3)
			assertLifecycleCandidates(t, step.RawObservation, 3)
			recovered := p06Observe(f, 6, graphReadFresh, graphCut{})
			var reopened p06Observation
			rejoined := f.replaceBeforeHeal(1, func(_ *graphProcess, _ processResponse) {
				reopened = p06Observe(f, 1, graphReadAt, cut)
				if !reflect.DeepEqual(old, reopened) {
					t.Fatal("same-directory old source changed before catch-up")
				}
			})
			retained := p06Observe(f, 6, graphReadAt, cut)
			if !reflect.DeepEqual(old, retained) {
				t.Fatal("old cut changed after mutation/recovery")
			}
			through := f.indexes()
			writeNativeFaultArtifact(t, f.n.dir, nativeFaultArtifact{Version: 1, Profile: nativeFaultProfile, Case: "storage-" + kind, Original: proposal.wire, Manifest: command.manifest, Before: old, Retained: retained, Recovered: recovered, Reopened: reopened, Processes: []pairedFaultProcess{failed, rejoined}, From: from, Through: through, Records: p06Records(f, from, through), IO: &receipt})
		})
	}
}
