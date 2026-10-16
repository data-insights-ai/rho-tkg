package graphapply

import (
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
	"sync"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/dist/replica"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

func TestNativeStateOwnerMatchesWholeNativeIdentityAndSpendsBeforeObservation(t *testing.T) {
	parent, err := newNativeBatchOwner(t.Context(), t.TempDir(), 1, [16]byte{1}, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	defer parent.close()
	o, err := newNativeStateOwner(parent)
	if err != nil {
		t.Fatal(err)
	}
	defer o.close()
	id := raftlog.StableStateIdentity{Applied: 3, SourceIndex: 7, SourceCutID: [32]byte{1}, ManifestID: [32]byte{2}, TargetBank: 1, TargetGeneration: 4}
	c := nativeStateControl{Version: 1, Action: "arm", Cookie: o.state.Cookie, Sequence: 1, Kind: raftlog.StableStateImportBegin, Side: "observe", Filter: nativeStateFilter{SourceIndex: id.SourceIndex, SourceCutID: id.SourceCutID}}
	call := func(c nativeStateControl) error {
		wire, err := nativeStateControlWire(c)
		if err != nil {
			return err
		}
		_, err = o.control(wire)
		return err
	}
	if err := call(c); err != nil {
		t.Fatal(err)
	}
	foreign := id
	foreign.SourceCutID[0] ^= 1
	if _, ok := o.StableStateSelection(c.Kind, foreign); ok {
		t.Fatal("foreign native source selected")
	}
	s, ok := o.StableStateSelection(c.Kind, id)
	w := raftlog.StableStorageWork{Entries: 3, Bytes: 1 << 20, OutputBytes: 2048}
	if !ok || s.Identity != id || !o.ReserveStableStateObservation(s, w) {
		t.Fatal("complete native identity/prepayment")
	}
	if err := call(c); err != nil || o.state.WorkBytes != w.Bytes {
		t.Fatal("replay refunded grant", err)
	}
	wrong := s
	wrong.Identity.TargetGeneration++
	if o.ReserveStableStateObservation(wrong, w) {
		t.Fatal("partial native identity accepted")
	}
	e := raftlog.StableStateObservation{Selection: s, Kind: c.Kind, Stage: 1, BeforeMetadataBytes: 100, MetadataBytes: 100, BeforeMetadataSHA256: [32]byte{3}, MetadataSHA256: [32]byte{4}, BatchBytes: 128, BatchPayloadSHA256: [32]byte{5}, Work: w}
	o.ObserveStableState(e)
	e.Stage, e.Synced = 2, true
	o.ObserveStableState(e)
	if !nativeStateReceiptValid(o.state, nativeStateReceipt{Cookie: o.state.Cookie, Run: o.state.Run, Member: o.state.Member, PID: o.state.PID, Sequence: c.Sequence, Control: c, DeadlineUnixNanos: o.state.DeadlineUnixNanos}) {
		t.Fatal("same-live observed pair")
	}
	bad := o.state
	bad.Events[1].BeforeMetadataSHA256[0] ^= 1
	if nativeStateReceiptValid(bad, o.state) {
		t.Fatal("prospective substituted as before metadata")
	}
	c.Sequence++
	c.Kind, c.Side = raftlog.StableStateGenerationClear, "observe"
	c.Filter = nativeStateFilter{MatchTarget: true, TargetBank: id.TargetBank, TargetGeneration: id.TargetGeneration}
	if err := call(c); err != nil {
		t.Fatal(err)
	}
	s, ok = o.StableStateSelection(c.Kind, id)
	if !ok || o.ReserveStableStateObservation(s, raftlog.StableStorageWork{Entries: 3, Bytes: 1<<42 + 1, OutputBytes: 2048}) || o.state.State != "unavailable" {
		t.Fatal("unfunded native traversal")
	}
	o.close()
	o.close()
	c.Sequence++
	if err := call(c); !errors.Is(err, replica.ErrUnavailable) {
		t.Fatal("closed owner revived", err)
	}
	if c.Filter.matches(foreign) == false {
		t.Fatal("clear selector accidentally claims unrelated source certification")
	}
	if (nativeStateFilter{SourceIndex: 7, SourceCutID: id.SourceCutID, Sequence: 1}).valid(raftlog.StableStateImportBegin) {
		t.Fatal("ignored filter field")
	}
	if (nativeStateFilter{Applied: 1}).valid(raftlog.StableStateInitialize) || (nativeStateFilter{}).valid(raftlog.StableStateInitialize) {
		t.Fatal("import composition pretended to admit genesis initialization")
	}
	if _, ok := any(&nativeBatchFS{}).(raftlog.StableStateObserver); ok {
		t.Fatal("legacy FS gained observer work")
	}
	if _, err := decodeNativeStateControl([]byte(nativeStateMarker + `{"Version":1,"Action":"inspect","extra":true}`)); !errors.Is(err, errInvalid) {
		t.Fatal("open diagnostic grammar", err)
	}
}

type nativeStateArtifact struct {
	nativeFaultArtifact
	Kind                            raftlog.StableStateKind
	Side                            string
	Run                             [16]byte
	DeadlineUnixNanos               int64
	Arm, State                      nativeStateReceipt
	Warm                            *nativeStateReceipt
	Snapshot                        []nativeFaultSnapshotBoundary
	SelectedSnapshot                int
	ReceiptReads, ReceiptGrantBytes uint64
}

func nativeStateSnapshotCase(t *testing.T, kind raftlog.StableStateKind, side string) {
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
			t.Fatal("original state run", err)
		}
		copy(run[:], wire)
	}
	if os.Getenv("RHO_GRAPH_TRANSITION_DEADLINE") == "" {
		t.Setenv("RHO_GRAPH_TRANSITION_DEADLINE", strconv.FormatInt(time.Now().Add(3*time.Minute).UnixNano(), 10))
	}
	deadline, err := strconv.ParseInt(os.Getenv("RHO_GRAPH_TRANSITION_DEADLINE"), 10, 64)
	if err != nil || strconv.FormatInt(deadline, 10) != os.Getenv("RHO_GRAPH_TRANSITION_DEADLINE") || deadline <= time.Now().UnixNano() || deadline-time.Now().UnixNano() > int64(1800*time.Second) || run == ([16]byte{}) {
		t.Fatal("fixed state deadline/run", err)
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
	prior := f.n.all[3].call(t, processRequest{op: processStateOp})
	processes := []pairedFaultProcess{f.kill(3, "replica")}
	f.n.groups[0].blocked = map[uint64]bool{3: true}
	defer func() { f.n.groups[0].blocked = nil }()
	proposal, from := f.proposal(3), f.indexes()
	capture, err := nativeFaultCaptureManifest(f.n.all[1].mux.hub, 5)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = capture() }()
	result := f.run(5, proposal)
	businessManifest, err := capture()
	if err != nil {
		t.Fatal(err)
	}
	published := f.n.all[1].call(t, processRequest{op: processPublish})
	if published.stats[statCheckpoint] <= prior.applied {
		t.Fatal("genuine publication must pass lagged prefix")
	}
	phase := map[raftlog.StableStateKind]string{raftlog.StableStateImportBegin: "before-begin", raftlog.StableStateImportAppend: "before-append", raftlog.StableStateImportSeal: "before-verify", raftlog.StableStateActivation: "before-activate", raftlog.StableStateImportRetire: "before-close", raftlog.StableStateGenerationClear: "before-close"}[kind]
	if phase == "" {
		t.Fatal("unsupported actual state recipe")
	}
	f.nativeSnapshotState = phase
	var arm nativeStateReceipt
	var receipt nativeStateReceipt
	var warm *nativeStateReceipt
	var notices []nativeFaultSnapshotBoundary
	var manifestID [32]byte
	injected := false
	selectedSnapshot := -1
	var reads uint64
	var mu sync.Mutex
	readExpected := func(expected nativeStateReceipt) (nativeStateReceipt, error) {
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
		return nativeStateRead(filepath.Join(f.n.dir, "3"), expected) // Prepaid128KiB per16KiB authentication.
	}
	hub := f.n.all[1].mux.hub
	hub.mu.Lock()
	if hub.failedStep != nil {
		hub.mu.Unlock()
		t.Fatal("state owner already installed")
	}
	hub.failedStep = func(job graphProcessPacketJob, actual error) bool {
		if actual == nil || job.packet.To != 3 {
			return false
		}
		mu.Lock()
		expected := arm
		mu.Unlock()
		if expected.Sequence == 0 || hub.peer(3).p.cmd.Process.Pid != int(expected.PID) {
			return false
		}
		if _, err := readExpected(expected); err != nil {
			return false
		}
		f.mu.Lock()
		f.down[3] = true
		f.mu.Unlock()
		return true
	}
	hub.mu.Unlock()
	clear := sync.OnceFunc(func() { hub.mu.Lock(); hub.failedStep = nil; hub.mu.Unlock() })
	defer clear()
	f.nativeSnapshotHook = func(b nativeFaultSnapshotBoundary) bool {
		if b.Donor != 1 || b.Receiver != 3 {
			t.Fatal("actual state snapshot escaped selected source")
		}
		notices = append(notices, b)
		if injected {
			return true
		}
		p := f.n.all[3]
		owner, err := nativeStateCall(ctx, p, nativeStateControl{Version: 1, Action: "inspect"})
		if err != nil || owner.Run != run || owner.Member != 3 || owner.DeadlineUnixNanos > deadline {
			t.Fatal("actual state owner", err)
		}
		if b.Phase == "after-begin" {
			r, err := decodeProcessResponse(b.ResponseWire)
			if err != nil || len(r.aux) != 32 || len(r.handles) != 1 || r.handles[0] != b.Owner || b.Owner == 0 {
				t.Fatal("actual Begin ACK/owner", err)
			}
			copy(manifestID[:], r.aux)
			if kind == raftlog.StableStateGenerationClear || kind == raftlog.StableStateImportRetire {
				if !nativeStateReceiptValid(owner, owner) || owner.State != "observed" || owner.Events[0].Selection.Identity.ManifestID != manifestID {
					t.Fatal("same-live native Begin target missing")
				}
				copy := owner
				warm = &copy
			}
			return true
		}
		warmBegin := b.Phase == "before-begin" && phase == "before-close"
		if b.Phase != phase && !warmBegin {
			return true
		}
		// Decode the actual donor manifest against this exact existing source
		// binding. This observation never supplies a native Begin/claim handle.
		ns, _ := certifiedProcessNamespace(1)
		spec := partitionCodecRequest(t, 1, defaultMaterializerLimits())
		part, _ := spec.declaration.Partition(ns.partition)
		m, err := raftlog.DecodeApplicationSnapshotManifestForBinding(b.Manifest, 172, raftlog.ApplicationBinding{Identity: raftlog.ApplicationIdentity{Graph: [16]byte(ns.graph), Partition: ns.partition, Group: part.Group}, SemanticContractID: certifiedGraphSemanticContractID()})
		if err != nil || m.Version != 5 {
			t.Fatal("actual state source manifest", err)
		}
		filter := nativeStateFilter{SourceIndex: m.Index, SourceCutID: m.CutID, ManifestID: manifestID}
		selectedKind, selectedSide := kind, side
		if warmBegin {
			selectedKind, selectedSide = raftlog.StableStateImportBegin, "observe"
			filter.ManifestID = [32]byte{}
		}
		if kind == raftlog.StableStateGenerationClear && !warmBegin {
			if warm == nil {
				t.Fatal("clear requires genuine same-live Begin target")
			}
			id := warm.Events[0].Selection.Identity
			filter = nativeStateFilter{MatchTarget: true, TargetBank: id.TargetBank, TargetGeneration: id.TargetGeneration}
		}
		if kind == raftlog.StableStateImportAppend && !warmBegin {
			chunk, err := decodePairedProcessChunk(b.Chunk)
			if err != nil || chunk.Sequence != 0 || chunk.ManifestID != manifestID {
				t.Fatal("first actual bounded Append", err)
			}
			filter.MatchSequence, filter.Sequence = true, 1 // Native identity is the successful prospective next cursor.
		}
		control := nativeStateControl{Version: 1, Action: "arm", Cookie: owner.Cookie, Sequence: owner.Sequence + 1, Kind: selectedKind, Side: selectedSide, Filter: filter}
		armed, err := nativeStateCall(ctx, p, control)
		if err != nil || armed.State != "armed" || armed.Control != control {
			t.Fatal("genuine state arm", err)
		}
		if warmBegin {
			return true
		}
		mu.Lock()
		arm = armed
		mu.Unlock()
		selectedSnapshot = len(notices) - 1
		op := processRequest{index: b.Owner}
		switch kind {
		case raftlog.StableStateImportBegin:
			op.op, op.data = processBeginImport, b.Manifest
		case raftlog.StableStateImportAppend:
			op.op, op.data = processAppend, b.Chunk
		case raftlog.StableStateImportSeal:
			op.op = processVerify
		case raftlog.StableStateActivation:
			op.op, op.packet = processActivate, b.Packet
		case raftlog.StableStateImportRetire, raftlog.StableStateGenerationClear:
			op.op = processImportClose
		}
		r, transport := p.exchange(ctx, op)
		if !nativeBatchFailedStepAllowed(r, transport) {
			t.Fatal("state selector did not cause genuine target transport loss", transport, processResponseError(r))
		}
		receipt, err = readExpected(armed)
		if err != nil {
			t.Fatal("unmatched/expired state cannot qualify", err)
		}
		processes = append(processes, nativeTransitionReap(t, f, 3, receipt.Member, receipt.PID, receipt, "native-state-transition-kill"))
		f.n.groups[0].blocked = map[uint64]bool{3: true}
		injected = true
		f.nativeSnapshotState = ""
		return false
	}
	defer func() { f.nativeSnapshotHook = nil; f.nativeSnapshotState = "" }()
	interrupted := f.replaceBeforeHeal(3, func(_ *graphProcess, _ processResponse) { f.n.groups[0].blocked = nil })
	if !injected || interrupted.Ready || interrupted.Action != "reopen-interrupted" {
		t.Fatal("armed state was not observed in actual import")
	}
	processes = append(processes, interrupted)
	clear()
	var reopened p06Observation
	rejoined := f.replaceBeforeHeal(3, func(_ *graphProcess, state processResponse) {
		f.n.groups[0].blocked = nil
		if state.applied < prior.applied {
			t.Fatal("state interruption lost old acknowledged prefix")
		}
		if kind == raftlog.StableStateActivation && side == "after" {
			id := receipt.Events[0].Selection.Identity
			if state.stats[statCheckpoint] < id.Applied || state.cutID != id.SourceCutID || state.manifestID != id.ManifestID {
				t.Fatal("acknowledged native activation batch missing before catch-up")
			}
		}
		f.reconnect(2, 3)
		reopened = p06Observe(f, 2, graphReadAt, cut)
		if !reflect.DeepEqual(old, reopened) {
			t.Fatal("replacement local old cut changed before catch-up")
		}
	})
	processes = append(processes, rejoined)
	if len(notices) < 3 || notices[len(notices)-1].Phase != "after-activate" {
		t.Fatal("genuine retry lacks EOF/activation")
	}
	step := p03Complete(f, 6, "edge-create", proposal, result, from, before)
	f.assertView(step.After, 3)
	pairedFaultAssertCDC(t, step, 3)
	assertLifecycleCandidates(t, step.RawObservation, 3)
	recovered := p06Observe(f, 6, graphReadFresh, graphCut{})
	retained := p06Observe(f, 6, graphReadAt, cut)
	if !reflect.DeepEqual(old, retained) {
		t.Fatal("state recovery substituted current for old")
	}
	through := f.indexes()
	mu.Lock()
	actualReads := reads
	mu.Unlock()
	artifact := nativeStateArtifact{nativeFaultArtifact: nativeFaultArtifact{Version: 4, Profile: nativeStateProfile, Case: fmt.Sprintf("state-import-k%d-%s", kind, side), Original: proposal.wire, Manifest: businessManifest, Before: old, Retained: retained, Recovered: recovered, Reopened: reopened, From: from, Through: through, Records: p06Records(f, from, through), Processes: processes}, Kind: kind, Side: side, Run: run, DeadlineUnixNanos: deadline, Arm: arm, State: receipt, Warm: warm, Snapshot: notices, SelectedSnapshot: selectedSnapshot, ReceiptReads: actualReads, ReceiptGrantBytes: actualReads * 131072}
	path := nativeStateWrite(t, filepath.Join(f.n.dir, "native-"+artifact.Case+".jsonl"), artifact)
	check := exec.CommandContext(ctx, "python3", "-B", filepath.Join("..", "..", "..", "..", "tools", "v5-substrate", "native_transition_state_check.py"), path)
	var diagnostic processStderr
	check.Stdout, check.Stderr = &diagnostic, &diagnostic
	if err := check.Run(); err != nil {
		body, _ := diagnostic.state()
		t.Fatal("independent actual state/prefix/serial checker", err, body)
	}
	if _, overflow := diagnostic.state(); overflow {
		t.Fatal("state checker diagnostic bound")
	}
}

// Twelve actual OS-child cells. The same native import lifecycle supplies every
// batch, including unclaimed retirement and last-reference physical cleanup.
// GC/reader, genesis startup and whole P01–P09 conjunction remain separate.
func TestNativeStateImportSyncBoundariesRecoverGenuineSourceAndRetainedCut(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for _, kind := range []raftlog.StableStateKind{raftlog.StableStateImportBegin, raftlog.StableStateImportAppend, raftlog.StableStateImportSeal, raftlog.StableStateActivation, raftlog.StableStateImportRetire, raftlog.StableStateGenerationClear} {
		for _, side := range []string{"before", "after"} {
			t.Run(fmt.Sprintf("k%d-%s", kind, side), func(t *testing.T) { nativeStateSnapshotCase(t, kind, side) })
		}
	}
}

func nativeStateWrite(t *testing.T, path string, a nativeStateArtifact) string {
	t.Helper()
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if file != nil {
			if err := file.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	digest, total := sha256.New(), 0
	write := func(row any, sealed bool) {
		b, err := json.Marshal(row)
		if err != nil || len(b)+1 > 512<<10 || len(b)+1 > (16<<20)-total {
			t.Fatal("finite state artifact", err)
		}
		b = append(b, '\n')
		total += len(b)
		if sealed {
			_, _ = digest.Write(b)
		}
		if n, err := file.Write(b); err != nil || n != len(b) {
			t.Fatal("state artifact write", err)
		}
	}
	records, snapshots := a.Records, a.Snapshot
	a.Records, a.Snapshot = nil, nil
	write(struct {
		Type     string
		Artifact nativeStateArtifact
	}{"header", a}, true)
	if len(records) > 512 || len(snapshots) > 16 {
		t.Fatal("state source/notice bound")
	}
	for _, record := range records {
		write(struct {
			Type   string
			Record lifecycleRawRecord
		}{"record", record}, true)
	}
	for i, snapshot := range snapshots {
		write(struct {
			Type     string
			Sequence int
			Boundary nativeFaultSnapshotBoundary
		}{"snapshot-boundary", i, snapshot}, true)
	}
	write(struct {
		Type               string
		Rows, SnapshotRows int
		Through            [2]uint64
		PayloadSHA256      string
	}{"footer", len(records), len(snapshots), a.Through, hex.EncodeToString(digest.Sum(nil))}, false)
	if err := errors.Join(file.Sync(), file.Close()); err != nil {
		t.Fatal(err)
	}
	file = nil
	t.Log("actual state/process source artifact", path)
	return path
}
