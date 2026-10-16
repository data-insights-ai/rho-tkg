package graphapply

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

type nativeRetentionCall struct {
	Member            uint64
	Request, Response []byte
	Lost              bool
}
type nativeRetentionPoint struct {
	Cut      lifecycleRawCut
	Response []byte
}
type nativeRetentionArtifact struct {
	Version                                     uint8
	Profile, Side                               string
	Run                                         [16]byte
	DeadlineUnixNanos                           int64
	Old, Current, Reopened                      nativeRetentionPoint
	Attempts                                    []nativeGraphCostAttempt
	Records                                     []lifecycleRawRecord
	From, Through                               [2]uint64
	Calls                                       []nativeRetentionCall
	Arm, State                                  nativeStateReceipt
	Processes                                   []pairedFaultProcess
	BeforeState, LocalState, AfterState         []byte
	OwnerCalls, ReceiptReads, ReceiptGrantBytes uint64
}

func TestNativeRetentionStateDoorKeepsIgnoredFieldsAndMissingOwnersInert(t *testing.T) {
	if _, err := nativeTransitionPinProjection(t.Context(), nil, nil); !errors.Is(err, errInvalid) {
		t.Fatal("borrowed native snapshot requires an actual live owner", err)
	}
	if (nativeProtocolStateRecipe{Family: "gc", Partition: 3, Side: "before", Kind: raftlog.StableStorageReady}).valid() {
		t.Fatal("GC cursor transition was relabelled as a Ready entry")
	}
	t.Setenv("RHO_GRAPH_STATE_RETENTION_PROCESS", "off")
	if o, err := newNativeRetentionOwner(nil, nil); o != nil || err != nil {
		t.Fatal("default created a native owner", err)
	}
	t.Setenv("RHO_GRAPH_STATE_RETENTION_PROCESS", "1")
	if _, err := newNativeRetentionOwner(nil, nil); !errors.Is(err, errInvalid) {
		t.Fatal("nil owner", err)
	}
	c := nativeRetentionControl{Version: 1, Action: "inspect"}
	wire, err := nativeRetentionWire(c)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeNativeRetentionControl(wire); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []nativeRetentionControl{{Version: 1, Action: "inspect", Index: 1}, {Version: 1, Action: "submit", Original: []byte{1}}, {Version: 1, Action: "recover"}, {Version: 1, Action: "prepare"}, {Version: 1, Action: "open"}} {
		b, err := nativeRetentionWire(bad)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := decodeNativeRetentionControl(b); !errors.Is(err, errInvalid) {
			t.Fatal("ignored/inert fields became authority", err)
		}
	}
	var absent *nativeRetentionOwner
	if _, _, err := absent.event(wire); !errors.Is(err, errInvalid) {
		t.Fatal("missing native owner", err)
	}
	if err := absent.close(); err != nil {
		t.Fatal(err)
	}
}

func nativeRetentionPointAt(t *testing.T, n *certifiedProcessNetwork, actor uint64, mode graphReadMode, cut graphCut, want int64) (nativeRetentionPoint, graphCut) {
	t.Helper()
	request, err := encodeGraphE15ReadRequest(graphE15ReadRequest{mode: mode, cut: cut})
	if err != nil {
		t.Fatal(err)
	}
	opened := n.all[actor].call(t, processRequest{op: processSourceCertifiedOpen, data: request})
	handle, actual, err := decodeGraphE15ReadToken(opened.aux)
	if err != nil || handle == 0 || len(actual.certificates) != 2 {
		t.Fatal("genuine AS7 full cut", err)
	}
	response := n.all[actor].call(t, processRequest{op: processCostObserve, index: handle, entity: 7})
	n.all[actor].call(t, processRequest{op: processSourceCertifiedClose, index: handle})
	var entity nativeLifecycleEntity
	if err := json.Unmarshal(response.aux, &entity); err != nil || entity.ID != 7 || !entity.Found || entity.Kind != 2 || entity.Source != 1 || entity.Target != 9 || entity.Mode != 1 || entity.Type != "edge" || !entity.Effective || entity.EffectiveLife != 8 || len(entity.Labels) != 0 || !entity.Property.Present || entity.Property.Null || entity.Property.Kind != "i64" || entity.Property.Handle != uint64(want) || entity.Property.I64 != want {
		t.Fatal("independent literal useful edge/property", entity, err)
	}
	for _, c := range actual.certificates {
		if c.scope.semantic != durableColumnGraphSemanticContractID() {
			t.Fatal("foreign source agreement")
		}
	}
	raw, err := lifecycleRawCertificate(actual)
	if err != nil {
		t.Fatal(err)
	}
	return nativeRetentionPoint{raw, p06ProcessWire(t, response)}, actual
}

func nativeRetentionReplace(t *testing.T, n *certifiedProcessNetwork, prior *graphProcess, spec nativeGraphCostSpec) (*graphProcess, pairedFaultProcess) {
	t.Helper()
	if !prior.closed || prior.cmd.ProcessState == nil {
		t.Fatal("unreaped retention owner")
	}
	body, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	p := startGraphProcess(t, filepath.Join(n.dir, "1"), 1, "RHO_GRAPH_CERTIFIED_PROCESS=1", "RHO_GRAPH_SOURCE_PROCESS=1", "RHO_GRAPH_DURABLE_COLUMN_PROCESS=1", "RHO_GRAPH_PROCESS_REOPEN=1", "RHO_GRAPH_COST_SPEC="+string(body))
	hub := prior.mux.hub
	mux := &graphProcessMux{p: p, id: 1, hub: hub, pending: make(map[uint64]chan graphProcessAnswer), relays: make(map[uint64]context.CancelFunc), relayQueue: make(chan graphProcessRelayJob, processSourceWorkers)}
	p.mux = mux
	hub.mu.Lock()
	hub.peers[1] = mux
	hub.mu.Unlock()
	n.all[1] = p
	n.groups[0].nodes[1] = p
	hub.wg.Go(mux.readLoop)
	for range processSourceWorkers {
		hub.wg.Go(mux.relayLoop)
	}
	state := p.call(t, processRequest{op: processStateOp})
	wire := p06ProcessWire(t, state)
	volume := filepath.Join(n.dir, "1")
	raw, err := json.Marshal(struct {
		Member, PID                        uint64
		Instance, PreviousInstance, Volume string
		Ready                              bool
		LocallyRestoredRaw                 []byte
	}{1, uint64(p.cmd.Process.Pid), pairedGraphFaultInstance(1, p), pairedGraphFaultInstance(1, prior), volume, true, wire})
	if err != nil {
		t.Fatal(err)
	}
	return p, pairedFaultProcess{EventID: 2, Action: "native-retention-local-reopen", Role: "replica", Member: 1, Instance: pairedGraphFaultInstance(1, p), PreviousInstance: pairedGraphFaultInstance(1, prior), Volume: volume, Ready: true, Raw: raw}
}

func nativeRetentionCase(t *testing.T, side string) {
	t.Helper()
	var run [16]byte
	if _, err := io.ReadFull(rand.Reader, run[:]); err != nil {
		t.Fatal(err)
	}
	// The exact same admitted deadline/run reaches the original AND replacement.
	if os.Getenv("RHO_GRAPH_TRANSITION_RUN") != "" {
		b, err := hex.DecodeString(os.Getenv("RHO_GRAPH_TRANSITION_RUN"))
		if err != nil || len(b) != 16 || hex.EncodeToString(b) != os.Getenv("RHO_GRAPH_TRANSITION_RUN") {
			t.Fatal("retention run", err)
		}
		copy(run[:], b)
	} else {
		t.Setenv("RHO_GRAPH_TRANSITION_RUN", hex.EncodeToString(run[:]))
	}
	if os.Getenv("RHO_GRAPH_TRANSITION_DEADLINE") == "" {
		t.Setenv("RHO_GRAPH_TRANSITION_DEADLINE", strconv.FormatInt(time.Now().Add(3*time.Minute).UnixNano(), 10))
	}
	deadline, err := strconv.ParseInt(os.Getenv("RHO_GRAPH_TRANSITION_DEADLINE"), 10, 64)
	if err != nil || deadline <= time.Now().UnixNano() || deadline-time.Now().UnixNano() > int64(1800*time.Second) || strconv.FormatInt(deadline, 10) != os.Getenv("RHO_GRAPH_TRANSITION_DEADLINE") || run == ([16]byte{}) {
		t.Fatal("fixed retention deadline", err)
	}
	t.Setenv("RHO_GRAPH_STABLE_STORAGE_PROCESS", "1")
	t.Setenv("RHO_GRAPH_STABLE_STATE_PROCESS", "1")
	t.Setenv("RHO_GRAPH_STATE_RETENTION_PROCESS", "1")
	ctx, cancel := context.WithDeadline(t.Context(), time.Unix(0, deadline))
	defer cancel()
	spec := nativeGraphCostSpec{Version: 1, Facts: 1, CrossPercent: 100, Writers: 1}
	n := newNativeColumnProcessNetwork(t, spec)
	var from [2]uint64
	for g, member := range []uint64{1, 4} {
		from[g] = n.all[member].call(t, processRequest{op: processStateOp}).applied
	}
	var attempts []nativeGraphCostAttempt
	submit := func(phase uint64) {
		events := make(chan nativeGraphCostClientEvent, 16)
		_, err := nativeGraphCostClientSubmit(ctx, n, spec, phase, 0, true, 0, nil, events)
		if err != nil {
			t.Fatal("actual useful original", err)
		}
		for len(events) > 0 {
			e := <-events
			if e.attempt != nil {
				if e.attempt.Classification != "commit" {
					t.Fatal("unexpected useful attempt refusal", e.attempt.Classification)
				}
				attempts = append(attempts, *e.attempt)
			}
		}
	}
	for phase := uint64(1); phase <= 4; phase++ {
		submit(phase)
	}
	old, oldCut := nativeRetentionPointAt(t, n, 2, graphReadFresh, graphCut{}, 5)
	submit(5)
	current, currentCut := nativeRetentionPointAt(t, n, 2, graphReadFresh, graphCut{}, 6)
	var oldHome, currentHome graphCertificate
	for _, c := range oldCut.certificates {
		if c.scope.partition == 3 {
			oldHome = c
		}
	}
	for _, c := range currentCut.certificates {
		if c.scope.partition == 3 {
			currentHome = c
		}
	}
	if oldHome.sourceIndex == 0 || currentHome.sourceIndex <= oldHome.sourceIndex || len(attempts) != 5 {
		t.Fatal("distinct actual historical/current source and originals")
	}
	var through [2]uint64
	for g, member := range []uint64{1, 4} {
		through[g] = n.all[member].call(t, processRequest{op: processStateOp}).applied
	}
	var records []lifecycleRawRecord
	for g, member := range []uint64{1, 4} {
		if through[g] > 128 {
			t.Fatal("finite retained pre-floor prefix")
		}
		for index := from[g] + 1; index <= through[g]; index++ {
			r := n.all[member].call(t, processRequest{op: processEntryRecord, index: index})
			records = append(records, lifecycleRawRecord{Partition: []uint64{3, 8}[g], Index: index, Outcome: bytes.Clone(r.outcome), CDC: bytes.Clone(r.changes)})
		}
	}
	var calls []nativeRetentionCall
	var total uint64
	call := func(p *graphProcess, c nativeRetentionControl) (nativeRetentionReceipt, processResponse, error) {
		if total >= 128 {
			t.Fatal("whole-query native control allowance")
		}
		total++
		wire, err := nativeRetentionWire(c)
		if err != nil {
			t.Fatal(err)
		}
		r, transport := p.exchange(ctx, processRequest{op: processNativeFaultControl, data: wire})
		lost := nativeBatchFailedStepAllowed(r, transport)
		calls = append(calls, nativeRetentionCall{Member: 1, Request: wire, Response: p06ProcessWire(t, r), Lost: lost})
		var receipt nativeRetentionReceipt
		if transport == nil && (len(r.aux) > 0 || r.code == 0) {
			d := json.NewDecoder(bytes.NewReader(r.aux))
			d.DisallowUnknownFields()
			if err := d.Decode(&receipt); err != nil || receipt.Version != 1 || receipt.Profile != nativeRetentionProfile || receipt.PID != uint64(p.cmd.Process.Pid) || receipt.Run != run || receipt.DeadlineUnixNanos > deadline {
				t.Fatal("actual bounded native GC owner", err)
			}
		}
		return receipt, r, errors.Join(transport, processResponseError(r))
	}
	target := n.all[1]
	owner, _, err := call(target, nativeRetentionControl{Version: 1, Action: "inspect"})
	if err != nil {
		t.Fatal(err)
	}
	sequence := uint64(0)
	control := func(action string) nativeRetentionControl {
		sequence++
		return nativeRetentionControl{Version: 1, Action: action, Cookie: owner.Cookie, Sequence: sequence}
	}
	c := control("prepare")
	if _, err := io.ReadFull(rand.Reader, c.Request[:]); err != nil {
		t.Fatal(err)
	}
	floor := currentHome.sourceIndex
	c.Floors = [6]uint64{floor, floor, floor + 1, floor, floor, currentHome.index}
	prepared, _, err := call(target, c)
	if err != nil || len(prepared.Original) == 0 {
		t.Fatal("genuine local GGC original", err)
	}
	_, submitted, err := call(target, control("submit"))
	if err != nil || len(submitted.packets) == 0 {
		t.Fatal("real proposal/ACK packets missing", err)
	}
	// The real quorum packets remain owned by the parent until AFTER this open.
	c = control("open")
	c.Index = oldHome.sourceIndex
	late, _, err := call(target, c)
	if err != nil || late.Root.Index != oldHome.sourceIndex || !bytes.Equal(late.Root.Image, oldHome.image[:]) {
		t.Fatal("actual late captured old root", err)
	}
	n.groups[0].enqueue(t, submitted)
	n.groups[0].drain(t)
	original, _, err := call(target, control("original"))
	if err != nil || !original.Found {
		t.Fatal("actual immutable installed GGC", err)
	}
	result, err := decodeGraphRetentionResult(original.Outcome)
	if err != nil || result.status != graphRetentionApplied || result.id != sha256.Sum256(prepared.Original) || result.index <= late.ViewIndex || result.source < late.ViewIndex {
		t.Fatal("actual plan original/late-view ordering", err)
	}
	// Actual native helper/control entries, if any, are retained too. The fixed
	// raw prefix ends at the installed original, never a guessed +1 endpoint.
	if result.index > 128 || result.index < through[0] {
		t.Fatal("finite complete installed-plan prefix")
	}
	for index := through[0] + 1; index <= result.index; index++ {
		r := target.call(t, processRequest{op: processEntryRecord, index: index})
		records = append(records, lifecycleRawRecord{Partition: 3, Index: index, Outcome: bytes.Clone(r.outcome), CDC: bytes.Clone(r.changes)})
	}
	through[0] = result.index
	c = control("root")
	c.Index = oldHome.sourceIndex
	held, _, err := call(target, c)
	if err != nil || !bytes.Equal(held.Root.Image, late.Root.Image) || held.Root.ImageHash != late.Root.ImageHash {
		t.Fatal("committed floor revoked captured native reader", err)
	}
	blocked, rawBlocked, err := call(target, control("collect"))
	if !errors.Is(err, raftlog.ErrLimit) || rawBlocked.code != 2 || blocked.Progress != (raftlog.ApplicationRetentionProgress{}) || blocked.Work != (raftlog.ApplicationProofWork{}) {
		t.Fatal("collector ignored genuine late reader", err)
	}
	if _, _, err := call(target, control("close")); err != nil {
		t.Fatal(err)
	}
	beforeState := target.call(t, processRequest{op: processStateOp})
	beforeWire := p06ProcessWire(t, beforeState)
	stateOwner, err := nativeStateCall(ctx, target, nativeStateControl{Version: 1, Action: "inspect"})
	if err != nil {
		t.Fatal(err)
	}
	arm, err := nativeStateCall(ctx, target, nativeStateControl{Version: 1, Action: "arm", Cookie: stateOwner.Cookie, Sequence: 1, Kind: raftlog.StableStateRetentionPage, Side: side, Filter: nativeStateFilter{SourceIndex: result.source, PlanID: result.id}})
	if err != nil {
		t.Fatal(err)
	}
	_, lostReply, err := call(target, control("collect"))
	if !nativeBatchFailedStepAllowed(lostReply, err) {
		t.Fatal("native collection did not cause matched process loss", err)
	}
	receiptReads, receiptGrant := uint64(1), uint64(131072) // Admit the complete local read/decode/hash BEFORE its IO.
	if err := ctx.Err(); err != nil {
		t.Fatal(err)
	}
	state, err := nativeStateRead(filepath.Join(n.dir, "1"), arm)
	if err != nil {
		t.Fatal("actual GC sync receipt", err)
	}
	// This facade supplies only existing actual process ownership to the shared
	// OS reaper. It constructs no Host, Store, snapshot, graph proof or outcome.
	processOwner := &pairedGraphFaultNetwork{t: t, n: n, down: make(map[uint64]bool), intentionalKills: make(map[*graphProcess]bool)}
	failed := nativeTransitionReap(t, processOwner, 1, state.Member, state.PID, state, "native-state-transition-kill")
	replacement, rejoined := nativeRetentionReplace(t, n, target, spec)
	localState := replacement.call(t, processRequest{op: processStateOp})
	if localState.applied != beforeState.applied || !bytes.Equal(localState.image, beforeState.image) || localState.stats[statGeneration] != beforeState.stats[statGeneration] || localState.stats[statViews] != 0 {
		t.Fatal("same-dir native floor/cursor restart changed logical state")
	}
	fresh, _, err := call(replacement, nativeRetentionControl{Version: 1, Action: "inspect"})
	if err != nil || fresh.Cookie == owner.Cookie {
		t.Fatal("lost owner claimed portable authority", err)
	}
	// Foreign old cookie is explicitly refused before authenticating the original.
	bad := nativeRetentionControl{Version: 1, Action: "recover", Cookie: owner.Cookie, Sequence: 1, Original: prepared.Original}
	_, badReply, badErr := call(replacement, bad)
	if badErr == nil || badReply.code != 3 {
		t.Fatal("lost live owner accepted after restart", badErr)
	}
	owner = fresh
	sequence = 0
	c = control("recover")
	c.Original = prepared.Original
	recovered, _, err := call(replacement, c)
	if err != nil || !recovered.Found || !bytes.Equal(recovered.Outcome, original.Outcome) || !bytes.Equal(recovered.CDC, original.CDC) {
		t.Fatal("same installed original changed after restart", err)
	}
	var progress raftlog.ApplicationRetentionProgress
	for page := 0; !progress.Complete && page < 64; page++ {
		r, _, err := call(replacement, control("collect"))
		if err != nil || r.Progress.PlanID != result.id || r.Progress.SourceIndex != result.source || r.Progress.InstalledIndex != result.index || r.Progress.Section < progress.Section || r.Work.Records > 16 || r.Work.Bytes > 1<<20 {
			t.Fatal("native persisted cursor resume", err)
		}
		progress = r.Progress
	}
	if !progress.Complete {
		t.Fatal("finite actual GC EOF unavailable")
	}
	now, _, err := call(replacement, control("current"))
	if err != nil || now.Plan.ID != result.id || now.Plan.RootFloor != floor || !bytes.Equal(now.Root.Image, beforeState.image) {
		t.Fatal("native current/plan changed", err)
	}
	anchor, _, err := call(replacement, control("anchor"))
	if err != nil || len(anchor.Plan.Roots) != 1 || anchor.Root.Index != anchor.Plan.Roots[0].Index || anchor.Root.ImageHash != anchor.Plan.Roots[0].ImageHash {
		t.Fatal("actual semantic genesis anchor", err)
	}
	c = control("open")
	c.Index = oldHome.sourceIndex
	expired, expiredRaw, err := call(replacement, c)
	if !errors.Is(err, raftlog.ErrApplicationExpired) || expiredRaw.code != 9 || expired.Root.Index != 0 || expired.Root.Image != nil {
		t.Fatal("below-floor old root became current/empty", err)
	}
	if _, _, err := call(replacement, control("close")); err != nil {
		t.Fatal(err)
	}
	afterState := replacement.call(t, processRequest{op: processStateOp})
	if afterState.stats[statActiveRecords] >= beforeState.stats[statActiveRecords] || afterState.stats[statViews] != 0 {
		t.Fatal("real physical collection/reader release absent")
	}
	// Actor3 has never bound the former member1 source stream. Actual At now
	// reads replacement1 under a fresh live association, without a new cut.
	reopened, _ := nativeRetentionPointAt(t, n, 3, graphReadAt, currentCut, 6)
	// Ordinary quorum progress and graceful cleanup follow the local proofs.
	n.groups[0].event(t, 1, processRequest{op: processCampaign})
	a := nativeRetentionArtifact{Version: 1, Profile: nativeRetentionProfile, Side: side, Run: run, DeadlineUnixNanos: deadline, Old: old, Current: current, Reopened: reopened, Attempts: attempts, Records: records, From: from, Through: through, Calls: calls, Arm: arm, State: state, Processes: []pairedFaultProcess{failed, rejoined}, BeforeState: beforeWire, LocalState: p06ProcessWire(t, localState), AfterState: p06ProcessWire(t, afterState), OwnerCalls: total, ReceiptReads: receiptReads, ReceiptGrantBytes: receiptGrant}
	path := nativeRetentionWrite(t, filepath.Join(n.dir, "native-retention-"+side+".jsonl"), a)
	check := exec.CommandContext(ctx, "python3", "-B", filepath.Join("..", "..", "..", "..", "tools", "v5-substrate", "native_transition_retention_check.py"), path)
	var diagnostic processStderr
	check.Stdout, check.Stderr = &diagnostic, &diagnostic
	if err := check.Run(); err != nil {
		body, _ := diagnostic.state()
		t.Fatal("independent genuine reader/floor/GC checker", err, body)
	}
	if _, overflow := diagnostic.state(); overflow {
		t.Fatal("retention checker diagnostic bound")
	}
}

func TestNativeRetentionReaderAndGCSyncKeepOriginalCursorAndCurrentAcrossRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for _, side := range []string{"before", "after"} {
		t.Run(side, func(t *testing.T) { nativeRetentionCase(t, side) })
	}
}

func nativeRetentionWrite(t *testing.T, path string, a nativeRetentionArtifact) string {
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
			t.Fatal("bounded retention artifact", err)
		}
		b = append(b, '\n')
		total += len(b)
		if sealed {
			_, _ = digest.Write(b)
		}
		if n, err := file.Write(b); err != nil || n != len(b) {
			t.Fatal("retention artifact write", err)
		}
	}
	records, calls := a.Records, a.Calls
	a.Records, a.Calls = nil, nil
	write(struct {
		Type     string
		Artifact nativeRetentionArtifact
	}{"header", a}, true)
	if len(records) > 256 || len(calls) > 128 {
		t.Fatal("finite complete GC source capture")
	}
	for _, r := range records {
		write(struct {
			Type   string
			Record lifecycleRawRecord
		}{"record", r}, true)
	}
	for i, c := range calls {
		write(struct {
			Type     string
			Sequence int
			Call     nativeRetentionCall
		}{"call", i, c}, true)
	}
	write(struct {
		Type          string
		Rows, Calls   int
		Through       [2]uint64
		PayloadSHA256 string
	}{"footer", len(records), len(calls), a.Through, hex.EncodeToString(digest.Sum(nil))}, false)
	if err := errors.Join(file.Sync(), file.Close()); err != nil {
		t.Fatal(err)
	}
	file = nil
	t.Log("actual retained reader/GC source", path)
	return path
}
