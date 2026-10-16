package graphapply

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

func TestNativePairedGraphFaultSixOperationsActualProcessesAndRetainedCuts(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	f := newPairedGraphFaultNetwork(t)
	initial := f.observe(4, graphReadFresh, graphCut{}, false)
	f.assertView(initial.View, 0)
	f.trace.InitialCut = initial.Cut
	base := f.indexes()
	before := initial
	names := []string{"node-a", "node-b", "edge-create", "precommit-update", "postcommit-update", "zone-loss-update"}
	proposals := make([]allocationProposal, 6)
	results := make([]crossProcessResult, 6)
	for stage := 1; stage <= 3; stage++ {
		p := f.proposal(stage)
		from := f.indexes()
		actor := uint64(4)
		if stage == 1 {
			actor = 1
		}
		result := f.run(actor, p)
		step := f.complete(names[stage-1], p, result, from, before)
		f.assertView(step.After, stage)
		pairedFaultAssertCDC(t, step, stage)
		assertLifecycleCandidates(t, step.RawObservation, stage)
		proposals[stage-1], results[stage-1] = p, result
		before = pairedFaultObserver(step)
	}
	seedCut, err := pairedFaultCut(f.trace.Steps[2].Cut)
	if err != nil {
		t.Fatal(err)
	}
	held, _ := f.open(4, graphReadAt, seedCut)
	heldFirst := nativeLifecycleRead(t, f.n, 4, held, 1)
	heldClosed := false
	defer func() {
		if !heldClosed {
			f.n.all[4].call(t, processRequest{op: processSourceCertifiedClose, index: held})
		}
	}()
	// Caller5 then6 are distinct still-live authenticated origins. Their rejoined
	// incarnations are voter-only; surviving service caches are never reset.
	for stage := 4; stage <= 5; stage++ {
		actor, recoverer := uint64(5), uint64(6)
		afterService := false
		caseName, marker := "precommit-uncertainty", "actual source-produced Decide before service delivery"
		if stage == 5 {
			actor, recoverer = 6, 4
			afterService = true
			caseName = "lost-committed-reply"
			marker = "actual service persisted COMMIT before caller delivery"
		}
		p := f.proposal(stage)
		from := f.indexes()
		entered, release := f.gate(actor, afterService)
		call, cancel := f.beginRun(actor, p)
		boundary := f.awaitBoundary(entered)
		var processEvents []pairedFaultProcess
		if afterService {
			reply, e := decodeGraphWireResponse(boundary.Reply)
			if e != nil || reply.err != nil {
				t.Fatal("actual pre-caller durable reply", reply.err, e)
			}
			decision, e := decodeCrossStatus(reply.body)
			if e != nil || decision.phase != crossDecide || !decision.commit || decision.ns.partition != 8 {
				t.Fatal("actual durable service D", decision, e)
			}
			command, e := decodePartitionGraphCommand(p.wire, defaultMaterializerLimits())
			if e != nil {
				t.Fatal(e)
			}
			f.packetDrain()
			record := f.n.all[4].call(t, processRequest{op: processEntryRecord, index: decision.index})
			if !bytes.Equal(record.outcome, reply.body) || len(record.changes) != 0 {
				t.Fatal("pre-kill service D is not actual stored original")
			}
			result := pairedFaultResult{Coordinator: 8, Request: hex.EncodeToString(command.request.id[:]), Transaction: hex.EncodeToString(decision.tx[:]), Round: decision.round}
			raw, _ := json.Marshal(struct {
				Response, Outcome []byte
				Partition, Index  uint64
			}{boundary.Reply, record.outcome, 8, decision.index})
			p4 := f.n.all[4]
			processEvents = append(processEvents, pairedFaultProcess{EventID: f.event(), Action: "commit", Role: "replica", Member: 4, Instance: pairedFaultInstance(4, p4), Volume: filepath.Join(f.n.dir, "4"), Raw: raw, Result: &result})
		}
		killed := f.kill(actor, "client")
		processEvents = append(processEvents, killed)
		callerRaw := f.unknown(call)
		cancel()
		release()
		f.drain()
		fault := pairedFaultCase{Case: caseName, Operation: names[stage-1], Marker: marker, MarkerRaw: boundary.Request, CallerClassification: "unknown", CallerRaw: callerRaw, ProcessEvidence: processEvents}
		if afterService {
			fault.MarkerRaw = boundary.Reply
		} else {
			pending := f.pending()
			fault.Intermediate = &pending
		}
		result := f.run(recoverer, p)
		step := f.complete(names[stage-1], p, result, from, before)
		f.assertView(step.After, stage)
		pairedFaultAssertCDC(t, step, stage)
		assertLifecycleCandidates(t, step.RawObservation, 3)
		fault.OriginalRequest = step.OriginalRequest
		fault.Observer = pairedFaultObserver(step)
		retry := f.replay(recoverer, names[stage-1], p, result, base)
		fault.Retry = &retry
		if afterService {
			changed := f.changed(p, base)
			fault.ChangedPayload = &changed
		}
		reopened := f.replace(actor)
		fault.ProcessEvidence = append(fault.ProcessEvidence, reopened)
		// This real barrier proves voter5 is caught up before the later client6 kill.
		// No role/commit metadata or authenticated source sequence is restored.
		proposals[stage-1], results[stage-1] = p, result
		f.trace.Faults = append(f.trace.Faults, fault)
		before = pairedFaultObserver(step)
	}
	heldAfter := nativeLifecycleRead(t, f.n, 4, held, 1)
	if !reflect.DeepEqual(heldAfter, heldFirst) {
		t.Fatal("held seed property/life substituted ABA current5", heldFirst, heldAfter)
	}
	heldView, err := lifecycleNormalizeView(heldAfter, f.trace.IdentityMapping, f.trace.OperationMapping)
	if err != nil {
		t.Fatal(err)
	}
	f.n.all[4].call(t, processRequest{op: processSourceCertifiedClose, index: held})
	heldClosed = true
	f.trace.OldReads = append(f.trace.OldReads, pairedFaultOldRead{lifecycleOldRead: lifecycleOldRead{Operation: "edge-create", Mode: "held-old", Cut: f.trace.Steps[2].Cut, View: heldView, RawObservation: heldAfter}, Checkpoint: "before-cohort-stop", EventID: f.event()})

	// Freeze the actual source-produced operation while source1 is still live.
	// Value6 is reused, so no new issuance or replacement Value definition occurs.
	p := f.proposal(6)
	from := f.indexes()
	leaderKilled := f.kill(1, "replica")
	f.elect(1, 2)
	f.reconnect(4, 2)
	result := f.run(4, p)
	step := f.complete(names[5], p, result, from, before)
	f.assertView(step.After, 6)
	pairedFaultAssertCDC(t, step, 6)
	assertLifecycleCandidates(t, step.RawObservation, 3)
	commitRaw, _ := json.Marshal(step.RawRecords)
	commitEvent := f.commitEvent(result, commitRaw)
	f.mu.Lock()
	stillDown := f.down[1]
	f.mu.Unlock()
	if !stillDown {
		t.Fatal("quorum progress was observed only after rejoin")
	}
	zone := pairedFaultCase{Case: "quorum-preserving-failure", Operation: names[5], Marker: "actual leader1 SIGKILL; leader2 own ReadIndex and COMMIT while1 down", MarkerRaw: leaderKilled.Raw, CallerClassification: "committed", CallerRaw: bytes.Clone(f.lastCoordinateRaw), OriginalRequest: step.OriginalRequest, Observer: pairedFaultObserver(step), ProcessEvidence: []pairedFaultProcess{leaderKilled, commitEvent}}
	leaderRejoined := f.replace(1)
	zone.ProcessEvidence = append(zone.ProcessEvidence, leaderRejoined)
	f.trace.Faults = append(f.trace.Faults, zone)
	proposals[5], results[5] = p, result

	stops := f.stopAll()
	reopens := f.reopenAll()
	final := f.observe(4, graphReadFresh, graphCut{}, false)
	f.assertView(final.View, 6)
	final.RawRecords = f.inventory(base)
	reopenedFault := pairedFaultCase{Case: "same-volume-reopen", Operation: names[5], Marker: "all six OS voters stopped/reaped before any same-volume replacement", MarkerRaw: stops[5].Raw, CallerClassification: "observer-only", CallerRaw: bytes.Clone(f.lastObservationRaw), OriginalRequest: step.OriginalRequest, Observer: final, ProcessEvidence: append(stops, reopens...)}
	for index := 0; index < 5; index++ {
		cut, e := pairedFaultCut(f.trace.Steps[index].Cut)
		if e != nil {
			t.Fatal(e)
		}
		old := f.observe(4, graphReadAt, cut, false)
		f.assertView(old.View, index+1)
		if !reflect.DeepEqual(old.Cut, f.trace.Steps[index].Cut) || !reflect.DeepEqual(old.View, f.trace.Steps[index].After) {
			t.Fatal("same-volume reopened old cut changed", names[index])
		}
		f.trace.OldReads = append(f.trace.OldReads, pairedFaultOldRead{lifecycleOldRead: lifecycleOldRead{Operation: names[index], Mode: "reopened-at", Cut: old.Cut, View: old.View, RawObservation: old.RawObservation}, Checkpoint: "after-cohort-reopen", EventID: f.event()})
	}
	for index := range proposals {
		actor := uint64(4)
		if index == 0 {
			actor = 1
		}
		reopenedFault.ReopenRetries = append(reopenedFault.ReopenRetries, f.replay(actor, names[index], proposals[index], results[index], base))
	}
	f.trace.Faults = append(f.trace.Faults, reopenedFault)
	finalInventory := f.inventory(base)
	var canonicalChanges int
	for _, s := range f.trace.Steps {
		canonicalChanges += len(s.LogicalCDC)
	}
	if canonicalChanges != 7 || len(f.trace.Steps) != 6 || len(f.trace.OldReads) != 6 {
		t.Fatal("paired complete fixture inventory", canonicalChanges, len(f.trace.Steps), len(f.trace.OldReads))
	}
	f.trace.RawCounts["applicationRecords"] = len(finalInventory)
	f.trace.RawCounts["componentChanges"] = canonicalChanges
	f.trace.RawCounts["certifiedReadOperations"] = f.reads
	f.trace.RawCounts["snapshotPageOperations"] = f.snapshotPages
	f.trace.RawCounts["scheduledDeadPeerDrops"] = f.dropped
	f.trace.RawCounts["installPhysicalWriteCount"] = nil
	f.trace.RawCounts["resourceScope"] = "96 separately admitted certified reads; aggregate IPC allowance4096+96*4096, shared packet/frame/queue guards retained; no production quota/reset or algorithmic bound"
	for id, child := range f.n.all {
		s := child.call(t, processRequest{op: processStateOp})
		if s.stats[statViews] != 0 || s.stats[statPinnedBytes] != 0 || len(s.handles) != 2 || s.handles[0] != 0 || s.handles[1] != 0 {
			t.Fatal("paired final owner leak", id, s.stats, s.handles)
		}
	}
	wire, err := json.MarshalIndent(f.trace, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if len(wire) > 8<<20 {
		t.Fatal("paired complete raw trace output cap", len(wire))
	}
	path := filepath.Join(f.n.dir, "native-paired-graph-fault-trace.json")
	if err := os.WriteFile(path, append(wire, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Log("native paired actual fault trace", path)
}
func pairedFaultInstance(id uint64, p *graphProcess) string { return pairedGraphFaultInstance(id, p) }

func TestNativePairedGraphFaultTransportRefusesUnscheduledErrorsAndBoundsDrops(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	called := 0
	filter := func(replica.Packet) (bool, error) { called++; return true, nil }
	for count := 1; count <= processDeliveries; count++ {
		drop, e := pairedGraphFaultPacketDisposition(count, filter, replica.Packet{})
		if e != nil || !drop {
			t.Fatal(drop, e)
		}
	}
	if drop, e := pairedGraphFaultPacketDisposition(processDeliveries+1, filter, replica.Packet{}); drop || !errors.Is(e, errLimit) || called != processDeliveries {
		t.Fatal("scheduled drops bypassed original delivery bound", drop, e, called)
	}
	if drop, e := pairedGraphFaultPacketDisposition(1, func(replica.Packet) (bool, error) { return false, io.EOF }, replica.Packet{}); drop || !errors.Is(e, io.EOF) {
		t.Fatal("unscheduled error was suppressed", drop, e)
	}
	if drop, e := pairedGraphFaultPacketDisposition(1, nil, replica.Packet{}); drop || e != nil {
		t.Fatal("legacy delivery changed", drop, e)
	}
}

func TestNativePairedGraphFaultPendingReplyRequiresExactTerminalNativeRefusal(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	incarnation := [16]byte{7}
	wireErr := graphWireError{graphErrorMask(replica.ErrUnavailable)}
	joined := errors.Join(errGraphCutPending, wireErr)
	code, detail := processError(joined)
	body, err := json.Marshal(pairedFaultPendingDiagnostic{Pending: true, Error: joined.Error()})
	if err != nil {
		t.Fatal(err)
	}
	valid := processResponse{incarnation: incarnation, sequence: 110, code: code, causeMask: processCauseMask(joined), detail: detail, aux: body}
	got, err := pairedGraphFaultPendingReply(valid, wireErr, incarnation)
	if err != nil || got.Classification != "pending" || !bytes.Equal(got.Raw, body) {
		t.Fatal("exact native terminal refusal rejected", got, err)
	}
	clear(body)
	if bytes.Equal(got.Raw, body) {
		t.Fatal("pending diagnostic aliases caller frame")
	}
	valid.aux = bytes.Clone(got.Raw)
	for name, transport := range map[string]error{
		"success": nil, "EOF": io.EOF, "deadline": context.DeadlineExceeded,
		"joined EOF": errors.Join(wireErr, io.EOF), "joined deadline": errors.Join(wireErr, context.DeadlineExceeded),
		"wrapped wire": fmt.Errorf("wrapped: %w", wireErr), "generic unclassified": graphWireError{graphErrorMask(errRemoteOperationFailure)},
		"limit": graphWireError{graphErrorMask(errLimit)}, "mixed wire causes": graphWireError{graphErrorMask(errors.Join(replica.ErrUnavailable, errLimit))},
	} {
		r, err := pairedGraphFaultPendingReply(valid, transport, incarnation)
		if !errors.Is(err, errInvalid) || !reflect.DeepEqual(r, pairedFaultIntermediate{}) {
			t.Fatal("arbitrary transport accepted", name, r, err)
		}
	}
	for name, change := range map[string]func(*processResponse){
		"wrong incarnation": func(r *processResponse) { r.incarnation = [16]byte{8} },
		"no sequence":       func(r *processResponse) { r.sequence = 0 },
		"successful code":   func(r *processResponse) { r.code = 0 },
		"generic code":      func(r *processResponse) { r.code = 6 },
		"missing cause":     func(r *processResponse) { r.causeMask = 0 },
		"detail mismatch":   func(r *processResponse) { r.detail = "Unavailable alone" },
		"malformed body":    func(r *processResponse) { r.aux = []byte("{") },
		"invented Pending":  func(r *processResponse) { r.aux = []byte(`{"Pending":true,"Token":null,"Error":"Unavailable alone"}`) },
		"not pending": func(r *processResponse) {
			r.aux = bytes.Replace(r.aux, []byte(`"Pending":true`), []byte(`"Pending":false`), 1)
		},
		"partial token": func(r *processResponse) {
			r.aux = bytes.Replace(r.aux, []byte(`"Token":null`), []byte(`"Token":"AQ=="`), 1)
		},
		"empty nonnil token": func(r *processResponse) {
			r.aux = bytes.Replace(r.aux, []byte(`"Token":null`), []byte(`"Token":""`), 1)
		},
		"extra field": func(r *processResponse) {
			r.aux = append(bytes.Clone(r.aux[:len(r.aux)-1]), []byte(`,"Extra":true}`)...)
		},
	} {
		r := valid
		change(&r)
		got, err := pairedGraphFaultPendingReply(r, wireErr, incarnation)
		if !errors.Is(err, errInvalid) || !reflect.DeepEqual(got, pairedFaultIntermediate{}) {
			t.Fatal("unauthenticated/invented terminal classification", name, got, err)
		}
	}
	if _, err := pairedGraphFaultPendingReply(valid, wireErr, [16]byte{}); !errors.Is(err, errInvalid) {
		t.Fatal("missing configured incarnation accepted", err)
	}
}

func TestNativePairedGraphFaultReconnectRefusesInvalidAndBusyActualOwners(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	f := newPairedGraphFaultNetwork(t)
	// Invalid association must not alter the live connection; the actual seed
	// then runs over the original authenticated1/4 streams.
	for _, input := range [][]byte{nil, allocationP08Words(4, 4), allocationP08Words(2, 3)} {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		r, e := f.n.all[4].exchange(ctx, processRequest{op: processPairedReconnect, data: input})
		cancel()
		if !errors.Is(errors.Join(e, processResponseError(r)), errInvalid) {
			t.Fatal("invalid peer association accepted", e, processResponseError(r))
		}
	}
	initial := f.observe(4, graphReadFresh, graphCut{}, false)
	p := f.proposal(1)
	from := f.indexes()
	result := f.run(1, p)
	step := f.complete("node-a", p, result, from, initial)
	f.assertView(step.After, 1)
	handle, _ := f.open(4, graphReadFresh, graphCut{})
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	r, e := f.n.all[4].exchange(ctx, processRequest{op: processPairedReconnect, data: allocationP08Words(2, 4)})
	cancel()
	if !errors.Is(errors.Join(e, processResponseError(r)), errInvalid) {
		t.Fatal("live retained owner allowed reconnect", e, processResponseError(r))
	}
	f.n.all[4].call(t, processRequest{op: processSourceCertifiedClose, index: handle})
	f.drain()
	// The server keeps closed lease records for replay. They must remain in
	// place, while their released native views no longer block reconnect.
	retained := f.n.all[4].call(t, processRequest{op: processPairedConnections, entity: 4})
	c := graphCursor{b: retained.aux}
	c.tag()
	if slots := c.u64(); c.err != nil || len(c.b) != 0 || slots == 0 {
		t.Fatal("missing actual retained source lease records", slots, c.err)
	}
	f.reconnect(4, 2)
}

func TestNativePairedGraphFaultNoOpUsesActualRetainedNormalEntry(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	var missing context.Context
	if wire, err := pairedGraphFaultRecordWire(missing, nil, 0); wire != nil || !errors.Is(err, errInvalid) {
		t.Fatal("nil record inspection", err)
	}
	f := newPairedGraphFaultNetwork(t)
	r := f.n.all[1].call(t, processRequest{op: processLifecycleRecord, index: 2})
	var record pairedFaultRecord
	if err := json.Unmarshal(r.aux, &record); err != nil {
		t.Fatal(err)
	}
	if record.NoOp == nil || record.NoOp.Index != 2 || record.NoOp.Partition != 3 || record.NoOp.Term == 0 || len(record.NoOp.Entry) == 0 || len(record.Outcome) != 0 || len(record.CDC) != 0 {
		t.Fatal("actual election entry lacks exact empty normal evidence", record)
	}
	proposal := f.proposal(1)
	result := f.run(1, proposal)
	r = f.n.all[1].call(t, processRequest{op: processLifecycleRecord, index: result.decision.index})
	// Decode into a fresh owner: JSON's omitted NoOp field must not retain the
	// earlier inspection's pointer when a different record is read.
	var business pairedFaultRecord
	if err := json.Unmarshal(r.aux, &business); err != nil {
		t.Fatal(err)
	}
	if business.NoOp != nil || len(business.Outcome) == 0 || business.Index != result.decision.index {
		t.Fatal("business outcome qualified as an election no-op", business)
	}
}
