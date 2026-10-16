package graphapply

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"os/exec"
	"path/filepath"
	"slices"
	"syscall"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
)

func TestNativeGraphCostRecordPagesRejectWrongBoundaryGapAndOversize(t *testing.T) {
	certificate := graphCertificate{scope: allocationScope{partition: 3}, sourceIndex: 18, image: [172]byte{1}}
	page := nativeGraphCostRecordPage{Partition: 3, End: 18, Next: 17, Image: bytes.Clone(certificate.image[:])}
	for index := uint64(2); index <= 17; index++ {
		page.Records = append(page.Records, nativeGraphCostRecord{nativeLifecycleRecord: nativeLifecycleRecord{Partition: 3, Index: index, Outcome: []byte{1}, CDC: []byte{2}}})
	}
	wire, err := nativeGraphCostEncodePage(page)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := nativeGraphCostDecodePage(wire, 3, 1, certificate)
	if err != nil || len(decoded.Records) != 16 || decoded.Next != 17 || !bytes.Equal(decoded.Records[0].CDC, []byte{2}) {
		t.Fatal(decoded, err)
	}
	clear(wire)
	if decoded.Image[0] != 1 || decoded.Records[0].Outcome[0] != 1 {
		t.Fatal("page output aliases wire")
	}
	for _, mutate := range []func(*nativeGraphCostRecordPage){
		func(p *nativeGraphCostRecordPage) { p.Partition = 8 },
		func(p *nativeGraphCostRecordPage) { p.End++ },
		func(p *nativeGraphCostRecordPage) { p.Next++ },
		func(p *nativeGraphCostRecordPage) { p.Image[0]++ },
		func(p *nativeGraphCostRecordPage) { p.Records = p.Records[1:] },
		func(p *nativeGraphCostRecordPage) { p.Records[0].Index++ },
		func(p *nativeGraphCostRecordPage) { p.Records[0].Partition = 8 },
		func(p *nativeGraphCostRecordPage) { p.Records[0], p.Records[1] = p.Records[1], p.Records[0] },
	} {
		copyPage := page
		copyPage.Image = bytes.Clone(page.Image)
		copyPage.Records = append([]nativeGraphCostRecord(nil), page.Records...)
		mutate(&copyPage)
		wire, err := nativeGraphCostEncodePage(copyPage)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := nativeGraphCostDecodePage(wire, 3, 1, certificate); !errors.Is(err, errCorrupt) {
			t.Fatal("invalid page accepted", err)
		}
	}
	page.Records = nil
	page.Next = certificate.sourceIndex
	wire, err = nativeGraphCostEncodePage(page)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := nativeGraphCostDecodePage(wire, 3, 18, certificate); err != nil {
		t.Fatal("exact root-only page", err)
	}
	if _, err := nativeGraphCostDecodePage(wire, 3, 19, certificate); !errors.Is(err, errCorrupt) {
		t.Fatal(err)
	}
	if _, err := nativeGraphCostDecodePage(wire, 8, 18, certificate); !errors.Is(err, errCorrupt) {
		t.Fatal(err)
	}
	for _, malformed := range [][]byte{nil, wire[:len(wire)-1], append(bytes.Clone(wire), 0)} {
		if _, err := nativeGraphCostDecodePage(malformed, 3, 18, certificate); !errors.Is(err, errCorrupt) {
			t.Fatal(err)
		}
	}
	if _, err := nativeGraphCostDecodePage(make([]byte, nativeGraphCostPageBytes+1), 3, 18, certificate); !errors.Is(err, errLimit) {
		t.Fatal(err)
	}
	page.Records = make([]nativeGraphCostRecord, nativeGraphCostBatchCommands+1)
	if _, err := nativeGraphCostEncodePage(page); !errors.Is(err, errLimit) {
		t.Fatal(err)
	}
	page.Records = []nativeGraphCostRecord{{nativeLifecycleRecord: nativeLifecycleRecord{Outcome: make([]byte, nativeLifecycleBytes)}}}
	if _, err := nativeGraphCostEncodePage(page); !errors.Is(err, errLimit) {
		t.Fatal(err)
	}
	// Collection count is refused before allocation, even with a short wire.
	wire, err = nativeGraphCostEncodePage(nativeGraphCostRecordPage{Image: make([]byte, 172)})
	if err != nil {
		t.Fatal(err)
	}
	binary.BigEndian.PutUint32(wire[len(wire)-4:], nativeGraphCostBatchCommands+1)
	if _, err := nativeGraphCostDecodePage(wire, 3, 18, certificate); !errors.Is(err, errLimit) {
		t.Fatal(err)
	}
	for _, request := range []processRequest{{}, {index: 2, entity: 1, data: []byte{0}}, {entity: 1, data: []byte{2}}} {
		if _, err := nativeGraphCostRecordsWire(t.Context(), nil, request); !errors.Is(err, errInvalid) {
			t.Fatal(err)
		}
	}
}

// Inert codec adversaries only; actual outcome/CDC production and multi-page
// native histories are exercised by TestNativeGraphCostSmallActualProcesses.
func TestNativeGraphCostBatchedCDCStillRequiresExactOriginalAndEveryEffect(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for _, ctx := range []context.Context{nil, t.Context()} {
		if out, err := nativeGraphCostCoordinatorManifest(ctx, nil, crossProcessResult{}); !errors.Is(err, errInvalid) || out != nil {
			t.Fatal("nil receipt owner", out, err)
		}
		if out, err := nativeGraphCostInspectRecord(ctx, nil, nativeLifecycleRecord{}); !errors.Is(err, errInvalid) || out.CDCClass != "" || out.Outcome != nil {
			t.Fatal("nil inspection owner", out, err)
		}
	}
	spec := nativeGraphCostSpec{Version: 1, Facts: 17, Writers: 1}
	scope := lifecycleAllScope{}
	command := nativeGraphCostSample{phase: 4, Fact: 0, partition: 3, index: 20, hash: [32]byte{1}, identity: [16]byte{2}}
	original := outcome{ns: namespace{graph: idalloc.GraphID{1}, partition: 3}, kind: graphOperations, index: 20, hash: command.hash, identity: [16]byte{2}, disposition: applied}
	wire, err := encodeTypedGraphOutcome(original, true)
	if err != nil {
		t.Fatal(err)
	}
	changes := []nativeLifecycleChange{
		{Owner: 7, Component: byte(graphstate.Presence), Scope: []byte{1}, Before: nativeLifecycleCell{Kind: "absent"}, After: nativeLifecycleCell{Present: true, Revision: 1, Provenance: 4, Kind: "life", Handle: 8}},
		{Owner: 7, Life: 8, Component: byte(graphstate.ScalarProperty), Name: "p", Scope: []byte{1}, Before: nativeLifecycleCell{Kind: "absent"}, After: nativeLifecycleCell{Present: true, Revision: 1, Provenance: 4, Kind: "i64", Handle: 5, I64: 5}},
	}
	record := nativeLifecycleRecord{Partition: 3, Index: 20, Outcome: wire, CDC: []byte{1}, Changes: changes}
	seen := []byte{0}
	if err := nativeGraphCostVerifyRecord(spec, []nativeGraphCostSample{command}, seen, record, scope); err != nil || nativeGraphCostComplete([]nativeGraphCostSample{command}, seen) != nil {
		t.Fatal(seen, err)
	}
	for _, mutate := range []func(*nativeLifecycleRecord){
		func(r *nativeLifecycleRecord) { r.Outcome = nil },
		func(r *nativeLifecycleRecord) { r.Index++ },
		func(r *nativeLifecycleRecord) { r.Partition = 8 },
		func(r *nativeLifecycleRecord) { r.Changes = append(r.Changes, r.Changes[0]) },
		func(r *nativeLifecycleRecord) { r.Changes[0].Owner++ },
		func(r *nativeLifecycleRecord) { r.Changes[0].After.Provenance++ },
		func(r *nativeLifecycleRecord) { r.Changes[1].After.I64 = 6 },
	} {
		altered := record
		altered.Changes = append([]nativeLifecycleChange(nil), changes...)
		mutate(&altered)
		if err := nativeGraphCostVerifyRecord(spec, []nativeGraphCostSample{command}, []byte{0}, altered, scope); !errors.Is(err, errCorrupt) {
			t.Fatal("wrong original/effect accepted", err)
		}
	}
	wrongIdentity := command
	wrongIdentity.identity[0]++
	if err := nativeGraphCostVerifyRecord(spec, []nativeGraphCostSample{wrongIdentity}, []byte{0}, record, scope); !errors.Is(err, errCorrupt) {
		t.Fatal("wrong original identity", err)
	}
	wrong := command
	wrong.hash[0]++
	if err := nativeGraphCostVerifyRecord(spec, []nativeGraphCostSample{wrong}, []byte{0}, record, scope); !errors.Is(err, errCorrupt) {
		t.Fatal(err)
	}
	if err := nativeGraphCostVerifyRecord(spec, []nativeGraphCostSample{command, command}, []byte{0, 0}, record, scope); !errors.Is(err, errCorrupt) {
		t.Fatal("ambiguous command", err)
	}
	if err := nativeGraphCostVerifyRecord(spec, nil, nil, record, scope); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	cross := command
	cross.Cross = true
	cross.participants = [2]uint64{3, 8}
	cross.tx, cross.digest, cross.round = [16]byte{3}, [32]byte{4}, 5
	status := crossStatus{ns: original.ns, tx: cross.tx, digest: cross.digest, phase: crossResolve, commit: true, round: 5, index: 20}
	record.Outcome, err = encodeCrossStatus(status)
	if err != nil {
		t.Fatal(err)
	}
	if err := nativeGraphCostVerifyRecord(spec, []nativeGraphCostSample{cross}, []byte{0}, record, scope); err != nil {
		t.Fatal(err)
	}
	crossSeen := []byte{0}
	if err := nativeGraphCostVerifyRecord(spec, []nativeGraphCostSample{cross}, crossSeen, record, scope); err != nil {
		t.Fatal(err)
	}
	if err := nativeGraphCostComplete([]nativeGraphCostSample{cross}, crossSeen); !errors.Is(err, errCorrupt) {
		t.Fatal("missing empty participant accepted", err)
	}
	// A validation-only participant still writes a canonical nonempty GCE with
	// zero logical effects; it must bind the exact original decision and manifest.
	empty := record
	empty.Partition = 8
	empty.Index = 21
	empty.Changes = nil
	emptyStatus := status
	emptyStatus.ns.partition = 8
	emptyStatus.index = empty.Index
	encodeEmpty := func(r *nativeLifecycleRecord, st crossStatus) {
		t.Helper()
		var err error
		r.Outcome, err = encodeCrossStatus(st)
		if err != nil {
			t.Fatal(err)
		}
		logical, err := encodeTypedGraphChanges(graphChanges{ns: st.ns}, defaultMaterializerLimits(), true)
		if err != nil {
			t.Fatal(err)
		}
		r.CDC, err = encodeChangeEnvelope(outcome{ns: st.ns, kind: graphOperations, identity: st.tx, hash: st.digest, index: st.index, disposition: applied}, logical, defaultMaterializerLimits())
		if err != nil {
			t.Fatal(err)
		}
	}
	encodeEmpty(&empty, emptyStatus)
	if err := nativeGraphCostVerifyRecord(spec, []nativeGraphCostSample{cross}, crossSeen, empty, scope); err != nil {
		t.Fatal("original empty participant", err)
	}
	if err := nativeGraphCostComplete([]nativeGraphCostSample{cross}, crossSeen); err != nil {
		t.Fatal("complete participants", err)
	}
	if err := nativeGraphCostVerifyRecord(spec, []nativeGraphCostSample{cross}, crossSeen, empty, scope); !errors.Is(err, errCorrupt) {
		t.Fatal("duplicate empty participant accepted", err)
	}
	for _, mutate := range []func(*crossStatus){
		func(st *crossStatus) { st.tx[0]++ },
		func(st *crossStatus) { st.digest[0]++ },
		func(st *crossStatus) { st.round++ },
		func(st *crossStatus) { st.ns.graph[0]++ },
		func(st *crossStatus) { st.ns.partition = 9 },
		func(st *crossStatus) { st.index++ },
		func(st *crossStatus) { st.phase = crossDecide },
		func(st *crossStatus) { st.commit = false },
	} {
		altered := empty
		alteredStatus := emptyStatus
		mutate(&alteredStatus)
		encodeEmpty(&altered, alteredStatus)
		if err := nativeGraphCostVerifyRecord(spec, []nativeGraphCostSample{cross}, []byte{0}, altered, scope); !errors.Is(err, errCorrupt) {
			t.Fatal("unknown empty original accepted", err)
		}
	}
	for _, cdc := range [][]byte{{1}, empty.CDC[:len(empty.CDC)-1], append(bytes.Clone(empty.CDC), 0)} {
		altered := empty
		altered.CDC = cdc
		if err := nativeGraphCostVerifyRecord(spec, []nativeGraphCostSample{cross}, []byte{0}, altered, scope); !errors.Is(err, errCorrupt) {
			t.Fatal("noncanonical empty business accepted", err)
		}
	}
	localOnlyFootprint := cross
	localOnlyFootprint.participants = [2]uint64{3}
	if err := nativeGraphCostVerifyRecord(spec, []nativeGraphCostSample{localOnlyFootprint}, []byte{0}, empty, scope); !errors.Is(err, errCorrupt) {
		t.Fatal("unlisted participant accepted", err)
	}
	if err := nativeGraphCostComplete([]nativeGraphCostSample{localOnlyFootprint}, []byte{(1 << byte(graphstate.Presence)) | (1 << byte(graphstate.ScalarProperty)) | (1 << 6)}); err != nil {
		t.Fatal("single-participant footprint", err)
	}
	control := nativeLifecycleRecord{Partition: 8, Index: 21}
	if err := nativeGraphCostVerifyRecord(spec, []nativeGraphCostSample{cross}, []byte{0}, control, scope); err != nil {
		t.Fatal("zero-byte control", err)
	}
	if err := nativeGraphCostComplete([]nativeGraphCostSample{cross}, []byte{0}); !errors.Is(err, errCorrupt) {
		t.Fatal("control supplied business completion", err)
	}
	cross.round++
	if err := nativeGraphCostVerifyRecord(spec, []nativeGraphCostSample{cross}, []byte{0}, record, scope); !errors.Is(err, errCorrupt) {
		t.Fatal("wrong cross decision", err)
	}
	// Removing one business effect leaves the checkpoint's exact cardinality
	// incomplete; neither a valid original nor another fact can supply it.
	record.Outcome = wire
	record.Changes = changes[:1]
	seen[0] = 0
	if err := nativeGraphCostVerifyRecord(spec, []nativeGraphCostSample{command}, seen, record, scope); err != nil {
		t.Fatal(seen, err)
	}
	if err := nativeGraphCostComplete([]nativeGraphCostSample{command}, seen); !errors.Is(err, errCorrupt) {
		t.Fatal("missing business CDC accepted", err)
	}
	for _, phase := range []uint64{1, 2, 3, 5} {
		command.phase = phase
		want := byte(1 << byte(graphstate.Presence))
		if phase == 5 {
			want = 1 << byte(graphstate.ScalarProperty)
		}
		if err := nativeGraphCostComplete([]nativeGraphCostSample{command}, []byte{want}); err != nil {
			t.Fatal(err)
		}
	}
	// Genuine native receipts prove metadata binds stored entries, rather than
	// trusting a self-consistent copied outcome or a fabricated empty CDC.
	n, coordinator, operations, claims, _ := coordinatorNativeFixture(t)
	from := [2]uint64{n.hosts[0][0].driver.Applied(), n.hosts[1][0].driver.Applied()}
	result, err := coordinator.Run(t.Context(), n.requestID(), operations, 37, claims)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := nativeGraphCostCoordinatorManifest(t.Context(), n.hosts[0][0], result)
	if err != nil || len(manifest) == 0 {
		t.Fatal("genuine original receipt", err)
	}
	wrongReceipt := result
	wrongReceipt.decision.digest[0]++
	if out, err := nativeGraphCostCoordinatorManifest(t.Context(), n.hosts[0][0], wrongReceipt); !errors.Is(err, errCorrupt) || out != nil {
		t.Fatal("wrong receipt digest", out, err)
	}
	canceled, stop := context.WithCancel(t.Context())
	stop()
	if out, err := nativeGraphCostCoordinatorManifest(canceled, n.hosts[0][0], result); !errors.Is(err, context.Canceled) || out != nil {
		t.Fatal("canceled receipt retained output", out, err)
	}
	classes := map[string]int{}
	for group, first := range from {
		host := n.hosts[group][0]
		for index := first + 1; index <= host.driver.Applied(); index++ {
			raw, err := nativeGraphRecordRead(t.Context(), host, index, true)
			if err != nil {
				t.Fatal(err)
			}
			inspected, err := nativeGraphCostInspectRecord(t.Context(), host, raw)
			if err != nil || inspected.Original.Request == "" || inspected.Original.Provenance != 37 || inspected.Original.Digest == "" || len(inspected.Original.Participants) == 0 || inspected.RawHashes.OutcomeSHA256 == "" {
				t.Fatal("actual metadata binding", inspected, err)
			}
			if inspected.SourceBoundary.Index != index || len(inspected.SourceBoundary.ImageSHA256) != 64 {
				t.Fatal("missing same-index retained root", inspected.SourceBoundary)
			}
			nilDTO := raw
			nilDTO.CDC = nil
			if out, err := nativeGraphCostInspectRecord(t.Context(), host, nilDTO); !errors.Is(err, errCorrupt) || out.CDCClass != "" {
				t.Fatal("copied nil DTO supplied present empty frame", out, err)
			}
			classes[inspected.CDCClass]++
			if inspected.CDCClass != "logical" && inspected.CDCClass != "participant-empty" {
				continue
			}
			for _, mutate := range []func(*nativeLifecycleRecord){
				func(r *nativeLifecycleRecord) {
					r.CDC = nil
					r.Changes = nil
					r.Entities = nil
					r.Lives = nil
					r.Values = nil
				},
				func(r *nativeLifecycleRecord) { r.Partition++ },
				func(r *nativeLifecycleRecord) { r.Outcome = nil },
				func(r *nativeLifecycleRecord) {
					st, e := decodeCrossStatus(r.Outcome)
					if e != nil {
						t.Fatal(e)
					}
					st.digest[0]++
					r.Outcome, e = encodeCrossStatus(st)
					if e != nil {
						t.Fatal(e)
					}
				},
			} {
				altered := raw
				mutate(&altered)
				if out, err := nativeGraphCostInspectRecord(t.Context(), host, altered); err == nil || out.CDCClass != "" || out.Outcome != nil {
					t.Fatal("forged metadata input accepted/output", out, err)
				}
			}
		}
	}
	if classes["logical"] == 0 || classes["participant-empty"] == 0 || classes["protocol-empty"] == 0 {
		t.Fatal("missing genuine CDC classes", classes)
	}
	// A genuine bootstrap outcome is outside the named cost control codecs.
	host := n.hosts[0][0]
	unknown := false
	for index := uint64(2); index <= from[0]; index++ {
		outcome, err := host.machine.store.ApplicationRecord(t.Context(), index, true, 64<<10)
		if err != nil {
			t.Fatal(err)
		}
		if len(outcome) == 0 || bytes.HasPrefix(outcome, []byte("GCS\x01")) || bytes.HasPrefix(outcome, []byte("GSO\x01")) {
			continue
		}
		if _, err := decodePartitionGraphOutcome(outcome, host.machine.ns); err == nil {
			continue
		}
		cdc, err := host.machine.store.ApplicationRecord(t.Context(), index, false, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		if out, err := nativeGraphCostInspectRecord(t.Context(), host, nativeLifecycleRecord{Partition: host.machine.ns.partition, Index: index, Outcome: outcome, CDC: cdc}); !errors.Is(err, errCorrupt) || out.CDCClass != "" {
			t.Fatal("unknown outcome classified empty control", out, err)
		}
		unknown = true
		break
	}
	if !unknown {
		t.Fatal("no genuine unknown cost outcome exercised")
	}

	command.phase = 0
	if err := nativeGraphCostComplete([]nativeGraphCostSample{command}, []byte{0}); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if err := nativeGraphCostComplete(nil, nil); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
}

func TestGraphSourceProcessInitialSendDeadlineClosesActualStoppedChildPipe(t *testing.T) {
	p := startGraphProcess(t, filepath.Join(t.TempDir(), "store"), 1, "RHO_GRAPH_CERTIFIED_PROCESS=1", "RHO_GRAPH_SOURCE_PROCESS=1")
	attachGraphProcessMuxes(t, map[uint64]*graphProcess{1: p})
	p.call(t, processRequest{op: processStateOp}) // Actual source-mode child handshake.
	if err := p.cmd.Process.Signal(syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		reap, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		_ = p.shutdown(reap, true)
	})
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	// Larger than the real pipe capacity; the stopped child cannot drain it.
	response, err := p.exchange(ctx, processRequest{op: processStateOp, data: bytes.Repeat([]byte{1}, 1<<20)})
	if !errors.Is(err, context.DeadlineExceeded) || response.code != 0 || response.applied != 0 || len(response.outcome)+len(response.aux) != 0 {
		t.Fatal("blocked send became ACK", response, err)
	}
	p.mux.mu.Lock()
	left, failed := len(p.mux.pending), p.mux.err
	p.mux.mu.Unlock()
	if left != 0 || !errors.Is(failed, context.DeadlineExceeded) {
		t.Fatal("expired send retained transport owner", left, failed)
	}
	if _, err := p.exchange(t.Context(), processRequest{op: processStateOp}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("failed pipe reused", err)
	}
	reap, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	err = p.shutdown(reap, true)
	if _, ok := errors.AsType[*exec.ExitError](err); !ok {
		t.Fatal("missing actual killed-child ExitError", err)
	}
	if p.cmd.ProcessState == nil {
		t.Fatal("stopped child not reaped", err)
	}
	status, ok := p.cmd.ProcessState.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatal("expected actual SIGKILL reap", p.cmd.ProcessState, err)
	}
}

func TestGraphSourceProcessAlreadyCanceledExchangeDoesNotSend(t *testing.T) {
	m, _, _ := processCancelWireFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := m.exchange(ctx, processRequest{op: processStateOp}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	m.mu.Lock()
	pending, failed, sequence := len(m.pending), m.err, m.p.sequence
	m.mu.Unlock()
	if pending != 0 || failed != nil || sequence != 0 {
		t.Fatal("canceled call sent/poisoned transport", pending, failed, sequence)
	}
}

// A whole client window is verified against its originals, independently of
// completion order; splitting this into wire-page sized windows would accept
// or reject effects against the wrong commands after all 32 have committed.
func TestNativeGraphCostThirtyTwoCommandWindowRejectsOtherWaveOriginals(t *testing.T) {
	spec := nativeGraphCostSpec{Version: 1, Facts: 65, Writers: 32}
	commands := make([]nativeGraphCostSample, nativeGraphCostWindowCommands)
	records := make([]nativeLifecycleRecord, nativeGraphCostWindowCommands)
	scope := lifecycleAllScope{}
	for i := range commands {
		fact := uint64(i + 1)
		command := nativeGraphCostSample{phase: 5, Fact: fact, partition: 3, index: 100 + fact, identity: [16]byte{byte(i + 1)}, hash: [32]byte{byte(i + 1)}}
		original := outcome{ns: namespace{graph: idalloc.GraphID{1}, partition: 3}, kind: graphOperations, identity: command.identity, hash: command.hash, index: command.index, disposition: applied}
		wire, err := encodeTypedGraphOutcome(original, true)
		if err != nil {
			t.Fatal(err)
		}
		owner, life, _, _ := spec.edge(fact)
		records[i] = nativeLifecycleRecord{Partition: 3, Index: command.index, Outcome: wire, CDC: []byte{1}, Changes: []nativeLifecycleChange{{Owner: owner, Life: life, Component: byte(graphstate.ScalarProperty), Name: "p", Scope: []byte{1}, Before: nativeLifecycleCell{Present: true, Kind: "i64", Handle: 5, I64: 5, Revision: 10, Provenance: 4 + fact}, After: nativeLifecycleCell{Present: true, Kind: "i64", Handle: 6, I64: 6, Revision: 11, Provenance: 4 + spec.Facts + fact}}}}
		commands[i] = command
	}
	slices.Reverse(commands)
	seen := make([]byte, len(commands))
	for _, record := range records {
		if err := nativeGraphCostVerifyRecord(spec, commands, seen, record, scope); err != nil {
			t.Fatal("completion order changed identity join", err)
		}
	}
	if err := nativeGraphCostComplete(commands, seen); err != nil {
		t.Fatal(err)
	}
	other := slices.Clone(commands)
	other[0].identity[0]++
	if err := nativeGraphCostVerifyRecord(spec, other, make([]byte, len(other)), records[len(records)-1], scope); !errors.Is(err, errCorrupt) {
		t.Fatal("other wave identity supplied completion", err)
	}
	if err := nativeGraphCostVerifyRecord(spec, commands[:16], make([]byte, 16), records[0], scope); !errors.Is(err, errCorrupt) {
		t.Fatal("page-sized split accepted another half-window", err)
	}
	if err := nativeGraphCostVerifyRecord(spec, commands, seen, records[0], scope); !errors.Is(err, errCorrupt) {
		t.Fatal("duplicate original/effect accepted", err)
	}
}
