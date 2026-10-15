package graphapply

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type nativeCreateUpdatePhase struct {
	Phase                               string
	StartNanos, StopNanos, WallNanos    int64
	SingleParticipant, MultiParticipant uint64
	ReadParticipants, WriteParticipants [2]uint64
}

func nativeCreateUpdatePhaseStop(name string, began, started time.Time) nativeCreateUpdatePhase {
	ended := time.Now()
	return nativeCreateUpdatePhase{Phase: name, StartNanos: began.Sub(started).Nanoseconds(), StopNanos: ended.Sub(started).Nanoseconds(), WallNanos: ended.Sub(began).Nanoseconds()}
}

type nativeCreateUpdateResult struct {
	nativeSelectorsResult
	RowSemantics, BusinessFootprint, Coordination string
	SetupTransactions                             uint64
	BusinessPhases                                [2]nativeCreateUpdatePhase
	ImmutableColumns                              *nativeImmutableColumnsEnd `json:",omitempty"`
	Correction                                    *nativeLateCorrectionEnd   `json:",omitzero"`
}

// Genuine workload parent, not an in-process semantic substitute. The fixed
// profile still admits all1/8/32 callers and both actual source/column layouts.
func TestNativeGraphCreateUpdateWorkloadCell(t *testing.T) {
	if testing.Short() || os.Getenv("RHO_GRAPH_NATIVE_WORKLOAD") != nativeCreateUpdateFormat {
		t.Skip("explicit native create/update workload required")
	}
	nativeScalarCreateUpdateWorkload(t, nativeCreateUpdateFamily)
}

// Explicit scalar schedules share genuine factories/originals/source and
// lifecycle reducers. Cost9's wrapper remains its existing literal schedule;
// Cost10/11 add publication/read/probe observations and a held column owner.
func nativeScalarCreateUpdateWorkload(t *testing.T, family string) {
	t.Helper()
	var spec nativeSelectorsSpec
	var err error
	format, prefix := nativeCreateUpdateFormat, "rho-native-create-update-"
	switch family {
	case nativeCreateUpdateFamily:
		spec, err = nativeCreateUpdateSpecFromEnv()
	case nativeImmutableColumnsFamily:
		spec, err = nativeImmutableColumnsSpecFromEnv()
		format, prefix = nativeImmutableColumnsFormat, "rho-native-immutable-columns-"
	case nativeLateCorrectionFamily:
		spec, err = nativeLateCorrectionSpecFromEnv()
		format, prefix = nativeLateCorrectionFormat, "rho-native-late-correction-"
	default:
		t.Fatal("unsupported scalar source family")
	}
	if err != nil {
		t.Fatal(err)
	}
	if os.Getenv("RHO_GRAPH_NATIVE_WORKLOAD") != format {
		t.Fatal("explicit scalar source format required")
	}
	admission, digest, err := nativeCreateUpdateAdmissionFromEnv(spec)
	if family == nativeImmutableColumnsFamily {
		admission, digest, err = nativeImmutableColumnsAdmissionFromEnv(spec)
	}
	if family == nativeLateCorrectionFamily {
		admission, digest, err = nativeLateCorrectionAdmissionFromEnv(spec)
	}
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithDeadline(t.Context(), time.Unix(0, admission.DeadlineUnixNanos))
	defer cancel()
	started := time.Now()
	output := os.Getenv("RHO_GRAPH_WORKLOAD_OUTPUT")
	if !filepath.IsAbs(output) {
		t.Fatal("absolute owned artifact root required")
	}
	if err := os.Mkdir(output, 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RHO_GRAPH_PROCESS_ARTIFACT", output)
	open := func(name string) *json.Encoder {
		f, err := os.OpenFile(filepath.Join(output, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := f.Close(); err != nil {
				t.Error(err)
			}
		})
		return json.NewEncoder(f)
	}
	grants, attempts, operations := open("grants.jsonl"), open("attempts.jsonl"), open("operations.jsonl")
	reads, sources, processes, columns := open("reads.jsonl"), open("sources.jsonl"), open("processes.jsonl"), open("columns.jsonl")
	env, err := nativeScalarCreateUpdateChildEnv(spec, admission, false)
	if err != nil {
		t.Fatal(err)
	}
	p := nativeCreateUpdateCost(spec)
	if family == nativeImmutableColumnsFamily {
		p = nativeImmutableColumnsCost(spec)
	}
	if family == nativeLateCorrectionFamily {
		p = nativeLateCorrectionCost(spec)
	}
	n := nativeScalarWorkloadNetwork(t, p, env, prefix, durableColumnGraphSemanticContractID(), grants, func(n *certifiedProcessNetwork) { nativeCreateUpdateAttach(t, n, spec, admission) })
	result := nativeCreateUpdateResult{nativeSelectorsResult: nativeSelectorsResult{Version: 1, Format: format, Engine: "native-raft", Spec: spec, Admission: admission, AdmissionSHA256: digest, Directory: n.dir, ArtifactDirectory: output, ProcessInstances: 12, RetentionContract: "snapshot-C-retained-full-prefix-v1"}, SetupTransactions: 3, BusinessFootprint: "complete actual graph operation/read/write/claim footprint under authenticated installed ranges; setup excluded", Coordination: "local Submit for one business partition; cross Run for multi-participant business; actual manifest participants/empty control coordination reported separately"}
	var columnObserver *nativeImmutableColumnsObservations
	if family == nativeImmutableColumnsFamily || family == nativeLateCorrectionFamily {
		columnObserver, err = newNativeScalarColumnObservations(output, spec, digest)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := columnObserver.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	result.RowSemantics = "strict-partition-local-create-update"
	if spec.CrossPercent != 0 {
		result.RowSemantics = "mixed-or-cross-partition-create-update"
	}
	var correction *nativeLateCorrectionIndexes
	if family == nativeLateCorrectionFamily {
		result.RowSemantics = "late-scalar-correction-first-half"
		correction, err = newNativeLateCorrectionIndexes(output, spec, digest)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := correction.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	result.FramesCeiling, err = nativeCreateUpdateFrames(p)
	if family == nativeImmutableColumnsFamily {
		result.FramesCeiling, err = nativeImmutableColumnsFrames(p)
	}
	if family == nativeLateCorrectionFamily {
		result.FramesCeiling, err = nativeLateCorrectionFrames(p)
	}
	if err != nil {
		t.Fatal(err)
	}
	// Initial graph cut uses the same genuine source API and closes its actual
	// temporary owner; this is not a synthetic named selector observation.
	body, err := encodeGraphE15ReadRequest(graphE15ReadRequest{mode: graphReadFresh})
	if err != nil {
		t.Fatal(err)
	}
	response, err := nativeGraphCostExchange(ctx, n.all[2], processRequest{op: processSourceCertifiedOpen, data: body})
	if err != nil {
		t.Fatal(err)
	}
	handle, initial, err := decodeGraphE15ReadToken(response.aux)
	if err != nil || handle == 0 || len(initial.certificates) != 2 {
		t.Fatal("initial native certificate", err)
	}
	for _, cert := range initial.certificates {
		if cert.scope.semantic != durableColumnGraphSemanticContractID() {
			t.Fatal("initial source agreement")
		}
	}
	if _, err := nativeGraphCostExchange(ctx, n.all[2], processRequest{op: processSourceCertifiedClose, index: handle}); err != nil {
		t.Fatal(err)
	}
	result.Initial, err = nativeFeedCutWire(initial)
	if err != nil {
		t.Fatal(err)
	}
	driver := &nativeCreateUpdateDriver{spec: spec}
	admissions := newNativeGraphCostAdmissions()
	admissions.originals, admissions.same = 64, 8
	admissions.extra, admissions.beginOriginal = driver.retry, driver.beginOriginal
	phase := func(number, from uint64) {
		if family == nativeLateCorrectionFamily && number == 5 {
			nativeScalarWorkloadPhaseRange(t, ctx, n, p, admissions, started, number, from, spec.Facts/2, attempts, operations, &result.Completed)
		} else {
			nativeScalarWorkloadPhase(t, ctx, n, p, admissions, started, number, from, attempts, operations, &result.Completed)
		}
	}
	for setup := uint64(1); setup <= 3; setup++ {
		phase(setup, setup-1)
	}
	result.SetupNanos = time.Since(started).Nanoseconds()
	phaseStarted := time.Now()
	phase(4, 0)
	result.BusinessPhases[0] = nativeCreateUpdatePhaseStop("create", phaseStarted, started)
	client := &nativeSelectorsClient{t: t, ctx: ctx, n: n, stream: reads, stage: "old-fresh"}
	var scanSequence [2]uint64
	client.observed = func(q nativeSelectorsInput, r nativeSelectorsReply) error {
		if q.Action != "scan" || client.stage != "old-fresh" && client.stage != "fresh-current" {
			return nil
		}
		index := 0
		if client.stage == "fresh-current" {
			index = 1
		}
		if family == nativeLateCorrectionFamily {
			return nativeLateCorrectionCount(spec, &result.BusinessPhases[index], &scanSequence[index], q, r)
		}
		return nativeCreateUpdateCount(spec, &result.BusinessPhases[index], &scanSequence[index], q, r)
	}
	oldQuery, old := nativeSelectorsQuery(t, client, nativeSelectorsInput{Action: "prepare", Request: nativeFeedRequest(digest, "query", 1), Mode: graphReadFresh})
	result.Old, result.OldToken = oldQuery.Cut, oldQuery.Token
	sequence := uint64(0)
	nativeSelectorsObserve(t, client, spec, oldQuery.Handle, 5, &sequence, nativeCreateUpdateCheckPoint)
	client.invoke(nativeSelectorsInput{Action: "close", Handle: oldQuery.Handle}, nil)
	var pairs [2]nativeGraphCostColumnPair
	var heldColumns [2]*nativeGraphEdgeUniqueColumnClient
	if spec.Layout == "columns" {
		pairs[0] = nativeScalarPublishColumns(t, ctx, n, p, initial, old, nativeFeedRequest(digest, "old-columns", 0), "old-seal", false, &result.ColumnFailed[0], columns, columnObserver)
		if columnObserver != nil {
			heldColumns = nativeGraphEdgeUniqueColumnClients(t, ctx, n, &pairs[0], "old-after-update", false, &result.ColumnFailed[0], columns)
		}
	}
	phaseStarted = time.Now()
	phase(5, 0)
	result.BusinessPhases[1] = nativeCreateUpdatePhaseStop("update", phaseStarted, started)
	result.BusinessNanos = result.BusinessPhases[0].WallNanos + result.BusinessPhases[1].WallNanos
	client.stage = "same-pin-old-after-update"
	held, heldCut := nativeSelectorsQuery(t, client, nativeSelectorsInput{Action: "reopen", Request: nativeFeedRequest(digest, "query", 2), Token: result.OldToken})
	if !feedCutEqual(heldCut, old) {
		t.Fatal("same retained old owner changed C")
	}
	sequence = 0
	nativeSelectorsObserve(t, client, spec, held.Handle, 5, &sequence, nativeCreateUpdateCheckPoint)
	if correction != nil {
		correction.observe(t, client, held.Handle, old, &sequence)
	}
	client.invoke(nativeSelectorsInput{Action: "close", Handle: held.Handle}, nil)
	client.stage = "fresh-current"
	current, final := nativeSelectorsQuery(t, client, nativeSelectorsInput{Action: "prepare", Request: nativeFeedRequest(digest, "query", 3), Mode: graphReadFresh})
	result.Final, result.FinalToken = current.Cut, current.Token
	sequence = 0
	nativeSelectorsObserve(t, client, spec, current.Handle, 6, &sequence, nativeCreateUpdateCheckPoint)
	if correction != nil {
		correction.observe(t, client, current.Handle, final, &sequence)
	}
	client.invoke(nativeSelectorsInput{Action: "close", Handle: current.Handle}, nil)
	if spec.Layout == "columns" {
		if columnObserver != nil {
			columnObserver.retireRefused(t, ctx, heldColumns)
			nativeScalarReadColumns(t, heldColumns, &pairs[0], p, old, false, false, columnObserver, false)
			nativeGraphEdgeUniqueColumnCloseClients(heldColumns)
		}
		pairs[1] = nativeScalarPublishColumns(t, ctx, n, p, initial, final, nativeFeedRequest(digest, "final-columns", 0), "final-seal", true, &result.ColumnFailed[1], columns, columnObserver)
		if columnObserver == nil {
			oldClients := nativeGraphEdgeUniqueColumnClients(t, ctx, n, &pairs[0], "old-after-update", false, &result.ColumnFailed[0], columns)
			nativeFeedReadColumns(t, oldClients, &pairs[0], p, old, false, false)
			nativeGraphEdgeUniqueColumnCloseClients(oldClients)
		}
		if columnObserver != nil {
			for generation := range pairs {
				pair := &pairs[generation]
				clients := nativeGraphEdgeUniqueColumnClients(t, ctx, n, pair, fmt.Sprintf("scan-points-%d", generation), false, &result.ColumnFailed[generation], columns)
				nativeScalarReadColumns(t, clients, pair, p, pair.Final, false, false, columnObserver, true)
				nativeGraphEdgeUniqueColumnCloseClients(clients)
			}
		}
	}
	source := func(stage string) [2]uint64 {
		return nativeScalarWorkloadSources(t, ctx, n, initial, final, stage, sources, func(after, through uint64, boundary []byte) ([]byte, error) {
			return nativeSelectorsInputWire(nativeSelectorsInput{Version: 1, Action: "records", From: after, Through: through, Cut: boundary})
		})
	}
	result.SourceRows = source("before-stop")
	rows := 3 + 4*spec.Facts
	if family == nativeLateCorrectionFamily {
		rows = 3 + 3*spec.Facts
	}
	if result.SourceRows[0]+result.SourceRows[1] != rows {
		t.Fatal("complete native scalar CDC census")
	}
	result.Transport = append(result.Transport, nativeCreateUpdateRestart(t, ctx, n, spec, admission, processes))
	for index, token := range [][]byte{result.OldToken, result.FinalToken} {
		client.stage = []string{"reopened-old", "reopened-final"}[index]
		query, actual := nativeSelectorsQuery(t, client, nativeSelectorsInput{Action: "reopen", Request: nativeFeedRequest(digest, "query", uint64(4+index)), Token: token})
		want, value := old, int64(5)
		if index == 1 {
			want, value = final, 6
			result.Reopened = query.Cut
		}
		if !feedCutEqual(actual, want) {
			t.Fatal("restart substituted current for retained cut")
		}
		sequence = 0
		nativeSelectorsObserve(t, client, spec, query.Handle, value, &sequence, nativeCreateUpdateCheckPoint)
		client.invoke(nativeSelectorsInput{Action: "release", Handle: query.Handle}, nil)
	}
	if spec.Layout == "columns" {
		for generation := range pairs {
			pair := &pairs[generation]
			for _, cached := range []bool{false, true} {
				clients := nativeGraphEdgeUniqueColumnClients(t, ctx, n, pair, fmt.Sprintf("reopened-%d-%t", generation, cached), false, &result.ColumnFailed[generation], columns)
				nativeScalarReadColumns(t, clients, pair, p, pair.Final, cached, false, columnObserver, false)
				nativeGraphEdgeUniqueColumnCloseClients(clients)
			}
			clients := nativeGraphEdgeUniqueColumnClients(t, ctx, n, pair, fmt.Sprintf("retire-%d", generation), false, &result.ColumnFailed[generation], columns)
			for _, c := range clients {
				r := c.call(nativeGraphCostColumnRequest{Action: nativeColumnsRetire})
				if r.Retention == nil || r.Retention.State != nativeColumnRetired || !r.Closed {
					t.Fatal("native generation retirement incomplete")
				}
				c.terminalProbe(pair.ID, r.Retention.Token)
			}
			result.ColumnBindings[generation], result.ColumnManifests[generation] = pair.Bindings, pair.Manifests
		}
	}
	if rows := source("reopened-replayed"); rows != result.SourceRows {
		t.Fatal("native reopen invented/omitted business effects")
	}
	result.Reaped = 6 + nativeGraphCostStop(t, n, 2, processes)
	result.Transport = append(result.Transport, n.all[1].mux.hub.workload.receipt(n.all[1].mux.hub))
	for member := uint64(1); member <= 6; member++ {
		logical, allocated, err := nativeGraphCostFileBytes(filepath.Join(n.dir, fmt.Sprint(member)))
		if err != nil || !nativeColumnAdd(result.PersistedFileBytes, logical) || !nativeColumnAdd(result.AllocatedFileBytes, allocated) {
			t.Fatal("all-voter actual disk census", err)
		}
		result.PersistedFileBytes += logical
		result.AllocatedFileBytes += allocated
	}
	result.OriginalCaptures, result.ExtraInvocations, result.QueryExtraInvocations = driver.originals, driver.extra, client.extra
	for _, phase := range result.BusinessPhases {
		useful := spec.Facts
		if family == nativeLateCorrectionFamily && phase.Phase == "update" {
			useful /= 2
		}
		if phase.SingleParticipant+phase.MultiParticipant != useful || phase.MultiParticipant != useful*spec.CrossPercent/100 {
			t.Fatal("observed full business footprint differs from admitted transaction fraction")
		}
		result.LogicalCross += phase.MultiParticipant
		result.LogicalLocal += phase.SingleParticipant
	}
	result.TotalNanos = time.Since(started).Nanoseconds()
	result.DiagnosticNanos = result.TotalNanos - result.SetupNanos - result.BusinessNanos
	if ctx.Err() != nil || result.DiagnosticNanos < 0 || result.Reaped != 12 {
		t.Fatal("incomplete native process/timing lifecycle", ctx.Err())
	}
	result.Scope = "genuine native local Submit at0 and actual mixed/cross graph variants; every update is Set6 then CorrectPresent with real existing-relation and endpoint-life predicates and one extra native presence revision/CDC row; remote update business reads are distinct from owner-only logical writes and full protocol participation; complete originals/old-current/index/CDC/voter/native+cache columns/reopen; setup excluded; installed-grant planned IDs, not Recipient.Next; source-only/no runtime/capacity/matrix/efficiency acceptance"
	if columnObserver != nil {
		summary, err := columnObserver.finish()
		if err != nil {
			t.Fatal(err)
		}
		result.ImmutableColumns = &summary
		result.Scope = "genuine native immutable publication/native+binary cache scans and batched scan-based typed probes, held old column reader across guarded updates, actual held-reader Retire refusal then authenticated Close/Retire/keeper refusal and physical same-directory restart; KV is source-scan reference; all retained derived bytes remain counted, no native physical derived-GC/source-GC or measurement/capacity/nonlocal parity acceptance"
	}
	if correction != nil {
		summary, err := correction.finish()
		if err != nil {
			t.Fatal(err)
		}
		result.Correction = &summary
		result.Scope += "; actual first-half scalar corrections append at new cut; complete old/current out/in5/6/phantom7 incident-source scans under same retained query owner, not value-index latency; unchanged cohort remains5"
	}
	if err := open("result.json").Encode(result); err != nil {
		t.Fatal(err)
	}
}
