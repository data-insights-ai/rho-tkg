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

type nativeSelectorsResult struct {
	Version                                                int                 `json:"version"`
	Format                                                 string              `json:"format"`
	Engine                                                 string              `json:"engine"`
	Spec                                                   nativeSelectorsSpec `json:"spec"`
	Admission                                              nativeSelectorsAdmission
	AdmissionSHA256                                        [32]byte
	Initial, Old, Final, Reopened                          []byte
	OldToken, FinalToken                                   []byte
	AfterLocal, AfterTransaction                           [16]byte
	Directory, ArtifactDirectory                           string
	Completed                                              [32]uint64
	OriginalCaptures, FramesCeiling                        uint64
	LogicalCross, LogicalLocal                             uint64
	ExtraInvocations, QueryExtraInvocations                int
	SourceRows                                             [2]uint64
	ColumnManifests                                        [2][2]nativeColumnManifest
	ColumnBindings                                         [2][2]nativeColumnBinding
	ColumnFailed                                           [2]uint64
	Transport                                              []nativeWorkloadTransportReceipt
	ProcessInstances, Reaped                               int
	PersistedFileBytes, AllocatedFileBytes                 uint64
	SetupNanos, BusinessNanos, DiagnosticNanos, TotalNanos int64
	RetentionContract, Scope                               string
	BusinessPhases                                         *[2]nativeGuardedScalarPhase `json:",omitzero"`
}

func nativeSelectorsQuery(t *testing.T, c *nativeSelectorsClient, q nativeSelectorsInput) (nativeSelectorsReply, graphCut) {
	t.Helper()
	started := time.Now()
	defer func() { c.openNanos = time.Since(started).Nanoseconds() }()
	r := c.invoke(q, nil)
	cut, err := nativeSelectorsCutDecode(r.Cut)
	if err != nil || r.Handle == 0 {
		t.Fatal("actual mode-selected query cut", err)
	}
	for _, cert := range cut.certificates {
		if cert.scope.semantic != durableColumnGraphSemanticContractID() {
			t.Fatal("unmatched actual source agreement")
		}
	}
	if len(cut.certificates) == 2 && q.Action == "prepare" {
		if len(r.Original) == 0 {
			t.Fatal("missing exact recipe before first issuance")
		}
		// The prepare reply has already been streamed. Publication now performs
		// genuine Issue and the complete all-participant common-live barrier.
		r.Token = c.invoke(nativeSelectorsInput{Action: "publish", Handle: r.Handle}, nil).Token
		if _, err := decodeRetainedPinToken(r.Token); err != nil {
			t.Fatal(err)
		}
	}
	return r, cut
}

func nativeSelectorsObserve(t *testing.T, c *nativeSelectorsClient, s nativeSelectorsSpec, handle uint64, expected int64, sequence *uint64, checks ...func(nativeFeedSpec, nativeFeedPoint, int64) error) {
	t.Helper()
	check := nativeFeedCheckPoint
	if s.Version == 2 && s.Family == nativeSelectorsFamily {
		check = nativeGuardedScalarCheckPoint
	}
	if len(checks) > 1 || len(checks) == 1 && checks[0] == nil {
		t.Fatal("one explicit fresh point validator required")
	}
	if len(checks) == 1 {
		check = checks[0]
	}
	started := time.Now()
	var count uint64
	pointSpec := nativeFeedSpec{Facts: s.Facts, CrossPercent: s.CrossPercent}
	if s.Version == 2 && s.Family == nativeSelectorsFamily {
		pointSpec.Version, pointSpec.Family = s.Version, s.Family
	}
	for {
		*sequence++
		r := c.invoke(nativeSelectorsInput{Action: "scan", Handle: handle, Sequence: *sequence}, nil)
		if len(r.Points) != len(r.Candidates) || len(r.Points) > 16 {
			t.Fatal("posting candidate/hydration omission")
		}
		for i, p := range r.Points {
			if expected == 0 || p.ID != 7+2*count || r.Candidates[i] != p.ID || check(pointSpec, nativeFeedPoint{Fact: count, Entity: p}, expected) != nil {
				t.Fatal("complete native posting scan differs at exact cut", count, p)
			}
			count++
		}
		if r.EOF {
			break
		}
		if *sequence > (s.Facts+15)/16+2 {
			t.Fatal("posting scan did not reach bounded real EOF")
		}
	}
	if expected == 0 && count != 0 || expected != 0 && count != s.Facts {
		t.Fatal("native exact-set scan cardinality", count)
	}
	scanNanos := time.Since(started).Nanoseconds()
	pointsStarted := time.Now()
	*sequence++
	ids := []uint64{1, 3, s.cost().remoteNode(), 7, 7 + 2*(s.Facts-1), s.cost().homeCount()}
	r := c.invoke(nativeSelectorsInput{Action: "points", Handle: handle, Sequence: *sequence, IDs: ids}, nil)
	if len(r.Points) != len(ids) {
		t.Fatal("fixed batched point omission")
	}
	for i, point := range r.Points {
		if point.ID != ids[i] || point.Found != (expected != 0 && i != len(ids)-1) {
			t.Fatal("point existence/current/phantom substitution", point)
		}
	}
	pointsNanos := time.Since(pointsStarted).Nanoseconds()
	// The completed query's SAME input recovers its exact last bounded result.
	// It performs no fresh capture or implicit cursor advance after EOF.
	replayed := c.invoke(nativeSelectorsInput{Action: "points", Handle: handle, Sequence: *sequence, IDs: ids}, nil)
	if !nativeColumnSame(r, replayed) {
		t.Fatal("SAME point query reply changed")
	}
	if err := c.stream.Encode(nativeSelectorsTiming{Type: "observation-timing", Stage: c.stage, OpenNanos: c.openNanos, ScanNanos: scanNanos, PointsNanos: pointsNanos, Rows: count}); err != nil {
		t.Fatal(err)
	}
}

func nativeSelectorsScoped(t *testing.T, c *nativeSelectorsClient, s nativeSelectorsSpec, digest [32]byte, mode graphReadMode, at graphCut, ordinal uint64, partition uint64) graphCut {
	t.Helper()
	var wire []byte
	var err error
	if len(at.certificates) != 0 {
		wire, err = nativeFeedCutWire(at)
		if err != nil {
			t.Fatal(err)
		}
	}
	r, actual := nativeSelectorsQuery(t, c, nativeSelectorsInput{Action: "prepare", Request: nativeFeedRequest(digest, "scoped-query", ordinal), Mode: mode, Cut: wire, Partitions: []uint64{partition}})
	if len(actual.certificates) != 1 || actual.certificates[0].scope.partition != partition || len(r.Original)+len(r.Token) != 0 {
		t.Fatal("scoped capture fabricated full native pin/source coverage")
	}
	inside, outside := uint64(1), s.cost().remoteNode()
	if partition == 8 {
		inside, outside = outside, inside
	}
	refusalStarted := time.Now()
	c.invoke(nativeSelectorsInput{Action: "points", Handle: r.Handle, Sequence: 1, IDs: []uint64{outside}}, errGraphCutScope)
	refusalNanos := time.Since(refusalStarted).Nanoseconds()
	pointsStarted := time.Now()
	point := c.invoke(nativeSelectorsInput{Action: "points", Handle: r.Handle, Sequence: 1, IDs: []uint64{inside}}, nil)
	if len(point.Points) != 1 || point.Points[0].ID != inside || !point.Points[0].Found || !point.Points[0].Effective || point.Points[0].EffectiveLife != inside+1 {
		t.Fatal("scope refusal poisoned subsequent authentic own-source read")
	}
	if err := c.stream.Encode(nativeSelectorsTiming{Type: "observation-timing", Stage: c.stage, OpenNanos: c.openNanos, PointsNanos: time.Since(pointsStarted).Nanoseconds(), ScopeRefusalNanos: refusalNanos}); err != nil {
		t.Fatal(err)
	}
	c.invoke(nativeSelectorsInput{Action: "close", Handle: r.Handle}, nil)
	return actual
}

// Genuine OS-child source-authored family. It is not a unit-suite shortcut,
// runtime/capacity receipt or a claim that unlike engine retention is equal.
func TestNativeGraphSnapshotSelectorsWorkloadCell(t *testing.T) {
	if testing.Short() || os.Getenv("RHO_GRAPH_NATIVE_WORKLOAD") != nativeSelectorsFormat {
		t.Skip("explicit native selectors workload required")
	}
	spec, err := nativeSelectorsSpecFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	admission, digest, err := nativeSelectorsAdmissionFromEnv(spec)
	if err != nil {
		t.Fatal(err)
	}
	runNativeSnapshotSelectorsWorkload(t, spec, admission, digest)
}

func runNativeSnapshotSelectorsWorkload(t *testing.T, spec nativeSelectorsSpec, admission nativeSelectorsAdmission, digest [32]byte) {
	t.Helper()
	var err error
	guarded := spec.Version == 2 && spec.Family == nativeSelectorsFamily
	format, version := nativeSelectorsFormat, 1
	if guarded {
		format, version = nativeGuardedSelectorsFormat, 2
	}
	ctx, cancel := context.WithDeadline(t.Context(), time.Unix(0, admission.DeadlineUnixNanos))
	defer cancel()
	started := time.Now()
	output := os.Getenv("RHO_GRAPH_PROCESS_ARTIFACT")
	if output == "" {
		t.Fatal("owned artifact directory required")
	}
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
	n := nativeSelectorsNetwork(t, spec, admission, grants)
	client := &nativeSelectorsClient{t: t, ctx: ctx, n: n, stream: reads, stage: "initial-empty-fresh"}
	result := nativeSelectorsResult{Version: version, Format: format, Engine: "native-raft", Spec: spec, Admission: admission, AdmissionSHA256: digest, Directory: n.dir, ArtifactDirectory: output, ProcessInstances: 12, RetentionContract: "snapshot-C-retained-full-prefix-v1"}
	result.FramesCeiling, err = nativeSelectorsFrames(spec.cost())
	if guarded {
		result.FramesCeiling, err = nativeGuardedScalarFrames(spec.cost())
		result.BusinessPhases = new([2]nativeGuardedScalarPhase)
	}
	if err != nil {
		t.Fatal(err)
	}
	result.LogicalCross = 2 * (spec.Facts * spec.CrossPercent / 100)
	result.LogicalLocal = 3 + 2*spec.Facts - result.LogicalCross
	initialQuery, initial := nativeSelectorsQuery(t, client, nativeSelectorsInput{Action: "prepare", Request: nativeFeedRequest(digest, "query", 1), Mode: graphReadFresh})
	result.Initial = initialQuery.Cut
	sequence := uint64(0)
	nativeSelectorsObserve(t, client, spec, initialQuery.Handle, 0, &sequence)
	client.invoke(nativeSelectorsInput{Action: "release", Handle: initialQuery.Handle}, nil)
	client.stage = "initial-empty-stable"
	stableEmpty, emptyCut := nativeSelectorsQuery(t, client, nativeSelectorsInput{Action: "prepare", Request: nativeFeedRequest(digest, "query", 2), Mode: graphReadStable})
	if !feedCutEqual(emptyCut, initial) {
		t.Fatal("stable empty changed native certificate")
	}
	sequence = 0
	nativeSelectorsObserve(t, client, spec, stableEmpty.Handle, 0, &sequence)
	client.invoke(nativeSelectorsInput{Action: "release", Handle: stableEmpty.Handle}, nil)
	driver := &nativeSelectorsDriver{ctx: ctx, n: n, spec: spec, started: started, admissions: newNativeGraphCostAdmissions()}
	driver.admissions.originals, driver.admissions.same = 64, 8
	driver.admissions.extra, driver.admissions.beginOriginal = driver.retry, driver.beginOriginal
	result.SetupNanos = time.Since(started).Nanoseconds()
	businessStarted := time.Now()
	for phase := uint64(1); phase <= 3; phase++ {
		driver.phase(t, phase, phase-1, attempts, operations, &result.Completed)
	}
	if guarded {
		result.SetupNanos = time.Since(started).Nanoseconds()
		businessStarted = time.Now()
	}
	driver.phase(t, 4, 0, attempts, operations, &result.Completed)
	result.BusinessNanos = time.Since(businessStarted).Nanoseconds()
	if guarded {
		result.BusinessPhases[0] = nativeGuardedScalarPhaseStop("create", businessStarted, started, spec.Facts)
	}
	var scoped [2]graphCut
	for group, part := range []uint64{3, 8} {
		client.stage = fmt.Sprintf("old-scope-fresh-%d", part)
		scoped[group] = nativeSelectorsScoped(t, client, spec, digest, graphReadFresh, graphCut{}, uint64(3+group), part)
	}
	client.stage = "old-fresh"
	oldQuery, old := nativeSelectorsQuery(t, client, nativeSelectorsInput{Action: "prepare", Request: nativeFeedRequest(digest, "query", 5), Mode: graphReadFresh})
	result.Old, result.OldToken = oldQuery.Cut, oldQuery.Token
	sequence = 0
	nativeSelectorsObserve(t, client, spec, oldQuery.Handle, 5, &sequence)
	client.invoke(nativeSelectorsInput{Action: "close", Handle: oldQuery.Handle}, nil)
	var pairs [2]nativeGraphCostColumnPair
	if spec.Layout == "columns" {
		pairs[0] = nativeFeedPublishColumns(t, ctx, n, spec.cost(), initial, old, nativeFeedRequest(digest, "old-columns", 0), "old-seal", false, &result.ColumnFailed[0], columns)
	}
	businessStarted = time.Now()
	after := driver.phase(t, 5, 0, attempts, operations, &result.Completed)
	result.BusinessNanos += time.Since(businessStarted).Nanoseconds()
	if guarded {
		result.BusinessPhases[1] = nativeGuardedScalarPhaseStop("update", businessStarted, started, spec.Facts)
		result.BusinessNanos = result.BusinessPhases[0].WallNanos + result.BusinessPhases[1].WallNanos
	}
	client.stage = "stable-after-update"
	stable, stableCut := nativeSelectorsQuery(t, client, nativeSelectorsInput{Action: "prepare", Request: nativeFeedRequest(digest, "query", 6), Mode: graphReadStable})
	if !feedCutEqual(stableCut, old) {
		t.Fatal("Stable silently became current state")
	}
	sequence = 0
	nativeSelectorsObserve(t, client, spec, stable.Handle, 5, &sequence)
	client.invoke(nativeSelectorsInput{Action: "release", Handle: stable.Handle}, nil)
	for group, part := range []uint64{3, 8} {
		client.stage = fmt.Sprintf("stable-scope-after-update-%d", part)
		actual := nativeSelectorsScoped(t, client, spec, digest, graphReadStable, scoped[group], uint64(7+group), part)
		if !feedCutEqual(actual, scoped[group]) {
			t.Fatal("known scoped Stable certificate changed")
		}
	}
	client.stage = "same-pin-old-after-update"
	held, heldCut := nativeSelectorsQuery(t, client, nativeSelectorsInput{Action: "reopen", Request: nativeFeedRequest(digest, "query", 9), Token: result.OldToken})
	if !feedCutEqual(heldCut, old) {
		t.Fatal("same durable old owner changed C")
	}
	sequence = 0
	nativeSelectorsObserve(t, client, spec, held.Handle, 5, &sequence)
	client.invoke(nativeSelectorsInput{Action: "close", Handle: held.Handle}, nil)
	client.stage = "at-old-after-update"
	at, _ := nativeSelectorsQuery(t, client, nativeSelectorsInput{Action: "prepare", Request: nativeFeedRequest(digest, "query", 10), Mode: graphReadAt, Cut: result.Old})
	sequence = 0
	nativeSelectorsObserve(t, client, spec, at.Handle, 5, &sequence)
	client.invoke(nativeSelectorsInput{Action: "release", Handle: at.Handle}, nil)
	client.stage = "after-current"
	afterInput := nativeSelectorsInput{Action: "prepare", Request: nativeFeedRequest(digest, "query", 11), Mode: graphReadAfter, Coordinator: 3}
	if after.tx != ([16]byte{}) {
		afterInput.Transaction, result.AfterTransaction = after.tx, after.tx
	} else {
		afterInput.Local, result.AfterLocal = after.identity, after.identity
	}
	finalQuery, final := nativeSelectorsQuery(t, client, afterInput)
	result.Final, result.FinalToken = finalQuery.Cut, finalQuery.Token
	sequence = 0
	nativeSelectorsObserve(t, client, spec, finalQuery.Handle, 6, &sequence)
	client.invoke(nativeSelectorsInput{Action: "close", Handle: finalQuery.Handle}, nil)
	client.stage = "fresh-current"
	freshQuery, freshCut := nativeSelectorsQuery(t, client, nativeSelectorsInput{Action: "prepare", Request: nativeFeedRequest(digest, "query", 12), Mode: graphReadFresh})
	if !feedCutEqual(freshCut, final) {
		t.Fatal("quiescent Fresh changed complete business certificate")
	}
	sequence = 0
	nativeSelectorsObserve(t, client, spec, freshQuery.Handle, 6, &sequence)
	client.invoke(nativeSelectorsInput{Action: "release", Handle: freshQuery.Handle}, nil)
	if spec.Layout == "columns" {
		pairs[1] = nativeFeedPublishColumns(t, ctx, n, spec.cost(), initial, final, nativeFeedRequest(digest, "final-columns", 0), "final-seal", true, &result.ColumnFailed[1], columns)
		oldClients := nativeGraphEdgeUniqueColumnClients(t, ctx, n, &pairs[0], "old-after-update", false, &result.ColumnFailed[0], columns)
		nativeFeedReadColumns(t, oldClients, &pairs[0], spec.cost(), old, false, false)
		nativeGraphEdgeUniqueColumnCloseClients(oldClients)
	}
	result.SourceRows = nativeSelectorsSources(t, ctx, n, initial, final, "before-stop", sources)
	if result.SourceRows[0]+result.SourceRows[1] != 3+(3+boolUint(guarded))*spec.Facts {
		t.Fatal("complete native scalar before/after row census", result.SourceRows)
	}
	result.Transport = append(result.Transport, nativeSelectorsRestart(t, ctx, n, spec, admission, processes))
	for index, token := range [][]byte{result.OldToken, result.FinalToken} {
		client.stage = []string{"reopened-old", "reopened-final"}[index]
		query, actual := nativeSelectorsQuery(t, client, nativeSelectorsInput{Action: "reopen", Request: nativeFeedRequest(digest, "query", uint64(13+index)), Token: token})
		want, value := old, int64(5)
		if index == 1 {
			want, value = final, 6
			result.Reopened = query.Cut
		}
		if !feedCutEqual(actual, want) {
			t.Fatal("reopen used copied metadata as a current cut")
		}
		sequence = 0
		nativeSelectorsObserve(t, client, spec, query.Handle, value, &sequence)
		client.invoke(nativeSelectorsInput{Action: "release", Handle: query.Handle}, nil)
	}
	if spec.Layout == "columns" {
		for generation := range pairs {
			pair := &pairs[generation]
			for _, cached := range []bool{false, true} {
				clients := nativeGraphEdgeUniqueColumnClients(t, ctx, n, pair, fmt.Sprintf("reopened-%d-%t", generation, cached), false, &result.ColumnFailed[generation], columns)
				nativeFeedReadColumns(t, clients, pair, spec.cost(), pair.Final, cached, false)
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
	if rows := nativeSelectorsSources(t, ctx, n, initial, final, "reopened-replayed", sources); rows != result.SourceRows {
		t.Fatal("reopen/retirement invented another business delta")
	}
	result.Reaped = 6 + nativeGraphCostStop(t, n, 2, processes)
	result.Transport = append(result.Transport, n.all[1].mux.hub.workload.receipt(n.all[1].mux.hub))
	for member := uint64(1); member <= 6; member++ {
		logical, allocated, err := nativeGraphCostFileBytes(filepath.Join(n.dir, fmt.Sprint(member)))
		if err != nil || !nativeColumnAdd(result.PersistedFileBytes, logical) || !nativeColumnAdd(result.AllocatedFileBytes, allocated) {
			t.Fatal("all-voter actual file byte census", err)
		}
		result.PersistedFileBytes += logical
		result.AllocatedFileBytes += allocated
	}
	result.OriginalCaptures, result.ExtraInvocations, result.QueryExtraInvocations = driver.originals, driver.extra, client.extra
	result.TotalNanos = time.Since(started).Nanoseconds()
	result.DiagnosticNanos = result.TotalNanos - result.SetupNanos - result.BusinessNanos
	if ctx.Err() != nil || result.DiagnosticNanos < 0 || result.Reaped != 12 {
		t.Fatal("terminal ownership/timing", ctx.Err())
	}
	result.Scope = "Genuine native Fresh/Stable/After/At, scoped refusal, complete posting scans and typed old/current/raw/original/voter/column/cache/reopen answers. Planned IDs use authentic installed-grant references, not Recipient.Next; allocator API/cost matching, runtime/full matrix and retention-equivalent costs remain unqualified."
	if err := open("result.json").Encode(result); err != nil {
		t.Fatal(err)
	}
}
