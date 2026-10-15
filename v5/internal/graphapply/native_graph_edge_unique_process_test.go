package graphapply

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

const nativeGraphEdgeUniqueFormat = "native-edge-endpoint-unique-v1"

type nativeGraphEdgeUniqueSpec struct {
	Version      int    `json:"version"`
	Family       string `json:"family"`
	Facts        uint64 `json:"facts"`
	CrossPercent uint64 `json:"crossPercent"`
	Writers      int    `json:"writers"`
	Layout       string `json:"layout"`
}

func (s nativeGraphEdgeUniqueSpec) cost() nativeGraphCostSpec {
	return nativeGraphCostSpec{Version: 4, Facts: s.Facts, CrossPercent: s.CrossPercent, Writers: s.Writers}
}
func nativeGraphEdgeUniqueSpecFromEnv() (nativeGraphEdgeUniqueSpec, error) {
	var s nativeGraphEdgeUniqueSpec
	body := os.Getenv("RHO_GRAPH_WORKLOAD_SPEC")
	if len(body) == 0 || len(body) > 4096 {
		return s, errInvalid
	}
	d := json.NewDecoder(bytes.NewBufferString(body))
	d.DisallowUnknownFields()
	if err := d.Decode(&s); err != nil {
		return s, errors.Join(errInvalid, err)
	}
	if err := d.Decode(new(any)); !errors.Is(err, io.EOF) || s.Version != 1 || s.Family != nativeGraphEdgeUniqueFamily || s.Layout != "kv" && s.Layout != "columns" || nativeGraphEdgeUniqueValidate(s.cost()) != nil {
		return s, errInvalid
	}
	return s, nil
}

type nativeGraphEdgeUniqueResult struct {
	Version                                    int                       `json:"version"`
	Format                                     string                    `json:"format"`
	Engine                                     string                    `json:"engine"`
	Spec                                       nativeGraphEdgeUniqueSpec `json:"spec"`
	Admission                                  nativeGraphEdgeUniqueRunBinding
	BinarySHA256                               string
	Initial, Old, Final, BeforeStop, Replayed  []byte
	Directory, ArtifactDirectory               string
	Attempts, Commits, Aborts, Refusals        uint64
	Completed                                  [32]uint64
	ExtraInvocations                           int
	FramesCeiling                              uint64
	SourceRows                                 [2]uint64
	ColumnPairs                                [2]nativeGraphCostColumnPair `json:"-"`
	ColumnManifests                            [2][2]nativeColumnManifest
	ColumnBindings                             [2][2]nativeColumnBinding
	ColumnFailed                               [2]uint64
	Reaped, ProcessInstances                   int
	PersistedFileBytes, AllocatedFileBytes     uint64
	TotalNanos                                 int64
	SetupNanos, BusinessNanos, DiagnosticNanos int64
	OccupiedDiagnosticNanos                    int64
	GrantReplays                               int
	Scope                                      string
}

// This is an explicit workload dispatcher, excluded from routine short CI.
// It is source-authored only until the separate correctness/measurement phase.
func TestNativeGraphEdgeUniqueWorkloadCell(t *testing.T) {
	if testing.Short() {
		t.Skip("explicit full native workload excluded from routine short gates")
	}
	if os.Getenv("RHO_GRAPH_NATIVE_WORKLOAD") != nativeGraphEdgeUniqueFormat {
		t.Skip("explicit fresh edge family required")
	}
	spec, err := nativeGraphEdgeUniqueSpecFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	admission, digest, err := nativeGraphEdgeUniqueAdmission(spec.cost())
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Unix(0, admission.DeadlineUnixNanos)
	ctx, stop := context.WithDeadline(t.Context(), deadline)
	defer stop()
	started := time.Now()
	output := os.Getenv("RHO_GRAPH_WORKLOAD_OUTPUT")
	if !filepath.IsAbs(output) {
		t.Fatal("absolute owned artifact root required")
	}
	if err := os.Mkdir(output, 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RHO_GRAPH_PROCESS_ARTIFACT", output)
	open := func(name string) (*os.File, *json.Encoder) {
		f, err := os.OpenFile(filepath.Join(output, name), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := f.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
				t.Error(err)
			}
		})
		return f, json.NewEncoder(f)
	}
	_, grants := open("grants.jsonl")
	n := newNativeGraphEdgeUniqueNetwork(t, spec.cost(), deadline, admission.BudgetSeconds, grants)
	d := &nativeGraphEdgeUniqueDriver{n: n, spec: spec.cost(), ctx: ctx, slots: make(chan uint64, 2), started: started}
	d.slots <- 1
	d.slots <- 2
	_, operations := open("operations.jsonl")
	_, points := open("points.jsonl")
	_, sources := open("sources.jsonl")
	_, processes := open("processes.jsonl")
	_, states := open("restore.jsonl")
	_, duplicates := open("duplicates.jsonl")
	winners, _ := open("winners.jsonl")
	frozen, _ := open("frozen.jsonl")
	closes, closeWriter := open("close-originals.jsonl")
	result := nativeGraphEdgeUniqueResult{Version: 1, Format: nativeGraphEdgeUniqueFormat, Engine: "native-raft", Spec: spec, Admission: admission, BinarySHA256: nativeContentionExecutableSHA(t), Directory: n.dir, ArtifactDirectory: output, ProcessInstances: 12}
	result.FramesCeiling, err = nativeGraphEdgeUniqueFrames(spec.cost())
	if err != nil {
		t.Fatal(err)
	}
	emit := func(a nativeGraphEdgeUniqueAttempt) error {
		result.Attempts++
		if a.Client < 0 || a.Client >= spec.Writers {
			return errCorrupt
		}
		result.Completed[a.Client]++
		if a.StartNanos < 0 || a.StopNanos-a.StartNanos != a.ElapsedNanos || a.ElapsedNanos <= 0 || a.AdmissionNanos < 0 || a.AdmissionNanos > a.ElapsedNanos {
			return errCorrupt
		}
		if a.Role == "occupied" {
			result.OccupiedDiagnosticNanos += a.ElapsedNanos
		}
		switch a.Classification {
		case "commit":
			result.Commits++
		case "abort":
			result.Aborts++
		case "pre-prepare-unique", "pre-prepare-endpoint-life":
			result.Refusals++
		default:
			return errors.Join(errCorrupt, errors.New(a.Error))
		}
		if result.Attempts > 10*spec.Facts+7 {
			return errLimit
		}
		body, err := json.Marshal(a)
		if err != nil || len(body)+1 > 512<<10 {
			return errors.Join(errLimit, err)
		}
		if _, err = nativeGraphEdgeUniqueWriteOperation(operations, body); err != nil {
			return err
		}
		if a.Operation.Phase == "close" && a.Classification == "commit" {
			return closeWriter.Encode(a)
		}
		return nil
	}
	initial := nativeGraphEdgeUniqueFresh(t, n)
	result.Initial = nativeGraphCostColumnCutInput(t, initial)
	result.SetupNanos = time.Since(started).Nanoseconds()
	businessStarted := time.Now()
	for fact := uint64(0); fact < 3; fact++ {
		a, err := d.submit(nativeGraphEdgeUniqueJob{Operation: nativeGraphEdgeUniqueOperation(spec.cost(), "setup", fact, 0), Attempt: 1}, int(fact%uint64(spec.Writers)))
		if err != nil || a.Classification != "commit" {
			t.Fatal("real setup", err)
		}
		if err := emit(a); err != nil {
			t.Fatal(err)
		}
	}
	nativeGraphEdgeUniqueClaims(t, d, winners, emit)
	old := nativeGraphEdgeUniqueFresh(t, n)
	result.Old = nativeGraphCostColumnCutInput(t, old)
	nativeGraphEdgeUniqueFreezeOld(t, d, winners, frozen, emit)
	result.BusinessNanos = time.Since(businessStarted).Nanoseconds() - result.OccupiedDiagnosticNanos
	var columns *json.Encoder
	if spec.Layout == "columns" {
		_, columns = open("columns.jsonl")
		result.ColumnPairs[0] = nativeGraphEdgeUniqueColumnPublish(t, ctx, n, spec.cost(), initial, old, nativeGraphEdgeUniqueGenerationID("old", digest), "old-seal", false, &result.ColumnFailed[0], columns)
	}
	businessStarted = time.Now()
	for fact := uint64(0); fact < 4; fact++ {
		operation := nativeGraphEdgeUniqueOperation(spec.cost(), "life", fact, 0)
		a, err := d.submit(nativeGraphEdgeUniqueJob{Operation: operation, Attempt: 1}, int((operation.Ordinal-1)%uint64(spec.Writers)))
		if err != nil || a.Classification != "commit" {
			t.Fatal("genuine endpoint close/new-life", err)
		}
		if err := emit(a); err != nil {
			t.Fatal(err)
		}
	}
	for _, phase := range []string{"stale", "replace", "frozen-close", "close"} {
		nativeGraphEdgeUniquePhase(t, d, frozen, phase, emit)
	}
	if err := closes.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := closes.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(closes)
	scanner.Buffer(make([]byte, 64<<10), 512<<10)
	var last nativeGraphEdgeUniqueAttempt
	var replayCount uint64
	for scanner.Scan() {
		var a nativeGraphEdgeUniqueAttempt
		if json.Unmarshal(scanner.Bytes(), &a) != nil {
			t.Fatal("saved actual original")
		}
		nativeGraphEdgeUniqueReplay(t, d, a, duplicates)
		last = a
		replayCount++
	}
	if err := scanner.Err(); err != nil || replayCount != spec.Facts {
		t.Fatal("complete original old-close replay census", err)
	}
	final := nativeGraphEdgeUniqueFresh(t, n)
	result.Final = nativeGraphCostColumnCutInput(t, final)
	if result.Commits != 3*spec.Facts+7 {
		t.Fatal("complete genuine useful lifecycle", result.Commits)
	}
	result.BusinessNanos += time.Since(businessStarted).Nanoseconds()
	nativeGraphEdgeUniquePoints(t, n, spec.cost(), winners, old, false, "old-after-ABA", points)
	nativeGraphEdgeUniquePoints(t, n, spec.cost(), winners, final, true, "final", points)
	if columns != nil {
		result.ColumnPairs[1] = nativeGraphEdgeUniqueColumnPublish(t, ctx, n, spec.cost(), initial, final, nativeGraphEdgeUniqueGenerationID("final", digest), "final-seal", true, &result.ColumnFailed[1], columns)
		clients := nativeGraphEdgeUniqueColumnClients(t, ctx, n, &result.ColumnPairs[0], "old-after-ABA", false, &result.ColumnFailed[0], columns)
		nativeGraphEdgeUniqueColumnReadPair(t, clients, &result.ColumnPairs[0], spec.cost(), old, false, false)
		nativeGraphEdgeUniqueColumnCloseClients(clients)
	}
	beforeStop := nativeGraphEdgeUniqueFresh(t, n)
	result.BeforeStop = nativeGraphCostColumnCutInput(t, beforeStop)
	result.SourceRows = nativeGraphEdgeUniqueSources(t, ctx, n, initial, beforeStop, "before-stop", sources)
	if result.SourceRows[0]+result.SourceRows[1] != 5*spec.Facts+7 {
		t.Fatal("native typed CDC exact full census", result.SourceRows)
	}
	nativeGraphEdgeUniqueRestart(t, n, spec.cost(), deadline, admission.BudgetSeconds, processes, states)
	nativeGraphEdgeUniquePoints(t, n, spec.cost(), winners, old, false, "reopened-old", points)
	nativeGraphEdgeUniquePoints(t, n, spec.cost(), winners, final, true, "reopened-final", points)
	nativeGraphEdgeUniqueReplay(t, d, last, duplicates)
	_, grantReplays := open("grant-replays.jsonl")
	result.GrantReplays = int(nativeContentionReplayGrants(t, ctx, n, filepath.Join(output, "grants.jsonl"), grantReplays)) // #nosec G115 -- actual finite <=18 grant originals.
	if columns != nil {
		for generation := range result.ColumnPairs {
			pair := &result.ColumnPairs[generation]
			for _, cached := range []bool{false, true} {
				clients := nativeGraphEdgeUniqueColumnClients(t, ctx, n, pair, fmt.Sprintf("reopened-%d-cached-%t", generation, cached), false, &result.ColumnFailed[generation], columns)
				nativeGraphEdgeUniqueColumnReadPair(t, clients, pair, spec.cost(), pair.Final, cached, false)
				nativeGraphEdgeUniqueColumnCloseClients(clients)
			}
			clients := nativeGraphEdgeUniqueColumnClients(t, ctx, n, pair, fmt.Sprintf("retire-%d", generation), false, &result.ColumnFailed[generation], columns)
			for _, client := range clients {
				r := client.call(nativeGraphCostColumnRequest{Action: nativeColumnsRetire})
				if r.Retention == nil || r.Retention.State != nativeColumnRetired || !r.Closed {
					t.Fatal("native retirement not terminal")
				}
				client.terminalProbe(pair.ID, r.Retention.Token)
			}
			result.ColumnManifests[generation], result.ColumnBindings[generation] = pair.Manifests, pair.Bindings
		}
	}
	replayed := nativeGraphEdgeUniqueFresh(t, n)
	result.Replayed = nativeGraphCostColumnCutInput(t, replayed)
	if actual := nativeGraphEdgeUniqueSources(t, ctx, n, initial, replayed, "reopened-replayed", sources); actual != result.SourceRows {
		t.Fatal("replay or retirement changed business CDC")
	}
	result.ExtraInvocations = d.extra
	result.Reaped = nativeGraphCostStop(t, n, 2, processes)
	for member := uint64(1); member <= 6; member++ {
		logical, allocated, err := nativeGraphCostFileBytes(filepath.Join(n.dir, fmt.Sprint(member)))
		if err != nil || !nativeColumnAdd(result.PersistedFileBytes, logical) || !nativeColumnAdd(result.AllocatedFileBytes, allocated) {
			t.Fatal("all voter/derived/cache bytes", err)
		}
		result.PersistedFileBytes += logical
		result.AllocatedFileBytes += allocated
	}
	result.TotalNanos = time.Since(started).Nanoseconds()
	result.DiagnosticNanos = result.TotalNanos - result.SetupNanos - result.BusinessNanos
	if result.BusinessNanos <= 0 || result.DiagnosticNanos < result.OccupiedDiagnosticNanos {
		t.Fatal("coherent business/diagnostic timing partition")
	}
	result.Scope = "One actual fresh effective-unique edge/endpoint-life native cell. Complete authentic original/voter/CDC histories, old/current exact typed observations, durable derived/cache generations and same-directory restart; fixed declared caller budget and independent writers. Source only until separately executed; no full matrix/capacity/performance acceptance."
	if ctx.Err() != nil || result.Reaped != 6 {
		t.Fatal("terminal owned cleanup", ctx.Err())
	}
	for client := 0; client < spec.Writers; client++ {
		if result.Completed[client] == 0 {
			t.Fatal("declared writer never completed an actual request", client)
		}
	}
	f, end := open("result.json")
	if err := errors.Join(end.Encode(result), f.Close()); err != nil {
		t.Fatal(err)
	}
}

// Marshal admission is performed before the encoder owns a representation.
func nativeGraphEdgeUniqueWriteOperation(encoder *json.Encoder, body []byte) (int, error) {
	return len(body), encoder.Encode(json.RawMessage(body))
}
