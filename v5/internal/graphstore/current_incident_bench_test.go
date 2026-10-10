package graphstore

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

type currentIncidentBuild struct {
	Ended, Active                         int
	ActiveFirst                           bool
	ElapsedNS                             int64
	Operations, Batches, Writes, Refusals int
	AcceptedBatchWidths                   [17]int
	Work, CorrectionWork                  PageWork
	Retention                             raftlog.ApplicationUsage
	DirectoryLogicalFileBytes             int64
	BeforeCorrection, AfterCorrection     uint64
	OperationDigest                       string
}
type currentIncidentRead struct {
	Run           int
	AppliedIndex  uint64
	ValidPosition int64
	ElapsedNS     int64
	Complete      bool
	IDs           []graphstate.EntityID
	Refused       bool
	Error         string
	ActualWork    PageWork
	Counters      currentIncidentCounters
	// ReadMemStats is process-wide and includes any Pebble background activity.
	// It is neither the owned-output ledger nor a query-only/RSS measurement.
	ProcessAllocatedBytes, ProcessAllocations uint64
}
type currentIncidentMeasurement struct {
	Contract                  string
	Toolchain, OS, Arch, Base string
	GOMAXPROCS                int
	SourceSHA256              map[string]string
	Build                     currentIncidentBuild
	ExpectedIDs               []graphstate.EntityID
	Reads                     []currentIncidentRead
	Interpretation            string
}

// Construction uses at most16 operations per real Plan->Stage->Install batch,
// and a second acknowledged batch closes each ended relationship. The same
// immutable fixture serves all five reads. Never make five duplicate stores.
func buildCurrentIncidentPopulation(t testing.TB, ended int, activeFirst bool, fsys vfs.FS, dir string) (*currentIncidentFixture, currentIncidentBuild, []graphstate.EntityID, func(graphstate.EntityID) bool) {
	t.Helper()
	if ended < 0 || ended > 100_000 {
		t.Fatal("unsupported bounded fixture size", ended)
	}
	started := time.Now()
	f := newCurrentIncidentFixture(t, fsys, dir)
	w := f.span(t, 0, 100)
	f.apply(t, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: w}, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 2, Life: 11, Scope: w})
	activeStart := graphstate.EntityID(3 + ended)
	if activeFirst {
		activeStart = 3
	}
	activeEnd := activeStart + 9
	wasEnded := func(id graphstate.EntityID) bool {
		return id >= 3 && id < graphstate.EntityID(ended+13) && (id < activeStart || id > activeEnd)
	}
	for first := 3; first < ended+13; first += 8 {
		ops := make([]graphstate.Operation, 0, 16)
		closes := make([]graphstate.Operation, 0, 8)
		for raw := first; raw < min(first+8, ended+13); raw++ {
			id := graphstate.EntityID(raw)
			ops = append(ops, graphstate.Operation{Kind: graphstate.CreateRelationship, Owner: id, Life: 31, Scope: w, Record: graphstate.EntityRecord{Type: "R", Source: 1, Target: 2, Mode: graphstate.LifeBound}, Binding: graphstate.LifeRecord{SourceLife: 11, TargetLife: 11}}, graphstate.Operation{Kind: graphstate.Set, Owner: id, Life: 31, Scope: w, Name: "payload", Value: graphstate.String(strings.Repeat("p", 256)), ValueID: graphstate.ValueID(100 + raw)})
			if wasEnded(id) {
				closes = append(closes, graphstate.Operation{Kind: graphstate.Close, Owner: id, Life: 31, Scope: f.span(t, 50, 100)})
			}
		}
		f.applyBounded(t, ops, 2)
		if len(closes) > 0 {
			f.applyBounded(t, closes, 1)
		}
		if (first-3)%10_000 == 0 {
			t.Logf("built %d/%d relationships", min(first+5, ended+10), ended+10)
		}
	}
	before := f.index
	beforeWork := f.BuildWork
	restored := graphstate.EntityID(0)
	removed := graphstate.EntityID(0)
	want := make([]graphstate.EntityID, 10)
	for i := range want {
		want[i] = activeStart + graphstate.EntityID(i)
	}
	if ended > 0 {
		restored = 3
		if activeFirst {
			restored = 13
		}
		removed = activeStart
		f.apply(t, graphstate.Operation{Kind: graphstate.Correct, Owner: restored, Life: 31, Scope: f.span(t, 70, 80), Present: true}, graphstate.Operation{Kind: graphstate.Correct, Owner: removed, Life: 31, Scope: f.span(t, 70, 80), Present: false})
		want[0] = restored
		slices.Sort(want)
	}
	classifyEnded := func(id graphstate.EntityID) bool {
		return wasEnded(id) && id != restored || id == removed && removed != 0
	}
	usage, err := f.db.ApplicationUsage()
	if err != nil {
		t.Fatal(err)
	}
	build := currentIncidentBuild{Ended: ended, Active: 10, ActiveFirst: activeFirst, ElapsedNS: time.Since(started).Nanoseconds(), Operations: f.BuildOperations, Batches: f.BuildBatches, Writes: f.BuildWrites, Refusals: f.BuildRefusals, AcceptedBatchWidths: f.AcceptedBatchWidths, Work: f.BuildWork, CorrectionWork: currentWorkDelta(f.BuildWork, beforeWork), Retention: usage, BeforeCorrection: before, AfterCorrection: f.index, OperationDigest: hex.EncodeToString(f.digest.Sum(nil))}
	if fsys == vfs.Default {
		if err := filepath.WalkDir(dir, func(_ string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !entry.IsDir() {
				info, err := entry.Info()
				if err != nil {
					return err
				}
				build.DirectoryLogicalFileBytes += info.Size()
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	return f, build, want, classifyEnded
}

func TestCurrentIncidentMeasurementSmallBaselineIsNotActiveIndexEvidence(t *testing.T) {
	for _, early := range []bool{true, false} {
		t.Run(fmt.Sprint(early), func(t *testing.T) {
			f, build, want, ended := buildCurrentIncidentPopulation(t, 12, early, vfs.NewMem(), "current-small")
			q := currentIncidentQuery{Endpoint: 1, Life: 11, At: f.position(t, 75), Visible: graphstate.Effective, Type: "R"}
			ids, complete, counters, work, err := f.observe(t, f.index, q, ended, GraphLimits{})
			if err != nil || !complete {
				t.Fatal(err)
			}
			currentAssertIDs(t, ids, want)
			if counters.RawEndedDelivered != 12 || counters.EndedComponentCalls == 0 || counters.EndedComponentActualWork.DecodedCells == 0 || work.DecodedCells == 0 || build.CorrectionWork.PatchPages == 0 {
				t.Fatal("baseline hid ended decoding or correction work", counters, work, build)
			}
			if counters.EndedValueCalls != 0 {
				t.Fatal("ended properties should not be projected", counters)
			}
			// Active IDs at the start cannot justify stopping after ten. The raw
			// tail still has to complete, or the adapter must return a refusal.
			ids, complete, _, _, err = f.observe(t, f.index, q, ended, GraphLimits{MaxSourceRows: 150})
			if !errors.Is(err, ErrResourceLimit) || complete || len(ids) != 0 {
				t.Fatal("cliff returned an incomplete ten-result answer", ids, complete, err)
			}
		})
	}
}

// Explicit opt-in only:
// RHO_CURRENT_INCIDENT_MEASURE=1 go test -timeout45m -run
// '^TestCurrentIncidentMeasurements$' -v ./internal/graphstore
// Sizes default to10000,100000. RHO_CURRENT_INCIDENT_SIZES may select either.
// RHO_CURRENT_INCIDENT_OUTPUT names an existing external output directory.
// Normal short/full tests skip all large builds. Each subtest removes its one
// disk store before the next size, independent of where reports are retained.
func TestCurrentIncidentMeasurements(t *testing.T) {
	if testing.Short() || os.Getenv("RHO_CURRENT_INCIDENT_MEASURE") != "1" {
		t.Skip("explicit large disk measurement opt-in required")
	}
	sizes := os.Getenv("RHO_CURRENT_INCIDENT_SIZES")
	if sizes == "" {
		sizes = "10000,100000"
	}
	for text := range strings.SplitSeq(sizes, ",") {
		n, err := strconv.Atoi(text)
		if err != nil || n != 10_000 && n != 100_000 {
			t.Fatal("select only10000 or100000", text)
		}
		t.Run(text, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "store")
			f, build, want, ended := buildCurrentIncidentPopulation(t, n, false, vfs.Default, dir)
			measurement := currentIncidentMeasurement{Contract: "latest captured local view; explicit IntegerZ axis at75; endpoint1/life11; typeR; Effective; raw candidates then Project; maximum64 selected IDs; no active index", Toolchain: runtime.Version(), OS: runtime.GOOS, Arch: runtime.GOARCH, Base: os.Getenv("RHO_CURRENT_INCIDENT_BASE"), GOMAXPROCS: runtime.GOMAXPROCS(0), Build: build, ExpectedIDs: want, SourceSHA256: make(map[string]string), Interpretation: "ActualWork/DecodedCells are production PageWork counters and repeated materialization, not unique row counts. API calls/deliveries are separate. Process allocations include background work. Build/disk/retention are graphstore-fixture costs with placeholder command/CDC/outcome, without graphapply allocator/request composition; not production total storage. Build Work counts accepted staging only; elapsed time includes refused retries. Directory bytes are observed file lengths, not allocated disk/RSS. Five reads reuse a just-built warm store, not five cold-cache experiments. The wide rational case is a small correctness control, not a100k-wide benchmark. Refused reads provide no complete/partial answer and do not pass PLAN4.2."}
			for _, name := range []string{"current_incident_contract_test.go", "current_incident_bench_test.go"} {
				source, err := os.ReadFile(name)
				if err != nil {
					t.Fatal(err)
				}
				h := sha256.Sum256(source)
				measurement.SourceSHA256[name] = hex.EncodeToString(h[:])
			}
			q := currentIncidentQuery{Endpoint: 1, Life: 11, At: f.position(t, 75), Visible: graphstate.Effective, Type: "R"}
			for run := range 5 {
				runtime.GC()
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				started := time.Now()
				ids, complete, counters, work, err := f.observe(t, f.index, q, ended, GraphLimits{})
				elapsed := time.Since(started)
				runtime.ReadMemStats(&after)
				read := currentIncidentRead{Run: run + 1, AppliedIndex: f.index, ValidPosition: 75, ElapsedNS: elapsed.Nanoseconds(), Complete: complete, IDs: ids, ActualWork: work, Counters: counters, ProcessAllocatedBytes: after.TotalAlloc - before.TotalAlloc, ProcessAllocations: after.Mallocs - before.Mallocs}
				if err != nil {
					read.Refused = errors.Is(err, ErrResourceLimit) || errors.Is(err, graphstate.ErrResourceLimit)
					read.Error = err.Error()
					if !read.Refused || complete || len(ids) != 0 {
						t.Fatal("unexpected measured read failure", read)
					}
				} else {
					if !complete {
						t.Fatal("success without complete coverage")
					}
					currentAssertIDs(t, ids, want)
				}
				measurement.Reads = append(measurement.Reads, read)
				t.Logf("read %d ended=%d complete=%t refused=%t records=%d bytes=%d decoded_cells=%d ns=%d", run+1, n, complete, read.Refused, work.Records, work.Bytes, work.DecodedCells, elapsed.Nanoseconds())
			}
			data, err := json.MarshalIndent(measurement, "", "  ")
			if err != nil {
				t.Fatal(err)
			}
			t.Log(string(data))
			if output := os.Getenv("RHO_CURRENT_INCIDENT_OUTPUT"); output != "" {
				if err := os.WriteFile(filepath.Join(output, fmt.Sprintf("current-incident-%d.json", n)), append(data, '\n'), 0600); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

// Optional repeatable microbenchmark; construction is excluded by b.Loop.
// The large five-read evidence above is the controlled measurement lane.
func BenchmarkCurrentIncidentBaseline(b *testing.B) {
	if os.Getenv("RHO_CURRENT_INCIDENT_BENCH") != "1" {
		b.Skip("explicit benchmark opt-in required")
	}
	f, _, _, ended := buildCurrentIncidentPopulation(b, 64, false, vfs.NewMem(), "current-benchmark")
	q := currentIncidentQuery{Endpoint: 1, Life: 11, At: f.position(b, 75), Visible: graphstate.Effective, Type: "R"}
	b.ReportAllocs()
	for b.Loop() {
		_, complete, _, _, err := f.observe(b, f.index, q, ended, GraphLimits{})
		if err != nil || !complete {
			b.Fatal(err)
		}
	}
}
