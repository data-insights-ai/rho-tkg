package raftlog

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
)

// This is fallback storage evidence, not evidence that a consumption index is
// required. Qualified immutable identity catalogs may make it unnecessary.
// Timing/allocations cover installation with the common Raft/application work;
// exact logical common retention is reported separately. Disk totals include
// backend metadata/logs; no payload-only ratio is presented as total cost.
type consumptionMeasurement struct {
	Case                                               string          `json:"case"`
	Candidate                                          consumptionKind `json:"candidate"`
	Repeat                                             int             `json:"repeat"`
	IDs, Commands                                      int
	RetainedBytes, CommonBytes, ConsumptionBytes       uint64
	RetainedRecords, CommonRecords, ConsumptionRecords uint64
	IngestNanos, AllocatedBytes, Allocations           uint64
	ClosedBeforeCompact, ClosedAfterCompact            map[string]int64
}

func consumptionFootprint(tb testing.TB, fs vfs.FS, dir string) map[string]int64 {
	tb.Helper()
	sizes := map[string]int64{"total": 0, "wal": 0, "sst": 0, "manifest": 0, "options": 0, "other": 0}
	var walk func(string)
	files := 0
	walk = func(path string) {
		names, err := fs.List(path)
		if err != nil {
			tb.Fatal(err)
		}
		for _, name := range names {
			file := filepath.Join(path, name)
			info, err := fs.Stat(file)
			if err != nil {
				tb.Fatal(err)
			}
			if info.IsDir() {
				walk(file)
				continue
			}
			files++
			if files > 4096 {
				tb.Fatal("bounded footprint file count exceeded")
			}
			kind := "other"
			switch {
			case strings.HasSuffix(name, ".log"):
				kind = "wal"
			case strings.HasSuffix(name, ".sst"):
				kind = "sst"
			case strings.HasPrefix(name, "MANIFEST-"):
				kind = "manifest"
			case strings.HasPrefix(name, "OPTIONS-"):
				kind = "options"
			}
			sizes[kind] += info.Size()
			sizes["total"] += info.Size()
		}
	}
	walk(dir)
	return sizes
}
func measureConsumption(tb testing.TB, fs vfs.FS, dir string, kind consumptionKind, fixture consumptionFixture, disk bool) consumptionMeasurement {
	tb.Helper()
	s, cfg, ledger, baseBytes := consumptionOpen(tb, fs, dir, kind)
	initialUsage, err := s.ApplicationUsage()
	if err != nil {
		tb.Fatal(err)
	}
	measurement := consumptionMeasurement{Case: fixture.name, Candidate: kind, IDs: len(fixture.ids), CommonBytes: baseBytes, CommonRecords: initialUsage.RetainedRecords}
	if measurement.IDs > 4096 {
		tb.Fatal("fixture exceeds actual-used ID cap")
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	start := time.Now()
	index := uint64(2)
	middle := index
	middleCount := 0
	for from := 0; from < len(fixture.ids); from += fixture.batch {
		to := min(from+fixture.batch, len(fixture.ids))
		var extra uint64
		var records int
		index, extra, records = consumptionCommit(tb, s, ledger, fixture.ids[from:to])
		measurement.ConsumptionBytes += extra
		measurement.ConsumptionRecords += uint64(records)
		measurement.Commands++
		if middleCount == 0 && to >= len(fixture.ids)/2 {
			middle, middleCount = index, to
		}
	}
	measurement.IngestNanos = uint64(time.Since(start).Nanoseconds())
	runtime.ReadMemStats(&after)
	measurement.AllocatedBytes = after.TotalAlloc - before.TotalAlloc
	measurement.Allocations = after.Mallocs - before.Mallocs
	usage, err := s.ApplicationUsage()
	if err != nil {
		tb.Fatal(err)
	}
	measurement.RetainedBytes, measurement.RetainedRecords = usage.RetainedBytes, usage.RetainedRecords
	measurement.CommonBytes = usage.RetainedBytes - measurement.ConsumptionBytes
	measurement.CommonRecords = usage.RetainedRecords - measurement.ConsumptionRecords
	// Independently derive the common envelope ledger, including all MVCC roots,
	// CDC/outcomes and the same durable allocator/grant/recipient setup.
	commonWant := baseBytes + uint64(measurement.Commands*(3*(9+appFrameBytes)+140+48)+8*len(fixture.ids))
	if measurement.CommonBytes != commonWant || measurement.CommonRecords != initialUsage.RetainedRecords+uint64(3*measurement.Commands) {
		tb.Fatal("common ledgers diverged", measurement, commonWant)
	}
	if disk {
		consumptionAssert(tb, s, ledger, middle, fixture.ids[:middleCount])
		consumptionAssert(tb, s, ledger, index, fixture.ids)
		if err := s.Close(); err != nil {
			tb.Fatal(err)
		}
		measurement.ClosedBeforeCompact = consumptionFootprint(tb, fs, dir)
		cfg.Create = false
		s, err = Open(cfg)
		if err != nil {
			tb.Fatal(err)
		}
		if err := consumptionCompact(tb.Context(), s); err != nil {
			tb.Fatal(err)
		}
		consumptionAssert(tb, s, ledger, 2, nil)
		consumptionAssert(tb, s, ledger, middle, fixture.ids[:middleCount])
		consumptionAssert(tb, s, ledger, index, fixture.ids)
		compacted, err := s.ApplicationUsage()
		if err != nil || compacted.RetainedBytes != usage.RetainedBytes || compacted.RetainedRecords != usage.RetainedRecords {
			tb.Fatal("compaction changed retained ledger", err)
		}
		if err := s.ScrubApplication(tb.Context()); err != nil {
			tb.Fatal(err)
		}
		if err := s.Close(); err != nil {
			tb.Fatal(err)
		}
		measurement.ClosedAfterCompact = consumptionFootprint(tb, fs, dir)
		// A second reopen verifies completed compacted storage, not just live cache.
		s, err = Open(cfg)
		if err != nil {
			tb.Fatal(err)
		}
		consumptionAssert(tb, s, ledger, middle, fixture.ids[:middleCount])
		consumptionAssert(tb, s, ledger, index, fixture.ids)
	}
	if err := s.Close(); err != nil {
		tb.Fatal(err)
	}
	return measurement
}
func BenchmarkIDConsumption(b *testing.B) {
	for _, fixture := range consumptionFixtures() {
		for _, kind := range consumptionKinds {
			b.Run(fmt.Sprintf("%s/%s", fixture.name, kind), func(b *testing.B) {
				b.ReportAllocs()
				var sample consumptionMeasurement
				for b.Loop() {
					sample = measureConsumption(b, vfs.NewMem(), "benchmark", kind, fixture, false)
				}
				ids := float64(sample.IDs)
				b.ReportMetric(float64(sample.RetainedBytes)/ids, "retained-B/id")
				b.ReportMetric(float64(sample.CommonBytes)/ids, "common-B/id")
				b.ReportMetric(float64(sample.ConsumptionBytes)/ids, "consumption-B/id")
				b.ReportMetric(float64(sample.ConsumptionRecords)/ids, "consumption-records/id")
				b.ReportMetric(float64(sample.IngestNanos)/ids, "install-ns/id")
				b.ReportMetric(float64(sample.AllocatedBytes)/ids, "install-allocated-B/id")
				b.ReportMetric(float64(sample.Allocations)/ids, "install-allocs/id")
			})
		}
	}
}
func TestIDConsumptionDiskMeasurement(t *testing.T) {
	if os.Getenv("RHO_ID_CONSUMPTION_DISK") != "1" {
		t.Skip("explicit bounded disk measurement; set RHO_ID_CONSUMPTION_DISK=1")
	}
	if runtime.Version() != "go1.26.9" {
		t.Fatal("measurement toolchain must be go1.26.9", runtime.Version())
	}
	for repeat := range 3 {
		for caseIndex, fixture := range consumptionFixtures() {
			// Rotate order deterministically so each candidate occupies every position.
			kinds := slices.Clone(consumptionKinds)
			for position := range len(kinds) {
				kind := kinds[(position+repeat+caseIndex)%len(kinds)]
				dir := filepath.Join(t.TempDir(), "db")
				measurement := measureConsumption(t, vfs.Default, dir, kind, fixture, true)
				measurement.Repeat = repeat + 1
				data, err := json.Marshal(measurement)
				if err != nil {
					t.Fatal(err)
				}
				t.Log(string(data))
			}
		}
	}
}
