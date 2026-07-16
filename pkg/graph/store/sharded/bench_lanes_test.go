package sharded_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/ingest"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
)

// S4 throughput acceptance harness (ADR-0007 §S4).
//
// These benchmarks measure the concurrent-ingest insert rate and report it as a
// custom "inserts/s" metric so the ratios in the plan's MEASURED BAR are read
// directly. Run them on the OWNER's hardware (Apple M4 Max) — a dev-box / CI
// number is explicitly NOT claimable as meeting the bar:
//
//	go test -run '^$' -bench 'BenchmarkIngest' -benchmem -count=5 \
//	    ./pkg/graph/store/sharded/
//
// Acceptance (same-run medians):
//   - BenchmarkIngestShardedLanes/lanes-8 ≥ 2.5× BenchmarkIngestSingleStoreP8
//     (plan target ~1.1M/s → ≥2.75M/s on the M4 Max).
//   - The change-log-on variant within 1.2× of change-log-off.
//
// If the disk/flush wall lands earlier, capture an honest pprof
// (-cpuprofile / -memprofile) rather than reporting a smaller number as the bar.

const (
	benchSessions        = 8   // concurrent producer sessions
	benchSubmitChunkSize = 256 // node creates coalesced per Submit
)

// benchInsertRate drives `sessions` concurrent ingest sessions that together
// create b.N nodes, and reports the aggregate inserts/s. Each session submits in
// chunks so the group-commit path (not per-node round-trips) is measured.
func benchInsertRate(b *testing.B, g *graph.Graph, sessions int) {
	b.Helper()
	perSession := b.N / sessions
	remainder := b.N % sessions

	b.ResetTimer()
	var wg sync.WaitGroup
	for w := 0; w < sessions; w++ {
		count := perSession
		if w < remainder {
			count++
		}
		wg.Add(1)
		go func(count int) {
			defer wg.Done()
			sess, err := g.Ingest().NewSession(ingest.IngestOptions{Concurrent: true})
			if err != nil {
				b.Error(err)
				return
			}
			defer func() { _ = sess.Close() }()
			inChunk := 0
			for i := 0; i < count; i++ {
				if _, err := sess.AddNode([]string{"Event"}, map[string]any{"i": int64(i)}); err != nil {
					b.Error(err)
					return
				}
				inChunk++
				if inChunk >= benchSubmitChunkSize {
					if _, err := sess.Submit(); err != nil {
						b.Error(err)
						return
					}
					inChunk = 0
				}
			}
			if inChunk > 0 {
				if _, err := sess.Submit(); err != nil {
					b.Error(err)
				}
			}
		}(count)
	}
	wg.Wait()
	b.StopTimer()

	if secs := b.Elapsed().Seconds(); secs > 0 {
		b.ReportMetric(float64(b.N)/secs, "inserts/s")
	}
}

// benchInsertRateBulk is benchInsertRate using the write-only bulk door
// (Session.AddNodes), which skips the per-node isolation DeepCopy — the
// mass-ingestion path where created nodes are not needed as endpoints.
func benchInsertRateBulk(b *testing.B, g *graph.Graph, sessions int) {
	b.Helper()
	perSession := b.N / sessions
	remainder := b.N % sessions

	b.ResetTimer()
	var wg sync.WaitGroup
	for w := 0; w < sessions; w++ {
		count := perSession
		if w < remainder {
			count++
		}
		wg.Add(1)
		go func(count int) {
			defer wg.Done()
			sess, err := g.Ingest().NewSession(ingest.IngestOptions{Concurrent: true})
			if err != nil {
				b.Error(err)
				return
			}
			defer func() { _ = sess.Close() }()
			for remaining := count; remaining > 0; {
				chunk := benchSubmitChunkSize
				if chunk > remaining {
					chunk = remaining
				}
				if err := sess.AddNodes([]string{"Event"}, map[string]any{"i": int64(chunk)}, chunk); err != nil {
					b.Error(err)
					return
				}
				if _, err := sess.Submit(); err != nil {
					b.Error(err)
					return
				}
				remaining -= chunk
			}
		}(count)
	}
	wg.Wait()
	b.StopTimer()

	if secs := b.Elapsed().Seconds(); secs > 0 {
		b.ReportMetric(float64(b.N)/secs, "inserts/s")
	}
}

// BenchmarkIngestShardedLanesBulk is the write-only mass-ingestion variant of
// BenchmarkIngestShardedLanes: same topology, but the DeepCopy-free bulk door.
func BenchmarkIngestShardedLanesBulk(b *testing.B) {
	const lanes uint8 = 8
	st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2 + lanes})
	if err != nil {
		b.Fatal(err)
	}
	g, err := graph.New(graph.Config{Store: st, SnowflakeNodeID: 0, IngestLanes: lanes})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = g.Close() }()
	benchInsertRateBulk(b, g, int(lanes))
}

// BenchmarkIngestShardedLanesBulkScaling measures how the write-only bulk path
// scales with shard/lane count (== core count on the box). It answers "how far
// toward 10M/s does adding shards get us" — throughput should climb roughly with
// lanes until the box runs out of cores or the shared store LSM cost dominates.
func BenchmarkIngestShardedLanesBulkScaling(b *testing.B) {
	for _, lanes := range []uint8{2, 4, 8, 16} {
		b.Run(fmt.Sprintf("lanes-%d", lanes), func(b *testing.B) {
			st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2 + uint8(lanes)})
			if err != nil {
				b.Fatal(err)
			}
			g, err := graph.New(graph.Config{Store: st, SnowflakeNodeID: 0, IngestLanes: lanes})
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = g.Close() }()
			benchInsertRateBulk(b, g, int(lanes))
		})
	}
}

// BenchmarkIngestShardedLanes is the S4 target: badger-backed sharded store,
// 8 lanes over 8 lane slots (plus the interactive pair), 8 concurrent sessions
// each pinned to its own slot -> its own shard -> one batched door per group.
func BenchmarkIngestShardedLanes(b *testing.B) {
	for _, lanes := range []uint8{4, 8} {
		b.Run(fmt.Sprintf("lanes-%d", lanes), func(b *testing.B) {
			st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2 + lanes})
			if err != nil {
				b.Fatal(err)
			}
			g, err := graph.New(graph.Config{Store: st, SnowflakeNodeID: 0, IngestLanes: lanes})
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = g.Close() }()
			benchInsertRate(b, g, int(lanes))
		})
	}
}

// BenchmarkIngestShardedLanesChangeLog measures the change-log-on penalty on the
// 8-lane sharded store (must stay within 1.2× of change-log-off).
func BenchmarkIngestShardedLanesChangeLog(b *testing.B) {
	const lanes uint8 = 8
	st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2 + lanes, ChangeLog: true})
	if err != nil {
		b.Fatal(err)
	}
	g, err := graph.New(graph.Config{Store: st, SnowflakeNodeID: 0, IngestLanes: lanes})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = g.Close() }()
	benchInsertRate(b, g, int(lanes))
}

// BenchmarkIngestSingleStoreP8 is the baseline the S4 bar is a ratio against:
// a single badger store, 8 concurrent sessions, NO lanes (IngestLanes=0 -> the
// interactive even/odd pair). The sharded-lanes benchmark should be >= 2.5x this.
func BenchmarkIngestSingleStoreP8(b *testing.B) {
	st, err := badger.New(badger.Config{InMemory: true})
	if err != nil {
		b.Fatal(err)
	}
	g, err := graph.New(graph.Config{Store: st, SnowflakeNodeID: 0})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = g.Close() }()
	benchInsertRate(b, g, benchSessions)
}

// BenchmarkIngestMemoryP8 is the PURE-CODE ceiling: memory.Store (Go maps, live
// objects — NO msgpack wire encode, NO LSM skiplist, NO memtable arena), 8
// concurrent sessions. Comparing this to BenchmarkIngestSingleStoreP8 (single
// badger) isolates the badger cost; comparing to the sharded-lanes benchmark
// isolates the core-layer (lock/alloc) cost from the store cost.
func BenchmarkIngestMemoryP8(b *testing.B) {
	g, err := graph.New(graph.Config{Store: memory.New(), SnowflakeNodeID: 0})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = g.Close() }()
	benchInsertRate(b, g, benchSessions)
}

// BenchmarkIngestMemoryScaling reveals the contention penalty: if throughput
// does not scale ~linearly with session count, the ceiling is shared-state lock
// contention (c.mu), not per-insert work.
func BenchmarkIngestMemoryScaling(b *testing.B) {
	for _, p := range []int{1, 2, 4, 8, 16} {
		b.Run(fmt.Sprintf("p-%d", p), func(b *testing.B) {
			g, err := graph.New(graph.Config{Store: memory.New(), SnowflakeNodeID: 0})
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = g.Close() }()
			benchInsertRate(b, g, p)
		})
	}
}
