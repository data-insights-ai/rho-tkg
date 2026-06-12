package badger

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// OPT15 win proof (Pattern 18). A hub node with many versioned edges, most of
// which are EXPIRED at the query time. The decode path msgpack-decodes every
// incident edge just to reject it; the inline-stamp path rejects from the cached
// {vf,vt} with no decode. Run:
//
//	go test ./pkg/graph/store/badger/ -run x -bench 'OutgoingRelsAt' -benchmem
//
// Both sub-benchmarks count the same surviving edges over the identical store —
// the gap is pure decode-elimination.
func benchHubStore(b *testing.B) (*Store, types.NodeID, types.Instant) {
	b.Helper()
	bs, err := New(Config{InMemory: true})
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	b.Cleanup(func() { bs.Close() })

	const (
		hub        = int64(1)
		numTargets = 256
		numEdges   = 4000
		queryAt    = types.Instant(1_000_000)
	)
	hubNode := types.NewNode(types.NodeID(snowflake.ID(hub)), 1, nil)
	if err := bs.PutNode(hubNode); err != nil {
		b.Fatalf("PutNode hub: %v", err)
	}
	for t := int64(0); t < numTargets; t++ {
		n := types.NewNode(types.NodeID(snowflake.ID(100+t)), 1, nil)
		if err := bs.PutNode(n); err != nil {
			b.Fatalf("PutNode target: %v", err)
		}
	}
	// ~90% expired before queryAt, ~10% open-ended (valid at queryAt).
	for i := int64(0); i < numEdges; i++ {
		target := types.NodeID(snowflake.ID(100 + i%numTargets))
		r := types.NewRelationship(types.RelID(snowflake.ID(10_000+i)), 1, types.NodeID(snowflake.ID(hub)), target)
		vf := types.Instant(1000 + i)
		vt := types.Instant(0) // open
		if i%10 != 0 {
			vt = vf + 500 // expired well before queryAt
		}
		r.SetTemporal(&types.TemporalMetadata{ValidFrom: vf, ValidTo: vt})
		if err := bs.PutRelationship(r); err != nil {
			b.Fatalf("PutRelationship: %v", err)
		}
	}
	return bs, types.NodeID(snowflake.ID(hub)), queryAt
}

// BenchmarkOutgoingRelsAt_Decode is the baseline: decode every incident edge,
// then apply the canonical temporal filter.
func BenchmarkOutgoingRelsAt_Decode(b *testing.B) {
	bs, hub, queryAt := benchHubStore(b)
	opts := QueryOpts{ValidAt: queryAt}
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		count := 0
		err := bs.ForEachOutgoingRel(hub, 0, func(r *types.Relationship) bool {
			if storepkg.MatchesTemporalFilter(r.ID().SnowflakeID(), r.Temporal(), opts) {
				count++
			}
			return true
		})
		if err != nil {
			b.Fatalf("scan: %v", err)
		}
		if count == 0 {
			b.Fatal("expected some valid edges")
		}
	}
}

// BenchmarkOutgoingRelsAt_InlineStamp is the OPT15 path: reject expired edges
// from the inline stamp with no decode.
func BenchmarkOutgoingRelsAt_InlineStamp(b *testing.B) {
	bs, hub, queryAt := benchHubStore(b)
	opts := QueryOpts{ValidAt: queryAt}
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		count := 0
		err := bs.ForEachAdjacentEndpointAt(hub, 0, false, opts, func(_ types.RelID, _ types.NodeID) bool {
			count++
			return true
		})
		if err != nil {
			b.Fatalf("scan: %v", err)
		}
		if count == 0 {
			b.Fatal("expected some valid edges")
		}
	}
}
