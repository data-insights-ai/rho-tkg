package badger

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// benchPutNodeStats measures per-PutNode cost with the planner-stat maintenance
// sweep enabled vs disabled, on a wide (12-property, 3-label) node — the shape
// where the per-write sweep (presence + NDV + type-class, once per label×key)
// costs the most. Run: go test -bench BenchmarkPutNodeStats -benchmem
// ./pkg/graph/store/badger/
func benchPutNodeStats(b *testing.B, disable bool) {
	b.Helper()
	bs, err := New(Config{
		InMemory:            true,
		FlushInterval:       1<<63 - 1,
		DisablePlannerStats: disable,
	})
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	b.Cleanup(func() { _ = bs.Close() })

	props := map[string]any{
		"id": int64(0), "name": "Ada Lovelace", "email": "ada@example.com",
		"score": 3.5, "age": int64(36), "city": "London", "active": true,
		"rank": int64(1), "tier": "gold", "region": "eu", "lang": "en",
		"notes": "a reasonably long free-text field standing in for real data",
	}
	labels := []uint16{2, 3, 4}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n := types.NewNode(types.NodeID(snowflake.ID(int64(i)+1)), labels[0], labels[1:])
		for k, v := range props {
			if k == "id" {
				v = int64(i)
			}
			if err := n.SetProperty(k, v); err != nil {
				b.Fatalf("SetProperty %s: %v", k, err)
			}
		}
		if err := bs.PutNode(n); err != nil {
			b.Fatalf("PutNode: %v", err)
		}
	}
}

func BenchmarkPutNodeStatsEnabled(b *testing.B)  { benchPutNodeStats(b, false) }
func BenchmarkPutNodeStatsDisabled(b *testing.B) { benchPutNodeStats(b, true) }
