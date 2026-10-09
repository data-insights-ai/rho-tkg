package badger

import (
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestHistoryPresenceBuildScanCost guards the presence build's key scan
// (scanHistoryTops, which reads the highest version per ID) against the cost of
// the ID-only scan it replaced (ForEachNodeHistoryID): on IDs with 1, 3, 12 or 40
// rows it must stay within a small factor of it (deep IDs cost a reverse seek and
// a seek to the next ID, so their limit is loose). The first version walked a
// reverse seek per multi-row ID and ran 1.6-2x slower on the 3-row shape.
// Timing is best-of-N on one reopened store; skipped under -short.
func TestHistoryPresenceBuildScanCost(t *testing.T) {
	if testing.Short() {
		t.Skip("timing guard")
	}
	const ids = 10_000
	for _, shape := range []struct {
		name  string
		rows  int
		limit float64
	}{{"1 row", 1, 1.35}, {"3 rows", 3, 1.35}, {"12 rows", 12, 1.35}, {"40 rows", 40, 6}} {
		t.Run(shape.name, func(t *testing.T) {
			dir := t.TempDir()
			bs := openBulkPresenceStore(t, dir, false)
			for i := 1; i <= ids; i++ {
				for v := 0; v < shape.rows; v++ {
					n := types.NewNode(types.NodeID(snowflake.ID(i)), 1, nil)
					n.SetVersion(uint32(v))
					if err := bs.PutNodeVersion(n.ID(), uint32(v), n); err != nil {
						t.Fatalf("put: %v", err)
					}
				}
			}
			if err := bs.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			bs = openBulkPresenceStore(t, dir, false)
			t.Cleanup(func() { _ = bs.Close() })

			best := func(run func() error) time.Duration {
				var min time.Duration
				for i := 0; i < 9; i++ {
					start := time.Now()
					if err := run(); err != nil {
						t.Fatalf("scan: %v", err)
					}
					if d := time.Since(start); i == 0 || d < min {
						min = d
					}
				}
				return min
			}
			var idOnly, tops int
			var ratio float64
			// A shared host scatters timings; a regression fails every attempt.
			for attempt := 0; attempt < 3; attempt++ {
				old := best(func() error {
					idOnly = 0
					return bs.ForEachNodeHistoryID(func(types.NodeID) bool { idOnly++; return true })
				})
				cur := best(func() error {
					res, err := bs.scanHistoryTops(storepkg.KeyHistNode)
					tops = len(res)
					return err
				})
				if idOnly != ids || tops != ids {
					t.Fatalf("ID-only scan saw %d IDs, tops scan %d, want %d", idOnly, tops, ids)
				}
				ratio = float64(cur) / float64(old)
				t.Logf("%s: ID-only scan %v, tops scan %v (x%.2f)", shape.name, old, cur, ratio)
				if ratio <= shape.limit {
					return
				}
			}
			t.Fatalf("%s: the tops scan takes x%.2f of the ID-only scan in three attempts, limit x%.2f", shape.name, ratio, shape.limit)
		})
	}
}
