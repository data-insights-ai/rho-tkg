package badger

import (
	"context"
	"fmt"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestReadSnapshotProbeOnly_DecodesNothing (R2-5 HEADREAD round 2, the probe
// fix): on a store opened with HistoryPresenceProbeOnly, a metered snapshot's
// HasNodeHistory / HasRelHistory answers from keys alone: no history row or
// skeleton decoded (HistoryMetaDecodesForTest and HistoryRowDecodesForTest
// unchanged), nothing charged (HistoryExamined 0, DecodedBytes 0), and the
// store's own answer: rows pending, flushed, every flushed row masked by a
// buffered delete, and an ID with no history. Node and rel; 10 and 80
// versions. It decoded every skeleton (N) to answer.
func TestReadSnapshotProbeOnly_DecodesNothing(t *testing.T) {
	t.Parallel()
	for _, k := range presenceKinds() {
		for _, versions := range []int{10, 80} {
			t.Run(fmt.Sprintf("%s/%d", k.name, versions), func(t *testing.T) {
				t.Parallel()
				bs := newFlushParkStore(t, func(c *Config) { c.HistoryPresenceProbeOnly = true })
				const id, none = 90, 91
				for v := range versions {
					if err := k.put(bs, id, uint32(v)); err != nil { // #nosec G115 -- a test size
						t.Fatal(err)
					}
				}
				snapHas := func(s *readSnapshot, x int64) (bool, error) {
					if k.name == "node" {
						return s.HasNodeHistory(types.NodeID(snowflake.ID(x)))
					}
					return s.HasRelHistory(types.RelID(snowflake.ID(x)))
				}
				check := func(step string, x int64, want bool) {
					t.Helper()
					if got, err := k.has(bs, x); err != nil || got != want {
						t.Fatalf("%s: store has(%d) = %v, %v; want %v", step, x, got, err, want)
					}
					s := bs.NewReadSnapshot().(*readSnapshot)
					defer s.Close()
					_, meter := storecontract.WithReadBudget(context.Background(), storecontract.ReadBudget{})
					s.MeterReads(meter.Tally("probe"))
					metas, rows := bs.HistoryMetaDecodesForTest(), bs.HistoryRowDecodesForTest()
					got, err := snapHas(s, x)
					dm, dr, w := bs.HistoryMetaDecodesForTest()-metas, bs.HistoryRowDecodesForTest()-rows, meter.Work()
					t.Logf("%s: has(%d) = %v; HistoryExamined %d, DecodedBytes %d, skeletons decoded %d, rows %d", step, x, got, w.HistoryExamined, w.DecodedBytes, dm, dr)
					if err != nil || got != want {
						t.Fatalf("%s: snapshot has(%d) = %v, %v; want the store's %v", step, x, got, err, want)
					}
					if w.HistoryExamined != 0 || w.DecodedBytes != 0 || dm != 0 || dr != 0 {
						t.Errorf("%s: has(%d) read HistoryExamined %d, DecodedBytes %d, %d skeletons, %d rows; want keys only: 0 each",
							step, x, w.HistoryExamined, w.DecodedBytes, dm, dr)
					}
				}
				check("pending", id, true)
				check("pending", none, false)
				if err := bs.Flush(); err != nil {
					t.Fatal(err)
				}
				check("flushed", id, true)
				check("flushed", none, false)
				if err := k.trimFrom(bs, id, 0); err != nil {
					t.Fatal(err)
				}
				check("every row deleted, the deletes buffered", id, false)
				if err := bs.Flush(); err != nil {
					t.Fatal(err)
				}
				check("every row deleted, flushed", id, false)
			})
		}
	}
}
