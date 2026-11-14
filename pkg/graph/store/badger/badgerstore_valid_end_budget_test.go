package badger

import (
	"context"
	"errors"
	"testing"
	"time"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestValidEndOverlayRows_ChargeEachHistoryRowBeforeItsDecode (R2 late counts,
// ai-soc review 14 Nov 3.3): an entity with buffered writes is read by
// validEndOverlayRows, which decoded its whole history, committed rows and
// buffered rows alike, and only then counted the history versions (and the
// stored bytes not at all). Node 1 has 30 history versions, 15 flushed and 15
// buffered, and a buffered current row. Under a History limit of 3 the first
// call refuses at the 4th version, before the other 26 are decoded; under a
// DecodedBytes limit of 1 it refuses at the first row; with no limit it counts
// the 30 versions and the bytes of the rows it decodes.
func TestValidEndOverlayRows_ChargeEachHistoryRowBeforeItsDecode(t *testing.T) {
	t.Parallel()
	const versions = 30
	open := func(t *testing.T) *Store {
		bs, err := New(Config{Dir: t.TempDir(), FlushInterval: time.Hour})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = bs.Close() })
		must := func(err error) {
			t.Helper()
			if err != nil {
				t.Fatal(err)
			}
		}
		must(bs.PutNode(veNode(1, 0, 10, 0, 0)))
		for v := uint32(1); v <= versions; v++ {
			must(bs.PutNodeVersion(types.NodeID(1), v, veNode(1, v, types.Instant(10+v), 0, 0)))
			if v == versions/2 {
				must(bs.Flush())
			}
		}
		must(bs.ReplaceNode(veNode(1, versions+1, 100, 0, 0))) // a buffered current row: the entity is read from the overlay
		return bs
	}
	call := func(bs *Store, b storecontract.ReadBudget) (*storecontract.ReadMeter, error) {
		_, m := storecontract.WithReadBudget(context.Background(), b)
		_, err := bs.NodeValidEndCandidatesMetered([]uint16{1}, 1_000, 0, m.Tally("probe"))
		return m, err
	}

	m, err := call(open(t), storecontract.ReadBudget{})
	if w := m.Work(); err != nil || w.HistoryExamined != versions || w.DecodedBytes == 0 || w.Candidates != 1 {
		t.Errorf("unlimited = %+v, %v; want the %d versions counted, the stored bytes and 1 candidate", w, err, versions)
	}
	t.Logf("unlimited: %+v", m.Work())

	for _, c := range []struct {
		limit string
		b     storecontract.ReadBudget
		want  uint64 // history versions counted at the refusal
	}{
		{"history examined", storecontract.ReadBudget{HistoryExamined: 3}, 4},
		{"decoded bytes", storecontract.ReadBudget{DecodedBytes: 1}, 1},
	} {
		t.Run(c.limit, func(t *testing.T) {
			m, err := call(open(t), c.b)
			var rb *storecontract.ReadBudgetError
			if !errors.As(err, &rb) || rb.Limit != c.limit {
				t.Fatalf("%s: %v; want ErrReadBudget at %q", c.limit, err, c.limit)
			}
			if got := m.Work().HistoryExamined; got != c.want {
				t.Fatalf("%s: %d history versions counted at the refusal (%+v), want %d: the history was decoded before it was counted", c.limit, got, m.Work(), c.want)
			}
		})
	}
}
