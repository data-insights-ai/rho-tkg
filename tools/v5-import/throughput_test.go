package importmap

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/graph"
)

// wideStream is n node creates of 16 int properties each: a few dozen of them
// fill the store's stage-record limit, so a 256-op Update is refused.
func wideStream(n int) *Result {
	r := &Result{}
	for id := int64(1); id <= int64(n); id++ {
		props := map[string]PropValue{}
		for k := range 16 {
			props[fmt.Sprintf("p%02d", k)] = PropValue{graph.Int(id*100 + int64(k))}
		}
		r.Ops = append(r.Ops, Op{Op: "create_node", Entity: "node", ID: id, Life: 1, Version: 1, Labels: []string{"A"}, Props: props, Scope: Scope{From: At(id), To: PosInf}, Recorded: id})
	}
	return r
}

func importWide(tb testing.TB, n int) (*Report, time.Duration) {
	tb.Helper()
	res := wideStream(n)
	cfg, err := res.Config(baseConfig(tb, ""), Options{})
	if err != nil {
		tb.Fatal(err)
	}
	g, err := graph.Open(context.Background(), cfg)
	if err != nil {
		tb.Fatal(err)
	}
	defer g.Close()
	t0 := time.Now()
	rep, err := res.Import(context.Background(), g, cfg, Options{})
	if err != nil {
		tb.Fatal(err)
	}
	return rep, time.Since(t0)
}

// A batch the store refuses as too large is staged work thrown away. Once a
// size is refused the import must not keep trying it: at most one refusal per
// two commits, startup included (it was two to three refusals per commit:
// every batch began at 256 again and halved down).
func TestImportRemembersFittingBatchSize(t *testing.T) {
	rep, el := importWide(t, 640)
	t.Logf("640 wide creates: %d commits, %d refused, %s", rep.Commits, rep.Refused, el.Round(time.Millisecond))
	if rep.Commits == 0 || rep.Refused == 0 {
		t.Fatalf("the stream must hit the stage limit: %d commits, %d refused", rep.Commits, rep.Refused)
	}
	if rep.Refused*2 > rep.Commits {
		t.Fatalf("%d refused Updates for %d commits: the import retries sizes the store already refused", rep.Refused, rep.Commits)
	}
}

// BenchmarkImportWide reports import throughput on the wide stream.
func BenchmarkImportWide(b *testing.B) {
	for b.Loop() {
		rep, el := importWide(b, 2000)
		b.ReportMetric(2000/el.Seconds(), "ops/s")
		b.ReportMetric(float64(rep.Refused), "refused")
	}
}
