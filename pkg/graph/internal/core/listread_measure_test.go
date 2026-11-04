//go:build measure

package core

import (
	"context"
	"os"
	"runtime/pprof"
	"strconv"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestListReadRestoreCost (LISTREAD) times sigma's restore on a tiered store
// reopened cold: ByLabelAndProperties pages, then HasHistory per node.
// LISTREAD_N nodes (default 200000), a fifth with history; LISTREAD_PROF
// writes a CPU profile per phase.
func TestListReadRestoreCost(t *testing.T) {
	n := 200000
	if v, err := strconv.Atoi(os.Getenv("LISTREAD_N")); err == nil {
		n = v
	}
	dir := t.TempDir()
	open := func() *Core {
		ts, err := tiered.New(tiered.Config{DataDir: dir, ShardWindow: 24 * time.Hour, RefLabels: []string{"Page"}})
		if err != nil {
			t.Fatal(err)
		}
		g, err := New(Config{Store: ts})
		if err != nil {
			t.Fatal(err)
		}
		return g
	}
	g := open()
	ctx := context.Background()
	if err := g.Index.CreateComposite("Page", []string{"kind", "tenant"}); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	var ids []types.NodeID
	for done := 0; done < n; done += 10000 {
		props := make([]map[string]any, 0, 10000)
		for i := done; i < done+10000 && i < n; i++ {
			props = append(props, map[string]any{"kind": "blob", "tenant": "t1", "seq": i, "hash": "h" + strconv.Itoa(i)})
		}
		tx, err := g.BeginTx()
		if err != nil {
			t.Fatal(err)
		}
		nodes, err := tx.AddNodes([]string{"Page"}, props)
		if err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		for _, x := range nodes {
			ids = append(ids, x.ID())
		}
	}
	for i := 0; i < len(ids); i += 5 {
		if _, err := g.Nodes.Update(ctx, ids[i], map[string]any{"seq": -i - 1}); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	t.Logf("built %d nodes, %d with history, %s", n, (n+4)/5, time.Since(start).Round(time.Millisecond))

	g = open()
	defer g.Close()
	prof := func(name string) func() {
		if os.Getenv("LISTREAD_PROF") == "" {
			return func() {}
		}
		f, err := os.Create(os.Getenv("LISTREAD_PROF") + "." + name + ".pprof")
		if err != nil {
			t.Fatal(err)
		}
		_ = pprof.StartCPUProfile(f)
		return func() { pprof.StopCPUProfile(); _ = f.Close() }
	}
	stop := prof("pages")
	start = time.Now()
	var listed []types.NodeID
	var after types.EntityID
	for {
		page, err := g.Nodes.ByLabelAndProperties("Page", map[string]any{"kind": "blob", "tenant": "t1"}, storepkg.QueryOpts{Limit: 1000, After: after})
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			break
		}
		for _, x := range page {
			listed = append(listed, x.ID())
		}
		after = types.EntityID(page[len(page)-1].ID())
	}
	pages := time.Since(start)
	stop()
	stop = prof("has")
	start = time.Now()
	with := 0
	for _, id := range listed {
		h, err := g.Nodes.HasHistory(id)
		if err != nil {
			t.Fatal(err)
		}
		if h {
			with++
		}
	}
	has := time.Since(start)
	stop()
	t.Logf("cold reopen: pages %d nodes in %s (%.1f µs/node); HasHistory %d true in %s (%.1f µs/node)",
		len(listed), pages.Round(time.Millisecond), float64(pages.Microseconds())/float64(len(listed)),
		with, has.Round(time.Millisecond), float64(has.Microseconds())/float64(len(listed)))
	if len(listed) != n || with != (n+4)/5 {
		t.Fatalf("listed %d want %d, history %d want %d", len(listed), n, with, (n+4)/5)
	}
}
