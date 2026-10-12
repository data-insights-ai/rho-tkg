package core

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
)

// TestEffectiveValidFrom_BadgerReopenFirstCall is sigma-tkgd's restore path
// (backlog 51): a badger directory holding N retired events — half retired by
// tkg_valid_to at create, half deleted — and 5 live ones; the store is closed
// and reopened, and the FIRST bounded call (which builds the membership
// sidecar, tracks the type / label and re-reads its members' temporal fields)
// streams exactly the live entities and reads only them as timelines. It logs
// the first and second bounded call against the first unbounded call after
// another reopen. VALID_END_REOPEN_N sets N (default 3,000).
func TestEffectiveValidFrom_BadgerReopenFirstCall(t *testing.T) {
	n := 3_000
	if v, err := strconv.Atoi(os.Getenv("VALID_END_REOPEN_N")); err == nil && v > 0 {
		n = v
	}
	if testing.Short() && n > 3_000 {
		n = 3_000
	}
	const live = 5
	ctx := context.Background()
	dir := t.TempDir()
	open := func() *Core {
		t.Helper()
		bs, err := badger.New(badger.Config{Dir: dir})
		if err != nil {
			t.Fatal(err)
		}
		g, err := New(Config{Store: bs})
		if err != nil {
			t.Fatal(err)
		}
		return g
	}
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	g := open()
	anchor, err := g.Nodes.Add(ctx, []string{"Host"}, nil)
	must(err)
	base, _ := g.Temporal.NowTx()
	timed := map[string]any{"tkg_valid_from": base - 10, "tkg_valid_to": base - 5}
	for i := 0; i < n; i++ {
		var props map[string]any
		if i%2 == 0 {
			props = timed
		}
		nd, err := g.Nodes.Add(ctx, []string{"Event"}, props)
		must(err)
		r, err := g.Rels.AddByID(ctx, "SEEN", anchor.ID(), nd.ID(), props)
		must(err)
		if i%2 == 1 {
			must(g.Rels.Delete(ctx, r.ID()))
			must(g.Nodes.Delete(ctx, nd.ID()))
		}
	}
	for i := 0; i < live; i++ {
		nd, err := g.Nodes.Add(ctx, []string{"Event"}, nil)
		must(err)
		_, err = g.Rels.AddByID(ctx, "SEEN", anchor.ID(), nd.ID(), nil)
		must(err)
	}
	pin, _ := g.Temporal.NowTx()
	must(g.Close())

	// The first unbounded call after a reopen, for comparison.
	g = open()
	unbounded := map[bool]time.Duration{}
	for _, rel := range []bool{true, false} {
		start := time.Now()
		var err error
		if rel {
			err = g.Temporal.ForEachRelEffectiveByType("SEEN", pin, func(temporal.RelSegment) bool { return true })
		} else {
			err = g.Temporal.ForEachNodeEffectiveByLabels([]string{"Event"}, pin, func(temporal.NodeSegment) bool { return true })
		}
		must(err)
		unbounded[rel] = time.Since(start)
	}
	must(g.Close())

	g = open()
	defer g.Close()
	reads := 0
	g.chainLoadTestHook = func() { reads++ }
	count := func(rel bool) (int, time.Duration) {
		t.Helper()
		k := 0
		start := time.Now()
		var err error
		if rel {
			err = g.Temporal.ForEachRelEffectiveByTypeFrom("SEEN", pin, pin, func(temporal.RelSegment) bool { k++; return true })
		} else {
			err = g.Temporal.ForEachNodeEffectiveByLabelsFrom([]string{"Event"}, pin, pin, func(temporal.NodeSegment) bool { k++; return true })
		}
		must(err)
		return k, time.Since(start)
	}
	for _, rel := range []bool{true, false} {
		kind := "nodes Event"
		if rel {
			kind = "rels SEEN"
		}
		reads = 0
		k1, d1 := count(rel)
		r1 := reads
		reads = 0
		k2, d2 := count(rel)
		r2 := reads
		t.Logf("%s, %d retired + %d live, after reopen: first bounded call %v (%d segments, %d timelines read), second %v (%d read); first unbounded call %v",
			kind, n, live, d1.Round(time.Millisecond), k1, r1, d2.Round(time.Microsecond), r2, unbounded[rel].Round(time.Millisecond))
		if k1 != live || k2 != live {
			t.Fatalf("%s streamed %d / %d segments, want the %d live", kind, k1, k2, live)
		}
		if r1 != live || r2 != live {
			t.Fatalf("%s read %d then %d timelines, want %d both times", kind, r1, r2, live)
		}
	}
}
