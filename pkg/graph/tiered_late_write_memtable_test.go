package graph_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// R3-3 LATEWRITE, the design's L1 (tasks/evidence/R3/design-latewrite-floor.txt
// section 3): with WarmMemTableSize 0 a writable open of a shard that is cold
// at the open takes an 8 MB memtable instead of MemTableSize, so a late write
// into a closed day allocates an arena of 10.4 MB instead of 83 MB. Badger
// creates the open's WAL at 2 x its MemTableSize (memtable.go:117), so the
// .mem file's apparent size is the memtable that open used.

const (
	lwHot  = 64 << 20
	lwCold = 8 << 20
)

// lwWAL is the apparent size of the largest WAL in the day-22 shard (0: none,
// the shard is closed or open read-only).
func lwWAL(t *testing.T, dir string) int64 {
	t.Helper()
	mems, err := filepath.Glob(filepath.Join(dir, "events", "2026-07-22", "*.mem"))
	if err != nil {
		t.Fatal(err)
	}
	var largest int64
	for _, m := range mems {
		info, err := os.Stat(m)
		if err != nil {
			t.Fatal(err)
		}
		largest = max(largest, info.Size())
	}
	return largest
}

func lwShard(t *testing.T, ts *tiered.Store) *tiered.EventShard {
	t.Helper()
	es := ts.EventShardsForTest()["2026-07-22"]
	if es == nil || es.Tier() != tiered.TierCold {
		t.Fatalf("day 22's shard %+v, want cold", es)
	}
	return es
}

// lwCloseIdle closes every idle cold handle.
func lwCloseIdle(t *testing.T, ts *tiered.Store) {
	t.Helper()
	ts.SetIdleTimeoutForTest(time.Millisecond)
	time.Sleep(5 * time.Millisecond)
	ts.CloseIdleShardsForTest()
	if s, _ := lwShard(t, ts).StoreStateForTest(); s != nil {
		t.Fatal("day 22 still open after the idle close")
	}
}

// lwAnswers is what the graph answers for id: the current row, every version,
// each version as of its own transaction time, and the hash chain. IDs and
// transaction stamps differ between stores and are left out.
func lwAnswers(t *testing.T, g *graphpkg.Graph, id types.NodeID) string {
	t.Helper()
	norm := func(n *types.Node) string {
		from, to, _ := n.ValidRange()
		return fmt.Sprintf("v%d %v valid[%d,%d)", n.Version(), n.PropertiesMap(), from, to)
	}
	var b strings.Builder
	cur, err := g.Nodes().Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	fmt.Fprintf(&b, "current %s\n", norm(cur))
	hist, err := g.Nodes().History(id)
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	for _, v := range hist {
		txFrom, _, _ := v.TxStamps()
		asOf, err := g.Temporal().NodeAsOf(id, txFrom)
		if err != nil {
			t.Fatalf("NodeAsOf(%d): %v", txFrom, err)
		}
		fmt.Fprintf(&b, "history %s | as of its tx %s\n", norm(v), norm(asOf))
	}
	ok, err := g.Hash().VerifyNodeChain(id)
	fmt.Fprintf(&b, "chain %v %v\n", ok, err)
	return b.String()
}

// lwRun writes one node into the day-22 shard (made on demand, cold: the clock
// is day 25 and ColdAfter 48 h), updates it, then late-writes it twice: into
// the closed shard and beside its read-only handle. It returns the WAL each
// writable open of day 22 left and the answers before and after a reopen.
func lwRun(t *testing.T, warm int64) (wals []int64, before, after string) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	open := func() (*tiered.Store, *graphpkg.Graph) {
		ts, err := tiered.New(tiered.Config{DataDir: dir, ShardWindow: 24 * time.Hour, ShardWindowLocation: time.UTC,
			ColdAfter: 48 * time.Hour, FlushInterval: 1<<63 - 1, MintAtValidFrom: true,
			Clock:        func() time.Time { return time.Date(2026, 7, 25, 9, 0, 0, 0, time.UTC) },
			MemTableSize: lwHot, WarmMemTableSize: warm, MaxOpenColdShards: 1})
		if err != nil {
			t.Fatal(err)
		}
		g, err := graphpkg.New(graphpkg.Config{Store: ts})
		if err != nil {
			t.Fatal(err)
		}
		return ts, g
	}
	ts, g := open()
	n, err := g.Nodes().Add(ctx, []string{"Signal"}, map[string]any{"k": "v1",
		"tkg_valid_from": types.Instant(time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC).UnixMilli())})
	if err != nil {
		t.Fatal(err)
	}
	lwShard(t, ts)
	wals = append(wals, lwWAL(t, dir)) // the window shard made on demand
	if _, err := g.Nodes().Update(ctx, n.ID(), map[string]any{"k": "v2"}); err != nil {
		t.Fatal(err)
	}

	lwCloseIdle(t, ts)
	if _, err := g.Nodes().Update(ctx, n.ID(), map[string]any{"k": "v3"}); err != nil {
		t.Fatalf("late write into the closed day: %v", err)
	}
	if s, ro := lwShard(t, ts).StoreStateForTest(); s == nil || ro {
		t.Fatalf("after the late write day 22 open %v read-only %v, want open writable", s != nil, ro)
	}
	wals = append(wals, lwWAL(t, dir))

	lwCloseIdle(t, ts)
	if _, err := g.Nodes().Get(ctx, n.ID()); err != nil {
		t.Fatal(err)
	}
	if s, ro := lwShard(t, ts).StoreStateForTest(); s == nil || !ro {
		t.Fatalf("after the cold read day 22 open %v read-only %v, want a read-only handle kept", s != nil, ro)
	}
	if _, err := g.Nodes().Update(ctx, n.ID(), map[string]any{"k": "v4", "late": true}); err != nil {
		t.Fatalf("late write beside the read-only handle: %v", err)
	}
	if s, ro := lwShard(t, ts).StoreStateForTest(); s == nil || ro {
		t.Fatalf("after the second late write day 22 open %v read-only %v, want open writable", s != nil, ro)
	}
	wals = append(wals, lwWAL(t, dir))

	before = lwAnswers(t, g, n.ID())
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	_, g = open()
	defer func() { _ = g.Close() }()
	after = lwAnswers(t, g, n.ID())
	return wals, before, after
}

// TestLateWriteColdShardOpensAtSmallMemtable: MemTableSize 64 MB, no
// WarmMemTableSize. Every writable open of day 22, cold at each open (the
// window shard made on demand, the late write into the closed day, the late
// write beside a read-only handle), takes an 8 MB memtable: WAL 16 MB, not
// 128. The control (WarmMemTableSize 64 MB, the hot size everywhere) opens at
// 64 MB and answers the same: current row, three earlier versions, each as of its
// transaction time, the hash chain; before and after a reopen.
func TestLateWriteColdShardOpensAtSmallMemtable(t *testing.T) {
	if testing.Short() {
		t.Skip("opens 64 MB memtables: not in -short")
	}
	wals, before, after := lwRun(t, 0)
	cwals, cbefore, cafter := lwRun(t, lwHot)
	t.Logf("WAL of day 22's writable opens: %v (control %v)", wals, cwals)
	t.Logf("answers:\n%s", before)
	for i, w := range wals {
		if w != 2*lwCold {
			t.Errorf("writable open %d of the cold day: WAL %d, want 2 x %d (an 8 MB memtable)", i, w, lwCold)
		}
	}
	for i, w := range cwals {
		if w != 2*lwHot {
			t.Fatalf("control: writable open %d: WAL %d, want 2 x %d", i, w, lwHot)
		}
	}
	if strings.Count(cbefore, "history ") != 3 || !strings.Contains(cbefore, "chain true <nil>") {
		t.Fatalf("control answers are not three earlier versions and a valid chain:\n%s", cbefore)
	}
	if before != cbefore || after != cafter || after != before {
		t.Fatalf("answers differ from the control at the hot size:\nbefore:\n%s\ncontrol:\n%s\nafter reopen:\n%s\ncontrol after reopen:\n%s",
			before, cbefore, after, cafter)
	}
}
