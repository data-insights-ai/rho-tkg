package graph_test

import (
	"context"
	"crypto/sha256"
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
// section 3) and the grow (design-latewrite-fix.txt section 1): with
// WarmMemTableSize 0 a writable open of a shard that is cold at the open
// takes an 8 MiB memtable instead of MemTableSize, so a late write into a
// closed day allocates an arena of 10.4 MiB instead of 83.2 MiB; a handle
// that has committed 32 MiB on it is closed by the handle sweep and the next
// open takes MemTableSize. Badger creates the open's WAL at 2 x its
// MemTableSize (memtable.go:117), so the .mem file's apparent size is the
// memtable that open used.

const (
	lwHot  = 64 << 20
	lwCold = 8 << 20
	lwPad  = 60000
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

// lwCloseIdle closes every idle cold handle, then sets the idle timeout back
// so the idle loop leaves the next handle alone.
func lwCloseIdle(t *testing.T, ts *tiered.Store) {
	t.Helper()
	ts.SetIdleTimeoutForTest(time.Millisecond)
	time.Sleep(5 * time.Millisecond)
	ts.CloseIdleShardsForTest()
	ts.SetIdleTimeoutForTest(time.Hour)
	if s, _ := lwShard(t, ts).StoreStateForTest(); s != nil {
		t.Fatal("day 22 still open after the idle close")
	}
}

// lwProps is a row's properties with each long string replaced by its
// digest.
func lwProps(m map[string]any) string {
	c := make(map[string]any, len(m))
	for k, v := range m {
		if s, ok := v.(string); ok && len(s) > 64 {
			v = fmt.Sprintf("%d bytes sha256 %x", len(s), sha256.Sum256([]byte(s)))
		}
		c[k] = v
	}
	return fmt.Sprint(c)
}

// lwRow is a row of four pads of 60,000 bytes (about 234 KiB; a property
// value is at most 64 KiB), each starting with tag and the pad's number.
func lwRow(tag string, props map[string]any) map[string]any {
	for p := 0; p < 4; p++ {
		props[fmt.Sprintf("pad%d", p)] = fmt.Sprintf("%s/%d/", tag, p) + strings.Repeat("p", lwPad)
	}
	return props
}

// lwAnswers is what the graph answers for id: the current row, every version,
// each version as of its own transaction time, and the hash chain. IDs and
// transaction stamps differ between stores and are left out.
func lwAnswers(t *testing.T, g *graphpkg.Graph, id types.NodeID) string {
	t.Helper()
	norm := func(n *types.Node) string {
		from, to, _ := n.ValidRange()
		return fmt.Sprintf("v%d %s valid[%d,%d)", n.Version(), lwProps(n.PropertiesMap()), from, to)
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

// lwRelAnswers is lwAnswers for a relationship.
func lwRelAnswers(t *testing.T, g *graphpkg.Graph, id types.RelID) string {
	t.Helper()
	norm := func(r *types.Relationship) string {
		from, to, _ := r.ValidRange()
		return fmt.Sprintf("v%d %s valid[%d,%d)", r.Version(), lwProps(r.PropertiesMap()), from, to)
	}
	var b strings.Builder
	cur, err := g.Rels().Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Rels().Get: %v", err)
	}
	fmt.Fprintf(&b, "rel current %s\n", norm(cur))
	hist, err := g.Rels().History(id)
	if err != nil {
		t.Fatalf("Rels().History: %v", err)
	}
	for _, v := range hist {
		txFrom, _, _ := v.TxStamps()
		asOf, err := g.Temporal().RelAsOf(id, txFrom)
		if err != nil {
			t.Fatalf("RelAsOf(%d): %v", txFrom, err)
		}
		fmt.Fprintf(&b, "rel history %s | as of its tx %s\n", norm(v), norm(asOf))
	}
	ok, err := g.Hash().VerifyRelChain(id)
	fmt.Fprintf(&b, "rel chain %v %v\n", ok, err)
	return b.String()
}

// lwLines counts the lines of s that begin with prefix.
func lwLines(s, prefix string) int {
	n := 0
	for _, line := range strings.Split(s, "\n") {
		if strings.HasPrefix(line, prefix) {
			n++
		}
	}
	return n
}

// lwDay22 is an instant on day 22 (cold: the clock is day 25, ColdAfter 48 h).
func lwDay22(hour, minute int) types.Instant {
	return types.Instant(time.Date(2026, 7, 22, hour, minute, 0, 0, time.UTC).UnixMilli())
}

// lwOpen opens the tiered store and graph at dir: MemTableSize 64 MiB, the
// clock at day 25.
func lwOpen(t *testing.T, dir string, warm int64) (*tiered.Store, *graphpkg.Graph) {
	t.Helper()
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

// lwRun writes two nodes and a relationship into the day-22 shard (made on
// demand, cold), updates them, then late-writes three times: into the closed
// shard, beside its read-only handle, and a batch of 1.8 MiB into the closed
// shard (above the 1.2 MiB transaction limit of an 8 MiB memtable: staged;
// one transaction at 64 MiB). It returns the WAL each writable open of day 22
// left and the answers (both nodes, the relationship, the batch's nodes)
// before and after a reopen.
func lwRun(t *testing.T, warm int64) (wals []int64, before, after string) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	ts, g := lwOpen(t, dir, warm)
	n, err := g.Nodes().Add(ctx, []string{"Signal"}, map[string]any{"k": "v1", "tkg_valid_from": lwDay22(12, 0)})
	if err != nil {
		t.Fatal(err)
	}
	lwShard(t, ts)
	wals = append(wals, lwWAL(t, dir)) // the window shard made on demand
	n2, err := g.Nodes().Add(ctx, []string{"Signal"}, map[string]any{"k": "w1", "tkg_valid_from": lwDay22(13, 0)})
	if err != nil {
		t.Fatal(err)
	}
	rel, err := g.Rels().Add(ctx, "LINKS", n, n2, map[string]any{"r": "r1", "tkg_valid_from": lwDay22(13, 30)})
	if err != nil {
		t.Fatal(err)
	}
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
	if _, err := g.Rels().Update(ctx, rel.ID(), map[string]any{"r": "r2"}); err != nil {
		t.Fatalf("late relationship update: %v", err)
	}
	wals = append(wals, lwWAL(t, dir))

	// The late batch: 8 rows of 234 KiB (1.8 MiB) in one flush.
	lwCloseIdle(t, ts)
	var batch []types.NodeID
	for i := 0; i < 8; i++ {
		b, err := g.Nodes().Add(ctx, []string{"Signal"}, lwRow(fmt.Sprintf("b%d", i), map[string]any{"b": i, "tkg_valid_from": lwDay22(14, i)}))
		if err != nil {
			t.Fatalf("late batch row %d: %v", i, err)
		}
		batch = append(batch, b.ID())
	}
	if err := ts.Flush(); err != nil {
		t.Fatalf("late batch flush: %v", err)
	}
	wals = append(wals, lwWAL(t, dir))

	answers := func(g *graphpkg.Graph) string {
		s := lwAnswers(t, g, n.ID()) + lwAnswers(t, g, n2.ID()) + lwRelAnswers(t, g, rel.ID())
		for _, id := range batch {
			s += lwAnswers(t, g, id)
		}
		return s
	}
	before = answers(g)
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	_, g = lwOpen(t, dir, warm)
	defer func() { _ = g.Close() }()
	after = answers(g)
	return wals, before, after
}

// TestLateWriteColdShardOpensAtSmallMemtable: MemTableSize 64 MiB, no
// WarmMemTableSize. Every writable open of day 22, cold at each open (the
// window shard made on demand, the late write into the closed day, the late
// write beside a read-only handle, the late batch of 1.8 MiB), takes an 8 MiB
// memtable: WAL 16 MiB, not 128. The control (WarmMemTableSize 64 MiB, the
// hot size everywhere) opens at 64 MiB and answers the same: for both nodes,
// the relationship and the batch's nodes the current row, every version, each
// as of its transaction time, the hash chain; before and after a reopen.
func TestLateWriteColdShardOpensAtSmallMemtable(t *testing.T) {
	if testing.Short() {
		t.Skip("opens 64 MiB memtables: not in -short")
	}
	wals, before, after := lwRun(t, 0)
	cwals, cbefore, cafter := lwRun(t, lwHot)
	t.Logf("WAL of day 22's writable opens: %v (control %v)", wals, cwals)
	t.Logf("answers:\n%s", before)
	for i, w := range wals {
		if w != 2*lwCold {
			t.Errorf("writable open %d of the cold day: WAL %d, want 2 x %d (an 8 MiB memtable)", i, w, lwCold)
		}
	}
	for i, w := range cwals {
		if w != 2*lwHot {
			t.Fatalf("control: writable open %d: WAL %d, want 2 x %d", i, w, lwHot)
		}
	}
	if h, rh, c := lwLines(cbefore, "history "), lwLines(cbefore, "rel history "), lwLines(cbefore, "chain true <nil>"); h != 3 || rh != 1 || c != 2+8 {
		t.Fatalf("control answers: %d earlier node versions, %d earlier relationship versions, %d valid node chains; want 3, 1, 10:\n%s", h, rh, c, cbefore)
	}
	if !strings.Contains(cbefore, "rel chain true <nil>") {
		t.Fatalf("control: the relationship's chain is not valid:\n%s", cbefore)
	}
	if before != cbefore || after != cafter || after != before {
		t.Fatalf("answers differ from the control at the hot size:\nbefore:\n%s\ncontrol:\n%s\nafter reopen:\n%s\ncontrol after reopen:\n%s",
			before, cbefore, after, cafter)
	}
}

// lwBackfill writes 176 rows of 234 KiB (40 MiB) into cold day 22 through
// g.Nodes().Add, with a relationship from every fourth row to the one before,
// flushing every 4 rows (1 MiB, one plain write on either memtable). The
// first 8 rows run on a fresh small handle; after 40 MiB one sweep pass and
// one more row. It returns the WAL after the 8 rows and after the last row,
// and the answers of three sampled nodes (each updated once) and one
// relationship, before and after a reopen.
func lwBackfill(t *testing.T, warm int64) (walA, walB int64, before, after string) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	ts, g := lwOpen(t, dir, warm)
	if _, err := g.Nodes().Add(ctx, []string{"Signal"}, map[string]any{"seed": true, "tkg_valid_from": lwDay22(0, 1)}); err != nil {
		t.Fatal(err)
	}
	ts.SweepHandlesForTest() // the window shard's first handle closes
	if s, _ := lwShard(t, ts).StoreStateForTest(); s != nil {
		lwCloseIdle(t, ts)
	}
	ts.SetIdleTimeoutForTest(time.Hour)
	var nodes []*types.Node
	var rel *types.Relationship
	add := func(i int) {
		n, err := g.Nodes().Add(ctx, []string{"Signal"}, lwRow(fmt.Sprintf("i%d", i), map[string]any{"i": i, "tkg_valid_from": lwDay22(1+i/60, i%60)}))
		if err != nil {
			t.Fatalf("backfill row %d: %v", i, err)
		}
		if i%4 == 3 {
			r, err := g.Rels().Add(ctx, "NEXT", nodes[i-1], n, map[string]any{"i": i, "tkg_valid_from": lwDay22(1+i/60, i%60)})
			if err != nil {
				t.Fatalf("backfill relationship %d: %v", i, err)
			}
			if rel == nil {
				rel = r
			}
			if err := ts.Flush(); err != nil {
				t.Fatal(err)
			}
		}
		nodes = append(nodes, n)
	}
	for i := 0; i < 8; i++ {
		add(i)
	}
	walA = lwWAL(t, dir)
	for i := 8; i < 176; i++ {
		add(i)
	}
	ts.SweepHandlesForTest()
	add(176)
	if err := ts.Flush(); err != nil {
		t.Fatal(err)
	}
	walB = lwWAL(t, dir)
	sample := []types.NodeID{nodes[0].ID(), nodes[88].ID(), nodes[176].ID()}
	for _, id := range sample {
		if _, err := g.Nodes().Update(ctx, id, map[string]any{"seen": true}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := g.Rels().Update(ctx, rel.ID(), map[string]any{"seen": true}); err != nil {
		t.Fatal(err)
	}
	answers := func(g *graphpkg.Graph) string {
		s := lwRelAnswers(t, g, rel.ID())
		for _, id := range sample {
			s += lwAnswers(t, g, id)
		}
		return s
	}
	before = answers(g)
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	_, g = lwOpen(t, dir, warm)
	defer func() { _ = g.Close() }()
	after = answers(g)
	return walA, walB, before, after
}

// TestLateWriteBackfillGrowsTheColdHandle (T3): a backfill of 40 MiB into a
// cold day through the public doors runs on the 8 MiB handle until it has
// committed 32 MiB; one sweep pass closes it and the next write opens at
// MemTableSize (64 MiB). Control: WarmMemTableSize = MemTableSize, 64 MiB
// throughout; the sampled answers (three nodes and one relationship: current,
// every version, as of each, the chain) equal, before and after a reopen.
func TestLateWriteBackfillGrowsTheColdHandle(t *testing.T) {
	if testing.Short() {
		t.Skip("writes 40 MiB through 64 MiB memtables: not in -short")
	}
	walA, walB, before, after := lwBackfill(t, 0)
	cwalA, cwalB, cbefore, cafter := lwBackfill(t, lwHot)
	t.Logf("WAL after 2 MiB / after 40 MiB and a pass: %d / %d (control %d / %d)", walA, walB, cwalA, cwalB)
	if cwalA != 2*lwHot || cwalB != 2*lwHot {
		t.Fatalf("control: WAL %d / %d, want 2 x %d", cwalA, cwalB, lwHot)
	}
	if walA != 2*lwCold || walB != 2*lwHot {
		t.Errorf("WAL %d before the threshold, %d after; want 2 x %d, then 2 x %d", walA, walB, lwCold, lwHot)
	}
	if h, rh, c := lwLines(cbefore, "history "), lwLines(cbefore, "rel history "), lwLines(cbefore, "chain true <nil>"); h != 3 || rh != 1 || c != 3 || !strings.Contains(cbefore, "rel chain true <nil>") {
		t.Fatalf("control answers: %d earlier node versions, %d earlier relationship versions, %d valid node chains; want 3, 1, 3 and a valid relationship chain:\n%s", h, rh, c, cbefore)
	}
	if before != cbefore || after != cafter || after != before {
		t.Fatalf("answers differ from the control at the hot size:\nbefore:\n%s\ncontrol:\n%s\nafter reopen:\n%s\ncontrol after reopen:\n%s",
			before, cbefore, after, cafter)
	}
}
