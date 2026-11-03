package core

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// captureOwnerIndexLog routes the owner index's log lines to a buffer for the
// test (not parallel: the seam is the package's).
func captureOwnerIndexLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := ownerIndexLogger
	ownerIndexLogger = func() *slog.Logger { return slog.New(slog.NewTextHandler(&buf, nil)) }
	t.Cleanup(func() { ownerIndexLogger = prev })
	return &buf
}

func openSeedCore(t *testing.T, dir string) (*Core, *badger.Store) {
	t.Helper()
	bs, err := badger.New(badger.Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	c, err := New(Config{Store: bs, SnowflakeNodeID: 3})
	if err != nil {
		t.Fatal(err)
	}
	return c, bs
}

// TestOwnerIndexSeedPagesAndLogs (UNIQUELOOK review F4): the seed reads the
// label in pages, never the whole label in one slice, logs its progress and
// a final count, and every row is found afterwards.
func TestOwnerIndexSeedPagesAndLogs(t *testing.T) {
	logs := captureOwnerIndexLog(t)
	prevPage, prevEvery := ownerSeedPageSize, ownerSeedProgressEvery
	ownerSeedPageSize, ownerSeedProgressEvery = 3, 0
	t.Cleanup(func() { ownerSeedPageSize, ownerSeedProgressEvery = prevPage, prevEvery })
	ctx := context.Background()
	c, _ := openSeedCore(t, t.TempDir())
	defer c.Close()
	ids := map[string]types.NodeID{}
	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("p%d", i)
		n, err := c.Nodes.Add(ctx, []string{"Host"}, map[string]any{"key": k})
		if err != nil {
			t.Fatal(err)
		}
		ids[k] = n.ID()
	}
	if err := c.Constraints.CreateUnique(ctx, "Host", "key"); err != nil {
		t.Fatal(err)
	}
	out := logs.String()
	if got := strings.Count(out, "graph: seeding unique owner index"); got < 4 {
		t.Errorf("%d progress lines for 4 pages of 3, want at least 4:\n%s", got, out)
	}
	if !strings.Contains(out, "graph: seeded unique owner index") || !strings.Contains(out, "rows=10") || !strings.Contains(out, "pages=4") {
		t.Errorf("no final line with rows=10 pages=4:\n%s", out)
	}
	for k, want := range ids {
		n, created, err := c.Nodes.GetOrCreateByKey(ctx, "Host", "key", k, nil)
		if err != nil || created || n.ID() != want {
			t.Fatalf("GetOrCreate(%s): created %v err %v", k, created, err)
		}
	}
}

// TestOwnerIndexSeedFailureAtOpenLogged (UNIQUELOOK review F4, F5): a store
// whose rows hold one value twice (written past the kernel) opens without
// blocking; the open's seed fails, says so in the log, and leaves the index
// unready, so the lookups return both holders.
func TestOwnerIndexSeedFailureAtOpenLogged(t *testing.T) {
	logs := captureOwnerIndexLog(t)
	ctx := context.Background()
	dir := t.TempDir()
	c, bs := openSeedCore(t, dir)
	if err := c.Constraints.CreateUnique(ctx, "Host", "key"); err != nil {
		t.Fatal(err)
	}
	a, err := c.Nodes.Add(ctx, []string{"Host"}, map[string]any{"key": "dup"})
	if err != nil {
		t.Fatal(err)
	}
	tok := a.LabelTokenRawAt(0)
	twin := types.NewNode(types.NodeID(a.ID()+1), tok, nil)
	if err := twin.SetProperty("key", "dup"); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNode(twin); err != nil { // past the kernel: no claim, no check
		t.Fatal(err)
	}
	if err := bs.DropUniqueOwnerIndex(tok, "key"); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	c, bs = openSeedCore(t, dir)
	defer c.Close()
	if bs.UniqueOwnerIndexReady(tok, "key") {
		t.Fatal("the index is ready over two holders")
	}
	out := logs.String()
	if !strings.Contains(out, "graph: unique owner index left unready") || !strings.Contains(out, "label=Host") {
		t.Errorf("the open's failed seed was not logged:\n%s", out)
	}
	if strings.Contains(out, "dup") {
		t.Errorf("the log names a value:\n%s", out)
	}
	rows, err := c.Nodes.ByLabelAndProperty("Host", "key", "dup", storepkg.QueryOpts{})
	if err != nil || len(rows) != 2 {
		t.Fatalf("holders %d (%v), want 2", len(rows), err)
	}
}
