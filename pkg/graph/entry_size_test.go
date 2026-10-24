package graph_test

// KEYSIZE / VALUESIZE through the public API (tasks/evidence/USAGE/size_test.go.txt
// was the red run): an entry badger cannot hold is refused at the write door
// with store.ErrEntryTooLarge, and the store keeps persisting.

import (
	"context"
	"errors"
	"strings"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
)

func esGraph(t *testing.T, cfg badger.Config) (*graphpkg.Graph, *badger.Store) {
	t.Helper()
	bs, err := badger.New(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	g, err := graphpkg.New(graphpkg.Config{Store: bs})
	if err != nil {
		t.Fatalf("graph: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g, bs
}

// KEYSIZE: a string value rho accepts (under its 65,536-byte limit) on a label
// with an on-disk property index makes an index key above badger's 65,000.
// A correct store either refuses the write at the door or stores it; it must
// not acknowledge the write and then fail to persist it.
func TestEntryTooLargeKeySizeThroughGraph(t *testing.T) {
	ctx := context.Background()
	g, bs := esGraph(t, badger.Config{Dir: t.TempDir(), PropertyIndexOnDisk: true})
	if err := g.Index().CreateProperty("Doc", "body"); err != nil {
		t.Fatal(err)
	}
	long := strings.Repeat("x", 65_100)
	n, addErr := g.Nodes().Add(ctx, []string{"Doc"}, map[string]any{"body": long})
	flushErr := bs.Flush()
	t.Logf("KEYSIZE: Add of a %d-byte indexed string: err=%v; Flush: err=%.160v", len(long), addErr, flushErr)
	if !errors.Is(addErr, storepkg.ErrEntryTooLarge) {
		t.Errorf("Add of an over-long indexed string: %v, want ErrEntryTooLarge", addErr)
	}
	if addErr == nil && flushErr != nil {
		t.Errorf("the write was acknowledged but cannot be persisted: %.120v", flushErr)
		// Does it take other writes with it? An ordinary node written after it:
		ok, err := g.Nodes().Add(ctx, []string{"Note"}, map[string]any{"t": "short"})
		if err != nil {
			t.Fatal(err)
		}
		again := bs.Flush()
		t.Logf("KEYSIZE: an ordinary node written afterwards (%v), next Flush: err=%.120v", ok.ID(), again)
		if again != nil {
			t.Errorf("one oversized key blocks every later write from persisting: %.120v", again)
		}
	}
	if addErr == nil && flushErr == nil {
		got, err := g.Nodes().ByLabelAndProperty("Doc", "body", long, storepkg.QueryOpts{})
		t.Logf("KEYSIZE: lookup by the value: %d nodes, err %v (node %v)", len(got), err, n.ID())
	}
}

// VALUESIZE: a row above ValueLogFileSize (rho allows 1 MB) and, in memory, a
// row of 1 MB or more: badger refuses the value outright (txn.go:368-371).
func TestEntryTooLargeValueSizeThroughGraph(t *testing.T) {
	ctx := context.Background()
	props := map[string]any{}
	for i := 0; i < 24; i++ { // 24 x 60 KB = 1.4 MB row, every value under rho's 64 KB limit
		props[string(rune('a'+i))+"p"] = strings.Repeat("y", 60_000)
	}
	for _, c := range []struct {
		name string
		cfg  badger.Config
	}{
		{"value-log file 1 MB", badger.Config{Dir: t.TempDir(), ValueLogFileSize: 1 << 20}},
		{"in memory", badger.Config{InMemory: true}},
		{"defaults", badger.Config{Dir: t.TempDir()}},
	} {
		t.Run(c.name, func(t *testing.T) {
			g, bs := esGraph(t, c.cfg)
			_, addErr := g.Nodes().Add(ctx, []string{"Big"}, props)
			flushErr := bs.Flush()
			t.Logf("VALUESIZE %s: Add of a 1.4 MB row: err=%v; Flush: err=%.160v", c.name, addErr, flushErr)
			if c.name != "defaults" && !errors.Is(addErr, storepkg.ErrEntryTooLarge) {
				t.Errorf("Add of a row the store cannot hold: %v, want ErrEntryTooLarge", addErr)
			}
			if c.name == "defaults" && (addErr != nil || flushErr != nil) {
				t.Errorf("default options must take a 1.4 MB row: %v / %v", addErr, flushErr)
			}
			if addErr == nil && flushErr != nil {
				t.Errorf("the write was acknowledged but cannot be persisted: %.200v", flushErr)
			}
		})
	}
}
