package graph_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	tieredpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// treeState is every file and directory under the roots: its size, mode,
// mtime and, for a file, its sha256.
func treeState(t *testing.T, roots ...string) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, root := range roots {
		err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			info, err := d.Info()
			if err != nil {
				return err
			}
			s := info.Mode().String() + " " + info.ModTime().Format(time.RFC3339Nano)
			if info.Mode().IsRegular() {
				b, err := os.ReadFile(p) // #nosec G304 -- the test's own temp dir
				if err != nil {
					return err
				}
				sum := sha256.Sum256(b)
				s += " " + hex.EncodeToString(sum[:])
			}
			out[p] = s
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	return out
}

// A tiered store opened with Config.ReadOnly, under graph.New as ai-soc's
// check-store opens it, answers labels, reference types, a record node and a
// type's facts by a property from its reference, hot, warm and cold shards,
// refuses every write, changes no byte, mode or mtime of any file of the data
// and cold volumes, and is refused while a writer holds the store.
func TestTieredReadOnlyOpenWritesNothing(t *testing.T) {
	dir, cold := t.TempDir(), t.TempDir()
	var now atomic.Int64
	now.Store(time.Now().UnixNano())
	clock := func() time.Time { return time.Unix(0, now.Load()) }
	cfg := tieredpkg.Config{
		DataDir: dir, ColdDir: cold, RefLabels: []string{"Opened", "Case"}, RefTypes: []string{"ABOUT"},
		ShardWindow: 24 * time.Hour, ColdAfter: 3 * 24 * time.Hour, MaxOpenColdShards: 1, Clock: clock,
		SyncWrites: true, RelsByMint: true, AdjacencyIndexOnDisk: true,
	}
	open := func(c tieredpkg.Config) (*graphpkg.Graph, *tieredpkg.Store, error) {
		ts, err := tieredpkg.New(c)
		if err != nil {
			return nil, nil, err
		}
		g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 3, Store: ts, DurableCommit: !c.ReadOnly, UntimedValidStart: graphpkg.UntimedUnbounded})
		if err != nil {
			return nil, nil, errors.Join(err, ts.Close())
		}
		return g, ts, nil
	}
	ctx := context.Background()
	g, ts, err := open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	caseNode, err := g.Nodes().Add(ctx, []string{"Case"}, map[string]any{"name": "c1"})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int{}
	day := func(field string, n int) {
		for i := 0; i < n; i++ {
			ev, err := g.Nodes().Add(ctx, []string{"Event"}, map[string]any{"i": int64(i)})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := g.Rels().Add(ctx, "CASEINPUT", ev, caseNode, map[string]any{"field": field}); err != nil {
				t.Fatal(err)
			}
			want[field]++
		}
		now.Add(int64(24 * time.Hour))
		if err := ts.RotateHotShard(); err != nil {
			t.Fatal(err)
		}
	}
	day("user", 3)
	day("host", 2)
	day("ip", 1)
	archived, err := g.Nodes().Add(ctx, []string{"Case"}, map[string]any{"name": "old"})
	if err != nil {
		t.Fatal(err)
	}
	if err := ts.ArchiveNode(archived.ID()); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Nodes().Add(ctx, []string{"Opened"}, map[string]any{"layoutTypes": "ABOUT"}); err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	// A writable reopen past ColdAfter demotes the first day; it moves to the cold volume.
	now.Add(int64(2 * 24 * time.Hour))
	g, ts, err = open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	colds := coldShards(t, ts)
	for _, name := range colds {
		if err := ts.MoveShard(ctx, name, tieredpkg.VolumeCold); err != nil {
			t.Fatal(err)
		}
	}
	if len(colds) == 0 {
		t.Fatal("no shard went cold")
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}

	before := treeState(t, dir, cold)
	now.Add(int64(30 * 24 * time.Hour)) // would demote every shard on a writable open
	ro := cfg
	ro.ReadOnly = true
	g, ts, err = open(ro)
	if err != nil {
		t.Fatal(err)
	}
	for label, want := range map[string]bool{"Opened": true, "Case": true, "Event": false} {
		if ref, err := g.Admin().IsReferenceLabel(label); err != nil || ref != want {
			t.Fatalf("IsReferenceLabel(%s) = %v, %v", label, ref, err)
		}
	}
	opened, err := g.Nodes().ByLabel("Opened", graphpkg.QueryOpts{NoSort: true, Depth: storepkg.DepthAll})
	if err != nil || len(opened) != 1 {
		t.Fatalf("Opened records %d, %v", len(opened), err)
	}
	if v, _ := opened[0].GetProperty("layoutTypes"); v != "ABOUT" {
		t.Fatalf("layout record %v", v)
	}
	// An archived reference node is read from the archive shard, opened read-only.
	if n, err := ts.GetNode(archived.ID()); err != nil || n == nil {
		t.Fatalf("archived node: %v, %v", n, err)
	}
	rs, err := g.Rels().ByType("CASEINPUT", graphpkg.QueryOpts{NoSort: true, Depth: storepkg.DepthAll})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]int{}
	for _, r := range rs {
		v, _ := r.GetProperty("field")
		s, _ := v.(string)
		got[s]++
	}
	if len(got) != len(want) || got["user"] != want["user"] || got["host"] != want["host"] || got["ip"] != want["ip"] {
		t.Fatalf("CASEINPUT by field %v, want %v", got, want)
	}

	// Every write is refused, with ErrReadOnly or the shard's own ErrInvalidStoreMutation: graph writes and DDL,
	// rotation, moves, purges, catalog rebuild, repair, delete, archive and Clear.
	refused := func(what string, err error) {
		t.Helper()
		if !errors.Is(err, tieredpkg.ErrReadOnly) && !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
			t.Errorf("%s on a read-only store: %v, want ErrReadOnly or ErrInvalidStoreMutation", what, err)
		}
	}
	_, err = g.Nodes().Add(ctx, []string{"Event"}, nil)
	refused("node add", err)
	_, err = g.Rels().Add(ctx, "CASEINPUT", opened[0], caseNode, nil)
	refused("relationship add", err)
	refused("CreateTemporal", g.Index().CreateTemporal("Event"))
	if err := ts.RotateHotShard(); !errors.Is(err, tieredpkg.ErrReadOnly) {
		t.Fatalf("rotation: %v, want ErrReadOnly", err)
	}
	for _, name := range coldShards(t, ts) {
		if err := ts.MoveShard(ctx, name, tieredpkg.VolumeHot); !errors.Is(err, tieredpkg.ErrReadOnly) {
			t.Fatalf("move: %v, want ErrReadOnly", err)
		}
	}
	past := types.Instant(clock().Add(time.Hour).UnixNano())
	_, err = ts.PurgeNodesByLabelBefore(1, past, 0)
	refused("PurgeNodesByLabelBefore", err)
	_, err = ts.PurgeNodesByLabelValidToBefore(1, past, 0)
	refused("PurgeNodesByLabelValidToBefore", err)
	_, err = ts.PurgeRelsByTypeBefore(1, past, 0)
	refused("PurgeRelsByTypeBefore", err)
	_, err = ts.PurgeNodesByIDs([]types.NodeID{caseNode.ID()}, nil)
	refused("PurgeNodesByIDs", err)
	refused("LogRangePurge", ts.LogRangePurge(1, past, 0))
	refused("LogRelRangePurge", ts.LogRelRangePurge(1, past, 0))
	refused("RebuildCatalog", ts.RebuildCatalog())
	_, err = ts.RunRepair()
	refused("RunRepair", err)
	_, err = ts.VerifyShard(nil, "reference")
	refused("VerifyShard", err)
	refused("DeleteNode", ts.DeleteNode(caseNode.ID()))
	refused("ArchiveNode", ts.ArchiveNode(caseNode.ID()))
	refused("Clear", ts.Clear())
	// A second read-only open shares the store; a writer is refused while it is open.
	g2, _, err := open(ro)
	if err != nil {
		t.Fatalf("second read-only open: %v", err)
	}
	if _, _, err := open(cfg); err == nil || !strings.Contains(err.Error(), "directory lock") {
		t.Fatalf("writable open beside a read-only one: %v", err)
	}
	if err := errors.Join(g2.Close(), g.Close()); err != nil {
		t.Fatal(err)
	}
	after := treeState(t, dir, cold)
	for p, s := range before {
		if after[p] != s {
			t.Errorf("%s changed: %q -> %q", p, s, after[p])
		}
	}
	for p := range after {
		if _, ok := before[p]; !ok {
			t.Errorf("%s made by the read-only open", p)
		}
	}

	// While a writer holds the store, a read-only open is refused.
	g, _, err = open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	if _, _, err := open(ro); err == nil || !strings.Contains(err.Error(), "directory lock") {
		t.Fatalf("read-only open beside a writer: %v", err)
	}
}

// coldShards are the names of the store's cold shards.
func coldShards(t *testing.T, ts *tieredpkg.Store) []string {
	t.Helper()
	infos, err := ts.ListShards()
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range infos {
		if e.Tier == tieredpkg.TierCold && e.Kind == tieredpkg.ShardEvent {
			out = append(out, e.Name)
		}
	}
	return out
}
