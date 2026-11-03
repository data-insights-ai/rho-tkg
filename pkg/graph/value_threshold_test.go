package graph_test

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"io/fs"
	"path/filepath"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
)

// VALUETHRESH (ai-soc R3, tasks/evidence/VALUETHRESH/measure.txt): with
// ValueThreshold 64 KB a 100 KB page row goes to badger's value log, the LSM
// keeping a pointer, through every door that builds badger stores (graph's
// BadgerDir, tiered, sharded); at the default (1 MB) it stays in the LSM.

func vtDirBytes(t *testing.T, dir string) (sst, vlog int64) {
	t.Helper()
	_ = filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		fi, err := d.Info()
		if err != nil {
			return nil
		}
		switch filepath.Ext(p) {
		case ".sst":
			sst += fi.Size()
		case ".vlog":
			vlog += fi.Size()
		}
		return nil
	})
	return sst, vlog
}

func TestValueThresholdKeepsRowsOutOfTheLSM(t *testing.T) {
	blob := func() string { // incompressible, so the LSM cannot shrink it
		b := make([]byte, 37_500)
		_, _ = rand.Read(b)
		return base64.StdEncoding.EncodeToString(b) // 50,000 bytes
	}
	doors := []struct {
		name string
		open func(t *testing.T, dir string, thr int64) *graphpkg.Graph
	}{
		{"badger", func(t *testing.T, dir string, thr int64) *graphpkg.Graph {
			g, err := graphpkg.New(graphpkg.Config{BadgerDir: dir, ValueThreshold: thr})
			if err != nil {
				t.Fatal(err)
			}
			return g
		}},
		{"tiered", func(t *testing.T, dir string, thr int64) *graphpkg.Graph {
			ts, err := tiered.New(tiered.Config{DataDir: dir, RefLabels: []string{"Page"}, ShardWindow: time.Hour, ValueThreshold: thr})
			if err != nil {
				t.Fatal(err)
			}
			g, err := graphpkg.New(graphpkg.Config{Store: ts})
			if err != nil {
				t.Fatal(err)
			}
			return g
		}},
		{"sharded", func(t *testing.T, dir string, thr int64) *graphpkg.Graph {
			st, err := sharded.New(sharded.Config{Dir: dir, BaseSlot: 0, SlotCount: 2, ValueThreshold: thr})
			if err != nil {
				t.Fatal(err)
			}
			g, err := graphpkg.New(graphpkg.Config{Store: st})
			if err != nil {
				t.Fatal(err)
			}
			return g
		}},
	}
	for _, door := range doors {
		for _, thr := range []int64{0, 64 << 10} {
			name := door.name + "/default"
			if thr > 0 {
				name = door.name + "/64KB"
			}
			t.Run(name, func(t *testing.T) {
				dir := t.TempDir()
				g := door.open(t, dir, thr)
				if _, err := g.Nodes().Add(context.Background(), []string{"Page"}, map[string]any{"a": blob(), "b": blob()}); err != nil {
					t.Fatal(err)
				}
				if err := g.Close(); err != nil { // the memtable goes to an L0 table
					t.Fatal(err)
				}
				sst, vlog := vtDirBytes(t, dir)
				if thr == 0 {
					if sst < 100_000 || vlog >= 100_000 {
						t.Fatalf("default threshold: %d table bytes, %d value-log bytes; want the 100 KB row in the LSM", sst, vlog)
					}
					return
				}
				if sst >= 64<<10 || vlog < 100_000 {
					t.Fatalf("ValueThreshold 64 KB: %d table bytes, %d value-log bytes; want the 100 KB row in the value log, the LSM under 64 KB", sst, vlog)
				}
			})
		}
	}
}
