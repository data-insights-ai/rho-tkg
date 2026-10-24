package sharded

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"testing"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// CLEARSHARDS. A process dies inside a sharded Clear after one of its stages:
// each shard's Clear, then the catalog. The anchor (shard 0) holds the
// property-key registry every shard's rows decode with. The reopened store
// must open and hold the old rows or none, and a property key first written
// after the reopen must read back as itself. The child runs the real Clear and
// exits there without Close; only the exit point comes from the test seam.

const (
	scEnvDir   = "RHO_SHARDED_CLEARCRASH_DIR"
	scEnvStage = "RHO_SHARDED_CLEARCRASH_STAGE"
	scSlots    = 3
	scPerSlot  = 4
)

func scStages() []string {
	var out []string
	for i := 0; i < scSlots; i++ {
		out = append(out, fmt.Sprintf("shard %d", i))
	}
	return append(out, "catalog")
}

// scOpen opens the store the way graph.New does: the property-key registry is
// loaded from the anchor (or new) and installed on every shard.
func scOpen(t *testing.T, dir string) *Store {
	t.Helper()
	st, err := New(Config{Dir: dir, BaseSlot: 0, SlotCount: scSlots, ChangeLog: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	keys := registrypkg.NewPropertyKeyRegistry()
	if _, err := st.LoadPropertyKeyRegistry(keys); err != nil {
		_ = st.Close()
		t.Fatalf("load property-key registry: %v", err)
	}
	st.SetPropertyKeyRegistry(keys)
	return st
}

// TestShardedClearCrashChild is the child: it clears the store and dies after
// stage RHO_SHARDED_CLEARCRASH_STAGE.
func TestShardedClearCrashChild(t *testing.T) {
	dir := os.Getenv(scEnvDir)
	if dir == "" {
		t.Skip("child of TestShardedClearCrashAtEveryStage")
	}
	stage := os.Getenv(scEnvStage)
	clearStepFaultForTest = func(s string) error {
		if s == stage {
			os.Exit(3) // the crash: no Close, no further stage
		}
		return nil
	}
	st := scOpen(t, dir)
	err := st.Clear()
	t.Logf("Clear returned %v without reaching stage %q", err, stage)
	os.Exit(4)
}

func TestShardedClearCrashAtEveryStage(t *testing.T) {
	for _, stage := range scStages() {
		t.Run("crash after "+stage, func(t *testing.T) {
			dir := t.TempDir()
			st := scOpen(t, dir)
			var ids []types.NodeID
			for slot := uint8(0); slot < scSlots; slot++ {
				for i := int64(1); i <= scPerSlot; i++ {
					n := types.NewNode(mkNodeID(slot, i), 3, nil)
					_ = n.SetProperty("zone", fmt.Sprintf("z%d", i%3))
					if err := st.PutNode(n); err != nil {
						t.Fatal(err)
					}
					ids = append(ids, n.ID())
				}
			}
			// A running store's flush commits the registry ahead of its rows; the
			// closing flush leaves that to the graph's Close.
			if err := st.Flush(); err != nil {
				t.Fatal(err)
			}
			if err := st.Close(); err != nil {
				t.Fatal(err)
			}

			cmd := exec.Command(os.Args[0], "-test.run=^TestShardedClearCrashChild$", "-test.count=1")
			cmd.Env = append(os.Environ(), scEnvDir+"="+dir, scEnvStage+"="+stage)
			out, _ := cmd.CombinedOutput()
			if code := cmd.ProcessState.ExitCode(); code != 3 {
				t.Fatalf("the child did not die after %q (exit %d):\n%s", stage, code, out)
			}

			st = scOpen(t, dir) // fails the test if the store cannot open
			defer st.Close()
			all, allErr := st.AllNodes(QueryOpts{})
			if allErr != nil {
				t.Errorf("crash after %q: reading every stored node fails: %v", stage, allErr)
			}
			old := allErr == nil && len(all) == len(ids)
			empty := allErr == nil && len(all) == 0
			if !old && !empty {
				t.Errorf("crash after %q: %d of %d nodes stored: neither the old store nor an empty one", stage, len(all), len(ids))
			}
			for _, n := range all {
				if v, ok := n.GetProperty("zone"); !ok || v == nil {
					t.Errorf("crash after %q: node %d lost its property zone", stage, n.ID())
					break
				}
			}

			fresh := types.NewNode(mkNodeID(1, 99), 4, nil)
			_ = fresh.SetProperty("invoice_no", "A-1")
			if err := st.PutNode(fresh); err != nil {
				t.Fatal(err)
			}
			if err := st.Flush(); err != nil {
				t.Fatal(err)
			}
			got, err := st.GetNode(fresh.ID())
			if err != nil {
				t.Fatalf("crash after %q: the node written after the reopen: %v", stage, err)
			}
			if v, _ := got.GetProperty("invoice_no"); v != "A-1" {
				t.Errorf("crash after %q: the node written after the reopen reads invoice_no %v", stage, v)
			}
			for _, id := range ids {
				back, err := st.GetNode(id)
				if err != nil && !errors.Is(err, ErrNodeNotFound) {
					t.Errorf("crash after %q: an old node: %v", stage, err)
					break
				}
				if err == nil {
					if _, has := back.GetProperty("invoice_no"); has {
						t.Errorf("crash after %q: an old node answers under the new key invoice_no", stage)
						break
					}
				}
			}
			t.Logf("crash after %q: %d of %d stored", stage, len(all), len(ids))
		})
	}
}
