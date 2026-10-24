package tiered

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// CLEARSHARDS. A process dies inside a tiered Clear after one of its stages:
// the index files, the reference shard, an event shard, the index definitions
// and archive, the catalog. The reference shard holds the property-key
// registry every shard's rows decode with. The reopened store must open and
// hold the old rows or none, and a property key first written after the
// reopen must read back as itself. The child runs the real Clear and exits
// there without Close; only the exit point comes from the test seam.

const (
	tcEnvDir   = "RHO_TIERED_CLEARCRASH_DIR"
	tcEnvStage = "RHO_TIERED_CLEARCRASH_STAGE"
	tcEvents   = 12
	tcRefs     = 5
)

var tcStages = []string{"index files", "reference shard", "event shard", "index defs and archive", "catalog"}

// tcOpen opens the store the way graph.New does: fixed label tokens (the
// graph's label registry), and the property-key registry loaded from the
// reference shard (or new) and installed on every shard.
func tcOpen(t *testing.T, dir string) (*Store, *registrypkg.LabelRegistry) {
	t.Helper()
	ts, err := New(Config{DataDir: dir, RefLabels: []string{"Case"}, ShardWindow: 7 * 24 * time.Hour})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	labels := registrypkg.NewLabelRegistry()
	for _, name := range []string{"Case", "Signal", "Invoice"} {
		if _, err := labels.GetOrCreate(name); err != nil {
			t.Fatal(err)
		}
	}
	ts.SetLabelRegistry(labels)
	keys := registrypkg.NewPropertyKeyRegistry()
	if _, err := ts.LoadPropertyKeyRegistry(keys); err != nil {
		_ = ts.Close()
		t.Fatalf("load property-key registry: %v", err)
	}
	ts.SetPropertyKeyRegistry(keys)
	return ts, labels
}

func tcToken(t *testing.T, labels *registrypkg.LabelRegistry, name string) uint16 {
	t.Helper()
	tok, err := labels.GetOrCreate(name)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

// TestTieredClearCrashChild is the child: it clears the store and dies after
// stage RHO_TIERED_CLEARCRASH_STAGE.
func TestTieredClearCrashChild(t *testing.T) {
	dir := os.Getenv(tcEnvDir)
	if dir == "" {
		t.Skip("child of TestTieredClearCrashAtEveryStage")
	}
	stage := os.Getenv(tcEnvStage)
	clearStepFaultForTest = func(s string) error {
		if s == stage {
			os.Exit(3) // the crash: no Close, no further stage
		}
		return nil
	}
	ts, _ := tcOpen(t, dir)
	err := ts.Clear()
	t.Logf("Clear returned %v without reaching stage %q", err, stage)
	os.Exit(4)
}

func TestTieredClearCrashAtEveryStage(t *testing.T) {
	for _, stage := range tcStages {
		t.Run("crash after "+stage, func(t *testing.T) {
			dir := t.TempDir()
			ts, labels := tcOpen(t, dir)
			gen := tieredNodeGen(t)
			for i := 0; i < tcEvents+tcRefs; i++ {
				label := "Signal"
				if i >= tcEvents {
					label = "Case"
				}
				n := types.NewNode(types.NodeID(gen.Generate()), tcToken(t, labels, label), nil)
				_ = n.SetProperty("zone", fmt.Sprintf("z%d", i%3))
				if err := ts.PutNode(n); err != nil {
					t.Fatal(err)
				}
			}
			// A running store's flush commits the registry ahead of its rows; the
			// closing flush leaves that to the graph's Close.
			if err := ts.Flush(); err != nil {
				t.Fatal(err)
			}
			if err := ts.Close(); err != nil {
				t.Fatal(err)
			}

			cmd := exec.Command(os.Args[0], "-test.run=^TestTieredClearCrashChild$", "-test.count=1")
			cmd.Env = append(os.Environ(), tcEnvDir+"="+dir, tcEnvStage+"="+stage)
			out, _ := cmd.CombinedOutput()
			if code := cmd.ProcessState.ExitCode(); code != 3 {
				t.Fatalf("the child did not die after %q (exit %d):\n%s", stage, code, out)
			}

			ts, labels = tcOpen(t, dir) // fails the test if the store cannot open
			defer ts.Close()
			count := func(label string) int {
				ns, err := ts.NodesByLabel(tcToken(t, labels, label), QueryOpts{})
				if err != nil {
					t.Errorf("crash after %q: %s nodes: %v", stage, label, err)
					return -1
				}
				return len(ns)
			}
			events, refs := count("Signal"), count("Case")
			all, allErr := ts.AllNodes(QueryOpts{})
			if allErr != nil {
				t.Errorf("crash after %q: reading every stored node fails: %v", stage, allErr)
			}
			old := events == tcEvents && refs == tcRefs
			empty := events == 0 && refs == 0 && allErr == nil && len(all) == 0
			if !old && !empty {
				t.Errorf("crash after %q: %d Signal and %d Case nodes, %d stored: neither the old store nor an empty one", stage, events, refs, len(all))
			}
			for _, n := range all {
				if v, ok := n.GetProperty("zone"); !ok || v == nil {
					t.Errorf("crash after %q: node %d lost its property zone", stage, n.ID())
					break
				}
			}

			fresh := types.NewNode(types.NodeID(gen.Generate()), tcToken(t, labels, "Invoice"), nil)
			_ = fresh.SetProperty("invoice_no", "A-1")
			if err := ts.PutNode(fresh); err != nil {
				t.Fatal(err)
			}
			if err := ts.Flush(); err != nil {
				t.Fatal(err)
			}
			got, err := ts.GetNode(fresh.ID())
			if err != nil {
				t.Fatalf("crash after %q: the node written after the reopen: %v", stage, err)
			}
			if v, _ := got.GetProperty("invoice_no"); v != "A-1" {
				t.Errorf("crash after %q: the node written after the reopen reads invoice_no %v", stage, v)
			}
			for _, n := range all {
				back, err := ts.GetNode(n.ID())
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
			t.Logf("crash after %q: %d Signal, %d Case, %d stored", stage, events, refs, len(all))
		})
	}
}
