package badger

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// CLEARMETA (USAGE W5). A process dies inside Clear (Admin().Reset() on a
// badger store with the change log on) right after one of Clear's durable
// steps. The reopened store must open and hold the old rows or none, and a
// property key first written after the reopen must read back as itself: the
// store's property-key registry is metadata, and before CLEARMETA Clear
// deleted it ahead of the data, so the old rows' key tokens were handed out
// again. The child process runs the real Clear and exits there without Close;
// only the exit point comes from the test seam.

const (
	ccEnvDir  = "RHO_CLEARCRASH_DIR"
	ccEnvStep = "RHO_CLEARCRASH_STEP"
	ccNodes   = 21
)

// ccOpen opens the store the way graph.New does: the property-key registry is
// loaded from the store's metadata (or new) and installed, so rows carry key
// tokens, as in production.
func ccOpen(t *testing.T, dir string) *Store {
	t.Helper()
	bs, err := New(Config{Dir: dir, ChangeLog: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	reg := registrypkg.NewPropertyKeyRegistry()
	if _, err := bs.LoadPropertyKeyRegistry(reg); err != nil {
		_ = bs.Close()
		t.Fatalf("load property-key registry: %v", err)
	}
	bs.SetPropertyKeyRegistry(reg)
	return bs
}

// TestClearCrashChild is the child: it clears the store and dies after step
// RHO_CLEARCRASH_STEP.
func TestClearCrashChild(t *testing.T) {
	dir := os.Getenv(ccEnvDir)
	if dir == "" {
		t.Skip("child of TestClearCrashAtEveryDurableStep")
	}
	step, _ := strconv.Atoi(os.Getenv(ccEnvStep))
	clearStepFaultForTest = func(s int) error {
		if s == step {
			os.Exit(3) // the crash: no Close, no further step
		}
		return nil
	}
	bs := ccOpen(t, dir)
	err := bs.Clear()
	t.Logf("Clear returned %v without reaching step %d", err, step)
	os.Exit(4)
}

func TestClearCrashAtEveryDurableStep(t *testing.T) {
	for step := 1; step <= 4; step++ {
		t.Run(fmt.Sprintf("crash after step %d", step), func(t *testing.T) {
			dir := t.TempDir()
			bs := ccOpen(t, dir)
			for i := 0; i < ccNodes; i++ {
				n := types.NewNode(types.NodeID(snowflake.ID(7000+i)), 3, nil)
				_ = n.SetProperty("zone", fmt.Sprintf("z%d", i%3))
				if err := bs.PutNode(n); err != nil {
					t.Fatal(err)
				}
			}
			// A running store's flush commits the registry ahead of its rows
			// (persistRegistryIfGrew); the closing flush leaves that to the
			// graph's Close, which this store-level test has not.
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			if err := bs.Close(); err != nil {
				t.Fatal(err)
			}

			cmd := exec.Command(os.Args[0], "-test.run=^TestClearCrashChild$", "-test.count=1")
			cmd.Env = append(os.Environ(), ccEnvDir+"="+dir, ccEnvStep+"="+strconv.Itoa(step))
			out, _ := cmd.CombinedOutput()
			if code := cmd.ProcessState.ExitCode(); code != 3 {
				t.Fatalf("the child did not die at step %d (exit %d):\n%s", step, code, out)
			}

			// A crash inside a Reset must not leave a store that cannot open.
			bs, err := New(Config{Dir: dir, ChangeLog: true})
			if err != nil {
				t.Fatalf("reopen after a crash at step %d refused: %v", step, err)
			}
			_ = bs.Close()
			bs = ccOpen(t, dir)
			defer bs.Close()
			all, err := bs.AllNodes(QueryOpts{})
			if err != nil {
				t.Errorf("crash at step %d: reading every stored node fails: %v", step, err)
			}
			old := len(all) == ccNodes
			if !old && len(all) != 0 {
				t.Errorf("crash at step %d: %d of %d nodes stored: neither the old store nor an empty one", step, len(all), ccNodes)
			}
			for _, n := range all {
				if v, ok := n.GetProperty("zone"); !ok || v == nil {
					t.Errorf("crash at step %d: node %d lost its property zone", step, n.ID())
					break
				}
			}

			// A key first written after the reopen takes a fresh token; the
			// old rows must not start answering under it.
			fresh := types.NewNode(types.NodeID(snowflake.ID(9000)), 4, nil)
			_ = fresh.SetProperty("invoice_no", "A-1")
			if err := bs.PutNode(fresh); err != nil {
				t.Fatal(err)
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			got, err := bs.GetNode(fresh.ID())
			if err != nil {
				t.Fatalf("crash at step %d: the node written after the reopen: %v", step, err)
			}
			if v, _ := got.GetProperty("invoice_no"); v != "A-1" {
				t.Errorf("crash at step %d: the node written after the reopen reads invoice_no %v", step, v)
			}
			if old {
				n, err := bs.GetNode(types.NodeID(snowflake.ID(7000)))
				if err != nil && !errors.Is(err, ErrNodeNotFound) {
					t.Errorf("crash at step %d: an old node: %v", step, err)
				} else if err == nil {
					if _, has := n.GetProperty("invoice_no"); has {
						t.Errorf("crash at step %d: an old node answers under the new key invoice_no", step)
					}
				}
			}
			t.Logf("crash at step %d: reopened %s", step, map[bool]string{true: "with the old rows", false: "empty"}[old])
		})
	}
}
