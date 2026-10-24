package graph_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
)

// CLEARMETA (USAGE W5). A process dies inside Clear (g.Admin().Reset() on a
// badger store with the change log on) right after one of Clear's durable
// steps. The reopened store must open, answer as before the Clear or as an
// empty store (no row left on disk), and a label written after the reopen must
// answer only its own nodes.
// The child process runs the real Clear and exits there without Close; only
// the exit point comes from the test seam.

const (
	cmEnvDir  = "RHO_CLEARMETA_DIR"
	cmEnvStep = "RHO_CLEARMETA_STEP"
)

func cmOpen(t *testing.T, dir string) *graphpkg.Graph {
	t.Helper()
	bs, err := badger.New(badger.Config{Dir: dir, ChangeLog: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	g, err := graphpkg.New(graphpkg.Config{Store: bs, AllowReset: true})
	if err != nil {
		t.Fatalf("graph: %v", err)
	}
	return g
}

// TestClearMetaCrashChild is the child: it clears the store and dies after
// step RHO_CLEARMETA_STEP.
func TestClearMetaCrashChild(t *testing.T) {
	dir := os.Getenv(cmEnvDir)
	if dir == "" {
		t.Skip("child of TestClearCrashAtEveryDurableStep")
	}
	step, _ := strconv.Atoi(os.Getenv(cmEnvStep))
	defer badger.SetClearStepFaultForTest(func(s int) error {
		if s == step {
			os.Exit(3) // the crash: no Close, no further step
		}
		return nil
	})()
	g := cmOpen(t, dir)
	err := g.Admin().Reset()
	t.Logf("Reset returned %v without reaching step %d", err, step)
	os.Exit(4)
}

func TestClearCrashAtEveryDurableStep(t *testing.T) {
	ctx := context.Background()
	for step := 1; step <= 4; step++ {
		t.Run(fmt.Sprintf("crash after step %d", step), func(t *testing.T) {
			dir := t.TempDir()
			g := cmOpen(t, dir)
			if err := g.Index().CreateProperty("Host", "zone"); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 20; i++ {
				if _, err := g.Nodes().Add(ctx, []string{"Host"}, map[string]any{"zone": fmt.Sprintf("z%d", i%3)}); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := g.Nodes().Add(ctx, []string{"Account"}, nil); err != nil {
				t.Fatal(err)
			}
			if err := g.Close(); err != nil {
				t.Fatal(err)
			}

			cmd := exec.Command(os.Args[0], "-test.run=^TestClearMetaCrashChild$", "-test.count=1")
			cmd.Env = append(os.Environ(), cmEnvDir+"="+dir, cmEnvStep+"="+strconv.Itoa(step))
			out, _ := cmd.CombinedOutput()
			if code := cmd.ProcessState.ExitCode(); code != 3 {
				t.Fatalf("the child did not die at step %d (exit %d):\n%s", step, code, out)
			}

			// A crash inside a Reset must not leave a store that cannot open.
			bs, err := badger.New(badger.Config{Dir: dir, ChangeLog: true})
			if err != nil {
				t.Fatalf("reopen after a crash at step %d refused: %v", step, err)
			}
			g, err = graphpkg.New(graphpkg.Config{Store: bs, AllowReset: true})
			if err != nil {
				_ = bs.Close()
				t.Fatalf("graph after a crash at step %d refused: %v", step, err)
			}
			defer g.Close()
			// Every row on disk, decoded: label queries alone answer 0 when the
			// registries are gone and the rows remain.
			all, allErr := bs.AllNodes(storepkg.QueryOpts{})
			if allErr != nil {
				t.Errorf("crash at step %d: reading every stored node fails: %v", step, allErr)
			}
			count := func(label string) int {
				ns, err := g.Nodes().ByLabel(label, storepkg.QueryOpts{})
				if err != nil {
					t.Errorf("crash at step %d: ByLabel(%s) fails: %v", step, label, err)
					return -1
				}
				return len(ns)
			}
			hosts, accounts := count("Host"), count("Account")
			t.Logf("crash at step %d: after reopen Host %d, Account %d (before the Clear 20 and 1)", step, hosts, accounts)
			old := hosts == 20 && accounts == 1
			empty := hosts == 0 && accounts == 0 && allErr == nil && len(all) == 0
			if !old && !empty {
				t.Errorf("crash at step %d: %d Host and %d Account nodes, %d stored: neither the old store nor an empty one", step, hosts, accounts, len(all))
			}
			if _, err := g.Nodes().Add(ctx, []string{"Invoice"}, nil); err != nil {
				t.Fatal(err)
			}
			if inv := count("Invoice"); inv != 1 && inv != -1 {
				t.Errorf("crash at step %d: a label written after the reopen answers %d nodes for 1 written (old: %v, empty: %v)", step, inv, old, empty)
			}
			t.Logf("crash at step %d: reopened %s", step, map[bool]string{true: "with the old rows", false: "empty or neither"}[old])
		})
	}
}
