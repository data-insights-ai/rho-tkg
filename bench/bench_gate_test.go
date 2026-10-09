package bench

import (
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// The bench gate (bench-compare.sh) runs bench-gate.awk over benchstat's CSV.
// The CSV holds one block per unit: sec/op first, then any custom metric a
// benchmark reports with b.ReportMetric (e.g. build-ms), then B/op and
// allocs/op. Only the sec/op block is a time gate. The fixtures in
// testdata/gate are real benchstat -format csv output of the *.txt files
// beside them.
//
// Faulty gate caught: one that leaves the sec/op block only at a B/op or
// allocs/op header and so reads a custom metric's rows as sec/op (identical
// code failed the gate on a +46 % build-ms sample: backlog 8 review).
func runGate(t *testing.T, csv string) (string, error) {
	t.Helper()
	if _, err := exec.LookPath("awk"); err != nil {
		t.Fatalf("awk not on PATH: %v", err)
	}
	cmd := exec.Command("awk", "-v", "thr=30", "-f", "bench-gate.awk", csv) // #nosec G204 -- fixed test inputs
	cmd.Env = append(os.Environ(), "LC_ALL=C")                              // as bench-compare.sh runs it
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func TestBenchGateIgnoresCustomMetricBlocks(t *testing.T) {
	out, err := runGate(t, "testdata/gate/custom-metric.csv")
	if err != nil {
		t.Fatalf("identical times with a noisy custom metric failed the gate: %v\n%s", err, out)
	}
}

func TestBenchGateStillCatchesTimeRegression(t *testing.T) {
	out, err := runGate(t, "testdata/gate/time-regression.csv")
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 1 {
		t.Fatalf("a +43 %% sec/op regression passed the gate: err=%v\n%s", err, out)
	}
	if !strings.Contains(out, "REGRESSION PinnedRelPropertyLookup/memory/20000/1type/sigma/matches200/current-32") {
		t.Fatalf("gate output does not name the regressed benchmark:\n%s", out)
	}
	if strings.Contains(out, "matches200/pinned-32") {
		t.Fatalf("gate flagged the unchanged benchmark (its build-ms moved):\n%s", out)
	}
}
