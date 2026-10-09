package bench

import (
	"errors"
	"os"
	"os/exec"
	"regexp"
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
func runGate(t *testing.T, csv string, vars ...string) (string, error) {
	t.Helper()
	if _, err := exec.LookPath("awk"); err != nil {
		t.Fatalf("awk not on PATH: %v", err)
	}
	args := []string{"-v", "thr=30"}
	for _, v := range vars {
		args = append(args, "-v", v)
	}
	args = append(args, "-f", "bench-gate.awk", csv)
	cmd := exec.Command("awk", args...)        // #nosec G204 -- fixed test inputs
	cmd.Env = append(os.Environ(), "LC_ALL=C") // as bench-compare.sh runs it
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

// The PinnedRelPropertyLookup family is gated on allocs/op (+10 %), and on time
// only for its canary rows (memory and badger x {1type/sigma,
// 5types/unrelated-x10} x matches200): its time rows swing tens of percent
// between identical runs on a loaded runner, while allocs/op does not move,
// and the regression that matters — the lookup falling back to the history
// fold — multiplies allocs (1,423 -> 106,555). The family_* fixtures are real
// benchstat CSV of the family-*.txt files beside them (5 samples each).

// Faulty gate caught: gating every family row on time (an allocs-only row
// swinging +200 % fails identical code).
func TestBenchGateFamilyTimeSwingOnAllocsOnlyRowPasses(t *testing.T) {
	out, err := runGate(t, "testdata/gate/family-timeswing.csv")
	if err != nil {
		t.Fatalf("a +200 %% time swing on an allocs-only family row with stable allocs failed the gate: %v\n%s", err, out)
	}
}

// Faulty gate caught: no allocs gate (the fold fallback keeps the time of a
// small fixture under the threshold and passes).
func TestBenchGateFamilyAllocsRegressionFails(t *testing.T) {
	out, err := runGate(t, "testdata/gate/family-allocs.csv")
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 1 {
		t.Fatalf("allocs 1,423 -> 106,555 on a family row passed the gate: err=%v\n%s", err, out)
	}
	if !strings.Contains(out, "ALLOCS REGRESSION PinnedRelPropertyLookup/memory/20000/1type/sigma/matches200/pinned-32") {
		t.Fatalf("gate output does not name the allocs regression:\n%s", out)
	}
	if strings.Contains(out, "RelPropertyLookup10k") || strings.Contains(out, "current-32") {
		t.Fatalf("gate flagged a row whose allocs did not move:\n%s", out)
	}
}

// Faulty gate caught: dropping the family from the time gate altogether (a
// canary row regressing +60 % must still fail).
func TestBenchGateFamilyCanaryTimeRegressionFails(t *testing.T) {
	out, err := runGate(t, "testdata/gate/family-canary-time.csv")
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 1 {
		t.Fatalf("a +60 %% time regression on a canary row passed the gate: err=%v\n%s", err, out)
	}
	if !strings.Contains(out, "REGRESSION PinnedRelPropertyLookup/badger/20000/5types/unrelated-x10/matches200/pinned-32") {
		t.Fatalf("gate output does not name the canary row:\n%s", out)
	}
}

// family=none switches the allocs gate off and time-gates every row again
// (the escape hatch bench-compare.sh exposes as ALLOCS_GATE_FAMILY=none).
func TestBenchGateFamilyNoneTimeGatesEveryRow(t *testing.T) {
	out, err := runGate(t, "testdata/gate/family-timeswing.csv", "family=none")
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 1 || !strings.Contains(out, "REGRESSION PinnedRelPropertyLookup/memory/20000/1type/nochurn/matches200/pinned-32") {
		t.Fatalf("family=none must time-gate the allocs-only row: err=%v\n%s", err, out)
	}
	out, err = runGate(t, "testdata/gate/family-allocs.csv", "family=none")
	if err != nil {
		t.Fatalf("family=none must not allocs-gate: %v\n%s", err, out)
	}
}

// TestPinnedRelGateMatrixMatchesTheGate ties the benchmark's default matrix to
// the gate's defaults (read from bench-gate.awk itself): 24 allocs-gated rows,
// of which exactly the 8 canary rows are time-gated, no sharded and no broad
// row. Faulty set-ups caught: a canary regex that matches nothing (the family
// silently loses its time gate), sharded or broad back in the default run, a
// family regex that misses the benchmark.
func TestPinnedRelGateMatrixMatchesTheGate(t *testing.T) {
	src, err := os.ReadFile("bench-gate.awk")
	if err != nil {
		t.Fatal(err)
	}
	pick := func(name string) *regexp.Regexp {
		m := regexp.MustCompile(`if \(` + name + ` == ""\) ` + name + ` = "([^"]+)"`).FindSubmatch(src)
		if m == nil {
			t.Fatalf("no default %s in bench-gate.awk", name)
		}
		return regexp.MustCompile(string(m[1]))
	}
	family, canary := pick("family"), pick("canary")
	rows := pinnedRelRows([]int{20_000}, false)
	if len(rows) != 24 {
		t.Fatalf("default matrix has %d rows, want 24: %v", len(rows), rows)
	}
	timeGated := 0
	for _, r := range rows {
		name := "PinnedRelPropertyLookup/" + r + "-32" // benchstat drops the Benchmark prefix
		if !family.MatchString(name) {
			t.Fatalf("row %s is outside the allocs-gated family", name)
		}
		if strings.Contains(r, "sharded") || strings.Contains(r, "broad") {
			t.Fatalf("row %s belongs to the measurement matrix only", r)
		}
		if canary.MatchString(name) {
			timeGated++
			if !strings.Contains(r, "1type/sigma") && !strings.Contains(r, "5types/unrelated-x10") {
				t.Fatalf("canary matches the non-canary row %s", r)
			}
		}
	}
	if timeGated != 8 {
		t.Fatalf("%d time-gated rows, want 8", timeGated)
	}
	if full := pinnedRelRows([]int{20_000}, true); len(full) != 54 {
		t.Fatalf("full matrix has %d rows, want 54", len(full))
	}
}
