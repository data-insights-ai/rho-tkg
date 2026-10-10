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

// The PinnedRelPropertyLookup family is gated on allocs/op (+10 %) ALONE by
// default: on a shared host its time rows swung +42..+178 % between identical
// runs while allocs/op did not move, and the regression it exists to catch —
// the lookup falling back to the history fold — multiplies allocs (1,423 ->
// 106,555). Its time rows are reported, not gated. canary=canary (the
// TIME_CANARY bench-compare.sh passes through) opts its 8 canary rows back
// into the time gate, for quiet runners. The family-* fixtures are real
// benchstat CSV of the family-*.txt files beside them (5 samples each).

func wantGateFailure(t *testing.T, out string, err error, what string) {
	t.Helper()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 1 {
		t.Fatalf("%s passed the gate: err=%v\n%s", what, err, out)
	}
}

// Faulty gate caught: time-gating the family by default (identical code fails
// on a canary row's +200 % swing).
func TestBenchGateFamilyTimeSwingPassesByDefault(t *testing.T) {
	for _, csv := range []string{"testdata/gate/family-canaryswing.csv", "testdata/gate/family-timeswing.csv"} {
		if out, err := runGate(t, csv); err != nil {
			t.Fatalf("%s: a +200 %% time swing on a family row with flat allocs failed the default gate: %v\n%s", csv, err, out)
		}
	}
}

// Faulty gate caught: an opt-in canary that does not re-enable the time gate.
func TestBenchGateFamilyTimeSwingFailsWithTheCanaryOptIn(t *testing.T) {
	out, err := runGate(t, "testdata/gate/family-canaryswing.csv", "canary=canary")
	wantGateFailure(t, out, err, "a +200 % canary time swing with TIME_CANARY=canary")
	if !strings.Contains(out, "REGRESSION PinnedRelPropertyLookup/badger/20000/5types/unrelated-x10/matches200/pinned-32") {
		t.Fatalf("gate output does not name the canary row:\n%s", out)
	}
	// Only canary rows: the +200 % swing of the non-canary nochurn row is not gated.
	if out, err := runGate(t, "testdata/gate/family-timeswing.csv", "canary=canary"); err != nil {
		t.Fatalf("the opt-in canary time-gated a non-canary row: %v\n%s", err, out)
	}
	// An explicit regex works the same as the keyword.
	out, err = runGate(t, "testdata/gate/family-canary-time.csv", "canary=unrelated-x10/matches200/")
	wantGateFailure(t, out, err, "a +60 % time regression matching an explicit TIME_CANARY regex")
}

// Faulty gate caught: no allocs gate (the fold fallback keeps the time of a
// small fixture under the threshold and passes).
func TestBenchGateFamilyAllocsRegressionFails(t *testing.T) {
	out, err := runGate(t, "testdata/gate/family-allocs.csv")
	wantGateFailure(t, out, err, "allocs 1,423 -> 106,555 on a family row")
	if !strings.Contains(out, "ALLOCS REGRESSION PinnedRelPropertyLookup/memory/20000/1type/sigma/matches200/pinned-32") {
		t.Fatalf("gate output does not name the allocs regression:\n%s", out)
	}
	if strings.Contains(out, "RelPropertyLookup10k") || strings.Contains(out, "current-32") {
		t.Fatalf("gate flagged a row whose allocs did not move:\n%s", out)
	}
}

// Faulty gate caught: dropping the time gate for every row instead of the
// family (a non-family row regressing +60 % must fail), or a geomean still
// computed over the family's swinging rows.
func TestBenchGateNonFamilyTimeRegressionFails(t *testing.T) {
	out, err := runGate(t, "testdata/gate/family-nonfamily-time.csv")
	wantGateFailure(t, out, err, "a +60 % time regression on a non-family row")
	if !strings.Contains(out, "REGRESSION RelPropertyLookup10k/memory/indexed-32") {
		t.Fatalf("gate output does not name the non-family row:\n%s", out)
	}
	if strings.Contains(out, "PinnedRelPropertyLookup") {
		t.Fatalf("gate flagged a family row on time:\n%s", out)
	}
	if out, err := runGate(t, "testdata/gate/family-canaryswing.csv"); err != nil || strings.Contains(out, "geomean") {
		t.Fatalf("the geomean included the family's time rows: %v\n%s", err, out)
	}
}

// The first fixture's regression sits on a family canary row: reported by the
// opt-in canary, not by the default gate.
func TestBenchGateStillCatchesTimeRegression(t *testing.T) {
	out, err := runGate(t, "testdata/gate/time-regression.csv", "canary=canary")
	wantGateFailure(t, out, err, "a +43 % sec/op regression on a canary row with TIME_CANARY=canary")
	if !strings.Contains(out, "REGRESSION PinnedRelPropertyLookup/memory/20000/1type/sigma/matches200/current-32") {
		t.Fatalf("gate output does not name the regressed benchmark:\n%s", out)
	}
	if strings.Contains(out, "matches200/pinned-32") {
		t.Fatalf("gate flagged the unchanged benchmark (its build-ms moved):\n%s", out)
	}
}

// family=none switches the allocs gate off and time-gates every row again
// (the escape hatch bench-compare.sh exposes as ALLOCS_GATE_FAMILY=none).
func TestBenchGateFamilyNoneTimeGatesEveryRow(t *testing.T) {
	out, err := runGate(t, "testdata/gate/family-timeswing.csv", "family=none")
	wantGateFailure(t, out, err, "family=none with a +200 % row")
	if !strings.Contains(out, "REGRESSION PinnedRelPropertyLookup/memory/20000/1type/nochurn/matches200/pinned-32") {
		t.Fatalf("family=none must time-gate the family row:\n%s", out)
	}
	if out, err := runGate(t, "testdata/gate/family-allocs.csv", "family=none"); err != nil {
		t.Fatalf("family=none must not allocs-gate: %v\n%s", err, out)
	}
}

// TestPinnedRelGateMatrixMatchesTheGate ties the benchmark's default matrix to
// the gate (regexes read from bench-gate.awk itself): 24 allocs-gated rows, no
// time-gated one by default, exactly 8 with the documented opt-in canary, no
// sharded and no broad row. Faulty set-ups caught: a family regex that misses
// the benchmark, a documented canary that matches nothing or a non-canary
// row, sharded or broad back in the default run.
func TestPinnedRelGateMatrixMatchesTheGate(t *testing.T) {
	src, err := os.ReadFile("bench-gate.awk")
	if err != nil {
		t.Fatal(err)
	}
	pick := func(pattern string) *regexp.Regexp {
		m := regexp.MustCompile(pattern).FindSubmatch(src)
		if m == nil {
			t.Fatalf("bench-gate.awk has no %s", pattern)
		}
		return regexp.MustCompile(string(m[1]))
	}
	family := pick(`if \(family == ""\) family = "([^"]+)"`)
	canary := pick(`documented_canary = "([^"]+)"`)
	if regexp.MustCompile(`if \(canary == ""\) canary = "`).Match(src) {
		t.Fatal("bench-gate.awk time-gates the family by default")
	}
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
				t.Fatalf("the documented canary matches the non-canary row %s", r)
			}
		}
	}
	if timeGated != 8 {
		t.Fatalf("the documented canary time-gates %d rows, want 8", timeGated)
	}
	if full := pinnedRelRows([]int{20_000}, true); len(full) != 54 {
		t.Fatalf("full matrix has %d rows, want 54", len(full))
	}
}

// The LatestStamps family (backlog 30) joins the allocs-gated family: its door
// is 0 allocs/op and the regression it exists to catch — the door falling back
// to the History fold — costs 230,388 allocs/op at 10,000 versions. A zero
// baseline cannot be gated in percent, so a family row with 0 allocs/op fails
// as soon as it allocates. The latest-* fixtures are real benchstat CSV of the
// latest-*.txt files beside them (5 samples each).
//
// Faulty gates caught: a family regex without LatestStamps (the row is only
// time-gated, so the +74,000,000 % row fails on time, not allocs — and passes
// when the fold is fast enough), and a percent rule that skips a 0 baseline.
func TestBenchGateLatestStampsZeroAllocsRegressionFails(t *testing.T) {
	out, err := runGate(t, "testdata/gate/latest-allocs.csv", "thr=1000000000")
	wantGateFailure(t, out, err, "allocs 0 -> 230,388 on a LatestStamps row")
	if !strings.Contains(out, "ALLOCS REGRESSION LatestStamps/badger/versions=10000") {
		t.Fatalf("gate output does not name the allocs regression:\n%s", out)
	}
	if strings.Contains(out, "LatestStamps/memory") {
		t.Fatalf("gate flagged a row whose allocs did not move:\n%s", out)
	}
}

// Faulty gate caught: time-gating the LatestStamps family (identical code
// swings on shared hosts; its 30-80 ns rows swing more than most).
func TestBenchGateLatestStampsTimeSwingPassesByDefault(t *testing.T) {
	if out, err := runGate(t, "testdata/gate/latest-timeswing.csv"); err != nil {
		t.Fatalf("a +200 %% time swing on LatestStamps rows with flat allocs failed the default gate: %v\n%s", err, out)
	}
}
