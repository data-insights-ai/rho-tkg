package testutil

import (
	"bytes"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestRunChildHelper is the child body; it only acts when RHO_TESTUTIL_MODE is set.
func TestRunChildHelper(t *testing.T) {
	switch os.Getenv("RHO_TESTUTIL_MODE") {
	case "":
		return
	case "ok":
		_, _ = os.Stdout.WriteString("HELLO " + os.Getenv("RHO_TESTUTIL_VALUE") + "\n")
	case "fail":
		t.Fatal("child failed on purpose")
	case "exit2":
		_, _ = os.Stdout.WriteString("BEFORE_EXIT\n")
		os.Exit(2)
	case "kill":
		_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
		time.Sleep(time.Minute)
	case "sleep":
		time.Sleep(time.Minute)
	}
}

func TestRunChildReportsKillAndExitCode(t *testing.T) {
	cases := []struct {
		name, mode         string
		code               int
		signal             syscall.Signal
		timedOut, hasExit  bool
		contains, excludes string
	}{
		{"clean", "ok", 0, 0, false, false, "HELLO v1", "BEFORE_EXIT"},
		{"exit1", "fail", 1, 0, false, true, "child failed on purpose", "HELLO"},
		{"exit2", "exit2", 2, 0, false, true, "BEFORE_EXIT", "HELLO"},
		{"sigkill", "kill", -1, syscall.SIGKILL, false, true, "", "HELLO"},
		{"timeout", "sleep", -1, syscall.SIGKILL, true, true, "", "HELLO"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			start := time.Now()
			r := RunChild(t, "TestRunChildHelper", []string{"RHO_TESTUTIL_MODE=" + c.mode, "RHO_TESTUTIL_VALUE=v1"}, 3*time.Second)
			if c.timedOut && time.Since(start) > 10*time.Second {
				t.Fatalf("timeout did not kill the child promptly: %v", time.Since(start))
			}
			if r.ExitCode != c.code || r.Signal != c.signal || r.TimedOut != c.timedOut || (r.Exit != nil) != c.hasExit || !r.Reaped {
				t.Fatalf("classification: %+v", r)
			}
			if c.contains != "" && !r.Contains(c.contains) || r.Contains(c.excludes) {
				t.Fatalf("output: %q", r.Output)
			}
			if r.Killed() != (c.signal == syscall.SIGKILL) {
				t.Fatalf("Killed: %+v", r)
			}
		})
	}
}

func TestRunChildPassesEnvAndInheritsParent(t *testing.T) {
	t.Setenv("RHO_TESTUTIL_INHERITED", "yes")
	r := RunChild(t, "TestRunChildHelper", []string{"RHO_TESTUTIL_MODE=ok", "RHO_TESTUTIL_VALUE=a b=c"}, 5*time.Second)
	if r.Err != nil || !r.Contains("HELLO a b=c") {
		t.Fatalf("env not passed: %+v %q", r, r.Output)
	}
	// parent env still visible to the child (os.Environ() is the base)
	if !bytes.Contains([]byte(strings.Join(os.Environ(), "\n")), []byte("RHO_TESTUTIL_INHERITED=yes")) {
		t.Fatal("setup")
	}
}

func TestRunChildExtraArgsAndMissingTestName(t *testing.T) {
	r := RunChild(t, "TestNoSuchChildAnywhere", []string{"RHO_TESTUTIL_MODE=ok"}, 5*time.Second, "-test.count=1")
	if r.Err != nil || r.Contains("HELLO") || !r.Contains("no tests to run") {
		t.Fatalf("missing test name must run nothing: %+v %q", r, r.Output)
	}
	r = RunChild(t, "TestRunChildHelper", []string{"RHO_TESTUTIL_MODE=ok"}, 5*time.Second, "-test.count=1", "-test.v")
	if !r.Contains("--- PASS: TestRunChildHelper") {
		t.Fatalf("extra args not passed: %q", r.Output)
	}
}

func TestRunCheckerExitAndMissingScript(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("no python3")
	}
	r := RunChecker(t, "no_such_checker.py", "x", 5*time.Second)
	if r.Err == nil || r.ExitCode == 0 {
		t.Fatalf("missing checker must fail: %+v", r)
	}
}
