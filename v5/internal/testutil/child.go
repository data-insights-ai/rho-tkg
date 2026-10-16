// Package testutil holds the shared helpers for tests that launch the current
// test binary as a child process (crash and fault seams) or run the Python
// substrate checkers.
package testutil

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"
)

// Result is the classified outcome of one child process.
type Result struct {
	Output   []byte          // combined stdout and stderr
	Err      error           // error from Run, nil on exit code 0
	Exit     *exec.ExitError // non-nil when the child ran and ended non-zero or by signal
	ExitCode int             // 0 clean, n for exit(n), -1 when ended by a signal or never ran
	Signal   syscall.Signal  // terminating signal, 0 when the child exited normally
	TimedOut bool            // the timeout expired (the child was killed by the supervisor)
	Reaped   bool            // the child process was started and waited for
	PID      int
}

// Contains reports whether the combined output contains s.
func (r Result) Contains(s string) bool { return bytes.Contains(r.Output, []byte(s)) }

// Killed reports whether the child ended by SIGKILL.
func (r Result) Killed() bool { return r.Signal == syscall.SIGKILL }

// RunChild runs the current test binary with -test.run=^testName$ (plus
// extraArgs) and env appended to the parent environment. The child is killed
// when timeout expires or the test context ends.
func RunChild(t testing.TB, testName string, env []string, timeout time.Duration, extraArgs ...string) Result {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	args := append([]string{"-test.run=^" + testName + "$"}, extraArgs...)
	return run(ctx, exec.CommandContext(ctx, os.Args[0], args...), append(os.Environ(), env...)) // #nosec G204 -- current test binary and a fixed selector.
}

// RunChecker runs tools/v5-substrate/<script> on path with python3 -B.
func RunChecker(t testing.TB, script, path string, timeout time.Duration) Result {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("testutil: cannot locate the repository")
	}
	checker := filepath.Join(filepath.Dir(file), "..", "..", "..", "tools", "v5-substrate", script)
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	return run(ctx, exec.CommandContext(ctx, "python3", "-B", checker, path), os.Environ()) // #nosec G204 -- fixed interpreter, repository checker, test-owned path.
}

func run(ctx context.Context, cmd *exec.Cmd, env []string) Result {
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	r := Result{Output: out, Err: err, ExitCode: cmd.ProcessState.ExitCode(), TimedOut: ctx.Err() != nil, Reaped: cmd.ProcessState != nil}
	if cmd.Process != nil {
		r.PID = cmd.Process.Pid
	}
	if exit, ok := errors.AsType[*exec.ExitError](err); ok {
		r.Exit = exit
		if ws, ok := exit.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
			r.Signal = ws.Signal()
		}
	}
	return r
}
