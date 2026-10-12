package graphapply

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"
)

// Fault only the actual Close RPC's write. The real child remains live waiting
// for its input; no application state, child response or exit cause is forged.
type processShutdownWriteFault struct {
	io.WriteCloser
	cause error
	used  bool
}

func (p *processShutdownWriteFault) Write(wire []byte) (int, error) {
	if !p.used {
		p.used = true
		return 0, p.cause
	}
	return p.WriteCloser.Write(wire)
}

func TestGraphProcessShutdownKillPrecedesInputEOFAndGracefulCloseRemainsClean(t *testing.T) {
	for _, mode := range []string{"force", "close-rpc-error", "graceful"} {
		t.Run(mode, func(t *testing.T) {
			child := startGraphProcess(t, t.TempDir(), 1)
			// Complete a real native child request before observing cleanup. The
			// child then blocks on its own stdin; a pre-kill EOF can exit cleanly.
			response := child.call(t, processRequest{op: processStateOp})
			if response.incarnation == ([16]byte{}) || child.cmd.ProcessState != nil {
				t.Fatal("actual native child not live before shutdown")
			}
			var phases []string
			var killResults []error
			child.shutdownObserve = func(phase string, err error) {
				// Observe only. Assertions occur after shutdown and Cmd.Wait have
				// finished, never inside the unconditional cleanup/reap path.
				phases = append(phases, phase)
				if phase == "kill" {
					killResults = append(killResults, err)
				}
			}
			writeFault := errors.New("shutdown Close RPC write fault")
			if mode == "close-rpc-error" {
				child.stdin = &processShutdownWriteFault{WriteCloser: child.stdin, cause: writeFault}
			}
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			err := child.shutdown(ctx, mode == "force")
			cancel()
			if child.cmd.ProcessState == nil || !child.closed || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				t.Fatal("shutdown did not produce a bounded actual reap", err)
			}
			if mode == "graceful" {
				if err != nil || !child.cmd.ProcessState.Success() || len(phases) != 1 || phases[0] != "stdin-close" || len(killResults) != 0 {
					t.Fatal("successful Close RPC became a forced exit", phases, err, child.cmd.ProcessState)
				}
			} else {
				if len(phases) != 2 || phases[0] != "kill" || phases[1] != "stdin-close" || len(killResults) != 1 || killResults[0] != nil {
					t.Fatal("actual kill must precede permitting input EOF", phases, killResults, err)
				}
				if _, found := errors.AsType[*exec.ExitError](err); !found {
					t.Fatal("missing actual killed-child reap", err)
				}
				status, found := child.cmd.ProcessState.Sys().(syscall.WaitStatus)
				if !found || !status.Signaled() || status.Signal() != syscall.SIGKILL {
					t.Fatal("forced shutdown was not actual SIGKILL", child.cmd.ProcessState, err)
				}
				if mode == "close-rpc-error" && !errors.Is(err, writeFault) {
					t.Fatal("error cleanup lost original write sentinel", err)
				}
			}
			if err := child.cmd.Process.Signal(syscall.Signal(0)); !errors.Is(err, os.ErrProcessDone) {
				t.Fatal("reaped child still live", err)
			}
			if _, err := child.stdin.Write([]byte{1}); !errors.Is(err, os.ErrClosed) {
				t.Fatal("shutdown stdin leak", err)
			}
			if _, err := child.stdout.Read(make([]byte, 1)); !errors.Is(err, os.ErrClosed) {
				t.Fatal("shutdown stdout leak", err)
			}
			phaseCount := len(phases)
			repeated := child.shutdown(t.Context(), true)
			if repeated != err || len(phases) != phaseCount {
				t.Fatal("repeated cleanup changed result or repeated kill/close", repeated, err, phases)
			}
		})
	}
}
