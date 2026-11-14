// Command race runs go test -race -short ./... under the host race lock
// shared with sigma (make race).
package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
)

func main() {
	if top, err := exec.Command("git", "rev-parse", "--show-toplevel").Output(); err == nil {
		_ = os.Chdir(strings.TrimSpace(string(top)))
	}
	lock(raceLock())
	cmd := exec.Command("go", "test", "-race", "-short", "-timeout", "30m", "./...") // #nosec G204 -- fixed flags
	cmd.Env = append(os.Environ(), "GOFLAGS=-trimpath")
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Run(); err != nil {
		os.Exit(1)
	}
}

func lock(path string) {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		fail(err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600) // #nosec G304 -- a lock file path
	if err != nil {
		fail(err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); errors.Is(err, syscall.EWOULDBLOCK) {
		fmt.Printf("race: waiting for %s\n", path)
		if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
			fail(err)
		}
	} else if err != nil {
		fail(err)
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "race:", err)
	os.Exit(1)
}

func raceLock() string {
	home, err := os.UserHomeDir()
	if err != nil {
		fail(err)
	}
	if v := os.Getenv("RACE_LOCK"); v != "" {
		return v
	}
	return filepath.Join(home, ".cache", "race-gate.lock")
}