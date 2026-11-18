// Package testtier marks tests that are too slow for the development run.
//
// `go test -short` is the development run and must stay fast. A test that
// takes a noticeable fraction of a second calls Slow first: it then skips
// under -short, unless RHO_GATE is set. `make check`, `make test` and
// `make race` set RHO_GATE=1, so the gate runs every such test.
package testtier

import (
	"os"
	"testing"
)

// Slow skips t in the development run (`go test -short` without RHO_GATE).
func Slow(t testing.TB) {
	t.Helper()
	if testing.Short() && os.Getenv("RHO_GATE") == "" {
		t.Skip("slow: runs in make check (RHO_GATE=1)")
	}
}
