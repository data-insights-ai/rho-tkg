//go:build !race

package graph_test

// isRaceEnabled reports whether the test binary was built with -race. The
// randomized differentials run fewer seeds under -short -race (about 10x
// slower per op) so the race run on a 4-vCPU CI runner stays well inside Go's
// 10 min per-package timeout; the full sizes stay behind !testing.Short().
func isRaceEnabled() bool { return false }
