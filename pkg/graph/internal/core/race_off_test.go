//go:build !race

package core

// isRaceEnabled reports whether the binary was built with -race. Used to
// skip allocator-sensitive RAM-bound benchmarks under the race detector,
// where added shadow-memory bookkeeping perturbs heap measurements
// enough to defeat micro-scale comparisons.
func isRaceEnabled() bool { return false }
