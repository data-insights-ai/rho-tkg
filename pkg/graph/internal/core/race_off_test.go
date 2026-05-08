//go:build !race

// Cannot be collapsed with race_on_test.go via runtime.RaceEnabled — that
// symbol is part of the runtime/race build-tagged package, not the public
// runtime package, so it is not reachable from external code without using
// the same //go:build race / !race tags this pair already employs.

package core

// isRaceEnabled reports whether the binary was built with -race. Used to
// skip allocator-sensitive RAM-bound benchmarks under the race detector,
// where added shadow-memory bookkeeping perturbs heap measurements
// enough to defeat micro-scale comparisons.
func isRaceEnabled() bool { return false }
