//go:build !race

package memory_test

// raceEnabled reports whether the test binary was built with -race. The
// runtime does not export it, hence the build-tagged pair. Heap-accounting
// gates skip under the race detector, whose shadow memory distorts them.
const raceEnabled = false
