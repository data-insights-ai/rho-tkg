//go:build race

package memory_test

// raceEnabled reports whether the test binary was built with -race (see
// race_off_test.go); heap-accounting gates skip under it.
const raceEnabled = true
