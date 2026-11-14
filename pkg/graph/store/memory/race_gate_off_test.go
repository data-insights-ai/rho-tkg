//go:build !race

package memory

// raceEnabled reports whether the test binary was built with -race; the
// -short long-tier gates keep running under -race.
const raceEnabled = false
