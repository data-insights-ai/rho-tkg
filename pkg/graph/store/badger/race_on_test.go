//go:build race

package badger

// raceEnabled reports whether the test binary was built with -race; timing
// guards skip under it (the detector scales iterator steps and seeks unevenly).
const raceEnabled = true
