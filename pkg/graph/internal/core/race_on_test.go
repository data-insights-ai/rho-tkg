//go:build race

// Cannot be collapsed with race_off_test.go — see the matching note there.

package core

func isRaceEnabled() bool { return true }
