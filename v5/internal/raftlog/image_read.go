package raftlog

import (
	"errors"

	"github.com/cockroachdb/pebble/v2"
)

// storedReadFailure separates unavailable evidence from a verified integrity
// diagnosis. Missing mandatory records and backend-marked corruption
// retain ErrCorrupt; operational failures preserve their original cause.
func storedReadFailure(err error) error {
	if errors.Is(err, pebble.ErrNotFound) || pebble.IsCorruptionError(err) {
		return errors.Join(ErrCorrupt, err)
	}
	return err
}
