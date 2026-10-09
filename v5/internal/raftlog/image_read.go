package raftlog

import (
	"errors"

	"github.com/cockroachdb/pebble/v2"
)

// imageReadFailure distinguishes unavailable image bytes from a verified
// integrity diagnosis. Missing mandatory images and backend-marked corruption
// retain ErrCorrupt; operational failures preserve their original cause.
func imageReadFailure(err error) error {
	if errors.Is(err, pebble.ErrNotFound) || pebble.IsCorruptionError(err) {
		return errors.Join(ErrCorrupt, err)
	}
	return err
}
