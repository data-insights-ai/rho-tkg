//go:build windows || plan9 || js || wasip1 || aix

package tiered

import "errors"

// lockShardDir is not built here: badger locks a shard differently on these
// platforms, so an offline move could not tell an open store.
func lockShardDir(string) (func(), error) {
	return nil, errors.New("an offline move needs a unix directory lock")
}
