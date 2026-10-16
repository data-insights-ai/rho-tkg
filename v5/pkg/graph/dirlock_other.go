//go:build !unix

package graph

import "fmt"

const lockFileName = "rho-v5.lock"

type dirLock struct{}

// lockDir refuses: without a directory lock a second Open could corrupt the store.
func lockDir(string) (*dirLock, error) {
	return nil, fmt.Errorf("%w: directory graphs need a unix platform (no directory lock)", ErrInvalidConfig)
}

func (*dirLock) unlock() {}
