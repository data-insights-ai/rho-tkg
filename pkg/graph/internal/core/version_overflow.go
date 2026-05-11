package core

import "math"

func nextEntityVersion(current uint32) (uint32, error) {
	if current == math.MaxUint32 {
		return 0, ErrVersionOverflow
	}
	return current + 1, nil
}
