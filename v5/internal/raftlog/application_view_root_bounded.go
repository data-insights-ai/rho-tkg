package raftlog

import (
	"context"
	"crypto/sha256"
)

// RootBounded returns this retained view's genuine local root with an exact-cap
// owned image. maxImageBytes bounds only image bytes, not the fixed result's
// metadata or heap/RSS; zero permits an empty opaque image. Size refusal occurs
// before hashing or copying and changes no pins, accounting or poison state.
// Lifetime/cancellation/storage errors retain the existing view lock order.
// The root supplies no readiness, certified-cut, lease or installation authority.
func (v *ApplicationView) RootBounded(ctx context.Context, maxImageBytes int) (ApplicationRoot, error) {
	if v == nil || v.s == nil || v.index == 0 || ctx == nil || maxImageBytes < 0 {
		return ApplicationRoot{}, ErrInvalid
	}
	if err := v.lock(ctx); err != nil {
		return ApplicationRoot{}, err
	}
	defer v.unlock()
	if len(v.image) > maxImageBytes {
		return ApplicationRoot{}, ErrLimit
	}
	return ApplicationRoot{Generation: v.generation, Index: v.index, Image: copyApplicationBytes(v.image), ImageHash: sha256.Sum256(v.image)}, nil
}
