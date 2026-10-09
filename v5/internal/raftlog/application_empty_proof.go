package raftlog

import (
	"context"
	"crypto/sha256"
	"math"
)

// ProveNoApplicationData proves that the current active application bank has
// never contained a KV version, including tombstones. It relies on the durable
// retained-record ledger: exactly one root/change/outcome triple is retained at
// every index through App.Through, and application records are never GC'd.
// A future format with envelope compaction or application GC must decline this
// proof until it supplies an equally strong invariant. This is not admission:
// installation must still guard the returned generation, index and image hash.
// Stale/nonempty/canceled/closed views refuse without poisoning the store.
func (v *ApplicationView) ProveNoApplicationData(ctx context.Context) (ApplicationRoot, error) {
	if err := v.lock(ctx); err != nil {
		return ApplicationRoot{}, err
	}
	defer v.unlock()
	s := v.s
	hash := sha256.Sum256(v.image)
	if v.bank != s.activeBank() || v.generation != s.activeGeneration() || v.index != s.meta.Applied || hash != s.meta.ImageHash {
		return ApplicationRoot{}, ErrInvalid
	}
	a := s.meta.App
	if !a.Policy.Enabled() || a.Through != v.index || a.Through == 0 || a.Through > math.MaxUint64/3 || uint64(len(v.image)) != s.meta.ImageBytes || len(v.image) > a.Policy.MaxImageBytes {
		return ApplicationRoot{}, v.fail(ErrCorrupt)
	}
	envelopes := 3 * a.Through
	if a.Records < envelopes {
		return ApplicationRoot{}, v.fail(ErrCorrupt)
	}
	if a.Records != envelopes {
		return ApplicationRoot{}, ErrInvalid
	}
	return ApplicationRoot{Generation: v.generation, Index: v.index, Image: copyApplicationBytes(v.image), ImageHash: hash}, nil
}
