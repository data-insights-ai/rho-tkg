// Package apiutil holds generic helpers shared by the pkg/graph sub-API
// wrapper packages (nodes, rels, index, tier, stats, …). Before this package
// existed, iterateForEach and cloneStrings were copy-pasted verbatim between
// nodes/api.go, rels/api.go, and index/api.go, and cloneShardInfo/
// cloneCounts in tier/stats were structurally-identical single-type clones
// of the same slice/map-copy shape — a fix landing in one copy risked never
// reaching the others (BACKLOG 7h, lesson A1 class).
package apiutil

import "context"

// CloneSlice returns an independent copy of s, or nil if s is nil — the
// defensive-copy contract every sub-API accessor returning a slice must
// honor (CLAUDE.md "Defensive Copying").
func CloneSlice[T any](s []T) []T {
	if s == nil {
		return nil
	}
	out := make([]T, len(s))
	copy(out, s)
	return out
}

// CloneMap returns an independent copy of m, or nil if m is nil — the map
// counterpart of CloneSlice.
func CloneMap[K comparable, V any](m map[K]V) map[K]V {
	if m == nil {
		return nil
	}
	out := make(map[K]V, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// IterateForEach drives a Go 1.23+ range-over-func Seq2 from an
// already-validated ForEach-shaped streaming primitive (scan): ctx is
// checked once per row (non-blocking, before each yield) and the scan stops
// immediately either on ctx cancellation — yielding (zero, ctx.Err())
// exactly once — or when the consumer's yield returns false (a normal early
// stop, nothing further is yielded). Any error the scan itself returns (and
// did not already surface via a per-row ctx check) is yielded once at the
// end.
func IterateForEach[T any](ctx context.Context, yield func(T, error) bool, scan func(fn func(T) bool) error) {
	var zero T
	if err := ctx.Err(); err != nil {
		yield(zero, err)
		return
	}
	stopped := false
	err := scan(func(v T) bool {
		if cErr := ctx.Err(); cErr != nil {
			stopped = true
			yield(zero, cErr)
			return false
		}
		if !yield(v, nil) {
			stopped = true
			return false
		}
		return true
	})
	if stopped {
		return
	}
	if err != nil {
		yield(zero, err)
	}
}
