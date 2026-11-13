package store

// PropertyMemberWork counts one diagnostic membership call, excluding callback work.
// Counts describe logical work and container cardinalities, never physical I/O or heap bytes.
type PropertyMemberWork struct {
	// IteratorSteps counts loop entries before filtering, including older MVCC versions
	// and the first longer-prefix or beyond-range boundary item examined.
	IteratorSteps uint64
	// IteratorSeeks counts iterator seeks of a time-ordered windowed read
	// (RELDISKWIN option B: one per axis read); 0 for the other walks.
	IteratorSeeks uint64
	// OverlayOpsInspected counts flushing then pending operations enumerated: prefix
	// walks visit matching prefixes only; range walks inspect all buffered operations.
	OverlayOpsInspected uint64
	// OverlayOpsMatched counts inspected operations admitted to the overlay after
	// range filtering; repeated operations on one key count separately.
	OverlayOpsMatched uint64
	// OverlaySetsEmitted counts final overlay set entries passed to the member fold.
	OverlaySetsEmitted uint64
	// DiskEntriesEmitted counts disk entries passed to the fold after version,
	// deletion, boundary and overlay filtering; several entries may name one entity.
	DiskEntriesEmitted uint64
	// MembersEmitted counts callback invocations, including the invocation returning
	// false; available on RAM paths too. Nested callback operations are excluded.
	MembersEmitted uint64
	// OverlaySetsPeak is the maximum number of distinct keys in the overlay set map.
	OverlaySetsPeak uint64
	// OverlayDeletesPeak is the maximum delete-map cardinality; a later set can
	// leave a delete-map key present, so this is not the number of final tombstones.
	OverlayDeletesPeak uint64
	// FoldMembersPeak is the largest number of distinct entity IDs held by the fold.
	// The fold only grows, so its final cardinality is also its peak, including on error.
	FoldMembersPeak uint64
	// MemberSliceLen is the materialized folded-member slice length before an empty
	// or reversed valid window clears it; early callback stops do not reduce it.
	MemberSliceLen uint64
	// MemberSliceCap is that slice's capacity before window filtering or callbacks;
	// it counts member slots, not allocated bytes or memory retained after return.
	MemberSliceCap uint64
	// Supported means disk work counters apply. On RAM paths they are unsupported,
	// while MembersEmitted, Completed and CallbackStopped still report real behavior.
	Supported bool
	// Completed means the method returned without error, including a callback stop.
	// An error leaves false and preserves counters for work done before that error.
	Completed bool
	// CallbackStopped means a callback returned false; false also covers no callbacks.
	CallbackStopped bool
}
