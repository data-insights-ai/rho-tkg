package types

// NodeColumnReader is a random-access point lookup over a cached columnar snapshot
// of one label's nodes (X5 DocValues). It is the read interface the expand-
// aggregation column path uses to fetch a target node's properties by ID without
// materializing the node, exposed across the store boundary so the internal column
// type does not leak.
//
// Row fills the caller's vals/present buffers (length == the snapshot's requested
// property count, in that requested order) for the node id and reports whether id
// is a MEMBER of the snapshot's label. A cleared present[i] means the member lacks
// that (buildable) property — vals[i] is nil, the GetProperty(absent) shape. A
// non-member returns false and the buffers are untouched.
//
// Epoch is the node-mutation epoch the snapshot was built at; the consumer pairs it
// with NodeMutationEpoch()/RelMutationEpoch() for the Gate-2 staleness re-check.
type NodeColumnReader interface {
	Row(id NodeID, vals []any, present []bool) bool
	Epoch() uint64
}

// NodeColumnRowReader is an OPTIONAL extension of NodeColumnReader a snapshot
// may implement (type-assert it): its rows by position, so one snapshot can be
// read in ranges by several workers at once (morsels), each taking
// [lo, hi) of 0..Len()-1, instead of one ForEachDocValues pass from the start.
//
// Len is the snapshot's member count. RowAt fills vals/present for the member
// at position i exactly as Row does for its ID, and returns that ID; i outside
// 0..Len()-1 panics. Every member is at exactly one position; positions run in
// ascending ID order on memory and badger snapshots (a tiered snapshot
// concatenates its shards' rows, each shard ascending). Safe for concurrent
// use; vals/present belong to the caller (one pair per worker).
type NodeColumnRowReader interface {
	NodeColumnReader
	Len() int
	RowAt(i int, vals []any, present []bool) NodeID
}
