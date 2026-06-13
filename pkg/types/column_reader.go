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
