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

// ColumnValue is one typed cell of a column snapshot's row
// (NodeColumnValueReader.RowValues). Kind is 0 where the member lacks the
// property (Row's cleared present bit), else ColumnVectorInt (Int),
// ColumnVectorFloat (Flt) or ColumnVectorString (Str, the snapshot's
// dictionary term, shared and read-only); the other fields are zero.
type ColumnValue struct {
	Kind ColumnVectorKind
	Int  int64
	Flt  float64
	Str  string
}

// NodeColumnValueReader is an OPTIONAL extension of NodeColumnReader a
// snapshot may implement (type-assert it; memory, badger, tiered and sharded
// snapshots do): Row's lookup with each value typed instead of boxed, so a
// read allocates nothing, the first one included (Row boxes a string
// column's whole dictionary on its first read).
//
// RowValues fills vals (length == the snapshot's requested property count,
// in that order) for the node id and reports whether id is a member; a
// non-member returns false and vals is untouched. Each value is the one Row
// hands out (an int64, a float64 or a string), typed. Safe for concurrent
// use; vals belongs to the caller.
type NodeColumnValueReader interface {
	NodeColumnReader
	RowValues(id NodeID, vals []ColumnValue) bool
}

// RowValuesFromRow is RowValues over a reader that has only Row: its boxed
// values converted (a composite snapshot's part that does not implement
// NodeColumnValueReader). It allocates Row's buffers per call.
func RowValuesFromRow(r NodeColumnReader, id NodeID, vals []ColumnValue) bool {
	boxed, present := make([]any, len(vals)), make([]bool, len(vals))
	if !r.Row(id, boxed, present) {
		return false
	}
	for i, v := range boxed {
		vals[i] = ColumnValue{}
		if !present[i] {
			continue
		}
		switch x := v.(type) {
		case int64:
			vals[i] = ColumnValue{Kind: ColumnVectorInt, Int: x}
		case float64:
			vals[i] = ColumnValue{Kind: ColumnVectorFloat, Flt: x}
		case string:
			vals[i] = ColumnValue{Kind: ColumnVectorString, Str: x}
		}
	}
	return true
}
