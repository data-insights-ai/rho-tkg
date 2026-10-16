package types

// ColumnVectorKind is the storage class of one column of a ColumnChunk.
type ColumnVectorKind uint8

// Column vector kinds. A column of no present value is ColumnVectorInt (the
// snapshot's numeric rule: an all-absent column is numeric).
const (
	// ColumnVectorInt: every present value is an int64, in Ints.
	ColumnVectorInt ColumnVectorKind = iota + 1
	// ColumnVectorFloat: every present value is a float64, in Flts.
	ColumnVectorFloat
	// ColumnVectorNumeric: integers and floats mixed; IsFloat selects Flts
	// (true) or Ints (false) per row.
	ColumnVectorNumeric
	// ColumnVectorString: Dict[Codes[i]] is the value.
	ColumnVectorString
	// ColumnVectorMixed: strings and numbers mixed (a composite snapshot,
	// tiered or sharded, whose column holds strings on one part and numbers
	// on another). Tags[i] is row i's own kind, ColumnVectorInt (Ints[i]),
	// ColumnVectorFloat (Flts[i]) or ColumnVectorString (Dict[Codes[i]]);
	// an absent row (Present[i] false) has tag 0 and zero slots.
	ColumnVectorMixed
)

// ColumnVector is one column of a ColumnChunk, one slot per row of the
// chunk. A row whose Present is false lacks the property (RowAt's nil); its
// slots are zero. Only the slices of the column's kind are filled: Ints for
// Int, Flts for Float, Ints, Flts and IsFloat for Numeric, Codes for
// String, Tags, Ints, Flts and Codes for Mixed; the others are left empty
// (their capacity kept).
//
// Dict is the snapshot's dictionary of a String column, shared by every
// chunk of the snapshot and every goroutine reading it: READ-ONLY. A code
// names the same term in every chunk of one snapshot (a consumer interns
// the dictionary once per snapshot and maps codes through it).
type ColumnVector struct {
	Kind    ColumnVectorKind
	Present []bool
	Ints    []int64
	Flts    []float64
	IsFloat []bool
	Codes   []uint32
	Tags    []ColumnVectorKind
	Dict    []string
}

// ColumnChunk is a range of a column snapshot's rows as typed vectors
// (NodeColumnChunkReader.ReadChunk): IDs[i] is the member at position
// from+i, Cols[c] the snapshot's c-th requested key. The reader reuses the
// slices' capacity, so a chunk read again allocates nothing; the caller
// owns the chunk (one per goroutine).
//
// Ordinals is parallel to IDs when the snapshot comes from an ordinal door
// (nodes.API.DocValuesSnapshotOrdinal / DocValuesSnapshotMultiOrdinal):
// each member's dense node ordinal, the one ForEachDocValuesOrdinal hands
// out. Every other snapshot leaves it empty (capacity kept).
type ColumnChunk struct {
	IDs      []NodeID
	Ordinals []uint32
	Cols     []ColumnVector
}

// NodeColumnChunkReader is an OPTIONAL extension of NodeColumnRowReader a
// snapshot may implement (type-assert it; memory, badger, tiered and
// sharded snapshots do): the snapshot's rows by position as typed vectors
// instead of one boxed row at a time (sigma-tkgd round 9, W10's typed chunk
// scan).
//
// ReadChunk fills chunk with the members at positions [from, to): the same
// members, order and values RowAt hands out for each position (an int64 in
// Ints, a float64 in Flts, a string as Dict[Codes]), resizing chunk's slices
// to to-from rows and len(Cols) to the snapshot's requested key count.
// 0 <= from <= to <= Len() or it panics. Safe for concurrent use, one chunk
// per goroutine. The snapshot is immutable: a write after it changes no
// chunk (Epoch is the staleness check).
type NodeColumnChunkReader interface {
	NodeColumnRowReader
	ReadChunk(from, to int, chunk *ColumnChunk)
}
