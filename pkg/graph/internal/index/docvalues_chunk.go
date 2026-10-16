package index

import (
	"sync"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Typed chunk reads of a column snapshot by position (sigma-tkgd round 9,
// types.NodeColumnChunkReader): the snapshot's typed storage copied into the
// caller's vectors, no boxing, no allocation once the chunk has capacity. A
// composite snapshot (tiered shards, sharded slots) reads its parts through
// ChunkConcat, which unifies the parts' column kinds and dictionaries (a
// column of strings on one part and numbers on another reads as
// types.ColumnVectorMixed, round 10). An OrdinalSnapshot (round 10) also
// hands out the members' node ordinals.

var (
	_ types.NodeColumnChunkReader = (*PointSnapshot[types.NodeID])(nil)
	_ types.NodeColumnChunkReader = (*OrdinalSnapshot)(nil)
)

// chunkKind is the vector kind of a built column.
func (c *docColumn) chunkKind() types.ColumnVectorKind {
	switch {
	case c.typ == ColString:
		return types.ColumnVectorString
	case c.isFloat != nil:
		return types.ColumnVectorNumeric
	case c.flts != nil && c.ints == nil:
		return types.ColumnVectorFloat
	default:
		return types.ColumnVectorInt
	}
}

// anyPresent reports whether some row holds the property.
func (c *docColumn) anyPresent() bool {
	for _, w := range c.present {
		if w != 0 {
			return true
		}
	}
	return false
}

func resized[E any](s []E, n int) []E {
	if cap(s) < n {
		return make([]E, n)
	}
	return s[:n]
}

// sizeChunk sets chunk to n rows and width columns, reusing capacity.
func sizeChunk(chunk *types.ColumnChunk, n, width int) {
	chunk.IDs = resized(chunk.IDs, n)
	if cap(chunk.Cols) < width {
		cols := make([]types.ColumnVector, width)
		copy(cols, chunk.Cols)
		chunk.Cols = cols
	}
	chunk.Cols = chunk.Cols[:width]
}

// sizeVector sets v to kind and n rows, the kind's slices sized, the others
// emptied (capacity kept).
func sizeVector(v *types.ColumnVector, kind types.ColumnVectorKind, n int) {
	v.Kind = kind
	v.Present = resized(v.Present, n)
	v.Ints, v.Flts, v.IsFloat, v.Codes, v.Tags = v.Ints[:0], v.Flts[:0], v.IsFloat[:0], v.Codes[:0], v.Tags[:0]
	v.Dict = nil
	switch kind {
	case types.ColumnVectorInt:
		v.Ints = resized(v.Ints, n)
	case types.ColumnVectorFloat:
		v.Flts = resized(v.Flts, n)
	case types.ColumnVectorNumeric:
		v.Ints, v.Flts, v.IsFloat = resized(v.Ints, n), resized(v.Flts, n), resized(v.IsFloat, n)
	case types.ColumnVectorString:
		v.Codes = resized(v.Codes, n)
	case types.ColumnVectorMixed:
		v.Ints, v.Flts = resized(v.Ints, n), resized(v.Flts, n)
		v.Codes, v.Tags = resized(v.Codes, n), resized(v.Tags, n)
	}
}

// fillVector writes rows [from, to) of c into v at at, v sized for kind
// (c's own kind or a wider one: Numeric over Int or Float, Mixed over any).
// remap, when non-nil, maps c's dictionary codes to v's.
func (c *docColumn) fillVector(v *types.ColumnVector, at, from, to int, remap []uint32) {
	n := to - from
	present := v.Present[at : at+n]
	for i := 0; i < n; { // a word of the bitset at a time
		bit := from + i
		w, k := c.present[bit>>6]>>uint(bit&63), min(64-bit&63, n-i)
		for j := range present[i : i+k] {
			present[i+j] = w&1 != 0
			w >>= 1
		}
		i += k
	}
	switch v.Kind {
	case types.ColumnVectorMixed:
		c.fillMixed(v, at, from, present, remap)
	case types.ColumnVectorString:
		codes := v.Codes[at : at+n]
		if c.typ != ColString { // an all-absent part: zero slots
			clear(codes)
			return
		}
		src := c.codes[from:to]
		if remap == nil {
			copy(codes, src) // an absent row's code is 0 by construction
			return
		}
		for i, code := range src {
			codes[i] = remap[code]
		}
		for i, p := range present {
			if !p {
				codes[i] = 0
			}
		}
	case types.ColumnVectorInt:
		ints := v.Ints[at : at+n]
		if c.ints == nil {
			clear(ints)
		} else {
			copy(ints, c.ints[from:to])
		}
	case types.ColumnVectorFloat:
		flts := v.Flts[at : at+n]
		if c.flts == nil {
			clear(flts)
		} else {
			copy(flts, c.flts[from:to])
		}
	case types.ColumnVectorNumeric:
		ints, flts, isFloat := v.Ints[at:at+n], v.Flts[at:at+n], v.IsFloat[at:at+n]
		if c.ints == nil {
			clear(ints)
		} else {
			copy(ints, c.ints[from:to])
		}
		if c.flts == nil {
			clear(flts)
		} else {
			copy(flts, c.flts[from:to])
		}
		for i := range isFloat {
			switch {
			case c.isFloat != nil:
				isFloat[i] = c.isFloat.get(from + i)
			default:
				isFloat[i] = c.flts != nil && c.ints == nil
			}
			if !present[i] {
				isFloat[i] = false
			}
		}
		// A uniform part keeps zero in the half it never built; a mixed
		// part's unused half is zero by construction (buildNumericColumn).
	}
}

// fillMixed writes the rows of c (present as given) into the Mixed vector
// v at at: each present row tagged with its own kind, a string's code
// through remap when non-nil.
func (c *docColumn) fillMixed(v *types.ColumnVector, at, from int, present []bool, remap []uint32) {
	n := len(present)
	ints, flts, codes, tags := v.Ints[at:at+n], v.Flts[at:at+n], v.Codes[at:at+n], v.Tags[at:at+n]
	clear(ints)
	clear(flts)
	clear(codes)
	clear(tags)
	for i, p := range present {
		if !p {
			continue
		}
		row := from + i
		if c.typ == ColString {
			code := c.codes[row]
			if remap != nil {
				code = remap[code]
			}
			codes[i], tags[i] = code, types.ColumnVectorString
			continue
		}
		if iv, fv, isFloat := c.numericAt(row); isFloat {
			flts[i], tags[i] = fv, types.ColumnVectorFloat
		} else {
			ints[i], tags[i] = iv, types.ColumnVectorInt
		}
	}
}

// ReadChunk fills chunk with the members at positions [from, to)
// (types.NodeColumnChunkReader); bounds outside 0 <= from <= to <= Len()
// panic. chunk.Ordinals is left empty.
func (s *PointSnapshot[T]) ReadChunk(from, to int, chunk *types.ColumnChunk) {
	if from < 0 || to < from || to > len(s.l.ids) {
		panic("column snapshot: chunk out of range")
	}
	n := to - from
	sizeChunk(chunk, n, len(s.cols))
	chunk.Ordinals = chunk.Ordinals[:0]
	for i, id := range s.l.ids[from:to] {
		chunk.IDs[i] = types.NodeID(id.SnowflakeID())
	}
	for k, c := range s.cols {
		v := &chunk.Cols[k]
		sizeVector(v, c.chunkKind(), n)
		if c.typ == ColString {
			v.Dict = c.dict
		}
		c.fillVector(v, 0, from, to, nil)
	}
}

// OrdinalSnapshot is a PointSnapshot whose chunks also carry the members'
// node ordinals (ColumnChunk.Ordinals), ords[i] the ordinal of position i
// (the ForEachRowOrdinal alignment).
type OrdinalSnapshot struct {
	*PointSnapshot[types.NodeID]
	ords []uint32
}

// NewOrdinalSnapshot binds ords (one per member, in position order) to
// snap; ok=false when the lengths differ.
func NewOrdinalSnapshot(snap *PointSnapshot[types.NodeID], ords []uint32) (*OrdinalSnapshot, bool) {
	if snap == nil || len(ords) != snap.Len() {
		return nil, false
	}
	return &OrdinalSnapshot{PointSnapshot: snap, ords: ords}, true
}

// LabelOrdinalSnapshot binds col's rows to propKeys with their ordinals
// (ords in col's row order, as ForEachRowOrdinal takes them); ok=false
// where NewPointSnapshot declines or the ordinals do not match the rows.
func LabelOrdinalSnapshot(col *LabelDocValues, propKeys []string, ords []uint32) (*OrdinalSnapshot, bool) {
	ps, ok := col.NewPointSnapshot(propKeys)
	if !ok {
		return nil, false
	}
	return NewOrdinalSnapshot(ps, ords)
}

// ReadChunk is PointSnapshot.ReadChunk with chunk.Ordinals filled.
func (s *OrdinalSnapshot) ReadChunk(from, to int, chunk *types.ColumnChunk) {
	s.PointSnapshot.ReadChunk(from, to, chunk)
	chunk.Ordinals = append(chunk.Ordinals[:0], s.ords[from:to]...)
}

// ChunkConcat reads the rows of several part snapshots concatenated in
// order (a tiered label over its shards, a sharded label over its slots) as
// typed chunks: per column the parts' kinds unified (integers and floats to
// Numeric, strings and numbers to Mixed; a part without a present value
// takes any kind) and, for a string or mixed column, the parts'
// dictionaries merged once (first use) with a code table per part. When
// every part is an OrdinalSnapshot the chunks carry ordinals too.
type ChunkConcat struct {
	parts   []*PointSnapshot[types.NodeID]
	ords    [][]uint32 // per part, when withOrds
	offsets []int      // offsets[i] = first row of part i; offsets[len] = Len
	kinds   []types.ColumnVectorKind
	// withOrds: every part carries ordinals (OrdinalSnapshot).
	withOrds bool

	once  sync.Once
	dicts [][]string   // per column (string columns)
	remap [][][]uint32 // per column, per part
}

// NewChunkConcat returns the chunk reader over parts (nil entries are
// parts without members), or ok=false where a part is not a point snapshot
// of this package (PointSnapshot or OrdinalSnapshot) or the parts bind
// different column counts.
func NewChunkConcat(parts []types.NodeColumnReader) (*ChunkConcat, bool) {
	c := &ChunkConcat{offsets: make([]int, 1, len(parts)+1), withOrds: true}
	width := -1
	for _, r := range parts {
		var p *PointSnapshot[types.NodeID]
		switch x := r.(type) {
		case nil:
			continue
		case *PointSnapshot[types.NodeID]:
			p, c.withOrds = x, false
		case *OrdinalSnapshot:
			p = x.PointSnapshot
			c.ords = append(c.ords, x.ords)
		default:
			return nil, false
		}
		if width >= 0 && len(p.cols) != width {
			return nil, false
		}
		width = len(p.cols)
		c.parts = append(c.parts, p)
		c.offsets = append(c.offsets, c.offsets[len(c.offsets)-1]+p.Len())
	}
	if !c.withOrds {
		c.ords = nil
	}
	if width < 0 {
		width = 0
	}
	c.kinds = make([]types.ColumnVectorKind, width)
	for k := range width {
		var kind types.ColumnVectorKind
		for _, p := range c.parts {
			col := p.cols[k]
			if !col.anyPresent() {
				continue
			}
			pk := col.chunkKind()
			switch {
			case kind == 0 || kind == pk:
				kind = pk
			case kind == types.ColumnVectorMixed || kind == types.ColumnVectorString || pk == types.ColumnVectorString:
				kind = types.ColumnVectorMixed
			default:
				kind = types.ColumnVectorNumeric
			}
		}
		if kind == 0 {
			kind = types.ColumnVectorInt
		}
		c.kinds[k] = kind
	}
	return c, true
}

// HasOrdinals reports whether the chunks carry ordinals: every part is an
// OrdinalSnapshot. A concatenation with a plain part hands out none, so an
// ordinal door declines it instead of serving chunks without them.
func (c *ChunkConcat) HasOrdinals() bool { return c.withOrds }

// Len is the rows of every part.
func (c *ChunkConcat) Len() int { return c.offsets[len(c.offsets)-1] }

// mergeDicts builds each string column's merged dictionary and the parts'
// code tables (once; a part's own dictionary in its order first, so a
// one-part string column keeps its codes).
func (c *ChunkConcat) mergeDicts() {
	c.dicts = make([][]string, len(c.kinds))
	c.remap = make([][][]uint32, len(c.kinds))
	for k, kind := range c.kinds {
		if kind != types.ColumnVectorString && kind != types.ColumnVectorMixed {
			continue
		}
		index := map[string]uint32{}
		var dict []string
		c.remap[k] = make([][]uint32, len(c.parts))
		for pi, p := range c.parts {
			col := p.cols[k]
			if col.typ != ColString {
				continue
			}
			table := make([]uint32, len(col.dict))
			for code, term := range col.dict {
				merged, ok := index[term]
				if !ok {
					merged = uint32(len(dict)) // #nosec G115 -- bounded by MaxDocValuesNodes per part
					dict = append(dict, term)
					index[term] = merged
				}
				table[code] = merged
			}
			c.remap[k][pi] = table
		}
		c.dicts[k] = dict
	}
}

// ReadChunk fills chunk with the rows [from, to) of the concatenation;
// bounds outside 0 <= from <= to <= Len() panic.
func (c *ChunkConcat) ReadChunk(from, to int, chunk *types.ColumnChunk) {
	if from < 0 || to < from || to > c.Len() {
		panic("column snapshot: chunk out of range")
	}
	c.once.Do(c.mergeDicts)
	n := to - from
	sizeChunk(chunk, n, len(c.kinds))
	chunk.Ordinals = chunk.Ordinals[:0]
	if c.withOrds {
		chunk.Ordinals = resized(chunk.Ordinals, n)
	}
	for k, kind := range c.kinds {
		sizeVector(&chunk.Cols[k], kind, n)
		if kind == types.ColumnVectorString || kind == types.ColumnVectorMixed {
			chunk.Cols[k].Dict = c.dicts[k]
		}
	}
	at := 0
	for pi, p := range c.parts {
		lo, hi := max(from, c.offsets[pi]), min(to, c.offsets[pi+1])
		if lo >= hi {
			continue
		}
		pf, pt := lo-c.offsets[pi], hi-c.offsets[pi]
		for i, id := range p.l.ids[pf:pt] {
			chunk.IDs[at+i] = id
		}
		if c.withOrds {
			copy(chunk.Ordinals[at:], c.ords[pi][pf:pt])
		}
		for k, col := range p.cols {
			var remap []uint32
			if c.kinds[k] == types.ColumnVectorString || c.kinds[k] == types.ColumnVectorMixed {
				remap = c.remap[k][pi]
			}
			col.fillVector(&chunk.Cols[k], at, pf, pt, remap)
		}
		at += hi - lo
	}
}
