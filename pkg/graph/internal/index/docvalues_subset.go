package index

// SubsetDocValues builds, from the rows src holds at the ascending ordinals
// ords, the snapshot BuildDocValues(epoch, those IDs, keys, getters) would
// build from the entities themselves (round 7 request 6: a label
// intersection's columns from a member label's columns, no row read): the
// same IDs, validity columns and zone map, and per key a column classified
// and stored as BuildDocValues stores it for that subset (an all-absent key
// is an empty numeric column, a numeric column collapses to int64, float64 or
// mixed by the subset's values, a string dictionary is numbered in the
// subset's order). ok=false where src cannot stand in for the entities: a key
// src has no usable column for (absent or unbuildable — a mixed column of the
// whole label may be uniform on the subset, which only the entities can
// tell), or rows without validity columns.
func SubsetDocValues[T EntityID](epoch uint64, src *DocValues[T], ords []int, keys []string) (*DocValues[T], bool) {
	n := len(ords)
	if n > 0 && !src.hasTemporal {
		return nil, false
	}
	for _, key := range keys {
		if !src.Has(key) {
			return nil, false
		}
	}
	ids := make([]T, n)
	for j, o := range ords {
		ids[j] = src.ids[o]
	}
	l := &DocValues[T]{epoch: epoch, ids: ids, cols: make(map[string]*docColumn, len(keys))}
	if n > 0 {
		l.validFrom = make([]int64, n)
		l.validTo = make([]int64, n)
		l.hasTemporal = true
		for j, o := range ords {
			l.validFrom[j], l.validTo[j] = src.validFrom[o], src.validTo[o]
		}
		l.buildZoneMap()
	}
	for _, key := range keys {
		if _, done := l.cols[key]; done {
			continue
		}
		l.cols[key] = subsetColumn(src.cols[key], ords)
	}
	return l, true
}

// subsetColumn is buildColumn over a usable column's rows at ords.
func subsetColumn(c *docColumn, ords []int) *docColumn {
	n := len(ords)
	present := newBitset(n)
	count := 0
	for j, o := range ords {
		if c.present.get(o) {
			present.set(j)
			count++
		}
	}
	if c.typ == ColString && count > 0 {
		out := &docColumn{typ: ColString, present: present, n: n, codes: make([]uint32, n)}
		codes := make(map[uint32]uint32)
		for j, o := range ords {
			if !present.get(j) {
				continue
			}
			code, ok := codes[c.codes[o]]
			if !ok {
				code = uint32(len(out.dict)) // #nosec G115 -- dictionary rows are bounded by MaxDocValuesNodes
				out.dict = append(out.dict, c.dict[c.codes[o]])
				codes[c.codes[o]] = code
			}
			out.codes[j] = code
		}
		return out
	}
	out := &docColumn{typ: ColNumeric, present: present, n: n}
	ints := make([]int64, n)
	var flts []float64
	var isFloat bitset
	floats := 0
	for j, o := range ords {
		if !present.get(j) {
			continue
		}
		iv, fv, isF := c.numericAt(o)
		if !isF {
			ints[j] = iv
			continue
		}
		if flts == nil {
			flts = make([]float64, n)
			isFloat = newBitset(n)
		}
		flts[j] = fv
		isFloat.set(j)
		floats++
	}
	switch floats {
	case 0:
		out.ints = ints
	case count:
		out.flts = flts
	default:
		out.ints, out.flts, out.isFloat = ints, flts, isFloat
	}
	return out
}
