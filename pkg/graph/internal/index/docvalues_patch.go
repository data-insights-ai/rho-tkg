package index

import (
	"math/bits"
	"slices"
)

// Patch returns a NEW snapshot equal to this one with the rows of changed
// re-read: the in-place-update counterpart of Extend (round 5 request 4,
// sigma-tkgd C4r/C4s: one property write to a node of a 1,000,000-node badger
// label made the next column read decode the whole label again, 4.5 s).
//
// Every changed ID must already be a row of this snapshot; the ordinal vector
// is unchanged and shared (it is immutable). Each column is copied as typed
// arrays (no entity is read for an unchanged row), the changed rows are
// overwritten from getProp, and the column is brought back into the exact
// representation BuildDocValues would give the same values: a numeric column
// collapses to uniformly int64, uniformly float64 or mixed by its present
// rows; a string column's dictionary is renumbered in first-appearance order.
// Validity columns and the blocks of the zone map that hold a changed row are
// recomputed.
//
// REFUSES (returns nil, the caller rebuilds) when:
//   - changed is empty or names an ID this snapshot does not hold (an insert
//     or a membership change is not an in-place update);
//   - the temporal shape does not match (as Extend);
//   - a column is unbuildable (a changed row might make it buildable);
//   - a changed value does not fit its column's class (a string in a numeric
//     column or the reverse, any other kind), or a column ends up with no
//     present row (a rebuild classifies those, Patch does not guess).
//
// The receiver is never mutated.
func (l *DocValues[T]) Patch(epoch uint64, changed []T,
	getProp func(T, string) (any, bool),
	getTemporal func(T) (validFrom, validTo int64, ok bool)) *DocValues[T] {

	if l == nil || len(changed) == 0 {
		return nil
	}
	if (getTemporal == nil) != !l.hasTemporal {
		return nil
	}
	ords := make([]int, 0, len(changed))
	rowIDs := make([]T, 0, len(changed))
	for _, id := range changed {
		ord, ok := l.lookup(id)
		if !ok {
			return nil
		}
		ords = append(ords, ord)
		rowIDs = append(rowIDs, id)
	}

	out := &DocValues[T]{
		epoch: epoch,
		ids:   l.ids,
		cols:  make(map[string]*docColumn, len(l.cols)),
	}
	for key, c := range l.cols {
		p := patchColumn(c, rowIDs, ords, key, getProp)
		if p == nil {
			return nil
		}
		out.cols[key] = p
	}

	if l.hasTemporal {
		out.hasTemporal = true
		out.validFrom = slices.Clone(l.validFrom)
		out.validTo = slices.Clone(l.validTo)
		for i, id := range rowIDs {
			f, t, ok := getTemporal(id)
			if !ok {
				f, t = 0, 0
			}
			out.validFrom[ords[i]], out.validTo[ords[i]] = f, t
		}
		out.patchZoneMap(l, ords)
	}
	return out
}

// patchZoneMap copies src's zone map and recomputes the blocks holding ords.
func (l *DocValues[T]) patchZoneMap(src *DocValues[T], ords []int) {
	l.zoneMinFrom = slices.Clone(src.zoneMinFrom)
	l.zoneMaxFrom = slices.Clone(src.zoneMaxFrom)
	l.zoneMinTo = slices.Clone(src.zoneMinTo)
	l.zoneMaxTo = slices.Clone(src.zoneMaxTo)
	l.zoneOpenEnded = slices.Clone(src.zoneOpenEnded)
	blocks := make([]int, 0, len(ords))
	for _, ord := range ords {
		blocks = append(blocks, ord/zoneBlockSize)
	}
	slices.Sort(blocks)
	for _, b := range slices.Compact(blocks) {
		l.computeZoneBlock(b, len(l.ids))
	}
}

// patchColumn copies c with the rows at ords re-read (ids[i] is the row at
// ords[i]) and normalised to BuildDocValues' representation, or nil when the
// patch would change the column's class (see Patch).
func patchColumn[T EntityID](c *docColumn, ids []T, ords []int, key string,
	getProp func(T, string) (any, bool)) *docColumn {

	present := slices.Clone(c.present)
	switch c.typ {
	case ColString:
		codes := slices.Clone(c.codes)
		dict := slices.Clone(c.dict)
		known := make(map[string]uint32, len(dict))
		for i, s := range dict {
			known[s] = uint32(i) // #nosec G115 -- dictionary rows are bounded by MaxDocValuesNodes
		}
		for i, id := range ids {
			ord := ords[i]
			v, ok := getProp(id, key)
			if !ok || v == nil {
				present.clear(ord)
				codes[ord] = 0
				continue
			}
			s, isStr := v.(string)
			if !isStr {
				return nil
			}
			code, found := known[s]
			if !found {
				code = uint32(len(dict)) // #nosec G115 -- dictionary rows are bounded by MaxDocValuesNodes
				dict = append(dict, s)
				known[s] = code
			}
			codes[ord] = code
			present.set(ord)
		}
		if present.count() == 0 {
			return nil
		}
		out := &docColumn{typ: ColString, present: present, n: c.n}
		out.dict, out.codes = renumberDict(dict, codes, present, c.n)
		return out

	case ColNumeric:
		n := c.n
		ints := c.ints
		flts := c.flts
		var isFloat bitset
		switch {
		case c.isFloat != nil:
			isFloat = slices.Clone(c.isFloat)
		case c.flts != nil && c.ints == nil:
			isFloat = slices.Clone(c.present) // uniformly float: every present row
		default:
			isFloat = newBitset(n)
		}
		ints = cloneOrZero(ints, n)
		flts = cloneOrZero(flts, n)
		for i, id := range ids {
			ord := ords[i]
			ints[ord], flts[ord] = 0, 0
			isFloat.clear(ord)
			v, ok := getProp(id, key)
			if !ok || v == nil {
				present.clear(ord)
				continue
			}
			if !isNumericScalar(v) {
				return nil
			}
			iv, fv, isF := normalizeNumeric(v)
			if isF {
				flts[ord] = fv
				isFloat.set(ord)
			} else {
				ints[ord] = iv
			}
			present.set(ord)
		}
		presentCount, floatCount := present.count(), isFloat.count()
		if presentCount == 0 {
			return nil
		}
		out := &docColumn{typ: ColNumeric, present: present, n: n}
		switch floatCount {
		case 0:
			out.ints = ints
		case presentCount:
			out.flts = flts
		default:
			out.ints, out.flts, out.isFloat = ints, flts, isFloat
		}
		return out
	}
	return nil // colUnbuildable
}

// cloneOrZero copies src, or allocates n zeros when src is nil.
func cloneOrZero[E int64 | float64](src []E, n int) []E {
	if src == nil {
		return make([]E, n)
	}
	return slices.Clone(src)
}

// renumberDict rewrites a string column's dictionary in first-appearance
// order over the present rows, dropping terms no row uses, as
// buildStringColumn numbers a fresh build.
func renumberDict(dict []string, codes []uint32, present bitset, n int) ([]string, []uint32) {
	remap := make([]int32, len(dict))
	for i := range remap {
		remap[i] = -1
	}
	out := make([]string, 0, len(dict))
	for ord := 0; ord < n; ord++ {
		if !present.get(ord) {
			codes[ord] = 0
			continue
		}
		old := codes[ord]
		if remap[old] < 0 {
			remap[old] = int32(len(out)) // #nosec G115 -- dictionary rows are bounded by MaxDocValuesNodes
			out = append(out, dict[old])
		}
		codes[ord] = uint32(remap[old]) // #nosec G115 -- non-negative by construction
	}
	return out, codes
}

func (b bitset) clear(i int) { b[i>>6] &^= 1 << uint(i&63) }

func (b bitset) count() int {
	n := 0
	for _, w := range b {
		n += bits.OnesCount64(w)
	}
	return n
}

// Contains reports whether id is a row of this snapshot.
func (l *DocValues[T]) Contains(id T) bool {
	if l == nil {
		return false
	}
	_, ok := l.lookup(id)
	return ok
}

// SameIDs reports whether a and b share one ID vector (a Patch result shares
// its receiver's), so anything derived from the rows alone carries over.
func SameIDs[T EntityID](a, b *DocValues[T]) bool {
	if a == nil || b == nil || len(a.ids) != len(b.ids) {
		return false
	}
	return len(a.ids) == 0 || &a.ids[0] == &b.ids[0]
}
