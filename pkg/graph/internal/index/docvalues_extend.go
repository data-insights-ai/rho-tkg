package index

import (
	"cmp"
	"slices"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Extend returns a NEW snapshot covering this snapshot's rows plus newIDs, without
// re-reading a single existing row.
//
// WHY THIS EXISTS. A snapshot is invalidated by epoch advance, so any write to a
// label throws its columns away and the next read rebuilds the WHOLE label. Rebuild
// is O(label size) while a read may want a handful of rows, so under a workload that
// both writes and reads a label the cache can cost more than it saves — one rebuild
// measured at ~3.8x a full typed scan, and unboundedly worse for a partial read.
//
// The dominant write shape is an APPEND: new nodes carrying labels that already
// exist. That case needs no merge, no tombstones and no re-read — the new rows are
// simply more ordinals. Extend serves exactly that case and refuses everything else,
// because a silent wrong answer here is worse than a rebuild.
//
// IMMUTABILITY IS PRESERVED. The result is a new *LabelDocValues; the receiver is
// never mutated, so an in-flight reader holding it keeps a consistent view. Value
// columns are rebuilt as new arrays (they must grow), but the caller's existing
// snapshot and every slice it handed out stay valid.
//
// REFUSES (returns nil) when:
//   - newIDs is empty, or any of them already exists in this snapshot. A duplicate
//     means the write was an UPDATE, not an append, and an update can change a value
//     this snapshot already captured.
//   - any newID sorts BEFORE the current maximum. Ordinals must stay sorted by ID
//     for lookup's binary search, and an out-of-order arrival means IDs are not
//     being minted monotonically — the assumption the append case rests on.
//   - this snapshot has temporal columns but no temporal accessor is supplied (or
//     vice versa). A half-populated validity column reads as "valid for all time"
//     for the new rows.
//   - a new row's value does not fit the existing column's type. Widening an int64
//     column because one appended row is a float would change which values every
//     consumer's equality test matches.
//
// A nil return is always safe: the caller falls back to a full rebuild.
func (l *LabelDocValues) Extend(epoch uint64, newIDs []types.NodeID,
	getProp func(types.NodeID, string) (any, bool),
	getTemporal func(types.NodeID) (validFrom, validTo int64, ok bool)) *LabelDocValues {

	if l == nil || len(newIDs) == 0 {
		return nil
	}
	if (getTemporal == nil) != !l.hasTemporal {
		return nil // temporal shape must match, or the new rows get a bogus validity
	}

	added := slices.Clone(newIDs)
	slices.SortFunc(added, func(a, b types.NodeID) int {
		return cmp.Compare(a.SnowflakeID(), b.SnowflakeID())
	})
	// Strictly greater than the current maximum, and strictly increasing among
	// themselves — anything else is not an append.
	if len(l.nodeIDs) > 0 {
		maxCur := l.nodeIDs[len(l.nodeIDs)-1]
		if cmp.Compare(added[0].SnowflakeID(), maxCur.SnowflakeID()) <= 0 {
			return nil
		}
	}
	for i := 1; i < len(added); i++ {
		if cmp.Compare(added[i].SnowflakeID(), added[i-1].SnowflakeID()) <= 0 {
			return nil // duplicate within the batch
		}
	}

	oldN := len(l.nodeIDs)
	n := oldN + len(added)
	if n > MaxDocValuesNodes {
		return nil // over cap — the store's own rule, not ours to bend
	}

	out := &LabelDocValues{
		epoch:   epoch,
		nodeIDs: append(slices.Clone(l.nodeIDs), added...),
		cols:    make(map[string]*docColumn, len(l.cols)),
	}

	for key, c := range l.cols {
		ext := extendColumn(c, added, key, oldN, n, getProp)
		if ext == nil {
			return nil // a value did not fit the column's type
		}
		out.cols[key] = ext
	}

	if l.hasTemporal {
		out.validFrom = append(slices.Clone(l.validFrom), make([]int64, len(added))...)
		out.validTo = append(slices.Clone(l.validTo), make([]int64, len(added))...)
		out.hasTemporal = true
		for i, id := range added {
			if f, t, ok := getTemporal(id); ok {
				out.validFrom[oldN+i], out.validTo[oldN+i] = f, t
			}
		}
		out.extendZoneMap(l, oldN)
	}
	return out
}

// extendZoneMap recomputes only the blocks an append can have changed. Every block
// strictly before the one holding ordinal oldN is untouched — its rows and their
// bounds are identical — so its min/max carry over verbatim. Only the block that
// oldN falls in (which may have been partial and just gained rows) and any block
// after it need computing.
//
// Recomputing everything would be correct but would leave an O(label size) term in
// what is meant to be an O(appended) operation.
func (l *LabelDocValues) extendZoneMap(src *LabelDocValues, oldN int) {
	n := len(l.nodeIDs)
	blocks := (n + zoneBlockSize - 1) / zoneBlockSize
	l.zoneMinFrom = make([]int64, blocks)
	l.zoneMaxFrom = make([]int64, blocks)
	l.zoneMinTo = make([]int64, blocks)
	l.zoneMaxTo = make([]int64, blocks)
	l.zoneOpenEnded = make([]bool, blocks)

	// Blocks entirely below oldN are unchanged. oldN/zoneBlockSize is the first block
	// that can contain a new row; it is recomputed even when oldN lands exactly on a
	// boundary (then it is simply a wholly-new block).
	carry := oldN / zoneBlockSize
	if carry > len(src.zoneMinFrom) {
		carry = len(src.zoneMinFrom)
	}
	copy(l.zoneMinFrom, src.zoneMinFrom[:carry])
	copy(l.zoneMaxFrom, src.zoneMaxFrom[:carry])
	copy(l.zoneMinTo, src.zoneMinTo[:carry])
	copy(l.zoneMaxTo, src.zoneMaxTo[:carry])
	copy(l.zoneOpenEnded, src.zoneOpenEnded[:carry])

	for b := carry; b < blocks; b++ {
		l.computeZoneBlock(b, n)
	}
}

// computeZoneBlock fills one block's min/max from the validity columns.
func (l *LabelDocValues) computeZoneBlock(b, n int) {
	lo := b * zoneBlockSize
	hi := min(lo+zoneBlockSize, n)
	minF, maxF := l.validFrom[lo], l.validFrom[lo]
	var minT, maxT int64
	open, seenClosed := false, false
	for ord := lo; ord < hi; ord++ {
		f, t := l.validFrom[ord], l.validTo[ord]
		minF, maxF = min(minF, f), max(maxF, f)
		if t == 0 {
			open = true
			continue // an open-ended row contributes no upper bound
		}
		if !seenClosed {
			minT, maxT, seenClosed = t, t, true
			continue
		}
		minT, maxT = min(minT, t), max(maxT, t)
	}
	l.zoneMinFrom[b], l.zoneMaxFrom[b] = minF, maxF
	l.zoneMinTo[b], l.zoneMaxTo[b] = minT, maxT
	l.zoneOpenEnded[b] = open
}

// extendColumn produces a column covering oldN existing ordinals plus the appended
// ids. Existing values are COPIED, never re-read. Returns nil if an appended value
// does not fit the column's established type.
func extendColumn(c *docColumn, added []types.NodeID, key string, oldN, n int,
	getProp func(types.NodeID, string) (any, bool)) *docColumn {

	if c.typ == colUnbuildable {
		// Stays unbuildable — cheap and correct; the consumer already falls back.
		return &docColumn{typ: colUnbuildable, n: n}
	}

	present := newBitset(n)
	copy(present, c.present)

	switch c.typ {
	case ColString:
		out := &docColumn{typ: ColString, present: present, n: n,
			dict:  slices.Clone(c.dict),
			codes: append(slices.Clone(c.codes), make([]uint32, len(added))...)}
		idx := make(map[string]uint32, len(out.dict))
		for i, s := range out.dict {
			idx[s] = uint32(i)
		}
		for i, id := range added {
			v, ok := getProp(id, key)
			if !ok || v == nil {
				continue
			}
			s, isStr := v.(string)
			if !isStr {
				return nil // a non-string in a string column
			}
			code, seen := idx[s]
			if !seen {
				code = uint32(len(out.dict))
				out.dict = append(out.dict, s)
				idx[s] = code
			}
			out.codes[oldN+i] = code
			present.set(oldN + i)
		}
		return out

	case ColNumeric:
		// Existing rows are carried by GROWING THE SOURCE ARRAYS, not by re-reading
		// or looping them: one memmove per array instead of n iterations, which is
		// what makes an append cheaper than a rebuild rather than merely equal to it.
		// Only the halves the source actually had are allocated, so a uniform column
		// keeps its single-array footprint unless an appended value forces the other.
		pad := len(added)
		var ints []int64
		var flts []float64
		var isFloat bitset
		if c.ints != nil {
			ints = append(slices.Clone(c.ints), make([]int64, pad)...)
		}
		if c.flts != nil {
			flts = append(slices.Clone(c.flts), make([]float64, pad)...)
		}
		if c.isFloat != nil {
			isFloat = newBitset(n)
			copy(isFloat, c.isFloat)
		}
		// A source that was uniformly float has no int half, and vice versa. Track
		// which kinds are live so the collapse below stays exact.
		hadFloat := c.flts != nil
		hadInt := c.ints != nil

		for i, id := range added {
			v, ok := getProp(id, key)
			if !ok || v == nil {
				continue
			}
			if !isNumericScalar(v) {
				return nil // a string/bool/list in a numeric column
			}
			iv, fv, isF := normalizeNumeric(v)
			ord := oldN + i
			if isF {
				if flts == nil {
					flts = make([]float64, n)
				}
				if isFloat == nil {
					isFloat = newBitset(n)
					// Every pre-existing row was an int, so no backfill is needed —
					// a cleared selector bit already means "read the int half".
				}
				flts[ord] = fv
				isFloat.set(ord)
				hadFloat = true
			} else {
				if ints == nil {
					ints = make([]int64, n)
				}
				if isFloat == nil && hadFloat {
					// Source was uniformly float; the selector must now exist and
					// mark every EXISTING row as float before this int row lands.
					isFloat = newBitset(n)
					for o := 0; o < oldN; o++ {
						if c.present.get(o) {
							isFloat.set(o)
						}
					}
				}
				ints[ord] = iv
				hadInt = true
			}
			present.set(ord)
		}

		out := &docColumn{typ: ColNumeric, present: present, n: n}
		switch {
		case hadFloat && hadInt:
			out.ints, out.flts, out.isFloat = ints, flts, isFloat
		case hadFloat:
			out.flts = flts
		default:
			out.ints = ints
		}
		if out.ints == nil && out.flts == nil {
			out.ints = make([]int64, n) // all-absent column, treated as int
		}
		return out
	}
	return nil
}

// isNumericScalar reports whether v is one of the numeric widths normalizeNumeric
// accepts, distinguishing a genuine zero from an unsupported type.
func isNumericScalar(v any) bool {
	switch v.(type) {
	case int64, int, int32, int16, int8,
		uint64, uint, uint32, uint16, uint8,
		float64, float32:
		return true
	}
	return false
}
