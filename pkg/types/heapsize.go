package types

// Approximate Go heap footprint estimation for cache byte budgets
// (enterprise-scale ceiling 4: caches budgeted by count, not bytes).
//
// The estimates intentionally approximate RESIDENT heap bytes — struct
// sizes, slice backing arrays, string bytes, map buckets — not wire
// encoding size, because the budget bounds process memory. Estimates are
// conservative-cheap: O(properties) with zero allocation, no reflection on
// the allowlisted fast paths. Unknown (registered custom) types fall back
// to a fixed pessimistic constant rather than walking user structs.

const (
	// approxStringHeader/SliceHeader/word are the amd64/arm64 sizes.
	approxWord         = 8
	approxStringHeader = 16
	approxSliceHeader  = 24
	approxMapBase      = 48 // hmap struct, before buckets
	approxMapPerEntry  = 32 // bucket share + key/value headers (order of magnitude)
	approxIfaceBox     = 16 // interface header for boxed values
	// approxUnknownValue covers registered custom property types — walking
	// user-defined structs is not worth the cost; assume a sizable value.
	approxUnknownValue = 256
	// approxTemporalMeta is the TemporalMetadata struct (4 instants + flags).
	approxTemporalMeta = 72
	// approxIntegrity covers the integrity struct + hash string.
	approxIntegrity = 96
)

// approxValueBytes estimates the heap bytes held by a property value.
// depth bounds recursion on nested containers (shared with the
// maxPropertyDepth validation cap, so validated values never hit it).
func approxValueBytes(v any, depth int) int {
	if depth > maxPropertyDepth {
		return 0
	}
	switch tv := v.(type) {
	case nil:
		return 0
	case bool, int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		return approxIfaceBox
	case string:
		return approxStringHeader + len(tv)
	case TemporalValue:
		return approxStringHeader + 8 + len(tv.Value)
	case []byte:
		return approxSliceHeader + len(tv)
	case []string:
		n := approxSliceHeader
		for _, s := range tv {
			n += approxStringHeader + len(s)
		}
		return n
	case []int:
		return approxSliceHeader + len(tv)*approxWord
	case []int64:
		return approxSliceHeader + len(tv)*approxWord
	case []float64:
		return approxSliceHeader + len(tv)*approxWord
	case []float32:
		return approxSliceHeader + len(tv)*4
	case []bool:
		return approxSliceHeader + len(tv)
	case []any:
		n := approxSliceHeader
		for _, e := range tv {
			n += approxIfaceBox + approxValueBytes(e, depth+1)
		}
		return n
	case map[string]string:
		n := approxMapBase
		for k, val := range tv {
			n += approxMapPerEntry + len(k) + len(val)
		}
		return n
	case map[string]any:
		n := approxMapBase
		for k, val := range tv {
			n += approxMapPerEntry + len(k) + approxValueBytes(val, depth+1)
		}
		return n
	default:
		return approxUnknownValue
	}
}

// ApproxHeapBytes estimates the heap bytes held by the property slice:
// backing array + per-entry key strings and values.
func (ps PropertySlice) ApproxHeapBytes() int {
	n := approxSliceHeader + len(ps)*(approxStringHeader+approxIfaceBox)
	for i := range ps {
		n += len(ps[i].Key) + approxValueBytes(ps[i].Value, 0)
	}
	return n
}

// ApproxHeapBytes estimates the node's resident heap footprint: struct,
// extra labels, temporal/integrity metadata, and properties. Used by
// byte-budgeted caches; see the package comment in heapsize.go for the
// estimation contract. Nil-safe (returns 0).
func (n *Node) ApproxHeapBytes() int {
	if n == nil {
		return 0
	}
	size := 80 // Node struct (documented layout)
	if len(n.extraLabels) > 0 {
		size += approxSliceHeader + len(n.extraLabels)*2
	}
	if n.temporal != nil {
		size += approxTemporalMeta
	}
	if n.integrity != nil {
		size += approxIntegrity
	}
	return size + n.properties.ApproxHeapBytes()
}

// ApproxHeapBytes estimates the relationship's resident heap footprint.
// Same contract as (*Node).ApproxHeapBytes. Nil-safe (returns 0).
func (r *Relationship) ApproxHeapBytes() int {
	if r == nil {
		return 0
	}
	size := 72 // Relationship struct (documented layout)
	if r.temporal != nil {
		size += approxTemporalMeta
	}
	if r.integrity != nil {
		size += approxIntegrity
	}
	return size + r.properties.ApproxHeapBytes()
}
