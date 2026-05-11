// Package integrity provides the SHA-256 content-hashing primitives that the
// graph layer uses for entity-version integrity (`tkg_hash`/`tkg_prev_hash`).
//
// Hash output is part of the on-disk format: any change to the byte layout
// invalidates every existing `Hash`/`PrevHash` value persisted across stores
// and would break `Graph.Hash.VerifyNodeChain` / `Graph.Hash.VerifyRelChain`.
// Treat the byte layout as a versioned wire format.
package integrity

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"sort"
	"sync"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/storeutil"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// hashBufPool provides reusable byte buffers for hash input construction.
// Buffers are grown as needed and returned to the pool after use, amortizing
// allocation cost to near zero in steady state.
var hashBufPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 0, 256)
		return &buf
	},
}

// ComputeNodeHash computes a SHA-256 hash of the node's content.
// The hash covers: id, version, sorted labels, and sorted properties.
// Returns the hex-encoded hash string (64 characters).
func ComputeNodeHash(n *types.Node, labels []string) string {
	if n == nil {
		panic(types.ErrNilNode)
	}
	return computeNodeHashFromProps(n, labels, n.Properties())
}

// ComputeNodeHashChecked computes a SHA-256 hash of the node's content and
// returns property hashing failures as errors instead of panicking.
func ComputeNodeHashChecked(n *types.Node, labels []string) (hash string, err error) {
	if n == nil {
		return "", types.ErrNilNode
	}
	props := n.Properties()
	if propertySliceNeedsHashRecover(props) {
		return computeNodeHashCheckedWithRecover(n, labels, props)
	}
	return computeNodeHashFromProps(n, labels, props), nil
}

func computeNodeHashCheckedWithRecover(n *types.Node, labels []string, props types.PropertySlice) (hash string, err error) {
	bp := hashBufPool.Get().(*[]byte)
	buf := (*bp)[:0]
	defer func() {
		*bp = buf
		hashBufPool.Put(bp)
		if r := recover(); r != nil {
			hash = ""
			err = fmt.Errorf("%w: compute node hash panic: %v", types.ErrUnsupportedValueType, r)
		}
	}()
	hash, buf = computeNodeHashWithBuffer(buf, n, labels, props)
	return hash, nil
}

func computeNodeHashFromProps(n *types.Node, labels []string, props types.PropertySlice) string {
	bp := hashBufPool.Get().(*[]byte)
	buf := (*bp)[:0]
	hash, buf := computeNodeHashWithBuffer(buf, n, labels, props)

	*bp = buf
	hashBufPool.Put(bp)

	return hash
}

func computeNodeHashWithBuffer(buf []byte, n *types.Node, labels []string, props types.PropertySlice) (string, []byte) {
	buf = binary.BigEndian.AppendUint64(buf, uint64(n.ID().SnowflakeID())) // #nosec G115 — snowflake IDs use 63 bits
	buf = binary.BigEndian.AppendUint32(buf, n.Version())

	// Defensive sort — caller may pass unsorted labels. The single-label
	// create path is already canonical, so avoid an otherwise guaranteed
	// one-element slice allocation there.
	sorted := labels
	if len(labels) > 1 {
		sorted = make([]string, len(labels))
		copy(sorted, labels)
		sort.Strings(sorted)
	}

	for _, label := range sorted {
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(label))) // #nosec G115 — label length bounded by MaxNameLength (256)
		buf = append(buf, label...)
	}

	buf = appendProperties(buf, props)

	sum := sha256.Sum256(buf)
	var hexBuf [64]byte
	hex.Encode(hexBuf[:], sum[:])

	return string(hexBuf[:]), buf
}

// ComputeRelHash computes a SHA-256 hash of the relationship's content.
// The hash covers: id, version, type name, start ID, end ID, and sorted properties.
// Returns the hex-encoded hash string (64 characters).
func ComputeRelHash(r *types.Relationship, typeName string) string {
	if r == nil {
		panic(types.ErrNilRelationship)
	}
	return computeRelHashFromProps(r, typeName, r.Properties())
}

// ComputeRelHashChecked computes a SHA-256 hash of the relationship's content
// and returns property hashing failures as errors instead of panicking.
func ComputeRelHashChecked(r *types.Relationship, typeName string) (hash string, err error) {
	if r == nil {
		return "", types.ErrNilRelationship
	}
	props := r.Properties()
	if propertySliceNeedsHashRecover(props) {
		return computeRelHashCheckedWithRecover(r, typeName, props)
	}
	return computeRelHashFromProps(r, typeName, props), nil
}

func computeRelHashCheckedWithRecover(r *types.Relationship, typeName string, props types.PropertySlice) (hash string, err error) {
	bp := hashBufPool.Get().(*[]byte)
	buf := (*bp)[:0]
	defer func() {
		*bp = buf
		hashBufPool.Put(bp)
		if recovered := recover(); recovered != nil {
			hash = ""
			err = fmt.Errorf("%w: compute relationship hash panic: %v", types.ErrUnsupportedValueType, recovered)
		}
	}()
	hash, buf = computeRelHashWithBuffer(buf, r, typeName, props)
	return hash, nil
}

func computeRelHashFromProps(r *types.Relationship, typeName string, props types.PropertySlice) string {
	bp := hashBufPool.Get().(*[]byte)
	buf := (*bp)[:0]
	hash, buf := computeRelHashWithBuffer(buf, r, typeName, props)

	*bp = buf
	hashBufPool.Put(bp)

	return hash
}

func computeRelHashWithBuffer(buf []byte, r *types.Relationship, typeName string, props types.PropertySlice) (string, []byte) {
	buf = binary.BigEndian.AppendUint64(buf, uint64(r.ID().SnowflakeID())) // #nosec G115 — snowflake IDs use 63 bits
	buf = binary.BigEndian.AppendUint32(buf, r.Version())
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(typeName))) // #nosec G115 — type name bounded by MaxNameLength (256)
	buf = append(buf, typeName...)
	buf = binary.BigEndian.AppendUint64(buf, uint64(r.StartNodeID().SnowflakeID())) // #nosec G115 — snowflake IDs use 63 bits
	buf = binary.BigEndian.AppendUint64(buf, uint64(r.EndNodeID().SnowflakeID()))   // #nosec G115 — snowflake IDs use 63 bits

	buf = appendProperties(buf, props)

	sum := sha256.Sum256(buf)
	var hexBuf [64]byte
	hex.Encode(hexBuf[:], sum[:])

	return string(hexBuf[:]), buf
}

func propertySliceNeedsHashRecover(props types.PropertySlice) bool {
	for _, p := range props {
		if propertyValueNeedsHashRecover(p.Value) {
			return true
		}
	}
	return false
}

func propertyValueNeedsHashRecover(v any) bool {
	switch val := v.(type) {
	case nil,
		bool, string,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64,
		[]string, []int, []int64, []float32, []float64, []byte, []bool,
		map[string]string:
		return false
	case []any:
		for _, elem := range val {
			if propertyValueNeedsHashRecover(elem) {
				return true
			}
		}
		return false
	case map[string]any:
		for _, elem := range val {
			if propertyValueNeedsHashRecover(elem) {
				return true
			}
		}
		return false
	default:
		_, ok := v.(types.HashableValue)
		return ok
	}
}

// appendProperties appends sorted properties to buf in a deterministic format.
// PropertySlice is already sorted by key — no re-sort needed.
func appendProperties(buf []byte, props types.PropertySlice) []byte {
	for _, p := range props {
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(p.Key))) // #nosec G115 — key length bounded by MaxPropertyKeyLength (256)
		buf = append(buf, p.Key...)
		buf = appendPropertyValue(buf, p.Value)
	}
	return buf
}

// appendPropertyValue appends a typed binary representation of a property value
// to buf. Each value is prefixed with a type tag byte from wire.go, ensuring
// type-distinct hashing (int(1) vs string("1") produce different hashes).
// Maps sort keys before hashing for deterministic output. []any recurses.
func appendPropertyValue(buf []byte, v any) []byte {
	buf = append(buf, storepkg.PropertyTypeTag(v))

	// Nil properties hash to their type tag alone. Common case from loaders
	// that map SQL NULL to Go nil — without this branch, ComputeNodeHash
	// panics in the default case below.
	if v == nil {
		return buf
	}

	switch val := v.(type) {
	case bool:
		if val {
			buf = append(buf, 1)
		} else {
			buf = append(buf, 0)
		}
	// Signed→unsigned casts below are bit reinterpretations for deterministic hashing;
	// the numeric value is irrelevant, only the bit pattern matters.
	case int:
		buf = binary.BigEndian.AppendUint64(buf, uint64(int64(val))) // #nosec G115
	case int8:
		buf = append(buf, byte(val)) // #nosec G115
	case int16:
		buf = binary.BigEndian.AppendUint16(buf, uint16(val)) // #nosec G115
	case int32:
		buf = binary.BigEndian.AppendUint32(buf, uint32(val)) // #nosec G115
	case int64:
		buf = binary.BigEndian.AppendUint64(buf, uint64(val)) // #nosec G115
	case uint:
		buf = binary.BigEndian.AppendUint64(buf, uint64(val))
	case uint8:
		buf = append(buf, val)
	case uint16:
		buf = binary.BigEndian.AppendUint16(buf, val)
	case uint32:
		buf = binary.BigEndian.AppendUint32(buf, val)
	case uint64:
		buf = binary.BigEndian.AppendUint64(buf, val)
	case float32:
		buf = binary.BigEndian.AppendUint32(buf, math.Float32bits(val))
	case float64:
		buf = binary.BigEndian.AppendUint64(buf, math.Float64bits(val))
	// All uint32(len(...)) casts below are safe: property values are bounded
	// by MaxPropertyValueSize (64K) and MaxPropertiesPerEntity (1000).
	case string:
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(val))) // #nosec G115
		buf = append(buf, val...)
	case []string:
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(val))) // #nosec G115
		for _, s := range val {
			buf = binary.BigEndian.AppendUint32(buf, uint32(len(s))) // #nosec G115
			buf = append(buf, s...)
		}
	case []int:
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(val))) // #nosec G115
		for _, n := range val {
			buf = binary.BigEndian.AppendUint64(buf, uint64(int64(n))) // #nosec G115
		}
	case []int64:
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(val))) // #nosec G115
		for _, n := range val {
			buf = binary.BigEndian.AppendUint64(buf, uint64(n)) // #nosec G115
		}
	case []float32:
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(val))) // #nosec G115
		for _, f := range val {
			buf = binary.BigEndian.AppendUint32(buf, math.Float32bits(f))
		}
	case []float64:
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(val))) // #nosec G115
		for _, f := range val {
			buf = binary.BigEndian.AppendUint64(buf, math.Float64bits(f))
		}
	case []byte:
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(val))) // #nosec G115
		buf = append(buf, val...)
	case []bool:
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(val))) // #nosec G115
		for _, b := range val {
			if b {
				buf = append(buf, 1)
			} else {
				buf = append(buf, 0)
			}
		}
	case []any:
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(val))) // #nosec G115
		for _, elem := range val {
			buf = appendPropertyValue(buf, elem)
		}
	case map[string]any:
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(val))) // #nosec G115
		keys := make([]string, 0, len(val))
		for k := range val {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			buf = binary.BigEndian.AppendUint32(buf, uint32(len(k))) // #nosec G115
			buf = append(buf, k...)
			buf = appendPropertyValue(buf, val[k])
		}
	case map[string]string:
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(val))) // #nosec G115
		keys := make([]string, 0, len(val))
		for k := range val {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			buf = binary.BigEndian.AppendUint32(buf, uint32(len(k))) // #nosec G115
			buf = append(buf, k...)
			buf = binary.BigEndian.AppendUint32(buf, uint32(len(val[k]))) // #nosec G115
			buf = append(buf, val[k]...)
		}
	default:
		// Custom property types (e.g. pkg/spatial Point/Polygon/MultiPolygon)
		// may participate in hashing by implementing types.HashableValue. The
		// type must also be registered via types.RegisterPropertyStructType so
		// PropertySlice.Set accepts it as a property value.
		if h, ok := v.(types.HashableValue); ok {
			hb := h.HashBytes()
			buf = binary.BigEndian.AppendUint32(buf, uint32(len(hb))) // #nosec G115
			buf = append(buf, hb...)
			return buf
		}
		panic(fmt.Sprintf("graph: appendPropertyValue: unsupported type %T (value does not implement types.HashableValue)", v))
	}
	return buf
}
