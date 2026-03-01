package graph

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"hash"
	"io"
	"sort"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3/pkg/types"
)

// mustWrite writes binary data to a hash.Hash, panicking on error.
// hash.Hash.Write is documented to never return an error, but
// binary.Write wraps it in an interface that technically can.
// Panicking surfaces any future stdlib behavioral change immediately.
func mustWrite(h hash.Hash, data any) {
	if err := binary.Write(h, binary.BigEndian, data); err != nil {
		panic("graph: hash write: " + err.Error())
	}
}

// mustWriteString writes a string to a hash.Hash, panicking on error.
func mustWriteString(h hash.Hash, s string) {
	if _, err := io.WriteString(h, s); err != nil {
		panic("graph: hash write string: " + err.Error())
	}
}

// ComputeNodeHash computes a SHA-256 hash of the node's content.
// The hash covers: id, version, sorted labels, and sorted properties.
// Returns the hex-encoded hash string (64 characters).
func ComputeNodeHash(n *types.Node, labels []string) string {
	h := sha256.New()

	mustWrite(h, int64(n.InternalID().SnowflakeID()))
	mustWrite(h, n.Version())

	// Defensive sort — caller may pass unsorted labels.
	sorted := make([]string, len(labels))
	copy(sorted, labels)
	sort.Strings(sorted)

	for _, label := range sorted {
		mustWrite(h, uint32(len(label)))
		mustWriteString(h, label)
	}

	writeProperties(h, n.Properties())
	return hex.EncodeToString(h.Sum(nil))
}

// ComputeRelHash computes a SHA-256 hash of the relationship's content.
// The hash covers: id, version, type name, start ID, end ID, and sorted properties.
// Returns the hex-encoded hash string (64 characters).
func ComputeRelHash(r *types.Relationship, typeName string) string {
	h := sha256.New()

	mustWrite(h, int64(r.InternalID().SnowflakeID()))
	mustWrite(h, r.Version())
	mustWrite(h, uint32(len(typeName)))
	mustWriteString(h, typeName)
	mustWrite(h, int64(r.StartNodeID().SnowflakeID()))
	mustWrite(h, int64(r.EndNodeID().SnowflakeID()))

	writeProperties(h, r.Properties())
	return hex.EncodeToString(h.Sum(nil))
}

// writeProperties writes sorted properties to the hasher in a deterministic format.
// PropertySlice is already sorted by key — no re-sort needed.
func writeProperties(h hash.Hash, props types.PropertySlice) {
	for _, p := range props {
		mustWrite(h, uint32(len(p.Key)))
		mustWriteString(h, p.Key)
		writePropertyValue(h, p.Value)
	}
}

// writePropertyValue writes a typed binary representation of a property value to
// the hasher. Each value is prefixed with a type tag byte from wire.go, ensuring
// type-distinct hashing (int(1) vs string("1") produce different hashes).
// Maps sort keys before hashing for deterministic output. []any recurses.
func writePropertyValue(h hash.Hash, v any) {
	tag := propertyTypeTag(v)
	mustWrite(h, tag)

	switch val := v.(type) {
	case bool:
		if val {
			mustWrite(h, byte(1))
		} else {
			mustWrite(h, byte(0))
		}
	case int:
		mustWrite(h, int64(val))
	case int8:
		mustWrite(h, val)
	case int16:
		mustWrite(h, val)
	case int32:
		mustWrite(h, val)
	case int64:
		mustWrite(h, val)
	case uint:
		mustWrite(h, uint64(val))
	case uint8:
		mustWrite(h, val)
	case uint16:
		mustWrite(h, val)
	case uint32:
		mustWrite(h, val)
	case uint64:
		mustWrite(h, val)
	case float32:
		mustWrite(h, val)
	case float64:
		mustWrite(h, val)
	case string:
		mustWrite(h, uint32(len(val)))
		mustWriteString(h, val)
	case []string:
		mustWrite(h, uint32(len(val)))
		for _, s := range val {
			mustWrite(h, uint32(len(s)))
			mustWriteString(h, s)
		}
	case []int:
		mustWrite(h, uint32(len(val)))
		for _, n := range val {
			mustWrite(h, int64(n))
		}
	case []int64:
		mustWrite(h, uint32(len(val)))
		for _, n := range val {
			mustWrite(h, n)
		}
	case []float64:
		mustWrite(h, uint32(len(val)))
		for _, f := range val {
			mustWrite(h, f)
		}
	case []byte:
		mustWrite(h, uint32(len(val)))
		mustWriteString(h, string(val))
	case []bool:
		mustWrite(h, uint32(len(val)))
		for _, b := range val {
			if b {
				mustWrite(h, byte(1))
			} else {
				mustWrite(h, byte(0))
			}
		}
	case []any:
		mustWrite(h, uint32(len(val)))
		for _, elem := range val {
			writePropertyValue(h, elem)
		}
	case map[string]any:
		mustWrite(h, uint32(len(val)))
		keys := make([]string, 0, len(val))
		for k := range val {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			mustWrite(h, uint32(len(k)))
			mustWriteString(h, k)
			writePropertyValue(h, val[k])
		}
	case map[string]string:
		mustWrite(h, uint32(len(val)))
		keys := make([]string, 0, len(val))
		for k := range val {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			mustWrite(h, uint32(len(k)))
			mustWriteString(h, k)
			mustWrite(h, uint32(len(val[k])))
			mustWriteString(h, val[k])
		}
	default:
		// Unknown type: write nothing beyond the tag.
		// This case should not be reachable because PropertySlice.Set()
		// validates types at insertion via the allowlist.
	}
}
