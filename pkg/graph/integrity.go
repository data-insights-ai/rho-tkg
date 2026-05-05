package graph

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// VerifyNodeHashChain verifies the full hash chain for a node.
// Returns (true, nil) if the chain is valid. Returns (false, nil) if a hash
// mismatch or broken PrevHash link is detected. Returns (false, err) on I/O
// failure or if the node never existed (no current entity AND no history).
//
// Handles deleted entities: if the current node is gone (ErrNodeNotFound) but
// history exists, verifies the history chain alone. Labels are extracted from
// the last history entry's internal tokens.
func (g *Graph) VerifyNodeHashChain(id types.NodeID) (bool, error) {
	current, err := g.store.GetNode(id)
	if err != nil && !errors.Is(err, ErrNodeNotFound) {
		return false, err
	}
	// current may be nil for deleted entities.

	history, err := g.store.GetNodeHistory(id)
	if err != nil {
		return false, err
	}

	if current == nil && len(history) == 0 {
		return false, ErrNodeNotFound
	}

	// Build chain: history (ascending version order) + current (if exists).
	chain := make([]*types.Node, 0, len(history)+1)
	chain = append(chain, history...)
	if current != nil {
		chain = append(chain, current)
	}

	for i, entry := range chain {
		ig := entry.Integrity()
		if ig == nil {
			return false, nil
		}

		if entry.Version() == 0 {
			// Genesis: PrevHash must be empty.
			if ig.PrevHash != "" {
				return false, nil
			}
		} else if i > 0 {
			// Non-genesis with predecessor in chain: verify PrevHash link.
			prevIG := chain[i-1].Integrity()
			if prevIG == nil {
				return false, nil
			}
			if ig.PrevHash != prevIG.Hash {
				return false, nil
			}
		}
		// else: i == 0 && version != 0 → truncated history, skip link check.
		// Hash recomputation below still verifies content integrity.

		// Recompute hash and compare with stored.
		labels := g.NodeLabels(entry)
		computed := ComputeNodeHash(entry, labels)
		if ig.Hash != computed {
			return false, nil
		}
	}

	return true, nil
}

// VerifyRelHashChain verifies the full hash chain for a relationship.
// Returns (true, nil) if the chain is valid. Returns (false, nil) if a hash
// mismatch or broken PrevHash link is detected. Returns (false, err) on I/O
// failure or if the relationship never existed (no current AND no history).
//
// Handles deleted entities: if the current relationship is gone (ErrRelNotFound)
// but history exists, verifies the history chain alone.
func (g *Graph) VerifyRelHashChain(id types.RelID) (bool, error) {
	current, err := g.store.GetRelationship(id)
	if err != nil && !errors.Is(err, ErrRelNotFound) {
		return false, err
	}
	// current may be nil for deleted entities.

	history, err := g.store.GetRelHistory(id)
	if err != nil {
		return false, err
	}

	if current == nil && len(history) == 0 {
		return false, ErrRelNotFound
	}

	// Build chain: history (ascending version order) + current (if exists).
	chain := make([]*types.Relationship, 0, len(history)+1)
	chain = append(chain, history...)
	if current != nil {
		chain = append(chain, current)
	}

	// Extract type name from the best available source.
	typeSource := current
	if typeSource == nil {
		typeSource = chain[len(chain)-1]
	}
	typeName := g.RelationshipType(typeSource)

	for i, entry := range chain {
		ig := entry.Integrity()
		if ig == nil {
			return false, nil
		}

		if entry.Version() == 0 {
			// Genesis: PrevHash must be empty.
			if ig.PrevHash != "" {
				return false, nil
			}
		} else if i > 0 {
			// Non-genesis with predecessor in chain: verify PrevHash link.
			prevIG := chain[i-1].Integrity()
			if prevIG == nil {
				return false, nil
			}
			if ig.PrevHash != prevIG.Hash {
				return false, nil
			}
		}
		// else: i == 0 && version != 0 → truncated history, skip link check.

		// Recompute hash and compare with stored.
		computed := ComputeRelHash(entry, typeName)
		if ig.Hash != computed {
			return false, nil
		}
	}

	return true, nil
}

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
	bp := hashBufPool.Get().(*[]byte)
	buf := (*bp)[:0]

	buf = binary.BigEndian.AppendUint64(buf, uint64(n.ID().SnowflakeID())) // #nosec G115 — snowflake IDs use 63 bits
	buf = binary.BigEndian.AppendUint32(buf, n.Version())

	// Defensive sort — caller may pass unsorted labels.
	sorted := make([]string, len(labels))
	copy(sorted, labels)
	sort.Strings(sorted)

	for _, label := range sorted {
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(label))) // #nosec G115 — label length bounded by MaxNameLength (256)
		buf = append(buf, label...)
	}

	buf = appendProperties(buf, n.Properties())

	sum := sha256.Sum256(buf)
	var hexBuf [64]byte
	hex.Encode(hexBuf[:], sum[:])

	*bp = buf
	hashBufPool.Put(bp)

	return string(hexBuf[:])
}

// ComputeRelHash computes a SHA-256 hash of the relationship's content.
// The hash covers: id, version, type name, start ID, end ID, and sorted properties.
// Returns the hex-encoded hash string (64 characters).
func ComputeRelHash(r *types.Relationship, typeName string) string {
	bp := hashBufPool.Get().(*[]byte)
	buf := (*bp)[:0]

	buf = binary.BigEndian.AppendUint64(buf, uint64(r.ID().SnowflakeID())) // #nosec G115 — snowflake IDs use 63 bits
	buf = binary.BigEndian.AppendUint32(buf, r.Version())
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(typeName))) // #nosec G115 — type name bounded by MaxNameLength (256)
	buf = append(buf, typeName...)
	buf = binary.BigEndian.AppendUint64(buf, uint64(r.StartNodeID().SnowflakeID())) // #nosec G115 — snowflake IDs use 63 bits
	buf = binary.BigEndian.AppendUint64(buf, uint64(r.EndNodeID().SnowflakeID()))   // #nosec G115 — snowflake IDs use 63 bits

	buf = appendProperties(buf, r.Properties())

	sum := sha256.Sum256(buf)
	var hexBuf [64]byte
	hex.Encode(hexBuf[:], sum[:])

	*bp = buf
	hashBufPool.Put(bp)

	return string(hexBuf[:])
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
	buf = append(buf, propertyTypeTag(v))

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
