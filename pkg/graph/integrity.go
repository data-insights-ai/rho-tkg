package graph

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"hash"
	"io"
	"sort"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3/pkg/types"
)

// VerifyNodeHashChain verifies the full hash chain for a node.
// Returns (true, nil) if the chain is valid. Returns (false, nil) if a hash
// mismatch or broken PrevHash link is detected. Returns (false, err) on I/O
// failure or if the node does not exist (ErrNodeNotFound).
func (g *Graph) VerifyNodeHashChain(id snowflake.ID) (bool, error) {
	current, err := g.store.GetNode(id)
	if err != nil {
		return false, err
	}

	history, err := g.store.GetNodeHistory(id)
	if err != nil {
		return false, err
	}

	// Build chain: history (ascending version order) + current.
	chain := make([]*types.Node, 0, len(history)+1)
	chain = append(chain, history...)
	chain = append(chain, current)

	labels := g.NodeLabels(current)

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
// failure or if the relationship does not exist (ErrRelNotFound).
func (g *Graph) VerifyRelHashChain(id snowflake.ID) (bool, error) {
	current, err := g.store.GetRelationship(id)
	if err != nil {
		return false, err
	}

	history, err := g.store.GetRelHistory(id)
	if err != nil {
		return false, err
	}

	// Build chain: history (ascending version order) + current.
	chain := make([]*types.Relationship, 0, len(history)+1)
	chain = append(chain, history...)
	chain = append(chain, current)

	typeName := g.RelationshipType(current)

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
