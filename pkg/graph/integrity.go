package graph

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"sort"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3/pkg/types"
)

// ComputeNodeHash computes a SHA-256 hash of the node's content.
// The hash covers: id, version, sorted labels, and sorted properties.
// Returns the hex-encoded hash string (64 characters).
func ComputeNodeHash(n *types.Node, labels []string) string {
	h := sha256.New()

	_ = binary.Write(h, binary.BigEndian, int64(n.InternalID().SnowflakeID()))
	_ = binary.Write(h, binary.BigEndian, n.Version())

	// Defensive sort — caller may pass unsorted labels.
	sorted := make([]string, len(labels))
	copy(sorted, labels)
	sort.Strings(sorted)

	for _, label := range sorted {
		_ = binary.Write(h, binary.BigEndian, uint32(len(label)))
		_, _ = io.WriteString(h, label)
	}

	writeProperties(h, n.Properties())
	return hex.EncodeToString(h.Sum(nil))
}

// ComputeRelHash computes a SHA-256 hash of the relationship's content.
// The hash covers: id, version, type name, start ID, end ID, and sorted properties.
// Returns the hex-encoded hash string (64 characters).
func ComputeRelHash(r *types.Relationship, typeName string) string {
	h := sha256.New()

	_ = binary.Write(h, binary.BigEndian, int64(r.InternalID().SnowflakeID()))
	_ = binary.Write(h, binary.BigEndian, r.Version())
	_ = binary.Write(h, binary.BigEndian, uint32(len(typeName)))
	_, _ = io.WriteString(h, typeName)
	_ = binary.Write(h, binary.BigEndian, int64(r.StartNodeID().SnowflakeID()))
	_ = binary.Write(h, binary.BigEndian, int64(r.EndNodeID().SnowflakeID()))

	writeProperties(h, r.Properties())
	return hex.EncodeToString(h.Sum(nil))
}

// writeProperties writes sorted properties to the hasher in a deterministic format.
// PropertySlice is already sorted by key — no re-sort needed.
func writeProperties(h hash.Hash, props types.PropertySlice) {
	for _, p := range props {
		_ = binary.Write(h, binary.BigEndian, uint32(len(p.Key)))
		_, _ = io.WriteString(h, p.Key)
		valStr := fmt.Sprintf("%v", p.Value)
		_ = binary.Write(h, binary.BigEndian, uint32(len(valStr)))
		_, _ = io.WriteString(h, valStr)
	}
}
