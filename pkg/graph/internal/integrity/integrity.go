// Package integrity provides the SHA-256 content-hashing primitives that the
// graph layer uses for entity-version integrity (`tkg_hash`/`tkg_prev_hash`).
//
// Hash output is part of the on-disk format: any change to the byte layout
// invalidates every existing `Hash`/`PrevHash` value persisted across stores
// and would break `Graph.Hash().VerifyNodeChain` / `Graph.Hash().VerifyRelChain`.
// Treat the byte layout as a versioned wire format.
package integrity

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

const (
	initialHashBufferCap   = 256
	maxPooledHashBufferCap = 256 * 1024
)

// hashBufPool provides reusable byte buffers for hash input construction.
// Buffers are grown as needed and returned to the pool after use, amortizing
// allocation cost to near zero in steady state.
var hashBufPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 0, initialHashBufferCap)
		return &buf
	},
}

func putHashBuffer(bp *[]byte, buf []byte) {
	if cap(buf) > maxPooledHashBufferCap {
		buf = make([]byte, 0, initialHashBufferCap)
	} else {
		buf = buf[:0]
	}
	*bp = buf
	hashBufPool.Put(bp)
}

// ComputeNodeHash computes a SHA-256 hash of the node's content.
// The hash covers: id, version, sorted labels, and sorted properties.
// Returns the hex-encoded hash string (64 characters).
func ComputeNodeHash(n *types.Node, labels []string) string {
	if n == nil {
		panic(types.ErrNilNode)
	}
	return computeNodeHash(n, labels)
}

// ComputeNodeHashChecked computes a SHA-256 hash of the node's content and
// returns property hashing failures as errors instead of panicking.
func ComputeNodeHashChecked(n *types.Node, labels []string) (hash string, err error) {
	if n == nil {
		return "", types.ErrNilNode
	}
	if n.PropertyHashNeedsRecover() {
		return computeNodeHashCheckedWithRecover(n, labels)
	}
	return computeNodeHash(n, labels), nil
}

func computeNodeHashCheckedWithRecover(n *types.Node, labels []string) (hash string, err error) {
	bp := hashBufPool.Get().(*[]byte)
	buf := (*bp)[:0]
	defer func() {
		putHashBuffer(bp, buf)
		if r := recover(); r != nil {
			hash = ""
			err = fmt.Errorf("%w: compute node hash panic: %v", types.ErrUnsupportedValueType, r)
		}
	}()
	hash, buf = computeNodeHashWithBuffer(buf, n, labels)
	return hash, nil
}

func computeNodeHash(n *types.Node, labels []string) string {
	bp := hashBufPool.Get().(*[]byte)
	buf := (*bp)[:0]
	hash, buf := computeNodeHashWithBuffer(buf, n, labels)

	putHashBuffer(bp, buf)

	return hash
}

func computeNodeHashWithBuffer(buf []byte, n *types.Node, labels []string) (string, []byte) {
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

	buf = n.AppendPropertyHashBytes(buf)

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
	return computeRelHash(r, typeName)
}

// ComputeRelHashChecked computes a SHA-256 hash of the relationship's content
// and returns property hashing failures as errors instead of panicking.
func ComputeRelHashChecked(r *types.Relationship, typeName string) (hash string, err error) {
	if r == nil {
		return "", types.ErrNilRelationship
	}
	if r.PropertyHashNeedsRecover() {
		return computeRelHashCheckedWithRecover(r, typeName)
	}
	return computeRelHash(r, typeName), nil
}

func computeRelHashCheckedWithRecover(r *types.Relationship, typeName string) (hash string, err error) {
	bp := hashBufPool.Get().(*[]byte)
	buf := (*bp)[:0]
	defer func() {
		putHashBuffer(bp, buf)
		if recovered := recover(); recovered != nil {
			hash = ""
			err = fmt.Errorf("%w: compute relationship hash panic: %v", types.ErrUnsupportedValueType, recovered)
		}
	}()
	hash, buf = computeRelHashWithBuffer(buf, r, typeName)
	return hash, nil
}

func computeRelHash(r *types.Relationship, typeName string) string {
	bp := hashBufPool.Get().(*[]byte)
	buf := (*bp)[:0]
	hash, buf := computeRelHashWithBuffer(buf, r, typeName)

	putHashBuffer(bp, buf)

	return hash
}

func computeRelHashWithBuffer(buf []byte, r *types.Relationship, typeName string) (string, []byte) {
	buf = binary.BigEndian.AppendUint64(buf, uint64(r.ID().SnowflakeID())) // #nosec G115 — snowflake IDs use 63 bits
	buf = binary.BigEndian.AppendUint32(buf, r.Version())
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(typeName))) // #nosec G115 — type name bounded by MaxNameLength (256)
	buf = append(buf, typeName...)
	buf = binary.BigEndian.AppendUint64(buf, uint64(r.StartNodeID().SnowflakeID())) // #nosec G115 — snowflake IDs use 63 bits
	buf = binary.BigEndian.AppendUint64(buf, uint64(r.EndNodeID().SnowflakeID()))   // #nosec G115 — snowflake IDs use 63 bits

	buf = r.AppendPropertyHashBytes(buf)

	sum := sha256.Sum256(buf)
	var hexBuf [64]byte
	hex.Encode(hexBuf[:], sum[:])

	return string(hexBuf[:]), buf
}

func propertySliceNeedsHashRecover(props types.PropertySlice) bool {
	for _, p := range props {
		if types.PropertyValueHashNeedsRecover(p.Value) {
			return true
		}
	}
	return false
}

func propertyValueNeedsHashRecover(v any) bool {
	return types.PropertyValueHashNeedsRecover(v)
}

// appendPropertyValue appends a typed binary representation of a property value
// to buf. Each value is prefixed with a type tag byte from wire.go, ensuring
// type-distinct hashing (int(1) vs string("1") produce different hashes).
// Maps sort keys before hashing for deterministic output. []any recurses.
func appendPropertyValue(buf []byte, v any) []byte {
	return types.AppendPropertyValueHashBytes(buf, v)
}
