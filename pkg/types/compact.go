package types

import "encoding/hex"

// Compact frozen metadata (P7).
//
// A store's cached rows are frozen: nothing mutates them, and the accessors
// already hand out copies of their temporal and integrity metadata
// (Temporal() and Integrity() copy on a frozen entity). So a frozen row does
// not need the two separate metadata objects an entity under construction
// carries (TemporalMetadata, 96 B; RelIntegrity, 128 B, plus the 64-byte hex
// hash string; NodeIntegrity, 96 B). CompactFrozenCopy packs what a first
// version uses into one small object instead:
//
//   - relationship (96 B with the allocator's rounding, was 288 B): valid
//     from/to, tx from, the content hash as 32 raw bytes, the two endpoint
//     hashes (strings shared with the endpoint nodes' rows);
//   - node (48 B, was 192 B): valid from/to, tx from, the content hash string
//     (kept a string because every relationship written while the node is
//     current shares it as its endpoint hash).
//
// Everything else keeps today's two objects: a row with any other temporal
// field set (tx to, created/updated/deleted at, created/updated by, a base
// entity — i.e. updated, closed, deleted and history versions), a previous
// hash, provenance (author, signature, authorization), a hash that is not
// 64 lowercase hex characters, or missing temporal or integrity metadata. The
// compact form is an exact re-encoding or it is not used.
//
// Invariant: meta != nil implies frozen, temporal == nil and integrity == nil.
// Every accessor rebuilds the public structs from it, so callers see the same
// values, the same nil-ness and the same hash strings as before.

// relMeta is a frozen first-version relationship's temporal + integrity
// metadata.
type relMeta struct {
	validFrom, validTo, txFrom Instant
	hash                       [32]byte
	fromNodeHash, toNodeHash   string
}

// nodeMeta is a frozen first-version node's temporal + integrity metadata.
type nodeMeta struct {
	validFrom, validTo, txFrom Instant
	hash                       string
}

// decodeCanonicalHash decodes a 64-character lowercase hex hash into dst. It
// reports false for anything hex.EncodeToString would not produce, so the
// round trip back to a string is exact.
func decodeCanonicalHash(s string, dst *[32]byte) bool {
	if len(s) != 64 {
		return false
	}
	for i := 0; i < 32; i++ {
		hi, ok1 := lowerHexNibble(s[2*i])
		lo, ok2 := lowerHexNibble(s[2*i+1])
		if !ok1 || !ok2 {
			return false
		}
		dst[i] = hi<<4 | lo
	}
	return true
}

func lowerHexNibble(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	}
	return 0, false
}

// temporalFitsCompact reports whether tm sets no field beyond valid from/to
// and tx from.
func temporalFitsCompact(tm *TemporalMetadata) bool {
	return tm.TxTo == 0 && tm.CreatedAt == 0 && tm.UpdatedAt == 0 && tm.DeletedAt == 0 &&
		tm.CreatedBy == "" && tm.UpdatedBy == "" && tm.baseEntityID == 0
}

// compactRelMeta packs tm and ig, or reports false when the compact form
// cannot represent them exactly.
func compactRelMeta(tm *TemporalMetadata, ig *RelIntegrity) (*relMeta, bool) {
	if tm == nil || ig == nil || !temporalFitsCompact(tm) || ig.PrevHash != "" ||
		ig.AuthorID != "" || ig.Signature != nil || ig.AuthorizedBy != "" || ig.AuthorizationLevel != 0 {
		return nil, false
	}
	m := &relMeta{validFrom: tm.ValidFrom, validTo: tm.ValidTo, txFrom: tm.TxFrom, fromNodeHash: ig.FromNodeHash, toNodeHash: ig.ToNodeHash}
	if !decodeCanonicalHash(ig.Hash, &m.hash) {
		return nil, false
	}
	return m, true
}

// integrity rebuilds the RelIntegrity the row was compacted from: a fresh,
// caller-owned object with the same hash strings.
func (m *relMeta) integrity() *RelIntegrity {
	return &RelIntegrity{Hash: hex.EncodeToString(m.hash[:]), FromNodeHash: m.fromNodeHash, ToNodeHash: m.toNodeHash}
}

// compactNodeMeta packs tm and ig, or reports false when the compact form
// cannot represent them exactly.
func compactNodeMeta(tm *TemporalMetadata, ig *NodeIntegrity) (*nodeMeta, bool) {
	if tm == nil || ig == nil || !temporalFitsCompact(tm) || ig.Hash == "" || ig.PrevHash != "" ||
		ig.AuthorID != "" || ig.Signature != nil || ig.AuthorizedBy != "" || ig.AuthorizationLevel != 0 {
		return nil, false
	}
	return &nodeMeta{validFrom: tm.ValidFrom, validTo: tm.ValidTo, txFrom: tm.TxFrom, hash: ig.Hash}, true
}

// CompactFrozenCopy returns a frozen, independent copy of r in the compact
// form stores cache (see the comment at the top of compact.go). It answers
// every accessor exactly as r.DeepCopy() followed by Freeze() would, and
// DeepCopy on it returns the ordinary mutable form. Nil-safe.
func (r *Relationship) CompactFrozenCopy() *Relationship {
	if r == nil {
		return nil
	}
	cp := &Relationship{
		id:         r.id,
		startID:    r.startID,
		endID:      r.endID,
		relType:    r.relType,
		version:    r.version,
		properties: r.properties.DeepCopy(),
		frozen:     true,
	}
	switch {
	case r.meta != nil:
		cp.meta = r.meta // immutable once built
	default:
		if m, ok := compactRelMeta(r.temporal, r.integrity); ok {
			cp.meta = m
			break
		}
		if r.temporal != nil {
			tm := *r.temporal
			cp.temporal = &tm
		}
		cp.integrity = r.integrity.DeepCopy()
	}
	return cp
}

// CompactFrozenCopy is the node counterpart of
// (*Relationship).CompactFrozenCopy. Nil-safe.
func (n *Node) CompactFrozenCopy() *Node {
	if n == nil {
		return nil
	}
	cp := &Node{
		id:           n.id,
		primaryLabel: n.primaryLabel,
		version:      n.version,
		properties:   n.properties.DeepCopy(),
		frozen:       true,
	}
	if len(n.extraLabels) > 0 {
		cp.extraLabels = make([]labelToken, len(n.extraLabels))
		copy(cp.extraLabels, n.extraLabels)
	}
	switch {
	case n.meta != nil:
		cp.meta = n.meta // immutable once built
	default:
		if m, ok := compactNodeMeta(n.temporal, n.integrity); ok {
			cp.meta = m
			break
		}
		if n.temporal != nil {
			tm := *n.temporal
			cp.temporal = &tm
		}
		cp.integrity = n.integrity.DeepCopy()
	}
	return cp
}
