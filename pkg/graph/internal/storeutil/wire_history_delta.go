package storeutil

import (
	"bytes"
	"fmt"
	"sort"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/vmihailenco/msgpack/v5"
)

// ADR-0009 / B6 — anchor+delta version-history storage.
//
// Version-history rows (badger 0x07/0x08) were full entity snapshots: for a wide
// entity (5..20 properties, some with large unchanged values) every version bump
// re-serialized the whole blob. Anchor+delta storage keeps a FULL ANCHOR every
// HistoryAnchorInterval versions and, between anchors, stores a DELTA that carries
// only the properties that CHANGED vs the interval's anchor — the large unchanged
// values are stored once per interval, not once per version. Measured 39% less
// history storage after block-Snappy on history-heavy wide entities
// (wire_b6_history_gate_test.go).
//
// On-disk framing (self-describing, zero migration):
//   - ANCHOR row: the raw full-snapshot marshal, UNCHANGED. A msgpack map header
//     (0x80..0x8f / 0xde / 0xdf) is its first byte, so a legacy pre-B6 row is
//     transparently an anchor — no per-row version flag, no backfill.
//   - DELTA row: a 1-byte historyDeltaTag ('D' = 0x44) prefix + msgpack(delta).
//     0x44 is never a msgpack map header, so the first byte alone disambiguates.
//
// Reconstruction correctness vs byte-exactness: Apply need only reproduce the
// correct *entity* (the decoder's PropertySlice re-sorts and Verify*Chain
// recomputes the content hash from canonical state), NOT byte-identical wire.
// Replica byte-exactness instead rides on Diff being DETERMINISTIC: primary and
// replica compute the same delta bytes from LSN-ordered-identical (anchor, target)
// inputs. Diff therefore sorts ps/pr by a total identity order.

// HistoryAnchorInterval is the version spacing between full anchors. A version V
// with V%HistoryAnchorInterval == 0 is stored full; the rest are deltas against
// the nearest lower anchor (V - V%HistoryAnchorInterval). Random-access
// reconstruction is bounded to two point reads (anchor + target delta).
const HistoryAnchorInterval = 16

// historyDeltaTag prefixes a delta history value. 'D' (0x44) can never be the
// first byte of a msgpack map, so it disambiguates delta from anchor/legacy.
const historyDeltaTag = 'D'

// AnchorVersionFor returns the anchor version governing version v.
func AnchorVersionFor(v uint64) uint64 { return v - v%HistoryAnchorInterval }

// IsAnchorVersion reports whether version v is stored as a full anchor.
func IsAnchorVersion(v uint64) bool { return v%HistoryAnchorInterval == 0 }

// HistoryKind classifies a raw history value's storage form.
type HistoryKind uint8

const (
	// HistoryFull is a full snapshot: a new anchor OR a legacy (untagged) row.
	HistoryFull HistoryKind = iota
	// HistoryDelta is a delta against its interval anchor.
	HistoryDelta
)

// HistoryValueKindOf classifies a raw history value by its first byte. An empty
// value is treated as HistoryFull (the decoder then fails on the empty payload,
// preserving existing error behavior).
func HistoryValueKindOf(raw []byte) HistoryKind {
	if len(raw) > 0 && raw[0] == historyDeltaTag {
		return HistoryDelta
	}
	return HistoryFull
}

// propKeyRef identifies a property by its wire key. A tokenized (v2) row uses
// Token (Key == ""); a v1 row uses Key (Token == 0). Both are consistent within
// one marshaled row, so identity comparison is well-defined.
type propKeyRef struct {
	Token uint16 `msgpack:"kt,omitempty"`
	Key   string `msgpack:"k,omitempty"`
}

func propRefOf(p PropertyWire) propKeyRef { return propKeyRef{Token: p.KeyToken, Key: p.Key} }

// lessPropRef is the total order used to make Diff output deterministic.
func lessPropRef(a, b propKeyRef) bool {
	if a.Token != b.Token {
		return a.Token < b.Token
	}
	return a.Key < b.Key
}

// NodeHistoryDelta is the delta payload for a node version vs its anchor.
type NodeHistoryDelta struct {
	// Meta is the target version's full wire with Properties elided — every
	// non-property field (temporal, both hashes, labels, version) verbatim.
	Meta NodeWire `msgpack:"m"`
	// PS holds properties present-and-changed or added vs the anchor.
	PS []PropertyWire `msgpack:"ps,omitempty"`
	// PR holds identities of anchor properties absent from the target.
	PR []propKeyRef `msgpack:"pr,omitempty"`
}

// RelHistoryDelta mirrors NodeHistoryDelta for relationships.
type RelHistoryDelta struct {
	Meta RelWire        `msgpack:"m"`
	PS   []PropertyWire `msgpack:"ps,omitempty"`
	PR   []propKeyRef   `msgpack:"pr,omitempty"`
}

// diffProperties computes (added-or-changed, removed) between an anchor and a
// target property slice, byte-comparing each property's marshal so "changed"
// means "different on the wire" (over-inclusion is impossible, under-inclusion
// would be a correctness bug). Output is sorted by identity for determinism.
func diffProperties(anchor, target []PropertyWire) (ps []PropertyWire, pr []propKeyRef) {
	anchorBytes := make(map[propKeyRef][]byte, len(anchor))
	for i := range anchor {
		b, err := msgpack.Marshal(anchor[i])
		if err != nil {
			// A property that cannot marshal here would also fail the row marshal;
			// treat it as absent so the target copy (in ps) becomes authoritative.
			continue
		}
		anchorBytes[propRefOf(anchor[i])] = b
	}
	targetSet := make(map[propKeyRef]struct{}, len(target))
	for i := range target {
		ref := propRefOf(target[i])
		targetSet[ref] = struct{}{}
		prev, ok := anchorBytes[ref]
		if !ok {
			ps = append(ps, target[i])
			continue
		}
		cur, err := msgpack.Marshal(target[i])
		if err != nil || !bytes.Equal(prev, cur) {
			ps = append(ps, target[i])
		}
	}
	for ref := range anchorBytes {
		if _, ok := targetSet[ref]; !ok {
			pr = append(pr, ref)
		}
	}
	sort.Slice(ps, func(i, j int) bool { return lessPropRef(propRefOf(ps[i]), propRefOf(ps[j])) })
	sort.Slice(pr, func(i, j int) bool { return lessPropRef(pr[i], pr[j]) })
	return ps, pr
}

// applyProperties reconstructs the target property set from the anchor set plus
// the delta's ps/pr. Output order is the identity order (the entity decoder
// re-sorts, so wire order is immaterial to correctness).
func applyProperties(anchor, ps []PropertyWire, pr []propKeyRef) []PropertyWire {
	removed := make(map[propKeyRef]struct{}, len(pr))
	for _, r := range pr {
		removed[r] = struct{}{}
	}
	merged := make(map[propKeyRef]PropertyWire, len(anchor)+len(ps))
	for i := range anchor {
		ref := propRefOf(anchor[i])
		if _, gone := removed[ref]; gone {
			continue
		}
		merged[ref] = anchor[i]
	}
	for i := range ps {
		merged[propRefOf(ps[i])] = ps[i]
	}
	if len(merged) == 0 {
		return nil
	}
	refs := make([]propKeyRef, 0, len(merged))
	for ref := range merged {
		refs = append(refs, ref)
	}
	sort.Slice(refs, func(i, j int) bool { return lessPropRef(refs[i], refs[j]) })
	out := make([]PropertyWire, 0, len(refs))
	for _, ref := range refs {
		out = append(out, merged[ref])
	}
	return out
}

// SortWirePropertiesByKey sorts properties into the canonical key-string
// ascending order the entity decoder validates (WireToNode* rejects unsorted
// rows). Reconstruction produces properties in token-identity order, which only
// matches key order when tokens were assigned alphabetically; callers resolve
// KeyToken→Key first, then sort here. Ties (all resolved) break by KeyToken.
func SortWirePropertiesByKey(props []PropertyWire) {
	sort.SliceStable(props, func(i, j int) bool {
		if props[i].Key != props[j].Key {
			return props[i].Key < props[j].Key
		}
		return props[i].KeyToken < props[j].KeyToken
	})
}

// DiffNodeHistory builds the delta for target vs anchor.
func DiffNodeHistory(anchor, target NodeWire) NodeHistoryDelta {
	meta := target
	meta.Properties = nil
	ps, pr := diffProperties(anchor.Properties, target.Properties)
	return NodeHistoryDelta{Meta: meta, PS: ps, PR: pr}
}

// ApplyNodeHistory reconstructs a target wire from its anchor and delta.
func ApplyNodeHistory(anchor NodeWire, d NodeHistoryDelta) NodeWire {
	w := d.Meta
	w.Properties = applyProperties(anchor.Properties, d.PS, d.PR)
	return w
}

// DiffRelHistory / ApplyRelHistory mirror the node functions.
func DiffRelHistory(anchor, target RelWire) RelHistoryDelta {
	meta := target
	meta.Properties = nil
	ps, pr := diffProperties(anchor.Properties, target.Properties)
	return RelHistoryDelta{Meta: meta, PS: ps, PR: pr}
}

func ApplyRelHistory(anchor RelWire, d RelHistoryDelta) RelWire {
	w := d.Meta
	w.Properties = applyProperties(anchor.Properties, d.PS, d.PR)
	return w
}

// EncodeNodeHistoryDelta returns the on-disk delta value: historyDeltaTag + the
// msgpack payload. Deterministic given (anchor, target).
func EncodeNodeHistoryDelta(d NodeHistoryDelta) ([]byte, error) {
	body, err := msgpack.Marshal(d)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, len(body)+1)
	out = append(out, historyDeltaTag)
	return append(out, body...), nil
}

// DecodeNodeHistoryDelta decodes a tagged delta value produced by
// EncodeNodeHistoryDelta. It rejects a value that is not a delta (missing tag).
func DecodeNodeHistoryDelta(raw []byte) (NodeHistoryDelta, error) {
	var d NodeHistoryDelta
	if HistoryValueKindOf(raw) != HistoryDelta {
		return d, fmt.Errorf("%w: history value is not a delta", storepkg.ErrCorruptWire)
	}
	if err := SafeUnmarshal(raw[1:], &d); err != nil {
		return d, err
	}
	return d, nil
}

// EncodeRelHistoryDelta / DecodeRelHistoryDelta mirror the node functions.
func EncodeRelHistoryDelta(d RelHistoryDelta) ([]byte, error) {
	body, err := msgpack.Marshal(d)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, len(body)+1)
	out = append(out, historyDeltaTag)
	return append(out, body...), nil
}

func DecodeRelHistoryDelta(raw []byte) (RelHistoryDelta, error) {
	var d RelHistoryDelta
	if HistoryValueKindOf(raw) != HistoryDelta {
		return d, fmt.Errorf("%w: history value is not a delta", storepkg.ErrCorruptWire)
	}
	if err := SafeUnmarshal(raw[1:], &d); err != nil {
		return d, err
	}
	return d, nil
}
