package storeutil

import (
	"fmt"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	"github.com/vmihailenco/msgpack/v5"
)

// This file defines the on-disk body layout for change-log records and the
// framing that wraps a record value. The public taxonomy (store.ChangeTag,
// store.ChangeRecord, store.ChangeFeedCapability) lives in pkg/graph/store; the
// bodies live here because they reference the NodeWire/RelWire wire types.
//
// A persisted change-log value is: tag(1 byte) || msgpack(body). The body shape
// is determined by the tag:
//
//	ChangeNodePut / ChangeRelPut                     -> bare NodeWire / RelWire
//	ChangeNodeDelete                                 -> NodeDeleteBody
//	ChangeRelDelete                                  -> RelDeleteBody
//	ChangeNodeHistoryVersion                         -> HistoryVersionNodeBody
//	ChangeRelHistoryVersion                          -> HistoryVersionRelBody
//	ChangeNodeHistoryTruncate / ChangeRelHistoryTruncate -> HistoryTruncateBody
//	ChangeMeta                                       -> MetaBody
//	ChangeClear                                      -> (no body; empty payload)

// NodeDeleteBody is the ChangeNodeDelete payload. A node delete has two shapes:
//   - hard cascade (WithHistory == false): the node and its connected
//     relationships are removed with no tombstone/history; CascadedRelIDs lists
//     the relationship IDs the cascade removed so a replica deletes them too.
//   - with-history (WithHistory == true): Tombstone is the node tombstone
//     version row and RelTombstones are the connected-relationship tombstone
//     rows; a replica appends them as history.
//
// CascadedRelIDs contains exactly the relationship rows the cascade REMOVED
// (orphan-only adjacency entries with no current row are purged but excluded),
// SORTED ascending — so the record is deterministic and byte-identical across
// backends regardless of internal map-iteration order.
type NodeDeleteBody struct {
	ID             int64     `msgpack:"id"`
	WithHistory    bool      `msgpack:"wh,omitempty"`
	Tombstone      *NodeWire `msgpack:"tn,omitempty"`
	RelTombstones  []RelWire `msgpack:"rt,omitempty"`
	CascadedRelIDs []int64   `msgpack:"cr,omitempty"`
}

// RelDeleteBody is the ChangeRelDelete payload. WithHistory == false is a hard
// delete of the current row; WithHistory == true carries the tombstone version.
type RelDeleteBody struct {
	ID          int64    `msgpack:"id"`
	WithHistory bool     `msgpack:"wh,omitempty"`
	Tombstone   *RelWire `msgpack:"tr,omitempty"`
}

// HistoryVersionNodeBody is the ChangeNodeHistoryVersion payload: a history row
// written at an EXPLICIT version slot (distinct from the current row). Version
// is the slot; Wire is the full node state at that version.
type HistoryVersionNodeBody struct {
	Version uint64   `msgpack:"v"`
	Wire    NodeWire `msgpack:"w"`
}

// HistoryVersionRelBody is the ChangeRelHistoryVersion payload.
type HistoryVersionRelBody struct {
	Version uint64  `msgpack:"v"`
	Wire    RelWire `msgpack:"w"`
}

// HistoryTruncateBody is the ChangeNodeHistoryTruncate / ChangeRelHistoryTruncate
// payload. IsTrim distinguishes the two history-pruning operations: truncate
// keeps the Bound most-recent versions; trim-from drops versions at or above the
// Bound version.
type HistoryTruncateBody struct {
	ID     int64 `msgpack:"id"`
	IsTrim bool  `msgpack:"tr,omitempty"`
	Bound  int64 `msgpack:"b"`
}

// MetaBody is the ChangeMeta payload: a mirrored MetaSet key/value.
type MetaBody struct {
	Key   string `msgpack:"k"`
	Value []byte `msgpack:"v,omitempty"`
}

// EncodeChangeValue frames a change-log record value as tag(1 byte) || payload.
// payload is the msgpack body (already marshaled); ChangeClear passes nil.
func EncodeChangeValue(tag storepkg.ChangeTag, payload []byte) []byte {
	v := make([]byte, 1+len(payload))
	v[0] = byte(tag)
	copy(v[1:], payload)
	return v
}

// SplitChangeValue separates a framed change-log value into its tag and body
// payload. It fails closed with store.ErrCorruptWire on an empty value or an
// unknown tag, so a corrupt or hostile 0x09 row never reaches a decoder that
// could panic on it.
func SplitChangeValue(value []byte) (storepkg.ChangeTag, []byte, error) {
	if len(value) == 0 {
		return 0, nil, fmt.Errorf("%w: empty change-log value", storepkg.ErrCorruptWire)
	}
	tag := storepkg.ChangeTag(value[0])
	if !tag.Valid() {
		return 0, nil, fmt.Errorf("%w: unknown change-log tag %d", storepkg.ErrCorruptWire, value[0])
	}
	return tag, value[1:], nil
}

// MarshalChangeBody msgpack-encodes a change-log body. msgpack.Marshal is
// panic-free, so no SafeUnmarshal-style guard is needed on the encode side.
func MarshalChangeBody(body any) ([]byte, error) {
	return msgpack.Marshal(body)
}

// --- Shared record-payload builders ---
//
// These are the SINGLE source of truth for change-log record bodies, used by
// every backend (badger, memory) so the feeds are byte-identical for the same
// logical mutation. Put/Rel-put records are bare wires (MarshalNodeWire /
// MarshalRelWire); the builders below cover the composite bodies.

// NodeHistoryVersionPayload builds a ChangeNodeHistoryVersion body.
func NodeHistoryVersionPayload(version uint32, n *types.Node) ([]byte, error) {
	w, err := NodeToWireChecked(n)
	if err != nil {
		return nil, err
	}
	return MarshalChangeBody(HistoryVersionNodeBody{Version: uint64(version), Wire: w})
}

// RelHistoryVersionPayload builds a ChangeRelHistoryVersion body.
func RelHistoryVersionPayload(version uint32, r *types.Relationship) ([]byte, error) {
	w, err := RelToWireChecked(r)
	if err != nil {
		return nil, err
	}
	return MarshalChangeBody(HistoryVersionRelBody{Version: uint64(version), Wire: w})
}

// NodeDeleteWithHistoryPayload builds a with-history ChangeNodeDelete body: the
// node tombstone, the connected-relationship tombstones, and the cascaded rel IDs.
func NodeDeleteWithHistoryPayload(id snowflake.ID, nodeTombstone *types.Node, relTombstones []storepkg.RelTombstone) ([]byte, error) {
	tw, err := NodeToWireChecked(nodeTombstone)
	if err != nil {
		return nil, err
	}
	body := NodeDeleteBody{ID: int64(id), WithHistory: true, Tombstone: &tw}
	for _, rt := range relTombstones {
		rw, err := RelToWireChecked(rt.Tombstone)
		if err != nil {
			return nil, err
		}
		body.RelTombstones = append(body.RelTombstones, rw)
		body.CascadedRelIDs = append(body.CascadedRelIDs, int64(rt.ID.SnowflakeID()))
	}
	return MarshalChangeBody(body)
}

// RelDeleteWithHistoryPayload builds a with-history ChangeRelDelete body.
func RelDeleteWithHistoryPayload(id snowflake.ID, tombstone *types.Relationship) ([]byte, error) {
	tw, err := RelToWireChecked(tombstone)
	if err != nil {
		return nil, err
	}
	return MarshalChangeBody(RelDeleteBody{ID: int64(id), WithHistory: true, Tombstone: &tw})
}

// --- Decoders (used by the replica-apply path; all fail closed) ---
//
// Every decoder routes through SafeUnmarshal: a corrupt or hostile body must
// return store.ErrCorruptWire, never panic the process (lesson 47).

// DecodeNodePut decodes a ChangeNodePut payload.
func DecodeNodePut(payload []byte) (NodeWire, error) {
	var w NodeWire
	if err := SafeUnmarshal(payload, &w); err != nil {
		return NodeWire{}, fmt.Errorf("change-log: node put body: %w", err)
	}
	return w, nil
}

// DecodeRelPut decodes a ChangeRelPut payload.
func DecodeRelPut(payload []byte) (RelWire, error) {
	var w RelWire
	if err := SafeUnmarshal(payload, &w); err != nil {
		return RelWire{}, fmt.Errorf("change-log: rel put body: %w", err)
	}
	return w, nil
}

// DecodeNodeDelete decodes a ChangeNodeDelete payload.
func DecodeNodeDelete(payload []byte) (NodeDeleteBody, error) {
	var b NodeDeleteBody
	if err := SafeUnmarshal(payload, &b); err != nil {
		return NodeDeleteBody{}, fmt.Errorf("change-log: node delete body: %w", err)
	}
	return b, nil
}

// DecodeRelDelete decodes a ChangeRelDelete payload.
func DecodeRelDelete(payload []byte) (RelDeleteBody, error) {
	var b RelDeleteBody
	if err := SafeUnmarshal(payload, &b); err != nil {
		return RelDeleteBody{}, fmt.Errorf("change-log: rel delete body: %w", err)
	}
	return b, nil
}

// DecodeHistoryVersionNode decodes a ChangeNodeHistoryVersion payload.
func DecodeHistoryVersionNode(payload []byte) (HistoryVersionNodeBody, error) {
	var b HistoryVersionNodeBody
	if err := SafeUnmarshal(payload, &b); err != nil {
		return HistoryVersionNodeBody{}, fmt.Errorf("change-log: node history-version body: %w", err)
	}
	return b, nil
}

// DecodeHistoryVersionRel decodes a ChangeRelHistoryVersion payload.
func DecodeHistoryVersionRel(payload []byte) (HistoryVersionRelBody, error) {
	var b HistoryVersionRelBody
	if err := SafeUnmarshal(payload, &b); err != nil {
		return HistoryVersionRelBody{}, fmt.Errorf("change-log: rel history-version body: %w", err)
	}
	return b, nil
}

// DecodeHistoryTruncate decodes a ChangeNodeHistoryTruncate / ChangeRelHistoryTruncate payload.
func DecodeHistoryTruncate(payload []byte) (HistoryTruncateBody, error) {
	var b HistoryTruncateBody
	if err := SafeUnmarshal(payload, &b); err != nil {
		return HistoryTruncateBody{}, fmt.Errorf("change-log: history-truncate body: %w", err)
	}
	return b, nil
}

// DecodeMeta decodes a ChangeMeta payload.
func DecodeMeta(payload []byte) (MetaBody, error) {
	var b MetaBody
	if err := SafeUnmarshal(payload, &b); err != nil {
		return MetaBody{}, fmt.Errorf("change-log: meta body: %w", err)
	}
	return b, nil
}
