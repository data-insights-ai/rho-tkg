package storeutil

import (
	"fmt"
	"sync/atomic"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

const (
	maxWireUint16 = 1<<16 - 1
	maxWireUint32 = 1<<32 - 1
)

// CurrentWireFormatVersion is the per-row format version written into every
// NodeWire/RelWire from this release on. Rows persisted before versioning
// existed decode with FormatVersion == 0 and are treated as version 1 — the
// two layouts are identical, the explicit field simply makes future layouts
// self-describing. Decoding a row with a version GREATER than
// maxReadableWireFormatVersion (BinaryHashWireFormatVersion, below)
// fails closed with store.ErrWireFormatVersionUnsupported instead of silently
// zero-filling fields this binary does not know about.
//
// Bump this constant ONLY together with: (1) the custom EncodeMsgpack
// emitters in wire_encode.go (lesson 39 — struct tags alone do nothing),
// (2) a decode path for the new layout, and (3) the store-level marker logic
// in the badger backend.
//
// v2 (ADR-0006 §4.5): the transaction-time tail (tf/tt) moved from omitempty
// mid-map fields to a FIXED-WIDTH, always-present trailing slot (last two map
// entries), so the ingest pipeline can pre-encode a row with a zero tail on the
// producer thread and have the single applier patch in the stamped TxFrom/TxTo
// with a bounded memory write (PatchWireTemporalTail) instead of a second
// msgpack pass. Decoders accept BOTH v1 (omitempty tail, absent fv = legacy)
// and v2 (fixed tail) transparently — msgpack map keys are self-describing, so
// no version branch is needed on read; the fv guard still fails a FUTURE
// version closed with ErrWireFormatVersionUnsupported. See wire_temporal_tail.go.
const CurrentWireFormatVersion = 2

// StoreFormatVersion is the store-level marker a Badger directory is stamped
// with (WireFormatVersionKey). It is above CurrentWireFormatVersion since
// backlog 50: this release keeps the ex-adjacency index (0x0D) at every
// relationship delete, and a release before it — which refuses a marker above
// its own CurrentWireFormatVersion 2 — must not open the store and delete
// relationships without it. Row encodings are unchanged. A feature raises it
// further (BinaryHashStoreFormatVersion, ElementsStoreFormatVersion).
const StoreFormatVersion = 3

// BinaryHashStoreFormatVersion is the store-level marker of a directory whose
// rows may carry binary integrity hashes (BinaryHashWireFormatVersion, backlog
// 61): a release that cannot read them must refuse the store. A writable open
// raises the marker to it (never lowering a higher one; the one tracker:
// badger raiseFormatMarkerPendingLocked) when this release's writer writes binary hashes (writesBinaryHashes);
// every reader that knows a marker at or above it reads binary rows.
const BinaryHashStoreFormatVersion = 4

// ElementsStoreFormatVersion is the store-level marker of a directory that
// persisted an Elements property index definition (backlog 63, definition key
// "el"). A binary that predates it would skip "el", load the index plain and
// key scalars only, so every contains read would miss rows, also after a
// re-upgrade trusting that binary's open image: it must refuse the store. The
// marker is raised only when such a definition is persisted; every other store
// keeps StoreFormatVersion. 4 is taken by the integrity-hash format (backlog
// 61), so this is 5.
const ElementsStoreFormatVersion = 5

// VoidStoreFormatVersion is the store-level marker of a directory that holds
// an excise's void piece (backlog 64, VoidWireFormatVersion rows). A binary
// that predates it must refuse the store at open rather than fail on the
// first void row mid-operation (or, before row versioning, read the piece as
// a valid version). The marker is raised only when a void row is written;
// every other store keeps StoreFormatVersion. 4 is taken by the
// integrity-hash format (backlog 61) and 5 by the Elements index definitions
// (backlog 63), so this is 6.
const VoidStoreFormatVersion = 6

// UntimedUnboundedStoreFormatVersion is the store-level marker of a directory
// whose untimed valid-start mode is UntimedUnbounded or a conversion to it
// started (backlog 73, MetaKV key untimed_valid_start). Row encodings are
// unchanged (the start is an ordinary ValidFrom), but a binary that predates
// it ignores the key and would write untimed first versions starting at their
// TxFrom into the store, a silent mix of the two rules: it must refuse the
// store. Raised with the key on the shard that holds it (badger
// MetaSetDurableRaising); a TxFrom store keeps its marker. 4 to 6 are taken
// (binary hashes, Elements definitions, void rows), so this is 7.
const UntimedUnboundedStoreFormatVersion = 7

// MaxReadableStoreFormatVersion is the highest store-level marker this binary
// opens (the max of every marker it knows how to read).
const MaxReadableStoreFormatVersion = UntimedUnboundedStoreFormatVersion

// writesBinaryHashes selects the writer's hash form (backlog 61, a two-step
// rollout under the deprecation ritual). Step 1 (the release before): every
// reader accepts binary hashes (row fv 4) and markers up to
// MaxReadableStoreFormatVersion, the writer wrote hex. Step 2 (this release):
// the writer writes binary and a writable open raises the marker to
// BinaryHashStoreFormatVersion, so a release before step 1 refuses the store.
const writesBinaryHashes = true

// binaryHashWriter is writesBinaryHashes, a variable only so a test can play
// the other step's writer (SetBinaryHashWriterForTest).
var binaryHashWriter atomic.Bool

func init() { binaryHashWriter.Store(writesBinaryHashes) }

// WritesBinaryHashes reports whether the writer writes integrity hashes as
// raw bytes (and a writable open raises the store marker to
// BinaryHashStoreFormatVersion).
func WritesBinaryHashes() bool { return binaryHashWriter.Load() }

// SetBinaryHashWriterForTest switches the writer's hash form (and with it the
// marker a writable open raises) and returns the restore. Test seam only: not
// for production use.
func SetBinaryHashWriterForTest(on bool) (restore func()) {
	prev := binaryHashWriter.Swap(on)
	return func() { binaryHashWriter.Store(prev) }
}

// RetractedWireFormatVersion is the per-row format version of a retraction
// tombstone (Retracted, map key "rx", backlog 43). The layout is v2's; the
// bump exists so a binary that predates the marker (it decodes fv <= 2 and
// skips the unknown key "rx", which would answer a retraction as a plain
// Delete) fails closed with store.ErrWireFormatVersionUnsupported. Only rows
// with Retracted carry it: every other row, and the store-level format marker,
// stay at CurrentWireFormatVersion, so an older binary still opens stores,
// exports and change feeds that hold no retraction. Rows v4.49.1 wrote (fv=2
// plus "rx") still decode as retractions.
const RetractedWireFormatVersion = 3

// BinaryHashWireFormatVersion is the per-row format version of a row whose
// integrity hashes (h, ph, fnh, tnh) are stored as 32 raw bytes (msgpack bin)
// instead of 64 hex characters (backlog 61): 34 bytes a hash instead of 66.
// The layout is v2's. Every row the binary-hash writer (writesBinaryHashes)
// writes with a canonical hash carries it (a row of a graph with
// Config.DisableIntegrityHashes has none and keeps the version it carries);
// the bump exists so a binary that predates it — whose decoder
// would read the 32 bytes as a 32-character hash — fails the row closed with
// store.ErrWireFormatVersionUnsupported. Decoders read both forms (the map
// value's msgpack type names its form), so every older row, export and change
// feed still reads.
const BinaryHashWireFormatVersion = 4

// VoidWireFormatVersion is the per-row format version of an excise's void
// piece (types.TemporalMetadata.Void, map key "vd", backlog 64). The layout is
// v2's; the bump exists so a binary that predates it — whose decoder skips
// the unknown key "vd" and would read the piece as a valid version, answering
// inside the excised span — fails the row closed with
// store.ErrWireFormatVersionUnsupported, also in an export, a change feed or a
// replica. Only void rows carry it. 4 is taken by the integrity-hash format
// (backlog 61), so this is 5.
const VoidWireFormatVersion = 5

// maxReadableWireFormatVersion is the highest per-row format version every
// decoder accepts. A variable only so a test can play an older reader.
var maxReadableWireFormatVersion uint8 = VoidWireFormatVersion

// SetMaxReadableWireFormatVersionForTest plays a reader that accepts row
// format versions up to v and returns the restore. Test seam only.
func SetMaxReadableWireFormatVersionForTest(v uint8) (restore func()) {
	prev := maxReadableWireFormatVersion
	maxReadableWireFormatVersion = v
	return func() { maxReadableWireFormatVersion = prev }
}

// checkVoidWire rejects a void marker on a row that cannot be a void piece:
// an excise appends bounded, non-empty, live rows only.
func checkVoidWire(what string, void bool, vf, vt, deletedAt int64, retracted bool) error {
	if !void {
		return nil
	}
	if vt == 0 || vf >= vt || deletedAt != 0 || retracted {
		return fmt.Errorf("%s: void marker on a row valid [%d, %d) deleted_at %d retracted %v", what, vf, vt, deletedAt, retracted)
	}
	return nil
}

// checkRowFormatVersion fails a row written by a newer release closed.
func checkRowFormatVersion(what string, fv uint8) error {
	if fv > maxReadableWireFormatVersion {
		return fmt.Errorf("%s: row format version %d, this binary supports up to %d: %w",
			what, fv, maxReadableWireFormatVersion, storepkg.ErrWireFormatVersionUnsupported)
	}
	return nil
}

// emittedRowFormatVersion is the fv an encoder writes: the highest version
// a feature of the row needs — a void piece VoidWireFormatVersion, a row with
// a binary integrity hash BinaryHashWireFormatVersion, a retraction tombstone
// RetractedWireFormatVersion (all v2 layout) — else the version it carries,
// so its bytes are unchanged.
func emittedRowFormatVersion(fv uint8, retracted, binaryHash, void bool) uint8 {
	if void {
		fv = max(fv, VoidWireFormatVersion)
	}
	if binaryHash {
		fv = max(fv, BinaryHashWireFormatVersion)
	}
	if retracted {
		fv = max(fv, RetractedWireFormatVersion)
	}
	return fv
}

// NodeWire is the msgpack wire format for Node entities.
// All token values are stored as int (maps to msgpack integer).
// Temporal instants are stored as int64 (Unix milliseconds).
//
// IDs use int64 (not types.NodeID / types.RelID / types.EntityID) by design:
// this is the on-disk wire format. Existing Badger databases were written with
// int64 IDs; changing the field type breaks msgpack unmarshalling of every
// pre-existing file. The Graph layer wraps these int64 values into typed IDs
// at the deserialization boundary (MustWireToNode / MustWireToRel). Tier D — see
// keys.go for the chokepoint invariant.
type NodeWire struct {
	// FormatVersion is the per-row wire format version. 0 means the row was
	// written before versioning existed (decode as version 1). See
	// CurrentWireFormatVersion for the bump protocol.
	FormatVersion uint8          `msgpack:"fv,omitempty"`
	ID            int64          `msgpack:"id"`
	PrimaryLabel  int            `msgpack:"pl"`
	ExtraLabels   []int          `msgpack:"el,omitempty"`
	Properties    []PropertyWire `msgpack:"p,omitempty"`
	Version       int            `msgpack:"v"`
	HasTemporal   bool           `msgpack:"ht,omitempty"`
	ValidFrom     int64          `msgpack:"vf,omitempty"`
	ValidTo       int64          `msgpack:"vt,omitempty"`
	TxFrom        int64          `msgpack:"tf,omitempty"`
	TxTo          int64          `msgpack:"tt,omitempty"`
	CreatedAt     int64          `msgpack:"ca,omitempty"`
	UpdatedAt     int64          `msgpack:"ua,omitempty"`
	DeletedAt     int64          `msgpack:"da,omitempty"`
	// Retracted is types.TemporalMetadata.Retracted (backlog 43): an
	// optional key emitted only on a retraction tombstone, so every other
	// row encodes exactly as before. A binary that predates it skips the key
	// (unknown map keys are skipped) and reads the tombstone as a Delete.
	Retracted bool `msgpack:"rx,omitempty"`
	// Void is types.TemporalMetadata.Void (backlog 64): emitted only on an
	// excise's void piece, which carries VoidWireFormatVersion.
	Void               bool   `msgpack:"vd,omitempty"`
	CreatedBy          string `msgpack:"cb,omitempty"`
	UpdatedBy          string `msgpack:"ub,omitempty"`
	BaseEntityID       int64  `msgpack:"be,omitempty"`
	Hash               string `msgpack:"h,omitempty"`
	PrevHash           string `msgpack:"ph,omitempty"`
	AuthorID           string `msgpack:"aid,omitempty"`
	Signature          []byte `msgpack:"sig,omitempty"`
	AuthorizedBy       string `msgpack:"aby,omitempty"`
	AuthorizationLevel uint8  `msgpack:"al,omitempty"`
}

// RelWire is the msgpack wire format for Relationship entities.
type RelWire struct {
	// FormatVersion is the per-row wire format version. 0 means the row was
	// written before versioning existed (decode as version 1). See
	// CurrentWireFormatVersion for the bump protocol.
	FormatVersion uint8          `msgpack:"fv,omitempty"`
	ID            int64          `msgpack:"id"`
	RelType       int            `msgpack:"rt"`
	StartID       int64          `msgpack:"s"`
	EndID         int64          `msgpack:"e"`
	Properties    []PropertyWire `msgpack:"p,omitempty"`
	Version       int            `msgpack:"v"`
	HasTemporal   bool           `msgpack:"ht,omitempty"`
	ValidFrom     int64          `msgpack:"vf,omitempty"`
	ValidTo       int64          `msgpack:"vt,omitempty"`
	TxFrom        int64          `msgpack:"tf,omitempty"`
	TxTo          int64          `msgpack:"tt,omitempty"`
	CreatedAt     int64          `msgpack:"ca,omitempty"`
	UpdatedAt     int64          `msgpack:"ua,omitempty"`
	DeletedAt     int64          `msgpack:"da,omitempty"`
	// Retracted is types.TemporalMetadata.Retracted (backlog 43): an
	// optional key emitted only on a retraction tombstone, so every other
	// row encodes exactly as before. A binary that predates it skips the key
	// (unknown map keys are skipped) and reads the tombstone as a Delete.
	Retracted bool `msgpack:"rx,omitempty"`
	// Void is types.TemporalMetadata.Void (backlog 64): emitted only on an
	// excise's void piece, which carries VoidWireFormatVersion.
	Void               bool   `msgpack:"vd,omitempty"`
	CreatedBy          string `msgpack:"cb,omitempty"`
	UpdatedBy          string `msgpack:"ub,omitempty"`
	BaseEntityID       int64  `msgpack:"be,omitempty"`
	Hash               string `msgpack:"h,omitempty"`
	PrevHash           string `msgpack:"ph,omitempty"`
	FromNodeHash       string `msgpack:"fnh,omitempty"`
	ToNodeHash         string `msgpack:"tnh,omitempty"`
	AuthorID           string `msgpack:"aid,omitempty"`
	Signature          []byte `msgpack:"sig,omitempty"`
	AuthorizedBy       string `msgpack:"aby,omitempty"`
	AuthorizationLevel uint8  `msgpack:"al,omitempty"`
}

// PropertyWire is the msgpack wire format for a single property key-value pair.
// Type carries the original Go type tag so exact types survive msgpack round-trips.
//
// KeyToken (msgpack `kt`, omitempty) is the optional registry-allocated
// uint16 token for Key. When KeyToken != 0 the decoder resolves the actual
// key string via the PropertyKeyRegistry and Key may be empty (saves bytes
// on the wire). When KeyToken == 0, Key is the authoritative source — this
// keeps reads of pre-tokenization data correct and lets the encoder fall
// back to raw keys when the registry is full.
type PropertyWire struct {
	Key           string `msgpack:"k,omitempty"`
	KeyToken      uint16 `msgpack:"kt,omitempty"`
	Value         any    `msgpack:"v"`
	Type          byte   `msgpack:"t"`            // property type tag for faithful reconstruction
	Nil           bool   `msgpack:"n,omitempty"`  // typed nil slice/map marker
	CustomType    string `msgpack:"ct,omitempty"` // registered custom property element type
	CustomPointer bool   `msgpack:"cp,omitempty"` // original custom property form was *T
}

// --- Node conversion ---

// NodeToWire converts a Node to its wire format for serialization.
func NodeToWire(n *types.Node) NodeWire {
	w, err := NodeToWireChecked(n)
	if err != nil {
		panic(fmt.Sprintf("storeutil: NodeToWire: %v", err))
	}
	return w
}

// NodeToWireChecked converts a Node to its wire format for serialization.
func NodeToWireChecked(n *types.Node) (NodeWire, error) {
	if n == nil {
		return NodeWire{}, fmt.Errorf("node wire: nil node")
	}

	w := NodeWire{
		FormatVersion: CurrentWireFormatVersion,
		ID:            int64(n.ID().SnowflakeID()),
		PrimaryLabel:  int(n.PrimaryLabelToken().Value()),
		Version:       int(n.Version()),
	}

	extras := n.ExtraLabelTokens()
	if len(extras) > 0 {
		w.ExtraLabels = make([]int, len(extras))
		for i, t := range extras {
			w.ExtraLabels[i] = int(t.Value())
		}
	}

	props, err := propertiesToWireChecked(n.Properties())
	if err != nil {
		return NodeWire{}, fmt.Errorf("node wire: properties: %w", err)
	}
	w.Properties = props

	if tm := n.Temporal(); tm != nil {
		w.HasTemporal = true
		w.ValidFrom = int64(tm.ValidFrom)
		w.ValidTo = int64(tm.ValidTo)
		w.TxFrom = int64(tm.TxFrom)
		w.TxTo = int64(tm.TxTo)
		w.CreatedAt = int64(tm.CreatedAt)
		w.UpdatedAt = int64(tm.UpdatedAt)
		w.DeletedAt = int64(tm.DeletedAt)
		w.Retracted = tm.Retracted
		w.Void = tm.Void
		w.CreatedBy = tm.CreatedBy
		w.UpdatedBy = tm.UpdatedBy
		w.BaseEntityID = int64(tm.BaseEntityID().SnowflakeID())
	}

	if ig := n.Integrity(); ig != nil {
		w.Hash = ig.Hash
		w.PrevHash = ig.PrevHash
		w.AuthorID = ig.AuthorID
		w.Signature = types.CloneBytes(ig.Signature)
		w.AuthorizedBy = ig.AuthorizedBy
		w.AuthorizationLevel = ig.AuthorizationLevel
	}

	return w, nil
}

// MarshalNodeWire converts and MsgPack-encodes a Node.
func MarshalNodeWire(n *types.Node) ([]byte, error) {
	w, err := NodeToWireChecked(n)
	if err != nil {
		return nil, err
	}
	return marshalWirePooled(w)
}

// MustWireToNode reconstructs a Node from its wire format, PANICKING if the
// wire is invalid. BACKLOG 15k: renamed from WireToNode (the Must* prefix is
// the idiomatic Go signal for "panics on failure" — regexp.MustCompile,
// template.Must) to make it unmistakably distinct from the trust-boundary-
// safe WireToNodeChecked, which every real read path uses instead. Has zero
// production callers — this exists ONLY for test fixtures that construct an
// already-known-valid NodeWire and want the *types.Node without threading an
// error return through the test.
func MustWireToNode(w NodeWire) *types.Node {
	n := newNodeFromWire(w)
	if err := applyNodeWireFields(n, w, wireToProperties(w.Properties)); err != nil {
		panic(fmt.Sprintf("storeutil: MustWireToNode with invalid prevalidated properties: %v", err))
	}
	return n
}

func newNodeFromWire(w NodeWire) *types.Node {
	var extras []uint16
	for _, e := range w.ExtraLabels {
		extras = append(extras, uint16(e)) // #nosec G115 — token values from our own serialization, always in uint16 range
	}
	return types.NewNode(types.NodeID(w.ID), uint16(w.PrimaryLabel), extras) // #nosec G115 — token from our own serialization
}

func applyNodeWireFields(n *types.Node, w NodeWire, props types.PropertySlice) error {
	if err := n.SetProperties(props); err != nil {
		return err
	}
	n.SetVersion(uint32(w.Version)) // #nosec G115 — version from our own serialization

	if w.HasTemporal {
		tm := &types.TemporalMetadata{
			ValidFrom: types.Instant(w.ValidFrom),
			ValidTo:   types.Instant(w.ValidTo),
			TxFrom:    types.Instant(w.TxFrom),
			TxTo:      types.Instant(w.TxTo),
			CreatedAt: types.Instant(w.CreatedAt),
			UpdatedAt: types.Instant(w.UpdatedAt),
			DeletedAt: types.Instant(w.DeletedAt),
			Retracted: w.Retracted,
			Void:      w.Void,
			CreatedBy: w.CreatedBy,
			UpdatedBy: w.UpdatedBy,
		}
		if w.BaseEntityID != 0 {
			tm.SetBaseEntityID(types.EntityID(w.BaseEntityID))
		}
		n.SetTemporal(tm)
	}

	if w.Hash != "" || w.PrevHash != "" || w.AuthorID != "" || len(w.Signature) > 0 || w.AuthorizedBy != "" || w.AuthorizationLevel != 0 {
		n.SetIntegrity(&types.NodeIntegrity{
			Hash:               w.Hash,
			PrevHash:           w.PrevHash,
			AuthorID:           w.AuthorID,
			Signature:          types.CloneBytes(w.Signature),
			AuthorizedBy:       w.AuthorizedBy,
			AuthorizationLevel: w.AuthorizationLevel,
		})
	}

	return nil
}

// WireToNodeChecked validates and reconstructs a Node from persisted wire data.
// Use this on data read from durable stores, where valid MsgPack can still carry
// semantically invalid fields that must not enter Store state.
func WireToNodeChecked(w NodeWire) (*types.Node, error) {
	if err := validateNodeWireFields(w); err != nil {
		return nil, err
	}
	props, err := wireToPropertiesChecked(w.Properties)
	if err != nil {
		return nil, fmt.Errorf("node wire: properties: %w", err)
	}
	n := newNodeFromWire(w)
	if err := applyNodeWireFields(n, w, props); err != nil {
		return nil, fmt.Errorf("node wire: properties: %w", err)
	}
	return n, nil
}

// --- Relationship conversion ---

// RelToWire converts a Relationship to its wire format for serialization.
func RelToWire(r *types.Relationship) RelWire {
	w, err := RelToWireChecked(r)
	if err != nil {
		panic(fmt.Sprintf("storeutil: RelToWire: %v", err))
	}
	return w
}

// RelToWireChecked converts a Relationship to its wire format for serialization.
func RelToWireChecked(r *types.Relationship) (RelWire, error) {
	if r == nil {
		return RelWire{}, fmt.Errorf("relationship wire: nil relationship")
	}

	w := RelWire{
		FormatVersion: CurrentWireFormatVersion,
		ID:            int64(r.ID().SnowflakeID()),
		RelType:       int(r.TypeToken().Value()),
		StartID:       int64(r.StartNodeID().SnowflakeID()),
		EndID:         int64(r.EndNodeID().SnowflakeID()),
		Version:       int(r.Version()),
	}

	props, err := propertiesToWireChecked(r.Properties())
	if err != nil {
		return RelWire{}, fmt.Errorf("relationship wire: properties: %w", err)
	}
	w.Properties = props

	if tm := r.Temporal(); tm != nil {
		w.HasTemporal = true
		w.ValidFrom = int64(tm.ValidFrom)
		w.ValidTo = int64(tm.ValidTo)
		w.TxFrom = int64(tm.TxFrom)
		w.TxTo = int64(tm.TxTo)
		w.CreatedAt = int64(tm.CreatedAt)
		w.UpdatedAt = int64(tm.UpdatedAt)
		w.DeletedAt = int64(tm.DeletedAt)
		w.Retracted = tm.Retracted
		w.Void = tm.Void
		w.CreatedBy = tm.CreatedBy
		w.UpdatedBy = tm.UpdatedBy
		w.BaseEntityID = int64(tm.BaseEntityID().SnowflakeID())
	}

	if ig := r.Integrity(); ig != nil {
		w.Hash = ig.Hash
		w.PrevHash = ig.PrevHash
		w.FromNodeHash = ig.FromNodeHash
		w.ToNodeHash = ig.ToNodeHash
		w.AuthorID = ig.AuthorID
		w.Signature = types.CloneBytes(ig.Signature)
		w.AuthorizedBy = ig.AuthorizedBy
		w.AuthorizationLevel = ig.AuthorizationLevel
	}

	return w, nil
}

// MarshalRelWire converts and MsgPack-encodes a Relationship.
func MarshalRelWire(r *types.Relationship) ([]byte, error) {
	w, err := RelToWireChecked(r)
	if err != nil {
		return nil, err
	}
	return marshalWirePooled(w)
}

// MustWireToRel is the relationship mirror of MustWireToNode — see there for
// the naming rationale (BACKLOG 15k) and the "test-fixture only" contract.
func MustWireToRel(w RelWire) *types.Relationship {
	r := types.NewRelationship(
		types.RelID(w.ID),
		uint16(w.RelType), // #nosec G115 — token from our own serialization
		types.NodeID(w.StartID),
		types.NodeID(w.EndID),
	)
	if err := applyRelWireFields(r, w, wireToProperties(w.Properties)); err != nil {
		panic(fmt.Sprintf("storeutil: MustWireToRel with invalid prevalidated properties: %v", err))
	}
	return r
}

func applyRelWireFields(r *types.Relationship, w RelWire, props types.PropertySlice) error {
	if err := r.SetProperties(props); err != nil {
		return err
	}
	r.SetVersion(uint32(w.Version)) // #nosec G115 — version from our own serialization

	if w.HasTemporal {
		tm := &types.TemporalMetadata{
			ValidFrom: types.Instant(w.ValidFrom),
			ValidTo:   types.Instant(w.ValidTo),
			TxFrom:    types.Instant(w.TxFrom),
			TxTo:      types.Instant(w.TxTo),
			CreatedAt: types.Instant(w.CreatedAt),
			UpdatedAt: types.Instant(w.UpdatedAt),
			DeletedAt: types.Instant(w.DeletedAt),
			Retracted: w.Retracted,
			Void:      w.Void,
			CreatedBy: w.CreatedBy,
			UpdatedBy: w.UpdatedBy,
		}
		if w.BaseEntityID != 0 {
			tm.SetBaseEntityID(types.EntityID(w.BaseEntityID))
		}
		r.SetTemporal(tm)
	}

	if w.Hash != "" || w.PrevHash != "" || w.FromNodeHash != "" || w.ToNodeHash != "" || w.AuthorID != "" || len(w.Signature) > 0 || w.AuthorizedBy != "" || w.AuthorizationLevel != 0 {
		r.SetIntegrity(&types.RelIntegrity{
			Hash:               w.Hash,
			PrevHash:           w.PrevHash,
			FromNodeHash:       w.FromNodeHash,
			ToNodeHash:         w.ToNodeHash,
			AuthorID:           w.AuthorID,
			Signature:          types.CloneBytes(w.Signature),
			AuthorizedBy:       w.AuthorizedBy,
			AuthorizationLevel: w.AuthorizationLevel,
		})
	}

	return nil
}

// WireToRelChecked validates and reconstructs a Relationship from persisted
// wire data. Use this at store read boundaries instead of calling MustWireToRel
// directly on bytes that came from disk.
func WireToRelChecked(w RelWire) (*types.Relationship, error) {
	if err := validateRelWireFields(w); err != nil {
		return nil, err
	}
	props, err := wireToPropertiesChecked(w.Properties)
	if err != nil {
		return nil, fmt.Errorf("relationship wire: properties: %w", err)
	}
	r := types.NewRelationship(
		types.RelID(w.ID),
		uint16(w.RelType), // #nosec G115 — token already validated
		types.NodeID(w.StartID),
		types.NodeID(w.EndID),
	)
	if err := applyRelWireFields(r, w, props); err != nil {
		return nil, fmt.Errorf("relationship wire: properties: %w", err)
	}
	return r, nil
}

// propertiesToWire converts a PropertySlice to wire format.
// Each property's Go type is recorded in the Type tag for faithful reconstruction.
func propertiesToWire(ps types.PropertySlice) []PropertyWire {
	pw, err := propertiesToWireChecked(ps)
	if err != nil {
		panic(fmt.Sprintf("storeutil: propertiesToWire: %v", err))
	}
	return pw
}

func propertiesToWireChecked(ps types.PropertySlice) ([]PropertyWire, error) {
	if len(ps) == 0 {
		return nil, nil
	}
	pw := make([]PropertyWire, len(ps))
	for i, p := range ps {
		prop, err := propertyToWire(p)
		if err != nil {
			return nil, fmt.Errorf("property[%d] key %q: %w", i, p.Key, err)
		}
		pw[i] = prop
	}
	return pw, nil
}

// ValidatePropertyWireSlice checks that a property wire slice can be safely
// installed directly into a PropertySlice without breaking its invariants.
//
// Import paths read untrusted record streams, and durable store reads can see
// semantically corrupt rows from old bugs, disk corruption, or hostile fixtures.
// Both should validate property wire before constructing entities.
func ValidatePropertyWireSlice(pw []PropertyWire) error {
	_, err := wireToPropertiesChecked(pw)
	return err
}

func validatePropertyWire(p PropertyWire, index int, prev string) (types.Property, error) {
	if types.IsShadowKey(p.Key) {
		return types.Property{}, fmt.Errorf("property[%d] key %q uses reserved tkg_ prefix", index, p.Key)
	}
	if index > 0 && p.Key <= prev {
		return types.Property{}, fmt.Errorf("property[%d] key %q is not in strict sorted order after %q", index, p.Key, prev)
	}
	if p.Type > ptTemporal {
		return types.Property{}, fmt.Errorf("property[%d] key %q has unknown type tag %d", index, p.Key, p.Type)
	}
	if p.Type != ptCustom && (p.CustomType != "" || p.CustomPointer) {
		return types.Property{}, fmt.Errorf("property[%d] key %q has custom property metadata on non-custom type tag %d", index, p.Key, p.Type)
	}
	if p.Nil {
		if p.Value != nil {
			return types.Property{}, fmt.Errorf("property[%d] key %q marks typed nil but carries value type %T", index, p.Key, p.Value)
		}
		if !isNillablePropertyWireType(p.Type) {
			return types.Property{}, fmt.Errorf("property[%d] key %q marks non-nillable type tag %d as typed nil", index, p.Key, p.Type)
		}
	} else {
		if err := validatePropertyWireValue(p.Value, p.Type); err != nil {
			return types.Property{}, fmt.Errorf("property[%d] key %q type tag: %w", index, p.Key, err)
		}
	}
	if err := validateRawPropertyWireValue(p); err != nil {
		return types.Property{}, fmt.Errorf("property[%d] key %q raw value: %w", index, p.Key, err)
	}
	value, err := reconstructPropertyWireValue(p)
	if err != nil {
		return types.Property{}, fmt.Errorf("property[%d] key %q reconstruct: %w", index, p.Key, err)
	}
	if err := types.ValidatePropertyValue(value); err != nil {
		return types.Property{}, fmt.Errorf("property[%d] key %q reconstructed value: %w", index, p.Key, err)
	}
	var probe types.PropertySlice
	if err := probe.Set(p.Key, value); err != nil {
		return types.Property{}, fmt.Errorf("property[%d] key %q reconstructed value install: %w", index, p.Key, err)
	}
	return types.Property{Key: p.Key, Value: value}, nil
}

// validateRawPropertyWireValue vets the decoded wire value before it is
// reconstructed. An []any / map[string]any value can carry nested envelopes
// (wire_nested_temporal.go) that sit one or two levels deeper than the values
// they encode, so the property layer's depth limit would refuse a legitimate
// value at the deepest accepted nesting. For those tags only the raw nesting
// is bounded here (by the msgpack decode limit, so reconstruction recursion
// stays bounded for in-memory wire too); the full property validation,
// including the depth limit, runs on the reconstructed value right after.
func validateRawPropertyWireValue(p PropertyWire) error {
	if p.Type == ptSliceAny || p.Type == ptMapStrAny {
		if !rawWireDepthWithin(p.Value, 0) {
			return fmt.Errorf("%w: nesting exceeds %d levels", types.ErrMaxDepthExceeded, maxWireDecodeDepth)
		}
		return nil
	}
	return types.ValidatePropertyValue(p.Value)
}

func rawWireDepthWithin(v any, depth int) bool {
	if depth > maxWireDecodeDepth {
		return false
	}
	switch val := v.(type) {
	case []any:
		for _, e := range val {
			if !rawWireDepthWithin(e, depth+1) {
				return false
			}
		}
	case map[string]any:
		for _, e := range val {
			if !rawWireDepthWithin(e, depth+1) {
				return false
			}
		}
	}
	return true
}

// ValidateNodeWire checks that a NodeWire can be reconstructed without
// truncating tokens, accepting non-canonical label lists, or panicking in the
// types layer.
func ValidateNodeWire(w NodeWire) error {
	if err := validateNodeWireFields(w); err != nil {
		return err
	}
	if _, err := wireToPropertiesChecked(w.Properties); err != nil {
		return fmt.Errorf("node wire: properties: %w", err)
	}
	return nil
}

func validateNodeWireFields(w NodeWire) error {
	if err := checkRowFormatVersion("node wire", w.FormatVersion); err != nil {
		return err
	}
	if w.ID <= 0 {
		return fmt.Errorf("node wire: id must be positive, got %d", w.ID)
	}
	if w.Version < 0 || int64(w.Version) > maxWireUint32 {
		return fmt.Errorf("node wire: version %d outside uint32 range", w.Version)
	}
	if w.PrimaryLabel <= 0 {
		return fmt.Errorf("node wire: primary label token 0 is reserved")
	}
	if w.PrimaryLabel > maxWireUint16 {
		return fmt.Errorf("node wire: primary label token %d outside uint16 range", w.PrimaryLabel)
	}
	seen := make(map[int]struct{}, len(w.ExtraLabels))
	for i, t := range w.ExtraLabels {
		if t <= 0 {
			return fmt.Errorf("node wire: extra label[%d] token 0 is reserved", i)
		}
		if t > maxWireUint16 {
			return fmt.Errorf("node wire: extra label[%d] token %d outside uint16 range", i, t)
		}
		if t == w.PrimaryLabel {
			return fmt.Errorf("node wire: extra label[%d] duplicates primary label token %d", i, t)
		}
		if _, ok := seen[t]; ok {
			return fmt.Errorf("node wire: extra label[%d] duplicates token %d", i, t)
		}
		seen[t] = struct{}{}
	}
	if w.BaseEntityID < 0 {
		return fmt.Errorf("node wire: base entity id must be non-negative, got %d", w.BaseEntityID)
	}
	if !w.HasTemporal && nodeWireHasTemporalPayload(w) {
		return fmt.Errorf("node wire: temporal fields set while has_temporal is false")
	}
	if w.ValidFrom != 0 && w.ValidTo != 0 && w.ValidFrom >= w.ValidTo {
		return fmt.Errorf("node wire: valid_from %d must be before valid_to %d", w.ValidFrom, w.ValidTo)
	}
	if w.Retracted && w.DeletedAt == 0 {
		return fmt.Errorf("node wire: retraction marker on a row without deleted_at")
	}
	if err := checkVoidWire("node wire", w.Void, w.ValidFrom, w.ValidTo, w.DeletedAt, w.Retracted); err != nil {
		return err
	}
	return nil
}

func nodeWireHasTemporalPayload(w NodeWire) bool {
	return w.ValidFrom != 0 ||
		w.ValidTo != 0 ||
		w.TxFrom != 0 ||
		w.TxTo != 0 ||
		w.CreatedAt != 0 ||
		w.UpdatedAt != 0 ||
		w.DeletedAt != 0 ||
		w.Retracted ||
		w.Void ||
		w.CreatedBy != "" ||
		w.UpdatedBy != "" ||
		w.BaseEntityID != 0
}

// ValidateRelWire checks that a RelWire can be reconstructed without
// truncating tokens or panicking in the types layer.
func ValidateRelWire(w RelWire) error {
	if err := validateRelWireFields(w); err != nil {
		return err
	}
	if _, err := wireToPropertiesChecked(w.Properties); err != nil {
		return fmt.Errorf("relationship wire: properties: %w", err)
	}
	return nil
}

func validateRelWireFields(w RelWire) error {
	if err := checkRowFormatVersion("relationship wire", w.FormatVersion); err != nil {
		return err
	}
	if w.ID <= 0 {
		return fmt.Errorf("relationship wire: id must be positive, got %d", w.ID)
	}
	if w.StartID <= 0 {
		return fmt.Errorf("relationship wire: start id must be positive, got %d", w.StartID)
	}
	if w.EndID <= 0 {
		return fmt.Errorf("relationship wire: end id must be positive, got %d", w.EndID)
	}
	if w.Version < 0 || int64(w.Version) > maxWireUint32 {
		return fmt.Errorf("relationship wire: version %d outside uint32 range", w.Version)
	}
	if w.RelType <= 0 {
		return fmt.Errorf("relationship wire: type token 0 is reserved")
	}
	if w.RelType > maxWireUint16 {
		return fmt.Errorf("relationship wire: type token %d outside uint16 range", w.RelType)
	}
	if w.BaseEntityID < 0 {
		return fmt.Errorf("relationship wire: base entity id must be non-negative, got %d", w.BaseEntityID)
	}
	if !w.HasTemporal && relWireHasTemporalPayload(w) {
		return fmt.Errorf("relationship wire: temporal fields set while has_temporal is false")
	}
	if w.ValidFrom != 0 && w.ValidTo != 0 && w.ValidFrom >= w.ValidTo {
		return fmt.Errorf("relationship wire: valid_from %d must be before valid_to %d", w.ValidFrom, w.ValidTo)
	}
	if w.Retracted && w.DeletedAt == 0 {
		return fmt.Errorf("relationship wire: retraction marker on a row without deleted_at")
	}
	if err := checkVoidWire("rel wire", w.Void, w.ValidFrom, w.ValidTo, w.DeletedAt, w.Retracted); err != nil {
		return err
	}
	return nil
}

func relWireHasTemporalPayload(w RelWire) bool {
	return w.ValidFrom != 0 ||
		w.ValidTo != 0 ||
		w.TxFrom != 0 ||
		w.TxTo != 0 ||
		w.CreatedAt != 0 ||
		w.UpdatedAt != 0 ||
		w.DeletedAt != 0 ||
		w.Retracted ||
		w.Void ||
		w.CreatedBy != "" ||
		w.UpdatedBy != "" ||
		w.BaseEntityID != 0
}

// wireToProperties converts wire properties back to a PropertySlice.
// The unchecked path preserves the historical normalization behavior used by
// tests and by MustWireToNode/MustWireToRel; checked store reads use
// wireToPropertiesChecked so corrupt wire rows fail closed.
//
// The Type tag drives faithful reconstruction of the original Go type.
// Old data without Type (decoded as 0/ptUnknown) falls through to
// normalizeIntegersRecursive — same behavior as before the type tag was added.
func wireToProperties(pw []PropertyWire) types.PropertySlice {
	ps, err := wireToPropertiesUnchecked(pw)
	if err != nil {
		panic(fmt.Sprintf("storeutil: wireToProperties with invalid prevalidated property: %v", err))
	}
	return ps
}

func wireToPropertiesUnchecked(pw []PropertyWire) (types.PropertySlice, error) {
	if len(pw) == 0 {
		return nil, nil
	}
	ps := make(types.PropertySlice, len(pw))
	for i, p := range pw {
		value, err := reconstructPropertyWireValue(p)
		if err != nil {
			return nil, fmt.Errorf("property[%d] key %q: %w", i, p.Key, err)
		}
		ps[i] = types.Property{Key: p.Key, Value: value}
	}
	return ps, nil
}

func wireToPropertiesChecked(pw []PropertyWire) (types.PropertySlice, error) {
	if len(pw) == 0 {
		return nil, nil
	}
	ps := make(types.PropertySlice, len(pw))
	var prev string
	for i, p := range pw {
		prop, err := validatePropertyWire(p, i, prev)
		if err != nil {
			return nil, err
		}
		ps[i] = prop
		prev = p.Key
	}
	return ps, nil
}
