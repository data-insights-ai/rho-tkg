package storeutil

import (
	"fmt"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	"github.com/vmihailenco/msgpack/v5"
)

const (
	maxWireUint16 = 1<<16 - 1
	maxWireUint32 = 1<<32 - 1
)

// CurrentWireFormatVersion is the per-row format version written into every
// NodeWire/RelWire from this release on. Rows persisted before versioning
// existed decode with FormatVersion == 0 and are treated as version 1 — the
// two layouts are identical, the explicit field simply makes future layouts
// self-describing. Decoding a row with a version GREATER than this constant
// fails closed with store.ErrWireFormatVersionUnsupported instead of silently
// zero-filling fields this binary does not know about.
//
// Bump this constant ONLY together with: (1) the custom EncodeMsgpack
// emitters in wire_encode.go (lesson 39 — struct tags alone do nothing),
// (2) a decode path for the new layout, and (3) the store-level marker logic
// in the badger backend.
const CurrentWireFormatVersion = 1

// NodeWire is the msgpack wire format for Node entities.
// All token values are stored as int (maps to msgpack integer).
// Temporal instants are stored as int64 (Unix milliseconds).
//
// IDs use int64 (not types.NodeID / types.RelID / types.EntityID) by design:
// this is the on-disk wire format. Existing Badger databases were written with
// int64 IDs; changing the field type breaks msgpack unmarshalling of every
// pre-existing file. The Graph layer wraps these int64 values into typed IDs
// at the deserialization boundary (WireToNode / WireToRel). Tier D — see
// keys.go for the chokepoint invariant.
type NodeWire struct {
	// FormatVersion is the per-row wire format version. 0 means the row was
	// written before versioning existed (decode as version 1). See
	// CurrentWireFormatVersion for the bump protocol.
	FormatVersion      uint8          `msgpack:"fv,omitempty"`
	ID                 int64          `msgpack:"id"`
	PrimaryLabel       int            `msgpack:"pl"`
	ExtraLabels        []int          `msgpack:"el,omitempty"`
	Properties         []PropertyWire `msgpack:"p,omitempty"`
	Version            int            `msgpack:"v"`
	HasTemporal        bool           `msgpack:"ht,omitempty"`
	ValidFrom          int64          `msgpack:"vf,omitempty"`
	ValidTo            int64          `msgpack:"vt,omitempty"`
	TxFrom             int64          `msgpack:"tf,omitempty"`
	TxTo               int64          `msgpack:"tt,omitempty"`
	CreatedAt          int64          `msgpack:"ca,omitempty"`
	UpdatedAt          int64          `msgpack:"ua,omitempty"`
	DeletedAt          int64          `msgpack:"da,omitempty"`
	CreatedBy          string         `msgpack:"cb,omitempty"`
	UpdatedBy          string         `msgpack:"ub,omitempty"`
	BaseEntityID       int64          `msgpack:"be,omitempty"`
	Hash               string         `msgpack:"h,omitempty"`
	PrevHash           string         `msgpack:"ph,omitempty"`
	AuthorID           string         `msgpack:"aid,omitempty"`
	Signature          []byte         `msgpack:"sig,omitempty"`
	AuthorizedBy       string         `msgpack:"aby,omitempty"`
	AuthorizationLevel uint8          `msgpack:"al,omitempty"`
}

// RelWire is the msgpack wire format for Relationship entities.
type RelWire struct {
	// FormatVersion is the per-row wire format version. 0 means the row was
	// written before versioning existed (decode as version 1). See
	// CurrentWireFormatVersion for the bump protocol.
	FormatVersion      uint8          `msgpack:"fv,omitempty"`
	ID                 int64          `msgpack:"id"`
	RelType            int            `msgpack:"rt"`
	StartID            int64          `msgpack:"s"`
	EndID              int64          `msgpack:"e"`
	Properties         []PropertyWire `msgpack:"p,omitempty"`
	Version            int            `msgpack:"v"`
	HasTemporal        bool           `msgpack:"ht,omitempty"`
	ValidFrom          int64          `msgpack:"vf,omitempty"`
	ValidTo            int64          `msgpack:"vt,omitempty"`
	TxFrom             int64          `msgpack:"tf,omitempty"`
	TxTo               int64          `msgpack:"tt,omitempty"`
	CreatedAt          int64          `msgpack:"ca,omitempty"`
	UpdatedAt          int64          `msgpack:"ua,omitempty"`
	DeletedAt          int64          `msgpack:"da,omitempty"`
	CreatedBy          string         `msgpack:"cb,omitempty"`
	UpdatedBy          string         `msgpack:"ub,omitempty"`
	BaseEntityID       int64          `msgpack:"be,omitempty"`
	Hash               string         `msgpack:"h,omitempty"`
	PrevHash           string         `msgpack:"ph,omitempty"`
	FromNodeHash       string         `msgpack:"fnh,omitempty"`
	ToNodeHash         string         `msgpack:"tnh,omitempty"`
	AuthorID           string         `msgpack:"aid,omitempty"`
	Signature          []byte         `msgpack:"sig,omitempty"`
	AuthorizedBy       string         `msgpack:"aby,omitempty"`
	AuthorizationLevel uint8          `msgpack:"al,omitempty"`
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
	return msgpack.Marshal(w)
}

// WireToNode reconstructs a Node from its wire format.
func WireToNode(w NodeWire) *types.Node {
	n := newNodeFromWire(w)
	if err := applyNodeWireFields(n, w, wireToProperties(w.Properties)); err != nil {
		panic(fmt.Sprintf("storeutil: WireToNode with invalid prevalidated properties: %v", err))
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
	return msgpack.Marshal(w)
}

// WireToRel reconstructs a Relationship from its wire format.
func WireToRel(w RelWire) *types.Relationship {
	r := types.NewRelationship(
		types.RelID(w.ID),
		uint16(w.RelType), // #nosec G115 — token from our own serialization
		types.NodeID(w.StartID),
		types.NodeID(w.EndID),
	)
	if err := applyRelWireFields(r, w, wireToProperties(w.Properties)); err != nil {
		panic(fmt.Sprintf("storeutil: WireToRel with invalid prevalidated properties: %v", err))
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
// wire data. Use this at store read boundaries instead of calling WireToRel
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
	if err := types.ValidatePropertyValue(p.Value); err != nil {
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
	if w.FormatVersion > CurrentWireFormatVersion {
		return fmt.Errorf("node wire: row format version %d, this binary supports up to %d: %w",
			w.FormatVersion, CurrentWireFormatVersion, storepkg.ErrWireFormatVersionUnsupported)
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
	if w.FormatVersion > CurrentWireFormatVersion {
		return fmt.Errorf("relationship wire: row format version %d, this binary supports up to %d: %w",
			w.FormatVersion, CurrentWireFormatVersion, storepkg.ErrWireFormatVersionUnsupported)
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
		w.CreatedBy != "" ||
		w.UpdatedBy != "" ||
		w.BaseEntityID != 0
}

// wireToProperties converts wire properties back to a PropertySlice.
// The unchecked path preserves the historical normalization behavior used by
// tests and by WireToNode/WireToRel; checked store reads use
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
