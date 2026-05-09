package storeutil

import (
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

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
type PropertyWire struct {
	Key   string `msgpack:"k"`
	Value any    `msgpack:"v"`
	Type  byte   `msgpack:"t"` // property type tag for faithful reconstruction
}

// --- Node conversion ---

// NodeToWire converts a Node to its wire format for serialization.
func NodeToWire(n *types.Node) NodeWire {
	w := NodeWire{
		ID:           int64(n.ID().SnowflakeID()),
		PrimaryLabel: int(n.PrimaryLabelToken().Value()),
		Version:      int(n.Version()),
	}

	extras := n.ExtraLabelTokens()
	if len(extras) > 0 {
		w.ExtraLabels = make([]int, len(extras))
		for i, t := range extras {
			w.ExtraLabels[i] = int(t.Value())
		}
	}

	w.Properties = propertiesToWire(n.Properties())

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

	return w
}

// WireToNode reconstructs a Node from its wire format.
func WireToNode(w NodeWire) *types.Node {
	var extras []uint16
	for _, e := range w.ExtraLabels {
		extras = append(extras, uint16(e)) // #nosec G115 — token values from our own serialization, always in uint16 range
	}

	n := types.NewNode(types.NodeID(w.ID), uint16(w.PrimaryLabel), extras) // #nosec G115 — token from our own serialization
	n.SetProperties(wireToProperties(w.Properties))
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

	return n
}

// --- Relationship conversion ---

// RelToWire converts a Relationship to its wire format for serialization.
func RelToWire(r *types.Relationship) RelWire {
	w := RelWire{
		ID:      int64(r.ID().SnowflakeID()),
		RelType: int(r.TypeToken().Value()),
		StartID: int64(r.StartNodeID().SnowflakeID()),
		EndID:   int64(r.EndNodeID().SnowflakeID()),
		Version: int(r.Version()),
	}

	w.Properties = propertiesToWire(r.Properties())

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

	return w
}

// WireToRel reconstructs a Relationship from its wire format.
func WireToRel(w RelWire) *types.Relationship {
	r := types.NewRelationship(
		types.RelID(w.ID),
		uint16(w.RelType), // #nosec G115 — token from our own serialization
		types.NodeID(w.StartID),
		types.NodeID(w.EndID),
	)
	r.SetProperties(wireToProperties(w.Properties))
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

	return r
}
// propertiesToWire converts a PropertySlice to wire format.
// Each property's Go type is recorded in the Type tag for faithful reconstruction.
func propertiesToWire(ps types.PropertySlice) []PropertyWire {
	if len(ps) == 0 {
		return nil
	}
	pw := make([]PropertyWire, len(ps))
	for i, p := range ps {
		pw[i] = PropertyWire{Key: p.Key, Value: p.Value, Type: PropertyTypeTag(p.Value)}
	}
	return pw
}

// wireToProperties converts wire properties back to a PropertySlice.
// Wire data comes from our own serialization, so values are already validated
// and sorted — build the slice directly without re-validation.
//
// The Type tag drives faithful reconstruction of the original Go type.
// Old data without Type (decoded as 0/ptUnknown) falls through to
// normalizeIntegersRecursive — same behavior as before the type tag was added.
func wireToProperties(pw []PropertyWire) types.PropertySlice {
	if len(pw) == 0 {
		return nil
	}
	ps := make(types.PropertySlice, len(pw))
	for i, p := range pw {
		ps[i] = types.Property{Key: p.Key, Value: reconstructTypedValue(p.Value, p.Type)}
	}
	return ps
}
