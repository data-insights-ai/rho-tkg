package storeutil

import (
	"testing"

	"github.com/vmihailenco/msgpack/v5"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
)

func TestNodeWireEncodeMsgpackAllOptionalFieldsRoundTrip(t *testing.T) {
	in := NodeWire{
		ID:                 1,
		PrimaryLabel:       2,
		ExtraLabels:        []int{3, 4},
		Properties:         []PropertyWire{{Key: "p", Value: "v", Type: ptString}},
		Version:            5,
		HasTemporal:        true,
		ValidFrom:          10,
		ValidTo:            20,
		TxFrom:             30,
		TxTo:               40,
		CreatedAt:          50,
		UpdatedAt:          60,
		DeletedAt:          70,
		CreatedBy:          "creator",
		UpdatedBy:          "updater",
		BaseEntityID:       80,
		Hash:               "hash",
		PrevHash:           "prev",
		AuthorID:           "author",
		Signature:          []byte("sig"),
		AuthorizedBy:       "authz",
		AuthorizationLevel: 9,
	}

	data, err := msgpack.Marshal(in)
	if err != nil {
		t.Fatalf("msgpack.Marshal(NodeWire): %v", err)
	}
	var got NodeWire
	if err := msgpack.Unmarshal(data, &got); err != nil {
		t.Fatalf("msgpack.Unmarshal(NodeWire): %v", err)
	}

	if got.ID != in.ID || got.PrimaryLabel != in.PrimaryLabel || got.Version != in.Version {
		t.Fatalf("base fields = (%d,%d,%d), want (%d,%d,%d)", got.ID, got.PrimaryLabel, got.Version, in.ID, in.PrimaryLabel, in.Version)
	}
	if len(got.ExtraLabels) != 2 || got.ExtraLabels[0] != 3 || got.ExtraLabels[1] != 4 {
		t.Fatalf("ExtraLabels = %v", got.ExtraLabels)
	}
	if len(got.Properties) != 1 || got.Properties[0].Key != "p" || got.Properties[0].Type != ptString {
		t.Fatalf("Properties = %#v", got.Properties)
	}
	if !got.HasTemporal || got.ValidFrom != 10 || got.ValidTo != 20 || got.TxFrom != 30 || got.TxTo != 40 ||
		got.CreatedAt != 50 || got.UpdatedAt != 60 || got.DeletedAt != 70 ||
		got.CreatedBy != "creator" || got.UpdatedBy != "updater" || got.BaseEntityID != 80 {
		t.Fatalf("temporal fields did not round-trip: %#v", got)
	}
	if got.Hash != "hash" || got.PrevHash != "prev" || got.AuthorID != "author" ||
		string(got.Signature) != "sig" || got.AuthorizedBy != "authz" || got.AuthorizationLevel != 9 {
		t.Fatalf("integrity fields did not round-trip: %#v", got)
	}
}

func TestRelWireEncodeMsgpackAllOptionalFieldsRoundTrip(t *testing.T) {
	in := RelWire{
		ID:                 1,
		RelType:            2,
		StartID:            3,
		EndID:              4,
		Properties:         []PropertyWire{{Key: "p", Value: "v", Type: ptString}},
		Version:            5,
		HasTemporal:        true,
		ValidFrom:          10,
		ValidTo:            20,
		TxFrom:             30,
		TxTo:               40,
		CreatedAt:          50,
		UpdatedAt:          60,
		DeletedAt:          70,
		CreatedBy:          "creator",
		UpdatedBy:          "updater",
		BaseEntityID:       80,
		Hash:               "hash",
		PrevHash:           "prev",
		FromNodeHash:       "from",
		ToNodeHash:         "to",
		AuthorID:           "author",
		Signature:          []byte("sig"),
		AuthorizedBy:       "authz",
		AuthorizationLevel: 9,
	}

	data, err := msgpack.Marshal(in)
	if err != nil {
		t.Fatalf("msgpack.Marshal(RelWire): %v", err)
	}
	var got RelWire
	if err := msgpack.Unmarshal(data, &got); err != nil {
		t.Fatalf("msgpack.Unmarshal(RelWire): %v", err)
	}

	if got.ID != in.ID || got.RelType != in.RelType || got.StartID != in.StartID || got.EndID != in.EndID || got.Version != in.Version {
		t.Fatalf("base fields did not round-trip: %#v", got)
	}
	if len(got.Properties) != 1 || got.Properties[0].Key != "p" || got.Properties[0].Type != ptString {
		t.Fatalf("Properties = %#v", got.Properties)
	}
	if !got.HasTemporal || got.ValidFrom != 10 || got.ValidTo != 20 || got.TxFrom != 30 || got.TxTo != 40 ||
		got.CreatedAt != 50 || got.UpdatedAt != 60 || got.DeletedAt != 70 ||
		got.CreatedBy != "creator" || got.UpdatedBy != "updater" || got.BaseEntityID != 80 {
		t.Fatalf("temporal fields did not round-trip: %#v", got)
	}
	if got.Hash != "hash" || got.PrevHash != "prev" || got.FromNodeHash != "from" || got.ToNodeHash != "to" ||
		got.AuthorID != "author" || string(got.Signature) != "sig" || got.AuthorizedBy != "authz" || got.AuthorizationLevel != 9 {
		t.Fatalf("integrity fields did not round-trip: %#v", got)
	}
}

func TestPropertyWireEncodeMsgpackCustomFieldsRoundTrip(t *testing.T) {
	in := PropertyWire{
		Key:           "custom",
		Value:         []byte("payload"),
		Type:          ptCustom,
		CustomType:    "storeutil.wireValueDirectCustom",
		CustomPointer: true,
	}

	data, err := msgpack.Marshal(in)
	if err != nil {
		t.Fatalf("msgpack.Marshal(PropertyWire): %v", err)
	}
	var got PropertyWire
	if err := msgpack.Unmarshal(data, &got); err != nil {
		t.Fatalf("msgpack.Unmarshal(PropertyWire): %v", err)
	}
	if got.Key != in.Key || got.Type != in.Type || got.CustomType != in.CustomType || !got.CustomPointer {
		t.Fatalf("PropertyWire metadata = %#v, want %#v", got, in)
	}
	if string(got.Value.([]byte)) != "payload" {
		t.Fatalf("PropertyWire value = %#v", got.Value)
	}
}

func TestMarshalRelWireEncodesValidRelationship(t *testing.T) {
	r := types.NewRelationship(types.RelID(1), 2, types.NodeID(3), types.NodeID(4))
	if err := r.SetProperty("name", "edge"); err != nil {
		t.Fatalf("SetProperty: %v", err)
	}

	data, err := MarshalRelWire(r)
	if err != nil {
		t.Fatalf("MarshalRelWire: %v", err)
	}
	var got RelWire
	if err := msgpack.Unmarshal(data, &got); err != nil {
		t.Fatalf("msgpack.Unmarshal(RelWire): %v", err)
	}
	if got.ID != 1 || got.RelType != 2 || got.StartID != 3 || got.EndID != 4 {
		t.Fatalf("RelWire = %#v", got)
	}
}
