package storeutil

import "testing"

func TestValidateNodeWireDirectBranches(t *testing.T) {
	tests := []struct {
		name string
		wire NodeWire
	}{
		{name: "missing id", wire: NodeWire{ID: 0, PrimaryLabel: 1}},
		{name: "negative version", wire: NodeWire{ID: 1, PrimaryLabel: 1, Version: -1}},
		{name: "version overflow", wire: NodeWire{ID: 1, PrimaryLabel: 1, Version: int(maxWireUint32) + 1}},
		{name: "extra label zero", wire: NodeWire{ID: 1, PrimaryLabel: 1, ExtraLabels: []int{0}}},
		{name: "extra label overflow", wire: NodeWire{ID: 1, PrimaryLabel: 1, ExtraLabels: []int{maxWireUint16 + 1}}},
		{name: "extra label duplicates primary", wire: NodeWire{ID: 1, PrimaryLabel: 1, ExtraLabels: []int{1}}},
		{name: "negative base entity", wire: NodeWire{ID: 1, PrimaryLabel: 1, BaseEntityID: -1}},
		{name: "unknown property tag", wire: NodeWire{ID: 1, PrimaryLabel: 1, Properties: []PropertyWire{{Key: "a", Value: "x", Type: ptCustom + 1}}}},
		{name: "unsorted properties", wire: NodeWire{ID: 1, PrimaryLabel: 1, Properties: []PropertyWire{{Key: "b", Value: "x", Type: ptString}, {Key: "a", Value: "x", Type: ptString}}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidateNodeWire(tt.wire); err == nil {
				t.Fatal("ValidateNodeWire returned nil, want error")
			}
			if _, err := WireToNodeChecked(tt.wire); err == nil {
				t.Fatal("WireToNodeChecked returned nil, want error")
			}
		})
	}
}

func TestValidateRelWireDirectBranches(t *testing.T) {
	tests := []struct {
		name string
		wire RelWire
	}{
		{name: "missing id", wire: RelWire{ID: 0, RelType: 1, StartID: 2, EndID: 3}},
		{name: "missing end", wire: RelWire{ID: 1, RelType: 1, StartID: 2, EndID: 0}},
		{name: "negative version", wire: RelWire{ID: 1, RelType: 1, StartID: 2, EndID: 3, Version: -1}},
		{name: "version overflow", wire: RelWire{ID: 1, RelType: 1, StartID: 2, EndID: 3, Version: int(maxWireUint32) + 1}},
		{name: "negative base entity", wire: RelWire{ID: 1, RelType: 1, StartID: 2, EndID: 3, BaseEntityID: -1}},
		{name: "unknown property tag", wire: RelWire{ID: 1, RelType: 1, StartID: 2, EndID: 3, Properties: []PropertyWire{{Key: "a", Value: "x", Type: ptCustom + 1}}}},
		{name: "unsorted properties", wire: RelWire{ID: 1, RelType: 1, StartID: 2, EndID: 3, Properties: []PropertyWire{{Key: "b", Value: "x", Type: ptString}, {Key: "a", Value: "x", Type: ptString}}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidateRelWire(tt.wire); err == nil {
				t.Fatal("ValidateRelWire returned nil, want error")
			}
			if _, err := WireToRelChecked(tt.wire); err == nil {
				t.Fatal("WireToRelChecked returned nil, want error")
			}
		})
	}
}

func TestWireToCheckedAcceptsMinimalValidWire(t *testing.T) {
	node, err := WireToNodeChecked(NodeWire{ID: 1, PrimaryLabel: 2})
	if err != nil {
		t.Fatalf("WireToNodeChecked: %v", err)
	}
	if node.ID() != 1 || node.PrimaryLabelToken().Value() != 2 {
		t.Fatalf("node = id %d label %d", node.ID(), node.PrimaryLabelToken().Value())
	}

	rel, err := WireToRelChecked(RelWire{ID: 3, RelType: 4, StartID: 1, EndID: 2})
	if err != nil {
		t.Fatalf("WireToRelChecked: %v", err)
	}
	if rel.ID() != 3 || rel.TypeToken().Value() != 4 || rel.StartNodeID() != 1 || rel.EndNodeID() != 2 {
		t.Fatalf("relationship = id %d type %d start %d end %d", rel.ID(), rel.TypeToken().Value(), rel.StartNodeID(), rel.EndNodeID())
	}
}
