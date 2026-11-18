package storeutil

import (
	"strings"
	"testing"
)

// The field checks return plain errors (no sentinel): each row names the
// message of the check that must refuse it, so a row refused by some other
// check fails.
func TestValidateNodeWireDirectBranches(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		wire NodeWire
		want string
	}{
		{name: "missing id", wire: NodeWire{ID: 0, PrimaryLabel: 1}, want: "id must be positive, got 0"},
		{name: "negative version", wire: NodeWire{ID: 1, PrimaryLabel: 1, Version: -1}, want: "version -1 outside uint32 range"},
		{name: "version overflow", wire: NodeWire{ID: 1, PrimaryLabel: 1, Version: int(maxWireUint32) + 1}, want: "version 4294967296 outside uint32 range"},
		{name: "extra label zero", wire: NodeWire{ID: 1, PrimaryLabel: 1, ExtraLabels: []int{0}}, want: "extra label[0] token 0 is reserved"},
		{name: "extra label overflow", wire: NodeWire{ID: 1, PrimaryLabel: 1, ExtraLabels: []int{maxWireUint16 + 1}}, want: "extra label[0] token 65536 outside uint16 range"},
		{name: "extra label duplicates primary", wire: NodeWire{ID: 1, PrimaryLabel: 1, ExtraLabels: []int{1}}, want: "duplicates primary label token 1"},
		{name: "negative base entity", wire: NodeWire{ID: 1, PrimaryLabel: 1, BaseEntityID: -1}, want: "base entity id must be non-negative, got -1"},
		{name: "unknown property tag", wire: NodeWire{ID: 1, PrimaryLabel: 1, Properties: []PropertyWire{{Key: "a", Value: "x", Type: ptTemporal + 1}}}, want: "has unknown type tag"},
		{name: "unsorted properties", wire: NodeWire{ID: 1, PrimaryLabel: 1, Properties: []PropertyWire{{Key: "b", Value: "x", Type: ptString}, {Key: "a", Value: "x", Type: ptString}}}, want: "not in strict sorted order"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidateNodeWire(tt.wire); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("ValidateNodeWire = %v, want an error containing %q", err, tt.want)
			}
			if _, err := WireToNodeChecked(tt.wire); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("WireToNodeChecked = %v, want an error containing %q", err, tt.want)
			}
		})
	}
}

func TestValidateRelWireDirectBranches(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		wire RelWire
		want string
	}{
		{name: "missing id", wire: RelWire{ID: 0, RelType: 1, StartID: 2, EndID: 3}, want: "relationship wire: id must be positive, got 0"},
		{name: "missing end", wire: RelWire{ID: 1, RelType: 1, StartID: 2, EndID: 0}, want: "end id must be positive, got 0"},
		{name: "negative version", wire: RelWire{ID: 1, RelType: 1, StartID: 2, EndID: 3, Version: -1}, want: "version -1 outside uint32 range"},
		{name: "version overflow", wire: RelWire{ID: 1, RelType: 1, StartID: 2, EndID: 3, Version: int(maxWireUint32) + 1}, want: "version 4294967296 outside uint32 range"},
		{name: "negative base entity", wire: RelWire{ID: 1, RelType: 1, StartID: 2, EndID: 3, BaseEntityID: -1}, want: "base entity id must be non-negative, got -1"},
		{name: "unknown property tag", wire: RelWire{ID: 1, RelType: 1, StartID: 2, EndID: 3, Properties: []PropertyWire{{Key: "a", Value: "x", Type: ptTemporal + 1}}}, want: "has unknown type tag"},
		{name: "unsorted properties", wire: RelWire{ID: 1, RelType: 1, StartID: 2, EndID: 3, Properties: []PropertyWire{{Key: "b", Value: "x", Type: ptString}, {Key: "a", Value: "x", Type: ptString}}}, want: "not in strict sorted order"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidateRelWire(tt.wire); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("ValidateRelWire = %v, want an error containing %q", err, tt.want)
			}
			if _, err := WireToRelChecked(tt.wire); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("WireToRelChecked = %v, want an error containing %q", err, tt.want)
			}
		})
	}
}

func TestWireToCheckedAcceptsMinimalValidWire(t *testing.T) {
	t.Parallel()
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
