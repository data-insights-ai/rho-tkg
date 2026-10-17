package types

import "testing"

// TestEffectiveValidFrom: the start rule on a row (backlog 67): the stored
// ValidFrom when set (UnboundedValidFrom included), else TxFrom, else the
// legacy UpdatedAt, else the ID's mint instant; frozen rows (meta) alike; nil
// rows 0.
func TestEffectiveValidFrom(t *testing.T) {
	id := NodeID(819163212807929856)
	mint := id.MintInstant()
	cases := []struct {
		name string
		tm   *TemporalMetadata
		want Instant
	}{
		{"no metadata: mint", nil, mint},
		{"explicit", &TemporalMetadata{ValidFrom: 5, TxFrom: 9}, 5},
		{"unbounded", &TemporalMetadata{ValidFrom: UnboundedValidFrom, TxFrom: 9}, UnboundedValidFrom},
		{"negative explicit", &TemporalMetadata{ValidFrom: -3, TxFrom: 9}, -3},
		{"update row: TxFrom", &TemporalMetadata{TxFrom: 9, UpdatedAt: 7}, 9},
		{"legacy: UpdatedAt", &TemporalMetadata{UpdatedAt: 7}, 7},
		{"empty metadata: mint", &TemporalMetadata{}, mint},
	}
	for _, tc := range cases {
		n := NewNode(id, 1, nil)
		r := NewRelationship(RelID(id), 1, id, id)
		if tc.tm != nil {
			cp, cp2 := *tc.tm, *tc.tm
			n.SetTemporal(&cp)
			r.SetTemporal(&cp2)
		}
		if got := n.EffectiveValidFrom(); got != tc.want {
			t.Errorf("%s: node %d, want %d", tc.name, got, tc.want)
		}
		if got := r.EffectiveValidFrom(); got != tc.want {
			t.Errorf("%s: rel %d, want %d", tc.name, got, tc.want)
		}
		if got := tc.tm.EffectiveValidFrom(mint); got != tc.want {
			t.Errorf("%s: metadata %d, want %d", tc.name, got, tc.want)
		}
		n.Freeze()
		r.Freeze()
		if got := n.EffectiveValidFrom(); got != tc.want {
			t.Errorf("%s: frozen node %d, want %d", tc.name, got, tc.want)
		}
		if got := r.EffectiveValidFrom(); got != tc.want {
			t.Errorf("%s: frozen rel %d, want %d", tc.name, got, tc.want)
		}
	}
	var n *Node
	var r *Relationship
	if n.EffectiveValidFrom() != 0 || r.EffectiveValidFrom() != 0 {
		t.Fatal("nil row: want 0")
	}
}
