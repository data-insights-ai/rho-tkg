package types

import "testing"

func TestValidEndpointPresenceAccessors(t *testing.T) {
	var n *Node
	var r *Relationship
	if n.ValidEndpointPresence() != 0 || r.ValidEndpointPresence() != 0 {
		t.Fatal("nil presence")
	}
	n = NewNode(1, 1, nil)
	r = NewRelationship(2, 1, 1, 3)
	if n.ValidEndpointPresence() != 0 || r.ValidEndpointPresence() != 0 {
		t.Fatal("missing metadata presence")
	}
	m := &TemporalMetadata{ValidEndpoints: EndpointsExplicit | EndpointFromFinite | EndpointToFinite, ValidFrom: -1, ValidTo: 0}
	n.SetTemporal(m)
	r.SetTemporal(m)
	if n.ValidEndpointPresence() != m.ValidEndpoints || r.ValidEndpointPresence() != m.ValidEndpoints {
		t.Fatal("mutable metadata lost presence")
	}
	n = n.CompactFrozenCopy()
	r = r.CompactFrozenCopy()
	if n.ValidEndpointPresence() != m.ValidEndpoints || r.ValidEndpointPresence() != m.ValidEndpoints {
		t.Fatal("frozen metadata lost presence")
	}
	if got := testing.AllocsPerRun(100, func() { _ = n.ValidEndpointPresence(); _ = r.ValidEndpointPresence() }); got != 0 {
		t.Fatal("presence accessor allocated", got)
	}
}
