package types

// ValidEndpointPresence returns the valid endpoint interpretation without
// materializing a frozen metadata copy. Compact rows always use legacy bounds.
func (n *Node) ValidEndpointPresence() EndpointPresence {
	if n == nil || n.temporal == nil {
		return 0
	}
	return n.temporal.ValidEndpoints
}

// ValidEndpointPresence is the relationship counterpart.
func (r *Relationship) ValidEndpointPresence() EndpointPresence {
	if r == nil || r.temporal == nil {
		return 0
	}
	return r.temporal.ValidEndpoints
}
