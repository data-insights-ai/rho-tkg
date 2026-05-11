package types

// CloneBytes returns an independent copy of b.
// Returns nil if b is nil.
func CloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	cp := make([]byte, len(b))
	copy(cp, b)
	return cp
}

// NodeIntegrity holds integrity/hash-chain fields for nodes.
// Populated by the graph layer.
type NodeIntegrity struct {
	// Hash is the integrity hash of the current node state.
	Hash string
	// PrevHash links to the previous version's hash (empty for first version).
	PrevHash string
	// AuthorID is an optional caller-supplied identifier for the entity that
	// created or last updated this version. Set via tkg_author_id in Add/Update
	// props; stored as a shadow property (not in the PropertySlice).
	AuthorID string
	// Signature is an optional caller-supplied cryptographic signature over
	// this version's content. Set via tkg_signature in Add/Update props;
	// stored as a shadow property (not in the PropertySlice).
	Signature []byte
	// AuthorizedBy is an optional caller-supplied identifier for the entity that
	// authorized this version. Set via tkg_authorized_by in Add/Update props.
	AuthorizedBy string
	// AuthorizationLevel is an optional caller-supplied authorization tier
	// (0=none). Set via tkg_auth_level in Add/Update props.
	AuthorizationLevel uint8
}

// DeepCopy returns an independent clone of the NodeIntegrity.
// Signature is deep-copied so mutations to the copy never affect the original.
func (ig *NodeIntegrity) DeepCopy() *NodeIntegrity {
	if ig == nil {
		return nil
	}
	cp := *ig
	cp.Signature = CloneBytes(ig.Signature)
	return &cp
}

// RelIntegrity holds integrity/hash-chain fields for relationships.
// Populated by the graph layer.
type RelIntegrity struct {
	// Hash is the integrity hash of the current relationship state.
	Hash string
	// PrevHash links to the previous version's hash (empty for first version).
	PrevHash string
	// FromNodeHash is the hash of the start node at the time this relationship
	// was written. Used for cross-validation; NOT included in ComputeRelHash
	// (to avoid cascading hash invalidation on node updates).
	FromNodeHash string
	// ToNodeHash is the hash of the end node at the time this relationship
	// was written. Used for cross-validation; NOT included in ComputeRelHash.
	ToNodeHash string
	// AuthorID is an optional caller-supplied identifier for the entity that
	// created or last updated this version. Set via tkg_author_id in Add/Update
	// props; stored as a shadow property (not in the PropertySlice).
	AuthorID string
	// Signature is an optional caller-supplied cryptographic signature over
	// this version's content. Set via tkg_signature in Add/Update props;
	// stored as a shadow property (not in the PropertySlice).
	Signature []byte
	// AuthorizedBy is an optional caller-supplied identifier for the entity that
	// authorized this version. Set via tkg_authorized_by in Add/Update props.
	AuthorizedBy string
	// AuthorizationLevel is an optional caller-supplied authorization tier
	// (0=none). Set via tkg_auth_level in Add/Update props.
	AuthorizationLevel uint8
}

// DeepCopy returns an independent clone of the RelIntegrity.
// Signature is deep-copied so mutations to the copy never affect the original.
func (ig *RelIntegrity) DeepCopy() *RelIntegrity {
	if ig == nil {
		return nil
	}
	cp := *ig
	cp.Signature = CloneBytes(ig.Signature)
	return &cp
}
