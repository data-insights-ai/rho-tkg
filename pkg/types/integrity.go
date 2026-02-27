package types

// NodeIntegrity holds integrity/hash-chain fields for nodes.
// Populated by the graph layer.
type NodeIntegrity struct {
	// Hash is the integrity hash of the current node state.
	Hash string
	// PrevHash links to the previous version's hash (empty for first version).
	PrevHash string
}

// RelIntegrity holds integrity/hash-chain fields for relationships.
// Populated by the graph layer.
type RelIntegrity struct {
	// Hash is the integrity hash of the current relationship state.
	Hash string
	// PrevHash links to the previous version's hash (empty for first version).
	PrevHash string
}
