package memory

import "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"

// GetNodeHistoryEntry returns a deep copy of the stored history entry for the
// given node ID and version. Returns nil if no entry exists. Acquires the
// Store read lock for the duration of the lookup.
//
// Exported solely so package graph (and downstream tests) can inspect history
// entries without exporting the underlying maps. The shape mirrors what the
// pre-restructure tests reached for via the unexported `nodeHistory` field.
func (ms *Store) GetNodeHistoryEntry(id types.NodeID, version uint32) *types.Node {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	hist, ok := ms.nodeHistory[id]
	if !ok {
		return nil
	}
	n, ok := hist[version]
	if !ok {
		return nil
	}
	return n.DeepCopy()
}

// SetNodeHistoryEntryForTest replaces the stored history entry for the given
// node ID and version with a deep copy of n. No-op if the entry doesn't exist.
//
// Exported solely so package graph tests can inject tampered history entries
// to verify integrity-chain detection. Not for production use.
func (ms *Store) SetNodeHistoryEntryForTest(id types.NodeID, version uint32, n *types.Node) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	hist, ok := ms.nodeHistory[id]
	if !ok {
		return
	}
	if _, exists := hist[version]; !exists {
		return
	}
	hist[version] = n.DeepCopy()
}

// SetNodeForTest replaces the stored current node entry with a deep copy of n.
// No-op if the entry doesn't exist.
//
// Exported solely for tampering tests; not for production use.
func (ms *Store) SetNodeForTest(id types.NodeID, n *types.Node) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if _, exists := ms.nodes[id]; !exists {
		return
	}
	ms.nodes[id] = n.DeepCopy()
}
