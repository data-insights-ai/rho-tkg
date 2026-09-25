package memory

// sharedNodeBytesForTest is the store-level endpoint dictionary's value bytes
// (NodeIDs, first-hash codes, hashes; the writer's lookup maps excluded).
func sharedNodeBytesForTest(ms *Store) int {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if ms.segNodeDict == nil {
		return 0
	}
	return ms.segNodeDict.Bytes()
}

// sharedStringBytesForTest is a declared string column's store-level value
// dictionary size.
func sharedStringBytesForTest(ms *Store, tok uint16, col string) int {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	st := ms.segTypes[tok]
	if st == nil || st.dicts.Strings[col] == nil {
		return 0
	}
	return st.dicts.Strings[col].Bytes()
}

// sealerBusyForTest reports whether a background seal is running or due.
func sealerBusyForTest(ms *Store) bool {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.sealerRunning
}
