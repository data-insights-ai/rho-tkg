package memory

// SealedSectionBytesForTest sums every section's encoded size over the
// declared type's segments (the scale test's breakdown).
func SealedSectionBytesForTest(ms *Store, tok uint16) map[string]int {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	out := map[string]int{}
	if st := ms.segTypes[tok]; st != nil {
		for _, sg := range st.segs {
			for name, n := range sg.seg.SectionBytes() {
				out[name] += n
			}
		}
	}
	return out
}

// segNodeSections are the per-segment sections that describe endpoints:
// identity, hashes and the adjacency directory.
var segNodeSections = []string{"nodes", "nodehash", "ephash.exc", "outcsr", "incsr.off"}

// NodeBytesForTest splits the bytes a declared type spends on endpoints into
// what every segment repeats (perSegment) and what the store holds once
// (shared).
func NodeBytesForTest(ms *Store, tok uint16) (perSegment, shared int) {
	secs := SealedSectionBytesForTest(ms, tok)
	for _, name := range segNodeSections {
		perSegment += secs[name]
	}
	return perSegment, sharedNodeBytesForTest(ms)
}
