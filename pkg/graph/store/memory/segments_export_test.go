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

// SealLogEntry is one seal of the store (see sealRecord).
type SealLogEntry = sealRecord

// SetSealLogForTest installs fn as the store's seal log (nil removes it).
// Set it before the first write.
func SetSealLogForTest(ms *Store, fn func(SealLogEntry)) { ms.sealLog = fn }

// SetSegDirHookForTest installs fn as the segment directory's step hook.
func SetSegDirHookForTest(ms *Store, fn func(step string) error) bool {
	ms.mu.RLock()
	d := ms.segDir
	ms.mu.RUnlock()
	if d == nil {
		return false
	}
	d.SetHook(fn)
	return true
}
