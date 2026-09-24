package memory

import (
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/segment"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// SealedColumnScanForTest is the internal column path over a declared type's
// current sealed rows: one segment.Batch decode per page, reading IDs,
// endpoints, valid/tx time and every declared column (dictionary codes or
// stored integers), skipping dead rows, building no relationship. It is what
// S5's columnar door will hand out; the S2 scale test measures it next to the
// row doors. Returns the rows visited and a checksum.
func SealedColumnScanForTest(ms *Store, tok uint16) (rows, sum int64, err error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	st := ms.segTypes[tok]
	if st == nil {
		return 0, 0, nil
	}
	for _, sg := range st.segs {
		err = sg.seg.ScanBatches(func(b *segment.Batch) bool {
			cols := b.Columns()
			for k := 0; k < b.N; k++ {
				if _, dead := ms.segDead[types.RelID(b.IDs[k])]; dead {
					continue
				}
				rows++
				sum += b.IDs[k] ^ b.StartIDs[k] ^ b.EndIDs[k] ^ b.ValidFrom[k] ^ b.ValidTo[k] ^ b.TxFrom[k]
			}
			for c := range cols {
				if codes, _, _, ok := b.StringColumn(c); ok {
					for _, x := range codes {
						sum += x
					}
				} else if vals, _, ok := b.IntColumn(c); ok {
					for _, x := range vals {
						sum += x
					}
				}
			}
			return true
		})
		if err != nil {
			return rows, sum, err
		}
	}
	return rows, sum, nil
}

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
