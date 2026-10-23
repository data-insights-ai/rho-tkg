package core

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// The head cache is bounded by pieces: an answer above headMaxEntryPieces is
// not kept (and drops the entry it would replace), and the weight held never
// passes headCacheMaxPieces however many entries are put.
func TestHeadCache_BoundedByPieces(t *testing.T) {
	t.Parallel()
	var h headCache
	h.put(headKey{id: 1}, &headEntry{pieces: make([]headPiece, 3)})
	h.put(headKey{id: 1}, &headEntry{pieces: make([]headPiece, headMaxEntryPieces+1)})
	if h.get(headKey{id: 1}) != nil {
		t.Fatal("an answer above the per-entry limit was kept, or the replaced one survived")
	}
	if n, w := h.size(); n != 0 || w != 0 {
		t.Fatalf("after the drop: %d entries, weight %d", n, w)
	}
	e := &headEntry{pieces: make([]headPiece, 200)}
	for i := range 40_000 {
		h.put(headKey{id: snowflake.ID(i + 1)}, e)
		if i%997 == 0 {
			if _, w := h.size(); w > headCacheMaxPieces {
				t.Fatalf("put %d: weight %d above %d", i, w, headCacheMaxPieces)
			}
		}
	}
	n, w := h.size()
	if n == 0 || w != n*201 || w > headCacheMaxPieces {
		t.Fatalf("%d entries, weight %d: the weight does not follow the entries or passes the bound", n, w)
	}
	t.Logf("40000 puts of 200 pieces: %d entries kept, weight %d of %d", n, w, headCacheMaxPieces)
}
