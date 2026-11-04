package core

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
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

// Generations (RELHIST P5 option C) weigh their pieces, at most headMaxGens
// of them and headMaxEntryPieces pieces are kept (the oldest go first), none
// is pushed from a trimmed entry or by a write at the entry's newest TxFrom,
// and a push never writes into the generations it extends.
func TestHeadCache_Generations(t *testing.T) {
	t.Parallel()
	var h headCache
	h.put(headKey{id: 1}, &headEntry{pieces: make([]headPiece, 3), bounds: make([]types.Instant, 2),
		gens: []headGen{{pieces: make([]headPiece, 4)}, {pieces: make([]headPiece, 5)}}})
	if _, w := h.size(); w != 3+2+1+4+5 {
		t.Fatalf("weight %d, want %d: the generations' pieces are not counted", w, 3+2+1+4+5)
	}

	whole := func(maxTx types.Instant, pieces int) *headEntry {
		return &headEntry{maxTx: maxTx, pieces: make([]headPiece, pieces), pieceFrom: headWhole, boundFrom: headWhole}
	}
	e := whole(10, 1)
	for i := range headMaxGens + 2 {
		out := *e
		out.maxTx = e.maxTx + 10
		out.gens = e.gensAfter(out.maxTx)
		if n := len(out.gens); n != min(i+1, headMaxGens) {
			t.Fatalf("push %d: %d generations", i, n)
		}
		if g := out.gens[len(out.gens)-1]; g.maxTx != e.maxTx || g.txEnd != out.maxTx {
			t.Fatalf("push %d: newest generation [%d, %d), want [%d, %d)", i, g.maxTx, g.txEnd, e.maxTx, out.maxTx)
		}
		if len(e.gens) > 0 && &out.gens[0] == &e.gens[0] {
			t.Fatalf("push %d: the extension shares the generations it extends", i)
		}
		e = &out
	}
	if got, want := e.gens[0].maxTx, e.maxTx-10*headMaxGens; got != want {
		t.Fatalf("oldest generation from %d, want %d: not the oldest dropped", got, want)
	}

	same := *e
	same.gens = e.gensAfter(e.maxTx)
	if len(same.gens) != len(e.gens) || &same.gens[0] != &e.gens[0] {
		t.Fatalf("a write at the newest TxFrom pushed a generation: %d, was %d", len(same.gens), len(e.gens))
	}
	trimmed := whole(10, 1)
	trimmed.pieceFrom = 5
	out := *trimmed
	out.gens = trimmed.gensAfter(20)
	if out.gens != nil {
		t.Fatalf("a trimmed entry pushed a generation: %d", len(out.gens))
	}

	big := whole(10, headMaxEntryPieces/2+1)
	out = *big
	out.maxTx = 20
	out.gens = big.gensAfter(20)
	if gens := out.gensAfter(30); len(gens) != 1 || gens[0].maxTx != 20 {
		t.Fatalf("generations over headMaxEntryPieces pieces: %d kept", len(gens))
	}
}
