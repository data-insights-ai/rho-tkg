package core

import (
	"math/rand/v2"
	"strings"
	"testing"
)

// TestBitemporalOracleExercisesExcise guards the harness itself (backlog 64):
// over the harness's own seeds the op mix applies excises (void pieces the
// probes then resolve against) and also meets refused ones (a span where
// nothing is valid). Catches an op-mix change that silently stops drawing
// the excise ops, which would leave the void-piece resolution unfuzzed.
func TestBitemporalOracleExercisesExcise(t *testing.T) {
	t.Parallel()
	applied, refused := 0, 0
	const base uint64 = 0xB17E_0DE_10 // TestBitemporalOracleHarness's seeds
	for i := 0; i < 30; i++ {
		seed := base + uint64(i)
		g, err := New(Config{AllowTxBackfill: true})
		if err != nil {
			t.Fatal(err)
		}
		w := newWorld(t, g, rand.New(rand.NewPCG(seed, seed^0x9E3779B97F4A7C15)))
		w.setup()
		for j := 0; j < 12; j++ {
			w.step()
		}
		for _, l := range w.log {
			if !strings.HasPrefix(l, "excise") {
				continue
			}
			if strings.HasSuffix(l, "-> ok") {
				applied++
			} else {
				refused++
			}
		}
		g.Close()
	}
	if applied == 0 || refused == 0 {
		t.Fatalf("excise ops over the short harness seeds: %d applied, %d refused; want both > 0", applied, refused)
	}
}
