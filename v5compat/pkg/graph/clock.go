package graph

import (
	"sync/atomic"
	"time"

	v5 "github.com/data-insights-ai/rho-tkg/v5/pkg/graph"
)

// nowMs is the wall clock (a variable so tests can step it back).
var nowMs = wallMs

func wallMs() int64 { return time.Now().UnixMilli() }

// The compat transaction clock is the v5 commit clock. CommittedTx is the
// knowledge instant of the newest v5 commit (Receipt.KnowledgeInstant). A write
// stamps its ops with Recorded: the caller's instant on the backfill doors, else
// the instant the commit will take. The engine takes max(wall, previous+1); the
// compat layer predicts the same value under the writer lock, which is at most
// the engine's because the wall clock is read before the Update opens (the engine
// reads it again when it does), and the engine refuses a Recorded above its own. A v4 pin is then Fresh() with
// Selector.Known <= pin; nothing but the v5 store holds the clock.

// committed is the newest commit instant seen (0 until the first receipt).
type clock struct{ last atomic.Int64 }

func (c *clock) note(r v5.Receipt) {
	k, ok := r.KnowledgeInstant()
	if !ok {
		return
	}
	for {
		cur := c.last.Load()
		if int64(k) <= cur || c.last.CompareAndSwap(cur, int64(k)) {
			return
		}
	}
}

// committedTx is the newest commit instant.
func (g *Graph) committedTx() int64 { return g.clk.last.Load() }

// next is the instant a clock write would take: the wall clock, but strictly
// above every instant committed so far.
func (g *Graph) next() int64 { return max(nowMs(), g.committedTx()+1) }

// nowValid is the implicit valid-time "now" of the v4 list doors: the later of
// the wall clock and the commit clock.
func (g *Graph) nowValid() int64 { return max(nowMs(), g.committedTx()) }
