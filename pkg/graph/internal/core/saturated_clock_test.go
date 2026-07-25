package core

import (
	"context"
	"math"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Written BEFORE the fix, from the property alone.
//
// now() must never hand the SAME instant to two callers.
//
// v4.24.5 made the clock saturate at MaxInt64 instead of wrapping to MinInt64 —
// wrapping turned every subsequent TxFrom negative, so saturating was strictly
// better. But saturation replaced one silent corruption with another: at the
// ceiling, every call returns the identical instant, so a version and the version
// it supersedes are stamped the same and the superseded row's transaction
// interval [TxFrom, TxTo) collapses to ZERO WIDTH. An AS-OF read can then see
// neither version, or both.
//
// It is silent in the same way the wrap was: TxFrom and TxTo are outside the
// integrity hash, so VerifyNodeChain and VerifyRelChain keep reporting the store
// as healthy.
//
// The floor can no longer reach MaxInt64 through any door — all three untrusted
// doors are bounded (v4.24.5-9) — so this is defence in depth, exactly like the
// saturation it corrects. Monotonicity is the clock's whole contract; it should
// not quietly stop holding at the top of the range.
func TestCommitClock_SaturationNeverStampsTwoRowsIdentically(t *testing.T) {
	ctx := context.Background()
	g, err := New(Config{Store: memory.New(), SnowflakeNodeID: 1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	// Park the clock one tick below the ceiling: the next mint may legitimately
	// take MaxInt64, and the one after has nowhere to go.
	g.lastInstant.Store(math.MaxInt64 - 1)

	// The invariant is about STORED ROWS, not about now() itself — now() has no
	// error return, so it cannot report exhaustion. What must never happen is two
	// committed versions carrying the same transaction stamp.
	stamps := map[types.Instant]int{}
	for i := 0; i < 3; i++ {
		n, err := g.Nodes.Add(ctx, []string{"T"}, map[string]any{"i": i})
		if err != nil {
			// Refusing the write is the CORRECT outcome at exhaustion: loud
			// beats silently duplicating a stamp.
			continue
		}
		tx := n.Temporal().TxFrom
		stamps[tx]++
		if stamps[tx] > 1 {
			t.Fatalf("two committed rows share TxFrom=%d. At the ceiling the clock hands out the "+
				"same instant repeatedly, so a version and the version it supersedes are stamped "+
				"identically and the superseded row's transaction interval collapses to ZERO WIDTH. "+
				"An AS-OF read then sees neither version or both — invisibly, because TxFrom/TxTo are "+
				"outside the integrity hash and the chain verifiers still report the store healthy. "+
				"At exhaustion the write must FAIL, not silently duplicate.", tx)
		}
	}
}
