package graphapply

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func nativeTransitionCutInventory() []nativeTransitionCutRecipe {
	var recipes []nativeTransitionCutRecipe
	for _, phase := range []byte{graphCutFence, graphCutCertify} {
		for _, partition := range []uint64{3, 8} {
			for _, side := range []string{"before-submit", "after-applied"} {
				recipes = append(recipes, nativeTransitionCutRecipe{Version: 1, Phase: phase, Partition: partition, Side: side})
			}
		}
	}
	return recipes
}

func TestNativeCutTransitionInventoryRequiresActualPhaseParticipantAndSide(t *testing.T) {
	seen := make(map[nativeTransitionCutRecipe]bool)
	for _, recipe := range nativeTransitionCutInventory() {
		if !recipe.valid() || seen[recipe] {
			t.Fatal("duplicate or foreign cut transition")
		}
		seen[recipe] = true
	}
	if len(seen) != 8 {
		t.Fatal("finite actual cut inventory")
	}
	for _, bad := range []nativeTransitionCutRecipe{{Version: 1, Phase: 3, Partition: 3, Side: "after-applied"}, {Version: 1, Phase: 1, Partition: 4, Side: "before-submit"}, {Version: 1, Phase: 2, Partition: 8, Side: "after-submit"}} {
		if bad.valid() {
			t.Fatal("unsupported selector became a passing no-op")
		}
	}
}

func TestNativeCutTransitionsKeepConcurrentWholeTransactionAndOldSourceThroughRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for _, recipe := range nativeTransitionCutInventory() {
		if testing.Short() && !(recipe.Phase == graphCutFence && recipe.Partition == 3 && recipe.Side == "before-submit" || recipe.Phase == graphCutCertify && recipe.Partition == 8 && recipe.Side == "after-applied") {
			continue
		}
		t.Run(fmt.Sprintf("phase%d-p%d-%s", recipe.Phase, recipe.Partition, recipe.Side), func(t *testing.T) {
			artifact := nativeTransitionCutCase(t, recipe)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, "python3", "-B", filepath.Join("..", "..", "..", "..", "tools", "v5-substrate", "native_transition_cut_check.py"), artifact)
			var diagnostics processStderr
			command.Stdout, command.Stderr = &diagnostics, &diagnostics
			if err := command.Run(); err != nil {
				body, _ := diagnostics.state()
				t.Fatal("independent actual cut/original/serial check", err, body)
			}
			if _, full := diagnostics.state(); full {
				t.Fatal("bounded checker diagnostics")
			}
		})
	}
}
