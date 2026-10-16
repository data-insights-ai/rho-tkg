package graphapply

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func nativeTransitionMoveInventory() []nativeTransitionMoveRecipe {
	var recipes []nativeTransitionMoveRecipe
	for phase := byte(1); phase <= 9; phase++ {
		for _, partition := range []uint64{3, 8} {
			for _, page := range []uint64{0, 1, 3} {
				for _, side := range []string{"before-submit", "after-applied"} {
					candidate := nativeTransitionMoveRecipe{Version: 1, Phase: phase, Partition: partition, Page: page, Side: side}
					if candidate.valid() {
						recipes = append(recipes, candidate)
					}
				}
			}
		}
	}
	return recipes
}

func TestNativeTransferTransitionInventoryKeepsActualPhaseSidesAndPagesFinite(t *testing.T) {
	recipes := nativeTransitionMoveInventory()
	if len(recipes) != 34 {
		t.Fatal("declared finite transition inventory", len(recipes))
	}
	seen := make(map[nativeTransitionMoveRecipe]bool, len(recipes))
	for _, recipe := range recipes {
		if !recipe.valid() || seen[recipe] {
			t.Fatal("foreign/duplicate move cell")
		}
		seen[recipe] = true
	}
	for _, bad := range []nativeTransitionMoveRecipe{
		{Version: 1, Phase: 6, Partition: 8, Side: "after-applied"},
		{Version: 1, Phase: 4, Partition: 8, Page: 2, Side: "before-submit"},
		{Version: 1, Phase: 1, Partition: 3, Side: "after-submit"},
	} {
		if bad.valid() {
			t.Fatal("unsupported selector became successful empty case")
		}
	}
}

func TestNativeTransferTransitionsPreserveFactoryOriginalAcrossReassociationAndReopen(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for _, recipe := range nativeTransitionMoveInventory() {
		// Short mode exercises two genuine process cells, including the
		// before-first-Fence lost-owner refusal and returned target activation.
		// Full mode authors every declared actual phase/participant/page side;
		// neither mode is called whole-D or physical IO attribution evidence.
		if testing.Short() && !(recipe.Phase == 1 && recipe.Partition == 3 && recipe.Side == "before-submit" || recipe.Phase == 7 && recipe.Partition == 8 && recipe.Side == "after-applied") {
			continue
		}
		t.Run(fmt.Sprintf("phase%d-p%d-page%d-%s", recipe.Phase, recipe.Partition, recipe.Page, recipe.Side), func(t *testing.T) {
			artifact := nativeTransitionMoveCase(t, recipe)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, "python3", "-B", filepath.Join("..", "..", "..", "tools", "v5-substrate", "native_transition_move_check.py"), artifact)
			var diagnostics processStderr
			command.Stdout, command.Stderr = &diagnostics, &diagnostics
			if err := command.Run(); err != nil {
				body, _ := diagnostics.state()
				t.Fatal("independent genuine transfer transition check", err, body)
			}
			if _, full := diagnostics.state(); full {
				t.Fatal("bounded checker diagnostics")
			}
		})
	}
}
