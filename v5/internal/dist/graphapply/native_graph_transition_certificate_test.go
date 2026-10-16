package graphapply

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func nativeTransitionCertificateInventory() []nativeTransitionCutRecipe {
	var recipes []nativeTransitionCutRecipe
	for _, partition := range []uint64{3, 8} {
		for _, side := range []string{"before-submit", "after-applied"} {
			recipes = append(recipes, nativeTransitionCutRecipe{Version: 1, Phase: graphCutCertify, Partition: partition, Side: side})
		}
	}
	return recipes
}

func TestNativeFirstCertificateInventoryAndOriginalIndexRemainDistinctFromReplies(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	seen := make(map[nativeTransitionCutRecipe]bool)
	for _, recipe := range nativeTransitionCertificateInventory() {
		if !nativeTransitionCertificateRecipeValid(recipe) || seen[recipe] {
			t.Fatal("finite first-publication recipe")
		}
		seen[recipe] = true
	}
	if len(seen) != 4 || nativeTransitionCertificateRecipeValid(nativeTransitionCutRecipe{Version: 1, Phase: graphCutFence, Partition: 3, Side: "after-applied"}) {
		t.Fatal("fence/repeated-control inventory substituted first publication")
	}
	var records []lifecycleRawRecord
	for _, item := range []struct{ partition, index, round uint64 }{{3, 11, 4}, {8, 12, 4}, {3, 17, 7}, {3, 21, 7}} {
		wire := encodeGraphCutResult(graphCutResult{phase: graphCutCertify, status: graphCutApplied, round: item.round, index: item.index})
		records = append(records, lifecycleRawRecord{Partition: item.partition, Index: item.index, Outcome: wire})
	}
	if index, err := nativeTransitionCertificateFirst(records, 3, 7); err != nil || index != 17 {
		t.Fatal("later repeated Certify replaced original GSC index", index, err)
	}
	if _, err := nativeTransitionCertificateFirst(records, 8, 7); !errors.Is(err, errInvalid) {
		t.Fatal("missing participant certificate invented", err)
	}
	records[2].Index++
	if _, err := nativeTransitionCertificateFirst(records, 3, 7); !errors.Is(err, errCorrupt) {
		t.Fatal("different native reply index accepted", err)
	}
}

func TestNativeFirstCertificatePublicationKeepsDistinctOldStateAndStoredOriginalThroughRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for _, recipe := range nativeTransitionCertificateInventory() {
		if testing.Short() && !(recipe.Partition == 3 && recipe.Side == "before-submit" || recipe.Partition == 8 && recipe.Side == "after-applied") {
			continue
		}
		t.Run(fmt.Sprintf("p%d-%s", recipe.Partition, recipe.Side), func(t *testing.T) {
			artifact := nativeTransitionCertificateCase(t, recipe)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, "python3", "-B", filepath.Join("..", "..", "..", "..", "tools", "v5-substrate", "native_transition_certificate_check.py"), artifact)
			var diagnostics processStderr
			command.Stdout, command.Stderr = &diagnostics, &diagnostics
			if err := command.Run(); err != nil {
				body, _ := diagnostics.state()
				t.Fatal("independent first-publication/prefix/serial check", err, body)
			}
			if _, full := diagnostics.state(); full {
				t.Fatal("bounded checker diagnostics")
			}
		})
	}
}
