package graphapply

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestNativeComposedReplayRecipesKeepGenuineCausalInventoryAndFiniteActions(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	count := 0
	for _, first := range []string{"A", "B"} {
		for _, phase := range []string{"registered", "prepared"} {
			for _, fenceFirst := range []bool{false, true} {
				r := nativeTransitionComposedRecipe{Version: 1, Seed: 20261015, First: first, Phase: phase, FenceFirst: fenceFirst}
				if r.valid() != (first == "A" || fenceFirst) {
					t.Fatal("invented causal order or missing genuine schedule")
				}
				if r.valid() {
					count++
				}
			}
		}
	}
	if count != 6 {
		t.Fatal("finite mandatory causal schedules")
	}
	r := nativeTransitionComposedRecipe{Version: 1, Seed: 7, First: "A", Phase: "registered", FenceFirst: true,
		Actions: []nativeTransitionComposedAction{{At: "held", Kind: "tick", Member: 3, Ticks: 1}, {At: "transferred", Kind: "duplicate", Original: "A"}}}
	if !r.valid() {
		t.Fatal("actual Tick and same-original action refused")
	}
	r.Actions[0].At, r.Actions[0].Kind, r.Actions[0].Original = "held", "duplicate", "B"
	r.Actions[0].Member, r.Actions[0].Ticks = 0, 0
	if r.valid() {
		t.Fatal("held pending original became a completed duplicate")
	}
	r.Actions = []nativeTransitionComposedAction{{At: "transferred", Kind: "tick", Member: 1, Ticks: 1}, {At: "held", Kind: "tick", Member: 2, Ticks: 1}}
	if r.valid() {
		t.Fatal("deletion/replay changed mandatory causal stage order")
	}
}

// Capture preservation does not grant a passing semantic verdict. After all
// real children have closed, the actual independent checker must pass; the
// external recorded controller can reauthenticate a completed failed capture
// and minimize only the SAME independent semantic failure across fresh cohorts.
func TestNativeComposedTransitionRecordedReplayCell(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	raw := os.Getenv("RHO_GRAPH_COMPOSED_RECIPE")
	if raw == "" {
		t.Skip("explicit fresh native composed recorded replay cell")
	}
	TestNativeFaultFamilyTwoConflictingOriginalsFenceAndRealTransferExploreCausalOrders(t)
	if t.Failed() {
		return
	}
	var recipe nativeTransitionComposedRecipe
	if len(raw) > 4096 || json.Unmarshal([]byte(raw), &recipe) != nil || !recipe.valid() {
		t.Fatal("recorded source recipe changed")
	}
	artifact := filepath.Join(os.Getenv("RHO_GRAPH_COMPOSED_OUTPUT"), fmt.Sprintf("native-composed-explore-%s-%s-fence-%t.jsonl", recipe.First, recipe.Phase, recipe.FenceFirst))
	deadline, err := strconv.ParseInt(os.Getenv("RHO_GRAPH_TRANSITION_DEADLINE"), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	runCtx, stopRun := context.WithDeadline(t.Context(), time.Unix(0, deadline))
	defer stopRun()
	ctx, cancel := context.WithTimeout(runCtx, 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "python3", "-B", filepath.Join("..", "..", "..", "..", "tools", "v5-substrate", "native_transition_composed_check.py"), artifact)
	var diagnostics processStderr
	command.Stdout, command.Stderr = &diagnostics, &diagnostics
	if err := command.Run(); err != nil {
		body, _ := diagnostics.state()
		t.Fatal("independent composed verdict failed; preserved capture is not accepted", err, body)
	}
	if _, full := diagnostics.state(); full {
		t.Fatal("bounded independent verdict diagnostics")
	}
}
