package graphapply

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

func TestNativeProtocolCampaignLeaderAndMessagePrograms(t *testing.T) {
	for _, layout := range []string{"original", "rotated"} {
		for _, message := range []string{"literal", "delay", "reorder", "duplicate"} {
			spec := nativeProtocolProgramSpec{1, layout, message}
			t.Run(layout+"/"+message, func(t *testing.T) {
				for index, recipe := range nativeProtocolScopeInventory() {
					if testing.Short() && index != 0 && index != 11 && index != 36 && index != 47 {
						continue
					}
					t.Run(recipe.name(), func(t *testing.T) { nativeTransitionScopeCaseOwned(t, recipe.Mode, &spec, recipe) })
				}
				for index, recipe := range nativeProtocolFenceLiveInventory() {
					if testing.Short() && index != 0 && index != 11 && index != 36 && index != 47 {
						continue
					}
					t.Run(recipe.name(), func(t *testing.T) { nativeProtocolFenceCase(t, recipe, spec) })
				}
				for index, recipe := range nativeProtocolFenceInventory() {
					if recipe.Schedule != "persisted-fence-before-application" || !nativeProtocolProgramFenceReachable(recipe, spec) {
						continue
					}
					if testing.Short() && index != 48 && index != 51 && index != 52 && index != 75 {
						continue
					}
					t.Run(recipe.name(), func(t *testing.T) { nativeProtocolFenceCase(t, recipe, spec) })
				}
				for index, recipe := range nativeProtocolLiveP06Inventory() {
					if recipe.Kind == raftlog.StableStorageReady && message != "literal" {
						continue // Actual submitting-leader pre-emission ordering, not a packet match.
					}
					if testing.Short() && index != 0 && index != 7 && index != 16 && index != 23 {
						continue
					}
					t.Run(recipe.name(), func(t *testing.T) { nativeProtocolLiveP06Case(t, recipe, spec) })
				}
				for index, recipe := range nativeProtocolCoordinatorInventory() {
					if recipe.Phase == crossResolve && recipe.Partition == recipe.Coordinator && recipe.Kind == raftlog.StableStorageReady && message != "literal" {
						continue // Selected serving leader stops before its own Ready messages.
					}
					if testing.Short() && index != 0 && index != 19 && index != 24 && index != 47 {
						continue
					}
					t.Run(recipe.name(), func(t *testing.T) { nativeProtocolCoordinatorCase(t, recipe, spec) })
				}
				for index, recipe := range nativeProtocolPartialInventory() {
					if recipe.Phase == crossResolve && recipe.Partition == 8 && recipe.Kind == raftlog.StableStorageReady && message != "literal" {
						continue
					}
					if testing.Short() && index != 0 && index != 7 && index != 16 && index != 23 {
						continue
					}
					t.Run(recipe.name(), func(t *testing.T) { nativeFaultPartialCaseOwned(t, &spec, recipe) })
				}
				for index, recipe := range nativeProtocolFrozenInventory() {
					if testing.Short() && index != 0 && index != 15 && index != 43 && index != 44 && index != 87 {
						continue
					}
					t.Run(recipe.name(), func(t *testing.T) { nativeDecisionCaseOwned(t, recipe.Order, &spec, recipe) })
				}
				for index, recipe := range nativeProtocolConditionalInventory() {
					if testing.Short() && index != 0 && index != 24 && index != 48 && index != 96 && index != 191 && index != 215 {
						continue
					}
					t.Run(recipe.name(), func(t *testing.T) { nativeProtocolConditionalCase(t, recipe, spec) })
				}
			})
		}
	}
}

func TestNativeProtocolFrozenProgramPreservesActualQueueMemberAndFollower(t *testing.T) {
	for _, recipe := range nativeProtocolFrozenInventory() {
		for _, layout := range []string{"original", "rotated"} {
			spec := nativeProtocolProgramSpec{1, layout, "literal"}
			selected := nativeProtocolProgramFrozenSelector(recipe, spec)
			wantActor, wantMember := recipe.selector().Actor, recipe.selector().Member
			if layout == "rotated" {
				if recipe.Original == "B" {
					wantActor = 6
				}
				wantMember = 1
				if recipe.Partition == 8 {
					wantMember = 4
				}
			}
			if !selected.valid() || selected.Actor != wantActor || selected.Member != wantMember || selected.Actor == selected.Member {
				t.Fatal("frozen native candidate owner became a relabelled follower", recipe, selected)
			}
		}
	}
	if nativeProtocolProgramFrozenSelector(nativeProtocolFrozenRecipe{}, nativeProtocolProgramSpec{1, "rotated", "literal"}) != (nativeConjunctionSelector{}) {
		t.Fatal("invalid recipe gained a native selector")
	}
}

func TestNativeProtocolLocalFenceMessageApplicabilityPreservesNativeEmissionOrder(t *testing.T) {
	for _, recipe := range nativeProtocolFenceInventory() {
		for _, message := range []string{"literal", "delay", "reorder", "duplicate"} {
			spec := nativeProtocolProgramSpec{1, "rotated", message}
			want := !(recipe.Schedule == "persisted-fence-before-application" && recipe.Phase == 0 && recipe.Kind == raftlog.StableStorageReady && message != "literal")
			if nativeProtocolProgramFenceReachable(recipe, spec) != want {
				t.Fatal("pre-emission Raft-only ordering was confused with an unimplemented semantic recipe", recipe, message)
			}
		}
	}
}

func TestNativeProtocolProgramRejectsNominalLayoutAndRetainsOriginalOwner(t *testing.T) {
	for _, layout := range []string{"original", "rotated"} {
		for _, message := range []string{"literal", "delay", "reorder", "duplicate"} {
			if !(nativeProtocolProgramSpec{1, layout, message}).valid() {
				t.Fatal("declared real program rejected", layout, message)
			}
		}
	}
	for _, spec := range []nativeProtocolProgramSpec{{}, {2, "original", "literal"}, {1, "virtual-renumber", "literal"}, {1, "rotated", "drop-labelled-only"}} {
		if spec.valid() {
			t.Fatal("nominal identity/delivery program admitted", spec)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	f := &pairedGraphFaultNetwork{t: t, parent: ctx, n: &certifiedProcessNetwork{}}
	p, err := newNativeProtocolProgram(ctx, f, nativeProtocolProgramSpec{1, "original", "literal"})
	if err != nil || f.protocolProgram != p || p.evidence.OutputGrant != 16<<20 || p.output.Used() != 16<<20 {
		t.Fatal("finite ownership/admission before effects", err)
	}
	if _, err := newNativeProtocolProgram(ctx, f, p.spec); !errors.Is(err, errLimit) || f.protocolProgram != p {
		t.Fatal("second owner replaced existing admission", err)
	}
	if _, err := p.target(3, 3); !errors.Is(err, errInvalid) {
		t.Fatal("recipe became an election without a genuine native reply", err)
	}
	for range 2 {
		if _, err := p.close(); !errors.Is(err, errInvalid) {
			t.Fatal("unmatched/incomplete program qualified after repeated close", err)
		}
	}
	if f.protocolProgram != nil {
		t.Fatal("incomplete owner retained fixture slot")
	}
	other, stop := context.WithTimeout(t.Context(), 2*time.Minute)
	defer stop()
	if _, err := newNativeProtocolProgram(other, f, p.spec); !errors.Is(err, errInvalid) {
		t.Fatal("replacement obtained a fresh deadline", err)
	}
}

func TestNativeProtocolStateRecoverySelectionRejectsUnboundPooledProfile(t *testing.T) {
	t.Setenv("RHO_GRAPH_PROTOCOL_STATE_RECOVERY", "")
	if got, err := nativeProtocolStateAgreement(); err != nil || got != rangeMoveSpanSemanticContractID() {
		t.Fatal("literal old state agreement changed", err)
	}
	for _, value := range []string{"0", "3", "true", "04", "4"} {
		t.Setenv("RHO_GRAPH_PROTOCOL_STATE_RECOVERY", value)
		t.Setenv("RHO_GRAPH_PROTOCOL_STATE_PROCESS", "")
		if got, err := nativeProtocolStateAgreement(); !errors.Is(err, errInvalid) || got != ([32]byte{}) {
			t.Fatal("unselected/invalid pooled profile acquired native identity", value, err)
		}
	}
	t.Setenv("RHO_GRAPH_PROTOCOL_STATE_PROCESS", "1")
	t.Setenv("RHO_GRAPH_PROTOCOL_STATE_RECOVERY", "4")
	t.Setenv("RHO_GRAPH_COST_SPEC", "foreign-cost12")
	if got, err := nativeProtocolStateAgreement(); !errors.Is(err, errInvalid) || got != ([32]byte{}) {
		t.Fatal("workload profile borrowed D-only pooled authority", err)
	}
	t.Setenv("RHO_GRAPH_COST_SPEC", "")
	t.Setenv("RHO_GRAPH_TRANSFER_PROCESS", "")
	for _, key := range []string{"RHO_GRAPH_CERTIFIED_PROCESS", "RHO_GRAPH_SOURCE_PROCESS", "RHO_GRAPH_STABLE_STORAGE_PROCESS", "RHO_GRAPH_STABLE_STATE_PROCESS", "RHO_GRAPH_TRANSITION_PROCESS"} {
		t.Setenv(key, "1")
	}
	for _, key := range []string{"RHO_GRAPH_FEED_PROCESS", "RHO_GRAPH_FEED_RETAINED_PROCESS", "RHO_GRAPH_EDGE_UNIQUE_PROCESS", "RHO_GRAPH_SELECTORS_PROCESS", "RHO_GRAPH_CREATE_UPDATE_PROCESS", "RHO_GRAPH_IMMUTABLE_COLUMNS_PROCESS", "RHO_GRAPH_LATE_CORRECTION_PROCESS", "RHO_GRAPH_NATIVE_WORKLOAD", "RHO_GRAPH_WORKLOAD_SPEC", "RHO_GRAPH_DURABLE_COLUMN_PROCESS", "RHO_GRAPH_CONDITIONAL_PROCESS", "RHO_GRAPH_RETENTION_PROCESS", "RHO_GRAPH_STATE_RETENTION_PROCESS", "RHO_GRAPH_NATIVE_FAULT_PROCESS", "RHO_GRAPH_DECISION_FAULT_PROCESS"} {
		t.Setenv(key, "")
	}
	t.Setenv("RHO_GRAPH_RANGE_PROCESS", "1") // The actual small typed factory is permitted.
	t.Setenv("RHO_GRAPH_TRANSITION_RUN", "01000000000000000000000000000000")
	t.Setenv("RHO_GRAPH_TRANSITION_DEADLINE", strconv.FormatInt(time.Now().Add(time.Minute).UnixNano(), 10))
	if got, err := nativeProtocolStateAgreement(); err != nil || got != rangeMovePooledSemanticContractID() {
		t.Fatal("fully bound explicit pooled state fixture refused before Open", err)
	}
	t.Setenv("RHO_GRAPH_TRANSFER_PROCESS", "1")
	if got, err := nativeProtocolStateAgreement(); !errors.Is(err, errInvalid) || got != ([32]byte{}) {
		t.Fatal("actual fresh Cost12 transfer bootstrap borrowed D-only profile", err)
	}
}

func TestNativeProtocolPacketBindingUsesActualBootstrapAgreement(t *testing.T) {
	t.Setenv("RHO_GRAPH_CONDITIONAL_PROCESS", "")
	f := &pairedGraphFaultNetwork{n: &certifiedProcessNetwork{}}
	if f.nativeAgreement() != certifiedGraphSemanticContractID() {
		t.Fatal("ordinary packet binding changed")
	}
	f.n.rangeMoves = true
	if f.nativeAgreement() != rangeMoveSemanticContractID() {
		t.Fatal("old real range fixture binding changed")
	}
	t.Setenv("RHO_GRAPH_CONDITIONAL_PROCESS", "1")
	if f.nativeAgreement() != conditionalGraphSemanticContractID() {
		t.Fatal("actual conditional child borrowed certified packet binding")
	}
	for _, agreement := range [][32]byte{rangeMoveSpanSemanticContractID(), rangeMovePooledSemanticContractID()} {
		f.bootstrapAgreement = agreement
		if f.nativeAgreement() != agreement {
			t.Fatal("validated native bootstrap replaced by nominal environment/header")
		}
	}
}
