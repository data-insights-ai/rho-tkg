package graphapply

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/dist/replica"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

func TestNativeProtocolPendingDiagnosticPreservesOrdinaryP05AndRefusesJoinedErrors(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for _, actual := range []error{replica.ErrUnavailable, errors.Join(errCrossProcessPending, replica.ErrUnavailable), graphWireError{mask: 1<<3 | 1<<10}, processRemoteError{mask: 1 << 7, code: 3}} {
		if !nativeProtocolPendingLoss(actual) {
			t.Fatal("actual known unavailable source cause refused", actual)
		}
	}
	for _, actual := range []error{nil, errCrossProcessPending, errLimit, errCorrupt, context.Canceled, context.DeadlineExceeded, errRemoteOperationFailure, errors.New("unknown backend error"), errors.Join(replica.ErrUnavailable, errLimit), errors.Join(replica.ErrUnavailable, errRemoteOperationFailure), errors.Join(replica.ErrUnavailable, errors.New("joined unknown error")), graphWireError{mask: 1 << 13}, graphWireError{mask: 1<<3 | 1<<13}, graphWireError{mask: 1<<3 | 1<<2}, processRemoteError{mask: 1 << 3, code: 3}, processRemoteError{mask: 1<<7 | 1<<3, code: 3}, processRemoteError{mask: 1 << 7, code: 2}} {
		if nativeProtocolPendingLoss(actual) {
			t.Fatal("non-native-loss cause became pending eligibility", actual)
		}
	}
	ordinary, err := json.Marshal(p05Attempt{})
	want := `{"Result":null,"Conflict":false,"Closed":false,"Unavailable":false,"Expired":false,"Invalid":false,"Error":""}`
	if err != nil || string(ordinary) != want {
		t.Fatal("ordinary P05 diagnostic bytes changed", string(ordinary), err)
	}
}

func TestNativeProtocolConditionalBootstrapMatchesActualExplicitChildAgreement(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	t.Setenv("RHO_GRAPH_CONDITIONAL_PROCESS", "1")
	if !pairedFaultConditionalFixture() {
		t.Fatal("actual conditional child was relabelled as ordinary certified")
	}
	for _, value := range []string{"", "0", "true", nativeProtocolCampaignProfile} {
		t.Setenv("RHO_GRAPH_CONDITIONAL_PROCESS", value)
		if pairedFaultConditionalFixture() {
			t.Fatal("ordinary child borrowed conditional semantic", value)
		}
	}
}

func TestNativeProtocolLiveP06RegistryKeepsFirstCallAndRecoveryCausalityDistinct(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	seen := make(map[nativeProtocolLiveP06Recipe]bool)
	prepares, decisions := 0, 0
	for _, row := range nativeProtocolLiveP06Inventory() {
		if !row.valid() || seen[row] {
			t.Fatal("duplicate/infeasible live-run native occurrence")
		}
		seen[row] = true
		if row.Phase == crossPrepare {
			prepares++
		} else {
			decisions++
			if row.Coordinator != 8 {
				t.Fatal("active first caller rebound a dead A endpoint")
			}
		}
	}
	if len(seen) != 24 || prepares != 16 || decisions != 8 {
		t.Fatal("exact reachable first-live-call inventory", len(seen), prepares, decisions)
	}
	for _, phase := range []crossPhase{crossRegister, crossDecide, crossResolve} {
		if (nativeProtocolLiveP06Recipe{Coordinator: 3, Commit: true, Phase: phase, Kind: raftlog.StableStorageApplication, Side: "after"}).valid() {
			t.Fatal("unreachable first-live-call phase became a recovery surrogate", phase)
		}
	}
}

func TestNativeProtocolP01RegistryPreservesScopeAndActualParticipantOccurrences(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	seen := make(map[nativeProtocolScopeRecipe]bool)
	for _, row := range nativeProtocolScopeInventory() {
		if !row.valid() || seen[row] {
			t.Fatal("duplicate/inapplicable P01 native occurrence")
		}
		seen[row] = true
	}
	if len(seen) != 48 {
		t.Fatal("exact two-scope-mode/six-original/native-side inventory", len(seen))
	}
	for _, row := range []nativeProtocolScopeRecipe{
		{graphReadFresh, crossRegister, 3, raftlog.StableStorageReady, "before"},
		{graphReadAfter, crossDecide, 3, raftlog.StableStorageApplication, "after"},
		{graphReadAt, crossPrepare, 3, raftlog.StableStorageReady, "before"},
	} {
		if row.valid() {
			t.Fatal("wrong coordinator/At became the P01 scope scenario", row)
		}
	}
}

func TestNativeProtocolP03RegistrySeparatesPreDecisionAndQuorumLossCausality(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	seen := make(map[nativeProtocolCoordinatorRecipe]bool)
	early, outage, otherResolve := 0, 0, 0
	for _, r := range nativeProtocolCoordinatorInventory() {
		if !r.valid() || seen[r] {
			t.Fatal("duplicate or infeasible P03 actual occurrence")
		}
		seen[r] = true
		if r.Phase != crossResolve {
			early++
		} else if r.Partition == r.Coordinator {
			outage++
		} else {
			otherResolve++
		}
	}
	if len(seen) != 48 || early != 32 || outage != 8 || otherResolve != 8 {
		t.Fatal("finite actual P03 causal inventory", len(seen), early, outage, otherResolve)
	}
	for _, r := range []nativeProtocolCoordinatorRecipe{
		{Coordinator: 3, Phase: crossRegister, Partition: 8, Kind: raftlog.StableStorageReady, Side: "before"},
		{Coordinator: 8, Phase: crossDecide, Partition: 3, Kind: raftlog.StableStorageApplication, Side: "after"},
	} {
		if r.valid() {
			t.Fatal("foreign coordinator command became a P03 recipe", r)
		}
	}
}

func TestNativeProtocolP03PartialRegistrySeparatesLiveInstallFromRecoveryOutage(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	seen := make(map[nativeProtocolPartialRecipe]bool)
	early, outage := 0, 0
	for _, row := range nativeProtocolPartialInventory() {
		if !row.valid() || seen[row] {
			t.Fatal("duplicate/inapplicable partial-install native occurrence")
		}
		seen[row] = true
		if row.Phase == crossResolve && row.Partition == 8 {
			outage++
		} else {
			early++
		}
	}
	if len(seen) != 24 || early != 20 || outage != 4 {
		t.Fatal("actual early partial/live versus later B recovery inventory", len(seen), early, outage)
	}
}

func TestNativeProtocolP05RegistryKeepsEveryActualDependencyMutation(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	seen := make(map[nativeProtocolConditionalRecipe]bool)
	counts := make(map[string]int)
	for _, row := range nativeProtocolConditionalInventory() {
		if !row.valid() || seen[row] {
			t.Fatal("duplicate/inapplicable P05 original occurrence", row)
		}
		seen[row] = true
		counts[row.Schedule]++
	}
	if len(seen) != 216 || counts["write-skew-A-first"] != 24 || counts["write-skew-B-first"] != 24 || counts["empty-range-and-absent-key-insertion"] != 48 || counts["temporal-life-value-ABA"] != 96 || counts["authority-fence-before-prepare"] != 24 {
		t.Fatal("actual nine business invocations across five dependency scenarios", counts)
	}
	for _, row := range []nativeProtocolConditionalRecipe{
		{Schedule: "write-skew-A-first", Operation: 2, Phase: crossPrepare, Partition: 3, Kind: raftlog.StableStorageReady, Side: "before"},
		{Schedule: "write-skew-B-first", Operation: 1, Phase: crossDecide, Partition: 3, Kind: raftlog.StableStorageReady, Side: "before"},
		{Schedule: "temporal-life-value-ABA", Operation: 5, Phase: crossPrepare, Partition: 8, Kind: raftlog.StableStorageReady, Side: "before"},
	} {
		if row.valid() {
			t.Fatal("extra/preflight-only attempt became an actual native business phase", row)
		}
	}
	wire, err := json.Marshal(nativeProtocolConditionalEvidence{})
	var diagnostic map[string]json.RawMessage
	if err != nil || json.Unmarshal(wire, &diagnostic) != nil || string(diagnostic["TargetReopened"]) != "null" {
		t.Fatal("P05 without a third native certified query invented local historical data", string(wire), err)
	}
}

func TestNativeProtocolP04RegistryDoesNotInventUnpreparedParticipantOrWireAuthority(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	seen := make(map[nativeProtocolFrozenRecipe]bool)
	a, b, candidates := 0, 0, 0
	for _, row := range nativeProtocolFrozenInventory() {
		if !row.valid() || seen[row] {
			t.Fatal("duplicate/inapplicable frozen-original occurrence", row)
		}
		seen[row] = true
		if row.Original == "A" {
			a++
		} else {
			b++
		}
		if row.Candidate != "" {
			candidates++
		}
		selector := row.selector()
		if !selector.valid() || selector.Actor == selector.Member || selector.Member == 1 || selector.Member == 4 {
			t.Fatal("native follower selector killed original authority", selector)
		}
	}
	if len(seen) != 88 || a != 32 || b != 56 || candidates != 16 {
		t.Fatal("two actual queue orders/four A and seven B durable inputs", len(seen), a, b, candidates)
	}
	for _, row := range []nativeProtocolFrozenRecipe{
		{Order: "commit-first", Original: "A", Phase: crossPrepare, Partition: 8, Kind: raftlog.StableStorageReady, Side: "before"},
		{Order: "commit-first", Original: "B", Phase: crossDecide, Partition: 8, Kind: raftlog.StableStorageReady, Side: "before"},
		{Order: "abort-first", Original: "A", Phase: crossDecide, Partition: 3, Candidate: "all-YES", Kind: raftlog.StableStorageReady, Side: "after"},
	} {
		if row.valid() {
			t.Fatal("unexecuted or imported candidate became a real original phase", row)
		}
	}
}

func TestNativeProtocolPlanningCaptureSpendsBoundedSourceCopiesWithoutCreatingReplies(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	// Codec-only input. No server, source owner, certificate or native reply is
	// present, so this cannot qualify an actual campaign cell.
	wire, err := encodeGraphSourceRequest(graphSourceRequest{kind: sourceOpen, sequence: 1, nonce: [16]byte{1}})
	if err != nil {
		t.Fatal(err)
	}
	legacy := &nativeConjunctionCapture{actor: 3}
	if err := legacy.before(t.Context(), 3, 3, wire); err != nil || legacy.refused || legacy.sources != nil || legacy.sourceOutput != 0 || legacy.sourceRetained != 0 {
		t.Fatal("ordinary capture allocated/retained planning-source observations", err)
	}
	fresh := &nativeConjunctionCapture{actor: 3, planningSources: true}
	for _, part := range []uint64{3, 8} {
		if err := fresh.before(t.Context(), 3, part, wire); err != nil {
			t.Fatal(err)
		}
	}
	if len(fresh.sources) != 2 || fresh.sourceOutput != 2*(8*(4096+nativeBatchReceiptBytes)+8192) || fresh.sourceRetained != uint64(2*len(wire)) || fresh.attempts != 0 || len(fresh.requests) != 0 {
		t.Fatal("two planning attempts borrowed submit allowance or hid their owned bytes")
	}
	if err := fresh.after(t.Context(), 3, 3, wire, []byte("inert malformed reply")); err != nil || !fresh.refused || len(fresh.sources[0].Reply) != 0 {
		t.Fatal("malformed observation became an authenticated planning source", err)
	}
	if err := fresh.before(t.Context(), 3, 3, wire); err != nil || len(fresh.sources) != 2 || fresh.sourceRetained != uint64(2*len(wire)) || fresh.sourceOutput != 2*(8*(4096+nativeBatchReceiptBytes)+8192) {
		t.Fatal("third source observation escaped fixed two-source ownership", err)
	}
}

func TestNativeProtocolP02LiveRegistryDoesNotRelabelPersistenceStageOrRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	seen := make(map[nativeProtocolFenceRecipe]bool)
	for _, row := range nativeProtocolFenceLiveInventory() {
		if !row.valid() || seen[row] {
			t.Fatal("duplicate/inapplicable live Prepare/fence occurrence")
		}
		seen[row] = true
	}
	if len(seen) != 48 {
		t.Fatal("exact two live ordering/six-original/native-side inventory", len(seen))
	}
	for _, schedule := range []string{"recovery", ""} {
		if (nativeProtocolFenceRecipe{schedule, crossPrepare, 3, raftlog.StableStorageReady, "after"}).valid() {
			t.Fatal("separate persistence/leader-loss recipe became a live-call substitute", schedule)
		}
	}
	stage := nativeProtocolFenceRecipe{"persisted-fence-before-application", crossPrepare, 3, raftlog.StableStorageReady, "after"}
	if !stage.valid() || seen[stage] {
		t.Fatal("genuine separate persistence/source-plan predecessor conflated with a live source hold")
	}
	all := make(map[nativeProtocolFenceRecipe]bool)
	stageRows, localFence := 0, 0
	for _, row := range nativeProtocolFenceInventory() {
		if !row.valid() || all[row] {
			t.Fatal("duplicate/invalid finite P02 ordering/native transition")
		}
		all[row] = true
		if row.Schedule == "persisted-fence-before-application" {
			stageRows++
			if row.Phase == 0 {
				localFence++
			}
		}
	}
	if len(all) != 76 || stageRows != 28 || localFence != 4 {
		t.Fatal("complete three-ordering P02 source inventory", len(all), stageRows, localFence)
	}
}

func TestNativeProtocolFenceSelectorRequiresActualCutFamilyAndWholeScope(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	// Inert codec/diagnostic refusal only. This scope and wire have no Host,
	// configured native store, source owner or proposal admission authority.
	scope := allocationScope{graph: [16]byte{1}, partition: 3, group: [16]byte{1}, ownership: 1, topology: 1, declaration: [32]byte{1}, semantic: certifiedGraphSemanticContractID()}
	selector := nativeConjunctionSelector{Actor: 1, Partition: 3, Member: 1, Kind: raftlog.StableStorageReady, Side: "after", Fence: true, FenceScope: scope}
	if !selector.valid() {
		t.Fatal("explicit local fence diagnostic selector refused")
	}
	for _, mutate := range []func(*nativeConjunctionSelector){
		func(s *nativeConjunctionSelector) { s.Phase = crossRegister },
		func(s *nativeConjunctionSelector) { s.Partition = 8 },
		func(s *nativeConjunctionSelector) { s.FenceScope = allocationScope{} },
		func(s *nativeConjunctionSelector) { s.Fence = false },
	} {
		bad := selector
		mutate(&bad)
		if bad.valid() {
			t.Fatal("nominal cross phase/foreign scope became a fence selector")
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	prior := 0
	gate := &nativeConjunctionBatchGate{ctx: ctx, selector: selector, priorBefore: func(context.Context, uint64, uint64, []byte) error { prior++; return nil }}
	if err := gate.before(ctx, 1, 3, []byte("unrelated inert source traffic")); err != nil || prior != 1 || gate.candidates != 0 || gate.arm.Sequence != 0 {
		t.Fatal("local fence observer changed unrelated source traffic", err)
	}
	certificate, err := encodeGraphCutCommand(graphCutCommand{scope: scope, configuration: [32]byte{1}, round: 2, phase: graphCutCertify})
	if err != nil {
		t.Fatal(err)
	}
	if err := gate.armObserved(ctx, allocationProposal{target: scope, wire: certificate}); !errors.Is(err, errInvalid) || gate.candidates != 1 || gate.decodeWork != 4096+32*processFrameBytes || gate.arm.Sequence != 0 || len(gate.selected) != 0 {
		t.Fatal("Certify masqueraded as genuine PrepareGraphFence or reached native RPC", err)
	}
	budget := nativeProtocolArtifactAllowance(t)
	if budget.Used() != 16<<20 || budget.Remaining() != 0 {
		t.Fatal("separate finite diagnostic output allowance not prepaid")
	}
}

// Inert callback/owner guards only: the mux below has no child, service, native
// proposal, vote or receipt. It cannot supply any campaign semantic evidence.
func TestNativeProtocolReplyHoldKeepsOneLiveContextAndSpentDiagnosticGrant(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	hub := &graphProcessHub{}
	priorCalls := 0
	hub.afterRelay = func(context.Context, uint64, uint64, []byte, []byte) error {
		priorCalls++
		return nil
	}
	f := &pairedGraphFaultNetwork{parent: ctx, n: &certifiedProcessNetwork{all: map[uint64]*graphProcess{1: {}, 3: {mux: &graphProcessMux{hub: hub}}}}}
	hub.peers = map[uint64]*graphProcessMux{1: {p: f.n.all[1]}}
	// Nonzero codec shape only, not a native certificate or service identity.
	scope := allocationScope{graph: [16]byte{1}, partition: 3, group: [16]byte{1}, ownership: 1, topology: 1, declaration: [32]byte{1}, semantic: [32]byte{1}}
	selector := nativeProtocolReplySelector{Actor: 3, Partition: 3, Phase: crossPrepare, Transaction: [16]byte{1}, Peer: graphPeerIdentity{scope: scope, voter: 1}}
	hold, err := newNativeProtocolReplyHold(ctx, f, selector)
	if err != nil {
		t.Fatal(err)
	}
	defer hold.close()
	if _, err := newNativeProtocolReplyHold(ctx, f, selector); !errors.Is(err, errLimit) {
		t.Fatal("second live hold must refuse before callback replacement", err)
	}
	if f.protocolReplyHold != hold {
		t.Fatal("failed second hold changed original ownership")
	}
	if err := hold.after(ctx, 5, 8, nil, nil); err != nil || hold.candidates != 0 || priorCalls != 1 {
		t.Fatal("unrelated traffic changed or consumed selected-source work", err)
	}
	for range nativeProtocolReplyCandidates + 1 {
		if err := hold.after(ctx, 3, 3, []byte("inert malformed diagnostic input"), nil); err != nil {
			t.Fatal("diagnostic failure invented a native result", err)
		}
	}
	if hold.candidates != nativeProtocolReplyCandidates || hold.work != nativeProtocolReplyCandidates*nativeProtocolReplyDecode || hold.output != 0 || hold.retained != 0 || hold.selected || !hold.closed || len(hold.entered) != 0 {
		t.Fatal("unmatched/exhausted observation became a successful source reply")
	}
	hold.release()
	hold.release()
	hold.close()
	hold.close()
	if f.protocolReplyHold != nil {
		t.Fatal("released hold retained its fixture slot")
	}
	if err := hub.afterRelay(ctx, 3, 3, nil, nil); err != nil || priorCalls != nativeProtocolReplyCandidates+3 {
		t.Fatal("LIFO teardown did not restore the actual prior callback", err, priorCalls)
	}
	hub.relayTargets = map[uint64]map[uint64]uint64{3: {3: 2}}
	if _, err := newNativeProtocolReplyHold(ctx, f, selector); !errors.Is(err, errInvalid) || f.protocolReplyHold != nil {
		t.Fatal("self-reported reply peer replaced the actual configured route", err)
	}
	hub.relayTargets = nil
	longer, stop := context.WithTimeout(t.Context(), 2*time.Minute)
	defer stop()
	if _, err := newNativeProtocolReplyHold(longer, f, selector); !errors.Is(err, errInvalid) || f.protocolReplyHold != nil {
		t.Fatal("a new hold extended the original fixture deadline", err)
	}
	f.parent = nil
	if _, err := newNativeProtocolReplyHold(ctx, f, selector); !errors.Is(err, errInvalid) {
		t.Fatal("fresh causal hold admitted an ordinary nil-context parent", err)
	}
}
