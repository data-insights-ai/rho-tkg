package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func TestNativeProtocolPendingMoveCatchupUsesCommittedAppliedPrefix(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n, coordinator, intent, _, _ := rangeMoveCoordinatorFixture(t, 64)
	lost := &movePoolLostDescribeReply{graphPeerWireTransport: coordinator.base.transports[3]}
	coordinator.base.transports[3] = lost
	pending, err := runRangeMoveToCompletion(t, coordinator, intent)
	if !errors.Is(err, errRangeMovePending) || !errors.Is(err, io.EOF) || len(lost.command) == 0 || pending.committed {
		t.Fatal("genuine pending move did not retain its original", err)
	}
	for _, host := range n.hosts[0] {
		if _, err := host.machine.observeAllocation(allocationQuery{kind: observeConfiguration, nonce: [16]byte{1}}, host.driver.Applied()); !errors.Is(err, errRangeMovePending) {
			t.Fatal("pending move unexpectedly grants allocation observation", err)
		}
		state := processResponse{applied: host.driver.Applied()}
		_, state.image, err = host.machine.store.Checkpoint()
		if err != nil {
			t.Fatal(err)
		}
		state.stats, _, _, err = processStats(host.machine.store)
		if err != nil || !nativeProtocolMoveCheckpointReady(state) {
			t.Fatal("real committed/applied checkpoint rejected because semantic config is fenced", err)
		}
		for _, bad := range []processResponse{
			func() processResponse { s := state; s.code = 1; return s }(),
			func() processResponse { s := state; s.stats[statCommit] = s.applied + 1; return s }(),
			func() processResponse { s := state; s.stats[statCheckpoint]--; return s }(),
			func() processResponse { s := state; s.image = nil; s.ready = true; return s }(),
		} {
			if nativeProtocolMoveCheckpointReady(bad) {
				t.Fatal("forged ready or incomplete native apply passed")
			}
		}
	}
	// Catch-up cannot complete the business operation or replace its identity.
	resume := pending.resume
	done := rangeMoveKeeperResume(t, coordinator, resume)
	if !done.committed || done.resume != resume || sha256.Sum256(lost.command) == ([32]byte{}) {
		t.Fatal("same native move did not complete through ordinary Resume")
	}
}

func TestNativeProtocolStateActionKeepsActualRetainedOwnership(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	n := collectedGraphNetworkWithRecovery(t, func(p *raftlog.ApplicationPolicy) {
		p.RetainedApplicationBytes, p.RetainedApplicationRecords = 256<<20, 131072
	}, rangeMovePooledSemanticContractID(), raftlog.ApplicationRecoveryConfig{Version: 4})
	_, publisher, _, _ := retainedPublisherFromNetwork(t, n)
	h := n.hosts[0][0]
	stats := func() [statCount]uint64 {
		t.Helper()
		actual, _, _, err := processStats(h.machine.store)
		if err != nil {
			t.Fatal(err)
		}
		return actual
	}
	clean := stats()
	owner, err := beginRetainedFeed(ctx, publisher, n.requestID(), graphReadRequest{mode: graphReadFresh}, 1, retainedFeedGuardPolicy(nativeProtocolStatePages))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := owner.Close(); err != nil {
			t.Error("actual retained feed cleanup", err)
		}
	}()
	if _, err := owner.Publish(ctx); err != nil {
		t.Fatal(err)
	}
	held, err := owner.OpenSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := held.Close(); err != nil {
			t.Error("actual held snapshot cleanup", err)
		}
	}()
	before := stats()
	if before[statViews] == 0 || before[statViewBytes] == 0 {
		t.Fatal("genuine feed snapshot did not own native views", before)
	}
	proof := n.read(0, allocationQuery{kind: observeAllocator})
	original, err := h.TransferAllocator(n.requestID(), proof)
	if err != nil {
		t.Fatal(err)
	}
	committed := n.submit(0, original)
	after := stats()
	t.Logf("actual retained ownership around allocator transfer: before views=%d viewBytes=%d pins=%d; after views=%d viewBytes=%d pins=%d", before[statViews], before[statViewBytes], before[statPinnedBytes], after[statViews], after[statViewBytes], after[statPinnedBytes])
	if committed.reason != reasonNone || committed.disposition != applied || !nativeProtocolStateOwnershipUnchanged(before, after) {
		t.Fatal("genuine retained owner mistaken for operation leak", committed, before, after)
	}
	assertCertifiedProjection(t, certifiedView(t, held), 1, true, nil)
	// A genuinely additional native view is a leak relative to the SAME
	// observed ownership. Closing it must restore that exact baseline.
	extra, err := h.machine.store.ApplicationView(h.driver.Applied())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := extra.Close(); err != nil {
			t.Error("additional native view cleanup", err)
		}
	}()
	if leaked := stats(); nativeProtocolStateOwnershipUnchanged(before, leaked) {
		t.Fatal("additional actual native view was not detected", before, leaked)
	}
	if err := extra.Close(); err != nil {
		t.Fatal(err)
	}
	if recovered := stats(); !nativeProtocolStateOwnershipUnchanged(before, recovered) {
		t.Fatal("closing additional view did not restore exact baseline", before, recovered)
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	if released := stats(); nativeProtocolStateOwnershipUnchanged(before, released) || !nativeProtocolStateOwnershipUnchanged(clean, released) {
		t.Fatal("dropping the existing held owner went undetected", before, released)
	}
}

func TestNativeProtocolStateHeldSnapshotKeepsCumulativeProjectionBudget(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	n := collectedGraphNetworkWithRecovery(t, func(p *raftlog.ApplicationPolicy) {
		p.RetainedApplicationBytes, p.RetainedApplicationRecords = 256<<20, 131072
	}, rangeMovePooledSemanticContractID(), raftlog.ApplicationRecoveryConfig{Version: 4})
	_, publisher, _, _ := retainedPublisherFromNetwork(t, n)
	owner, err := beginRetainedFeed(ctx, publisher, n.requestID(), graphReadRequest{mode: graphReadFresh}, 1, retainedFeedGuardPolicy(nativeProtocolStatePages))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	}()
	if _, err := owner.Publish(ctx); err != nil {
		t.Fatal(err)
	}
	read, err := owner.OpenSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := read.Close(); err != nil {
			t.Error(err)
		}
	}()
	before, count := nativeProtocolStateSnapshotBudgets(read)
	again, sameCount := nativeProtocolStateSnapshotBudgets(read)
	if count != 2 || sameCount != count || before != again {
		t.Fatal("passive diagnostic mutated actual source ledger", before, again, count, sameCount)
	}
	for i := range 2 {
		work := read.Work()
		if _, err := nativeTransitionPinProjectionFor(ctx, publisher.coordinator.host, read, true); err != nil {
			budgets, count := nativeProtocolStateSnapshotBudgets(read)
			t.Fatalf("projection%d workBefore=%+v workAfter=%+v sources=%+v: %v", i, work, read.Work(), budgets[:count], err)
		}
		after, afterCount := nativeProtocolStateSnapshotBudgets(read)
		if afterCount != count {
			t.Fatal("projection replaced actual retained sources", before, after)
		}
		for source := range count {
			prior, actual := before[source], after[source]
			if actual.Partition != prior.Partition || actual.Voter != prior.Voter || actual.CookieSequence != prior.CookieSequence || actual.Calls <= prior.Calls || actual.Rows <= prior.Rows || actual.Bytes <= prior.Bytes || actual.OutputLeft >= prior.OutputLeft || actual.MaxCalls != prior.MaxCalls || actual.MaxRows != prior.MaxRows || actual.MaxBytes != prior.MaxBytes || actual.PageRows != prior.PageRows || actual.PageBytes != prior.PageBytes {
				t.Fatal("projection reset budget or replaced source", prior, actual)
			}
		}
		t.Logf("genuine projection%d workBefore=%+v workAfter=%+v budgets=%+v", i, work, read.Work(), after[:afterCount])
		before = after
	}
}

func TestNativeProtocolStateSnapshotProjectionKeepsOldValuesAfterMutation(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	n := collectedGraphNetworkWithRecovery(t, func(p *raftlog.ApplicationPolicy) {
		p.RetainedApplicationBytes, p.RetainedApplicationRecords = 256<<20, 131072
	}, rangeMovePooledSemanticContractID(), raftlog.ApplicationRecoveryConfig{Version: 4})
	_, publisher, edge, _ := retainedPublisherFromNetwork(t, n)
	// These are genuine installed recipient sessions, not decoded authority.
	grants := [2]grantReference{}
	for group := range 2 {
		proof := n.read(group, allocationQuery{kind: observeRecipient, session: idalloc.RecipientSession{ID: [16]byte{byte(group + 1)}}})
		grants[group] = grantReference{session: proof.observation.recipient.session, sequence: 1}
	}
	window := edge[0].Scope
	set := func(group int, owner graphstate.EntityID, valueID graphstate.ValueID, value int64, healthy bool) {
		t.Helper()
		ops := []graphstate.Operation{{Kind: graphstate.Set, Owner: owner, Life: graphstate.LifeID(owner + 1), Scope: window, Name: "p", Value: graphstate.I64(value), ValueID: valueID}}
		if healthy {
			ops = append(ops, graphstate.Operation{Kind: graphstate.AddLabel, Owner: owner, Life: graphstate.LifeID(owner + 1), Scope: window, Name: "healthy"})
		}
		tx, proposal := n.crossPlan(ops, []freshBinding{{role: valueBinding, id: uint64(valueID), grant: grants[group]}}, 23)
		n.crossResolve(n.crossCommit(tx, proposal))
	}
	set(0, 1, 5, 7, false)
	set(1, 17, 19, 13, false)
	// The genuine cross-edge needs the original native grant, not caller IDs.
	edgeClaims := []freshBinding{{role: entityBinding, id: 3, grant: grants[0]}, {role: lifeBinding, owner: 3, id: 4, grant: grants[0]}}
	tx, proposal := n.crossPlan(edge, edgeClaims, 23)
	n.crossResolve(n.crossCommit(tx, proposal))
	owner, err := beginRetainedFeed(ctx, publisher, n.requestID(), graphReadRequest{mode: graphReadFresh}, 1, retainedFeedGuardPolicy(nativeProtocolStatePages))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	}()
	if _, err := owner.Publish(ctx); err != nil {
		t.Fatal(err)
	}
	held, err := owner.OpenSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := held.Close(); err != nil {
			t.Error(err)
		}
	}()
	before, count := nativeProtocolStateSnapshotBudgets(held)
	old, err := nativeTransitionPinProjectionFor(ctx, publisher.coordinator.host, held, true)
	if err != nil {
		t.Fatal("first genuine projection", err)
	}
	set(0, 1, 7, 9, true)
	// Re-project the SAME original held cut after a real business mutation. This
	// must consume the original cumulative ledger, without a new capture/grant.
	again, err := nativeTransitionPinProjectionFor(ctx, publisher.coordinator.host, held, true)
	if err != nil {
		budgets, actual := nativeProtocolStateSnapshotBudgets(held)
		t.Fatalf("second held projection work=%+v budgets=%+v: %v", held.Work(), budgets[:actual], err)
	}
	if !reflect.DeepEqual(old, again) {
		t.Fatal("old held snapshot substituted current values")
	}
	after, actual := nativeProtocolStateSnapshotBudgets(held)
	if count != actual {
		t.Fatal("held source count changed")
	}
	for i := range count {
		if after[i].CookieSequence != before[i].CookieSequence || after[i].Partition != before[i].Partition || after[i].Voter != before[i].Voter || after[i].Calls <= before[i].Calls || after[i].MaxCalls != before[i].MaxCalls {
			t.Fatal("source identity or original allowance changed", before[i], after[i])
		}
	}
	current, err := publisher.coordinator.OpenCertified(ctx, graphReadRequest{mode: graphReadFresh}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := current.Close(); err != nil {
			t.Error(err)
		}
	}()
	now, err := nativeTransitionPinProjectionFor(ctx, publisher.coordinator.host, current, true)
	if err != nil {
		t.Fatal("current exact projection", err)
	}
	if reflect.DeepEqual(old, now) || len(old.Entities) != 4 || len(now.Entities) != 4 || old.Entities[0].Property.I64 != 7 || now.Entities[0].Property.I64 != 9 || len(old.Facts[0].Labels) != 0 || !slices.Equal(now.Facts[0].Labels, []string{"healthy"}) {
		t.Fatal("two-phase oracle did not distinguish exact original and current", old, now)
	}
	for _, projection := range []*nativeTransitionPinRead{old, now} {
		if len(projection.Facts) != 32 || projection.Facts[6].Exists || projection.Entities[2].Found {
			t.Fatal("phantom unallocated entity7", projection)
		}
	}

	// A completed real projection is not a successful result if its owned
	// cleanup fails. Retire a genuine source scope, then repeat its retirement
	// through the exact deferred seam; this is an actual ownership refusal.
	arena, _, _, err := nativeTransitionPinObservationSources(held)
	if err != nil {
		t.Fatal(err)
	}
	_, finish, err := nativeTransitionPinGetReuse(ctx, held, arena)
	if err != nil {
		t.Fatal(err)
	}
	if err := finish(); err != nil {
		t.Fatal(err)
	}
	completed := old
	var cleanupErr error
	nativeTransitionPinFinishProjection(&completed, &cleanupErr, finish)
	if completed != nil || !errors.Is(cleanupErr, errInvalid) {
		t.Fatal("failed cleanup returned a completed projection", completed, cleanupErr)
	}
}

func TestNativeProtocolStateReplayBarrierRequiresNewNativeInstallation(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n, coordinator, ops, claims, _ := coordinatorNativeFixture(t)
	id := n.requestID()
	result, err := coordinator.Run(t.Context(), id, ops, 37, claims)
	if err != nil || !result.decision.commit {
		t.Fatal(err)
	}
	decision := n.crossReadPhase(0, result.resume.tx, crossDecide)
	stateOf := func(h *allocationHost) processResponse {
		t.Helper()
		r := processResponse{applied: h.driver.Applied()}
		_, r.image, err = h.machine.store.Checkpoint()
		if err != nil {
			t.Fatal(err)
		}
		r.stats, _, _, err = processStats(h.machine.store)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	recordOf := func(h *allocationHost, index uint64) processResponse {
		t.Helper()
		var r processResponse
		r.outcome, err = h.machine.store.ApplicationRecord(t.Context(), index, true, 64<<10)
		if err != nil {
			t.Fatal(err)
		}
		r.changes, err = h.machine.store.ApplicationRecord(t.Context(), index, false, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	for group, partition := range []uint64{3, 8} {
		h := n.hosts[group][0]
		before := stateOf(h)
		proposal, err := n.hosts[0][0].ResolveCrossParticipant(partition, decision)
		if err != nil {
			t.Fatal(err)
		}
		queued, err := h.Submit(proposal)
		if err != nil || len(queued.packets) == 0 {
			t.Fatal("genuine duplicate must await native delivery", err)
		}
		// This is the same real observation used by the child pollSubmit path:
		// immutable completion exists even while the duplicate is unapplied.
		early, _, err := h.machine.observeCrossTransaction(crossQuery{tx: result.resume.tx, nonce: [16]byte{1}, phase: crossResolve}, h.driver.Applied())
		if err != nil || early.phase != crossResolve || early.tx != result.resume.tx || h.driver.Applied() != before.applied {
			t.Fatal("actual early terminal observation", early, err)
		}
		original := recordOf(h, early.index)
		earlyBody, err := encodeCrossStatus(early)
		if err != nil {
			t.Fatal(err)
		}
		// The actual source reply contains this terminal status and no CDC;
		// it must not be promoted into a newly installed native record.
		if err := nativeProtocolStateReplayInstalled(result, before, stateOf(h), proposal.wire, processResponse{outcome: earlyBody}, original); !errors.Is(err, errInvalid) {
			t.Fatal("old terminal reply substituted for new replay installation", err)
		}
		n.pump(group, queued)
		for _, voter := range n.hosts[group] {
			after := stateOf(voter)
			record := recordOf(voter, after.applied)
			entries, err := voter.machine.store.Entries(after.applied, after.applied+1, 64<<10)
			if err != nil || len(entries) != 1 || !bytes.Equal(entries[0].GetData(), proposal.wire) {
				t.Fatal("actual installed command differs from queued SAME original", err)
			}
			if err := nativeProtocolStateReplayInstalled(result, before, after, entries[0].GetData(), record, original); err != nil {
				t.Fatal("actual fully installed replay rejected", partition, err)
			}
			withCDC := record
			withCDC.changes = original.changes
			if err := nativeProtocolStateReplayInstalled(result, before, after, proposal.wire, withCDC, original); !errors.Is(err, errInvalid) {
				t.Fatal("original business CDC emitted a second time", err)
			}
			// Earlier genuine Register/Prepare/Decide commands cannot qualify
			// this boundary even though a late duplicate may return Resolve.
			seen := make(map[crossPhase]bool)
			for index := uint64(2); index <= before.applied; index++ {
				entries, err := voter.machine.store.Entries(index, index+1, 64<<10)
				if err != nil || len(entries) != 1 {
					t.Fatal(err)
				}
				command, _, err := decodeCrossCommand(entries[0].GetData(), defaultMaterializerLimits())
				if err != nil || command.phase == crossResolve {
					continue
				}
				seen[command.phase] = true
				if err := nativeProtocolStateReplayInstalled(result, before, after, entries[0].GetData(), record, original); !errors.Is(err, errInvalid) {
					t.Fatal("non-Resolve command substituted for queued terminal replay", command.phase, err)
				}
				if err := nativeProtocolStateReplayInstalled(result, before, after, proposal.wire, record, recordOf(voter, index)); !errors.Is(err, errInvalid) {
					t.Fatal("different retained original substituted for exact Resolve", command.phase, err)
				}
			}
			if !seen[crossPrepare] || group == 0 && (!seen[crossRegister] || !seen[crossDecide]) {
				t.Fatal("missing genuine adverse native phases", seen)
			}
		}
	}
	beforeChanged := [2]uint64{n.hosts[0][0].driver.Applied(), n.hosts[1][0].driver.Applied()}
	if changed, err := coordinator.Run(t.Context(), id, ops, 38, claims); !errors.Is(err, errInvalid) || changed.decision != (crossStatus{}) || beforeChanged != [2]uint64{n.hosts[0][0].driver.Applied(), n.hosts[1][0].driver.Applied()} {
		t.Fatal("changed body installed a decision or native prefix", changed, err)
	}
	h := n.hosts[0][0]
	before := stateOf(h)
	late, err := h.PreparePartitionGraphOperations(n.requestID(), []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Scope: ops[0].Scope, Name: "not-a-replay-barrier"}}, 39, nil)
	if err != nil {
		t.Fatal(err)
	}
	n.submitPartitionGraph(0, late, reasonNone)
	after := stateOf(h)
	if err := nativeProtocolStateReplayInstalled(result, before, after, late.wire, recordOf(h, after.applied), recordOf(h, decision.status.index)); !errors.Is(err, errInvalid) {
		t.Fatal("late business effect accepted as replay completion", err)
	}
}

func TestNativeProtocolPooledStateLeaderAndMessagePrograms(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for _, layout := range []string{"original", "rotated"} {
		for _, message := range []string{"literal", "delay", "reorder", "duplicate"} {
			spec := nativeProtocolProgramSpec{Version: 1, Layout: layout, Message: message}
			t.Run(layout+"/"+message, func(t *testing.T) {
				for index, recipe := range nativeProtocolStateInventory() {
					// Accepted P08 issuance and its genuine pooled8 observations
					// retain their exact original parents/profile attribution.
					if recipe.Family == "allocation" {
						continue
					}
					if testing.Short() && index != 0 && index != 15 && index != 31 && index != 47 && index != 67 && index != 68 && index != 99 && index != 127 && index != 128 && index != 136 && index != 138 {
						continue
					}
					t.Run(fmt.Sprintf("%s-phase%d-p%d-page%d-kind%d-%s", recipe.Family, recipe.Phase, recipe.Partition, recipe.Page, recipe.Kind, recipe.Side), func(t *testing.T) {
						t.Setenv("RHO_GRAPH_PROTOCOL_STATE_RECOVERY", "4")
						artifact, deadline := nativeProtocolStateCaseAt(t, recipe, spec)
						ctx, cancel := nativeProtocolStateCheckerContext(t, deadline)
						defer cancel()
						command := exec.CommandContext(ctx, "python3", "-B", filepath.Join("..", "..", "..", "tools", "v5-substrate", "native_protocol_campaign_state_program_check.py"), artifact)
						var diagnostics processStderr
						command.Stdout, command.Stderr = &diagnostics, &diagnostics
						if err := command.Run(); err != nil {
							body, _ := diagnostics.state()
							t.Fatal("independent state roles/messages and full semantic oracle", err, body)
						}
						if _, overflow := diagnostics.state(); overflow {
							t.Fatal("bounded state program diagnostics")
						}
					})
				}
			})
		}
	}
}

func TestNativeProtocolStateProgramsKeepNativeIdentityAndCausalWindows(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	ctx, cancel := nativeProtocolStateCheckerContext(t, time.Now().Add(-time.Second).UnixNano())
	defer cancel()
	if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatal("checker reacquired expired source lifetime", ctx.Err())
	}
	// Inert callbacks exercise ownership only; none supplies a native receipt,
	// source proof, term, role mapping or successful business observation.
	h := &graphProcessHub{}
	before, after := 0, 0
	h.beforeRelay = func(context.Context, uint64, uint64, []byte) error { before++; return nil }
	h.afterRelay = func(context.Context, uint64, uint64, []byte, []byte) error { after++; return nil }
	f := &pairedGraphFaultNetwork{t: t, n: &certifiedProcessNetwork{all: map[uint64]*graphProcess{4: {mux: &graphProcessMux{hub: h}}}}}
	_, release, restore := nativeProtocolStateCommittedGate(f)
	h.mu.Lock()
	activeBefore, activeAfter := h.beforeRelay, h.afterRelay
	h.mu.Unlock()
	if err := activeBefore(t.Context(), 0, 0, nil); err != nil {
		t.Fatal(err)
	}
	if err := activeAfter(t.Context(), 0, 0, nil, nil); err != nil {
		t.Fatal(err)
	}
	if before != 1 || after != 1 {
		t.Fatal("actual observation hooks lost while hold owns its window", before, after)
	}
	release()
	for range 2 {
		if err := restore(); err != nil {
			t.Fatal("idempotent same callback owner restoration", err)
		}
	}
	if err := h.beforeRelay(t.Context(), 0, 0, nil); err != nil {
		t.Fatal(err)
	}
	if err := h.afterRelay(t.Context(), 0, 0, nil, nil); err != nil {
		t.Fatal(err)
	}
	if before != 2 || after != 2 {
		t.Fatal("restored exact callbacks were replaced by no-ops")
	}
	late := nativeProtocolStateSavedHooks(h)
	for range 2 {
		if err := late(); !errors.Is(err, errInvalid) {
			t.Fatal("non-LIFO replacement overwrote active owner", err)
		}
	}
	if err := h.beforeRelay(t.Context(), 0, 0, nil); err != nil || before != 3 {
		t.Fatal("refusal mutated live hooks", err)
	}
	allocation := 0
	for _, recipe := range nativeProtocolStateInventory() {
		if recipe.Family == "allocation" {
			allocation++
		}
	}
	if allocation != 8 {
		t.Fatal("accepted issuance source attribution changed", allocation)
	}
}

func TestNativeProtocolPoolProofDoesNotCancelSharedClientLifetime(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	// An expired diagnostic operation must fail before IPC. It cannot replace
	// the shared feed client's independently retained original context.
	original := t.Context()
	expired, cancel := context.WithCancel(original)
	cancel()
	process := &graphProcess{mux: &graphProcessMux{}}
	network := &certifiedProcessNetwork{leaders: [2]uint64{2, 6}, all: map[uint64]*graphProcess{2: process}}
	client := &nativeProtocolStateClient{t: t, ctx: original, n: network, member: 2, cookie: [16]byte{1}}
	capture := &nativeProtocolPoolCapture{t: t, ctx: expired, n: network, clients: map[uint64]*nativeProtocolStateClient{2: client}, processes: map[uint64]*graphProcess{2: process}}
	if _, err := capture.proof(original, 3, nil, nil); !errors.Is(err, context.Canceled) {
		t.Fatal("expired proof did not refuse before native effects", err)
	}
	if client.ctx != original || client.ctx.Err() != nil || client.sequence != 1 {
		t.Fatal("diagnostic replaced client lifetime or refunded failed sequence", client.ctx.Err(), client.sequence)
	}
}

func TestNativeProtocolMoveBoundaryReportsEarlyFailureWithoutRenewingDeadline(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	ctx := t.Context()
	done := make(chan graphProcessAnswer, 1)
	done <- graphProcessAnswer{err: errInvalid}
	actual, err := nativeProtocolStateWaitMoveBoundary(ctx, make(chan struct{}), done)
	if !errors.Is(err, errInvalid) || actual == nil || !errors.Is(actual.err, errInvalid) || ctx.Err() != nil {
		t.Fatal("failed invocation hidden until original deadline", err)
	}
	lost := make(chan struct{})
	close(lost)
	if _, err := nativeProtocolStateWaitMoveBoundary(ctx, lost, nil); err != nil {
		t.Fatal("actual observed loss replaced by absent caller reply", err)
	}
}

func TestNativeProtocolPooledMoveFactoryKeepsActualIntentAndFinitePools(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	t.Setenv("RHO_GRAPH_FAULT_FAMILY_PROCESS", "1")
	t.Setenv("RHO_GRAPH_DURABLE_MODEL", "off")
	n := collectedGraphNetworkWithRecovery(t, nil, rangeMovePooledSemanticContractID(), raftlog.ApplicationRecoveryConfig{Version: 4})
	_, _, _, _ = retainedPublisherFromNetwork(t, n)
	owner, err := newNativeLogicalTransitionOwner(t.Context(), n.hosts[0][0], 1)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.close()
	family, _, err := newNativeFaultFamilyOwner(t.Context(), t.TempDir(), 1, true)
	if err != nil {
		t.Fatal(err)
	}
	defer family.close()
	input := nativeTransitionMovePrepare{Version: 1, Cookie: owner.cookie, FamilyCookie: family.cookie, RangeID: 1, Target: 8, Pages: graphE17AdmittedMovePages, Rows: 8, Bytes: 16 << 10}
	body, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	request := processRequest{op: processNativeTransitionControl, sequence: 1, data: append([]byte(nativeTransitionMovePrepareMarker), body...)}
	wire, err := owner.prepareMove(t.Context(), family, [16]byte{1}, request)
	if err != nil {
		t.Fatal("actual pooled factory admission", err)
	}
	if family.move == nil || family.move.source != owner.host.machine.scope() || !family.move.pools.valid(true) || family.move.pools != (rangeMovePools{2 << 20, 8 << 20, 8 << 20}) || family.moveCalls != 1 {
		t.Fatal("pooled factory omitted exact finite grants or actual private intent")
	}
	original := family.move
	replay, err := owner.prepareMove(t.Context(), family, [16]byte{1}, request)
	if err != nil || string(replay) != string(wire) || family.move != original || family.moveCalls != 1 {
		t.Fatal("factory replay replaced private intent or repeated admission", err)
	}
}

func TestNativeProtocolStateStartBindsParentAndChildAgreement(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	// The parent is an ordinary test process, not an already selected replica.
	// Starting the fixture must bind every required role before it validates
	// its first genuine proposal; child-only extraEnv cannot repair this parent.
	t.Setenv("RHO_GRAPH_CERTIFIED_PROCESS", "")
	t.Setenv("RHO_GRAPH_SOURCE_PROCESS", "")
	t.Setenv("RHO_GRAPH_PAIRED_PROCESS", "")
	t.Setenv("RHO_GRAPH_PROTOCOL_STATE_RECOVERY", "4")
	ctx, run, deadline := nativeProtocolStateStart(t)
	// startGraphProcess inherits this exact role before Open. The actual
	// replacement already selects it; initial voters must own the same bounded
	// snapshot registry before Offers is polled during native catch-up.
	if os.Getenv("RHO_GRAPH_PAIRED_PROCESS") != "1" {
		t.Fatal("initial state cohort lacks replacement snapshot-owner role")
	}
	if run == ([16]byte{}) || ctx.Err() != nil {
		t.Fatal("state start lost original admission", ctx.Err())
	}
	if actual, ok := ctx.Deadline(); !ok || actual.UnixNano() != deadline {
		t.Fatal("parent acquired a different deadline")
	}
	if selected, err := nativeProtocolStateProfileFromEnv(); err != nil || !selected {
		t.Fatal("parent and child profile selection disagree", selected, err)
	}
	if agreement, err := nativeProtocolStateAgreement(); err != nil || agreement != rangeMovePooledSemanticContractID() {
		t.Fatal("parent cannot validate actual pooled child bootstrap", err)
	}
	// The correction supplies missing admission, rather than weakening the
	// selector: either omitted base role must still refuse the pooled contract.
	for _, key := range []string{"RHO_GRAPH_CERTIFIED_PROCESS", "RHO_GRAPH_SOURCE_PROCESS"} {
		t.Setenv(key, "")
		if agreement, err := nativeProtocolStateAgreement(); !errors.Is(err, errInvalid) || agreement != ([32]byte{}) {
			t.Fatal("missing actual base role admitted", key, err)
		}
		t.Setenv(key, "1")
	}
	t.Setenv("RHO_GRAPH_PROTOCOL_STATE_RECOVERY", "")
	if agreement, err := nativeProtocolStateAgreement(); err != nil || agreement != rangeMoveSpanSemanticContractID() {
		t.Fatal("literal Recovery3 agreement changed", err)
	}
}

func TestNativeProtocolStateCompoundCleanupKeepsPrimitiveControlSlot(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for _, action := range []string{"close", "snapshot-close", "release", "expire", "cancel", "pin-close"} {
		body, err := json.Marshal(nativeProtocolStateControl{Version: 1, Cookie: [16]byte{1}, Sequence: 1, Action: action})
		if err != nil {
			t.Fatal(err)
		}
		r := processRequest{op: processNativeTransitionControl, data: append([]byte(nativeProtocolStateMarker), body...)}
		if !nativeProtocolStateTerminalControl(r) {
			t.Fatal("actual encoded terminal not recognized", action)
		}
		control, terminal := nativeProtocolStateWorkerClass(r, true)
		if control || !terminal {
			t.Fatal("compound cleanup occupies primitive slot or loses terminal lifetime", action, control, terminal)
		}
		// A malformed/sibling request cannot adopt NPS terminal lifetime.
		r.index = 1
		if control, terminal = nativeProtocolStateWorkerClass(r, false); control || terminal {
			t.Fatal("malformed terminal admitted", action)
		}
	}
	primitive, err := encodeGraphSourceRequest(graphSourceRequest{kind: sourceClose, sequence: 1, nonce: [16]byte{1}, cookie: graphSourceCookie{issuer: [16]byte{2}, sequence: 3, secret: [16]byte{4}}})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeGraphSourceRequest(primitive)
	if err != nil || decoded.kind != sourceClose {
		t.Fatal("actual native close framing", err)
	}
	if control, terminal := nativeProtocolStateWorkerClass(processRequest{op: processSourceServe, entity: 2, data: primitive}, true); !control || !terminal {
		t.Fatal("nested native close lost reserved lane or cleanup lifetime")
	}
	for _, control := range []bool{false, true} {
		// Primitive native cleanup and all unrelated request classes remain on
		// their existing lane, with exactly the original lifetime selection.
		r := processRequest{op: processSourceServe}
		actual, terminal := nativeProtocolStateWorkerClass(r, control)
		if actual != control || terminal != control {
			t.Fatal("primitive or unrelated lane changed", control)
		}
	}
}

func TestNativeProtocolPooledTailBackpressurePreservesMoveOwner(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	n, publisher, ops, wires := retainedPublisherFromNetwork(t, collectedGraphNetworkWithRecovery(t, func(p *raftlog.ApplicationPolicy) {
		p.RetainedApplicationBytes, p.RetainedApplicationRecords = 256<<20, 131072
		if err := nativeProtocolStatePooledTailPolicy(p); err != nil {
			t.Fatal(err)
		}
	}, rangeMovePooledSemanticContractID(), raftlog.ApplicationRecoveryConfig{Version: 4}))
	policy := retainedFeedGuardPolicy(64)
	deadline, _ := ctx.Deadline()
	policy.deadlineUnixNanos = deadline.UnixNano()
	feed, err := beginRetainedFeed(ctx, publisher, n.requestID(), graphReadRequest{mode: graphReadFresh}, 1, policy)
	if err != nil {
		t.Fatal(err)
	}
	token, err := feed.Publish(ctx)
	if err != nil {
		t.Fatal(err)
	}
	held, err := feed.OpenSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	foreign, err := beginRetainedFeed(ctx, publisher, n.requestID(), graphReadRequest{mode: graphReadFresh}, 1, policy)
	if err != nil {
		t.Fatal(err)
	}
	other, err := foreign.Publish(ctx)
	if err != nil || bytes.Equal(other.definition, token.definition) {
		t.Fatal(err)
	}
	if err = foreign.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err = foreign.Close(); err != nil {
		t.Fatal(err)
	}
	if err = held.Close(); err != nil {
		t.Fatal(err)
	}
	if err = feed.Close(); err != nil {
		t.Fatal(err)
	}
	feed, err = openRetainedFeed(ctx, publisher, token)
	if err != nil {
		t.Fatal(err)
	}
	held, err = feed.OpenSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := held.Close(); err != nil {
			t.Error(err)
		}
		if err := feed.Close(); err != nil {
			t.Error(err)
		}
	}()
	intent, err := n.hosts[0][0].PreparePooledRangeMove(ctx, n.requestID(), 1, 8, graphE17AdmittedMovePages, 8, 16<<10, rangeMovePools{2 << 20, 8 << 20, 8 << 20})
	if err != nil {
		t.Fatal(err)
	}
	h := n.hosts[0][0]
	// Queue only genuine pre-fence originals. They must remain unable to
	// mutate the fenced graph while exercising the actual native tail limit.
	queued := make([]allocationProposal, nativeProtocolStateTailEntries)
	for i := range queued {
		queued[i], err = h.PreparePartitionGraphOperations(n.requestID(), []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Scope: ops[0].Scope, Name: "must-not-install"}}, uint64(100+i), nil)
		if err != nil {
			t.Fatal(err)
		}
	}
	publisher.coordinator.transports[8] = &rangeMoveCoordinatorWire{nativeGraphTestTransport: &nativeGraphTestTransport{service: wires[1].service}, dropFence: true}
	move, err := newRangeMoveCoordinatorOn(publisher.coordinator)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := move.Run(ctx, intent)
	if !errors.Is(err, errRangeMovePending) || !errors.Is(err, io.EOF) || pending.source.phase != rangeMoveFence || pending.source.result != rangeMoveApplied {
		t.Fatal("actual source Fence before unknown target", pending, err)
	}
	protected := rangeMoveAdmissionOwner(t, h)
	beforeDrain := rangeMoveImportGuardReader(t, h).base
	var deliveries []replica.Packet
	for i := range nativeProtocolStateTailEntries - 1 {
		event, err := h.Submit(queued[i])
		if err != nil {
			t.Fatal("finite pending tail refused before admitted window", i, err)
		}
		deliveries = append(deliveries, event.packets...)
	}
	if _, err := h.Submit(queued[nativeProtocolStateTailEntries-1]); !errors.Is(err, raftlog.ErrLimit) {
		t.Fatal("native tail did not backpressure", err)
	}
	n.pump(0, allocationEvent{packets: deliveries})
	if owner := rangeMoveAdmissionOwner(t, h); owner != protected {
		t.Fatal("backpressure lost recovery owner", owner, protected)
	}
	afterDrain := rangeMoveImportGuardReader(t, h).base
	if !bytes.Equal(beforeDrain.Image, afterDrain.Image) || beforeDrain.ImageHash != afterDrain.ImageHash {
		t.Fatal("fenced queued writes changed graph image")
	}
	targetWire := publisher.coordinator.transports[8]
	zeroGate := &rangeMoveImportGuardGate{graphPeerWireTransport: targetWire, mode: 2}
	publisher.coordinator.transports[8] = zeroGate
	rangeMoveImportGuardResumeToPause(t, move, pending.resume, zeroGate)
	zeroCommand, err := decodeRangeMoveCommand(zeroGate.importZero.wire)
	if err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*rangeMoveStatus){
		func(s *rangeMoveStatus) { s.sequence++ },
	} {
		bad := zeroCommand
		bad.proofs = slices.Clone(zeroCommand.proofs)
		for i := range bad.proofs {
			if bad.proofs[i].ns.partition == intent.source.partition {
				mutate(&bad.proofs[i])
				break
			}
		}
		if _, err := rangeMoveImportGuardStage(t, n.hosts[1][0], bad); !errors.Is(err, errInvalid) {
			t.Fatal("unauthenticated source summary reached Import0 batch", err)
		}
	}
	gate := &rangeMoveAdmissionImportGate{graphPeerWireTransport: targetWire, sequence: 1}
	publisher.coordinator.transports[8] = gate
	paused, pauseErr := runMovePoolResume(t, move, pending.resume)
	if !gate.stopped || !errors.Is(pauseErr, io.EOF) || !errors.Is(pauseErr, errRangeMovePending) || paused.resume != pending.resume {
		t.Fatal("genuine Import0 did not reach next-page boundary", pauseErr)
	}
	target := n.hosts[1][2]
	q, cfg := rangeMoveBaselineGuardReader(t, target.machine, target.driver.Applied())
	status, found, err := readRangeMoveStatus(q, pending.resume.move)
	if err != nil || !found || status.phase != rangeMoveImport || status.sequence != 0 {
		t.Fatal("actual installed Import0", status, err)
	}
	progressWire, found, err := q.getBounded(rangeMoveProgressKey(q.ns, status.move), rangeMoveSpanProgressBytes)
	if err != nil || !found {
		t.Fatal(err)
	}
	progress, err := decodeRangeMoveProgress(progressWire, q.arena)
	if err != nil || progress.imported == nil || progress.replayPages != 0 || progress.retentionRows == 0 || status.rows != progress.imported.Rows+progress.retentionRows || status.size != progress.imported.Bytes+progress.retainedBytes() {
		t.Fatal("Import0 published full manifest instead of installed prefix", status, err)
	}
	original, found, err := crossControl(q, rangeMoveControlKey(q.ns, status.move, 'M'), 256)
	if err != nil || !found || original.Index != status.index {
		t.Fatal("actual atomically installed manifest proof", err)
	}
	nativeProtocolAssertManifestProofRefusals(t, intent, status, progress, original)
	baseline, err := target.machine.rangeMoveCatalogBaseline(q, cfg)
	if err != nil || baseline == nil {
		t.Fatal("genuine Import0 baseline", err)
	}
	if err := baseline.Close(); err != nil {
		t.Fatal(err)
	}
	if err := q.view.Close(); err != nil {
		t.Fatal(err)
	}
	configuration := n.configs[1][2]
	configuration.Create = false
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := raftlog.Open(configuration)
	if err != nil {
		t.Fatal(err)
	}
	machine, err := newRangeMovePooledMaterializer(store, target.machine.ns, n.d, target.machine.limits)
	if err != nil {
		t.Fatal("same-directory Import0 Restore", err)
	}
	reopened, err := openAllocationHost(machine, 3, replica.Config{Store: store, ApplicationSnapshotSends: replica.ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 4 << 20}})
	if err != nil {
		t.Fatal(err)
	}
	n.hosts[1][2] = reopened
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	})
	publisher.coordinator.transports[8] = targetWire
	result, err := runMovePoolResume(t, move, pending.resume)
	if err != nil || !result.committed || result.resume != pending.resume {
		t.Fatal("same move did not complete after real tail drain", result.source, result.target, err)
	}
	completedQ := rangeMoveImportGuardReader(t, n.hosts[1][0])
	completedWire, found, err := completedQ.getBounded(rangeMoveProgressKey(completedQ.ns, result.resume.move), rangeMoveSpanProgressBytes)
	if err != nil || !found {
		t.Fatal(err)
	}
	completed, err := decodeRangeMoveProgress(completedWire, completedQ.arena)
	if err != nil {
		t.Fatal(err)
	}
	if digest, err := targetMoveManifestDigest(intent, result.target, completed, raftlog.ApplicationControlRecord{}, false); err != nil || digest != result.target.digest {
		t.Fatal("complete old image lost original full manifest binding", err)
	}
}

func TestNativeProtocolStateTailPolicyIsExplicitAndBounded(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	if err := nativeProtocolStatePooledTailPolicy(nil); !errors.Is(err, errInvalid) {
		t.Fatal("nil policy", err)
	}
	base := raftlog.DefaultApplicationPolicy(1)
	base.RetainedApplicationBytes = 256 << 20
	for _, mutate := range []func(*raftlog.ApplicationPolicy){
		func(p *raftlog.ApplicationPolicy) { p.MaxTailEntries-- },
		func(p *raftlog.ApplicationPolicy) { p.MaxInstallBytes-- },
		func(p *raftlog.ApplicationPolicy) { p.RetainedApplicationBytes-- },
	} {
		p := base
		mutate(&p)
		before := p
		if err := nativeProtocolStatePooledTailPolicy(&p); !errors.Is(err, errInvalid) || p != before {
			t.Fatal("foreign policy changed", err)
		}
	}
	p := base
	if err := nativeProtocolStatePooledTailPolicy(&p); err != nil || p.MaxTailEntries != 19 {
		t.Fatal("explicit D-only admission", err, p.MaxTailEntries)
	}
	p.MaxTailEntries = base.MaxTailEntries
	if p != base {
		t.Fatal("other budgets changed")
	}
}

// These are malformed diagnostic records, never native proposals or authority.
func nativeProtocolAssertManifestProofRefusals(t *testing.T, intent rangeMoveIntent, status rangeMoveStatus, progress rangeMoveProgress, original raftlog.ApplicationControlRecord) {
	t.Helper()
	if digest, err := targetMoveManifestDigest(intent, status, progress, original, true); err != nil || digest != status.digest || progress.replayPages != 0 {
		t.Fatal("genuine manifest changed installed cursor", err)
	}
	if _, err := targetMoveManifestDigest(intent, status, progress, original, false); !errors.Is(err, errCorrupt) {
		t.Fatal("missing original accepted incomplete replay", err)
	}
	for _, mutate := range []func(*rangeMoveStatus){
		func(s *rangeMoveStatus) { s.ns.partition = intent.target.partition },
		func(s *rangeMoveStatus) { s.move[0] ^= 1 },
		func(s *rangeMoveStatus) { s.intent[0] ^= 1 },
		func(s *rangeMoveStatus) { s.sequence++ },
		func(s *rangeMoveStatus) { s.phase = rangeMoveCapture },
		func(s *rangeMoveStatus) { s.result = rangeMoveLimit },
		func(s *rangeMoveStatus) { s.rows++ },
		func(s *rangeMoveStatus) { s.size++ },
		func(s *rangeMoveStatus) { s.digest[0] ^= 1 },
	} {
		bad := original
		proof, err := decodeRangeMoveStatus(original.Value)
		if err != nil {
			t.Fatal(err)
		}
		mutate(&proof)
		bad.Value, err = encodeRangeMoveStatus(proof)
		if err != nil {
			t.Fatal("canonical adversarial proof", err)
		}
		if _, err := targetMoveManifestDigest(intent, status, progress, bad, true); !errors.Is(err, errCorrupt) {
			t.Fatal("foreign or resealed original accepted", err)
		}
	}
	for _, mutate := range []func(*raftlog.ApplicationControlRecord){
		func(r *raftlog.ApplicationControlRecord) { r.Key = []byte("foreign") },
		func(r *raftlog.ApplicationControlRecord) { r.Index = 0 },
		func(r *raftlog.ApplicationControlRecord) {
			r.Value = bytes.Clone(r.Value)
			r.Value[len(r.Value)-1] ^= 1
		},
	} {
		bad := original
		mutate(&bad)
		if _, err := targetMoveManifestDigest(intent, status, progress, bad, true); !errors.Is(err, errCorrupt) {
			t.Fatal("foreign control envelope accepted", err)
		}
	}
	for _, mutate := range []func(*rangeMoveProgress){
		func(p *rangeMoveProgress) { p.descriptor.Digest[0] ^= 1 },
		func(p *rangeMoveProgress) { p.reference.packet[0] ^= 1 },
		func(p *rangeMoveProgress) { p.replayEnd.digest[0] ^= 1 },
	} {
		bad := progress
		mutate(&bad)
		wire, err := encodeRangeMoveProgress(bad, moveSpanOutput(t))
		if err != nil {
			t.Fatal("canonical adversarial progress", err)
		}
		bad, err = decodeRangeMoveProgress(wire, moveSpanOutput(t))
		if err != nil {
			t.Fatal("resealed diagnostic framing", err)
		}
		if _, err := targetMoveManifestDigest(intent, status, bad, original, true); !errors.Is(err, errCorrupt) {
			t.Fatal("changed manifest accepted against original proof", err)
		}
	}
	badStatus := status
	badStatus.rows++
	if _, err := targetMoveManifestDigest(intent, badStatus, progress, original, true); !errors.Is(err, errCorrupt) {
		t.Fatal("changed installed prefix accepted", err)
	}
}

func TestNativeCollectedRangeMoveImportZeroPrefixAndRetainedOldView(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for _, profile := range []struct {
		name      string
		agreement raftlog.ApplicationSemanticContractID
		recovery  uint32
	}{{"collected", collectedGraphSemanticContractID(), 2}, {"span", rangeMoveSpanSemanticContractID(), 3}, {"batched", rangeMoveGraphBatchSemanticContractID(), 3}} {
		t.Run(profile.name, func(t *testing.T) {
			n, publisher, ops, _ := retainedPublisherFromNetwork(t, collectedGraphNetworkWithRecovery(t, func(p *raftlog.ApplicationPolicy) {
				p.RetainedApplicationBytes, p.RetainedApplicationRecords, p.MaxTailEntries = 256<<20, 131072, nativeProtocolStateTailEntries
			}, profile.agreement, raftlog.ApplicationRecoveryConfig{Version: profile.recovery}))
			view, closeOld := crossView(t, n, 0, [2]uint64{})
			defer closeOld()
			assertCertifiedProjection(t, view, 1, true, nil)
			intent, err := n.hosts[0][0].PrepareRangeMove(t.Context(), n.requestID(), 1, 8, 32, 32, 16<<10)
			if err != nil {
				t.Fatal(err)
			}
			move, err := newRangeMoveCoordinatorOn(publisher.coordinator)
			if err != nil {
				t.Fatal(err)
			}
			targetWire := publisher.coordinator.transports[8]
			gate := &rangeMoveAdmissionImportGate{graphPeerWireTransport: targetWire, sequence: 1}
			publisher.coordinator.transports[8] = gate
			pending, err := runRangeMoveToCompletion(t, move, intent)
			if !gate.stopped || !errors.Is(err, io.EOF) || !errors.Is(err, errRangeMovePending) {
				t.Fatal("actual Import0 before page1", err)
			}
			h := n.hosts[1][2]
			q, cfg := rangeMoveBaselineGuardReader(t, h.machine, h.driver.Applied())
			status, found, err := readRangeMoveStatus(q, pending.resume.move)
			if err != nil || !found || status.phase != rangeMoveImport || status.sequence != 0 || status.rows == 0 || status.size == 0 {
				t.Fatal("actual retained prefix", status, err)
			}
			baseline, err := h.machine.rangeMoveCatalogBaseline(q, cfg)
			if err != nil || baseline == nil {
				t.Fatal("strict Import0 baseline", err)
			}
			if err := baseline.Close(); err != nil {
				t.Fatal(err)
			}
			if err := h.machine.Restore(q.base.Index, q.base.Image); err != nil {
				t.Fatal("same native Import0 Restore", err)
			}
			if err := q.view.Close(); err != nil {
				t.Fatal(err)
			}
			publisher.coordinator.transports[8] = targetWire
			done, err := runMovePoolResume(t, move, pending.resume)
			if err != nil || !done.committed || done.resume != pending.resume {
				t.Fatal("same original completion", err)
			}
			proposal, err := n.hosts[1][0].PreparePartitionGraphOperations(n.requestID(), []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Scope: ops[0].Scope, Name: "after-handoff"}}, 313, nil)
			if err != nil {
				t.Fatal(err)
			}
			n.submitPartitionGraph(1, proposal, reasonNone)
			current, closeCurrent := crossView(t, n, 0, [2]uint64{})
			assertCertifiedProjection(t, current, 1, true, []string{"after-handoff"})
			closeCurrent()
			assertCertifiedProjection(t, view, 1, true, nil)
		})
	}
}

func TestNativeMoveReplayBatchKeepsDeclarationBindingAndRecordCharges(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n, _, _, _, _ := rangeMoveCoordinatorFixture(t, 32)
	window, err := temporal.All(graphGenesisBinding(t).Axis())
	if err != nil {
		t.Fatal(err)
	}
	for i := range 16 {
		proposal, err := n.hosts[0][0].PreparePartitionGraphOperations(n.requestID(), []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Scope: window, Name: fmt.Sprintf("original-%d", i)}}, uint64(400+i), nil)
		if err != nil {
			t.Fatal(err)
		}
		n.submitPartitionGraph(0, proposal, reasonNone)
	}
	source := rangeMoveReplayNativeReader(t, n.hosts[0][0], 32<<20)
	batch, err := exportMoveReplayBatch(source, source.ns, rangeMoveReplayStart(), 64<<10, 16)
	if err != nil || len(batch.pages) < 16 {
		t.Fatal("actual bounded multi-original export", len(batch.pages), err)
	}
	target := rangeMoveReplayNativeReader(t, n.hosts[1][0], 32<<20)
	before := target.arena.Used()
	cursor, err := stageMoveReplayBatch(target, source.ns, target.ns, [16]byte{91}, batch, rangeMoveReplayStart())
	if err != nil || !rangeMoveReplaySame(cursor, batch.pages[len(batch.pages)-1].next) || len(target.writes) < 16 {
		t.Fatal("actual bounded target reduction", len(target.writes), err)
	}
	payload := 0
	for _, row := range target.writes {
		if row.Deleted || !bytes.HasPrefix(row.Key, recordKey(target.ns, 20)) {
			t.Fatal("replay reducer changed ownership namespace")
		}
		payload += len(row.Key) + len(row.Value)
	}
	if source.rows == 0 || target.rows == 0 || source.arena.Used() <= 2*rangeMoveReplayRecordBytes+8192 || target.arena.Used() <= before+2*rangeMoveReplayRecordBytes+8192+payload {
		t.Fatal("batch omitted declaration or record work/ownership charges")
	}
	foreign := source.ns
	foreign.graph[0] ^= 1
	refused := rangeMoveReplayNativeReader(t, n.hosts[1][0], 32<<20)
	if _, err := stageMoveReplayBatch(refused, foreign, refused.ns, [16]byte{91}, batch, rangeMoveReplayStart()); !errors.Is(err, errInvalid) || len(refused.writes) != 0 {
		t.Fatal("foreign source graph reached batch effects", err)
	}
	wrongView := rangeMoveReplayNativeReader(t, n.hosts[1][0], 32<<20)
	wrongView.ns = source.ns
	if _, err := exportMoveReplayBatch(wrongView, source.ns, rangeMoveReplayStart(), 64<<10, 16); !errors.Is(err, errInvalid) {
		t.Fatal("declaration reused on a different native view", err)
	}
	undeclared := source.ns
	undeclared.partition = 999
	refused = rangeMoveReplayNativeReader(t, n.hosts[1][0], 32<<20)
	if _, err := stageMoveReplayBatch(refused, undeclared, refused.ns, [16]byte{91}, batch, rangeMoveReplayStart()); !errors.Is(err, errInvalid) || len(refused.writes) != 0 {
		t.Fatal("undeclared origin reached batch effects", err)
	}
}

func TestNativeProtocolStateSourceGetReuseOwnsInvocationAndCopies(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	ctx := t.Context()
	source, _ := authenticatedNativeSourceFixture(t, ctx)
	defer func() {
		if err := source.Close(); err != nil {
			t.Error(err)
		}
	}()
	page, err := source.Scan(ctx, nil, nil, nil, raftlog.ReadBudget{Rows: 1, Bytes: 4096})
	if err != nil || len(page.Rows) != 1 {
		t.Fatal("genuine row", page, err)
	}
	key := bytes.Clone(page.Rows[0].Key)
	arena := source.endpoint.budget.output.(*graphstate.OutputBudget)
	owner := new(graphSourceGetOwner)
	if err := source.beginGetReuse(ctx, owner, arena); err != nil {
		t.Fatal(err)
	}
	defer func() {
		owner.closed.Store(true)
		if err := source.endGetReuse(owner); err != nil {
			t.Error(err)
		}
	}()
	selected := context.WithValue(ctx, graphSourceGetContextKey{}, owner)
	calls := source.endpoint.budget.calls
	var original raftlog.KV
	for i := range 8 {
		row, found, err := source.Get(selected, key, 4096)
		if err != nil || !found {
			t.Fatal("native same key", row, found, err)
		}
		if i == 0 {
			original = raftlog.KV{Key: bytes.Clone(row.Key), Value: bytes.Clone(row.Value)}
		}
		if !bytes.Equal(row.Key, original.Key) || !bytes.Equal(row.Value, original.Value) {
			t.Fatal("caller mutated cached original")
		}
		row.Key[0] ^= 0xff
		if len(row.Value) > 0 {
			row.Value[0] ^= 0xff
		}
	}
	if actual := source.endpoint.budget.calls - calls; actual != 1 {
		t.Fatalf("same actual key repeated %d RPCs, want1", actual)
	}
	// Concurrent unrelated contexts must go to the genuine server every time.
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, caller := range []context.Context{selected, ctx} {
		wg.Go(func() {
			for range 3 {
				row, found, err := source.Get(caller, key, 4096)
				if err != nil || !found || !bytes.Equal(row.Value, original.Value) {
					errs <- errors.Join(err, errCorrupt)
					return
				}
			}
			errs <- nil
		})
	}
	wg.Wait()
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if actual := source.endpoint.budget.calls - calls; actual != 4 {
		t.Fatal("unrelated context borrowed reuse", actual)
	}
	absent := append(bytes.Clone(key), 0xff)
	before := source.endpoint.budget.calls
	for range 2 {
		row, found, err := source.Get(selected, absent, 4096)
		if err != nil || found || len(row.Key)+len(row.Value) != 0 {
			t.Fatal("complete absence", row, found, err)
		}
	}
	if source.endpoint.budget.calls-before != 1 {
		t.Fatal("complete absence refetched")
	}
	// Native Get permits a complete never-written absence even at maximum0.
	// Compare that actual result, rather than assuming maximum0 means default.
	native, found, err := source.Get(ctx, absent, 0)
	if err != nil || found || len(native.Key)+len(native.Value) != 0 {
		t.Fatal("native absent maximum0", native, found, err)
	}
	cached, found, err := source.Get(selected, absent, 0)
	if err != nil || found || len(cached.Key)+len(cached.Value) != 0 {
		t.Fatal("cached absent maximum0", cached, found, err)
	}
	if err := source.mu.lock(ctx); err != nil {
		t.Fatal(err)
	}
	waiting, stop := context.WithTimeout(selected, 10*time.Millisecond)
	_, _, waitErr := source.Get(waiting, key, 4096)
	stop()
	source.mu.unlock()
	if !errors.Is(waitErr, context.DeadlineExceeded) {
		t.Fatal("expired waiter returned a cache hit", waitErr)
	}

	canceled, cancel := context.WithCancel(selected)
	cancel()
	if _, _, err := source.Get(canceled, key, 4096); !errors.Is(err, context.Canceled) {
		t.Fatal("cancel hit", err)
	}
	if _, _, err := source.Get(selected, key, len(original.Key)+len(original.Value)-1); !errors.Is(err, raftlog.ErrLimit) {
		t.Fatal("maximum bypass", err)
	}
	root := source.info.root
	source.info.root.Index++
	if _, _, err := source.Get(selected, key, 4096); !errors.Is(err, graphstore.ErrSourceUnavailable) {
		t.Fatal("root bypass", err)
	}
	source.info.root = root
	cookie := source.info.cookie
	source.info.cookie.sequence++
	if _, _, err := source.Get(selected, key, 4096); !errors.Is(err, graphstore.ErrSourceUnavailable) {
		t.Fatal("cookie bypass", err)
	}
	source.info.cookie = cookie
	// A failed Get may neither consume a slot nor later masquerade as absence.
	unknown := append(bytes.Clone(key), 0xfe)
	count := len(source.getReuse.entries)
	if _, _, err := source.Get(selected, unknown, graphSourceFrameBytes); !errors.Is(err, errLimit) {
		t.Fatal("bad limit", err)
	}
	if len(source.getReuse.entries) != count {
		t.Fatal("failed read cached")
	}
	if err := source.beginGetReuse(ctx, new(graphSourceGetOwner), arena); !errors.Is(err, graphstore.ErrSourceUnavailable) {
		t.Fatal("overlapping owner", err)
	}
	// The finite retained window never grows with the number of distinct misses.
	for i := range graphSourceGetEntries + 2 {
		absent := append(bytes.Clone(key), byte(i>>8), byte(i), 0xfd)
		if _, found, err := source.Get(selected, absent, 4096); err != nil || found {
			t.Fatal("native absent capacity", found, err)
		}
	}
	if len(source.getReuse.entries) != graphSourceGetEntries || source.getReuse.bytes > graphSourceGetBytes {
		t.Fatal("unbounded retained window")
	}
	overflow := append(bytes.Clone(key), 0xfb, 0xfc)
	before = source.endpoint.budget.calls
	for range 2 {
		if _, found, err := source.Get(selected, overflow, 4096); err != nil || found {
			t.Fatal("overflow must still use native source", found, err)
		}
	}
	if source.endpoint.budget.calls-before != 2 {
		t.Fatal("overflow implicitly expanded window")
	}
	// Hit copies remain charged even after the first native result was funded.
	if err := arena.Reserve(arena.Remaining()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := source.Get(selected, key, 4096); !errors.Is(err, errLimit) {
		t.Fatal("exhausted original output bypassed", err)
	}
	owner.closed.Store(true)
	if _, _, err := source.Get(selected, key, 4096); !errors.Is(err, graphstore.ErrSourceUnavailable) {
		t.Fatal("ended token hit", err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := source.Get(selected, key, 4096); !errors.Is(err, graphstore.ErrSourceUnavailable) {
		t.Fatal("closed owner hit", err)
	}
}

func TestNativeProtocolStateSourceGetReuseRetiresDelayedReply(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	ctx := t.Context()
	source, transport := authenticatedNativeSourceFixture(t, ctx)
	defer func() {
		if err := source.Close(); err != nil {
			t.Error(err)
		}
	}()
	arena := source.endpoint.budget.output.(*graphstate.OutputBudget)
	owner := new(graphSourceGetOwner)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := source.beginGetReuse(canceled, owner, arena); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled admission", err)
	}
	if err := source.beginGetReuse(ctx, nil, arena); !errors.Is(err, errInvalid) {
		t.Fatal("nil owner", err)
	}
	wrong, _ := graphstate.NewOutputBudget(32 << 20)
	if err := source.beginGetReuse(ctx, owner, wrong); !errors.Is(err, errInvalid) {
		t.Fatal("foreign output arena", err)
	}
	if err := source.beginGetReuse(ctx, owner, arena); err != nil {
		t.Fatal(err)
	}
	selected := context.WithValue(ctx, graphSourceGetContextKey{}, owner)
	transport.pause.Store(true)
	done := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Go(func() { _, _, err := source.Get(selected, []byte("actual-native-absence"), 4096); done <- err })
	select {
	case <-transport.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("native delayed reply was not reached")
	}
	owner.closed.Store(true)
	close(transport.release)
	wg.Wait()
	if err := <-done; !errors.Is(err, graphstore.ErrSourceUnavailable) {
		t.Fatal("retired reply inserted into scope", err)
	}
	if len(source.getReuse.entries) != 0 {
		t.Fatal("late reply retained")
	}
	if err := source.endGetReuse(owner); err != nil {
		t.Fatal(err)
	}
	next := new(graphSourceGetOwner)
	if err := source.beginGetReuse(ctx, next, arena); err != nil {
		t.Fatal(err)
	}
	defer func() {
		next.closed.Store(true)
		if err := source.endGetReuse(next); err != nil {
			t.Error(err)
		}
	}()
	if err := source.endGetReuse(owner); !errors.Is(err, errInvalid) {
		t.Fatal("old owner removed new scope", err)
	}
	if _, _, err := source.Get(selected, []byte("actual-native-absence"), 4096); !errors.Is(err, graphstore.ErrSourceUnavailable) {
		t.Fatal("old token borrowed new owner", err)
	}
	fresh := context.WithValue(ctx, graphSourceGetContextKey{}, next)
	transport.pause.Store(false)
	if _, found, err := source.Get(fresh, []byte("actual-native-absence"), 4096); err != nil || found {
		t.Fatal("new native absence", found, err)
	}
	// A full retained-byte window bypasses insertion, without widening the cap.
	reuse := source.getReuse
	saved := reuse.bytes
	reuse.bytes = graphSourceGetBytes
	calls := source.endpoint.budget.calls
	for range 2 {
		if _, found, err := source.Get(fresh, []byte("another-actual-absence"), 4096); err != nil || found {
			t.Fatal(found, err)
		}
	}
	if source.endpoint.budget.calls-calls != 2 || len(reuse.entries) != 1 {
		t.Fatal("retained byte cap expanded")
	}
	reuse.bytes = saved
	// Original owner cancellation refuses a previously cached complete absence.
	source.cancel()
	if _, _, err := source.Get(fresh, []byte("actual-native-absence"), 4096); !errors.Is(err, context.Canceled) || !errors.Is(err, graphstore.ErrSourceUnavailable) {
		t.Fatal("source lifetime bypass", err)
	}
}
