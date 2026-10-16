package graphapply

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

func crossRequestsWithLoser(n *allocationTestNetwork, loser int) [2]requestID {
	n.t.Helper()
	var ids [2]requestID
	for group := range 2 {
		var selected [16]byte
		for value := 1; value < 256; value++ {
			id := requestID{byte(value), 0xa1} // #nosec G115 -- finite fixture selector1..255.
			tx := crossTransactionID(n.hosts[group][0].machine.ns, id)
			if ids[group] == (requestID{}) || group == loser && bytes.Compare(tx[:], selected[:]) > 0 || group != loser && bytes.Compare(tx[:], selected[:]) < 0 {
				ids[group], selected = id, tx
			}
		}
	}
	a, b := crossTransactionID(n.hosts[0][0].machine.ns, ids[0]), crossTransactionID(n.hosts[1][0].machine.ns, ids[1])
	if (bytes.Compare(a[:], b[:]) > 0) != (loser == 0) {
		n.t.Fatal("fixture failed to select requested qualified priority")
	}
	return ids
}

func crossRawRequestKey(t *testing.T, tx crossTransaction) requestID {
	t.Helper()
	arena, err := graphstate.NewOutputBudget(tx.owner.machine.limits.outputBytes)
	if err != nil {
		t.Fatal(err)
	}
	m, err := decodeCrossManifest(tx.manifest, tx.owner.machine.limits, arena)
	if err != nil {
		t.Fatal(err)
	}
	return m.request.id
}

func TestCrossPartitionTransactionIdentityQualifiesGraphCoordinatorAndRawRetryKey(t *testing.T) {
	n := namespace{graph: idalloc.GraphID{1}, partition: 3}
	id := requestID{80}
	got := crossTransactionID(n, id)
	if hex.EncodeToString(got[:]) != "38f2b96ed539c46af3fe828133f2d8ff" {
		t.Fatal("domain/field-order wire vector changed", got)
	}
	otherGraph, otherCoordinator := n, n
	otherGraph.graph[0] = 2
	otherCoordinator.partition = 8
	if crossTransactionID(otherGraph, id) == got || crossTransactionID(otherCoordinator, id) == got || crossTransactionID(n, requestID{81}) == got || got == [16]byte(id) {
		t.Fatal("qualified identities alias")
	}
	old := raftlog.ApplicationSemanticContractID(sha256.Sum256([]byte(partitionWriteSemanticDescriptor + "cross-graph=ordered-coarse-prepare-kernel-v1\x00recovery=protected-v2\x00capacity-abort=global-TxID-owner-v1\x00")))
	if crossPartitionSemanticContractID() == old || partitionGraphAgreement(old) {
		t.Fatal("raw-TxID agreement accepted as corrected agreement")
	}
}

func TestCrossPartitionSameRequestIDTwoCoordinatorsHaveRecoverableProgress(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for _, loserGroup := range []int{1, 0} {
		t.Run(fmt.Sprint("loser-coordinator-", loserGroup), func(t *testing.T) {
			n := newCrossPartitionNetwork(t)
			ops, claims := crossEdgeFixture(t, n)
			id := requestID{80} // The independent review's exact failing request.
			if loserGroup == 0 {
				found := false
				for value := 1; value < 256; value++ {
					candidate := requestID{byte(value), 0xa2} // #nosec G115 -- finite fixture selector1..255.
					a, b := crossTransactionID(n.hosts[0][0].machine.ns, candidate), crossTransactionID(n.hosts[1][0].machine.ns, candidate)
					if bytes.Compare(a[:], b[:]) > 0 {
						id, found = candidate, true
						break
					}
				}
				if !found {
					t.Fatal("fixture failed to reverse qualified priority")
				}
			}
			var txs [2]crossTransaction
			var regs [2]crossProof
			var registrations [2]allocationProposal
			for group := range 2 {
				tx, reg := crossPlanAt(n, group, id, ops, claims, uint64(24+group))
				txs[group], registrations[group] = tx, reg
				status := n.crossSubmit(group, reg)
				regs[group] = n.crossReadPhase(group, status.tx, crossRegister)
				if crossRawRequestKey(t, tx) != id {
					t.Fatal("caller retry key changed")
				}
				if replay := n.crossSubmit(group, reg); replay != status {
					t.Fatal("exact registration retry changed status", replay, status)
				}
			}
			if regs[0].status.tx == regs[1].status.tx || txs[0].digest == txs[1].digest {
				t.Fatal("two coordinator registrations alias")
			}
			// Changing content under the same coordinator key cannot create another Tx.
			changed, proposal := crossPlanAt(n, 0, id, ops, claims, 99)
			if changed.digest == txs[0].digest {
				t.Fatal("adversarial content was unchanged")
			}
			if e, err := n.hosts[0][0].Submit(proposal); !errors.Is(err, errInvalid) || len(e.packets) != 0 {
				t.Fatal("same coordinator key replaced manifest", err)
			}
			first, err := n.hosts[0][0].PrepareCrossParticipant(txs[0], 3, regs[0], nil)
			if err != nil {
				t.Fatal(err)
			}
			if yes := n.crossSubmit(0, first); !yes.yes {
				t.Fatal(yes)
			}
			vote := n.crossReadPhase(0, regs[0].status.tx, crossPrepare)
			bFirst, err := n.hosts[1][0].PrepareCrossParticipant(txs[1], 3, regs[1], nil)
			if err != nil {
				t.Fatal(err)
			}
			aSecond, err := n.hosts[0][0].PrepareCrossParticipant(txs[0], 8, regs[0], []crossProof{vote})
			if err != nil {
				t.Fatal(err)
			}
			for group, p := range [2]allocationProposal{bFirst, aSecond} {
				e, err := n.hosts[group][0].Submit(p)
				if !errors.Is(err, raftlog.ErrLimit) || len(e.packets) != 0 {
					t.Fatal("healthy contention was not an exact resource conflict", err)
				}
			}
			proof := n.capacityRead(1, aSecond)
			loser, _, coordinator := proof.observation.loser()
			if loser != regs[loserGroup].status.tx || coordinator != n.hosts[loserGroup][0].machine.ns.partition {
				t.Fatal("qualified priority chose wrong coordinator")
			}
			// Reopen with both registrations and A's YES retained before using authority.
			crossReopenAll(t, n)
			for group := range 2 {
				txs[group], err = n.hosts[group][0].RecoverCrossTransaction(regs[group])
				if err != nil || crossRawRequestKey(t, txs[group]) != id {
					t.Fatal("recovery lost coordinator retry identity", err)
				}
			}
			abort, existing, err := n.hosts[loserGroup][0].AbortCrossTransactionForCapacity(txs[loserGroup], proof)
			if err != nil || existing != (crossStatus{}) {
				t.Fatal("bound conflict failed to authorize own abort", existing, err)
			}
			if status := n.crossSubmit(loserGroup, abort); status.commit {
				t.Fatal("loser committed")
			}
			loserDecision := n.crossReadPhase(loserGroup, loser, crossDecide)
			crossResolveAt(n, loserGroup, loserDecision, loserGroup)
			winner := 1 - loserGroup
			var votes []crossProof
			for group, partition := range []uint64{3, 8} {
				p, err := n.hosts[winner][0].PrepareCrossParticipant(txs[winner], partition, regs[winner], votes)
				if err != nil {
					t.Fatal(err)
				}
				if yes := n.crossSubmit(group, p); !yes.yes {
					t.Fatal("loser did not free winner", yes)
				}
				votes = append(votes, n.crossReadPhase(group, regs[winner].status.tx, crossPrepare))
			}
			commit, err := n.hosts[winner][0].DecideCrossTransaction(txs[winner], votes)
			if err != nil {
				t.Fatal(err)
			}
			if status := n.crossSubmit(winner, commit); !status.commit {
				t.Fatal("winner failed to commit", status)
			}
			winnerDecision := n.crossReadPhase(winner, regs[winner].status.tx, crossDecide)
			crossResolveAt(n, winner, winnerDecision, 0, 1)
			crossReopenAll(t, n)
			if got := n.crossReadPhase(loserGroup, loser, crossDecide); got.status != loserDecision.status {
				t.Fatal("ABORT changed after winner/reopen")
			}
			if got := n.crossReadPhase(winner, winnerDecision.status.tx, crossDecide); got.status != winnerDecision.status {
				t.Fatal("COMMIT changed after reopen")
			}
			// Exact terminal retries cannot resurrect an intent or overwrite a decision.
			for group := range 2 {
				got := n.crossSubmit(group, registrations[group])
				if got.phase != crossResolve || got.commit != (group == winner) {
					t.Fatal("late registration resurrected transaction", got)
				}
				for _, voter := range n.hosts[group] {
					view, err := voter.machine.store.ApplicationView(voter.driver.Applied())
					if err != nil {
						t.Fatal(err)
					}
					owner, _, err := view.RecoveryOwner(t.Context(), raftlog.ReadBudget{Rows: 2, Bytes: 4096})
					if err != nil || owner.ID != ([32]byte{}) {
						t.Fatal("terminal transaction kept completion ownership", owner, err)
					}
					if err := view.Close(); err != nil {
						t.Fatal(err)
					}
				}
			}
			for member := range 3 {
				view, close := crossView(t, n, member, [2]uint64{})
				edge, err := view.Entity(t.Context(), 3)
				close()
				if err != nil || !edge.Found {
					t.Fatal("winner effects missing on an endpoint replica", member, err)
				}
			}
		})
	}
}

func TestCrossPartitionForeignIdentityPreservesCompletedLocalRequestReplay(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCrossPartitionNetwork(t)
	ops, _ := crossEdgeFixture(t, n)
	id := requestID{80}
	local, err := n.hosts[1][0].PreparePartitionGraphOperations(id, []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 17, Life: 18, Name: "local-before-cross", Scope: ops[0].Scope}}, 24, nil)
	if err != nil {
		t.Fatal(err)
	}
	original := n.submitPartitionGraph(1, local, reasonNone)
	tx, reg := crossPlanAt(n, 0, id, []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Name: "cross-only", Scope: ops[0].Scope}}, nil, 25)
	decision := n.crossCommit(tx, reg)
	n.crossResolve(decision)
	wanted := original
	wanted.disposition = requestReplay
	if replay := n.submitPartitionGraph(1, local, reasonNone); replay != wanted {
		t.Fatal("foreign coordinator poisoned exact local retry", replay, original)
	}
	for _, voter := range n.hosts[1] {
		projection, _ := partitionProjection(t, voter, voter.driver.Applied(), 17, graphGenesisBinding(t).Axis())
		if len(projection.Labels) != 1 || projection.Labels[0] != "local-before-cross" {
			t.Fatal("retry changed local effects", projection)
		}
	}
}

func TestCrossPartitionSameCoordinatorLocalRequestKeyCannotRegisterCrossIdentity(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCrossPartitionNetwork(t)
	ops, _ := crossEdgeFixture(t, n)
	id := requestID{80}
	local, err := n.hosts[0][0].PreparePartitionGraphOperations(id, []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Name: "local-only", Scope: ops[0].Scope}}, 24, nil)
	if err != nil {
		t.Fatal(err)
	}
	original := n.submitPartitionGraph(0, local, reasonNone)
	tx, reg := crossPlanAt(n, 0, id, ops, nil, 25)
	if e, err := n.hosts[0][0].Submit(reg); !errors.Is(err, errInvalid) || len(e.packets) != 0 {
		t.Fatal("same coordinator raw local key admitted cross", err)
	}
	wanted := original
	wanted.disposition = requestReplay
	if replay := n.submitPartitionGraph(0, local, reasonNone); replay != wanted {
		t.Fatal("refused cross replaced local request")
	}
	qualified := crossTransactionID(n.hosts[0][0].machine.ns, crossRawRequestKey(t, tx))
	if status, manifest, err := n.hosts[0][0].machine.observeCrossTransaction(crossQuery{tx: qualified}, n.hosts[0][0].driver.Applied()); !errors.Is(err, replica.ErrUnavailable) || status != (crossStatus{}) || len(manifest) != 0 {
		t.Fatal("refused cross acquired state", status, err)
	}
}
