package graphapply

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/dist/replica"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

type p04ControlIdentity struct {
	found bool
	index uint64
	bytes int
	hash  [32]byte
}

// A fixed census of genuine retained controls, not a manufactured phase proof.
func p04Controls(t *testing.T, n *allocationTestNetwork, m crossManifest, registered bool, votes [2]crossStatus, decision *crossStatus) [2][3][4]p04ControlIdentity {
	t.Helper()
	var census [2][3][4]p04ControlIdentity
	for group, hosts := range n.hosts {
		for member, h := range hosts {
			func() {
				view, err := h.machine.store.ApplicationView(h.driver.Applied())
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := view.Close(); err != nil {
						t.Error(err)
					}
				}()
				for slot, tag := range []byte{'R', 'P', 'K', 'D'} {
					var expected []byte
					want := false
					switch tag {
					case 'R':
						want = registered && (group == 0 || votes[group].index != 0)
						if want {
							expected, err = encodeCrossManifest(m, h.machine.limits)
						}
					case 'P':
						want = votes[group].index != 0
						if want {
							expected, err = encodeCrossStatus(votes[group])
						}
					case 'K':
						want = votes[group].index != 0
						expected = m.parts[group].kernel
					case 'D':
						want = group == 0 && decision != nil
						if want {
							expected, err = encodeCrossStatus(*decision)
						}
					}
					if err != nil {
						t.Fatal(err)
					}
					budget := raftlog.ReadBudget{Rows: 2, Bytes: min(view.ReadLimits().Bytes, 4096+2*max(256, len(expected)))}
					record, found, _, err := view.GetControl(t.Context(), crossControlKey(h.machine.ns, m.tx, tag), budget)
					if err != nil || found != want || found && !bytes.Equal(record.Value, expected) {
						t.Fatal("P04 exact original control", group, member, tag, found, want, err)
					}
					census[group][member][slot] = p04ControlIdentity{found, record.Index, len(record.Value), sha256.Sum256(record.Value)}
				}
			}()
		}
	}
	return census
}

func p04Graph(t *testing.T, n *allocationTestNetwork, ops []graphstate.Operation, present bool) {
	t.Helper()
	for member := range 3 {
		func() {
			view, close := crossView(t, n, member, [2]uint64{})
			defer close()
			assertCertifiedProjection(t, view, 1, true, nil)
			assertCertifiedProjection(t, view, 17, true, nil)
			edge, err := view.Entity(t.Context(), 3)
			if err != nil || edge.Found != present || present && (edge.Record.Source != 1 || edge.Record.Target != 17 || edge.Record.Type != "cross-edge") {
				t.Fatal("P04 original cross edge", edge, err)
			}
			var want []graphstate.EntityID
			if present {
				want = []graphstate.EntityID{3}
			}
			assertCertifiedIncident(t, view, ops[0].Scope, 1, 2, want)
			assertCertifiedIncident(t, view, ops[0].Scope, 17, 18, want)
			phantom, err := view.Entity(t.Context(), 4)
			if err != nil || phantom.Found {
				t.Fatal("P04 phantom life became entity", phantom, err)
			}
		}()
	}
}

// Six actual disk-backed native Driver/Host voters; this is not six OS-child
// acceptance. Existing queued COMMIT/capacity-ABORT schedules remain separate.
func TestCrossProcessP04FrozenParticipantsEpochsAndDigestsSurviveDelayedVoteAndReopen(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCrossPartitionNetwork(t)
	ops, claims := crossEdgeFixture(t, n)
	id := n.requestID()
	tx, register := crossPlanAt(n, 0, id, ops, claims, 24)
	decode := func(wire []byte) crossManifest {
		arena, err := graphstate.NewOutputBudget(n.hosts[0][0].machine.limits.outputBytes)
		if err != nil {
			t.Fatal(err)
		}
		m, err := decodeCrossManifest(wire, n.hosts[0][0].machine.limits, arena)
		if err != nil {
			t.Fatal(err)
		}
		return m
	}
	m := decode(tx.manifest)
	if len(m.parts) != 2 || m.parts[0].binding.Partition != 3 || m.parts[1].binding.Partition != 8 || m.request.id != id || m.digest != tx.digest || m.tx != crossTransactionID(n.hosts[0][0].machine.ns, id) {
		t.Fatal("P04 complete frozen transaction identity", m)
	}
	for _, part := range m.parts {
		declared, found := n.d.Partition(part.binding.Partition)
		if !found || part.binding.TxID != m.tx || part.binding.OwnershipEpoch != declared.OwnershipEpoch || part.binding.TemplateDigest != m.template || part.binding.EffectDigest == ([32]byte{}) {
			t.Fatal("P04 frozen participant binding", part.binding)
		}
	}
	var votes [2]crossStatus
	clean := p04Controls(t, n, m, false, votes, nil)
	short := m
	short.parts = short.parts[:1]
	shortWire, err := encodeCrossManifest(short, n.hosts[0][0].machine.limits)
	if err != nil {
		t.Fatal(err)
	}
	command, _, err := decodeCrossCommand(register.wire, n.hosts[0][0].machine.limits)
	if err != nil {
		t.Fatal(err)
	}
	command.manifest = shortWire
	badWire, err := encodeCrossCommand(command, n.hosts[0][0].machine.limits)
	if err != nil {
		t.Fatal(err)
	}
	refuse := func(proposal allocationProposal, sentinel error) {
		event, err := n.hosts[0][0].Submit(proposal)
		if !errors.Is(err, sentinel) || len(event.packets) != 0 {
			t.Fatal("P04 hostile original admitted", err, sentinel)
		}
	}
	refuse(allocationProposal{target: register.target, wire: badWire}, errInvalid)
	if got := p04Controls(t, n, m, false, votes, nil); got != clean {
		t.Fatal("short manifest installed controls")
	}
	p04Graph(t, n, ops, false)
	n.crossSubmit(0, register)
	registration := n.crossReadPhase(0, m.tx, crossRegister)
	first, err := n.hosts[0][0].PrepareCrossParticipant(tx, 3, registration, nil)
	if err != nil {
		t.Fatal(err)
	}
	stale := first
	stale.target.ownership-- // The fixture's epoch2 becomes a positive stale epoch1.
	refuse(stale, errInvalid)
	votes[0] = n.crossSubmit(0, first)
	if !votes[0].yes {
		t.Fatal("P04 first genuine YES", votes[0])
	}
	firstProof := n.crossReadPhase(0, m.tx, crossPrepare)
	frozen := p04Controls(t, n, m, true, votes, nil)
	if _, err := n.hosts[0][0].DecideCrossTransaction(tx, []crossProof{firstProof}); !errors.Is(err, replica.ErrUnavailable) {
		t.Fatal("P04 missing frozen participant became decision", err)
	}
	changedOps := slices.Clone(ops)
	changedOps = append(changedOps, graphstate.Operation{Kind: graphstate.AddLabel, Owner: 17, Life: 18, Scope: ops[0].Scope, Name: "p04-forbidden-extra-effect"})
	changed, changedRegister := crossPlanAt(n, 0, id, changedOps, claims, 24)
	changedManifest := decode(changed.manifest)
	if changed.digest == m.digest || sha256.Sum256(changedManifest.original) == sha256.Sum256(m.original) || changedManifest.parts[1].binding.EffectDigest == m.parts[1].binding.EffectDigest {
		t.Fatal("P04 substituted business/kernel was unchanged")
	}
	refuse(changedRegister, errInvalid)
	// A correctly checksummed command carrying a substituted real kernel must
	// also fail actual Host admission, rather than borrowing the first YES.
	mixed := m
	mixed.parts = slices.Clone(m.parts)
	mixed.parts[1] = changedManifest.parts[1]
	command.manifest, err = encodeCrossManifest(mixed, n.hosts[0][0].machine.limits)
	if err != nil {
		t.Fatal(err)
	}
	badWire, err = encodeCrossCommand(command, n.hosts[0][0].machine.limits)
	if err != nil {
		t.Fatal(err)
	}
	refuse(allocationProposal{target: register.target, wire: badWire}, errCorrupt)
	if got := p04Controls(t, n, m, true, votes, nil); got != frozen {
		t.Fatal("P04 refusal rewrote frozen R/P/K")
	}
	p04Graph(t, n, ops, false)
	second, err := n.hosts[0][0].PrepareCrossParticipant(tx, 8, registration, []crossProof{firstProof})
	if err != nil {
		t.Fatal(err)
	}
	votes[1] = n.crossSubmit(1, second)
	if !votes[1].yes {
		t.Fatal("P04 delayed genuine YES", votes[1])
	}
	prepared := p04Controls(t, n, m, true, votes, nil)
	p04Graph(t, n, ops, false)
	secondProof := n.crossReadPhase(1, m.tx, crossPrepare)
	decide, err := n.hosts[0][0].DecideCrossTransaction(tx, []crossProof{firstProof, secondProof})
	if err != nil {
		t.Fatal(err)
	}
	decision := n.crossSubmit(0, decide)
	if !decision.commit || decision.digest != m.digest || decision.tx != m.tx || decision.round <= max(votes[0].floor, votes[1].floor) {
		t.Fatal("P04 exact first COMMIT", decision)
	}
	decided := p04Controls(t, n, m, true, votes, &decision)
	for group := range 2 {
		for member := range 3 {
			for slot := range 3 {
				if decided[group][member][slot] != prepared[group][member][slot] {
					t.Fatal("P04 decision rewrote R/P/K")
				}
			}
		}
	}
	if again := n.crossSubmit(0, decide); again != decision {
		t.Fatal("P04 duplicate changed first decision", again, decision)
	}
	n.crossResolve(n.crossReadPhase(0, m.tx, crossDecide))
	if got := p04Controls(t, n, m, true, votes, &decision); got != decided {
		t.Fatal("P04 resolve rewrote original controls")
	}
	p04Graph(t, n, ops, true)
	crossReopenAll(t, n)
	if got := n.crossReadPhase(0, m.tx, crossDecide).status; got != decision {
		t.Fatal("P04 reopen changed first decision", got, decision)
	}
	if got := p04Controls(t, n, m, true, votes, &decision); got != decided {
		t.Fatal("P04 reopen changed frozen controls")
	}
	if again := n.crossSubmit(0, decide); again != decision {
		t.Fatal("P04 reopened duplicate changed decision", again, decision)
	}
	p04Graph(t, n, ops, true)
}
