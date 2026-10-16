package graphapply

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

// Native six-voter unit transport, not six OS-process or fault acceptance.
// Every capacity fact comes from this actual Host's completed ReadIndex.
type capacityNativeTestPump struct{ *nativeGraphTestPump }

func (p *capacityNativeTestPump) Capacity(ctx context.Context, proposal allocationProposal, nonce [16]byte, limits graphCapacityLimits) (crossCapacityReply, error) {
	event, err := p.host.readCrossCapacityConflict(proposal, nonce, limits)
	if err != nil {
		return crossCapacityReply{}, err
	}
	queue, replies := event.packets, event.capacityReplies
	for count := 0; len(queue) != 0; count++ {
		if count >= 1024 {
			return crossCapacityReply{}, errLimit
		}
		if err := ctx.Err(); err != nil {
			return crossCapacityReply{}, err
		}
		packet := queue[0]
		queue = queue[1:]
		if packet.Snapshot || packet.To < 1 || packet.To > 3 {
			return crossCapacityReply{}, errInvalid
		}
		e, err := p.n.hosts[p.group][packet.To-1].Step(packet)
		if err != nil {
			return crossCapacityReply{}, err
		}
		if len(queue)+len(e.packets) > 128 || len(replies)+len(e.capacityReplies) > 16 {
			return crossCapacityReply{}, errLimit
		}
		queue = append(queue, e.packets...)
		replies = append(replies, e.capacityReplies...)
	}
	if len(replies) != 1 || replies[0].id != event.readID || replies[0].err == nil && replies[0].proof.observation.readID != event.readID {
		return crossCapacityReply{}, errInvalid
	}
	return replies[0], nil
}

func capacityNativeCoordinator(t *testing.T, n *allocationTestNetwork) (*crossProcessCoordinator, []*graphPeerService) {
	t.Helper()
	transports := make(map[uint64]graphPeerWireTransport)
	services := make([]*graphPeerService, 2)
	for group, partition := range []uint64{3, 8} {
		host := n.hosts[group][0]
		server, err := newGraphSourceServer(t.Context(), host, defaultGraphSourceLimits(host.machine.limits))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := server.close(); err != nil {
				t.Error(err)
			}
		})
		pump := &capacityNativeTestPump{&nativeGraphTestPump{n: n, group: group, host: host}}
		service, err := newGraphPeerService(host, server, pump)
		if err != nil {
			t.Fatal(err)
		}
		services[group] = service
		transports[partition] = &nativeGraphTestTransport{service: service}
	}
	c, err := newCrossProcessCoordinator(n.hosts[0][0], transports)
	if err != nil {
		t.Fatal(err)
	}
	return c, services
}

func capacityNativeInvocation(t *testing.T, c *crossProcessCoordinator) (map[uint64]*graphAuthenticatedEndpoint, *graphRemoteBudget, *graphstate.OutputBudget) {
	t.Helper()
	arena, err := graphstate.NewOutputBudget(c.host.machine.limits.outputBytes)
	if err != nil {
		t.Fatal(err)
	}
	budget := newGraphRemoteBudget(c.host.machine.limits, arena)
	endpoints, err := c.endpoints(budget)
	if err != nil {
		t.Fatal(err)
	}
	return endpoints, budget, arena
}

func TestCrossProcessCapacityAuthenticatedPriorityAndImmutableDecision(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for loserGroup := range 2 {
		t.Run(fmt.Sprintf("priority-loser%d", loserGroup), func(t *testing.T) {
			n := newCrossPartitionNetwork(t)
			ops, claims := crossEdgeFixture(t, n)
			ids := crossRequestsWithLoser(n, loserGroup)
			var originals [2]crossTransaction
			var registered [2]crossProof
			for group := range 2 {
				tx, proposal := crossPlanAt(n, group, ids[group], ops, claims, uint64(24+group))
				originals[group] = tx
				status := n.crossSubmit(group, proposal)
				registered[group] = n.crossReadPhase(group, status.tx, crossRegister)
			}
			first, err := n.hosts[0][0].PrepareCrossParticipant(originals[0], 3, registered[0], nil)
			if err != nil {
				t.Fatal(err)
			}
			if status := n.crossSubmit(0, first); !status.yes {
				t.Fatal(status)
			}
			blocked, err := n.hosts[0][0].PrepareCrossParticipant(originals[0], 8, registered[0], []crossProof{n.crossReadPhase(0, registered[0].status.tx, crossPrepare)})
			if err != nil {
				t.Fatal(err)
			}
			c, services := capacityNativeCoordinator(t, n)
			endpoints, _, arena := capacityNativeInvocation(t, c)
			m, err := decodeCrossManifest(originals[0].manifest, c.host.machine.limits, arena)
			if err != nil {
				t.Fatal(err)
			}
			grant := graphCapacityLimits{128, 128 << 10, 1 << 20}
			authority, err := endpoints[8].capacity(t.Context(), blocked, m.tx, grant)
			if err != nil || !authority.validSeal() || authority.work.Records == 0 || authority.work.Bytes == 0 || authority.output == 0 {
				t.Fatal("completed bounded native authority", authority, err)
			}
			wire, err := encodeCapacityAuthorityBody(authority)
			if err != nil {
				t.Fatal(err)
			}
			inert, err := decodeCapacityAuthorityBody(wire, grant)
			if err != nil || inert.validSeal() {
				t.Fatal("wire decoder created authority", err)
			}
			if _, err := c.host.proposalFromCapacityAuthority(originals[loserGroup], inert, arena); !errors.Is(err, errInvalid) {
				t.Fatal("bare observation authorized abort", err)
			}
			bad := authority
			bad.observation.prepareHash[0] ^= 1
			if bad.validSeal() {
				t.Fatal("changed failed-prepare identity retained capability")
			}
			// An older embedding without the optional door remains unavailable.
			services[1].pump = &nativeGraphTestPump{n: n, group: 1, host: n.hosts[1][0]}
			if _, err := endpoints[8].capacity(t.Context(), blocked, m.tx, grant); !errors.Is(err, replica.ErrUnavailable) {
				t.Fatal("missing optional authority became a decision", err)
			}
			services[1].pump = &capacityNativeTestPump{&nativeGraphTestPump{n: n, group: 1, host: n.hosts[1][0]}}
			result, err := c.Run(t.Context(), ids[0], ops, 24, claims)
			if err != nil || result.decision.phase != crossDecide || result.decision.commit != (loserGroup == 1) || result.resume.tx != m.tx || result.decision.digest != m.digest {
				t.Fatal("global native priority settlement through ordinary Run", result, err)
			}
			again, err := c.Run(t.Context(), ids[0], ops, 24, claims)
			if err != nil || again != result {
				t.Fatal("capacity recovery replaced original result", again, err)
			}
			settled := n.crossReadPhase(loserGroup, registered[loserGroup].status.tx, crossDecide)
			if settled.status.commit || settled.status.round != 0 || settled.status.digest != originals[loserGroup].digest {
				t.Fatal("capacity settlement not original durable ABORT", settled.status)
			}
			// Only the loser's own group has its original in this prefix. The
			// opposite participant was skipped through a completed native absence.
			resolved := n.crossReadPhase(loserGroup, settled.status.tx, crossResolve)
			if resolved.status.commit || resolved.status.digest != settled.status.digest {
				t.Fatal("settled original did not release its actual participant", resolved.status)
			}
			for _, voter := range n.hosts[loserGroup] {
				cdc, err := voter.machine.store.ApplicationRecord(t.Context(), resolved.status.index, false, 64<<10)
				if err != nil || len(cdc) != 0 {
					t.Fatal("capacity ABORT emitted graph CDC", err)
				}
			}
		})
	}
	t.Run("committed holder wins", func(t *testing.T) {
		n := newCrossPartitionNetwork(t)
		ops, claims := crossEdgeFixture(t, n)
		ids := crossRequestsWithLoser(n, 1)
		b, register := crossPlanAt(n, 1, ids[1], ops, claims, 24)
		status := n.crossSubmit(1, register)
		registration := n.crossReadPhase(1, status.tx, crossRegister)
		var votes []crossProof
		for group, partition := range []uint64{3, 8} {
			p, err := n.hosts[1][0].PrepareCrossParticipant(b, partition, registration, votes)
			if err != nil {
				t.Fatal(err)
			}
			if got := n.crossSubmit(group, p); !got.yes {
				t.Fatal(got)
			}
			votes = append(votes, n.crossReadPhase(group, status.tx, crossPrepare))
		}
		commit, err := n.hosts[1][0].DecideCrossTransaction(b, votes)
		if err != nil {
			t.Fatal(err)
		}
		n.crossSubmit(1, commit)
		committed := n.crossReadPhase(1, status.tx, crossDecide)
		crossResolveAt(n, 1, committed, 0)
		a, register := crossPlanAt(n, 0, ids[0], []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Name: "later", Scope: ops[0].Scope}}, nil, 25)
		status = n.crossSubmit(0, register)
		registration = n.crossReadPhase(0, status.tx, crossRegister)
		first, err := n.hosts[0][0].PrepareCrossParticipant(a, 3, registration, nil)
		if err != nil {
			t.Fatal(err)
		}
		n.crossSubmit(0, first)
		blocked, err := n.hosts[0][0].PrepareCrossParticipant(a, 8, registration, []crossProof{n.crossReadPhase(0, status.tx, crossPrepare)})
		if err != nil {
			t.Fatal(err)
		}
		c, _ := capacityNativeCoordinator(t, n)
		endpoints, budget, arena := capacityNativeInvocation(t, c)
		m, err := decodeCrossManifest(a.manifest, c.host.machine.limits, arena)
		if err != nil {
			t.Fatal(err)
		}
		if decision, err := c.recoverCapacity(t.Context(), a, m, blocked, endpoints, budget); err != nil || decision.validSeal() {
			t.Fatal("committed holder recovery", decision, err)
		}
		if after := n.crossReadPhase(1, committed.status.tx, crossDecide); after.status != committed.status || !after.status.commit || !bytes.Equal(after.manifest, b.manifest) {
			t.Fatal("capacity reversed or replaced COMMIT", after.status)
		}
	})
	t.Run("secondary quota preserves original", func(t *testing.T) {
		arena, err := graphstate.NewOutputBudget(64 << 10)
		if err != nil {
			t.Fatal(err)
		}
		budget := newGraphRemoteBudget(defaultMaterializerLimits(), arena)
		if child, err := budget.capacityChild(2, 4096, 1, 4096, graphCapacityLimits{}); !errors.Is(err, errLimit) || child != nil || arena.Used() != 0 || budget.calls != 0 || budget.bytes != 0 || budget.rows != 0 {
			t.Fatal("refusal consumed/revived original quota", child, err)
		}
	})
	t.Run("distinct secondary footprint quota", func(t *testing.T) {
		// Inert arithmetic only: current coarse native profiles require every
		// declared participant. This does not invent a narrower native manifest.
		arena, err := graphstate.NewOutputBudget(4 << 20)
		if err != nil {
			t.Fatal(err)
		}
		budget := newGraphRemoteBudget(defaultMaterializerLimits(), arena)
		child, err := budget.capacityChild(2, 4096, 4*1+6, 4096, graphCapacityLimits{})
		if err != nil || child.maxCalls != 10 || budget.calls != 10 || budget.maxCalls-budget.calls < 4*2+6 {
			t.Fatal("secondary count changed desired completion floor", child, err)
		}
	})
}
