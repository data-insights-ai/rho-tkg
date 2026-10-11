package graphapply

import (
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

func certifiedEarlyAbort(t *testing.T, n *allocationTestNetwork) crossProof {
	t.Helper()
	ops, claims := crossEdgeFixture(t, n)
	claims[0].grant.sequence = 2
	tx, register := n.crossPlan(ops, claims, 24)
	registered := n.crossSubmit(0, register)
	registration := n.crossReadPhase(0, registered.tx, crossRegister)
	prepare, e := n.hosts[0][0].PrepareCrossParticipant(tx, 3, registration, nil)
	if e != nil {
		t.Fatal(e)
	}
	no := n.crossSubmit(0, prepare)
	if no.yes || no.why != reasonInvalid {
		t.Fatal("fixture needs real durable NO", no)
	}
	decision, e := n.hosts[0][0].DecideCrossTransaction(tx, []crossProof{n.crossReadPhase(0, registered.tx, crossPrepare)})
	if e != nil {
		t.Fatal(e)
	}
	aborted := n.crossSubmit(0, decision)
	if aborted.commit {
		t.Fatal("fixture did not abort", aborted)
	}
	return n.crossReadPhase(0, registered.tx, crossDecide)
}
func TestCertifiedGraphReviewEarlyAbortClosesWithoutResolvingAbsentParticipant(t *testing.T) {
	n := newCertifiedGraphNetwork(t)
	decision := certifiedEarlyAbort(t, n)
	c, _ := certifiedCoordinator(n)
	read, e := c.Open(t.Context(), graphReadRequest{mode: graphReadFresh}, nil)
	if e != nil || read == nil {
		t.Fatal("healthy early ABORT made Fresh fail", e)
	}
	defer read.Close()
	edge, e := certifiedView(t, read).Entity(t.Context(), 3)
	if e != nil || edge.Found {
		t.Fatal("ABORT produced graph effects", edge, e)
	}
	// The authority and immutable decision remain available after Fresh cleanup.
	actual := n.crossReadPhase(0, decision.status.tx, crossDecide)
	if actual.status != decision.status {
		t.Fatal("cleanup changed ABORT", actual.status, decision.status)
	}
	for _, h := range []*allocationHost{n.hosts[0][0], n.hosts[1][0]} {
		if _, e := h.Tick(); e != nil {
			t.Fatal("Fresh poisoned Driver", e)
		}
	}
}
func TestCertifiedGraphReviewOversizedScopeRefusesBeforeAllocation(t *testing.T) {
	n := newCertifiedGraphNetwork(t)
	oversized := []uint64{3, 8, 99}
	allocations := testing.AllocsPerRun(10, func() {
		scope, e := certifiedScope(n.d, oversized)
		if !errors.Is(e, errInvalid) || scope != nil {
			t.Fatal("oversized scope accepted", scope, e)
		}
	})
	if allocations != 0 {
		t.Fatalf("known oversized scope allocated %.0f times before rejection", allocations)
	}
}

// The delayed message is a real source-owned second-participant Prepare backed
// by the original durable registration and first-participant YES. Capacity
// contention then creates an immutable ABORT before that message is delivered.
func certifiedAbortedLatePrepare(t *testing.T, n *allocationTestNetwork) (crossProof, allocationProposal, crossTransaction, crossProof) {
	t.Helper()
	ops, claims := crossEdgeFixture(t, n)
	ids := crossRequestsWithLoser(n, 0)
	bad := append([]freshBinding(nil), claims...)
	bad[0].grant.sequence = 2
	other, register := crossPlanAt(n, 1, ids[1], ops, bad, 24)
	held := n.crossSubmit(1, register)
	otherRegistration := n.crossReadPhase(1, held.tx, crossRegister)
	tx, register := crossPlanAt(n, 0, ids[0], ops, claims, 25)
	registered := n.crossSubmit(0, register)
	registration := n.crossReadPhase(0, registered.tx, crossRegister)
	p, e := n.hosts[0][0].PrepareCrossParticipant(tx, 3, registration, nil)
	if e != nil {
		t.Fatal(e)
	}
	if yes := n.crossSubmit(0, p); !yes.yes {
		t.Fatal(yes)
	}
	first := n.crossReadPhase(0, registered.tx, crossPrepare)
	late, e := n.hosts[0][0].PrepareCrossParticipant(tx, 8, registration, []crossProof{first})
	if e != nil {
		t.Fatal(e)
	}
	conflict := n.capacityRead(1, late)
	abort, existing, e := n.hosts[0][0].AbortCrossTransactionForCapacity(tx, conflict)
	if e != nil || existing != (crossStatus{}) {
		t.Fatal(existing, e)
	}
	if status := n.crossSubmit(0, abort); status.commit {
		t.Fatal(status)
	}
	return n.crossReadPhase(0, registered.tx, crossDecide), late, other, otherRegistration
}
func certifiedFinishOtherAbort(t *testing.T, n *allocationTestNetwork, tx crossTransaction, registration crossProof) {
	t.Helper()
	h := n.hosts[1][0]
	p, e := h.PrepareCrossParticipant(tx, 3, registration, nil)
	if e != nil {
		t.Fatal(e)
	}
	if no := n.crossSubmit(0, p); no.yes || no.why != reasonInvalid {
		t.Fatal("real other NO required", no)
	}
	p, e = h.DecideCrossTransaction(tx, []crossProof{n.crossReadPhase(0, registration.status.tx, crossPrepare)})
	if e != nil {
		t.Fatal(e)
	}
	n.crossSubmit(1, p)
	crossResolveAt(n, 1, n.crossReadPhase(1, registration.status.tx, crossDecide), 0, 1)
}
func TestCertifiedGraphReviewAbortSkipPreservesOtherOwnerAndDelayedPrepareBeforeCertificate(t *testing.T) {
	n := newCertifiedGraphNetwork(t)
	decision, late, other, registration := certifiedAbortedLatePrepare(t, n)
	c, tr := certifiedCoordinator(n)
	h := n.hosts[0][0]
	observation, e := c.read(t.Context(), h, 1, 0)
	if e != nil {
		t.Fatal(e)
	}
	if e := c.resolvePending(t.Context(), observation.observation); e != nil {
		t.Fatal("verified absent participant failed", e)
	}
	// Skipping the absent aborted transaction must leave a different native
	// completion reservation and its persisted pointer byte-for-byte intact.
	remote := n.hosts[1][0]
	view, e := remote.machine.store.ApplicationView(remote.driver.Applied())
	if e != nil {
		t.Fatal(e)
	}
	owner, _, e := view.RecoveryOwner(t.Context(), raftlog.ReadBudget{Rows: 2, Bytes: 4096})
	if e != nil || owner.ID != crossRecoveryID(remote.machine.ns, registration.status.tx) || owner.Digest != other.digest {
		t.Fatal("ABORT skip retired different native owner", owner, e)
	}
	if e := view.Close(); e != nil {
		t.Fatal(e)
	}
	certifiedFinishOtherAbort(t, n, other, registration)
	injected := false
	tr.beforeSubmit = func(target *allocationHost, p allocationProposal) {
		command, e := decodeGraphCutCommand(p.wire)
		if e != nil {
			t.Fatal(e)
		}
		if target.machine.ns.partition == 8 && command.phase == graphCutCertify && !injected {
			injected = true
			if yes := n.crossSubmit(1, late); !yes.yes {
				t.Fatal("delayed source-owned Prepare did not acquire YES", yes)
			}
		}
	}
	if read, e := c.Open(t.Context(), graphReadRequest{mode: graphReadFresh}, nil); !errors.Is(e, errGraphCutPending) || read != nil {
		t.Fatal("late Prepare before certificate bypassed owner gate", read, e)
	}
	if !injected {
		t.Fatal("late message not delivered")
	}
	tr.beforeSubmit = nil
	read := certifiedOpen(t, c, graphReadRequest{mode: graphReadFresh}, nil)
	edge, e := certifiedView(t, read).Entity(t.Context(), 3)
	if e != nil || edge.Found {
		t.Fatal("late aborted kernel became graph effects", edge, e)
	}
	actual := n.crossReadPhase(0, decision.status.tx, crossDecide)
	if actual.status != decision.status {
		t.Fatal("late Prepare changed immutable ABORT", actual.status, decision.status)
	}
}
func TestCertifiedGraphReviewDelayedPrepareAfterCertificateKeepsOldCutAndNextFreshResolvesAbort(t *testing.T) {
	n := newCertifiedGraphNetwork(t)
	decision, late, other, registration := certifiedAbortedLatePrepare(t, n)
	c, _ := certifiedCoordinator(n)
	observation, e := c.read(t.Context(), n.hosts[0][0], 1, 0)
	if e != nil {
		t.Fatal(e)
	}
	if e := c.resolvePending(t.Context(), observation.observation); e != nil {
		t.Fatal(e)
	}
	certifiedFinishOtherAbort(t, n, other, registration)
	old := certifiedOpen(t, c, graphReadRequest{mode: graphReadFresh}, nil)
	cut, e := old.Cut()
	if e != nil {
		t.Fatal(e)
	}
	if yes := n.crossSubmit(1, late); !yes.yes {
		t.Fatal("late valid Prepare was not exercised", yes)
	}
	for _, read := range []*certifiedGraphRead{old, certifiedOpen(t, c, graphReadRequest{mode: graphReadAt, cut: cut}, nil)} {
		edge, e := certifiedView(t, read).Entity(t.Context(), 3)
		if e != nil || edge.Found {
			t.Fatal("late private intent changed old cut", edge, e)
		}
	}
	fresh := certifiedOpen(t, c, graphReadRequest{mode: graphReadFresh}, nil)
	edge, e := certifiedView(t, fresh).Entity(t.Context(), 3)
	if e != nil || edge.Found {
		t.Fatal("next Fresh installed aborted effect", edge, e)
	}
	for g := range 2 {
		status, _, e := n.hosts[g][0].machine.observeCrossTransaction(crossQuery{tx: decision.status.tx}, n.hosts[g][0].driver.Applied())
		if e != nil || status.phase != crossResolve || status.commit {
			t.Fatal("late ABORT left retained intent", status, e)
		}
	}
}
func TestCertifiedGraphReviewAbortNeedsCompletedManifestBoundParticipantAuthority(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(map[bool]string{false: "unavailable", true: "missing"}[missing], func(t *testing.T) {
			n := newCertifiedGraphNetwork(t)
			decision := certifiedEarlyAbort(t, n)
			c, tr := certifiedCoordinator(n)
			tr.participantUnavailable = 8
			if missing {
				tr.participantUnavailable = 0
				tr.participantMissing = true
			}
			read, e := c.Open(t.Context(), graphReadRequest{mode: graphReadFresh}, nil)
			want := replica.ErrUnavailable
			if missing {
				want = errInvalid
			}
			if read != nil || !errors.Is(e, want) {
				t.Fatal("missing authority treated as absence", read, e)
			}
			actual := n.crossReadPhase(0, decision.status.tx, crossDecide)
			if actual.status != decision.status {
				t.Fatal("authority failure changed ABORT", actual.status, decision.status)
			}
		})
	}
}

func certifiedReceiptIssuer(t *testing.T, local bool) (*allocationTestNetwork, graphCommitReceipt) {
	t.Helper()
	n := newCertifiedGraphNetwork(t)
	ops, claims := crossEdgeFixture(t, n)
	if local {
		h := n.hosts[0][0]
		id := n.requestID()
		p, e := h.PreparePartitionGraphOperations(id, []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Name: "issued-local", Scope: ops[0].Scope}}, 27, nil)
		if e != nil {
			t.Fatal(e)
		}
		n.submitPartitionGraph(0, p, reasonNone)
		_, tr := certifiedCoordinator(n)
		nonce, e := graphReadNonce()
		if e != nil {
			t.Fatal(e)
		}
		reply, e := tr.Read(t.Context(), h, graphCutQuery{kind: 3, request: [16]byte(id), nonce: nonce})
		if e != nil || reply.err != nil {
			t.Fatal(e, reply.err)
		}
		receipt, e := graphReceiptFromLocal(reply.proof)
		if e != nil {
			t.Fatal(e)
		}
		return n, receipt
	}
	tx, p := n.crossPlan(ops, claims, 27)
	decision := n.crossCommit(tx, p)
	n.crossResolve(decision)
	receipt, e := graphReceiptFromDecision(decision)
	if e != nil {
		t.Fatal(e)
	}
	return n, receipt
}
func TestCertifiedGraphReviewAfterRejectsGenuineUnrelatedAuthorityReceipts(t *testing.T) {
	for _, local := range []bool{true, false} {
		for _, variant := range []string{"topology", "group", "ownership", "configuration"} {
			t.Run(map[bool]string{true: "local", false: "cross"}[local]+"-"+variant, func(t *testing.T) {
				issuer, receipt := certifiedReceiptIssuer(t, local)
				target := newCrossPartitionNetworkSpecification(t, nil, certifiedGraphSemanticContractID(), func(r *declaredInitCommand) {
					if variant == "configuration" {
						r.maxBlock = 32
						return
					}
					entries := make([]graphstore.PartitionOwnership, r.declaration.Len())
					for i := range entries {
						entries[i], _ = r.declaration.PartitionAt(i)
					}
					epoch := r.declaration.TopologyEpoch()
					switch variant {
					case "topology":
						epoch++
					case "group":
						entries[0].Group[0] ^= 1
					case "ownership":
						entries[0].OwnershipEpoch++
					}
					d, e := graphstore.NewOwnershipDeclaration(r.declaration.Graph(), epoch, entries, defaultMaterializerLimits().catalog)
					if e != nil {
						t.Fatal(e)
					}
					r.declaration = d
					owners := make([]graphstore.BucketOwnership, 6)
					for family := range 3 {
						for bucket := range 2 {
							owners[family*2+bucket] = graphstore.BucketOwnership{Kind: graphstore.RoutingBucketKind(family + 1), Bucket: uint32(bucket), Partition: 3, Epoch: 1}
						}
					}
					routing, e := graphstore.NewPartitionRouting(d, 1, 1, owners, defaultMaterializerLimits().catalog)
					if e != nil {
						t.Fatal(e)
					}
					r.routing = new(routing)
				})
				if issuer.d.Graph() != target.d.Graph() {
					t.Fatal("receipt mismatch must use same GraphID")
				}
				c, _ := certifiedCoordinator(target)
				read, e := c.Open(t.Context(), graphReadRequest{mode: graphReadAfter, receipt: receipt}, nil)
				if read != nil {
					_ = read.Close()
				}
				if !errors.Is(e, errInvalid) || read != nil {
					t.Fatal("genuine unrelated authority receipt claimed included", variant, local, e)
				}
			})
		}
	}
}

func TestCertifiedGraphReviewAfterReceiptSurvivesIssuerCloseAndGenuineFollowerReopen(t *testing.T) {
	for _, local := range []bool{true, false} {
		t.Run(map[bool]string{true: "local", false: "cross"}[local], func(t *testing.T) {
			n, receipt := certifiedReceiptIssuer(t, local)
			crossReopenAll(t, n)
			c, tr := certifiedCoordinator(n)
			read := certifiedOpen(t, c, graphReadRequest{mode: graphReadAfter, receipt: receipt}, nil)
			cut, e := read.Cut()
			if e != nil {
				t.Fatal(e)
			}
			if local {
				assertCertifiedProjection(t, certifiedView(t, read), 1, true, []string{"issued-local"})
			} else {
				assertCertifiedProjection(t, certifiedView(t, read), 3, true, nil)
			}
			tr.member = 2
			follower := certifiedOpen(t, c, graphReadRequest{mode: graphReadAt, cut: cut}, nil)
			if local {
				assertCertifiedProjection(t, certifiedView(t, follower), 1, true, []string{"issued-local"})
			} else {
				assertCertifiedProjection(t, certifiedView(t, follower), 3, true, nil)
			}
		})
	}
}
