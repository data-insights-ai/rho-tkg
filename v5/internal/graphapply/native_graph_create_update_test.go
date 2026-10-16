package graphapply

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func TestNativeScalarQueryPagePreservesSharedSourceAllowance(t *testing.T) {
	n := collectedGraphNetworkWithAgreement(t, nil, durableColumnGraphSemanticContractID())
	ops, claims := crossEdgeFixture(t, n)
	c := remoteCertifiedFixture(t, n)
	committed, err := c.Run(t.Context(), n.requestID(), ops, 37, claims)
	if err != nil || !committed.decision.commit {
		t.Fatal("genuine relationship before retained query", err)
	}
	publisher, err := newRetainedPinPublisher(c)
	if err != nil {
		t.Fatal(err)
	}
	pin, err := publisher.Begin(t.Context(), n.requestID(), graphReadRequest{mode: graphReadFresh}, 100)
	if err != nil {
		t.Fatal(err)
	}
	token, err := pin.Publish(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 30*time.Second)
		defer cancel()
		if err := pin.Release(cleanup); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	spec := nativeSelectorsSpec{1, nativeCreateUpdateFamily, 20, 100, 1, "kv"}
	deadline, _ := ctx.Deadline()
	policy, err := nativeCreateUpdateReadPolicy(spec, nativeSelectorsAdmission{Family: nativeCreateUpdateFamily, Spec: spec, BudgetSeconds: 180, DeadlineUnixNanos: deadline.UnixNano()})
	if err != nil {
		t.Fatal(err)
	}
	s := &nativeSelectorsSessions{ctx: ctx, coordinator: c, policy: policy}
	owner := &nativeSelectorsQuery{token: token, predicate: graphstate.IncidentPredicate{Endpoint: 1, Window: ops[0].Scope}}
	first, _, err := s.page(ctx, owner, nil, true)
	if err != nil || !first.EOF || !slices.Equal(first.Candidates, []uint64{3}) || len(first.Points) != 1 || !first.Points[0].Effective {
		t.Fatal("complete genuine retained candidate page", first, err)
	}
	ids := make([]uint64, 16)
	for i := range ids {
		ids[i] = 3
	}
	beforeStarts, beforeCharged := s.starts, s.charged
	points, _, err := s.page(ctx, owner, ids, false)
	if err != nil || len(points.Points) != len(ids) {
		t.Fatal("bounded same-cut hydration refused within original allowance", len(points.Points), err)
	}
	grant, err := nativeColumnRetentionWork(c.host.machine.limits).scaled(uint64(len(ids)))
	if err != nil {
		t.Fatal(err)
	}
	wantCharged, err := beforeCharged.plus(grant)
	if err != nil || s.starts != beforeStarts+uint64(len(ids)) || s.charged != wantCharged {
		t.Fatal("hydration reset or failed to prepay the original query allowance", s.starts, s.charged, err)
	}
	for _, point := range points.Points {
		if point.ID != 3 || !point.Found || !point.Effective || point.EffectiveLife != 4 || point.Source != 1 || point.Target != 17 {
			t.Fatal("hydration changed genuine same-cut relationship", point)
		}
	}
	closed, err := c.Run(ctx, n.requestID(), []graphstate.Operation{{Kind: graphstate.Close, Owner: 3, Life: 4, Scope: ops[0].Scope}}, 41, nil)
	if err != nil || !closed.decision.commit {
		t.Fatal("genuine cross-partition relationship closure", err)
	}
	old, _, err := s.page(ctx, owner, []uint64{3}, false)
	if err != nil || len(old.Points) != 1 || !old.Points[0].Effective || old.Points[0].EffectiveLife != 4 {
		t.Fatal("old query substituted the closed current relationship", old, err)
	}
	current, err := c.OpenCertified(ctx, graphReadRequest{mode: graphReadFresh}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := current.Close(); err != nil {
			t.Error(err)
		}
	}()
	view, err := current.ReadView()
	if err != nil {
		t.Fatal(err)
	}
	point, err := nativeSelectorsEntity(ctx, view, c.host, 3)
	if err != nil || !point.Found || point.Effective {
		t.Fatal("current source failed to observe genuine relationship closure", point, err)
	}
	s.policy.Starts = s.starts
	beforeStarts, beforeCharged, beforeWork := s.starts, s.charged, owner.work
	if _, _, err := s.page(ctx, owner, []uint64{3}, false); !errors.Is(err, errLimit) || s.starts != beforeStarts || s.charged != beforeCharged || owner.work != beforeWork {
		t.Fatal("exhausted original allowance renewed a source or discarded prior work", err)
	}
}

func TestNativeScalarQueryAdmissionPrepaysHydrationAndSeparateExports(t *testing.T) {
	for _, facts := range []uint64{20, 1_000_000} {
		for _, passes := range []uint64{5, 9, nativeSelectorsQueries} {
			const seconds = uint64(180)
			const frames = uint64(3200)
			p, err := nativeScalarReadAllowance(facts, seconds, 1, frames, passes)
			if err != nil {
				t.Fatal(err)
			}
			// Every pass may hydrate N candidates, walk ceil(N/16) pages,
			// open/reopen once and issue the six named point probes. Each of
			// the 64 failed full pages can
			// have consumed its walker and every one of 16 hydrations.
			query := passes * (facts + (facts+15)/16 + 1 + 6)
			exports := 4 * ((frames + 3) / 4)
			failed := uint64(64 * 17)
			if p.Starts < query+exports+failed || p.Calls != 8*p.Starts {
				t.Fatal("complete queries borrowed the separate raw-export/retry grants", p.Starts, query, exports, failed)
			}
		}
	}
	if _, err := nativeScalarReadAllowance(20, 180, 1, 0, 5); !errors.Is(err, errInvalid) {
		t.Fatal("zero raw-export admission", err)
	}
	if _, err := nativeScalarReadAllowance(20, 180, 1, ^uint64(0), 5); !errors.Is(err, errLimit) {
		t.Fatal("overflowing source grant", err)
	}
}

// Authored guards only; the actual six-process workload is qualified later.
func TestNativeGraphCreateUpdateFreshProfileAndLocalChoice(t *testing.T) {
	if _, err := newNativeCreateUpdateSessions(context.Background(), nil, nil); !errors.Is(err, errInvalid) {
		t.Fatal("nil authentic query coordinator", err)
	}
	if _, err := nativeCreateUpdateColumnPolicy(nil, nativeSelectorsSpec{}, nativeSelectorsAdmission{}, [32]byte{}); !errors.Is(err, errInvalid) {
		t.Fatal("nil native column host", err)
	}
	for _, writers := range []int{1, 8, 32} {
		for _, cross := range []uint64{0, 10, 50, 100} {
			s := nativeSelectorsSpec{1, nativeCreateUpdateFamily, 40, cross, writers, "columns"}
			p := nativeCreateUpdateCost(s)
			if p.Version != 9 || p.validate() != nil || nativeSelectorsValidate(p) == nil || nativeFeedValidate(p) == nil {
				t.Fatal("fresh profile leaked into older families")
			}
			var multiple uint64
			for fact := uint64(0); fact < s.Facts; fact++ {
				if p.cross(fact) {
					multiple++
				}
			}
			if multiple*100 != s.Facts*cross || nativeGraphCostOperationID(p, 4, 0).Dataset != nativeGraphCostOperationID(nativeGraphCostSpec{1, 40, cross, writers}, 4, 0).Dataset {
				t.Fatal("business footprint/dataset changed with private profile")
			}
			if _, err := nativeCreateUpdateFrames(p); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := nativeCreateUpdateValidate(nativeGraphCostSpec{6, 40, 0, 1}); !errors.Is(err, errInvalid) {
		t.Fatal("selector profile relabelled", err)
	}
	if err := nativeCreateUpdateValidate(nativeGraphCostSpec{9, 20, 0, 32}); !errors.Is(err, errInvalid) {
		t.Fatal("unfunded actual caller seed", err)
	}
	s := nativeSelectorsSpec{1, nativeCreateUpdateFamily, 40, 50, 32, "kv"}
	a := nativeSelectorsAdmission{Family: nativeCreateUpdateFamily, Spec: s, BudgetSeconds: 180, DeadlineUnixNanos: 1}
	p, err := nativeCreateUpdateReadPolicy(s, a)
	if err != nil || p.Queries != 5 || p.Calls != 8*p.Starts || p.DeadlineUnixNanos != a.DeadlineUnixNanos {
		t.Fatal("five actual observations with one immutable finite allowance", err)
	}
	q := nativeSelectorsInput{Action: "scan", Sequence: 1}
	r := nativeSelectorsReply{Sequence: 1, Candidates: []uint64{7}, Points: []nativeLifecycleEntity{{ID: 7, Found: true, Effective: true, Kind: 2, Type: "edge", Mode: 1, Source: 1, Target: 3, EffectiveLife: 8}}}
	observed := nativeCreateUpdatePhase{Phase: "update"}
	var sequence uint64
	if err := nativeCreateUpdateCount(s, &observed, &sequence, q, r); err != nil || observed.SingleParticipant != 1 || observed.MultiParticipant != 0 {
		t.Fatal("actual observed one-participant descriptor", err)
	}
	if err := nativeCreateUpdateCount(s, &observed, &sequence, q, r); !errors.Is(err, errInvalid) || observed.SingleParticipant != 1 {
		t.Fatal("duplicate successful observation counted", err)
	}
	q.Sequence, r.Sequence = 2, 2
	r.Points[0].Target = 99
	if err := nativeCreateUpdateCount(s, &observed, &sequence, q, r); !errors.Is(err, errCorrupt) || sequence != 1 || observed.SingleParticipant != 1 {
		t.Fatal("unknown endpoint invented business locality", err)
	}
	r.Points[0].Target = nativeCreateUpdateCost(s).remoteNode()
	if err := nativeCreateUpdateCount(s, &observed, &sequence, q, r); err != nil || observed.ReadParticipants != [2]uint64{2, 1} || observed.WriteParticipants != [2]uint64{2, 0} {
		t.Fatal("remote update is a real endpoint read, not a logical remote property write", err)
	}
	row := nativeColumnRow{Owner: 7, Component: 1, Kind: 2, Mode: 1, Type: "edge", Source: 1, Target: 3, SourceLife: 2, TargetLife: 4, Axis: []byte{1}, Scope: []byte{1}, Before: nativeColumnCell{Present: true, Handle: 8, Revision: 10, Provenance: 4}, After: nativeColumnCell{Present: true, Handle: 8, Revision: 11, Provenance: 44}}
	if class, err := nativeGraphCostColumnRow(nativeCreateUpdateCost(s), row); err != nil || class != 0 {
		t.Fatal("fresh9 presence revision omitted from column census", err)
	}
	row.Before.Provenance++
	if _, err := nativeGraphCostColumnRow(nativeCreateUpdateCost(s), row); !errors.Is(err, errCorrupt) {
		t.Fatal("foreign before-presence accepted", err)
	}
	t.Run("genuine-native-guard-preserves-SEM06-and-refuses-revival", nativeCreateUpdateGuard)
}

func nativeCreateUpdateGuard(t *testing.T) {
	// Reuse actual six-voter native stores with their existing relationship
	// descriptor schema. Its Set has the SAME production life/predicate rules;
	// the Cost9 raw-original guard separately binds p and the exact5->6 scalars.
	n, c, edge, claims, _ := coordinatorNativeFixture(t)
	committed, err := c.Run(t.Context(), n.requestID(), edge, 37, claims)
	if err != nil || !committed.decision.commit {
		t.Fatal("actual cross-edge setup", err)
	}
	ops, _ := partitionNineOperations(t, 1)
	set := graphstate.Operation{Kind: graphstate.Set, Owner: 3, Life: 4, Scope: edge[0].Scope, Name: "descriptor", Value: ops[6].Value, ValueID: 5}
	committed, err = c.Run(t.Context(), n.requestID(), []graphstate.Operation{set}, 40, []freshBinding{{role: valueBinding, id: 5, grant: claims[0].grant}})
	if err != nil || !committed.decision.commit {
		t.Fatal("actual initial property", err)
	}
	held, closeHeld := crossView(t, n, 0, [2]uint64{})
	defer closeHeld()
	guarded := nativeScalarGuardedUpdate(nil, set)
	revision, err := state.NewRevision(100, 42)
	if err != nil {
		t.Fatal(err)
	}
	delta, err := graphstate.Plan(t.Context(), held, guarded, revision, defaultMaterializerLimits().graph.Planner)
	if err != nil || len(delta.Patches) != 2 || delta.Patches[0].Key.Kind != graphstate.ScalarProperty || delta.Patches[1].Key.Kind != graphstate.Presence {
		t.Fatal("atomic Set then presence reaffirmation", delta, err)
	}
	var remoteReads [3]bool
	for _, dependency := range delta.Dependencies {
		if dependency.Kind == graphstate.EntityDependency && dependency.Owner == 17 {
			remoteReads[0] = true
		}
		if dependency.Kind == graphstate.LifeDependency && dependency.Owner == 17 && dependency.Life == 18 {
			remoteReads[1] = true
		}
		if dependency.Kind == graphstate.ComponentDependency && dependency.Key.Owner == 17 && dependency.Key.Kind == graphstate.Presence {
			remoteReads[2] = true
		}
	}
	for _, patch := range delta.Patches {
		if patch.Key.Owner != 3 {
			t.Fatal("remote endpoint became a logical write", patch.Key)
		}
	}
	if remoteReads != [3]bool{true, true, true} {
		t.Fatal("guard failed to capture actual remote entity/life/presence dependencies", remoteReads)
	}
	closeNative := func(owner, life uint64) {
		// Closing the relationship has real remote endpoint dependencies. Use
		// the authentic planner/coordinator rather than submit it as local-only.
		closed, err := c.Run(t.Context(), n.requestID(), []graphstate.Operation{{Kind: graphstate.Close, Owner: graphstate.EntityID(owner), Life: graphstate.LifeID(life), Scope: set.Scope}}, 50+owner, nil)
		if err != nil || !closed.decision.commit {
			t.Fatal("actual native closure", owner, err)
		}
	}
	checkRefusal := func(closedRelation bool) {
		view, closeView := crossView(t, n, 0, [2]uint64{})
		defer closeView()
		var before [2][]byte
		for group := range 2 {
			_, before[group], err = n.hosts[group][0].machine.store.Checkpoint()
			if err != nil {
				t.Fatal(err)
			}
		}
		plain, plainErr := graphstate.Plan(t.Context(), view, []graphstate.Operation{set}, revision, defaultMaterializerLimits().graph.Planner)
		if closedRelation {
			if !errors.Is(plainErr, graphstate.ErrOwnerValidity) {
				t.Fatal("ordinary Set revived relation", plainErr)
			}
		} else if plainErr != nil || len(plain.Patches) != 1 {
			t.Fatal("SEM06 ordinary Set changed", plainErr)
		}
		failed, err := graphstate.Plan(t.Context(), view, guarded, revision, defaultMaterializerLimits().graph.Planner)
		if !errors.Is(err, graphstate.ErrOwnerValidity) || len(failed.Patches) != 0 || len(failed.Values) != 0 {
			t.Fatal("failed second guard exposed staged Set/revived relation", failed, err)
		}
		for group := range 2 {
			_, after, err := n.hosts[group][0].machine.store.Checkpoint()
			if err != nil || !bytes.Equal(before[group], after) {
				t.Fatal("refused plan changed native source", err)
			}
		}
	}
	closeNative(17, 18)
	checkRefusal(false)
	closeNative(3, 4)
	checkRefusal(true)
	position, err := temporal.RationalPosition(graphGenesisBinding(t).Axis(), temporal.RationalInt64(0))
	if err != nil {
		t.Fatal(err)
	}
	old, err := graphstate.Project(t.Context(), held, 3, position, graphstate.Effective, defaultMaterializerLimits().graph.Planner)
	if err != nil || !old.Active || len(old.Properties) != 1 || old.Properties[0].Name != "descriptor" {
		t.Fatal("old native view substituted masked/closed current", old, err)
	}
}
