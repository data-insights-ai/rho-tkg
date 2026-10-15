package graphapply

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

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
	closeNative := func(group int, owner, life uint64) {
		p, err := n.hosts[group][0].PreparePartitionGraphOperations(n.requestID(), []graphstate.Operation{{Kind: graphstate.Close, Owner: graphstate.EntityID(owner), Life: graphstate.LifeID(life), Scope: set.Scope}}, 50+owner, nil)
		if err != nil {
			t.Fatal(err)
		}
		n.submitPartitionGraph(group, p, reasonNone)
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
	closeNative(1, 17, 18)
	checkRefusal(false)
	closeNative(0, 3, 4)
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
