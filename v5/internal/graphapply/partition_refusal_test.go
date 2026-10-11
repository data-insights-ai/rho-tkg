package graphapply

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func TestPartitionSameAxisIDDifferentDefinitionRejectsCommittedThenProgresses(t *testing.T) {
	network, routing := partitionIntegrationNetwork(t)
	handle := network.activate(0, [16]byte{1})
	network.acquire(0, handle, 1, 16)
	host := network.hosts[0][0]
	_, _, axis := partitionValueFixture(t, routing, 3)
	scope, err := temporal.All(axis)
	if err != nil {
		t.Fatal(err)
	}
	operations := []graphstate.Operation{{Kind: graphstate.CreateNode, Owner: 14, Life: 15, Scope: scope, Record: graphstate.EntityRecord{Interpretation: graphstate.InterpretationObservation, TemporalRole: graphstate.TemporalRoleCausalOrder}}}
	claims := []freshBinding{{role: entityBinding, id: 14, grant: grantReference{session: handle.session, sequence: 1}}, {role: lifeBinding, owner: 14, id: 15, grant: grantReference{session: handle.session, sequence: 1}}}
	original, err := host.PreparePartitionGraphOperations(network.requestID(), operations, 23, claims)
	if err != nil {
		t.Fatal(err)
	}
	installed := network.submitPartitionGraph(0, original, reasonNone)
	descriptor := axis.Descriptor()
	descriptor.Reference = "conflicting-reference:v1"
	conflicting, err := temporal.NewAxis(descriptor, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	window, err := temporal.All(conflicting)
	if err != nil {
		t.Fatal(err)
	}
	invalid, err := host.PreparePartitionGraphOperations(network.requestID(), []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 14, Life: 15, Name: "bad", Scope: window}}, 24, nil)
	if err != nil {
		t.Fatal(err)
	}
	var images [3][]byte
	for i, replica := range network.hosts[0] {
		_, images[i], err = replica.machine.store.Checkpoint()
		if err != nil {
			t.Fatal(err)
		}
	}
	rejected := network.submitPartitionGraph(0, invalid, reasonInvalid)
	for i, replica := range network.hosts[0] {
		_, image, err := replica.machine.store.Checkpoint()
		if err != nil || !bytes.Equal(image, images[i]) {
			t.Fatal("axis rebind changed graph root", i, err)
		}
		cdc, err := replica.machine.store.ApplicationRecord(t.Context(), rejected.index, false, replica.machine.store.ApplicationLimits().MaxChangeBytes)
		if err != nil || len(cdc) != 0 {
			t.Fatal("axis rebind emitted CDC", i, err)
		}
		projected, _ := partitionProjection(t, replica, rejected.index, 14, axis)
		if !projected.Active || len(projected.Labels) != 0 || projected.Record.Axis.Descriptor() != axis.Descriptor() {
			t.Fatal("axis rebind changed current graph", projected)
		}
		old, _ := partitionProjection(t, replica, installed.index, 14, axis)
		if !old.Active || old.Record.Axis.Descriptor() != axis.Descriptor() {
			t.Fatal("axis rebind changed retained graph", old)
		}
	}
	valid, err := host.PreparePartitionGraphOperations(network.requestID(), []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 14, Life: 15, Name: "good", Scope: scope}}, 25, nil)
	if err != nil {
		t.Fatal(err)
	}
	accepted := network.submitPartitionGraph(0, valid, reasonNone)
	projection, _ := partitionProjection(t, host, accepted.index, 14, axis)
	if len(projection.Labels) != 1 || projection.Labels[0] != "good" {
		t.Fatal("local write did not progress after axis rejection", projection)
	}
}

func TestPartitionCheckedStagerRetainsAllNineDependencyKindsAndRefusesNilBudget(t *testing.T) {
	request := partitionCodecRequest(t, 0, defaultMaterializerLimits())
	network := newPartitionWriteNetworkWithRouting(t, request, partitionWriteSchemas())
	handle := network.activate(0, [16]byte{1})
	network.acquire(0, handle, 1, 16)
	host := network.hosts[0][0]
	proof := network.read(0, allocationQuery{kind: observeConfiguration})
	view, err := host.machine.store.ApplicationView(host.driver.Applied())
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	operations, _ := partitionNineOperations(t, 1)
	revision, err := state.NewRevision(1, 23)
	if err != nil {
		t.Fatal(err)
	}
	budget := graphstore.OwnershipBudget{SourceRows: 1024, SourceBytes: 4 << 20, OutputBytes: 16 << 20}
	if effects, work, err := graphstore.StagePartitionOperationsWithOutputBudget(t.Context(), view, network.d, graphGenesisBinding(t), proof.observation.configuration, operations, revision, host.machine.limits.catalog, host.machine.limits.graph, budget, nil); !errors.Is(err, graphstore.ErrInvalid) || len(effects.Writes) != 0 || work.Records != 0 {
		t.Fatal(effects, work, err)
	}
	parent, _ := graphstate.NewOutputBudget(32 << 20)
	effects, _, err := graphstore.StagePartitionOperationsWithOutputBudget(t.Context(), view, network.d, graphGenesisBinding(t), proof.observation.configuration, operations, revision, host.machine.limits.catalog, host.machine.limits.graph, budget, parent)
	if err != nil {
		t.Fatal(err)
	}
	var kinds [10]bool
	for _, dependency := range effects.Dependencies {
		if dependency.Kind < graphstate.EntityDependency || dependency.Kind > graphstate.IncidentDependency || dependency.View != effects.Delta.View || dependency.Version == 0 {
			t.Fatal("dependency source/fence erased", dependency)
		}
		kinds[dependency.Kind] = true
	}
	for kind := graphstate.EntityDependency; kind <= graphstate.IncidentDependency; kind++ {
		if !kinds[kind] {
			t.Fatal("dependency kind omitted", kind)
		}
	}
	if len(effects.Dependencies) != len(effects.Delta.Dependencies) || len(effects.Dependencies) != 58 {
		t.Fatal("complete planner dependencies not returned", len(effects.Dependencies))
	}
	assertPartitionNineDependencies(t, effects.Dependencies, operations, effects.Delta.View, graphstate.ReadVersion(host.driver.Applied()))
	if !reflect.DeepEqual(effects.Dependencies, effects.Delta.Dependencies) {
		t.Fatal("normalized dependency copy diverged")
	}
	if _, err := view.Root(); err != nil {
		t.Fatal("stager consumed borrowed view", err)
	}
}

// Expected predicates come from the nine-operation fixture: five new records,
// three typed property writes, two node-life incident predicates and all five
// life-scoped key prefixes. Exact multiplicities retain repeated validations.
func assertPartitionNineDependencies(t *testing.T, got []graphstate.Dependency, ops []graphstate.Operation, view graphstate.ViewID, version graphstate.ReadVersion) {
	t.Helper()
	var expected []graphstate.Dependency
	add := func(d graphstate.Dependency, count int) {
		d.View, d.Version = view, version
		for range count {
			expected = append(expected, d)
		}
	}
	all := ops[0].Scope
	for _, owner := range []graphstate.EntityID{1, 3, 5, 7, 9} {
		add(graphstate.Dependency{Kind: graphstate.EntityDependency, Owner: owner, Absent: true}, 1)
		add(graphstate.Dependency{Kind: graphstate.LifeDependency, Owner: owner, Life: graphstate.LifeID(owner + 1), Absent: true}, 1)
		add(graphstate.Dependency{Kind: graphstate.PrefixDependency, Prefix: graphstate.KeyPredicate{Owner: owner, Life: graphstate.LifeID(owner + 1)}}, 1)
	}
	for i, count := range []int{7, 6, 5, 2, 3} {
		add(graphstate.Dependency{Kind: graphstate.ComponentDependency, Key: graphstate.ComponentKey{Owner: graphstate.EntityID(1 + i*2), Kind: graphstate.Presence}, Window: all, Absent: true}, count)
	}
	for _, item := range []struct {
		key   graphstate.ComponentKey
		count int
	}{
		{graphstate.ComponentKey{Owner: 1, Life: 2, Kind: graphstate.Label, Name: "old"}, 1},
		{graphstate.ComponentKey{Owner: 1, Life: 2, Kind: graphstate.ScalarProperty, Name: "descriptor"}, 1},
		{graphstate.ComponentKey{Owner: 3, Life: 4, Kind: graphstate.ScalarProperty, Name: "p"}, 3},
		{graphstate.ComponentKey{Owner: 5, Life: 6, Kind: graphstate.ScalarProperty, Name: "descriptor"}, 1},
	} {
		add(graphstate.Dependency{Kind: graphstate.ComponentDependency, Key: item.key, Window: all, Absent: true}, item.count)
	}
	for _, item := range []struct {
		owner graphstate.EntityKind
		name  string
	}{
		{graphstate.Node, "descriptor"}, {graphstate.Node, "p"}, {graphstate.Relationship, "descriptor"},
	} {
		add(graphstate.Dependency{Kind: graphstate.SchemaDependency, OwnerKind: item.owner, Name: item.name}, 2)
	}
	for _, id := range []graphstate.ValueID{11, 12} {
		add(graphstate.Dependency{Kind: graphstate.ValueDependency, ValueID: id, Absent: true}, 1)
	}
	for _, value := range []graphstate.Scalar{ops[6].Value, ops[7].Value} {
		add(graphstate.Dependency{Kind: graphstate.ValueIdentityDependency, Value: value, Absent: true}, 1)
	}
	for _, owner := range []graphstate.EntityID{1, 3} {
		add(graphstate.Dependency{Kind: graphstate.IncidentDependency, Incident: graphstate.IncidentPredicate{Endpoint: owner, Life: graphstate.LifeID(owner + 1), Window: all}}, 1)
	}
	add(graphstate.Dependency{Kind: graphstate.UniquenessDependency, Unique: graphstate.UniquePredicate{Definition: partitionWriteSchemas()[1], Value: graphstate.I64(9), Window: all}}, 2)
	if len(expected) != 58 || len(got) != len(expected) {
		t.Fatal("complete expected dependency count", len(got), len(expected))
	}
	matched := make([]bool, len(expected))
	for _, actual := range got {
		found := false
		for i, want := range expected {
			if !matched[i] && reflect.DeepEqual(actual, want) {
				matched[i], found = true, true
				break
			}
		}
		if !found {
			t.Fatalf("unexpected or duplicate dependency: %+v", actual)
		}
	}
}

func TestPartitionAxisBusinessMarkerNeverMasksDerivedRebindingOrCorruption(t *testing.T) {
	if _, recognized := businessGraphReason(graphstore.ErrRebinding); recognized {
		t.Fatal("raw derived-index rebinding became business rejection")
	}
	marked := errors.Join(graphstate.ErrInvalidInput, graphstore.ErrRebinding)
	if why, recognized := businessGraphReason(marked); !recognized || why != reasonInvalid {
		t.Fatal(why, recognized)
	}
	for _, cause := range []error{graphstore.ErrCorrupt, graphstore.ErrPoisoned, graphstore.ErrResourceLimit} {
		if _, recognized := businessGraphReason(errors.Join(marked, cause)); recognized {
			t.Fatal("operational cause masked", cause)
		}
	}
}

func TestPartitionSameRequestAbsentAxisConflictsRefuseBeforeEffects(t *testing.T) {
	network, routing := partitionIntegrationNetwork(t)
	handle := network.activate(0, [16]byte{1})
	network.acquire(0, handle, 1, 16)
	host := network.hosts[0][0]
	_, _, axis := partitionValueFixture(t, routing, 3)
	d := axis.Descriptor()
	d.Reference = "conflicting-reference:v1"
	other, err := temporal.NewAxis(d, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	first, err := temporal.All(axis)
	if err != nil {
		t.Fatal(err)
	}
	second, err := temporal.All(other)
	if err != nil {
		t.Fatal(err)
	}
	ops := []graphstate.Operation{{Kind: graphstate.CreateNode, Owner: 14, Life: 15, Scope: first}, {Kind: graphstate.CreateNode, Owner: 12, Life: 13, Scope: second}}
	// The actual command codec already rejects duplicate AxisID definitions.
	// The public checked typed stager must preserve the same caller classification.
	if proposal, err := host.PreparePartitionGraphOperations(network.requestID(), ops, 23, nil); !errors.Is(err, errInvalid) || len(proposal.wire) != 0 {
		t.Fatal("codec admitted two definitions", err)
	}
	proof := network.read(0, allocationQuery{kind: observeConfiguration})
	view, err := host.machine.store.ApplicationView(host.driver.Applied())
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	before, err := view.Root()
	if err != nil {
		t.Fatal(err)
	}
	revision, _ := state.NewRevision(1, 23)
	out, work, err := graphstore.StagePartitionOperations(t.Context(), view, network.d, graphGenesisBinding(t), proof.observation.configuration, ops, revision, host.machine.limits.catalog, host.machine.limits.graph, graphstore.OwnershipBudget{SourceRows: 1024, SourceBytes: 4 << 20, OutputBytes: 16 << 20})
	if !errors.Is(err, graphstate.ErrInvalidInput) || !errors.Is(err, graphstore.ErrRebinding) || !reflect.DeepEqual(out, graphstore.GraphEffects{}) || work.Records == 0 {
		t.Fatal("same-request registration collision was not caller-invalid", work, err)
	}
	after, err := view.Root()
	if err != nil || !sameBase(before, after) {
		t.Fatal("refusal changed borrowed base", err)
	}
}

func TestPartitionActualGraphReplayMismatchNoopAndGuardedConflictPreserveOriginal(t *testing.T) {
	r := partitionCodecRequest(t, 1, defaultMaterializerLimits())
	n := newPartitionWriteNetworkWithRouting(t, r, []graphstate.PropertyDefinition{{Name: "members", Owner: graphstate.Node, Type: graphstate.ScalarI64, Cardinality: graphstate.SetCardinality}})
	handle := n.activate(0, [16]byte{1})
	n.acquire(0, handle, 1, 16)
	h := n.hosts[0][0]
	scope, err := temporal.All(graphGenesisBinding(t).Axis())
	if err != nil {
		t.Fatal(err)
	}
	ops := []graphstate.Operation{{Kind: graphstate.CreateNode, Owner: 1, Life: 2, Scope: scope}}
	claims := []freshBinding{{role: entityBinding, id: 1, grant: grantReference{session: handle.session, sequence: 1}}, {role: lifeBinding, owner: 1, id: 2, grant: grantReference{session: handle.session, sequence: 1}}}
	proposal, err := h.PreparePartitionGraphOperations(n.requestID(), ops, 23, claims)
	if err != nil {
		t.Fatal(err)
	}
	original := n.submitPartitionGraph(0, proposal, reasonNone)
	view, err := h.machine.store.ApplicationView(original.index)
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	base, err := view.Root()
	if err != nil {
		t.Fatal(err)
	}
	root, err := graphstore.DecodeRoot(base.Image)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := h.currentConfiguration()
	if err != nil {
		t.Fatal(err)
	}
	digest, err := cfg.digest()
	if err != nil {
		t.Fatal(err)
	}
	checked, _, err := graphstore.OpenPartitionGraphReadView(t.Context(), view, n.d, cfg.defaultAxis, digest, h.machine.limits.catalog, h.machine.limits.graph, graphstore.OwnershipBudget{SourceRows: 1024, SourceBytes: 4 << 20, OutputBytes: 16 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := checked.Entity(t.Context(), 1); err != nil || !got.Found {
		t.Fatal(got, err)
	}
	if err := checked.Close(); err != nil {
		t.Fatal(err)
	}
	revision, _ := state.NewRevision(1, 24)
	prior := graphReadBase{group: h.machine.binding.Identity.Group, guard: graphstore.SemanticGuard{Namespace: root.Namespace(), OwnershipEpoch: h.machine.owner, TopologyEpoch: n.d.TopologyEpoch(), SchemaVersion: 1, SemanticEpoch: root.SemanticEpoch(), EffectDigest: root.EffectDigest()}}
	conditional := partitionGraphCommand{configuration: digest, declaration: n.d.Digest(), routing: cfg.routingDigest, request: graphRequest{ns: h.machine.ns, kind: guardedGraphOperations, id: n.requestID(), revision: revision, readBase: prior, operations: []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Name: "conditional", Scope: scope}}}}
	wire, err := encodePartitionGraphCommand(conditional, h.machine.limits)
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := h.PreparePartitionGraphOperations(n.requestID(), []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Name: "later", Scope: scope}}, 25, nil)
	if err != nil {
		t.Fatal(err)
	}
	n.submitPartitionGraph(0, fresh, reasonNone)
	// Actual checked prior predicate becomes stale after a real graph mutation.
	n.submitPartitionGraph(0, allocationProposal{target: proposal.target, wire: wire}, reasonReadConflict)
	before, err := h.machine.store.ApplicationView(h.driver.Applied())
	if err != nil {
		t.Fatal(err)
	}
	defer before.Close()
	beforeRoot, err := before.Root()
	if err != nil {
		t.Fatal(err)
	}
	floor, found, err := before.Get(t.Context(), partitionRoundFloorKey(h.machine.ns), 1024)
	if err != nil || !found {
		t.Fatal(found, err)
	}
	replay := n.submitPartitionGraph(0, proposal, reasonNone)
	if replay.disposition != requestReplay || replay.index != original.index {
		t.Fatal("original graph decision lost", replay, original)
	}
	parsed, err := decodePartitionGraphCommand(proposal.wire, h.machine.limits)
	if err != nil {
		t.Fatal(err)
	}
	parsed.request.revision, _ = state.NewRevision(1, 99)
	changed, err := encodePartitionGraphCommand(parsed, h.machine.limits)
	if err != nil {
		t.Fatal(err)
	}
	n.submitPartitionGraph(0, allocationProposal{target: proposal.target, wire: changed}, reasonMismatch)
	noop, err := h.PreparePartitionGraphOperations(n.requestID(), []graphstate.Operation{{Kind: graphstate.Unset, Owner: 1, Life: 2, Name: "members", Scope: scope}}, 26, nil)
	if err != nil {
		t.Fatal(err)
	}
	n.submitPartitionGraph(0, noop, reasonNone)
	for _, host := range n.hosts[0] {
		current, err := host.machine.store.ApplicationView(host.driver.Applied())
		if err != nil {
			t.Fatal(err)
		}
		currentRoot, err := current.Root()
		if err != nil || !bytes.Equal(currentRoot.Image, beforeRoot.Image) {
			t.Fatal("replay/noop/conflict changed graph", err)
		}
		actual, found, err := current.Get(t.Context(), partitionRoundFloorKey(host.machine.ns), 1024)
		if err != nil || !found || !bytes.Equal(actual.Value, floor.Value) {
			t.Fatal("replay/noop advanced round", err)
		}
		changes, err := host.machine.store.ApplicationRecord(t.Context(), host.driver.Applied(), false, host.machine.store.ApplicationLimits().MaxChangeBytes)
		if err != nil || len(changes) != 0 {
			t.Fatal("noop emitted graph CDC", err)
		}
		if err := current.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if got, _ := partitionProjection(t, h, original.index, 1, graphGenesisBinding(t).Axis()); len(got.Labels) != 0 {
		t.Fatal("replay replaced old graph", got)
	}
	if got, _ := partitionProjection(t, h, h.driver.Applied(), 1, graphGenesisBinding(t).Axis()); !reflect.DeepEqual(got.Labels, []string{"later"}) {
		t.Fatal(got)
	}
}

func TestPartitionHostDirectNilClosedAndOrdinaryClaimRefusals(t *testing.T) {
	var nilHost *allocationHost
	if p, err := nilHost.PreparePartitionGraphOperations(requestID{1}, nil, 1, nil); !errors.Is(err, errInvalid) || len(p.wire) != 0 {
		t.Fatal(err)
	}
	emptyHost := &allocationHost{}
	if p, err := emptyHost.PreparePartitionGraphOperations(requestID{1}, nil, 1, nil); !errors.Is(err, errInvalid) || len(p.wire) != 0 {
		t.Fatal("uninitialized Host", err)
	}
	legacy := newGraphGenesisNetwork(t)
	if p, err := legacy.hosts[0][0].PreparePartitionGraphOperations(requestID{1}, nil, 1, nil); !errors.Is(err, errInvalid) || len(p.wire) != 0 {
		t.Fatal("old agreement admitted partition graph writes", err)
	}
	n, _ := newPartitionWriteNetwork(t)
	h := n.hosts[0][0]
	if _, err := h.PreparePartitionGraphOperations(requestID{}, nil, 1, nil); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	scope, err := temporal.All(graphGenesisBinding(t).Axis())
	if err != nil {
		t.Fatal(err)
	}
	p, err := h.PreparePartitionGraphOperations(n.requestID(), []graphstate.Operation{{Kind: graphstate.CreateNode, Owner: 1, Life: 2, Scope: scope}}, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	// No publication cannot certify the requested primary gap as global absence.
	n.submitPartitionGraph(0, p, reasonRoutingUnknown)
	handle := n.activate(0, [16]byte{1})
	n.acquire(0, handle, 1, 16)
	p, err = h.PreparePartitionGraphOperations(n.requestID(), []graphstate.Operation{{Kind: graphstate.CreateNode, Owner: 1, Life: 2, Scope: scope}}, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	n.submitPartitionGraph(0, p, reasonInvalid)
	closedStoreHost := n.hosts[0][1]
	if err := closedStoreHost.machine.store.Close(); err != nil {
		t.Fatal(err)
	}
	if p, err := closedStoreHost.PreparePartitionGraphOperations(requestID{1}, nil, 1, nil); !errors.Is(err, raftlog.ErrClosed) || len(p.wire) != 0 {
		t.Fatal("closed captured store", err)
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	if p, err := h.PreparePartitionGraphOperations(requestID{1}, nil, 1, nil); !errors.Is(err, replica.ErrStopped) || len(p.wire) != 0 {
		t.Fatal(err)
	}
}
