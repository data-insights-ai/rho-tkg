package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"math"
	"reflect"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

func partitionCodecRequest(t *testing.T, bits uint8, l materializerLimits) declaredInitCommand {
	t.Helper()
	d := declaredTestDeclaration(t, []graphstore.PartitionOwnership{{Partition: 3, OwnershipEpoch: 2, Group: [16]byte{4}}, {Partition: 8, OwnershipEpoch: 3, Group: [16]byte{8}}})
	owners := make([]graphstore.BucketOwnership, 3*(1<<bits))
	for family := range 3 {
		for bucket := range 1 << bits {
			owners[family*(1<<bits)+bucket] = graphstore.BucketOwnership{Kind: graphstore.RoutingBucketKind(family + 1), Bucket: uint32(bucket), Partition: 3, Epoch: 1}
		}
	}
	routing, err := graphstore.NewPartitionRouting(d, 1, bits, owners, l.catalog)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := types.NewDefaultAxisBinding(types.GraphID(d.Graph()), temporal.AxisID{1}, l.catalog.Temporal)
	if err != nil {
		t.Fatal(err)
	}
	authority, err := idalloc.NewAuthority([16]byte{7}, 2)
	if err != nil {
		t.Fatal(err)
	}
	return declaredInitCommand{ns: namespace{graph: idalloc.GraphID(d.Graph()), partition: 3}, attempt: bootstrapAttemptID{1}, declaration: d, authority: authority, maxBlock: 16, defaultAxis: binding, routing: new(routing), schemas: []graphstate.PropertyDefinition{{Name: "opaque", Owner: graphstate.Node, Type: graphstate.ScalarDescriptor, Cardinality: graphstate.ScalarCardinality}}}
}

func TestPartitionInitializationRoutingCodecUsesAdmittedCatalogPolicy(t *testing.T) {
	l := defaultMaterializerLimits()
	l.catalog.MaxRecordBytes = 1 << 20
	r := partitionCodecRequest(t, 13, l)
	size, err := r.routing.EncodedBytes(l.catalog)
	if err != nil || size <= graphstore.DefaultLimits().MaxRecordBytes {
		t.Fatalf("non-default fixture: %d %v", size, err)
	}
	wire, err := encodeDeclaredInit(r, l)
	if err != nil {
		t.Fatalf("admitted routing command serialized under unrelated defaults: %v", err)
	}
	got, err := decodeDeclaredInit(wire, l)
	if err != nil || got.routing == nil || got.routing.Digest() != r.routing.Digest() {
		t.Fatalf("roundtrip: %v", err)
	}
	cfg, err := r.validate(l)
	if err != nil {
		t.Fatal(err)
	}
	logical, err := encodeDeclaredInitialization(r, cfg, l.changeBytes, l)
	if err != nil {
		t.Fatalf("logical effect serialized under unrelated defaults: %v", err)
	}
	routeWire, err := graphstore.EncodePartitionRouting(*r.routing, r.declaration, l.catalog)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(logical, routeWire) {
		t.Fatal("logical initialization omits complete routing definition")
	}
	if _, err := encodeDeclaredInit(r, defaultMaterializerLimits()); !errors.Is(err, graphstore.ErrResourceLimit) {
		t.Fatalf("tight policy: %v", err)
	}
}

func TestPublishedProtocolGrantRejectsReservationAndSourceMismatches(t *testing.T) {
	l := defaultMaterializerLimits()
	r := partitionCodecRequest(t, 0, l)
	cfg, err := r.validate(l)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := cfg.digest()
	if err != nil {
		t.Fatal(err)
	}
	allocator, err := idalloc.NewState(r.ns.graph, r.authority, 16)
	if err != nil {
		t.Fatal(err)
	}
	_, block, _, err := idalloc.Reserve(&allocator, idalloc.Request{Graph: r.ns.graph, Authority: r.authority, Sequence: 1, Count: 4})
	if err != nil {
		t.Fatal(err)
	}
	g := protocolGrant{configuration: digest, source: allocationCoordinate{scope: allocationScope{graph: r.ns.graph, partition: 3, group: [16]byte{4}, ownership: 2, topology: r.declaration.TopologyEpoch(), declaration: r.declaration.Digest(), semantic: partitionWriteSemanticContractID()}, index: 17}, grant: idalloc.Grant{Request: idalloc.GrantRequest{Graph: r.ns.graph, Session: idalloc.RecipientSession{ID: [16]byte{1}, Incarnation: [16]byte{2}, Epoch: 1}, Sequence: 1, Count: 4}, Reservation: block}, installed: 2, publication: graphstore.RangePublication{Graph: graphstate.GraphID(r.ns.graph), RangeID: block.First, First: block.First, Last: block.Last, InitialPartition: 8, InitialEpoch: 3, Configuration: digest, SourcePartition: 3, SourceIndex: 17}}
	n := namespace{graph: r.ns.graph, partition: 8}
	wire, err := encodeProtocolGrant(n, g)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeProtocolGrant(wire, n)
	if err != nil || got != g {
		t.Fatalf("valid grant: %+v %v", got, err)
	}
	for _, field := range []string{"graph", "range", "last", "configuration", "source-partition", "source-index", "legacy-agreement"} {
		t.Run(field, func(t *testing.T) {
			bad := g
			switch field {
			case "graph":
				bad.publication.Graph = graphstate.GraphID{9}
			case "range":
				bad.publication.RangeID++
				bad.publication.First++
			case "last":
				bad.publication.Last++
			case "configuration":
				bad.publication.Configuration = [32]byte{9}
			case "source-partition":
				bad.publication.SourcePartition = 8
			case "source-index":
				bad.publication.SourceIndex++
			case "legacy-agreement":
				bad.source.scope.semantic = declaredSemanticContractID()
			}
			if _, err := encodeProtocolGrant(n, bad); !errors.Is(err, errInvalid) {
				t.Fatalf("publication not bound to immutable grant %s: %v", field, err)
			}
		})
	}
}

func newPartitionWriteNetwork(t *testing.T) (*allocationTestNetwork, graphstore.PartitionRouting) {
	t.Helper()
	r := partitionCodecRequest(t, 1, defaultMaterializerLimits())
	return newPartitionWriteNetworkWithRouting(t, r, graphGenesisSchemas()), *r.routing
}
func newPartitionWriteNetworkWithRouting(t *testing.T, r declaredInitCommand, schemas []graphstate.PropertyDefinition) *allocationTestNetwork {
	t.Helper()
	n := &allocationTestNetwork{t: t, d: r.declaration, pending: make(map[[16]byte]allocationProof)}
	for g, partition := range []uint64{3, 8} {
		disk := newDeclaredDiskGroup(t, n.d, partition, partitionWriteSemanticContractID())
		for j := range 3 {
			n.configs[g][j] = disk.configs[j]
			n.hosts[g][j] = &allocationHost{driver: disk.drivers[j], machine: disk.machines[j], session: [16]byte{byte(g + 1), byte(j + 1)}, pending: make(map[allocationReadID]allocationQuery), recipients: make(map[[16]byte]*allocationRecipientHandle)}
		}
	}
	home := n.hosts[0][0]
	binding := graphGenesisBinding(t)
	first, err := home.PreparePartitionInitialization(3, bootstrapAttemptID{1}, schemas, 16, binding, *r.routing, allocationProof{})
	if err != nil {
		t.Fatal(err)
	}
	n.submit(0, first)
	proof := n.read(0, allocationQuery{kind: observeConfiguration})
	second, err := home.PreparePartitionInitialization(8, bootstrapAttemptID{2}, schemas, 16, binding, *r.routing, proof)
	if err != nil {
		t.Fatal(err)
	}
	n.submit(1, second)
	return n
}

func TestPartitionPublicationCoInstallsActualRangesAndLogicalReplayOnSixDiskReplicas(t *testing.T) {
	n, routing := newPartitionWriteNetwork(t)
	old := n.hosts[1][0].driver.Applied()
	before, err := n.hosts[1][0].machine.store.ApplicationView(old)
	if err != nil {
		t.Fatal(err)
	}
	defer before.Close()
	var sessions [2]idalloc.RecipientSession
	for group := range 2 {
		handle := n.activate(group, [16]byte{byte(group + 1)})
		sessions[group] = handle.session
		n.acquire(group, handle, 1, 4)
	}
	for group := range 2 {
		for _, h := range n.hosts[group] {
			view, err := h.machine.store.ApplicationView(h.driver.Applied())
			if err != nil {
				t.Fatal(err)
			}
			base, err := view.RootBounded(t.Context(), 172)
			if err != nil {
				t.Fatal(err)
			}
			q := reader{ctx: t.Context(), view: view, base: base, ns: h.machine.ns, limits: h.machine.limits.allocation}
			cfg, err := h.machine.initialized(&q)
			if err != nil || cfg.routingDigest != routing.Digest() {
				t.Fatalf("ready routing: %v", err)
			}
			digest, err := cfg.digest()
			if err != nil {
				t.Fatal(err)
			}
			for recipient := range 2 {
				if group == 1 && recipient == 0 {
					continue
				}
				first := uint64(1 + recipient*4)
				p, o, work, err := graphstore.LookupPublishedRange(t.Context(), view, graphstore.Namespace{Graph: n.d.Graph(), Partition: h.machine.ns.partition}, first, digest, graphstore.OwnershipBudget{SourceRows: 32, SourceBytes: 1 << 20, OutputBytes: 1 << 20})
				wantPartition := uint64(3)
				wantEpoch := uint64(2)
				if recipient == 1 {
					wantPartition = 8
					wantEpoch = 3
				}
				if err != nil || p.First != first || p.Last != first+3 || p.InitialPartition != wantPartition || p.InitialEpoch != wantEpoch || p.SourcePartition != 3 || o.Partition != wantPartition || o.Epoch != wantEpoch || work.Records != 3 {
					t.Fatalf("actual published range: %+v %+v %+v %v", p, o, work, err)
				}
			}
			root, err := graphstore.DecodeRoot(base.Image)
			if err != nil || root.SemanticEpoch() <= 1 {
				t.Fatalf("routing changes omitted from logical root: epoch%d %v", root.SemanticEpoch(), err)
			}
			if err := view.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
	h := n.hosts[1][0]
	proof := n.read(0, allocationQuery{kind: observeGrant, session: sessions[1], sequence: 1, count: 4})
	install, err := n.hosts[0][0].InstallGrant(n.requestID(), 8, proof)
	if err != nil {
		t.Fatal(err)
	}
	replay := n.submit(1, install)
	if replay.disposition != controlRecovery {
		t.Fatal("exact retry not recovery", replay)
	}
	changes, err := h.machine.store.ApplicationRecord(t.Context(), h.driver.Applied(), false, h.machine.store.ApplicationLimits().MaxChangeBytes)
	if err != nil || len(changes) != 0 {
		t.Fatalf("retry fabricated routing change: %v", err)
	}
	cmd, err := decodeAllocationProtocolCommand(install.wire, h.machine.limits)
	if err != nil {
		t.Fatal(err)
	}
	cmd.id = n.requestID()
	cmd.remote.grant.publication.InitialPartition = 3
	cmd.remote.grant.publication.InitialEpoch = 2
	forged, err := encodeAllocationProtocolCommand(cmd, h.machine.limits)
	if err != nil {
		t.Fatal(err)
	}
	entry := replica.Entry{Index: h.driver.Applied() + 1, Generation: h.machine.store.ApplicationGeneration(), Term: 2, Data: forged}
	b, err := h.machine.Stage(entry, h.machine.store.ApplicationBudget())
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := decodeDeclaredOutcome(b.Outcome, h.machine.ns)
	if err != nil || outcome.reason != reasonMismatch || len(b.Changes) != 0 || len(b.Writes) != 1 {
		t.Fatalf("altered provenance exact retry: %+v rows%d %v", outcome, len(b.Writes), err)
	}
	cfg := proof.observation.grant.configuration
	if p, o, _, err := graphstore.LookupPublishedRange(t.Context(), before, graphstore.Namespace{Graph: n.d.Graph(), Partition: 8}, 5, cfg, graphstore.OwnershipBudget{SourceRows: 32, SourceBytes: 1 << 20, OutputBytes: 1 << 20}); !errors.Is(err, graphstore.ErrRoutingUnknown) || p != (graphstore.RangePublication{}) || o != (graphstore.RangeOwnership{}) {
		t.Fatalf("old view acquired future publication: %+v %+v %v", p, o, err)
	}
}

func TestPartitionCheckedReaderNeverClaimsUnknownOrRemoteEntityAbsence(t *testing.T) {
	n, _ := newPartitionWriteNetwork(t)
	remote := n.activate(1, [16]byte{2})
	n.acquire(1, remote, 1, 4)
	h := n.hosts[0][0]
	view, err := h.machine.store.ApplicationView(h.driver.Applied())
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	proof := n.read(0, allocationQuery{kind: observeConfiguration})
	v, work, err := graphstore.OpenPartitionGraphReadView(t.Context(), view, n.d, graphGenesisBinding(t), proof.observation.configuration, graphstore.Limits{}, graphstore.GraphLimits{}, graphstore.OwnershipBudget{SourceRows: 1024, SourceBytes: 4 << 20, OutputBytes: 4 << 20})
	if err != nil || work.Records < 14 {
		t.Fatalf("checked open: %+v %v", work, err)
	}
	defer v.Close()
	if got, err := v.Entity(t.Context(), 1); !errors.Is(err, graphstore.ErrRemoteParticipant) || got.Found || got.View != (graphstate.ViewID{}) {
		t.Fatalf("remote primary manufactured complete absence: %+v %v", got, err)
	}
	if got, err := v.Entity(t.Context(), 5); !errors.Is(err, graphstore.ErrRoutingUnknown) || got.Found || got.View != (graphstate.ViewID{}) {
		t.Fatalf("gap manufactured complete absence: %+v %v", got, err)
	}
	if _, err := view.Root(); err != nil {
		t.Fatal("read refusal poisoned borrow", err)
	}
}

func TestPartitionUniqueLocalMissCannotClaimAbsenceWithRemoteIdentityOwner(t *testing.T) {
	l := defaultMaterializerLimits()
	r := partitionCodecRequest(t, 0, l)
	owners := []graphstore.BucketOwnership{{Kind: graphstore.IdentityRoutingBucket, Partition: 8, Epoch: 1}, {Kind: graphstore.UniqueRoutingBucket, Partition: 3, Epoch: 1}, {Kind: graphstore.AxisRoutingBucket, Partition: 3, Epoch: 1}}
	routing, err := graphstore.NewPartitionRouting(r.declaration, 1, 0, owners, l.catalog)
	if err != nil {
		t.Fatal(err)
	}
	r.routing = new(routing)
	definition := graphstate.PropertyDefinition{Name: "p", Owner: graphstate.Node, Type: graphstate.ScalarI64, Cardinality: graphstate.ScalarCardinality, Unique: graphstate.UniqueScalar}
	n := newPartitionWriteNetworkWithRouting(t, r, []graphstate.PropertyDefinition{definition})
	h := n.hosts[0][0]
	view, err := h.machine.store.ApplicationView(h.driver.Applied())
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	proof := n.read(0, allocationQuery{kind: observeConfiguration})
	v, _, err := graphstore.OpenPartitionGraphReadView(t.Context(), view, n.d, graphGenesisBinding(t), proof.observation.configuration, l.catalog, l.graph, graphstore.OwnershipBudget{SourceRows: 1024, SourceBytes: 4 << 20, OutputBytes: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	scope, err := temporal.All(graphGenesisBinding(t).Axis())
	if err != nil {
		t.Fatal(err)
	}
	before := v.Work()
	page, err := v.UniqueCandidates(t.Context(), graphstate.UniquePredicate{Definition: definition, Value: graphstate.I64(7), Window: scope}, 0, graphstate.ReadBudget{Rows: 16, Bytes: 1 << 20})
	if !errors.Is(err, graphstore.ErrRemoteParticipant) || page.Complete || page.View != (graphstate.ViewID{}) || len(page.Claims) != 0 || v.Work().Bytes <= before.Bytes {
		t.Fatalf("local dictionary miss manufactured global absence: %+v work%+v->%+v %v", page, before, v.Work(), err)
	}
	if _, err := view.Root(); err != nil {
		t.Fatal("borrow poisoned by remote predicate", err)
	}
}

func TestPartitionPublicationRoundDirectRetainedReadersAndAtomicRefusals(t *testing.T) {
	n, routing := newPartitionWriteNetwork(t)
	handle := n.activate(0, [16]byte{1})
	n.acquire(0, handle, 1, 4)
	h := n.hosts[0][0]
	cfg, err := h.currentConfiguration()
	if err != nil {
		t.Fatal(err)
	}
	configuration, err := cfg.digest()
	if err != nil {
		t.Fatal(err)
	}
	view, err := h.machine.store.ApplicationView(h.driver.Applied())
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	base, err := view.Root()
	if err != nil {
		t.Fatal(err)
	}
	newReader := func() *reader {
		return &reader{ctx: t.Context(), view: view, ns: h.machine.ns, base: base, limits: h.machine.limits.allocation}
	}
	q := newReader()
	actual, err := h.machine.readRouting(q, cfg)
	if err != nil || actual.Digest() != routing.Digest() {
		t.Fatal(err)
	}
	badCfg := cfg
	badCfg.routingDigest = [32]byte{}
	if _, err := h.machine.readRouting(newReader(), badCfg); !errors.Is(err, errCorrupt) {
		t.Fatal(err)
	}
	badCfg = cfg
	badCfg.routingDigest = [32]byte{99}
	if _, err := h.machine.readRouting(newReader(), badCfg); !errors.Is(err, errCorrupt) {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	canceled := newReader()
	canceled.ctx = cancelled
	if _, err := h.machine.readRouting(canceled, cfg); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := readPartitionRoundFloor(canceled, configuration); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if round, err := readPartitionRoundFloor(newReader(), configuration); err != nil || round != 0 {
		t.Fatal(round, err)
	}

	zero, _ := graphstate.NewOutputBudget(0)
	limited := newReader()
	limited.arena = zero
	if err := writePartitionRoundFloor(limited, configuration, 1); !errors.Is(err, errLimit) || len(limited.writes) != 0 {
		t.Fatal("floor refused after allocation/publication", err)
	}
	fitting, _ := graphstate.NewOutputBudget(1 << 20)
	q = newReader()
	q.arena = fitting
	if err := writePartitionRoundFloor(q, configuration, 1); err != nil || len(q.writes) != 1 {
		t.Fatal(err)
	}
	if err := sortPartitionWrites([]raftlog.KV{{Key: []byte{1}}, {Key: []byte{1}}}); !errors.Is(err, errCorrupt) {
		t.Fatal(err)
	}
	proof := n.read(0, allocationQuery{kind: observeGrant, session: handle.session, sequence: 1, count: 4})
	g := proof.observation.grant
	q = newReader()
	a := allocationRecordReader{q: q, scope: h.machine.scope(), config: cfg, declaration: n.d}
	owner, err := a.rangeOwnership(g.publication)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.putPublication(g.publication, owner); err != nil || len(q.writes) != 0 {
		t.Fatal("exact publication retry changed writes", err)
	}
	for _, field := range []string{"graph", "source", "zero-config", "source-index"} {
		p := g.publication
		switch field {
		case "graph":
			p.Graph = graphstate.GraphID{99}
		case "source":
			p.SourcePartition = 8
		case "zero-config":
			p.Configuration = [32]byte{}
		case "source-index":
			p.SourceIndex++
		}
		if err := a.putPublication(p, owner); !errors.Is(err, errCorrupt) || len(q.writes) != 0 {
			t.Fatal(field, err)
		}
	}
	if _, err := a.rangeOwnership(graphstore.RangePublication{Graph: g.publication.Graph, RangeID: 999}); !errors.Is(err, errCorrupt) {
		t.Fatal("missing current ownership", err)
	}
	var nilHost *allocationHost
	if p, err := nilHost.PreparePartitionInitialization(3, bootstrapAttemptID{1}, nil, 16, graphGenesisBinding(t), routing, allocationProof{}); !errors.Is(err, errInvalid) || !reflect.DeepEqual(p, allocationProposal{}) {
		t.Fatal(err)
	}
	if _, err := h.PreparePartitionInitialization(3, bootstrapAttemptID{1}, nil, 16, types.DefaultAxisBinding{}, routing, allocationProof{}); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if _, err := h.PreparePartitionInitialization(3, bootstrapAttemptID{1}, nil, 16, graphGenesisBinding(t), graphstore.PartitionRouting{}, allocationProof{}); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	for _, field := range []string{"checksum", "config", "magic", "max"} {
		wire := encodePartitionRoundFloor(configuration, 0)
		switch field {
		case "checksum":
			wire[len(wire)-1] ^= 1
		case "config":
			wire[4] ^= 1
			wire = seal(wire[:44])
		case "magic":
			wire[0] ^= 1
			wire = seal(wire[:44])
		case "max":
			wire = encodePartitionRoundFloor(configuration, math.MaxUint64)
		}
		index, image, err := h.machine.store.Checkpoint()
		if err != nil {
			t.Fatal(err)
		}
		batch := raftlog.ApplicationBatch{BaseGeneration: h.machine.store.ApplicationGeneration(), BaseIndex: index, BaseImageHash: sha256.Sum256(image), Image: image, Writes: []raftlog.KV{{Key: partitionRoundFloorKey(h.machine.ns), Value: wire}}}
		hard, _, err := h.machine.store.InitialState()
		if err != nil {
			t.Fatal(err)
		}
		next := index + 1
		if err := h.machine.store.Persist(raft.Ready{HardState: &pb.HardState{Term: new(hard.GetTerm()), Vote: new(hard.GetVote()), Commit: new(next)}, Entries: []*pb.Entry{{Index: new(next), Term: new(hard.GetTerm()), Type: pb.EntryNormal.Enum(), Data: []byte("opaque corruption fixture")}}}); err != nil {
			t.Fatal(err)
		}
		if err := h.machine.store.InstallApplication(next, batch); err != nil {
			t.Fatal(err)
		}
		current, err := h.machine.store.ApplicationView(index + 1)
		if err != nil {
			t.Fatal(err)
		}
		probe := newReader()
		probe.view = current
		probe.base, err = current.Root()
		if err != nil {
			t.Fatal(err)
		}
		want := errCorrupt
		if field == "max" {
			want = errLimit
		}
		if _, err := readPartitionRoundFloor(probe, configuration); !errors.Is(err, want) {
			t.Fatal(field, err)
		}
		if err := h.machine.Restore(index+1, image); !errors.Is(err, want) {
			t.Fatal("restore accepted corrupt round", field, err)
		}
		if old, err := readPartitionRoundFloor(newReader(), configuration); err != nil || old != 0 {
			t.Fatal("current floor corruption changed retained floor", old, err)
		}
		if err := current.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := view.Root(); err != nil {
		t.Fatal("private refusal consumed borrow", err)
	}
}

func TestFinalReviewRegressedRangeEpochStaysCorruptThroughMaterializerAndGrantAdmission(t *testing.T) {
	n, routing := newPartitionWriteNetwork(t)
	handle := n.activate(0, [16]byte{1})
	n.acquire(0, handle, 1, 16)
	h := n.hosts[0][0]
	proof := n.read(0, allocationQuery{kind: observeGrant, session: handle.session, sequence: 1, count: 16})
	publication := proof.observation.grant.publication
	if publication.InitialEpoch < 2 {
		t.Fatal("regression fixture needs a nonzero older epoch", publication)
	}
	cfg, err := h.currentConfiguration()
	if err != nil {
		t.Fatal(err)
	}
	all, err := temporal.All(graphGenesisBinding(t).Axis())
	if err != nil {
		t.Fatal(err)
	}
	claims := []freshBinding{{role: entityBinding, id: 1, grant: grantReference{session: handle.session, sequence: 1}}, {role: lifeBinding, owner: 1, id: 2, grant: grantReference{session: handle.session, sequence: 1}}}
	proposal, err := h.PreparePartitionGraphOperations(n.requestID(), []graphstate.Operation{{Kind: graphstate.CreateNode, Owner: 1, Life: 2, Scope: all}}, 23, claims)
	if err != nil {
		t.Fatal(err)
	}
	command, err := decodePartitionGraphCommand(proposal.wire, h.machine.limits)
	if err != nil {
		t.Fatal(err)
	}
	index, image, err := h.machine.store.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	old, err := h.machine.store.ApplicationView(index)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	oldRoot, err := old.Root()
	if err != nil {
		t.Fatal(err)
	}
	expected, err := h.machine.Stage(replica.Entry{Index: index + 1, Generation: oldRoot.Generation, Data: proposal.wire}, h.machine.store.ApplicationBudget())
	if err != nil || len(expected.Writes) == 0 {
		t.Fatal("fitting actual materializer control", err)
	}
	oldReader := &reader{ctx: t.Context(), view: old, ns: h.machine.ns, base: oldRoot, limits: h.machine.limits.allocation}
	original := allocationRecordReader{q: oldReader, scope: h.machine.scope(), config: cfg, declaration: n.d}
	owner, err := original.rangeOwnership(publication)
	if err != nil || owner.Epoch != publication.InitialEpoch {
		t.Fatal(owner, err)
	}
	owner.Epoch = publication.InitialEpoch - 1
	wire, err := graphstore.EncodeRangeOwnership(owner)
	if err != nil {
		t.Fatal(err)
	}
	key := graphstore.RangeOwnershipKey(graphstore.Namespace{Graph: n.d.Graph(), Partition: 3}, publication.RangeID)
	batch := raftlog.ApplicationBatch{BaseGeneration: oldRoot.Generation, BaseIndex: index, BaseImageHash: oldRoot.ImageHash, Image: image, Writes: []raftlog.KV{{Key: key, Value: wire}}}
	hard, _, err := h.machine.store.InitialState()
	if err != nil {
		t.Fatal(err)
	}
	next := index + 1
	if err := h.machine.store.Persist(raft.Ready{HardState: &pb.HardState{Term: new(hard.GetTerm()), Vote: new(hard.GetVote()), Commit: new(next)}, Entries: []*pb.Entry{{Index: new(next), Term: new(hard.GetTerm()), Type: pb.EntryNormal.Enum(), Data: []byte("explicit regressed authority corruption fixture")}}}); err != nil {
		t.Fatal(err)
	}
	if err := h.machine.store.InstallApplication(next, batch); err != nil {
		t.Fatal(err)
	}
	current, err := h.machine.store.ApplicationView(next)
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close()
	base, err := current.Root()
	if err != nil {
		t.Fatal(err)
	}
	q := &reader{ctx: t.Context(), view: current, ns: h.machine.ns, base: base, limits: h.machine.limits.allocation}
	corrupted := allocationRecordReader{q: q, scope: h.machine.scope(), config: cfg, declaration: n.d}
	if got, err := corrupted.rangeOwnership(publication); !errors.Is(err, graphstore.ErrCorrupt) || got != (graphstore.RangeOwnership{}) {
		t.Fatal("grant lookup downgraded corruption", got, err)
	}
	delta := graphstate.Delta{Entities: []graphstate.EntityRecord{{ID: 1, Kind: graphstate.Node, Axis: all.Axis()}}}
	if why, err := corrupted.admitGraphDelta(command.request, delta); why != reasonNone || !errors.Is(err, graphstore.ErrCorrupt) || len(q.writes) != 0 {
		t.Fatal("grant admission downgraded corruption", why, err)
	}
	beforeUsage, err := h.machine.store.ApplicationUsage()
	if err != nil {
		t.Fatal(err)
	}
	refused, err := h.machine.Stage(replica.Entry{Index: next + 1, Generation: base.Generation, Data: proposal.wire}, h.machine.store.ApplicationBudget())
	if !errors.Is(err, graphstore.ErrCorrupt) || !reflect.DeepEqual(refused, raftlog.ApplicationBatch{}) {
		t.Fatal("materializer treated regressed lineage as business rejection", err)
	}
	afterIndex, afterImage, err := h.machine.store.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	afterUsage, err := h.machine.store.ApplicationUsage()
	if err != nil {
		t.Fatal(err)
	}
	if afterIndex != next || !bytes.Equal(afterImage, image) || afterUsage != beforeUsage {
		t.Fatal("corruption refusal published batch/outcome")
	}
	still, err := original.rangeOwnership(publication)
	if err != nil || still.Epoch != publication.InitialEpoch {
		t.Fatal("current corruption changed retained owner", still, err)
	}
	if why, err := original.admitGraphDelta(command.request, delta); why != reasonNone || err != nil {
		t.Fatal("retained grant authority refused", why, err)
	}
	if actual, err := h.machine.readRouting(oldReader, cfg); err != nil || actual.Digest() != routing.Digest() {
		t.Fatal("retained routing changed", err)
	}
}
