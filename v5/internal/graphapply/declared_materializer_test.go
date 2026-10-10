package graphapply

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

type declaredTestGroup struct {
	t           *testing.T
	declaration graphstore.OwnershipDeclaration
	ns          namespace
	stores      [3]*raftlog.Store
	machines    [3]*declaredMaterializer
	drivers     [3]*replica.Driver
	configs     [3]raftlog.Config
	reads       []replica.ReadResult
}

func newDeclaredTestGroup(t *testing.T, declaration graphstore.OwnershipDeclaration, partition uint64) *declaredTestGroup {
	t.Helper()
	entry, found := declaration.Partition(partition)
	if !found {
		t.Fatal("missing test partition")
	}
	g := &declaredTestGroup{t: t, declaration: declaration, ns: namespace{graph: idalloc.GraphID(declaration.Graph()), partition: partition}}
	for j := range 3 {
		id := uint64(j + 1)
		p := raftlog.DefaultApplicationPolicy(id)
		p.MaxImageBytes, p.MaxInstallWrites = 172, 256
		p.RetainedApplicationRecords, p.RetainedApplicationBytes = 8192, 16<<20
		cfg := raftlog.Config{Dir: "declared", FS: vfs.NewMem(), Create: true, Application: p, Transfer: raftlog.ApplicationTransferConfig{Identity: raftlog.ApplicationIdentity{Graph: [16]byte(g.ns.graph), Partition: partition, Group: entry.Group}, Contract: raftlog.ApplicationContractForPolicy(p), Limits: raftlog.DefaultApplicationTransferLimits()}, Generations: raftlog.ApplicationGenerationLimits{MaxBytes: 32 << 20, MaxRecords: 16384}, PublishedCuts: raftlog.ApplicationPublishedCutLimits{MaxTransferChunks: 4096}, Replication: raftlog.ApplicationReplicationConfig{Voters: [3]uint64{1, 2, 3}}, SemanticContractID: declaredSemanticContractID()}
		s, err := raftlog.Open(cfg)
		if err != nil {
			t.Fatal(err)
		}
		g.stores[j], g.configs[j] = s, cfg
		t.Cleanup(func() {
			if err := s.Close(); err != nil {
				t.Error(err)
			}
		})
		if err := graphstore.BootstrapBoundOwnership(s, s.ApplicationBinding(), declaration, [3]uint64{1, 2, 3}); err != nil {
			t.Fatal(err)
		}
		m, err := newDeclaredMaterializer(s, g.ns, declaration, defaultMaterializerLimits())
		if err != nil {
			t.Fatal(err)
		}
		g.machines[j] = m
		d, err := replica.Open(replica.Config{ID: id, Store: s, ApplicationMachine: m, ApplicationSnapshotSends: replica.ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 4 << 20}})
		if err != nil {
			t.Fatal(err)
		}
		g.drivers[j] = d
		t.Cleanup(func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	out, err := g.drivers[0].Campaign()
	if err != nil {
		t.Fatal(err)
	}
	g.pump(out)
	return g
}

func (g *declaredTestGroup) pump(out replica.Output) {
	g.t.Helper()
	queue := make([]replica.Packet, 0, 128)
	enqueue := func(next replica.Output) {
		g.t.Helper()
		if len(next.SnapshotSends) != 0 || len(queue)+len(next.Packets) > cap(queue) || len(g.reads)+len(next.Reads) > 16 {
			g.t.Fatal("bounded ordinary schedule unexpectedly requires more owners")
		}
		bytes := 64 * (cap(queue) + cap(next.Packets))
		for _, p := range queue {
			bytes += cap(p.Payload)
		}
		for _, p := range next.Packets {
			bytes += cap(p.Payload)
		}
		if bytes > 2<<20 {
			g.t.Fatal("packet representation budget exhausted")
		}
		queue = append(queue, next.Packets...)
		g.reads = append(g.reads, next.Reads...)
	}
	enqueue(out)
	for delivered := 0; len(queue) != 0; delivered++ {
		if delivered >= 1024 {
			g.t.Fatal("bounded delivery schedule exhausted")
		}
		packet := queue[0]
		copy(queue, queue[1:])
		queue[len(queue)-1] = replica.Packet{}
		queue = queue[:len(queue)-1]
		if packet.Snapshot || packet.To < 1 || packet.To > 3 || len(packet.Payload) > 1<<20 {
			g.t.Fatal("unexpected packet scope/type/size")
		}
		next, err := g.drivers[packet.To-1].Step(packet)
		if err != nil {
			g.t.Fatal(err)
		}
		enqueue(next)
	}
}

func (g *declaredTestGroup) submit(wire []byte) (uint64, []outcome) {
	g.t.Helper()
	out, err := g.drivers[0].Propose(wire)
	if err != nil {
		g.t.Fatal(err)
	}
	g.pump(out)
	index := g.drivers[0].Applied()
	var results []outcome
	for j, s := range g.stores {
		if g.drivers[j].Applied() != index {
			g.t.Fatal("follower not applied through exact command")
		}
		b, err := s.ApplicationRecord(g.t.Context(), index, true, graphOutcomeBytes)
		if err != nil {
			g.t.Fatal(err)
		}
		o, err := decodeDeclaredOutcome(b, g.ns)
		if err != nil {
			g.t.Fatal(err)
		}
		if o.kind != initDeclaredPartition || o.hash != sha256.Sum256(wire) {
			g.t.Fatal("outcome not bound to submitted command", o)
		}
		results = append(results, o)
	}
	if results[0] != results[1] || results[0] != results[2] {
		g.t.Fatal("replica outcomes differ", results)
	}
	return index, results
}

func TestDeclaredMaterializerAtomicInitRejectRetryAndReopen(t *testing.T) {
	d := declaredTestDeclaration(t, []graphstore.PartitionOwnership{{Partition: 3, OwnershipEpoch: 2, Group: [16]byte{4}}, {Partition: 8, OwnershipEpoch: 3, Group: [16]byte{4}}})
	g := newDeclaredTestGroup(t, d, 3)
	a, err := idalloc.NewAuthority([16]byte{7}, 5)
	if err != nil {
		t.Fatal(err)
	}
	r := declaredInitCommand{ns: g.ns, attempt: bootstrapAttemptID{11}, declaration: d, authority: a, maxBlock: 16, schemas: []graphstate.PropertyDefinition{{Name: "p", Owner: graphstate.Node, Type: graphstate.ScalarI64, Cardinality: graphstate.ScalarCardinality}}}
	wire, err := encodeDeclaredInit(r, defaultMaterializerLimits())
	if err != nil {
		t.Fatal(err)
	}
	bad := bytes.Clone(wire)
	bad[29] = 10 // distinct bounded bootstrap attempt identity.
	bad[len(bad)-1] ^= 1
	rejectedIndex, rejected := g.submit(bad)
	if rejected[0].reason != reasonInvalid || rejected[0].index != rejectedIndex || rejected[0].identity != ([16]byte{10}) {
		t.Fatal("malformed attempt not explicitly rejected", rejected)
	}
	for _, s := range g.stores {
		usage, err := s.ApplicationUsage()
		if err != nil || usage.RetainedRecords != 3*rejectedIndex {
			t.Fatal("rejection created a KV version", usage, err)
		}
	}
	initializedIndex, initialized := g.submit(wire)
	if initialized[0].reason != reasonNone || initialized[0].disposition != applied || initialized[0].index != initializedIndex {
		t.Fatal(initialized)
	}
	var images [3][]byte
	var changes [3][]byte
	for j, s := range g.stores {
		index, image, err := s.Checkpoint()
		if err != nil || index != initializedIndex {
			t.Fatal(index, err)
		}
		images[j] = image
		root, err := graphstore.DecodeRoot(image)
		if err != nil || root.SemanticEpoch() != 1 || root.NextPhysicalID() <= 5 {
			t.Fatal("co-init root not advanced exactly once", root, err)
		}
		view, err := s.ApplicationView(index)
		if err != nil {
			t.Fatal(err)
		}
		budget := graphstore.OwnershipBudget{SourceRows: 512, SourceBytes: 4 << 20, OutputBytes: 4 << 20}
		catalog, _, err := graphstore.OpenPartitionCatalog(t.Context(), view, graphstore.Namespace{Graph: d.Graph(), Partition: 3}, 2, graphstore.DefaultLimits(), graphstore.DefaultGraphLimits(), budget)
		if err != nil {
			t.Fatal(err)
		}
		property, found, err := catalog.Property(t.Context(), graphstate.Node, "p")
		if err != nil || !found || property != r.schemas[0] {
			t.Fatal(property, found, err)
		}
		if full, err := graphstore.OpenReadView(t.Context(), catalog, graphstore.DefaultGraphLimits()); full != nil || !errors.Is(err, graphstore.ErrTopologyUnsupported) {
			t.Fatal("local readiness fabricated graph-wide coverage", err)
		}
		row, found, err := view.Get(t.Context(), allocatorKey(g.ns), 4096)
		if err != nil || !found {
			t.Fatal("allocator not co-installed", err)
		}
		state, err := idalloc.DecodeCheckpoint(row.Value)
		if err != nil {
			t.Fatal(err)
		}
		allocation, err := idalloc.Inspect(&state)
		if err != nil || allocation.Authority != a || allocation.HighWater != 0 || allocation.MaxBlock != 16 {
			t.Fatal(allocation, err)
		}
		if err := view.Close(); err != nil {
			t.Fatal(err)
		}
		changes[j], err = s.ApplicationRecord(t.Context(), initializedIndex, false, 4096)
		if err != nil || len(changes[j]) == 0 {
			t.Fatal(err)
		}
		assertDeclaredInitCDC(t, changes[j], initialized[j])
		old, err := s.ApplicationRecord(t.Context(), rejectedIndex, true, graphOutcomeBytes)
		if err != nil {
			t.Fatal(err)
		}
		oldOutcome, err := decodeDeclaredOutcome(old, g.ns)
		if err != nil || oldOutcome != rejected[j] {
			t.Fatal("later initialization overwrote old rejection", oldOutcome, err)
		}
	}
	if !bytes.Equal(images[0], images[1]) || !bytes.Equal(images[0], images[2]) || !bytes.Equal(changes[0], changes[1]) || !bytes.Equal(changes[0], changes[2]) {
		t.Fatal("co-init root/typed CDC disagreement")
	}
	_, replay := g.submit(wire)
	if replay[0].disposition != requestReplay || replay[0].index != initializedIndex {
		t.Fatal("retry did not recover original coordinate", replay)
	}
	for j, s := range g.stores {
		newCDC, err := s.ApplicationRecord(t.Context(), g.drivers[j].Applied(), false, 4096)
		if err != nil || len(newCDC) != 0 {
			t.Fatal("retry emitted new logical CDC", newCDC, err)
		}
		_, current, err := s.Checkpoint()
		if err != nil || !bytes.Equal(current, images[j]) {
			t.Fatal("replay changed root", err)
		}
		if err := g.drivers[j].Close(); err != nil {
			t.Fatal(err)
		}
		cfg := g.configs[j]
		cfg.Create = false
		reopened, err := raftlog.Open(cfg)
		if err != nil {
			t.Fatal(err)
		}
		m, err := newDeclaredMaterializer(reopened, g.ns, d, defaultMaterializerLimits())
		if err != nil || m == nil {
			t.Fatal("atomic state did not reopen", err)
		}
		if err := reopened.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func assertDeclaredInitCDC(t *testing.T, envelope []byte, o outcome) {
	t.Helper()
	if len(envelope) < 97 || !bytes.Equal(envelope[:4], []byte{'G', 'C', 'E', 1}) || commandKind(envelope[4]) != initDeclaredPartition || !bytes.Equal(envelope[5:21], o.identity[:]) || !bytes.Equal(envelope[21:53], o.hash[:]) || binary.BigEndian.Uint64(envelope[53:61]) != o.index {
		t.Fatal("request-bound CDC envelope fields differ")
	}
	n := binary.BigEndian.Uint32(envelope[61:65])
	if uint64(n) != uint64(len(envelope)-97) {
		t.Fatal("CDC envelope length differs")
	}
	logical := envelope[65 : len(envelope)-32]
	// Independent Python struct.pack/hashlib vector covers graph/partition,
	// topology/schema, declaration, home/max-block, both digests and definitions.
	const golden = "47434402010000000000000000000000000000000000000000000003000000000000000900000000000000015f80fbd6eaf9bde08c59a8a6cbce183ab1ec9e66494434ba0fce54456e05773000000000000000030000000000000010c0fd64eefc6a01683b738cd42098eb9f38672e9f7327d0c4bb4aad9f9c77bebee3313a137f7433c5a965383f1580a9c065e56508e8d33d1e454226d1772ea65f00000001000000017001040100d39369d3ca0ec39925e85da8b41a15f94aebee4959dc6d804b8fbb41052a303e"
	if hex.EncodeToString(logical) != golden {
		t.Fatalf("logical initialization fields differ: %x", logical)
	}
	checksum := sha256.Sum256(envelope[:len(envelope)-32])
	if !bytes.Equal(checksum[:], envelope[len(envelope)-32:]) {
		t.Fatal("CDC envelope checksum differs")
	}
}

func TestDeclaredMaterializerHiddenMappingCannotBypassSeedProof(t *testing.T) {
	d := declaredTestDeclaration(t, []graphstore.PartitionOwnership{{Partition: 3, OwnershipEpoch: 2, Group: [16]byte{4}}, {Partition: 8, OwnershipEpoch: 3, Group: [16]byte{4}}})
	g := newDeclaredTestGroup(t, d, 3)
	a, err := idalloc.NewAuthority([16]byte{7}, 5)
	if err != nil {
		t.Fatal(err)
	}
	r := declaredInitCommand{ns: g.ns, attempt: bootstrapAttemptID{1}, declaration: d, authority: a, maxBlock: 16}
	wire, err := encodeDeclaredInit(r, defaultMaterializerLimits())
	if err != nil {
		t.Fatal(err)
	}
	s, m := g.stores[0], g.machines[0]
	index, image, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	view, err := s.ApplicationView(index)
	if err != nil {
		t.Fatal(err)
	}
	base, err := view.RootBounded(t.Context(), 172)
	if err := errors.Join(err, view.Close()); err != nil {
		t.Fatal(err)
	}
	// Deliberate opaque-storage corruption fixture, not a graph installation
	// acceptance path: retain a success mapping beneath an unready seed image.
	forged, err := encodeDeclaredOutcome(outcome{ns: g.ns, kind: initDeclaredPartition, identity: [16]byte(r.attempt), hash: sha256.Sum256(wire), index: index + 1, disposition: applied})
	if err != nil {
		t.Fatal(err)
	}
	hard, _, err := s.InitialState()
	if err != nil {
		t.Fatal(err)
	}
	next := index + 1
	hard.Commit = new(next)
	if err := s.Persist(raft.Ready{HardState: hard, Entries: []*pb.Entry{{Index: new(next), Term: new(hard.GetTerm()), Type: pb.EntryNormal.Enum(), Data: wire}}}); err != nil {
		t.Fatal(err)
	}
	b := batchAt(base)
	b.Writes = []raftlog.KV{{Key: declaredAttemptKey(g.ns, [16]byte(r.attempt)), Value: forged}}
	if err := s.InstallApplication(next, b); err != nil {
		t.Fatal(err)
	}
	before, err := s.ApplicationUsage()
	if err != nil {
		t.Fatal(err)
	}
	for _, data := range [][]byte{nil, wire} {
		result, err := m.Stage(replica.Entry{Generation: base.Generation, Index: next + 1, Term: hard.GetTerm(), Data: data}, s.ApplicationBudget())
		if !errors.Is(err, raftlog.ErrInvalid) || len(result.Image)+len(result.Outcome)+len(result.Changes)+len(result.Writes) != 0 {
			t.Fatal("forged mapping/noop bypassed seed proof", result, err)
		}
		after, err := s.ApplicationUsage()
		if err != nil || after != before {
			t.Fatal("refusal changed stored ledgers", before, after, err)
		}
		actualIndex, actualImage, err := s.Checkpoint()
		if err != nil || actualIndex != next || !bytes.Equal(actualImage, image) {
			t.Fatal("refusal changed stored root", actualIndex, err)
		}
	}
}

func TestDeclaredMaterializerOperationalBudgetsNeverPublishPartialInitialization(t *testing.T) {
	d := declaredTestDeclaration(t, []graphstore.PartitionOwnership{{Partition: 3, OwnershipEpoch: 2, Group: [16]byte{4}}, {Partition: 8, OwnershipEpoch: 3, Group: [16]byte{4}}})
	g := newDeclaredTestGroup(t, d, 3)
	a, err := idalloc.NewAuthority([16]byte{7}, 1)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := encodeDeclaredInit(declaredInitCommand{ns: g.ns, attempt: bootstrapAttemptID{1}, declaration: d, authority: a, maxBlock: 16}, defaultMaterializerLimits())
	if err != nil {
		t.Fatal(err)
	}
	s, m := g.stores[0], g.machines[0]
	index, image, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	usage, err := s.ApplicationUsage()
	if err != nil {
		t.Fatal(err)
	}
	entry := replica.Entry{Index: index + 1, Generation: s.ApplicationGeneration(), Term: 2, Data: wire}
	for _, dimension := range []string{"source-rows", "source-bytes", "command-bytes", "command-owned", "stage-rows", "output-bytes", "install-writes"} {
		t.Run(dimension, func(t *testing.T) {
			limited := *m
			budget := s.ApplicationBudget()
			switch dimension {
			case "source-rows":
				limited.limits.sourceRows = 1
			case "source-bytes":
				limited.limits.sourceBytes = 171
			case "command-bytes":
				limited.limits.commandBytes = len(wire) - 1
			case "command-owned":
				limited.limits.commandOwnedBytes = 1
			case "stage-rows":
				limited.limits.stageRows = 1
			case "output-bytes":
				limited.limits.outputBytes = 1
			case "install-writes":
				budget.Writes = 1
			}
			want := errLimit
			if dimension == "output-bytes" {
				want = graphstore.ErrResourceLimit
			}
			b, err := limited.Stage(entry, budget)
			if !errors.Is(err, want) || len(b.Image)+len(b.Writes)+len(b.Changes)+len(b.Outcome) != 0 {
				t.Fatal("local cap manufactured rejection/partial batch", dimension, b, err)
			}
			afterIndex, after, err := s.Checkpoint()
			if err != nil || afterIndex != index || !bytes.Equal(after, image) {
				t.Fatal("refusal changed root", err)
			}
			afterUsage, err := s.ApplicationUsage()
			if err != nil || afterUsage != usage {
				t.Fatal("refusal changed KV/outcome ledger", err)
			}
		})
	}
	// With resource relief the identical request still stages a complete batch;
	// pure Preflight remains non-reserving and installation belongs to Driver.
	b, err := m.Stage(entry, s.ApplicationBudget())
	if err != nil || len(b.Writes) < 7 || len(b.Outcome) != graphOutcomeBytes || len(b.Changes) == 0 {
		t.Fatal("relief did not produce complete initialization", b, err)
	}
}

func TestDeclaredMaterializerNilClosedMalformedAndFreshnessBoundaries(t *testing.T) {
	d := declaredTestDeclaration(t, []graphstore.PartitionOwnership{{Partition: 3, OwnershipEpoch: 2, Group: [16]byte{4}}, {Partition: 8, OwnershipEpoch: 3, Group: [16]byte{4}}})
	g := newDeclaredTestGroup(t, d, 3)
	l := defaultMaterializerLimits()
	if _, err := newDeclaredMaterializer(nil, g.ns, d, l); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	bad := l
	bad.sourceRows = 0
	if _, err := newDeclaredMaterializer(g.stores[0], g.ns, d, bad); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if _, err := newDeclaredMaterializer(g.stores[0], namespace{graph: g.ns.graph, partition: 99}, d, l); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if _, err := newDeclaredMaterializer(g.stores[0], namespace{graph: g.ns.graph, partition: 8}, d, l); !errors.Is(err, graphstore.ErrNamespace) {
		t.Fatal(err)
	}
	var nilMachine *declaredMaterializer
	if nilMachine.SemanticContractID() != declaredSemanticContractID() {
		t.Fatal("nil semantic capability changed")
	}
	if err := nilMachine.Restore(1, nil); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if _, err := nilMachine.Stage(replica.Entry{}, raftlog.ApplicationBudget{}); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	s, m := g.stores[0], g.machines[0]
	index, image, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Restore(0, image); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if err := m.Restore(index, nil); !errors.Is(err, errCorrupt) {
		t.Fatal(err)
	}
	mutant := bytes.Clone(image)
	mutant[0] ^= 1
	if err := m.Restore(index, mutant); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	for _, fixture := range []struct {
		name   string
		entry  replica.Entry
		budget raftlog.ApplicationBudget
		want   error
	}{
		{"empty-budget", replica.Entry{Index: index + 1}, raftlog.ApplicationBudget{}, errInvalid},
		{"oversized-command", replica.Entry{Index: index + 1, Data: make([]byte, (4<<20)+1)}, s.ApplicationBudget(), errLimit},
		{"future-base", replica.Entry{Index: index + 2, Generation: s.ApplicationGeneration()}, s.ApplicationBudget(), raftlog.ErrInvalid},
		{"wrong-generation", replica.Entry{Index: index + 1, Generation: s.ApplicationGeneration() + 1}, s.ApplicationBudget(), errInvalid},
		{"unknown-framing", replica.Entry{Index: index + 1, Generation: s.ApplicationGeneration(), Data: []byte("unknown")}, s.ApplicationBudget(), errCorrupt},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			b, err := m.Stage(fixture.entry, fixture.budget)
			if !errors.Is(err, fixture.want) || len(b.Image)+len(b.Outcome)+len(b.Changes)+len(b.Writes) != 0 {
				t.Fatal(b, err)
			}
		})
	}
	liveBudget := s.ApplicationBudget()
	if err := g.drivers[0].Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := newDeclaredMaterializer(s, g.ns, d, l); !errors.Is(err, raftlog.ErrClosed) {
		t.Fatal(err)
	}
	if err := m.Restore(index, image); !errors.Is(err, raftlog.ErrClosed) {
		t.Fatal(err)
	}
	if _, err := m.Stage(replica.Entry{Index: index + 1}, liveBudget); !errors.Is(err, raftlog.ErrClosed) {
		t.Fatal(err)
	}
}

func corruptDeclaredKV(t *testing.T, s *raftlog.Store, row raftlog.KV) uint64 {
	t.Helper()
	index, _, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	view, err := s.ApplicationView(index)
	if err != nil {
		t.Fatal(err)
	}
	base, err := view.RootBounded(t.Context(), 172)
	if closeErr := view.Close(); errors.Join(err, closeErr) != nil {
		t.Fatal(errors.Join(err, closeErr))
	}
	hard, _, err := s.InitialState()
	if err != nil {
		t.Fatal(err)
	}
	next := index + 1
	hard.Commit = new(next)
	if err := s.Persist(raft.Ready{HardState: hard, Entries: []*pb.Entry{{Index: new(next), Term: new(hard.GetTerm()), Type: pb.EntryNormal.Enum()}}}); err != nil {
		t.Fatal(err)
	}
	b := batchAt(base)
	b.Writes = []raftlog.KV{row}
	if err := s.InstallApplication(next, b); err != nil {
		t.Fatal(err)
	}
	return next
}

func TestDeclaredMaterializerEncounteredGenesisCorruptionNeverReplaysNewCommands(t *testing.T) {
	for _, kind := range []string{"missing-config", "malformed-config", "changed-topology", "source-missing-state", "source-wrong-max-block", "home-second-allocator"} {
		t.Run(kind, func(t *testing.T) {
			n := newAllocationTestNetwork(t)
			h := n.hosts[0][0]
			if kind == "home-second-allocator" {
				h = n.hosts[1][0]
			}
			index := h.driver.Applied()
			configuration, err := h.machine.observeAllocation(allocationQuery{kind: observeConfiguration, nonce: [16]byte{1}}, index)
			if err != nil {
				t.Fatal(err)
			}
			cfg := configuration.config
			row := raftlog.KV{Key: genesisConfigurationKey(h.machine.ns)}
			switch kind {
			case "missing-config":
				row.Deleted = true
			case "malformed-config":
				row.Value = []byte("malformed-genesis")
			case "changed-topology":
				cfg.topology++
				row.Value, err = encodeGenesisAllocationConfig(cfg)
			case "source-missing-state":
				row.Key, row.Deleted = allocatorKey(h.machine.ns), true
			case "source-wrong-max-block", "home-second-allocator":
				a, e := idalloc.NewAuthority([16]byte{7}, 1)
				if e != nil {
					t.Fatal(e)
				}
				state, e := idalloc.NewState(h.machine.ns.graph, a, 32)
				if e != nil {
					t.Fatal(e)
				}
				row.Key = allocatorKey(h.machine.ns)
				row.Value, err = idalloc.MarshalCheckpoint(&state)
			}
			if err != nil {
				t.Fatal(err)
			}
			// Explicit corrupt-storage injection, never a positive graph install.
			next := corruptDeclaredKV(t, h.machine.store, row)
			_, image, err := h.machine.store.Checkpoint()
			if err != nil {
				t.Fatal(err)
			}
			usage, err := h.machine.store.ApplicationUsage()
			if err != nil {
				t.Fatal(err)
			}
			if err := h.machine.Restore(next, image); !errors.Is(err, errCorrupt) {
				t.Fatal("corrupt co-init restored", err)
			}
			if m, err := newDeclaredMaterializer(h.machine.store, h.machine.ns, n.d, h.machine.limits); m != nil || !errors.Is(err, errCorrupt) {
				t.Fatal("factory accepted corrupt co-init", m, err)
			}
			b, err := h.machine.Stage(replica.Entry{Index: next + 1, Generation: h.machine.store.ApplicationGeneration(), Term: 2}, h.machine.store.ApplicationBudget())
			if !errors.Is(err, errCorrupt) || len(b.Image)+len(b.Outcome)+len(b.Changes)+len(b.Writes) != 0 {
				t.Fatal("noop manufactured success over corrupt co-init", b, err)
			}
			after, err := h.machine.store.ApplicationUsage()
			if err != nil || after != usage {
				t.Fatal("corruption refusal mutated retained state", err)
			}
		})
	}
}
