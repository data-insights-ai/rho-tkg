package graphapply

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

// Only test-local fixture/transport ownership lives here. The accepted ordinary
// Init fixture remains unchanged; this scenario needs actual disk reopen and
// separately owned snapshot packets rather than its snapshot-refusing pump.
func newDeclaredDiskGroup(t *testing.T, d graphstore.OwnershipDeclaration, partition uint64, agreement raftlog.ApplicationSemanticContractID) *declaredTestGroup {
	t.Helper()
	entry, found := d.Partition(partition)
	if !found {
		t.Fatal("snapshot fixture lacks home")
	}
	g := &declaredTestGroup{t: t, declaration: d, ns: namespace{graph: idalloc.GraphID(d.Graph()), partition: partition}}
	directory := t.TempDir()
	if agreement == graphGenesisSemanticContractID() || agreement == partitionWriteSemanticContractID() {
		var err error
		directory, err = os.MkdirTemp("", "rho-v5-graph-genesis-disk-")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if t.Failed() {
				t.Log("failure store preserved:", directory)
			} else if err := os.RemoveAll(directory); err != nil {
				t.Error(err)
			}
		})
	}
	for j := range 3 {
		id := uint64(j + 1)
		p := raftlog.DefaultApplicationPolicy(id)
		p.MaxImageBytes, p.MaxInstallWrites = 172, 256
		p.RetainedApplicationRecords, p.RetainedApplicationBytes = 8192, 16<<20
		cfg := raftlog.Config{Dir: filepath.Join(directory, fmt.Sprint(id)), Create: true, Application: p, Transfer: raftlog.ApplicationTransferConfig{Identity: raftlog.ApplicationIdentity{Graph: [16]byte(g.ns.graph), Partition: partition, Group: entry.Group}, Contract: raftlog.ApplicationContractForPolicy(p), Limits: raftlog.DefaultApplicationTransferLimits()}, Generations: raftlog.ApplicationGenerationLimits{MaxBytes: 32 << 20, MaxRecords: 16384}, PublishedCuts: raftlog.ApplicationPublishedCutLimits{MaxTransferChunks: 4096}, Replication: raftlog.ApplicationReplicationConfig{Voters: [3]uint64{1, 2, 3}}, SemanticContractID: agreement}
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
		if err := graphstore.BootstrapBoundOwnership(s, s.ApplicationBinding(), d, [3]uint64{1, 2, 3}); err != nil {
			t.Fatal(err)
		}
		m, err := newDeclaredMaterializerWithAgreement(s, g.ns, d, defaultMaterializerLimits(), agreement)
		if err != nil {
			t.Fatal(err)
		}
		g.machines[j] = m
		driver, err := replica.Open(replica.Config{ID: id, Store: s, ApplicationMachine: m, ApplicationSnapshotSends: replica.ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 4 << 20}})
		if err != nil {
			t.Fatal(err)
		}
		g.drivers[j] = driver
		t.Cleanup(func() {
			if err := driver.Close(); err != nil {
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

type declaredSnapshotTransport struct {
	t            *testing.T
	g            *declaredTestGroup
	dropFollower bool
	queue        []replica.Packet
	packet       replica.Packet
	send         *replica.SnapshotSend
	delivered    int
}

func (n *declaredSnapshotTransport) enqueue(out replica.Output) {
	n.t.Helper()
	snapshotAt := 0
	for _, p := range out.Packets {
		if p.Snapshot {
			if snapshotAt >= len(out.SnapshotSends) || n.send != nil || p.From != 1 || p.To != 3 {
				n.t.Fatal("one exact source-owned snapshot to isolated follower required")
			}
			n.packet, n.send = p, out.SnapshotSends[snapshotAt]
			snapshotAt++
			continue
		}
		if n.dropFollower && (p.From == 3 || p.To == 3) {
			continue
		}
		if p.To < 1 || p.To > 3 || len(n.queue) >= cap(n.queue) {
			n.t.Fatal("bounded packet count/scope exhausted")
		}
		n.queue = append(n.queue, p)
	}
	if snapshotAt != len(out.SnapshotSends) {
		n.t.Fatal("snapshot owner/packet association differs")
	}
	owned := 64*cap(n.queue) + cap(n.packet.Payload)
	for _, p := range n.queue {
		owned += cap(p.Payload)
	}
	if owned > 2<<20 {
		n.t.Fatal("bounded packet bytes exhausted")
	}
}

func (n *declaredSnapshotTransport) drain() {
	n.t.Helper()
	for len(n.queue) != 0 {
		if n.delivered >= 1024 {
			n.t.Fatal("bounded delivery schedule exhausted")
		}
		p := n.queue[0]
		copy(n.queue, n.queue[1:])
		n.queue[len(n.queue)-1] = replica.Packet{}
		n.queue = n.queue[:len(n.queue)-1]
		n.delivered++
		out, err := n.g.drivers[p.To-1].Step(p)
		if err != nil {
			n.t.Fatal(err)
		}
		n.enqueue(out)
	}
}

func (n *declaredSnapshotTransport) submit(wire []byte) (uint64, outcome, []byte) {
	n.t.Helper()
	out, err := n.g.drivers[0].Propose(wire)
	if err != nil {
		n.t.Fatal(err)
	}
	n.enqueue(out)
	n.drain()
	index := n.g.drivers[0].Applied()
	var original []byte
	var decoded outcome
	for j, s := range n.g.stores {
		if n.dropFollower && j == 2 {
			continue
		}
		if n.g.drivers[j].Applied() != index {
			n.t.Fatal("online voter did not apply exact command")
		}
		wireOutcome, err := s.ApplicationRecord(n.t.Context(), index, true, 4096)
		if err != nil {
			n.t.Fatal(err)
		}
		o, err := decodeDeclaredOutcome(wireOutcome, n.g.ns)
		if err != nil || o.hash != sha256.Sum256(wire) {
			n.t.Fatal(o, err)
		}
		if original == nil {
			original, decoded = wireOutcome, o
		} else if !bytes.Equal(original, wireOutcome) {
			n.t.Fatal("online outcome disagreement")
		}
	}
	return index, decoded, original
}

type declaredSnapshotRecords struct{ allocator, configuration []byte }

func assertDeclaredSnapshotView(t *testing.T, view *raftlog.ApplicationView, g *declaredTestGroup, identity [16]byte, expected []byte, original declaredSnapshotRecords) {
	t.Helper()
	b := graphstore.OwnershipBudget{SourceRows: 512, SourceBytes: 4 << 20, OutputBytes: 4 << 20}
	c, _, err := graphstore.OpenPartitionCatalog(t.Context(), view, graphstore.Namespace{Graph: graphstate.GraphID(g.ns.graph), Partition: g.ns.partition}, 2, graphstore.Limits{}, graphstore.GraphLimits{}, b)
	if err != nil {
		t.Fatal(err)
	}
	want := graphstate.PropertyDefinition{Name: "p", Owner: graphstate.Node, Type: graphstate.ScalarI64, Cardinality: graphstate.ScalarCardinality}
	actual, found, err := c.Property(t.Context(), graphstate.Node, "p")
	if err != nil || !found || actual != want {
		t.Fatal(actual, found, err)
	}
	if _, found, err := c.Property(t.Context(), graphstate.Node, "uninstalled"); err != nil || found {
		t.Fatal("rejected schema became visible", found, err)
	}
	row, found, err := view.Get(t.Context(), declaredAttemptKey(g.ns, identity), 4096)
	if err != nil || found != (expected != nil) || found && (!bytes.Equal(row.Value, expected) || row.Deleted) {
		t.Fatal("retained/current exact mapping differs", row, found, err)
	}
	row, found, err = view.Get(t.Context(), allocatorKey(g.ns), 4096)
	if err != nil || !found || !bytes.Equal(row.Value, original.allocator) {
		t.Fatal("exact original home allocator differs", err)
	}
	state, err := idalloc.DecodeCheckpoint(row.Value)
	if err != nil {
		t.Fatal(err)
	}
	info, err := idalloc.Inspect(&state)
	if err != nil || info.HighWater != 0 || info.MaxBlock != 16 || info.Authority.Owner() != ([16]byte{7}) || info.Authority.Epoch() != 5 {
		t.Fatal(info, err)
	}
	row, found, err = view.Get(t.Context(), genesisConfigurationKey(g.ns), 4096)
	if err != nil || !found || !bytes.Equal(row.Value, original.configuration) {
		t.Fatal("exact original configuration differs", err)
	}
	cfg, err := decodeGenesisAllocationConfig(row.Value, g.ns.graph)
	if err != nil || cfg.home != 3 || cfg.maxBlock != 16 || cfg.checkGenesisDeclaration(g.declaration) != nil {
		t.Fatal(cfg, err)
	}
	if cfg.version() == 2 {
		checked, _, err := graphstore.OpenPartitionCatalogWithDefaultAxis(t.Context(), view, graphstore.Namespace{Graph: graphstate.GraphID(g.ns.graph), Partition: g.ns.partition}, 2, cfg.defaultAxis, graphstore.Limits{}, graphstore.GraphLimits{}, b)
		if err != nil || checked == nil {
			t.Fatal("snapshot/reopen default descriptor", err)
		}
	}
}

func TestDeclaredInitSnapshotRetainedControlHistoryTailAndReopen(t *testing.T) {
	testDeclaredInitSnapshotRetainedControlHistoryTailAndReopen(t, declaredSemanticContractID(), types.DefaultAxisBinding{})
}
func testDeclaredInitSnapshotRetainedControlHistoryTailAndReopen(t *testing.T, agreement raftlog.ApplicationSemanticContractID, binding types.DefaultAxisBinding) {
	d := declaredTestDeclaration(t, []graphstore.PartitionOwnership{{Partition: 3, OwnershipEpoch: 2, Group: [16]byte{4}}, {Partition: 8, OwnershipEpoch: 3, Group: [16]byte{4}}})
	g := newDeclaredDiskGroup(t, d, 3, agreement)
	n := &declaredSnapshotTransport{t: t, g: g, queue: make([]replica.Packet, 0, 128)}
	a, err := idalloc.NewAuthority([16]byte{7}, 5)
	if err != nil {
		t.Fatal(err)
	}
	r := declaredInitCommand{ns: g.ns, attempt: bootstrapAttemptID{11}, declaration: d, authority: a, maxBlock: 16, defaultAxis: binding, schemas: []graphstate.PropertyDefinition{{Name: "p", Owner: graphstate.Node, Type: graphstate.ScalarI64, Cardinality: graphstate.ScalarCardinality}}}
	initWire, err := encodeDeclaredInit(r, defaultMaterializerLimits())
	if err != nil {
		t.Fatal(err)
	}
	oldIndex, initOutcome, initMapping := n.submit(initWire)
	if initOutcome.reason != reasonNone || initOutcome.index != oldIndex {
		t.Fatal(initOutcome)
	}
	old, err := g.stores[2].ApplicationView(oldIndex)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	oldRoot, err := old.RootBounded(t.Context(), 172)
	if err != nil || oldRoot.Generation != 1 {
		t.Fatal(oldRoot, err)
	}
	allocator, found, err := old.Get(t.Context(), allocatorKey(g.ns), 4096)
	if err != nil || !found {
		t.Fatal("pre-transfer allocator missing", err)
	}
	configuration, found, err := old.Get(t.Context(), genesisConfigurationKey(g.ns), 4096)
	if err != nil || !found {
		t.Fatal("pre-transfer configuration missing", err)
	}
	originalRecords := declaredSnapshotRecords{allocator.Value, configuration.Value}
	initCDC, err := g.stores[0].ApplicationRecord(t.Context(), oldIndex, false, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if binding == (types.DefaultAxisBinding{}) {
		assertDeclaredInitCDC(t, initCDC, initOutcome)
	} else {
		assertGraphGenesisInitializationCDC(t, initCDC, r, binding)
	}
	n.dropFollower = true
	var changedMapping []byte
	var changedIndex uint64
	for _, id := range []byte{12, 13} {
		attempt := r
		attempt.attempt = bootstrapAttemptID{id}
		attempt.schemas = []graphstate.PropertyDefinition{{Name: "uninstalled", Owner: graphstate.Node, Type: graphstate.ScalarI64, Cardinality: graphstate.ScalarCardinality}}
		wire, err := encodeDeclaredInit(attempt, defaultMaterializerLimits())
		if err != nil {
			t.Fatal(err)
		}
		var outcome outcome
		changedIndex, outcome, changedMapping = n.submit(wire)
		if outcome.reason != reasonAlreadyInitialized || outcome.index != changedIndex {
			t.Fatal(outcome)
		}
	}
	if g.drivers[2].Applied() != oldIndex {
		t.Fatal("isolated follower invented control mapping")
	}
	assertDeclaredSnapshotView(t, old, g, [16]byte{13}, nil, originalRecords)
	if err := g.stores[0].PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	first, err := g.stores[0].FirstIndex()
	if err != nil || first <= oldIndex {
		t.Fatal("publication did not reclaim old log; snapshot path not forced", first, oldIndex, err)
	}
	n.dropFollower = false
	pages := 0
	for range 20 {
		out, err := g.drivers[0].Tick()
		if err != nil {
			t.Fatal(err)
		}
		n.enqueue(out)
		n.drain()
		if n.send != nil {
			break
		}
		offers, err := g.drivers[0].SnapshotSends()
		if err != nil {
			t.Fatal(err)
		}
		for _, send := range offers {
			complete := false
			for !complete && pages < 128 {
				complete, err = send.Build(t.Context(), raftlog.ReadBudget{Rows: 8, Bytes: 64 << 10})
				pages++
				if err != nil {
					t.Fatal(err)
				}
			}
			if !complete {
				t.Fatal("manifest build page cap exhausted")
			}
			if err := g.drivers[0].SealSnapshotSend(send); err != nil {
				t.Fatal(err)
			}
		}
	}
	if n.send == nil {
		t.Fatal("real owned MsgSnap required, not log replay")
	}
	manifest, err := n.send.Manifest()
	if err != nil || manifest.Version != 3 || manifest.Index != changedIndex || manifest.SemanticContractID != agreement || manifest.Identity != g.stores[2].ApplicationBinding().Identity {
		t.Fatal(manifest, err)
	}
	imported, err := g.stores[2].BeginApplicationImport(t.Context(), manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer imported.Abort()
	complete := false
	for !complete && pages < 128 {
		chunk, err := n.send.Next(t.Context(), raftlog.ReadBudget{Rows: 8, Bytes: 64 << 10})
		pages++
		if err != nil || chunk.Version != 3 || chunk.CutID != manifest.CutID || chunk.Visited > 8 || chunk.VisitedBytes > 64<<10 {
			t.Fatal(chunk, err)
		}
		if err := imported.Append(t.Context(), chunk); err != nil {
			t.Fatal(err)
		}
		complete = chunk.Final
	}
	if !complete {
		t.Fatal("stream page cap exhausted")
	}
	if err := imported.Verify(t.Context()); err != nil {
		t.Fatal(err)
	}
	verified, err := imported.Status()
	if err != nil || !verified.Final || !verified.Verified {
		t.Fatal(verified, err)
	}
	prepared, err := imported.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	if g.stores[2].ApplicationGeneration() != 1 || g.drivers[2].Applied() != oldIndex {
		t.Fatal("dormant import activated early")
	}
	before, err := g.stores[2].ApplicationTransferUsage()
	if err != nil || before.Prepared != 1 || !before.Verified {
		t.Fatal(before, err)
	}
	beforeIndex, beforeImage, err := g.stores[2].Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	beforeApplied := g.drivers[2].Applied()
	assertInvalidUnchanged := func() {
		t.Helper()
		index, image, err := g.stores[2].Checkpoint()
		if err != nil || index != beforeIndex || !bytes.Equal(image, beforeImage) || g.drivers[2].Applied() != beforeApplied || g.stores[2].ApplicationGeneration() != 1 {
			t.Fatal("invalid activation changed active checkpoint/driver", index, beforeIndex, err)
		}
		if after, err := g.stores[2].ApplicationTransferUsage(); err != nil || after != before {
			t.Fatal("invalid activation changed dormant ownership", after, before, err)
		}
	}
	if out, err := g.drivers[2].StepApplicationSnapshot(n.packet, nil); !errors.Is(err, raftlog.ErrInvalid) || len(out.Packets)+len(out.SnapshotSends)+len(out.Reads) != 0 {
		t.Fatal(out, err)
	}
	assertInvalidUnchanged()
	bound := g.stores[2].ApplicationBinding()
	raw, err := replica.DecodeApplicationPacket(bound, n.packet, 16<<20)
	if err != nil {
		t.Fatal(err)
	}
	foreign := bound
	foreign.SemanticContractID = SemanticContractID()
	wrong, err := replica.EncodeApplicationPacket(foreign, raw, 16<<20)
	if err != nil {
		t.Fatal(err)
	}
	if out, err := g.drivers[2].StepApplicationSnapshot(wrong, prepared); !errors.Is(err, replica.ErrInvalid) || len(out.Packets)+len(out.SnapshotSends)+len(out.Reads) != 0 {
		t.Fatal(out, err)
	}
	assertInvalidUnchanged()
	assertDeclaredSnapshotView(t, old, g, [16]byte{13}, nil, originalRecords)
	activated, err := g.drivers[2].StepApplicationSnapshot(n.packet, prepared)
	if err != nil {
		t.Fatal(err)
	}
	if g.stores[2].ApplicationGeneration() != 2 || g.drivers[2].Applied() != manifest.Index {
		t.Fatal("actual AS3 generation transition required")
	}
	audit, err := g.stores[2].ApplicationActivationAudit()
	if err != nil || audit.Index != manifest.Index || audit.Term != manifest.Term || audit.CutID != manifest.CutID || audit.ManifestID != verified.ManifestID {
		t.Fatal(audit, verified, err)
	}
	feedback, err := g.drivers[0].ReportSnapshotSend(n.send, true)
	if err != nil {
		t.Fatal(err)
	}
	n.send = nil
	n.packet = replica.Packet{}
	n.enqueue(feedback)
	n.enqueue(activated)
	n.drain()
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
	if err := imported.Abort(); err != nil {
		t.Fatal(err)
	}
	assertDeclaredSnapshotView(t, old, g, [16]byte{13}, nil, originalRecords)
	if held, err := old.RootBounded(t.Context(), 172); err != nil || !reflect.DeepEqual(held, oldRoot) {
		t.Fatal("pinned retired bank/view changed", held, err)
	}
	current, err := g.stores[2].ApplicationView(changedIndex)
	if err != nil {
		t.Fatal(err)
	}
	assertDeclaredSnapshotView(t, current, g, [16]byte{13}, changedMapping, originalRecords)
	if err := current.Close(); err != nil {
		t.Fatal(err)
	}
	tail := r
	tail.attempt = bootstrapAttemptID{14}
	tail.schemas = []graphstate.PropertyDefinition{{Name: "uninstalled", Owner: graphstate.Node, Type: graphstate.ScalarI64, Cardinality: graphstate.ScalarCardinality}}
	tailWire, err := encodeDeclaredInit(tail, defaultMaterializerLimits())
	if err != nil {
		t.Fatal(err)
	}
	tailIndex, tailOutcome, tailMapping := n.submit(tailWire)
	if tailOutcome.reason != reasonAlreadyInitialized || tailOutcome.index != tailIndex || tailIndex <= manifest.Index {
		t.Fatal("post-activation control tail missing", tailOutcome, tailIndex, manifest.Index)
	}
	assertDeclaredSnapshotView(t, old, g, [16]byte{14}, nil, originalRecords)
	_, replay, _ := n.submit(initWire)
	if replay.disposition != requestReplay || replay.index != oldIndex {
		t.Fatal("original Init retry lost source coordinate", replay)
	}
	for j, s := range g.stores {
		cdc, err := s.ApplicationRecord(t.Context(), g.drivers[j].Applied(), false, 4096)
		if err != nil || len(cdc) != 0 {
			t.Fatal("retry emitted logical CDC", cdc, err)
		}
		original, err := s.ApplicationRecord(t.Context(), oldIndex, false, 4096)
		if err != nil || !bytes.Equal(original, initCDC) {
			t.Fatal("original GCD2 lost through activation/tail", err)
		}
		view, err := s.ApplicationView(g.drivers[j].Applied())
		if err != nil {
			t.Fatal(err)
		}
		assertDeclaredSnapshotView(t, view, g, [16]byte{14}, tailMapping, originalRecords)
		if err := view.Close(); err != nil {
			t.Fatal(err)
		}
		if _, image, err := s.Checkpoint(); err != nil || !bytes.Equal(image, manifest.Image) {
			t.Fatal("control/retry changed logical or physical root", j, err)
		}
	}
	assertDeclaredSnapshotView(t, old, g, [16]byte{13}, nil, originalRecords)
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	for j, s := range g.stores {
		usage, err := s.ApplicationTransferUsage()
		app, appErr := s.ApplicationUsage()
		if err != nil || appErr != nil || usage.Exports != 0 || usage.Import || usage.Prepared != 0 || usage.Claims != 0 || usage.PinnedLogicalBytes != 0 || app.Views != 0 {
			t.Fatal("snapshot/view owners retained", j, usage, app, err, appErr)
		}
	}
	if err := g.drivers[2].Close(); err != nil {
		t.Fatal(err)
	}
	cfg := g.configs[2]
	cfg.Create = false
	reopened, err := raftlog.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	m, err := newDeclaredMaterializerWithAgreement(reopened, g.ns, d, defaultMaterializerLimits(), agreement)
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := replica.Open(replica.Config{ID: 3, Store: reopened, ApplicationMachine: m, ApplicationSnapshotSends: replica.ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 4 << 20}})
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	if _, err := old.RootBounded(t.Context(), 172); !errors.Is(err, raftlog.ErrClosed) {
		t.Fatal("stale borrow became live after reopen", err)
	}
	if reopened.ApplicationGeneration() != 2 || restarted.Applied() != g.drivers[0].Applied() {
		t.Fatal("reopen lost activated generation/tail")
	}
	if recovered, err := reopened.ApplicationActivationAudit(); err != nil || recovered != audit {
		t.Fatal("reopen changed activation audit", recovered, err)
	}
	for _, index := range []uint64{oldIndex, changedIndex, tailIndex} {
		view, err := reopened.ApplicationView(index)
		if err != nil {
			t.Fatal(err)
		}
		expected := changedMapping
		if index == oldIndex {
			expected = nil
		}
		assertDeclaredSnapshotView(t, view, g, [16]byte{13}, expected, originalRecords)
		assertDeclaredSnapshotView(t, view, g, [16]byte{11}, initMapping, originalRecords)
		var expectedTail []byte
		if index == tailIndex {
			expectedTail = tailMapping
		}
		assertDeclaredSnapshotView(t, view, g, [16]byte{14}, expectedTail, originalRecords)
		if err := view.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if cdc, err := reopened.ApplicationRecord(t.Context(), oldIndex, false, 4096); err != nil || !bytes.Equal(cdc, initCDC) {
		t.Fatal("reopen changed original Init CDC", err)
	}
	if offers, err := restarted.SnapshotSends(); err != nil || len(offers) != 0 {
		t.Fatal("reopen resurrected source owners", offers, err)
	}
	if usage, err := reopened.ApplicationTransferUsage(); err != nil || usage.Import || usage.Prepared != 0 || usage.Claims != 0 || usage.Exports != 0 || usage.PinnedLogicalBytes != 0 {
		t.Fatal("reopen resurrected receiver owners", usage, err)
	}
}
