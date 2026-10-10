package graphapply

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"testing"
	"unsafe"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

func graphGenesisBinding(t *testing.T) types.DefaultAxisBinding {
	t.Helper()
	b, err := types.NewDefaultAxisBinding(types.GraphID{1}, temporal.AxisID{31}, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func newGraphGenesisNetwork(t *testing.T) *allocationTestNetwork {
	t.Helper()
	d := declaredTestDeclaration(t, []graphstore.PartitionOwnership{{Partition: 3, OwnershipEpoch: 2, Group: [16]byte{4}}, {Partition: 8, OwnershipEpoch: 3, Group: [16]byte{8}}})
	n := &allocationTestNetwork{t: t, d: d, pending: make(map[[16]byte]allocationProof)}
	for g, partition := range []uint64{3, 8} {
		disk := newDeclaredDiskGroup(t, d, partition, graphGenesisSemanticContractID())
		for j := range 3 {
			n.configs[g][j] = disk.configs[j]
			n.hosts[g][j] = &allocationHost{driver: disk.drivers[j], machine: disk.machines[j], session: [16]byte{byte(g + 1), byte(j + 1)}, pending: make(map[allocationReadID]allocationQuery), recipients: make(map[[16]byte]*allocationRecipientHandle)}
		}
	}
	return n
}
func graphGenesisSchemas() []graphstate.PropertyDefinition {
	return []graphstate.PropertyDefinition{{Name: "descriptor", Owner: graphstate.Node, Type: graphstate.ScalarDescriptor, Cardinality: graphstate.ScalarCardinality}, {Name: "descriptor", Owner: graphstate.Relationship, Type: graphstate.ScalarDescriptor, Cardinality: graphstate.ScalarCardinality}}
}
func assertGraphGenesisStored(t *testing.T, h *allocationHost, binding types.DefaultAxisBinding, index uint64) []byte {
	t.Helper()
	view, err := h.machine.store.ApplicationView(index)
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	row, found, err := view.Get(t.Context(), genesisConfigurationKey(h.machine.ns), 4096)
	if err != nil || !found {
		t.Fatal(row, found, err)
	}
	cfg, err := decodeGenesisAllocationConfig(row.Value, h.machine.ns.graph)
	if err != nil || cfg.defaultAxis != binding || cfg.semanticContractID() != graphGenesisSemanticContractID() {
		t.Fatal(cfg, err)
	}
	catalog, work, err := graphstore.OpenPartitionCatalogWithDefaultAxis(t.Context(), view, graphstore.Namespace{Graph: graphstate.GraphID(h.machine.ns.graph), Partition: h.machine.ns.partition}, h.machine.owner, binding, h.machine.limits.catalog, h.machine.limits.graph, graphstore.OwnershipBudget{SourceRows: 512, SourceBytes: 4 << 20, OutputBytes: 4 << 20})
	if err != nil || work.Records != 8 {
		t.Fatal(work, err)
	}
	for _, schema := range graphGenesisSchemas() {
		got, found, err := catalog.Property(t.Context(), schema.Owner, schema.Name)
		if err != nil || !found || got != schema {
			t.Fatal(got, found, err)
		}
	}
	axis, found, err := catalog.Axis(t.Context(), binding.Axis().Descriptor().ID)
	if err != nil || !found || binding.Check(binding.Graph(), axis, temporal.Limits{}) != nil {
		t.Fatal(axis, found, err)
	}
	_, image, err := h.machine.store.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	if err := h.machine.Restore(index, image); err != nil {
		t.Fatal("same-view restore", err)
	}
	return bytes.Clone(row.Value)
}

func TestGraphGenesisTwoDiskPartitionsExactBindingRetryConflictAndReopen(t *testing.T) {
	n := newGraphGenesisNetwork(t)
	home := n.hosts[0][0]
	binding := graphGenesisBinding(t)
	schemas := graphGenesisSchemas()
	first, err := home.PrepareGraphInitialization(3, bootstrapAttemptID{11}, schemas, 16, binding, allocationProof{})
	if err != nil {
		t.Fatal(err)
	}
	original := n.submit(0, first)
	// Partition all three voters for20 logical ticks: the old leader must lose
	// quorum and every voter's leader lease expires. No sleeps or private Raft
	// state mutation can make the election assertion pass.
	for range 20 {
		for _, host := range n.hosts[0] {
			event, err := host.Tick()
			if err != nil {
				t.Fatal(err)
			}
			if len(event.snapshotSends) != 0 || len(event.replies) != 0 {
				t.Fatal("unexpected isolated owner")
			}
		}
	}
	event, err := n.hosts[0][1].Campaign()
	if err != nil {
		t.Fatal(err)
	}
	n.pump(0, event)
	home = n.hosts[0][1]
	query := allocationQuery{kind: observeConfiguration, nonce: [16]byte{200}}
	read, err := home.Read(query)
	if err != nil {
		t.Fatal("intended voter2 is not current-term leader", err)
	}
	replies := n.pump(0, read)
	if len(replies) != 1 || replies[0].err != nil || replies[0].id != read.readID || replies[0].proof.owner != home {
		t.Fatal("new leader HOME proof", replies)
	}
	proof := replies[0].proof
	if _, err := n.hosts[0][0].Read(allocationQuery{kind: observeConfiguration, nonce: [16]byte{201}}); !errors.Is(err, replica.ErrUnavailable) {
		t.Fatal("old leader still serves leader-only ReadIndex", err)
	}

	second, err := home.PrepareGraphInitialization(8, bootstrapAttemptID{12}, schemas, 16, binding, proof)
	if err != nil {
		t.Fatal(err)
	}
	remote := n.submit(1, second)
	var wire []byte
	for g := range 2 {
		for _, h := range n.hosts[g] {
			got := assertGraphGenesisStored(t, h, binding, h.driver.Applied())
			if wire == nil {
				wire = got
			} else if !bytes.Equal(wire, got) {
				t.Fatal("partition/replica configuration disagreement")
			}
		}
	}
	replay := n.submit(0, first)
	if replay.disposition != requestReplay || replay.index != original.index {
		t.Fatal("retry reset genesis", replay, original)
	}
	replay = n.submit(1, second)
	if replay.disposition != requestReplay || replay.index != remote.index {
		t.Fatal("remote retry reset genesis", replay, remote)
	}
	other, err := types.NewDefaultAxisBinding(binding.Graph(), temporal.AxisID{32}, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := home.PrepareGraphInitialization(8, bootstrapAttemptID{13}, schemas, 16, other, proof); !errors.Is(err, idalloc.ErrPayloadMismatch) {
		t.Fatal("HOME proof accepted another default", err)
	}
	conflict, err := home.PrepareGraphInitialization(3, bootstrapAttemptID{11}, schemas, 16, other, allocationProof{})
	if err != nil {
		t.Fatal(err)
	}
	n.submitReason(0, conflict, reasonMismatch)
	conflict, err = home.PrepareGraphInitialization(3, bootstrapAttemptID{14}, schemas, 16, other, allocationProof{})
	if err != nil {
		t.Fatal(err)
	}
	n.submitReason(0, conflict, reasonAlreadyInitialized)
	for g := range 2 {
		for j, h := range n.hosts[g] {
			if got := assertGraphGenesisStored(t, h, binding, h.driver.Applied()); !bytes.Equal(wire, got) {
				t.Fatal("conflict/retry changed designation")
			}
			if err := h.driver.Close(); err != nil {
				t.Fatal(err)
			}
			cfg := n.configs[g][j]
			cfg.Create = false
			s, err := raftlog.Open(cfg)
			if err != nil {
				t.Fatal(err)
			}
			m, err := newGraphGenesisMaterializer(s, h.machine.ns, n.d, h.machine.limits)
			if err != nil {
				t.Fatal("reopen", err)
			}
			if _, err := newDeclaredMaterializer(s, h.machine.ns, n.d, h.machine.limits); !errors.Is(err, errInvalid) {
				t.Fatal("legacy machine accepted new store", err)
			}
			index, _, err := s.Checkpoint()
			if err != nil {
				t.Fatal(err)
			}
			fresh := &allocationHost{machine: m}
			if got := assertGraphGenesisStored(t, fresh, binding, index); !bytes.Equal(got, wire) {
				t.Fatal("reopen designation changed")
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestGraphGenesisCodecSchemaAndAgreementCrossing(t *testing.T) {
	n := newGraphGenesisNetwork(t)
	h := n.hosts[0][0]
	b := graphGenesisBinding(t)
	l := defaultMaterializerLimits()
	proposal, err := h.PrepareGraphInitialization(3, bootstrapAttemptID{11}, graphGenesisSchemas(), 16, b, allocationProof{})
	if err != nil {
		t.Fatal(err)
	}
	r, err := decodeDeclaredInit(proposal.wire, l)
	if err != nil || r.defaultAxis != b || len(r.schemas) != 2 {
		t.Fatal(r, err)
	}
	for i, schema := range r.schemas {
		if schema != graphGenesisSchemas()[i] {
			t.Fatal(schema)
		}
	}
	cfg, err := r.validate(l)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := encodeGenesisAllocationConfig(cfg)
	if err != nil || len(wire) != graphGenesisConfigBytes {
		t.Fatal(len(wire), err)
	}
	round, err := decodeGenesisAllocationConfig(wire, cfg.graph)
	if err != nil || round != cfg {
		t.Fatal(round, err)
	}
	if 2*unsafe.Sizeof(cfg)+64 > genesisCodecMetadataBytes {
		t.Fatal("two fixed configs/codec header exceed metadata", unsafe.Sizeof(cfg))
	}
	t.Logf("config=%d binding=%d observation=%d command=%d descriptor-variable=%d; fixed metadata=%d plus8*wire", unsafe.Sizeof(cfg), unsafe.Sizeof(b), unsafe.Sizeof(genesisObservation{}), unsafe.Sizeof(r), len(b.Axis().Descriptor().Reference)+len(b.Axis().Descriptor().CanonicalUnit), genesisCodecMetadataBytes)
	if graphGenesisSemanticContractID() == declaredSemanticContractID() || graphGenesisSemanticContractID() == SemanticContractID() {
		t.Fatal("agreements alias")
	}
	legacy := r
	legacy.defaultAxis = types.DefaultAxisBinding{}
	if _, err := encodeDeclaredInit(legacy, l); !errors.Is(err, errInvalid) {
		t.Fatal("legacy admitted descriptor schema", err)
	}
	legacy.schemas = nil
	oldWire, err := encodeDeclaredInit(legacy, l)
	if err != nil {
		t.Fatal(err)
	}
	index, _, err := h.machine.store.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	entry := replica.Entry{Index: index + 1, Generation: h.machine.store.ApplicationGeneration(), Term: 2, Data: oldWire}
	if batch, err := h.machine.Stage(entry, h.machine.store.ApplicationBudget()); !errors.Is(err, errCorrupt) || len(batch.Writes)+len(batch.Image)+len(batch.Outcome) != 0 {
		t.Fatal("new machine accepted old wire", batch, err)
	}
	old := newDeclaredTestGroup(t, n.d, 3)
	entry.Index = old.drivers[0].Applied() + 1
	entry.Generation = old.stores[0].ApplicationGeneration()
	entry.Data = proposal.wire
	if batch, err := old.machines[0].Stage(entry, old.stores[0].ApplicationBudget()); !errors.Is(err, errCorrupt) || len(batch.Writes)+len(batch.Image)+len(batch.Outcome) != 0 {
		t.Fatal("old machine accepted new wire", batch, err)
	}
	if _, err := newGraphGenesisMaterializer(old.stores[0], old.ns, n.d, l); !errors.Is(err, errInvalid) {
		t.Fatal("new machine promoted old store", err)
	}
	if _, err := h.PrepareInitialization(3, bootstrapAttemptID{15}, nil, 16, allocationProof{}); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	oldHost := &allocationHost{machine: old.machines[0], driver: old.drivers[0]}
	if _, err := oldHost.PrepareGraphInitialization(3, bootstrapAttemptID{15}, nil, 16, b, allocationProof{}); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	for _, field := range []int{124, 125, 127, 131} {
		mutant := bytes.Clone(wire)
		mutant[field] ^= 1
		sum := sha256.Sum256(mutant[:len(mutant)-32])
		copy(mutant[len(mutant)-32:], sum[:])
		if _, err := decodeGenesisAllocationConfig(mutant, cfg.graph); !errors.Is(err, errCorrupt) {
			t.Fatal("rechecksummed descriptor mutation accepted", field, err)
		}
	}
}

func TestGraphGenesisNilProofGraphAndBudgetRefusals(t *testing.T) {
	n := newGraphGenesisNetwork(t)
	h := n.hosts[0][0]
	binding := graphGenesisBinding(t)
	var absent *allocationHost
	if _, err := absent.PrepareGraphInitialization(3, bootstrapAttemptID{1}, nil, 16, binding, allocationProof{}); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if _, err := h.PrepareGraphInitialization(3, bootstrapAttemptID{1}, nil, 16, types.DefaultAxisBinding{}, allocationProof{}); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	wrong, err := types.NewDefaultAxisBinding(types.GraphID{99}, binding.Axis().Descriptor().ID, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.PrepareGraphInitialization(3, bootstrapAttemptID{1}, nil, 16, wrong, allocationProof{}); !errors.Is(err, types.ErrDefaultAxisMismatch) {
		t.Fatal(err)
	}
	if _, err := h.PrepareGraphInitialization(8, bootstrapAttemptID{1}, nil, 16, binding, allocationProof{}); !errors.Is(err, errInvalid) {
		t.Fatal("remote no completed HOME proof", err)
	}
	p, err := h.PrepareGraphInitialization(3, bootstrapAttemptID{1}, graphGenesisSchemas(), 16, binding, allocationProof{})
	if err != nil {
		t.Fatal(err)
	}
	s := h.machine.store
	index, image, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	before, err := s.ApplicationUsage()
	if err != nil {
		t.Fatal(err)
	}
	entry := replica.Entry{Index: index + 1, Generation: s.ApplicationGeneration(), Term: 2, Data: p.wire}
	for _, dimension := range []string{"source", "output", "install"} {
		t.Run(dimension, func(t *testing.T) {
			m := *h.machine
			budget := s.ApplicationBudget()
			switch dimension {
			case "source":
				m.limits.sourceRows = 1
			case "output":
				m.limits.outputBytes = 1024
			case "install":
				budget.Writes = 1
			}
			batch, err := m.Stage(entry, budget)
			if !errors.Is(err, errLimit) && !errors.Is(err, graphstore.ErrResourceLimit) {
				t.Fatal(err)
			}
			if len(batch.Writes)+len(batch.Image)+len(batch.Changes)+len(batch.Outcome) != 0 {
				t.Fatal("partial genesis", batch)
			}
			at, got, err := s.Checkpoint()
			if err != nil || at != index || !bytes.Equal(got, image) {
				t.Fatal("refusal changed root", err)
			}
			after, err := s.ApplicationUsage()
			if err != nil || after != before {
				t.Fatal("refusal changed ledger", after, err)
			}
		})
	}
	if batch, err := h.machine.Stage(entry, s.ApplicationBudget()); err != nil || len(batch.Writes) < 10 {
		t.Fatal("relief", len(batch.Writes), err)
	}
}

func TestGraphGenesisSnapshotRetainedBindingTailAndReopen(t *testing.T) {
	testDeclaredInitSnapshotRetainedControlHistoryTailAndReopen(t, graphGenesisSemanticContractID(), graphGenesisBinding(t))
}
func assertGraphGenesisInitializationCDC(t *testing.T, envelope []byte, r declaredInitCommand, binding types.DefaultAxisBinding) {
	t.Helper()
	if len(envelope) < 97+156+defaultAxisDescriptorBytes+4+32 || !bytes.Equal(envelope[:4], []byte{'G', 'C', 'E', 1}) || !bytes.Equal(envelope[5:21], r.attempt[:]) {
		t.Fatal("new genesis CDC framing")
	}
	logical := envelope[65 : len(envelope)-32]
	if !bytes.Equal(logical[:4], []byte{'G', 'C', 'D', 3}) || !bytes.Equal(logical[4:20], r.ns.graph[:]) || binary.BigEndian.Uint64(logical[20:28]) != r.ns.partition || binary.BigEndian.Uint64(logical[36:44]) != 1 {
		t.Fatal("new logical initialization fields")
	}
	c := graphCursor{b: logical[156 : len(logical)-32]}
	axis := readAxis(&c, defaultMaterializerLimits())
	if err := binding.Check(types.GraphID(r.ns.graph), axis, temporal.Limits{}); err != nil {
		t.Fatal("CDC designation", err)
	}
	count := c.count(16, 8)
	if count != len(r.schemas) {
		t.Fatal(count)
	}
	for _, schema := range r.schemas {
		if got := readSchema(&c, defaultMaterializerLimits()); got != schema {
			t.Fatal("CDC schema", got, schema)
		}
	}
	if c.err != nil || len(c.b) != 0 {
		t.Fatal(c.err, len(c.b))
	}
	for _, wire := range [][]byte{logical, envelope} {
		sum := sha256.Sum256(wire[:len(wire)-32])
		if !bytes.Equal(sum[:], wire[len(wire)-32:]) {
			t.Fatal("CDC checksum")
		}
	}
}

func TestGraphGenesisMissingCrossedMetadataRefusesCurrentButRetainsOldView(t *testing.T) {
	for _, kind := range []string{"missing-config", "old-config", "missing-axis", "crossed-axis", "crossed-view"} {
		t.Run(kind, func(t *testing.T) {
			n := newGraphGenesisNetwork(t)
			h := n.hosts[0][0]
			binding := graphGenesisBinding(t)
			p, err := h.PrepareGraphInitialization(3, bootstrapAttemptID{1}, graphGenesisSchemas(), 16, binding, allocationProof{})
			if err != nil {
				t.Fatal(err)
			}
			staged, err := h.machine.Stage(replica.Entry{Index: h.driver.Applied() + 1, Generation: h.machine.store.ApplicationGeneration(), Term: 2, Data: p.wire}, h.machine.store.ApplicationBudget())
			if err != nil {
				t.Fatal(err)
			}
			var axisRow raftlog.KV
			for _, row := range staged.Writes {
				if len(row.Value) > 4 && bytes.Equal(row.Value[:4], []byte{'G', 'C', 1, 1}) {
					axisRow = row
				}
			}
			if len(axisRow.Key) == 0 {
				t.Fatal("default axis not co-staged")
			}
			installed := n.submit(0, p)
			old, err := h.machine.store.ApplicationView(installed.index)
			if err != nil {
				t.Fatal(err)
			}
			defer old.Close()
			original := assertGraphGenesisStored(t, h, binding, installed.index)
			cfg, err := decodeGenesisAllocationConfig(original, h.machine.ns.graph)
			if err != nil {
				t.Fatal(err)
			}
			row := raftlog.KV{Key: genesisConfigurationKey(h.machine.ns)}
			switch kind {
			case "missing-config":
				row.Deleted = true
			case "old-config":
				cfg.defaultAxis = types.DefaultAxisBinding{}
				row.Value, err = encodeGenesisAllocationConfig(cfg)
			case "missing-axis":
				row.Key = axisRow.Key
				row.Deleted = true
			case "crossed-axis", "crossed-view":
				cfg.defaultAxis, err = types.NewDefaultAxisBinding(binding.Graph(), temporal.AxisID{32}, temporal.Limits{})
				if err == nil {
					row.Value, err = encodeGenesisAllocationConfig(cfg)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			at := corruptDeclaredKV(t, h.machine.store, row)
			_, image, err := h.machine.store.Checkpoint()
			if err != nil {
				t.Fatal(err)
			}
			before, err := h.machine.store.ApplicationUsage()
			if err != nil {
				t.Fatal(err)
			}
			if err := h.machine.Restore(at, image); !errors.Is(err, errCorrupt) && !errors.Is(err, graphstore.ErrCorrupt) {
				t.Fatal("corrupt default restored", err)
			}
			if _, err := h.machine.observeAllocation(allocationQuery{kind: observeConfiguration, nonce: [16]byte{22}}, at); !errors.Is(err, errCorrupt) && !errors.Is(err, graphstore.ErrCorrupt) {
				t.Fatal("corrupt ready read", err)
			}
			batch, err := h.machine.Stage(replica.Entry{Index: at + 1, Generation: h.machine.store.ApplicationGeneration(), Term: 2, Data: p.wire}, h.machine.store.ApplicationBudget())
			if !errors.Is(err, errCorrupt) && !errors.Is(err, graphstore.ErrCorrupt) || len(batch.Writes)+len(batch.Image)+len(batch.Changes)+len(batch.Outcome) != 0 {
				t.Fatal("replay bypassed default validation", batch, err)
			}
			after, err := h.machine.store.ApplicationUsage()
			if err != nil || after != before {
				t.Fatal("corruption refusal changed ledger", after, err)
			}
			if got := assertGraphGenesisStored(t, h, binding, installed.index); !bytes.Equal(got, original) {
				t.Fatal("retained designation aliased corrupt current")
			}
			if kind == "crossed-view" {
				local := graphstore.Namespace{Graph: n.d.Graph(), Partition: 3}
				catalog, work, err := graphstore.OpenPartitionCatalogWithDefaultAxis(t.Context(), old, local, 2, cfg.defaultAxis, graphstore.Limits{}, graphstore.GraphLimits{}, graphstore.OwnershipBudget{SourceRows: 512, SourceBytes: 4 << 20, OutputBytes: 4 << 20})
				if catalog != nil || !errors.Is(err, graphstore.ErrCorrupt) || work.Records != 8 {
					t.Fatal("current config paired with retained old axis", catalog, work, err)
				}
			}
		})
	}
}

func TestGraphGenesisIndependentConfigGolden(t *testing.T) {
	binding := graphGenesisBinding(t)
	cfg := genesisAllocationConfig{graph: idalloc.GraphID{1}, topology: 9, declaration: [32]byte{2}, home: 3, maxBlock: 16, schemas: [32]byte{4}, defaultAxis: binding}
	const golden = "4741430201000000000000000000000000000000000000000000000902000000000000000000000000000000000000000000000000000000000000000000000000000003000000000000001004000000000000000000000000000000000000000000000000000000000000001f00000000000000000000000000000002000100000028706f7369782d756e69782d65706f63683a313937302d30312d30315430303a30303a30305a3a76310000000b6d696c6c697365636f6e6461f4bba63b13696094d425225cdfc69eccc70e6aba6d53122b41278ce2f2941e"
	wire, err := encodeGenesisAllocationConfig(cfg)
	if err != nil || len(wire) != graphGenesisConfigBytes || cap(wire) != len(wire) || hex.EncodeToString(wire) != golden {
		t.Fatal("independent GAC2", len(wire), cap(wire), err)
	}
	digest, err := cfg.digest()
	if err != nil || hex.EncodeToString(digest[:]) != "49e8b559d7a9dc87f758220cf6ab5d1ba141348da92452e784ca9cdd382a5096" {
		t.Fatal("independent config digest", digest, err)
	}
}

func TestGraphGenesisTightDescriptorPolicyRefusesOperationallyBeforeEffects(t *testing.T) {
	n := newGraphGenesisNetwork(t)
	h := n.hosts[0][0]
	binding := graphGenesisBinding(t)
	p, err := h.PrepareGraphInitialization(3, bootstrapAttemptID{1}, graphGenesisSchemas(), 16, binding, allocationProof{})
	if err != nil {
		t.Fatal(err)
	}
	r, err := decodeDeclaredInit(p.wire, h.machine.limits)
	if err != nil {
		t.Fatal(err)
	}
	narrow := h.machine.limits
	narrow.catalog.Temporal.MaxDescriptorBytes = defaultAxisDescriptorBytes - 1
	if _, err := encodeDeclaredInit(r, narrow); !errors.Is(err, errLimit) || !errors.Is(err, temporal.ErrResourceLimit) {
		t.Fatal("producer treated cap as bad genesis", err)
	}
	if _, err := decodeDeclaredInit(p.wire, narrow); !errors.Is(err, errLimit) || !errors.Is(err, temporal.ErrResourceLimit) {
		t.Fatal("replica treated cap as bad genesis", err)
	}
	m := *h.machine
	m.limits = narrow
	s := m.store
	index, image, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	before, err := s.ApplicationUsage()
	if err != nil {
		t.Fatal(err)
	}
	entry := replica.Entry{Index: index + 1, Generation: s.ApplicationGeneration(), Term: 2, Data: p.wire}
	batch, err := m.Stage(entry, s.ApplicationBudget())
	if !errors.Is(err, errLimit) || len(batch.Writes)+len(batch.Image)+len(batch.Changes)+len(batch.Outcome) != 0 {
		t.Fatal("cap manufactured business outcome", batch, err)
	}
	after, err := s.ApplicationUsage()
	if err != nil || after != before {
		t.Fatal("refusal changed ledger", after, err)
	}
	at, current, err := s.Checkpoint()
	if err != nil || at != index || !bytes.Equal(current, image) {
		t.Fatal("refusal changed root", err)
	}
	if batch, err := h.machine.Stage(entry, s.ApplicationBudget()); err != nil || len(batch.Writes) < 10 {
		t.Fatal("unchanged request after relief", err)
	}
}

func TestGraphGenesisCompletedProofCannotCrossAgreementOrOriginatingHost(t *testing.T) {
	n := newGraphGenesisNetwork(t)
	h := n.hosts[0][0]
	binding := graphGenesisBinding(t)
	p, err := h.PrepareGraphInitialization(3, bootstrapAttemptID{1}, nil, 16, binding, allocationProof{})
	if err != nil {
		t.Fatal(err)
	}
	n.submit(0, p)
	proof := n.read(0, allocationQuery{kind: observeConfiguration})
	for _, kind := range []string{"old-agreement", "wrong-graph", "wrong-host", "wrong-axis"} {
		t.Run(kind, func(t *testing.T) {
			wrong := proof
			supplied := binding
			switch kind {
			case "old-agreement":
				wrong.observation.source.scope.semantic = declaredSemanticContractID()
			case "wrong-graph":
				wrong.observation.source.scope.graph = idalloc.GraphID{99}
			case "wrong-host":
				wrong.owner = n.hosts[0][1]
			case "wrong-axis":
				var err error
				supplied, err = types.NewDefaultAxisBinding(binding.Graph(), temporal.AxisID{32}, temporal.Limits{})
				if err != nil {
					t.Fatal(err)
				}
			}
			proposal, err := h.PrepareGraphInitialization(8, bootstrapAttemptID{2}, nil, 16, supplied, wrong)
			if !errors.Is(err, errInvalid) || len(proposal.wire) != 0 || proposal.target != (allocationScope{}) {
				t.Fatal("crossed proof produced proposal", proposal, err)
			}
		})
	}
}
