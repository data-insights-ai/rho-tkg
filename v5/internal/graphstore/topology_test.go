package graphstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/assertion"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func topologyStore(t *testing.T, fs vfs.FS, p raftlog.ApplicationPolicy) *raftlog.Store {
	t.Helper()
	s, err := raftlog.Open(raftlog.Config{Dir: "topology", FS: fs, Create: true, Application: p})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}
func bootstrapRoot(t *testing.T, s *raftlog.Store) Root {
	t.Helper()
	if err := BootstrapSinglePartition(s, testNamespace(), 3); err != nil {
		t.Fatal(err)
	}
	r, err := openCatalog(t, s, 1, Limits{}).Root()
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func wantTopology() SinglePartitionTopology {
	return SinglePartitionTopology{Graph: testNamespace().Graph, Partition: 7, OwnershipEpoch: 3, TopologyEpoch: 1, SchemaVersion: 1, IndexVersion: 0}
}
func assertTopology(t *testing.T, r Root) {
	t.Helper()
	got, err := r.SinglePartition()
	if err != nil || got != wantTopology() {
		t.Fatalf("topology=%+v err=%v", got, err)
	}
}
func TestBootstrapSinglePartitionDurableExactRoot(t *testing.T) {
	fs := vfs.NewMem()
	s := topologyStore(t, fs, raftlog.DefaultApplicationPolicy(19))
	r := bootstrapRoot(t, s)
	assertTopology(t, r)
	if r.Namespace() != testNamespace() || r.OwnershipEpoch() != 3 || r.SemanticEpoch() != 0 || r.NextPhysicalID() != 1 || r.EffectDigest() == ([32]byte{}) {
		t.Fatal(r)
	}
	index, image, err := s.Checkpoint()
	if err != nil || index != 1 || len(image) != singlePartitionRootBytes || !bytes.Equal(image[:4], []byte{'G', 'R', 2, 2}) {
		t.Fatal(index, len(image), err)
	}
	hard, conf, err := s.InitialState()
	if err != nil || hard.GetCommit() != 1 || len(conf.GetVoters()) != 1 || conf.GetVoters()[0] != 19 {
		t.Fatal(hard, conf, err)
	}
	usage, err := s.ApplicationUsage()
	if err != nil || usage.RetainedBytes != uint64(singlePartitionRootBytes+3*(9+36)) || usage.RetainedRecords != 3 || usage.CheckpointBytes != singlePartitionRootBytes || usage.SnapshotBytes != singlePartitionRootBytes || usage.ViewBytes != singlePartitionRootBytes {
		t.Fatal(usage, err)
	}
	if err := s.ScrubApplication(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := raftlog.Open(raftlog.Config{Dir: "topology", FS: fs, Application: raftlog.DefaultApplicationPolicy(19)})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got, err := openCatalog(t, reopened, 1, Limits{}).Root()
	if err != nil || got != r {
		t.Fatal(got, err)
	}
	assertTopology(t, got)
	_, reopenedImage, err := reopened.Checkpoint()
	if err != nil || !bytes.Equal(image, reopenedImage) {
		t.Fatal(err)
	}
	if err := BootstrapSinglePartition(reopened, testNamespace(), 3); !errors.Is(err, ErrInvalid) || !errors.Is(err, raftlog.ErrInvalid) {
		t.Fatal(err)
	}
}
func TestBootstrapSinglePartitionRefusalsAreAtomic(t *testing.T) {
	if err := BootstrapSinglePartition(nil, testNamespace(), 3); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		n      Namespace
		owner  uint64
		policy raftlog.ApplicationPolicy
		want   error
	}{
		{"zero graph", Namespace{Partition: 7}, 3, raftlog.DefaultApplicationPolicy(1), ErrNamespace},
		{"zero partition", Namespace{Graph: testNamespace().Graph}, 3, raftlog.DefaultApplicationPolicy(1), ErrNamespace},
		{"zero owner", testNamespace(), 0, raftlog.DefaultApplicationPolicy(1), ErrInvalid},
		{"disabled application", testNamespace(), 3, raftlog.ApplicationPolicy{}, ErrTopologyUnsupported},
		{"image budget", testNamespace(), 3, func() raftlog.ApplicationPolicy {
			p := raftlog.DefaultApplicationPolicy(1)
			p.MaxImageBytes = singlePartitionRootBytes - 1
			return p
		}(), ErrResourceLimit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := topologyStore(t, vfs.NewMem(), tc.policy)
			before, image, err := s.Checkpoint()
			if err != nil {
				t.Fatal(err)
			}
			usage, _ := s.ApplicationUsage()
			if err := BootstrapSinglePartition(s, tc.n, tc.owner); !errors.Is(err, tc.want) {
				t.Fatal(err)
			}
			after, next, err := s.Checkpoint()
			if err != nil || before != after || !bytes.Equal(image, next) {
				t.Fatal("refusal changed checkpoint", err)
			}
			current, _ := s.ApplicationUsage()
			if current != usage {
				t.Fatal("refusal changed ledgers", usage, current)
			}
			if _, err := s.ApplicationView(1); !errors.Is(err, raftlog.ErrInvalid) {
				t.Fatal(err)
			}
			if errors.Is(tc.want, ErrResourceLimit) {
				if err := BootstrapSinglePartition(s, tc.n, tc.owner); !errors.Is(err, raftlog.ErrLimit) {
					t.Fatal(err)
				}
			}
		})
	}
	s := topologyStore(t, vfs.NewMem(), raftlog.DefaultApplicationPolicy(1))
	r := bootstrapRoot(t, s)
	_, before, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	if err := BootstrapSinglePartition(s, Namespace{Graph: graphstate.GraphID{9}, Partition: 8}, 4); !errors.Is(err, ErrInvalid) || !errors.Is(err, raftlog.ErrInvalid) {
		t.Fatal(err)
	}
	_, after, err := s.Checkpoint()
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal(err)
	}
	assertTopology(t, r)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := BootstrapSinglePartition(s, testNamespace(), 3); !errors.Is(err, raftlog.ErrClosed) {
		t.Fatal(err)
	}
}
func TestBootstrapRefusesRecoveredEmptyAndPopulatedPrimitive(t *testing.T) {
	fs := vfs.NewMem()
	s := topologyStore(t, fs, raftlog.DefaultApplicationPolicy(1))
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	recovered, err := raftlog.Open(raftlog.Config{Dir: "topology", FS: fs, Application: raftlog.DefaultApplicationPolicy(1)})
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	if err := BootstrapSinglePartition(recovered, testNamespace(), 3); !errors.Is(err, ErrInvalid) || !errors.Is(err, raftlog.ErrInvalid) {
		t.Fatal(err)
	}
	if index, wire, err := recovered.Checkpoint(); err != nil || index != 0 || len(wire) != 0 {
		t.Fatal(index, wire, err)
	}
	db, r := newStore(t, vfs.NewMem())
	c := openCatalog(t, db, 1, Limits{})
	st := stage(t, c)
	a := testAxis(t, 2, temporal.ProfileIntegerZ)
	if err := st.Entity(t.Context(), refEntity(11), graphstate.EntityRecord{ID: 11, Kind: graphstate.Node, Axis: a}); err != nil {
		t.Fatal(err)
	}
	r, index := commitStage(t, db, r, st)
	_, before, err := db.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	usage, err := db.ApplicationUsage()
	if err != nil {
		t.Fatal(err)
	}
	if err := BootstrapSinglePartition(db, testNamespace(), 3); !errors.Is(err, ErrInvalid) || !errors.Is(err, raftlog.ErrInvalid) {
		t.Fatal(err)
	}
	afterIndex, after, err := db.Checkpoint()
	if err != nil || afterIndex != index || !bytes.Equal(before, after) {
		t.Fatal(err)
	}
	afterUsage, err := db.ApplicationUsage()
	if err != nil || usage != afterUsage {
		t.Fatal(err)
	}
	if _, err := r.SinglePartition(); !errors.Is(err, ErrTopologyUnsupported) {
		t.Fatal(err)
	}
	got, found, err := openCatalog(t, db, index, Limits{}).Entity(t.Context(), refEntity(11))
	if err != nil || !found || got.ID != 11 {
		t.Fatal(got, found, err)
	}
}
func TestSinglePartitionRootMethodsPreserveDeclaration(t *testing.T) {
	s := topologyStore(t, vfs.NewMem(), raftlog.DefaultApplicationPolicy(1))
	r := bootstrapRoot(t, s)
	before := r
	reserved, first, err := r.ReservePhysical(9)
	if err != nil || first != 1 || reserved.NextPhysicalID() != 10 || r != before {
		t.Fatal(reserved, first, err)
	}
	assertTopology(t, reserved)
	advanced, err := reserved.AdvanceEffects([32]byte{42})
	if err != nil || advanced.SemanticEpoch() != 1 || advanced.EffectDigest() != ([32]byte{42}) || advanced.NextPhysicalID() != 10 {
		t.Fatal(advanced, err)
	}
	assertTopology(t, advanced)
	wire, err := EncodeRoot(advanced)
	if err != nil || len(wire) != singlePartitionRootBytes || cap(wire) != len(wire) {
		t.Fatal(err)
	}
	decoded, err := DecodeRoot(wire)
	if err != nil || decoded != advanced {
		t.Fatal(decoded, err)
	}
	wire[4] ^= 1
	assertTopology(t, decoded)
	got, err := decoded.SinglePartition()
	if err != nil {
		t.Fatal(err)
	}
	got.Graph[0] = 9
	got.TopologyEpoch = 99
	if got == wantTopology() {
		t.Fatal("copy did not change")
	}
	assertTopology(t, decoded)
	if _, err := (Root{}).SinglePartition(); !errors.Is(err, ErrNamespace) {
		t.Fatal(err)
	}
	malformed := r
	malformed.owner = 0
	if _, err := malformed.SinglePartition(); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	// Known physical index versions preserve declaration metadata by value;
	// a root declaration alone does not prove that index pages exist.
	for _, version := range []uint64{1, 2, 3} {
		known := r
		known.topology.index = version
		reserved, first, err := known.ReservePhysical(1)
		if err != nil || first != 1 || reserved.next != 2 || known.next != 1 {
			t.Fatal("known format reservation changed original root", version, reserved, err)
		}
		advanced, err := reserved.AdvanceEffects([32]byte{1})
		if err != nil || advanced.epoch != 1 || advanced.topology != known.topology {
			t.Fatal("known format lost declaration", version, advanced, err)
		}
		wire, err := EncodeRoot(advanced)
		if err != nil {
			t.Fatal(version, err)
		}
		decoded, err := DecodeRoot(wire)
		if err != nil || decoded != advanced {
			t.Fatal("known format roundtrip", version, decoded, err)
		}
		topology, err := decoded.SinglePartition()
		want := wantTopology()
		want.IndexVersion = version
		if err != nil || topology != want {
			t.Fatal("known format topology", version, topology, err)
		}
	}
	malformed = r
	malformed.topology.index = 4
	if _, err := EncodeRoot(malformed); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := malformed.AdvanceEffects([32]byte{1}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, _, err := malformed.ReservePhysical(1); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	reserved.next = math.MaxUint64
	if _, _, err := reserved.ReservePhysical(1); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	advanced.epoch = math.MaxUint64
	if _, err := advanced.AdvanceEffects([32]byte{1}); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
}
func TestSinglePartitionRootCodecFailClosed(t *testing.T) {
	s := topologyStore(t, vfs.NewMem(), raftlog.DefaultApplicationPolicy(1))
	wire, err := EncodeRoot(bootstrapRoot(t, s))
	if err != nil {
		t.Fatal(err)
	}
	for i := range len(wire) {
		if _, err := DecodeRoot(wire[:i]); !errors.Is(err, ErrCorrupt) {
			t.Fatal(i, err)
		}
	}
	if _, err := DecodeRoot(append(bytes.Clone(wire), 0)); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
	for _, offset := range []int{0, 1, 2, 3, 4, 20, 28, 36, 44, 52, 84, 92, 100, 108, 139} {
		b := bytes.Clone(wire)
		b[offset] ^= 1
		if _, err := DecodeRoot(b); !errors.Is(err, ErrCorrupt) {
			t.Fatal(offset, err)
		}
	}
	for _, tc := range []struct {
		offset int
		value  uint64
	}{{20, 0}, {28, 0}, {44, 0}, {84, 0}, {84, 2}, {92, 0}, {92, 2}, {100, 4}} {
		b := bytes.Clone(wire)
		binary.BigEndian.PutUint64(b[tc.offset:], tc.value)
		checksum := sha256.Sum256(b[:len(b)-32])
		copy(b[len(b)-32:], checksum[:])
		if _, err := DecodeRoot(b); !errors.Is(err, ErrCorrupt) {
			t.Fatal(tc, err)
		}
	}
	b := bytes.Clone(wire)
	clear(b[4:20])
	checksum := sha256.Sum256(b[:108])
	copy(b[108:], checksum[:])
	if _, err := DecodeRoot(b); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
	primitive, _ := NewRoot(testNamespace(), 3)
	old, err := EncodeRoot(primitive)
	if err != nil || len(old) != 116 || !bytes.Equal(old[:4], []byte{'G', 'R', 1, 1}) {
		t.Fatal(err)
	}
	decoded, err := DecodeRoot(old)
	if err != nil || decoded != primitive {
		t.Fatal(err)
	}
	if _, err := decoded.SinglePartition(); !errors.Is(err, ErrTopologyUnsupported) {
		t.Fatal(err)
	}
}
func TestDeclaredTopologyStagingRefusesEverySeam(t *testing.T) {
	s := topologyStore(t, vfs.NewMem(), raftlog.DefaultApplicationPolicy(1))
	r := bootstrapRoot(t, s)
	c := openCatalog(t, s, 1, Limits{})
	if st, err := c.NewStage(t.Context()); !errors.Is(err, ErrTopologyUnsupported) || st != nil {
		t.Fatal(st, err)
	}
	if c.stages != 0 || c.records != 0 || c.stageBytes != 0 || c.poison != nil {
		t.Fatal("refusal retained staging")
	}
	// Defense in depth at operation: even a private accidentally constructed stage
	// must refuse all current mutation families without invoking their work.
	st := &Stage{c: c, writes: make(map[string]raftlog.KV)}
	calls := []func() error{
		func() error { return st.Axis(t.Context(), temporal.Axis{}) },
		func() error { return st.Property(t.Context(), graphstate.PropertyDefinition{}) },
		func() error { return st.Entity(t.Context(), EntityRef{}, graphstate.EntityRecord{}) },
		func() error { return st.Life(t.Context(), LifeRef{}, graphstate.LifeRecord{}) },
		func() error { return st.Value(t.Context(), ValueRef{}, graphstate.Scalar{}) },
		func() error { _, e := st.InternLocal(t.Context(), ValueRef{}, graphstate.Scalar{}); return e },
		func() error { _, e := StageComponentPatches(t.Context(), st, r, nil, PageLimits{}); return e },
		func() error { _, e := st.Associations(t.Context(), nil, AssociationLimits{}); return e },
	}
	for _, call := range calls {
		if err := call(); !errors.Is(err, ErrTopologyUnsupported) {
			t.Fatal(err)
		}
		if len(st.writes) != 0 || st.bytes != 0 || c.records != 0 || c.stageBytes != 0 || c.poison != nil {
			t.Fatal("operation refusal changed stage")
		}
	}
	if err := st.operation(t.Context(), func(*reader) error { t.Fatal("executed unsupported writer"); return nil }); !errors.Is(err, ErrTopologyUnsupported) {
		t.Fatal(err)
	}
	if err := c.failure(ErrTopologyUnsupported); !errors.Is(err, ErrTopologyUnsupported) || c.poison != nil {
		t.Fatal(err)
	}
	//nolint:staticcheck // SA1012: exercise the nil-context error contract.
	if st, err := c.NewStage(nil); !errors.Is(err, ErrInvalid) || st != nil {
		t.Fatal(st, err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if st, err := c.NewStage(canceled); !errors.Is(err, context.Canceled) || st != nil {
		t.Fatal(st, err)
	}
	if _, err := c.Root(); err != nil {
		t.Fatal(err)
	}
	if err := c.view.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Root(); !errors.Is(err, raftlog.ErrClosed) {
		t.Fatal(err)
	}
	if st, err := c.NewStage(t.Context()); !errors.Is(err, raftlog.ErrClosed) || st != nil {
		t.Fatal(st, err)
	}
}
func TestSinglePartitionHistoricalRootAndReadAccounting(t *testing.T) {
	s := topologyStore(t, vfs.NewMem(), raftlog.DefaultApplicationPolicy(1))
	r := bootstrapRoot(t, s)
	old := openCatalog(t, s, 1, Limits{})
	// Trusted opaque application seam publishes root metadata only. No graph data
	// or indexes are fabricated, and historical catalogs retain their own roots.
	reserved, _, err := r.ReservePhysical(2)
	if err != nil {
		t.Fatal(err)
	}
	next, index := commitRows(t, s, reserved, nil)
	current := openCatalog(t, s, index, Limits{})
	oldRoot, err := old.Root()
	if err != nil || oldRoot != r {
		t.Fatal(oldRoot, err)
	}
	newRoot, err := current.Root()
	if err != nil || newRoot != next || newRoot == oldRoot {
		t.Fatal(newRoot, err)
	}
	assertTopology(t, oldRoot)
	assertTopology(t, newRoot)
	q, err := current.reader(t.Context())
	if err != nil || q.bytes != singlePartitionRootBytes {
		t.Fatal(q, err)
	}
	q.maxBytes = singlePartitionRootBytes
	if err := q.materialize(1); !errors.Is(err, ErrResourceLimit) || q.bytes != singlePartitionRootBytes {
		t.Fatal(err)
	}
	l := DefaultAssociationLimits()
	l.MaxReadBytes = singlePartitionRootBytes - 1
	ref := assertion.Ref{Graph: testNamespace().Graph, ID: 1}
	if _, err := current.Association(t.Context(), ref, l); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	l.MaxReadBytes = singlePartitionRootBytes
	a, err := associationStart(t.Context(), current, testNamespace().Graph, l)
	if err != nil || a.q.bytes != singlePartitionRootBytes {
		t.Fatal(a, err)
	}
	query := AssociationQuery{Graph: testNamespace().Graph, Target: assertion.Target{Kind: assertion.EntityTarget, Entity: 1}}
	queryBytes, err := associationQueryBytes(query, a.limits, current.limits.MaxNameBytes)
	if err != nil {
		t.Fatal(err)
	}
	budget := graphstate.ReadBudget{Rows: 1, Bytes: 2*singlePartitionRootBytes + len(queryBytes) - 1}
	if _, err := current.Associations(t.Context(), query, nil, budget, AssociationLimits{}); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	if current.poison != nil {
		t.Fatal(current.poison)
	}
}
func TestConcurrentBootstrapPublishesOnlyOneDeclaration(t *testing.T) {
	s := topologyStore(t, vfs.NewMem(), raftlog.DefaultApplicationPolicy(1))
	results := make(chan error, 8)
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() { n := testNamespace(); n.Partition += uint64(i); results <- BootstrapSinglePartition(s, n, 3) })
	}
	wg.Wait()
	close(results)
	successes := 0
	for err := range results {
		if err == nil {
			successes++
		} else if !errors.Is(err, ErrInvalid) || !errors.Is(err, raftlog.ErrInvalid) {
			t.Fatal(err)
		}
	}
	if successes != 1 {
		t.Fatal(successes)
	}
	index, image, err := s.Checkpoint()
	if err != nil || index != 1 {
		t.Fatal(index, err)
	}
	r, err := DecodeRoot(image)
	if err != nil {
		t.Fatal(err)
	}
	got, err := r.SinglePartition()
	if err != nil || got.TopologyEpoch != 1 || got.SchemaVersion != 1 || got.IndexVersion != 0 || got.Partition < 7 || got.Partition > 14 {
		t.Fatal(got, err)
	}
	if usage, err := s.ApplicationUsage(); err != nil || usage.RetainedRecords != 3 {
		t.Fatal(usage, err)
	}
}

func TestBootstrapSinglePartitionBindsDurableTransferNamespace(t *testing.T) {
	fs := vfs.NewMem()
	p := raftlog.DefaultApplicationPolicy(1)
	transfer := raftlog.ApplicationTransferConfig{
		Identity: raftlog.ApplicationIdentity{Graph: testNamespace().Graph, Partition: testNamespace().Partition, Group: [16]byte{3}},
		Contract: raftlog.ApplicationContractForPolicy(p), Limits: raftlog.DefaultApplicationTransferLimits(),
	}
	cfg := raftlog.Config{Dir: "bound-topology", FS: fs, Create: true, Application: p, Transfer: transfer}
	s, err := raftlog.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	beforeUsage, err := s.ApplicationUsage()
	if err != nil {
		t.Fatal(err)
	}
	beforeTransfer, err := s.ApplicationTransferUsage()
	if err != nil {
		t.Fatal(err)
	}
	foreignGraph := testNamespace()
	foreignGraph.Graph[0]++
	foreignPartition := testNamespace()
	foreignPartition.Partition++
	for _, n := range []Namespace{foreignGraph, foreignPartition} {
		if err := BootstrapSinglePartition(s, n, 3); !errors.Is(err, ErrNamespace) {
			t.Fatal(n, err)
		}
		if index, image, err := s.Checkpoint(); err != nil || index != 0 || len(image) != 0 {
			t.Fatal(index, image, err)
		}
		if got, err := s.ApplicationUsage(); err != nil || got != beforeUsage {
			t.Fatal(got, err)
		}
		if got, err := s.ApplicationTransferUsage(); err != nil || got != beforeTransfer {
			t.Fatal(got, err)
		}
		if s.ApplicationIdentity() != transfer.Identity {
			t.Fatal("namespace refusal changed durable identity")
		}
	}
	r := bootstrapRoot(t, s)
	assertTopology(t, r)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := BootstrapSinglePartition(s, testNamespace(), 3); !errors.Is(err, raftlog.ErrClosed) {
		t.Fatal(err)
	}
	cfg.Create = false
	reopened, err := raftlog.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got, err := openCatalog(t, reopened, 1, Limits{}).Root()
	if err != nil || got != r || reopened.ApplicationIdentity() != transfer.Identity {
		t.Fatal(got, err)
	}
	assertTopology(t, got)
}

func TestPrimitivePageStageCannotPromoteTopologyDeclaration(t *testing.T) {
	db, primitive := newStore(t, vfs.NewMem())
	c := openCatalog(t, db, 1, Limits{})
	st := stage(t, c)
	axis := testAxis(t, 2, temporal.ProfileIntegerZ)
	if err := st.Entity(t.Context(), refEntity(1), graphstate.EntityRecord{ID: 1, Kind: graphstate.Node, Axis: axis}); err != nil {
		t.Fatal(err)
	}
	before, err := st.Writes()
	if err != nil {
		t.Fatal(err)
	}
	declared := bootstrapRoot(t, topologyStore(t, vfs.NewMem(), raftlog.DefaultApplicationPolicy(1)))
	result, err := StageComponentPatches(t.Context(), st, declared, nil, PageLimits{})
	if !errors.Is(err, ErrInvalid) {
		t.Fatal("primitive stage accepted declared private root", result, err)
	}
	after, err := st.Writes()
	if err != nil || len(before) != len(after) {
		t.Fatal(err)
	}
	for i := range before {
		if !bytes.Equal(before[i].Key, after[i].Key) || !bytes.Equal(before[i].Value, after[i].Value) {
			t.Fatal("root rejection changed prior stage")
		}
	}
	// Neither the primitive catalog nor its immutable root was promoted.
	current, err := c.Root()
	if err != nil || current != primitive {
		t.Fatal(current, err)
	}
	if _, err := current.SinglePartition(); !errors.Is(err, ErrTopologyUnsupported) {
		t.Fatal(err)
	}
	if c.poison != nil {
		t.Fatal("caller root mismatch poisoned catalog", c.poison)
	}
}
