package graphstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"reflect"
	"testing"
	"unsafe"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

func ownershipBudget() OwnershipBudget { return OwnershipBudget{64, 4 << 20, 4 << 20} }
func ownershipEntries(count int) []PartitionOwnership {
	entries := make([]PartitionOwnership, count)
	for i := range entries {
		entries[i] = PartitionOwnership{Partition: uint64(i + 1), OwnershipEpoch: uint64(i + 10), Group: [16]byte{9}}
	}
	return entries
}
func ownershipDecl(t *testing.T, count int) OwnershipDeclaration {
	t.Helper()
	d, err := NewOwnershipDeclaration(graphstate.GraphID{1}, 1, ownershipEntries(count), Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return d
}
func ownershipConfig(fs vfs.FS, local Namespace, group [16]byte, voter uint64) raftlog.Config {
	p := raftlog.DefaultApplicationPolicy(voter)
	p.MaxInstallWrites = 256
	p.RetainedApplicationRecords = 8192
	p.RetainedApplicationBytes = 16 << 20
	config := raftlog.Config{Dir: "ownership", FS: fs, Create: true, Application: p, Limits: raftlog.DefaultLimits(), SemanticContractID: sha256.Sum256([]byte("rho-tkg:ownership-declaration-only-fixture:v1")), Replication: raftlog.ApplicationReplicationConfig{Voters: [3]uint64{1, 2, 3}}, Generations: raftlog.ApplicationGenerationLimits{MaxBytes: 32 << 20, MaxRecords: 16384}, PublishedCuts: raftlog.ApplicationPublishedCutLimits{MaxTransferChunks: 1024}, Transfer: raftlog.ApplicationTransferConfig{Identity: raftlog.ApplicationIdentity{Graph: local.Graph, Partition: local.Partition, Group: group}, Contract: raftlog.ApplicationContractForPolicy(p), Limits: raftlog.ApplicationTransferLimits{MaxExports: 2, MaxChunkRows: 64, MaxChunkBytes: 256 << 10, MaxStagedBytes: 16 << 20, MaxStagedRecords: 8192, MaxPinnedLogicalBytes: 64 << 20}}}
	return config
}
func ownershipStore(t *testing.T, fs vfs.FS, local Namespace, d OwnershipDeclaration, group [16]byte) (*raftlog.Store, raftlog.Config, Root) {
	t.Helper()
	r, err := NewOwnershipRoot(local, d)
	if err != nil {
		t.Fatal(err)
	}
	image, err := EncodeRoot(r)
	if err != nil {
		t.Fatal(err)
	}
	config := ownershipConfig(fs, local, group, 1)
	s, err := raftlog.Open(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := s.Initialize([]uint64{1, 2, 3}, image); err != nil {
		t.Fatal(err)
	}
	return s, config, r
}
func ownershipView(t *testing.T, s *raftlog.Store, index uint64) *raftlog.ApplicationView {
	t.Helper()
	v, err := s.ApplicationView(index)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := v.Close(); err != nil {
			t.Error(err)
		}
	})
	return v
}
func ownershipInstallRaw(t *testing.T, s *raftlog.Store, image []byte, writes []raftlog.KV) uint64 {
	t.Helper()
	index, previous, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	next := index + 1
	if err := s.Persist(raft.Ready{HardState: &pb.HardState{Term: new(uint64(2)), Commit: new(next)}, Entries: []*pb.Entry{{Index: new(next), Term: new(uint64(2)), Type: pb.EntryNormal.Enum(), Data: []byte("opaque ownership fixture")}}}); err != nil {
		t.Fatal(err)
	}
	b := raftlog.ApplicationBatch{BaseGeneration: s.ApplicationGeneration(), BaseIndex: index, BaseImageHash: sha256.Sum256(previous), Image: image, Writes: writes}
	if _, err := s.ApplicationLimits().Preflight(b, s.Limits()); err != nil {
		t.Fatal(err)
	}
	if err := s.InstallApplication(next, b); err != nil {
		t.Fatal(err)
	}
	return next
}
func ownershipPublish(t *testing.T, s *raftlog.Store, d OwnershipDeclaration) (OwnershipEffects, uint64) {
	t.Helper()
	index, _, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	v := ownershipView(t, s, index)
	effects, _, err := StageOwnershipDeclaration(t.Context(), v, d, Limits{}, ownershipBudget())
	if err != nil {
		t.Fatal(err)
	}
	image, err := EncodeRoot(effects.Root)
	if err != nil {
		t.Fatal(err)
	}
	return effects, ownershipInstallRaw(t, s, image, effects.Writes)
}

func TestOwnershipDeclarationConstructorAndValueAccessors(t *testing.T) {
	for _, count := range []int{1, 2, 3, 4, 8} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			input := ownershipEntries(count)
			for i, j := 0, len(input)-1; i < j; i, j = i+1, j-1 {
				input[i], input[j] = input[j], input[i]
			}
			d, err := NewOwnershipDeclaration(graphstate.GraphID{1}, 7, input, Limits{})
			if err != nil || d.Graph() != (graphstate.GraphID{1}) || d.TopologyEpoch() != 7 || d.Len() != count || d.Digest() == ([32]byte{}) || cap(d.entries) != count {
				t.Fatal(d, err)
			}
			input[0].Group[0] = 99
			for i := range count {
				entry, found := d.PartitionAt(i)
				want := ownershipEntries(count)[i]
				if !found || entry != want {
					t.Fatal("sort/copy mismatch", entry, want)
				}
				lookup, found := d.Partition(want.Partition)
				if !found || lookup != want {
					t.Fatal("lookup mismatch", lookup)
				}
				entry.Group[0] = 0
				if again, _ := d.PartitionAt(i); again != want {
					t.Fatal("accessor alias")
				}
			}
			for _, index := range []int{-1, count} {
				if got, found := d.PartitionAt(index); found || got != (PartitionOwnership{}) {
					t.Fatal("invalid ordinal", got)
				}
			}
			if got, found := d.Partition(999); found || got != (PartitionOwnership{}) {
				t.Fatal("phantom partition", got)
			}
		})
	}
	zero := OwnershipDeclaration{}
	if zero.Graph() != (graphstate.GraphID{}) || zero.TopologyEpoch() != 0 || zero.Digest() != ([32]byte{}) || zero.Len() != 0 {
		t.Fatal(zero)
	}
	if _, found := zero.Partition(1); found {
		t.Fatal("zero declaration authoritative")
	}
}

func TestOwnershipDeclarationInvalidConstructionAndBounds(t *testing.T) {
	for _, tc := range []struct {
		name    string
		graph   graphstate.GraphID
		epoch   uint64
		entries []PartitionOwnership
	}{
		{"nil entries", graphstate.GraphID{1}, 1, nil},
		{"zero graph", graphstate.GraphID{}, 1, ownershipEntries(1)},
		{"zero epoch", graphstate.GraphID{1}, 0, ownershipEntries(1)},
		{"zero partition", graphstate.GraphID{1}, 1, []PartitionOwnership{{OwnershipEpoch: 1, Group: [16]byte{1}}}},
		{"zero owner", graphstate.GraphID{1}, 1, []PartitionOwnership{{Partition: 1, Group: [16]byte{1}}}},
		{"zero group", graphstate.GraphID{1}, 1, []PartitionOwnership{{Partition: 1, OwnershipEpoch: 1}}},
		{"duplicate partition", graphstate.GraphID{1}, 1, []PartitionOwnership{{1, 2, [16]byte{1}}, {1, 3, [16]byte{2}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NewOwnershipDeclaration(tc.graph, tc.epoch, tc.entries, Limits{})
			if !errors.Is(err, ErrInvalid) || got.Len() != 0 {
				t.Fatal(got, err)
			}
		})
	}
	l := DefaultLimits()
	maxEntries := (l.MaxRecordBytes - ownershipRecordFixedBytes) / ownershipEntryBytes
	if _, err := NewOwnershipDeclaration(graphstate.GraphID{1}, 1, ownershipEntries(maxEntries), l); err != nil {
		t.Fatal("exact record boundary", err)
	}
	input := ownershipEntries(maxEntries + 1)
	if allocations := testing.AllocsPerRun(100, func() {
		if _, err := NewOwnershipDeclaration(graphstate.GraphID{1}, 1, input, l); !errors.Is(err, ErrResourceLimit) {
			t.Fatal(err)
		}
	}); allocations != 0 {
		t.Fatal("oversized constructor copied input", allocations)
	}
	l.MaxReadRows = -1
	if _, err := NewOwnershipDeclaration(graphstate.GraphID{1}, 1, ownershipEntries(1), l); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}

func TestOwnershipRootGenericMethodsPreserveModesAndLegacyGoldens(t *testing.T) {
	d := ownershipDecl(t, 3)
	local := Namespace{Graph: d.Graph(), Partition: 1}
	root, err := NewOwnershipRoot(local, d)
	if err != nil || root.owner != 10 || root.epoch != 0 || root.next != 1 || root.ownershipMode != ownershipPending || root.ownershipDigest != d.Digest() {
		t.Fatal(root, err)
	}
	if _, err := NewOwnershipRoot(Namespace{}, d); !errors.Is(err, ErrNamespace) {
		t.Fatal(err)
	}
	if _, err := NewOwnershipRoot(local, OwnershipDeclaration{}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := NewOwnershipRoot(Namespace{Graph: graphstate.GraphID{2}, Partition: 1}, d); !errors.Is(err, ErrNamespace) {
		t.Fatal(err)
	}
	if _, err := NewOwnershipRoot(Namespace{Graph: d.Graph(), Partition: 99}, d); !errors.Is(err, ErrNamespace) {
		t.Fatal(err)
	}
	for _, mode := range []byte{ownershipPending, ownershipPublished} {
		r := root
		r.ownershipMode = mode
		r, first, err := r.ReservePhysical(2)
		if err != nil || first != 1 || r.next != 3 {
			t.Fatal(r, first, err)
		}
		r, err = r.AdvanceEffects(sha256.Sum256([]byte("logical advanced")))
		if err != nil || r.epoch != 1 || r.ownershipMode != mode || r.ownershipDigest != d.Digest() {
			t.Fatal(r, err)
		}
		wire, err := EncodeRoot(r)
		if err != nil || len(wire) != ownershipRootBytes || cap(wire) != len(wire) {
			t.Fatal(len(wire), err)
		}
		decoded, err := DecodeRoot(wire)
		if err != nil || decoded != r {
			t.Fatal(decoded, err)
		}
		if _, err := decoded.SinglePartition(); !errors.Is(err, ErrTopologyUnsupported) {
			t.Fatal(err)
		}
		if _, err := NewSemanticGuard(decoded); !errors.Is(err, ErrTopologyUnsupported) {
			t.Fatal(err)
		}
	}
	primitive, err := NewRoot(testNamespace(), 3)
	if err != nil {
		t.Fatal(err)
	}
	// Literal vectors independently generated from archived legacy framing
	// and its unchanged empty-root domain; no candidate output supplies golden bytes.
	for _, tc := range []struct {
		topology topologyDeclaration
		golden   string
	}{
		{topologyDeclaration{}, "47520101010000000000000000000000000000000000000000000007000000000000000300000000000000000000000000000001fde7884dcceb4dc81ae52d236e8a7887da67b35f1352f9418a1c86873516c77a9e91faab14cbcc3ab818179800d077f6f1b0d4b62164301ddb6e5a0b58ad7a96"},
		{bootstrapTopology, "47520202010000000000000000000000000000000000000000000007000000000000000300000000000000000000000000000001fde7884dcceb4dc81ae52d236e8a7887da67b35f1352f9418a1c86873516c77a0000000000000001000000000000000100000000000000001d3722f1eac3fb89f4099a052b832308de84342d2f667beab67825be49c322d0"},
		{keysOnlyTopology, "47520202010000000000000000000000000000000000000000000007000000000000000300000000000000000000000000000001fde7884dcceb4dc81ae52d236e8a7887da67b35f1352f9418a1c86873516c77a00000000000000010000000000000001000000000000000194b5a65a20f538d40631a5d3b7047255296cd44bcf6f10c903f558b91e77146e"},
		{legacyFullTopology, "47520202010000000000000000000000000000000000000000000007000000000000000300000000000000000000000000000001fde7884dcceb4dc81ae52d236e8a7887da67b35f1352f9418a1c86873516c77a000000000000000100000000000000010000000000000002ab60081b7f63c4f82fae7dfd6cece77e7c26fb6abd863f2e91e6c363d291c32b"},
		{fullTopology, "47520202010000000000000000000000000000000000000000000007000000000000000300000000000000000000000000000001fde7884dcceb4dc81ae52d236e8a7887da67b35f1352f9418a1c86873516c77a000000000000000100000000000000010000000000000003cf1e163e2e8ae9206d0984b7f4370250244c85ad6fe36fd830eba57cd74799b0"},
	} {
		r := primitive
		r.topology = tc.topology
		wire, err := EncodeRoot(r)
		if err != nil {
			t.Fatal(err)
		}
		golden, err := hex.DecodeString(tc.golden)
		if err != nil || !bytes.Equal(wire, golden) {
			t.Fatal("legacy bytes/digest changed", tc.topology, err)
		}
	}
	root.epoch = math.MaxUint64
	if _, err := root.AdvanceEffects(d.Digest()); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	root.next = math.MaxUint64
	if _, _, err := root.ReservePhysical(1); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
}

func TestOwnershipPendingMissingDoesNotValidateAbsentGroupAndPublishedHistory(t *testing.T) {
	d := ownershipDecl(t, 2)
	local := Namespace{Graph: d.Graph(), Partition: 1}
	s, cfg, seed := ownershipStore(t, vfs.NewMem(), local, d, [16]byte{9})
	old := ownershipView(t, s, 1)
	initial, work, err := ReadOwnershipDeclaration(t.Context(), old, Limits{}, ownershipBudget())
	if err != nil || initial.Published() || initial.Declaration().Len() != 0 || initial.Root() != seed || initial.Binding() != s.ApplicationBinding() || initial.Reference().Index != 1 || initial.Reference().Generation != 1 || initial.OwnedBytes() != ownershipMetadataBytes || work.Records != 1 {
		t.Fatal(initial, work, err)
	}
	effects, index := ownershipPublish(t, s, d)
	current := ownershipView(t, s, index)
	read, _, err := ReadOwnershipDeclaration(t.Context(), current, Limits{}, ownershipBudget())
	if err != nil || !read.Published() || read.Declaration().Digest() != d.Digest() || read.Root() != effects.Root || read.Reference().Index != index || read.Binding() != initial.Binding() {
		t.Fatal(read, err)
	}
	retained, _, err := ReadOwnershipDeclaration(t.Context(), old, Limits{}, ownershipBudget())
	if err != nil || retained.Published() || retained.Declaration().Len() != 0 || retained.Reference() != initial.Reference() || retained.Root() != seed {
		t.Fatal("old pending view consulted current declaration", retained, err)
	}
	replay, _, err := StageOwnershipDeclaration(t.Context(), current, d, Limits{}, ownershipBudget())
	if err != nil || len(replay.Writes) != 0 || replay.Root != effects.Root {
		t.Fatal("declaration retry rewrote immutable binding", replay, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.Create = false
	reopened, err := raftlog.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for _, at := range []uint64{1, index} {
		r, _, err := ReadOwnershipDeclaration(t.Context(), ownershipView(t, reopened, at), Limits{}, ownershipBudget())
		if err != nil || r.Published() != (at == index) || r.Reference().Index != at {
			t.Fatal("reopen retained state", r, err)
		}
	}
	wrong, _, wrongSeed := ownershipStore(t, vfs.NewMem(), local, d, [16]byte{99})
	wv := ownershipView(t, wrong, 1)
	unready, _, err := ReadOwnershipDeclaration(t.Context(), wv, Limits{}, ownershipBudget())
	if err != nil || unready.Published() || unready.Declaration().Len() != 0 || unready.Binding().Identity.Group != ([16]byte{99}) {
		t.Fatal("pending missing invented entry group validation", unready, err)
	}
	if _, _, err := StageOwnershipDeclaration(t.Context(), wv, d, Limits{}, ownershipBudget()); !errors.Is(err, ErrNamespace) {
		t.Fatal("supplied declaration group not checked", err)
	}
	wrongSeed.ownershipMode = ownershipPublished
	image, err := EncodeRoot(wrongSeed)
	if err != nil {
		t.Fatal(err)
	}
	l := DefaultLimits()
	wire, err := encodeOwnershipDeclaration(d, l)
	if err != nil {
		t.Fatal(err)
	}
	wi := ownershipInstallRaw(t, wrong, image, []raftlog.KV{{Key: ownershipKey(d.Graph(), d.TopologyEpoch(), d.Digest()), Value: wire}})
	if got, _, err := ReadOwnershipDeclaration(t.Context(), ownershipView(t, wrong, wi), Limits{}, ownershipBudget()); !errors.Is(err, ErrNamespace) || got.Root() != (Root{}) {
		t.Fatal("published wrong binding admitted", got, err)
	}
}

func TestOwnershipQualifiedPartitionsSharingGroupRemainSeparate(t *testing.T) {
	d := ownershipDecl(t, 2)
	stores := []*raftlog.Store{}
	for _, part := range []uint64{1, 2} {
		s, _, _ := ownershipStore(t, vfs.NewMem(), Namespace{Graph: d.Graph(), Partition: part}, d, [16]byte{9})
		stores = append(stores, s)
	}
	_, firstIndex := ownershipPublish(t, stores[0], d)
	first, _, err := ReadOwnershipDeclaration(t.Context(), ownershipView(t, stores[0], firstIndex), Limits{}, ownershipBudget())
	if err != nil || !first.Published() || first.Binding().Identity.Partition != 1 {
		t.Fatal(first, err)
	}
	second, _, err := ReadOwnershipDeclaration(t.Context(), ownershipView(t, stores[1], 1), Limits{}, ownershipBudget())
	if err != nil || second.Published() || second.Binding().Identity.Partition != 2 || second.Binding().Identity.Group != first.Binding().Identity.Group {
		t.Fatal("qualified identities aliased", second, err)
	}
	_, secondIndex := ownershipPublish(t, stores[1], d)
	second, _, err = ReadOwnershipDeclaration(t.Context(), ownershipView(t, stores[1], secondIndex), Limits{}, ownershipBudget())
	if err != nil || !second.Published() || second.Root().owner != 11 || first.Root().owner != 10 || second.Root().namespace == first.Root().namespace {
		t.Fatal(second, err)
	}
}

func TestOwnershipOperationBoundariesAndNoDurableEffectsOnRefusal(t *testing.T) {
	d := ownershipDecl(t, 3)
	s, _, _ := ownershipStore(t, vfs.NewMem(), Namespace{Graph: d.Graph(), Partition: 1}, d, [16]byte{9})
	v := ownershipView(t, s, 1)
	before, err := s.ApplicationUsage()
	if err != nil {
		t.Fatal(err)
	}
	stage, work, err := StageOwnershipDeclaration(t.Context(), v, d, Limits{}, ownershipBudget())
	if err != nil {
		t.Fatal(err)
	}
	for _, dim := range []string{"source", "output"} {
		for _, adjustment := range []int{-1, 0, 1} {
			b := ownershipBudget()
			if dim == "source" {
				b.SourceBytes = work.Bytes + adjustment
			} else {
				b.OutputBytes = stage.OwnedBytes + adjustment
			}
			got, used, err := StageOwnershipDeclaration(t.Context(), v, d, Limits{}, b)
			if adjustment < 0 {
				if !errors.Is(err, ErrResourceLimit) || !reflect.DeepEqual(got, OwnershipEffects{}) || used.Bytes > b.SourceBytes {
					t.Fatal(dim, adjustment, got, used, err)
				}
			} else if err != nil || len(got.Writes) != 1 || got.Root != stage.Root {
				t.Fatal(dim, adjustment, got, err)
			}
		}
	}
	after, err := s.ApplicationUsage()
	if err != nil || after != before {
		t.Fatal("staging/refusals published effects", after, before, err)
	}
	_, index := ownershipPublish(t, s, d)
	current := ownershipView(t, s, index)
	read, rw, err := ReadOwnershipDeclaration(t.Context(), current, Limits{}, ownershipBudget())
	if err != nil {
		t.Fatal(err)
	}
	for _, dim := range []string{"source", "output"} {
		for _, adjustment := range []int{-1, 0, 1} {
			b := ownershipBudget()
			if dim == "source" {
				b.SourceBytes = rw.Bytes + adjustment
			} else {
				b.OutputBytes = read.OwnedBytes() + adjustment
			}
			got, used, err := ReadOwnershipDeclaration(t.Context(), current, Limits{}, b)
			if adjustment < 0 {
				if !errors.Is(err, ErrResourceLimit) || got.Root() != (Root{}) || used.Bytes > b.SourceBytes {
					t.Fatal(dim, adjustment, got, used, err)
				}
			} else if err != nil || !got.Published() || got.Declaration().Digest() != d.Digest() {
				t.Fatal(dim, adjustment, got, err)
			}
		}
	}
	if allocations := testing.AllocsPerRun(100, func() {
		if _, _, err := ReadOwnershipDeclaration(t.Context(), current, Limits{}, OwnershipBudget{1, 1, 1}); !errors.Is(err, ErrResourceLimit) {
			t.Fatal(err)
		}
	}); allocations != 0 {
		t.Fatal("metadata allocated before initial budget admission", allocations)
	}
}

func TestOwnershipRootCodecAndPrivateGateRefusals(t *testing.T) {
	d := ownershipDecl(t, 2)
	root, err := NewOwnershipRoot(Namespace{Graph: d.Graph(), Partition: 1}, d)
	if err != nil {
		t.Fatal(err)
	}
	good, err := EncodeRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, edit := range []func([]byte){
		func(b []byte) { b[3] = 2 },
		func(b []byte) { clear(b[108:140]) },
		func(b []byte) { binary.BigEndian.PutUint64(b[100:108], 3) },
		func(b []byte) { binary.BigEndian.PutUint64(b[92:100], 2) },
		func(b []byte) { clear(b[84:92]) },
	} {
		bad := bytes.Clone(good)
		edit(bad)
		hash := sha256.Sum256(bad[:len(bad)-32])
		copy(bad[len(bad)-32:], hash[:])
		if _, err := DecodeRoot(bad); !errors.Is(err, ErrCorrupt) {
			t.Fatal("rechecksummed unsupported root admitted", hex.EncodeToString(bad), err)
		}
	}
	for _, b := range [][]byte{good[:len(good)-1], append(bytes.Clone(good), 0)} {
		if _, err := DecodeRoot(b); !errors.Is(err, ErrCorrupt) {
			t.Fatal(err)
		}
	}
	s, _, seed := ownershipStore(t, vfs.NewMem(), root.namespace, d, [16]byte{9})
	v := ownershipView(t, s, 1)
	if _, err := OpenCatalog(v, root.namespace, root.owner, Limits{}); !errors.Is(err, ErrTopologyUnsupported) {
		t.Fatal("GR3 gained legacy catalog capability", err)
	}
	c := &Catalog{root: seed}
	stage := &Stage{c: c}
	changed := seed
	changed.ownershipMode = ownershipPublished
	if err := validatePrivateRoot(stage, changed); !errors.Is(err, ErrInvalid) {
		t.Fatal("mode private promotion", err)
	}
	changed = seed
	changed.ownershipDigest[0]++
	if err := validatePrivateRoot(stage, changed); !errors.Is(err, ErrInvalid) {
		t.Fatal("digest private rebinding", err)
	}
}

func TestOwnershipMetadataSizeAllowancesRemainConservative(t *testing.T) {
	if unsafe.Sizeof(PartitionOwnership{}) != ownershipEntryBytes || unsafe.Sizeof(OwnershipDeclaration{}) > ownershipDeclarationMetadataBytes {
		t.Fatal("declaration ledger undercount")
	}
	// Read and effects are alternative results. Each live-set estimate includes
	// local metadata copies plus hash/sort fixed scratch, not process memory.
	readLive := unsafe.Sizeof(ownershipReader{}) + unsafe.Sizeof(OwnershipRead{}) + unsafe.Sizeof(raftlog.ApplicationRoot{}) + unsafe.Sizeof(Root{}) + unsafe.Sizeof(OwnershipDeclaration{}) + unsafe.Sizeof(raftlog.ApplicationBinding{}) + 256
	stageLive := unsafe.Sizeof(ownershipReader{}) + unsafe.Sizeof(OwnershipEffects{}) + 2*unsafe.Sizeof(raftlog.ApplicationRoot{}) + 2*unsafe.Sizeof(Root{}) + 2*unsafe.Sizeof(OwnershipDeclaration{}) + unsafe.Sizeof(raftlog.ApplicationBinding{}) + 256
	if max(readLive, stageLive) > ownershipMetadataBytes {
		t.Fatal("metadata allowance undercounts justified live sets", readLive, stageLive)
	}
	if unsafe.Sizeof(GraphEffects{}) > 512 || unsafe.Sizeof(fullStageState{}) > fullStageMetadataBytes || unsafe.Sizeof(indexedStageState{}) > indexedStageMetadataBytes || unsafe.Sizeof(ReadView{})+unsafe.Sizeof(PageReader{})+unsafe.Sizeof(fullIndexDescriptor{})+128 > fullViewMetadataBytes {
		t.Fatal("added Root fields break existing metadata allowances")
	}
}

func TestOwnershipInvalidBorrowAndEmptyProofRefusals(t *testing.T) {
	d := ownershipDecl(t, 1)
	s, _, root := ownershipStore(t, vfs.NewMem(), Namespace{Graph: d.Graph(), Partition: 1}, d, [16]byte{9})
	v := ownershipView(t, s, 1)
	if _, _, err := ReadOwnershipDeclaration(t.Context(), nil, Limits{}, ownershipBudget()); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, _, err := StageOwnershipDeclaration(t.Context(), nil, d, Limits{}, ownershipBudget()); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	//nolint:staticcheck // SA1012: deliberate nil-context contract test.
	if _, _, err := ReadOwnershipDeclaration(nil, v, Limits{}, ownershipBudget()); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	//nolint:staticcheck // SA1012: deliberate nil-context contract test.
	if _, _, err := StageOwnershipDeclaration(nil, v, d, Limits{}, ownershipBudget()); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err := ReadOwnershipDeclaration(canceled, v, Limits{}, ownershipBudget()); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, _, err := StageOwnershipDeclaration(canceled, v, d, Limits{}, ownershipBudget()); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	image, err := EncodeRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	index := ownershipInstallRaw(t, s, image, []raftlog.KV{{Key: []byte("hidden"), Value: []byte("not an allocator")}})
	if _, _, err := StageOwnershipDeclaration(t.Context(), v, d, Limits{}, ownershipBudget()); !errors.Is(err, raftlog.ErrInvalid) {
		t.Fatal("stale proof", err)
	}
	current := ownershipView(t, s, index)
	if _, _, err := StageOwnershipDeclaration(t.Context(), current, d, Limits{}, ownershipBudget()); !errors.Is(err, raftlog.ErrInvalid) {
		t.Fatal("hidden data silently reinitialized", err)
	}
	if err := current.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ReadOwnershipDeclaration(t.Context(), current, Limits{}, ownershipBudget()); !errors.Is(err, raftlog.ErrClosed) {
		t.Fatal(err)
	}
}

func TestOwnershipAdversarialStoredRecordsAndScopeAreFailClosed(t *testing.T) {
	d := ownershipDecl(t, 2)
	for _, tc := range []struct {
		name string
		edit func(*Root, *[]raftlog.KV)
		want error
	}{
		{"missing", func(_ *Root, rows *[]raftlog.KV) { *rows = nil }, ErrCorrupt},
		{"tombstone", func(_ *Root, rows *[]raftlog.KV) { (*rows)[0].Deleted = true; (*rows)[0].Value = nil }, ErrCorrupt},
		{"header", func(_ *Root, rows *[]raftlog.KV) { (*rows)[0].Value[3] = 9 }, ErrCorrupt},
		{"truncated", func(_ *Root, rows *[]raftlog.KV) { (*rows)[0].Value = (*rows)[0].Value[:65] }, ErrCorrupt},
		{"trailing", func(_ *Root, rows *[]raftlog.KV) { (*rows)[0].Value = append((*rows)[0].Value, 0) }, ErrCorrupt},
		{"count overflow", func(_ *Root, rows *[]raftlog.KV) { binary.BigEndian.PutUint32((*rows)[0].Value[28:32], math.MaxUint32) }, ErrCorrupt},
		{"digest", func(_ *Root, rows *[]raftlog.KV) { (*rows)[0].Value[len((*rows)[0].Value)-1]++ }, ErrCorrupt},
		{"zero group", func(_ *Root, rows *[]raftlog.KV) { clear((*rows)[0].Value[48:64]) }, ErrCorrupt},
		{"duplicate partition", func(_ *Root, rows *[]raftlog.KV) { copy((*rows)[0].Value[64:72], (*rows)[0].Value[32:40]) }, ErrCorrupt},
		{"other graph", func(_ *Root, rows *[]raftlog.KV) { (*rows)[0].Value[4]++ }, ErrCorrupt},
		{"other epoch", func(_ *Root, rows *[]raftlog.KV) { binary.BigEndian.PutUint64((*rows)[0].Value[20:28], 2) }, ErrCorrupt},
		{"pending with data", func(root *Root, _ *[]raftlog.KV) { root.ownershipMode = ownershipPending }, ErrCorrupt},
		{"wrong owner", func(root *Root, _ *[]raftlog.KV) { root.owner++ }, ErrStaleOwner},
		{"wrong namespace", func(root *Root, _ *[]raftlog.KV) { root.namespace.Partition = 2 }, ErrNamespace},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, root := ownershipStore(t, vfs.NewMem(), Namespace{Graph: d.Graph(), Partition: 1}, d, [16]byte{9})
			root.ownershipMode = ownershipPublished
			wire, err := encodeOwnershipDeclaration(d, DefaultLimits())
			if err != nil {
				t.Fatal(err)
			}
			rows := []raftlog.KV{{Key: ownershipKey(d.Graph(), d.TopologyEpoch(), d.Digest()), Value: wire}}
			tc.edit(&root, &rows)
			image, err := EncodeRoot(root)
			if err != nil {
				t.Fatal(err)
			}
			at := ownershipInstallRaw(t, s, image, rows)
			before, err := s.ApplicationUsage()
			if err != nil {
				t.Fatal(err)
			}
			view := ownershipView(t, s, at)
			got, _, err := ReadOwnershipDeclaration(t.Context(), view, Limits{}, ownershipBudget())
			if !errors.Is(err, tc.want) || got.Root() != (Root{}) || got.Published() {
				t.Fatal("corrupt record produced metadata", got, err)
			}
			usage, err := s.ApplicationUsage()
			if err != nil || usage.RetainedBytes != before.RetainedBytes || usage.RetainedRecords != before.RetainedRecords || usage.TailEntries != before.TailEntries {
				t.Fatal("read refusal changed retained state", usage, before, err)
			}
		})
	}
}

func TestOwnershipReadNeverUsesLaterRecordAtOlderPublishedRoot(t *testing.T) {
	d := ownershipDecl(t, 1)
	s, _, root := ownershipStore(t, vfs.NewMem(), Namespace{Graph: d.Graph(), Partition: 1}, d, [16]byte{9})
	root.ownershipMode = ownershipPublished
	image, err := EncodeRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	badIndex := ownershipInstallRaw(t, s, image, nil)
	old := ownershipView(t, s, badIndex)
	wire, err := encodeOwnershipDeclaration(d, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	goodIndex := ownershipInstallRaw(t, s, image, []raftlog.KV{{Key: ownershipKey(d.Graph(), d.TopologyEpoch(), d.Digest()), Value: wire}})
	if got, _, err := ReadOwnershipDeclaration(t.Context(), old, Limits{}, ownershipBudget()); !errors.Is(err, ErrCorrupt) || got.Published() {
		t.Fatal("future declaration repaired a past missing record", got, err)
	}
	got, _, err := ReadOwnershipDeclaration(t.Context(), ownershipView(t, s, goodIndex), Limits{}, ownershipBudget())
	if err != nil || !got.Published() || got.Reference().Index != goodIndex {
		t.Fatal(got, err)
	}
}

func TestOwnershipRehashedStructuralViolationsCannotHideBehindDigest(t *testing.T) {
	d := ownershipDecl(t, 2)
	for _, tc := range []struct {
		name string
		edit func([]byte)
	}{
		{"zero group", func(b []byte) { clear(b[48:64]) }},
		{"duplicate partition", func(b []byte) { copy(b[64:72], b[32:40]) }},
		{"descending partitions", func(b []byte) {
			var first [32]byte
			copy(first[:], b[32:64])
			copy(b[32:64], b[64:96])
			copy(b[64:96], first[:])
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, root := ownershipStore(t, vfs.NewMem(), Namespace{Graph: d.Graph(), Partition: 1}, d, [16]byte{9})
			wire, err := encodeOwnershipDeclaration(d, DefaultLimits())
			if err != nil {
				t.Fatal(err)
			}
			tc.edit(wire)
			// Independently hash the actual malformed logical byte body. Root,
			// logical key and record digest all agree, so only shape/order checks
			// can refuse these records; a stale checksum is not the explanation.
			h := sha256.New()
			_, _ = h.Write([]byte(ownershipHashDomain))
			_, _ = h.Write(wire[4 : len(wire)-32])
			var digest [32]byte
			copy(digest[:], h.Sum(nil))
			copy(wire[len(wire)-32:], digest[:])
			root.ownershipDigest, root.ownershipMode = digest, ownershipPublished
			image, err := EncodeRoot(root)
			if err != nil {
				t.Fatal(err)
			}
			index := ownershipInstallRaw(t, s, image, []raftlog.KV{{Key: ownershipKey(d.Graph(), d.TopologyEpoch(), digest), Value: wire}})
			got, _, err := ReadOwnershipDeclaration(t.Context(), ownershipView(t, s, index), Limits{}, ownershipBudget())
			if !errors.Is(err, ErrCorrupt) || got.Published() || got.Root() != (Root{}) {
				t.Fatal("matching hashes bypassed structural validation", got, err)
			}
		})
	}
}

func TestOwnershipStageSeedRebindingPolicyAndClosedRefusals(t *testing.T) {
	d := ownershipDecl(t, 2)
	s, _, root := ownershipStore(t, vfs.NewMem(), Namespace{Graph: d.Graph(), Partition: 1}, d, [16]byte{9})
	v := ownershipView(t, s, 1)
	otherEntries := ownershipEntries(2)
	otherEntries[1].OwnershipEpoch++
	other, err := NewOwnershipDeclaration(d.Graph(), d.TopologyEpoch(), otherEntries, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		decl OwnershipDeclaration
		l    Limits
		b    OwnershipBudget
		want error
	}{
		{"empty declaration", OwnershipDeclaration{}, Limits{}, ownershipBudget(), ErrInvalid},
		{"changed declaration", other, Limits{}, ownershipBudget(), ErrRebinding},
		{"bad catalog policy", d, Limits{MaxReadRows: -1}, ownershipBudget(), ErrInvalid},
		{"bad operation rows", d, Limits{}, OwnershipBudget{0, 4096, 4096}, ErrInvalid},
		{"bad operation bytes", d, Limits{}, OwnershipBudget{1, -1, 4096}, ErrInvalid},
		{"bad output bytes", d, Limits{}, OwnershipBudget{1, 4096, -1}, ErrInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, _, err := StageOwnershipDeclaration(t.Context(), v, tc.decl, tc.l, tc.b)
			if !errors.Is(err, tc.want) || !reflect.DeepEqual(got, OwnershipEffects{}) {
				t.Fatal(got, err)
			}
		})
	}
	advanced, _, err := root.ReservePhysical(1)
	if err != nil {
		t.Fatal(err)
	}
	image, err := EncodeRoot(advanced)
	if err != nil {
		t.Fatal(err)
	}
	index := ownershipInstallRaw(t, s, image, nil)
	if _, _, err := StageOwnershipDeclaration(t.Context(), ownershipView(t, s, index), d, Limits{}, ownershipBudget()); !errors.Is(err, ErrTopologyUnsupported) {
		t.Fatal("advanced pending root silently reinitialized", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ReadOwnershipDeclaration(t.Context(), v, Limits{}, ownershipBudget()); !errors.Is(err, raftlog.ErrClosed) {
		t.Fatal(err)
	}
	if _, _, err := StageOwnershipDeclaration(t.Context(), v, d, Limits{}, ownershipBudget()); !errors.Is(err, raftlog.ErrClosed) {
		t.Fatal(err)
	}
}

func TestOwnershipExactRecordAndStagePolicyBoundary(t *testing.T) {
	l := DefaultLimits()
	count := (l.MaxRecordBytes - ownershipRecordFixedBytes) / ownershipEntryBytes
	d, err := NewOwnershipDeclaration(graphstate.GraphID{1}, 1, ownershipEntries(count), l)
	if err != nil {
		t.Fatal(err)
	}
	s, _, _ := ownershipStore(t, vfs.NewMem(), Namespace{Graph: d.Graph(), Partition: 1}, d, [16]byte{9})
	v := ownershipView(t, s, 1)
	want := 128 + 2*ownershipKeyBytes + ownershipRecordFixedBytes + ownershipEntryBytes*count + 64
	for _, adjustment := range []int{-1, 0, 1} {
		policy := l
		policy.MaxStageBytes = want + adjustment
		got, _, err := StageOwnershipDeclaration(t.Context(), v, d, policy, ownershipBudget())
		if adjustment < 0 {
			if !errors.Is(err, ErrResourceLimit) || len(got.Writes) != 0 {
				t.Fatal(got, err)
			}
		} else if err != nil || len(got.Writes) != 1 || len(got.Writes[0].Value) != l.MaxRecordBytes {
			t.Fatal(got.Root, err)
		}
	}
}
