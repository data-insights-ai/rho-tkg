package graphstore

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"path"
	"slices"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"google.golang.org/protobuf/proto"
)

const boundBootstrapGolden = "47520202010000000000000000000000000000000000000000000007000000000000000300000000000000000000000000000001fde7884dcceb4dc81ae52d236e8a7887da67b35f1352f9418a1c86873516c77a0000000000000001000000000000000100000000000000001d3722f1eac3fb89f4099a052b832308de84342d2f667beab67825be49c322d0"

func boundBootstrapConfig(fs vfs.FS) raftlog.Config {
	p := raftlog.DefaultApplicationPolicy(1)
	return raftlog.Config{Dir: "bound", FS: fs, Create: true, Application: p,
		Transfer:    raftlog.ApplicationTransferConfig{Identity: raftlog.ApplicationIdentity{Graph: [16]byte{1}, Partition: 7, Group: [16]byte{9}}, Contract: raftlog.ApplicationContractForPolicy(p), Limits: raftlog.DefaultApplicationTransferLimits()},
		Generations: raftlog.ApplicationGenerationLimits{MaxBytes: 2 << 30, MaxRecords: 2000000}, PublishedCuts: raftlog.ApplicationPublishedCutLimits{MaxTransferChunks: 4096}, Replication: raftlog.ApplicationReplicationConfig{Voters: [3]uint64{1, 2, 3}}, SemanticContractID: raftlog.ApplicationSemanticContractID{5}}
}
func boundBootstrapOpen(t *testing.T, cfg raftlog.Config) *raftlog.Store {
	t.Helper()
	s, e := raftlog.Open(cfg)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if e := s.Close(); e != nil {
			t.Error(e)
		}
	})
	return s
}
func boundBootstrapFiles(t *testing.T, fs vfs.FS, dir string) [32]byte {
	t.Helper()
	names, e := fs.List(dir)
	if e != nil {
		t.Fatal(e)
	}
	slices.Sort(names)
	h := sha256.New()
	for _, name := range names {
		filename := path.Join(dir, name)
		info, e := fs.Stat(filename)
		if e != nil {
			t.Fatal(e)
		}
		if info.IsDir() {
			sum := boundBootstrapFiles(t, fs, filename)
			_, _ = h.Write(sum[:])
			continue
		}
		f, e := fs.Open(filename)
		if e != nil {
			t.Fatal(e)
		}
		b, e := io.ReadAll(f)
		e = errors.Join(e, f.Close())
		if e != nil {
			t.Fatal(e)
		}
		_, _ = h.Write([]byte(name))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write(b)
	}
	var sum [32]byte
	copy(sum[:], h.Sum(nil))
	return sum
}

type boundBootstrapState struct {
	files              [32]byte
	image, hard, conf  []byte
	index, first, last uint64
	usage              raftlog.ApplicationUsage
	transfer           raftlog.ApplicationTransferUsage
	binding            raftlog.ApplicationBinding
}

func boundBootstrapSnapshot(t *testing.T, s *raftlog.Store, cfg raftlog.Config) boundBootstrapState {
	t.Helper()
	i, b, e := s.Checkpoint()
	if e != nil {
		t.Fatal(e)
	}
	hard, conf, e := s.InitialState()
	if e != nil {
		t.Fatal(e)
	}
	first, e := s.FirstIndex()
	if e != nil {
		t.Fatal(e)
	}
	last, e := s.LastIndex()
	if e != nil {
		t.Fatal(e)
	}
	usage, e := s.ApplicationUsage()
	if e != nil {
		t.Fatal(e)
	}
	transfer := raftlog.ApplicationTransferUsage{}
	if cfg.Transfer != (raftlog.ApplicationTransferConfig{}) {
		transfer, e = s.ApplicationTransferUsage()
		if e != nil {
			t.Fatal(e)
		}
	}
	hb, e := proto.MarshalOptions{Deterministic: true}.Marshal(hard)
	if e != nil {
		t.Fatal(e)
	}
	cb, e := proto.MarshalOptions{Deterministic: true}.Marshal(conf)
	if e != nil {
		t.Fatal(e)
	}
	return boundBootstrapState{boundBootstrapFiles(t, cfg.FS, cfg.Dir), b, hb, cb, i, first, last, usage, transfer, s.ApplicationBinding()}
}
func boundBootstrapUnchanged(t *testing.T, before, after boundBootstrapState) {
	t.Helper()
	if before.files != after.files || before.index != after.index || before.first != after.first || before.last != after.last || before.usage != after.usage || before.transfer != after.transfer || before.binding != after.binding || !bytes.Equal(before.image, after.image) || !bytes.Equal(before.hard, after.hard) || !bytes.Equal(before.conf, after.conf) {
		t.Fatal("refusal changed complete physical fingerprint/metadata/ledgers")
	}
}
func TestBoundBootstrapExactSeedAndLegacyParity(t *testing.T) {
	cfg := boundBootstrapConfig(vfs.NewMem())
	s := boundBootstrapOpen(t, cfg)
	if e := BootstrapBoundSinglePartition(s, s.ApplicationBinding(), 3, [3]uint64{1, 2, 3}); e != nil {
		t.Fatal(e)
	}
	i, image, e := s.Checkpoint()
	if e != nil || i != 1 {
		t.Fatal(i, e)
	}
	want, e := hex.DecodeString(boundBootstrapGolden)
	t.Logf("Checkpoint len=%d cap=%d sha256=%x goldenEqual=%t", len(image), cap(image), sha256.Sum256(image), bytes.Equal(image, want))
	if e != nil || len(image) != 140 || cap(image) > s.Limits().MaxSnapshotBytes || !bytes.Equal(image, want) {
		t.Fatal("frozen GR2 seed bytes", e)
	}
	root, e := DecodeRoot(image)
	if e != nil {
		t.Fatal(e)
	}
	got, e := root.SinglePartition()
	if e != nil || got != (SinglePartitionTopology{Graph: [16]byte{1}, Partition: 7, OwnershipEpoch: 3, TopologyEpoch: 1, SchemaVersion: 1, IndexVersion: 0}) || root.SemanticEpoch() != 0 || root.NextPhysicalID() != 1 {
		t.Fatal(got, root, e)
	}
	hard, conf, e := s.InitialState()
	if e != nil || hard.GetTerm() != 1 || hard.GetCommit() != 1 || !slices.Equal(conf.GetVoters(), []uint64{1, 2, 3}) {
		t.Fatal(hard, conf, e)
	}
	usage, e := s.ApplicationUsage()
	if e != nil || usage.RetainedBytes != 275 || usage.RetainedRecords != 3 || usage.CheckpointBytes != 140 || usage.SnapshotBytes != 140 || usage.Views != 0 || usage.ViewBytes != 0 {
		t.Fatal(usage, e)
	}
	v, e := s.ApplicationView(1)
	if e != nil {
		t.Fatal(e)
	}
	viewRoot, e := v.Root()
	if e != nil || len(viewRoot.Image) != 140 || cap(viewRoot.Image) != len(viewRoot.Image) || !bytes.Equal(viewRoot.Image, want) {
		t.Fatal("exact-capacity View.Root contract", e)
	}
	empty, e := v.ProveNoApplicationData(t.Context())
	if e != nil || !bytes.Equal(empty.Image, want) {
		t.Fatal("seed granted graph data/readiness", e)
	}
	c, e := OpenCatalog(v, Namespace{Graph: [16]byte{1}, Partition: 7}, 3, Limits{})
	if e != nil {
		t.Fatal(e)
	}
	if read, e := OpenReadView(t.Context(), c, GraphLimits{}); !errors.Is(e, ErrTopologyUnsupported) || read != nil {
		t.Fatal("seed is not Full readiness", e)
	}
	if e := v.Close(); e != nil {
		t.Fatal(e)
	}
	if e := s.ScrubApplication(t.Context()); e != nil {
		t.Fatal(e)
	}
	legacyCfg := raftlog.Config{Dir: "legacy", FS: vfs.NewMem(), Create: true, Application: raftlog.DefaultApplicationPolicy(19)}
	legacy := boundBootstrapOpen(t, legacyCfg)
	if e := BootstrapSinglePartition(legacy, Namespace{Graph: [16]byte{1}, Partition: 7}, 3); e != nil {
		t.Fatal(e)
	}
	_, old, e := legacy.Checkpoint()
	if e != nil || !bytes.Equal(old, want) {
		t.Fatal("legacy helper changed bytes", e)
	}
	before := boundBootstrapSnapshot(t, s, cfg)
	if e := s.Close(); e != nil {
		t.Fatal(e)
	}
	cfg.Create = false
	s = boundBootstrapOpen(t, cfg)
	after := boundBootstrapSnapshot(t, s, cfg)
	if before.index != after.index || !bytes.Equal(before.image, after.image) || !bytes.Equal(before.hard, after.hard) || !bytes.Equal(before.conf, after.conf) || before.usage != after.usage || before.binding != after.binding {
		t.Fatal("reopen changed initialized seed")
	}
	stable := boundBootstrapSnapshot(t, s, cfg)
	if e := BootstrapBoundSinglePartition(s, s.ApplicationBinding(), 3, [3]uint64{1, 2, 3}); !errors.Is(e, ErrInvalid) || !errors.Is(e, raftlog.ErrInvalid) {
		t.Fatal(e)
	}
	boundBootstrapUnchanged(t, stable, boundBootstrapSnapshot(t, s, cfg))
}
func TestBoundBootstrapBindingVotersAndOwnerRefusals(t *testing.T) {
	cfg := boundBootstrapConfig(vfs.NewMem())
	s := boundBootstrapOpen(t, cfg)
	binding := s.ApplicationBinding()
	if e := BootstrapBoundSinglePartition(nil, binding, 3, [3]uint64{1, 2, 3}); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
	cases := []struct {
		name    string
		binding raftlog.ApplicationBinding
		owner   uint64
		voters  [3]uint64
		want    error
	}{
		{"zero binding", raftlog.ApplicationBinding{}, 3, [3]uint64{1, 2, 3}, ErrInvalid},
		{"zero epoch", binding, 0, [3]uint64{1, 2, 3}, ErrInvalid},
		{"zero voters", binding, 3, [3]uint64{}, ErrInvalid},
		{"duplicate", binding, 3, [3]uint64{1, 1, 3}, ErrInvalid},
		{"unsorted", binding, 3, [3]uint64{1, 3, 2}, ErrInvalid},
		{"missing local", binding, 3, [3]uint64{2, 3, 4}, ErrInvalid},
		{"different configured voters", binding, 3, [3]uint64{1, 2, 4}, ErrInvalid},
	}
	for _, field := range []string{"graph", "partition", "group", "semantic", "zero graph", "zero partition", "zero group", "zero semantic"} {
		b := binding
		want := ErrInvalid
		switch field {
		case "graph":
			b.Identity.Graph[0]++
			want = ErrNamespace
		case "partition":
			b.Identity.Partition++
			want = ErrNamespace
		case "group":
			b.Identity.Group[0]++
		case "semantic":
			b.SemanticContractID[0]++
		case "zero graph":
			clear(b.Identity.Graph[:])
		case "zero partition":
			b.Identity.Partition = 0
		case "zero group":
			clear(b.Identity.Group[:])
		case "zero semantic":
			clear(b.SemanticContractID[:])
		}
		cases = append(cases, struct {
			name    string
			binding raftlog.ApplicationBinding
			owner   uint64
			voters  [3]uint64
			want    error
		}{field, b, 3, [3]uint64{1, 2, 3}, want})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := boundBootstrapSnapshot(t, s, cfg)
			if e := BootstrapBoundSinglePartition(s, tc.binding, tc.owner, tc.voters); !errors.Is(e, tc.want) {
				t.Fatal(e, tc.want)
			}
			boundBootstrapUnchanged(t, before, boundBootstrapSnapshot(t, s, cfg))
		})
	}
	if e := BootstrapBoundSinglePartition(s, binding, 3, [3]uint64{1, 2, 3}); e != nil {
		t.Fatal("valid retry after refusal", e)
	}
	if e := s.Close(); e != nil {
		t.Fatal(e)
	}
	if e := BootstrapBoundSinglePartition(s, binding, 3, [3]uint64{1, 2, 3}); !errors.Is(e, raftlog.ErrClosed) || errors.Is(e, ErrCorrupt) {
		t.Fatal(e)
	}
	disabled := raftlog.Config{Dir: "disabled", FS: vfs.NewMem(), Create: true}
	d := boundBootstrapOpen(t, disabled)
	if e := BootstrapBoundSinglePartition(d, binding, 3, [3]uint64{1, 2, 3}); !errors.Is(e, ErrTopologyUnsupported) {
		t.Fatal(e)
	}
}
func TestBoundBootstrapRecoveredEmptyAndPrimitiveCannotPromote(t *testing.T) {
	cfg := boundBootstrapConfig(vfs.NewMem())
	s := boundBootstrapOpen(t, cfg)
	if e := s.Close(); e != nil {
		t.Fatal(e)
	}
	cfg.Create = false
	s = boundBootstrapOpen(t, cfg)
	before := boundBootstrapSnapshot(t, s, cfg)
	if e := BootstrapBoundSinglePartition(s, s.ApplicationBinding(), 3, [3]uint64{1, 2, 3}); !errors.Is(e, ErrInvalid) || !errors.Is(e, raftlog.ErrInvalid) {
		t.Fatal(e)
	}
	boundBootstrapUnchanged(t, before, boundBootstrapSnapshot(t, s, cfg))
	primitive, r := newStore(t, vfs.NewMem())
	c := openCatalog(t, primitive, 1, Limits{})
	st := stage(t, c)
	axis := testAxis(t, 2, 1)
	if e := st.Entity(t.Context(), refEntity(11), graphstate.EntityRecord{ID: 11, Kind: graphstate.Node, Axis: axis}); e != nil {
		t.Fatal(e)
	}
	_, index := commitStage(t, primitive, r, st)
	_, image, e := primitive.Checkpoint()
	if e != nil {
		t.Fatal(e)
	}
	usage, e := primitive.ApplicationUsage()
	if e != nil {
		t.Fatal(e)
	}
	if e := BootstrapBoundSinglePartition(primitive, raftlog.ApplicationBinding{Identity: raftlog.ApplicationIdentity{Graph: [16]byte{1}, Partition: 7, Group: [16]byte{9}}, SemanticContractID: raftlog.ApplicationSemanticContractID{5}}, 3, [3]uint64{1, 2, 3}); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
	next, nextImage, e := primitive.Checkpoint()
	if e != nil || next != index || !bytes.Equal(image, nextImage) {
		t.Fatal(e)
	}
	nextUsage, e := primitive.ApplicationUsage()
	if e != nil || nextUsage != usage {
		t.Fatal(e)
	}
	entity, found, e := openCatalog(t, primitive, index, Limits{}).Entity(t.Context(), refEntity(11))
	if e != nil || !found || entity.ID != 11 {
		t.Fatal(entity, found, e)
	}
}
func TestBoundBootstrapIndependentQuotaEdges(t *testing.T) {
	for _, dimension := range []string{"image", "generation bytes", "generation records"} {
		for _, short := range []bool{true, false} {
			t.Run(dimension+map[bool]string{true: "/short", false: "/exact"}[short], func(t *testing.T) {
				cfg := boundBootstrapConfig(vfs.NewMem())
				switch dimension {
				case "image":
					cfg.Application.MaxImageBytes = 140
					if short {
						cfg.Application.MaxImageBytes--
					}
					cfg.Transfer.Contract = raftlog.ApplicationContractForPolicy(cfg.Application)
				case "generation bytes":
					cfg.Generations.MaxBytes = 275
					if short {
						cfg.Generations.MaxBytes--
					}
				case "generation records":
					cfg.Generations.MaxRecords = 3
					if short {
						cfg.Generations.MaxRecords--
					}
				}
				s := boundBootstrapOpen(t, cfg)
				before := boundBootstrapSnapshot(t, s, cfg)
				e := BootstrapBoundSinglePartition(s, s.ApplicationBinding(), 3, [3]uint64{1, 2, 3})
				if short {
					if !errors.Is(e, ErrResourceLimit) || !errors.Is(e, raftlog.ErrLimit) || errors.Is(e, ErrCorrupt) {
						t.Fatal(e)
					}
					boundBootstrapUnchanged(t, before, boundBootstrapSnapshot(t, s, cfg))
				} else if e != nil {
					t.Fatal(e)
				}
			})
		}
	}
	// Legal Open preflights larger worst-case metadata/descriptor headroom than
	// Initialize. A low metadata cap is an Open refusal, never a fabricated
	// Initialize branch. Existing raftlog metadata tests own exact sizing.
	cfg := boundBootstrapConfig(vfs.NewMem())
	cfg.Limits = raftlog.DefaultLimits()
	cfg.Limits.MaxEntryBytes = 256
	cfg.Limits.MaxReadBytes = 512
	cfg.Limits.MaxReadyBytes = 1024
	if s, e := raftlog.Open(cfg); !errors.Is(e, raftlog.ErrLimit) || s != nil {
		t.Fatal(s, e)
	}
}
func TestBoundBootstrapConcurrentExactlyOneWinner(t *testing.T) {
	cfg := boundBootstrapConfig(vfs.NewMem())
	s := boundBootstrapOpen(t, cfg)
	binding := s.ApplicationBinding()
	results := make(chan error, 8)
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() { results <- BootstrapBoundSinglePartition(s, binding, uint64(i+1), [3]uint64{1, 2, 3}) })
	}
	wg.Wait()
	close(results)
	wins := 0
	for e := range results {
		if e == nil {
			wins++
		} else if !errors.Is(e, ErrInvalid) || !errors.Is(e, raftlog.ErrInvalid) {
			t.Fatal(e)
		}
	}
	i, image, e := s.Checkpoint()
	if e != nil || i != 1 || wins != 1 {
		t.Fatal(i, wins, e)
	}
	root, e := DecodeRoot(image)
	if e != nil || root.OwnershipEpoch() < 1 || root.OwnershipEpoch() > 8 || root.SemanticEpoch() != 0 || root.NextPhysicalID() != 1 {
		t.Fatal(root, e)
	}
	usage, e := s.ApplicationUsage()
	if e != nil || usage.RetainedBytes != 275 || usage.RetainedRecords != 3 {
		t.Fatal(usage, e)
	}
}
