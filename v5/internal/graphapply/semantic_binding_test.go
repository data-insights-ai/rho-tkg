package graphapply

import (
	"bytes"
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

// This fixed-three storage fixture does not open a Driver or enable traffic.
// Its real GR2 seed is copied from the existing sole-partition bootstrap door.
func boundMaterializerStore(t *testing.T, id raftlog.ApplicationSemanticContractID, identity raftlog.ApplicationIdentity, image []byte) (*raftlog.Store, raftlog.Config) {
	t.Helper()
	p := raftlog.DefaultApplicationPolicy(1)
	cfg := raftlog.Config{Dir: "bound", FS: vfs.NewMem(), Create: true, Application: p,
		Transfer:      raftlog.ApplicationTransferConfig{Identity: identity, Contract: raftlog.ApplicationContractForPolicy(p), Limits: raftlog.DefaultApplicationTransferLimits()},
		Generations:   raftlog.ApplicationGenerationLimits{MaxBytes: 2 << 30, MaxRecords: 2_000_000},
		PublishedCuts: raftlog.ApplicationPublishedCutLimits{MaxTransferChunks: 1_000_000},
		Replication:   raftlog.ApplicationReplicationConfig{Voters: [3]uint64{1, 2, 3}}, SemanticContractID: id}
	s, err := raftlog.Open(cfg)
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
	return s, cfg
}

func TestMaterializerSemanticBindingBeforeRestoreAndPreservesStore(t *testing.T) {
	seed := openGraphFixture(t)
	_, image, err := seed.s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	identity := raftlog.ApplicationIdentity{Graph: [16]byte(seed.n.graph), Partition: seed.n.partition, Group: [16]byte{77}}
	for _, name := range []string{"matching", "semantic-mismatch", "graph-mismatch", "partition-mismatch", "matching-malformed"} {
		t.Run(name, func(t *testing.T) {
			id, scope, stored := SemanticContractID(), identity, image
			var expected error
			switch name {
			case "semantic-mismatch":
				id[0] ^= 1
				stored = []byte("malformed-root")
				expected = replica.ErrInvalid
			case "graph-mismatch":
				scope.Graph[1] ^= 1
				stored = []byte("malformed-root")
				expected = graphstore.ErrNamespace
			case "partition-mismatch":
				scope.Partition++
				stored = []byte("malformed-root")
				expected = graphstore.ErrNamespace
			case "matching-malformed":
				stored = []byte("malformed-root")
				expected = graphstore.ErrCorrupt
			}
			s, cfg := boundMaterializerStore(t, id, scope, stored)
			beforeIndex, beforeImage, err := s.Checkpoint()
			if err != nil {
				t.Fatal(err)
			}
			beforeGeneration, beforeBinding := s.ApplicationGeneration(), s.ApplicationBinding()
			beforeUsage, err := s.ApplicationTransferUsage()
			if err != nil {
				t.Fatal(err)
			}
			m, err := newMaterializer(s, seed.n, 1, defaultMaterializerLimits())
			if expected != nil {
				if m != nil || !errors.Is(err, expected) {
					t.Fatal(m, err)
				}
				if name == "semantic-mismatch" && !errors.Is(err, errInvalid) {
					t.Fatal("factory sentinel lost", err)
				}
				if name != "matching-malformed" && errors.Is(err, graphstore.ErrCorrupt) {
					t.Fatal("Restore ran before binding refusal", err)
				}
			} else if err != nil || m == nil {
				t.Fatal(err)
			}
			afterIndex, afterImage, err := s.Checkpoint()
			if err != nil || beforeIndex != afterIndex || !bytes.Equal(beforeImage, afterImage) || s.ApplicationGeneration() != beforeGeneration || s.ApplicationBinding() != beforeBinding {
				t.Fatal("factory changed durable state", err)
			}
			afterUsage, err := s.ApplicationTransferUsage()
			if err != nil || beforeUsage != afterUsage {
				t.Fatal("factory leaked handle or changed usage", beforeUsage, afterUsage, err)
			}
			v, err := s.ApplicationView(afterIndex)
			if err != nil {
				t.Fatal("refusal poisoned store", err)
			}
			root, err := v.Root()
			if err != nil || root.Generation != beforeGeneration || !bytes.Equal(root.Image, stored) {
				t.Fatal(root, err)
			}
			if err := v.Close(); err != nil {
				t.Fatal(err)
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
			if reopened.ApplicationBinding() != beforeBinding || reopened.ApplicationGeneration() != beforeGeneration {
				t.Fatal("reopen changed binding/generation")
			}
			if _, err := newMaterializer(reopened, seed.n, 1, defaultMaterializerLimits()); expected != nil && !errors.Is(err, expected) || expected == nil && err != nil {
				t.Fatal("reopen factory", err)
			}
		})
	}
}

func TestMaterializerLegacyBindingCompatibilityAndClosedSentinel(t *testing.T) {
	f := openGraphFixture(t)
	if f.s.ApplicationBinding() != (raftlog.ApplicationBinding{}) {
		t.Fatal("legacy fixture acquired binding")
	}
	index, image, err := f.s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	m, err := newMaterializer(f.s, f.n, 1, defaultMaterializerLimits())
	if err != nil || m == nil {
		t.Fatal(err)
	}
	after, unchanged, err := f.s.Checkpoint()
	if err != nil || after != index || !bytes.Equal(image, unchanged) || f.s.ApplicationGeneration() != 0 {
		t.Fatal("legacy factory changed state", err)
	}
	if err := f.s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := newMaterializer(f.s, f.n, 1, defaultMaterializerLimits()); !errors.Is(err, raftlog.ErrClosed) {
		t.Fatal(err)
	}
}
