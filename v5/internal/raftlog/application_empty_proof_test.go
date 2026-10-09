package raftlog

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
)

func TestProveNoApplicationDataFreshNoopsAndOwnedOutput(t *testing.T) {
	for _, generations := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "generation"}[generations], func(t *testing.T) {
			var s *Store
			if generations {
				s = generationStore(t, vfs.NewMem())
			} else {
				s = openApplication(t, vfs.NewMem(), DefaultApplicationPolicy(1))
			}
			v := viewApplication(t, s, 1)
			proof, err := v.ProveNoApplicationData(t.Context())
			if err != nil || proof.Index != 1 || cap(proof.Image) != len(proof.Image) || generations && proof.Generation != 1 {
				t.Fatal(proof, err)
			}
			proof.Image[0] ^= 1
			again, err := v.ProveNoApplicationData(t.Context())
			if err != nil || string(again.Image) != "initial" {
				t.Fatal("proof aliases view", err)
			}
			// Real empty normal Raft entries retain complete triples, but no KV versions.
			persist(t, s, 2, 2, ent(2, 2, ""))
			if err := s.InstallApplication(2, ApplicationBatch{BaseGeneration: again.Generation, BaseIndex: again.Index, BaseImageHash: again.ImageHash, Image: again.Image}); err != nil {
				t.Fatal(err)
			}
			if _, err := v.ProveNoApplicationData(t.Context()); !errors.Is(err, ErrInvalid) || s.poison != nil {
				t.Fatal("stale proof poisoned", err)
			}
			current := viewApplication(t, s, 2)
			if got, err := current.ProveNoApplicationData(t.Context()); err != nil || got.Index != 2 {
				t.Fatal("noop prevented empty proof", got, err)
			}
			if err := s.ScrubApplication(t.Context()); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestProveNoApplicationDataRejectsHiddenVersionsAndTombstones(t *testing.T) {
	for _, deleted := range []bool{false, true} {
		s := openApplication(t, vfs.NewMem(), DefaultApplicationPolicy(1))
		index := applyApplication(t, s, "initial", KV{Key: []byte("unrelated-hidden-key"), Deleted: deleted})
		v := viewApplication(t, s, index)
		if got, err := v.ProveNoApplicationData(t.Context()); !errors.Is(err, ErrInvalid) || got.Index != 0 || s.poison != nil {
			t.Fatal("hidden data accepted/poisoned", got, err)
		}
		if _, err := v.Root(); err != nil {
			t.Fatal("ordinary rejection poisoned", err)
		}
	}
}
func TestProveNoApplicationDataNilCancellationClosedAndGenerationABA(t *testing.T) {
	var nilView *ApplicationView
	if _, err := nilView.ProveNoApplicationData(t.Context()); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	s := generationStore(t, vfs.NewMem())
	v := viewApplication(t, s, 1)
	//nolint:staticcheck // Direct nil-context contract test must exercise rejection.
	if _, err := v.ProveNoApplicationData(nil); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := v.ProveNoApplicationData(ctx); !errors.Is(err, context.Canceled) || s.poison != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	s.meta.Gen.Banks[0].Generation++
	s.meta.Gen.HighWater++
	s.mu.Unlock()
	if _, err := v.ProveNoApplicationData(t.Context()); !errors.Is(err, ErrClosed) || s.poison != nil {
		t.Fatal("same-index/hash generation reused", err)
	}
	s.mu.Lock()
	s.meta.Gen.Banks[0].Generation--
	s.meta.Gen.HighWater--
	s.mu.Unlock()
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := v.ProveNoApplicationData(t.Context()); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}
func TestProveNoApplicationDataCorruptLedgerAndImage(t *testing.T) {
	for _, mutation := range []string{"under-count", "overflow", "through", "image-length", "hash"} {
		t.Run(mutation, func(t *testing.T) {
			s := openApplication(t, vfs.NewMem(), DefaultApplicationPolicy(1))
			v := viewApplication(t, s, 1)
			switch mutation {
			case "under-count":
				s.meta.App.Records = 2
			case "overflow":
				s.meta.App.Through = math.MaxUint64
				s.meta.Applied = math.MaxUint64
				v.index = math.MaxUint64
			case "through":
				s.meta.App.Through++
			case "image-length":
				s.meta.ImageBytes++
			case "hash":
				v.image[0] ^= 1
			}
			_, err := v.ProveNoApplicationData(t.Context())
			// A hash mismatch is an ordinary stale-base refusal, not a verified I/O error.
			if mutation == "hash" {
				if !errors.Is(err, ErrInvalid) || s.poison != nil {
					t.Fatal(err)
				}
				return
			}
			if !errors.Is(err, ErrCorrupt) || s.poison == nil {
				t.Fatal("bad ledger accepted/unpoisoned", err)
			}
		})
	}
}
