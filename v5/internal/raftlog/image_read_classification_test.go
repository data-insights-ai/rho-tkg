package raftlog

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/cockroachdb/pebble/v2/vfs/errorfs"
)

// Adapted from the reviewed independent image-read-classification SST fixture:
// actual flushed/reopened SST reads, a one-byte cache and a counting ReadAt
// injector. The external evidence/patch files remain unchanged and unapplied.
func imageReadSSTFixture(t *testing.T) (*Store, Config, ApplicationCutReference, []byte, *errorfs.Toggle, *atomic.Int64, error) {
	t.Helper()
	cause := errors.New("image read operational SST failure")
	var reads atomic.Int64
	toggle := &errorfs.Toggle{Injector: errorfs.InjectorFunc(func(op errorfs.Op) error {
		if op.Kind == errorfs.OpFileReadAt && strings.HasSuffix(op.Path, ".sst") {
			reads.Add(1)
			return cause
		}
		return nil
	})}
	fs := errorfs.Wrap(vfs.NewMem(), toggle)
	p, tc := transferConfig(1)
	p.ReclaimEntries = 1
	l := DefaultLimits()
	l.CacheBytes = 1
	cfg := Config{Dir: "db", FS: fs, Create: true, Limits: l, Application: p, Transfer: tc, Generations: ApplicationGenerationLimits{2 << 30, 2000000}, PublishedCuts: ApplicationPublishedCutLimits{1000000}}
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		toggle.Off()
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := s.Initialize([]uint64{1}, []byte("initial")); err != nil {
		t.Fatal(err)
	}
	image := make([]byte, 32<<10)
	for j := range image {
		image[j] = byte((j*73 + j/251) % 251)
	}
	generationApply(t, s, string(image), KV{Key: []byte("a"), Value: []byte("retained")})
	if err := s.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	cut, err := s.PublishedApplicationCut()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.Create = false
	s, err = Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	names, err := fs.List("db")
	if err != nil {
		t.Fatal(err)
	}
	sst := false
	for _, name := range names {
		sst = sst || strings.HasSuffix(name, ".sst")
	}
	if !sst {
		t.Fatal("no actual SST in fixture")
	}
	return s, cfg, cut, image, toggle, &reads, cause
}
func imageReadCall(t *testing.T, s *Store, cut ApplicationCutReference, door string) error {
	t.Helper()
	switch door {
	case "snapshot":
		out, err := s.Snapshot()
		if out != nil {
			t.Errorf("failed image read returned Snapshot: %v", out)
		}
		return err
	case "checkpoint":
		index, image, err := s.Checkpoint()
		if index != 0 || image != nil {
			t.Errorf("failed image read returned checkpoint: %d", index)
		}
		return err
	case "export":
		e, err := s.BeginPublishedApplicationExport(t.Context(), cut.ID)
		if e != nil {
			t.Cleanup(func() { _ = e.Close() })
			t.Error("failed image read returned export")
		}
		return err
	default:
		t.Fatal("unknown read door")
		return ErrInvalid
	}
}
func TestImageReadOperationalSSTFailurePreservesClassificationAndOwnership(t *testing.T) {
	for _, door := range []string{"snapshot", "checkpoint", "export"} {
		t.Run(door, func(t *testing.T) {
			s, cfg, cut, image, toggle, count, cause := imageReadSSTFixture(t)
			view := viewApplication(t, s, 2)
			defer view.Close()
			before := readyMetadataFingerprint(t, s)
			usage, _ := s.ApplicationTransferUsage()
			views, viewBytes := s.views, s.viewBytes
			refs := s.generationRefs[0].refs
			toggle.On()
			err := imageReadCall(t, s, cut, door)
			toggle.Off()
			callErr := err
			if count.Load() == 0 {
				t.Fatal("actual SST ReadAt fault did not fire")
			}
			if !errors.Is(err, cause) || !errors.Is(s.poison, cause) {
				t.Fatal("original IO cause lost", err, s.poison)
			}
			if s.pinnedApplicationBytes != usage.PinnedLogicalBytes || len(s.applicationExports) != usage.Exports || s.views != views || s.viewBytes != viewBytes || s.generationRefs[0].refs != refs || readyMetadataFingerprint(t, s) != before {
				t.Fatal("failed read changed durable bytes or ownership")
			}
			_, later := s.LastIndex()
			if !errors.Is(later, ErrPoisoned) || !errors.Is(later, cause) {
				t.Fatal("fail-stop policy changed", later)
			}
			t.Logf("door=%s SST reads=%d first=%v poison=%v follow-up=%v", door, count.Load(), callErr, s.poison, later)
			classified := errors.Is(err, ErrCorrupt) || errors.Is(s.poison, ErrCorrupt) || errors.Is(later, ErrCorrupt)
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			if err := view.Close(); err != nil {
				t.Fatal(err)
			}
			recovered, err := Open(cfg)
			if err != nil {
				t.Fatal("fault-free reopen", err)
			}
			defer recovered.Close()
			if readyMetadataFingerprint(t, recovered) != before {
				t.Fatal("read fault changed recovered bytes")
			}
			index, actual, err := recovered.Checkpoint()
			if err != nil || index != 2 || !bytes.Equal(actual, image) {
				t.Fatal(index, err)
			}
			v := viewApplication(t, recovered, 2)
			row, found, err := v.Get(t.Context(), []byte("a"), 256)
			if err != nil || !found || row.Deleted || string(row.Value) != "retained" {
				t.Fatal(row, found, err)
			}
			if classified {
				t.Errorf("operational SST failure was labeled corruption: %v", callErr)
			}
		})
	}
}
func TestImageReadStoredCorruptionRetainsDiagnosis(t *testing.T) {
	for _, door := range []string{"snapshot", "checkpoint", "export"} {
		for _, fault := range []string{"missing", "hash", "length"} {
			t.Run(door+"/"+fault, func(t *testing.T) {
				s, _, cut, image, toggle, count, _ := imageReadSSTFixture(t)
				toggle.Off()
				key := snapshotKey
				if door == "checkpoint" {
					key = imageKey
				}
				switch fault {
				case "missing":
					if err := s.db.Delete(key, pebble.Sync); err != nil {
						t.Fatal(err)
					}
				case "hash":
					bad := bytes.Clone(image)
					bad[len(bad)-1] ^= 1
					if err := s.db.Set(key, bad, pebble.Sync); err != nil {
						t.Fatal(err)
					}
				case "length":
					if err := s.db.Set(key, image[:len(image)-1], pebble.Sync); err != nil {
						t.Fatal(err)
					}
				}
				before := readyMetadataFingerprint(t, s)
				err := imageReadCall(t, s, cut, door)
				if !errors.Is(err, ErrCorrupt) || !errors.Is(s.poison, ErrCorrupt) || count.Load() != 0 || readyMetadataFingerprint(t, s) != before || s.pinnedApplicationBytes != 0 || len(s.applicationExports) != 0 {
					t.Fatal("stored corruption diagnosis or refusal changed", err)
				}
			})
		}
	}
}

func TestImageReadFailureClassifierPreservesBackendAndOperationalCauses(t *testing.T) {
	cause := errors.New("original image read cause")
	//nolint:staticcheck // SA1019: author a backend marker only for the pure predicate control; never inject it into SST IO.
	marked := errors.Join(pebble.ErrCorruption, cause)
	for _, tc := range []struct {
		name       string
		err, cause error
		corrupt    bool
	}{
		{"nil", nil, nil, false}, {"operational", cause, cause, false}, {"wrapped", fmt.Errorf("read: %w", cause), cause, false}, {"missing", pebble.ErrNotFound, pebble.ErrNotFound, true}, {"wrapped missing", fmt.Errorf("read: %w", pebble.ErrNotFound), pebble.ErrNotFound, true}, {"backend marked", fmt.Errorf("backend: %w", marked), cause, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := imageReadFailure(tc.err)
			if !errors.Is(got, tc.cause) || errors.Is(got, ErrCorrupt) != tc.corrupt {
				t.Fatal(got)
			}
			if !tc.corrupt && got != tc.err {
				t.Fatal("operational error altered", got, tc.err)
			}
			if pebble.IsCorruptionError(got) != pebble.IsCorruptionError(tc.err) {
				t.Fatal("backend marker lost", got)
			}
		})
	}
}
