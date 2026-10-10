package raftlog

import (
	"bytes"
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestControlPublishedCaptureExcludesLaterJournalAndCountsWork(t *testing.T) {
	s, _ := newControlStore(t, nil, 1)
	installControl(t, s, "seed", ApplicationControlPut{Key: []byte("reg"), Value: []byte("original")})
	if err := s.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	cut, _ := s.PublishedApplicationCut()
	e, err := s.BeginPublishedApplicationExport(t.Context(), cut.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	installControl(t, s, "later", ApplicationControlPut{Key: []byte("dec"), Value: []byte("later-decision")})
	if err := s.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	m := buildPublished(t, e, ReadBudget{1, 4096})
	stream := collectPublished(t, e, ReadBudget{1, 4096})
	if m.Index != 2 || m.ControlRecords != 1 || m.CutID != cut.ID || !bytes.Equal(m.Image, []byte("seed")) {
		t.Fatal("capture moved", m)
	}
	for _, c := range stream {
		if err := walkSnapshotChunk(c.Data, m.Contract, func(k, raw []byte) error {
			if k[0] == 18 {
				key, index, err := decodeControlKey(0, k, 1024)
				if err != nil || string(key) != "reg" || index != 2 {
					t.Fatal("future journal leaked", key, index, err)
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := e.Next(t.Context(), ReadBudget{1, 4096}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}
func TestControlIndependentEmptyProofRejectsAnyGraphKVAndStaleRoot(t *testing.T) {
	s, _ := newControlStore(t, vfs.NewMem(), 1)
	installControl(t, s, "seed", ApplicationControlPut{Key: []byte("reg"), Value: []byte("original")})
	v := viewApplication(t, s, 2)
	if _, err := v.ProveNoApplicationData(t.Context()); err != nil {
		t.Fatal(err)
	}
	index, b := controlBatch(t, s, "seed", nil, KV{Key: []byte("graph"), Deleted: true})
	if err := s.InstallApplication(index, b); err != nil {
		t.Fatal(err)
	}
	now := viewApplication(t, s, index)
	if _, err := now.ProveNoApplicationData(t.Context()); !errors.Is(err, ErrInvalid) {
		t.Fatal("tombstone ignored", err)
	}
	if _, err := v.ProveNoApplicationData(t.Context()); !errors.Is(err, ErrInvalid) {
		t.Fatal("stale empty proof accepted", err)
	}
}
func TestControlFreshAndReopenModeNeverRetrofits(t *testing.T) {
	s, cfg := newControlStore(t, nil, 1)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.Create = false
	bad := cfg
	bad.Controls.Version = 2
	if _, err := Open(bad); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	r, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if r.ApplicationControls().Version != 1 {
		t.Fatal("mode lost")
	}
	for _, edit := range []func(*Config){func(c *Config) { c.SemanticContractID = ApplicationSemanticContractID{} }, func(c *Config) { c.Replication = ApplicationReplicationConfig{} }, func(c *Config) { c.Transfer.Contract.Version = 1 }} {
		c := cfg
		c.Create = true
		c.FS = vfs.NewMem()
		edit(&c)
		if _, err := Open(c); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid optin admitted", err)
		}
	}
}

func TestControlTwoBankRetirementKeepsCapturedEvidenceAndDeletesDisjointRange(t *testing.T) {
	s, _ := newControlStore(t, nil, 1)
	installControl(t, s, "seed", ApplicationControlPut{Key: []byte("r"), Value: []byte("old")})
	if err := s.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	old := viewApplication(t, s, 2)
	cut, err := s.PublishedApplicationCut()
	if err != nil {
		t.Fatal(err)
	}
	export, err := s.BeginPublishedApplicationExport(t.Context(), cut.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer export.Close()
	oldManifest := buildPublished(t, export, ReadBudget{1, 4096})
	donor, _ := newControlStore(t, nil, 2)
	installControl(t, donor, "seed", ApplicationControlPut{Key: []byte("r"), Value: []byte("new")})
	installControl(t, donor, "seed", ApplicationControlPut{Key: []byte("d"), Value: []byte("decision")})
	if err := donor.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	m, chunks := controlStream(t, donor)
	i, err := s.BeginApplicationImport(t.Context(), m)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range chunks {
		if err := i.Append(t.Context(), c); err != nil {
			t.Fatal(err)
		}
	}
	if err := i.Verify(t.Context()); err != nil {
		t.Fatal(err)
	}
	p, err := i.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := EncodeApplicationSnapshotDescriptor(m)
	if err != nil {
		t.Fatal(err)
	}
	snap := &pb.Snapshot{Data: descriptor, Metadata: &pb.SnapshotMetadata{Index: new(m.Index), Term: new(m.Term), ConfState: m.ConfState}}
	claim, err := s.ClaimPreparedApplicationSnapshot(p, snap)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.PersistApplicationReady(raft.Ready{Snapshot: snap, HardState: &pb.HardState{Term: new(uint64(5)), Commit: new(m.Index)}}, claim); err != nil {
		t.Fatal(err)
	}
	if err := claim.Close(); err != nil {
		t.Fatal(err)
	}
	now := viewApplication(t, s, 3)
	for _, tc := range []struct {
		view *ApplicationView
		want string
	}{{old, "old"}, {now, "new"}} {
		row, found, _, err := tc.view.GetControl(t.Context(), []byte("r"), ReadBudget{2, 4096})
		if err != nil || !found || string(row.Value) != tc.want || row.Index != 2 {
			t.Fatal("physical generation substituted", row, found, err)
		}
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	before := readyMetadataFingerprint(t, s)
	if next, err := s.BeginApplicationImport(t.Context(), m); next != nil || !errors.Is(err, ErrLimit) || readyMetadataFingerprint(t, s) != before {
		t.Fatal("export-pinned retired bank reused", err)
	}
	if s.meta.Gen.Banks[0].State != bankRetired || s.meta.Gen.Banks[0].ControlRecords != 1 {
		t.Fatal("retired bank lost control ledgers", s.meta.Gen)
	}
	seen := 0
	for _, c := range collectPublished(t, export, ReadBudget{1, 4096}) {
		if err := walkSnapshotChunk(c.Data, oldManifest.Contract, func(k, raw []byte) error {
			if k[0] == 18 {
				seen++
				value, deleted, err := inspectAppFrame(k, raw, 1024)
				if err != nil || deleted || string(value) != "old" {
					t.Fatal("captured control changed", value, deleted, err)
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if seen != 1 {
		t.Fatal("old captured control omitted", seen)
	}
	if err := export.Close(); err != nil {
		t.Fatal(err)
	}
	if _, closer, err := s.db.Get(controlVersionKey(0, []byte("r"), 2)); !errors.Is(err, pebble.ErrNotFound) {
		if closer != nil {
			_ = closer.Close()
		}
		t.Fatal("retired tag18 survived last owner", err)
	}
	i, err = s.BeginApplicationImport(t.Context(), m)
	if err != nil || i.bank != 0 || i.generation != 3 {
		t.Fatal("freed bank not reusable", err)
	}
	for _, c := range chunks {
		if err := i.Append(t.Context(), c); err != nil {
			t.Fatal(err)
		}
	}
	if err := i.Verify(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := i.Abort(); err != nil {
		t.Fatal(err)
	}
	if _, closer, err := s.db.Get(controlVersionKey(0, []byte("d"), 3)); !errors.Is(err, pebble.ErrNotFound) {
		if closer != nil {
			_ = closer.Close()
		}
		t.Fatal("aborted tag18 survived", err)
	}
	row, found, _, err := now.GetControl(t.Context(), []byte("d"), ReadBudget{2, 4096})
	if err != nil || !found || string(row.Value) != "decision" || row.Index != 3 {
		t.Fatal("tag18 cleanup erased active tag19", row, found, err)
	}
	if err := s.ScrubApplication(t.Context()); err != nil {
		t.Fatal(err)
	}
}
