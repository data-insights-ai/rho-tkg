package raftlog

import (
	"bytes"
	"encoding/binary"
	"errors"
	"path/filepath"
	"testing"
)

// Authored contract/codec guards only; sparse publication/collection and native
// graph-pin histories require the later operative AS7 packages and qualification.
func TestApplicationRetentionContractMetadataAndLegacyRefusal(t *testing.T) {
	p := DefaultApplicationPolicy(1)
	controls, recovery := ApplicationControlConfig{Version: 1}, ApplicationRecoveryConfig{Version: 2}
	disabled, err := ApplicationContractForPolicyWithRetention(p, controls, recovery, ApplicationRetentionConfig{})
	old, oldErr := ApplicationContractForPolicyWithRecovery(p, controls, recovery)
	if err != nil || oldErr != nil || disabled != old || disabled.Version != 4 {
		t.Fatal("disabled retention changed old contract", disabled, err, oldErr)
	}
	retention := ApplicationRetentionConfig{Version: 1}
	fresh, err := ApplicationContractForPolicyWithRetention(p, controls, recovery, retention)
	want := old
	want.Version = 5
	if err != nil || fresh != want {
		t.Fatal("fresh profile changed numerical quotas", fresh, err)
	}
	if _, err := ApplicationContractForPolicyWithRetention(p, controls, recovery, ApplicationRetentionConfig{Version: 2}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := ApplicationContractForPolicyWithRetention(p, controls, ApplicationRecoveryConfig{Version: 1}, retention); !errors.Is(err, ErrInvalid) {
		t.Fatal("retention borrowed RS1 profile", err)
	}
	var nilStore *Store
	if nilStore.ApplicationRetention() != (ApplicationRetentionConfig{}) {
		t.Fatal("nil retention inspection")
	}
	r := applicationRetentionMetadata{Config: retention, State: ApplicationRetentionState{
		ID: [32]byte{1}, PreviousID: [32]byte{2}, SourceHash: [32]byte{3}, RetiredCutID: [32]byte{4},
		SourceIndex: 20, InstalledIndex: 21, RootFloor: 20, FeedFloor: 21, OutcomeFloor: 20, RecoveryFloor: 20, DataFloor: 20,
		Roots: []ApplicationRetentionRoot{{Index: 1, ImageHash: [32]byte{5}}, {Index: 15, ImageHash: [32]byte{6}, Data: true}},
	}, Cursor: applicationRetentionCursor{Section: 1, Bytes: 30, Hash: [32]byte{7}}}
	wire := appendRetentionMetadata(nil, r)
	got, tail, err := decodeRetentionMetadata(wire, 21)
	if err != nil || len(tail) != 0 || !bytes.Equal(appendRetentionMetadata(nil, got), wire) || len(wire) != retentionMetadataBytes(r) {
		t.Fatal("complete bounded plan/cursor codec", err)
	}
	wire[8+128+56+4+8]++
	if got.State.Roots[0].ImageHash != ([32]byte{5}) || got.State.Roots[1].ImageHash != ([32]byte{6}) || !got.State.Roots[1].Data {
		t.Fatal("decoded anchor borrowed wire or lost data protection")
	}
	oversized := appendRetentionMetadata(nil, r)
	binary.BigEndian.PutUint32(oversized[8+128+56:8+128+56+4], 65)
	if _, _, err := decodeRetentionMetadata(oversized, 21); !errors.Is(err, ErrCorrupt) {
		t.Fatal("oversized anchor allocation", err)
	}
	changed := r
	changed.State.InstalledIndex = 22
	if err := changed.syntax(22); !errors.Is(err, ErrInvalid) {
		t.Fatal("plan skipped its actual installation", err)
	}
	changed = r
	changed.State.SourceIndex, changed.State.InstalledIndex = ^uint64(0), 0
	if err := changed.syntax(0); !errors.Is(err, ErrInvalid) {
		t.Fatal("overflowing installation index", err)
	}
	changed = r
	changed.State.FeedFloor = 22
	if err := changed.syntax(21); !errors.Is(err, ErrInvalid) {
		t.Fatal("future feed floor", err)
	}
	changed = r
	changed.Cursor.Hash = [32]byte{}
	if err := changed.syntax(21); !errors.Is(err, ErrInvalid) {
		t.Fatal("unbound local cursor", err)
	}
	// Fresh configuration is genuinely persisted; the capability cannot be
	// retrofitted on reopen. No store initialization or portable token is asserted.
	cfg := Config{Dir: filepath.Join(t.TempDir(), "retention"), Create: true, Application: p, Controls: controls, Recovery: recovery, Retention: retention,
		SemanticContractID: ApplicationSemanticContractID{9}, Replication: ApplicationReplicationConfig{Voters: [3]uint64{1, 2, 3}},
		Transfer:    ApplicationTransferConfig{Identity: ApplicationIdentity{Graph: [16]byte{1}, Partition: 1, Group: [16]byte{2}}, Contract: fresh, Limits: DefaultApplicationTransferLimits()},
		Generations: ApplicationGenerationLimits{MaxBytes: 2 << 30, MaxRecords: 2_000_000}, PublishedCuts: ApplicationPublishedCutLimits{MaxTransferChunks: 4096},
	}
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if s.ApplicationRetention() != retention {
		t.Fatal("actual configured retention")
	}
	encoded, err := encodeMeta(s.meta)
	if err != nil || !bytes.Equal(encoded[:4], []byte("RLM9")) {
		t.Fatal("fresh metadata profile", err)
	}
	decoded, err := decodeMeta(encoded, s.limits)
	if err != nil || decoded.Retention.Config != retention || len(encoded) != int(metadataBytes(s.meta)) {
		t.Fatal("fresh metadata accounting", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.Create = false
	reopened, err := Open(cfg)
	if err != nil {
		t.Fatal("fresh exact reopen", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.Retention = ApplicationRetentionConfig{}
	if reopened, err := Open(cfg); reopened != nil || !errors.Is(err, ErrInvalid) {
		t.Fatal("legacy reopen acquired retention", reopened, err)
	}
	cfg.Retention = retention
	cfg.Transfer.Contract = old
	if reopened, err := Open(cfg); reopened != nil || !errors.Is(err, ErrInvalid) {
		t.Fatal("old agreement admitted sparse profile", reopened, err)
	}
}
