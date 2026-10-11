package raftlog

import (
	"bytes"
	"errors"
	"testing"
	"unsafe"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// A genuine prepared AS4 audit must be checked before readers release for Claim.
func TestPreparedSnapshotCheckPermitsLiveAS4AuditReaders(t *testing.T) {
	s, p, expected := preparedReadFixture(t, 4, nil)
	at := expected.GetMetadata().GetIndex()
	current, err := p.OpenReadView(t.Context(), at, 8)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = current.Close() })
	history, err := p.OpenReadView(t.Context(), 1, 8)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = history.Close() })
	root, err := current.RootBounded(t.Context(), 8)
	if err != nil || root.Index != at || string(root.Image) != "r3" {
		t.Fatal("genuine prepared current reader", root, err)
	}
	old, err := history.RootBounded(t.Context(), 8)
	if err != nil || old.Index != 1 || string(old.Image) != "seed" {
		t.Fatal("genuine prepared historical reader", old, err)
	}
	control, found, _, err := current.GetControl(t.Context(), []byte{'A', 0}, ReadBudget{Rows: 2, Bytes: 4096})
	if err != nil || !found || control.Index != 2 || string(control.Value) != "reg" {
		t.Fatal("AS4 control evidence", control, found, err)
	}
	before, err := s.ApplicationTransferUsage()
	if err != nil || before.Prepared != 1 || before.Claims != 0 || p.readers != 2 || p.claim != nil {
		t.Fatal("prepared-reader ownership baseline", before, err)
	}
	refs, refValues := s.generationRefs, [2]generationRef{}
	for i, ref := range refs {
		if ref != nil {
			refValues[i] = *ref
		}
	}
	input := proto.Clone(expected)
	descriptor := bytes.Clone(p.descriptor)
	fingerprint := readyMetadataFingerprint(t, s)
	if claim, err := s.ClaimPreparedApplicationSnapshot(p, expected); claim != nil || !errors.Is(err, ErrLimit) {
		t.Fatal("Claim must still refuse live audit readers", claim, err)
	}
	afterClaim, err := s.ApplicationTransferUsage()
	if err != nil || afterClaim != before || p.readers != 2 || p.claim != nil {
		t.Fatal("Claim refusal changed ownership", afterClaim, err)
	}
	t.Log("genuine AS4 current/history/control readers live; Claim refused; ownership unchanged")
	if err := s.CheckPreparedApplicationSnapshot(p, expected); err != nil {
		if errors.Is(err, raft.ErrUnavailable) {
			t.Fatalf("preclaim snapshot check unavailable with genuine live AS4 audit readers: %v", err)
		}
		t.Fatalf("preclaim snapshot check refused genuine live AS4 audit readers: %v", err)
	}
	after, err := s.ApplicationTransferUsage()
	if err != nil || after != before || p.readers != 2 || p.claim != nil || len(s.applicationSnapshotClaims) != 0 || s.generationRefs != refs {
		t.Fatal("nonmutating check changed owners or pins", after, err)
	}
	for i, ref := range refs {
		if ref != nil && *ref != refValues[i] {
			t.Fatal("nonmutating check changed generation reference", i)
		}
	}
	if !proto.Equal(input, expected) || !bytes.Equal(descriptor, p.descriptor) || fingerprint != readyMetadataFingerprint(t, s) {
		t.Fatal("nonmutating check changed snapshot, descriptor or stored bytes")
	}
	if got, err := current.RootBounded(t.Context(), 8); err != nil || !bytes.Equal(got.Image, root.Image) {
		t.Fatal("check consumed the audit reader", got, err)
	}
}

func TestPreparedSnapshotCheckExactMetadataAndBounds(t *testing.T) {
	s, p, expected := preparedReadFixture(t, 4, nil)
	before, err := s.ApplicationTransferUsage()
	if err != nil {
		t.Fatal(err)
	}
	descriptor := bytes.Clone(p.descriptor)
	fingerprint := readyMetadataFingerprint(t, s)
	cases := []struct {
		name string
		edit func(*pb.Snapshot)
		want error
	}{
		{"descriptor-limit", func(x *pb.Snapshot) { x.Data = make([]byte, 513) }, ErrLimit},
		{"descriptor-mismatch", func(x *pb.Snapshot) { x.Data[0] ^= 1 }, ErrInvalid},
		{"nil-metadata", func(x *pb.Snapshot) { x.Metadata = nil }, ErrInvalid},
		{"nil-confstate", func(x *pb.Snapshot) { x.Metadata.ConfState = nil }, ErrInvalid},
		{"absent-index", func(x *pb.Snapshot) { x.Metadata.Index = nil }, ErrInvalid},
		{"absent-term", func(x *pb.Snapshot) { x.Metadata.Term = nil }, ErrInvalid},
		{"wrong-index", func(x *pb.Snapshot) { x.Metadata.Index = new(uint64(1)) }, ErrInvalid},
		{"wrong-term", func(x *pb.Snapshot) { x.Metadata.Term = new(uint64(99)) }, ErrInvalid},
		{"explicit-false-presence", func(x *pb.Snapshot) { x.Metadata.ConfState.AutoLeave = new(false) }, ErrInvalid},
		{"voter-mismatch", func(x *pb.Snapshot) { x.Metadata.ConfState.Voters[2] = 4 }, ErrInvalid},
		{"learner-mismatch", func(x *pb.Snapshot) { x.Metadata.ConfState.Learners = []uint64{4} }, ErrInvalid},
		{"snapshot-unknown", func(x *pb.Snapshot) { x.ProtoReflect().SetUnknown([]byte{0x20, 1}) }, ErrInvalid},
		{"metadata-unknown", func(x *pb.Snapshot) { x.Metadata.ProtoReflect().SetUnknown([]byte{0x20, 1}) }, ErrInvalid},
		{"confstate-unknown", func(x *pb.Snapshot) { x.Metadata.ConfState.ProtoReflect().SetUnknown([]byte{0x30, 1}) }, ErrInvalid},
		{"large-list", func(x *pb.Snapshot) { x.Metadata.ConfState.Voters = make([]uint64, 4096) }, ErrLimit},
		{"large-metadata-unknown", func(x *pb.Snapshot) { x.Metadata.ProtoReflect().SetUnknown(bytes.Repeat([]byte{0x20, 1}, 65)) }, ErrLimit},
		{"large-confstate-unknown", func(x *pb.Snapshot) {
			x.Metadata.ConfState.ProtoReflect().SetUnknown(bytes.Repeat([]byte{0x30, 1}, 65))
		}, ErrLimit},
		{"combined-unknown", func(x *pb.Snapshot) {
			x.Metadata.ProtoReflect().SetUnknown(bytes.Repeat([]byte{0x20, 1}, 32))
			x.Metadata.ConfState.ProtoReflect().SetUnknown(bytes.Repeat([]byte{0x30, 1}, 33))
		}, ErrLimit},
		{"four-list-total", func(x *pb.Snapshot) {
			x.Metadata.ConfState = &pb.ConfState{Voters: make([]uint64, 32), Learners: make([]uint64, 32), VotersOutgoing: make([]uint64, 32), LearnersNext: make([]uint64, 33)}
		}, ErrLimit},
		{"lower-bound-128-with-framing", func(x *pb.Snapshot) {
			x.Metadata.ProtoReflect().SetUnknown(bytes.Repeat([]byte{0x20, 1}, 16))
			x.Metadata.ConfState = &pb.ConfState{Voters: make([]uint64, 24), Learners: make([]uint64, 24), VotersOutgoing: make([]uint64, 16), LearnersNext: make([]uint64, 16)}
			x.Metadata.ConfState.ProtoReflect().SetUnknown(bytes.Repeat([]byte{0x30, 1}, 8))
		}, ErrLimit},
		{"bounded-list-large-varints", func(x *pb.Snapshot) {
			x.Metadata.ConfState.Voters = make([]uint64, 16)
			for i := range x.Metadata.ConfState.Voters {
				x.Metadata.ConfState.Voters[i] = ^uint64(0)
			}
		}, ErrLimit},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := proto.Clone(expected).(*pb.Snapshot)
			tc.edit(input)
			unchanged := proto.Clone(input)
			if err := s.CheckPreparedApplicationSnapshot(p, input); !errors.Is(err, tc.want) {
				t.Fatal("Check", err, tc.want)
			}
			if claim, err := s.ClaimPreparedApplicationSnapshot(p, input); claim != nil || !errors.Is(err, tc.want) {
				t.Fatal("Claim", claim, err, tc.want)
			}
			after, err := s.ApplicationTransferUsage()
			if err != nil || after != before || p.claim != nil || p.readers != 0 || !proto.Equal(input, unchanged) || !bytes.Equal(p.descriptor, descriptor) {
				t.Fatal("refusal changed input or ownership", after, err)
			}
		})
	}
	if fingerprint != readyMetadataFingerprint(t, s) {
		t.Fatal("refusals changed stored metadata")
	}
	if err := s.CheckPreparedApplicationSnapshot(p, expected); err != nil {
		t.Fatal("refusals consumed prepared capability", err)
	}
}

func TestPreparedSnapshotCheckIdentityLifetimeAndClaimOrdering(t *testing.T) {
	s, p, expected := preparedReadFixture(t, 4, nil)
	foreign, _, _ := preparedReadFixture(t, 4, nil)
	oversized := proto.Clone(expected).(*pb.Snapshot)
	oversized.Data = make([]byte, 513)
	copied := *p
	for _, tc := range []struct {
		name     string
		store    *Store
		prepared *PreparedApplicationSnapshot
		snapshot *pb.Snapshot
	}{
		{"nil-store", nil, p, expected}, {"nil-prepared", s, nil, expected},
		{"foreign-before-size", foreign, p, oversized}, {"copied-before-size", s, &copied, oversized},
		{"empty-prepared", s, &PreparedApplicationSnapshot{}, expected}, {"nil-expected", s, p, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.store.CheckPreparedApplicationSnapshot(tc.prepared, tc.snapshot); !errors.Is(err, ErrInvalid) {
				t.Fatal(err)
			}
		})
	}
	before, err := s.ApplicationTransferUsage()
	if err != nil {
		t.Fatal(err)
	}
	claim, err := s.ClaimPreparedApplicationSnapshot(p, expected)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := claim.Close(); err != nil {
			t.Error(err)
		}
	})
	claimed, err := s.ApplicationTransferUsage()
	if err != nil || claimed.Claims != 1 {
		t.Fatal(claimed, err)
	}
	for _, input := range []*pb.Snapshot{expected, oversized} {
		if err := s.CheckPreparedApplicationSnapshot(p, input); !errors.Is(err, ErrLimit) {
			t.Fatal("active claim", err)
		}
		after, err := s.ApplicationTransferUsage()
		if err != nil || after != claimed || p.claim != claim {
			t.Fatal("check changed claim", after, err)
		}
	}
	if err := claim.Close(); err != nil {
		t.Fatal(err)
	}
	restored, err := s.ApplicationTransferUsage()
	if err != nil || restored != before {
		t.Fatal("claim release", restored, err)
	}
	if err := s.CheckPreparedApplicationSnapshot(p, expected); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.CheckPreparedApplicationSnapshot(p, oversized); !errors.Is(err, ErrClosed) {
		t.Fatal("closed before size", err)
	}
	closedStore, live, snapshot := preparedReadFixture(t, 4, nil)
	if err := closedStore.Close(); err != nil {
		t.Fatal(err)
	}
	if err := closedStore.CheckPreparedApplicationSnapshot(live, snapshot); !errors.Is(err, ErrClosed) {
		t.Fatal("closed store", err)
	}
}

func TestPreparedSnapshotCheckReaderReleaseAndZeroOwnership(t *testing.T) {
	s, p, expected := preparedReadFixture(t, 4, nil)
	current, err := p.OpenReadView(t.Context(), expected.GetMetadata().GetIndex(), 8)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := current.Close(); err != nil {
			t.Error(err)
		}
	})
	history, err := p.OpenReadView(t.Context(), 1, 8)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := history.Close(); err != nil {
			t.Error(err)
		}
	})
	before, err := s.ApplicationTransferUsage()
	if err != nil || before.PinnedLogicalBytes != 3078 || s.views != 2 || s.viewBytes != 6 {
		t.Fatal(before, err)
	}
	if err := s.CheckPreparedApplicationSnapshot(p, expected); err != nil {
		t.Fatal(err)
	}
	if claim, err := s.ClaimPreparedApplicationSnapshot(p, &pb.Snapshot{}); claim != nil || !errors.Is(err, ErrLimit) {
		t.Fatal("readers before malformed match", claim, err)
	}
	after, err := s.ApplicationTransferUsage()
	if err != nil || after != before || p.readers != 2 {
		t.Fatal("reader check ownership", after, err)
	}
	if err := current.Close(); err != nil {
		t.Fatal(err)
	}
	if err := history.Close(); err != nil {
		t.Fatal(err)
	}
	released, err := s.ApplicationTransferUsage()
	if err != nil || released.PreparedBytes != 1024 || released.PinnedLogicalBytes != 1024 || released.Claims != 0 || p.readers != 0 || s.views != 0 || s.viewBytes != 0 {
		t.Fatal("explicit reader release", released, err)
	}
	claim, err := s.ClaimPreparedApplicationSnapshot(p, expected)
	if err != nil {
		t.Fatal("readers prevented later claim", err)
	}
	if err := claim.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	final, err := s.ApplicationTransferUsage()
	if err != nil || final.Prepared != 0 || final.Claims != 0 || final.PinnedLogicalBytes != 0 || final.PreparedBytes != 0 || final.ClaimBytes != 0 {
		t.Fatal("released owners", final, err)
	}
}

func TestPreparedSnapshotCheckFixedLogicalWorkLayout(t *testing.T) {
	// Logical source-work bound, not an allocation or physical-heap measurement.
	fixed := unsafe.Sizeof(pb.Snapshot{}) + unsafe.Sizeof(pb.SnapshotMetadata{}) + unsafe.Sizeof(pb.ConfState{}) + unsafe.Sizeof(ApplicationSnapshotManifest{}) + unsafe.Sizeof([4][]uint64{}) + 16*unsafe.Sizeof(uint64(0))
	// Two descriptors <=512, at most128 uint64 values plus128 unknown wire bytes.
	logical := fixed + 2*512 + 128*8 + 128
	if logical > 4096 {
		t.Fatal("fixed logical work exceeds bound", fixed, logical)
	}
	t.Logf("fixed objects and locals %d; bounded logical inspection %d <=4096; retained ownership delta zero", fixed, logical)
}

// Integration guard on the accepted P3 AS5 source; this is not original RED evidence.
func TestPreparedSnapshotCheckAS5RecoveryAndClaimGuard(t *testing.T) {
	for _, tight := range []bool{false, true} {
		t.Run(map[bool]string{false: "original-policy", true: "tight-policy"}[tight], func(t *testing.T) {
			s, p, expected, want := preparedRecoveryContractFixture(t, tight)
			view, err := p.OpenReadView(t.Context(), 3, want.MaxImageBytes)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = view.Close() })
			past, err := view.RootAt(t.Context(), 2, want.MaxImageBytes)
			if err != nil || string(past.Image) != "past" {
				t.Fatal(past, err)
			}
			before, err := s.ApplicationTransferUsage()
			if err != nil {
				t.Fatal(err)
			}
			fingerprint := readyMetadataFingerprint(t, s)
			if err := s.CheckPreparedApplicationSnapshot(p, expected); err != nil {
				t.Fatal("actual AS5 check with live audit reader", err)
			}
			after, err := s.ApplicationTransferUsage()
			if err != nil || after != before || p.claim != nil || p.readers != 1 || readyMetadataFingerprint(t, s) != fingerprint {
				t.Fatal("AS5 check changed ownership or stored bytes", after, err)
			}
			contract, err := p.ApplicationContract(t.Context())
			if err != nil || contract != want || contract.Version != 3 {
				t.Fatal("AS5 recovery contract changed", contract, err)
			}
			if claim, err := p.Claim(expected); claim != nil || !errors.Is(err, ErrLimit) {
				t.Fatal("check bypassed AS5 reader exclusion", claim, err)
			}
			if err := view.Close(); err != nil {
				t.Fatal(err)
			}
			claim, err := p.Claim(expected)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = claim.Close() })
			if err := s.CheckPreparedApplicationSnapshot(p, expected); !errors.Is(err, ErrLimit) {
				t.Fatal("check bypassed AS5 active claim", err)
			}
			raw, ready := readyMatchRaw(t, s, expected)
			root, err := s.PersistApplicationReady(ready, claim)
			if err != nil || root.Generation != 2 || root.Index != 3 || string(root.Image) != "now" {
				t.Fatal("actual AS5 claim activation", root, err)
			}
			raw.Advance(ready)
			if err := s.CheckPreparedApplicationSnapshot(p, expected); !errors.Is(err, ErrClosed) {
				t.Fatal("consumed AS5 prepared handle remained usable", err)
			}
		})
	}
}
