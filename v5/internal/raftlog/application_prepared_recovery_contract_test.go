package raftlog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
)

// Actual AS5 export/import/Prepare unit fixture; no Driver or quorum authority.
func preparedRecoveryContractFixture(t *testing.T, tight bool) (*Store, *PreparedApplicationSnapshot, *pb.Snapshot, ApplicationContract) {
	t.Helper()
	open := func(voter uint64) *Store {
		return boundedControlStore(t, func(c *Config) {
			c.Application.LocalVoter = voter
			c.Recovery = ApplicationRecoveryConfig{Version: 1}
			if tight {
				c.Application.MaxImageBytes, c.Application.MaxOutcomeBytes = 16, 512
			}
		})
	}
	donor, receiver := open(2), open(1)
	reservation := ApplicationRecoveryReservation{ID: [32]byte{91}, Digest: [32]byte{92}, Credits: []ApplicationRecoveryCredit{{Phase: 1, CommandBytes: 6, RetainedBytes: 1000, RetainedRecords: 4}}}
	for _, step := range []struct {
		index uint64
		image string
	}{{2, "past"}, {3, "now"}} {
		base, image, err := donor.Checkpoint()
		if err != nil {
			t.Fatal(err)
		}
		persist(t, donor, 2, step.index, ent(step.index, 2, "native-state"))
		b := ApplicationBatch{BaseGeneration: donor.ApplicationGeneration(), BaseIndex: base, BaseImageHash: sha256.Sum256(image), Image: []byte(step.image)}
		if step.index == 2 {
			b.RecoveryReserve = &reservation
		}
		if err := donor.InstallApplication(step.index, b); err != nil {
			t.Fatal(err)
		}
	}
	if err := donor.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	cut, err := donor.PublishedApplicationCut()
	if err != nil {
		t.Fatal(err)
	}
	export, err := donor.BeginPublishedApplicationExport(t.Context(), cut.ID)
	if err != nil {
		t.Fatal(err)
	}
	manifest := buildPublished(t, export, ReadBudget{Rows: 1, Bytes: 4096})
	chunks := collectPublished(t, export, ReadBudget{Rows: 1, Bytes: 4096})
	if err := export.Close(); err != nil {
		t.Fatal(err)
	}
	want := ApplicationContract{Version: 3, MaxKeyBytes: 1024, MaxValueBytes: 1 << 20, MaxImageBytes: 64 << 10, MaxPageRows: 4096, MaxPageBytes: 2 << 20, MaxInstallWrites: 1, MaxInstallBytes: 1664, MaxChangeBytes: 1 << 20, MaxOutcomeBytes: 64 << 10}
	if tight {
		want.MaxImageBytes, want.MaxOutcomeBytes = 16, 512
	}
	if manifest.Version != 5 || manifest.Contract != want || manifest.Index != 3 || !sameRecoveryReservation(manifest.RecoveryReservation, reservation) || !bytes.Equal(manifest.Image, []byte("now")) {
		t.Fatal("complete actual AS5 baseline", manifest.Version, manifest.Contract, manifest.RecoveryReservation)
	}
	prepared, snapshot := semanticPrepared(t, receiver, manifest, chunks)
	return receiver, prepared, snapshot, want
}

func TestPreparedApplicationContractDropsRecoveryOnActualAS5(t *testing.T) {
	for _, tight := range []bool{false, true} {
		t.Run(map[bool]string{false: "original-policy", true: "tight-policy"}[tight], func(t *testing.T) {
			s, p, _, want := preparedRecoveryContractFixture(t, tight)
			view, err := p.OpenReadView(t.Context(), 3, want.MaxImageBytes)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := view.Close(); err != nil {
					t.Error(err)
				}
			})
			old, err := view.RootAt(t.Context(), 2, want.MaxImageBytes)
			if err != nil || !bytes.Equal(old.Image, []byte("past")) {
				t.Fatal("actual retained past root", old, err)
			}
			current, err := view.RootBounded(t.Context(), want.MaxImageBytes)
			if err != nil || !bytes.Equal(current.Image, []byte("now")) {
				t.Fatal("actual distinct current root", current, err)
			}
			before := readyMetadataFingerprint(t, s)
			transfer, err := s.ApplicationTransferUsage()
			if err != nil {
				t.Fatal(err)
			}
			usage, err := s.ApplicationUsage()
			if err != nil {
				t.Fatal(err)
			}
			got, err := p.ApplicationContract(t.Context())
			if err != nil || got != want {
				t.Fatalf("prepared AS5 dropped immutable recovery contract or bounds: got=%+v want=%+v err=%v", got, want, err)
			}
			got.Version, got.MaxImageBytes = 1, 1
			again, err := p.ApplicationContract(t.Context())
			if err != nil || again != want {
				t.Fatal("returned value rebound owner policy", again, err)
			}
			afterTransfer, err := s.ApplicationTransferUsage()
			if err != nil || afterTransfer != transfer {
				t.Fatal("contract read changed native pins/owners", afterTransfer, transfer, err)
			}
			afterUsage, err := s.ApplicationUsage()
			if err != nil || afterUsage != usage || readyMetadataFingerprint(t, s) != before {
				t.Fatal("contract read changed native metadata/views", afterUsage, usage, err)
			}
			if err := view.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPreparedApplicationContractAS5PreservesGuardsAndClaimLifetime(t *testing.T) {
	s, p, snapshot, want := preparedRecoveryContractFixture(t, false)
	check := func(owner *PreparedApplicationSnapshot, ctx context.Context, wanted error) {
		t.Helper()
		got, err := owner.ApplicationContract(ctx)
		if !errors.Is(err, wanted) || got != (ApplicationContract{}) {
			t.Fatal("whole-zero native guard", got, err, wanted)
		}
	}
	copied := *p
	for _, bad := range []*PreparedApplicationSnapshot{nil, {}, &copied} {
		check(bad, t.Context(), ErrInvalid)
	}
	check(p, nil, ErrInvalid)
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	check(p, cancelled, context.Canceled)
	claim, err := p.Claim(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	before := readyMetadataFingerprint(t, s)
	transfer, err := s.ApplicationTransferUsage()
	if err != nil {
		t.Fatal(err)
	}
	got, err := p.ApplicationContract(t.Context())
	if err != nil || got != want || p.claim != claim || p.readers != 0 {
		t.Fatal("native live claim contract", got, err)
	}
	after, err := s.ApplicationTransferUsage()
	if err != nil || after != transfer || readyMetadataFingerprint(t, s) != before {
		t.Fatal("native claim mutated", after, transfer, err)
	}
	raw, ready := readyMatchRaw(t, s, snapshot)
	if _, err := s.PersistApplicationReady(ready, claim); err != nil {
		t.Fatal(err)
	}
	raw.Advance(ready)
	check(p, t.Context(), ErrClosed)
	if err := claim.Close(); err != nil {
		t.Fatal(err)
	}
	_, closed, _, _ := preparedRecoveryContractFixture(t, true)
	if err := closed.Close(); err != nil {
		t.Fatal(err)
	}
	check(closed, t.Context(), ErrClosed)
}
