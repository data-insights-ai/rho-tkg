package raftlog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// These arithmetic/ownership guards do not claim Store or Driver integration.
// The live public admission/installation tests follow at the shared seam.
func TestRecoveryLedgerGrowthAndAllFourQuotaCliffs(t *testing.T) {
	p, l := DefaultApplicationPolicy(1), DefaultLimits()
	r := ApplicationRecoveryReservation{ID: [32]byte{1}, Digest: [32]byte{2}, Credits: []ApplicationRecoveryCredit{{Phase: 1, CommandBytes: 10, RetainedBytes: 100, RetainedRecords: 4}, {Phase: 2, CommandBytes: 20, RetainedBytes: 200, RetainedRecords: 8}}}
	g, err := recoveryGrowth(r, p, l)
	// Two actual ordered entry keys and74-byte frames, plus10+20 payload bytes.
	if err != nil || g != (applicationRecoveryGrowth{bytes: 300, records: 12, logBytes: 196, logRecords: 2}) {
		t.Fatal(g, err)
	}
	for _, resource := range []string{"application-bytes", "application-records", "log-bytes", "log-records"} {
		for _, delta := range []int{-1, 0, 1} {
			b, n, lb, ln := p.RetainedApplicationBytes-300, p.RetainedApplicationRecords-12, l.MaxRetainedBytes-196, l.MaxRetainedEntries-2
			adjust := func(v uint64) uint64 {
				if delta < 0 {
					return v + 1
				}
				if delta > 0 {
					return v - 1
				}
				return v
			}
			switch resource {
			case "application-bytes":
				b = adjust(b)
			case "application-records":
				n = adjust(n)
			case "log-bytes":
				lb = adjust(lb)
			case "log-records":
				ln = adjust(ln)
			}
			err := recoveryFits(g, b, n, lb, ln, p, l)
			if delta < 0 {
				if !errors.Is(err, ErrLimit) {
					t.Fatal(resource, delta, err)
				}
			} else if err != nil {
				t.Fatal(resource, delta, err)
			}
		}
	}
}

func TestRecoveryLedgerReservationOwnsCreditsAndRefusesChangedRetry(t *testing.T) {
	p, l := DefaultApplicationPolicy(1), DefaultLimits()
	r := ApplicationRecoveryReservation{ID: [32]byte{1}, Digest: [32]byte{2}, Credits: []ApplicationRecoveryCredit{{Phase: 1, CommandBytes: 10, RetainedBytes: 100, RetainedRecords: 4}}}
	m, err := (applicationRecoveryMetadata{Config: ApplicationRecoveryConfig{Version: 1}}).reserve(r, p, l)
	if err != nil {
		t.Fatal(err)
	}
	r.Credits[0].CommandBytes = 99
	if m.Reservation.Credits[0].CommandBytes != 10 {
		t.Fatal("borrowed caller credit array")
	}
	retry := ApplicationRecoveryReservation{ID: m.Reservation.ID, Digest: m.Reservation.Digest, Credits: []ApplicationRecoveryCredit{{Phase: 1, CommandBytes: 10, RetainedBytes: 100, RetainedRecords: 4}}}
	got, err := m.reserve(retry, p, l)
	if err != nil || !reflect.DeepEqual(got, m) {
		t.Fatal("exact retry changed reservation", err)
	}
	if _, err := m.reserve(r, p, l); !errors.Is(err, ErrLimit) {
		t.Fatal("changed phase allowance accepted", err)
	}
	r.ID[0] = 3
	if _, err := m.reserve(r, p, l); !errors.Is(err, ErrLimit) {
		t.Fatal("competing recovery owner accepted", err)
	}
	hash := sha256.Sum256([]byte("actual admitted command"))
	use := ApplicationRecoveryUse{ID: m.Reservation.ID, Digest: m.Reservation.Digest, CommandDigest: hash, Phase: 1}
	credit, err := m.credit(use)
	if err != nil || credit != retry.Credits[0] || !recoveryCommandMatches(use, hash[:]) {
		t.Fatal(credit, err)
	}
	other := sha256.Sum256([]byte("other command"))
	if recoveryCommandMatches(use, other[:]) || recoveryCommandMatches(use, nil) {
		t.Fatal("command digest mismatch accepted")
	}
	for _, mutate := range []func(*ApplicationRecoveryUse){func(u *ApplicationRecoveryUse) { u.ID[0]++ }, func(u *ApplicationRecoveryUse) { u.Digest[0]++ }, func(u *ApplicationRecoveryUse) { u.Phase++ }, func(u *ApplicationRecoveryUse) { u.CommandDigest = [32]byte{} }} {
		bad := use
		mutate(&bad)
		if got, err := m.credit(bad); !errors.Is(err, ErrInvalid) || got != (ApplicationRecoveryCredit{}) {
			t.Fatal(got, err)
		}
	}
}

func TestRecoveryLedgerRejectsInvalidAndOverflowWithoutPublication(t *testing.T) {
	p, l := DefaultApplicationPolicy(1), DefaultLimits()
	r := ApplicationRecoveryReservation{ID: [32]byte{1}, Digest: [32]byte{2}, Credits: []ApplicationRecoveryCredit{{Phase: 1, CommandBytes: 10, RetainedBytes: 100, RetainedRecords: 4}}}
	for _, mutate := range []func(*ApplicationRecoveryReservation){func(r *ApplicationRecoveryReservation) { r.ID = [32]byte{} }, func(r *ApplicationRecoveryReservation) { r.Digest = [32]byte{} }, func(r *ApplicationRecoveryReservation) { r.Credits = nil }, func(r *ApplicationRecoveryReservation) { r.Credits[0].Phase = 0 }, func(r *ApplicationRecoveryReservation) { r.Credits[0].CommandBytes = -1 }, func(r *ApplicationRecoveryReservation) { r.Credits[0].CommandBytes = l.MaxEntryBytes }, func(r *ApplicationRecoveryReservation) { r.Credits[0].RetainedRecords = 0 }, func(r *ApplicationRecoveryReservation) { r.Credits = append(r.Credits, r.Credits[0]) }} {
		bad := r
		bad.Credits = append([]ApplicationRecoveryCredit(nil), r.Credits...)
		mutate(&bad)
		if g, err := recoveryGrowth(bad, p, l); !errors.Is(err, ErrInvalid) || g != (applicationRecoveryGrowth{}) {
			t.Fatal(g, err)
		}
	}
	for _, c := range []ApplicationRecoveryConfig{{Version: 0}, {Version: 3}} {
		m := applicationRecoveryMetadata{Config: c}
		if got, err := m.reserve(r, p, l); !errors.Is(err, ErrInvalid) || !reflect.DeepEqual(got, m) {
			t.Fatal(got, err)
		}
	}
	if _, err := checkedRecoveryAdd(math.MaxUint64, 1); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	if got, err := checkedRecoveryAdd(math.MaxUint64-1, 1); err != nil || got != math.MaxUint64 {
		t.Fatal(got, err)
	}
}

func TestRecoveryReserveIgnoredByInstallationCannotProtectFutureCompletion(t *testing.T) {
	s := boundedControlStore(t, func(c *Config) {
		c.Recovery = ApplicationRecoveryConfig{Version: 1}
		c.Application.RetainedApplicationBytes = 4096
		c.Application.RetainedApplicationRecords = 64
	})
	if err := s.AdmitApplication(1); err != nil {
		t.Fatal("ordinary admission fitting control before reserve", err)
	}
	reservation := ApplicationRecoveryReservation{ID: [32]byte{1}, Digest: [32]byte{2}, Credits: []ApplicationRecoveryCredit{{Phase: 1, CommandBytes: 10, RetainedBytes: 1500, RetainedRecords: 4}, {Phase: 2, CommandBytes: 10, RetainedBytes: 1500, RetainedRecords: 4}}}
	base, image, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	persist(t, s, 2, 2, ent(2, 2, "prepare-capacity"))
	batch := ApplicationBatch{BaseGeneration: s.ApplicationGeneration(), BaseIndex: base, BaseImageHash: sha256.Sum256(image), Image: image, RecoveryReserve: &reservation}
	if err := s.InstallApplication(2, batch); err != nil {
		t.Fatal("configured native reservation installation", err)
	}
	admitErr := s.AdmitApplication(1)
	t.Logf("native owner=%x expected=%x reserved phase bytes=%d ordinary admission=%v", s.meta.Recovery.Reservation.ID, reservation.ID, uint64(3000), admitErr)
	if s.meta.Recovery.Reservation.ID != reservation.ID || !errors.Is(admitErr, ErrLimit) {
		t.Fatalf("real installed completion obligation ignored by ordinary Store admission: owner=%x ordinary=%v", s.meta.Recovery.Reservation.ID, admitErr)
	}
}

func TestRecoveryReserveMustNotBeSilentlyIgnoredByDisabledLegacyStore(t *testing.T) {
	s := boundedControlStore(t, func(*Config) {})
	index, image, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	reservation := ApplicationRecoveryReservation{ID: [32]byte{1}, Digest: [32]byte{2}, Credits: []ApplicationRecoveryCredit{{Phase: 1, CommandBytes: 10, RetainedBytes: 100, RetainedRecords: 4}}}
	persist(t, s, 2, 2, ent(2, 2, "unknown completion capacity"))
	batch := ApplicationBatch{BaseGeneration: s.ApplicationGeneration(), BaseIndex: index, BaseImageHash: sha256.Sum256(image), Image: image, RecoveryReserve: &reservation}
	if err := s.InstallApplication(2, batch); !errors.Is(err, ErrInvalid) {
		t.Fatal("legacy door silently accepted unsupported recovery semantics", err)
	}
}

func TestRecoveryModeLostByInitializeCannotKeepNativeConfiguration(t *testing.T) {
	s := boundedControlStore(t, func(c *Config) { c.Recovery = ApplicationRecoveryConfig{Version: 1} })
	raw, closer, err := s.db.Get(metaKey)
	if err != nil {
		t.Fatal(err)
	}
	magic := string(raw[:4])
	if err := closer.Close(); err != nil {
		t.Fatal(err)
	}
	if s.meta.Recovery.Config != (ApplicationRecoveryConfig{Version: 1}) || magic != "RLM8" {
		t.Fatalf("Initialize dropped the selected native recovery mode: mode=%+v physical=%s", s.meta.Recovery.Config, magic)
	}
}

type exactRecoveryTestVerifier struct{ use ApplicationRecoveryUse }

func (v *exactRecoveryTestVerifier) VerifyApplicationRecovery(_ context.Context, data []byte, _ *ApplicationView) (ApplicationRecoveryPurpose, error) {
	if !bytes.Equal(data, []byte("finish")) {
		return ApplicationRecoveryPurpose{}, ErrInvalid
	}
	use := v.use
	use.CommandDigest = sha256.Sum256(data)
	return ApplicationRecoveryPurpose{Use: &use}, nil
}

func TestRecoveryBoundPhaseCannotCompleteThroughActualInstallation(t *testing.T) {
	s := boundedControlStore(t, func(c *Config) {
		c.Recovery = ApplicationRecoveryConfig{Version: 1}
		c.Application.RetainedApplicationBytes = 4096
		c.Application.RetainedApplicationRecords = 64
	})
	_, image, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	r := ApplicationRecoveryReservation{ID: [32]byte{1}, Digest: [32]byte{2}, Credits: []ApplicationRecoveryCredit{{Phase: 1, CommandBytes: 6, RetainedBytes: 1500, RetainedRecords: 4}}}
	persist(t, s, 2, 2, ent(2, 2, "prepare"))
	if err := s.InstallApplication(2, ApplicationBatch{BaseGeneration: s.ApplicationGeneration(), BaseIndex: 1, BaseImageHash: sha256.Sum256(image), Image: image, RecoveryReserve: &r}); err != nil {
		t.Fatal(err)
	}
	use := ApplicationRecoveryUse{ID: r.ID, Digest: r.Digest, Phase: 1, CommandDigest: sha256.Sum256([]byte("finish"))}
	if err := s.BindApplicationRecoveryVerifier(s.ApplicationBinding(), &exactRecoveryTestVerifier{use: use}); err != nil {
		t.Fatal(err)
	}
	before := s.meta.Recovery
	if claim, err := s.AdmitApplicationRecovery(t.Context(), []byte("finish-forged-header")); !errors.Is(err, ErrInvalid) || claim != nil || !reflect.DeepEqual(before, s.meta.Recovery) {
		t.Fatal("unvalidated header constructed or changed recovery credit", err)
	}
	claim, err := s.AdmitApplicationRecovery(t.Context(), []byte("finish"))
	if err != nil {
		t.Fatal(err)
	}
	persist(t, s, 2, 3, ent(3, 2, "finish"))
	batch := ApplicationBatch{BaseGeneration: s.ApplicationGeneration(), BaseIndex: 2, BaseImageHash: sha256.Sum256(image), Image: image, RecoveryUse: &use, RecoveryAdmission: claim}
	if err := s.InstallApplication(3, batch); err != nil {
		t.Fatalf("a validated exact-command terminal phase cannot consume its real reserved capacity: %v", err)
	}
	if s.meta.Recovery.Reservation.ID != ([32]byte{}) || len(s.meta.Recovery.Reservation.Credits) != 0 {
		t.Fatal("fulfilled completion credit was not released", s.meta.Recovery)
	}
}

// The follower may persist registration while its application root still lags.
// That queued obligation must already protect future logical completion space.
func TestRecoveryQueuedReservationIgnoredBeforeApplicationCanSpendCompletion(t *testing.T) {
	s := boundedControlStore(t, func(c *Config) {
		c.Recovery = ApplicationRecoveryConfig{Version: 1}
		c.Application.RetainedApplicationBytes = 12000
		c.Application.RetainedApplicationRecords = 64
	})
	r := ApplicationRecoveryReservation{ID: [32]byte{3}, Digest: [32]byte{4}, Credits: []ApplicationRecoveryCredit{
		{Phase: 1, CommandBytes: 6, RetainedBytes: 4200, RetainedRecords: 4},
		{Phase: 2, CommandBytes: 6, RetainedBytes: 4200, RetainedRecords: 4},
	}}
	verifier := &queuedRecoveryTestVerifier{reserve: r}
	if err := s.BindApplicationRecoveryVerifier(s.ApplicationBinding(), verifier); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AdmitApplicationRecovery(t.Context(), []byte("register")); err != nil {
		t.Fatal("fitting protected registration before proposal", err)
	}
	// Independent RS1 seed envelope117 + three AV1 frames139 =256.
	// 256 + 2*1664 +8400 =11984 fits; +17 =12001 refuses the same12000 cap.
	if s.meta.Recovery.Bytes != 117 || s.meta.App.Bytes != 139 {
		t.Fatal("independent initial physical census", s.meta.Recovery.Bytes, s.meta.App.Bytes)
	}
	verifier.reserve.Credits[0].RetainedBytes += 17
	tooLarge, refusal := s.AdmitApplicationRecovery(t.Context(), []byte("register"))
	if !errors.Is(refusal, ErrLimit) || tooLarge != nil {
		t.Fatal("adjacent creation refusal", tooLarge, refusal)
	}
	verifier.reserve.Credits[0].RetainedBytes -= 17
	before := s.meta.Recovery
	persist(t, s, 2, 2, ent(2, 2, "register"))
	if s.meta.Applied != 1 || !sameRecoveryReservation(before.Reservation, s.meta.Recovery.Reservation) {
		t.Fatal("queued registration changed applied reservation")
	}
	if err := s.AdmitApplication(1); !errors.Is(err, ErrLimit) {
		t.Fatalf("queued registration's 8400-byte future obligation is invisible to ordinary admission: %v", err)
	}
}

type queuedRecoveryTestVerifier struct {
	reserve ApplicationRecoveryReservation
}

func (v *queuedRecoveryTestVerifier) VerifyApplicationRecovery(_ context.Context, data []byte, _ *ApplicationView) (ApplicationRecoveryPurpose, error) {
	if bytes.Equal(data, []byte("register")) {
		return ApplicationRecoveryPurpose{Reserve: &v.reserve}, nil
	}
	if bytes.Equal(data, []byte("finish")) || bytes.Equal(data, []byte("finish1")) || bytes.Equal(data, []byte("finish2")) || bytes.Equal(data, []byte("finish3")) {
		phase := uint32(1)
		if len(data) == 7 {
			phase = uint32(data[6] - '0')
		}
		u := ApplicationRecoveryUse{ID: v.reserve.ID, Digest: v.reserve.Digest, Phase: phase, CommandDigest: sha256.Sum256(data)}
		return ApplicationRecoveryPurpose{Use: &u}, nil
	}
	return ApplicationRecoveryPurpose{}, nil
}

func TestRecoveryOneCreditQueuedReservationRoundTripAndReopen(t *testing.T) {
	var cfg Config
	s := boundedControlStore(t, func(c *Config) { c.Recovery = ApplicationRecoveryConfig{Version: 1}; cfg = *c })
	r := ApplicationRecoveryReservation{ID: [32]byte{5}, Digest: [32]byte{6}, Credits: []ApplicationRecoveryCredit{{Phase: 1, CommandBytes: 6, RetainedBytes: 200, RetainedRecords: 3}}}
	if err := s.BindApplicationRecoveryVerifier(s.ApplicationBinding(), &queuedRecoveryTestVerifier{reserve: r}); err != nil {
		t.Fatal(err)
	}
	persist(t, s, 2, 2, ent(2, 2, "register"))
	wire, err := encodeMeta(s.meta)
	if err != nil {
		t.Fatal(err)
	}
	round, err := decodeMeta(wire, s.limits)
	if err != nil || !reflect.DeepEqual(round.Recovery, s.meta.Recovery) {
		t.Fatalf("valid one-credit queued frame cannot roundtrip: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.Transfer.Contract, err = ApplicationContractForPolicyWithRecovery(cfg.Application, cfg.Controls, cfg.Recovery)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Create = false
	reopened, err := Open(cfg)
	if err != nil {
		t.Fatalf("valid one-credit queued frame cannot reopen: %v", err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	})
	if !reflect.DeepEqual(reopened.meta.Recovery, round.Recovery) || reopened.meta.Applied != 1 {
		t.Fatal("queued protection or applied root changed on reopen")
	}
}

func TestRecoveryReservedSmallPhasesMustFitBeyondGenericPendingWorstCase(t *testing.T) {
	s := boundedControlStore(t, func(c *Config) {
		c.Recovery = ApplicationRecoveryConfig{Version: 1}
		c.Application.RetainedApplicationBytes = 4096
		c.Application.RetainedApplicationRecords = 64
	})
	_, image, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	r := ApplicationRecoveryReservation{ID: [32]byte{7}, Digest: [32]byte{8}, Credits: []ApplicationRecoveryCredit{
		{Phase: 1, CommandBytes: 7, RetainedBytes: 300, RetainedRecords: 4},
		{Phase: 2, CommandBytes: 7, RetainedBytes: 300, RetainedRecords: 4},
	}}
	persist(t, s, 2, 2, ent(2, 2, "prepare"))
	if err := s.InstallApplication(2, ApplicationBatch{BaseGeneration: s.ApplicationGeneration(), BaseIndex: 1, BaseImageHash: sha256.Sum256(image), Image: image, RecoveryReserve: &r}); err != nil {
		t.Fatal(err)
	}
	if err := s.BindApplicationRecoveryVerifier(s.ApplicationBinding(), &queuedRecoveryTestVerifier{reserve: r}); err != nil {
		t.Fatal(err)
	}
	for _, state := range []struct {
		r    ApplicationRecoveryReservation
		want uint64
	}{{ApplicationRecoveryReservation{}, 256}, {ApplicationRecoveryReservation{ID: r.ID, Digest: r.Digest, Credits: r.Credits[:1]}, 280}} {
		b, n, e := s.meta.App.Policy.measureRecovery(ApplicationBatch{Image: image}, state.r)
		if e != nil || b != state.want || n != 4 {
			t.Fatal("independent physical phase envelope", b, n, e)
		}
	}
	before := s.meta.Recovery
	rd := raft.Ready{HardState: &pb.HardState{Term: new(uint64(2)), Commit: new(uint64(5))}, Entries: []*pb.Entry{ent(3, 2, "finish1"), ent(4, 2, "finish2"), ent(5, 2, "ordinary")}}
	// Three ordinary maxima cost4992 >4096. Two300-byte completion bounds
	// plus one1664-byte ordinary maximum cost2264. Each no-write native
	// phase independently owns three AV1 frames139 plus RS1 (117/141):
	// closed256 / one-credit280. Neither configured ceiling is raised.
	if err := s.Persist(rd); err != nil {
		t.Fatalf("reserved exact phase headroom rejected before classified reduction: %v", err)
	}
	if !sameRecoveryReservation(before.Reservation, s.meta.Recovery.Reservation) || s.meta.Applied != 2 {
		t.Fatal("queued phase projection consumed applied ownership")
	}
	protected, b, n, err := recoveryProjected(s.meta, s.limits, 3)
	if err != nil || protected != (applicationRecoveryGrowth{}) || b != 2264 || n != 13 {
		t.Fatal("actual classified pending costs", protected, b, n, err)
	}
}

func TestRecoveryQueuedExactPhaseRetryCannotDoubleSpendOrBlockCompletion(t *testing.T) {
	s := boundedControlStore(t, func(c *Config) {
		c.Recovery = ApplicationRecoveryConfig{Version: 1}
		c.Application.RetainedApplicationBytes = 4096
		c.Application.RetainedApplicationRecords = 64
	})
	_, image, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	r := ApplicationRecoveryReservation{ID: [32]byte{9}, Digest: [32]byte{10}, Credits: []ApplicationRecoveryCredit{{Phase: 1, CommandBytes: 6, RetainedBytes: 1500, RetainedRecords: 4}}}
	persist(t, s, 2, 2, ent(2, 2, "prepare"))
	if err := s.InstallApplication(2, ApplicationBatch{BaseGeneration: s.ApplicationGeneration(), BaseIndex: 1, BaseImageHash: sha256.Sum256(image), Image: image, RecoveryReserve: &r}); err != nil {
		t.Fatal(err)
	}
	if err := s.BindApplicationRecoveryVerifier(s.ApplicationBinding(), &queuedRecoveryTestVerifier{reserve: r}); err != nil {
		t.Fatal(err)
	}
	before := s.meta.Recovery
	if err := s.Persist(raft.Ready{HardState: &pb.HardState{Term: new(uint64(2)), Commit: new(uint64(4))}, Entries: []*pb.Entry{ent(3, 2, "finish"), ent(4, 2, "finish")}}); err != nil {
		t.Fatalf("exact queued phase retry cannot persist for deterministic recovery: %v", err)
	}
	protected, b, n, err := recoveryProjected(s.meta, s.limits, 2)
	if err != nil || protected != (applicationRecoveryGrowth{}) || b != 3000 || n != 8 || !sameRecoveryReservation(before.Reservation, s.meta.Recovery.Reservation) {
		t.Fatal("retry released or double consumed applied credit", protected, b, n, err)
	}
	// Two copies are already committed. Fresh proposal admission would book a
	// third1500-byte pending copy; installation uses its existing committed token.
	if extra, err := s.AdmitApplicationRecovery(t.Context(), []byte("finish")); !errors.Is(err, ErrLimit) || extra != nil {
		t.Fatal("unbacked third fresh proposal admitted", extra, err)
	}
	claim, err := s.RecoveryAdmissionForCommittedEntry(t.Context(), 3)
	if err != nil {
		t.Fatal(err)
	}
	use := ApplicationRecoveryUse{ID: r.ID, Digest: r.Digest, Phase: 1, CommandDigest: sha256.Sum256([]byte("finish"))}
	if err := s.InstallApplication(3, ApplicationBatch{BaseGeneration: s.ApplicationGeneration(), BaseIndex: 2, BaseImageHash: sha256.Sum256(image), Image: image, RecoveryUse: &use, RecoveryAdmission: claim}); err != nil {
		t.Fatal(err)
	}
	if s.meta.Recovery.Reservation.ID != ([32]byte{}) || len(s.meta.Recovery.Queued) != 1 {
		t.Fatal("first completed phase has wrong remaining ownership")
	}
	// Actual materializer retry already observes the durable completed phase, so
	// it emits only the ordinary exact replay envelopes and claims no credit twice.
	if err := s.InstallApplication(4, ApplicationBatch{BaseGeneration: s.ApplicationGeneration(), BaseIndex: 3, BaseImageHash: sha256.Sum256(image), Image: image}); err != nil {
		t.Fatal("completed exact replay cannot advance", err)
	}
	if s.meta.Applied != 4 || len(s.meta.Recovery.Queued) != 0 || s.meta.Recovery.Reservation.ID != ([32]byte{}) {
		t.Fatal("retry retained or recreated fulfilled obligation")
	}
}

func TestRecoveryQueuedSuffixReplacementAndRejectedUsePreserveAppliedOwner(t *testing.T) {
	s := boundedControlStore(t, func(c *Config) {
		c.Recovery = ApplicationRecoveryConfig{Version: 1}
		c.Application.RetainedApplicationBytes = 12000
		c.Application.RetainedApplicationRecords = 64
	})
	r := ApplicationRecoveryReservation{ID: [32]byte{11}, Digest: [32]byte{12}, Credits: []ApplicationRecoveryCredit{{Phase: 1, CommandBytes: 6, RetainedBytes: 8500, RetainedRecords: 4}}}
	if err := s.BindApplicationRecoveryVerifier(s.ApplicationBinding(), &queuedRecoveryTestVerifier{reserve: r}); err != nil {
		t.Fatal(err)
	}
	persist(t, s, 2, 1, ent(2, 2, "register"))
	if err := s.AdmitApplication(1); !errors.Is(err, ErrLimit) {
		t.Fatal("queued guard before replacement", err)
	}
	persist(t, s, 3, 1, ent(2, 3, "ordinary replacement"))
	if len(s.meta.Recovery.Queued) != 0 || s.meta.Recovery.Reservation.ID != ([32]byte{}) {
		t.Fatal("uncommitted suffix recipe survived replacement")
	}
	if err := s.AdmitApplication(1); err != nil {
		t.Fatal("removed queued obligation still backpressures", err)
	}
	before := readyMetadataFingerprint(t, s)
	beforeRecovery := s.meta.Recovery
	if err := s.Persist(raft.Ready{HardState: &pb.HardState{Term: new(uint64(3)), Commit: new(uint64(2))}, Entries: []*pb.Entry{ent(2, 3, "finish")}}); !errors.Is(err, ErrInvalid) {
		t.Fatal("use without applied or projected reservation", err)
	}
	if before != readyMetadataFingerprint(t, s) || !reflect.DeepEqual(beforeRecovery, s.meta.Recovery) || s.poison != nil {
		t.Fatal("rejected use changed metadata/credit backing")
	}
}

// Native Store/actual RawNode activation evidence, not a graph quorum/cut proof.
func TestRecoverySnapshotActivationMustReplaceOldOwnerWithCapturedOwner(t *testing.T) {
	var receiverConfig Config
	receiver := boundedControlStore(t, func(c *Config) { c.Recovery = ApplicationRecoveryConfig{Version: 1}; receiverConfig = *c })
	donor := boundedControlStore(t, func(c *Config) { c.Recovery = ApplicationRecoveryConfig{Version: 1}; c.Application.LocalVoter = 2 })
	alpha := ApplicationRecoveryReservation{ID: [32]byte{21}, Digest: [32]byte{22}, Credits: []ApplicationRecoveryCredit{{Phase: 1, CommandBytes: 6, RetainedBytes: 1500, RetainedRecords: 4}}}
	beta := ApplicationRecoveryReservation{ID: [32]byte{23}, Digest: [32]byte{24}, Credits: []ApplicationRecoveryCredit{{Phase: 1, CommandBytes: 6, RetainedBytes: 1500, RetainedRecords: 4}}}
	installReserve := func(s *Store, r ApplicationRecoveryReservation, data string) {
		t.Helper()
		base, image, err := s.Checkpoint()
		if err != nil {
			t.Fatal(err)
		}
		persist(t, s, 2, base+1, ent(base+1, 2, data))
		if err := s.InstallApplication(base+1, ApplicationBatch{BaseGeneration: s.ApplicationGeneration(), BaseIndex: base, BaseImageHash: sha256.Sum256(image), Image: image, RecoveryReserve: &r}); err != nil {
			t.Fatal(err)
		}
	}
	installReserve(receiver, alpha, "initial-alpha")
	installReserve(donor, alpha, "initial-alpha")
	if err := donor.BindApplicationRecoveryVerifier(donor.ApplicationBinding(), &twoRecoveryOwnersTestVerifier{first: alpha, next: beta}); err != nil {
		t.Fatal(err)
	}
	claim, err := donor.AdmitApplicationRecovery(t.Context(), []byte("finish"))
	if err != nil {
		t.Fatal(err)
	}
	_, image, err := donor.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	use := ApplicationRecoveryUse{ID: alpha.ID, Digest: alpha.Digest, Phase: 1, CommandDigest: sha256.Sum256([]byte("finish"))}
	persist(t, donor, 2, 3, ent(3, 2, "finish"))
	if err := donor.InstallApplication(3, ApplicationBatch{BaseGeneration: donor.ApplicationGeneration(), BaseIndex: 2, BaseImageHash: sha256.Sum256(image), Image: image, RecoveryUse: &use, RecoveryAdmission: claim}); err != nil {
		t.Fatal(err)
	}
	installReserve(donor, beta, "next-beta")
	if err := donor.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	cut, err := donor.PublishedApplicationCut()
	if err != nil {
		t.Fatal(err)
	}
	// The durable export must remain Beta even after live ownership advances.
	lateClaim, lateErr := donor.AdmitApplicationRecovery(t.Context(), []byte("finish"))
	if lateErr != nil {
		t.Fatal(lateErr)
	}
	lateUse := ApplicationRecoveryUse{ID: beta.ID, Digest: beta.Digest, Phase: 1, CommandDigest: sha256.Sum256([]byte("finish"))}
	persist(t, donor, 2, 5, ent(5, 2, "finish"))
	if err := donor.InstallApplication(5, ApplicationBatch{BaseGeneration: donor.ApplicationGeneration(), BaseIndex: 4, BaseImageHash: sha256.Sum256(image), Image: image, RecoveryUse: &lateUse, RecoveryAdmission: lateClaim}); err != nil {
		t.Fatal(err)
	}
	gamma := ApplicationRecoveryReservation{ID: [32]byte{25}, Digest: [32]byte{26}, Credits: beta.Credits}
	installReserve(donor, gamma, "next-gamma")
	if !sameRecoveryReservation(donor.meta.Recovery.Reservation, gamma) {
		t.Fatal("live post-cut guard missing")
	}
	export, err := donor.BeginPublishedApplicationExport(t.Context(), cut.ID)
	if err != nil {
		t.Fatal(err)
	}
	manifest := buildPublished(t, export, ReadBudget{Rows: 1, Bytes: 4096})
	chunks := collectPublished(t, export, ReadBudget{Rows: 1, Bytes: 4096})
	if manifest.Version != 5 || manifest.Index != 4 || !sameRecoveryReservation(manifest.RecoveryReservation, beta) || sameRecoveryReservation(manifest.RecoveryReservation, gamma) {
		t.Fatal("published recovery root followed later live ownership", manifest.RecoveryReservation.ID)
	}
	if err := export.Close(); err != nil {
		t.Fatal(err)
	}
	prepared, snapshot := semanticPrepared(t, receiver, manifest, chunks)
	activation, err := prepared.Claim(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	raw, ready := readyMatchRaw(t, receiver, snapshot)
	root, err := receiver.PersistApplicationReady(ready, activation)
	if err != nil {
		t.Fatal(err)
	}
	raw.Advance(ready)
	if err := activation.Close(); err != nil {
		t.Fatal(err)
	}
	if root.Index != 4 || root.Generation != 2 || !sameRecoveryReservation(receiver.meta.Recovery.Reservation, beta) {
		t.Fatalf("snapshot kept old/lost captured completion owner: got=%x want=%x old=%x root=%+v", receiver.meta.Recovery.Reservation.ID, beta.ID, alpha.ID, root)
	}
	if err := receiver.Close(); err != nil {
		t.Fatal(err)
	}
	receiverConfig.Transfer.Contract, err = ApplicationContractForPolicyWithRecovery(receiverConfig.Application, receiverConfig.Controls, receiverConfig.Recovery)
	if err != nil {
		t.Fatal(err)
	}
	receiverConfig.Create = false
	reopened, err := Open(receiverConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	})
	if !sameRecoveryReservation(reopened.meta.Recovery.Reservation, beta) {
		t.Fatal("snapshot completion owner lost on restart")
	}
}

func TestRecoverySnapshotSameReadySuffixRequiresIncomingOwnerAdmission(t *testing.T) {
	var receiverConfig Config
	receiver := boundedControlStore(t, func(c *Config) { c.Recovery = ApplicationRecoveryConfig{Version: 1}; receiverConfig = *c })
	donor := boundedControlStore(t, func(c *Config) { c.Recovery = ApplicationRecoveryConfig{Version: 1}; c.Application.LocalVoter = 2 })
	alpha := ApplicationRecoveryReservation{ID: [32]byte{21}, Digest: [32]byte{22}, Credits: []ApplicationRecoveryCredit{{Phase: 1, CommandBytes: 6, RetainedBytes: 1500, RetainedRecords: 4}}}
	beta := ApplicationRecoveryReservation{ID: [32]byte{23}, Digest: [32]byte{24}, Credits: []ApplicationRecoveryCredit{{Phase: 1, CommandBytes: 6, RetainedBytes: 1500, RetainedRecords: 4}}}
	installReserve := func(s *Store, r ApplicationRecoveryReservation, data string) {
		t.Helper()
		base, image, err := s.Checkpoint()
		if err != nil {
			t.Fatal(err)
		}
		persist(t, s, 2, base+1, ent(base+1, 2, data))
		if err := s.InstallApplication(base+1, ApplicationBatch{BaseGeneration: s.ApplicationGeneration(), BaseIndex: base, BaseImageHash: sha256.Sum256(image), Image: image, RecoveryReserve: &r}); err != nil {
			t.Fatal(err)
		}
	}
	installReserve(receiver, alpha, "initial-alpha")
	installReserve(donor, alpha, "initial-alpha")
	if err := donor.BindApplicationRecoveryVerifier(donor.ApplicationBinding(), &twoRecoveryOwnersTestVerifier{first: alpha, next: beta}); err != nil {
		t.Fatal(err)
	}
	claim, err := donor.AdmitApplicationRecovery(t.Context(), []byte("finish"))
	if err != nil {
		t.Fatal(err)
	}
	_, image, err := donor.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	use := ApplicationRecoveryUse{ID: alpha.ID, Digest: alpha.Digest, Phase: 1, CommandDigest: sha256.Sum256([]byte("finish"))}
	persist(t, donor, 2, 3, ent(3, 2, "finish"))
	if err := donor.InstallApplication(3, ApplicationBatch{BaseGeneration: donor.ApplicationGeneration(), BaseIndex: 2, BaseImageHash: sha256.Sum256(image), Image: image, RecoveryUse: &use, RecoveryAdmission: claim}); err != nil {
		t.Fatal(err)
	}
	installReserve(donor, beta, "next-beta")
	if err := donor.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	cut, err := donor.PublishedApplicationCut()
	if err != nil {
		t.Fatal(err)
	}
	// The durable export must remain Beta even after live ownership advances.
	lateClaim, lateErr := donor.AdmitApplicationRecovery(t.Context(), []byte("finish"))
	if lateErr != nil {
		t.Fatal(lateErr)
	}
	lateUse := ApplicationRecoveryUse{ID: beta.ID, Digest: beta.Digest, Phase: 1, CommandDigest: sha256.Sum256([]byte("finish"))}
	persist(t, donor, 2, 5, ent(5, 2, "finish"))
	if err := donor.InstallApplication(5, ApplicationBatch{BaseGeneration: donor.ApplicationGeneration(), BaseIndex: 4, BaseImageHash: sha256.Sum256(image), Image: image, RecoveryUse: &lateUse, RecoveryAdmission: lateClaim}); err != nil {
		t.Fatal(err)
	}
	gamma := ApplicationRecoveryReservation{ID: [32]byte{25}, Digest: [32]byte{26}, Credits: beta.Credits}
	installReserve(donor, gamma, "next-gamma")
	if !sameRecoveryReservation(donor.meta.Recovery.Reservation, gamma) {
		t.Fatal("live post-cut guard missing")
	}
	export, err := donor.BeginPublishedApplicationExport(t.Context(), cut.ID)
	if err != nil {
		t.Fatal(err)
	}
	manifest := buildPublished(t, export, ReadBudget{Rows: 1, Bytes: 4096})
	chunks := collectPublished(t, export, ReadBudget{Rows: 1, Bytes: 4096})
	if manifest.Version != 5 || manifest.Index != 4 || !sameRecoveryReservation(manifest.RecoveryReservation, beta) || sameRecoveryReservation(manifest.RecoveryReservation, gamma) {
		t.Fatal("published recovery root followed later live ownership", manifest.RecoveryReservation.ID)
	}
	if err := export.Close(); err != nil {
		t.Fatal(err)
	}
	receiverVerifier := &twoRecoveryOwnersTestVerifier{first: alpha, next: beta}
	if err := receiver.BindApplicationRecoveryVerifier(receiver.ApplicationBinding(), receiverVerifier); err != nil {
		t.Fatal(err)
	}
	prepared, snapshot := semanticPrepared(t, receiver, manifest, chunks)
	preparedView, err := prepared.OpenReadView(t.Context(), manifest.Index, 4)
	if err != nil {
		t.Fatal(err)
	}
	preparedOwner, _, err := preparedView.RecoveryOwner(t.Context(), ReadBudget{Rows: 1, Bytes: 4096})
	if err != nil || preparedOwner != (ApplicationRecoveryOwner{ID: beta.ID, Digest: beta.Digest}) {
		t.Fatal("prepared owner fell back to old active Alpha", preparedOwner, err)
	}
	if err := preparedView.Close(); err != nil {
		t.Fatal(err)
	}
	activation, err := prepared.Claim(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	raw, rawErr := raft.NewRawNode(&raft.Config{ID: 1, Storage: receiver, Applied: receiver.meta.Applied, ElectionTick: 10, HeartbeatTick: 1, MaxSizePerMsg: 1 << 20, MaxCommittedSizePerReady: 1 << 20, MaxInflightMsgs: 16})
	if rawErr != nil {
		t.Fatal(rawErr)
	}
	if err := raw.Step(&pb.Message{Type: pb.MsgSnap.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(5)), Snapshot: proto.Clone(snapshot).(*pb.Snapshot)}); err != nil {
		t.Fatal(err)
	}
	if err := raw.Step(&pb.Message{Type: pb.MsgApp.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(5)), Index: new(uint64(4)), LogTerm: new(uint64(2)), Commit: new(uint64(5)), Entries: []*pb.Entry{ent(5, 2, "finish")}}); err != nil {
		t.Fatal(err)
	}
	ready := raw.Ready()
	if raft.IsEmptySnap(ready.Snapshot) || len(ready.Entries) != 1 || ready.Entries[0].GetIndex() != 5 {
		t.Fatal("actual same Ready snapshot+phase suffix absent", ready)
	}
	before := readyMetadataFingerprint(t, receiver)
	pins := receiver.pinnedApplicationBytes
	refusal := errors.New("incoming selected verifier refuses suffix")
	receiverVerifier.failure = refusal
	failedRoot, failedErr := receiver.PersistApplicationReady(ready, activation)
	if !errors.Is(failedErr, refusal) || failedRoot.Index != 0 || failedRoot.Generation != 0 || failedRoot.Image != nil || failedRoot.ImageHash != ([32]byte{}) {
		t.Fatal("incoming verifier cause lost", failedRoot, failedErr)
	}
	if readyMetadataFingerprint(t, receiver) != before || receiver.views != 0 || prepared.readers != 0 || receiver.pinnedApplicationBytes != pins || activation.consumed || receiver.poison != nil {
		t.Fatal("failed incoming classification changed active state or leaked prepared view")
	}
	receiverVerifier.failure = nil
	wrong := ready
	wrong.Snapshot = proto.Clone(ready.Snapshot).(*pb.Snapshot)
	wrong.Snapshot.Data[0] ^= 1
	if r, err := receiver.PersistApplicationReady(wrong, activation); !errors.Is(err, ErrInvalid) || r.Index != 0 {
		t.Fatal("crossed snapshot admitted its suffix", r, err)
	}
	if r, err := receiver.PersistApplicationReady(ready, nil); !errors.Is(err, ErrInvalid) || r.Index != 0 {
		t.Fatal("nil claim", r, err)
	}
	if err := activation.Close(); err != nil {
		t.Fatal(err)
	}
	if r, err := receiver.PersistApplicationReady(ready, activation); !errors.Is(err, ErrClosed) || r.Index != 0 {
		t.Fatal("closed incoming claim", r, err)
	}
	activation, err = prepared.Claim(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	root, err := receiver.PersistApplicationReady(ready, activation)
	if err != nil {
		t.Fatal(err)
	}
	raw.Advance(ready)
	if err := activation.Close(); err != nil {
		t.Fatal(err)
	}
	if root.Index != 4 || root.Generation != 2 || !sameRecoveryReservation(receiver.meta.Recovery.Reservation, beta) {
		t.Fatalf("snapshot kept old/lost captured completion owner: got=%x want=%x old=%x root=%+v", receiver.meta.Recovery.Reservation.ID, beta.ID, alpha.ID, root)
	}
	phaseClaim, claimErr := receiver.RecoveryAdmissionForCommittedEntry(t.Context(), 5)
	if claimErr != nil || phaseClaim == nil || phaseClaim.purpose.Use == nil || phaseClaim.purpose.Use.ID != beta.ID {
		t.Fatalf("actual incoming recovery suffix has no Beta phase admission: %v claim%v", claimErr, phaseClaim)
	}
	suffixUse := ApplicationRecoveryUse{ID: beta.ID, Digest: beta.Digest, Phase: 1, CommandDigest: sha256.Sum256([]byte("finish"))}
	if err := receiver.InstallApplication(5, ApplicationBatch{BaseGeneration: root.Generation, BaseIndex: root.Index, BaseImageHash: root.ImageHash, Image: root.Image, RecoveryUse: &suffixUse, RecoveryAdmission: phaseClaim}); err != nil {
		t.Fatal("incoming bound completion cannot install", err)
	}
	if receiver.meta.Recovery.Reservation.ID != ([32]byte{}) {
		t.Fatal("incoming suffix did not consume Beta once")
	}
	if err := receiver.Close(); err != nil {
		t.Fatal(err)
	}
	receiverConfig.Transfer.Contract, err = ApplicationContractForPolicyWithRecovery(receiverConfig.Application, receiverConfig.Controls, receiverConfig.Recovery)
	if err != nil {
		t.Fatal(err)
	}
	receiverConfig.Create = false
	reopened, err := Open(receiverConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	})
	if reopened.meta.Recovery.Reservation.ID != ([32]byte{}) || reopened.meta.Applied != 5 {
		t.Fatal("resolved suffix lost on restart")
	}
}

type twoRecoveryOwnersTestVerifier struct {
	first, next ApplicationRecoveryReservation
	failure     error
}

func (v *twoRecoveryOwnersTestVerifier) VerifyApplicationRecovery(ctx context.Context, data []byte, view *ApplicationView) (ApplicationRecoveryPurpose, error) {
	if v.failure != nil {
		return ApplicationRecoveryPurpose{}, v.failure
	}
	if !bytes.Equal(data, []byte("finish")) {
		return ApplicationRecoveryPurpose{}, nil
	}
	root, err := view.RootBounded(ctx, 4)
	if err != nil {
		return ApplicationRecoveryPurpose{}, err
	}
	r := v.first
	if root.Index >= 4 {
		r = v.next
	}
	u := ApplicationRecoveryUse{ID: r.ID, Digest: r.Digest, Phase: 1, CommandDigest: sha256.Sum256(data)}
	return ApplicationRecoveryPurpose{Use: &u}, nil
}

func TestRecoveryPublicCapacityHelpersAndCommittedAdmission(t *testing.T) {
	p := DefaultApplicationPolicy(1)
	control := ApplicationControlConfig{Version: 1}
	native := ApplicationRecoveryConfig{Version: 1}
	old, e := ApplicationContractForPolicyWithControls(p, control)
	if e != nil {
		t.Fatal(e)
	}
	for _, r := range []ApplicationRecoveryConfig{{}, native} {
		c, err := ApplicationContractForPolicyWithRecovery(p, control, r)
		expected := old
		if r.Version == 1 {
			expected.Version = 3
		}
		if err != nil || c != expected {
			t.Fatal("versioned exact numerical profile", c, err)
		}
	}
	for _, row := range []struct {
		p ApplicationPolicy
		c ApplicationControlConfig
		r ApplicationRecoveryConfig
	}{{ApplicationPolicy{}, control, native}, {p, ApplicationControlConfig{}, native}, {p, control, ApplicationRecoveryConfig{Version: 2}}} {
		c, err := ApplicationContractForPolicyWithRecovery(row.p, row.c, row.r)
		if !errors.Is(err, ErrInvalid) || c != (ApplicationContract{}) {
			t.Fatal(c, err)
		}
	}
	for _, row := range []struct {
		n int
		b uint64
	}{{0, 117}, {1, 141}, {2, 165}} {
		b, n, err := ApplicationRecoveryRootOverhead(row.n)
		if err != nil || b != row.b || n != 1 {
			t.Fatal("independent9+36+72+24n", b, n, err)
		}
	}
	for _, row := range []struct {
		n    int
		want error
	}{{-1, ErrInvalid}, {math.MaxInt, ErrLimit}} {
		b, n, err := ApplicationRecoveryRootOverhead(row.n)
		if !errors.Is(err, row.want) || b != 0 || n != 0 {
			t.Fatal(b, n, err)
		}
	}
	var nilStore *Store
	if nilStore.ApplicationRecovery() != (ApplicationRecoveryConfig{}) {
		t.Fatal("nil configuration")
	}
	if a, err := nilStore.RecoveryAdmissionForCommittedEntry(t.Context(), 1); !errors.Is(err, ErrInvalid) || a != nil {
		t.Fatal(a, err)
	}
	s := boundedControlStore(t, func(c *Config) { c.Recovery = native })
	if s.ApplicationRecovery() != native {
		t.Fatal("immutable mode")
	}
	if err := s.BindApplicationRecoveryVerifier(s.ApplicationBinding(), nil); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	r := ApplicationRecoveryReservation{ID: [32]byte{31}, Digest: [32]byte{32}, Credits: []ApplicationRecoveryCredit{{Phase: 1, CommandBytes: 6, RetainedBytes: 1000, RetainedRecords: 4}}}
	if err := s.BindApplicationRecoveryVerifier(s.ApplicationBinding(), &queuedRecoveryTestVerifier{reserve: r}); err != nil {
		t.Fatal(err)
	}
	if err := s.BindApplicationRecoveryVerifier(s.ApplicationBinding(), &queuedRecoveryTestVerifier{reserve: r}); !errors.Is(err, ErrInvalid) {
		t.Fatal("second selected verifier", err)
	}
	persist(t, s, 2, 2, ent(2, 2, "register"))
	a, err := s.RecoveryAdmissionForCommittedEntry(t.Context(), 2)
	if err != nil || a == nil || a.purpose.Reserve == nil || !sameRecoveryReservation(*a.purpose.Reserve, r) {
		t.Fatal("actual exact committed binding", a, err)
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	for _, row := range []struct {
		ctx   context.Context
		index uint64
		want  error
	}{{nil, 2, ErrInvalid}, {cancelled, 2, context.Canceled}, {t.Context(), 1, ErrInvalid}, {t.Context(), 3, ErrInvalid}} {
		a, err := s.RecoveryAdmissionForCommittedEntry(row.ctx, row.index)
		if !errors.Is(err, row.want) || a != nil {
			t.Fatal(a, err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if s.ApplicationRecovery() != native {
		t.Fatal("closed configuration changed")
	}
	if a, err := s.RecoveryAdmissionForCommittedEntry(t.Context(), 2); !errors.Is(err, ErrClosed) || a != nil {
		t.Fatal(a, err)
	}
}

// This selected verifier is a test fixture, not a caller-supplied admission hook.
type recoveryAdmissionTestVerifier func(context.Context, []byte, *ApplicationView) (ApplicationRecoveryPurpose, error)

func (f recoveryAdmissionTestVerifier) VerifyApplicationRecovery(ctx context.Context, data []byte, view *ApplicationView) (ApplicationRecoveryPurpose, error) {
	return f(ctx, data, view)
}

func TestRecoveryAdmitApplicationRecoveryDirectRefusalsReleaseBorrowedView(t *testing.T) {
	var nilStore *Store
	if a, err := nilStore.AdmitApplicationRecovery(t.Context(), nil); !errors.Is(err, ErrInvalid) || a != nil {
		t.Fatal(a, err)
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	for _, name := range []string{"nil-context", "cancelled", "disabled", "unbound", "closed", "verifier-error", "both-purposes", "invalid-reserve", "unbound-use", "ordinary-fitting"} {
		t.Run(name, func(t *testing.T) {
			s := boundedControlStore(t, func(c *Config) {
				if name != "disabled" {
					c.Recovery = ApplicationRecoveryConfig{Version: 1}
				}
			})
			sentinel := errors.New("selected verifier failure")
			r := ApplicationRecoveryReservation{ID: [32]byte{41}, Digest: [32]byte{42}, Credits: []ApplicationRecoveryCredit{{Phase: 1, CommandBytes: 6, RetainedBytes: 1000, RetainedRecords: 4}}}
			verifier := recoveryAdmissionTestVerifier(func(_ context.Context, data []byte, v *ApplicationView) (ApplicationRecoveryPurpose, error) {
				root, err := v.RootBounded(t.Context(), 4)
				if err != nil || root.Index != 1 || !bytes.Equal(root.Image, []byte("seed")) {
					t.Fatal("selected verifier did not receive actual same retained root", root, err)
				}
				switch name {
				case "verifier-error":
					return ApplicationRecoveryPurpose{}, sentinel
				case "both-purposes":
					return ApplicationRecoveryPurpose{Reserve: &r, Use: &ApplicationRecoveryUse{}}, nil
				case "invalid-reserve":
					return ApplicationRecoveryPurpose{Reserve: &ApplicationRecoveryReservation{}}, nil
				case "unbound-use":
					return ApplicationRecoveryPurpose{Use: &ApplicationRecoveryUse{ID: r.ID, Digest: r.Digest, CommandDigest: sha256.Sum256(data), Phase: 1}}, nil
				default:
					return ApplicationRecoveryPurpose{}, nil
				}
			})
			if name != "disabled" && name != "unbound" {
				if err := s.BindApplicationRecoveryVerifier(s.ApplicationBinding(), verifier); err != nil {
					t.Fatal(err)
				}
			}
			ctx := t.Context()
			want := ErrInvalid
			switch name {
			case "nil-context":
				ctx = nil //nolint:staticcheck // SA1012: direct nil-context public refusal contract.
			case "cancelled":
				ctx, want = cancelled, context.Canceled
			case "closed":
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				want = ErrClosed
			case "verifier-error":
				want = sentinel
			case "ordinary-fitting":
				want = nil
			}
			before := s.meta
			views, viewBytes, pins := s.views, s.viewBytes, s.pinnedApplicationBytes
			a, err := s.AdmitApplicationRecovery(ctx, []byte("finish"))
			if want == nil {
				if err != nil || a == nil || a.purpose.Reserve != nil || a.purpose.Use != nil {
					t.Fatal("fitting ordinary guard", a, err)
				}
			} else if !errors.Is(err, want) || a != nil {
				t.Fatal(a, err, want)
			}
			if !reflect.DeepEqual(s.meta, before) || s.views != views || s.viewBytes != viewBytes || s.pinnedApplicationBytes != pins || s.poison != nil {
				t.Fatal("admission refusal/inspection mutated state or retained its borrowed view")
			}
		})
	}
}

func TestRecoveryAdmitApplicationRecoveryRejectsChangedCapturedFence(t *testing.T) {
	s := boundedControlStore(t, func(c *Config) { c.Recovery = ApplicationRecoveryConfig{Version: 1} })
	advanced := false
	verifier := recoveryAdmissionTestVerifier(func(_ context.Context, _ []byte, v *ApplicationView) (ApplicationRecoveryPurpose, error) {
		if advanced {
			return ApplicationRecoveryPurpose{}, nil
		}
		root, err := v.RootBounded(t.Context(), 4)
		if err != nil || root.Index != 1 {
			t.Fatal(root, err)
		}
		advanced = true
		persist(t, s, 2, 2, ent(2, 2, "ordinary"))
		if err := s.InstallApplication(2, ApplicationBatch{BaseGeneration: root.Generation, BaseIndex: root.Index, BaseImageHash: root.ImageHash, Image: root.Image}); err != nil {
			t.Fatal(err)
		}
		return ApplicationRecoveryPurpose{}, nil
	})
	if err := s.BindApplicationRecoveryVerifier(s.ApplicationBinding(), verifier); err != nil {
		t.Fatal(err)
	}
	a, err := s.AdmitApplicationRecovery(t.Context(), []byte("finish"))
	if !errors.Is(err, ErrInvalid) || a != nil || !advanced || s.meta.Applied != 2 || s.meta.Recovery.Reservation.ID != ([32]byte{}) || s.views != 0 || s.viewBytes != 0 || s.poison != nil {
		t.Fatal("changed actual fence admitted or borrowed source leaked", a, err, s.meta.Applied, s.views)
	}
	if a, err := s.AdmitApplicationRecovery(t.Context(), []byte("ordinary")); err != nil || a == nil || a.base.Index != 2 {
		t.Fatal("new actual root fitting guard", a, err)
	}
}

func TestRecoveryDurableQueueMalformedFramesAndChangedCommandCannotReopen(t *testing.T) {
	p, l := DefaultApplicationPolicy(1), DefaultLimits()
	r := ApplicationRecoveryReservation{ID: [32]byte{51}, Digest: [32]byte{52}, Credits: []ApplicationRecoveryCredit{{Phase: 1, CommandBytes: 6, RetainedBytes: 1000, RetainedRecords: 4}}}
	hash := sha256.Sum256([]byte("finish"))
	reserve := appendRecoveryQueue(nil, []applicationRecoveryQueued{{Index: 2, ReservationOriginIndex: 2, CommandDigest: hash, Purpose: applicationRecoveryQueuedPurpose{Reserve: &r}}})
	u := ApplicationRecoveryUse{ID: r.ID, Digest: r.Digest, CommandDigest: hash, Phase: 1}
	use := appendRecoveryQueue(nil, []applicationRecoveryQueued{{Index: 3, CommandDigest: hash, Purpose: applicationRecoveryQueuedPurpose{Use: &u}, Credit: r.Credits[0]}})
	for _, row := range []struct {
		name   string
		wire   []byte
		mutate func([]byte) []byte
	}{
		{"short", reserve, func(b []byte) []byte { return b[:7] }},
		{"magic", reserve, func(b []byte) []byte { b[0]++; return b }},
		{"count", reserve, func(b []byte) []byte { binary.BigEndian.PutUint32(b[4:8], math.MaxUint32); return b }},
		{"payload-short", use, func(b []byte) []byte { return b[:len(b)-1] }},
		{"reserve-short", reserve, func(b []byte) []byte { binary.BigEndian.PutUint32(b[49:53], 67); return b }},
		{"reserve-credit-count", reserve, func(b []byte) []byte { binary.BigEndian.PutUint32(b[125:129], 2); return b }},
		{"reserve-command", reserve, func(b []byte) []byte { binary.BigEndian.PutUint32(b[133:137], math.MaxUint32); return b }},
		{"reserve-phase", reserve, func(b []byte) []byte { binary.BigEndian.PutUint32(b[129:133], 0); return b }},
		{"use-size", use, func(b []byte) []byte { binary.BigEndian.PutUint32(b[49:53], 123); return b }},
		{"use-command", use, func(b []byte) []byte { binary.BigEndian.PutUint32(b[157:161], math.MaxUint32); return b }},
		{"kind", use, func(b []byte) []byte { b[48] = 9; return b }},
	} {
		t.Run(row.name, func(t *testing.T) {
			b := row.mutate(bytes.Clone(row.wire))
			before := bytes.Clone(b)
			q, tail, err := decodeRecoveryQueue(b, p, l)
			if !errors.Is(err, ErrCorrupt) || q != nil || tail != nil || !bytes.Equal(b, before) {
				t.Fatal("malformed durable queue decoded or mutated its borrowed wire", q, tail, err)
			}
		})
	}
	for _, wire := range [][]byte{reserve, use} {
		q, tail, err := decodeRecoveryQueue(wire, p, l)
		if err != nil || len(q) != 1 || len(tail) != 0 || !bytes.Equal(appendRecoveryQueue(nil, q), wire) {
			t.Fatal("independent valid frame parity", q, tail, err)
		}
	}
	var cfg Config
	s := boundedControlStore(t, func(c *Config) { c.Recovery = ApplicationRecoveryConfig{Version: 1}; cfg = *c })
	if err := s.BindApplicationRecoveryVerifier(s.ApplicationBinding(), &queuedRecoveryTestVerifier{reserve: r}); err != nil {
		t.Fatal(err)
	}
	persist(t, s, 2, 2, ent(2, 2, "register"))
	m := s.meta
	m.Recovery.Queued = append([]applicationRecoveryQueued(nil), m.Recovery.Queued...)
	m.Recovery.Queued[0].CommandDigest[0] ^= 1
	wire, err := encodeMeta(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.db.Set(metaKey, wire, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.Create = false
	cfg.Transfer.Contract, err = ApplicationContractForPolicyWithRecovery(cfg.Application, cfg.Controls, cfg.Recovery)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := Open(cfg)
	if !errors.Is(err, ErrCorrupt) || opened != nil {
		if opened != nil {
			_ = opened.Close()
		}
		t.Fatal("changed durable queue hash must not authorize another command on reopen", opened, err)
	}
}

// Missing-capability RED: the explicit own terminal retirement contract must not
// leave an unvisited Prepare obligation behind after ABORT and Resolve.
func TestRecoveryOwnTerminalAbortMustRetireUnusedPrepareWithoutSpendingResolve(t *testing.T) {
	s := boundedControlStore(t, func(c *Config) {
		c.Recovery = ApplicationRecoveryConfig{Version: 1}
		// Use the existing default policy for the three-credit root's encoding;
		// the smaller 1664-byte two-credit fixture is not this workload's policy.
		c.Application.MaxInstallBytes = DefaultApplicationPolicy(1).MaxInstallBytes
	})
	r := ApplicationRecoveryReservation{ID: [32]byte{61}, Digest: [32]byte{62}, Credits: []ApplicationRecoveryCredit{
		{Phase: 2, CommandBytes: 7, RetainedBytes: 1000, RetainedRecords: 4},
		{Phase: 3, CommandBytes: 7, RetainedBytes: 1000, RetainedRecords: 4},
		{Phase: 4, CommandBytes: 7, RetainedBytes: 1000, RetainedRecords: 4},
	}}
	base, image, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	persist(t, s, 2, 2, ent(2, 2, "register"))
	if err := s.InstallApplication(2, ApplicationBatch{BaseGeneration: s.ApplicationGeneration(), BaseIndex: base, BaseImageHash: sha256.Sum256(image), Image: image, RecoveryReserve: &r}); err != nil {
		t.Fatal(err)
	}
	verifier := recoveryAdmissionTestVerifier(func(_ context.Context, data []byte, _ *ApplicationView) (ApplicationRecoveryPurpose, error) {
		u := ApplicationRecoveryUse{ID: r.ID, Digest: r.Digest, CommandDigest: sha256.Sum256(data), Phase: 4}
		if bytes.Equal(data, []byte("abort")) {
			u.Phase, u.RetireBefore = 3, 3
		}
		return ApplicationRecoveryPurpose{Use: &u}, nil
	})
	if err := s.BindApplicationRecoveryVerifier(s.ApplicationBinding(), verifier); err != nil {
		t.Fatal(err)
	}
	for _, phase := range []struct {
		index uint64
		data  string
		use   ApplicationRecoveryUse
	}{
		{3, "abort", ApplicationRecoveryUse{ID: r.ID, Digest: r.Digest, CommandDigest: sha256.Sum256([]byte("abort")), Phase: 3, RetireBefore: 3}},
		{4, "resolve", ApplicationRecoveryUse{ID: r.ID, Digest: r.Digest, CommandDigest: sha256.Sum256([]byte("resolve")), Phase: 4}},
	} {
		a, err := s.AdmitApplicationRecovery(t.Context(), []byte(phase.data))
		if err != nil {
			t.Fatal(err)
		}
		persist(t, s, 2, phase.index, ent(phase.index, 2, phase.data))
		if phase.index == 3 {
			before := readyMetadataFingerprint(t, s)
			for _, retire := range []uint32{0, 2, 4, math.MaxUint32} {
				bad := phase.use
				bad.RetireBefore = retire
				err := s.InstallApplication(3, ApplicationBatch{BaseGeneration: s.ApplicationGeneration(), BaseIndex: 2, BaseImageHash: sha256.Sum256(image), Image: image, RecoveryUse: &bad, RecoveryAdmission: a})
				if !errors.Is(err, ErrInvalid) || readyMetadataFingerprint(t, s) != before || s.poison != nil {
					t.Fatal("forged terminal retirement escaped exact admission", retire, err)
				}
			}
		}
		if err := s.InstallApplication(phase.index, ApplicationBatch{BaseGeneration: s.ApplicationGeneration(), BaseIndex: phase.index - 1, BaseImageHash: sha256.Sum256(image), Image: image, RecoveryUse: &phase.use, RecoveryAdmission: a}); err != nil {
			t.Fatal(err)
		}
		if phase.index == 3 {
			if s.meta.Recovery.Reservation.ID != r.ID || s.meta.Recovery.Reservation.Digest != r.Digest || !reflect.DeepEqual(s.meta.Recovery.Reservation.Credits, r.Credits[2:]) {
				t.Fatal("own ABORT must retire only unused Prepare and consumed Decide, preserving Resolve", s.meta.Recovery.Reservation)
			}
		} else if s.meta.Recovery.Reservation.ID != ([32]byte{}) || len(s.meta.Recovery.Reservation.Credits) != 0 {
			t.Fatal("completed own ABORT/Resolve left unused Prepare blocking fresh work", s.meta.Recovery.Reservation)
		}
	}
	if !reflect.DeepEqual(r.Credits, []ApplicationRecoveryCredit{{Phase: 2, CommandBytes: 7, RetainedBytes: 1000, RetainedRecords: 4}, {Phase: 3, CommandBytes: 7, RetainedBytes: 1000, RetainedRecords: 4}, {Phase: 4, CommandBytes: 7, RetainedBytes: 1000, RetainedRecords: 4}}) {
		t.Fatal("borrowed original credits mutated")
	}
}

func retirementRecoveryStore(t *testing.T, selectVerifier ...func(ApplicationRecoveryReservation) recoveryAdmissionTestVerifier) (*Store, Config, ApplicationRecoveryReservation, recoveryAdmissionTestVerifier) {
	t.Helper()
	var cfg Config
	s := boundedControlStore(t, func(c *Config) {
		c.Recovery = ApplicationRecoveryConfig{Version: 1}
		c.Application.MaxInstallBytes = DefaultApplicationPolicy(1).MaxInstallBytes
		cfg = *c
	})
	r := ApplicationRecoveryReservation{ID: [32]byte{71}, Digest: [32]byte{72}, Credits: []ApplicationRecoveryCredit{
		{Phase: 2, CommandBytes: 8, RetainedBytes: 1000, RetainedRecords: 4},
		{Phase: 3, CommandBytes: 8, RetainedBytes: 1000, RetainedRecords: 4},
		{Phase: 4, CommandBytes: 8, RetainedBytes: 1000, RetainedRecords: 4},
	}}
	verifier := recoveryAdmissionTestVerifier(func(_ context.Context, data []byte, _ *ApplicationView) (ApplicationRecoveryPurpose, error) {
		if bytes.Equal(data, []byte("register")) {
			return ApplicationRecoveryPurpose{Reserve: &r}, nil
		}
		u := ApplicationRecoveryUse{ID: r.ID, Digest: r.Digest, CommandDigest: sha256.Sum256(data)}
		switch string(data) {
		case "prepare":
			u.Phase = 2
		case "abort":
			u.Phase, u.RetireBefore = 3, 3
		case "resolve":
			u.Phase = 4
		default:
			return ApplicationRecoveryPurpose{}, nil
		}
		return ApplicationRecoveryPurpose{Use: &u}, nil
	})
	if len(selectVerifier) > 0 {
		verifier = selectVerifier[0](r)
	}
	if err := s.BindApplicationRecoveryVerifier(s.ApplicationBinding(), verifier); err != nil {
		t.Fatal(err)
	}
	return s, cfg, r, verifier
}

func TestRecoveryQueuedAbortReopenMustKeepRetirementAndUnusedEscrow(t *testing.T) {
	s, cfg, r, verifier := retirementRecoveryStore(t)
	_, image, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	persist(t, s, 2, 2, ent(2, 2, "register"))
	if err := s.InstallApplication(2, ApplicationBatch{BaseGeneration: s.ApplicationGeneration(), BaseIndex: 1, BaseImageHash: sha256.Sum256(image), Image: image, RecoveryReserve: &r}); err != nil {
		t.Fatal(err)
	}
	persist(t, s, 2, 3, ent(3, 2, "abort"))
	future, pendingBytes, pendingRecords, err := recoveryProjected(s.meta, s.limits, 1)
	// Uninstalled ABORT may convert only Decide into pending charge. The unused
	// Prepare and still-needed Resolve stay protected across suffix replacement.
	if err != nil || future.bytes != 2000 || future.records != 8 || pendingBytes != 1000 || pendingRecords != 4 || !sameRecoveryReservation(s.meta.Recovery.Reservation, r) {
		t.Fatal("queued ABORT speculatively refunded unused capacity", future, pendingBytes, pendingRecords, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.Create = false
	cfg.Transfer.Contract, err = ApplicationContractForPolicyWithRecovery(cfg.Application, cfg.Controls, cfg.Recovery)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	})
	if len(reopened.meta.Recovery.Queued) != 1 || reopened.meta.Recovery.Queued[0].Purpose.Use.RetireBefore != 3 {
		t.Fatal("reopen discarded exact own-terminal retirement binding", reopened.meta.Recovery.Queued)
	}
	if err := reopened.BindApplicationRecoveryVerifier(reopened.ApplicationBinding(), verifier); err != nil {
		t.Fatal(err)
	}
	a, err := reopened.RecoveryAdmissionForCommittedEntry(t.Context(), 3)
	if err != nil {
		t.Fatal(err)
	}
	u := ApplicationRecoveryUse{ID: r.ID, Digest: r.Digest, CommandDigest: sha256.Sum256([]byte("abort")), Phase: 3, RetireBefore: 3}
	if err := reopened.InstallApplication(3, ApplicationBatch{BaseGeneration: reopened.ApplicationGeneration(), BaseIndex: 2, BaseImageHash: sha256.Sum256(image), Image: image, RecoveryUse: &u, RecoveryAdmission: a}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(reopened.meta.Recovery.Reservation.Credits, r.Credits[2:]) {
		t.Fatal("installed ABORT failed exact retirement", reopened.meta.Recovery.Reservation)
	}
}

func TestRecoveryCachedRegisterAfterQueuedPrepareMustNotRecreditOrBlock(t *testing.T) {
	s, _, r, _ := retirementRecoveryStore(t)
	before := readyMetadataFingerprint(t, s)
	rd := raft.Ready{HardState: &pb.HardState{Term: new(uint64(2)), Commit: new(uint64(6))}, Entries: []*pb.Entry{ent(2, 2, "register"), ent(3, 2, "prepare"), ent(4, 2, "register"), ent(5, 2, "abort"), ent(6, 2, "resolve")}}
	if err := s.Persist(rd); err != nil {
		t.Fatalf("exact cached registration after queued Prepare conflicts with its own remaining phases: %v", err)
	}
	if readyMetadataFingerprint(t, s) == before || s.meta.Applied != 1 || s.meta.Recovery.Reservation.ID != ([32]byte{}) {
		t.Fatal("queued fixture changed applied ownership or persisted nothing")
	}
	_, image, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	for index := uint64(2); index <= 6; index++ {
		a, err := s.RecoveryAdmissionForCommittedEntry(t.Context(), index)
		if err != nil {
			t.Fatal(err)
		}
		b := ApplicationBatch{BaseGeneration: s.ApplicationGeneration(), BaseIndex: index - 1, BaseImageHash: sha256.Sum256(image), Image: image, RecoveryAdmission: a}
		switch index {
		case 2:
			b.RecoveryReserve = &r
		case 3:
			b.RecoveryUse = &ApplicationRecoveryUse{ID: r.ID, Digest: r.Digest, CommandDigest: sha256.Sum256([]byte("prepare")), Phase: 2}
		case 5:
			b.RecoveryUse = &ApplicationRecoveryUse{ID: r.ID, Digest: r.Digest, CommandDigest: sha256.Sum256([]byte("abort")), Phase: 3, RetireBefore: 3}
		case 6:
			b.RecoveryUse = &ApplicationRecoveryUse{ID: r.ID, Digest: r.Digest, CommandDigest: sha256.Sum256([]byte("resolve")), Phase: 4}
		}
		if err := s.InstallApplication(index, b); err != nil {
			t.Fatal(index, err)
		}
	}
	if s.meta.Applied != 6 || len(s.meta.Recovery.Queued) != 0 || s.meta.Recovery.Reservation.ID != ([32]byte{}) {
		t.Fatal("cached registration resurrected terminal capacity", s.meta.Recovery)
	}
}

func TestRecoveryOwnerUsesExactBorrowedCutAndBoundsInspectionWithoutCreditClone(t *testing.T) {
	s, _, r, verifier := retirementRecoveryStore(t)
	_, image, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	persist(t, s, 2, 2, ent(2, 2, "register"))
	if err := s.InstallApplication(2, ApplicationBatch{BaseGeneration: s.ApplicationGeneration(), BaseIndex: 1, BaseImageHash: sha256.Sum256(image), Image: image, RecoveryReserve: &r}); err != nil {
		t.Fatal(err)
	}
	old, err := s.ApplicationView(2)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := old.Close(); err != nil {
			t.Error(err)
		}
	})
	// Three credits: RS1 72+3*24=144, AV1 adds36. Independent charge is
	// fixed128+4*key9, plus key9+frame180+CRC13+4*payload144+scalar64 =1006 bytes, one point.
	owner, work, err := old.RecoveryOwner(t.Context(), ReadBudget{Rows: 1, Bytes: 1006})
	if err != nil || owner != (ApplicationRecoveryOwner{ID: r.ID, Digest: r.Digest}) || work != (ApplicationProofWork{Records: 1, Bytes: 1006}) {
		t.Fatal(owner, work, err)
	}
	before := readyMetadataFingerprint(t, s)
	limited, w, e := old.RecoveryOwner(t.Context(), ReadBudget{Rows: 1, Bytes: 1005})
	if !errors.Is(e, ErrLimit) || limited != (ApplicationRecoveryOwner{}) || w != (ApplicationProofWork{Records: 1, Bytes: 164}) || readyMetadataFingerprint(t, s) != before || s.poison != nil {
		t.Fatal("independent one-byte refusal", limited, w, e)
	}
	for _, step := range []struct {
		index uint64
		data  string
	}{{3, "abort"}, {4, "resolve"}} {
		purpose, err := verifier.VerifyApplicationRecovery(t.Context(), []byte(step.data), old)
		if err != nil {
			t.Fatal(err)
		}
		a, err := s.AdmitApplicationRecovery(t.Context(), []byte(step.data))
		if err != nil {
			t.Fatal(err)
		}
		persist(t, s, 2, step.index, ent(step.index, 2, step.data))
		if err := s.InstallApplication(step.index, ApplicationBatch{BaseGeneration: s.ApplicationGeneration(), BaseIndex: step.index - 1, BaseImageHash: sha256.Sum256(image), Image: image, RecoveryUse: purpose.Use, RecoveryAdmission: a}); err != nil {
			t.Fatal(err)
		}
	}
	current, err := s.ApplicationView(4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := current.Close(); err != nil {
			t.Error(err)
		}
	})
	currentOwner, _, err := current.RecoveryOwner(t.Context(), ReadBudget{Rows: 1, Bytes: 4096})
	if err != nil || currentOwner != (ApplicationRecoveryOwner{}) {
		t.Fatal("terminal current owner", currentOwner, err)
	}
	oldOwner, _, err := old.RecoveryOwner(t.Context(), ReadBudget{Rows: 1, Bytes: 4096})
	if err != nil || oldOwner != owner {
		t.Fatal("retained owner followed current metadata", oldOwner, err)
	}
	var nilView *ApplicationView
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	copied := ApplicationView{s: old.s, index: old.index, bank: old.bank, generation: old.generation}
	for _, row := range []struct {
		view   *ApplicationView
		ctx    context.Context
		budget ReadBudget
		want   error
	}{
		{nilView, t.Context(), ReadBudget{Rows: 1, Bytes: 4096}, ErrInvalid},
		{&copied, t.Context(), ReadBudget{Rows: 1, Bytes: 4096}, ErrInvalid},
		{old, nil, ReadBudget{Rows: 1, Bytes: 4096}, ErrInvalid},
		{old, cancelled, ReadBudget{Rows: 1, Bytes: 4096}, context.Canceled},
		{old, t.Context(), ReadBudget{}, ErrInvalid},
		{old, t.Context(), ReadBudget{Rows: 1, Bytes: 163}, ErrLimit},
	} {
		o, _, err := row.view.RecoveryOwner(row.ctx, row.budget)
		if !errors.Is(err, row.want) || o != (ApplicationRecoveryOwner{}) {
			t.Fatal(o, err, row.want)
		}
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	if o, _, err := old.RecoveryOwner(t.Context(), ReadBudget{Rows: 1, Bytes: 4096}); !errors.Is(err, ErrClosed) || o != (ApplicationRecoveryOwner{}) {
		t.Fatal(o, err)
	}
}

func TestRecoveryStaleQueuedRegisterAndUseCannotResurrectAbortOrDisplaceNewOwner(t *testing.T) {
	fresh := ApplicationRecoveryReservation{ID: [32]byte{81}, Digest: [32]byte{82}, Credits: []ApplicationRecoveryCredit{{Phase: 9, CommandBytes: 8, RetainedBytes: 1000, RetainedRecords: 4}}}
	// The genuine public binding selects this fixed table once before traffic.
	s, cfg, old, _ := retirementRecoveryStore(t, func(old ApplicationRecoveryReservation) recoveryAdmissionTestVerifier {
		return recoveryAdmissionTestVerifier(func(_ context.Context, data []byte, _ *ApplicationView) (ApplicationRecoveryPurpose, error) {
			switch string(data) {
			case "register":
				return ApplicationRecoveryPurpose{Reserve: &old}, nil
			case "new":
				return ApplicationRecoveryPurpose{Reserve: &fresh}, nil
			case "abort":
				return ApplicationRecoveryPurpose{Use: &ApplicationRecoveryUse{ID: old.ID, Digest: old.Digest, CommandDigest: sha256.Sum256(data), Phase: 3, RetireBefore: 3}}, nil
			case "resolve":
				return ApplicationRecoveryPurpose{Use: &ApplicationRecoveryUse{ID: old.ID, Digest: old.Digest, CommandDigest: sha256.Sum256(data), Phase: 4, RetireBefore: 4}}, nil
			case "prepare":
				return ApplicationRecoveryPurpose{Use: &ApplicationRecoveryUse{ID: old.ID, Digest: old.Digest, CommandDigest: sha256.Sum256(data), Phase: 2}}, nil
			default:
				return ApplicationRecoveryPurpose{}, nil
			}
		})
	})
	_, image, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	persistReady := raft.Ready{HardState: &pb.HardState{Term: new(uint64(2)), Commit: new(uint64(6))}, Entries: []*pb.Entry{ent(2, 2, "register"), ent(3, 2, "abort"), ent(4, 2, "resolve"), ent(5, 2, "register"), ent(6, 2, "prepare")}}
	if err := s.Persist(persistReady); err != nil {
		t.Fatal(err)
	}
	for index := uint64(2); index <= 4; index++ {
		a, err := s.RecoveryAdmissionForCommittedEntry(t.Context(), index)
		if err != nil {
			t.Fatal(err)
		}
		b := ApplicationBatch{BaseGeneration: s.ApplicationGeneration(), BaseIndex: index - 1, BaseImageHash: sha256.Sum256(image), Image: image, RecoveryAdmission: a}
		if index == 2 {
			b.RecoveryReserve = &old
		} else {
			phase, retire, data := uint32(3), uint32(3), "abort"
			if index == 4 {
				phase, retire, data = 4, 4, "resolve"
			}
			b.RecoveryUse = &ApplicationRecoveryUse{ID: old.ID, Digest: old.Digest, CommandDigest: sha256.Sum256([]byte(data)), Phase: phase, RetireBefore: retire}
		}
		if err := s.InstallApplication(index, b); err != nil {
			t.Fatal(index, err)
		}
	}
	if s.meta.Recovery.Reservation.ID != ([32]byte{}) || len(s.meta.Recovery.Queued) != 2 || s.meta.Recovery.Queued[0].ReservationOriginIndex != 2 {
		t.Fatal("old cached origin/terminal owner", s.meta.Recovery)
	}
	// A newer owner can book behind the two bounded old no-op recipes. Neither
	// stale registration nor retired Prepare may manufacture future old credits.
	persist(t, s, 2, 7, ent(7, 2, "new"))
	growth, _, _, err := recoveryProjected(s.meta, s.limits, 3)
	if err != nil || growth.bytes != 1000 || growth.records != 4 {
		t.Fatal("cached old intent displaced new protected owner", growth, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.Create = false
	cfg.Transfer.Contract, err = ApplicationContractForPolicyWithRecovery(cfg.Application, cfg.Controls, cfg.Recovery)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	})
	if reopened.meta.Recovery.Queued[0].ReservationOriginIndex != 2 {
		t.Fatal("reopen lost exact retired origin")
	}
	for index := uint64(5); index <= 7; index++ {
		b := ApplicationBatch{BaseGeneration: reopened.ApplicationGeneration(), BaseIndex: index - 1, BaseImageHash: sha256.Sum256(image), Image: image}
		if index == 7 {
			b.RecoveryReserve = &fresh
		}
		if index == 6 {
			before := readyMetadataFingerprint(t, reopened)
			bad := b
			bad.Writes = []KV{{Key: []byte("x"), Value: make([]byte, 800)}}
			// Payload800 + one AV1 data frame/key + four fixed native envelopes
			// exceeds the old1000-byte phase. The ordinary default has room.
			if err := reopened.InstallApplication(index, bad); !errors.Is(err, ErrLimit) || readyMetadataFingerprint(t, reopened) != before || reopened.poison != nil {
				t.Fatal("retired replay escaped its pending backing charge", err)
			}
		}
		if err := reopened.InstallApplication(index, b); err != nil {
			t.Fatal(index, err)
		}
	}
	if !sameRecoveryReservation(reopened.meta.Recovery.Reservation, fresh) || len(reopened.meta.Recovery.Queued) != 0 {
		t.Fatal("old cached recipes resurrected or changed new owner", reopened.meta.Recovery)
	}
}

func TestRecoveryScrubApplicationMustInspectRetainedAndCurrentNativeRoots(t *testing.T) {
	for _, index := range []uint64{2, 4} {
		t.Run(fmt.Sprint(index), func(t *testing.T) {
			s, _, r, verifier := retirementRecoveryStore(t)
			_, image, err := s.Checkpoint()
			if err != nil {
				t.Fatal(err)
			}
			persist(t, s, 2, 2, ent(2, 2, "register"))
			if err := s.InstallApplication(2, ApplicationBatch{BaseGeneration: s.ApplicationGeneration(), BaseIndex: 1, BaseImageHash: sha256.Sum256(image), Image: image, RecoveryReserve: &r}); err != nil {
				t.Fatal(err)
			}
			for _, step := range []struct {
				index uint64
				data  string
			}{{3, "abort"}, {4, "resolve"}} {
				purpose, err := verifier.VerifyApplicationRecovery(t.Context(), []byte(step.data), nil)
				if err != nil {
					t.Fatal(err)
				}
				a, err := s.AdmitApplicationRecovery(t.Context(), []byte(step.data))
				if err != nil {
					t.Fatal(err)
				}
				persist(t, s, 2, step.index, ent(step.index, 2, step.data))
				if err := s.InstallApplication(step.index, ApplicationBatch{BaseGeneration: s.ApplicationGeneration(), BaseIndex: step.index - 1, BaseImageHash: sha256.Sum256(image), Image: image, RecoveryUse: purpose.Use, RecoveryAdmission: a}); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.ScrubApplication(t.Context()); err != nil {
				t.Fatal("valid native census guard", err)
			}
			key := appIndexKey(recoveryRootTag+s.activeBank(), index)
			raw, closer, err := s.db.Get(key)
			if err != nil {
				t.Fatal(err)
			}
			bad := bytes.Clone(raw)
			if err := closer.Close(); err != nil {
				t.Fatal(err)
			}
			bad[len(bad)-1] ^= 1
			if err := s.db.Set(key, bad, pebble.Sync); err != nil {
				t.Fatal(err)
			}
			before := readyMetadataFingerprint(t, s)
			if err := s.ScrubApplication(t.Context()); !errors.Is(err, ErrCorrupt) {
				t.Fatal("native recovery root escaped complete scrub", index, err)
			}
			if !errors.Is(s.poison, ErrCorrupt) || readyMetadataFingerprint(t, s) != before {
				t.Fatal("scrub failed to poison corruption or mutated durable rows")
			}
		})
	}
}

func TestRecoveryCurrentRootCreditMismatchRefusesOwnerAndScrub(t *testing.T) {
	for _, door := range []string{"owner", "scrub"} {
		t.Run(door, func(t *testing.T) {
			s, _, r, _ := retirementRecoveryStore(t)
			_, image, err := s.Checkpoint()
			if err != nil {
				t.Fatal(err)
			}
			persist(t, s, 2, 2, ent(2, 2, "register"))
			if err := s.InstallApplication(2, ApplicationBatch{BaseGeneration: s.ApplicationGeneration(), BaseIndex: 1, BaseImageHash: sha256.Sum256(image), Image: image, RecoveryReserve: &r}); err != nil {
				t.Fatal(err)
			}
			v, err := s.ApplicationView(2)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := v.Close(); !errors.Is(err, ErrCorrupt) {
					t.Errorf("closing the poisoned borrowed view must retain ErrCorrupt: %v", err)
				}
			})
			changed := cloneRecoveryReservation(r)
			changed.Credits[0].RetainedBytes--
			key := appIndexKey(recoveryRootTag+s.activeBank(), 2)
			if err := s.db.Set(key, appFrame(key, appendRecoveryRoot(changed), false), pebble.Sync); err != nil {
				t.Fatal(err)
			}
			before := readyMetadataFingerprint(t, s)
			if door == "owner" {
				o, _, err := v.RecoveryOwner(t.Context(), ReadBudget{Rows: 1, Bytes: 4096})
				if !errors.Is(err, ErrCorrupt) || o != (ApplicationRecoveryOwner{}) {
					t.Fatal("validly framed credit mismatch escaped owner inspection", o, err)
				}
			} else if err := s.ScrubApplication(t.Context()); !errors.Is(err, ErrCorrupt) {
				t.Fatal("validly framed credit mismatch escaped scrub", err)
			}
			if !errors.Is(s.poison, ErrCorrupt) || readyMetadataFingerprint(t, s) != before {
				t.Fatal("corruption refusal mutated data or failed to poison")
			}
		})
	}
}
