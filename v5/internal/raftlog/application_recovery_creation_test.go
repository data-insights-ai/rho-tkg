package raftlog

import (
	"errors"
	"fmt"
	"testing"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

// The RLM8 native root is a fourth envelope, in addition to one possible
// write and the three ordinary AV1 envelopes. Every pending install owns five
// records, even when its eventual ordinary batch contains no writes.
func TestRecoveryCreationPendingNativeRootMustBeAdmittedBeforeProposal(t *testing.T) {
	for _, phaseRecords := range []uint64{100, 95, 94} {
		t.Run(fmt.Sprint(phaseRecords), func(t *testing.T) {
			s := boundedControlStore(t, func(c *Config) {
				c.Recovery = ApplicationRecoveryConfig{Version: 1}
				c.Application.RetainedApplicationRecords = 128
			})
			reserve := ApplicationRecoveryReservation{ID: [32]byte{90}, Digest: [32]byte{91}, Credits: []ApplicationRecoveryCredit{{Phase: 1, CommandBytes: 6, RetainedBytes: 1000, RetainedRecords: phaseRecords}}}
			if err := s.BindApplicationRecoveryVerifier(s.ApplicationBinding(), &queuedRecoveryTestVerifier{reserve: reserve}); err != nil {
				t.Fatal(err)
			}
			if err := s.Persist(raft.Ready{HardState: &pb.HardState{Term: new(uint64(2)), Commit: new(uint64(1))}, Entries: []*pb.Entry{ent(2, 2, "ordinary2"), ent(3, 2, "ordinary3"), ent(4, 2, "ordinary4"), ent(5, 2, "ordinary5")}}); err != nil {
				t.Fatal(err)
			}
			usage, err := s.ApplicationUsage()
			if err != nil || s.meta.Applied != 1 || s.meta.Last != 5 || usage.RetainedRecords != 4 || s.meta.App.Policy.MaxInstallWrites != 1 {
				t.Fatal("independent seed/pending census", usage, err)
			}
			before := readyMetadataFingerprint(t, s)
			admission, err := s.AdmitApplicationRecovery(t.Context(), []byte("register"))
			const currentRecords, pendingAllowance, nativeInstallRecords, ceiling = 4, 6, 5, 128
			required := currentRecords + pendingAllowance*nativeInstallRecords + phaseRecords
			t.Logf("independent record ledger: %d + %d*%d + %d = %d, ceiling %d; admission=%v token=%v", currentRecords, pendingAllowance, nativeInstallRecords, phaseRecords, required, ceiling, err, admission != nil)
			if readyMetadataFingerprint(t, s) != before || s.poison != nil {
				t.Fatal("pure admission changed durable state")
			}
			if required > ceiling {
				if !errors.Is(err, ErrLimit) || admission != nil {
					t.Fatalf("pending native recovery-root record escaped preproposal admission: required=%d token=%v err=%v", required, admission != nil, err)
				}
				return
			}
			if required != ceiling || err != nil || admission == nil {
				t.Fatal("exact 128-record creation boundary refused", required, admission, err)
			}
			if err := s.Persist(raft.Ready{HardState: &pb.HardState{Term: new(uint64(2)), Commit: new(uint64(6))}, Entries: []*pb.Entry{ent(6, 2, "register")}}); err != nil {
				t.Fatal("admitted exact creation cannot persist", err)
			}
			if s.meta.Last != 6 || s.meta.Applied != 1 || len(s.meta.Recovery.Queued) != 1 || !sameRecoveryReservation(*s.meta.Recovery.Queued[0].Purpose.Reserve, reserve) {
				t.Fatal("fitting creation lost queued capacity")
			}
		})
	}
}
