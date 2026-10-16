package replica

import (
	"errors"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

func recoveryCreationDriverGroup(t *testing.T) map[uint64]*Driver {
	t.Helper()
	nodes := map[uint64]*Driver{}
	for id := uint64(1); id <= 3; id++ {
		p := raftlog.DefaultApplicationPolicy(id)
		p.MaxInstallWrites, p.MaxInstallBytes = 1, 1664
		p.RetainedApplicationRecords = 128
		controls := raftlog.ApplicationControlConfig{Version: 1}
		recovery := raftlog.ApplicationRecoveryConfig{Version: 1}
		contract, err := raftlog.ApplicationContractForPolicyWithRecovery(p, controls, recovery)
		if err != nil {
			t.Fatal(err)
		}
		cfg := raftlog.Config{Dir: fmt.Sprintf("creation-%d", id), FS: vfs.NewMem(), Create: true, Application: p, Controls: controls, Recovery: recovery, Transfer: raftlog.ApplicationTransferConfig{Identity: raftlog.ApplicationIdentity{Graph: [16]byte{1}, Partition: 1, Group: [16]byte{2}}, Contract: contract, Limits: raftlog.DefaultApplicationTransferLimits()}, Generations: raftlog.ApplicationGenerationLimits{MaxBytes: 2 << 30, MaxRecords: 2_000_000}, PublishedCuts: raftlog.ApplicationPublishedCutLimits{MaxTransferChunks: 10_000}, Replication: raftlog.ApplicationReplicationConfig{Voters: [3]uint64{1, 2, 3}}, SemanticContractID: raftlog.ApplicationSemanticContractID{42}}
		s, err := raftlog.Open(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Initialize([]uint64{1, 2, 3}, nil); err != nil {
			t.Fatal(err)
		}
		machine := &recoveryDriverFixture{store: s, reservation: raftlog.ApplicationRecoveryReservation{ID: [32]byte{90}, Digest: [32]byte{91}, Credits: []raftlog.ApplicationRecoveryCredit{{Phase: 1, CommandBytes: 6, RetainedBytes: 1000, RetainedRecords: 91}}}}
		d, err := Open(Config{ID: id, Store: s, ApplicationMachine: machine, ApplicationSnapshotSends: ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 4 << 20}})
		if err != nil {
			t.Fatal(err)
		}
		nodes[id] = d
		t.Cleanup(func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	return nodes
}

func TestRecoveryCreationNativeRootRefusesLocalAndForwardedBeforeRawNode(t *testing.T) {
	for _, sender := range []uint64{1, 3} {
		t.Run(fmt.Sprint(sender), func(t *testing.T) {
			nodes := recoveryCreationDriverGroup(t)
			deliverApplication(t, nodes, packets(t, nodes[1].Campaign))
			applied := nodes[1].Applied()
			usage, err := nodes[1].store.ApplicationUsage()
			if err != nil || applied != 2 || usage.RetainedRecords != 8 {
				t.Fatal("independent seed/election four-envelope census", applied, usage, err)
			}
			var held []Packet
			for range 4 {
				out, err := nodes[1].Propose([]byte("ordinary"))
				if err != nil {
					t.Fatal("fitting actual pending entry", err)
				}
				held = append(held, out.Packets...)
			}
			lastBefore, err := nodes[1].store.LastIndex()
			if err != nil || lastBefore != applied+4 {
				t.Fatal("four pending entries absent", lastBefore, err)
			}
			// Eight installed seed/election records + six pending/headroom installs
			// (one write+three AV1+one RS1 each) +91 phase records =129 >128.
			if 8+6*5+91 != 129 {
				t.Fatal("independent admission ledger")
			}
			beforeUsage, err := nodes[1].store.ApplicationUsage()
			if err != nil {
				t.Fatal(err)
			}
			out, err := nodes[sender].Propose([]byte("register"))
			if err == nil && sender != 1 {
				if len(out.Packets) != 1 || out.Packets[0].To != 1 {
					t.Fatal("actual forwarded MsgProp absent", out)
				}
				out, err = nodes[1].Step(out.Packets[0])
			}
			if !errors.Is(err, raftlog.ErrLimit) || errors.Is(err, ErrStopped) || len(out.Packets) != 0 {
				t.Fatalf("creation exceeded128 records after RawNode mutation instead of healthy refusal: err=%v packets=%d", err, len(out.Packets))
			}
			lastAfter, err := nodes[1].store.LastIndex()
			nodes[1].mu.Lock()
			stopped := nodes[1].stopped
			nodes[1].mu.Unlock()
			afterUsage, usageErr := nodes[1].store.ApplicationUsage()
			if err != nil || usageErr != nil || lastAfter != lastBefore || nodes[1].Applied() != applied || stopped != nil || beforeUsage != afterUsage {
				t.Fatal("refusal changed log/application or stopped Driver", lastAfter, err, usageErr, stopped)
			}
			// Deliver the previously admitted ordinary entries. Each no-write
			// installation owns four envelopes, giving24 current records. A later
			// ordinary entry fits24+2*5 well below the unchanged128 ceiling.
			deliverApplication(t, nodes, held)
			for id, d := range nodes {
				u, err := d.store.ApplicationUsage()
				if err != nil || d.Applied() != applied+4 || u.RetainedRecords != 24 {
					t.Fatal("previously admitted entries lost progress", id, d.Applied(), u, err)
				}
			}
			deliverApplication(t, nodes, packets(t, func() (Output, error) { return nodes[sender].Propose([]byte("ordinary")) }))
			for id, d := range nodes {
				u, err := d.store.ApplicationUsage()
				if err != nil || d.Applied() != applied+5 || u.RetainedRecords != 28 {
					t.Fatal("healthy later ordinary progress absent", id, d.Applied(), u, err)
				}
			}

		})
	}
}
