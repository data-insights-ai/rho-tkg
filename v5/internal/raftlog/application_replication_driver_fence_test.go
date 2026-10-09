package raftlog_test

import (
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

type closedReplicationMachine struct{ restored bool }

func (m *closedReplicationMachine) Restore(uint64, []byte) error { m.restored = true; return nil }
func (m *closedReplicationMachine) Stage(replica.Entry, raftlog.ApplicationBudget) (raftlog.ApplicationBatch, error) {
	return raftlog.ApplicationBatch{}, raftlog.ErrInvalid
}
func TestApplicationReplicationDriverRemainsClosedBeforeRestore(t *testing.T) {
	p := raftlog.DefaultApplicationPolicy(1)
	tc := raftlog.ApplicationTransferConfig{Identity: raftlog.ApplicationIdentity{Graph: [16]byte{1}, Partition: 7, Group: [16]byte{3}}, Contract: raftlog.ApplicationContractForPolicy(p), Limits: raftlog.DefaultApplicationTransferLimits()}
	s, err := raftlog.Open(raftlog.Config{Dir: "db", FS: vfs.NewMem(), Create: true, Application: p, Transfer: tc, Generations: raftlog.ApplicationGenerationLimits{MaxBytes: 2 << 30, MaxRecords: 2000000}, PublishedCuts: raftlog.ApplicationPublishedCutLimits{MaxTransferChunks: 1000000}, Replication: raftlog.ApplicationReplicationConfig{Voters: [3]uint64{1, 2, 3}}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Initialize([]uint64{1, 2, 3}, nil); err != nil {
		t.Fatal(err)
	}
	machine := &closedReplicationMachine{}
	if _, err := replica.Open(replica.Config{ID: 1, Store: s, ApplicationMachine: machine}); !errors.Is(err, replica.ErrInvalid) || machine.restored {
		t.Fatal("Driver fence relaxed", err, machine.restored)
	}
}
