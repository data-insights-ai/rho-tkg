package replica

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// Test-only machine proves same-batch storage/actual Ready integration, not graph semantics.
type controlApplication struct{ replicatedApplication }

func (m *controlApplication) Stage(e Entry, b raftlog.ApplicationBudget) (raftlog.ApplicationBatch, error) {
	result, err := m.replicatedApplication.Stage(e, b)
	if err != nil {
		return result, err
	}
	if len(e.Data) > 0 {
		result.ControlPuts = []raftlog.ApplicationControlPut{{Key: []byte(fmt.Sprintf("dec:%020d", e.Index)), Value: bytes.Clone(result.Outcome)}, {Key: []byte(fmt.Sprintf("reg:%020d", e.Index)), Value: bytes.Clone(e.Data)}}
	}
	return result, nil
}
func controlDriver(t *testing.T, id uint64) (*Driver, *controlApplication) {
	t.Helper()
	p := raftlog.DefaultApplicationPolicy(id)
	ctrl := raftlog.ApplicationControlConfig{Version: 1}
	contract, err := raftlog.ApplicationContractForPolicyWithControls(p, ctrl)
	if err != nil {
		t.Fatal(err)
	}
	cfg := raftlog.Config{Dir: fmt.Sprint("control-", id), FS: vfs.NewMem(), Create: true, Application: p, Controls: ctrl, Transfer: raftlog.ApplicationTransferConfig{Identity: raftlog.ApplicationIdentity{Graph: [16]byte{1}, Partition: 1, Group: [16]byte{2}}, Contract: contract, Limits: raftlog.DefaultApplicationTransferLimits()}, Generations: raftlog.ApplicationGenerationLimits{MaxBytes: 2 << 30, MaxRecords: 2000000}, PublishedCuts: raftlog.ApplicationPublishedCutLimits{MaxTransferChunks: 10000}, Replication: raftlog.ApplicationReplicationConfig{Voters: [3]uint64{1, 2, 3}}, SemanticContractID: raftlog.ApplicationSemanticContractID{17}}
	s, err := raftlog.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Initialize([]uint64{1, 2, 3}, nil); err != nil {
		t.Fatal(err)
	}
	m := &controlApplication{replicatedApplication: replicatedApplication{semantic: cfg.SemanticContractID}}
	d, err := Open(Config{ID: id, Store: s, ApplicationMachine: m, ApplicationSnapshotSends: ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 4 << 20}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	})
	return d, m
}
func TestControlActualThreeDriverSnapshotReadyPreservesJournal(t *testing.T) {
	nodes := map[uint64]*Driver{}
	machines := map[uint64]*controlApplication{}
	for id := uint64(1); id <= 3; id++ {
		nodes[id], machines[id] = controlDriver(t, id)
	}
	deliverApplication(t, nodes, packets(t, nodes[1].Campaign))
	isolated := nodes[3]
	delete(nodes, 3)
	deliverApplication(t, nodes, packets(t, func() (Output, error) { return nodes[1].Propose(command(7)) }))
	old := nodes[1].Applied()
	deliverApplication(t, nodes, packets(t, func() (Output, error) { return nodes[1].Propose(command(11)) }))
	latest := nodes[1].Applied()
	if err := nodes[1].store.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	nodes[3] = isolated
	for attempts := 0; attempts < 24 && isolated.store.ApplicationGeneration() != 2; attempts++ {
		out, err := nodes[1].Tick()
		if err != nil {
			t.Fatal(err)
		}
		if len(out.SnapshotSends) > 0 {
			deliverPreparedApplication(t, nodes, nodes[1], out)
		} else {
			deliverApplication(t, nodes, out.Packets)
		}
		offers, err := nodes[1].SnapshotSends()
		if err != nil {
			t.Fatal(err)
		}
		for _, send := range offers {
			buildSender(t, send)
			if err := nodes[1].SealSnapshotSend(send); err != nil {
				t.Fatal(err)
			}
		}
	}
	if isolated.store.ApplicationGeneration() != 2 || machines[3].value != 18 {
		t.Fatal("actual snapshot Ready did not activate", isolated.store.ApplicationGeneration(), machines[3].value)
	}
	for id, d := range nodes {
		v, err := d.store.ApplicationView(latest)
		if err != nil {
			t.Fatal(err)
		}
		for _, index := range []uint64{old, latest} {
			for _, prefix := range []string{"dec", "reg"} {
				want := []byte("accepted")
				if prefix == "reg" {
					want = []byte{0, 0, 0, 0, 0, 0, 0, 7}
					if index == latest {
						want = []byte{0, 0, 0, 0, 0, 0, 0, 11}
					}
				}
				key := []byte(fmt.Sprintf("%s:%020d", prefix, index))
				row, found, _, err := v.GetControl(t.Context(), key, raftlog.ReadBudget{Rows: 2, Bytes: 4096})
				if err != nil || !found || row.Index != index || !bytes.Equal(row.Key, key) || !bytes.Equal(row.Value, want) {
					t.Fatal(id, key, row, found, err)
				}
			}
		}
		if err := v.Close(); err != nil {
			t.Fatal(err)
		}
		before, err := d.store.ApplicationView(old)
		if err != nil {
			t.Fatal(err)
		}
		if _, found, _, err := before.GetControl(t.Context(), []byte(fmt.Sprintf("reg:%020d", latest)), raftlog.ReadBudget{Rows: 2, Bytes: 4096}); err != nil || found {
			t.Fatal("historic view leaked later journal", id, found, err)
		}
		if err := before.Close(); err != nil {
			t.Fatal(err)
		}
		if err := d.store.ScrubApplication(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
}
