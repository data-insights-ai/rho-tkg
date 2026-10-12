package replica

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// This uses the existing trusted recovery fixture's exact Reserve/Use semantics
// and unchanged 18,875,000-byte policy. It makes no private admission capability.
type capacityRecoveryRegressionMachine struct {
	recoveryDriverFixture
	t                *testing.T
	stages, restores map[uint64]int
	verifyCalls      int
	purposes         map[string]raftlog.ApplicationRecoveryPurpose
}

func (m *capacityRecoveryRegressionMachine) Restore(index uint64, image []byte) error {
	m.restores[index]++
	if m.restores[index] != 1 {
		m.t.Errorf("duplicate Restore effect at actual committed index %d", index)
		return ErrInvalid
	}
	return m.recoveryDriverFixture.Restore(index, image)
}

func (m *capacityRecoveryRegressionMachine) Stage(e Entry, budget raftlog.ApplicationBudget) (raftlog.ApplicationBatch, error) {
	m.stages[e.Index]++
	if m.stages[e.Index] != 1 {
		m.t.Errorf("duplicate Stage effect at actual committed index %d", e.Index)
		return raftlog.ApplicationBatch{}, ErrInvalid
	}
	hard, _, err := m.store.InitialState()
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	if e.Index > hard.GetCommit() {
		m.t.Errorf("protected Stage %d above durable commit %d", e.Index, hard.GetCommit())
		return raftlog.ApplicationBatch{}, ErrInvalid
	}
	return m.recoveryDriverFixture.Stage(e, budget)
}

func (m *capacityRecoveryRegressionMachine) VerifyApplicationRecovery(ctx context.Context, data []byte, view *raftlog.ApplicationView) (raftlog.ApplicationRecoveryPurpose, error) {
	root, err := view.RootBounded(ctx, 0)
	if err != nil {
		return raftlog.ApplicationRecoveryPurpose{}, err
	}
	purpose, err := m.recoveryDriverFixture.VerifyApplicationRecovery(ctx, data, view)
	if err != nil {
		return purpose, err
	}
	// This fixture promises only Reserve/Use. An ephemeral application
	// dependency is not an equivalent capacity recipe on this immutable view.
	if purpose.ApplyBefore != nil {
		m.t.Error("protected capacity fixture returned an application dependency")
		return raftlog.ApplicationRecoveryPurpose{}, ErrInvalid
	}
	m.verifyCalls++
	key := fmt.Sprintf("%d/%d/%x", root.Generation, root.Index, data)
	if previous, seen := m.purposes[key]; seen && (previous.ApplyBefore != nil || !reflect.DeepEqual(previous.Reserve, purpose.Reserve) || !reflect.DeepEqual(previous.Use, purpose.Use)) {
		m.t.Errorf("repeated classification changed exact purpose on immutable view %s", key)
		return raftlog.ApplicationRecoveryPurpose{}, ErrInvalid
	}
	m.purposes[key] = purpose
	return purpose, nil
}

func newCapacityRecoveryRegressionNetwork(t *testing.T) (*capacityRegressionNetwork, map[uint64]*capacityRecoveryRegressionMachine) {
	t.Helper()
	n := &capacityRegressionNetwork{t: t, nodes: map[uint64]*Driver{}}
	machines := map[uint64]*capacityRecoveryRegressionMachine{}
	for id := uint64(1); id <= 3; id++ {
		p := raftlog.DefaultApplicationPolicy(id)
		p.RetainedApplicationBytes = 18_875_000
		controls := raftlog.ApplicationControlConfig{Version: 1}
		recovery := raftlog.ApplicationRecoveryConfig{Version: 1}
		contract, err := raftlog.ApplicationContractForPolicyWithRecovery(p, controls, recovery)
		if err != nil {
			t.Fatal(err)
		}
		cfg := raftlog.Config{Dir: fmt.Sprintf("independent-protected-capacity-%d", id), FS: vfs.NewMem(), Create: true, Application: p, Controls: controls, Recovery: recovery,
			Transfer:    raftlog.ApplicationTransferConfig{Identity: raftlog.ApplicationIdentity{Graph: [16]byte{1}, Partition: 1, Group: [16]byte{2}}, Contract: contract, Limits: raftlog.DefaultApplicationTransferLimits()},
			Generations: raftlog.ApplicationGenerationLimits{MaxBytes: 2 << 30, MaxRecords: 2_000_000}, PublishedCuts: raftlog.ApplicationPublishedCutLimits{MaxTransferChunks: 10_000},
			Replication: raftlog.ApplicationReplicationConfig{Voters: [3]uint64{1, 2, 3}}, SemanticContractID: raftlog.ApplicationSemanticContractID{42}}
		store, err := raftlog.Open(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Initialize([]uint64{1, 2, 3}, nil); err != nil {
			t.Fatal(err)
		}
		m := &capacityRecoveryRegressionMachine{t: t, stages: map[uint64]int{}, restores: map[uint64]int{}, purposes: map[string]raftlog.ApplicationRecoveryPurpose{},
			recoveryDriverFixture: recoveryDriverFixture{store: store, reservation: raftlog.ApplicationRecoveryReservation{ID: [32]byte{1}, Digest: [32]byte{2}, Credits: []raftlog.ApplicationRecoveryCredit{
				{Phase: 1, CommandBytes: 6, RetainedBytes: 5 << 20, RetainedRecords: 5}, {Phase: 2, CommandBytes: 6, RetainedBytes: 5 << 20, RetainedRecords: 5}}}}}
		d, err := Open(Config{ID: id, Store: store, ApplicationMachine: m, ApplicationSnapshotSends: ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 4 << 20}})
		if err != nil {
			_ = store.Close()
			t.Fatal(err)
		}
		n.nodes[id], machines[id] = d, m
		t.Cleanup(func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	return n, machines
}

func TestIndependentCapacityProtectedReserveUseCatchup(t *testing.T) {
	n, machines := newCapacityRecoveryRegressionNetwork(t)
	n.campaign(1, nil)
	before := n.nodes[3].Applied()
	target := machines[3]
	clear(target.stages)
	clear(target.restores)
	clear(target.purposes)
	target.verifyCalls = 0
	n.seen = nil
	for _, data := range []string{"register", "finish", "ordinary", "ordinary", "ordinary", "ordinary"} {
		out, err := n.nodes[1].Propose([]byte(data))
		if err != nil {
			t.Fatal("serial protected command must fit unchanged policy", data, err)
		}
		if err := n.pump(out.Packets, func(p Packet) bool { return p.To != 3 }); err != nil {
			t.Fatal(err)
		}
	}
	want := before + 6
	if n.nodes[1].Applied() != want || n.nodes[3].Applied() != before {
		t.Fatal("actual committed protected lag absent")
	}
	if err := n.catchup(1); err != nil {
		t.Fatalf("legally serial-fitting Reserve/Use+ordinary catch-up must progress: %v", err)
	}
	index, image, err := target.store.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	if index != want || len(image) != 0 || n.nodes[3].Applied() != want || n.nodes[3].raw.BasicStatus().Applied != want {
		t.Fatal("protected physical/Raft frontiers failed to reconcile", index, n.nodes[3].Applied(), n.nodes[3].raw.BasicStatus().Applied)
	}
	for index := before + 1; index <= want; index++ {
		if target.stages[index] != 1 || target.restores[index] != 1 {
			t.Fatal("protected effect missing or repeated", index, target.stages, target.restores)
		}
	}
	entrySlots := 0
	for _, a := range n.seen {
		if a.to == 3 {
			entrySlots += a.entries
		}
	}
	// One full fast-path classification plus one linear fallback walk is
	// permitted; verification is not an exactly-once callback contract.
	if entrySlots < 6 || target.verifyCalls > 2*entrySlots {
		t.Fatal("classifier work exceeded two linear passes", target.verifyCalls, entrySlots)
	}
	view, err := target.store.ApplicationView(want)
	if err != nil {
		t.Fatal(err)
	}
	owner, _, ownerErr := view.RecoveryOwner(t.Context(), raftlog.ReadBudget{Rows: 1, Bytes: 4096})
	control, found, _, controlErr := view.GetControl(t.Context(), []byte("f"), raftlog.ReadBudget{Rows: 2, Bytes: 4096})
	closeErr := view.Close()
	if ownerErr != nil || controlErr != nil || closeErr != nil || !found || string(control.Value) != "done" || owner != (raftlog.ApplicationRecoveryOwner{ID: target.reservation.ID, Digest: target.reservation.Digest}) {
		t.Fatal("completion control or still-protected owner lost", owner, ownerErr, controlErr, closeErr)
	}
	if err := target.store.Scrub(); err != nil {
		t.Fatal(err)
	}
	// Publication is performed only after the two actual frontiers agree. The
	// real manifest exposes remaining credits, without fabricating a snapshot.
	if err := target.store.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	cut, err := target.store.PublishedApplicationCut()
	if err != nil {
		t.Fatal(err)
	}
	export, err := target.store.BeginPublishedApplicationExport(t.Context(), cut.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer export.Close()
	done := false
	for range 100 {
		done, err = export.BuildManifest(t.Context(), raftlog.ReadBudget{Rows: 1, Bytes: 4096})
		if err != nil {
			t.Fatal(err)
		}
		if done {
			break
		}
	}
	if !done {
		t.Fatal("bounded real manifest build did not finish")
	}
	manifest, err := export.Manifest()
	if err != nil {
		t.Fatal(err)
	}
	remaining := manifest.RecoveryReservation
	if remaining.ID != target.reservation.ID || remaining.Digest != target.reservation.Digest || !reflect.DeepEqual(remaining.Credits, []raftlog.ApplicationRecoveryCredit{target.reservation.Credits[1]}) {
		t.Fatal("Use consumed unrelated phase2 credit or recreated phase1", remaining)
	}
	if p := target.store.ApplicationLimits(); p.RetainedApplicationBytes != 18_875_000 || p.MaxInstallBytes != 4<<20 {
		t.Fatal("fixture quotas changed", p)
	}
}
