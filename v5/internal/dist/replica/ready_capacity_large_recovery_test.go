package replica

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// The selected trusted verifier recognizes this fixture's ordinary additive
// commands and returns no protected purpose. Recovery being enabled must not
// turn the maximum speculative owner count into a limit on ordinary entries.
type capacityLargeRecoveryMachine struct {
	capacityRegressionMachine
	stages, restores map[uint64]int
	verifyCalls      int
}

func (m *capacityLargeRecoveryMachine) VerifyApplicationRecovery(ctx context.Context, data []byte, view *raftlog.ApplicationView) (raftlog.ApplicationRecoveryPurpose, error) {
	if ctx == nil || view == nil || len(data) != 0 && len(data) != 8 {
		return raftlog.ApplicationRecoveryPurpose{}, raftlog.ErrInvalid
	}
	root, err := view.RootBounded(ctx, 8)
	if err != nil {
		return raftlog.ApplicationRecoveryPurpose{}, err
	}
	if root.Index != m.index {
		return raftlog.ApplicationRecoveryPurpose{}, raftlog.ErrInvalid
	}
	m.verifyCalls++
	return raftlog.ApplicationRecoveryPurpose{}, nil
}

func (m *capacityLargeRecoveryMachine) Stage(e Entry, budget raftlog.ApplicationBudget) (raftlog.ApplicationBatch, error) {
	m.stages[e.Index]++
	if m.stages[e.Index] != 1 {
		m.t.Errorf("large ordinary recovery command staged twice at %d", e.Index)
		return raftlog.ApplicationBatch{}, ErrInvalid
	}
	return m.capacityRegressionMachine.Stage(e, budget)
}

func (m *capacityLargeRecoveryMachine) Restore(index uint64, image []byte) error {
	m.restores[index]++
	if m.restores[index] != 1 {
		m.t.Errorf("large ordinary recovery command restored twice at %d", index)
		return ErrInvalid
	}
	return m.capacityRegressionMachine.Restore(index, image)
}

func newCapacityLargeRecoveryNetwork(t *testing.T) (*capacityRegressionNetwork, map[uint64]*capacityLargeRecoveryMachine) {
	t.Helper()
	n := &capacityRegressionNetwork{t: t, nodes: map[uint64]*Driver{}, machines: map[uint64]*capacityRegressionMachine{}}
	machines := map[uint64]*capacityLargeRecoveryMachine{}
	for id := uint64(1); id <= 3; id++ {
		// Preserve the original ordinary fixture's quotas. Only the declared
		// legal read/reclaim profile changes: one read may hold 256 entries and
		// the leader retains the suffix until at least 512 entries accumulate.
		p := raftlog.DefaultApplicationPolicy(id)
		p.MaxInstallWrites, p.RetainedApplicationRecords = 256, 8192
		p.RetainedApplicationBytes = 64 << 20
		if id == 3 {
			p.RetainedApplicationBytes = 16 << 20
		}
		p.ReclaimEntries = 512
		limits := raftlog.DefaultLimits()
		limits.MaxReadEntries = 256
		controls := raftlog.ApplicationControlConfig{Version: 1}
		recovery := raftlog.ApplicationRecoveryConfig{Version: 1}
		contract, err := raftlog.ApplicationContractForPolicyWithRecovery(p, controls, recovery)
		if err != nil {
			t.Fatal(err)
		}
		cfg := raftlog.Config{Dir: fmt.Sprintf("independent-large-recovery-%d", id), FS: vfs.NewMem(), Create: true,
			Limits: limits, Application: p, Controls: controls, Recovery: recovery,
			Transfer:    raftlog.ApplicationTransferConfig{Identity: raftlog.ApplicationIdentity{Graph: [16]byte{101}, Partition: 1, Group: [16]byte{102}}, Contract: contract, Limits: raftlog.DefaultApplicationTransferLimits()},
			Generations: raftlog.ApplicationGenerationLimits{MaxBytes: 256 << 20, MaxRecords: 32768}, PublishedCuts: raftlog.ApplicationPublishedCutLimits{MaxTransferChunks: 100000},
			Replication: raftlog.ApplicationReplicationConfig{Voters: [3]uint64{1, 2, 3}}, SemanticContractID: raftlog.ApplicationSemanticContractID{17}}
		store, err := raftlog.Open(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Initialize([]uint64{1, 2, 3}, nil); err != nil {
			_ = store.Close()
			t.Fatal(err)
		}
		m := &capacityLargeRecoveryMachine{capacityRegressionMachine: capacityRegressionMachine{t: t, store: store}, stages: map[uint64]int{}, restores: map[uint64]int{}}
		d, err := Open(Config{ID: id, Store: store, ApplicationMachine: m, ApplicationSnapshotSends: ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 4 << 20}})
		if err != nil {
			_ = store.Close()
			t.Fatal(err)
		}
		n.nodes[id], n.machines[id], machines[id] = d, &m.capacityRegressionMachine, m
		t.Logf("large recovery node%d: limits=%+v policy=%+v packetBytes=%d outputBytes=%d inflightBytes=%d uncommittedBytes=%d snapshotSends=%+v", id, d.config.Limits, p, d.config.MaxPacketBytes, d.config.MaxOutputBytes, d.config.MaxInflightBytes, d.config.MaxUncommittedBytes, d.config.ApplicationSnapshotSends)
		t.Cleanup(func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	return n, machines
}

func TestIndependentCapacityLargeRecoveryOrdinaryAppend(t *testing.T) {
	n, machines := newCapacityLargeRecoveryNetwork(t)
	n.campaign(1, nil)
	before := n.nodes[3].Applied()
	target := machines[3]
	clear(target.stages)
	clear(target.restores)
	target.staged = nil
	target.verifyCalls = 0
	n.seen = nil
	const count = 256
	wantValues := make([]uint64, count)
	for i := range count {
		wantValues[i] = uint64(i + 1)
		n.propose(1, wantValues[i], func(p Packet) bool { return p.To != 3 })
	}
	wantIndex := before + count
	wantSum := uint64(count * (count + 1) / 2)
	if n.nodes[1].Applied() != wantIndex || n.nodes[2].Applied() != wantIndex || n.nodes[3].Applied() != before {
		t.Fatal("actual 256-command committed lag absent", n.nodes[1].Applied(), n.nodes[2].Applied(), n.nodes[3].Applied())
	}
	first, err := n.nodes[1].store.FirstIndex()
	if err != nil || first > before+1 {
		t.Fatal("source compacted the genuine catch-up suffix", first, before+1, err)
	}
	if err := n.catchup(1); err != nil {
		// Transport records the original packet before Step, including a refusal.
		// Report whether the reproduction reached the intended 256-entry append.
		t.Fatalf("ordinary recovery-enabled bulk catch-up must progress: %v; original appends=%+v verifierCalls=%d", err, n.seen, target.verifyCalls)
	}
	if !slices.ContainsFunc(n.seen, func(a capacityRegressionAppend) bool {
		return a.to == 3 && a.entries == count && a.previous == before && a.commit == wantIndex && a.last == wantIndex
	}) {
		t.Fatal("genuine original 256-entry committed Append absent", n.seen)
	}
	if !slices.Equal(target.staged, wantValues) {
		t.Fatal("ordinary command values missing, reordered or repeated", target.staged)
	}
	for index := before + 1; index <= wantIndex; index++ {
		if target.stages[index] != 1 || target.restores[index] != 1 {
			t.Fatal("ordinary Stage/Restore effect must occur once", index, target.stages[index], target.restores[index])
		}
	}
	for id := uint64(1); id <= 3; id++ {
		n.assertState(id, wantIndex, wantSum)
		hard, _, err := n.nodes[id].store.InitialState()
		last, lastErr := n.nodes[id].store.LastIndex()
		if err != nil || lastErr != nil || hard.GetCommit() != wantIndex || n.nodes[id].raw.BasicStatus().GetCommit() != wantIndex || last != wantIndex {
			t.Fatal("large ordinary durable commit/tail frontier", id, hard.GetCommit(), last, err, lastErr)
		}
		p, limits := n.nodes[id].store.ApplicationLimits(), n.nodes[id].store.Limits()
		wantQuota := uint64(64 << 20)
		if id == 3 {
			wantQuota = 16 << 20
		}
		if p.RetainedApplicationBytes != wantQuota || p.MaxInstallBytes != 4<<20 || p.MaxInstallWrites != 256 || p.RetainedApplicationRecords != 8192 || p.ReclaimEntries != 512 || limits.MaxReadEntries != 256 {
			t.Fatal("fixture raised quotas instead of exercising legal profile", id, p, limits)
		}
		defaultLimits := raftlog.DefaultLimits()
		defaultLimits.MaxReadEntries = 256
		if limits != defaultLimits || n.nodes[id].config.ApplicationSnapshotSends != (ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 4 << 20}) {
			t.Fatal("packet/log/admission budgets changed beyond declared read profile", id, limits, n.nodes[id].config.ApplicationSnapshotSends)
		}
	}
	entrySlots := 0
	for _, a := range n.seen {
		if a.to == 3 {
			entrySlots += a.entries
		}
	}
	// A known speculative-count refusal happens before callbacks. A corrected
	// implementation may retain either this path or one full classification;
	// permit at most that full pass plus one forward prefix walk, never N² work.
	if target.verifyCalls < count || target.verifyCalls > 2*entrySlots {
		t.Fatal("large ordinary verification exceeded linear bound", target.verifyCalls, entrySlots)
	}
}
