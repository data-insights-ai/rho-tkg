package replica

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// Cached Stage metadata comes only from successful native Restore. Restore
// opens a real view, permitting a controlled MaxViews failure after synced
// installation without fabricated callback errors or consensus inputs.
type retainedPrefixApplication struct {
	*recoveryAuditApplication
	root        raftlog.ApplicationRoot
	retainStage uint64
	held        []*raftlog.ApplicationView
}

func (m *retainedPrefixApplication) ApplicationCallbackLimits() ApplicationCallbackLimits {
	return ApplicationCallbackLimits{StageWork: 64 << 10, StageOwned: 64 << 10, RestoreWork: 64 << 10, RestoreOwned: 64 << 10, CleanupWork: 64 << 10, CleanupOwned: 64 << 10}
}
func (m *retainedPrefixApplication) Restore(index uint64, image []byte) error {
	if len(image) != 0 {
		return ErrInvalid
	}
	v, err := m.store.ApplicationView(index)
	if err != nil {
		return err
	}
	root, err := v.RootBounded(context.Background(), 0)
	err = errors.Join(err, v.Close())
	if err != nil {
		return err
	}
	m.root = root
	m.index = index
	return nil
}
func (m *retainedPrefixApplication) Stage(e Entry, _ raftlog.ApplicationBudget) (raftlog.ApplicationBatch, error) {
	b := raftlog.ApplicationBatch{BaseGeneration: e.Generation, BaseIndex: m.root.Index, BaseImageHash: m.root.ImageHash, Image: m.root.Image}
	if len(e.Data) == 0 {
		return b, nil
	}
	if bytes.Equal(e.Data, []byte("register")) {
		b.RecoveryReserve = &m.reservation
		b.ControlPuts = []raftlog.ApplicationControlPut{{Key: []byte("r"), Value: m.reservation.Digest[:]}}
		return b, nil
	}
	if len(e.Data) != recoveryWorkEntryBytes-74 || e.Data[0] != 'W' || e.Data[1] < 1 || e.Data[1] > 64 {
		return raftlog.ApplicationBatch{}, ErrInvalid
	}
	b.ControlPuts = []raftlog.ApplicationControlPut{{Key: []byte{'w', e.Data[1]}, Value: []byte("done")}}
	// Controlled fixture pressure begins AFTER native Persist/verifier and
	// cached Stage construction. The view-limit error comes from actual Restore.
	if e.Index == m.retainStage {
		for range 2 {
			v, err := m.store.ApplicationView(m.root.Index)
			if err != nil {
				return raftlog.ApplicationBatch{}, err
			}
			m.held = append(m.held, v)
		}
	}
	return b, nil
}
func TestApplicationSnapshotIgnoredAfterNativeRestoreFailureMustFundRetainedPrefixBeforeClaim(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	nodes := make(map[uint64]*Driver)
	configs := make(map[uint64]raftlog.Config)
	var targetMachine *retainedPrefixApplication
	for id := uint64(1); id <= 3; id++ {
		l := raftlog.DefaultLimits()
		l.MaxEntryBytes = recoveryWorkEntryBytes
		l.MaxReadBytes = recoveryWorkEntryBytes + 32
		l.MaxReadEntries = 1
		l.MaxReadyBytes = recoveryWorkEntryBytes + 64<<10
		l.MaxRetainedBytes = 512 << 20
		p := raftlog.DefaultApplicationPolicy(id)
		p.MaxInstallBytes = 4096
		p.MaxTailEntries = 2
		p.MaxTailBytes = 128 << 20
		p.MaxViews = 2
		p.ReclaimEntries = 512
		if id == 3 {
			p.ReclaimEntries = 66
		} // election+register+64 large entries, Base1.
		controls := raftlog.ApplicationControlConfig{Version: 1}
		recovery := raftlog.ApplicationRecoveryConfig{Version: 1}
		contract, err := raftlog.ApplicationContractForPolicyWithRecovery(p, controls, recovery)
		if err != nil {
			t.Fatal(err)
		}
		cfg := raftlog.Config{Dir: fmt.Sprintf("retained-prefix-%d", id), FS: vfs.NewMem(), Create: true, Limits: l, Application: p, Controls: controls, Recovery: recovery, Transfer: raftlog.ApplicationTransferConfig{Identity: raftlog.ApplicationIdentity{Graph: [16]byte{1}, Partition: 1, Group: [16]byte{2}}, Contract: contract, Limits: raftlog.DefaultApplicationTransferLimits()}, Generations: raftlog.ApplicationGenerationLimits{MaxBytes: 2 << 30, MaxRecords: 2_000_000}, PublishedCuts: raftlog.ApplicationPublishedCutLimits{MaxTransferChunks: 10_000}, Replication: raftlog.ApplicationReplicationConfig{Voters: [3]uint64{1, 2, 3}}, SemanticContractID: raftlog.ApplicationSemanticContractID{42}}
		configs[id] = cfg
		s, err := raftlog.Open(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Initialize([]uint64{1, 2, 3}, nil); err != nil {
			t.Fatal(errors.Join(err, s.Close()))
		}
		machine := &retainedPrefixApplication{recoveryAuditApplication: &recoveryAuditApplication{recoveryDriverFixture: &recoveryDriverFixture{store: s, reservation: raftlog.ApplicationRecoveryReservation{ID: [32]byte{1}, Digest: [32]byte{2}, Credits: []raftlog.ApplicationRecoveryCredit{{Phase: 1, CommandBytes: 6, RetainedBytes: 4096, RetainedRecords: 5}}}}}}
		if id == 3 {
			targetMachine = machine
		}
		packet := l.MaxReadBytes + 64<<10
		output := 2 * packet
		if id == 3 {
			output = packet + 2*snapshotSendOutputBytes
		} // native follower ACK/ignored-snapshot response.
		d, err := Open(Config{ID: id, Store: s, Limits: l, ApplicationMachine: machine, MaxPacketBytes: packet, MaxOutputBytes: output, ApplicationSnapshotSends: ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 4 << 20}})
		if err != nil {
			t.Fatal(errors.Join(err, s.Close()))
		}
		nodes[id] = d
		t.Cleanup(func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	out, err := nodes[1].Campaign()
	if err != nil {
		t.Fatal(err)
	}
	deliverApplication(t, nodes, out.Packets)
	out, err = nodes[1].Propose([]byte("register"))
	if err != nil {
		t.Fatal(err)
	}
	deliverApplication(t, nodes, out.Packets)
	for sequence := byte(1); sequence < 64; sequence++ {
		data := bytes.Repeat([]byte{0xa5}, recoveryWorkEntryBytes-74)
		data[0], data[1] = 'W', sequence
		out, err := nodes[1].Propose(data)
		if err != nil {
			t.Fatal(sequence, err)
		}
		deliverApplication(t, nodes, out.Packets)
	}
	target := nodes[3]
	if target.Applied() != 66 {
		t.Fatal("native retained-prefix baseline", target.Applied())
	}
	first, err := target.store.FirstIndex()
	if err != nil || first != 2 {
		t.Fatal("prefix prematurely reclaimed", first, err)
	}
	data := bytes.Repeat([]byte{0xa5}, recoveryWorkEntryBytes-74)
	data[0], data[1] = 'W', 64
	out, err = nodes[1].Propose(data)
	if err != nil {
		t.Fatal(err)
	}
	var quorum []Packet
	for _, p := range out.Packets {
		if p.To != 3 {
			quorum = append(quorum, p)
		}
	} // Withhold genuine uncommitted append64.
	delete(nodes, 3)
	deliverApplication(t, nodes, quorum)
	nodes[3] = target
	if nodes[1].Applied() != 67 {
		t.Fatal("genuine quorum did not commit final entry")
	}
	// The next genuine heartbeat-response asks the leader for its missing entry.
	var rejected Packet
	var retry Packet
	for range 4 {
		out, err := nodes[1].Tick()
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range out.Packets {
			if p.To != 3 {
				deliverApplication(t, map[uint64]*Driver{1: nodes[1], 2: nodes[2]}, []Packet{p})
				continue
			}
			next, err := target.Step(p)
			if err != nil {
				t.Fatal(err)
			}
			for _, response := range next.Packets {
				msg, err := nodes[1].decodeBoundPacket(response, false)
				if err != nil {
					t.Fatal(err)
				}
				fromLeader, err := nodes[1].Step(response)
				if err != nil {
					t.Fatal(err)
				}
				if msg.GetReject() {
					rejected = response
				}
				for _, candidate := range fromLeader.Packets {
					if candidate.To != 3 {
						continue
					}
					m, err := target.decodeBoundPacket(candidate, false)
					if err != nil {
						t.Fatal(err)
					}
					if len(m.Entries) == 1 && m.Entries[0].GetIndex() == 67 && m.GetCommit() == 67 {
						retry = candidate
					}
					if len(m.Entries) == 0 {
						refusal, err := target.Step(candidate)
						if err != nil {
							t.Fatal(err)
						}
						for _, r := range refusal.Packets {
							decoded, err := nodes[1].decodeBoundPacket(r, false)
							if err != nil {
								t.Fatal(err)
							}
							if decoded.GetReject() {
								rejected = r
							}
							retransmit, err := nodes[1].Step(r)
							if err != nil {
								t.Fatal(err)
							}
							for _, c := range retransmit.Packets {
								m, err := target.decodeBoundPacket(c, false)
								if err != nil {
									t.Fatal(err)
								}
								if c.To == 3 && len(m.Entries) == 1 && m.GetCommit() == 67 {
									retry = c
								}
							}
						}
					}
				}
			}
		}
		if len(retry.Payload) != 0 {
			break
		}
	}
	if len(retry.Payload) == 0 || len(rejected.Payload) == 0 || target.Applied() != 66 {
		t.Fatal("actual combined append+commit retry unavailable")
	}
	if err := nodes[1].store.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	// Replay the actual reject to reset native progress after source compaction.
	out, err = nodes[1].Step(rejected)
	if err != nil {
		t.Fatal(err)
	}
	var snap Packet
	var send *SnapshotSend
	pumpSnapshot := func(out Output) {
		var queue []Packet
		enqueue := func(out Output) {
			for _, p := range out.Packets {
				if p.Snapshot {
					if p.From != 1 || p.To != 3 || len(out.SnapshotSends) != 1 || send != nil {
						t.Fatal("native snapshot association")
					}
					snap, send = p, out.SnapshotSends[0]
				} else {
					queue = append(queue, p)
				}
			}
		}
		enqueue(out)
		for delivered := 0; send == nil && len(queue) != 0; delivered++ {
			if delivered >= 10000 {
				t.Fatal("bounded native snapshot fallback")
			}
			p := queue[0]
			queue = queue[1:]
			if p.To == 3 {
				m, err := target.decodeBoundPacket(p, false)
				if err != nil {
					t.Fatal(err)
				}
				if len(m.Entries) > 0 {
					continue
				}
			}
			next, err := nodes[p.To].Step(p)
			if err != nil {
				t.Fatal("native source quorum/fallback", p.From, p.To, err)
			}
			enqueue(next)
		}
	}
	pumpSnapshot(out)
	for range 24 {
		if send != nil {
			break
		}
		offers, err := nodes[1].SnapshotSends()
		if err != nil {
			t.Fatal(err)
		}
		for _, offer := range offers {
			ready := false
			for range 1000 {
				var err error
				ready, err = offer.Build(t.Context(), raftlog.ReadBudget{Rows: 1, Bytes: 4096})
				if err != nil {
					t.Fatal(err)
				}
				if ready {
					break
				}
			}
			if !ready {
				t.Fatal("bounded prefix manifest build incomplete")
			}
			if err := nodes[1].SealSnapshotSend(offer); err != nil {
				t.Fatal(err)
			}
		}
		out, err := nodes[1].Tick()
		if err != nil {
			t.Fatal(err)
		}
		pumpSnapshot(out)
	}
	if send == nil {
		t.Fatal("real compacted snapshot unavailable")
	}
	manifest, err := send.Manifest()
	if err != nil || manifest.Index != 67 || manifest.Version != 5 {
		t.Fatal("actual AS5 prefix cut", manifest.Index, err)
	}
	var chunks []raftlog.ApplicationSnapshotChunk
	for range 1000 {
		chunk, err := send.Next(t.Context(), raftlog.ReadBudget{Rows: 1, Bytes: 4096})
		if err != nil {
			t.Fatal(err)
		}
		chunks = append(chunks, chunk)
		if chunk.Final {
			break
		}
	}
	if len(chunks) == 0 || !chunks[len(chunks)-1].Final {
		t.Fatal("bounded native chunk capture incomplete")
	}
	targetMachine.retainStage = 67
	failed, err := target.Step(retry)
	if !errors.Is(err, raftlog.ErrLimit) || !zeroPendingOutput(failed) {
		t.Fatal("native Restore capacity did not fail after synced installation", failed, err)
	}
	index, _, checkpointErr := target.store.Checkpoint()
	first, firstErr := target.store.FirstIndex()
	if checkpointErr != nil || firstErr != nil || index != 67 || first != 2 || target.Applied() != 66 || targetMachine.root.Index != 66 {
		t.Fatal("post-Install/pre-Reclaim failure coordinates", index, first, target.Applied(), targetMachine.root.Index, checkpointErr, firstErr)
	}
	for _, v := range targetMachine.held {
		if err := v.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}
	cfg := configs[3]
	cfg.Create = false
	store, err := raftlog.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	freshMachine := &retainedPrefixApplication{recoveryAuditApplication: &recoveryAuditApplication{recoveryDriverFixture: &recoveryDriverFixture{store: store, reservation: targetMachine.reservation}}}
	target, err = Open(Config{ID: 3, Store: store, Limits: cfg.Limits, ApplicationMachine: freshMachine, MaxPacketBytes: cfg.Limits.MaxReadBytes + 64<<10, MaxOutputBytes: cfg.Limits.MaxReadBytes + 64<<10 + 2*snapshotSendOutputBytes, ApplicationSnapshotSends: ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 4 << 20}})
	if err != nil {
		t.Fatal(errors.Join(err, store.Close()))
	}
	t.Cleanup(func() {
		if err := target.Close(); err != nil {
			t.Error(err)
		}
	})
	first, err = store.FirstIndex()
	if err != nil || first != 2 || target.Applied() != 67 {
		t.Fatal("reopen lost retained prefix", first, target.Applied(), err)
	}
	imported, err := store.BeginApplicationImport(t.Context(), manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer imported.Abort()
	for _, chunk := range chunks {
		if err := imported.Append(t.Context(), chunk); err != nil {
			t.Fatal(err)
		}
	}
	if err := imported.Verify(t.Context()); err != nil {
		t.Fatal(err)
	}
	prepared, err := imported.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	budget := ApplicationAuditBudget{WorkBytes: 32 << 20, OwnedBytes: 32 << 20}
	pending, err := target.BeginApplicationSnapshot(t.Context(), snap, prepared, budget)
	if err != nil {
		t.Fatal(err)
	}
	defer pending.Close()
	for range 2 {
		if _, err := pending.Advance(t.Context(), budget); err != nil {
			t.Fatal(err)
		}
	}
	contract, err := prepared.ApplicationContract(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	work, owned, err := target.applicationCommitRequirements(pending.owned, pending.auditOwned, contract)
	if err != nil {
		t.Fatal(err)
	}
	const prefixHashWork uint64 = 64 * (recoveryWorkEntryBytes - 74)
	t.Logf("native Restore-view failure/reopen prefix64 bytes=%d; entire configured=%d", prefixHashWork, work)
	before := freshMachine.verifies
	out, err = pending.Commit(t.Context(), ApplicationAuditBudget{WorkBytes: prefixHashWork - 1, OwnedBytes: owned})
	afterFirst, firstErr := store.FirstIndex()
	if !errors.Is(err, ErrLimit) || !zeroPendingOutput(out) || firstErr != nil || afterFirst != 2 {
		t.Fatalf("shared ignored-snapshot Commit underfunded retained-prefix range hash: work=%d lower=%d first=%d err=%v", work, prefixHashWork, afterFirst, err)
	}
	if freshMachine.verifies != before {
		t.Fatal("ignored snapshot reverified stable entries")
	}
	out, err = pending.Commit(t.Context(), fundedPendingCommitBudget())
	afterFirst, firstErr = store.FirstIndex()
	if err != nil || firstErr != nil || afterFirst != 68 || out.Applied != 67 || store.ApplicationGeneration() != 1 || freshMachine.verifies != before {
		t.Fatal("funded ignored snapshot failed real reclamation", afterFirst, out.Applied, err, firstErr)
	}
}
