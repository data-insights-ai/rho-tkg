package replica

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// Fixed empty-image test machine exercises actual Driver capacity doors only;
// graph transaction semantics are covered by the separate real graph fixture.
type recoveryDriverFixture struct {
	store       *raftlog.Store
	index       uint64
	reservation raftlog.ApplicationRecoveryReservation
}

func (m *recoveryDriverFixture) SemanticContractID() raftlog.ApplicationSemanticContractID {
	return raftlog.ApplicationSemanticContractID{42}
}
func (m *recoveryDriverFixture) Restore(index uint64, image []byte) error {
	if len(image) != 0 {
		return ErrInvalid
	}
	m.index = index
	return nil
}
func (m *recoveryDriverFixture) VerifyApplicationRecovery(ctx context.Context, data []byte, v *raftlog.ApplicationView) (raftlog.ApplicationRecoveryPurpose, error) {
	if v == nil || ctx == nil {
		return raftlog.ApplicationRecoveryPurpose{}, raftlog.ErrInvalid
	}
	root, err := v.RootBounded(ctx, 0)
	if err != nil {
		return raftlog.ApplicationRecoveryPurpose{}, err
	}
	if bytes.Equal(data, []byte("register")) {
		return raftlog.ApplicationRecoveryPurpose{Reserve: &m.reservation}, nil
	}
	if bytes.Equal(data, []byte("finish")) {
		record, found, _, err := v.GetControl(ctx, []byte("f"), raftlog.ReadBudget{Rows: 2, Bytes: 4096})
		if err != nil {
			return raftlog.ApplicationRecoveryPurpose{}, err
		}
		if found {
			if !bytes.Equal(record.Value, []byte("done")) {
				return raftlog.ApplicationRecoveryPurpose{}, raftlog.ErrCorrupt
			}
			return raftlog.ApplicationRecoveryPurpose{}, nil
		}
		u := raftlog.ApplicationRecoveryUse{ID: m.reservation.ID, Digest: m.reservation.Digest, Phase: 1, CommandDigest: sha256.Sum256(data)}
		return raftlog.ApplicationRecoveryPurpose{Use: &u}, nil
	}
	_ = root
	return raftlog.ApplicationRecoveryPurpose{}, nil
}
func (m *recoveryDriverFixture) Stage(e Entry, budget raftlog.ApplicationBudget) (raftlog.ApplicationBatch, error) {
	view, err := m.store.ApplicationView(m.index)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	base, err := view.RootBounded(context.Background(), 0)
	completed := false
	if err == nil && bytes.Equal(e.Data, []byte("finish")) {
		record, found, _, readErr := view.GetControl(context.Background(), []byte("f"), raftlog.ReadBudget{Rows: 2, Bytes: 4096})
		err = readErr
		completed = found
		if err == nil && found && !bytes.Equal(record.Value, []byte("done")) {
			err = raftlog.ErrCorrupt
		}
	}
	closeErr := view.Close()
	if err = errors.Join(err, closeErr); err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	b := raftlog.ApplicationBatch{BaseGeneration: e.Generation, BaseIndex: base.Index, BaseImageHash: base.ImageHash, Image: base.Image}
	switch string(e.Data) {
	case "register":
		b.RecoveryReserve = &m.reservation
		b.ControlPuts = []raftlog.ApplicationControlPut{{Key: []byte("r"), Value: m.reservation.Digest[:]}}
	case "finish":
		if completed {
			break
		} // exact already-installed retry publishes no duplicate effects
		u := raftlog.ApplicationRecoveryUse{ID: m.reservation.ID, Digest: m.reservation.Digest, Phase: 1, CommandDigest: sha256.Sum256(e.Data)}
		b.RecoveryUse = &u
		b.ControlPuts = []raftlog.ApplicationControlPut{{Key: []byte("f"), Value: []byte("done")}}
	}
	if len(b.ControlPuts) > budget.Writes {
		return raftlog.ApplicationBatch{}, raftlog.ErrLimit
	}
	return b, nil
}
func recoveryDriverGroup(t *testing.T) map[uint64]*Driver {
	t.Helper()
	nodes := map[uint64]*Driver{}
	for id := uint64(1); id <= 3; id++ {
		p := raftlog.DefaultApplicationPolicy(id)
		// Independent empty-image ledgers: seed252 + election252. Registration
		// preflight504 + two4MiB installs +10MiB future =18,874,872 fits this cap;
		// its actual380-byte registration then exhausts ordinary two-install headroom.
		p.RetainedApplicationBytes = 18_875_000
		ctrl := raftlog.ApplicationControlConfig{Version: 1}
		recovery := raftlog.ApplicationRecoveryConfig{Version: 1}
		contract, err := raftlog.ApplicationContractForPolicyWithRecovery(p, ctrl, recovery)
		if err != nil {
			t.Fatal(err)
		}
		cfg := raftlog.Config{Dir: fmt.Sprintf("recovery-%d", id), FS: vfs.NewMem(), Create: true, Application: p, Controls: ctrl, Recovery: recovery, Transfer: raftlog.ApplicationTransferConfig{Identity: raftlog.ApplicationIdentity{Graph: [16]byte{1}, Partition: 1, Group: [16]byte{2}}, Contract: contract, Limits: raftlog.DefaultApplicationTransferLimits()}, Generations: raftlog.ApplicationGenerationLimits{MaxBytes: 2 << 30, MaxRecords: 2_000_000}, PublishedCuts: raftlog.ApplicationPublishedCutLimits{MaxTransferChunks: 10_000}, Replication: raftlog.ApplicationReplicationConfig{Voters: [3]uint64{1, 2, 3}}, SemanticContractID: raftlog.ApplicationSemanticContractID{42}}
		s, err := raftlog.Open(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Initialize([]uint64{1, 2, 3}, nil); err != nil {
			t.Fatal(err)
		}
		m := &recoveryDriverFixture{store: s, reservation: raftlog.ApplicationRecoveryReservation{ID: [32]byte{1}, Digest: [32]byte{2}, Credits: []raftlog.ApplicationRecoveryCredit{{Phase: 1, CommandBytes: 6, RetainedBytes: 5 << 20, RetainedRecords: 5}, {Phase: 2, CommandBytes: 6, RetainedBytes: 5 << 20, RetainedRecords: 5}}}}
		d, err := Open(Config{ID: id, Store: s, ApplicationMachine: m, ApplicationSnapshotSends: ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 4 << 20}})
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
func TestRecoveryDriverLocalAndForwardedPhaseCompleteUnderOrdinaryBackpressure(t *testing.T) {
	for _, sender := range []uint64{1, 3} {
		t.Run(fmt.Sprint(sender), func(t *testing.T) {
			nodes := recoveryDriverGroup(t)
			deliverApplication(t, nodes, packets(t, nodes[1].Campaign))
			deliverApplication(t, nodes, packets(t, func() (Output, error) { return nodes[1].Propose([]byte("register")) }))
			registered := nodes[1].Applied()
			for _, d := range nodes {
				if err := d.store.AdmitApplication(1); !errors.Is(err, raftlog.ErrLimit) {
					t.Fatal("ordinary fitting-cliff guard missing", err)
				}
			}
			out, err := nodes[sender].Propose([]byte("finish"))
			if err != nil {
				t.Fatalf("bound terminal phase stopped real local/forwarded Driver: %v", err)
			}
			deliverApplication(t, nodes, out.Packets)
			for id, d := range nodes {
				if d.Applied() <= registered {
					t.Fatal("phase did not reach voter", id, d.Applied(), registered)
				}
				v, err := d.store.ApplicationView(d.Applied())
				if err != nil {
					t.Fatal(err)
				}
				record, found, _, err := v.GetControl(t.Context(), []byte("f"), raftlog.ReadBudget{Rows: 2, Bytes: 4096})
				if err != nil || !found || string(record.Value) != "done" {
					t.Fatal("exact completed control absent", id, record, found, err)
				}
				if err := v.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestRecoveryDriverQueuedExactRetryMustRefuseBeforeRawNodeAndKeepFirstCompletion(t *testing.T) {
	for _, sender := range []uint64{1, 3} {
		t.Run(fmt.Sprint(sender), func(t *testing.T) {
			nodes := recoveryDriverGroup(t)
			deliverApplication(t, nodes, packets(t, nodes[1].Campaign))
			deliverApplication(t, nodes, packets(t, func() (Output, error) { return nodes[1].Propose([]byte("register")) }))
			registered := nodes[1].Applied()
			usage, err := nodes[1].store.ApplicationUsage()
			if err != nil || usage.RetainedBytes != 884 {
				t.Fatal("independent252+252+380 physical census", usage, err)
			}
			// Existing policy: each queued copy retains min(5MiB,4MiB)=4MiB;
			// the unused second phase remains5MiB. Three copies+884 fit18,875,000,
			// four copies+884 exceed it. The fixture's original ceilings stay exact.
			if uint64(3*(4<<20)+(5<<20))+884 > 18_875_000 || uint64(4*(4<<20)+(5<<20))+884 <= 18_875_000 {
				t.Fatal("independent adjacent pending cliff")
			}
			var held []Packet
			for copyIndex := 0; copyIndex < 4; copyIndex++ {
				lastBefore, err := nodes[1].store.LastIndex()
				if err != nil {
					t.Fatal(err)
				}
				out, err := nodes[sender].Propose([]byte("finish"))
				if err == nil && sender != 1 {
					if len(out.Packets) != 1 || out.Packets[0].To != 1 {
						t.Fatal("actual one-command forwarded MsgProp absent", out)
					}
					out, err = nodes[1].Step(out.Packets[0])
				}
				if copyIndex < 3 {
					if err != nil {
						t.Fatal("fitting queued copy", copyIndex, err)
					}
					held = append(held, out.Packets...)
				} else {
					if !errors.Is(err, raftlog.ErrLimit) || errors.Is(err, ErrStopped) {
						t.Fatalf("exact pending retry did not refuse before RawNode: copy=%d err=%v", copyIndex, err)
					}
					lastAfter, readErr := nodes[1].store.LastIndex()
					nodes[1].mu.Lock()
					stopped := nodes[1].stopped
					nodes[1].mu.Unlock()
					if readErr != nil || lastAfter != lastBefore || stopped != nil {
						t.Fatalf("pending retry refusal stopped healthy Driver or changed durable log: %v last=%d/%d stop=%v", readErr, lastBefore, lastAfter, stopped)
					}
				}
				if nodes[1].Applied() != registered {
					t.Fatal("fixture unexpectedly delivered quorum/apply before admission cliff")
				}
			}
			deliverApplication(t, nodes, held)
			for id, d := range nodes {
				if d.Applied() != registered+3 {
					t.Fatal("previously admitted exact completion/retries lost progress", id, d.Applied(), registered)
				}
				v, err := d.store.ApplicationView(d.Applied())
				if err != nil {
					t.Fatal(err)
				}
				record, found, _, err := v.GetControl(t.Context(), []byte("f"), raftlog.ReadBudget{Rows: 2, Bytes: 4096})
				if err != nil || !found || !bytes.Equal(record.Value, []byte("done")) {
					t.Fatal("one exact durable completed control absent", id, record, found, err)
				}
				if err := v.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
