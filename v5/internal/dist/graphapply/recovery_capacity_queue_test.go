package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/dist/replica"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// These are privileged storage-contract tests, not lifecycle admission or
// replica readiness. Replay only original canonical entries from a real native
// source. No read proof, decision, kernel, or application state is invented.
func recoveryQueueReceiver(t *testing.T, n *allocationTestNetwork, group int, through uint64, prefix ...[]*pb.Entry) *declaredMaterializer {
	t.Helper()
	cfg := n.configs[group][2]
	cfg.Dir, cfg.Create = t.TempDir(), true
	s, err := raftlog.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := graphstore.BootstrapBoundOwnership(s, s.ApplicationBinding(), n.d, [3]uint64{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	source := n.hosts[group][0].machine
	m, err := newDeclaredMaterializerWithAgreement(s, source.ns, n.d, source.limits, source.SemanticContractID())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.BindApplicationRecoveryVerifier(s.ApplicationBinding(), m); err != nil {
		t.Fatal(err)
	}
	hard, _, err := source.store.InitialState()
	if err != nil {
		t.Fatal(err)
	}
	for next := uint64(2); next <= through; {
		var entries []*pb.Entry
		if len(prefix) != 0 {
			for _, entry := range prefix[0] {
				if entry.GetIndex() >= next && entry.GetIndex() <= through {
					entries = append(entries, entry)
				}
			}
		} else {
			entries, err = source.store.Entries(next, through+1, uint64(source.store.Limits().MaxReadBytes)) // #nosec G115 -- positive validated Store limit.
		}
		if err != nil || len(entries) == 0 {
			t.Fatal("missing original native prefix", next, err)
		}
		for _, entry := range entries {
			h := proto.Clone(hard).(*pb.HardState)
			h.Commit = new(entry.GetIndex())
			if err := s.Persist(raft.Ready{HardState: h, Entries: []*pb.Entry{entry}}); err != nil {
				t.Fatal("original prefix persistence", entry.GetIndex(), err)
			}
			batch, err := m.Stage(replica.Entry{Index: entry.GetIndex(), Term: entry.GetTerm(), Generation: s.ApplicationGeneration(), Data: entry.GetData()}, s.ApplicationBudget())
			if err != nil {
				t.Fatal("original prefix Stage", entry.GetIndex(), err)
			}
			if batch.RecoveryReserve != nil || batch.RecoveryUse != nil {
				batch.RecoveryAdmission, err = s.RecoveryAdmissionForCommittedEntry(t.Context(), entry.GetIndex())
				if err != nil {
					t.Fatal(err)
				}
			}
			if err := s.InstallApplication(entry.GetIndex(), batch); err != nil {
				t.Fatal("original prefix install", entry.GetIndex(), err)
			}
			if err := m.Restore(entry.GetIndex(), batch.Image); err != nil {
				t.Fatal(err)
			}
			next = entry.GetIndex() + 1
		}
	}
	return m
}

// Observe actual leader MsgApp entries BEFORE ordinary native drive can publish
// and compact their log. The exact packets still follow the original schedule.
type recoveryQueueCapturePump struct {
	*nativeGraphTestPump
	entries []*pb.Entry
	bytes   int
}

func (p *recoveryQueueCapturePump) Submit(ctx context.Context, proposal allocationProposal) ([]byte, error) {
	event, err := p.host.Submit(proposal)
	if err != nil {
		return nil, err
	}
	for _, packet := range event.packets {
		raw, err := replica.DecodeApplicationPacket(p.host.machine.binding, packet, processFrameBytes)
		if err != nil {
			return nil, err
		}
		message := &pb.Message{}
		if err := (proto.UnmarshalOptions{RecursionLimit: 8}).Unmarshal(raw.Payload, message); err != nil {
			return nil, err
		}
		for _, entry := range message.GetEntries() {
			if len(p.entries) != 0 && entry.GetIndex() <= p.entries[len(p.entries)-1].GetIndex() {
				continue // another voter receives the same original leader entry
			}
			cost := proto.Size(entry) + 64
			if len(p.entries) >= 1024 || cost > (8<<20)-p.bytes {
				return nil, errLimit
			}
			p.bytes += cost
			p.entries = append(p.entries, proto.Clone(entry).(*pb.Entry))
		}
	}
	if _, err := p.drive(ctx, event); err != nil {
		return nil, err
	}
	return p.host.machine.store.ApplicationRecord(ctx, p.host.driver.Applied(), true, 64<<10)
}

func recoveryQueueEntries(t *testing.T, source *declaredMaterializer, after uint64) []*pb.Entry {
	t.Helper()
	last, err := source.store.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	entries, err := source.store.Entries(after+1, last+1, uint64(source.store.Limits().MaxReadBytes)) // #nosec G115 -- positive validated Store limit.
	if err != nil || len(entries) == 0 || entries[len(entries)-1].GetIndex() != last {
		t.Fatal("bounded original native suffix", err)
	}
	return entries
}

func recoveryQueueUnchanged(t *testing.T, m *declaredMaterializer, before uint64, image []byte, last uint64) {
	t.Helper()
	index, got, err := m.store.Checkpoint()
	end, endErr := m.store.LastIndex()
	if err != nil || endErr != nil || index != before || !bytes.Equal(got, image) || end != last {
		t.Fatal("refusal changed native storage", index, end, err, endErr)
	}
}

func TestRecoveryCapacityCrossQueuedOriginalReserveAndUse(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCrossPartitionNetwork(t)
	ops, claims := crossEdgeFixture(t, n)
	tx, register := n.crossPlan(ops, claims, 24)
	base := n.hosts[0][0].driver.Applied()
	decision := n.crossCommit(tx, register)
	n.crossResolve(decision)
	entries := recoveryQueueEntries(t, n.hosts[0][0].machine, base)
	if len(entries) != 4 {
		t.Fatal("native four-phase suffix", len(entries))
	}
	for _, earlier := range []bool{false, true} {
		t.Run(map[bool]string{false: "one-ready", true: "earlier-persisted-unapplied"}[earlier], func(t *testing.T) {
			m := recoveryQueueReceiver(t, n, 0, base)
			index, image, err := m.store.Checkpoint()
			if err != nil {
				t.Fatal(err)
			}
			remaining := entries
			if earlier {
				if err := m.store.Persist(raft.Ready{Entries: entries[:1]}); err != nil {
					t.Fatal("original Reserve", err)
				}
				remaining = entries[1:]
			}
			if err := m.store.Persist(raft.Ready{Entries: remaining}); err != nil {
				t.Fatal("genuine queued Reserve did not classify bound Use", err)
			}
			recoveryQueueUnchanged(t, m, index, image, entries[3].GetIndex())
			for _, entry := range entries[1:] {
				want := errInvalid
				if crossPhase(entry.GetData()[4]) == crossDecide {
					want = replica.ErrUnavailable
				}
				batch, err := m.Stage(replica.Entry{Index: index + 1, Term: entry.GetTerm(), Generation: m.store.ApplicationGeneration(), Data: entry.GetData()}, m.store.ApplicationBudget())
				if !errors.Is(err, want) || !reflect.ValueOf(batch).IsZero() {
					t.Fatal("prospective capacity became unapplied phase authority", entry.GetIndex(), err)
				}
			}
		})
	}
	for _, entry := range entries[1:] {
		t.Run(fmt.Sprintf("orphan-phase-%d", entry.GetData()[4]), func(t *testing.T) {
			m := recoveryQueueReceiver(t, n, 0, base)
			index, image, _ := m.store.Checkpoint()
			view, err := m.store.ApplicationView(index)
			if err != nil {
				t.Fatal(err)
			}
			purpose, err := m.VerifyApplicationRecovery(t.Context(), entry.GetData(), view)
			err = errors.Join(err, view.Close())
			if err != nil || purpose.Use == nil || purpose.Reserve != nil || purpose.Use.CommandDigest != sha256.Sum256(entry.GetData()) {
				t.Fatal("canonical prospective Use recipe", err)
			}
			if _, err := m.store.AdmitApplicationRecovery(t.Context(), entry.GetData()); !errors.Is(err, raftlog.ErrInvalid) {
				t.Fatal("orphan proposal got credit", err)
			}
			if err := m.store.Persist(raft.Ready{Entries: []*pb.Entry{entry}}); !errors.Is(err, raftlog.ErrInvalid) {
				t.Fatal("orphan queued command got credit", err)
			}
			recoveryQueueUnchanged(t, m, index, image, index)
		})
	}
	other, nextRegister := n.crossPlan([]graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Scope: ops[0].Scope, Name: "different-transaction"}}, nil, 25)
	nextBase := n.hosts[0][0].driver.Applied()
	nextDecision := n.crossCommit(other, nextRegister)
	n.crossResolve(nextDecision)
	nextEntries := recoveryQueueEntries(t, n.hosts[0][0].machine, nextBase)
	for _, mutation := range []string{"owner", "proof-digest", "proof-phase", "truncated"} {
		t.Run(mutation, func(t *testing.T) {
			m := recoveryQueueReceiver(t, n, 0, base)
			index, image, _ := m.store.Checkpoint()
			if err := m.store.Persist(raft.Ready{Entries: entries[:1]}); err != nil {
				t.Fatal(err)
			}
			wire := entries[3].GetData()
			switch mutation {
			case "owner":
				wire = nextEntries[len(nextEntries)-1].GetData()
			case "truncated":
				wire = wire[:len(wire)-1]
			default:
				command, _, err := decodeCrossCommand(wire, m.limits)
				if err != nil {
					t.Fatal(err)
				}
				if mutation == "proof-digest" {
					command.proofs[0].digest[0] ^= 1
				} else {
					command.proofs[0].phase = crossPrepare
				}
				wire, err = encodeCrossCommand(command, m.limits)
				if err != nil {
					t.Fatal(err)
				}
			}
			bad := proto.Clone(entries[1]).(*pb.Entry)
			bad.Data = wire
			err := m.store.Persist(raft.Ready{Entries: []*pb.Entry{bad}})
			if !errors.Is(err, raftlog.ErrInvalid) && !errors.Is(err, errInvalid) && !errors.Is(err, errCorrupt) {
				t.Fatal("unbound prospective cross command persisted", mutation, err)
			}
			recoveryQueueUnchanged(t, m, index, image, entries[0].GetIndex())
		})
	}
}

func TestRecoveryCapacityRangeQueuedFenceAndCapture(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n, coordinator, intent, wires, _ := rangeMoveCoordinatorFixture(t, 32)
	bases := [2]uint64{n.hosts[0][0].driver.Applied(), n.hosts[1][0].driver.Applied()}
	base := bases[0]
	var prefixes [2][]*pb.Entry
	var captures [2]*recoveryQueueCapturePump
	var err error
	for group := range 2 {
		prefixes[group], err = n.hosts[group][0].machine.store.Entries(2, bases[group]+1, uint64(n.hosts[group][0].machine.store.Limits().MaxReadBytes)) // #nosec G115 -- positive validated Store limit.
		if err != nil {
			t.Fatal(err)
		}
		pump, ok := wires[group].service.pump.(*nativeGraphTestPump)
		if !ok {
			t.Fatal("actual native fixture pump missing")
		}
		captures[group] = &recoveryQueueCapturePump{nativeGraphTestPump: pump}
		wires[group].service.pump = captures[group]
	}
	done, err := runRangeMoveToCompletion(t, coordinator, intent)
	if err != nil || !done.committed {
		t.Fatal("actual native move", err)
	}
	entries := captures[0].entries
	if len(entries) < 2 {
		t.Fatal("native range suffix")
	}
	for group := range 2 {
		entries := captures[group].entries
		for _, earlier := range []bool{false, true} {
			t.Run(fmt.Sprintf("group%d/%s", group, map[bool]string{false: "one-ready", true: "earlier-persisted-unapplied"}[earlier]), func(t *testing.T) {
				m := recoveryQueueReceiver(t, n, group, bases[group], prefixes[group])
				index, image, _ := m.store.Checkpoint()
				view, err := m.store.ApplicationView(index)
				if err != nil {
					t.Fatal(err)
				}
				for _, entry := range entries[1:] {
					purpose, err := m.VerifyApplicationRecovery(t.Context(), entry.GetData(), view)
					if err != nil || purpose.Use == nil || purpose.Reserve != nil {
						t.Fatal("actual move continuation not classified", entry.GetIndex(), err)
					}
				}
				if err := view.Close(); err != nil {
					t.Fatal(err)
				}
				remaining := entries[:2]
				if earlier {
					if err := m.store.Persist(raft.Ready{Entries: entries[:1]}); err != nil {
						t.Fatal(err)
					}
					remaining = entries[1:2]
				}
				if err := m.store.Persist(raft.Ready{Entries: remaining}); err != nil {
					t.Fatal("queued native Fence/Capture", err)
				}
				recoveryQueueUnchanged(t, m, index, image, entries[1].GetIndex())
				batch, err := m.Stage(replica.Entry{Index: index + 1, Term: entries[1].GetTerm(), Generation: m.store.ApplicationGeneration(), Data: entries[1].GetData()}, m.store.ApplicationBudget())
				if !errors.Is(err, errInvalid) || !reflect.ValueOf(batch).IsZero() {
					t.Fatal("queued Fence became applied authority", err)
				}
			})
		}
	}
	for _, mutation := range []string{"orphan", "owner", "digest", "phase", "proof", "scope", "configuration"} {
		t.Run(mutation, func(t *testing.T) {
			m := recoveryQueueReceiver(t, n, 0, base, prefixes[0])
			index, image, _ := m.store.Checkpoint()
			command, err := decodeRangeMoveCommand(entries[1].GetData())
			if err != nil {
				t.Fatal(err)
			}
			changed := intent
			switch mutation {
			case "owner":
				changed.id[0] ^= 1
			case "digest":
				changed.pageRows--
			case "phase":
				command.phase = rangeMoveFinish
			case "proof":
				command.proofs = []rangeMoveStatus{done.source}
				command.proofs[0].intent[0] ^= 1
			case "scope":
				changed.target.group[0] ^= 1
			case "configuration":
				changed.publication.Configuration[0] ^= 1
			}
			command.intent, err = encodeRangeMoveIntent(changed)
			if err != nil {
				t.Fatal(err)
			}
			wire, err := encodeRangeMoveCommand(command)
			if err != nil {
				t.Fatal(err)
			}
			last := index
			if mutation != "orphan" {
				if err := m.store.Persist(raft.Ready{Entries: entries[:1]}); err != nil {
					t.Fatal(err)
				}
				last = entries[0].GetIndex()
			}
			bad := proto.Clone(entries[1]).(*pb.Entry)
			bad.Data = wire
			if err := m.store.Persist(raft.Ready{Entries: []*pb.Entry{bad}}); !errors.Is(err, raftlog.ErrInvalid) && !errors.Is(err, errInvalid) {
				t.Fatal("unbound prospective range command persisted", mutation, err)
			}
			recoveryQueueUnchanged(t, m, index, image, last)
		})
	}
}

func TestRecoveryCapacityOwnerRetirementBeforeNewCommand(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for _, kind := range []string{"fence", "cross"} {
		t.Run(kind, func(t *testing.T) {
			n, coordinator, intent, wires, _ := rangeMoveCoordinatorFixture(t, 32)
			base := n.hosts[1][0].driver.Applied()
			source := n.hosts[1][0].machine
			prefix, err := source.store.Entries(2, base+1, uint64(source.store.Limits().MaxReadBytes)) // #nosec G115 -- positive validated Store limit.
			if err != nil {
				t.Fatal(err)
			}
			pump, ok := wires[1].service.pump.(*nativeGraphTestPump)
			if !ok {
				t.Fatal("native target pump missing")
			}
			capture := &recoveryQueueCapturePump{nativeGraphTestPump: pump}
			wires[1].service.pump = capture
			done, err := runRangeMoveToCompletion(t, coordinator, intent)
			if err != nil || !done.committed {
				t.Fatal(err)
			}
			finish := done.target.index
			if kind == "fence" {
				next, err := n.hosts[1][0].PrepareRangeMove(t.Context(), n.requestID(), intent.publication.RangeID, 3, 32, 64, 8<<10)
				if err != nil {
					t.Fatal("actual reverse placement", err)
				}
				wire, err := encodeRangeMoveIntent(next)
				if err != nil {
					t.Fatal(err)
				}
				for _, phase := range []rangeMovePhase{rangeMoveFence, rangeMoveCapture} {
					output, err := graphstate.NewOutputBudget(8 << 20)
					if err != nil {
						t.Fatal(err)
					}
					proposal, err := n.hosts[1][0].prepareRangeMoveCommand(wire, phase, 8, 0, nil, nil, output)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := capture.Submit(t.Context(), proposal); err != nil {
						t.Fatal("actual reverse source phase", phase, err)
					}
				}
			} else {
				window, err := temporal.All(graphGenesisBinding(t).Axis())
				if err != nil {
					t.Fatal(err)
				}
				tx, register := crossPlanAt(n, 1, n.requestID(), []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Scope: window, Name: "after-move"}}, nil, 25)
				wire, err := capture.Submit(t.Context(), register)
				if err != nil {
					t.Fatal(err)
				}
				registered, err := decodeCrossStatus(wire)
				if err != nil {
					t.Fatal(err)
				}
				registration := n.crossReadPhase(1, registered.tx, crossRegister)
				proposal, err := n.hosts[1][0].PrepareCrossParticipant(tx, 3, registration, nil)
				if err != nil {
					t.Fatal(err)
				}
				n.crossSubmit(0, proposal)
				vote := n.crossReadPhase(0, registered.tx, crossPrepare)
				proposal, err = n.hosts[1][0].PrepareCrossParticipant(tx, 8, registration, []crossProof{vote})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := capture.Submit(t.Context(), proposal); err != nil {
					t.Fatal(err)
				}
			}
			all := append(append([]*pb.Entry(nil), prefix...), capture.entries...)
			var suffix []*pb.Entry
			for _, entry := range capture.entries {
				if entry.GetIndex() >= finish {
					suffix = append(suffix, entry)
				}
			}
			if len(suffix) != 3 || suffix[0].GetIndex() != finish {
				t.Fatal("actual Finish and two later native commands", len(suffix))
			}
			for _, earlier := range []bool{false, true} {
				t.Run(map[bool]string{false: "same-ready", true: "already-durable-unapplied"}[earlier], func(t *testing.T) {
					m := recoveryQueueReceiver(t, n, 1, finish-1, all)
					index, image, err := m.store.Checkpoint()
					if err != nil {
						t.Fatal(err)
					}
					if kind == "fence" && !earlier {
						t.Run("actual-reader-refusals", func(t *testing.T) {
							view, err := m.store.ApplicationView(index)
							if err != nil {
								t.Fatal(err)
							}
							base, err := view.Root()
							if err != nil {
								t.Fatal(err)
							}
							for _, canceled := range []bool{false, true} {
								ctx, cancel := context.WithCancel(t.Context())
								q := &reader{ctx: ctx, view: view, ns: m.ns, base: base, limits: m.limits.allocation}
								want := raftlog.ErrLimit
								if canceled {
									cancel()
									want = context.Canceled
								} else {
									// Actual RecoveryOwner fixed bookkeeping fits, but its
									// retained frame inspection cannot fit this read grant.
									q.limits.readBytes = 164
								}
								purpose, err := m.recoveryAfterRangeMove(q)
								cancel()
								if !errors.Is(err, want) || purpose.ApplyBefore != nil || purpose.Reserve != nil || purpose.Use != nil || len(q.writes) != 0 {
									t.Fatal("read refusal granted a dependency or graph writes", canceled, purpose, err)
								}
							}
							if err := view.Close(); err != nil {
								t.Fatal(err)
							}
							recoveryQueueUnchanged(t, m, index, image, index)
						})
					}
					refusal := m.store.Persist(raft.Ready{Entries: suffix})
					if !errors.Is(refusal, raftlog.ErrApplicationPrerequisite) || !errors.Is(refusal, errRangeMovePending) {
						t.Fatal("ordinary Persist guessed future eligibility or lost cause", refusal)
					}
					remaining := suffix
					if earlier {
						if err := m.store.Persist(raft.Ready{Entries: suffix[:1]}); err != nil {
							t.Fatal(err)
						}
						remaining = suffix[1:]
					}
					hard, _, err := source.store.InitialState()
					if err != nil {
						t.Fatal(err)
					}
					ready := raft.Ready{HardState: hard, Entries: remaining}
					progress, err := m.store.BeginApplicationReadyProgress(m.binding, ready, hard.GetCommit(), index)
					if err != nil {
						t.Fatal(err)
					}
					if full, err := progress.PersistFull(); err != nil || full {
						t.Fatal("exact retirement did not choose bounded progress", full, err)
					}
					last := index
					if earlier {
						last = finish
					}
					// Classification does not install anything or change the image.
					recoveryQueueUnchanged(t, m, index, image, last)
					through, err := progress.CommitPredecessor()
					if err != nil {
						t.Fatal(err)
					}
					install := func(through uint64) {
						t.Helper()
						for index < through {
							index++
							var entry *pb.Entry
							for _, candidate := range suffix {
								if candidate.GetIndex() == index {
									entry = candidate
								}
							}
							if entry == nil {
								t.Fatal("missing actual committed command", index)
							}
							batch, err := m.Stage(replica.Entry{Index: index, Term: entry.GetTerm(), Generation: m.store.ApplicationGeneration(), Data: entry.GetData()}, m.store.ApplicationBudget())
							if err != nil {
								t.Fatal("ordered Stage", index, err)
							}
							if batch.RecoveryReserve != nil || batch.RecoveryUse != nil {
								batch.RecoveryAdmission, err = m.store.RecoveryAdmissionForCommittedEntry(t.Context(), index)
								if err != nil {
									t.Fatal(err)
								}
							}
							if err := m.store.InstallApplication(index, batch); err != nil {
								t.Fatal(err)
							}
							if err := m.Restore(index, batch.Image); err != nil {
								t.Fatal(err)
							}
						}
					}
					install(through)
					for range len(suffix) {
						through, done, err := progress.AppliedLimit()
						if err != nil {
							t.Fatal(err)
						}
						install(through)
						if done {
							break
						}
						if _, err := progress.PersistPrefix(); err != nil {
							t.Fatal("reclassification after actual retirement", err)
						}
					}
					through, complete, err := progress.AppliedLimit()
					if err != nil {
						t.Fatal(err)
					}
					install(through)
					if !complete || index != suffix[2].GetIndex() {
						t.Fatal("bounded sequence did not apply all actual commands", index)
					}
					// Synthetic Store ownership only: actual RawNode advancement is
					// covered by the production Driver and separate process fixture.
					if err := progress.AdvanceRaw(index); err != nil {
						t.Fatal(err)
					}
					if err := progress.Close(); err != nil {
						t.Fatal(err)
					}
				})
			}
		})
	}
}
