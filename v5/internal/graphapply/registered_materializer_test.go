package graphapply

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
	pb "go.etcd.io/raft/v3/raftpb"
)

// This test breaks a missing Genesis Stage, using actual durable quorum commit.
// It retains the evidence directory and never fabricates a Raft entry or seed DB.
func TestRegisteredMaterializerRealDriverGenesis(t *testing.T) {
	decode := func(text string) []byte {
		t.Helper()
		out, err := hex.DecodeString(text)
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	wantImage := decode("475202020900000000000000000000000000000000000000000000010000000000000001000000000000000000000000000000012a388b7751e1f7b22e5d41079a5f4562d9c1c4148cef1d761efefb8436c4afaa000000000000000100000000000000010000000000000000730980d6ee417843828970b2de0e06740227a9698f352d77108b2b26a748b926")
	wantSource := decode("4a47533100010900000000000000000000000000000000000000000000010a00000000000000000000000000000001000000000000000100000000000000010000008c27ebac460514d5ca92d142aacb7292d997eeacd0c2a6132e559d72e5334454db000000000000000103000000000000000100000000000000020000000000000003358026a048d72e4de703da156369117d06cfa1877da82291f3d23416afd9626d0000000000000002000000000000040000000000001000000000000000010000000000000000100000000000002000000000000000001000000000000040000000000000001000000000000000010000")
	wantCommand := decode("474a5131000100000000f44a47533100010900000000000000000000000000000000000000000000010a00000000000000000000000000000001000000000000000100000000000000010000008c27ebac460514d5ca92d142aacb7292d997eeacd0c2a6132e559d72e5334454db000000000000000103000000000000000100000000000000020000000000000003358026a048d72e4de703da156369117d06cfa1877da82291f3d23416afd9626d0000000000000002000000000000040000000000001000000000000000010000000000000000100000000000002000000000000000001000000000000040000000000000001000000000000000010000")
	wantKey := decode("484b5331010900000000000000000000000000000000000000000000010a0000000000000000000000000000000001")
	wantSeed := decode("48435231000101000900000000000000000000000000000000000000000000010a000000000000000000000000000000f14f6bcb17676555f19f3bfdd1671491418a4d121eec6146bb29e82d3c34416500010000000000000003000000fc4a47533100010900000000000000000000000000000000000000000000010a00000000000000000000000000000001000000000000000100000000000000010000008c27ebac460514d5ca92d142aacb7292d997eeacd0c2a6132e559d72e5334454db000000000000000103000000000000000100000000000000020000000000000003358026a048d72e4de703da156369117d06cfa1877da82291f3d23416afd9626d00000000000000020000000000000400000000000010000000000000000100000000000000001000000000000020000000000000000010000000000000400000000000000010000000000000000100000000000000000002d32809eaf3c72a77596598b6e599d2241c16c1563be01fdac2d4c7d63bc5d782")
	wantCreated := decode("4a4f52310001040000000000000003000001484b5331010900000000000000000000000000000000000000000000010a000000000000000000000000000000000100000000000000030000000000000002f14f6bcb17676555f19f3bfdd1671491418a4d121eec6146bb29e82d3c344165a75627067ad614feb7ffc367af2c6267f901e8a21d1357dce28cf5b8f736c042")
	evidenceRoot := os.Getenv("RHO_REGISTERED_GENESIS_EVIDENCE_ROOT")
	dir, err := os.MkdirTemp(evidenceRoot, "real-three-driver-")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("retained durable cohort: %s", dir)
	var stores [3]*raftlog.Store
	var drivers [3]*replica.Driver
	var originalStageErr error
	t.Cleanup(func() {
		t.Logf("original Stage/event cause preserved separately: %v", originalStageErr)
		for j := range 3 {
			if drivers[j] != nil {
				first, again := drivers[j].Close(), drivers[j].Close()
				if !errors.Is(again, first) {
					t.Errorf("voter%d repeated cleanup differs: %v / %v", j+1, first, again)
				}
				if first != nil {
					t.Errorf("voter%d cleanup: %v", j+1, first)
				}
			} else if stores[j] != nil {
				if err := stores[j].Close(); err != nil {
					t.Errorf("owned pre-Driver store%d: %v", j+1, err)
				}
			}
		}
	})
	empty := func(s *raftlog.Store, index uint64, totalBytes, totalRecords uint64) {
		t.Helper()
		v, err := s.ApplicationView(index)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := v.Close(); err != nil {
				t.Errorf("view cleanup: %v", err)
			}
		}()
		root, err := v.RootBounded(t.Context(), 140)
		if err != nil {
			t.Fatal(err)
		}
		proof, err := v.ProveNoApplicationData(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if !sameBase(root, proof) || !bytes.Equal(root.Image, wantImage) {
			t.Fatal("current GR2 empty proof/root changed")
		}
		usage, err := s.ApplicationControlUsage()
		if err != nil {
			t.Fatal(err)
		}
		if usage.Bytes != 0 || usage.Records != 0 || usage.TotalBytes != totalBytes || usage.TotalRecords != totalRecords {
			t.Fatalf("actual control/graph ledgers: %+v", usage)
		}
	}
	var command []byte
	for j := range 3 {
		id := uint64(j + 1)
		policy := raftlog.DefaultApplicationPolicy(id)
		controls := raftlog.ApplicationControlConfig{Version: 1}
		contract, err := raftlog.ApplicationContractForPolicyWithControls(policy, controls)
		if err != nil {
			t.Fatal(err)
		}
		cfg := raftlog.Config{Dir: filepath.Join(dir, fmt.Sprint(id)), Create: true, Application: policy, Controls: controls,
			Transfer:    raftlog.ApplicationTransferConfig{Identity: raftlog.ApplicationIdentity{Graph: [16]byte{9}, Partition: 1, Group: [16]byte{10}}, Contract: contract, Limits: raftlog.DefaultApplicationTransferLimits()},
			Generations: raftlog.ApplicationGenerationLimits{MaxBytes: 2 << 30, MaxRecords: 2000000}, PublishedCuts: raftlog.ApplicationPublishedCutLimits{MaxTransferChunks: 10000},
			Replication: raftlog.ApplicationReplicationConfig{Voters: [3]uint64{1, 2, 3}}, SemanticContractID: registeredSemanticContractID()}
		s, err := raftlog.Open(cfg)
		if err != nil {
			t.Fatal(err)
		}
		stores[j] = s
		if err := graphstore.BootstrapBoundSinglePartition(s, s.ApplicationBinding(), 1, [3]uint64{1, 2, 3}); err != nil {
			t.Fatal(err)
		}
		hard, cs, err := s.InitialState()
		if err != nil {
			t.Fatal(err)
		}
		term, err := s.Term(1)
		if err != nil {
			t.Fatal(err)
		}
		first, err := s.FirstIndex()
		if err != nil {
			t.Fatal(err)
		}
		last, err := s.LastIndex()
		if err != nil {
			t.Fatal(err)
		}
		if hard.GetTerm() != 1 || hard.GetCommit() != 1 || term != 1 || first != 2 || last != 1 || !slices.Equal(cs.GetVoters(), []uint64{1, 2, 3}) {
			t.Fatal("actual Initialize roles/membership differ")
		}
		empty(s, 1, 275, 3)
		m, err := newRegisteredMaterializer(s, registeredOperationLimits{sourceBytes: 8 << 20, outputBytes: 8 << 20})
		if err != nil {
			t.Fatal(err)
		}
		source, err := encodeGenesisSource(m.source, registeredJournalBudget{walkBytes: 33554432})
		if err != nil || !bytes.Equal(source, wantSource) {
			t.Fatalf("actual pre-election source244 differs: %x %v", source, err)
		}
		wire, err := encodeGenesisCommand(m.source, registeredJournalBudget{walkBytes: 33554432})
		if err != nil || !bytes.Equal(wire, wantCommand) {
			t.Fatalf("actual pre-election GJQ255 differs: %x %v", wire, err)
		}
		if j == 0 {
			command = wire
		} else if !bytes.Equal(wire, command) {
			t.Fatal("actual voter sources disagree")
		}
		d, err := replica.Open(replica.Config{ID: id, Store: s, ApplicationMachine: m, ApplicationSnapshotSends: replica.ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 4 << 20}})
		if err != nil {
			t.Fatal(err)
		}
		drivers[j] = d
	}
	pump := func(out replica.Output) (replica.Output, uint64, error) {
		queue := make([]replica.Packet, 0, 128)
		initial, previous := out, out
		var active replica.Packet
		enqueue := func(next replica.Output) error {
			if len(next.SnapshotSends) != 0 || len(next.Reads) != 0 || len(next.Packets) > cap(queue)-len(queue) {
				return errLimit
			}
			// Account simultaneous transport retention, including aliases
			// conservatively twice; this is not a Driver allocation/RSS cap.
			owned := 64*cap(queue) + 64 + 3*128 + cap(active.Payload)
			for _, packets := range [][]replica.Packet{queue, initial.Packets, previous.Packets, next.Packets} {
				owned += 64 * cap(packets)
				for _, packet := range packets {
					if cap(packet.Payload) > 1<<20 {
						return errLimit
					}
					owned += cap(packet.Payload)
				}
			}
			if owned > 2<<20 {
				return errLimit
			}
			queue = append(queue, next.Packets...)
			return nil
		}
		if err := enqueue(out); err != nil {
			return out, 0, err
		}
		for delivered := 0; len(queue) != 0; delivered++ {
			if delivered >= 1024 {
				return replica.Output{}, 0, errLimit
			}
			p := queue[0]
			active = p
			copy(queue, queue[1:])
			queue[len(queue)-1] = replica.Packet{}
			queue = queue[:len(queue)-1]
			if p.Snapshot || p.From < 1 || p.From > 3 || p.To < 1 || p.To > 3 || cap(p.Payload) > 1<<20 {
				return replica.Output{}, 0, errInvalid
			}
			next, err := drivers[p.To-1].Step(p)
			if err != nil {
				return next, p.To, err
			}
			if err := enqueue(next); err != nil {
				return next, p.To, err
			}
			previous = next
		}
		return replica.Output{}, 0, nil
	}
	out, err := drivers[0].Campaign()
	if err != nil {
		t.Fatal("actual election setup", err)
	}
	if _, _, err := pump(out); err != nil {
		t.Fatal("actual election delivery", err)
	}
	for j, s := range stores {
		index, image, err := s.Checkpoint()
		if err != nil {
			t.Fatal(err)
		}
		term, err := s.Term(2)
		if err != nil {
			t.Fatal(err)
		}
		hard, _, err := s.InitialState()
		if err != nil {
			t.Fatal(err)
		}
		entries, err := s.Entries(2, 3, uint64(s.Limits().MaxReadBytes))
		if err != nil {
			t.Fatal(err)
		}
		if index != 2 || drivers[j].Applied() != 2 || term != 2 || hard.GetCommit() != 2 || !bytes.Equal(image, wantImage) || len(entries) != 1 || entries[0].GetIndex() != 2 || entries[0].GetTerm() != 2 || entries[0].GetType() != pb.EntryNormal || len(entries[0].GetData()) != 0 {
			t.Fatal("observed noop2/term2 fixture roles differ")
		}
		empty(s, 2, 550, 6)
		v, err := s.ApplicationView(1)
		if err != nil {
			t.Fatal(err)
		}
		source, readErr := v.RootBounded(t.Context(), 140)
		closeErr := v.Close()
		if err := errors.Join(readErr, closeErr); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(source.Image, wantImage) {
			t.Fatal("retained source1 changed")
		}
	}
	out, stageErr := drivers[0].Propose(command)
	failedVoter := uint64(1)
	if stageErr == nil {
		out, failedVoter, stageErr = pump(out)
	}
	originalStageErr = stageErr
	if stageErr != nil {
		if !errors.Is(stageErr, errRegisteredStageUnavailable) || !errors.Is(stageErr, replica.ErrStopped) || failedVoter != 1 {
			t.Fatalf("not intended committed GJQ Stage refusal: voter%d %v", failedVoter, stageErr)
		}
		if out.Applied != 0 || out.Packets != nil || out.Reads != nil || out.SnapshotSends != nil {
			t.Fatal("failed actual Driver event output not zero")
		}
		stoppedOut, stoppedErr := drivers[0].Tick()
		if !errors.Is(stoppedErr, replica.ErrStopped) || !errors.Is(stoppedErr, errRegisteredStageUnavailable) || stoppedOut.Applied != 0 || stoppedOut.Packets != nil || stoppedOut.Reads != nil || stoppedOut.SnapshotSends != nil {
			t.Fatalf("subsequent public event lost original stop cause or zero output: %v", stoppedErr)
		}
	}
	leader := stores[0]
	hard, _, err := leader.InitialState()
	if err != nil {
		t.Fatal(err)
	}
	term, err := leader.Term(3)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := leader.Entries(3, 4, uint64(leader.Limits().MaxReadBytes))
	if err != nil {
		t.Fatal(err)
	}
	if hard.GetCommit() < 3 || term != 2 || len(entries) != 1 || entries[0].GetIndex() != 3 || entries[0].GetTerm() != 2 || entries[0].GetType() != pb.EntryNormal || !bytes.Equal(entries[0].GetData(), wantCommand) {
		t.Fatal("actual quorum committed Genesis3/term2 not proved")
	}
	for j, s := range stores {
		index, image, err := s.Checkpoint()
		if err != nil {
			t.Fatal(err)
		}
		last, err := s.LastIndex()
		if err != nil {
			t.Fatal(err)
		}
		state, _, err := s.InitialState()
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("voter%d actual last%d commit%d applied%d checkpoint%d", j+1, last, state.GetCommit(), drivers[j].Applied(), index)
		if stageErr != nil {
			if index != 2 || drivers[j].Applied() != 2 || !bytes.Equal(image, wantImage) {
				t.Fatal("unavailable Stage advanced or changed application")
			}
			empty(s, 2, 550, 6)
			v, err := s.ApplicationView(index)
			if err != nil {
				t.Fatal(err)
			}
			_, found, _, readErr := v.GetControl(t.Context(), wantKey, raftlog.ReadBudget{Rows: 2, Bytes: 4096})
			closeErr := v.Close()
			if err := errors.Join(readErr, closeErr); err != nil {
				t.Fatal(err)
			}
			if found {
				t.Fatal("OriginSeed installed despite unavailable Stage")
			}
		}
	}
	index, _, err := leader.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	v, err := leader.ApplicationView(index)
	if err != nil {
		t.Fatal(err)
	}
	seed, found, _, readErr := v.GetControl(t.Context(), wantKey, raftlog.ReadBudget{Rows: 2, Bytes: 4096})
	closeErr := v.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		t.Fatal(err)
	}
	// This expected-success assertion is the first meaningful missing-Genesis RED.
	if !found || seed.Index != 3 || !bytes.Equal(seed.Key, wantKey) || !bytes.Equal(seed.Value, wantSeed) {
		t.Fatalf("want independent complete OriginSeed378 at observed3/term2; found%t index%d value%x; actual committed Stage cause:%v", found, seed.Index, seed.Value, originalStageErr)
	}
	created, err := leader.ApplicationRecord(t.Context(), 3, true, 145)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(created, wantCreated) {
		t.Fatalf("complete CreatedGenesis JOR145: %x", created)
	}

}
