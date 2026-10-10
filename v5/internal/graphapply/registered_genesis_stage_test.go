package graphapply

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
	"go.etcd.io/raft/v3"
)

type registeredStageTestGroup struct {
	t       *testing.T
	configs [3]raftlog.Config
	stores  [3]*raftlog.Store
	drivers [3]*replica.Driver
}

func registeredStageHex(t *testing.T, text string) []byte {
	t.Helper()
	out, err := hex.DecodeString(text)
	if err != nil {
		t.Fatal(err)
	}
	return out
}
func newRegisteredStageTestGroup(t *testing.T, graph, group byte) *registeredStageTestGroup {
	t.Helper()
	dir, err := os.MkdirTemp(os.Getenv("RHO_REGISTERED_GENESIS_EVIDENCE_ROOT"), "regular-three-driver-")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("retained regular cohortGraph%d/Group%d: %s", graph, group, dir)
	g := &registeredStageTestGroup{t: t}
	t.Cleanup(func() {
		for j := range 3 {
			if g.drivers[j] != nil {
				if err := g.drivers[j].Close(); err != nil {
					t.Errorf("driver cleanup: %v", err)
				}
			} else if g.stores[j] != nil {
				if err := g.stores[j].Close(); err != nil {
					t.Errorf("store cleanup: %v", err)
				}
			}
		}
	})
	for j := range 3 {
		p := raftlog.DefaultApplicationPolicy(uint64(j + 1))
		ctrl := raftlog.ApplicationControlConfig{Version: 1}
		contract, err := raftlog.ApplicationContractForPolicyWithControls(p, ctrl)
		if err != nil {
			t.Fatal(err)
		}
		cfg := raftlog.Config{Dir: filepath.Join(dir, fmt.Sprint(j+1)), Create: true, Application: p, Controls: ctrl,
			Transfer:    raftlog.ApplicationTransferConfig{Identity: raftlog.ApplicationIdentity{Graph: [16]byte{graph}, Partition: 1, Group: [16]byte{group}}, Contract: contract, Limits: raftlog.DefaultApplicationTransferLimits()},
			Generations: raftlog.ApplicationGenerationLimits{MaxBytes: 2 << 30, MaxRecords: 2000000}, PublishedCuts: raftlog.ApplicationPublishedCutLimits{MaxTransferChunks: 10000}, Replication: raftlog.ApplicationReplicationConfig{Voters: [3]uint64{1, 2, 3}}, SemanticContractID: registeredSemanticContractID()}
		s, err := raftlog.Open(cfg)
		if err != nil {
			t.Fatal(err)
		}
		g.stores[j], g.configs[j] = s, cfg
		if err := graphstore.BootstrapBoundSinglePartition(s, s.ApplicationBinding(), 1, [3]uint64{1, 2, 3}); err != nil {
			t.Fatal(err)
		}
		m, err := newRegisteredMaterializer(s, registeredOperationLimits{sourceBytes: 8 << 20, outputBytes: 8 << 20})
		if err != nil {
			t.Fatal(err)
		}
		d, err := replica.Open(replica.Config{ID: uint64(j + 1), Store: s, ApplicationMachine: m, ApplicationSnapshotSends: replica.ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 4 << 20}})
		if err != nil {
			t.Fatal(err)
		}
		g.drivers[j] = d
	}
	out, err := g.drivers[0].Campaign()
	if err != nil {
		t.Fatal(err)
	}
	g.pump(out)
	g.roles(2, 2)
	return g
}
func registeredStagePump(drivers [3]*replica.Driver, out replica.Output) (replica.Output, uint64, error) {
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
func (g *registeredStageTestGroup) pump(out replica.Output) {
	g.t.Helper()
	if _, voter, err := registeredStagePump(g.drivers, out); err != nil {
		g.t.Fatalf("unexpected actual Step voter%d: %v", voter, err)
	}
}
func (g *registeredStageTestGroup) roles(index, term uint64) {
	g.t.Helper()
	for j, s := range g.stores {
		cp, _, err := s.Checkpoint()
		if err != nil {
			g.t.Fatal(err)
		}
		actual, err := s.Term(index)
		if err != nil {
			g.t.Fatal(err)
		}
		hard, _, err := s.InitialState()
		if err != nil {
			g.t.Fatal(err)
		}
		if cp != index || g.drivers[j].Applied() != index || actual != term || hard.GetCommit() != index {
			g.t.Fatalf("conditional actual roles differ voter%d: cp%d applied%d term%d commit%d", j+1, cp, g.drivers[j].Applied(), actual, hard.GetCommit())
		}
	}
}
func (g *registeredStageTestGroup) retainedOrigin(j int, key, want []byte) {
	g.t.Helper()
	cp, _, err := g.stores[j].Checkpoint()
	if err != nil {
		g.t.Fatal(err)
	}
	v, err := g.stores[j].ApplicationView(cp)
	if err != nil {
		g.t.Fatal(err)
	}
	row, found, _, readErr := v.GetControl(g.t.Context(), key, raftlog.ReadBudget{Rows: 2, Bytes: 4096})
	closeErr := v.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		g.t.Fatal(err)
	}
	if !found || row.Index != 3 || !bytes.Equal(row.Key, key) || !bytes.Equal(row.Value, want) {
		g.t.Fatal("original S3/term2 record rewritten or missing")
	}
	usage, err := g.stores[j].ApplicationControlUsage()
	if err != nil {
		g.t.Fatal(err)
	}
	if usage.Bytes != 510 || usage.Records != 1 {
		g.t.Fatalf("control family ledger changed: %+v", usage)
	}
}

// Breaks replay using current/new term, replacement, and reopen guessing source.
func TestRegisteredGenesisHigherTermReplayReopenAndRefuseReplacement(t *testing.T) {
	g := newRegisteredStageTestGroup(t, 5, 6)
	command := registeredStageHex(t, "474a5131000100000000f44a47533100010500000000000000000000000000000000000000000000010600000000000000000000000000000001000000000000000100000000000000010000008cf9686b196a04e763a26de24b71981267151af29afcc3eca5394c41282e795ec3000000000000000103000000000000000100000000000000020000000000000003358026a048d72e4de703da156369117d06cfa1877da82291f3d23416afd9626d0000000000000002000000000000040000000000001000000000000000010000000000000000100000000000002000000000000000001000000000000040000000000000001000000000000000010000")
	key := registeredStageHex(t, "484b533101050000000000000000000000000000000000000000000001060000000000000000000000000000000001")
	origin := registeredStageHex(t, "484352310001010005000000000000000000000000000000000000000000000106000000000000000000000000000000b7b8b279a1fc706f1f856f619cf5dcd49f492bfa60c4bc8ba45285711d01fbf400010000000000000003000000fc4a47533100010500000000000000000000000000000000000000000000010600000000000000000000000000000001000000000000000100000000000000010000008cf9686b196a04e763a26de24b71981267151af29afcc3eca5394c41282e795ec3000000000000000103000000000000000100000000000000020000000000000003358026a048d72e4de703da156369117d06cfa1877da82291f3d23416afd9626d000000000000000200000000000004000000000000100000000000000001000000000000000010000000000000200000000000000000100000000000004000000000000000100000000000000001000000000000000000029c41e643f95cfc316ea45d3fddf6b3580e57c1e00d96f9160c2eb649cdf12d99")
	created := registeredStageHex(t, "4a4f52310001040000000000000003000001484b53310105000000000000000000000000000000000000000000000106000000000000000000000000000000000100000000000000030000000000000002b7b8b279a1fc706f1f856f619cf5dcd49f492bfa60c4bc8ba45285711d01fbf41daa1e175778ee5da509a4588255a4b27df7ca536ced7d4b92eae86a02d9d0c1")
	original := registeredStageHex(t, "4a4f52310001050000000000000005000001484b53310105000000000000000000000000000000000000000000000106000000000000000000000000000000000100000000000000030000000000000002b7b8b279a1fc706f1f856f619cf5dcd49f492bfa60c4bc8ba45285711d01fbf4593c74cb56f5e70a0542c10170d2ae39c544e822dcf26a1eb791679128d98a80")
	out, err := g.drivers[0].Propose(command)
	if err != nil {
		t.Fatal("missing actual Genesis behavior", err)
	}
	g.pump(out)
	g.roles(3, 2)
	for j := range 3 {
		g.retainedOrigin(j, key, origin)
		result, err := g.stores[j].ApplicationRecord(t.Context(), 3, true, 145)
		if err != nil || !bytes.Equal(result, created) {
			t.Fatalf("full Created JOR: %x %v", result, err)
		}
	}
	// Stock public schedule: expire quorum/leases with20 isolated Tick rounds.
	// Discard real output; no private role/term/log mutation or forced coordinates.
	for range 20 {
		for _, d := range g.drivers {
			out, err := d.Tick()
			if err != nil || len(out.Reads) != 0 || len(out.SnapshotSends) != 0 {
				t.Fatalf("isolated public Tick: %v", err)
			}
		}
	}
	out, err = g.drivers[1].Campaign()
	if err != nil {
		t.Fatal(err)
	}
	g.pump(out)
	g.roles(4, 3)
	for j := range 3 {
		noop, err := g.stores[j].Entries(4, 5, uint64(g.stores[j].Limits().MaxReadBytes))
		if err != nil || len(noop) != 1 || len(noop[0].GetData()) != 0 {
			t.Fatal("higher-term genuine empty noop differs", err)
		}
		g.retainedOrigin(j, key, origin)
	}
	out, err = g.drivers[1].Propose(command)
	if err != nil {
		t.Fatal(err)
	}
	g.pump(out)
	g.roles(5, 3)
	for j := range 3 {
		g.retainedOrigin(j, key, origin)
		result, err := g.stores[j].ApplicationRecord(t.Context(), 5, true, 145)
		if err != nil || !bytes.Equal(result, original) {
			t.Fatalf("higher E5/term3 must retain original S3/term2 JOR: %x %v", result, err)
		}
		usage, err := g.stores[j].ApplicationControlUsage()
		if err != nil || usage.TotalBytes != 2175 || usage.TotalRecords != 16 {
			t.Fatalf("replay/noop full ledgers: %+v %v", usage, err)
		}
	}
	// Actual ordinary prefix publication compacts the original Raft entry;
	// application versions/Origin still survive, unlike current-term guesses.
	for j := range 3 {
		if err := g.stores[j].PublishSnapshot(); err != nil {
			t.Fatal(err)
		}
		if err := g.stores[j].ReclaimApplication(); err != nil {
			t.Fatal(err)
		}
		_, err := g.stores[j].Term(3)
		if !errors.Is(err, raft.ErrCompacted) {
			t.Fatalf("original term not actually compacted: %v", err)
		}
		first, err := g.stores[j].FirstIndex()
		if err != nil || first != 6 {
			t.Fatalf("actual published prefix boundary: %d %v", first, err)
		}
		g.retainedOrigin(j, key, origin)
	}
	// New actual constructor after Close must derive source from durable Origin,
	// not strict fresh Initialize/current hard term3 or another GR3 agreement.
	for j := range 3 {
		if err := g.drivers[j].Close(); err != nil {
			t.Fatal(err)
		}
		g.drivers[j], g.stores[j] = nil, nil
		cfg := g.configs[j]
		cfg.Create = false
		s, err := raftlog.Open(cfg)
		if err != nil {
			t.Fatal(err)
		}
		g.stores[j] = s
		m, err := newRegisteredMaterializer(s, registeredOperationLimits{sourceBytes: 8 << 20, outputBytes: 8 << 20})
		if err != nil {
			t.Fatal("durable Origin reopen", err)
		}
		captured, err := encodeGenesisCommand(m.source, registeredJournalBudget{walkBytes: 33554432})
		if err != nil || !bytes.Equal(captured, command) {
			t.Fatal("reopen substituted current term/source", err)
		}
		d, err := replica.Open(replica.Config{ID: uint64(j + 1), Store: s, ApplicationMachine: m, ApplicationSnapshotSends: replica.ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 4 << 20}})
		if err != nil {
			t.Fatal(err)
		}
		g.drivers[j] = d
		if d.Applied() != 5 {
			t.Fatal("reopened progress not actual synced5")
		}
		g.retainedOrigin(j, key, origin)
	}
	// Invalid same-key source must fail a real committed Stage, never replace.
	out, err = g.drivers[1].Campaign()
	if err != nil {
		t.Fatal(err)
	}
	g.pump(out)
	changed := bytes.Clone(command)
	changed[117] = 2
	out, err = g.drivers[1].Propose(changed)
	if err == nil {
		out, _, err = registeredStagePump(g.drivers, out)
	}

	if !errors.Is(err, replica.ErrStopped) || !errors.Is(err, errInvalid) || out.Applied != 0 || out.Packets != nil || out.Reads != nil || out.SnapshotSends != nil {
		t.Fatalf("replacement not refused with zero actual output: %v", err)
	}
	for j := range 3 {
		g.retainedOrigin(j, key, origin)
	}
}

// Guard: reopening advanced application state without Origin never repairs it.
func TestRegisteredGenesisMissingOriginReopenRefuses(t *testing.T) {
	g := newRegisteredStageTestGroup(t, 7, 8)
	for j := range 3 {
		if err := g.drivers[j].Close(); err != nil {
			t.Fatal(err)
		}
		g.drivers[j], g.stores[j] = nil, nil
		cfg := g.configs[j]
		cfg.Create = false
		s, err := raftlog.Open(cfg)
		if err != nil {
			t.Fatal(err)
		}
		g.stores[j] = s
		m, err := newRegisteredMaterializer(s, registeredOperationLimits{sourceBytes: 8 << 20, outputBytes: 8 << 20})
		if !errors.Is(err, errInvalid) || m != nil {
			t.Fatalf("missing Origin repaired/accepted afterapplied2: %v", err)
		}
		cp, _, err := s.Checkpoint()
		if err != nil || cp != 2 {
			t.Fatal("refusal changed checkpoint", err)
		}
		usage, err := s.ApplicationControlUsage()
		if err != nil || usage.Bytes != 0 || usage.Records != 0 {
			t.Fatal("refusal created control state", err)
		}
	}
}
