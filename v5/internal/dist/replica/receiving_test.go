package replica

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func receiverTestStore(t *testing.T, fs vfs.FS) (*raftlog.Store, raftlog.Config) {
	t.Helper()
	return receiverTestStoreAt(t, fs, "receiver")
}

func receiverTestStoreAt(t *testing.T, fs vfs.FS, dir string) (*raftlog.Store, raftlog.Config) {
	t.Helper()
	p := raftlog.DefaultApplicationPolicy(3)
	p.MaxInstallWrites, p.RetainedApplicationRecords, p.RetainedApplicationBytes = 256, 8192, 16<<20
	c := raftlog.Config{Dir: dir, FS: fs, Create: true, Application: p,
		Transfer:    raftlog.ApplicationTransferConfig{Identity: raftlog.ApplicationIdentity{Graph: [16]byte{101}, Partition: 1, Group: [16]byte{102}}, Contract: raftlog.ApplicationContractForPolicy(p), Limits: raftlog.DefaultApplicationTransferLimits()},
		Generations: raftlog.ApplicationGenerationLimits{MaxBytes: 256 << 20, MaxRecords: 32768}, PublishedCuts: raftlog.ApplicationPublishedCutLimits{MaxTransferChunks: 100000},
		Replication: raftlog.ApplicationReplicationConfig{Voters: [3]uint64{1, 2, 3}}, SemanticContractID: raftlog.ApplicationSemanticContractID{17}}
	s, err := raftlog.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s, c
}

func receiverTestConfig(s *raftlog.Store, term uint64) ApplicationReceivingConfig {
	return ApplicationReceivingConfig{Store: s, Binding: s.ApplicationBinding(), ID: 3, Source: 1, Term: term}
}

func receiverTestOpen(t *testing.T, c ApplicationReceivingConfig) *ApplicationReceiver {
	t.Helper()
	r, err := OpenApplicationReceiver(c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := r.Close(); err != nil {
			t.Error(err)
		}
	})
	return r
}

// Synthetic packet fixtures are privileged mechanical input tests only. They
// are never replacement/quorum/retirement authority. The native source test
// below obtains its Append and ReadIndex completion from actual Drivers.
func receiverTestMessage() *pb.Message {
	return &pb.Message{Type: pb.MsgApp.Enum(), From: new(uint64(1)), To: new(uint64(3)), Term: new(uint64(2)),
		Index: new(uint64(2)), LogTerm: new(uint64(2)), Commit: new(uint64(3)),
		Entries: []*pb.Entry{{Index: new(uint64(3)), Term: new(uint64(2)), Type: pb.EntryNormal.Enum(), Data: command(1)}}}
}

func receiverTestPacket(t *testing.T, s *raftlog.Store, m *pb.Message) Packet {
	t.Helper()
	wire, err := proto.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	p, err := EncodeApplicationPacket(s.ApplicationBinding(), Packet{From: 1, To: 3, Payload: wire}, 16<<20)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func receiverTestDecode(t *testing.T, binding raftlog.ApplicationBinding, p Packet) *pb.Message {
	t.Helper()
	inner, err := DecodeApplicationPacket(binding, p, 16<<20)
	if err != nil {
		t.Fatal(err)
	}
	m := new(pb.Message)
	if err := proto.Unmarshal(inner.Payload, m); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestApplicationReceiverNilConstructorAndBorrowedLifetime(t *testing.T) {
	for _, r := range []*ApplicationReceiver{nil, {}} {
		if out, err := r.StepReceiving(Packet{}); out != nil || !errors.Is(err, ErrInvalid) {
			t.Fatal(out, err)
		}
		if err := r.Close(); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"nil-store", "zero-ID", "wrong-local", "zero-source", "same-source", "foreign-source", "zero-term", "ceiling", "binding", "limits", "packet-small", "packet-large", "output-small", "output-large", "durable-term-ahead", "initialized", "closed"} {
		t.Run(name, func(t *testing.T) {
			s, _ := receiverTestStore(t, vfs.NewMem())
			c := receiverTestConfig(s, 2)
			want := ErrInvalid
			switch name {
			case "nil-store":
				c.Store = nil
			case "zero-ID":
				c.ID = 0
			case "wrong-local":
				c.ID = 2
			case "zero-source":
				c.Source = 0
			case "same-source":
				c.Source = 3
			case "foreign-source":
				c.Source = 4
			case "zero-term":
				c.Term = 0
			case "ceiling":
				c.Term = replicaTermCeiling
				want = ErrLimit
			case "binding":
				c.Binding.Identity.Graph[0]++
			case "limits":
				c.Limits = s.Limits()
				c.Limits.MaxReadEntries++
			case "packet-small":
				c.MaxPacketBytes = 1
			case "packet-large":
				c.MaxPacketBytes = 66 << 20
			case "output-small":
				c.MaxOutputBytes = 1
			case "output-large":
				c.MaxOutputBytes = 257 << 20
			case "durable-term-ahead":
				if err := s.PersistApplicationReceiving(raft.Ready{HardState: &pb.HardState{Term: new(uint64(3))}}); err != nil {
					t.Fatal(err)
				}
			case "initialized":
				if err := s.Initialize([]uint64{1, 2, 3}, nil); err != nil {
					t.Fatal(err)
				}
			case "closed":
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if r, err := OpenApplicationReceiver(c); r != nil || !errors.Is(err, want) || name == "closed" && !errors.Is(err, raftlog.ErrClosed) {
				t.Fatal(r, err)
			}
			if name != "closed" {
				if _, _, err := s.InitialState(); err != nil {
					t.Fatal("failed construction closed borrowed Store", err)
				}
			}
		})
	}
	s, _ := receiverTestStore(t, vfs.NewMem())
	r := receiverTestOpen(t, receiverTestConfig(s, 2))
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if out, err := r.StepReceiving(receiverTestPacket(t, s, receiverTestMessage())); out != nil || !errors.Is(err, ErrStopped) {
		t.Fatal(out, err)
	}
	if state, err := s.ApplicationReceivingState(); err != nil || state.Term != 0 {
		t.Fatal("Receiver.Close closed/changed borrowed Store", state, err)
	}
}

func TestApplicationReceiverPreStepWhitelistAndBoundsLeaveStateUnchanged(t *testing.T) {
	for _, name := range []string{"vote", "prevote", "heartbeat", "timeout-now", "proposal", "read", "read-response", "local", "lower-term", "higher-term", "zero-term", "zero-prefix", "zero-logterm", "future-logterm", "index-overflow", "gap", "entry-zero-term", "entry-future-term", "config-entry", "snapshot-field", "context", "reject-flag", "inner-from", "inner-to", "outer-from", "outer-to", "outer-snapshot", "RP-scope", "RP-length", "unknown", "oversize-entry", "entry-count", "oversize-packet"} {
		t.Run(name, func(t *testing.T) {
			s, _ := receiverTestStore(t, vfs.NewMem())
			r := receiverTestOpen(t, receiverTestConfig(s, 2))
			m := receiverTestMessage()
			want := ErrInvalid
			switch name {
			case "vote":
				m.Type = pb.MsgVote.Enum()
			case "prevote":
				m.Type = pb.MsgPreVote.Enum()
			case "heartbeat":
				m.Type = pb.MsgHeartbeat.Enum()
			case "timeout-now":
				m.Type = pb.MsgTimeoutNow.Enum()
			case "proposal":
				m.Type = pb.MsgProp.Enum()
			case "read":
				m.Type = pb.MsgReadIndex.Enum()
			case "read-response":
				m.Type = pb.MsgReadIndexResp.Enum()
			case "local":
				m.Type = pb.MsgHup.Enum()
			case "lower-term":
				m.Term = new(uint64(1))
			case "higher-term":
				m.Term = new(uint64(3))
			case "zero-term":
				m.Term = new(uint64(0))
			case "zero-prefix":
				m.Index, m.LogTerm = new(uint64(0)), new(uint64(0))
				m.Entries[0].Index = new(uint64(1))
			case "zero-logterm":
				m.LogTerm = new(uint64(0))
			case "future-logterm":
				m.LogTerm = new(uint64(3))
			case "index-overflow":
				m.Index = new(uint64(math.MaxUint64))
			case "gap":
				m.Entries[0].Index = new(uint64(4))
			case "entry-zero-term":
				m.Entries[0].Term = new(uint64(0))
			case "entry-future-term":
				m.Entries[0].Term = new(uint64(3))
			case "config-entry":
				m.Entries[0].Type = pb.EntryConfChangeV2.Enum()
			case "snapshot-field":
				m.Snapshot = &pb.Snapshot{}
			case "context":
				m.Context = []byte("not-authentication")
			case "reject-flag":
				m.Reject = new(true)
			case "inner-from":
				m.From = new(uint64(2))
			case "inner-to":
				m.To = new(uint64(2))
			case "unknown":
				m.ProtoReflect().SetUnknown([]byte{0x80, 0x01, 0x01})
			case "oversize-entry":
				m.Entries[0].Data = make([]byte, s.Limits().MaxEntryBytes)
				want = ErrLimit
			case "entry-count":
				for i := 1; i <= s.Limits().MaxReadEntries; i++ {
					m.Entries = append(m.Entries, &pb.Entry{Index: new(uint64(3 + i)), Term: new(uint64(2))})
				}
				want = ErrLimit
			}
			p := receiverTestPacket(t, s, m)
			switch name {
			case "outer-from":
				p.From = 2
			case "outer-to":
				p.To = 2
			case "outer-snapshot":
				p.Snapshot = true
			case "RP-scope":
				p.Payload[4]++
			case "RP-length":
				p.Payload = p.Payload[:len(p.Payload)-1]
			case "oversize-packet":
				p.Payload = make([]byte, r.config.MaxPacketBytes+1)
				want = ErrLimit
			}
			before, err := s.ApplicationReceivingState()
			if err != nil {
				t.Fatal(err)
			}
			if out, err := r.StepReceiving(p); out != nil || !errors.Is(err, want) {
				t.Fatal("pre-Step refusal", out, err)
			}
			if after, err := s.ApplicationReceivingState(); err != nil || after != before || r.raw.BasicStatus().GetTerm() != 0 || r.raw.BasicStatus().GetVote() != 0 || r.raw.HasReady() || r.stopped != nil {
				t.Fatal("untrusted packet changed Raft/durable state", after, err)
			}
			if out, err := r.StepReceiving(receiverTestPacket(t, s, receiverTestMessage())); err != nil || len(out) != 1 {
				t.Fatal("clean refusal stopped later valid mechanical traffic", out, err)
			}
		})
	}
}

func TestApplicationReceiverDurableRejectOwnedPacketsAndReopen(t *testing.T) {
	fs := vfs.NewCrashableMem()
	s, cfg := receiverTestStore(t, fs)
	r := receiverTestOpen(t, receiverTestConfig(s, 2))
	p := receiverTestPacket(t, s, receiverTestMessage())
	input := bytes.Clone(p.Payload)
	out, err := r.StepReceiving(p)
	if err != nil || len(out) != 1 {
		t.Fatal(out, err)
	}
	expected := bytes.Clone(out[0].Payload)
	reject := receiverTestDecode(t, s.ApplicationBinding(), out[0])
	state, err := s.ApplicationReceivingState()
	if err != nil || state.Term != 2 || reject.GetType() != pb.MsgAppResp || !reject.GetReject() || reject.GetIndex() != 2 || reject.GetRejectHint() != 0 || reject.GetTerm() != state.Term {
		t.Fatal("actual rejection did not follow durable term", reject, state, err)
	}
	if !bytes.Equal(p.Payload, input) {
		t.Fatal("borrowed packet mutated")
	}
	clear(out[0].Payload)
	again, err := r.StepReceiving(p)
	if err != nil || len(again) != 1 || !bytes.Equal(again[0].Payload, expected) {
		t.Fatal("returned packet aliases receiver state", again, err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.Create, cfg.FS = false, fs.CrashClone(vfs.CrashCloneCfg{})
	reopened, err := raftlog.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	next := receiverTestOpen(t, receiverTestConfig(reopened, 2))
	if out, err := next.StepReceiving(p); err != nil || len(out) != 1 {
		t.Fatal("persisted known term did not allow mechanical restart", out, err)
	}
	if err := reopened.Initialize([]uint64{1, 2, 3}, nil); !errors.Is(err, raftlog.ErrInvalid) {
		t.Fatal("restart bootstrapped old identity", err)
	}
	if _, _, err := s.InitialState(); err != nil {
		t.Fatal("receiver close released borrowed Store", err)
	}
}

func TestApplicationReceiverBorrowedStoreFailureStopsAndPreservesCause(t *testing.T) {
	s, _ := receiverTestStore(t, vfs.NewMem())
	r := receiverTestOpen(t, receiverTestConfig(s, 2))
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	p := receiverTestPacket(t, s, receiverTestMessage())
	for range 2 {
		if out, err := r.StepReceiving(p); out != nil || !errors.Is(err, ErrStopped) || !errors.Is(err, raftlog.ErrClosed) {
			t.Fatal("operational cause lost/output escaped", out, err)
		}
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if out, err := r.StepReceiving(p); out != nil || !errors.Is(err, ErrStopped) || !errors.Is(err, raftlog.ErrClosed) {
		t.Fatal("close erased stopped cause", out, err)
	}
}

func receiverNativeBarrier(t *testing.T, n *capacityRegressionNetwork) ReadResult {
	t.Helper()
	token := []byte("actual-surviving-quorum-receiving-barrier")
	out, err := n.nodes[1].ReadIndex(token)
	if err != nil {
		t.Fatal(err)
	}
	reads, queue := out.Reads, out.Packets
	for deliveries := 0; len(queue) > 0; deliveries++ {
		if deliveries >= 1000 {
			t.Fatal("bounded actual read transport")
		}
		p := queue[0]
		queue = queue[1:]
		if p.To == 3 {
			continue
		}
		next, err := n.nodes[p.To].Step(p)
		if err != nil {
			t.Fatal(err)
		}
		reads = append(reads, next.Reads...)
		queue = append(queue, next.Packets...)
	}
	if len(reads) != 1 || !bytes.Equal(reads[0].Context, token) || reads[0].Index == 0 || reads[0].Index > n.nodes[1].Applied() {
		t.Fatal("actual correlated applied quorum read absent", reads)
	}
	return reads[0]
}

func TestApplicationReceiverNativeQuorumAppendRejectProducesSnapshotSend(t *testing.T) {
	n, original, barrier, cut := receiverNativeAppendFixture(t)
	source := n.nodes[1]
	term := cut.Term
	message := receiverTestDecode(t, source.binding, original)
	s, _ := receiverTestStore(t, vfs.NewMem())
	r := receiverTestOpen(t, receiverTestConfig(s, term))
	replies, err := r.StepReceiving(original)
	if err != nil || len(replies) != 1 {
		t.Fatal(replies, err)
	}
	state, err := s.ApplicationReceivingState()
	rejection := receiverTestDecode(t, source.binding, replies[0])
	if err != nil || state.Term != term || !rejection.GetReject() || rejection.GetIndex() != message.GetIndex() || rejection.GetRejectHint() != 0 {
		t.Fatal("original native reject before durable term", rejection, state, err)
	}
	response, err := source.Step(replies[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := n.pump(response.Packets, func(p Packet) bool { return p.To != 3 }); err != nil {
		t.Fatal(err)
	}
	offers, err := source.SnapshotSends()
	if err != nil || len(offers) != 1 {
		t.Fatal("native rejection did not create actual SnapshotSend", offers, err)
	}
	send := offers[0]
	ready := false
	for range 100 {
		ready, err = send.Build(t.Context(), raftlog.ReadBudget{Rows: 1, Bytes: 4096})
		if err != nil {
			t.Fatal(err)
		}
		if ready {
			break
		}
	}
	if !ready {
		t.Fatal("bounded genuine snapshot build")
	}
	manifest, err := send.Manifest()
	if err != nil || manifest.CutID != cut.ID || manifest.Index < barrier.Index || manifest.Term != term || manifest.SemanticContractID != source.binding.SemanticContractID {
		t.Fatal("actual offered snapshot/source mismatch", manifest, err)
	}
	if err := source.SealSnapshotSend(send); err != nil {
		t.Fatal(err)
	}
	// Seal alone emits nothing. Real source work drives its original RawNode
	// storage callback again; no fabricated response or Progress reset is used.
	next, err := source.Propose(command(5))
	if err != nil {
		t.Fatal(err)
	}
	if err := n.pump(next.Packets, func(p Packet) bool { return p.To != 3 }); err != nil {
		t.Fatal(err)
	}
	index := slices.IndexFunc(next.Packets, func(p Packet) bool { return p.To == 3 && p.Snapshot })
	if index < 0 || len(next.SnapshotSends) != 1 || next.SnapshotSends[0] != send {
		t.Fatal("actual source MsgSnap/owner pair absent", next)
	}
	snapshot := receiverTestDecode(t, source.binding, next.Packets[index])
	if snapshot.GetTerm() != term || snapshot.GetSnapshot().GetMetadata().GetIndex() != manifest.Index || snapshot.GetSnapshot().GetMetadata().GetTerm() != term {
		t.Fatal("actual snapshot term/index binding", snapshot)
	}
	if out, err := r.StepReceiving(next.Packets[index]); out != nil || !errors.Is(err, ErrInvalid) {
		t.Fatal("initial receiver admitted activation traffic", out, err)
	}
	if after, err := s.ApplicationReceivingState(); err != nil || after != state {
		t.Fatal("rejected snapshot changed empty receiving state", after, err)
	}
	if _, err := source.ReportSnapshotSend(send, false); err != nil {
		t.Fatal("actual refused delivery feedback", err)
	}
	t.Logf("native read barrier=%d term=%d originalAppendPrev=%d actualPublished=%d actualSnapshot=%d; no activation", barrier.Index, term, message.GetIndex(), cut.Index, manifest.Index)
}

func receiverNativeAppendFixture(t *testing.T) (*capacityRegressionNetwork, Packet, ReadResult, raftlog.ApplicationCutReference) {
	t.Helper()
	n := newCapacityRegressionNetwork(t, false)
	n.campaign(1, nil)
	for _, value := range []uint64{1, 2, 3} {
		n.propose(1, value, func(p Packet) bool { return p.To != 3 })
	}
	// This local Driver control is not a credential-service retirement proof.
	// After closing the old voter, only actual voters1/2 complete the barrier.
	if err := n.nodes[3].Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := n.nodes[1].ReportUnreachable(3); err != nil {
		t.Fatal(err)
	}
	barrier := receiverNativeBarrier(t, n)
	source := n.nodes[1]
	term, err := source.store.Term(barrier.Index)
	if err != nil || term != source.raw.BasicStatus().GetTerm() {
		t.Fatal("current-term read entry absent", term, err)
	}
	produced, err := source.Propose(command(4))
	if err != nil {
		t.Fatal(err)
	}
	var original Packet
	for _, p := range produced.Packets {
		if p.To == 3 {
			original = p
		}
	}
	if err := n.pump(produced.Packets, func(p Packet) bool { return p.To != 3 }); err != nil {
		t.Fatal(err)
	}
	if len(original.Payload) == 0 || original.Snapshot {
		t.Fatal("genuine source ordinary append absent")
	}
	message := receiverTestDecode(t, source.binding, original)
	if message.GetType() != pb.MsgApp || message.GetIndex() == 0 || message.GetTerm() != term {
		t.Fatal("original source nonzero-prefix Append", message)
	}
	if err := source.store.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	cut, err := source.store.PublishedApplicationCut()
	if err != nil || cut.Index < barrier.Index || cut.Term != term {
		t.Fatal("published cut does not cover actual current-term barrier", cut, barrier, err)
	}
	return n, original, barrier, cut
}

func TestApplicationReceiverRepeatedSerializedRejects(t *testing.T) {
	s, _ := receiverTestStore(t, vfs.NewMem())
	r := receiverTestOpen(t, receiverTestConfig(s, 2))
	for i := range 4 {
		m := receiverTestMessage()
		m.Index, m.LogTerm, m.Entries = new(uint64(i+1)), new(uint64(2)), nil
		out, err := r.StepReceiving(receiverTestPacket(t, s, m))
		if err != nil || len(out) != 1 || receiverTestDecode(t, s.ApplicationBinding(), out[0]).GetIndex() != uint64(i+1) {
			t.Fatal(fmt.Sprint(i), out, err)
		}
	}
}

func TestApplicationReceiverConcurrentStepCloseBorrowsStore(t *testing.T) {
	s, _ := receiverTestStore(t, vfs.NewMem())
	r := receiverTestOpen(t, receiverTestConfig(s, 2))
	p := receiverTestPacket(t, s, receiverTestMessage())
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			out, err := r.StepReceiving(p)
			if err != nil && !errors.Is(err, ErrStopped) || err == nil && len(out) != 1 || err != nil && out != nil {
				t.Errorf("serialized Step/Close result: %v %v", out, err)
			}
		})
	}
	wg.Go(func() {
		if err := r.Close(); err != nil {
			t.Error(err)
		}
	})
	wg.Wait()
	if state, err := s.ApplicationReceivingState(); err != nil || state.Term != 0 && state.Term != 2 {
		t.Fatal("receiver lifecycle touched borrowed Store", state, err)
	}
}
