package replica

import (
	"encoding/binary"
	"fmt"
	"slices"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// These regressions deliver only original public Driver packets. They never
// synthesize an Append, commit index, persisted log, or RawNode Ready.
type capacityRegressionMachine struct {
	applicationFixture
	t         *testing.T
	store     *raftlog.Store
	staged    []uint64
	forbidden []uint64
}

func (*capacityRegressionMachine) SemanticContractID() raftlog.ApplicationSemanticContractID {
	return raftlog.ApplicationSemanticContractID{17}
}

func (m *capacityRegressionMachine) Stage(e Entry, budget raftlog.ApplicationBudget) (raftlog.ApplicationBatch, error) {
	hard, _, err := m.store.InitialState()
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	if e.Index > hard.GetCommit() {
		m.t.Errorf("Stage applied index %d above actual durable commit %d", e.Index, hard.GetCommit())
		return raftlog.ApplicationBatch{}, ErrInvalid
	}
	if len(e.Data) == 8 {
		value := binary.BigEndian.Uint64(e.Data)
		if slices.Contains(m.forbidden, value) {
			m.t.Errorf("Stage applied replaced uncommitted old payload %d at index %d", value, e.Index)
			return raftlog.ApplicationBatch{}, ErrInvalid
		}
		m.staged = append(m.staged, value)
	}
	return m.applicationFixture.Stage(e, budget)
}

type capacityRegressionNetwork struct {
	t        *testing.T
	nodes    map[uint64]*Driver
	machines map[uint64]*capacityRegressionMachine
	seen     []capacityRegressionAppend
	// Withhold every follower's successful Append ACK above the original commit.
	// A heartbeat can retransmit the held tail to either follower, so holding
	// only the lagging receiver's ACK would still permit a genuine new quorum.
	// Rejections name the failed probe index, not an acknowledged entry; deliver
	// them unchanged so Raft can discover and resend the receiver's missing tail.
	holdAppendACKAbove uint64
	heldReplies        []Packet
}

type capacityRegressionAppend struct {
	from, to, previous, commit, last uint64
	entries                          int
}

func newCapacityRegressionNetwork(t *testing.T, smallPage bool) *capacityRegressionNetwork {
	t.Helper()
	n := &capacityRegressionNetwork{t: t, nodes: map[uint64]*Driver{}, machines: map[uint64]*capacityRegressionMachine{}}
	for id := uint64(1); id <= 3; id++ {
		p := raftlog.DefaultApplicationPolicy(id)
		p.MaxInstallWrites = 256
		p.RetainedApplicationRecords = 8192
		p.RetainedApplicationBytes = 64 << 20
		l := raftlog.DefaultLimits()
		if id == 3 {
			p.RetainedApplicationBytes = 16 << 20
			if smallPage {
				l.MaxReadEntries = 2
				p.ReclaimEntries = 2
			}
		}
		cfg := raftlog.Config{Dir: fmt.Sprintf("independent-capacity-%d", id), FS: vfs.NewMem(), Create: true, Limits: l, Application: p,
			Transfer:    raftlog.ApplicationTransferConfig{Identity: raftlog.ApplicationIdentity{Graph: [16]byte{101}, Partition: 1, Group: [16]byte{102}}, Contract: raftlog.ApplicationContractForPolicy(p), Limits: raftlog.DefaultApplicationTransferLimits()},
			Generations: raftlog.ApplicationGenerationLimits{MaxBytes: 256 << 20, MaxRecords: 32768}, PublishedCuts: raftlog.ApplicationPublishedCutLimits{MaxTransferChunks: 100000},
			Replication: raftlog.ApplicationReplicationConfig{Voters: [3]uint64{1, 2, 3}}, SemanticContractID: raftlog.ApplicationSemanticContractID{17}}
		store, err := raftlog.Open(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Initialize([]uint64{1, 2, 3}, nil); err != nil {
			t.Fatal(err)
		}
		machine := &capacityRegressionMachine{t: t, store: store}
		d, err := Open(Config{ID: id, Store: store, ApplicationMachine: machine, ApplicationSnapshotSends: ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 4 << 20}})
		if err != nil {
			_ = store.Close()
			t.Fatal(err)
		}
		n.nodes[id], n.machines[id] = d, machine
		t.Cleanup(func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	return n
}

func (n *capacityRegressionNetwork) decode(p Packet) *pb.Message {
	n.t.Helper()
	if p.Snapshot {
		n.t.Fatal("ordinary regression unexpectedly requested snapshot")
	}
	inner, err := DecodeApplicationPacket(n.nodes[p.To].binding, p, n.nodes[p.To].config.MaxPacketBytes)
	if err != nil {
		n.t.Fatal(err)
	}
	m := new(pb.Message)
	if err := proto.Unmarshal(inner.Payload, m); err != nil {
		n.t.Fatal(err)
	}
	return m
}

func (n *capacityRegressionNetwork) pump(queue []Packet, keep func(Packet) bool) error {
	for count := 0; len(queue) > 0; count++ {
		if count >= 10000 {
			n.t.Fatal("bounded transport exhausted")
		}
		p := queue[0]
		queue = queue[1:]
		if keep != nil && !keep(p) {
			continue
		}
		m := n.decode(p)
		if n.holdAppendACKAbove != 0 && p.From != 1 && m.GetType() == pb.MsgAppResp && !m.GetReject() && m.GetIndex() > n.holdAppendACKAbove {
			n.heldReplies = append(n.heldReplies, p)
			continue
		}
		if m.GetType() == pb.MsgApp && len(m.Entries) > 0 {
			n.seen = append(n.seen, capacityRegressionAppend{p.From, p.To, m.GetIndex(), m.GetCommit(), m.Entries[len(m.Entries)-1].GetIndex(), len(m.Entries)})
		}
		out, err := n.nodes[p.To].Step(p)
		if err != nil {
			return fmt.Errorf("original packet %d->%d type=%v prev=%d commit=%d entries=%d: %w", p.From, p.To, m.GetType(), m.GetIndex(), m.GetCommit(), len(m.Entries), err)
		}
		queue = append(queue, out.Packets...)
	}
	return nil
}

func (n *capacityRegressionNetwork) campaign(id uint64, keep func(Packet) bool) {
	n.t.Helper()
	out, err := n.nodes[id].Campaign()
	if err != nil {
		n.t.Fatal(err)
	}
	if err := n.pump(out.Packets, keep); err != nil {
		n.t.Fatal(err)
	}
}

func (n *capacityRegressionNetwork) propose(id, value uint64, keep func(Packet) bool) []Packet {
	n.t.Helper()
	out, err := n.nodes[id].Propose(command(value))
	if err != nil {
		n.t.Fatal(err)
	}
	if keep != nil {
		if err := n.pump(out.Packets, keep); err != nil {
			n.t.Fatal(err)
		}
	}
	return out.Packets
}

// A stale response is replayed unchanged to the original leader. It lets
// Raft itself probe/reject/retransmit the suffix before a heartbeat commits the
// receiver's old stable page. No packet field or authority is manufactured.
func (n *capacityRegressionNetwork) heartbeatResponse(leader, follower uint64) Packet {
	n.t.Helper()
	out, err := n.nodes[leader].Tick()
	if err != nil {
		n.t.Fatal(err)
	}
	for _, p := range out.Packets {
		if p.To != follower || n.decode(p).GetType() != pb.MsgHeartbeat {
			continue
		}
		reply, err := n.nodes[follower].Step(p)
		if err != nil {
			n.t.Fatal(err)
		}
		for _, response := range reply.Packets {
			if n.decode(response).GetType() == pb.MsgHeartbeatResp {
				return response
			}
		}
	}
	n.t.Fatal("genuine heartbeat response absent")
	return Packet{}
}

func (n *capacityRegressionNetwork) catchup(leader uint64) error {
	for range 12 {
		out, err := n.nodes[leader].Tick()
		if err != nil {
			return err
		}
		if err := n.pump(out.Packets, nil); err != nil {
			return err
		}
		if n.nodes[3].Applied() == n.nodes[leader].Applied() {
			return nil
		}
	}
	frontier := func(id uint64) string {
		d := n.nodes[id]
		hard, _, hardErr := d.store.InitialState()
		last, lastErr := d.store.LastIndex()
		return fmt.Sprintf("node%d driverApplied=%d rawApplied=%d commit=%d last=%d stateErr=%v lastErr=%v", id, d.Applied(), d.raw.BasicStatus().Applied, hard.GetCommit(), last, hardErr, lastErr)
	}
	return fmt.Errorf("original packets did not catch up within twelve ticks: leader[%s] follower[%s]", frontier(leader), frontier(3))
}

func (n *capacityRegressionNetwork) assertState(id, index, value uint64) {
	n.t.Helper()
	d := n.nodes[id]
	applied, image, err := d.store.Checkpoint()
	if err != nil {
		n.t.Fatal(err)
	}
	hard, _, err := d.store.InitialState()
	if err != nil {
		n.t.Fatal(err)
	}
	status := d.raw.BasicStatus()
	if applied != index || d.Applied() != index || status.Applied != index || hard.GetCommit() < index || n.machines[id].value != value || len(image) != 8 || binary.BigEndian.Uint64(image) != value {
		n.t.Fatalf("frontier/value divergence node%d store=%d driver=%d raw=%d hardcommit=%d value=%d want index=%d value=%d", id, applied, d.Applied(), status.Applied, hard.GetCommit(), n.machines[id].value, index, value)
	}
	if err := d.store.Scrub(); err != nil {
		n.t.Fatal("post-catchup durable scrub", err)
	}
}

func TestIndependentCapacityOriginalCommittedThreeAndFour(t *testing.T) {
	for _, count := range []int{3, 4} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			n := newCapacityRegressionNetwork(t, false)
			n.campaign(1, nil)
			before := n.nodes[3].Applied()
			for i := range count {
				n.propose(1, uint64(i+1), func(p Packet) bool { return p.To != 3 })
			}
			if n.nodes[3].Applied() != before || n.nodes[1].Applied() != before+uint64(count) {
				t.Fatal("fixture did not commit with a genuinely lagging follower")
			}
			if err := n.catchup(1); err != nil {
				t.Fatalf("legal committed catch-up must progress: %v", err)
			}
			n.assertState(3, n.nodes[1].Applied(), uint64(count*(count+1)/2))
			found := slices.ContainsFunc(n.seen, func(a capacityRegressionAppend) bool { return a.to == 3 && a.entries == count && a.commit >= a.last })
			if !found {
				t.Fatal("fixture did not deliver the original committed bulk Append", n.seen)
			}
		})
	}
}

func TestIndependentCapacityMixedCommittedAndUncommittedTail(t *testing.T) {
	n := newCapacityRegressionNetwork(t, false)
	n.campaign(1, nil)
	for _, v := range []uint64{10, 20, 30} {
		n.propose(1, v, func(p Packet) bool { return p.To != 3 })
	}
	committed := n.nodes[1].Applied()
	n.holdAppendACKAbove = committed
	held := n.propose(1, 40, nil)
	if n.nodes[1].Applied() != committed {
		t.Fatal("tail accidentally committed")
	}
	if err := n.catchup(1); err != nil {
		t.Fatalf("mixed genuine tail must preserve committed progress: %v", err)
	}
	n.assertState(3, committed, 60)
	n.assertState(1, committed, 60)
	leaderHard, _, err := n.nodes[1].store.InitialState()
	if err != nil || leaderHard.GetCommit() != committed || n.nodes[1].raw.BasicStatus().GetCommit() != committed {
		t.Fatal("mixed phase advanced original leader commit before genuine ACK release", leaderHard.GetCommit(), n.nodes[1].raw.BasicStatus().GetCommit(), err)
	}
	hard, _, err := n.nodes[3].store.InitialState()
	if err != nil {
		t.Fatal(err)
	}
	last, err := n.nodes[3].store.LastIndex()
	if err != nil || hard.GetCommit() != committed || last != committed+1 {
		t.Fatal("mixed tail fabricated commit or failed to persist uncommitted suffix", hard.GetCommit(), last, err)
	}
	if slices.Contains(n.machines[3].staged, uint64(40)) {
		t.Fatal("uncommitted tail applied")
	}
	found := slices.ContainsFunc(n.seen, func(a capacityRegressionAppend) bool {
		return a.to == 3 && a.entries == 4 && a.commit == committed && a.last > committed
	})
	if !found {
		t.Fatal("fixture did not deliver an original mixed four-entry Append", n.seen)
	}
	if len(n.heldReplies) == 0 {
		t.Fatal("fixture did not withhold a genuine append acknowledgment of the uncommitted tail")
	}
	// Only now release actual peer ACKs, permitting a real new quorum commit.
	n.holdAppendACKAbove = 0
	if err := n.pump(n.heldReplies, nil); err != nil {
		t.Fatal(err)
	}
	if err := n.pump(held, func(p Packet) bool { return p.To != 3 }); err != nil {
		t.Fatal(err)
	}
	if err := n.catchup(1); err != nil {
		t.Fatal(err)
	}
	n.assertState(3, committed+1, 100)
}

func TestIndependentCapacitySmallCommittedPageKeepsRaftLogReadable(t *testing.T) {
	n := newCapacityRegressionNetwork(t, true)
	n.campaign(1, nil)
	originalResponse := n.heartbeatResponse(1, 3)
	before := n.nodes[3].Applied()
	var original, replies []Packet
	for _, v := range []uint64{10, 20, 30} {
		original = append(original, n.propose(1, v, nil)...)
	}
	for _, p := range original {
		if p.To != 3 {
			continue
		}
		m := n.decode(p)
		if m.GetType() != pb.MsgApp {
			continue
		}
		if m.GetCommit() != before || len(m.Entries) != 1 {
			t.Fatal("fixture pending append is not original single-entry authority", m)
		}
		out, err := n.nodes[3].Step(p)
		if err != nil {
			t.Fatal(err)
		}
		replies = append(replies, out.Packets...)
	}
	last, err := n.nodes[3].store.LastIndex()
	if err != nil || last != before+3 || n.nodes[3].Applied() != before {
		t.Fatal("three genuine unapplied durable rows not installed", last, err)
	}
	page, err := n.nodes[3].store.Entries(before+1, last+1, ^uint64(0))
	if err != nil || len(page) != 2 {
		t.Fatal("actual Store committed-read page is not capped at two", len(page), err)
	}
	if err := n.pump(replies, func(p Packet) bool { return p.To != 3 }); err != nil {
		t.Fatal(err)
	}
	if err := n.pump(original, func(p Packet) bool { return p.To != 3 }); err != nil {
		t.Fatal(err)
	}
	for _, v := range []uint64{7, 8} {
		n.propose(1, v, func(p Packet) bool { return p.To != 3 })
	}
	if err := n.pump([]Packet{originalResponse}, nil); err != nil {
		t.Fatalf("original suffix behind short committed page must progress: %v", err)
	}
	if err := n.catchup(1); err != nil {
		t.Fatalf("bounded committed-page catch-up must reconcile both frontiers: %v", err)
	}
	n.assertState(3, before+5, 75)
	if got := n.machines[3].staged; !slices.Equal(got, []uint64{10, 20, 30, 7, 8}) {
		t.Fatal("actual later Ready pages repeated or omitted an effect", got)
	}
	found := slices.ContainsFunc(n.seen, func(a capacityRegressionAppend) bool {
		return a.to == 3 && a.previous == last && a.entries == 2 && a.commit >= a.last
	})
	if !found {
		t.Fatal("fixture did not deliver the original suffix behind a short stable page", n.seen)
	}
}

func TestIndependentCapacityOverlappingSuffixNeverAppliesOldPayload(t *testing.T) {
	n := newCapacityRegressionNetwork(t, false)
	n.campaign(3, nil)
	before := n.nodes[3].Applied()
	for _, v := range []uint64{91, 92} {
		n.propose(3, v, nil)
	}
	oldLast, err := n.nodes[3].store.LastIndex()
	if err != nil || oldLast != before+2 || n.machines[3].value != 0 {
		t.Fatal("old leader did not retain genuine uncommitted suffix", oldLast, err)
	}
	n.machines[3].forbidden = []uint64{91, 92}
	// Expire the real voter lease without manufacturing a vote or changing
	// Raft internals. Voter 1's PreVote packets stay partitioned/discarded.
	for range 20 {
		if _, err := n.nodes[1].Tick(); err != nil {
			t.Fatal(err)
		}
	}
	n.campaign(2, func(p Packet) bool { return p.To != 3 && p.From != 3 })
	for _, v := range []uint64{7, 8, 9} {
		n.propose(2, v, func(p Packet) bool { return p.To != 3 && p.From != 3 })
	}
	if n.nodes[2].Applied() != before+4 {
		t.Fatal("replacement leader did not commit actual election noop plus three commands")
	}
	if err := n.catchup(2); err != nil {
		t.Fatalf("overlapping genuine committed suffix must replace old values safely: %v", err)
	}
	n.assertState(3, before+4, 24)
	if got := n.machines[3].staged; !slices.Equal(got, []uint64{7, 8, 9}) {
		t.Fatal("replaced payload or duplicate application", got)
	}
	found := slices.ContainsFunc(n.seen, func(a capacityRegressionAppend) bool {
		return a.from == 2 && a.to == 3 && a.previous == before && a.entries == 4 && a.commit >= a.last
	})
	if !found {
		t.Fatal("fixture did not deliver original overlapping four-entry replacement", n.seen)
	}
}
