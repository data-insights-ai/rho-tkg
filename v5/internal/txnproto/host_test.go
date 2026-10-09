package txnproto

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

type network struct {
	hosts     map[uint64]*Host
	files     map[uint64]*vfs.MemFS
	replies   map[uint64][]Reply
	drop      map[uint64]bool
	reverse   bool
	duplicate bool
}

func newNetwork(t *testing.T) *network {
	t.Helper()
	n := &network{hosts: map[uint64]*Host{}, files: map[uint64]*vfs.MemFS{}, replies: map[uint64][]Reply{}, drop: map[uint64]bool{}}
	for g := uint8(0); g < 2; g++ {
		peers := []uint64{uint64(g)*3 + 1, uint64(g)*3 + 2, uint64(g)*3 + 3}
		for _, id := range peers {
			fs := vfs.NewCrashableMem()
			s, e := raftlog.Open(raftlog.Config{Dir: fmt.Sprint(id), FS: fs, Create: true})
			if e != nil {
				t.Fatal(e)
			}
			if e = s.Initialize(peers, nil); e != nil {
				t.Fatal(e)
			}
			h, e := OpenHost(config(g), s, id)
			if e != nil {
				t.Fatal(e)
			}
			n.hosts[id] = h
			n.files[id] = fs
		}
	}
	t.Cleanup(func() {
		for _, h := range n.hosts {
			if e := h.Close(); e != nil {
				t.Error(e)
			}
		}
	})
	for _, id := range []uint64{1, 4} {
		e, err := n.hosts[id].Campaign()
		n.run(t, id, e, err)
	}
	return n
}
func (n *network) run(t *testing.T, from uint64, e Event, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	n.replies[from] = append(n.replies[from], e.Replies...)
	queue := e.Packets
	if n.duplicate && len(queue) > 0 {
		queue = append(queue, queue[0])
	}
	for steps := 0; len(queue) > 0; steps++ {
		if steps > 20000 {
			t.Fatal("unbounded test network")
		}
		j := 0
		if n.reverse {
			j = len(queue) - 1
		}
		p := queue[j]
		queue = append(queue[:j], queue[j+1:]...)
		if n.drop[p.To] || n.drop[p.From] {
			continue
		}
		h := n.hosts[p.To]
		if h == nil {
			continue
		}
		out, err := h.Step(p)
		if err != nil {
			t.Fatalf("packet%d->%d: %v", p.From, p.To, err)
		}
		n.replies[p.To] = append(n.replies[p.To], out.Replies...)
		queue = append(queue, out.Packets...)
	}
}
func (n *network) submit(t *testing.T, id uint64, p Proposal, e error) {
	t.Helper()
	if e != nil {
		t.Fatal(e)
	}
	out, err := n.hosts[id].Submit(p)
	n.run(t, id, out, err)
}
func (n *network) read(t *testing.T, id uint64, q Query) Proof {
	t.Helper()
	n.replies[id] = nil
	e, err := n.hosts[id].Read(q)
	n.run(t, id, e, err)
	for _, r := range n.replies[id] {
		if r.Query == q {
			if r.Err != nil {
				t.Fatal(r.Err)
			}
			return r.Proof
		}
	}
	t.Fatal("no authoritative read reply")
	return Proof{}
}
func (n *network) reopen(t *testing.T, id uint64) {
	t.Helper()
	crash := n.files[id].CrashClone(vfs.CrashCloneCfg{})
	if e := n.hosts[id].Close(); e != nil {
		t.Fatal(e)
	}
	s, e := raftlog.Open(raftlog.Config{Dir: fmt.Sprint(id), FS: crash})
	if e != nil {
		t.Fatal(e)
	}
	g := uint8((id - 1) / 3)
	h, e := OpenHost(config(g), s, id)
	if e != nil {
		t.Fatal(e)
	}
	n.hosts[id] = h
	n.files[id] = crash
}

func TestExactlyTwoThreeReplicaGroupsQuorumEvidenceAndRecovery(t *testing.T) {
	n := newNetwork(t)
	n.reverse = true
	n.duplicate = true
	// Dropped follower traffic leaves exactly two durable voters per group.
	n.drop[3] = true
	n.drop[6] = true
	edge := tx("durable-edge", 1, participant(0, 0, Effect{Key: "edge", Value: 8}, Effect{Key: "out", Value: 8}), participant(1, 0, Effect{Key: "in", Value: 8}))
	p, e := Register(edge)
	n.submit(t, 4, p, e)
	r := n.read(t, 4, Query{Kind: Registration, TxID: edge.ID})
	p, e = Prepare(r, Proof{})
	n.submit(t, 1, p, e)
	av := n.read(t, 1, Query{Kind: Prepared, TxID: edge.ID})
	if e = n.hosts[1].SaveCheckpoint(); e != nil {
		t.Fatal(e)
	}
	n.reopen(t, 1)
	// Re-elect after replay; retained prepared lock must still be present.
	n.elect(t, 1)
	var out Event
	var err error
	if n.hosts[1].machine.state.Lock != edge.ID {
		t.Fatal("prepared lock lost at reopen")
	}
	p, e = Prepare(r, av)
	n.submit(t, 4, p, e)
	bv := n.read(t, 4, Query{Kind: Prepared, TxID: edge.ID})
	p, e = RecordVote(av)
	n.submit(t, 4, p, e)
	p, e = RecordVote(bv)
	n.submit(t, 4, p, e)
	p, e = Decide(r, true)
	n.submit(t, 4, p, e)
	// Lose the application decision response and reopen coordinator leader from
	// durable log without an application checkpoint after its decision.
	n.reopen(t, 4)
	n.elect(t, 4)
	d := n.read(t, 4, Query{Kind: Decided, TxID: edge.ID})
	p, e = Register(edge)
	n.submit(t, 4, p, e)
	again := n.read(t, 4, Query{Kind: Decided, TxID: edge.ID})
	if !reflect.DeepEqual(d.View().Decision, again.View().Decision) {
		t.Fatal("lost response reexecuted request")
	}
	// A-only Fresh must collect B coordinator's durable decision.
	ca := n.read(t, 1, Query{Kind: Collection})
	if _, e = ChooseRound([]Proof{ca}, nil, 0); !errors.Is(e, ErrUnavailable) {
		t.Fatal(e)
	}
	round, e := ChooseRound([]Proof{ca}, []Proof{d}, 0)
	if e != nil {
		t.Fatal(e)
	}
	p, e = Resolve(d)
	n.submit(t, 1, p, e)
	for _, id := range []uint64{1, 4} {
		p, e = Fence(round)
		n.submit(t, id, p, e)
		p, e = Certify(round)
		n.submit(t, id, p, e)
	}
	// B has decision but has not installed: its authoritative query is pending.
	n.replies[4] = nil
	out, err = n.hosts[4].Read(Query{Kind: Certified, Round: round})
	n.run(t, 4, out, err)
	if len(n.replies[4]) != 1 || !errors.Is(n.replies[4][0].Err, ErrPending) {
		t.Fatal("half installation certified", n.replies[4])
	}
	p, e = Resolve(d)
	n.submit(t, 4, p, e)
	p, e = Certify(round)
	n.submit(t, 4, p, e)
	pa := n.read(t, 1, Query{Kind: Certified, Round: round})
	pb := n.read(t, 4, Query{Kind: Certified, Round: round})
	c, e := AssembleCut([]uint8{0, 1}, []Proof{pa, pb})
	if e != nil {
		t.Fatal(e)
	}
	compareHostOracle(t, [2]*Host{n.hosts[1], n.hosts[4]}, c, []whole{{edge, round}}, round)
	// Heal/deliver reordered and duplicate traffic to lagging followers, then
	// follower reads use certified application positions, never local max indexes.
	delete(n.drop, 3)
	delete(n.drop, 6)
	for range 6 {
		for _, id := range []uint64{1, 4} {
			out, err = n.hosts[id].Tick()
			n.run(t, id, out, err)
		}
	}
	for _, id := range []uint64{2, 3, 5, 6} {
		if _, e = n.hosts[id].At(c); e != nil {
			t.Fatalf("follower%d: %v", id, e)
		}
	}
	for _, id := range []uint64{1, 4} {
		if e = n.hosts[id].SaveCheckpoint(); e != nil {
			t.Fatal(e)
		}
		n.reopen(t, id)
	}
	compareHostOracle(t, [2]*Host{n.hosts[1], n.hosts[4]}, c, []whole{{edge, round}}, round)
}

func TestHostNeverManufacturesAuthorityDuringQuorumLoss(t *testing.T) {
	n := newNetwork(t)
	if _, e := n.hosts[2].Read(Query{Kind: Collection}); !errors.Is(e, replica.ErrUnavailable) {
		t.Fatal("follower proof", e)
	}
	n.drop[2] = true
	n.drop[3] = true
	out, e := n.hosts[1].Read(Query{Kind: Collection})
	if e != nil {
		t.Fatal(e)
	}
	n.run(t, 1, out, e)
	if len(n.replies[1]) != 0 {
		t.Fatal("isolated leader manufactured proof")
	}
	if _, e := Prepare(Proof{}, Proof{}); !errors.Is(e, ErrUnavailable) {
		t.Fatal(e)
	}
	if _, e := Resolve(Proof{}); !errors.Is(e, ErrUnavailable) {
		t.Fatal(e)
	}
	if _, e := n.hosts[1].Submit(Proposal{}); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
	if _, e := n.hosts[1].Read(Query{Kind: "unknown"}); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
	if _, e := OpenHost(config(0), nil, 1); !errors.Is(e, replica.ErrInvalid) {
		t.Fatal(e)
	}
}

func (n *network) elect(t *testing.T, id uint64) {
	t.Helper()
	base := ((id-1)/3)*3 + 1
	for range 80 {
		for peer := base; peer < base+3; peer++ {
			if n.drop[peer] {
				continue
			}
			out, err := n.hosts[peer].Tick()
			n.run(t, peer, out, err)
		}
		for peer := base; peer < base+3; peer++ {
			if n.drop[peer] {
				continue
			}
			n.replies[peer] = nil
			out, err := n.hosts[peer].Read(Query{Kind: Collection})
			if errors.Is(err, replica.ErrUnavailable) {
				continue
			}
			n.run(t, peer, out, err)
			if len(n.replies[peer]) == 0 {
				continue
			}
			if peer == id {
				return
			}
			out, err = n.hosts[peer].TransferLeader(id)
			n.run(t, peer, out, err)
		}
	}
	t.Fatal("bounded election retry exhausted")
}

func TestHostLeadershipTransferAndNilInputs(t *testing.T) {
	n := newNetwork(t)
	out, e := n.hosts[1].TransferLeader(2)
	n.run(t, 1, out, e)
	p := n.read(t, 2, Query{Kind: Collection})
	if p.View().Group != 0 || p.View().Index == 0 {
		t.Fatal("transfer failed")
	}
	invalid := string([]byte{0xff})
	if _, e = n.hosts[2].Read(Query{Kind: Registration, TxID: invalid}); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
	var h *Host
	for _, fn := range []func() (Event, error){h.Campaign, h.Tick, func() (Event, error) { return h.Step(replica.Packet{}) }, func() (Event, error) { return h.Submit(Proposal{}) }, func() (Event, error) { return h.Read(Query{}) }, func() (Event, error) { return h.TransferLeader(1) }} {
		if _, e := fn(); !errors.Is(e, ErrInvalid) {
			t.Fatal(e)
		}
	}
	if e = h.SaveCheckpoint(); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
	if _, e = h.At(Cut{}); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
	if e = h.Close(); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
}

func compareHostOracle(t *testing.T, hs [2]*Host, c Cut, history []whole, round uint64) {
	t.Helper()
	if _, ok := serialOrder(history); !ok {
		t.Fatal("nonserializable replicated history")
	}
	want := modelAt(history, round)
	for g, h := range hs {
		s, e := h.At(c)
		if e != nil {
			t.Fatal(e)
		}
		if !reflect.DeepEqual(s.Values, want[g]) {
			t.Fatalf("group%d got%v want%v", g, s.Values, want[g])
		}
	}
}

func TestAuthoritativeLocalEntryNeedsOnlyItsOwnGroup(t *testing.T) {
	n := newNetwork(t)
	bBefore := n.hosts[4].driver.Applied()
	for _, id := range []uint64{4, 5, 6} {
		n.drop[id] = true
	}
	x := tx("onlyA", 0, participant(0, 0, Effect{Key: "local", Value: 3}))
	p, e := Local(x)
	n.submit(t, 1, p, e)
	d := n.read(t, 1, Query{Kind: Decided, TxID: x.ID})
	if !d.View().Decision.Commit || len(d.View().Decision.Votes) != 0 {
		t.Fatal("local entry did not decide atomically")
	}
	if n.hosts[4].driver.Applied() != bBefore {
		t.Fatal("local write involved remote group")
	}
	round := d.View().Decision.Round
	p, e = Fence(round)
	n.submit(t, 1, p, e)
	p, e = Certify(round)
	n.submit(t, 1, p, e)
	cp := n.read(t, 1, Query{Kind: Certified, Round: round})
	c, e := AssembleCut([]uint8{0}, []Proof{cp})
	if e != nil {
		t.Fatal(e)
	}
	got, e := n.hosts[1].At(c)
	if e != nil {
		t.Fatal(e)
	}
	exact(t, got, map[string]int64{"local": 3})
	if _, e = n.hosts[4].At(c); !errors.Is(e, ErrUnavailable) {
		t.Fatal(e)
	}
}
