package txnproto

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

func TestRecipientCachedWritesLostReplyAndIndependentHandoff(t *testing.T) {
	n := allocationNetwork(t)
	service := n.read(t, 1, Query{Kind: AllocatorState})
	p, e := n.hosts[1].TransferAllocator(service)
	n.submit(t, 1, p, e)
	one := activateRecipient(t, n, 1, [16]byte{1})
	two := activateRecipient(t, n, 4, [16]byte{2})
	c1, g1 := acquire(t, n, one, 1, 4)
	c2, g2 := acquire(t, n, two, 1, 4)
	a, e := c1.Next()
	if e != nil {
		t.Fatal(e)
	}
	b, e := c2.Next()
	if e != nil || a == b {
		t.Fatal(a, b, e)
	}
	for _, id := range []uint64{1, 2, 3} {
		n.drop[id] = true
	}
	next, e := c2.Next()
	if e != nil {
		t.Fatal(e)
	}
	x := tx("cachedB", 1, participant(1, 0, Effect{Key: "new", Value: int64(next)}))
	p, e = LocalIDs(x, g2, []uint64{next})
	n.submit(t, 4, p, e)
	d := n.read(t, 4, Query{Kind: Decided, TxID: x.ID})
	if !d.View().Decision.Commit {
		t.Fatal("cached B write required A")
	}
	if _, e = two.Acquire(t.Context(), 2, 2, func(context.Context, Proposal, Query) (Reply, error) { return Reply{}, ErrUnavailable }); !errors.Is(e, idalloc.ErrUnknown) {
		t.Fatal(e)
	}
	if _, e = two.Acquire(t.Context(), 2, 2, func(context.Context, Proposal, Query) (Reply, error) {
		t.Fatal("burned replay delivered")
		return Reply{}, nil
	}); !errors.Is(e, idalloc.ErrAlreadyReserved) {
		t.Fatal(e)
	}
	for _, id := range []uint64{1, 2, 3} {
		delete(n.drop, id)
	}
	replacement := activateRecipient(t, n, 4, [16]byte{2})
	_, fresh := acquire(t, n, replacement, 1, 2)
	x = tx("stale", 1, participant(1, 1, Effect{Key: "bad", Value: 1}))
	p, e = LocalIDs(x, g2, []uint64{b})
	n.submit(t, 4, p, e)
	if !errors.Is(n.hosts[4].machine.last, ErrStale) {
		t.Fatal("old recipient committed", n.hosts[4].machine.last)
	}
	x = tx("other", 0, participant(0, 0, Effect{Key: "ok", Value: int64(a)}))
	p, e = LocalIDs(x, g1, []uint64{a})
	n.submit(t, 1, p, e)
	if n.hosts[1].machine.last != nil {
		t.Fatal("other recipient globally invalidated")
	}
	if fresh.View().Grant.First <= g2.View().Grant.Last {
		t.Fatal("reserved IDs reused after handoff")
	}
}

func allocationConfig(g uint8) Config {
	c := config(g)
	c.Namespace = idalloc.GraphID{42}
	c.AllocatorOwner = [16]byte{99}
	c.AllocatorEpoch = 1
	c.MaxIDBlock = 16
	return c
}
func allocationNetwork(t *testing.T) *network {
	t.Helper()
	n := &network{hosts: map[uint64]*Host{}, files: map[uint64]*vfs.MemFS{}, replies: map[uint64][]Reply{}, drop: map[uint64]bool{}}
	for g := uint8(0); g < 2; g++ {
		base := uint64(g)*3 + 1
		for id := base; id < base+3; id++ {
			fs := vfs.NewCrashableMem()
			s, e := raftlog.Open(raftlog.Config{Dir: fmt.Sprint(id), FS: fs, Create: true})
			if e != nil {
				t.Fatal(e)
			}
			if e = s.Initialize([]uint64{base, base + 1, base + 2}, nil); e != nil {
				t.Fatal(e)
			}
			h, e := OpenHost(allocationConfig(g), s, id)
			if e != nil {
				t.Fatal(e)
			}
			n.hosts[id] = h
			n.files[id] = fs
		}
	}
	t.Cleanup(func() {
		for _, h := range n.hosts {
			h.Close()
		}
	})
	for _, id := range []uint64{1, 4} {
		out, e := n.hosts[id].Campaign()
		n.run(t, id, out, e)
	}
	return n
}
func activateRecipient(t *testing.T, n *network, id uint64, key [16]byte) *RecipientHandle {
	t.Helper()
	var current Proof
	out, e := n.hosts[1].Read(Query{Kind: RecipientState, RecipientID: key})
	n.replies[1] = nil
	n.run(t, 1, out, e)
	for _, r := range n.replies[1] {
		if r.Err == nil {
			current = r.Proof
		}
	}
	p, e := n.hosts[id].BeginRecipient(key, current)
	n.submit(t, 1, p, e)
	begin := n.read(t, 1, Query{Kind: RecipientState, RecipientID: key})
	for _, dest := range []uint64{1, 4} {
		p, e = FenceRecipient(begin)
		n.submit(t, dest, p, e)
	}
	a := n.read(t, 1, Query{Kind: RecipientState, RecipientID: key})
	b := n.read(t, 4, Query{Kind: RecipientState, RecipientID: key})
	p, e = RecordRecipientFence(b)
	n.submit(t, 1, p, e)
	ack := n.read(t, 1, Query{Kind: RecipientAck, RecipientID: key})
	p, e = ActivateRecipient(a, ack)
	n.submit(t, 1, p, e)
	active := n.read(t, 1, Query{Kind: RecipientState, RecipientID: key})
	p, e = PublishRecipient(active)
	n.submit(t, 4, p, e)
	// Opening a replacement in the SAME live Host simulates losing that issuer;
	// production recovery changes the Host incarnation. Old handles remain fenced.
	// OpenRecipient replaces a fenced older handle, preserving at-most-once creation per incarnation.
	proof := n.read(t, id, Query{Kind: RecipientState, RecipientID: key})
	handle, e := n.hosts[id].OpenRecipient(proof)
	if e != nil {
		t.Fatal(e)
	}
	return handle
}
func allocationTransport(t *testing.T, n *network, home uint64) func(context.Context, Proposal, Query) (Reply, error) {
	return func(_ context.Context, intent Proposal, q Query) (Reply, error) {
		service := n.read(t, 1, Query{Kind: AllocatorState})
		p, e := n.hosts[1].AuthorizeReserve(intent, service)
		if e != nil {
			return Reply{}, e
		}
		n.submit(t, 1, p, nil)
		original := n.read(t, 1, q)
		if home != 1 {
			p, e = InstallGrant(original)
			if e != nil {
				return Reply{}, e
			}
			n.submit(t, home, p, nil)
		}
		proof := n.read(t, home, q)
		for _, r := range n.replies[home] {
			if r.ReadID == proof.readID {
				return r, nil
			}
		}
		return Reply{}, ErrUnavailable
	}
}
func acquire(t *testing.T, n *network, h *RecipientHandle, seq, count uint64) (*idalloc.Cursor, Proof) {
	t.Helper()
	home := uint64(h.host.machine.config.Group)*3 + 1
	c, e := h.Acquire(t.Context(), seq, count, allocationTransport(t, n, home))
	if e != nil {
		t.Fatal(e)
	}
	q := grantQuery(idalloc.GrantRequest{Graph: h.host.machine.config.Namespace, Session: h.session, Sequence: seq, Count: count})
	return c, n.read(t, home, q)
}

func TestAllocatorExactRetryServiceRecoveryAndProofCorrelation(t *testing.T) {
	n := allocationNetwork(t)
	seed := n.read(t, 1, Query{Kind: AllocatorState})
	p, e := n.hosts[1].TransferAllocator(seed)
	n.submit(t, 1, p, e)
	h := activateRecipient(t, n, 4, [16]byte{9})
	_, g := acquire(t, n, h, 1, 3)
	current := n.read(t, 1, Query{Kind: AllocatorState})
	r := g.View().Grant.Request
	intent, e := allocationProposal(allocationCommand{Op: "reserve", Request: &r})
	if e != nil {
		t.Fatal(e)
	}
	p, e = n.hosts[1].AuthorizeReserve(intent, current)
	n.submit(t, 1, p, e)
	recovered := n.read(t, 1, grantQuery(r))
	if *recovered.View().Grant != *g.View().Grant {
		t.Fatal("lost response retry allocated a new range")
	}
	bad := r
	bad.Count++
	intent, e = allocationProposal(allocationCommand{Op: "reserve", Request: &bad})
	if e != nil {
		t.Fatal(e)
	}
	p, e = n.hosts[1].AuthorizeReserve(intent, current)
	n.submit(t, 1, p, e)
	if !errors.Is(n.hosts[1].machine.last, ErrMismatch) {
		t.Fatal("payload mismatch accepted")
	}
	// A lost response after commit burns this attempt, though its exact range is recoverable.
	var saved Reply
	_, e = h.Acquire(t.Context(), 2, 2, func(ctx context.Context, p Proposal, q Query) (Reply, error) {
		saved, _ = allocationTransport(t, n, 4)(ctx, p, q)
		return Reply{}, ErrUnavailable
	})
	if !errors.Is(e, idalloc.ErrUnknown) {
		t.Fatal(e)
	}
	if _, e = h.Acquire(t.Context(), 2, 2, func(context.Context, Proposal, Query) (Reply, error) { return saved, nil }); !errors.Is(e, idalloc.ErrAlreadyReserved) {
		t.Fatal(e)
	}
	// A different invocation cannot be fulfilled by a cached receipt/question.
	_, e = h.Acquire(t.Context(), 3, 2, func(context.Context, Proposal, Query) (Reply, error) { return saved, nil })
	if !errors.Is(e, ErrMismatch) {
		t.Fatal("old ReadID/nonce accepted", e)
	}
	// Rotate reservation service ONLY; already cached B blocks remain valid.
	oldService := n.read(t, 1, Query{Kind: AllocatorState})
	oldHigh := oldService.View().Allocator.HighWater
	if e = n.hosts[1].SaveCheckpoint(); e != nil {
		t.Fatal(e)
	}
	crash := n.files[1].CrashClone(vfs.CrashCloneCfg{})
	n.hosts[1].Close()
	s, e := raftlog.Open(raftlog.Config{Dir: "1", FS: crash})
	if e != nil {
		t.Fatal(e)
	}
	host, e := OpenHost(allocationConfig(0), s, 1)
	if e != nil {
		t.Fatal(e)
	}
	n.hosts[1] = host
	n.files[1] = crash
	n.elect(t, 1)
	now := n.read(t, 1, Query{Kind: AllocatorState})
	if now.View().Allocator.HighWater != oldHigh {
		t.Fatal("checked allocator checkpoint lost high-water")
	}
	if _, e = host.AuthorizeReserve(intent, now); !errors.Is(e, ErrStale) {
		t.Fatal("reopened service reused old incarnation", e)
	}
	p, e = host.TransferAllocator(now)
	n.submit(t, 1, p, e)
	_, fresh := acquire(t, n, h, 4, 2)
	if fresh.View().Grant.First <= oldHigh {
		t.Fatal("recovery reused IDs")
	}
	// B's process cannot reopen an existing recipient incarnation from inspection
	// or from A's proof. Its original cached recipient remains independent.
	if _, e = n.hosts[4].OpenRecipient(n.read(t, 1, Query{Kind: RecipientState, RecipientID: h.session.ID})); !errors.Is(e, ErrStale) {
		t.Fatal(e)
	}
}

func TestRecipientHandoffWaitsForIntentsAndRetainsOldDecidedCut(t *testing.T) {
	n := allocationNetwork(t)
	p, e := n.hosts[1].TransferAllocator(n.read(t, 1, Query{Kind: AllocatorState}))
	n.submit(t, 1, p, e)
	h := activateRecipient(t, n, 4, [16]byte{7})
	_, g := acquire(t, n, h, 1, 4)
	x := tx("pendingIDs", 0, participant(0, 0, Effect{Key: "owner", Value: 1}), participant(1, 0, Effect{Key: "posting", Value: 1}))
	p, e = RegisterIDs(x, g, []uint64{g.View().Grant.First})
	n.submit(t, 1, p, e)
	r := n.read(t, 1, Query{Kind: Registration, TxID: x.ID})
	p, e = Prepare(r, Proof{})
	n.submit(t, 1, p, e)
	av := n.read(t, 1, Query{Kind: Prepared, TxID: x.ID})
	p, e = Prepare(r, av)
	n.submit(t, 4, p, e)
	bv := n.read(t, 4, Query{Kind: Prepared, TxID: x.ID})
	for _, v := range []Proof{av, bv} {
		p, e = RecordVote(v)
		n.submit(t, 1, p, e)
	}
	p, e = Decide(r, true)
	n.submit(t, 1, p, e)
	d := n.read(t, 1, Query{Kind: Decided, TxID: x.ID})
	old := n.read(t, 1, Query{Kind: RecipientState, RecipientID: h.session.ID})
	p, e = n.hosts[4].BeginRecipient(h.session.ID, old)
	n.submit(t, 1, p, e)
	begin := n.read(t, 1, Query{Kind: RecipientState, RecipientID: h.session.ID})
	p, e = FenceRecipient(begin)
	n.submit(t, 1, p, e)
	if !errors.Is(n.hosts[1].machine.last, ErrPending) {
		t.Fatal("handoff stranded yes intent")
	}
	// An immutable old decision must install despite pending recipient fencing.
	for _, id := range []uint64{1, 4} {
		p, e = Resolve(d)
		n.submit(t, id, p, e)
		p, e = FenceRecipient(begin)
		n.submit(t, id, p, e)
	}
	a, b := n.read(t, 1, Query{Kind: RecipientState, RecipientID: h.session.ID}), n.read(t, 4, Query{Kind: RecipientState, RecipientID: h.session.ID})
	p, e = ActivateRecipient(a, b)
	n.submit(t, 1, p, e)
	p, e = PublishRecipient(n.read(t, 1, Query{Kind: RecipientState, RecipientID: h.session.ID}))
	n.submit(t, 4, p, e)
	round := d.View().Decision.Round
	var certs []Proof
	for _, id := range []uint64{1, 4} {
		p, e = Fence(round)
		n.submit(t, id, p, e)
		p, e = Certify(round)
		n.submit(t, id, p, e)
		certs = append(certs, n.read(t, id, Query{Kind: Certified, Round: round}))
	}
	c, e := AssembleCut([]uint8{0, 1}, certs)
	if e != nil {
		t.Fatal(e)
	}
	got, e := n.hosts[4].At(c)
	if e != nil {
		t.Fatal(e)
	}
	exact(t, got, map[string]int64{"posting": 1})
}

func TestMalformedAllocationKindsAndMissingClaimEvidence(t *testing.T) {
	m, e := New(allocationConfig(0))
	if e != nil {
		t.Fatal(e)
	}
	m.applied = 1
	before := digest(m.state)
	v := View{Namespace: m.config.Namespace, Graph: m.config.Graph, Topology: 1, Group: 0, Epoch: 7, Index: 1, Kind: AllocatorState, Allocator: &AllocatorView{Owner: [16]byte{99}, Epoch: 1, MaxBlock: 16}}
	p, e := allocationProposal(allocationCommand{Op: "activate", Remote: &v, Other: &v})
	if e != nil {
		t.Fatal(e)
	}
	if err := apply(t, m, p); !errors.Is(err, ErrInvalid) {
		t.Fatal("wrong proof kind accepted", err)
	}
	if digest(m.state) != before {
		t.Fatal("malformed activation mutated state")
	}
	x := tx("missing", 0, participant(0, 0))
	x.Namespace = m.config.Namespace
	x.Claim = &IDClaim{}
	raw, _ := json.Marshal(command{Version: 1, Kind: "local", Tx: &x})
	if e = m.Apply(replica.Entry{Index: m.applied + 1, Term: 1, Data: raw}); !errors.Is(e, ErrInvalid) {
		t.Fatal("decoded claim bypassed proof seam", e)
	}
	if digest(m.state) != before {
		t.Fatal("missing evidence mutated state")
	}
}

func TestAllocationClosedNilAndStrictNamespaceBoundaries(t *testing.T) {
	var h *Host
	var handle *RecipientHandle
	if _, e := h.TransferAllocator(Proof{}); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
	if _, e := h.BeginRecipient([16]byte{1}, Proof{}); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
	if _, e := h.AuthorizeReserve(Proposal{}, Proof{}); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
	if _, e := h.OpenRecipient(Proof{}); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
	if _, e := handle.Acquire(t.Context(), 1, 1, nil); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
	for _, fn := range []func() (Proposal, error){func() (Proposal, error) { return FenceRecipient(Proof{}) }, func() (Proposal, error) { return ActivateRecipient(Proof{}, Proof{}) }, func() (Proposal, error) { return PublishRecipient(Proof{}) }, func() (Proposal, error) { return InstallGrant(Proof{}) }, func() (Proposal, error) { return RecordRecipientFence(Proof{}) }, func() (Proposal, error) { return LocalIDs(Tx{}, Proof{}, nil) }, func() (Proposal, error) { return RegisterIDs(Tx{}, Proof{}, nil) }} {
		if _, e := fn(); !errors.Is(e, ErrUnavailable) {
			t.Fatal(e)
		}
	}
	for _, q := range []Query{{Kind: AllocatorState, RecipientID: [16]byte{1}}, {Kind: RecipientState}, {Kind: GrantState}, {Kind: RecipientAck, RecipientID: [16]byte{1}, Count: 1}, {Kind: Current, Nonce: [16]byte{1}}} {
		if e := checkQuery(q); !errors.Is(e, ErrInvalid) {
			t.Fatal(q, e)
		}
	}
	c := allocationConfig(0)
	c.Namespace = idalloc.GraphID{}
	if _, e := New(c); !errors.Is(e, ErrInvalid) {
		t.Fatal("name silently became namespace", e)
	}
	c = allocationConfig(0)
	c.MaxIDBlock = idalloc.MaxBlockSize + 1
	if _, e := New(c); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
	n := allocationNetwork(t)
	service := n.read(t, 1, Query{Kind: AllocatorState})
	p, e := n.hosts[1].TransferAllocator(service)
	n.submit(t, 1, p, e)
	i := activateRecipient(t, n, 1, [16]byte{1})
	_, g := acquire(t, n, i, 1, 2)
	active := n.read(t, 1, Query{Kind: RecipientState, RecipientID: i.session.ID})
	if _, e = n.hosts[1].OpenRecipient(active); !errors.Is(e, idalloc.ErrAlreadyReserved) {
		t.Fatal(e)
	}
	x := tx("outside", 0, participant(0, 0))
	p, e = LocalIDs(x, g, []uint64{g.View().Grant.Last + 1})
	n.submit(t, 1, p, e)
	if !errors.Is(n.hosts[1].machine.last, ErrInvalid) {
		t.Fatal("out-of-range ID accepted")
	}
	p, e = LocalIDs(x, g, []uint64{g.View().Grant.First, g.View().Grant.First})
	n.submit(t, 1, p, e)
	if !errors.Is(n.hosts[1].machine.last, ErrInvalid) {
		t.Fatal("duplicate ID claim accepted")
	}
	bad := g
	bad.view = bad.View()
	bad.view.Namespace = idalloc.GraphID{99}
	p, e = LocalIDs(x, bad, []uint64{g.View().Grant.First})
	n.submit(t, 1, p, e)
	if !errors.Is(n.hosts[1].machine.last, ErrStale) {
		t.Fatal("foreign namespace accepted")
	}
	service = n.read(t, 1, Query{Kind: AllocatorState})
	n.hosts[1].Close()
	for _, fn := range []func() (Proposal, error){func() (Proposal, error) { return n.hosts[1].TransferAllocator(service) }, func() (Proposal, error) { return n.hosts[1].BeginRecipient([16]byte{3}, Proof{}) }, func() (Proposal, error) { return n.hosts[1].AuthorizeReserve(Proposal{}, service) }} {
		if _, e := fn(); !errors.Is(e, ErrUnavailable) {
			t.Fatal("closed Host generated an incarnation proposal", e)
		}
	}
	if _, e = n.hosts[1].OpenRecipient(active); !errors.Is(e, ErrUnavailable) {
		t.Fatal(e)
	}
}

func TestBeforeQuorumReservationCannotIssueAndLateCommitIsBurned(t *testing.T) {
	n := allocationNetwork(t)
	p, e := n.hosts[1].TransferAllocator(n.read(t, 1, Query{Kind: AllocatorState}))
	n.submit(t, 1, p, e)
	i := activateRecipient(t, n, 4, [16]byte{4})
	service := n.read(t, 1, Query{Kind: AllocatorState})
	high := service.View().Allocator.HighWater
	n.drop[2] = true
	n.drop[3] = true
	var request idalloc.GrantRequest
	_, e = i.Acquire(t.Context(), 1, 2, func(_ context.Context, intent Proposal, q Query) (Reply, error) {
		request = *intent.command.Alloc.Request
		p, e := n.hosts[1].AuthorizeReserve(intent, service)
		if e != nil {
			return Reply{}, e
		}
		out, e := n.hosts[1].Submit(p)
		n.run(t, 1, out, e)
		out, e = n.hosts[1].Read(q)
		n.replies[1] = nil
		n.run(t, 1, out, e)
		if len(n.replies[1]) != 0 {
			t.Fatal("proof before quorum")
		}
		return Reply{}, ErrUnavailable
	})
	if !errors.Is(e, idalloc.ErrUnknown) {
		t.Fatal(e)
	}
	if n.hosts[1].machine.state.Grants[grantKey(request)] != nil {
		t.Fatal("tentative proposal became a reservation")
	}
	delete(n.drop, 2)
	delete(n.drop, 3)
	for range 4 {
		out, e := n.hosts[1].Tick()
		n.run(t, 1, out, e)
	}
	known := n.read(t, 1, grantQuery(request))
	if known.View().Grant.First <= high {
		t.Fatal("late commit corrupted high-water")
	}
	if _, e = i.Acquire(t.Context(), 1, 2, func(context.Context, Proposal, Query) (Reply, error) {
		t.Fatal("burned attempt resubmitted")
		return Reply{}, nil
	}); !errors.Is(e, idalloc.ErrAlreadyReserved) {
		t.Fatal(e)
	}
	_, later := acquire(t, n, i, 2, 2)
	if later.View().Grant.First <= known.View().Grant.Last {
		t.Fatal("uncertain reserved range reused")
	}
}

func TestAllocationReducerRejectsMalformedReplayAndQuotas(t *testing.T) {
	n := allocationNetwork(t)
	seed := n.read(t, 1, Query{Kind: AllocatorState})
	p, e := n.hosts[1].TransferAllocator(seed)
	n.submit(t, 1, p, e)
	i := activateRecipient(t, n, 4, [16]byte{5})
	_, grant := acquire(t, n, i, 1, 2)
	service := n.read(t, 1, Query{Kind: AllocatorState})
	active := n.read(t, 1, Query{Kind: RecipientState, RecipientID: i.session.ID})
	// These are strict reducer replay fixtures, NOT a way to construct runtime authority.
	reject := func(g uint8, a allocationCommand, want error) {
		t.Helper()
		m, _ := New(allocationConfig(g))
		m.state = clone(n.hosts[uint64(g)*3+1].machine.state)
		m.applied = 1
		before := digest(m.state)
		p, e := allocationProposal(a)
		if e != nil {
			t.Fatal(e)
		}
		err := apply(t, m, p)
		if !errors.Is(err, want) {
			t.Fatalf("%s got%v want%v", a.Op, err, want)
		}
		if digest(m.state) != before {
			t.Fatal("rejected transition mutated state")
		}
	}
	sv := service.View()
	gv := n.read(t, 1, grantQuery(grant.View().Grant.Request)).View()
	rv := active.View()
	reject(1, allocationCommand{Op: "service", Remote: &sv, Owner: [16]byte{4}, Epoch: sv.Allocator.Epoch + 1}, ErrStale)
	reject(0, allocationCommand{Op: "service", Remote: &sv, Epoch: sv.Allocator.Epoch + 1}, idalloc.ErrInvalid)
	stale := seed.View()
	reject(0, allocationCommand{Op: "service", Remote: &stale, Owner: [16]byte{4}, Epoch: 2}, idalloc.ErrStaleAuthority)
	reject(0, allocationCommand{Op: "begin", Record: &RecipientRecord{}}, ErrInvalid)
	missing := clone(*rv.Recipient)
	missing.Session.ID = [16]byte{77}
	missing.Previous = 1
	missing.Session.Epoch = 2
	missing.Phase = "pending"
	missing.LastSequence = 0
	missing.ReserveBytes = 0
	missing.ReserveSlots = 0
	missing.Ack = nil
	reject(0, allocationCommand{Op: "begin", Record: &missing}, ErrStale)
	reject(0, allocationCommand{Op: "fence", Remote: &rv}, ErrStale)
	reject(0, allocationCommand{Op: "publish", Remote: &rv}, ErrStale)
	reject(0, allocationCommand{Op: "ack", Remote: &rv}, ErrStale)
	reject(1, allocationCommand{Op: "activate", Remote: &rv, Other: &rv}, ErrStale)
	reject(0, allocationCommand{Op: "activate", Remote: &rv, Other: &rv}, ErrPending)
	reject(1, allocationCommand{Op: "reserve", Request: new(gv.Grant.Request), Remote: &sv}, ErrStale)
	r := gv.Grant.Request
	r.Sequence = 2
	r.Session.ID = [16]byte{77}
	reject(0, allocationCommand{Op: "reserve", Request: &r, Remote: &sv}, ErrStale)
	r.Graph = idalloc.GraphID{}
	reject(0, allocationCommand{Op: "reserve", Request: &r, Remote: &sv}, ErrInvalid)
	r = gv.Grant.Request
	r.Sequence = 2
	reject(0, allocationCommand{Op: "reserve", Request: &r, Remote: &stale}, ErrStale)
	bad := gv
	bad.Grant = new(clone(*gv.Grant))
	bad.Grant.First = 0
	reject(1, allocationCommand{Op: "install", Remote: &bad}, ErrInvalid)
	bad = gv
	bad.Grant = new(clone(*gv.Grant))
	bad.Grant.Owner = [16]byte{}
	reject(1, allocationCommand{Op: "install", Remote: &bad}, idalloc.ErrInvalid)
	bad = gv
	bad.Group = 1
	bad.Epoch = 11
	reject(1, allocationCommand{Op: "install", Remote: &bad}, ErrStale)
	malformed := sv
	malformed.Grant = gv.Grant
	reject(0, allocationCommand{Op: "service", Remote: &malformed, Owner: [16]byte{3}, Epoch: 3}, ErrInvalid)
	nilField := sv
	nilField.Allocator = nil
	reject(0, allocationCommand{Op: "service", Remote: &nilField, Owner: [16]byte{3}, Epoch: 3}, ErrInvalid)
	// All retained grant/recipient quotas reject before publishing allocator high-water.
	m, _ := New(allocationConfig(0))
	m.state = clone(n.hosts[1].machine.state)
	m.config.Limits.Transactions = 1
	r = gv.Grant.Request
	r.Sequence = 2
	before := digest(m.state)
	p, e = allocationProposal(allocationCommand{Op: "reserve", Request: &r, Remote: &sv})
	if err := apply(t, m, must(t, p, e)); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	if digest(m.state) != before {
		t.Fatal("quota changed allocator high-water")
	}
	// Existing exact grant retries remain available at the quota.
	p, e = allocationProposal(allocationCommand{Op: "reserve", Request: new(gv.Grant.Request), Remote: &sv})
	submit(t, m, must(t, p, e))
	tight := allocationConfig(0)
	tight.Limits.CommandBytes = 1024
	tight.Graph = strings.Repeat("\x00", 128)
	small, _ := New(tight)
	if e := small.grantFrameBudget(*gv.Grant); !errors.Is(e, ErrLimit) {
		t.Fatal("oversized delivery accepted", e)
	}
	record := *rv.Recipient
	record.Phase = "pending"
	record.Session.Epoch++
	record.Previous++
	record.LastSequence = 0
	record.ReserveBytes = 0
	record.ReserveSlots = 0
	record.Ack = nil
	if _, _, e := recipientBudget(tight, record); !errors.Is(e, ErrLimit) {
		t.Fatal("unresolvable handoff admitted", e)
	}
	// Checked allocator images fail closed rather than starting again at zero.
	m.state.AllocImage = []byte{1}
	if _, e := m.answer(Query{Kind: AllocatorState}, m.applied); !errors.Is(e, idalloc.ErrCorrupt) {
		t.Fatal(e)
	}
}

func TestDecodedAllocationUnionRequiresCompleteExactShape(t *testing.T) {
	m, _ := New(allocationConfig(0))
	before := digest(m.state)
	for _, a := range []allocationCommand{{Op: "unknown"}, {Op: "service"}, {Op: "begin"}, {Op: "fence"}, {Op: "activate", Remote: &View{}}, {Op: "reserve", Request: &idalloc.GrantRequest{}}, {Op: "publish", Remote: &View{}, Owner: [16]byte{1}}} {
		wire, _ := json.Marshal(command{Version: 1, Kind: "allocation", Alloc: &a})
		if e := m.Apply(replica.Entry{Index: 1, Term: 1, Data: wire}); !errors.Is(e, ErrInvalid) {
			t.Fatalf("%s malformed shape accepted: %v", a.Op, e)
		}
		if digest(m.state) != before {
			t.Fatal("malformed union changed allocator state")
		}
	}
}

func TestBareTransactionConstructorsRejectUnprovenIDClaims(t *testing.T) {
	x := tx("unproven", 0, participant(0, 0))
	x.Claim = &IDClaim{}
	for _, build := range []func(Tx) (Proposal, error){Register, Local} {
		proposal, err := build(x)
		if !errors.Is(err, ErrInvalid) {
			t.Fatalf("bare constructor accepted an ID claim: %v", err)
		}
		if proposal != (Proposal{}) {
			t.Fatal("rejection returned a nonzero proposal")
		}
		if err := validateCommand(proposal.command); !errors.Is(err, ErrInvalid) {
			t.Fatal("rejection left an accepted encoded command", err)
		}
	}
}
