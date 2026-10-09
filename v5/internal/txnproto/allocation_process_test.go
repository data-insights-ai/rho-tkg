package txnproto

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
)

type processDelivery struct {
	p Proposal
	q Query
}
type processCursorResult struct {
	cursor *idalloc.Cursor
	err    error
}
type processAcquisition struct {
	work   chan processDelivery
	reply  chan Reply
	result chan processCursorResult
	cancel context.CancelFunc
}

func processAllocationOperation(op string) bool {
	switch op {
	case "service", "recipient-begin", "recipient-fence", "recipient-ack", "recipient-activate", "recipient-publish", "recipient-open", "authorize", "install-grant", "acquire-begin", "acquire-finish", "acquire-unknown", "mint", "local-ids", "register-ids":
		return true
	}
	return false
}
func processAllocationDispatch(t *testing.T, h *Host, r processRequest, lookup func(string) (Proof, error), recipients map[string]*RecipientHandle, cursors map[string]*idalloc.Cursor, acquisitions map[string]*processAcquisition, out *processResponse) (Proposal, error) {
	switch r.Op {
	case "service", "recipient-fence", "recipient-ack", "recipient-publish", "recipient-open", "install-grant", "local-ids", "register-ids":
		p, e := lookup(r.Handle)
		if e != nil {
			return Proposal{}, e
		}
		switch r.Op {
		case "service":
			return h.TransferAllocator(p)
		case "recipient-fence":
			return FenceRecipient(p)
		case "recipient-ack":
			return RecordRecipientFence(p)
		case "recipient-publish":
			return PublishRecipient(p)
		case "install-grant":
			return InstallGrant(p)
		case "local-ids":
			return LocalIDs(r.Tx, p, r.IDs)
		case "register-ids":
			return RegisterIDs(r.Tx, p, r.IDs)
		case "recipient-open":
			i, e := h.OpenRecipient(p)
			if e != nil {
				return Proposal{}, e
			}
			key := fmt.Sprintf("recipient/%x/%d", i.session.ID, i.session.Epoch)
			recipients[key] = i
			out.Resource = key
			return Proposal{}, nil
		}
	case "recipient-begin":
		var p Proof
		var e error
		if r.Handle != "" {
			p, e = lookup(r.Handle)
		}
		if e != nil {
			return Proposal{}, e
		}
		return h.BeginRecipient(r.RecipientID, p)
	case "recipient-activate":
		p, e := lookup(r.Handle)
		if e != nil {
			return Proposal{}, e
		}
		other, e := lookup(r.Prior)
		if e != nil {
			return Proposal{}, e
		}
		return ActivateRecipient(p, other)
	case "authorize":
		p, e := lookup(r.Handle)
		if e != nil {
			return Proposal{}, e
		}
		if r.Proposal == nil {
			return Proposal{}, ErrInvalid
		}
		c, e := decode[command](r.Proposal.Bytes, DefaultLimits().CommandBytes)
		if e != nil {
			return Proposal{}, e
		}
		return h.AuthorizeReserve(Proposal{command: c}, p)
	case "acquire-begin":
		i := recipients[r.Handle]
		if i == nil {
			return Proposal{}, ErrUnavailable
		}
		if len(acquisitions) >= 8 {
			return Proposal{}, ErrLimit
		}
		ctx, cancel := context.WithCancel(t.Context())
		a := &processAcquisition{work: make(chan processDelivery, 1), reply: make(chan Reply, 1), result: make(chan processCursorResult, 1), cancel: cancel}
		key := fmt.Sprintf("attempt/%s/%d", r.Handle, r.GrantSequence)
		if acquisitions[key] != nil {
			cancel()
			return Proposal{}, idalloc.ErrAlreadyReserved
		}
		acquisitions[key] = a
		go func() {
			c, e := i.Acquire(ctx, r.GrantSequence, r.Count, func(ctx context.Context, p Proposal, q Query) (Reply, error) {
				a.work <- processDelivery{p, q}
				select {
				case reply := <-a.reply:
					return reply, nil
				case <-ctx.Done():
					return Reply{}, ctx.Err()
				}
			})
			a.result <- processCursorResult{c, e}
		}()
		select {
		case work := <-a.work:
			out.Resource = key
			out.AllocationQuery = work.q
			return work.p, nil
		case result := <-a.result:
			delete(acquisitions, key)
			cancel()
			return Proposal{}, result.err
		case <-time.After(10 * time.Second):
			cancel()
			return Proposal{}, ErrUnavailable
		}
	case "acquire-finish", "acquire-unknown":
		a := acquisitions[r.Prior]
		if a == nil {
			return Proposal{}, ErrUnavailable
		}
		reply := Reply{Err: ErrUnavailable}
		if r.Op == "acquire-finish" {
			p, e := lookup(r.Handle)
			if e != nil {
				return Proposal{}, e
			}
			reply = Reply{ReadID: p.readID, Query: p.question, Proof: p}
		}
		a.reply <- reply
		select {
		case result := <-a.result:
			delete(acquisitions, r.Prior)
			a.cancel()
			if result.err != nil {
				return Proposal{}, result.err
			}
			cursors[r.Prior] = result.cursor
			out.Resource = r.Prior
			return Proposal{}, nil
		case <-time.After(10 * time.Second):
			a.cancel()
			return Proposal{}, ErrUnavailable
		}
	case "mint":
		c := cursors[r.Handle]
		if c == nil {
			return Proposal{}, ErrUnavailable
		}
		id, e := c.Next()
		out.ID = id
		return Proposal{}, e
	}
	return Proposal{}, ErrInvalid
}

func (n *processCluster) recipient(home uint64, id [16]byte) string {
	n.t.Helper()
	var prior string
	if _, e := n.readResult(1, Query{Kind: RecipientState, RecipientID: id}); e == nil {
		prior = n.read(home, Query{Kind: RecipientState, RecipientID: id}).Handle
	} else if !errors.Is(e, ErrUnavailable) {
		n.t.Fatal(e)
	}
	n.submit(1, n.build(home, processRequest{Op: "recipient-begin", RecipientID: id, Handle: prior}))
	begin := n.read(1, Query{Kind: RecipientState, RecipientID: id})
	n.submit(1, n.build(1, processRequest{Op: "recipient-fence", Handle: begin.Handle}))
	n.submit(4, n.build(1, processRequest{Op: "recipient-fence", Handle: begin.Handle}))
	a := n.read(1, Query{Kind: RecipientState, RecipientID: id})
	b := n.read(4, Query{Kind: RecipientState, RecipientID: id})
	// The source A child must own BOTH proof handles; B's fence travels as a
	// source-produced proposal back to A, then A reads its installed acknowledgement.
	ack := n.build(4, processRequest{Op: "recipient-ack", Handle: b.Handle})
	n.submit(1, ack)
	other := n.read(1, Query{Kind: RecipientAck, RecipientID: id})
	n.submit(1, n.build(1, processRequest{Op: "recipient-activate", Handle: a.Handle, Prior: other.Handle}))
	active := n.read(1, Query{Kind: RecipientState, RecipientID: id})
	n.submit(4, n.build(1, processRequest{Op: "recipient-publish", Handle: active.Handle}))
	local := n.read(home, Query{Kind: RecipientState, RecipientID: id})
	return n.run(home, processRequest{Op: "recipient-open", Handle: local.Handle}).Resource
}
func (n *processCluster) acquireIDs(home uint64, recipient string, seq, count uint64, lose bool) (string, processEvidence) {
	out := n.run(home, processRequest{Op: "acquire-begin", Handle: recipient, GrantSequence: seq, Count: count})
	if out.Proposal == nil {
		n.t.Fatal("no refill intent")
	}
	service := n.read(1, Query{Kind: AllocatorState})
	p := n.build(1, processRequest{Op: "authorize", Proposal: out.Proposal, Handle: service.Handle})
	n.submit(1, p)
	g := n.read(1, out.AllocationQuery)
	if home != 1 {
		n.submit(home, n.build(1, processRequest{Op: "install-grant", Handle: g.Handle}))
		g = n.read(home, out.AllocationQuery)
	}
	if lose {
		r := n.rpc(home, processRequest{Op: "acquire-unknown", Prior: out.Resource})
		if !errors.Is(processErr(r.Err), idalloc.ErrUnknown) {
			n.t.Fatal(r.Err)
		}
		return "", g
	}
	result := n.run(home, processRequest{Op: "acquire-finish", Handle: g.Handle, Prior: out.Resource})
	return result.Resource, g
}

func TestSixProcessesRecipientIssuanceRecoveryAndCachedWriter(t *testing.T) {
	n := newProcessClusterMode(t, true)
	n.stage = "allocator-service"
	service := n.read(1, Query{Kind: AllocatorState})
	n.submit(1, n.build(1, processRequest{Op: "service", Handle: service.Handle}))
	one := n.recipient(1, [16]byte{1})
	two := n.recipient(4, [16]byte{2})
	cursorA, gA := n.acquireIDs(1, one, 1, 4, false)
	cursorB, gB := n.acquireIDs(4, two, 1, 4, false)
	a := n.run(1, processRequest{Op: "mint", Handle: cursorA}).ID
	b := n.run(4, processRequest{Op: "mint", Handle: cursorB}).ID
	if a == 0 || b == 0 || a == b || gA.View.Grant.Last >= gB.View.Grant.First {
		t.Fatal("global ranges overlap")
	}
	n.stage = "lost-grant-reply"
	_, unknown := n.acquireIDs(4, two, 2, 2, true)
	retry := n.rpc(4, processRequest{Op: "acquire-begin", Handle: two, GrantSequence: 2, Count: 2})
	if !errors.Is(processErr(retry.Err), idalloc.ErrAlreadyReserved) {
		t.Fatal("unknown created a fresh cursor", retry.Err)
	}
	recovered := n.read(1, unknown.Query)
	if *recovered.View.Grant != *unknown.View.Grant {
		t.Fatal("unknown range reallocated")
	}
	n.stage = "cached-B-without-A"
	for _, id := range []uint64{1, 2, 3} {
		n.drop[id] = true
	}
	next := n.run(4, processRequest{Op: "mint", Handle: cursorB}).ID
	x := tx("cached-process", 1, participant(1, 0, Effect{Key: "cached", Value: int64(next)}))
	n.submit(4, n.build(4, processRequest{Op: "local-ids", Handle: gB.Handle, Tx: x, IDs: []uint64{next}}))
	d := n.read(4, Query{Kind: Decided, TxID: x.ID})
	if !d.View.Decision.Commit {
		t.Fatal("B-local commit required A")
	}
	round := d.View.Decision.Round
	n.submit(4, n.build(4, processRequest{Op: "fence", Round: round}))
	n.submit(4, n.build(4, processRequest{Op: "certify", Round: round}))
	old := n.read(4, Query{Kind: Certified, Round: round})
	for _, id := range []uint64{1, 2, 3} {
		delete(n.drop, id)
	}
	n.stage = "recipient-SIGKILL"
	n.run(4, processRequest{Op: "checkpoint"})
	n.crashReopen(4)
	if got := n.rpc(4, processRequest{Op: "mint", Handle: cursorB}); !errors.Is(processErr(got.Err), ErrUnavailable) {
		t.Fatal("cursor restored after crash")
	}
	two = n.recipient(4, [16]byte{2})
	_, fresh := n.acquireIDs(4, two, 1, 2, false)
	if fresh.View.Grant.First <= unknown.View.Grant.Last {
		t.Fatal("reserved remainder reused")
	}
	// Recovered old proof data is for inspection/claims only. Current recipient
	// fences reject old-session new writes; they do not invalidate recipient A.
	legacy := n.read(4, gB.Query)
	x = tx("stale-process", 1, participant(1, 1, Effect{Key: "phantom", Value: 99}))
	n.submit(4, n.build(4, processRequest{Op: "local-ids", Handle: legacy.Handle, Tx: x, IDs: []uint64{b}}))
	if got := n.rpc(4, processRequest{Op: "last"}); !errors.Is(processErr(got.Err), ErrStale) {
		t.Fatal("stale recipient committed", got.Err)
	}
	a2 := n.run(1, processRequest{Op: "mint", Handle: cursorA}).ID
	if a2 != a+1 {
		t.Fatal("other cached writer invalidated")
	}
	old = n.read(4, Query{Kind: Certified, Round: round})
	state := n.run(4, processRequest{Op: "at", Handle: old.Handle}).State
	if state == nil {
		t.Fatal("old cut lost")
	}
	exact(t, *state, map[string]int64{"cached": int64(next)})
	n.stage = "service-SIGKILL"
	high := n.read(1, Query{Kind: AllocatorState}).View.Allocator.HighWater
	n.run(1, processRequest{Op: "checkpoint"})
	n.crashReopen(1)
	service = n.read(1, Query{Kind: AllocatorState})
	if service.View.Allocator.HighWater != high {
		t.Fatal("high-water reset")
	}
	n.submit(1, n.build(1, processRequest{Op: "service", Handle: service.Handle}))
	_, later := n.acquireIDs(4, two, 2, 2, false)
	if later.View.Grant.First <= high {
		t.Fatal("service recovery reused IDs")
	}
	t.Logf("two groups/six processes; exact ranges [%d,%d] [%d,%d]; recovered high=%d; transport events=%d", gA.View.Grant.First, gA.View.Grant.Last, gB.View.Grant.First, gB.View.Grant.Last, high, n.steps)
}

func TestSixProcessesRecipientPendingFenceCrashRecovery(t *testing.T) {
	for _, phase := range []string{"pending", "fenced"} {
		t.Run(phase, func(t *testing.T) {
			n := newProcessClusterMode(t, true)
			n.stage = "recipient-" + phase
			service := n.read(1, Query{Kind: AllocatorState})
			n.submit(1, n.build(1, processRequest{Op: "service", Handle: service.Handle}))
			n.recipient(4, [16]byte{8})
			home := n.read(4, Query{Kind: RecipientState, RecipientID: [16]byte{8}})
			n.submit(1, n.build(4, processRequest{Op: "recipient-begin", RecipientID: [16]byte{8}, Handle: home.Handle}))
			begin := n.read(1, Query{Kind: RecipientState, RecipientID: [16]byte{8}})
			if phase == "fenced" {
				n.submit(1, n.build(1, processRequest{Op: "recipient-fence", Handle: begin.Handle}))
				n.submit(4, n.build(1, processRequest{Op: "recipient-fence", Handle: begin.Handle}))
				b := n.read(4, Query{Kind: RecipientState, RecipientID: [16]byte{8}})
				n.submit(1, n.build(4, processRequest{Op: "recipient-ack", Handle: b.Handle}))
			}
			n.run(4, processRequest{Op: "checkpoint"})
			n.crashReopen(4)
			// Complete the abandoned incarnation through new source-owned proofs.
			begin = n.read(1, Query{Kind: RecipientState, RecipientID: [16]byte{8}})
			n.submit(1, n.build(1, processRequest{Op: "recipient-fence", Handle: begin.Handle}))
			n.submit(4, n.build(1, processRequest{Op: "recipient-fence", Handle: begin.Handle}))
			a := n.read(1, Query{Kind: RecipientState, RecipientID: [16]byte{8}})
			b := n.read(4, Query{Kind: RecipientState, RecipientID: [16]byte{8}})
			n.submit(1, n.build(4, processRequest{Op: "recipient-ack", Handle: b.Handle}))
			ack := n.read(1, Query{Kind: RecipientAck, RecipientID: [16]byte{8}})
			n.submit(1, n.build(1, processRequest{Op: "recipient-activate", Handle: a.Handle, Prior: ack.Handle}))
			active := n.read(1, Query{Kind: RecipientState, RecipientID: [16]byte{8}})
			n.submit(4, n.build(1, processRequest{Op: "recipient-publish", Handle: active.Handle}))
			dead := n.read(4, Query{Kind: RecipientState, RecipientID: [16]byte{8}})
			if reply := n.rpc(4, processRequest{Op: "recipient-open", Handle: dead.Handle}); !errors.Is(processErr(reply.Err), ErrStale) {
				t.Fatal("dead incarnation reopened", reply.Err)
			}
			current := n.recipient(4, [16]byte{8})
			cursor, g := n.acquireIDs(4, current, 1, 2, false)
			if g.View.Grant.Request.Session.Epoch != 3 || n.run(4, processRequest{Op: "mint", Handle: cursor}).ID == 0 {
				t.Fatal("recipient was stranded after incomplete handoff")
			}
		})
	}
}
