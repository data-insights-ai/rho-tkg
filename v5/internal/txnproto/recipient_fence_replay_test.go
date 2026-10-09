package txnproto

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
)

// Every accepted fence comes from a completed Host read. Either pending or
// fenced observations can be delayed until after the session is active.
func beginReplayRecipient(t *testing.T, n *network, home uint64, key [16]byte, current Proof) (Proof, [2]Proposal) {
	t.Helper()
	p, e := n.hosts[home].BeginRecipient(key, current)
	n.submit(t, 1, p, e)
	pending := n.read(t, 1, Query{Kind: RecipientState, RecipientID: key})
	fence, e := FenceRecipient(pending)
	if e != nil {
		t.Fatal(e)
	}
	for _, dest := range []uint64{1, 4} {
		n.submit(t, dest, fence, nil)
		if e := n.hosts[dest].machine.last; e != nil {
			t.Fatal(e)
		}
		before := clone(n.hosts[dest].machine.state)
		n.submit(t, dest, fence, nil)
		if n.hosts[dest].machine.last != nil || !reflect.DeepEqual(before, clone(n.hosts[dest].machine.state)) {
			t.Fatal("duplicate fence changed fenced state", dest)
		}
	}
	a := n.read(t, 1, Query{Kind: RecipientState, RecipientID: key})
	fenceFenced, e := FenceRecipient(a)
	if e != nil {
		t.Fatal(e)
	}
	return a, [2]Proposal{fence, fenceFenced}
}

func activateReplayRecipient(t *testing.T, n *network, home uint64, key [16]byte, a Proof, publish bool) Proof {
	t.Helper()
	b := n.read(t, 4, Query{Kind: RecipientState, RecipientID: key})
	p, e := RecordRecipientFence(b)
	n.submit(t, 1, p, e)
	ack := n.read(t, 1, Query{Kind: RecipientAck, RecipientID: key})
	p, e = ActivateRecipient(a, ack)
	n.submit(t, 1, p, e)
	active := n.read(t, 1, Query{Kind: RecipientState, RecipientID: key})
	if active.View().Recipient.Phase != "active" {
		t.Fatal("activation not committed")
	}
	if publish {
		p, e = PublishRecipient(active)
		n.submit(t, 4, p, e)
	}
	return n.read(t, home, Query{Kind: RecipientState, RecipientID: key})
}

func TestRecipientFenceReplayPreservesActiveIssuance(t *testing.T) {
	for _, home := range []uint64{1, 4} {
		t.Run(fmt.Sprint(home), func(t *testing.T) {
			n := allocationNetwork(t)
			p, e := n.hosts[1].TransferAllocator(n.read(t, 1, Query{Kind: AllocatorState}))
			n.submit(t, 1, p, e)
			key := [16]byte{71}
			a, fences := beginReplayRecipient(t, n, home, key, Proof{})
			active := activateReplayRecipient(t, n, home, key, a, true)
			h, e := n.hosts[home].OpenRecipient(active)
			if e != nil {
				t.Fatal(e)
			}
			cursor, grant := acquire(t, n, h, 3, 4)
			if n.read(t, 1, Query{Kind: RecipientState, RecipientID: key}).View().Recipient.LastSequence != 3 {
				t.Fatal("fixture lacks issued sequence")
			}
			for _, dest := range []uint64{1, 4} {
				before := clone(n.hosts[dest].machine.state)
				for _, fence := range fences {
					applied := n.hosts[dest].machine.applied
					n.submit(t, dest, fence, nil)
					if e := n.hosts[dest].machine.last; e != nil {
						t.Errorf("same-session replay at %d rejected: %v", dest, e)
					}
					if !reflect.DeepEqual(before, clone(n.hosts[dest].machine.state)) {
						t.Errorf("same-session replay at %d changed retained state: recipient before=%+v after=%+v", dest, before.Recipients[recipientKey(key)], n.hosts[dest].machine.state.Recipients[recipientKey(key)])
					}
					if n.hosts[dest].machine.applied <= applied {
						t.Error("replayed command did not advance applied position")
					}
				}
			}
			id, e := cursor.Next()
			if e != nil || id != 1 {
				t.Fatal("cached cursor", id, e)
			}
			group := uint8((home - 1) / 3)
			x := tx("cached-replay", group, participant(group, 0, Effect{Key: "cached", Value: int64(id)}))
			p, e = LocalIDs(x, grant, []uint64{id})
			n.submit(t, home, p, e)
			if e = n.hosts[home].machine.last; e != nil {
				t.Errorf("cached local write after replay: %v", e)
			}
			if _, e = h.Acquire(t.Context(), 4, 2, allocationTransport(t, n, home)); e != nil {
				t.Errorf("refill after replay: %v", e)
			}
			if t.Failed() {
				return
			}
			exact(t, *n.read(t, home, Query{Kind: Current}).View().State, map[string]int64{"cached": 1})
			if !n.read(t, home, Query{Kind: Decided, TxID: x.ID}).View().Decision.Commit {
				t.Fatal("cached write aborted")
			}
			if got := n.read(t, home, grantQuery(grant.View().Grant.Request)).View().Grant; *got != *grant.View().Grant {
				t.Fatal("retained grant changed")
			}
			// The newer epoch must also refuse a retained old fence.
			newer := activateRecipient(t, n, home, key)
			_, fresh := acquire(t, n, newer, 1, 2)
			for _, dest := range []uint64{1, 4} {
				before := clone(n.hosts[dest].machine.state)
				for _, fence := range fences {
					n.submit(t, dest, fence, nil)
					if !errors.Is(n.hosts[dest].machine.last, ErrStale) {
						t.Fatal("old fence accepted", dest, n.hosts[dest].machine.last)
					}
					if !reflect.DeepEqual(before, clone(n.hosts[dest].machine.state)) {
						t.Fatal("old fence changed current state", dest)
					}
				}
			}
			if newer.session.Epoch != 2 || fresh.View().Grant.First != 7 {
				t.Fatal("new session reused old allocation", newer.session, fresh.View().Grant)
			}
		})
	}
}

func TestRecipientFenceReplayRequiresExactIdentity(t *testing.T) {
	n := allocationNetwork(t)
	key := [16]byte{72}
	a, fences := beginReplayRecipient(t, n, 1, key, Proof{})
	b := n.read(t, 4, Query{Kind: RecipientState, RecipientID: key})
	activateReplayRecipient(t, n, 1, key, a, true)
	activateRecipient(t, n, 4, [16]byte{73})
	// These altered command payloads are rejection fixtures only. Behavioral
	// acceptance above uses untouched opaque Host proofs, never constructed Views.
	cases := []struct {
		name  string
		alter func(*View)
	}{
		{"ID", func(v *View) { v.Recipient.Session.ID[0]++ }},
		{"incarnation", func(v *View) { v.Recipient.Session.Incarnation[0]++ }},
		{"epoch", func(v *View) { v.Recipient.Session.Epoch++ }},
		{"home", func(v *View) { v.Recipient.Home = 1 }},
		{"predecessor", func(v *View) { v.Recipient.Previous += 2 }},
		{"group", func(v *View) { v.Group = 1; v.Epoch = n.hosts[4].machine.config.Epochs[1] }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := Proposal{command: clone(fences[0].command)}
			c.alter(p.command.Alloc.Remote)
			for _, dest := range []uint64{1, 4} {
				before := clone(n.hosts[dest].machine.state)
				n.submit(t, dest, p, nil)
				if !errors.Is(n.hosts[dest].machine.last, ErrStale) {
					t.Fatal("mismatched fence accepted", dest, n.hosts[dest].machine.last)
				}
				if !reflect.DeepEqual(before, clone(n.hosts[dest].machine.state)) {
					t.Fatal("mismatched fence mutated state", dest)
				}
			}
		})
	}
	// A genuine B fenced observation is not an A handoff request either.
	p, e := FenceRecipient(b)
	for _, dest := range []uint64{1, 4} {
		before := clone(n.hosts[dest].machine.state)
		n.submit(t, dest, p, e)
		if !errors.Is(n.hosts[dest].machine.last, ErrStale) || !reflect.DeepEqual(before, clone(n.hosts[dest].machine.state)) {
			t.Fatal("B observation admitted as source fence", dest)
		}
	}
}

func TestRecipientFenceReplayPreservesFencedPredecessorRecovery(t *testing.T) {
	for _, home := range []uint64{1, 4} {
		t.Run(fmt.Sprint(home), func(t *testing.T) {
			n := allocationNetwork(t)
			key := [16]byte{73}
			a, oldFences := beginReplayRecipient(t, n, home, key, Proof{})
			activeA := activateReplayRecipient(t, n, 1, key, a, false)
			if n.read(t, 4, Query{Kind: RecipientState, RecipientID: key}).View().Recipient.Phase != "fenced" {
				t.Fatal("publication loss fixture missing")
			}
			// Home B may recover from A's active proof despite its own prior fenced copy.
			a, fences := beginReplayRecipient(t, n, home, key, activeA)
			active := activateReplayRecipient(t, n, home, key, a, true)
			if active.View().Recipient.Session.Epoch != 2 {
				t.Fatal("fenced predecessor was not replaced")
			}
			for _, dest := range []uint64{1, 4} {
				before := clone(n.hosts[dest].machine.state)
				for _, fence := range fences {
					n.submit(t, dest, fence, nil)
					if n.hosts[dest].machine.last != nil {
						t.Fatal("current replay rejected", dest)
					}
				}
				for _, fence := range oldFences {
					n.submit(t, dest, fence, nil)
					if !errors.Is(n.hosts[dest].machine.last, ErrStale) {
						t.Fatal("old epoch fence accepted", dest)
					}
				}
				if !reflect.DeepEqual(before, clone(n.hosts[dest].machine.state)) {
					t.Fatal("recovery state changed", dest)
				}
			}
		})
	}
}

func TestRecipientFenceReplayDoesNotBypassNewSessionQuiescence(t *testing.T) {
	for _, prepared := range []bool{false, true} {
		t.Run(fmt.Sprint(prepared), func(t *testing.T) {
			n := allocationNetwork(t)
			p, e := n.hosts[1].TransferAllocator(n.read(t, 1, Query{Kind: AllocatorState}))
			n.submit(t, 1, p, e)
			key := [16]byte{74}
			a, oldFences := beginReplayRecipient(t, n, 4, key, Proof{})
			active := activateReplayRecipient(t, n, 4, key, a, true)
			h, e := n.hosts[4].OpenRecipient(active)
			if e != nil {
				t.Fatal(e)
			}
			_, g := acquire(t, n, h, 1, 2)
			x := tx("unresolved", 0, participant(0, 0, Effect{Key: "a", Value: 1}), participant(1, 0, Effect{Key: "b", Value: 1}))
			p, e = RegisterIDs(x, g, []uint64{1})
			n.submit(t, 1, p, e)
			reg := n.read(t, 1, Query{Kind: Registration, TxID: x.ID})
			if prepared {
				p, e = Prepare(reg, Proof{})
				n.submit(t, 1, p, e)
				av := n.read(t, 1, Query{Kind: Prepared, TxID: x.ID})
				p, e = Prepare(reg, av)
				n.submit(t, 4, p, e)
				bv := n.read(t, 4, Query{Kind: Prepared, TxID: x.ID})
				if !av.View().Vote.Yes || !bv.View().Vote.Yes {
					t.Fatal("missing prepared yes fixture")
				}
			}
			// Same-session replays do not quiesce or mutate unrelated ongoing work.
			for _, dest := range []uint64{1, 4} {
				before := clone(n.hosts[dest].machine.state)
				for _, fence := range oldFences {
					n.submit(t, dest, fence, nil)
					if n.hosts[dest].machine.last != nil {
						t.Fatal("current replay rejected", dest)
					}
				}
				if !reflect.DeepEqual(before, clone(n.hosts[dest].machine.state)) {
					t.Fatal("current replay changed unresolved work", dest)
				}
			}
			p, e = n.hosts[4].BeginRecipient(key, active)
			n.submit(t, 1, p, e)
			pending := n.read(t, 1, Query{Kind: RecipientState, RecipientID: key})
			p, e = FenceRecipient(pending)
			dests := []uint64{1}
			if prepared {
				dests = append(dests, 4)
			}
			for _, dest := range dests {
				before := clone(n.hosts[dest].machine.state)
				n.submit(t, dest, p, e)
				if !errors.Is(n.hosts[dest].machine.last, ErrPending) {
					t.Fatal("new session bypassed unresolved work", dest, n.hosts[dest].machine.last)
				}
				if !reflect.DeepEqual(before, clone(n.hosts[dest].machine.state)) {
					t.Fatal("blocked fence changed state", dest)
				}
			}
		})
	}
}

// Existing six-process recovery cases do not retain an old fence until AFTER
// activation and issuance; this case would therefore fail on the old transition.
func TestSixProcessesRecipientFenceReplayAndReopen(t *testing.T) {
	for _, home := range []uint64{1, 4} {
		t.Run(fmt.Sprint(home), func(t *testing.T) {
			n := newProcessClusterMode(t, true)
			key := [16]byte{75}
			service := n.read(1, Query{Kind: AllocatorState})
			n.submit(1, n.build(1, processRequest{Op: "service", Handle: service.Handle}))
			n.submit(1, n.build(home, processRequest{Op: "recipient-begin", RecipientID: key}))
			pending := n.read(1, Query{Kind: RecipientState, RecipientID: key})
			fence := n.build(1, processRequest{Op: "recipient-fence", Handle: pending.Handle})
			for _, dest := range []uint64{1, 4} {
				n.submit(dest, fence)
			}
			fencedA := n.read(1, Query{Kind: RecipientState, RecipientID: key})
			fencedFence := n.build(1, processRequest{Op: "recipient-fence", Handle: fencedA.Handle})
			fencedB := n.read(4, Query{Kind: RecipientState, RecipientID: key})
			n.submit(1, n.build(4, processRequest{Op: "recipient-ack", Handle: fencedB.Handle}))
			ack := n.read(1, Query{Kind: RecipientAck, RecipientID: key})
			n.submit(1, n.build(1, processRequest{Op: "recipient-activate", Handle: fencedA.Handle, Prior: ack.Handle}))
			active := n.read(1, Query{Kind: RecipientState, RecipientID: key})
			n.submit(4, n.build(1, processRequest{Op: "recipient-publish", Handle: active.Handle}))
			local := n.read(home, Query{Kind: RecipientState, RecipientID: key})
			h := n.run(home, processRequest{Op: "recipient-open", Handle: local.Handle}).Resource
			cursor, g := n.acquireIDs(home, h, 3, 4, false)
			for _, dest := range []uint64{1, 4} {
				before := n.read(dest, Query{Kind: RecipientState, RecipientID: key}).View.Recipient
				for _, p := range []*processProposal{fence, fencedFence} {
					n.submit(dest, p)
					if err := processErr(n.rpc(dest, processRequest{Op: "last"}).Err); err != nil {
						t.Fatal("process fence replay rejected", dest, err)
					}
					after := n.read(dest, Query{Kind: RecipientState, RecipientID: key}).View.Recipient
					if !reflect.DeepEqual(before, after) {
						t.Fatal("process fence replay changed session", dest, before, after)
					}
				}
			}
			id := n.run(home, processRequest{Op: "mint", Handle: cursor}).ID
			group := uint8((home - 1) / 3)
			x := tx("replay-process", group, participant(group, 0, Effect{Key: "cached", Value: int64(id)}))
			n.submit(home, n.build(home, processRequest{Op: "local-ids", Handle: g.Handle, IDs: []uint64{id}, Tx: x}))
			if !n.read(home, Query{Kind: Decided, TxID: x.ID}).View.Decision.Commit {
				t.Fatal("process cached write aborted")
			}
			_, refill := n.acquireIDs(home, h, 4, 2, false)
			if id != 1 || refill.View.Grant.First != 5 {
				t.Fatal("process cached/refill range changed", id, refill.View.Grant)
			}
			// No explicit checkpoint: replay actual durable log on SIGKILL/reopen.
			n.crashReopen(home)
			exact(t, *n.read(home, Query{Kind: Current}).View.State, map[string]int64{"cached": 1})
			if got := n.read(home, g.Query).View.Grant; *got != *g.View.Grant {
				t.Fatal("reopen lost old grant")
			}
			now := n.read(home, Query{Kind: RecipientState, RecipientID: key}).View.Recipient
			if now.Phase != "active" || now.Session != g.View.Grant.Request.Session {
				t.Fatal("reopen regressed active session", now)
			}
			replacement := n.recipient(home, key)
			if home == 1 {
				service = n.read(1, Query{Kind: AllocatorState})
				n.submit(1, n.build(1, processRequest{Op: "service", Handle: service.Handle}))
			}
			_, fresh := n.acquireIDs(home, replacement, 1, 2, false)
			for _, dest := range []uint64{1, 4} {
				before := n.read(dest, Query{Kind: RecipientState, RecipientID: key}).View.Recipient
				for _, p := range []*processProposal{fence, fencedFence} {
					n.submit(dest, p)
					if !errors.Is(processErr(n.rpc(dest, processRequest{Op: "last"}).Err), ErrStale) {
						t.Fatal("old process fence accepted", dest)
					}
				}
				if !reflect.DeepEqual(before, n.read(dest, Query{Kind: RecipientState, RecipientID: key}).View.Recipient) {
					t.Fatal("old process fence changed replacement", dest)
				}
			}
			if fresh.View.Grant.First != 7 || fresh.View.Grant.Request.Session.Epoch != 2 {
				t.Fatal("reopen reused reserved range")
			}
		})
	}
}
