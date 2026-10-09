package txnproto

import (
	"encoding/json"
	"errors"
	"math"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestRepeatedQuestionCannotReusePreCommitReadInvocation(t *testing.T) {
	n := newNetwork(t)
	old, err := n.hosts[1].Read(Query{Kind: Current})
	if err != nil {
		t.Fatal(err)
	}
	var delayed []replica.Packet
	for _, p := range old.Packets {
		if p.To != 2 {
			continue
		}
		o, e := n.hosts[2].Step(p)
		if e != nil {
			t.Fatal(e)
		}
		for _, r := range o.Packets {
			m := new(pb.Message)
			if e = proto.Unmarshal(r.Payload, m); e != nil {
				t.Fatal(e)
			}
			if r.To == 1 && m.GetType() == pb.MsgHeartbeatResp {
				delayed = append(delayed, r)
			}
		}
	}
	if len(delayed) == 0 {
		t.Fatal("no old acknowledgement")
	}
	n.drop[1] = true
	n.elect(t, 2)
	x := tx("after-election", 0, participant(0, 0, Effect{Key: "x", Value: 7}))
	p, e := Local(x)
	n.submit(t, 2, p, e)
	current := n.read(t, 2, Query{Kind: Current})
	if current.View().State.Values["x"].Value != 7 {
		t.Fatal("no committed write")
	}
	later, e := n.hosts[1].Read(Query{Kind: Current})
	if e != nil {
		t.Fatal(e)
	}
	if later.ReadID == old.ReadID || later.ReadID == (ReadID{}) {
		t.Fatal("identical questions reused invocation ID")
	}
	if len(later.Replies) > 0 {
		t.Fatal("unexpected immediate response")
	}
	foundOld := false
	for _, r := range delayed {
		o, e := n.hosts[1].Step(r)
		if e != nil {
			t.Fatal(e)
		}
		for _, reply := range o.Replies {
			if reply.Err != nil {
				t.Fatal(reply.Err)
			}
			if reply.ReadID != old.ReadID || reply.ReadID == later.ReadID {
				t.Fatal("old barrier answered new invocation", reply.ReadID)
			}
			foundOld = true
			v := reply.Proof.View()
			if _, ok := v.State.Values["x"]; ok {
				t.Fatal("fresh data")
			}
			if v.Index >= current.View().Index {
				t.Fatal("expected older proof", v.Index)
			}
		}
	}
	if !foundOld {
		t.Fatal("old barrier did not finish")
	}
	// Deliver the later invocation's own packets only after the remote commit.
	// The newer term fences the old leader, so it cannot complete the later read.
	n.replies[1] = nil
	delete(n.drop, 1)
	n.run(t, 1, later, nil)
	for _, reply := range n.replies[1] {
		if reply.ReadID == later.ReadID {
			t.Fatal("later invocation got minority proof")
		}
	}
	if _, err := n.hosts[1].Read(Query{Kind: Current}); !errors.Is(err, replica.ErrUnavailable) {
		t.Fatal(err)
	}
	fresh := n.read(t, 2, Query{Kind: Current})
	if fresh.View().State.Values["x"].Value != 7 {
		t.Fatal("new leader omitted committed write")
	}
}

func TestHostReadInvocationQuotaCancellationAndAcceptanceID(t *testing.T) {
	n := newNetwork(t)
	expected := map[ReadID]bool{}
	var last Event
	var first ReadID
	for j := range 32 {
		event, err := n.hosts[1].Read(Query{Kind: Current})
		if err != nil || event.ReadID == (ReadID{}) || len(event.Replies) != 0 {
			t.Fatal(j, event, err)
		}
		if expected[event.ReadID] {
			t.Fatal("duplicate invocation ID")
		}
		expected[event.ReadID] = true
		last = event
		if j == 0 {
			first = event.ReadID
		}
	}
	refused, err := n.hosts[1].Read(Query{Kind: Current})
	if !errors.Is(err, replica.ErrLimit) || refused.ReadID != (ReadID{}) {
		t.Fatal("same-query overflow", refused, err)
	}
	h := n.hosts[1]
	context, _ := json.Marshal(readRequest{ID: first, Query: Query{Kind: Current}})
	if err = h.driver.CancelReadIndex(context); err != nil {
		t.Fatal(err)
	}
	if _, err = h.driver.ReadIndex(context); !errors.Is(err, replica.ErrInvalid) {
		t.Fatal("cancelled token retried", err)
	}
	// Cancellation keeps RawNode admission reserved until quorum completion.
	if _, err = h.Read(Query{Kind: Current}); !errors.Is(err, replica.ErrLimit) {
		t.Fatal("cancellation released hidden queue", err)
	}
	n.replies[1] = nil
	n.run(t, 1, last, nil)
	seen := map[ReadID]bool{}
	for _, reply := range n.replies[1] {
		if reply.ReadID == first || !expected[reply.ReadID] || seen[reply.ReadID] || reply.Err != nil {
			t.Fatal("misbound/cancelled/duplicate reply", reply)
		}
		seen[reply.ReadID] = true
	}
	if len(seen) != 31 {
		t.Fatal("outstanding reads did not finish", len(seen))
	}
	event, err := h.Read(Query{Kind: Current})
	if err != nil || event.ReadID == (ReadID{}) {
		t.Fatal("quota not released", event, err)
	}
	n.run(t, 1, event, nil)
	h.readSequence = math.MaxUint64
	overflow, err := h.Read(Query{Kind: Current})
	if !errors.Is(err, ErrLimit) || overflow.ReadID != (ReadID{}) {
		t.Fatal("sequence wrapped", overflow, err)
	}
	if h.readSequence != math.MaxUint64 {
		t.Fatal("overflow changed sequence")
	}
}

func TestHostReadIDChangesOnReopenAndRejectsForeignSession(t *testing.T) {
	n := newNetwork(t)
	old, err := n.hosts[1].Read(Query{Kind: Current})
	if err != nil {
		t.Fatal(err)
	}
	if err = n.hosts[1].SaveCheckpoint(); err != nil {
		t.Fatal(err)
	}
	n.reopen(t, 1)
	n.elect(t, 1)
	h := n.hosts[1]
	event, err := h.Read(Query{Kind: Current})
	if err != nil {
		t.Fatal(err)
	}
	if event.ReadID.Session == old.ReadID.Session || event.ReadID == (ReadID{}) {
		t.Fatal("reopen reused session")
	}
	n.replies[1] = nil
	n.run(t, 1, event, nil)
	matched := false
	for _, reply := range n.replies[1] {
		if reply.ReadID == event.ReadID {
			matched = true
		}
		if reply.ReadID == old.ReadID {
			t.Fatal("reopened host delivered old read")
		}
	}
	if !matched {
		t.Fatal("new session read did not finish")
	}
	// Internal malformed output cannot manufacture an ID or a proof.
	for _, id := range []ReadID{old.ReadID, {Session: h.readSession}, {Session: h.readSession, Sequence: h.readSequence + 1}} {
		context, _ := json.Marshal(readRequest{ID: id, Query: Query{Kind: Current}})
		got, err := h.event(replica.Output{Applied: h.driver.Applied(), Reads: []replica.ReadResult{{Index: h.driver.Applied(), Context: context}}}, nil)
		if !errors.Is(err, ErrInvalid) || got.ReadID != (ReadID{}) || len(got.Replies) != 0 {
			t.Fatal("invalid invocation produced proof", got, err)
		}
	}
}

func TestProcessReplyPreservesOlderInvocationSequence(t *testing.T) {
	n := newProcessCluster(t)
	n.stage = "read-invocation"
	old := n.rpc(1, processRequest{Op: "read", Query: Query{Kind: Current}})
	if err := processErr(old.Err); err != nil || old.ReadID == (ReadID{}) {
		t.Fatal(old, err)
	}
	var delayed []replica.Packet
	for _, p := range old.Packets {
		if p.To != 2 {
			continue
		}
		out := n.rpc(2, processRequest{Op: "step", Packet: p})
		if err := processErr(out.Err); err != nil {
			t.Fatal(err)
		}
		for _, packet := range out.Packets {
			m := new(pb.Message)
			if err := proto.Unmarshal(packet.Payload, m); err != nil {
				t.Fatal(err)
			}
			if packet.To == 1 && m.GetType() == pb.MsgHeartbeatResp {
				delayed = append(delayed, packet)
			}
		}
	}
	if len(delayed) == 0 {
		t.Fatal("no prior read acknowledgement")
	}
	n.drop[1] = true
	n.elect(2)
	x := tx("process-after-election", 0, participant(0, 0, Effect{Key: "x", Value: 7}))
	p := n.build(2, processRequest{Op: "local", Tx: x})
	n.submit(2, p)
	fresh := n.read(2, Query{Kind: Current})
	if fresh.View.State.Values["x"].Value != 7 {
		t.Fatal("write not durably installed")
	}
	later := n.rpc(1, processRequest{Op: "read", Query: Query{Kind: Current}})
	if err := processErr(later.Err); err != nil || later.ReadID == old.ReadID || len(later.Evidence) != 0 {
		t.Fatal("later request not distinct", later, err)
	}
	found := false
	for _, packet := range delayed {
		out := n.rpc(1, processRequest{Op: "step", Packet: packet})
		if err := processErr(out.Err); err != nil {
			t.Fatal(err)
		}
		for _, evidence := range out.Evidence {
			found = true
			if evidence.ReadID != old.ReadID || evidence.Sequence != old.Sequence || evidence.ReadID == later.ReadID || evidence.Sequence == later.Sequence || evidence.Err != "" {
				t.Fatal("older reply rebound to later RPC", evidence, old, later)
			}
			if _, ok := evidence.View.State.Values["x"]; ok {
				t.Fatal("expected older snapshot")
			}
		}
	}
	if !found {
		t.Fatal("old request did not receive its own evidence")
	}
	delete(n.drop, 1)
	n.replies[1] = nil
	n.deliver(1, later)
	for _, evidence := range n.replies[1] {
		if evidence.ReadID == later.ReadID {
			t.Fatal("new invocation got stale proof", evidence)
		}
	}
}
